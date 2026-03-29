package catbird

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ListenerHandler is called when a notification matches a registered pattern.
// Handlers run synchronously in the listener goroutine — don't block.
type ListenerHandler func(topic, message string)

// Listener receives cross-node notifications via pg NOTIFY and dispatches
// them to registered handlers based on topic patterns.
// Create with NewListener, register handlers with Handle, then call Start.
type Listener struct {
	id       string
	pool     *pgxpool.Pool
	logger   *slog.Logger
	schema   string
	handlers []listenerBinding
}

type listenerBinding struct {
	pattern string
	handler ListenerHandler
}

// NewListener creates a new Listener.
func NewListener(pool *pgxpool.Pool) *Listener {
	return &Listener{
		id:     uuid.NewString(),
		pool:   pool,
		logger: slog.Default(),
	}
}

// ID returns the unique identifier for this Listener instance.
// Pass to NotifyOpts.From to skip delivery to this Listener.
func (l *Listener) ID() string {
	return l.id
}

// WithLogger sets the Listener logger.
func (l *Listener) WithLogger(logger *slog.Logger) *Listener {
	l.logger = logger
	return l
}

// Handle registers a handler for the given topic pattern.
// Patterns use the same syntax as Bind: "." separates tokens,
// "*" matches one token, "#" matches zero or more trailing tokens.
// Must be called before Start.
func (l *Listener) Handle(pattern string, fn ListenerHandler) *Listener {
	l.handlers = append(l.handlers, listenerBinding{pattern: pattern, handler: fn})
	return l
}

// Start runs the Listener's LISTEN loop. Blocks until ctx is cancelled.
func (l *Listener) Start(ctx context.Context) error {
	if err := l.pool.QueryRow(ctx, "SELECT current_schema").Scan(&l.schema); err != nil {
		return fmt.Errorf("catbird: listener cannot determine schema: %w", err)
	}

	l.logger.InfoContext(ctx, "listener: started")

	l.listen(ctx)
	return nil
}

func (l *Listener) listen(ctx context.Context) {
	for {
		if err := l.listenOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			l.logger.WarnContext(ctx, "listener: connection lost, reconnecting", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}
}

func (l *Listener) listenOnce(ctx context.Context) error {
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	pgConn := conn.Conn()

	ch := channelName(l.schema, "cb_wire")
	if _, err := pgConn.Exec(ctx, "LISTEN "+pgx.Identifier{ch}.Sanitize()); err != nil {
		return err
	}

	l.logger.InfoContext(ctx, "listener: listening", "channel", ch)

	for {
		notification, waitErr := pgConn.WaitForNotification(ctx)
		if waitErr != nil {
			return waitErr
		}

		var msg wireMessage
		if err := json.Unmarshal([]byte(notification.Payload), &msg); err != nil {
			l.logger.WarnContext(ctx, "listener: invalid NOTIFY payload", "error", err)
			continue
		}

		// Skip messages from this listener
		if msg.SentBy != nil && *msg.SentBy == l.id {
			continue
		}

		l.dispatch(msg)
	}
}

var listenerDispatchPool = sync.Pool{
	New: func() any {
		s := make([]ListenerHandler, 0, 8)
		return &s
	},
}

func (l *Listener) dispatch(msg wireMessage) {
	ptr := listenerDispatchPool.Get().(*[]ListenerHandler)
	matched := (*ptr)[:0]

	for i := range l.handlers {
		if matchTopic(l.handlers[i].pattern, msg.Topic) {
			matched = append(matched, l.handlers[i].handler)
		}
	}

	for _, fn := range matched {
		fn(msg.Topic, msg.Message)
	}

	*ptr = matched[:0]
	listenerDispatchPool.Put(ptr)
}
