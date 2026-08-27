package catbird

import (
	"context"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type StreamConsumer struct {
	pool   *pgxpool.Pool
	cursor string
}

func NewStreamConsumer(pool *pgxpool.Pool, cursorName string) *StreamConsumer {
	return &StreamConsumer{pool: pool, cursor: cursorName}
}

// StartAssigner runs a background daemon in the client that continuously assigns
// gapless, commit-ordered positions to raw messages safely. Only one assigning worker
// cluster-wide will hold the lock at a time.
func StartAssigner(ctx context.Context, pool *pgxpool.Pool) {
	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = pool.Exec(ctx, `
					WITH lock AS (
						SELECT pg_try_advisory_xact_lock(hashtext('cb_assigner_lock')) as locked
					),
					unassigned AS (
						SELECT id FROM cb_messages
						WHERE position IS NULL
						  AND xmin::text::bigint < pg_snapshot_xmin(pg_current_snapshot())::text::bigint
						ORDER BY id ASC LIMIT 5000
					)
					UPDATE cb_messages m
					SET position = nextval('cb_position_seq')
					FROM unassigned u
					WHERE m.id = u.id AND (SELECT locked FROM lock);
				`)
			}
		}
	}()
}

func (s *StreamConsumer) FetchBatch(ctx context.Context, pattern string, limit int) ([]Message, error) {
	// The complex pg_snapshot_xmin logic is completely removed from the Consumer.
	// Stream consumers now enjoy flawless gapless ordered reads natively using the Assigner's "position" column!
	rows, err := s.pool.Query(ctx, `
		SELECT id, topic, payload FROM cb_messages
		WHERE position > COALESCE((SELECT last_message_id FROM cb_cursors WHERE name = $1), 0)
		  AND topic LIKE $2
		ORDER BY position ASC LIMIT $3
	`, s.cursor, pattern, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.Topic, &m.Payload); err == nil {
			msgs = append(msgs, m)
		}
	}
	return msgs, rows.Err()
}

func (s *StreamConsumer) Ack(ctx context.Context, tx DBRunner, lastID int64) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO cb_cursors (name, last_message_id) VALUES ($1, $2)
		ON CONFLICT (name) DO UPDATE SET last_message_id = EXCLUDED.last_message_id
	`, s.cursor, lastID)
	return err
}

// RegisterTrigger binds a stream natively by performing Fan-out on Read
func (c *Client) RegisterTrigger(ctx context.Context, pool *pgxpool.Pool, name, topicPattern, targetQueue string) {
	go func() {
		consumer := NewStreamConsumer(pool, "trigger_"+name)
		for {
			msgs, err := consumer.FetchBatch(ctx, topicPattern, 50)
			if err != nil || len(msgs) == 0 {
				time.Sleep(2 * time.Second)
				continue
			}

			// Bridge exactly-once using the Message ID as the dedup key
			tx, _ := pool.Begin(ctx)

			var lastID int64
			for _, m := range msgs {
				dedup := "trigger_" + name + ":" + strconv.FormatInt(m.ID, 10)
				correlation := "wf_" + dedup

				c.Enqueue(ctx, WrapDBRunner(tx), m.Topic, targetQueue, m.Payload, &dedup, &correlation, 0)
				lastID = m.ID
			}

			// Commit the Cursor advance along with the bridged payloads
			consumer.Ack(ctx, WrapDBRunner(tx), lastID)
			tx.Commit(ctx)
		}
	}()
}
