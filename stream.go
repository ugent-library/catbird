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

func (s *StreamConsumer) FetchBatch(ctx context.Context, pattern string, limit int) ([]Message, error) {
	// Uses the cb_messages(topic, id) index to make rare-event subset polling blazingly fast (O(log N))
	// snapshot_xmin securely skips uncommitted gaps without background Assigner daemons.
	rows, err := s.pool.Query(ctx, `
		SELECT id, topic, payload FROM cb_messages
		WHERE id > COALESCE((SELECT last_message_id FROM cb_cursors WHERE name = $1), 0)
		  AND topic LIKE $2
		  AND xmin::text::bigint < pg_snapshot_xmin(pg_current_snapshot())::text::bigint
		ORDER BY id ASC LIMIT $3
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
