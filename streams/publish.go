package streams

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
)

type PublishOpts struct {
	Key        string         // deduplication key: keep-oldest
	Headers    map[string]any // cb_ keys are reserved
	Recipients []string       // who the message is for; read back as Message.Recipients, matched as $.recipients
	Delay      time.Duration  // relative delayed delivery
	DeliverAt  time.Time      // absolute delayed delivery
}

func Publish(ctx context.Context, conn Conn, stream, topic string, payload any, opts ...PublishOpts) (Ref, error) {
	var o PublishOpts
	if len(opts) > 0 {
		o = opts[0]
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return Ref{}, err
	}
	headers := o.Headers
	if headers == nil {
		headers = map[string]any{}
	}

	var ref Ref
	err = conn.QueryRow(ctx,
		`SELECT p.ref_kind, p.ref_id, p.existing
		 FROM cb_stream_publish($1, $2, $3, $4, $5, $6, $7, $8) p`,
		stream, nullText(topic), json.RawMessage(body), headers, o.Recipients,
		nullText(o.Key), nullInterval(o.Delay), nullTime(o.DeliverAt),
	).Scan(&ref.Kind, &ref.ID, &ref.Existing)
	if err != nil {
		return Ref{}, wrapErr(err)
	}
	return ref, nil
}

// BatchMessage is one element of PublishMessages. Zero fields mean the
// same as their Publish counterparts: no topic, no headers, no
// recipients, no deduplication, immediate delivery.
type BatchMessage struct {
	Topic      string
	Payload    any
	Headers    map[string]any // cb_ keys are reserved
	Recipients []string       // who the message is for; read back as Message.Recipients, matched as $.recipients
	Key        string         // deduplication key: keep-oldest
	Delay      time.Duration  // relative delayed delivery
	DeliverAt  time.Time      // absolute delayed delivery; not with Delay
}

// PublishMessages publishes several messages in one call: the batch
// equivalent of calling Publish once per message, atomic when conn is a
// transaction. Returns one Ref per message, in input order.
func PublishMessages(ctx context.Context, conn Conn, stream string, msgs []BatchMessage) ([]Ref, error) {
	if len(msgs) == 0 {
		return nil, nil
	}

	// the envelope keys the SQL side reads; zero fields are left out
	type envelope struct {
		Topic      string         `json:"topic,omitempty"`
		Payload    any            `json:"payload"`
		Headers    map[string]any `json:"headers,omitempty"`
		Recipients []string       `json:"recipients,omitempty"`
		Key        string         `json:"key,omitempty"`
		Delay      float64        `json:"delay,omitempty"` // seconds
		DeliverAt  *time.Time     `json:"deliver_at,omitempty"`
	}
	envs := make([]envelope, len(msgs))
	for i, m := range msgs {
		envs[i] = envelope{
			Topic:      m.Topic,
			Payload:    m.Payload,
			Headers:    m.Headers,
			Recipients: m.Recipients,
			Key:        m.Key,
			Delay:      m.Delay.Seconds(),
		}
		if !m.DeliverAt.IsZero() {
			envs[i].DeliverAt = &msgs[i].DeliverAt
		}
	}

	b, err := json.Marshal(envs)
	if err != nil {
		return nil, err
	}

	rows, err := conn.Query(ctx, `SELECT p.ref_kind, p.ref_id, p.existing
		FROM cb_stream_publish_messages($1, $2) p`, stream, json.RawMessage(b))
	if err != nil {
		return nil, wrapErr(err)
	}
	refs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Ref])
	return refs, wrapErr(err)
}
