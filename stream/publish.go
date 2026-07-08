package stream

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type PublishOpts struct {
	Key       string         // deduplication key: keep-oldest
	Headers   map[string]any // cb_ keys are reserved
	Delay     time.Duration  // relative delayed delivery
	DeliverAt time.Time      // absolute delayed delivery
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
		 FROM cb_stream_publish($1, $2, $3, $4, $5, $6, $7) p`,
		stream, nullText(topic), json.RawMessage(body), headers,
		nullText(o.Key), nullInterval(o.Delay), nullTime(o.DeliverAt),
	).Scan(&ref.Kind, &ref.ID, &ref.Existing)
	if err != nil {
		return Ref{}, wrapErr(err)
	}
	return ref, nil
}

func PublishPayloads(ctx context.Context, conn Conn, stream, topic string, payloads []any) ([]int64, error) {
	arr := make(pgtype.FlatArray[json.RawMessage], 0, len(payloads))
	for _, p := range payloads {
		b, err := json.Marshal(p)
		if err != nil {
			return nil, err
		}
		arr = append(arr, b)
	}

	rows, err := conn.Query(ctx, `SELECT cb_stream_publish_payloads($1, $2, $3)`,
		stream, nullText(topic), arr)
	if err != nil {
		return nil, wrapErr(err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	return ids, wrapErr(err)
}
