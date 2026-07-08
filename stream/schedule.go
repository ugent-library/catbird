package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type CatchUpPolicy string

const (
	CatchUpSkip CatchUpPolicy = "skip"
	CatchUpAll  CatchUpPolicy = "all"
)

// ScheduleOpts is the whole schedule: omitted fields mean the defaults,
// never "keep what is there".
type ScheduleOpts struct {
	Every   time.Duration  // required; a change re-anchors at now + Every
	Topic   string         // "" = no topic
	Payload any            // '{}' by default
	Headers map[string]any // cb_ keys are reserved
	CatchUp CatchUpPolicy  // skip by default
	StartAt time.Time      // next fire; the one deliberate state poke
}

// DefineSchedule declares the schedule that publishes the template message
// to the stream every interval. The call is the whole config; an identical
// declaration writes nothing.
func DefineSchedule(ctx context.Context, conn Conn, stream, name string, opts ScheduleOpts) error {
	if opts.Every <= 0 {
		return fmt.Errorf("catbird: schedule %s.%s needs a positive interval (got %s)",
			stream, name, opts.Every)
	}

	var payload any
	if opts.Payload != nil {
		b, err := json.Marshal(opts.Payload)
		if err != nil {
			return err
		}
		payload = json.RawMessage(b)
	}
	var headers any
	if opts.Headers != nil {
		headers = opts.Headers
	}

	_, err := conn.Exec(ctx,
		`SELECT cb_stream_define_schedule($1, $2, $3, $4, $5, $6, $7, $8)`,
		stream, name, nullInterval(opts.Every), nullText(opts.Topic), payload, headers,
		nullText(string(opts.CatchUp)), nullTime(opts.StartAt))
	return wrapErr(err)
}

func DeleteSchedule(ctx context.Context, conn Conn, stream, name string) (bool, error) {
	var deleted bool
	err := conn.QueryRow(ctx, `SELECT cb_stream_delete_schedule($1, $2)`,
		stream, name).Scan(&deleted)
	return deleted, wrapErr(err)
}
