package catbird

import "context"

// NotifyOpts configures notification delivery.
type NotifyOpts struct {
	// SentBy identifies the sender. Wire and Listener instances that match
	// this ID will skip delivery, avoiding echo. Use wire.ID() or listener.ID().
	SentBy string
}

// Notify sends an ephemeral notification via pg NOTIFY.
// Every Wire and Listener instance (on any node) picks it up and delivers
// to its local subscribers. Set NotifyOpts.SentBy to skip delivery to the sender.
func Notify(ctx context.Context, conn Conn, topic, message string, opts ...NotifyOpts) error {
	var sentBy *string
	if len(opts) > 0 && opts[0].SentBy != "" {
		sentBy = &opts[0].SentBy
	}
	_, err := conn.Exec(ctx,
		`SELECT cb_notify(topic => $1, message => $2, sent_by => $3)`,
		topic, ptrOrNil(message), sentBy)
	return err
}
