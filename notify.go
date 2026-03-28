package catbird

import "context"

// Notify sends an ephemeral notification to Wire SSE subscribers via pg NOTIFY.
// Every Wire instance (on any node) picks it up and delivers to its local
// SSE subscribers. No Wire reference needed — works from anywhere.
func Notify(ctx context.Context, conn Conn, topic, event, data string) error {
	_, err := conn.Exec(ctx,
		`SELECT cb_notify(topic => $1, event => $2, data => $3)`,
		topic, event, ptrOrNil(data))
	return err
}
