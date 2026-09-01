package wire

import (
	"context"
	"net/http"

	"github.com/ugent-library/catbird"
)

// ServePoll answers one poll for the page holding token. Where the token
// travels is the route's decision — a query parameter, a header, a cookie —
// wire only takes the string. An invalid token answers 401.
func (w *Wire) ServePoll(rw http.ResponseWriter, r *http.Request, token string) {
	t, err := w.Verify(token)
	if err != nil {
		rw.WriteHeader(http.StatusUnauthorized)
		return
	}
	w.Serve(rw, r, t)
}

// Serve is ServePoll past the signature check, for a route that builds or
// verifies the token itself — one that derives the topics from the session.
//
// It reads after the token's cursor with the token's topics, dispatches the
// batch through the renderer, writes every fragment as one HTML response, and
// acks the last position read. Nothing waiting is a 204. Every fragment
// addresses its own element — hx-swap-oob, or whatever the page's library
// reads — so the polling element itself swaps nothing.
func (w *Wire) Serve(rw http.ResponseWriter, r *http.Request, t Token) {
	if t.Cursor == "" {
		// The form where the page holds the position and polls with ?after=
		// is not built yet.
		w.logger.Error("wire: token names no cursor")
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	cursor := catbird.Cursor{Name: t.Cursor, Patterns: t.Topics}
	messages, err := cursor.Read(r.Context(), w.db, w.limit)
	if err != nil {
		w.logger.Error("wire: poll read failed", "cursor", t.Cursor, "err", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	// An empty poll reads and writes nothing: an ack here would touch a
	// cursor row on every idle poll of every open page.
	if len(messages) == 0 {
		rw.WriteHeader(http.StatusNoContent)
		return
	}
	html := w.rd.dispatch(r, messages, w.logger)
	if len(html) == 0 {
		rw.WriteHeader(http.StatusNoContent)
	} else {
		rw.Header().Set("Content-Type", "text/html; charset=utf-8")
		rw.Write(html)
	}

	// The ack comes after the response, on a context the client cannot
	// cancel: sent is seen, so a crash between the two shows a message once
	// more and loses nothing, where acking first could lose what was never
	// sent. It advances past unmatched and dropped messages alike — every
	// message read was named by the token, so a skipped message was allowed,
	// just not rendered.
	last := messages[len(messages)-1].Position
	if err := cursor.Ack(context.WithoutCancel(r.Context()), w.db, last); err != nil {
		w.logger.Error("wire: ack failed, the next poll repeats these messages", "cursor", t.Cursor, "err", err)
	}
}
