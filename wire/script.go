package wire

import (
	_ "embed"
	"net/http"
)

//go:embed wire.js
var script []byte

// ServeScript serves the provided browser glue (wire.js): EventSource →
// DOM CustomEvents plus declarative data-wire-swap, for pages not using
// the htmx SSE extension. The frames themselves are plain SSE, so using
// this script, the htmx extension, or a hand-rolled listener are all the
// same contract.
func (w *Wire) ServeScript(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	rw.Header().Set("Cache-Control", "no-cache")
	_, _ = rw.Write(script)
}
