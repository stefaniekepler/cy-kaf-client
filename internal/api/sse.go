// sse.go implements writeSSE, this repo's first flusher-based streaming
// response helper. Task 9's GetTopicMessagesV2 (handlers_message.go) is its
// first and, as of this task, only caller: it exposes app's
// MessageService.Browse (a callback-driven "emit one BrowseEvent at a time"
// API) as a Server-Sent-Events HTTP response.
package api

import (
	"encoding/json"
	"net/http"
)

// writeSSE drives one Server-Sent-Events response: it calls produce exactly
// once, handing it a send function that JSON-marshals its argument, frames
// it as one SSE "data: <json>\n\n" event, writes it to w and flushes it to
// the client immediately (so each event reaches the client as soon as it's
// produced, not buffered until the handler returns).
//
// Response headers (Content-Type: text/event-stream, Cache-Control:
// no-cache, Connection: keep-alive) are written lazily -- only on the FIRST
// send call, never up front. That laziness is deliberate, not an
// optimization: it's what lets an error surfaced before any event exists
// (e.g. GetTopicMessagesV2's unknown-cluster lookup, which runs before
// Browse's first EventPhase emit) still turn into an ordinary JSON error
// response instead of a broken/half-open SSE stream. Concretely:
//
//   - produce returns an error and send was never called (headers never
//     written): writeSSE writes nothing at all and returns that error
//     unchanged, so the caller (GetTopicMessagesV2) is still free to write
//     a normal application/json error response with whatever status code
//     the error maps to.
//   - produce returns an error after send was called at least once
//     (headers already written, response already committed to 200 +
//     text/event-stream): there is no way to change the status code
//     anymore. writeSSE just ends the stream and reports success (nil) to
//     the caller -- there's nothing left for the caller to write. This
//     "committed" bookkeeping flips as soon as a send call *begins*
//     writing (before the first w.Write of that call, not after it
//     succeeds): that matches real net/http, where calling Write locks in
//     status 200 the instant it's called, whether or not the underlying
//     transport write itself ends up succeeding -- so even a send whose
//     very first byte fails to go out (client already gone) still counts
//     as committed, and produce's resulting error is swallowed rather than
//     handed to the caller to turn into a second, competing status code.
//   - the client disconnects (r.Context() is canceled by net/http once
//     that happens): send checks r.Context().Err() before doing any work
//     on every call and returns it as an ordinary error the moment it sees
//     one, rather than attempting (and possibly hanging on, or silently
//     succeeding into a socket buffer for) a write nobody will ever read.
//     That error flows back up through the caller's own emit callback --
//     e.g. Browse already treats a non-nil emit error as "stop now" for
//     every mode, tailing included -- so produce returns non-nil, and the
//     already-committed branch above ends the stream cleanly: no panic, no
//     leaked goroutine/loop still browsing records nobody is listening for.
//
// Flusher access goes through http.NewResponseController (Go 1.20+), which
// unwraps any http.ResponseWriter wrapping chain to find the underlying
// http.Flusher, rather than a direct `w.(http.Flusher)` type assertion --
// see TestFlusherSurvivesMiddlewareChain (sse_test.go) for why this
// matters here: server.go's middleware.Recoverer is third-party code this
// package doesn't control, so whether it wraps w in a way that would hide
// a direct type assertion isn't this package's fact to assume either way.
func writeSSE(w http.ResponseWriter, r *http.Request, produce func(send func(v any) error) error) error {
	rc := http.NewResponseController(w)
	headersWritten := false

	send := func(v any) error {
		if err := r.Context().Err(); err != nil {
			return err
		}
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		if !headersWritten {
			h := w.Header()
			h.Set("Content-Type", "text/event-stream")
			h.Set("Cache-Control", "no-cache")
			h.Set("Connection", "keep-alive")
			headersWritten = true
		}
		frame := make([]byte, 0, len("data: ")+len(data)+len("\n\n"))
		frame = append(frame, "data: "...)
		frame = append(frame, data...)
		frame = append(frame, '\n', '\n')
		if _, err := w.Write(frame); err != nil {
			return err
		}
		return rc.Flush()
	}

	err := produce(send)
	if err != nil && !headersWritten {
		return err
	}
	return nil
}
