package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
)

// TestFlusherSurvivesMiddlewareChain is Step 1 of this task (see the task
// brief): before writing writeSSE at all, prove whether http.Flusher
// survives server.go's actual middleware stack (Recoverer -> hostAllowlist
// -> readOnlyGuard) all the way down to a handler, or whether one of them
// wraps the ResponseWriter and hides it. middleware.Recoverer is the only
// third-party middleware in that stack -- this package's own hostAllowlist/
// readOnlyGuard are plain closures that call next.ServeHTTP(w, r) with the
// same w they received (see middleware.go), so they're already known not to
// wrap: Recoverer is the one genuine unknown, per server.go's comment on its
// mount line.
//
// This drives through the REAL NewServer router, not a hand-rebuilt parallel
// middleware chain (a clone would silently drift if server.go's actual
// wiring ever changed -- the whole point of Step 1 is a regression guard on
// that real wiring). NewServer's returned http.Handler is concretely a
// *chi.Mux satisfying chi.Router -- server.go:61 already mounts its own
// ad-hoc route (/actuator/health) directly on that same mux after
// middleware registration, the same pattern this test uses for its probe
// route -- so this type-asserts the handler back to chi.Router and mounts
// one more ad-hoc probe route on it before the first request ever reaches
// the server (chi only forbids Use after routes exist; adding routes is
// always fine, and is exactly what NewServer itself does for the health
// route). The request is then driven over a genuine httptest.Server -- a
// real net/http connection ResponseWriter reaches the handler, not a bare
// httptest.ResponseRecorder (which would trivially satisfy http.Flusher
// regardless of what this test is trying to prove) -- so Recoverer ->
// hostAllowlist -> readOnlyGuard all genuinely run in front of the probe.
func TestFlusherSurvivesMiddlewareChain(t *testing.T) {
	var (
		directAssertOK bool
		flushErr       error
	)
	probeDone := make(chan struct{})

	h := NewServer(Deps{
		IsReadOnly: func(string) bool { return false },
		Static:     fstest.MapFS{},
	})
	router, ok := h.(chi.Router)
	require.True(t, ok, "NewServer must return a chi.Router-satisfying handler so an ad-hoc probe route can be mounted on the real mux")
	router.Get("/__test/sse-probe", func(w http.ResponseWriter, _ *http.Request) {
		defer close(probeDone)
		_, directAssertOK = w.(http.Flusher)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {}\n\n"))
		flushErr = http.NewResponseController(w).Flush()
	})

	srv := httptest.NewServer(h)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/__test/sse-probe", nil)
	require.NoError(t, err)
	req.Host = "127.0.0.1" // hostAllowlist requires this (see middleware.go)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	<-probeDone

	// This is the load-bearing assertion: http.NewResponseController(w).
	// Flush() must work through the real chain. writeSSE is built on this
	// fact, not on the direct assertion below.
	require.NoError(t, flushErr, "http.NewResponseController(w).Flush() must succeed through server.go's real middleware chain")
	t.Logf("flusher passthrough via the real NewServer router: direct w.(http.Flusher) assertion ok=%v, http.NewResponseController(w).Flush() ok=%v (err=%v)",
		directAssertOK, flushErr == nil, flushErr)
}

// --- writeSSE unit tests (package api: writeSSE is unexported) ---

// TestWriteSSE_PreSendErrorWritesNothing is the pre-stream-404 mechanism's
// foundation: produce erroring out before its first send call must leave w
// completely untouched (no headers, no body) so the caller is still free to
// write an ordinary JSON error response.
func TestWriteSSE_PreSendErrorWritesNothing(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/probe", nil)
	sentinel := errors.New("unknown cluster")

	err := writeSSE(rec, req, func(send func(v any) error) error {
		return sentinel // errors out before ever calling send
	})

	require.ErrorIs(t, err, sentinel)
	require.Empty(t, rec.Header().Get("Content-Type"))
	require.Equal(t, 0, rec.Body.Len())
	require.False(t, rec.Flushed)
}

// TestWriteSSE_PostSendErrorEndsStreamCleanly covers the "already committed"
// branch: once at least one event has been sent, the status code can no
// longer change, so a later produce error must be swallowed (nil) rather
// than propagated -- the caller has nothing left to write.
func TestWriteSSE_PostSendErrorEndsStreamCleanly(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/probe", nil)
	sentinel := errors.New("backend blew up mid-stream")

	err := writeSSE(rec, req, func(send func(v any) error) error {
		if sendErr := send(map[string]string{"type": "PHASE"}); sendErr != nil {
			return sendErr
		}
		return sentinel // fails only after one event already went out
	})

	require.NoError(t, err, "an error after >=1 send must be swallowed, not propagated")
	require.Equal(t, "text/event-stream", rec.Header().Get("Content-Type"))
	require.Contains(t, rec.Body.String(), `data: {"type":"PHASE"}`)
}

// TestWriteSSE_LazyHeadersAndFrameFormat locks the exact wire contract: no
// headers/body until the first send, headers set exactly once (not
// re-written on every event), and each frame is "data: <json>\n\n" -- two
// consecutive events must each get their own data:/\n\n pair, not merge.
func TestWriteSSE_LazyHeadersAndFrameFormat(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/probe", nil)

	err := writeSSE(rec, req, func(send func(v any) error) error {
		require.Empty(t, rec.Header().Get("Content-Type"), "headers must not exist before the first send")
		if err := send(map[string]int{"n": 1}); err != nil {
			return err
		}
		require.Equal(t, "text/event-stream", rec.Header().Get("Content-Type"))
		return send(map[string]int{"n": 2})
	})
	require.NoError(t, err)

	require.Equal(t, "text/event-stream", rec.Header().Get("Content-Type"))
	require.Equal(t, "no-cache", rec.Header().Get("Cache-Control"))
	require.Equal(t, "keep-alive", rec.Header().Get("Connection"))

	frames := strings.Split(strings.TrimSuffix(rec.Body.String(), "\n\n"), "\n\n")
	require.Equal(t, []string{`data: {"n":1}`, `data: {"n":2}`}, frames)
	require.True(t, rec.Flushed, "each send must flush")
}

type countingResponseWriter struct {
	http.ResponseWriter
	writes int
}

func (w *countingResponseWriter) Write(p []byte) (int, error) {
	w.writes++
	return w.ResponseWriter.Write(p)
}

func (w *countingResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func TestWriteSSEWritesOneCompleteFramePerEvent(t *testing.T) {
	rec := httptest.NewRecorder()
	w := &countingResponseWriter{ResponseWriter: rec}
	req := httptest.NewRequest(http.MethodGet, "/probe", nil)

	err := writeSSE(w, req, func(send func(any) error) error {
		require.NoError(t, send(map[string]int{"n": 1}))
		return send(map[string]int{"n": 2})
	})

	require.NoError(t, err)
	require.Equal(t, 2, w.writes)
	require.Equal(t, "data: {\"n\":1}\n\ndata: {\"n\":2}\n\n", rec.Body.String())
	require.True(t, rec.Flushed)
}

// TestWriteSSE_ContextCanceledBeforeFirstSendWritesNothing: if the request
// context is already done before produce ever calls send, send must report
// that as an ordinary error without writing anything -- same "nothing
// written" contract as any other pre-send error.
func TestWriteSSE_ContextCanceledBeforeFirstSendWritesNothing(t *testing.T) {
	rec := httptest.NewRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/probe", nil).WithContext(ctx)

	var sendErr error
	err := writeSSE(rec, req, func(send func(v any) error) error {
		sendErr = send(map[string]string{"type": "PHASE"})
		return sendErr
	})

	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, sendErr, context.Canceled)
	require.Equal(t, 0, rec.Body.Len())
}

// TestWriteSSE_ContextCanceledAfterFirstSendEndsStreamCleanly: mirrors
// TestWriteSSE_PostSendErrorEndsStreamCleanly but for the specific "client
// disconnected mid-stream" cause named in the brief -- context canceled
// between two sends must behave exactly like any other post-send error:
// writeSSE returns nil, no panic, and the second event never goes out.
func TestWriteSSE_ContextCanceledAfterFirstSendEndsStreamCleanly(t *testing.T) {
	rec := httptest.NewRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/probe", nil).WithContext(ctx)

	var secondSendErr error
	err := writeSSE(rec, req, func(send func(v any) error) error {
		if err := send(map[string]string{"type": "PHASE"}); err != nil {
			return err
		}
		cancel() // simulate the client going away right after the first event
		secondSendErr = send(map[string]string{"type": "MESSAGE"})
		return secondSendErr
	})

	require.NoError(t, err, "writeSSE must swallow a post-send context cancellation, not propagate it")
	require.ErrorIs(t, secondSendErr, context.Canceled)
	var frames []map[string]any
	for _, p := range strings.Split(strings.TrimSpace(rec.Body.String()), "\n\n") {
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(p, "data: ")), &m))
		frames = append(frames, m)
	}
	require.Len(t, frames, 1, "the second (canceled) event must never have been written")
}

// TestWriteSSE_MarshalErrorIsPreSendError: a value send can't JSON-marshal
// (e.g. a func) must behave exactly like any other pre-send error -- caught
// before headers are touched, since json.Marshal runs before the
// headers-written check.
func TestWriteSSE_MarshalErrorIsPreSendError(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/probe", nil)

	err := writeSSE(rec, req, func(send func(v any) error) error {
		return send(func() {}) // json.Marshal rejects func values
	})

	require.Error(t, err)
	require.Empty(t, rec.Header().Get("Content-Type"))
	require.Equal(t, 0, rec.Body.Len())
}

// failingResponseWriter wraps a real http.ResponseWriter (so Header/
// WriteHeader/the underlying buffer all behave normally) but makes the
// Nth-and-later Write calls fail, to exercise writeSSE's complete-frame
// write error path. It implements Unwrap so http.NewResponseController can
// still reach the wrapped ResponseWriter's Flush -- deliberately dogfooding
// the same wrapping/unwrapping concern Step 1
// (TestFlusherSurvivesMiddlewareChain) exists to guard against.
type failingResponseWriter struct {
	http.ResponseWriter
	failFrom int // 1-indexed: the failFrom'th Write call onward returns an error
	calls    int
}

func (f *failingResponseWriter) Write(p []byte) (int, error) {
	f.calls++
	if f.calls >= f.failFrom {
		return 0, errors.New("simulated write failure")
	}
	return f.ResponseWriter.Write(p)
}

func (f *failingResponseWriter) Unwrap() http.ResponseWriter { return f.ResponseWriter }

// TestWriteSSE_WriteFailureIsTreatedAsAlreadyCommitted exercises the one
// complete-frame Write call failing. Per writeSSE's doc comment, the error
// must be treated as "already committed" (writeSSE returns nil, not the
// error) because headersWritten flips to true before Write is attempted,
// matching real net/http (Write locks in status 200 the instant it's called,
// transport success or not).
func TestWriteSSE_WriteFailureIsTreatedAsAlreadyCommitted(t *testing.T) {
	fw := &failingResponseWriter{ResponseWriter: httptest.NewRecorder(), failFrom: 1}
	req := httptest.NewRequest(http.MethodGet, "/probe", nil)

	err := writeSSE(fw, req, func(send func(v any) error) error {
		return send(map[string]string{"type": "PHASE"})
	})

	require.NoError(t, err, "a write failure must still be swallowed as already-committed")
}
