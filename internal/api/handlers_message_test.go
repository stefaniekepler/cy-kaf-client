package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/api"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
)

// fakeMessageServicer implements api.MessageServicer for GetTopicMessagesV2
// tests: same "map presence = known cluster" convention as
// fakeSerdesServicer/fakeTopicServicer -- a cluster name with no registered
// behavior func reports appcluster.ErrUnknownCluster, exactly like Browse's
// real res.Lookup does for a name the Resolver doesn't recognize, before any
// event is ever emitted (see app/cluster/message.go's Browse: Lookup is
// literally its first statement).
//
// sendErr/sendKnown/lastSendSpec/lastSendTopic and deleteErr/deleteKnown/
// lastDeletePartitions/lastDeleteTopic (P1c Task 11) mirror
// fakeGroupServicer's "error map wins, else result-map presence = known
// cluster" convention (handlers_group_test.go) for Send/Delete.
type fakeMessageServicer struct {
	mu        sync.Mutex
	behavior  map[string]func(ctx context.Context, emit func(appcluster.BrowseEvent) error) error
	lastSpec  appcluster.BrowseSpec
	lastTopic string

	sendErr       map[string]error
	sendKnown     map[string]bool
	lastSendSpec  appcluster.SendSpec
	lastSendTopic string

	deleteErr            map[string]error
	deleteKnown          map[string]bool
	lastDeletePartitions []int32
	lastDeleteTopic      string
}

func newFakeMessageServicer() *fakeMessageServicer {
	return &fakeMessageServicer{
		behavior:    map[string]func(context.Context, func(appcluster.BrowseEvent) error) error{},
		sendErr:     map[string]error{},
		sendKnown:   map[string]bool{},
		deleteErr:   map[string]error{},
		deleteKnown: map[string]bool{},
	}
}

func (f *fakeMessageServicer) Browse(ctx context.Context, name, topic string, spec appcluster.BrowseSpec, emit func(appcluster.BrowseEvent) error) error {
	f.mu.Lock()
	f.lastSpec = spec
	f.lastTopic = topic
	fn, ok := f.behavior[name]
	f.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return fn(ctx, emit)
}

func (f *fakeMessageServicer) Send(_ context.Context, name, topic string, spec appcluster.SendSpec) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastSendSpec = spec
	f.lastSendTopic = topic
	if err, ok := f.sendErr[name]; ok {
		return err
	}
	if !f.sendKnown[name] {
		return fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return nil
}

func (f *fakeMessageServicer) Delete(_ context.Context, name, topic string, partitions []int32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastDeletePartitions = partitions
	f.lastDeleteTopic = topic
	if err, ok := f.deleteErr[name]; ok {
		return err
	}
	if !f.deleteKnown[name] {
		return fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return nil
}

// withMessages wires fs as Deps.Messages -- same pattern as withSerdes/
// withGroups/withTopics.
func withMessages(fs *fakeMessageServicer) testServerOption {
	return func(d *api.Deps) { d.Messages = fs }
}

// --- single-event contract validation ---
//
// getTopicMessagesV2's 200 response is text/event-stream carrying one
// TopicMessageEvent JSON object per SSE frame -- not one JSON body. The
// contract models it as `array of TopicMessageEvent` purely because OpenAPI
// has no native "one object per frame" shape, and openapi3filter.
// ValidateResponse has no content decoder for text/event-stream to begin
// with (it would need application/json to parse the body at all). So
// instead of the full validateAgainstContract (contractRouterFor +
// ValidateResponse, see contract_test.go), each decoded frame is validated
// directly against the TopicMessageEvent *component* schema via kin-
// openapi's Schema.VisitJSON -- same loaded contract/openapi.yaml, same
// schema, just applied to one decoded event instead of a whole response
// envelope. This is a test-only helper (see this task's brief).
var (
	topicMessageEventSchemaOnce sync.Once
	topicMessageEventSchema     *openapi3.Schema
	topicMessageEventSchemaErr  error
)

func loadTopicMessageEventSchema(t *testing.T) *openapi3.Schema {
	t.Helper()
	topicMessageEventSchemaOnce.Do(func() {
		doc, err := openapi3.NewLoader().LoadFromFile(contractPath(t))
		if err != nil {
			topicMessageEventSchemaErr = err
			return
		}
		ref, ok := doc.Components.Schemas["TopicMessageEvent"]
		if !ok || ref.Value == nil {
			topicMessageEventSchemaErr = fmt.Errorf("contract/openapi.yaml missing components.schemas.TopicMessageEvent")
			return
		}
		topicMessageEventSchema = ref.Value
	})
	require.NoError(t, topicMessageEventSchemaErr)
	return topicMessageEventSchema
}

func validateEventAgainstContract(t *testing.T, event map[string]any) {
	t.Helper()
	require.NoError(t, loadTopicMessageEventSchema(t).VisitJSON(event))
}

// parseSSEFrames splits a raw SSE response body on the blank-line frame
// separator, strips each frame's "data: " prefix, and JSON-decodes it --
// mirrors what @microsoft/fetch-event-source (the vendored frontend's real
// v2 client, per this task's brief) does on the wire.
func parseSSEFrames(t *testing.T, body []byte) []map[string]any {
	t.Helper()
	raw := strings.TrimSpace(string(body))
	if raw == "" {
		return nil
	}
	var frames []map[string]any
	for _, part := range strings.Split(raw, "\n\n") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		require.True(t, strings.HasPrefix(part, "data: "), "every SSE frame must start with 'data: ': %q", part)
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(part, "data: ")), &m))
		frames = append(frames, m)
	}
	return frames
}

// --- GetTopicMessagesV2 ---

// TestGetTopicMessagesV2_StreamsPhaseMessageDoneWithCursor is this task's
// main-path test, covering brief cases ①-④ in one flow: Content-Type is
// text/event-stream (①), every frame passes the TopicMessageEvent contract
// schema (②), every frame's type is one of the 4 real event kinds (③), and
// the terminal DONE frame carries a non-empty cursor.id (④) -- Browse's
// resume-cursor contract (CursorID rides on EventDone, see
// app/cluster/message.go's BrowseEvent doc comment; there is no separate
// CURSOR event kind).
func TestGetTopicMessagesV2_StreamsPhaseMessageDoneWithCursor(t *testing.T) {
	fake := newFakeMessageServicer()
	fake.behavior["prod"] = func(_ context.Context, emit func(appcluster.BrowseEvent) error) error {
		if err := emit(appcluster.BrowseEvent{Kind: appcluster.EventPhase, Phase: "Consuming"}); err != nil {
			return err
		}
		msg := &appcluster.DecodedMessage{
			Partition: 3, Offset: 42, TimestampMs: 1_700_000_000_000, TimestampType: "CREATE_TIME",
			Key: "k1", Value: "v1", KeySerde: "String", ValueSerde: "String",
			KeySize: 2, ValueSize: 2, HeadersSize: 0,
		}
		if err := emit(appcluster.BrowseEvent{Kind: appcluster.EventMessage, Message: msg}); err != nil {
			return err
		}
		return emit(appcluster.BrowseEvent{Kind: appcluster.EventDone, CursorID: "cursor-123"})
	}
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	_, code, hdr, body := doJSON(t, http.MethodGet, srv, "/api/clusters/prod/topics/t1/messages/v2", nil)
	require.Equal(t, 200, code)
	require.Equal(t, "text/event-stream", hdr.Get("Content-Type")) // ①

	frames := parseSSEFrames(t, body)
	require.Len(t, frames, 3)

	wantTypes := []string{"PHASE", "MESSAGE", "DONE"}
	for i, f := range frames {
		validateEventAgainstContract(t, f) // ②
		typ, _ := f["type"].(string)
		require.Contains(t, []string{"PHASE", "MESSAGE", "CONSUMING", "DONE"}, typ) // ③
		require.Equal(t, wantTypes[i], typ)
	}

	cursor, ok := frames[2]["cursor"].(map[string]any) // ④
	require.True(t, ok, "the DONE frame must carry a cursor object")
	require.NotEmpty(t, cursor["id"])

	msgFrame := frames[1]["message"].(map[string]any)
	require.InDelta(t, float64(42), msgFrame["offset"], 0)
	require.InDelta(t, float64(3), msgFrame["partition"], 0)
	require.Equal(t, "k1", msgFrame["key"])
	require.Equal(t, "v1", msgFrame["value"])
	require.NotEmpty(t, msgFrame["timestamp"]) // required non-pointer time.Time must always be set
	require.True(t, strings.HasSuffix(msgFrame["timestamp"].(string), "Z"),
		"timestamp must serialize in UTC (Z suffix), got %q", msgFrame["timestamp"])
}

// TestGetTopicMessagesV2_UnknownClusterIs404BeforeStreaming is brief case ⑤:
// Browse's first act is res.Lookup, before the first EventPhase send, so an
// unknown-cluster error must surface as an ordinary 404 JSON response, not a
// half-open/broken SSE stream.
func TestGetTopicMessagesV2_UnknownClusterIs404BeforeStreaming(t *testing.T) {
	fake := newFakeMessageServicer() // no behavior registered for "nope" -> ErrUnknownCluster
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	req, code, hdr, body := doJSON(t, http.MethodGet, srv, "/api/clusters/nope/topics/t1/messages/v2", nil)
	require.Equal(t, 404, code)
	require.Equal(t, "application/json", hdr.Get("Content-Type"))
	require.NotContains(t, hdr.Get("Content-Type"), "event-stream")
	assertErrorEnvelope(t, body, "cluster not found", "")
	// Contract declares 200/400 for this operation, not 404 -- an undocumented
	// status is a no-op pass for openapi3filter.ValidateResponse (see
	// GetSerdes' precedent, handlers_serde_test.go), kept here for parity.
	validateAgainstContract(t, req, code, hdr, body)
}

// TestGetTopicMessagesV2_BackendFailureIs500BeforeStreaming covers this
// endpoint's other error bucket: a non-ErrUnknownCluster failure before any
// event is sent (e.g. Browse's buildEmitSpec) is a 500, same two-bucket rule
// as every other endpoint in this package.
func TestGetTopicMessagesV2_BackendFailureIs500BeforeStreaming(t *testing.T) {
	fake := newFakeMessageServicer()
	fake.behavior["prod"] = func(_ context.Context, _ func(appcluster.BrowseEvent) error) error {
		return fmt.Errorf("kadm boom")
	}
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	_, code, hdr, body := doJSON(t, http.MethodGet, srv, "/api/clusters/prod/topics/t1/messages/v2", nil)
	require.Equal(t, 500, code)
	require.Equal(t, "application/json", hdr.Get("Content-Type"))
	assertErrorEnvelope(t, body, "failed to browse topic messages", "kadm boom")
}

// TestGetTopicMessagesV2_UnknownModeIs400 covers the defensive pre-stream
// 400: GetTopicMessagesV2Params.Mode binds straight from the query string
// with no format/enum validation at bind time (runtime.
// BindQueryParameterWithOptions with Type: "string" -- see models.gen.go),
// so an unrecognized mode value only surfaces once the handler's own
// generated.PollingMode -> appcluster.BrowseMode mapping falls through.
func TestGetTopicMessagesV2_UnknownModeIs400(t *testing.T) {
	fake := newFakeMessageServicer()
	fake.behavior["prod"] = func(_ context.Context, emit func(appcluster.BrowseEvent) error) error {
		t.Fatal("Browse must not be called for an unrecognized mode")
		return nil
	}
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	_, code, hdr, body := doJSON(t, http.MethodGet, srv, "/api/clusters/prod/topics/t1/messages/v2?mode=BOGUS", nil)
	require.Equal(t, 400, code)
	require.Equal(t, "application/json", hdr.Get("Content-Type"))
	assertErrorEnvelope(t, body, "polling mode", "")
}

// TestGetTopicMessagesV2_MapsQueryParamsToBrowseSpec locks the full
// generated.GetTopicMessagesV2Params -> appcluster.BrowseSpec field mapping,
// including the 7-value PollingMode -> BrowseMode enum mapping (this case
// exercises TO_TIMESTAMP, one of the six non-default values).
func TestGetTopicMessagesV2_MapsQueryParamsToBrowseSpec(t *testing.T) {
	fake := newFakeMessageServicer()
	fake.behavior["prod"] = func(_ context.Context, emit func(appcluster.BrowseEvent) error) error {
		return emit(appcluster.BrowseEvent{Kind: appcluster.EventDone})
	}
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	path := "/api/clusters/prod/topics/t1/messages/v2?" +
		"mode=TO_TIMESTAMP&partitions=0,2&limit=50&stringFilter=err&" +
		"smartFilterId=abc123&offset=7&timestamp=1700000000000&keySerde=String&valueSerde=Json&cursor=resume-tok"
	_, code, _, _ := doJSON(t, http.MethodGet, srv, path, nil)
	require.Equal(t, 200, code)

	spec := fake.lastSpec
	require.Equal(t, appcluster.ModeToTimestamp, spec.Mode)
	require.Equal(t, []int32{0, 2}, spec.Partitions)
	require.Equal(t, 50, spec.Limit)
	require.Equal(t, int64(7), spec.Offset)
	require.Equal(t, int64(1700000000000), spec.TimestampMs)
	require.Equal(t, "err", spec.StringFilter)
	require.Equal(t, "abc123", spec.SmartFilterID)
	require.Equal(t, "String", spec.KeySerde)
	require.Equal(t, "Json", spec.ValueSerde)
	require.Equal(t, "resume-tok", spec.Cursor)
	require.Equal(t, "t1", fake.lastTopic)
}

// TestGetTopicMessagesV2_DefaultModeIsLatest locks the documented default:
// no mode query param at all must map onto appcluster.ModeLatest (the
// upstream default), not a zero value that happens to alias it by
// coincidence.
func TestGetTopicMessagesV2_DefaultModeIsLatest(t *testing.T) {
	fake := newFakeMessageServicer()
	fake.behavior["prod"] = func(_ context.Context, emit func(appcluster.BrowseEvent) error) error {
		return emit(appcluster.BrowseEvent{Kind: appcluster.EventDone})
	}
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	_, code, _, _ := doJSON(t, http.MethodGet, srv, "/api/clusters/prod/topics/t1/messages/v2", nil)
	require.Equal(t, 200, code)
	require.Equal(t, appcluster.ModeLatest, fake.lastSpec.Mode)
}

// TestGetTopicMessagesV2_AllPollingModesMap locks the complete 7-value
// generated.PollingMode -> appcluster.BrowseMode enum mapping (the
// MapsQueryParamsToBrowseSpec test above only exercises TO_TIMESTAMP; the
// nil/default case is covered separately by DefaultModeIsLatest, and the
// unrecognized-value case by UnknownModeIs400).
func TestGetTopicMessagesV2_AllPollingModesMap(t *testing.T) {
	cases := []struct {
		mode string
		want appcluster.BrowseMode
	}{
		{"LATEST", appcluster.ModeLatest},
		{"EARLIEST", appcluster.ModeEarliest},
		{"TAILING", appcluster.ModeTailing},
		{"FROM_OFFSET", appcluster.ModeFromOffset},
		{"TO_OFFSET", appcluster.ModeToOffset},
		{"FROM_TIMESTAMP", appcluster.ModeFromTimestamp},
		{"TO_TIMESTAMP", appcluster.ModeToTimestamp},
	}
	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			fake := newFakeMessageServicer()
			fake.behavior["prod"] = func(_ context.Context, emit func(appcluster.BrowseEvent) error) error {
				return emit(appcluster.BrowseEvent{Kind: appcluster.EventDone})
			}
			srv := newTestServer(withMessages(fake))
			defer srv.Close()

			_, code, _, _ := doJSON(t, http.MethodGet, srv, "/api/clusters/prod/topics/t1/messages/v2?mode="+tc.mode, nil)
			require.Equal(t, 200, code)
			require.Equal(t, tc.want, fake.lastSpec.Mode)
		})
	}
}

// TestGetTopicMessagesV2_ConsumingAndMessageHeadersMapFully covers the two
// mapping branches TestGetTopicMessagesV2_StreamsPhaseMessageDoneWithCursor
// doesn't reach: an EventConsuming event (consumingToGenerated's full
// field-for-field copy) and a message carrying non-empty Headers plus an
// explicit TimestampType (decodedMessageToGenerated's two optional-field
// branches).
func TestGetTopicMessagesV2_ConsumingAndMessageHeadersMapFully(t *testing.T) {
	fake := newFakeMessageServicer()
	fake.behavior["prod"] = func(_ context.Context, emit func(appcluster.BrowseEvent) error) error {
		stats := &appcluster.ConsumingStats{
			BytesConsumed: 1024, ElapsedMs: 250, MessagesConsumed: 10,
			FilterApplyErrors: 2, IsCancelled: true,
		}
		if err := emit(appcluster.BrowseEvent{Kind: appcluster.EventConsuming, Consuming: stats}); err != nil {
			return err
		}
		msg := &appcluster.DecodedMessage{
			Partition: 1, Offset: 9, TimestampMs: 1_700_000_000_000, TimestampType: "LOG_APPEND_TIME",
			Key: "k", Value: "v", Headers: map[string]string{"trace-id": "abc"},
			KeySerde: "String", ValueSerde: "String", KeySize: 1, ValueSize: 1, HeadersSize: 8,
		}
		return emit(appcluster.BrowseEvent{Kind: appcluster.EventMessage, Message: msg})
	}
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	_, code, _, body := doJSON(t, http.MethodGet, srv, "/api/clusters/prod/topics/t1/messages/v2", nil)
	require.Equal(t, 200, code)

	frames := parseSSEFrames(t, body)
	require.Len(t, frames, 2)
	for _, f := range frames {
		validateEventAgainstContract(t, f)
	}

	require.Equal(t, "CONSUMING", frames[0]["type"])
	consuming, ok := frames[0]["consuming"].(map[string]any)
	require.True(t, ok, "the CONSUMING frame must carry a consuming object")
	require.InDelta(t, float64(1024), consuming["bytesConsumed"], 0)
	require.InDelta(t, float64(250), consuming["elapsedMs"], 0)
	require.InDelta(t, float64(10), consuming["messagesConsumed"], 0)
	require.InDelta(t, float64(2), consuming["filterApplyErrors"], 0)
	require.Equal(t, true, consuming["isCancelled"])

	msgFrame := frames[1]["message"].(map[string]any)
	headers, ok := msgFrame["headers"].(map[string]any)
	require.True(t, ok, "a non-empty Headers map must be carried through")
	require.Equal(t, "abc", headers["trace-id"])
	require.Equal(t, "LOG_APPEND_TIME", msgFrame["timestampType"])
}

// TestGetTopicMessagesV2_ClientDisconnectEndsStreamWithoutPanic is brief
// case ⑥. It drives the handler in-process (api.NewServer(...).ServeHTTP,
// not a live httptest.Server): the fake Browse cancels the request context
// itself right after its first successful emit, synchronously, from within
// the very ServeHTTP call this test makes -- there is no goroutine/timing
// race to manage, which is what makes this deterministic instead of relying
// on real TCP-level disconnect detection (see writeSSE's send: it checks
// r.Context().Err() before every write specifically so this is observable
// without one).
func TestGetTopicMessagesV2_ClientDisconnectEndsStreamWithoutPanic(t *testing.T) {
	fake := newFakeMessageServicer()
	ctx, cancel := context.WithCancel(context.Background())
	var secondEmitErr error
	fake.behavior["prod"] = func(_ context.Context, emit func(appcluster.BrowseEvent) error) error {
		if err := emit(appcluster.BrowseEvent{Kind: appcluster.EventPhase, Phase: "Consuming"}); err != nil {
			secondEmitErr = err
			return err
		}
		cancel() // simulate the client going away right after the first event landed
		secondEmitErr = emit(appcluster.BrowseEvent{Kind: appcluster.EventMessage, Message: &appcluster.DecodedMessage{}})
		return secondEmitErr
	}

	h := api.NewServer(api.Deps{
		Messages:   fake,
		IsReadOnly: func(string) bool { return false },
		Static:     fstest.MapFS{"index.html": {Data: []byte("<html>PUBLIC-PATH-VARIABLE</html>")}},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/clusters/prod/topics/t1/messages/v2", nil).WithContext(ctx)
	req.Host = "127.0.0.1"
	rec := httptest.NewRecorder()

	require.NotPanics(t, func() { h.ServeHTTP(rec, req) })
	require.ErrorIs(t, secondEmitErr, context.Canceled, "emit after client disconnect must surface the cancellation up through Browse")
	require.Equal(t, 200, rec.Code, "the response was already committed to 200 by the first event")

	frames := parseSSEFrames(t, rec.Body.Bytes())
	require.Len(t, frames, 1, "the second (post-cancel) event must never have reached the wire")
}

// --- GetTopicMessages (v1): thin seek-based adapter over the same browse
// engine (P1c Task 10) ---
//
// v1 has no frontend caller (the vendored frontend only ever drives v2, see
// this file's package doc note above) -- these tests exist for contract +
// parity-matrix completeness, not because any real client depends on this
// path. They deliberately reuse every v2 helper above (fakeMessageServicer,
// parseSSEFrames, validateEventAgainstContract, assertErrorEnvelope) rather
// than duplicating them: this handler's whole job is translating v1's seek
// query params onto the same appcluster.BrowseSpec v2 already produces, then
// running the exact same writeSSE + Browse + browseEventToGenerated pipeline.

// TestGetTopicMessages_MapsSeekAndStringFilter is this task's brief case ①:
// seekDirection=FORWARD + seekType=BEGINNING must map to ModeEarliest,
// limit passes through verbatim, and filterQueryType=STRING_CONTAINS+q sets
// StringFilter (not FilterCode). Also proves the response is a genuine SSE
// stream whose frame passes the TopicMessageEvent contract schema.
func TestGetTopicMessages_MapsSeekAndStringFilter(t *testing.T) {
	fake := newFakeMessageServicer()
	fake.behavior["prod"] = func(_ context.Context, emit func(appcluster.BrowseEvent) error) error {
		return emit(appcluster.BrowseEvent{Kind: appcluster.EventDone})
	}
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	path := "/api/clusters/prod/topics/t1/messages?seekDirection=FORWARD&seekType=BEGINNING&limit=2&filterQueryType=STRING_CONTAINS&q=hit"
	_, code, hdr, body := doJSON(t, http.MethodGet, srv, path, nil)
	require.Equal(t, 200, code)
	require.Equal(t, "text/event-stream", hdr.Get("Content-Type"))

	frames := parseSSEFrames(t, body)
	require.Len(t, frames, 1)
	validateEventAgainstContract(t, frames[0])

	spec := fake.lastSpec
	require.Equal(t, appcluster.ModeEarliest, spec.Mode)
	require.Equal(t, "hit", spec.StringFilter)
	require.Empty(t, spec.FilterCode, "STRING_CONTAINS must not populate FilterCode")
	require.Equal(t, 2, spec.Limit)
	require.Equal(t, "t1", fake.lastTopic)
}

// TestGetTopicMessages_CELScriptSetsFilterCode is brief case ②:
// filterQueryType=CEL_SCRIPT must route `q` into BrowseSpec.FilterCode (the
// inline-CEL app hook this task adds), not SmartFilterID -- v1 never sends a
// pre-registered smart-filter id, only raw CEL source.
func TestGetTopicMessages_CELScriptSetsFilterCode(t *testing.T) {
	fake := newFakeMessageServicer()
	fake.behavior["prod"] = func(_ context.Context, emit func(appcluster.BrowseEvent) error) error {
		return emit(appcluster.BrowseEvent{Kind: appcluster.EventDone})
	}
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	path := "/api/clusters/prod/topics/t1/messages?filterQueryType=CEL_SCRIPT&" + url.Values{"q": {"record.value=='hit'"}}.Encode()
	_, code, _, _ := doJSON(t, http.MethodGet, srv, path, nil)
	require.Equal(t, 200, code)

	spec := fake.lastSpec
	require.Equal(t, "record.value=='hit'", spec.FilterCode)
	require.Empty(t, spec.StringFilter)
	require.Empty(t, spec.SmartFilterID, "v1 CEL_SCRIPT must never populate SmartFilterID")
}

// TestGetTopicMessages_SeekModeMapping is brief case ③: table-driven
// coverage of the SeekDirection x SeekType -> BrowseMode mapping table,
// beyond the single BEGINNING/FORWARD combo case ① already exercises.
// BACKWARD+LATEST and TAILING are covered directly; the FORWARD+OFFSET row
// is covered together with SeekTo's partition/value parsing (case ③'s other
// half) in TestGetTopicMessages_SeekToParsesPartitionsAndOffset below, so
// this table doesn't repeat it query-param-for-query-param.
func TestGetTopicMessages_SeekModeMapping(t *testing.T) {
	cases := []struct {
		name          string
		seekDirection string
		seekType      string
		want          appcluster.BrowseMode
	}{
		{"BACKWARD+LATEST -> ModeLatest", "BACKWARD", "LATEST", appcluster.ModeLatest},
		{"TAILING (seekType irrelevant) -> ModeTailing", "TAILING", "OFFSET", appcluster.ModeTailing},
		{"FORWARD+TIMESTAMP -> ModeFromTimestamp", "FORWARD", "TIMESTAMP", appcluster.ModeFromTimestamp},
		{"BACKWARD+BEGINNING -> ModeEarliest (degenerate default)", "BACKWARD", "BEGINNING", appcluster.ModeEarliest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeMessageServicer()
			fake.behavior["prod"] = func(_ context.Context, emit func(appcluster.BrowseEvent) error) error {
				return emit(appcluster.BrowseEvent{Kind: appcluster.EventDone})
			}
			srv := newTestServer(withMessages(fake))
			defer srv.Close()

			path := fmt.Sprintf("/api/clusters/prod/topics/t1/messages?seekDirection=%s&seekType=%s", tc.seekDirection, tc.seekType)
			_, code, _, _ := doJSON(t, http.MethodGet, srv, path, nil)
			require.Equal(t, 200, code)
			require.Equal(t, tc.want, fake.lastSpec.Mode)
		})
	}
}

// TestGetTopicMessages_DefaultSeekIsTolerantForwardTailing pins the brief's
// stated defaults: SeekDirection absent defaults to FORWARD, SeekType absent
// defaults to LATEST -- which the mapping table resolves to ModeTailing
// (FORWARD+LATEST row), matching upstream's own tolerant fallback rather
// than a 400.
func TestGetTopicMessages_DefaultSeekIsTolerantForwardTailing(t *testing.T) {
	fake := newFakeMessageServicer()
	fake.behavior["prod"] = func(_ context.Context, emit func(appcluster.BrowseEvent) error) error {
		return emit(appcluster.BrowseEvent{Kind: appcluster.EventDone})
	}
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	_, code, _, _ := doJSON(t, http.MethodGet, srv, "/api/clusters/prod/topics/t1/messages", nil)
	require.Equal(t, 200, code)
	require.Equal(t, appcluster.ModeTailing, fake.lastSpec.Mode)
}

// TestGetTopicMessages_SeekToParsesPartitionsAndOffset is the other half of
// brief case ③: seekTo's kafka-ui "partition::value" format must populate
// Partitions with every entry's partition index, and (SeekType==OFFSET)
// Offset with the *first* entry's value only -- BrowseSpec.Offset is a
// single scalar, so per-partition seek granularity is intentionally lossy
// here (documented limitation, no real caller).
func TestGetTopicMessages_SeekToParsesPartitionsAndOffset(t *testing.T) {
	fake := newFakeMessageServicer()
	fake.behavior["prod"] = func(_ context.Context, emit func(appcluster.BrowseEvent) error) error {
		return emit(appcluster.BrowseEvent{Kind: appcluster.EventDone})
	}
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	path := "/api/clusters/prod/topics/t1/messages?" + url.Values{
		"seekDirection": {"FORWARD"},
		"seekType":      {"OFFSET"},
		"seekTo":        {"0::100,1::200"},
	}.Encode()
	_, code, _, _ := doJSON(t, http.MethodGet, srv, path, nil)
	require.Equal(t, 200, code)

	spec := fake.lastSpec
	require.Equal(t, appcluster.ModeFromOffset, spec.Mode)
	require.Equal(t, []int32{0, 1}, spec.Partitions)
	require.Equal(t, int64(100), spec.Offset, "must use the FIRST seekTo entry's value (0::100), not the second (1::200)")
}

// TestGetTopicMessages_SeekToTimestampUsesFirstValue is
// SeekToParsesPartitionsAndOffset's TIMESTAMP counterpart: SeekType==
// TIMESTAMP must route the first seekTo entry's value into TimestampMs, not
// Offset.
func TestGetTopicMessages_SeekToTimestampUsesFirstValue(t *testing.T) {
	fake := newFakeMessageServicer()
	fake.behavior["prod"] = func(_ context.Context, emit func(appcluster.BrowseEvent) error) error {
		return emit(appcluster.BrowseEvent{Kind: appcluster.EventDone})
	}
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	path := "/api/clusters/prod/topics/t1/messages?" + url.Values{
		"seekDirection": {"FORWARD"},
		"seekType":      {"TIMESTAMP"},
		"seekTo":        {"3::1700000000000"},
	}.Encode()
	_, code, _, _ := doJSON(t, http.MethodGet, srv, path, nil)
	require.Equal(t, 200, code)

	spec := fake.lastSpec
	require.Equal(t, appcluster.ModeFromTimestamp, spec.Mode)
	require.Equal(t, []int32{3}, spec.Partitions)
	require.Equal(t, int64(1700000000000), spec.TimestampMs)
	require.Zero(t, spec.Offset)
}

// TestGetTopicMessages_BadSeekToFormatIs400 covers the brief's explicit
// requirement that a malformed seekTo entry (missing the "::" separator, or
// a non-integer partition index) is a pre-stream 400, not a panic or a
// half-open SSE stream.
func TestGetTopicMessages_BadSeekToFormatIs400(t *testing.T) {
	fake := newFakeMessageServicer()
	fake.behavior["prod"] = func(_ context.Context, emit func(appcluster.BrowseEvent) error) error {
		t.Fatal("Browse must not be called for a malformed seekTo entry")
		return nil
	}
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	_, code, hdr, body := doJSON(t, http.MethodGet, srv, "/api/clusters/prod/topics/t1/messages?seekTo=not-a-valid-entry", nil)
	require.Equal(t, 400, code)
	require.Equal(t, "application/json", hdr.Get("Content-Type"))
	assertErrorEnvelope(t, body, "seekTo", "")
}

// TestGetTopicMessages_UnknownClusterIs404BeforeStreaming is brief case ④:
// same two-bucket error convention as v2 -- Browse's res.Lookup runs before
// its first EventPhase emit, so an unknown cluster is an ordinary 404 JSON
// response, never a broken/half-open SSE stream.
func TestGetTopicMessages_UnknownClusterIs404BeforeStreaming(t *testing.T) {
	fake := newFakeMessageServicer() // no behavior registered for "nope" -> ErrUnknownCluster
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	req, code, hdr, body := doJSON(t, http.MethodGet, srv, "/api/clusters/nope/topics/t1/messages", nil)
	require.Equal(t, 404, code)
	require.Equal(t, "application/json", hdr.Get("Content-Type"))
	require.NotContains(t, hdr.Get("Content-Type"), "event-stream")
	assertErrorEnvelope(t, body, "cluster not found", "")
	// Contract declares only 200 for this operation (same situation as v2,
	// see TestGetTopicMessagesV2_UnknownClusterIs404BeforeStreaming's own
	// comment) -- an undocumented status is a no-op pass for
	// openapi3filter.ValidateResponse, kept here for parity.
	validateAgainstContract(t, req, code, hdr, body)
}

// TestGetTopicMessages_KeySerdeAndValueSerdePassThrough locks the remaining
// two straight-passthrough fields the tests above don't otherwise touch.
func TestGetTopicMessages_KeySerdeAndValueSerdePassThrough(t *testing.T) {
	fake := newFakeMessageServicer()
	fake.behavior["prod"] = func(_ context.Context, emit func(appcluster.BrowseEvent) error) error {
		return emit(appcluster.BrowseEvent{Kind: appcluster.EventDone})
	}
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	path := "/api/clusters/prod/topics/t1/messages?keySerde=String&valueSerde=Json"
	_, code, _, _ := doJSON(t, http.MethodGet, srv, path, nil)
	require.Equal(t, 200, code)
	require.Equal(t, "String", fake.lastSpec.KeySerde)
	require.Equal(t, "Json", fake.lastSpec.ValueSerde)
}

// --- P1c Task 11: SendTopicMessages / DeleteTopicMessages ---

// TestSendTopicMessages_Success200 proves the full happy path: a valid
// CreateTopicMessage body decodes cleanly, maps onto SendSpec unchanged
// (asserted field-for-field against fake.lastSendSpec), and the endpoint
// reports 204 with no body -- the contract declares no response schema for
// this operation's success status.
func TestSendTopicMessages_Success200(t *testing.T) {
	fake := newFakeMessageServicer()
	fake.sendKnown["prod"] = true
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	reqBody := `{"partition":1,"key":"k1","value":"v1","headers":{"h":"hv"},"keySerde":"String","valueSerde":"Json"}`
	req, code, hdr, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/topics/t1/messages", reqBody)
	require.Equal(t, 204, code)
	require.Empty(t, body)

	require.Equal(t, "t1", fake.lastSendTopic)
	require.Equal(t, int32(1), fake.lastSendSpec.Partition)
	require.NotNil(t, fake.lastSendSpec.Key)
	require.Equal(t, "k1", *fake.lastSendSpec.Key)
	require.NotNil(t, fake.lastSendSpec.Value)
	require.Equal(t, "v1", *fake.lastSendSpec.Value)
	require.Equal(t, map[string]string{"h": "hv"}, fake.lastSendSpec.Headers)
	require.Equal(t, "String", fake.lastSendSpec.KeySerde)
	require.Equal(t, "Json", fake.lastSendSpec.ValueSerde)
	validateAgainstContract(t, req, code, hdr, body)
}

// TestSendTopicMessages_KeyOmittedStaysNil proves an omitted "key" field
// (as opposed to `"key":""`) reaches SendSpec.Key as a genuine nil -- the
// decode->SendSpec mapping (sendSpecFromGenerated) must not accidentally
// coerce "absent" into "pointer to empty string", since MessageService.Send
// treats the two very differently (nil = don't send this side at all).
func TestSendTopicMessages_KeyOmittedStaysNil(t *testing.T) {
	fake := newFakeMessageServicer()
	fake.sendKnown["prod"] = true
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	reqBody := `{"partition":0,"value":"v1"}`
	_, code, _, _ := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/topics/t1/messages", reqBody)
	require.Equal(t, 204, code)
	require.Nil(t, fake.lastSendSpec.Key)
	require.NotNil(t, fake.lastSendSpec.Value)
}

// TestSendTopicMessages_MalformedBodyIs400 proves a body that doesn't even
// parse as JSON is a pre-Send 400 -- Send must never be called (asserted via
// t.Fatal in the fake's Send, which this test never registers a cluster
// for anyway, but the malformed-JSON decode failure must short-circuit
// before Send is even reached).
//
// No validateAgainstContract here -- same reason as
// TestCreateTopicInvalidBodyIs400 (handlers_topic_test.go): kin-openapi's
// ValidateRequest itself tries to decode the (deliberately broken) request
// body against the schema and errors out before ever reaching response
// validation, so it isn't a usable assertion for this specific case.
func TestSendTopicMessages_MalformedBodyIs400(t *testing.T) {
	fake := newFakeMessageServicer()
	fake.sendKnown["prod"] = true
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	_, code, _, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/topics/t1/messages", `not valid json`)
	require.Equal(t, 400, code)
	assertErrorEnvelope(t, body, "invalid request body", "")
	require.Empty(t, fake.lastSendTopic, "Send must never be called for a malformed body")
}

// TestSendTopicMessages_SerializeFailureIs400 proves appcluster.ErrSerialize
// from Send maps to 400 (a client input error, distinct from the plain 500
// "everything else" bucket) -- covers the one extra error bucket this
// endpoint adds beyond the ordinary two (404 unknown-cluster, 500 else).
func TestSendTopicMessages_SerializeFailureIs400(t *testing.T) {
	fake := newFakeMessageServicer()
	fake.sendErr["prod"] = appcluster.ErrSerialize
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	req, code, hdr, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/topics/t1/messages", `{"partition":0,"value":"not-a-number"}`)
	require.Equal(t, 400, code)
	assertErrorEnvelope(t, body, "serialize", "")
	validateAgainstContract(t, req, code, hdr, body)
}

// TestSendTopicMessages_UnknownClusterIs404 covers the ordinary
// appcluster.ErrUnknownCluster -> 404 bucket.
func TestSendTopicMessages_UnknownClusterIs404(t *testing.T) {
	fake := newFakeMessageServicer() // "nope" never marked known -> ErrUnknownCluster
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	req, code, hdr, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/nope/topics/t1/messages", `{"partition":0,"value":"v1"}`)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

// TestSendTopicMessages_BackendErrorIs500 covers the generic "everything
// else" 500 bucket, and that the underlying error text is never leaked in
// the response body (serverError's own contract, see handlers_cluster.go).
func TestSendTopicMessages_BackendErrorIs500(t *testing.T) {
	fake := newFakeMessageServicer()
	fake.sendErr["prod"] = fmt.Errorf("kadm boom: broker unreachable")
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	_, code, _, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/topics/t1/messages", `{"partition":0,"value":"v1"}`)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to send message", "kadm boom")
}

// TestSendTopicMessagesReadOnlyIs403 is this task's mandatory readOnly-403
// coverage for a genuine write endpoint (repo CLAUDE.md's known-pitfalls
// rule): a read-only cluster's POST is rejected by readOnlyGuard before it
// ever reaches the handler -- Send must never be called.
func TestSendTopicMessagesReadOnlyIs403(t *testing.T) {
	fake := newFakeMessageServicer()
	fake.sendKnown["prod"] = true
	srv := newTestServer(withMessages(fake), withReadOnly("prod"))
	defer srv.Close()

	_, code, _, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/topics/t1/messages", `{"partition":0,"value":"v1"}`)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "read-only", "")
	require.Empty(t, fake.lastSendTopic, "Send must never be called for a read-only cluster")
}

// TestDeleteTopicMessages_Success204 proves the happy path: partitions
// passed through the comma-separated (explode:false) query param reach
// MessageServicer.Delete unchanged, and the endpoint reports 204 with no
// body.
func TestDeleteTopicMessages_Success204(t *testing.T) {
	fake := newFakeMessageServicer()
	fake.deleteKnown["prod"] = true
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	req, code, hdr, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/topics/t1/messages?partitions=0,2", nil)
	require.Equal(t, 204, code)
	require.Empty(t, body)
	require.Equal(t, "t1", fake.lastDeleteTopic)
	require.Equal(t, []int32{0, 2}, fake.lastDeletePartitions)
	validateAgainstContract(t, req, code, hdr, body)
}

// TestDeleteTopicMessages_OmittedPartitionsMeansAll proves an omitted
// `partitions` query param reaches Delete as nil (empty-means-all,
// cluster.MessageWriterPort.DeleteRecords' own convention) rather than an
// empty-but-non-nil slice -- not strictly load-bearing for Delete's
// behavior (both are falsy), but locks the handler's own passthrough
// against silently defaulting to something else later.
func TestDeleteTopicMessages_OmittedPartitionsMeansAll(t *testing.T) {
	fake := newFakeMessageServicer()
	fake.deleteKnown["prod"] = true
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	_, code, _, _ := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/topics/t1/messages", nil)
	require.Equal(t, 204, code)
	require.Nil(t, fake.lastDeletePartitions)
}

// TestDeleteTopicMessages_UnknownClusterIs404 covers the ordinary
// appcluster.ErrUnknownCluster -> 404 bucket.
func TestDeleteTopicMessages_UnknownClusterIs404(t *testing.T) {
	fake := newFakeMessageServicer() // "nope" never marked known -> ErrUnknownCluster
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	req, code, hdr, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/nope/topics/t1/messages", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

// TestDeleteTopicMessages_BackendErrorIs500 covers the generic "everything
// else" 500 bucket, and that the underlying error text is never leaked.
func TestDeleteTopicMessages_BackendErrorIs500(t *testing.T) {
	fake := newFakeMessageServicer()
	fake.deleteErr["prod"] = fmt.Errorf("kadm boom: delete records failed")
	srv := newTestServer(withMessages(fake))
	defer srv.Close()

	_, code, _, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/topics/t1/messages", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to delete messages", "kadm boom")
}

// TestDeleteTopicMessagesReadOnlyIs403 mirrors
// TestSendTopicMessagesReadOnlyIs403 for DELETE.
func TestDeleteTopicMessagesReadOnlyIs403(t *testing.T) {
	fake := newFakeMessageServicer()
	fake.deleteKnown["prod"] = true
	srv := newTestServer(withMessages(fake), withReadOnly("prod"))
	defer srv.Close()

	_, code, _, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/topics/t1/messages", nil)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "read-only", "")
	require.Empty(t, fake.lastDeleteTopic, "Delete must never be called for a read-only cluster")
}
