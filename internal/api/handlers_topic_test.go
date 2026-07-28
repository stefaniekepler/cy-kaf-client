package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/api"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// validateResponseOnlyAgainstContract is validateAgainstContract's (contract_
// test.go) response-only half: it skips openapi3filter.ValidateRequest and
// only runs ValidateResponse. Task 8d's configs leniency (handlers_topic.go's
// "P1b Task 8d" comment block: coerceConfigValue/coerceConfigMap) makes our
// REQUEST side deliberately more permissive than the contract's configs:
// additionalProperties: type: string schema — a numeric/boolean/null configs
// value is exactly the input this task exists to accept, so ValidateRequest
// rejecting it is expected and not a regression to catch. The RESPONSE shape
// is unchanged by this task, so that half must still hold — every one of
// this file's leniency tests below uses this instead of the full
// validateAgainstContract for that reason.
func validateResponseOnlyAgainstContract(t *testing.T, req *http.Request, status int, hdr http.Header, body []byte) {
	t.Helper()
	router := contractRouterFor(t)
	route, params, err := router.FindRoute(req)
	require.NoError(t, err, "端点必须存在于契约中: %s %s", req.Method, req.URL.Path)
	in := &openapi3filter.RequestValidationInput{Request: req, PathParams: params, Route: route}
	require.NoError(t, openapi3filter.ValidateResponse(req.Context(), &openapi3filter.ResponseValidationInput{
		RequestValidationInput: in, Status: status, Header: hdr,
		Body: io.NopCloser(bytes.NewReader(body)),
	}))
}

// topicDetailsFixture pairs the cached TopicState Details returns with the
// live configs it combines them with — mirrors what
// appcluster.TopicService.Details actually returns (see topic.go).
type topicDetailsFixture struct {
	state cluster.TopicState
	cfgs  []cluster.ConfigEntry
}

// fakeTopicServicer implements api.TopicServicer for the topics handler
// tests: canned per-cluster responses, same error-then-presence pattern as
// handlers_broker_test.go's fakeBrokerStater (an explicit per-cluster error
// wins, otherwise map presence is the "known cluster" signal). Fields are
// unexported but set directly by tests (same package) rather than through
// with*-style options: TopicServicer doesn't share an underlying struct
// with States/Brokers/LogDirs the way fakeBrokerStater does, so there's no
// composition to preserve across option calls — a plain struct literal via
// newFakeTopicServicer() plus direct field writes is simpler, and keeps a
// live reference around for asserting lastListQuery after the request.
type fakeTopicServicer struct {
	listPages     map[string]appcluster.TopicPage
	listErr       map[string]error
	lastListQuery map[string]appcluster.TopicListQuery

	details    map[string]topicDetailsFixture
	detailsErr map[string]error

	configs    map[string][]cluster.ConfigEntry
	configsErr map[string]error

	acls    map[string][]cluster.AclBinding
	aclsErr map[string]error

	producers    map[string][]cluster.ProducerState
	producersErr map[string]error

	// connectorsErr/connectorsKnown back Connectors (P1b Task 8a's empty
	// getTopicConnectors stub) — same "explicit error wins, else a known-
	// cluster marker" shape as deleteErr/deleteKnown below: Connectors has no
	// result payload to double as the "known cluster" signal either.
	connectorsErr   map[string]error
	connectorsKnown map[string]bool

	// --- P1b Task 5: write surface (per-cluster canned result/error, same
	// "error map wins, else result-map presence = known cluster" convention
	// as the read fields above) ---

	createResult   map[string]cluster.TopicState
	createErr      map[string]error
	lastCreateSpec map[string]cluster.TopicSpec

	deleteErr       map[string]error
	deleteKnown     map[string]bool // no result payload to double as "known cluster" -> explicit marker
	lastDeleteTopic map[string]string

	updateResult      map[string]cluster.TopicState
	updateErr         map[string]error
	lastUpdateDesired map[string]map[string]string

	recreateResult map[string]cluster.TopicState
	recreateErr    map[string]error

	cloneResult   map[string]cluster.TopicState
	cloneErr      map[string]error
	lastCloneArgs map[string][2]string // [sourceTopic, newTopic]

	increaseErr       map[string]error
	increaseKnown     map[string]bool
	lastIncreaseTotal map[string]int32

	changeRFErr        map[string]error
	changeRFKnown      map[string]bool
	lastChangeRFTarget map[string]int16
}

func newFakeTopicServicer() *fakeTopicServicer {
	return &fakeTopicServicer{
		listPages: map[string]appcluster.TopicPage{}, listErr: map[string]error{}, lastListQuery: map[string]appcluster.TopicListQuery{},
		details: map[string]topicDetailsFixture{}, detailsErr: map[string]error{},
		configs: map[string][]cluster.ConfigEntry{}, configsErr: map[string]error{},
		acls: map[string][]cluster.AclBinding{}, aclsErr: map[string]error{},
		producers: map[string][]cluster.ProducerState{}, producersErr: map[string]error{},
		connectorsErr: map[string]error{}, connectorsKnown: map[string]bool{},

		createResult: map[string]cluster.TopicState{}, createErr: map[string]error{}, lastCreateSpec: map[string]cluster.TopicSpec{},
		deleteErr: map[string]error{}, deleteKnown: map[string]bool{}, lastDeleteTopic: map[string]string{},
		updateResult: map[string]cluster.TopicState{}, updateErr: map[string]error{}, lastUpdateDesired: map[string]map[string]string{},
		recreateResult: map[string]cluster.TopicState{}, recreateErr: map[string]error{},
		cloneResult: map[string]cluster.TopicState{}, cloneErr: map[string]error{}, lastCloneArgs: map[string][2]string{},
		increaseErr: map[string]error{}, increaseKnown: map[string]bool{}, lastIncreaseTotal: map[string]int32{},
		changeRFErr: map[string]error{}, changeRFKnown: map[string]bool{}, lastChangeRFTarget: map[string]int16{},
	}
}

func (f *fakeTopicServicer) List(_ context.Context, name string, q appcluster.TopicListQuery) (appcluster.TopicPage, error) {
	f.lastListQuery[name] = q
	if err, ok := f.listErr[name]; ok {
		return appcluster.TopicPage{}, err
	}
	page, ok := f.listPages[name]
	if !ok {
		return appcluster.TopicPage{}, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return page, nil
}

func (f *fakeTopicServicer) Details(_ context.Context, name, _ string) (cluster.TopicState, []cluster.ConfigEntry, error) {
	if err, ok := f.detailsErr[name]; ok {
		return cluster.TopicState{}, nil, err
	}
	d, ok := f.details[name]
	if !ok {
		return cluster.TopicState{}, nil, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return d.state, d.cfgs, nil
}

func (f *fakeTopicServicer) Configs(_ context.Context, name, _ string) ([]cluster.ConfigEntry, error) {
	if err, ok := f.configsErr[name]; ok {
		return nil, err
	}
	cfgs, ok := f.configs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return cfgs, nil
}

func (f *fakeTopicServicer) Acls(_ context.Context, name, _ string) ([]cluster.AclBinding, error) {
	if err, ok := f.aclsErr[name]; ok {
		return nil, err
	}
	acls, ok := f.acls[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return acls, nil
}

func (f *fakeTopicServicer) ActiveProducers(_ context.Context, name, _ string) ([]cluster.ProducerState, error) {
	if err, ok := f.producersErr[name]; ok {
		return nil, err
	}
	ps, ok := f.producers[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return ps, nil
}

func (f *fakeTopicServicer) Connectors(_ context.Context, name, _ string) error {
	if err, ok := f.connectorsErr[name]; ok {
		return err
	}
	if !f.connectorsKnown[name] {
		return fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return nil
}

func (f *fakeTopicServicer) Create(_ context.Context, name string, spec cluster.TopicSpec) (cluster.TopicState, error) {
	f.lastCreateSpec[name] = spec
	if err, ok := f.createErr[name]; ok {
		return cluster.TopicState{}, err
	}
	ts, ok := f.createResult[name]
	if !ok {
		return cluster.TopicState{}, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return ts, nil
}

func (f *fakeTopicServicer) Delete(_ context.Context, name, topic string) error {
	f.lastDeleteTopic[name] = topic
	if err, ok := f.deleteErr[name]; ok {
		return err
	}
	if !f.deleteKnown[name] {
		return fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return nil
}

func (f *fakeTopicServicer) UpdateConfigs(_ context.Context, name, _ string, desired map[string]string) (cluster.TopicState, error) {
	f.lastUpdateDesired[name] = desired
	if err, ok := f.updateErr[name]; ok {
		return cluster.TopicState{}, err
	}
	ts, ok := f.updateResult[name]
	if !ok {
		return cluster.TopicState{}, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return ts, nil
}

func (f *fakeTopicServicer) Recreate(_ context.Context, name, _ string) (cluster.TopicState, error) {
	if err, ok := f.recreateErr[name]; ok {
		return cluster.TopicState{}, err
	}
	ts, ok := f.recreateResult[name]
	if !ok {
		return cluster.TopicState{}, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return ts, nil
}

func (f *fakeTopicServicer) Clone(_ context.Context, name, sourceTopic, newTopic string) (cluster.TopicState, error) {
	f.lastCloneArgs[name] = [2]string{sourceTopic, newTopic}
	if err, ok := f.cloneErr[name]; ok {
		return cluster.TopicState{}, err
	}
	ts, ok := f.cloneResult[name]
	if !ok {
		return cluster.TopicState{}, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return ts, nil
}

func (f *fakeTopicServicer) IncreasePartitions(_ context.Context, name, _ string, total int32) error {
	f.lastIncreaseTotal[name] = total
	if err, ok := f.increaseErr[name]; ok {
		return err
	}
	if !f.increaseKnown[name] {
		return fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return nil
}

func (f *fakeTopicServicer) ChangeReplicationFactor(_ context.Context, name, _ string, target int16) error {
	f.lastChangeRFTarget[name] = target
	if err, ok := f.changeRFErr[name]; ok {
		return err
	}
	if !f.changeRFKnown[name] {
		return fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return nil
}

// withTopics wires ft as Deps.Topics — the test keeps its own reference to
// ft so it can populate canned data before the request and read back
// call-recording fields (e.g. lastListQuery) after it.
func withTopics(ft *fakeTopicServicer) testServerOption {
	return func(d *api.Deps) { d.Topics = ft }
}

func sampleTopicState() cluster.TopicState {
	return cluster.TopicState{
		Name:              "orders",
		Internal:          false,
		ReplicationFactor: 2,
		SegmentSize:       2048,
		SegmentCount:      4,
		Partitions: []cluster.PartitionState{
			{ID: 0, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1, 2}, StartOffset: 0, EndOffset: 100},
			{ID: 1, Leader: 2, Replicas: []int32{1, 2}, ISR: []int32{1}, StartOffset: 0, EndOffset: 50}, // under-replicated
		},
	}
}

// --- GetTopics ---

func TestGetTopics(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.listPages["prod"] = appcluster.TopicPage{Topics: []cluster.TopicState{sampleTopicState()}, PageCount: 3}
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := getJSON(t, srv,
		"/api/clusters/prod/topics?page=2&perPage=10&search=ord&showInternal=true&orderBy=SIZE&sortOrder=DESC", &got)
	require.Equal(t, 200, code)
	require.Equal(t, float64(3), got["pageCount"])
	topics, ok := got["topics"].([]any)
	require.True(t, ok)
	require.Len(t, topics, 1)
	topic0, ok := topics[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "orders", topic0["name"])
	require.Equal(t, float64(2), topic0["partitionCount"])
	require.Equal(t, float64(2), topic0["replicationFactor"])
	require.Equal(t, float64(4), topic0["replicas"])       // sum(len(Replicas)) = 2+2
	require.Equal(t, float64(3), topic0["inSyncReplicas"]) // sum(len(ISR)) = 2+1
	require.Equal(t, float64(1), topic0["underReplicatedPartitions"])
	require.Equal(t, float64(2048), topic0["segmentSize"])
	require.Equal(t, float64(4), topic0["segmentCount"])
	require.Equal(t, float64(150), topic0["messagesCount"]) // Σ(EndOffset-StartOffset) = (100-0)+(50-0)
	require.NotContains(t, topic0, "partitions")            // list view omits per-partition detail (see topicToGenerated)
	validateAgainstContract(t, req, code, hdr, body)

	// query param passthrough: page/perPage/search/showInternal/orderBy/
	// sortOrder all reached TopicService.List unchanged.
	require.Equal(t, appcluster.TopicListQuery{
		Page: 2, PerPage: 10, Search: "ord", ShowInternal: true, OrderBy: "SIZE", SortOrder: "DESC",
	}, ft.lastListQuery["prod"])
}

// TestGetTopicsMessagesCount locks in topicMessagesCount's Σ per-partition
// max(0, EndOffset-StartOffset) rule across the offset-availability matrix
// P1b Task 8c's brief calls out. Exercised end-to-end through GetTopics
// (topicToGenerated, the list-row mapper, has no other public entry point —
// same handler-level-only convention this file already uses for its two
// sibling mapping helpers, topicTallies/topicCountPtrs, neither of which has
// a direct unit test either).
//
// The "empty topic" case is the one that actually unblocks the Topic Delete
// e2e scenario: a freshly-created topic's partitions have known 0/0 offsets
// (both >= 0), so topicMessagesCount must return a pointer to 0 rather than
// nil — the contract's `*int64 omitempty` only suppresses a nil pointer, not
// a pointer-to-zero, so this is what makes messagesCount:0 actually appear
// on the wire instead of being omitted (which the frontend renders as
// "N/A", breaking TopicsLocators.ts's row locator). Both the decoded JSON
// map and the raw response bytes are asserted for that distinction, so a
// nil/omitempty regression can't slip through either representation.
func TestGetTopicsMessagesCount(t *testing.T) {
	cases := []struct {
		name       string
		partitions []cluster.PartitionState
		wantOmit   bool
		want       int64
	}{
		{
			name: "empty topic: known 0/0 offsets sum to literal 0, not omitted",
			partitions: []cluster.PartitionState{
				{ID: 0, StartOffset: 0, EndOffset: 0},
				{ID: 1, StartOffset: 0, EndOffset: 0},
			},
			want: 0,
		},
		{
			name: "data present: sums per-partition end-start",
			partitions: []cluster.PartitionState{
				{ID: 0, StartOffset: 0, EndOffset: 10},
				{ID: 1, StartOffset: 0, EndOffset: 5},
			},
			want: 15,
		},
		{
			name: "all offsets unknown (-1 sentinel): omitted, not a false 0",
			partitions: []cluster.PartitionState{
				{ID: 0, StartOffset: -1, EndOffset: -1},
				{ID: 1, StartOffset: -1, EndOffset: -1},
			},
			wantOmit: true,
		},
		{
			// The unknown partition uses an ASYMMETRIC sentinel (StartOffset -1
			// but EndOffset a real 30), not the symmetric -1/-1: state.go's
			// ListStartOffsets/ListEndOffsets fail independently, so one side
			// landing while the other stays -1 is a real scrape outcome. It also
			// keeps this case honest as a guard — a symmetric -1/-1 has delta 0,
			// so deleting the `< 0` skip guard wouldn't change the sum and the
			// test couldn't catch that regression; with EndOffset 30, dropping
			// the guard would wrongly add max(0, 30-(-1))=31 and turn this red.
			name: "mixed known/unknown (asymmetric sentinel): sums only the known partition",
			partitions: []cluster.PartitionState{
				{ID: 0, StartOffset: 0, EndOffset: 20},
				{ID: 1, StartOffset: -1, EndOffset: 30},
			},
			want: 20,
		},
		{
			name:       "zero partitions: omitted",
			partitions: nil,
			wantOmit:   true,
		},
		{
			name: "end<start anomaly contributes 0, never negative",
			partitions: []cluster.PartitionState{
				{ID: 0, StartOffset: 10, EndOffset: 5},
			},
			want: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ft := newFakeTopicServicer()
			ft.listPages["prod"] = appcluster.TopicPage{
				Topics:    []cluster.TopicState{{Name: "t1", Partitions: tc.partitions}},
				PageCount: 1,
			}
			srv := newTestServer(withTopics(ft))
			defer srv.Close()

			var got map[string]any
			req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/topics", &got)
			require.Equal(t, 200, code)
			topics, ok := got["topics"].([]any)
			require.True(t, ok)
			require.Len(t, topics, 1)
			topic0, ok := topics[0].(map[string]any)
			require.True(t, ok)

			if tc.wantOmit {
				require.NotContains(t, topic0, "messagesCount")
				require.NotContains(t, string(body), "messagesCount")
			} else {
				require.Contains(t, topic0, "messagesCount", "key must be present even when the value is 0, not omitted")
				require.Equal(t, float64(tc.want), topic0["messagesCount"])
				require.Contains(t, string(body), fmt.Sprintf(`"messagesCount":%d`, tc.want), "raw JSON must carry the literal key:value, not omit it")
			}
			validateAgainstContract(t, req, code, hdr, body)
		})
	}
}

func TestGetTopicsUnknownClusterIs404(t *testing.T) {
	srv := newTestServer(withTopics(newFakeTopicServicer()))
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/topics", nil)
	require.Equal(t, 404, code)
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetTopicsBackendFailureIs500(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.listErr["prod"] = fmt.Errorf("kadm boom")
	srv := newTestServer(withTopics(ft))
	defer srv.Close()
	_, code, _, body := getJSON(t, srv, "/api/clusters/prod/topics", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to list topics", "kadm boom")
	// Contract only declares 200 for getTopics, so no validateAgainstContract here.
}

// --- GetTopicsCsv ---

func TestGetTopicsCsv(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.listPages["prod"] = appcluster.TopicPage{Topics: []cluster.TopicState{sampleTopicState()}, PageCount: 1}
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/api/clusters/prod/topics/csv", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, 200, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Content-Type"), "text/csv")
	b, _ := io.ReadAll(resp.Body)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	require.Len(t, lines, 2) // header + 1 data row
	require.Contains(t, lines[0], "name")
	validateAgainstContract(t, req, resp.StatusCode, resp.Header, b)

	// getTopicsCsv declares no page/perPage params — PerPage must be set to
	// "everything" (math.MaxInt32), not left at the List/getTopics default.
	require.Equal(t, math.MaxInt32, ft.lastListQuery["prod"].PerPage)
}

func TestGetTopicsCsvUnknownClusterIs404(t *testing.T) {
	srv := newTestServer(withTopics(newFakeTopicServicer()))
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/topics/csv", nil)
	require.Equal(t, 404, code)
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetTopicsCsvBackendFailureIs500(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.listErr["prod"] = fmt.Errorf("kadm boom")
	srv := newTestServer(withTopics(ft))
	defer srv.Close()
	_, code, _, body := getJSON(t, srv, "/api/clusters/prod/topics/csv", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to list topics", "kadm boom")
}

// --- GetTopicDetails ---

func TestGetTopicDetails(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.details["prod"] = topicDetailsFixture{
		state: sampleTopicState(),
		cfgs:  []cluster.ConfigEntry{{Name: "cleanup.policy", Value: "compact"}},
	}
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/topics/orders", &got)
	require.Equal(t, 200, code)
	require.Equal(t, "orders", got["name"])
	require.Equal(t, "COMPACT", got["cleanUpPolicy"])
	parts, ok := got["partitions"].([]any)
	require.True(t, ok)
	require.Len(t, parts, 2)
	p0, ok := parts[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, float64(0), p0["partition"])
	require.Equal(t, float64(1), p0["leader"])
	require.Equal(t, float64(0), p0["offsetMin"])
	require.Equal(t, float64(100), p0["offsetMax"])
	reps, ok := p0["replicas"].([]any)
	require.True(t, ok)
	require.Len(t, reps, 2)
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetTopicDetailsUnknownClusterIs404(t *testing.T) {
	srv := newTestServer(withTopics(newFakeTopicServicer()))
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/topics/t1", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetTopicDetailsBackendFailureIs500(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.detailsErr["prod"] = fmt.Errorf("kadm boom")
	srv := newTestServer(withTopics(ft))
	defer srv.Close()
	_, code, _, body := getJSON(t, srv, "/api/clusters/prod/topics/t1", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to get topic details", "kadm boom")
}

// TestGetTopicDetailsNotYetCachedOmitsPartitionAndReplicationCounts covers
// the GetTopicDetails read path's own not-yet-cached degrade: for a known
// cluster whose queried topic isn't in st.Topics yet (just-created / pre-
// first-scrape), appcluster.TopicService.Details returns
// cluster.TopicState{Name: topic} with no error — exercised directly by
// TestTopicServiceDetailsDegradesWhenTopicNotYetCached — leaving
// Partitions/ReplicationFactor at the Go zero value. The details response
// must omit partitionCount/replicationFactor entirely, not serialize a
// misleading literal 0, same as the write paths (topicDetailsToGenerated
// shares topicCountPtrs' <=0→nil rule). This is the endpoint-level lock on
// that serialization; the fixture reproduces the exact TopicState the
// degrade yields (the details endpoint routes through the Topics servicer,
// so the fake's details fixture — not newStatesTestServer's States wiring —
// is the api-layer seam).
func TestGetTopicDetailsNotYetCachedOmitsPartitionAndReplicationCounts(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.details["prod"] = topicDetailsFixture{
		state: cluster.TopicState{Name: "brand-new"}, // TopicService.Details' not-yet-cached degrade
		cfgs:  []cluster.ConfigEntry{{Name: "retention.ms", Value: "60000"}},
	}
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/topics/brand-new", &got)
	require.Equal(t, 200, code)
	require.Equal(t, "brand-new", got["name"])
	require.NotContains(t, got, "partitionCount", "unknown partition count must be omitted, not a misleading literal 0")
	require.NotContains(t, got, "replicationFactor", "unknown replication factor must be omitted, not a misleading literal 0")
	validateAgainstContract(t, req, code, hdr, body)
}

// --- GetTopicConfigs ---

func TestGetTopicConfigs(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.configs["prod"] = []cluster.ConfigEntry{
		{
			Name: "cleanup.policy", Value: "delete", Source: "DYNAMIC_TOPIC_CONFIG",
			Synonyms: []cluster.ConfigSynonym{{Name: "cleanup.policy", Value: "delete", Source: "DEFAULT_CONFIG"}},
		},
		{Name: "retention.ms", Value: "60000", Source: "DEFAULT_CONFIG"},
	}
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	var got []map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/topics/orders/config", &got)
	require.Equal(t, 200, code)
	require.Len(t, got, 2)
	require.Equal(t, "cleanup.policy", got[0]["name"])
	require.Equal(t, "delete", got[0]["value"])
	require.Equal(t, "DYNAMIC_TOPIC_CONFIG", got[0]["source"])
	require.Equal(t, "delete", got[0]["defaultValue"]) // extracted from the DEFAULT_CONFIG synonym
	syns, ok := got[0]["synonyms"].([]any)
	require.True(t, ok)
	require.Len(t, syns, 1)
	require.NotContains(t, got[1], "defaultValue") // no DEFAULT_CONFIG synonym on this entry
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetTopicConfigsUnknownClusterIs404(t *testing.T) {
	srv := newTestServer(withTopics(newFakeTopicServicer()))
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/topics/t1/config", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetTopicConfigsBackendFailureIs500(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.configsErr["prod"] = fmt.Errorf("kadm boom")
	srv := newTestServer(withTopics(ft))
	defer srv.Close()
	_, code, _, body := getJSON(t, srv, "/api/clusters/prod/topics/t1/config", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to describe topic configs", "kadm boom")
}

// --- ListTopicAcls ---

func TestListTopicAcls(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.acls["prod"] = []cluster.AclBinding{
		{Principal: "User:alice", Host: "*", ResourceName: "orders",
			ResourceType: "TOPIC", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW"},
	}
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	var got []map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/topics/orders/acls", &got)
	require.Equal(t, 200, code)
	require.Len(t, got, 1)
	require.Equal(t, "User:alice", got[0]["principal"])
	require.Equal(t, "TOPIC", got[0]["resourceType"])
	require.Equal(t, "LITERAL", got[0]["namePatternType"])
	require.Equal(t, "READ", got[0]["operation"])
	require.Equal(t, "ALLOW", got[0]["permission"])
	validateAgainstContract(t, req, code, hdr, body)
}

func TestListTopicAclsUnknownClusterIs404(t *testing.T) {
	srv := newTestServer(withTopics(newFakeTopicServicer()))
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/topics/t1/acls", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestListTopicAclsBackendFailureIs500(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.aclsErr["prod"] = fmt.Errorf("kadm boom")
	srv := newTestServer(withTopics(ft))
	defer srv.Close()
	_, code, _, body := getJSON(t, srv, "/api/clusters/prod/topics/t1/acls", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to list topic acls", "kadm boom")
}

// --- GetActiveProducerStates ---

func TestGetActiveProducerStates(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.producers["prod"] = []cluster.ProducerState{
		{Partition: 0, ProducerID: 42, ProducerEpoch: 1, LastSequence: 9, LastTimestamp: 1000,
			CoordinatorEpoch: 2, CurrentTransactionStartOffset: 5},
	}
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	var got []map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/topics/orders/activeproducers", &got)
	require.Equal(t, 200, code)
	require.Len(t, got, 1)
	require.Equal(t, float64(0), got[0]["partition"])
	require.Equal(t, float64(42), got[0]["producerId"])
	require.Equal(t, float64(1), got[0]["producerEpoch"])
	require.Equal(t, float64(9), got[0]["lastSequence"])
	require.Equal(t, float64(1000), got[0]["lastTimestampMs"])
	require.Equal(t, float64(2), got[0]["coordinatorEpoch"])
	require.Equal(t, float64(5), got[0]["currentTransactionStartOffset"])
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetActiveProducerStatesUnknownClusterIs404(t *testing.T) {
	srv := newTestServer(withTopics(newFakeTopicServicer()))
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/topics/t1/activeproducers", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetActiveProducerStatesBackendFailureIs500(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.producersErr["prod"] = fmt.Errorf("kadm boom")
	srv := newTestServer(withTopics(ft))
	defer srv.Close()
	_, code, _, body := getJSON(t, srv, "/api/clusters/prod/topics/t1/activeproducers", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to describe active producers", "kadm boom")
}

// --- GetTopicConnectors (P1b Task 8a: empty stub, see docs/superpowers/
// plans/2026-07-04-p1b-topics-groups.md's 2026-07-04 revision) ---

// TestGetTopicConnectors locks in the two hard constraints Task 8a's brief
// calls out: a known cluster gets 200, and the body is a genuine JSON array
// literal (never the JSON null a nil slice would serialize as — the
// vendored frontend's Topic.tsx unconditionally reads connectors.length,
// which throws on null). Asserting both the raw bytes and the decoded value
// catches either failure mode: a nil-slice regression would decode to a nil
// got (require.NotNil catches it) while also failing the raw "[]" match.
func TestGetTopicConnectors(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.connectorsKnown["prod"] = true
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	var got []any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/topics/orders/connectors", &got)
	require.Equal(t, 200, code)
	require.Equal(t, "[]", strings.TrimSpace(string(body)), "must serialize as a JSON array literal, never null")
	require.NotNil(t, got, "decoded value must be a non-nil empty slice, not null")
	require.Len(t, got, 0)
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetTopicConnectorsUnknownClusterIs404(t *testing.T) {
	srv := newTestServer(withTopics(newFakeTopicServicer()))
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/topics/t1/connectors", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetTopicConnectorsBackendFailureIs500(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.connectorsErr["prod"] = fmt.Errorf("kadm boom")
	srv := newTestServer(withTopics(ft))
	defer srv.Close()
	_, code, _, body := getJSON(t, srv, "/api/clusters/prod/topics/t1/connectors", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to get topic connectors", "kadm boom")
}

// --- CreateTopic ---

func TestCreateTopic(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.createResult["prod"] = sampleTopicState()
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/topics",
		`{"name":"orders","partitions":2,"replicationFactor":2,"configs":{"retention.ms":"1000"}}`)
	require.Equal(t, 201, code)
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, "orders", got["name"])
	require.Equal(t, float64(2), got["partitionCount"])
	validateAgainstContract(t, req, code, hdr, body)

	require.Equal(t, cluster.TopicSpec{
		Name: "orders", Partitions: 2, ReplicationFactor: 2,
		Configs: map[string]string{"retention.ms": "1000"},
	}, ft.lastCreateSpec["prod"])
}

func TestCreateTopicDefaultsReplicationFactorToClusterDefaultWhenOmitted(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.createResult["prod"] = sampleTopicState()
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	_, code, _, _ := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/topics", `{"name":"orders","partitions":2}`)
	require.Equal(t, 201, code)
	require.Equal(t, int16(-1), ft.lastCreateSpec["prod"].ReplicationFactor)
}

func TestCreateTopicInvalidBodyIs400(t *testing.T) {
	srv := newTestServer(withTopics(newFakeTopicServicer()))
	defer srv.Close()
	_, code, _, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/topics", `{`)
	require.Equal(t, 400, code)
	assertErrorEnvelope(t, body, "invalid request body", "")
}

func TestCreateTopicReadOnlyIs403(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.createResult["prod"] = sampleTopicState()
	srv := newTestServer(withTopics(ft), withReadOnly("prod"))
	defer srv.Close()
	_, code, _, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/topics", `{"name":"orders","partitions":2}`)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "read-only", "")
	require.Zero(t, ft.lastCreateSpec["prod"]) // 守卫拦在 handler 之前
}

func TestCreateTopicUnknownClusterIs404(t *testing.T) {
	srv := newTestServer(withTopics(newFakeTopicServicer()))
	defer srv.Close()
	req, code, hdr, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/nope/topics", `{"name":"orders","partitions":2}`)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestCreateTopicBackendFailureIs500(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.createErr["prod"] = fmt.Errorf("kadm boom")
	srv := newTestServer(withTopics(ft))
	defer srv.Close()
	_, code, _, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/topics", `{"name":"orders","partitions":2}`)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to create topic", "kadm boom")
}

// TestCreateTopicWithUnknownShapeOmitsPartitionAndReplicationCounts covers a
// partitions:-1 ("cluster default") create — appcluster.TopicService.Create
// synthesizes its response from spec's own values (synthesizeTopicState),
// which leaves Partitions/ReplicationFactor at the Go zero value when the
// spec says -1: an honest "unknown", not "this topic has zero partitions".
// A real Kafka topic can never have 0 partitions or 0 replicas, so the JSON
// response must omit partitionCount/replicationFactor entirely rather than
// serialize a misleading literal 0 (topicToGenerated's fix).
func TestCreateTopicWithUnknownShapeOmitsPartitionAndReplicationCounts(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.createResult["prod"] = cluster.TopicState{Name: "t1"} // synthesizeTopicState's output for partitions:-1
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/topics", `{"name":"t1","partitions":-1}`)
	require.Equal(t, 201, code)
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, "t1", got["name"])
	require.NotContains(t, got, "partitionCount", "unknown partition count must be omitted, not a misleading literal 0")
	require.NotContains(t, got, "replicationFactor", "unknown replication factor must be omitted, not a misleading literal 0")
	validateAgainstContract(t, req, code, hdr, body)
}

// --- CreateTopic: P1b Task 8d configs leniency ---

// TestCreateTopicNumericConfigValueCoercesToString mirrors
// TestUpdateTopicNumericConfigValueCoercesToString (below) for CreateTopic:
// Task 9's smoke run only actually caught the 400 on the Update path (the
// vendored frontend's Edit dialog is the one that sends a numeric configs
// value), but Create shares the exact same generated.TopicCreation.Configs
// *map[string]string shape/failure mode, so Task 8d's brief calls for the
// identical leniency fix here too — a preventive fix against the same class
// of bug on a currently-string-only Create path, and parity with Update.
//
// Does not call validateAgainstContract for the same reason the Update
// leniency tests don't: the request (a JSON number for a configs value) is
// deliberately more lenient than the contract's configs:
// additionalProperties: type: string schema. Only the response side is
// checked (validateResponseOnlyAgainstContract).
func TestCreateTopicNumericConfigValueCoercesToString(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.createResult["prod"] = sampleTopicState()
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/topics",
		`{"name":"orders","partitions":2,"replicationFactor":2,"configs":{"retention.bytes":1073741824}}`)
	require.Equal(t, 201, code)
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, "orders", got["name"])
	validateResponseOnlyAgainstContract(t, req, code, hdr, body)

	require.Equal(t, cluster.TopicSpec{
		Name: "orders", Partitions: 2, ReplicationFactor: 2,
		Configs: map[string]string{"retention.bytes": "1073741824"},
	}, ft.lastCreateSpec["prod"])
}

// TestCreateTopicObjectConfigValueIs400 is CreateTopic's counterpart to
// TestUpdateTopicObjectConfigValueIs400 (below) — both endpoints share
// coerceConfigMap, so both must reject the same "configs value is itself a
// nested JSON structure" boundary the same way. Uses an array rather than an
// object (TestUpdateTopicObjectConfigValueIs400's case) to cover
// coerceConfigValue's other non-scalar branch (s[0] == '[') as well. Keeps a
// reference to ft to assert the coerce failure short-circuits before Create
// is ever delegated — the Create twin of TestUpdateTopicObjectConfigValueIs400's
// require.Nil(lastUpdateDesired) check (lastCreateSpec records a struct value,
// so its untouched-marker is the zero TopicSpec, same as TestCreateTopicReadOnlyIs403).
func TestCreateTopicObjectConfigValueIs400(t *testing.T) {
	ft := newFakeTopicServicer()
	srv := newTestServer(withTopics(ft))
	defer srv.Close()
	_, code, _, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/topics",
		`{"name":"orders","partitions":2,"configs":{"retention.ms":[1,2,3]}}`)
	require.Equal(t, 400, code)
	assertErrorEnvelope(t, body, "invalid request body", "")
	require.Zero(t, ft.lastCreateSpec["prod"], "must reject before ever reaching TopicServicer.Create")
}

// --- DeleteTopic ---

func TestDeleteTopic(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.deleteKnown["prod"] = true
	srv := newTestServer(withTopics(ft))
	defer srv.Close()
	req, code, hdr, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/topics/orders", nil)
	require.Equal(t, 204, code)
	require.Equal(t, "orders", ft.lastDeleteTopic["prod"])
	validateAgainstContract(t, req, code, hdr, body)
}

func TestDeleteTopicReadOnlyIs403(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.deleteKnown["prod"] = true
	srv := newTestServer(withTopics(ft), withReadOnly("prod"))
	defer srv.Close()
	_, code, _, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/topics/orders", nil)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "read-only", "")
	require.Empty(t, ft.lastDeleteTopic["prod"])
}

func TestDeleteTopicUnknownClusterIs404(t *testing.T) {
	srv := newTestServer(withTopics(newFakeTopicServicer()))
	defer srv.Close()
	req, code, hdr, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/nope/topics/orders", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestDeleteTopicDisabledFeatureIs403(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.deleteErr["prod"] = appcluster.ErrTopicDeletionDisabled
	srv := newTestServer(withTopics(ft))
	defer srv.Close()
	_, code, _, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/topics/orders", nil)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "topic deletion is disabled", "")
}

func TestDeleteTopicBackendFailureIs500(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.deleteErr["prod"] = fmt.Errorf("kadm boom")
	srv := newTestServer(withTopics(ft))
	defer srv.Close()
	_, code, _, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/topics/orders", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to delete topic", "kadm boom")
}

// --- UpdateTopic ---

func TestUpdateTopic(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.updateResult["prod"] = sampleTopicState()
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/prod/topics/orders",
		`{"configs":{"retention.ms":"2000"}}`)
	require.Equal(t, 200, code)
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, "orders", got["name"])
	validateAgainstContract(t, req, code, hdr, body)
	require.Equal(t, map[string]string{"retention.ms": "2000"}, ft.lastUpdateDesired["prod"])
}

func TestUpdateTopicInvalidBodyIs400(t *testing.T) {
	srv := newTestServer(withTopics(newFakeTopicServicer()))
	defer srv.Close()
	_, code, _, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/prod/topics/orders", `{`)
	require.Equal(t, 400, code)
	assertErrorEnvelope(t, body, "invalid request body", "")
}

func TestUpdateTopicReadOnlyIs403(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.updateResult["prod"] = sampleTopicState()
	srv := newTestServer(withTopics(ft), withReadOnly("prod"))
	defer srv.Close()
	_, code, _, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/prod/topics/orders", `{"configs":{}}`)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "read-only", "")
	require.Nil(t, ft.lastUpdateDesired["prod"])
}

func TestUpdateTopicUnknownClusterIs404(t *testing.T) {
	srv := newTestServer(withTopics(newFakeTopicServicer()))
	defer srv.Close()
	req, code, hdr, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/nope/topics/orders", `{"configs":{}}`)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestUpdateTopicBackendFailureIs500(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.updateErr["prod"] = fmt.Errorf("kadm boom")
	srv := newTestServer(withTopics(ft))
	defer srv.Close()
	_, code, _, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/prod/topics/orders", `{"configs":{}}`)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to update topic configs", "kadm boom")
}

// TestUpdateTopicWithNotYetCachedTopicOmitsPartitionAndReplicationCounts
// covers the not-yet-cached fallback appcluster.TopicService's
// topicStateFromCache returns when a config update lands before the first
// StateCache scrape has picked the topic up (cluster.TopicState{Name: topic},
// Partitions/ReplicationFactor at the Go zero value) — same "absent, not a
// misleading literal 0" contract as the create-synthesized path.
func TestUpdateTopicWithNotYetCachedTopicOmitsPartitionAndReplicationCounts(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.updateResult["prod"] = cluster.TopicState{Name: "orders"} // topicStateFromCache's not-yet-cached fallback
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/prod/topics/orders", `{"configs":{"retention.ms":"2000"}}`)
	require.Equal(t, 200, code)
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, "orders", got["name"])
	require.NotContains(t, got, "partitionCount", "unknown partition count must be omitted, not a misleading literal 0")
	require.NotContains(t, got, "replicationFactor", "unknown replication factor must be omitted, not a misleading literal 0")
	validateAgainstContract(t, req, code, hdr, body)
}

// --- UpdateTopic: P1b Task 8d configs leniency ---
//
// Task 9's acceptance smoke run found that topic "Edit settings -> Update"
// (the vendored frontend's ONLY UI path that edits topic configs) 400ed on
// every submission: formatTopicUpdate (frontend/src/lib/hooks/api/
// topics.ts:204-224) sends a numeric retentionBytes configs value, not a
// string, and generated.TopicUpdate.Configs is a strict *map[string]string —
// json.Decode failed the whole body the instant it hit that one non-string
// value. Upstream's Java/Jackson backend tolerates numeric/boolean configs
// values (coerces to String on deserialize); user ruling: fix the Go backend
// to match (frontend is vendored/off-limits). The tests below lock in
// coerceConfigValue/coerceConfigMap's (handlers_topic.go) fix for that, plus
// the boundary cases the fix must still reject.

// TestUpdateTopicNumericConfigValueCoercesToString is this task's crux test:
// it reproduces Task 9's Bug 1 exactly (a numeric configs value must no
// longer 400) and locks in the one detail that's trivial to get wrong.
//
// 604800000 (a real, common retention.ms value) is deliberately a large
// integer: it is the exact case that breaks if coerceConfigValue is ever
// "simplified" to json.Unmarshal the value into a float64 and fmt.Sprint it
// back — float64 can't exactly represent every int64, and Go's default
// float formatting switches to scientific notation ("6.048e+08") well
// before 604800000, silently corrupting the config. Asserting the *exact*
// string "604800000" (not merely "some non-empty string") is what would
// catch that regression; the explicit require.NotEqual against the
// scientific-notation form makes the failure mode legible if it ever
// recurs.
//
// This test deliberately does NOT call validateAgainstContract: the request
// itself (a JSON number for a configs value) is intentionally more lenient
// than the contract's configs: additionalProperties: type: string schema —
// kin-openapi's ValidateRequest would (correctly, for the contract as
// written) reject it, which is the very divergence this task implements,
// not a regression to catch. Only the response side is checked
// (validateResponseOnlyAgainstContract) — the response shape is unchanged
// by this task.
func TestUpdateTopicNumericConfigValueCoercesToString(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.updateResult["prod"] = sampleTopicState()
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/prod/topics/orders",
		`{"configs":{"retention.ms":604800000}}`)
	require.Equal(t, 200, code)
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, "orders", got["name"])
	validateResponseOnlyAgainstContract(t, req, code, hdr, body)

	require.Equal(t, map[string]string{"retention.ms": "604800000"}, ft.lastUpdateDesired["prod"])
	require.NotEqual(t, "6.048e+08", ft.lastUpdateDesired["prod"]["retention.ms"],
		"must use the raw JSON token text, never round-trip the value through float64")
}

// TestUpdateTopicHugeIntegerConfigValuePreservesPrecisionBeyond2Pow53 is the
// deeper sibling of TestUpdateTopicNumericConfigValueCoercesToString: that
// one's 604800000 only pins down "the string is not scientific notation",
// which a value below 2^53 (float64's exact-integer ceiling) can't fully
// distinguish from "the value is still correct". This one pins down the
// stronger property — the exact integer VALUE survives — by using
// 9007199254740993 (= 2^53 + 1), the smallest positive integer float64
// cannot represent: round-tripping it through a float64 silently collapses
// it to 9007199254740992 (2^53), a wrong value with no scientific notation
// to give the corruption away. coerceConfigValue takes the raw JSON token
// text verbatim, so all 16 digits are preserved. This is the single test
// that turns red the day someone "optimizes" the raw-token path into an
// int64/float64 parse — the regression this whole task exists to prevent.
// Same contract-validation carve-out as the numeric test (a JSON number
// against `type: string` is the deliberate divergence, not a bug).
func TestUpdateTopicHugeIntegerConfigValuePreservesPrecisionBeyond2Pow53(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.updateResult["prod"] = sampleTopicState()
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	req, code, hdr, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/prod/topics/orders",
		`{"configs":{"retention.ms":9007199254740993}}`)
	require.Equal(t, 200, code)
	validateResponseOnlyAgainstContract(t, req, code, hdr, body)

	require.Equal(t, map[string]string{"retention.ms": "9007199254740993"}, ft.lastUpdateDesired["prod"],
		"all 16 digits must survive: 9007199254740993 = 2^53+1 collapses to 9007199254740992 if ever parsed through float64")
	require.NotEqual(t, "9007199254740992", ft.lastUpdateDesired["prod"]["retention.ms"],
		"a float64 round-trip would corrupt 2^53+1 down to 2^53 — the raw JSON token text must be used untouched")
}

// TestUpdateTopicNegativeSentinelConfigValueCoercesToString covers a negative
// JSON number — retention.ms=-1 is Kafka's own legal sentinel for "retain
// forever", so a "-" leading byte is a real, valid configs value the frontend
// could plausibly send as a bare number, not just the digit/quote/brace bytes
// the other cases exercise. coerceConfigValue's default (scalar) branch
// returns the raw token "-1" verbatim. Same contract-validation carve-out as
// its sibling numeric cases.
func TestUpdateTopicNegativeSentinelConfigValueCoercesToString(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.updateResult["prod"] = sampleTopicState()
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	req, code, hdr, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/prod/topics/orders",
		`{"configs":{"retention.ms":-1}}`)
	require.Equal(t, 200, code)
	validateResponseOnlyAgainstContract(t, req, code, hdr, body)

	require.Equal(t, map[string]string{"retention.ms": "-1"}, ft.lastUpdateDesired["prod"])
}

// TestUpdateTopicBooleanConfigValueCoercesToString covers the boolean half of
// the same leniency: a JSON true/false configs value must coerce to the
// literal strings "true"/"false" (Kafka's own boolean config string
// vocabulary), not fail the decode. Same contract-validation carve-out as
// TestUpdateTopicNumericConfigValueCoercesToString, same reason.
func TestUpdateTopicBooleanConfigValueCoercesToString(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.updateResult["prod"] = sampleTopicState()
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	req, code, hdr, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/prod/topics/orders",
		`{"configs":{"unclean.leader.election.enable":true,"delete.retention.ms":false}}`)
	require.Equal(t, 200, code)
	validateResponseOnlyAgainstContract(t, req, code, hdr, body)

	require.Equal(t, map[string]string{
		"unclean.leader.election.enable": "true",
		"delete.retention.ms":            "false",
	}, ft.lastUpdateDesired["prod"])
}

// TestUpdateTopicNullConfigValueIsSkipped covers the "边界" decision Task 8d's
// brief calls out for a JSON null configs value: skip the key entirely
// (conservative — the vendored frontend never actually sends one) rather
// than coercing it to the empty string, which would silently mean something
// different ("set this config to empty" instead of "leave it alone"). Mixed
// with an ordinary string-valued key in the same request to prove only the
// null key is dropped, not the whole map. Same contract-validation carve-out
// as the numeric/boolean cases: a null against `type: string` is exactly as
// much a deliberate divergence as a number or bool is.
func TestUpdateTopicNullConfigValueIsSkipped(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.updateResult["prod"] = sampleTopicState()
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	req, code, hdr, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/prod/topics/orders",
		`{"configs":{"retention.ms":"2000","cleanup.policy":null}}`)
	require.Equal(t, 200, code)
	validateResponseOnlyAgainstContract(t, req, code, hdr, body)

	require.Equal(t, map[string]string{"retention.ms": "2000"}, ft.lastUpdateDesired["prod"])
	require.NotContains(t, ft.lastUpdateDesired["prod"], "cleanup.policy")
}

// TestUpdateTopicObjectConfigValueIs400 covers the other boundary Task 8d's
// brief calls out: a configs value that's itself a JSON object is genuinely
// invalid (there's no sensible string coercion for a nested structure), so
// coerceConfigValue must error and the endpoint must still 400 — leniency
// for scalars doesn't mean "anything goes". Unlike the leniency tests above,
// this is a real rejection (same "invalid request body" 400 as a syntax
// error), not a divergence, so no contract-validation carve-out is needed —
// it simply isn't asserted against the contract, same convention every other
// *InvalidBodyIs400 test in this file already follows (kin-openapi's own
// request validation would reject the same body for the same reason before
// a response is even produced).
func TestUpdateTopicObjectConfigValueIs400(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.updateResult["prod"] = sampleTopicState()
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	_, code, _, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/prod/topics/orders",
		`{"configs":{"retention.ms":{"nested":"oops"}}}`)
	require.Equal(t, 400, code)
	assertErrorEnvelope(t, body, "invalid request body", "")
	require.Nil(t, ft.lastUpdateDesired["prod"], "must reject before ever reaching TopicServicer.UpdateConfigs")
}

// --- RecreateTopic ---

func TestRecreateTopic(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.recreateResult["prod"] = sampleTopicState()
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := doJSON(t, http.MethodPost, srv, "/api/clusters/prod/topics/orders", &got)
	require.Equal(t, 201, code)
	require.Equal(t, "orders", got["name"])
	validateAgainstContract(t, req, code, hdr, body)
}

func TestRecreateTopicReadOnlyIs403(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.recreateResult["prod"] = sampleTopicState()
	srv := newTestServer(withTopics(ft), withReadOnly("prod"))
	defer srv.Close()
	_, code, _, body := doJSON(t, http.MethodPost, srv, "/api/clusters/prod/topics/orders", nil)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "read-only", "")
}

func TestRecreateTopicUnknownClusterIs404(t *testing.T) {
	srv := newTestServer(withTopics(newFakeTopicServicer()))
	defer srv.Close()
	req, code, hdr, body := doJSON(t, http.MethodPost, srv, "/api/clusters/nope/topics/orders", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestRecreateTopicDisabledFeatureIs403(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.recreateErr["prod"] = appcluster.ErrTopicDeletionDisabled
	srv := newTestServer(withTopics(ft))
	defer srv.Close()
	_, code, _, body := doJSON(t, http.MethodPost, srv, "/api/clusters/prod/topics/orders", nil)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "topic deletion is disabled", "")
}

func TestRecreateTopicBackendFailureIs500(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.recreateErr["prod"] = fmt.Errorf("kadm boom")
	srv := newTestServer(withTopics(ft))
	defer srv.Close()
	_, code, _, body := doJSON(t, http.MethodPost, srv, "/api/clusters/prod/topics/orders", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to recreate topic", "kadm boom")
}

// --- CloneTopic ---

func TestCloneTopic(t *testing.T) {
	ft := newFakeTopicServicer()
	clone := sampleTopicState()
	clone.Name = "orders-clone"
	ft.cloneResult["prod"] = clone
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := doJSON(t, http.MethodPost, srv, "/api/clusters/prod/topics/orders/clone?newTopicName=orders-clone", &got)
	require.Equal(t, 201, code)
	require.Equal(t, "orders-clone", got["name"])
	validateAgainstContract(t, req, code, hdr, body)
	require.Equal(t, [2]string{"orders", "orders-clone"}, ft.lastCloneArgs["prod"])
}

func TestCloneTopicReadOnlyIs403(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.cloneResult["prod"] = sampleTopicState()
	srv := newTestServer(withTopics(ft), withReadOnly("prod"))
	defer srv.Close()
	_, code, _, body := doJSON(t, http.MethodPost, srv, "/api/clusters/prod/topics/orders/clone?newTopicName=orders-clone", nil)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "read-only", "")
}

func TestCloneTopicUnknownClusterIs404(t *testing.T) {
	srv := newTestServer(withTopics(newFakeTopicServicer()))
	defer srv.Close()
	req, code, hdr, body := doJSON(t, http.MethodPost, srv, "/api/clusters/nope/topics/orders/clone?newTopicName=orders-clone", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestCloneTopicBackendFailureIs500(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.cloneErr["prod"] = fmt.Errorf("kadm boom")
	srv := newTestServer(withTopics(ft))
	defer srv.Close()
	_, code, _, body := doJSON(t, http.MethodPost, srv, "/api/clusters/prod/topics/orders/clone?newTopicName=orders-clone", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to clone topic", "kadm boom")
}

// --- IncreaseTopicPartitions ---

func TestIncreaseTopicPartitions(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.increaseKnown["prod"] = true
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/prod/topics/orders/partitions",
		`{"totalPartitionsCount":6}`)
	require.Equal(t, 200, code)
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, "orders", got["topicName"])
	require.Equal(t, float64(6), got["totalPartitionsCount"])
	validateAgainstContract(t, req, code, hdr, body)
	require.Equal(t, int32(6), ft.lastIncreaseTotal["prod"])
}

func TestIncreaseTopicPartitionsInvalidBodyIs400(t *testing.T) {
	srv := newTestServer(withTopics(newFakeTopicServicer()))
	defer srv.Close()
	_, code, _, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/prod/topics/orders/partitions", `{`)
	require.Equal(t, 400, code)
	assertErrorEnvelope(t, body, "invalid request body", "")
}

func TestIncreaseTopicPartitionsReadOnlyIs403(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.increaseKnown["prod"] = true
	srv := newTestServer(withTopics(ft), withReadOnly("prod"))
	defer srv.Close()
	_, code, _, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/prod/topics/orders/partitions", `{"totalPartitionsCount":6}`)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "read-only", "")
}

func TestIncreaseTopicPartitionsUnknownClusterIs404(t *testing.T) {
	srv := newTestServer(withTopics(newFakeTopicServicer()))
	defer srv.Close()
	req, code, hdr, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/nope/topics/orders/partitions", `{"totalPartitionsCount":6}`)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestIncreaseTopicPartitionsBackendFailureIs500(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.increaseErr["prod"] = fmt.Errorf("kadm boom")
	srv := newTestServer(withTopics(ft))
	defer srv.Close()
	_, code, _, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/prod/topics/orders/partitions", `{"totalPartitionsCount":6}`)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to increase topic partitions", "kadm boom")
}

// --- ChangeReplicationFactor ---

func TestChangeReplicationFactor(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.changeRFKnown["prod"] = true
	srv := newTestServer(withTopics(ft))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/prod/topics/orders/replications",
		`{"totalReplicationFactor":3}`)
	require.Equal(t, 200, code)
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, "orders", got["topicName"])
	require.Equal(t, float64(3), got["totalReplicationFactor"])
	validateAgainstContract(t, req, code, hdr, body)
	require.Equal(t, int16(3), ft.lastChangeRFTarget["prod"])
}

func TestChangeReplicationFactorInvalidBodyIs400(t *testing.T) {
	srv := newTestServer(withTopics(newFakeTopicServicer()))
	defer srv.Close()
	_, code, _, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/prod/topics/orders/replications", `{`)
	require.Equal(t, 400, code)
	assertErrorEnvelope(t, body, "invalid request body", "")
}

func TestChangeReplicationFactorInvalidTargetIs400(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.changeRFErr["prod"] = appcluster.ErrInvalidReplicationFactor
	srv := newTestServer(withTopics(ft))
	defer srv.Close()
	req, code, hdr, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/prod/topics/orders/replications",
		`{"totalReplicationFactor":99}`)
	require.Equal(t, 400, code)
	assertErrorEnvelope(t, body, "invalid target replication factor", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestChangeReplicationFactorReadOnlyIs403(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.changeRFKnown["prod"] = true
	srv := newTestServer(withTopics(ft), withReadOnly("prod"))
	defer srv.Close()
	_, code, _, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/prod/topics/orders/replications", `{"totalReplicationFactor":3}`)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "read-only", "")
}

func TestChangeReplicationFactorUnknownClusterIs404(t *testing.T) {
	srv := newTestServer(withTopics(newFakeTopicServicer()))
	defer srv.Close()
	req, code, hdr, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/nope/topics/orders/replications", `{"totalReplicationFactor":3}`)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestChangeReplicationFactorBackendFailureIs500(t *testing.T) {
	ft := newFakeTopicServicer()
	ft.changeRFErr["prod"] = fmt.Errorf("kadm boom")
	srv := newTestServer(withTopics(ft))
	defer srv.Close()
	_, code, _, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/prod/topics/orders/replications", `{"totalReplicationFactor":3}`)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to change replication factor", "kadm boom")
}
