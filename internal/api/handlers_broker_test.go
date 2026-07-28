package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/api"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// fakeBrokerStater backs api.ClusterStater, api.LogDirser and api.BrokerAdmin
// for the brokers endpoint tests: a single fake wired into all three Deps
// fields (mirroring how app.StateCache and app.BrokerService together
// satisfy all three interfaces in production), so withState/withLogDirs/etc. options
// compose freely within one newTestServer call. Its zero value is unsafe
// (nil maps) — always construct via newFakeBrokerStater, never a bare
// literal, so every lookup misses cleanly rather than panicking on a nil map
// write or a nil interface.
type fakeBrokerStater struct {
	states    map[string]cluster.RuntimeState
	logdirs   map[string][]cluster.BrokerLogDirs
	logdirErr map[string]error // per-cluster errors for LogDirs

	brokerConfigs    map[string][]cluster.ConfigEntry // GetBrokerConfig canned responses
	brokerConfigsErr map[string]error                 // per-cluster errors for BrokerConfigs (cluster known but backend fails)

	// known marks clusters as existing for AlterBrokerConfig/MoveReplicaLogDir:
	// unlike BrokerConfigs/LogDirs above, these are write-only operations with
	// no natural "canned data" map whose key presence can double as the
	// "known cluster" signal, so they get an explicit one.
	known map[string]bool

	alterCalls map[string]alterConfigCall // last AlterBrokerConfig call per cluster (nil unless a test supplies its own map)
	alterErr   map[string]error           // per-cluster errors for AlterBrokerConfig

	moveCalls map[string]moveLogDirCall // last MoveReplicaLogDir call per cluster (nil unless a test supplies its own map)
	moveErr   map[string]error          // per-cluster errors for MoveReplicaLogDir
}

// alterConfigCall records one AlterBrokerConfig invocation's arguments, so
// tests can assert what a PUT request's body/path params reached the port —
// UpdateBrokerConfigByName responds 204 No Content, so this is the only way
// to observe what actually got forwarded.
type alterConfigCall struct {
	broker      int32
	name, value string
}

// moveLogDirCall mirrors alterConfigCall for MoveReplicaLogDir.
type moveLogDirCall struct {
	broker    int32
	topic     string
	partition int32
	dir       string
}

// newFakeBrokerStater builds a fakeBrokerStater with every map allocated
// (never nil), safe to use directly as a BrokerAdmin/LogDirser/ClusterStater
// with no clusters registered (every lookup reports unknown-cluster, never
// panics on a nil map write).
func newFakeBrokerStater() fakeBrokerStater {
	return fakeBrokerStater{
		states: map[string]cluster.RuntimeState{}, logdirs: map[string][]cluster.BrokerLogDirs{}, logdirErr: map[string]error{},
		brokerConfigs: map[string][]cluster.ConfigEntry{}, brokerConfigsErr: map[string]error{},
		known: map[string]bool{}, alterCalls: map[string]alterConfigCall{}, alterErr: map[string]error{},
		moveCalls: map[string]moveLogDirCall{}, moveErr: map[string]error{},
	}
}

func (f fakeBrokerStater) List(context.Context) []cluster.Snapshot { return nil }

func (f fakeBrokerStater) Get(_ context.Context, name string) (cluster.RuntimeState, bool) {
	st, ok := f.states[name]
	return st, ok
}

func (f fakeBrokerStater) Refresh(_ context.Context, name string) (cluster.RuntimeState, error) {
	if st, ok := f.states[name]; ok {
		return st, nil
	}
	return cluster.RuntimeState{}, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
}

// LogDirs replicates the real StateCache->Pool filtering contract (brokers
// empty/nil => everything; otherwise only the requested broker IDs), so a
// test hitting ?broker= actually exercises the handler's query parsing, not
// just a fake that ignores its arguments.
func (f fakeBrokerStater) LogDirs(_ context.Context, name string, brokers []int32) ([]cluster.BrokerLogDirs, error) {
	if err, ok := f.logdirErr[name]; ok {
		return nil, err
	}
	all, ok := f.logdirs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	if len(brokers) == 0 {
		return all, nil
	}
	want := map[int32]bool{}
	for _, b := range brokers {
		want[b] = true
	}
	var out []cluster.BrokerLogDirs
	for _, d := range all {
		if want[d.Broker] {
			out = append(out, d)
		}
	}
	return out, nil
}

// BrokerConfigs mirrors LogDirs' error-then-presence pattern: an explicit
// per-cluster error takes priority, otherwise absence from the canned-data
// map means "unknown cluster".
func (f fakeBrokerStater) BrokerConfigs(_ context.Context, name string, _ int32) ([]cluster.ConfigEntry, error) {
	if err, ok := f.brokerConfigsErr[name]; ok {
		return nil, err
	}
	cfgs, ok := f.brokerConfigs[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return cfgs, nil
}

// AlterBrokerConfig records its arguments into alterCalls (if the test
// supplied a map to record into) and reports unknown-cluster for any name
// not registered via withAlterableCluster/withAlterError.
func (f fakeBrokerStater) AlterBrokerConfig(_ context.Context, name string, broker int32, cfgName, value string) error {
	if err, ok := f.alterErr[name]; ok {
		return err
	}
	if !f.known[name] {
		return fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	if f.alterCalls != nil {
		f.alterCalls[name] = alterConfigCall{broker: broker, name: cfgName, value: value}
	}
	return nil
}

// MoveReplicaLogDir mirrors AlterBrokerConfig above.
func (f fakeBrokerStater) MoveReplicaLogDir(_ context.Context, name string, broker int32, topic string, partition int32, dir string) error {
	if err, ok := f.moveErr[name]; ok {
		return err
	}
	if !f.known[name] {
		return fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	if f.moveCalls != nil {
		f.moveCalls[name] = moveLogDirCall{broker: broker, topic: topic, partition: partition, dir: dir}
	}
	return nil
}

// brokerStaterFrom returns the fakeBrokerStater already wired into d by an
// earlier withState/withLogDirs/etc. option in the same newTestServer call,
// or a fresh one — letting options compose without clobbering each other.
func brokerStaterFrom(d *api.Deps) fakeBrokerStater {
	if fb, ok := d.States.(fakeBrokerStater); ok {
		return fb
	}
	return newFakeBrokerStater()
}

func withState(name string, st cluster.RuntimeState) testServerOption {
	return func(d *api.Deps) {
		fb := brokerStaterFrom(d)
		fb.states[name] = st
		d.States, d.LogDirs, d.Brokers = fb, fb, fb
	}
}

func withLogDirs(name string, dirs ...cluster.BrokerLogDirs) testServerOption {
	return func(d *api.Deps) {
		fb := brokerStaterFrom(d)
		fb.logdirs[name] = dirs
		d.States, d.LogDirs, d.Brokers = fb, fb, fb
	}
}

func withLogDirsError(name string, err error) testServerOption {
	return func(d *api.Deps) {
		fb := brokerStaterFrom(d)
		fb.logdirErr[name] = err
		d.States, d.LogDirs, d.Brokers = fb, fb, fb
	}
}

// withBrokerConfigs registers a canned GetBrokerConfig response for a cluster.
func withBrokerConfigs(name string, cfgs []cluster.ConfigEntry) testServerOption {
	return func(d *api.Deps) {
		fb := brokerStaterFrom(d)
		fb.brokerConfigs[name] = cfgs
		d.States, d.LogDirs, d.Brokers = fb, fb, fb
	}
}

// withBrokerConfigsError registers a cluster as known-but-failing for
// BrokerConfigs (backend-failure 500 path).
func withBrokerConfigsError(name string, err error) testServerOption {
	return func(d *api.Deps) {
		fb := brokerStaterFrom(d)
		fb.brokerConfigsErr[name] = err
		d.States, d.LogDirs, d.Brokers = fb, fb, fb
	}
}

// withAlterableCluster marks name as known to AlterBrokerConfig (so calls
// succeed instead of hitting unknown-cluster). If calls is non-nil, it must
// be a map the test itself constructed and retained: AlterBrokerConfig
// writes into it by reference (maps are reference types, so the rebind here
// is visible to every later copy of the fake), letting the test read back
// what the handler forwarded after a 204-No-Content round trip that has no
// body to assert on otherwise.
func withAlterableCluster(name string, calls map[string]alterConfigCall) testServerOption {
	return func(d *api.Deps) {
		fb := brokerStaterFrom(d)
		fb.known[name] = true
		if calls != nil {
			fb.alterCalls = calls
		}
		d.States, d.LogDirs, d.Brokers = fb, fb, fb
	}
}

// withAlterError registers a cluster as known-but-failing for
// AlterBrokerConfig (backend-failure 500 path).
func withAlterError(name string, err error) testServerOption {
	return func(d *api.Deps) {
		fb := brokerStaterFrom(d)
		fb.known[name] = true
		fb.alterErr[name] = err
		d.States, d.LogDirs, d.Brokers = fb, fb, fb
	}
}

// withMovableCluster mirrors withAlterableCluster for MoveReplicaLogDir.
func withMovableCluster(name string, calls map[string]moveLogDirCall) testServerOption {
	return func(d *api.Deps) {
		fb := brokerStaterFrom(d)
		fb.known[name] = true
		if calls != nil {
			fb.moveCalls = calls
		}
		d.States, d.LogDirs, d.Brokers = fb, fb, fb
	}
}

// withMoveError registers a cluster as known-but-failing for
// MoveReplicaLogDir (backend-failure 500 path).
func withMoveError(name string, err error) testServerOption {
	return func(d *api.Deps) {
		fb := brokerStaterFrom(d)
		fb.known[name] = true
		fb.moveErr[name] = err
		d.States, d.LogDirs, d.Brokers = fb, fb, fb
	}
}

func onlineStateOneBroker() cluster.RuntimeState {
	return cluster.RuntimeState{
		Status: cluster.StatusOnline,
		Brokers: []cluster.BrokerInfo{{ID: 1, Host: "h", Port: 9092,
			PartitionsLeader: 3, Partitions: 6, InSyncPartitions: 6}},
	}
}

func TestGetBrokers(t *testing.T) {
	srv := newTestServer(withState("prod", cluster.RuntimeState{
		Status: cluster.StatusOnline,
		Brokers: []cluster.BrokerInfo{{ID: 1, Host: "h", Port: 9092,
			PartitionsLeader: 3, Partitions: 6, InSyncPartitions: 6}},
		Disk: []cluster.DiskUsage{{Broker: 1, SegmentSize: 1024, SegmentCount: 4}},
	}))
	defer srv.Close()
	var got []map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/brokers", &got)
	require.Equal(t, 200, code)
	require.Len(t, got, 1)
	require.Equal(t, float64(1), got[0]["id"])
	require.Equal(t, "h", got[0]["host"])
	require.Equal(t, float64(3), got[0]["partitionsLeader"])
	require.Equal(t, float64(6), got[0]["partitions"])
	require.Equal(t, float64(6), got[0]["inSyncPartitions"])
	require.NotContains(t, got[0], "segmentSize") // contract Broker schema has no disk fields, so not mapped (see task-5 report)
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetBrokersUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/brokers", nil)
	require.Equal(t, 404, code)
	validateAgainstContract(t, req, code, hdr, body)
}

// TestGetAllBrokersLogdirsFiltersByQuery: fake 返回两 broker 的 log dirs；无
// query 时两者全回，?broker=1 过滤后只剩 broker 1 的那一条。
func TestGetAllBrokersLogdirsFiltersByQuery(t *testing.T) {
	srv := newTestServer(withLogDirs("prod",
		cluster.BrokerLogDirs{Broker: 1, Dir: "/kafka/data-0", Topics: []cluster.TopicLogDirs{
			{Topic: "t1", Partitions: []cluster.PartitionLogDir{{Partition: 0, Size: 100, OffsetLag: 1}}},
		}},
		cluster.BrokerLogDirs{Broker: 2, Dir: "/kafka/data-1", Topics: []cluster.TopicLogDirs{
			{Topic: "t1", Partitions: []cluster.PartitionLogDir{{Partition: 1, Size: 200, OffsetLag: 2}}},
		}},
	))
	defer srv.Close()

	var all []map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/brokers/logdirs", &all)
	require.Equal(t, 200, code)
	require.Len(t, all, 2)
	validateAgainstContract(t, req, code, hdr, body)

	var filtered []map[string]any
	req, code, hdr, body = getJSON(t, srv, "/api/clusters/prod/brokers/logdirs?broker=1", &filtered)
	require.Equal(t, 200, code)
	require.Len(t, filtered, 1)
	require.Equal(t, "/kafka/data-0", filtered[0]["name"])
	topics, ok := filtered[0]["topics"].([]any)
	require.True(t, ok)
	require.Len(t, topics, 1)
	topic0, ok := topics[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "t1", topic0["name"])
	parts, ok := topic0["partitions"].([]any)
	require.True(t, ok)
	require.Len(t, parts, 1)
	part0, ok := parts[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, float64(1), part0["broker"]) // 每个 partition 条目携带其归属 broker ID
	validateAgainstContract(t, req, code, hdr, body)
}

// TestGetAllBrokersLogdirsFiltersByMultipleBrokers covers the ?broker= param
// with more than one ID. The generated binding for this param is
// style=form/explode=false (contract declares no explode:true override), and
// oapi-codegen's runtime for that combination only understands ONE occurrence
// of the query key with comma-separated values (?broker=1,2) — a repeated key
// (?broker=1&broker=2) trips its own "parameter is not exploded, but is
// specified multiple times" guard before GetAllBrokersLogdirs (or even
// LogDirser.LogDirs) ever runs, surfacing as a plain-text 400 from the
// generated default ErrorHandlerFunc (not our JSON error envelope) —
// confirmed empirically below, not just by reading the runtime source
// (github.com/oapi-codegen/runtime@v1.4.2 bindparam.go
// BindQueryParameterWithOptions, style "form" + explode=false branch).
func TestGetAllBrokersLogdirsFiltersByMultipleBrokers(t *testing.T) {
	srv := newTestServer(withLogDirs("prod",
		cluster.BrokerLogDirs{Broker: 1, Dir: "/kafka/data-0"},
		cluster.BrokerLogDirs{Broker: 2, Dir: "/kafka/data-1"},
		cluster.BrokerLogDirs{Broker: 3, Dir: "/kafka/data-2"},
	))
	defer srv.Close()

	// Supported form: one occurrence, comma-separated -> binds as []int32{1,2}.
	var got []map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/brokers/logdirs?broker=1,2", &got)
	require.Equal(t, 200, code)
	require.Len(t, got, 2)
	validateAgainstContract(t, req, code, hdr, body)

	// Other form (repeated key): rejected at the binding layer, never reaches
	// the handler/port — documented here rather than only in the task report.
	code, _ = get(t, srv.URL+"/api/clusters/prod/brokers/logdirs?broker=1&broker=2")
	require.Equal(t, 400, code)
}

func TestGetAllBrokersLogdirsUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/brokers/logdirs", nil)
	require.Equal(t, 404, code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(body, &errResp))
	require.Contains(t, errResp["message"], "cluster not found")
	require.NotZero(t, errResp["code"]) // errorResponse fills contract-required fields
	require.NotEmpty(t, errResp["requestId"])
	require.NotZero(t, errResp["timestamp"])

	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetAllBrokersLogdirsBackendFailureIs500(t *testing.T) {
	srv := newTestServer(withLogDirsError("prod", fmt.Errorf("backend error")))
	defer srv.Close()
	_, code, _, body := getJSON(t, srv, "/api/clusters/prod/brokers/logdirs", nil)
	require.Equal(t, 500, code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(body, &errResp))
	require.Contains(t, errResp["message"], "failed to describe log dirs")
	require.NotZero(t, errResp["code"]) // errorResponse fills contract-required fields
	require.NotEmpty(t, errResp["requestId"])
	require.NotZero(t, errResp["timestamp"])
	// Contract only declares 200, so no validateAgainstContract for 500.
}

func TestGetBrokersCsvUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/brokers/csv", nil)
	require.Equal(t, 404, code)
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetBrokersCsv(t *testing.T) {
	srv := newTestServer(withState("prod", onlineStateOneBroker()))
	defer srv.Close()
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/api/clusters/prod/brokers/csv", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, 200, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Content-Type"), "text/csv")
	b, _ := io.ReadAll(resp.Body)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	require.Len(t, lines, 2) // 表头 + 1 数据行
	require.Contains(t, lines[0], "id")
	validateAgainstContract(t, req, resp.StatusCode, resp.Header, b)
}

// assertErrorEnvelope checks the three contract-required ErrorResponse fields
// errorResponse() fills beyond the bare message (code/requestId/timestamp),
// and that msg does not leak wantAbsent (the raw driver error text) — the
// "500 must not leak the underlying error" rule from the task brief.
func assertErrorEnvelope(t *testing.T, body []byte, wantMsg, wantAbsent string) {
	t.Helper()
	var errResp map[string]any
	require.NoError(t, json.Unmarshal(body, &errResp))
	require.Contains(t, errResp["message"], wantMsg)
	if wantAbsent != "" {
		require.NotContains(t, errResp["message"], wantAbsent)
	}
	require.NotZero(t, errResp["code"])
	require.NotEmpty(t, errResp["requestId"])
	require.NotZero(t, errResp["timestamp"])
}

// --- GetBrokerConfig ---

func TestGetBrokerConfig(t *testing.T) {
	srv := newTestServer(withBrokerConfigs("prod", []cluster.ConfigEntry{
		{
			Name: "log.retention.ms", Value: "604800000", Source: "DYNAMIC_BROKER_CONFIG",
			IsSensitive: false, IsReadOnly: false,
			Synonyms: []cluster.ConfigSynonym{{Name: "log.retention.ms", Value: "604800000", Source: "DEFAULT_CONFIG"}},
		},
		{Name: "sasl.jaas.config", Value: "", Source: "STATIC_BROKER_CONFIG", IsSensitive: true},
	}))
	defer srv.Close()

	var got []map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/brokers/1/configs", &got)
	require.Equal(t, 200, code)
	require.Len(t, got, 2)
	require.Equal(t, "log.retention.ms", got[0]["name"])
	require.Equal(t, "604800000", got[0]["value"])
	require.Equal(t, "DYNAMIC_BROKER_CONFIG", got[0]["source"])
	require.Equal(t, false, got[0]["isSensitive"])
	require.Equal(t, false, got[0]["isReadOnly"])
	syns, ok := got[0]["synonyms"].([]any)
	require.True(t, ok)
	require.Len(t, syns, 1)
	syn0, ok := syns[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "DEFAULT_CONFIG", syn0["source"])
	require.Equal(t, true, got[1]["isSensitive"])
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetBrokerConfigUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/brokers/1/configs", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetBrokerConfigBackendFailureIs500(t *testing.T) {
	srv := newTestServer(withBrokerConfigsError("prod", fmt.Errorf("kadm boom")))
	defer srv.Close()
	_, code, _, body := getJSON(t, srv, "/api/clusters/prod/brokers/1/configs", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to describe broker config", "kadm boom")
	// Contract only declares 200 for getBrokerConfig, so no validateAgainstContract here.
}

// bodyJSON issues method against path with a JSON request body, returning
// the request/response/body triple. http.Client.Do drains req.Body while
// writing the request to the wire, so a *http.Request already passed through
// Do can't be handed to validateAgainstContract as-is — its body would read
// back empty (kin-openapi would then report the requestBody itself as
// "required but missing", not a schema mismatch). Rewinding via req.GetBody
// (which http.NewRequest populates automatically for a *strings.Reader body)
// before returning fixes that for every caller in one place.
func bodyJSON(t *testing.T, method string, srv *httptest.Server, path, reqBody string) (*http.Request, int, http.Header, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(reqBody))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(resp.Body)
	req.Body, err = req.GetBody()
	require.NoError(t, err)
	return req, resp.StatusCode, resp.Header, respBody
}

// --- UpdateBrokerConfigByName ---

func TestUpdateBrokerConfigByName(t *testing.T) {
	calls := map[string]alterConfigCall{}
	srv := newTestServer(withAlterableCluster("prod", calls))
	defer srv.Close()

	req, code, hdr, body := bodyJSON(t, http.MethodPut, srv, "/api/clusters/prod/brokers/1/configs/log.retention.ms", `{"value":"2"}`)
	require.Equal(t, 204, code)
	require.Empty(t, body)
	validateAgainstContract(t, req, code, hdr, body)

	require.Equal(t, alterConfigCall{broker: 1, name: "log.retention.ms", value: "2"}, calls["prod"])
}

// TestUpdateBrokerConfigByNameInvalidBodyIs400 does not call
// validateAgainstContract: kin-openapi's request validation decodes the same
// malformed body against the BrokerConfigItem schema before the response is
// even produced, so it would fail for the same reason our handler's decode
// does — a request-validation failure, not a response-shape assertion, which
// is not what this test is checking. The 500 tests in this file skip contract
// validation for the analogous reason the getAllBrokersLogdirs 500 test does
// (task-5-report.md §"契约 500 声明查询结果"): the status code path isn't the
// thing under contract test here.
func TestUpdateBrokerConfigByNameInvalidBodyIs400(t *testing.T) {
	calls := map[string]alterConfigCall{}
	srv := newTestServer(withAlterableCluster("prod", calls))
	defer srv.Close()

	_, code, _, body := bodyJSON(t, http.MethodPut, srv, "/api/clusters/prod/brokers/1/configs/log.retention.ms", `{`)
	require.Equal(t, 400, code)
	assertErrorEnvelope(t, body, "invalid request body", "")
	require.Equal(t, alterConfigCall{}, calls["prod"]) // handler must not have reached the port
}

func TestUpdateBrokerConfigByNameUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := bodyJSON(t, http.MethodPut, srv, "/api/clusters/nope/brokers/1/configs/log.retention.ms", `{"value":"2"}`)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestUpdateBrokerConfigByNameBackendFailureIs500(t *testing.T) {
	srv := newTestServer(withAlterError("prod", fmt.Errorf("kadm boom")))
	defer srv.Close()
	_, code, _, body := bodyJSON(t, http.MethodPut, srv, "/api/clusters/prod/brokers/1/configs/log.retention.ms", `{"value":"2"}`)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to alter broker config", "kadm boom")
}

// --- UpdateBrokerTopicPartitionLogDir ---

func TestUpdateBrokerTopicPartitionLogDir(t *testing.T) {
	calls := map[string]moveLogDirCall{}
	srv := newTestServer(withMovableCluster("prod", calls))
	defer srv.Close()

	req, code, hdr, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/prod/brokers/1/logdirs",
		`{"topic":"t1","partition":2,"logDir":"/kafka/data-1"}`)
	require.Equal(t, 204, code)
	require.Empty(t, body)
	validateAgainstContract(t, req, code, hdr, body)

	require.Equal(t, moveLogDirCall{broker: 1, topic: "t1", partition: 2, dir: "/kafka/data-1"}, calls["prod"])
}

// TestUpdateBrokerTopicPartitionLogDirInvalidBodyIs400 skips
// validateAgainstContract for the same reason
// TestUpdateBrokerConfigByNameInvalidBodyIs400 does (see its comment).
func TestUpdateBrokerTopicPartitionLogDirInvalidBodyIs400(t *testing.T) {
	calls := map[string]moveLogDirCall{}
	srv := newTestServer(withMovableCluster("prod", calls))
	defer srv.Close()

	_, code, _, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/prod/brokers/1/logdirs", `{`)
	require.Equal(t, 400, code)
	assertErrorEnvelope(t, body, "invalid request body", "")
	require.Equal(t, moveLogDirCall{}, calls["prod"])
}

func TestUpdateBrokerTopicPartitionLogDirUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/nope/brokers/1/logdirs",
		`{"topic":"t1","partition":0,"logDir":"/d"}`)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestUpdateBrokerTopicPartitionLogDirBackendFailureIs500(t *testing.T) {
	srv := newTestServer(withMoveError("prod", fmt.Errorf("kadm boom")))
	defer srv.Close()
	_, code, _, body := bodyJSON(t, http.MethodPatch, srv, "/api/clusters/prod/brokers/1/logdirs",
		`{"topic":"t1","partition":0,"logDir":"/d"}`)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to move replica log dir", "kadm boom")
}

// --- GetBrokersMetrics ---

func TestGetBrokersMetrics(t *testing.T) {
	srv := newTestServer(withState("prod", cluster.RuntimeState{
		Status:  cluster.StatusOnline,
		Brokers: []cluster.BrokerInfo{{ID: 1, PartitionsLeader: 3, Partitions: 6, InSyncPartitions: 6}},
		Disk:    []cluster.DiskUsage{{Broker: 1, SegmentSize: 1024, SegmentCount: 4}},
	}))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/brokers/1/metrics", &got)
	require.Equal(t, 200, code)
	require.Equal(t, float64(1024), got["segmentSize"])
	require.Equal(t, float64(4), got["segmentCount"])
	metrics, ok := got["metrics"].([]any)
	require.True(t, ok)
	names := make([]string, 0, len(metrics))
	for _, m := range metrics {
		mm, ok := m.(map[string]any)
		require.True(t, ok)
		names = append(names, mm["name"].(string))
	}
	require.Contains(t, names, "partitions_leader")
	require.Contains(t, names, "partitions")
	require.Contains(t, names, "in_sync_partitions")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetBrokersMetricsUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/brokers/1/metrics", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}
