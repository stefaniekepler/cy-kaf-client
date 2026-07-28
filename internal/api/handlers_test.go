package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/api"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/version"
)

// fakeStater implements api.ClusterStater for tests: List serves a fixed
// snapshot list (P0-era behaviour, unchanged semantics), Get/Refresh serve
// from a name-keyed RuntimeState map, and report "unknown cluster" like the
// real app.StateCache does when a name isn't in the configured definitions.
type fakeStater struct {
	snaps      []cluster.Snapshot
	states     map[string]cluster.RuntimeState
	refreshErr map[string]error // per-cluster errors for Refresh (cluster known but backend fails)
}

func (f fakeStater) List(context.Context) []cluster.Snapshot { return f.snaps }

func (f fakeStater) Get(_ context.Context, name string) (cluster.RuntimeState, bool) {
	st, ok := f.states[name]
	return st, ok
}

func (f fakeStater) Refresh(_ context.Context, name string) (cluster.RuntimeState, error) {
	if err, ok := f.refreshErr[name]; ok {
		return cluster.RuntimeState{}, err
	}
	if st, ok := f.states[name]; ok {
		return st, nil
	}
	return cluster.RuntimeState{}, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
}

// testServerOption customizes the api.Deps newTestServer builds before
// constructing the server. This is the injection mechanism for per-test fakes
// (ClusterStater/LogDirser); it's kept orthogonal to test assertions so
// existing call sites only need their construction adapted, never their
// expectations (see withSnaps below, and withState/withLogDirs in
// handlers_broker_test.go).
type testServerOption func(*api.Deps)

func newTestServer(opts ...testServerOption) *httptest.Server {
	// Shared zero-value-safe fake, wired into States/LogDirs/Brokers alike
	// (mirrors production's shape, where *appcluster.StateCache and
	// *appcluster.BrokerService — both resolving cluster names via the same
	// *appcluster.Resolver — together satisfy all three Deps interfaces):
	// every lookup misses cleanly rather than panicking on a nil interface.
	defaultBrokers := newFakeBrokerStater()
	deps := api.Deps{
		States:       fakeStater{},
		LogDirs:      defaultBrokers,
		Brokers:      defaultBrokers,
		Topics:       newFakeTopicServicer(),             // 每次查找默认未知集群，见 withTopics 覆盖
		Groups:       newFakeGroupServicer(),             // 同上，见 withGroups 覆盖（P1b Task 6）
		Serdes:       newFakeSerdesServicer(),            // 同上，见 withSerdes 覆盖（P1c Task 3）
		Schemas:      newFakeSchemaServicer(),            // 同上，见 withSchemas 覆盖（P2a Task 3）
		Connects:     newFakeConnectServicer(),           // 同上，见 withConnects 覆盖（P2b Task 3）
		Acls:         &fakeAclServicer{},                 // 同上，见 withAcls 覆盖（P2c Task 6）
		Quotas:       &fakeQuotaServicer{},               // 同上，见 withQuotas 覆盖（P2c Task 7）
		SmartFilters: newFakeSmartFilterServicer(),       // 同上，见 withSmartFilters 覆盖（P1c Task 12）
		IsReadOnly:   func(string) bool { return false }, // 默认恒 false：见 withReadOnly 覆盖
		Build:        version.BuildInfo{Version: "0.1.0-test", Commit: "abc", BuildTime: "t"},
		Static:       fstest.MapFS{"index.html": {Data: []byte("<html>PUBLIC-PATH-VARIABLE</html>")}},
	}
	for _, opt := range opts {
		opt(&deps)
	}
	return httptest.NewServer(api.NewServer(deps))
}

// withReadOnly marks name as a read-only cluster for the readOnlyGuard
// middleware (Deps.IsReadOnly) — used by write-endpoint tests asserting the
// 403 read-only guard (see e.g. topics/groups write handlers).
func withReadOnly(name string) testServerOption {
	return func(d *api.Deps) { d.IsReadOnly = func(n string) bool { return n == name } }
}

// withSnaps replaces the default empty ClusterStater with one serving a fixed
// /api/clusters list — this is P0-era newTestServer(snaps)'s behaviour, now
// an explicit option instead of the sole positional argument.
func withSnaps(snaps []cluster.Snapshot) testServerOption {
	return func(d *api.Deps) { d.States = fakeStater{snaps: snaps} }
}

// withRefreshError registers a cluster as known-but-failing for Refresh
// (backend-failure 500 path).
func withRefreshError(name string, err error) testServerOption {
	return func(d *api.Deps) {
		fs := d.States.(fakeStater)
		if fs.refreshErr == nil {
			fs.refreshErr = map[string]error{}
		}
		if fs.states == nil {
			fs.states = map[string]cluster.RuntimeState{}
		}
		fs.refreshErr[name] = err
		d.States = fs
	}
}

// newStatesTestServer is like newTestServer but wires per-cluster RuntimeState
// fixtures for the Get/Refresh-backed endpoints (stats/metrics/cache).
func newStatesTestServer(states map[string]cluster.RuntimeState) *httptest.Server {
	h := api.NewServer(api.Deps{
		States:     fakeStater{states: states, refreshErr: map[string]error{}},
		IsReadOnly: func(string) bool { return false },
		Build:      version.BuildInfo{Version: "0.1.0-test", Commit: "abc", BuildTime: "t"},
		Static:     fstest.MapFS{"index.html": {Data: []byte("<html>PUBLIC-PATH-VARIABLE</html>")}},
	})
	return httptest.NewServer(h)
}

func doJSON(t *testing.T, method string, srv *httptest.Server, path string, out any) (*http.Request, int, http.Header, []byte) {
	t.Helper()
	req, _ := http.NewRequest(method, srv.URL+path, nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if out != nil {
		require.NoError(t, json.Unmarshal(body, out))
	}
	return req, resp.StatusCode, resp.Header, body
}

func getJSON(t *testing.T, srv *httptest.Server, path string, out any) (*http.Request, int, http.Header, []byte) {
	return doJSON(t, http.MethodGet, srv, path, out)
}

func TestAuthCompatEndpoints(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()

	var auth struct {
		AuthType string `json:"authType"`
	}
	req, code, hdr, body := getJSON(t, srv, "/api/config/authentication", &auth)
	require.Equal(t, 200, code)
	require.Equal(t, "DISABLED", auth.AuthType)
	validateAgainstContract(t, req, code, hdr, body)

	var authz struct {
		RbacEnabled bool `json:"rbacEnabled"`
	}
	req, code, hdr, body = getJSON(t, srv, "/api/authorization", &authz)
	require.Equal(t, 200, code)
	require.False(t, authz.RbacEnabled)
	validateAgainstContract(t, req, code, hdr, body)

	var info struct {
		EnabledFeatures []string `json:"enabledFeatures"`
		Build           struct {
			Version         string `json:"version"`
			IsLatestRelease bool   `json:"isLatestRelease"`
		} `json:"build"`
	}
	req, code, hdr, body = getJSON(t, srv, "/api/info", &info)
	require.Equal(t, 200, code)
	require.Equal(t, []string{"DYNAMIC_CONFIG"}, info.EnabledFeatures)
	require.Equal(t, "0.1.0-test", info.Build.Version)
	require.True(t, info.Build.IsLatestRelease) // D11: 版本检查关闭 → 恒为最新
	validateAgainstContract(t, req, code, hdr, body)

	_, code, _, body = getJSON(t, srv, "/actuator/health", nil)
	require.Equal(t, 200, code)
	require.JSONEq(t, `{"status":"UP"}`, string(body))
}

func TestGetClusters(t *testing.T) {
	srv := newTestServer(withSnaps([]cluster.Snapshot{{
		Definition: cluster.Definition{Name: "prod", ReadOnly: true,
			SchemaRegistry: cluster.SchemaRegistrySpec{URL: "http://sr"}, Connects: []cluster.ConnectSpec{{Name: "main"}}, KsqlURL: "http://ksql"},
		Status: cluster.StatusOnline, BrokerCount: 3,
		Features: []cluster.Feature{
			cluster.FeatureSchemaRegistry,
			cluster.FeatureKafkaConnect,
			cluster.FeatureKsqlDB,
			cluster.FeatureKafkaACLView,
		},
	}}))
	defer srv.Close()

	var got []map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters", &got)
	require.Equal(t, 200, code)
	require.Len(t, got, 1)
	require.Equal(t, "prod", got[0]["name"])
	require.Equal(t, "ONLINE", got[0]["status"])
	require.Equal(t, float64(3), got[0]["brokerCount"])
	require.Equal(t, true, got[0]["readOnly"])
	// Configured ecosystem features and read-only ACL visibility survive the
	// domain Feature -> contract ClusterFeatures mapping.
	require.Contains(t, got[0]["features"], "SCHEMA_REGISTRY")
	require.Contains(t, got[0]["features"], "KAFKA_CONNECT")
	require.Contains(t, got[0]["features"], "KSQL_DB")
	require.Contains(t, got[0]["features"], "KAFKA_ACL_VIEW")
	require.NotContains(t, got[0]["features"], "KAFKA_ACL_EDIT")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetClustersOffline(t *testing.T) {
	srv := newTestServer(withSnaps([]cluster.Snapshot{{
		Definition: cluster.Definition{Name: "dev"},
		Status:     cluster.StatusOffline,
	}}))
	defer srv.Close()

	var got []map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters", &got)
	require.Equal(t, 200, code)
	require.Len(t, got, 1)
	require.Equal(t, "OFFLINE", got[0]["status"])
	require.NotContains(t, got[0], "brokerCount") // offline → 不报 broker 数
	validateAgainstContract(t, req, code, hdr, body)
}

func TestUnimplementedEndpointReturns501(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	// KSQL 四端点已实现；Graphs 是当前批准范围外的明确 exempt surface，
	// 因此继续用它锁定 generated 501 桩边界。
	resp, err := http.Get(srv.URL + "/api/clusters/local/graphs")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusNotImplemented, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	require.Contains(t, string(body), "not implemented")
}

func sampleRuntimeState() cluster.RuntimeState {
	return cluster.RuntimeState{
		Definition: cluster.Definition{Name: "prod", ReadOnly: true},
		Status:     cluster.StatusOnline,
		Brokers:    []cluster.BrokerInfo{{ID: 1}, {ID: 2}},
		Controller: 1,
		TopicCount: 5,
		Partitions: cluster.PartitionCounts{Online: 10, Offline: 1, UnderReplicated: 2, InSync: 20, OutOfSync: 3},
		Disk: []cluster.DiskUsage{
			{Broker: 1, SegmentSize: 1024, SegmentCount: 4},
			{Broker: 2, SegmentSize: 2048, SegmentCount: 8},
		},
		Version: "3.8.0",
	}
}

func TestGetClusterStats(t *testing.T) {
	srv := newStatesTestServer(map[string]cluster.RuntimeState{"prod": sampleRuntimeState()})
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/stats", &got)
	require.Equal(t, 200, code)
	require.Equal(t, float64(2), got["brokerCount"])
	require.Equal(t, float64(1), got["activeControllers"]) // st.Controller, 契约字段语义是控制器 broker ID
	require.Equal(t, float64(10), got["onlinePartitionCount"])
	require.Equal(t, float64(1), got["offlinePartitionCount"])
	require.Equal(t, float64(2), got["underReplicatedPartitionCount"])
	require.Equal(t, float64(20), got["inSyncReplicasCount"])
	require.Equal(t, float64(3), got["outOfSyncReplicasCount"])
	require.Equal(t, "3.8.0", got["version"])
	diskUsage, ok := got["diskUsage"].([]any)
	require.True(t, ok)
	require.Len(t, diskUsage, 2)
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetClusterStatsUnknownClusterIs404(t *testing.T) {
	srv := newStatesTestServer(nil)
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/stats", nil)
	require.Equal(t, 404, code)
	validateAgainstContract(t, req, code, hdr, body)
}

func TestUpdateClusterInfo(t *testing.T) {
	srv := newStatesTestServer(map[string]cluster.RuntimeState{"prod": sampleRuntimeState()})
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := doJSON(t, http.MethodPost, srv, "/api/clusters/prod/cache", &got)
	require.Equal(t, 200, code)
	require.Equal(t, "prod", got["name"])
	require.Equal(t, "ONLINE", got["status"])
	require.Equal(t, float64(2), got["brokerCount"])
	validateAgainstContract(t, req, code, hdr, body)
}

func TestUpdateClusterInfoUnknownClusterIs404(t *testing.T) {
	srv := newStatesTestServer(nil)
	defer srv.Close()
	req, code, hdr, body := doJSON(t, http.MethodPost, srv, "/api/clusters/nope/cache", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestUpdateClusterInfoBackendFailureIs500(t *testing.T) {
	srv := newTestServer(withRefreshError("prod", fmt.Errorf("kadm boom")))
	defer srv.Close()
	_, code, _, body := doJSON(t, http.MethodPost, srv, "/api/clusters/prod/cache", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to refresh cluster", "kadm boom")
}

func TestGetClusterMetrics(t *testing.T) {
	srv := newStatesTestServer(map[string]cluster.RuntimeState{"prod": sampleRuntimeState()})
	defer srv.Close()

	var got struct {
		Items []map[string]any `json:"items"`
	}
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/metrics", &got)
	require.Equal(t, 200, code)
	require.NotEmpty(t, got.Items)
	names := make([]string, 0, len(got.Items))
	for _, it := range got.Items {
		names = append(names, it["name"].(string))
	}
	require.Contains(t, names, "broker_count")
	require.Contains(t, names, "topic_count")
	require.Contains(t, names, "kafka_topic_partitions")
	require.Contains(t, names, "broker_bytes_disk")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetClusterMetricsUnknownClusterIs404(t *testing.T) {
	srv := newStatesTestServer(nil)
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/metrics", nil)
	require.Equal(t, 404, code)
	validateAgainstContract(t, req, code, hdr, body)
}

// TestReadOnlyGuardEndToEnd locks in the full wiring this task adds — Deps.
// IsReadOnly through readOnlyGuard into the real chi router built by
// NewServer — beyond middleware_test.go's isolated TestReadOnlyGuard unit
// test: a write to a real, already-implemented write endpoint (broker config)
// on a read-only cluster is rejected with the 403 read-only envelope before
// ever reaching the port (calls stays empty), while the whitelisted
// POST /cache still gets through for that same read-only cluster — the
// whitelist shape T5/T6's topics/groups write-endpoint 403 tests will build
// on. This is also withReadOnly's only caller today (topics/groups don't
// exist yet); it stays exercised rather than dead code until T5/T6 grow their
// own.
func TestReadOnlyGuardEndToEnd(t *testing.T) {
	calls := map[string]alterConfigCall{}
	srv := newTestServer(
		withReadOnly("prod"),
		withState("prod", onlineStateOneBroker()),
		withAlterableCluster("prod", calls),
	)
	defer srv.Close()

	_, code, _, body := bodyJSON(t, http.MethodPut, srv, "/api/clusters/prod/brokers/1/configs/log.retention.ms", `{"value":"2"}`)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "read-only", "")
	require.Equal(t, alterConfigCall{}, calls["prod"]) // 守卫拦在 handler 之前：端口从未被调用

	// 白名单：POST /cache 即便对只读集群也放行（强制刷新缓存，不是"改状态"写）
	_, code, _, _ = doJSON(t, http.MethodPost, srv, "/api/clusters/prod/cache", nil)
	require.Equal(t, 200, code)
}
