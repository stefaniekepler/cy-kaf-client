package connect

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// These are fast, in-process unit tests against fakeConnect, a minimal
// hand-rolled stand-in for the Kafka Connect REST API (there is no
// maintained fake/mock library for it, unlike P2a's srfake for Schema
// Registry) -- they prove Pool's HTTP-shape wiring: request construction,
// response mapping, auth header, non-2xx error wrapping, and the P2b-D3
// aggregation skip-on-unreachable behavior. Genuine Connect REST semantics
// (real plugin validation, real connector lifecycle transitions, real
// task-vs-status field shapes) are exercised separately by
// client_integration_test.go against a live confluentinc/cp-kafka-connect
// container -- this fake's job is only to prove Pool's own plumbing, and its
// field names were themselves verified against that same live container
// while implementing client.go (see that file's wire-shape comment).

// fakeConnect is a tiny in-memory Connect worker: connectors is name ->
// config, states is name -> current lifecycle state string. Every handler
// below mirrors client.go's REST path mapping one-for-one.
type fakeConnect struct {
	t   *testing.T
	srv *httptest.Server

	mu         sync.Mutex
	connectors map[string]map[string]any
	states     map[string]string

	wantUser, wantPass string // "" wantUser = no basic auth required
	lastAuth           string
	lastRestartQuery   string
}

func newFakeConnect(t *testing.T) *fakeConnect {
	t.Helper()
	f := &fakeConnect{
		t:          t,
		connectors: map[string]map[string]any{},
		states:     map[string]string{},
	}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeConnect) url() string { return f.srv.URL }

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

//nolint:gocyclo // a single small routing table is clearer here than splitting a fake HTTP server across files
func (f *fakeConnect) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastAuth = r.Header.Get("Authorization")
	if f.wantUser != "" {
		user, pass, ok := r.BasicAuth()
		if !ok || user != f.wantUser || pass != f.wantPass {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
	}

	path := r.URL.Path
	switch {
	case path == "/connectors/"+trimmed(path, "/status") && strings.HasSuffix(path, "/status") && r.Method == http.MethodGet:
		name := strings.TrimSuffix(strings.TrimPrefix(path, "/connectors/"), "/status")
		state, ok := f.states[name]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeJSON(w, connectorStatusWire{
			Name:      name,
			Connector: connectorStateWire{State: state, WorkerID: "fake:8083"},
			Tasks:     []taskStatusWire{{ID: 0, State: state, WorkerID: "fake:8083"}},
			Type:      "source",
		})
	case strings.HasSuffix(path, "/config") && r.Method == http.MethodGet:
		name := strings.TrimSuffix(strings.TrimPrefix(path, "/connectors/"), "/config")
		cfg, ok := f.connectors[name]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeJSON(w, cfg)
	case strings.HasSuffix(path, "/config") && r.Method == http.MethodPut:
		name := strings.TrimSuffix(strings.TrimPrefix(path, "/connectors/"), "/config")
		var cfg map[string]any
		_ = json.NewDecoder(r.Body).Decode(&cfg)
		f.connectors[name] = cfg
		writeJSON(w, connectorInfoWire{Name: name, Config: cfg, Tasks: []taskIDWire{{Connector: name, Task: 0}}, Type: "source"})
	case strings.HasSuffix(path, "/tasks") && r.Method == http.MethodGet:
		name := strings.TrimSuffix(strings.TrimPrefix(path, "/connectors/"), "/tasks")
		if _, ok := f.connectors[name]; !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeJSON(w, []taskInfoWire{{ID: taskIDWire{Connector: name, Task: 0}, Config: map[string]any{"task.class": "fake.Task"}}})
	case strings.HasSuffix(path, "/pause") && r.Method == http.MethodPut:
		f.states[strings.TrimSuffix(strings.TrimPrefix(path, "/connectors/"), "/pause")] = "PAUSED"
		w.WriteHeader(http.StatusAccepted)
	case strings.HasSuffix(path, "/resume") && r.Method == http.MethodPut:
		f.states[strings.TrimSuffix(strings.TrimPrefix(path, "/connectors/"), "/resume")] = "RUNNING"
		w.WriteHeader(http.StatusAccepted)
	case strings.HasSuffix(path, "/stop") && r.Method == http.MethodPut:
		f.states[strings.TrimSuffix(strings.TrimPrefix(path, "/connectors/"), "/stop")] = "STOPPED"
		w.WriteHeader(http.StatusAccepted)
	case strings.Contains(path, "/tasks/") && strings.HasSuffix(path, "/restart") && r.Method == http.MethodPost:
		w.WriteHeader(http.StatusNoContent)
	case strings.HasSuffix(path, "/restart") && r.Method == http.MethodPost:
		f.states[strings.TrimSuffix(strings.TrimPrefix(path, "/connectors/"), "/restart")] = "RUNNING"
		f.lastRestartQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusNoContent)
	case strings.HasSuffix(path, "/offsets") && r.Method == http.MethodDelete:
		w.WriteHeader(http.StatusNoContent)
	case path == "/connector-plugins" && r.Method == http.MethodGet:
		writeJSON(w, []pluginWire{{Class: "org.example.FakeSource", Type: "source", Version: "1.0"}})
	case strings.HasSuffix(path, "/config/validate") && r.Method == http.MethodPut:
		writeJSON(w, validateResponseWire{
			Name:       "org.example.FakeSource",
			ErrorCount: 1,
			Groups:     []string{"Common"},
			Configs: []pluginConfigWire{{
				Definition: pluginConfigDefWire{
					Name: "name", Type: "STRING", Required: true, DefaultValue: nil,
					Importance: "HIGH", Documentation: "doc", Group: "Common", Width: "MEDIUM",
					DisplayName: "Name", Order: 1, Dependents: []string{},
				},
				Value: pluginConfigValueWire{
					Name: "name", Value: nil, RecommendedValues: []string{},
					Errors: []string{`Missing required configuration "name"`}, Visible: true,
				},
			}},
		})
	case path == "/connectors" && r.URL.Query().Get("expand") != "" && r.Method == http.MethodGet:
		// AllConnectors' bulk KIP-465 shape: {name: {status, info}}, same
		// per-connector fields the plain /status and plain GET
		// /connectors/{name} handlers below produce, just nested under one
		// map entry per connector instead of fetched separately.
		out := make(map[string]connectorExpandWire, len(f.connectors))
		for n, cfg := range f.connectors {
			state := f.states[n]
			out[n] = connectorExpandWire{
				Status: connectorStatusWire{
					Name:      n,
					Connector: connectorStateWire{State: state, WorkerID: "fake:8083"},
					Tasks:     []taskStatusWire{{ID: 0, State: state, WorkerID: "fake:8083"}},
					Type:      "source",
				},
				Info: connectorInfoWire{Name: n, Config: cfg, Tasks: []taskIDWire{{Connector: n, Task: 0}}, Type: "source"},
			}
		}
		writeJSON(w, out)
	case path == "/connectors" && r.Method == http.MethodGet:
		names := make([]string, 0, len(f.connectors))
		for n := range f.connectors {
			names = append(names, n)
		}
		sort.Strings(names)
		writeJSON(w, names)
	case path == "/connectors" && r.Method == http.MethodPost:
		var req createConnectorRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		f.connectors[req.Name] = req.Config
		f.states[req.Name] = "RUNNING"
		writeJSON(w, connectorInfoWire{Name: req.Name, Config: req.Config, Tasks: []taskIDWire{{Connector: req.Name, Task: 0}}, Type: "source"})
	case strings.HasPrefix(path, "/connectors/") && r.Method == http.MethodGet && !strings.Contains(strings.TrimPrefix(path, "/connectors/"), "/"):
		name := strings.TrimPrefix(path, "/connectors/")
		cfg, ok := f.connectors[name]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeJSON(w, connectorInfoWire{Name: name, Config: cfg, Tasks: []taskIDWire{{Connector: name, Task: 0}}, Type: "source"})
	case strings.HasPrefix(path, "/connectors/") && r.Method == http.MethodDelete && !strings.Contains(strings.TrimPrefix(path, "/connectors/"), "/"):
		name := strings.TrimPrefix(path, "/connectors/")
		delete(f.connectors, name)
		delete(f.states, name)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// trimmed is a routing helper: it's only ever compared against path itself
// in the first switch case's guard, so that branch is really just "does
// this path end with /status" -- spelled this way so the case's own suffix
// check (below) is what actually does the work; kept for symmetry with the
// other branches' TrimPrefix/TrimSuffix pairs.
func trimmed(path, suffix string) string {
	return strings.TrimPrefix(strings.TrimSuffix(path, suffix), "/connectors/") + suffix
}

func testDef(connectName, address string, auth *cluster.ConnectAuth) cluster.Definition {
	return cluster.Definition{
		Name:     "t",
		Connects: []cluster.ConnectSpec{{Name: connectName, Address: address, Auth: auth}},
	}
}

func TestPoolConnectorLifecycleAgainstFakeConnect(t *testing.T) {
	f := newFakeConnect(t)
	def := testDef("c", f.url(), nil)
	p := NewPool()
	ctx := context.Background()

	plugins, err := p.Plugins(ctx, def, "c")
	require.NoError(t, err)
	require.Equal(t, []cluster.ConnectorPlugin{{Class: "org.example.FakeSource"}}, plugins)

	validation, err := p.ValidatePlugin(ctx, def, "c", "org.example.FakeSource", map[string]any{})
	require.NoError(t, err)
	require.Equal(t, 1, validation.ErrorCount)
	require.Equal(t, "org.example.FakeSource", validation.Name)
	require.Equal(t, []string{"Common"}, validation.Groups)
	require.Len(t, validation.Configs, 1)
	require.Equal(t, "name", validation.Configs[0].Definition.Name)
	require.True(t, validation.Configs[0].Definition.Required)
	require.Equal(t, "", validation.Configs[0].Definition.DefaultValue) // nil default_value -> ""
	require.Equal(t, []string{`Missing required configuration "name"`}, validation.Configs[0].Value.Errors)

	created, err := p.CreateConnector(ctx, def, "c", "conn1", map[string]any{"connector.class": "org.example.FakeSource"})
	require.NoError(t, err)
	require.Equal(t, "conn1", created.Name)
	require.Equal(t, "c", created.ConnectName)
	require.Equal(t, "RUNNING", created.State)
	require.Equal(t, []int{0}, created.TaskIDs)
	require.Equal(t, "org.example.FakeSource", created.Config["connector.class"])

	names, err := p.Connectors(ctx, def, "c")
	require.NoError(t, err)
	require.Equal(t, []string{"conn1"}, names)

	refs, err := p.AllConnectors(ctx, def)
	require.NoError(t, err)
	require.Equal(t, []cluster.ConnectorRef{{
		ConnectName: "c", Name: "conn1", Type: "source", State: "RUNNING", WorkerID: "fake:8083",
		TasksCount: 1, FailedTasksCount: 0,
	}}, refs)

	got, err := p.Connector(ctx, def, "c", "conn1")
	require.NoError(t, err)
	require.Equal(t, created, got)

	cfg, err := p.ConnectorConfig(ctx, def, "c", "conn1")
	require.NoError(t, err)
	require.Equal(t, "org.example.FakeSource", cfg["connector.class"])

	tasks, err := p.ConnectorTasks(ctx, def, "c", "conn1")
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, cluster.ConnectorTask{ID: 0, State: "RUNNING", WorkerID: "fake:8083", Config: map[string]any{"task.class": "fake.Task"}}, tasks[0])

	require.NoError(t, p.UpdateConnectorState(ctx, def, "c", "conn1", "PAUSE"))
	paused, err := p.Connector(ctx, def, "c", "conn1")
	require.NoError(t, err)
	require.Equal(t, "PAUSED", paused.State)

	require.NoError(t, p.UpdateConnectorState(ctx, def, "c", "conn1", "RESUME"))
	resumed, err := p.Connector(ctx, def, "c", "conn1")
	require.NoError(t, err)
	require.Equal(t, "RUNNING", resumed.State)

	updated, err := p.SetConnectorConfig(ctx, def, "c", "conn1", map[string]any{"connector.class": "org.example.FakeSource", "topic": "t2"})
	require.NoError(t, err)
	require.Equal(t, "t2", updated.Config["topic"])

	reread, err := p.ConnectorConfig(ctx, def, "c", "conn1")
	require.NoError(t, err)
	require.Equal(t, "t2", reread["topic"])

	require.NoError(t, p.RestartConnectorTask(ctx, def, "c", "conn1", 0))
	require.NoError(t, p.ResetConnectorOffsets(ctx, def, "c", "conn1"))

	require.NoError(t, p.DeleteConnector(ctx, def, "c", "conn1"))
	names, err = p.Connectors(ctx, def, "c")
	require.NoError(t, err)
	require.Empty(t, names)
}

func TestUpdateConnectorStateMapsAllSixActionsToConnectREST(t *testing.T) {
	cases := []struct {
		action, wantMethod, wantPath, wantQuery string
	}{
		{"PAUSE", http.MethodPut, "/connectors/conn1/pause", ""},
		{"RESUME", http.MethodPut, "/connectors/conn1/resume", ""},
		{"STOP", http.MethodPut, "/connectors/conn1/stop", ""},
		{"RESTART", http.MethodPost, "/connectors/conn1/restart", ""},
		{"RESTART_ALL_TASKS", http.MethodPost, "/connectors/conn1/restart", "includeTasks=true"},
		{"RESTART_FAILED_TASKS", http.MethodPost, "/connectors/conn1/restart", "includeTasks=true&onlyFailed=true"},
	}
	for _, c := range cases {
		t.Run(c.action, func(t *testing.T) {
			var gotMethod, gotPath, gotQuery string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotMethod, gotPath, gotQuery = r.Method, r.URL.Path, r.URL.RawQuery
				w.WriteHeader(http.StatusNoContent)
			}))
			defer srv.Close()

			def := testDef("c", srv.URL, nil)
			p := NewPool()
			require.NoError(t, p.UpdateConnectorState(context.Background(), def, "c", "conn1", c.action))
			require.Equal(t, c.wantMethod, gotMethod)
			require.Equal(t, c.wantPath, gotPath)
			require.Equal(t, c.wantQuery, gotQuery)
		})
	}

	def := testDef("c", "http://unused.invalid", nil)
	p := NewPool()
	err := p.UpdateConnectorState(context.Background(), def, "c", "conn1", "BOGUS")
	require.Error(t, err)
}

func TestPoolConnectsAndAllConnectorsSkipUnreachableConnect(t *testing.T) {
	good := newFakeConnect(t)
	good.connectors["c1"] = map[string]any{"connector.class": "org.example.FakeSource"}
	good.states["c1"] = "RUNNING"

	// deadSrv is closed immediately so its URL is unreachable -- Connects
	// and AllConnectors must skip it (P2b-D3) rather than fail outright.
	deadSrv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := deadSrv.URL
	deadSrv.Close()

	def := cluster.Definition{
		Name: "t",
		Connects: []cluster.ConnectSpec{
			{Name: "good", Address: good.url()},
			{Name: "bad", Address: deadURL},
		},
	}
	p := NewPool()
	ctx := context.Background()

	cs, err := p.Connects(ctx, def)
	require.NoError(t, err)
	require.Equal(t, []cluster.ConnectCluster{{Name: "good", Address: good.url()}}, cs)

	refs, err := p.AllConnectors(ctx, def)
	require.NoError(t, err)
	require.Equal(t, []cluster.ConnectorRef{{
		ConnectName: "good", Name: "c1", Type: "source", State: "RUNNING", WorkerID: "fake:8083",
		TasksCount: 1, FailedTasksCount: 0,
	}}, refs)
}

func TestPoolUnknownConnectNameIsErrUnknownConnect(t *testing.T) {
	def := testDef("known", "http://unused.invalid", nil)
	p := NewPool()
	ctx := context.Background()

	_, err := p.Connectors(ctx, def, "missing")
	require.ErrorIs(t, err, ErrUnknownConnect)

	_, err = p.Connector(ctx, def, "missing", "any")
	require.ErrorIs(t, err, ErrUnknownConnect)

	err = p.DeleteConnector(ctx, def, "missing", "any")
	require.ErrorIs(t, err, ErrUnknownConnect)
}

func TestPoolBasicAuthHeaderAndNon2xxErrorWrapping(t *testing.T) {
	f := newFakeConnect(t)
	f.wantUser, f.wantPass = "u", "p"
	def := testDef("c", f.url(), &cluster.ConnectAuth{Username: "u", Password: "p"})
	p := NewPool()
	ctx := context.Background()

	_, err := p.Connectors(ctx, def, "c")
	require.NoError(t, err)
	wantHdr := "Basic " + base64.StdEncoding.EncodeToString([]byte("u:p"))
	require.Equal(t, wantHdr, f.lastAuth)

	// clientFor caches by (def.Name, connectName), so a different cluster
	// name is needed here -- otherwise this call would just hit the
	// already-cached (good-credentials) client for def.Name "t"/"c" and
	// never actually exercise the bad-credentials path.
	defBad := testDef("c", f.url(), &cluster.ConnectAuth{Username: "u", Password: "wrong"})
	defBad.Name = "t-bad-auth"
	_, err = p.Connectors(ctx, defBad, "c")
	require.Error(t, err)
	require.Contains(t, err.Error(), "401")
}

func TestPoolClientForCachesByClusterAndConnectName(t *testing.T) {
	f := newFakeConnect(t)
	def := testDef("c", f.url(), nil)
	p := NewPool()

	cl1, err := p.clientFor(def, "c")
	require.NoError(t, err)
	cl2, err := p.clientFor(def, "c")
	require.NoError(t, err)
	require.Same(t, cl1, cl2, "same (cluster, connect) pair must resolve to the same cached client")
}

func TestPoolClientForRotatesWhenConnectionSettingsChange(t *testing.T) {
	p := NewPool()
	base := cluster.Definition{
		Name: "local",
		Connects: []cluster.ConnectSpec{{
			Name:    "main",
			Address: "http://connect-a:8083",
		}},
	}
	first, err := p.clientFor(base, "main")
	require.NoError(t, err)

	changed := base
	changed.Connects = []cluster.ConnectSpec{{
		Name:    "main",
		Address: "http://connect-b:8083",
		Auth:    &cluster.ConnectAuth{Username: "u", Password: "p"},
	}}
	second, err := p.clientFor(changed, "main")
	require.NoError(t, err)

	require.NotSame(t, first, second, "a dynamic-config edit must not reuse the stale Connect client")
	require.Len(t, p.clients, 1, "retain only the current client for a (cluster, connect) pair")
}

func TestConnectResponseBodyExactBoundaryIsAccepted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(strings.Repeat("x", maxConnectResponseBytes)))
	}))
	t.Cleanup(server.Close)
	client := &connectClient{http: server.Client(), baseURL: server.URL}

	require.NoError(t, client.doOnce(context.Background(), http.MethodGet, "/", nil, nil))
}

func TestConnectOversizedSuccessResponseIsRejectedWithoutBodyLeak(t *testing.T) {
	const secret = "oversized-success-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", maxConnectResponseBytes) + secret))
	}))
	t.Cleanup(server.Close)
	client := &connectClient{http: server.Client(), baseURL: server.URL}

	err := client.doOnce(context.Background(), http.MethodGet, "/", nil, nil)
	require.ErrorIs(t, err, ErrConnectResponseTooLarge)
	require.NotContains(t, err.Error(), secret)
}

func TestConnectOversizedErrorResponseUsesStableTooLargeError(t *testing.T) {
	const secret = "oversized-error-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(strings.Repeat("x", maxConnectResponseBytes) + secret))
	}))
	t.Cleanup(server.Close)
	client := &connectClient{http: server.Client(), baseURL: server.URL}

	err := client.doOnce(context.Background(), http.MethodGet, "/", nil, nil)
	require.ErrorIs(t, err, ErrConnectResponseTooLarge)
	require.NotContains(t, err.Error(), secret)
	var status *statusError
	require.False(t, errors.As(err, &status))
}

func TestConnectResponseReadReturnsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &connectClient{
		http: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body: &cancelingBody{
					cancel: cancel,
				},
				Header: make(http.Header),
			}, nil
		})},
		baseURL: "http://connect.invalid",
	}

	err := client.doOnce(ctx, http.MethodGet, "/", nil, nil)
	require.ErrorIs(t, err, context.Canceled)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

type cancelingBody struct {
	cancel context.CancelFunc
}

func (body *cancelingBody) Read([]byte) (int, error) {
	body.cancel()
	return 0, context.Canceled
}

func (*cancelingBody) Close() error {
	return nil
}

var _ io.ReadCloser = (*cancelingBody)(nil)
