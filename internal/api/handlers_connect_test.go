package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/api"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// fakeConnectServicer implements api.ConnectServicer for the connect handler
// tests -- same "error map wins, else result-map presence = known, else
// ErrUnknownCluster/ErrUnknownConnect" convention (keyed by cluster name,
// then by "cluster/connectName" for the per-Connect methods) as
// handlers_schema_test.go's fakeSchemaServicer. Carries only the three
// methods Task 3 wires; Task 4/5 grow it alongside the interface.
type fakeConnectServicer struct {
	listResult map[string][]cluster.ConnectCluster
	listErr    map[string]error
	listKnown  map[string]bool

	pluginsResult map[string][]cluster.ConnectorPlugin // key: cluster/connectName
	pluginsErr    map[string]error
	pluginsKnown  map[string]bool

	validateResult map[string]cluster.PluginValidation // key: cluster/connectName/pluginName
	validateErr    map[string]error
	validateKnown  map[string]bool
	lastValidate   map[string]map[string]any

	allConnectorsResult map[string][]cluster.ConnectorRef // key: cluster
	allConnectorsErr    map[string]error

	connectorsResult map[string][]string // key: cluster/connectName
	connectorsErr    map[string]error
	connectorsKnown  map[string]bool

	connectorResult map[string]cluster.Connector // key: cluster/connectName/connectorName
	connectorErr    map[string]error
	connectorKnown  map[string]bool

	connectorConfigResult map[string]map[string]any // key: same as connectorResult
	connectorConfigErr    map[string]error
	connectorConfigKnown  map[string]bool

	connectorTasksResult map[string][]cluster.ConnectorTask // key: same as connectorResult
	connectorTasksErr    map[string]error
	connectorTasksKnown  map[string]bool

	// --- Task 5: connector write/action fixtures (key: same
	// "cluster/connectName/connectorName" composite as connectorResult
	// above; Create additionally keys on the request body's own name) ---

	createResult  map[string]cluster.Connector
	createErr     map[string]error
	createKnown   map[string]bool
	lastCreateCfg map[string]map[string]any

	deleteErr   map[string]error
	deleteKnown map[string]bool

	setConfigResult  map[string]cluster.Connector
	setConfigErr     map[string]error
	setConfigKnown   map[string]bool
	lastSetConfigCfg map[string]map[string]any

	updateStateErr   map[string]error
	updateStateKnown map[string]bool
	lastAction       map[string]string

	resetOffsetsErr   map[string]error
	resetOffsetsKnown map[string]bool

	restartTaskErr   map[string]error
	restartTaskKnown map[string]bool
	lastTaskID       map[string]int

	// calls records every Task 5 write/action method invocation (as
	// "MethodName key") -- readOnly-403 tests assert this stays empty,
	// proving readOnlyGuard rejected the request before it ever reached the
	// servicer (same intent as fakeSchemaServicer's per-method
	// lastRegisterSubject/lastDeleteSubject emptiness checks, generalized to
	// one shared log since these 6 methods don't all have a natural "last
	// arg" to inspect).
	calls []string
}

func newFakeConnectServicer() *fakeConnectServicer {
	return &fakeConnectServicer{
		listResult: map[string][]cluster.ConnectCluster{}, listErr: map[string]error{}, listKnown: map[string]bool{},
		pluginsResult: map[string][]cluster.ConnectorPlugin{}, pluginsErr: map[string]error{}, pluginsKnown: map[string]bool{},
		validateResult: map[string]cluster.PluginValidation{}, validateErr: map[string]error{}, validateKnown: map[string]bool{},
		lastValidate:        map[string]map[string]any{},
		allConnectorsResult: map[string][]cluster.ConnectorRef{}, allConnectorsErr: map[string]error{},
		connectorsResult: map[string][]string{}, connectorsErr: map[string]error{}, connectorsKnown: map[string]bool{},
		connectorResult: map[string]cluster.Connector{}, connectorErr: map[string]error{}, connectorKnown: map[string]bool{},
		connectorConfigResult: map[string]map[string]any{}, connectorConfigErr: map[string]error{}, connectorConfigKnown: map[string]bool{},
		connectorTasksResult: map[string][]cluster.ConnectorTask{}, connectorTasksErr: map[string]error{}, connectorTasksKnown: map[string]bool{},

		createResult: map[string]cluster.Connector{}, createErr: map[string]error{}, createKnown: map[string]bool{}, lastCreateCfg: map[string]map[string]any{},
		deleteErr: map[string]error{}, deleteKnown: map[string]bool{},
		setConfigResult: map[string]cluster.Connector{}, setConfigErr: map[string]error{}, setConfigKnown: map[string]bool{}, lastSetConfigCfg: map[string]map[string]any{},
		updateStateErr: map[string]error{}, updateStateKnown: map[string]bool{}, lastAction: map[string]string{},
		resetOffsetsErr: map[string]error{}, resetOffsetsKnown: map[string]bool{},
		restartTaskErr: map[string]error{}, restartTaskKnown: map[string]bool{}, lastTaskID: map[string]int{},
	}
}

func (f *fakeConnectServicer) ListConnects(_ context.Context, name string) ([]cluster.ConnectCluster, error) {
	if err, ok := f.listErr[name]; ok {
		return nil, err
	}
	if !f.listKnown[name] {
		return nil, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return f.listResult[name], nil
}

func (f *fakeConnectServicer) Plugins(_ context.Context, name, connectName string) ([]cluster.ConnectorPlugin, error) {
	k := name + "/" + connectName
	if err, ok := f.pluginsErr[k]; ok {
		return nil, err
	}
	if !f.listKnown[name] {
		return nil, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	if !f.pluginsKnown[k] {
		return nil, fmt.Errorf("%w: %q", appcluster.ErrUnknownConnect, connectName)
	}
	return f.pluginsResult[k], nil
}

func (f *fakeConnectServicer) ValidatePlugin(_ context.Context, name, connectName, pluginName string, cfg map[string]any) (cluster.PluginValidation, error) {
	k := name + "/" + connectName + "/" + pluginName
	f.lastValidate[k] = cfg
	if err, ok := f.validateErr[k]; ok {
		return cluster.PluginValidation{}, err
	}
	if !f.listKnown[name] {
		return cluster.PluginValidation{}, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	if !f.validateKnown[k] {
		return cluster.PluginValidation{}, fmt.Errorf("%w: %q", appcluster.ErrUnknownConnect, connectName)
	}
	return f.validateResult[k], nil
}

func (f *fakeConnectServicer) AllConnectors(_ context.Context, name string) ([]cluster.ConnectorRef, error) {
	if err, ok := f.allConnectorsErr[name]; ok {
		return nil, err
	}
	if !f.listKnown[name] {
		return nil, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	return f.allConnectorsResult[name], nil
}

func (f *fakeConnectServicer) Connectors(_ context.Context, name, connectName string) ([]string, error) {
	k := name + "/" + connectName
	if err, ok := f.connectorsErr[k]; ok {
		return nil, err
	}
	if !f.listKnown[name] {
		return nil, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	if !f.connectorsKnown[k] {
		return nil, fmt.Errorf("%w: %q", appcluster.ErrUnknownConnect, connectName)
	}
	return f.connectorsResult[k], nil
}

func (f *fakeConnectServicer) Connector(_ context.Context, name, connectName, connectorName string) (cluster.Connector, error) {
	k := name + "/" + connectName + "/" + connectorName
	if err, ok := f.connectorErr[k]; ok {
		return cluster.Connector{}, err
	}
	if !f.listKnown[name] {
		return cluster.Connector{}, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	if !f.connectorKnown[k] {
		return cluster.Connector{}, fmt.Errorf("%w: %q", appcluster.ErrUnknownConnect, connectName)
	}
	return f.connectorResult[k], nil
}

func (f *fakeConnectServicer) ConnectorConfig(_ context.Context, name, connectName, connectorName string) (map[string]any, error) {
	k := name + "/" + connectName + "/" + connectorName
	if err, ok := f.connectorConfigErr[k]; ok {
		return nil, err
	}
	if !f.listKnown[name] {
		return nil, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	if !f.connectorConfigKnown[k] {
		return nil, fmt.Errorf("%w: %q", appcluster.ErrUnknownConnect, connectName)
	}
	return f.connectorConfigResult[k], nil
}

func (f *fakeConnectServicer) ConnectorTasks(_ context.Context, name, connectName, connectorName string) ([]cluster.ConnectorTask, error) {
	k := name + "/" + connectName + "/" + connectorName
	if err, ok := f.connectorTasksErr[k]; ok {
		return nil, err
	}
	if !f.listKnown[name] {
		return nil, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	if !f.connectorTasksKnown[k] {
		return nil, fmt.Errorf("%w: %q", appcluster.ErrUnknownConnect, connectName)
	}
	return f.connectorTasksResult[k], nil
}

// --- Task 5: connector write/action methods ---

func (f *fakeConnectServicer) CreateConnector(_ context.Context, name, connectName, connectorName string, cfg map[string]any) (cluster.Connector, error) {
	k := name + "/" + connectName + "/" + connectorName
	f.calls = append(f.calls, "CreateConnector "+k)
	f.lastCreateCfg[k] = cfg
	if err, ok := f.createErr[k]; ok {
		return cluster.Connector{}, err
	}
	if !f.listKnown[name] {
		return cluster.Connector{}, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	if !f.createKnown[k] {
		return cluster.Connector{}, fmt.Errorf("%w: %q", appcluster.ErrUnknownConnect, connectName)
	}
	return f.createResult[k], nil
}

func (f *fakeConnectServicer) DeleteConnector(_ context.Context, name, connectName, connectorName string) error {
	k := name + "/" + connectName + "/" + connectorName
	f.calls = append(f.calls, "DeleteConnector "+k)
	if err, ok := f.deleteErr[k]; ok {
		return err
	}
	if !f.listKnown[name] {
		return fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	if !f.deleteKnown[k] {
		return fmt.Errorf("%w: %q", appcluster.ErrUnknownConnect, connectName)
	}
	return nil
}

func (f *fakeConnectServicer) SetConnectorConfig(_ context.Context, name, connectName, connectorName string, cfg map[string]any) (cluster.Connector, error) {
	k := name + "/" + connectName + "/" + connectorName
	f.calls = append(f.calls, "SetConnectorConfig "+k)
	f.lastSetConfigCfg[k] = cfg
	if err, ok := f.setConfigErr[k]; ok {
		return cluster.Connector{}, err
	}
	if !f.listKnown[name] {
		return cluster.Connector{}, fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	if !f.setConfigKnown[k] {
		return cluster.Connector{}, fmt.Errorf("%w: %q", appcluster.ErrUnknownConnect, connectName)
	}
	return f.setConfigResult[k], nil
}

func (f *fakeConnectServicer) UpdateConnectorState(_ context.Context, name, connectName, connectorName, action string) error {
	k := name + "/" + connectName + "/" + connectorName
	f.calls = append(f.calls, "UpdateConnectorState "+k)
	f.lastAction[k] = action
	if err, ok := f.updateStateErr[k]; ok {
		return err
	}
	if !f.listKnown[name] {
		return fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	if !f.updateStateKnown[k] {
		return fmt.Errorf("%w: %q", appcluster.ErrUnknownConnect, connectName)
	}
	return nil
}

func (f *fakeConnectServicer) ResetConnectorOffsets(_ context.Context, name, connectName, connectorName string) error {
	k := name + "/" + connectName + "/" + connectorName
	f.calls = append(f.calls, "ResetConnectorOffsets "+k)
	if err, ok := f.resetOffsetsErr[k]; ok {
		return err
	}
	if !f.listKnown[name] {
		return fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	if !f.resetOffsetsKnown[k] {
		return fmt.Errorf("%w: %q", appcluster.ErrUnknownConnect, connectName)
	}
	return nil
}

func (f *fakeConnectServicer) RestartConnectorTask(_ context.Context, name, connectName, connectorName string, taskID int) error {
	k := name + "/" + connectName + "/" + connectorName
	f.calls = append(f.calls, "RestartConnectorTask "+k)
	f.lastTaskID[k] = taskID
	if err, ok := f.restartTaskErr[k]; ok {
		return err
	}
	if !f.listKnown[name] {
		return fmt.Errorf("%w: %q", appcluster.ErrUnknownCluster, name)
	}
	if !f.restartTaskKnown[k] {
		return fmt.Errorf("%w: %q", appcluster.ErrUnknownConnect, connectName)
	}
	return nil
}

var _ api.ConnectServicer = (*fakeConnectServicer)(nil)

func withConnects(fs *fakeConnectServicer) testServerOption {
	return func(d *api.Deps) { d.Connects = fs }
}

// --- GetConnects ---

func TestGetConnects(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.listResult["prod"] = []cluster.ConnectCluster{{Name: "connect-1", Address: "http://c1:8083"}}
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	var got []map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/connects", &got)
	require.Equal(t, 200, code)
	require.Len(t, got, 1)
	require.Equal(t, "connect-1", got[0]["name"])
	require.Equal(t, "http://c1:8083", got[0]["address"])
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetConnectsUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/connects", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetConnectsBackendFailureIs500(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.listErr["prod"] = fmt.Errorf("connect rest boom")
	srv := newTestServer(withConnects(fs))
	defer srv.Close()
	_, code, _, body := getJSON(t, srv, "/api/clusters/prod/connects", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to list connects", "connect rest boom")
}

// --- GetConnectsCsv ---

func TestGetConnectsCsv(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.listResult["prod"] = []cluster.ConnectCluster{{Name: "connect-1", Address: "http://c1:8083"}}
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/connects/csv", nil)
	require.Equal(t, 200, code)
	require.Equal(t, "text/csv", hdr.Get("Content-Type"))
	require.Contains(t, string(body), "connect-1")
	require.Contains(t, string(body), "http://c1:8083")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetConnectsCsvUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/connects/csv", nil)
	require.Equal(t, 404, code)
	validateAgainstContract(t, req, code, hdr, body)
}

// --- GetConnectorPlugins ---

func TestGetConnectorPlugins(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.pluginsKnown["prod/connect-1"] = true
	fs.pluginsResult["prod/connect-1"] = []cluster.ConnectorPlugin{{Class: "io.confluent.connect.jdbc.JdbcSinkConnector"}}
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	var got []map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/connects/connect-1/plugins", &got)
	require.Equal(t, 200, code)
	require.Len(t, got, 1)
	require.Equal(t, "io.confluent.connect.jdbc.JdbcSinkConnector", got[0]["class"])
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetConnectorPluginsUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/connects/connect-1/plugins", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetConnectorPluginsUnknownConnectIs404(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/connects/nope/plugins", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "connect not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

// --- ValidateConnectorPluginConfig ---

func TestValidateConnectorPluginConfig(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.validateKnown["prod/connect-1/JdbcSinkConnector"] = true
	fs.validateResult["prod/connect-1/JdbcSinkConnector"] = cluster.PluginValidation{
		Name:       "JdbcSinkConnector",
		ErrorCount: 1,
		Groups:     []string{"Connection"},
		Configs: []cluster.PluginConfigEntry{{
			Definition: cluster.PluginConfigDef{Name: "connection.url", Type: "STRING", Required: true, Importance: "HIGH", Width: "LONG"},
			Value:      cluster.PluginConfigValue{Name: "connection.url", Errors: []string{"required"}, Visible: true},
		}},
	}
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	reqBody := `{"connector.class":"JdbcSinkConnector","connection.url":""}`
	req, code, hdr, body := bodyJSON(t, "PUT", srv, "/api/clusters/prod/connects/connect-1/plugins/JdbcSinkConnector/config/validate", reqBody)
	require.Equal(t, 200, code)
	var got map[string]any
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, "JdbcSinkConnector", got["name"])
	require.Equal(t, float64(1), got["errorCount"])
	configs, ok := got["configs"].([]any)
	require.True(t, ok)
	require.Len(t, configs, 1)
	validateAgainstContract(t, req, code, hdr, body)
	require.Equal(t, map[string]any{"connector.class": "JdbcSinkConnector", "connection.url": ""}, fs.lastValidate["prod/connect-1/JdbcSinkConnector"])
}

func TestValidateConnectorPluginConfigUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	_, code, _, body := bodyJSON(t, "PUT", srv, "/api/clusters/nope/connects/connect-1/plugins/JdbcSinkConnector/config/validate", `{}`)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
}

func TestValidateConnectorPluginConfigUnknownConnectIs404(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	srv := newTestServer(withConnects(fs))
	defer srv.Close()
	_, code, _, body := bodyJSON(t, "PUT", srv, "/api/clusters/prod/connects/nope/plugins/JdbcSinkConnector/config/validate", `{}`)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "connect not found", "")
}

func TestValidateConnectorPluginConfigBadBodyIs400(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	srv := newTestServer(withConnects(fs))
	defer srv.Close()
	_, code, _, _ := bodyJSON(t, "PUT", srv, "/api/clusters/prod/connects/connect-1/plugins/JdbcSinkConnector/config/validate", `not json`)
	require.Equal(t, 400, code)
}

// TestValidateConnectorPluginConfigReadOnlyIsAllowed locks in that
// validateConnectorPluginConfig -- a PUT, but a read-only-in-effect dry run
// -- is whitelisted past readOnlyGuard (P2b-D6): a read-only cluster still
// gets a 200 result, not a 403.
func TestValidateConnectorPluginConfigReadOnlyIsAllowed(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.validateKnown["prod/connect-1/JdbcSinkConnector"] = true
	fs.validateResult["prod/connect-1/JdbcSinkConnector"] = cluster.PluginValidation{Name: "JdbcSinkConnector"}
	srv := newTestServer(withConnects(fs), withReadOnly("prod"))
	defer srv.Close()

	_, code, _, _ := bodyJSON(t, "PUT", srv, "/api/clusters/prod/connects/connect-1/plugins/JdbcSinkConnector/config/validate", `{}`)
	require.Equal(t, 200, code) // whitelisted -> not 403
}

// --- GetAllConnectors ---

// TestGetAllConnectors locks in that GetAllConnectors carries real
// per-connector status/type/task-counts through from the domain ConnectorRef
// (P2b Task 4 review Fix 2) rather than the UNASSIGNED placeholder GetAllConnectors
// used before infra/connect.Pool.AllConnectors started calling Kafka
// Connect's bulk expand=status&expand=info endpoint.
func TestGetAllConnectors(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.allConnectorsResult["prod"] = []cluster.ConnectorRef{
		{ConnectName: "connect-1", Name: "jdbc-sink", Type: "sink", State: "RUNNING", WorkerID: "worker-1", TasksCount: 2, FailedTasksCount: 1},
		{ConnectName: "connect-1", Name: "s3-source", Type: "source", State: "PAUSED", WorkerID: "worker-2", TasksCount: 1},
	}
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	var got []map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/connectors", &got)
	require.Equal(t, 200, code)
	require.Len(t, got, 2)
	require.Equal(t, "connect-1", got[0]["connect"])
	require.Equal(t, "jdbc-sink", got[0]["name"])
	require.Equal(t, "SINK", got[0]["type"])
	require.Equal(t, float64(2), got[0]["tasksCount"])
	require.Equal(t, float64(1), got[0]["failedTasksCount"])
	status, ok := got[0]["status"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "RUNNING", status["state"])
	require.Equal(t, "worker-1", status["workerId"])

	status1, ok := got[1]["status"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "PAUSED", status1["state"])
	require.Equal(t, "SOURCE", got[1]["type"])
	validateAgainstContract(t, req, code, hdr, body)
}

// TestGetAllConnectorsSearchFilters locks in that the `search` query param
// (contract's GetAllConnectorsParams.Search) actually filters the result --
// e2e-p2b's "KafkaConnect search is working" scenario drives the frontend's
// top Search box, which sends `search` straight through to this endpoint and
// renders whatever comes back with no client-side re-filtering of its own
// (ListPage.tsx -> useConnectors -> api.getAllConnectors({ search }); the
// Table component's own filterPersister is for per-column filters, not this
// free-text box). Before this fix the handler discarded params entirely
// (`_ generated.GetAllConnectorsParams`), so typing into the search box was a
// no-op and every connector stayed visible -- caught live by e2e-p2b, not by
// a unit test, hence backfilling one here. Match is case-insensitive
// substring on connector name, connect name, type, or state, matching the
// frontend's "Search by Connect Name, Status or Type" placeholder (which
// undersells it -- connector name matches too, and has to, since that's what
// this scenario's "sink_postgres" search actually matches on).
func TestGetAllConnectorsSearchFilters(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.allConnectorsResult["prod"] = []cluster.ConnectorRef{
		{ConnectName: "first", Name: "sink_postgres_activities", Type: "sink", State: "RUNNING"},
		{ConnectName: "first", Name: "s3-sink", Type: "sink", State: "RUNNING"},
	}
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	var got []map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/connectors?search=sink_postgres", &got)
	require.Equal(t, 200, code)
	require.Len(t, got, 1)
	require.Equal(t, "sink_postgres_activities", got[0]["name"])
	validateAgainstContract(t, req, code, hdr, body)
}

// TestGetAllConnectorsCsvSearchFilters is TestGetAllConnectorsSearchFilters's
// sibling for the CSV export endpoint, which shares the same
// search/orderBy/sortOrder/fts params in the contract and the same
// AllConnectors data source.
func TestGetAllConnectorsCsvSearchFilters(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.allConnectorsResult["prod"] = []cluster.ConnectorRef{
		{ConnectName: "first", Name: "sink_postgres_activities", Type: "sink", State: "RUNNING"},
		{ConnectName: "first", Name: "s3-sink", Type: "sink", State: "RUNNING"},
	}
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/clusters/prod/connectors/csv?search=sink_postgres")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, 200, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), "sink_postgres_activities")
	require.NotContains(t, string(body), "s3-sink")
}

// TestGetAllConnectorsMissingStateFallsBackToUnassigned covers the defensive
// fallback for an empty ConnectorRef.State (e.g. a malformed/partial bulk
// response entry): FullConnectorInfo.status.state is a required enum with no
// "unknown" member, so an empty string is never a valid value to send over
// the wire -- it must fall back to UNASSIGNED rather than fail contract
// validation or panic.
func TestGetAllConnectorsMissingStateFallsBackToUnassigned(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.allConnectorsResult["prod"] = []cluster.ConnectorRef{{ConnectName: "connect-1", Name: "jdbc-sink"}}
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	var got []map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/connectors", &got)
	require.Equal(t, 200, code)
	require.Len(t, got, 1)
	status, ok := got[0]["status"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "UNASSIGNED", status["state"])
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetAllConnectorsUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/connectors", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetAllConnectorsBackendFailureIs500(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.allConnectorsErr["prod"] = fmt.Errorf("connect rest boom")
	srv := newTestServer(withConnects(fs))
	defer srv.Close()
	_, code, _, body := getJSON(t, srv, "/api/clusters/prod/connectors", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to list connectors", "connect rest boom")
}

// --- GetAllConnectorsCsv ---

// TestGetAllConnectorsCsv asserts the ACTUAL status/topics cell values (not
// just require.Contains against the raw body) -- P2b Task 4 review Fix 1
// locks in that csvCell's struct/slice fallback renders ConnectorStatus.State
// and a joined topics list, not Go's own struct/slice literal syntax.
func TestGetAllConnectorsCsv(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.allConnectorsResult["prod"] = []cluster.ConnectorRef{{
		ConnectName: "connect-1", Name: "jdbc-sink", Type: "sink",
		State: "RUNNING", WorkerID: "worker-1", TasksCount: 2, FailedTasksCount: 1,
	}}
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/connectors/csv", nil)
	require.Equal(t, 200, code)
	require.Equal(t, "text/csv", hdr.Get("Content-Type"))

	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	require.Len(t, lines, 2) // header + 1 data row
	cols := strings.Split(lines[0], ",")
	cells := strings.Split(lines[1], ",")
	cellFor := func(col string) string {
		for i, c := range cols {
			if c == col {
				return cells[i]
			}
		}
		t.Fatalf("column %q not found in header %v", col, cols)
		return ""
	}
	require.Equal(t, "connect-1", cellFor("connect"))
	require.Equal(t, "jdbc-sink", cellFor("name"))
	require.Equal(t, "RUNNING", cellFor("status"))
	require.NotContains(t, cellFor("status"), "{", "status cell must not leak Go struct syntax")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetAllConnectorsCsvUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/connectors/csv", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

// --- GetConnectors ---

func TestGetConnectors(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.connectorsKnown["prod/connect-1"] = true
	fs.connectorsResult["prod/connect-1"] = []string{"jdbc-sink", "s3-source"}
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	var got []string
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/connects/connect-1/connectors", &got)
	require.Equal(t, 200, code)
	require.Equal(t, []string{"jdbc-sink", "s3-source"}, got)
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetConnectorsUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/connects/connect-1/connectors", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetConnectorsUnknownConnectIs404(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/connects/nope/connectors", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "connect not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

// --- GetConnector ---

func TestGetConnector(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.connectorKnown["prod/connect-1/jdbc-sink"] = true
	fs.connectorResult["prod/connect-1/jdbc-sink"] = cluster.Connector{
		Name: "jdbc-sink", ConnectName: "connect-1", Type: "SINK", State: "RUNNING",
		WorkerID: "worker-1", Config: map[string]any{"connector.class": "JdbcSinkConnector"},
		TaskIDs: []int{0, 1}, Topics: []string{"topic-1"},
	}
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/connects/connect-1/connectors/jdbc-sink", &got)
	require.Equal(t, 200, code)
	require.Equal(t, "jdbc-sink", got["name"])
	require.Equal(t, "connect-1", got["connect"])
	require.Equal(t, "SINK", got["type"])
	status, ok := got["status"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "RUNNING", status["state"])
	require.Equal(t, "worker-1", status["workerId"])
	tasks, ok := got["tasks"].([]any)
	require.True(t, ok)
	require.Len(t, tasks, 2)
	config, ok := got["config"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "JdbcSinkConnector", config["connector.class"])
	validateAgainstContract(t, req, code, hdr, body)
}

// TestGetConnectorUpperCasesType guards against the bug where
// connectorToGenerated cast cluster.Connector.Type onto generated.ConnectorType
// verbatim: real Kafka Connect's REST API reports the "type" field lowercase
// ("source"/"sink" -- see infra/connect.Pool.toConnector, which passes
// info.Type through as-is), but the contract's ConnectorType enum is
// UPPERCASE-ONLY (SINK/SOURCE/UNKNOWN). Without upper-casing, GetConnector
// against a live cluster would emit an invalid enum value into a
// contract-required field. TestGetConnector above doesn't catch this because
// its fixture hard-codes an already-uppercase "SINK".
func TestGetConnectorUpperCasesType(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.connectorKnown["prod/connect-1/jdbc-source"] = true
	fs.connectorResult["prod/connect-1/jdbc-source"] = cluster.Connector{
		Name: "jdbc-source", ConnectName: "connect-1", Type: "source", State: "RUNNING",
		WorkerID: "worker-1", Config: map[string]any{"connector.class": "JdbcSourceConnector"},
	}
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/connects/connect-1/connectors/jdbc-source", &got)
	require.Equal(t, 200, code)
	require.Equal(t, "SOURCE", got["type"])
	validateAgainstContract(t, req, code, hdr, body)
}

// TestGetConnectorEmptyTypeFallsBackToUnknown guards the other half of the
// same fix: Connector.Type is a required, non-nullable field per the
// contract, so an empty/unrecognized domain Type must not be emitted as ""
// (an invalid enum value) -- it falls back to the contract's UNKNOWN member.
func TestGetConnectorEmptyTypeFallsBackToUnknown(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.connectorKnown["prod/connect-1/mystery"] = true
	fs.connectorResult["prod/connect-1/mystery"] = cluster.Connector{
		Name: "mystery", ConnectName: "connect-1", State: "RUNNING", WorkerID: "worker-1",
		Config: map[string]any{},
	}
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/connects/connect-1/connectors/mystery", &got)
	require.Equal(t, 200, code)
	require.Equal(t, "UNKNOWN", got["type"])
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetConnectorUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/connects/connect-1/connectors/jdbc-sink", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetConnectorUnknownConnectIs404(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/connects/nope/connectors/jdbc-sink", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "connect not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetConnectorBackendFailureIs500(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.connectorKnown["prod/connect-1/jdbc-sink"] = true
	fs.connectorErr["prod/connect-1/jdbc-sink"] = fmt.Errorf("connect rest boom")
	srv := newTestServer(withConnects(fs))
	defer srv.Close()
	_, code, _, body := getJSON(t, srv, "/api/clusters/prod/connects/connect-1/connectors/jdbc-sink", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to get connector", "connect rest boom")
}

// --- GetConnectorConfig ---

func TestGetConnectorConfig(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.connectorConfigKnown["prod/connect-1/jdbc-sink"] = true
	fs.connectorConfigResult["prod/connect-1/jdbc-sink"] = map[string]any{"connector.class": "JdbcSinkConnector"}
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	var got map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/connects/connect-1/connectors/jdbc-sink/config", &got)
	require.Equal(t, 200, code)
	require.Equal(t, "JdbcSinkConnector", got["connector.class"])
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetConnectorConfigUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/connects/connect-1/connectors/jdbc-sink/config", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetConnectorConfigUnknownConnectIs404(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/connects/nope/connectors/jdbc-sink/config", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "connect not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

// --- GetConnectorTasks ---

func TestGetConnectorTasks(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.connectorTasksKnown["prod/connect-1/jdbc-sink"] = true
	fs.connectorTasksResult["prod/connect-1/jdbc-sink"] = []cluster.ConnectorTask{
		{ID: 0, State: "RUNNING", WorkerID: "worker-1"},
		{ID: 1, State: "FAILED", WorkerID: "worker-2", Trace: "boom"},
	}
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	var got []map[string]any
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/connects/connect-1/connectors/jdbc-sink/tasks", &got)
	require.Equal(t, 200, code)
	require.Len(t, got, 2)
	status0, ok := got[0]["status"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "RUNNING", status0["state"])
	require.Equal(t, "worker-1", status0["workerId"])
	status1, ok := got[1]["status"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "FAILED", status1["state"])
	require.Equal(t, "boom", status1["trace"])
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetConnectorTasksUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := getJSON(t, srv, "/api/clusters/nope/connects/connect-1/connectors/jdbc-sink/tasks", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestGetConnectorTasksUnknownConnectIs404(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	req, code, hdr, body := getJSON(t, srv, "/api/clusters/prod/connects/nope/connectors/jdbc-sink/tasks", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "connect not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

// --- CreateConnector ---

func TestCreateConnector(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.createKnown["prod/connect-1/jdbc-sink"] = true
	fs.createResult["prod/connect-1/jdbc-sink"] = cluster.Connector{
		Name: "jdbc-sink", ConnectName: "connect-1", Type: "SINK", State: "RUNNING",
		WorkerID: "worker-1", Config: map[string]any{"connector.class": "JdbcSinkConnector"},
	}
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	reqBody := `{"name":"jdbc-sink","config":{"connector.class":"JdbcSinkConnector"}}`
	req, code, hdr, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/connects/connect-1/connectors", reqBody)
	require.Equal(t, 200, code)
	var got map[string]any
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, "jdbc-sink", got["name"])
	require.Equal(t, "connect-1", got["connect"])
	validateAgainstContract(t, req, code, hdr, body)
	require.Equal(t, map[string]any{"connector.class": "JdbcSinkConnector"}, fs.lastCreateCfg["prod/connect-1/jdbc-sink"])
}

func TestCreateConnectorBadBodyIs400(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	// No validateAgainstContract: kin-openapi's own request validation would
	// reject this malformed body itself (same reason
	// TestCreateNewSchemaBadBodyIs400/TestValidateConnectorPluginConfigBadBodyIs400
	// skip it).
	_, code, _, _ := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/connects/connect-1/connectors", `not json`)
	require.Equal(t, 400, code)
	require.Empty(t, fs.calls)
}

func TestCreateConnectorReadOnlyIs403(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.createKnown["prod/connect-1/jdbc-sink"] = true
	srv := newTestServer(withConnects(fs), withReadOnly("prod"))
	defer srv.Close()

	reqBody := `{"name":"jdbc-sink","config":{}}`
	_, code, _, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/connects/connect-1/connectors", reqBody)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "read-only", "")
	require.Empty(t, fs.calls)
}

func TestCreateConnectorUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/nope/connects/connect-1/connectors", `{"name":"jdbc-sink","config":{}}`)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestCreateConnectorUnknownConnectIs404(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	req, code, hdr, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/connects/nope/connectors", `{"name":"jdbc-sink","config":{}}`)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "connect not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestCreateConnectorBackendFailureIs500(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.createErr["prod/connect-1/jdbc-sink"] = fmt.Errorf("connect rest boom")
	srv := newTestServer(withConnects(fs))
	defer srv.Close()
	_, code, _, body := bodyJSON(t, http.MethodPost, srv, "/api/clusters/prod/connects/connect-1/connectors", `{"name":"jdbc-sink","config":{}}`)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to create connector", "connect rest boom")
}

// --- DeleteConnector ---

func TestDeleteConnector(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.deleteKnown["prod/connect-1/jdbc-sink"] = true
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	req, code, hdr, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/connects/connect-1/connectors/jdbc-sink", nil)
	require.Equal(t, 204, code)
	require.Empty(t, body)
	validateAgainstContract(t, req, code, hdr, body)
}

func TestDeleteConnectorReadOnlyIs403(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.deleteKnown["prod/connect-1/jdbc-sink"] = true
	srv := newTestServer(withConnects(fs), withReadOnly("prod"))
	defer srv.Close()

	_, code, _, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/connects/connect-1/connectors/jdbc-sink", nil)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "read-only", "")
	require.Empty(t, fs.calls)
}

func TestDeleteConnectorUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/nope/connects/connect-1/connectors/jdbc-sink", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestDeleteConnectorUnknownConnectIs404(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	req, code, hdr, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/connects/nope/connectors/jdbc-sink", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "connect not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestDeleteConnectorBackendFailureIs500(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.deleteKnown["prod/connect-1/jdbc-sink"] = true
	fs.deleteErr["prod/connect-1/jdbc-sink"] = fmt.Errorf("connect rest boom")
	srv := newTestServer(withConnects(fs))
	defer srv.Close()
	_, code, _, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/connects/connect-1/connectors/jdbc-sink", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to delete connector", "connect rest boom")
}

// --- SetConnectorConfig ---

func TestSetConnectorConfig(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.setConfigKnown["prod/connect-1/jdbc-sink"] = true
	fs.setConfigResult["prod/connect-1/jdbc-sink"] = cluster.Connector{
		Name: "jdbc-sink", ConnectName: "connect-1", Type: "SINK", State: "RUNNING",
		WorkerID: "worker-1", Config: map[string]any{"connector.class": "JdbcSinkConnector"},
	}
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	reqBody := `{"connector.class":"JdbcSinkConnector"}`
	req, code, hdr, body := bodyJSON(t, http.MethodPut, srv, "/api/clusters/prod/connects/connect-1/connectors/jdbc-sink/config", reqBody)
	require.Equal(t, 200, code)
	var got map[string]any
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, "jdbc-sink", got["name"])
	validateAgainstContract(t, req, code, hdr, body)
	require.Equal(t, map[string]any{"connector.class": "JdbcSinkConnector"}, fs.lastSetConfigCfg["prod/connect-1/jdbc-sink"])
}

func TestSetConnectorConfigBadBodyIs400(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	_, code, _, _ := bodyJSON(t, http.MethodPut, srv, "/api/clusters/prod/connects/connect-1/connectors/jdbc-sink/config", `not json`)
	require.Equal(t, 400, code)
	require.Empty(t, fs.calls)
}

func TestSetConnectorConfigReadOnlyIs403(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.setConfigKnown["prod/connect-1/jdbc-sink"] = true
	srv := newTestServer(withConnects(fs), withReadOnly("prod"))
	defer srv.Close()

	_, code, _, body := bodyJSON(t, http.MethodPut, srv, "/api/clusters/prod/connects/connect-1/connectors/jdbc-sink/config", `{}`)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "read-only", "")
	require.Empty(t, fs.calls)
}

func TestSetConnectorConfigUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := bodyJSON(t, http.MethodPut, srv, "/api/clusters/nope/connects/connect-1/connectors/jdbc-sink/config", `{}`)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestSetConnectorConfigUnknownConnectIs404(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	req, code, hdr, body := bodyJSON(t, http.MethodPut, srv, "/api/clusters/prod/connects/nope/connectors/jdbc-sink/config", `{}`)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "connect not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestSetConnectorConfigBackendFailureIs500(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.setConfigErr["prod/connect-1/jdbc-sink"] = fmt.Errorf("connect rest boom")
	srv := newTestServer(withConnects(fs))
	defer srv.Close()
	_, code, _, body := bodyJSON(t, http.MethodPut, srv, "/api/clusters/prod/connects/connect-1/connectors/jdbc-sink/config", `{}`)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to set connector config", "connect rest boom")
}

// --- UpdateConnectorState ---

func TestUpdateConnectorState(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.updateStateKnown["prod/connect-1/jdbc-sink"] = true
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	req, code, hdr, body := doJSON(t, http.MethodPost, srv, "/api/clusters/prod/connects/connect-1/connectors/jdbc-sink/action/PAUSE", nil)
	require.Equal(t, 204, code)
	require.Empty(t, body)
	validateAgainstContract(t, req, code, hdr, body)
	require.Equal(t, "PAUSE", fs.lastAction["prod/connect-1/jdbc-sink"])
}

// TestUpdateConnectorStateEveryAction locks in that all six of the
// contract's ConnectorAction enum values (P2b-D4) round-trip through
// string(action) unmodified.
func TestUpdateConnectorStateEveryAction(t *testing.T) {
	for _, action := range []string{"RESTART", "RESTART_ALL_TASKS", "RESTART_FAILED_TASKS", "PAUSE", "RESUME", "STOP"} {
		t.Run(action, func(t *testing.T) {
			fs := newFakeConnectServicer()
			fs.listKnown["prod"] = true
			fs.updateStateKnown["prod/connect-1/jdbc-sink"] = true
			srv := newTestServer(withConnects(fs))
			defer srv.Close()

			req, code, hdr, body := doJSON(t, http.MethodPost, srv, "/api/clusters/prod/connects/connect-1/connectors/jdbc-sink/action/"+action, nil)
			require.Equal(t, 204, code)
			validateAgainstContract(t, req, code, hdr, body)
			require.Equal(t, action, fs.lastAction["prod/connect-1/jdbc-sink"])
		})
	}
}

func TestUpdateConnectorStateReadOnlyIs403(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.updateStateKnown["prod/connect-1/jdbc-sink"] = true
	srv := newTestServer(withConnects(fs), withReadOnly("prod"))
	defer srv.Close()

	_, code, _, body := doJSON(t, http.MethodPost, srv, "/api/clusters/prod/connects/connect-1/connectors/jdbc-sink/action/PAUSE", nil)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "read-only", "")
	require.Empty(t, fs.calls)
}

func TestUpdateConnectorStateUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := doJSON(t, http.MethodPost, srv, "/api/clusters/nope/connects/connect-1/connectors/jdbc-sink/action/PAUSE", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestUpdateConnectorStateUnknownConnectIs404(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	req, code, hdr, body := doJSON(t, http.MethodPost, srv, "/api/clusters/prod/connects/nope/connectors/jdbc-sink/action/PAUSE", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "connect not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestUpdateConnectorStateBackendFailureIs500(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.updateStateErr["prod/connect-1/jdbc-sink"] = fmt.Errorf("connect rest boom")
	srv := newTestServer(withConnects(fs))
	defer srv.Close()
	_, code, _, body := doJSON(t, http.MethodPost, srv, "/api/clusters/prod/connects/connect-1/connectors/jdbc-sink/action/PAUSE", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to update connector state", "connect rest boom")
}

// --- ResetConnectorOffsets ---

func TestResetConnectorOffsets(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.resetOffsetsKnown["prod/connect-1/jdbc-sink"] = true
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	req, code, hdr, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/connects/connect-1/connectors/jdbc-sink/offsets", nil)
	require.Equal(t, 204, code)
	require.Empty(t, body)
	validateAgainstContract(t, req, code, hdr, body)
}

func TestResetConnectorOffsetsReadOnlyIs403(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.resetOffsetsKnown["prod/connect-1/jdbc-sink"] = true
	srv := newTestServer(withConnects(fs), withReadOnly("prod"))
	defer srv.Close()

	_, code, _, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/connects/connect-1/connectors/jdbc-sink/offsets", nil)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "read-only", "")
	require.Empty(t, fs.calls)
}

func TestResetConnectorOffsetsUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/nope/connects/connect-1/connectors/jdbc-sink/offsets", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestResetConnectorOffsetsUnknownConnectIs404(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	req, code, hdr, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/connects/nope/connectors/jdbc-sink/offsets", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "connect not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestResetConnectorOffsetsBackendFailureIs500(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.resetOffsetsErr["prod/connect-1/jdbc-sink"] = fmt.Errorf("connect rest boom")
	srv := newTestServer(withConnects(fs))
	defer srv.Close()
	_, code, _, body := doJSON(t, http.MethodDelete, srv, "/api/clusters/prod/connects/connect-1/connectors/jdbc-sink/offsets", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to reset connector offsets", "connect rest boom")
}

// --- RestartConnectorTask ---

func TestRestartConnectorTask(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.restartTaskKnown["prod/connect-1/jdbc-sink"] = true
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	req, code, hdr, body := doJSON(t, http.MethodPost, srv, "/api/clusters/prod/connects/connect-1/connectors/jdbc-sink/tasks/2/action/restart", nil)
	require.Equal(t, 204, code)
	require.Empty(t, body)
	validateAgainstContract(t, req, code, hdr, body)
	require.Equal(t, 2, fs.lastTaskID["prod/connect-1/jdbc-sink"])
}

func TestRestartConnectorTaskReadOnlyIs403(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.restartTaskKnown["prod/connect-1/jdbc-sink"] = true
	srv := newTestServer(withConnects(fs), withReadOnly("prod"))
	defer srv.Close()

	_, code, _, body := doJSON(t, http.MethodPost, srv, "/api/clusters/prod/connects/connect-1/connectors/jdbc-sink/tasks/2/action/restart", nil)
	require.Equal(t, 403, code)
	assertErrorEnvelope(t, body, "read-only", "")
	require.Empty(t, fs.calls)
}

func TestRestartConnectorTaskUnknownClusterIs404(t *testing.T) {
	srv := newTestServer()
	defer srv.Close()
	req, code, hdr, body := doJSON(t, http.MethodPost, srv, "/api/clusters/nope/connects/connect-1/connectors/jdbc-sink/tasks/0/action/restart", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "cluster not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestRestartConnectorTaskUnknownConnectIs404(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	srv := newTestServer(withConnects(fs))
	defer srv.Close()

	req, code, hdr, body := doJSON(t, http.MethodPost, srv, "/api/clusters/prod/connects/nope/connectors/jdbc-sink/tasks/0/action/restart", nil)
	require.Equal(t, 404, code)
	assertErrorEnvelope(t, body, "connect not found", "")
	validateAgainstContract(t, req, code, hdr, body)
}

func TestRestartConnectorTaskBackendFailureIs500(t *testing.T) {
	fs := newFakeConnectServicer()
	fs.listKnown["prod"] = true
	fs.restartTaskErr["prod/connect-1/jdbc-sink"] = fmt.Errorf("connect rest boom")
	srv := newTestServer(withConnects(fs))
	defer srv.Close()
	_, code, _, body := doJSON(t, http.MethodPost, srv, "/api/clusters/prod/connects/connect-1/connectors/jdbc-sink/tasks/0/action/restart", nil)
	require.Equal(t, 500, code)
	assertErrorEnvelope(t, body, "failed to restart connector task", "connect rest boom")
}
