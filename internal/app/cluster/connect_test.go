package cluster_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// fakeConnectPort implements cluster.KafkaConnectPort for ConnectService
// tests -- canned per-(cluster,connect[,plugin]) results, same
// map-keyed-by-composite-string shape as fakeSchemaPort. Task 3 only drives
// Connects/Plugins/ValidatePlugin; the remaining 11 methods (connector CRUD
// + actions) are stubbed to satisfy the interface and will grow real
// fixtures alongside Task 4/5's tests.
type fakeConnectPort struct {
	connects    map[string][]cluster.ConnectCluster // key: def.Name
	connectsErr map[string]error

	plugins    map[string][]cluster.ConnectorPlugin // key: connectKey(def.Name, connectName)
	pluginsErr map[string]error

	validate    map[string]cluster.PluginValidation // key: connectKey(def.Name, connectName) + "/" + pluginName
	validateErr map[string]error
	lastCfg     map[string]map[string]any // key: same as validate

	allConnectors    map[string][]cluster.ConnectorRef // key: def.Name
	allConnectorsErr map[string]error

	connectors    map[string][]string // key: connectKey(def.Name, connectName)
	connectorsErr map[string]error

	connector    map[string]cluster.Connector // key: connectKey(def.Name, connectName) + "/" + name
	connectorErr map[string]error

	connectorConfig    map[string]map[string]any // key: same as connector
	connectorConfigErr map[string]error

	connectorTasks    map[string][]cluster.ConnectorTask // key: same as connector
	connectorTasksErr map[string]error

	// --- Task 5: connector write/action fixtures (key: connectKey(def.Name,
	// connectName) + "/" + connectorName, same composite-key shape as
	// connector/connectorConfig/connectorTasks above) ---

	createConnector    map[string]cluster.Connector
	createConnectorErr map[string]error
	lastCreateCfg      map[string]map[string]any

	deleteConnectorErr map[string]error

	setConnectorConfig    map[string]cluster.Connector
	setConnectorConfigErr map[string]error
	lastSetConfigCfg      map[string]map[string]any

	updateConnectorStateErr map[string]error
	lastAction              map[string]string

	resetConnectorOffsetsErr map[string]error

	restartConnectorTaskErr map[string]error
	lastTaskID              map[string]int
}

func newFakeConnectPort() *fakeConnectPort {
	return &fakeConnectPort{
		connects: map[string][]cluster.ConnectCluster{}, connectsErr: map[string]error{},
		plugins: map[string][]cluster.ConnectorPlugin{}, pluginsErr: map[string]error{},
		validate: map[string]cluster.PluginValidation{}, validateErr: map[string]error{}, lastCfg: map[string]map[string]any{},
		allConnectors: map[string][]cluster.ConnectorRef{}, allConnectorsErr: map[string]error{},
		connectors: map[string][]string{}, connectorsErr: map[string]error{},
		connector: map[string]cluster.Connector{}, connectorErr: map[string]error{},
		connectorConfig: map[string]map[string]any{}, connectorConfigErr: map[string]error{},
		connectorTasks: map[string][]cluster.ConnectorTask{}, connectorTasksErr: map[string]error{},

		createConnector: map[string]cluster.Connector{}, createConnectorErr: map[string]error{}, lastCreateCfg: map[string]map[string]any{},
		deleteConnectorErr: map[string]error{},
		setConnectorConfig: map[string]cluster.Connector{}, setConnectorConfigErr: map[string]error{}, lastSetConfigCfg: map[string]map[string]any{},
		updateConnectorStateErr: map[string]error{}, lastAction: map[string]string{},
		resetConnectorOffsetsErr: map[string]error{},
		restartConnectorTaskErr:  map[string]error{}, lastTaskID: map[string]int{},
	}
}

func connectKey(clusterName, connectName string) string { return clusterName + "/" + connectName }

func (f *fakeConnectPort) Connects(_ context.Context, def cluster.Definition) ([]cluster.ConnectCluster, error) {
	if err, ok := f.connectsErr[def.Name]; ok {
		return nil, err
	}
	return f.connects[def.Name], nil
}

func (f *fakeConnectPort) Plugins(_ context.Context, def cluster.Definition, connectName string) ([]cluster.ConnectorPlugin, error) {
	k := connectKey(def.Name, connectName)
	if err, ok := f.pluginsErr[k]; ok {
		return nil, err
	}
	return f.plugins[k], nil
}

func (f *fakeConnectPort) ValidatePlugin(_ context.Context, def cluster.Definition, connectName, pluginName string, cfg map[string]any) (cluster.PluginValidation, error) {
	k := connectKey(def.Name, connectName) + "/" + pluginName
	f.lastCfg[k] = cfg
	if err, ok := f.validateErr[k]; ok {
		return cluster.PluginValidation{}, err
	}
	return f.validate[k], nil
}

func (f *fakeConnectPort) AllConnectors(_ context.Context, def cluster.Definition) ([]cluster.ConnectorRef, error) {
	if err, ok := f.allConnectorsErr[def.Name]; ok {
		return nil, err
	}
	return f.allConnectors[def.Name], nil
}

func (f *fakeConnectPort) Connectors(_ context.Context, def cluster.Definition, connectName string) ([]string, error) {
	k := connectKey(def.Name, connectName)
	if err, ok := f.connectorsErr[k]; ok {
		return nil, err
	}
	return f.connectors[k], nil
}

func (f *fakeConnectPort) Connector(_ context.Context, def cluster.Definition, connectName, name string) (cluster.Connector, error) {
	k := connectKey(def.Name, connectName) + "/" + name
	if err, ok := f.connectorErr[k]; ok {
		return cluster.Connector{}, err
	}
	return f.connector[k], nil
}

func (f *fakeConnectPort) ConnectorConfig(_ context.Context, def cluster.Definition, connectName, name string) (map[string]any, error) {
	k := connectKey(def.Name, connectName) + "/" + name
	if err, ok := f.connectorConfigErr[k]; ok {
		return nil, err
	}
	return f.connectorConfig[k], nil
}

func (f *fakeConnectPort) ConnectorTasks(_ context.Context, def cluster.Definition, connectName, name string) ([]cluster.ConnectorTask, error) {
	k := connectKey(def.Name, connectName) + "/" + name
	if err, ok := f.connectorTasksErr[k]; ok {
		return nil, err
	}
	return f.connectorTasks[k], nil
}

// --- Task 5: connector write/action methods ---

func (f *fakeConnectPort) CreateConnector(_ context.Context, def cluster.Definition, connectName, name string, cfg map[string]any) (cluster.Connector, error) {
	k := connectKey(def.Name, connectName) + "/" + name
	f.lastCreateCfg[k] = cfg
	if err, ok := f.createConnectorErr[k]; ok {
		return cluster.Connector{}, err
	}
	return f.createConnector[k], nil
}

func (f *fakeConnectPort) DeleteConnector(_ context.Context, def cluster.Definition, connectName, name string) error {
	k := connectKey(def.Name, connectName) + "/" + name
	if err, ok := f.deleteConnectorErr[k]; ok {
		return err
	}
	return nil
}

func (f *fakeConnectPort) SetConnectorConfig(_ context.Context, def cluster.Definition, connectName, name string, cfg map[string]any) (cluster.Connector, error) {
	k := connectKey(def.Name, connectName) + "/" + name
	f.lastSetConfigCfg[k] = cfg
	if err, ok := f.setConnectorConfigErr[k]; ok {
		return cluster.Connector{}, err
	}
	return f.setConnectorConfig[k], nil
}

func (f *fakeConnectPort) UpdateConnectorState(_ context.Context, def cluster.Definition, connectName, name, action string) error {
	k := connectKey(def.Name, connectName) + "/" + name
	f.lastAction[k] = action
	if err, ok := f.updateConnectorStateErr[k]; ok {
		return err
	}
	return nil
}

func (f *fakeConnectPort) ResetConnectorOffsets(_ context.Context, def cluster.Definition, connectName, name string) error {
	k := connectKey(def.Name, connectName) + "/" + name
	if err, ok := f.resetConnectorOffsetsErr[k]; ok {
		return err
	}
	return nil
}

func (f *fakeConnectPort) RestartConnectorTask(_ context.Context, def cluster.Definition, connectName, name string, taskID int) error {
	k := connectKey(def.Name, connectName) + "/" + name
	f.lastTaskID[k] = taskID
	if err, ok := f.restartConnectorTaskErr[k]; ok {
		return err
	}
	return nil
}

var _ cluster.KafkaConnectPort = (*fakeConnectPort)(nil)

// TestListConnectsForwardsPortAggregation locks in that ConnectService.
// ListConnects does no aggregation/filtering of its own: skip-bad behavior
// (an unreachable Connect worker is dropped, not surfaced as an error) lives
// entirely in infra/connect.Pool.Connects (P2b Task 1-2, covered by that
// package's own tests). This test models that already-filtered outcome --
// two Connect workers configured, only one reachable -- as a port fixture,
// and asserts ListConnects resolves the cluster name then returns the
// port's result verbatim.
func TestListConnectsForwardsPortAggregation(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{
		{Name: "connect-1", Address: "http://c1:8083"},
		{Name: "connect-2", Address: "http://c2:8083"}, // configured but unreachable -- skipped by the port
	}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	port := newFakeConnectPort()
	port.connects["prod"] = []cluster.ConnectCluster{{Name: "connect-1", Address: "http://c1:8083"}}
	svc := appcluster.NewConnectService(res, port)

	got, err := svc.ListConnects(context.Background(), "prod")
	require.NoError(t, err)
	require.Equal(t, []cluster.ConnectCluster{{Name: "connect-1", Address: "http://c1:8083"}}, got)
}

func TestListConnectsUnknownClusterIsErrUnknownCluster(t *testing.T) {
	res := appcluster.NewResolver(nil)
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	_, err := svc.ListConnects(context.Background(), "nope")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestConnectServicePlugins(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{{Name: "connect-1", Address: "http://c1:8083"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	port := newFakeConnectPort()
	port.plugins[connectKey("prod", "connect-1")] = []cluster.ConnectorPlugin{{Class: "io.confluent.connect.jdbc.JdbcSinkConnector"}}
	svc := appcluster.NewConnectService(res, port)

	got, err := svc.Plugins(context.Background(), "prod", "connect-1")
	require.NoError(t, err)
	require.Equal(t, []cluster.ConnectorPlugin{{Class: "io.confluent.connect.jdbc.JdbcSinkConnector"}}, got)
}

func TestConnectServicePluginsUnknownClusterIsErrUnknownCluster(t *testing.T) {
	res := appcluster.NewResolver(nil)
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	_, err := svc.Plugins(context.Background(), "nope", "connect-1")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

// TestConnectServicePluginsUnknownConnectIsErrUnknownConnect locks in the
// pre-validation resolution (see connect.go's ErrUnknownConnect doc
// comment): a known cluster but a connectName not present in its
// Definition.Connects must surface appcluster.ErrUnknownConnect without ever
// reaching the port.
func TestConnectServicePluginsUnknownConnectIsErrUnknownConnect(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{{Name: "connect-1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	port := newFakeConnectPort()
	port.pluginsErr[connectKey("prod", "nope")] = fmt.Errorf("must not be called")
	svc := appcluster.NewConnectService(res, port)

	_, err := svc.Plugins(context.Background(), "prod", "nope")
	require.ErrorIs(t, err, appcluster.ErrUnknownConnect)
}

func TestConnectServiceValidatePlugin(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{{Name: "connect-1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	port := newFakeConnectPort()
	want := cluster.PluginValidation{Name: "JdbcSinkConnector", ErrorCount: 1}
	port.validate[connectKey("prod", "connect-1")+"/"+"JdbcSinkConnector"] = want
	svc := appcluster.NewConnectService(res, port)

	cfg := map[string]any{"connector.class": "JdbcSinkConnector"}
	got, err := svc.ValidatePlugin(context.Background(), "prod", "connect-1", "JdbcSinkConnector", cfg)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, cfg, port.lastCfg[connectKey("prod", "connect-1")+"/"+"JdbcSinkConnector"])
}

func TestConnectServiceValidatePluginUnknownConnectIsErrUnknownConnect(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{{Name: "connect-1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	_, err := svc.ValidatePlugin(context.Background(), "prod", "nope", "JdbcSinkConnector", nil)
	require.ErrorIs(t, err, appcluster.ErrUnknownConnect)
}

func TestConnectServiceValidatePluginUnknownClusterIsErrUnknownCluster(t *testing.T) {
	res := appcluster.NewResolver(nil)
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	_, err := svc.ValidatePlugin(context.Background(), "nope", "connect-1", "JdbcSinkConnector", nil)
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

// --- Task 4: connector read methods ---

// TestConnectServiceAllConnectorsForwardsPortAggregation locks in the same
// "no filtering of its own" shape as TestListConnectsForwardsPortAggregation:
// two Connect workers configured, one unreachable and already dropped by the
// port (P2b-D3), AllConnectors resolves the cluster name then returns the
// port's already-filtered result verbatim.
func TestConnectServiceAllConnectorsForwardsPortAggregation(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{
		{Name: "connect-1", Address: "http://c1:8083"},
		{Name: "connect-2", Address: "http://c2:8083"}, // configured but unreachable -- skipped by the port
	}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	port := newFakeConnectPort()
	port.allConnectors["prod"] = []cluster.ConnectorRef{
		{ConnectName: "connect-1", Name: "jdbc-sink"},
		{ConnectName: "connect-1", Name: "s3-source"},
	}
	svc := appcluster.NewConnectService(res, port)

	got, err := svc.AllConnectors(context.Background(), "prod")
	require.NoError(t, err)
	require.Equal(t, []cluster.ConnectorRef{
		{ConnectName: "connect-1", Name: "jdbc-sink"},
		{ConnectName: "connect-1", Name: "s3-source"},
	}, got)
}

func TestConnectServiceAllConnectorsUnknownClusterIsErrUnknownCluster(t *testing.T) {
	res := appcluster.NewResolver(nil)
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	_, err := svc.AllConnectors(context.Background(), "nope")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestConnectServiceConnectors(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{{Name: "connect-1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	port := newFakeConnectPort()
	port.connectors[connectKey("prod", "connect-1")] = []string{"jdbc-sink"}
	svc := appcluster.NewConnectService(res, port)

	got, err := svc.Connectors(context.Background(), "prod", "connect-1")
	require.NoError(t, err)
	require.Equal(t, []string{"jdbc-sink"}, got)
}

func TestConnectServiceConnectorsUnknownConnectIsErrUnknownConnect(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{{Name: "connect-1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	_, err := svc.Connectors(context.Background(), "prod", "nope")
	require.ErrorIs(t, err, appcluster.ErrUnknownConnect)
}

func TestConnectServiceConnectorsUnknownClusterIsErrUnknownCluster(t *testing.T) {
	res := appcluster.NewResolver(nil)
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	_, err := svc.Connectors(context.Background(), "nope", "connect-1")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

// TestConnectServiceConnectorRoutesToConnectByName locks in that Connector
// routes to the Connect worker actually named by connectName, not just any
// configured Connect -- two Connect workers on the same cluster, same
// connector name on both, asserting the fixture keyed by the right
// (cluster,connectName) pair is the one returned.
func TestConnectServiceConnectorRoutesToConnectByName(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{
		{Name: "connect-1"}, {Name: "connect-2"},
	}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	port := newFakeConnectPort()
	port.connector[connectKey("prod", "connect-1")+"/"+"jdbc-sink"] = cluster.Connector{Name: "jdbc-sink", ConnectName: "connect-1", State: "RUNNING"}
	port.connector[connectKey("prod", "connect-2")+"/"+"jdbc-sink"] = cluster.Connector{Name: "jdbc-sink", ConnectName: "connect-2", State: "PAUSED"}
	svc := appcluster.NewConnectService(res, port)

	got, err := svc.Connector(context.Background(), "prod", "connect-2", "jdbc-sink")
	require.NoError(t, err)
	require.Equal(t, cluster.Connector{Name: "jdbc-sink", ConnectName: "connect-2", State: "PAUSED"}, got)
}

func TestConnectServiceConnectorUnknownConnectIsErrUnknownConnect(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{{Name: "connect-1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	_, err := svc.Connector(context.Background(), "prod", "nope", "jdbc-sink")
	require.ErrorIs(t, err, appcluster.ErrUnknownConnect)
}

func TestConnectServiceConnectorUnknownClusterIsErrUnknownCluster(t *testing.T) {
	res := appcluster.NewResolver(nil)
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	_, err := svc.Connector(context.Background(), "nope", "connect-1", "jdbc-sink")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestConnectServiceConnectorConfig(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{{Name: "connect-1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	port := newFakeConnectPort()
	want := map[string]any{"connector.class": "JdbcSinkConnector"}
	port.connectorConfig[connectKey("prod", "connect-1")+"/"+"jdbc-sink"] = want
	svc := appcluster.NewConnectService(res, port)

	got, err := svc.ConnectorConfig(context.Background(), "prod", "connect-1", "jdbc-sink")
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestConnectServiceConnectorConfigUnknownConnectIsErrUnknownConnect(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{{Name: "connect-1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	_, err := svc.ConnectorConfig(context.Background(), "prod", "nope", "jdbc-sink")
	require.ErrorIs(t, err, appcluster.ErrUnknownConnect)
}

func TestConnectServiceConnectorConfigUnknownClusterIsErrUnknownCluster(t *testing.T) {
	res := appcluster.NewResolver(nil)
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	_, err := svc.ConnectorConfig(context.Background(), "nope", "connect-1", "jdbc-sink")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestConnectServiceConnectorTasks(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{{Name: "connect-1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	port := newFakeConnectPort()
	want := []cluster.ConnectorTask{{ID: 0, State: "RUNNING", WorkerID: "worker-1"}}
	port.connectorTasks[connectKey("prod", "connect-1")+"/"+"jdbc-sink"] = want
	svc := appcluster.NewConnectService(res, port)

	got, err := svc.ConnectorTasks(context.Background(), "prod", "connect-1", "jdbc-sink")
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestConnectServiceConnectorTasksUnknownConnectIsErrUnknownConnect(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{{Name: "connect-1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	_, err := svc.ConnectorTasks(context.Background(), "prod", "nope", "jdbc-sink")
	require.ErrorIs(t, err, appcluster.ErrUnknownConnect)
}

func TestConnectServiceConnectorTasksUnknownClusterIsErrUnknownCluster(t *testing.T) {
	res := appcluster.NewResolver(nil)
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	_, err := svc.ConnectorTasks(context.Background(), "nope", "connect-1", "jdbc-sink")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

// --- Task 5: connector write/action methods ---

func TestConnectServiceCreateConnector(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{{Name: "connect-1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	port := newFakeConnectPort()
	want := cluster.Connector{Name: "jdbc-sink", ConnectName: "connect-1", State: "RUNNING"}
	port.createConnector[connectKey("prod", "connect-1")+"/"+"jdbc-sink"] = want
	svc := appcluster.NewConnectService(res, port)

	cfg := map[string]any{"connector.class": "JdbcSinkConnector"}
	got, err := svc.CreateConnector(context.Background(), "prod", "connect-1", "jdbc-sink", cfg)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, cfg, port.lastCreateCfg[connectKey("prod", "connect-1")+"/"+"jdbc-sink"])
}

func TestConnectServiceCreateConnectorUnknownConnectIsErrUnknownConnect(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{{Name: "connect-1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	port := newFakeConnectPort()
	port.createConnectorErr[connectKey("prod", "nope")+"/"+"jdbc-sink"] = fmt.Errorf("must not be called")
	svc := appcluster.NewConnectService(res, port)

	_, err := svc.CreateConnector(context.Background(), "prod", "nope", "jdbc-sink", nil)
	require.ErrorIs(t, err, appcluster.ErrUnknownConnect)
}

func TestConnectServiceCreateConnectorUnknownClusterIsErrUnknownCluster(t *testing.T) {
	res := appcluster.NewResolver(nil)
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	_, err := svc.CreateConnector(context.Background(), "nope", "connect-1", "jdbc-sink", nil)
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestConnectServiceDeleteConnector(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{{Name: "connect-1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	err := svc.DeleteConnector(context.Background(), "prod", "connect-1", "jdbc-sink")
	require.NoError(t, err)
}

func TestConnectServiceDeleteConnectorUnknownConnectIsErrUnknownConnect(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{{Name: "connect-1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	port := newFakeConnectPort()
	port.deleteConnectorErr[connectKey("prod", "nope")+"/"+"jdbc-sink"] = fmt.Errorf("must not be called")
	svc := appcluster.NewConnectService(res, port)

	err := svc.DeleteConnector(context.Background(), "prod", "nope", "jdbc-sink")
	require.ErrorIs(t, err, appcluster.ErrUnknownConnect)
}

func TestConnectServiceDeleteConnectorUnknownClusterIsErrUnknownCluster(t *testing.T) {
	res := appcluster.NewResolver(nil)
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	err := svc.DeleteConnector(context.Background(), "nope", "connect-1", "jdbc-sink")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestConnectServiceSetConnectorConfig(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{{Name: "connect-1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	port := newFakeConnectPort()
	want := cluster.Connector{Name: "jdbc-sink", ConnectName: "connect-1", State: "RUNNING"}
	port.setConnectorConfig[connectKey("prod", "connect-1")+"/"+"jdbc-sink"] = want
	svc := appcluster.NewConnectService(res, port)

	cfg := map[string]any{"connector.class": "JdbcSinkConnector"}
	got, err := svc.SetConnectorConfig(context.Background(), "prod", "connect-1", "jdbc-sink", cfg)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, cfg, port.lastSetConfigCfg[connectKey("prod", "connect-1")+"/"+"jdbc-sink"])
}

func TestConnectServiceSetConnectorConfigUnknownConnectIsErrUnknownConnect(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{{Name: "connect-1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	_, err := svc.SetConnectorConfig(context.Background(), "prod", "nope", "jdbc-sink", nil)
	require.ErrorIs(t, err, appcluster.ErrUnknownConnect)
}

func TestConnectServiceSetConnectorConfigUnknownClusterIsErrUnknownCluster(t *testing.T) {
	res := appcluster.NewResolver(nil)
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	_, err := svc.SetConnectorConfig(context.Background(), "nope", "connect-1", "jdbc-sink", nil)
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestConnectServiceUpdateConnectorState(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{{Name: "connect-1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	port := newFakeConnectPort()
	svc := appcluster.NewConnectService(res, port)

	err := svc.UpdateConnectorState(context.Background(), "prod", "connect-1", "jdbc-sink", "PAUSE")
	require.NoError(t, err)
	require.Equal(t, "PAUSE", port.lastAction[connectKey("prod", "connect-1")+"/"+"jdbc-sink"])
}

func TestConnectServiceUpdateConnectorStateUnknownConnectIsErrUnknownConnect(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{{Name: "connect-1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	err := svc.UpdateConnectorState(context.Background(), "prod", "nope", "jdbc-sink", "PAUSE")
	require.ErrorIs(t, err, appcluster.ErrUnknownConnect)
}

func TestConnectServiceUpdateConnectorStateUnknownClusterIsErrUnknownCluster(t *testing.T) {
	res := appcluster.NewResolver(nil)
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	err := svc.UpdateConnectorState(context.Background(), "nope", "connect-1", "jdbc-sink", "PAUSE")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestConnectServiceResetConnectorOffsets(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{{Name: "connect-1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	err := svc.ResetConnectorOffsets(context.Background(), "prod", "connect-1", "jdbc-sink")
	require.NoError(t, err)
}

func TestConnectServiceResetConnectorOffsetsUnknownConnectIsErrUnknownConnect(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{{Name: "connect-1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	err := svc.ResetConnectorOffsets(context.Background(), "prod", "nope", "jdbc-sink")
	require.ErrorIs(t, err, appcluster.ErrUnknownConnect)
}

func TestConnectServiceResetConnectorOffsetsUnknownClusterIsErrUnknownCluster(t *testing.T) {
	res := appcluster.NewResolver(nil)
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	err := svc.ResetConnectorOffsets(context.Background(), "nope", "connect-1", "jdbc-sink")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}

func TestConnectServiceRestartConnectorTask(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{{Name: "connect-1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	port := newFakeConnectPort()
	svc := appcluster.NewConnectService(res, port)

	err := svc.RestartConnectorTask(context.Background(), "prod", "connect-1", "jdbc-sink", 2)
	require.NoError(t, err)
	require.Equal(t, 2, port.lastTaskID[connectKey("prod", "connect-1")+"/"+"jdbc-sink"])
}

func TestConnectServiceRestartConnectorTaskUnknownConnectIsErrUnknownConnect(t *testing.T) {
	def := cluster.Definition{Name: "prod", Connects: []cluster.ConnectSpec{{Name: "connect-1"}}}
	res := appcluster.NewResolver([]cluster.Definition{def})
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	err := svc.RestartConnectorTask(context.Background(), "prod", "nope", "jdbc-sink", 0)
	require.ErrorIs(t, err, appcluster.ErrUnknownConnect)
}

func TestConnectServiceRestartConnectorTaskUnknownClusterIsErrUnknownCluster(t *testing.T) {
	res := appcluster.NewResolver(nil)
	svc := appcluster.NewConnectService(res, newFakeConnectPort())

	err := svc.RestartConnectorTask(context.Background(), "nope", "connect-1", "jdbc-sink", 0)
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}
