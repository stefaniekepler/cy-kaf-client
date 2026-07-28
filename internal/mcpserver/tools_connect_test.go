package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	domaincluster "github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/mcppolicy"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

const connectRawTestBound = 2000

type connectCall struct {
	name      string
	cluster   string
	connect   string
	connector string
	plugin    string
	action    string
	taskID    int
	config    map[string]any
}

type recordingConnectApp struct {
	mu sync.Mutex

	connects      []domaincluster.ConnectCluster
	connectors    []string
	allConnectors []domaincluster.ConnectorRef
	connector     domaincluster.Connector
	config        map[string]any
	tasks         []domaincluster.ConnectorTask
	plugins       []domaincluster.ConnectorPlugin
	validation    domaincluster.PluginValidation
	errs          map[string]error
	calls         []connectCall
}

func newRecordingConnectApp() *recordingConnectApp {
	return &recordingConnectApp{
		connects: []domaincluster.ConnectCluster{
			{Name: "worker-b", Address: "https://worker-b.example.test"},
			{Name: "worker-a", Address: "https://worker-a.example.test"},
		},
		connectors: []string{"sink-b", "sink-a"},
		allConnectors: []domaincluster.ConnectorRef{
			{ConnectName: "worker-b", Name: "sink-b", Type: "sink", State: "RUNNING", WorkerID: "node-b", TasksCount: 2, FailedTasksCount: 1},
			{ConnectName: "worker-a", Name: "sink-a", Type: "source", State: "PAUSED", WorkerID: "node-a", TasksCount: 1},
		},
		connector: domaincluster.Connector{
			Name:        "orders-sink",
			ConnectName: "worker-a",
			Type:        "sink",
			State:       "RUNNING",
			WorkerID:    "node-a",
			Config: map[string]any{
				"batch.size": 100,
				"database": map[string]any{
					"password": "do-not-return",
					"ssl":      true,
				},
			},
			TaskIDs: []int{1, 0},
			Topics:  []string{"orders"},
		},
		config: map[string]any{
			"connector.class": "example.Sink",
			"api_token":       "do-not-return",
			"retries":         3,
		},
		tasks: []domaincluster.ConnectorTask{
			{ID: 1, State: "FAILED", Trace: "safe trace", WorkerID: "node-b", Config: map[string]any{"client.secret": "do-not-return", "batch": 2}},
			{ID: 0, State: "RUNNING", WorkerID: "node-a", Config: map[string]any{"batch": 1}},
		},
		plugins: []domaincluster.ConnectorPlugin{
			{Class: "example.Source"},
			{Class: "example.Sink"},
		},
		validation: domaincluster.PluginValidation{
			Name:       "example.Sink",
			ErrorCount: 0,
			Groups:     []string{"Connection"},
			Configs: []domaincluster.PluginConfigEntry{
				{
					Definition: domaincluster.PluginConfigDef{
						Name: "database.password", Type: "PASSWORD", Required: true,
						DefaultValue: "do-not-return", Importance: "HIGH", Group: "Connection",
						Width: "MEDIUM", DisplayName: "Database password", Order: 1,
					},
					Value: domaincluster.PluginConfigValue{
						Name: "database.password", Value: "do-not-return", Visible: true,
					},
				},
				{
					Definition: domaincluster.PluginConfigDef{
						Name: "batch.size", Type: "INT", DefaultValue: "100",
						Importance: "MEDIUM", Group: "Connection", Width: "SHORT",
						DisplayName: "Batch size", Order: 2,
					},
					Value: domaincluster.PluginConfigValue{
						Name: "batch.size", Value: "200",
						RecommendedValues: []string{"100", "200"}, Visible: true,
					},
				},
			},
		},
		errs: make(map[string]error),
	}
}

func (f *recordingConnectApp) ListConnects(_ context.Context, cluster string) ([]domaincluster.ConnectCluster, error) {
	f.record(connectCall{name: "listConnects", cluster: cluster})
	return append([]domaincluster.ConnectCluster(nil), f.connects...), f.operationError("listConnects")
}

func (f *recordingConnectApp) Plugins(_ context.Context, cluster, connect string) ([]domaincluster.ConnectorPlugin, error) {
	f.record(connectCall{name: "plugins", cluster: cluster, connect: connect})
	return append([]domaincluster.ConnectorPlugin(nil), f.plugins...), f.operationError("plugins")
}

func (f *recordingConnectApp) ValidatePlugin(_ context.Context, cluster, connect, plugin string, config map[string]any) (domaincluster.PluginValidation, error) {
	f.record(connectCall{name: "validate", cluster: cluster, connect: connect, plugin: plugin, config: cloneConnectConfig(config)})
	return f.validation, f.operationError("validate")
}

func (f *recordingConnectApp) AllConnectors(_ context.Context, cluster string) ([]domaincluster.ConnectorRef, error) {
	f.record(connectCall{name: "allConnectors", cluster: cluster})
	return append([]domaincluster.ConnectorRef(nil), f.allConnectors...), f.operationError("allConnectors")
}

func (f *recordingConnectApp) Connectors(_ context.Context, cluster, connect string) ([]string, error) {
	f.record(connectCall{name: "connectors", cluster: cluster, connect: connect})
	return append([]string(nil), f.connectors...), f.operationError("connectors")
}

func (f *recordingConnectApp) Connector(_ context.Context, cluster, connect, connector string) (domaincluster.Connector, error) {
	f.record(connectCall{name: "connector", cluster: cluster, connect: connect, connector: connector})
	return cloneConnector(f.connector), f.operationError("connector")
}

func (f *recordingConnectApp) ConnectorConfig(_ context.Context, cluster, connect, connector string) (map[string]any, error) {
	f.record(connectCall{name: "config", cluster: cluster, connect: connect, connector: connector})
	return cloneConnectConfig(f.config), f.operationError("config")
}

func (f *recordingConnectApp) ConnectorTasks(_ context.Context, cluster, connect, connector string) ([]domaincluster.ConnectorTask, error) {
	f.record(connectCall{name: "tasks", cluster: cluster, connect: connect, connector: connector})
	return cloneConnectorTasks(f.tasks), f.operationError("tasks")
}

func (f *recordingConnectApp) CreateConnector(_ context.Context, cluster, connect, connector string, config map[string]any) (domaincluster.Connector, error) {
	f.record(connectCall{name: "create", cluster: cluster, connect: connect, connector: connector, config: cloneConnectConfig(config)})
	return cloneConnector(f.connector), f.operationError("create")
}

func (f *recordingConnectApp) DeleteConnector(_ context.Context, cluster, connect, connector string) error {
	f.record(connectCall{name: "delete", cluster: cluster, connect: connect, connector: connector})
	return f.operationError("delete")
}

func (f *recordingConnectApp) SetConnectorConfig(_ context.Context, cluster, connect, connector string, config map[string]any) (domaincluster.Connector, error) {
	f.record(connectCall{name: "setConfig", cluster: cluster, connect: connect, connector: connector, config: cloneConnectConfig(config)})
	return cloneConnector(f.connector), f.operationError("setConfig")
}

func (f *recordingConnectApp) UpdateConnectorState(_ context.Context, cluster, connect, connector, action string) error {
	f.record(connectCall{name: "state", cluster: cluster, connect: connect, connector: connector, action: action})
	return f.operationError("state")
}

func (f *recordingConnectApp) ResetConnectorOffsets(_ context.Context, cluster, connect, connector string) error {
	f.record(connectCall{name: "resetOffsets", cluster: cluster, connect: connect, connector: connector})
	return f.operationError("resetOffsets")
}

func (f *recordingConnectApp) RestartConnectorTask(_ context.Context, cluster, connect, connector string, taskID int) error {
	f.record(connectCall{name: "restartTask", cluster: cluster, connect: connect, connector: connector, taskID: taskID})
	return f.operationError("restartTask")
}

func (f *recordingConnectApp) record(call connectCall) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call)
}

func (f *recordingConnectApp) operationError(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.errs[name]
}

func (f *recordingConnectApp) snapshot() []connectCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]connectCall(nil), f.calls...)
}

func TestConnectToolsDelegateAllSixteenOperationsWithExactSDKResults(t *testing.T) {
	connectorResult := `{"config":{"batch.size":100,"database":{"password":"[REDACTED]","ssl":true}},"connect":"worker-a","name":"orders-sink","status":{"state":"RUNNING","trace":"","workerId":"node-a"},"tasks":[{"connector":"orders-sink","task":0},{"connector":"orders-sink","task":1}],"topics":["orders"],"type":"SINK"}`
	configBody := map[string]any{"connector.class": "example.Sink", "batch.size": float64(100)}
	tests := []struct {
		name       string
		input      map[string]any
		wantAccess AccessClass
		wantCall   connectCall
		wantText   string
	}{
		{
			name: "getConnects", input: map[string]any{"clusterName": "prod"},
			wantAccess: AccessReadOnly, wantCall: connectCall{name: "listConnects", cluster: "prod"},
			wantText: `{"result":[{"address":"https://worker-a.example.test","name":"worker-a"},{"address":"https://worker-b.example.test","name":"worker-b"}]}`,
		},
		{
			name: "getConnectsCsv", input: map[string]any{"clusterName": "prod"},
			wantAccess: AccessReadOnly, wantCall: connectCall{name: "listConnects", cluster: "prod"},
			wantText: `{"result":"name,address\nworker-a,https://worker-a.example.test\nworker-b,https://worker-b.example.test\n"}`,
		},
		{
			name: "getConnectors", input: map[string]any{"clusterName": "prod", "connectName": "worker-a"},
			wantAccess: AccessReadOnly, wantCall: connectCall{name: "connectors", cluster: "prod", connect: "worker-a"},
			wantText: `{"result":["sink-a","sink-b"]}`,
		},
		{
			name: "getConnector", input: map[string]any{"clusterName": "prod", "connectName": "worker-a", "connectorName": "orders-sink"},
			wantAccess: AccessReadOnly, wantCall: connectCall{name: "connector", cluster: "prod", connect: "worker-a", connector: "orders-sink"},
			wantText: connectorResult,
		},
		{
			name: "getAllConnectors", input: map[string]any{"clusterName": "prod"},
			wantAccess: AccessReadOnly, wantCall: connectCall{name: "allConnectors", cluster: "prod"},
			wantText: `{"result":[{"connect":"worker-a","failedTasksCount":0,"name":"sink-a","status":{"state":"PAUSED","workerId":"node-a"},"tasksCount":1,"type":"SOURCE"},{"connect":"worker-b","failedTasksCount":1,"name":"sink-b","status":{"state":"RUNNING","workerId":"node-b"},"tasksCount":2,"type":"SINK"}]}`,
		},
		{
			name: "getAllConnectorsCsv", input: map[string]any{"clusterName": "prod"},
			wantAccess: AccessReadOnly, wantCall: connectCall{name: "allConnectors", cluster: "prod"},
			wantText: `{"result":"connect,name,type,state,workerId,tasksCount,failedTasksCount\nworker-a,sink-a,SOURCE,PAUSED,node-a,1,0\nworker-b,sink-b,SINK,RUNNING,node-b,2,1\n"}`,
		},
		{
			name: "getConnectorConfig", input: map[string]any{"clusterName": "prod", "connectName": "worker-a", "connectorName": "orders-sink"},
			wantAccess: AccessReadOnly, wantCall: connectCall{name: "config", cluster: "prod", connect: "worker-a", connector: "orders-sink"},
			wantText: `{"api_token":"[REDACTED]","connector.class":"example.Sink","retries":3}`,
		},
		{
			name: "getConnectorTasks", input: map[string]any{"clusterName": "prod", "connectName": "worker-a", "connectorName": "orders-sink"},
			wantAccess: AccessReadOnly, wantCall: connectCall{name: "tasks", cluster: "prod", connect: "worker-a", connector: "orders-sink"},
			wantText: `{"result":[{"config":{"batch":1},"id":{"connector":"orders-sink","task":0},"status":{"id":0,"state":"RUNNING","trace":"","workerId":"node-a"}},{"config":{"batch":2,"client.secret":"[REDACTED]"},"id":{"connector":"orders-sink","task":1},"status":{"id":1,"state":"FAILED","trace":"[REDACTED]","workerId":"node-b"}}]}`,
		},
		{
			name: "getConnectorPlugins", input: map[string]any{"clusterName": "prod", "connectName": "worker-a"},
			wantAccess: AccessReadOnly, wantCall: connectCall{name: "plugins", cluster: "prod", connect: "worker-a"},
			wantText: `{"result":[{"class":"example.Sink"},{"class":"example.Source"}]}`,
		},
		{
			name:       "validateConnectorPluginConfig",
			input:      map[string]any{"clusterName": "prod", "connectName": "worker-a", "pluginName": "example.Sink", "body": configBody},
			wantAccess: AccessReadOnly, wantCall: connectCall{name: "validate", cluster: "prod", connect: "worker-a", plugin: "example.Sink", config: configBody},
			wantText: `{"configs":[{"definition":{"defaultValue":"100","displayName":"Batch size","documentation":"","group":"Connection","importance":"MEDIUM","name":"batch.size","order":2,"required":false,"type":"INT","width":"SHORT"},"value":{"name":"batch.size","recommendedValues":["100","200"],"value":"200","visible":true}},{"definition":{"defaultValue":"[REDACTED]","displayName":"Database password","documentation":"","group":"Connection","importance":"HIGH","name":"database.password","order":1,"required":true,"type":"PASSWORD","width":"MEDIUM"},"value":{"name":"database.password","value":"[REDACTED]","visible":true}}],"errorCount":0,"groups":["Connection"],"name":"example.Sink"}`,
		},
		{
			name:       "createConnector",
			input:      map[string]any{"clusterName": "prod", "connectName": "worker-a", "body": map[string]any{"name": "orders-sink", "config": configBody}},
			wantAccess: AccessWrite, wantCall: connectCall{name: "create", cluster: "prod", connect: "worker-a", connector: "orders-sink", config: configBody},
			wantText: connectorResult,
		},
		{
			name:       "deleteConnector",
			input:      map[string]any{"clusterName": "prod", "connectName": "worker-a", "connectorName": "orders-sink"},
			wantAccess: AccessWrite, wantCall: connectCall{name: "delete", cluster: "prod", connect: "worker-a", connector: "orders-sink"},
			wantText: `{"status":"deleted"}`,
		},
		{
			name:       "setConnectorConfig",
			input:      map[string]any{"clusterName": "prod", "connectName": "worker-a", "connectorName": "orders-sink", "body": configBody},
			wantAccess: AccessWrite, wantCall: connectCall{name: "setConfig", cluster: "prod", connect: "worker-a", connector: "orders-sink", config: configBody},
			wantText: connectorResult,
		},
		{
			name:       "updateConnectorState",
			input:      map[string]any{"clusterName": "prod", "connectName": "worker-a", "connectorName": "orders-sink", "action": "PAUSE"},
			wantAccess: AccessWrite, wantCall: connectCall{name: "state", cluster: "prod", connect: "worker-a", connector: "orders-sink", action: "PAUSE"},
			wantText: `{"action":"PAUSE","status":"updated"}`,
		},
		{
			name:       "restartConnectorTask",
			input:      map[string]any{"clusterName": "prod", "connectName": "worker-a", "connectorName": "orders-sink", "taskId": float64(2)},
			wantAccess: AccessWrite, wantCall: connectCall{name: "restartTask", cluster: "prod", connect: "worker-a", connector: "orders-sink", taskID: 2},
			wantText: `{"status":"restarted","taskId":2}`,
		},
		{
			name:       "resetConnectorOffsets",
			input:      map[string]any{"clusterName": "prod", "connectName": "worker-a", "connectorName": "orders-sink"},
			wantAccess: AccessWrite, wantCall: connectCall{name: "resetOffsets", cluster: "prod", connect: "worker-a", connector: "orders-sink"},
			wantText: `{"status":"reset"}`,
		},
	}
	require.Len(t, tests, 16)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newRecordingConnectApp()
			executor, _ := newConnectExecutor(t, app, false, true)
			spec := requireCatalogSpec(t, test.name)
			require.Equal(t, test.wantAccess, spec.Meta.Access)
			result, err := newSDKSession(t, spec, executor).CallTool(
				context.Background(), &mcp.CallToolParams{Name: test.name, Arguments: test.input},
			)
			require.NoError(t, err)
			require.False(t, result.IsError, callToolText(t, result))
			require.JSONEq(t, test.wantText, callToolText(t, result))
			requireStructuredJSONEq(t, test.wantText, result.StructuredContent)
			require.Equal(t, []connectCall{test.wantCall}, app.snapshot())
		})
	}
}

func TestConnectToolsKeepTenReadsVisibleAndGateSixWrites(t *testing.T) {
	executor, store := newConnectExecutor(t, newRecordingConnectApp(), false, false)
	session := newSDKCatalogSession(t, VisibleCatalog(*mustLoadPolicy(t, store)), executor)
	listed, err := session.ListTools(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, listed.Tools, 51)

	for _, name := range []string{
		"getConnects", "getConnectsCsv", "getConnectors", "getConnector",
		"getAllConnectors", "getAllConnectorsCsv", "getConnectorConfig",
		"getConnectorTasks", "getConnectorPlugins", "validateConnectorPluginConfig",
	} {
		require.NotNil(t, findTool(t, listed.Tools, name))
	}
	for _, name := range []string{
		"createConnector", "deleteConnector", "setConnectorConfig",
		"updateConnectorState", "restartConnectorTask", "resetConnectorOffsets",
	} {
		require.Nil(t, findToolOrNil(listed.Tools, name))
	}
}

func TestConnectToolsValidationIsReadOnlyAndLimitsConfigKeys(t *testing.T) {
	t.Run("read only cluster and policy may validate", func(t *testing.T) {
		app := newRecordingConnectApp()
		executor, _ := newConnectExecutor(t, app, true, false)
		result := callConnectTool(t, executor, "validateConnectorPluginConfig", map[string]any{
			"clusterName": "prod", "connectName": "worker-a", "pluginName": "example.Sink",
			"body": map[string]any{"batch.size": float64(100)},
		}, false)
		require.False(t, result.IsError, callToolText(t, result))
		require.Equal(t, "validate", app.snapshot()[0].name)
	})

	t.Run("more than two hundred keys is rejected before delegation", func(t *testing.T) {
		app := newRecordingConnectApp()
		body := make(map[string]any, 201)
		for index := 0; index < 201; index++ {
			body[fmt.Sprintf("key-%03d", index)] = index
		}
		executor, _ := newConnectExecutor(t, app, false, false)
		result := callConnectTool(t, executor, "validateConnectorPluginConfig", map[string]any{
			"clusterName": "prod", "connectName": "worker-a", "pluginName": "example.Sink",
			"body": body,
		}, true)
		require.Equal(t, "invalid_request", callToolText(t, result))
		require.Empty(t, app.snapshot())
	})
}

func TestValidatedConnectorQueryAcceptsSupportedValuesAndRejectsUnsafeInputs(t *testing.T) {
	t.Run("valid search order and sort are preserved", func(t *testing.T) {
		search := "orders"
		orderBy := generated.ConnectorColumnsToSortSTATUS
		sortOrder := generated.DESC
		fts := false

		got, err := validatedConnectorQuery(&search, &orderBy, &sortOrder, &fts)

		require.NoError(t, err)
		require.Equal(t, connectorQuery{
			Search:    "orders",
			OrderBy:   generated.ConnectorColumnsToSortSTATUS,
			SortOrder: generated.DESC,
		}, got)
	})

	t.Run("oversized search is rejected", func(t *testing.T) {
		search := strings.Repeat("x", maxClusterBrokerNameBytes+1)

		got, err := validatedConnectorQuery(&search, nil, nil, nil)

		require.ErrorIs(t, err, errInvalidRequest)
		require.Equal(t, connectorQuery{}, got)
	})

	t.Run("unknown order column is rejected", func(t *testing.T) {
		orderBy := generated.ConnectorColumnsToSort("CREDENTIAL")

		got, err := validatedConnectorQuery(nil, &orderBy, nil, nil)

		require.ErrorIs(t, err, errInvalidRequest)
		require.Equal(t, connectorQuery{}, got)
	})

	t.Run("unknown sort direction is rejected", func(t *testing.T) {
		sortOrder := generated.SortOrder("SIDEWAYS")

		got, err := validatedConnectorQuery(nil, nil, &sortOrder, nil)

		require.ErrorIs(t, err, errInvalidRequest)
		require.Equal(t, connectorQuery{}, got)
	})

	t.Run("full text search mode is rejected", func(t *testing.T) {
		fts := true

		got, err := validatedConnectorQuery(nil, nil, nil, &fts)

		require.ErrorIs(t, err, errInvalidRequest)
		require.Equal(t, connectorQuery{}, got)
	})
}

func TestConnectToolsBoundListsAndCSVDeterministically(t *testing.T) {
	app := newRecordingConnectApp()
	for index := maxListItems + 10; index >= 0; index-- {
		app.connectors = append(app.connectors, fmt.Sprintf("connector-%04d", index))
	}
	executor, _ := newConnectExecutor(t, app, false, false)
	result := callConnectTool(t, executor, "getConnectors", map[string]any{
		"clusterName": "prod", "connectName": "worker-a",
	}, false)
	var envelope struct {
		Result []string `json:"result"`
	}
	require.NoError(t, jsonUnmarshalResult(result, &envelope))
	require.Len(t, envelope.Result, maxListItems)
	require.Equal(t, "connector-0000", envelope.Result[0])
	require.Equal(t, "connector-0499", envelope.Result[maxListItems-1])

	csvApp := newRecordingConnectApp()
	csvApp.allConnectors = make([]domaincluster.ConnectorRef, 0, maxListItems+1)
	for index := 0; index <= maxListItems; index++ {
		csvApp.allConnectors = append(csvApp.allConnectors, domaincluster.ConnectorRef{
			ConnectName: "worker", Name: fmt.Sprintf("connector-%04d", index),
		})
	}
	csvExecutor, _ := newConnectExecutor(t, csvApp, false, false)
	csvResult := callConnectTool(t, csvExecutor, "getAllConnectorsCsv", map[string]any{
		"clusterName": "prod",
	}, false)
	require.False(t, csvResult.IsError, callToolText(t, csvResult))
	require.Equal(t, maxListItems+1, strings.Count(callToolText(t, csvResult), `\n`))
	require.NotContains(t, callToolText(t, csvResult), "connector-0500")
}

func TestConnectToolsRejectInvalidActionAndMapUnknownConnectSafely(t *testing.T) {
	t.Run("invalid action never reaches app", func(t *testing.T) {
		app := newRecordingConnectApp()
		executor, _ := newConnectExecutor(t, app, false, true)
		result := callConnectTool(t, executor, "updateConnectorState", map[string]any{
			"clusterName": "prod", "connectName": "worker-a",
			"connectorName": "orders-sink", "action": "EXECUTE",
		}, true)
		require.Equal(t, "invalid_request", callToolText(t, result))
		require.Empty(t, app.snapshot())
	})

	t.Run("known cluster without requested connect is not found and hides details", func(t *testing.T) {
		app := newRecordingConnectApp()
		app.errs["connectors"] = fmt.Errorf("%w: %s", appcluster.ErrUnknownConnect, "https://admin:password@worker.internal")
		executor, _ := newConnectExecutor(t, app, false, false)
		result := callConnectTool(t, executor, "getConnectors", map[string]any{
			"clusterName": "prod", "connectName": "missing",
		}, true)
		require.Equal(t, "not_found", callToolText(t, result))
		require.NotContains(t, callToolText(t, result), "password")
		require.NotContains(t, callToolText(t, result), "worker.internal")
	})
}

func TestConnectToolsRedactConfiguredURLCredentialsAndRawTraceDetails(t *testing.T) {
	t.Run("configured Connect address drops URL user info", func(t *testing.T) {
		app := newRecordingConnectApp()
		app.connects = []domaincluster.ConnectCluster{{
			Name: "worker-a", Address: "https://admin:do-not-return@worker.example.test:8083?region=west&access_token=do-not-return",
		}}
		executor, _ := newConnectExecutor(t, app, false, false)
		result := callConnectTool(t, executor, "getConnects", map[string]any{
			"clusterName": "prod",
		}, false)
		require.NotContains(t, callToolText(t, result), "admin")
		require.NotContains(t, callToolText(t, result), "do-not-return")
		require.Contains(t, callToolText(t, result), "https://worker.example.test:8083")
		require.Contains(t, callToolText(t, result), "access_token=%5BREDACTED%5D")
	})

	t.Run("connector and task traces never expose raw errors", func(t *testing.T) {
		app := newRecordingConnectApp()
		app.connector.Trace = "authentication failed: password=do-not-return"
		app.tasks[0].Trace = "request Authorization: Bearer do-not-return"
		executor, _ := newConnectExecutor(t, app, false, false)

		connector := callConnectTool(t, executor, "getConnector", map[string]any{
			"clusterName": "prod", "connectName": "worker-a", "connectorName": "orders-sink",
		}, false)
		require.NotContains(t, callToolText(t, connector), "do-not-return")
		require.Contains(t, callToolText(t, connector), `"trace":"[REDACTED]"`)

		tasks := callConnectTool(t, executor, "getConnectorTasks", map[string]any{
			"clusterName": "prod", "connectName": "worker-a", "connectorName": "orders-sink",
		}, false)
		require.NotContains(t, callToolText(t, tasks), "do-not-return")
		require.Contains(t, callToolText(t, tasks), `"trace":"[REDACTED]"`)
	})

	t.Run("credential validation recommendations and errors are redacted", func(t *testing.T) {
		app := newRecordingConnectApp()
		app.validation.Configs[0].Value.RecommendedValues = []string{"do-not-return"}
		app.validation.Configs[0].Value.Errors = []string{"rejected password do-not-return"}
		executor, _ := newConnectExecutor(t, app, false, false)
		result := callConnectTool(t, executor, "validateConnectorPluginConfig", map[string]any{
			"clusterName": "prod", "connectName": "worker-a", "pluginName": "example.Sink",
			"body": map[string]any{"database.password": "do-not-return"},
		}, false)
		require.NotContains(t, callToolText(t, result), "do-not-return")
		require.Contains(t, callToolText(t, result), `"recommendedValues":["[REDACTED]"]`)
		require.Contains(t, callToolText(t, result), `"errors":["[REDACTED]"]`)
	})
}

func TestConnectToolsRejectUnboundedNestedValidationLists(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*recordingConnectApp)
	}{
		{
			name: "groups",
			mutate: func(app *recordingConnectApp) {
				app.validation.Groups = make([]string, maxListItems+1)
			},
		},
		{
			name: "dependents",
			mutate: func(app *recordingConnectApp) {
				app.validation.Configs[0].Definition.Dependents = make([]string, maxListItems+1)
			},
		},
		{
			name: "recommendations",
			mutate: func(app *recordingConnectApp) {
				app.validation.Configs[1].Value.RecommendedValues = make([]string, maxListItems+1)
			},
		},
		{
			name: "errors",
			mutate: func(app *recordingConnectApp) {
				app.validation.Configs[1].Value.Errors = make([]string, maxListItems+1)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newRecordingConnectApp()
			test.mutate(app)
			executor, _ := newConnectExecutor(t, app, false, false)
			result := callConnectTool(t, executor, "validateConnectorPluginConfig", map[string]any{
				"clusterName": "prod", "connectName": "worker-a",
				"pluginName": "example.Sink", "body": map[string]any{},
			}, true)
			require.Equal(t, "result_too_large", callToolText(t, result))
		})
	}
}

func TestConnectToolsRedactAllConnectCredentialAliasesInTextAndStructuredContent(t *testing.T) {
	aliases := map[string]any{
		"basic.auth.user.info": "do-not-return-basic",
		"api.key":              "do-not-return-api-dot",
		"api_key":              "do-not-return-api-underscore",
		"apikey":               "do-not-return-api-compact",
		"authorization":        "do-not-return-authorization",
		"enabled":              true,
		"retries":              3,
	}

	t.Run("connector config", func(t *testing.T) {
		app := newRecordingConnectApp()
		app.config = cloneConnectConfig(aliases)
		executor, _ := newConnectExecutor(t, app, false, false)
		result := callConnectTool(t, executor, "getConnectorConfig", map[string]any{
			"clusterName": "prod", "connectName": "worker-a",
			"connectorName": "orders-sink",
		}, false)
		want := `{
			"basic.auth.user.info":"[REDACTED]",
			"api.key":"[REDACTED]",
			"api_key":"[REDACTED]",
			"apikey":"[REDACTED]",
			"authorization":"[REDACTED]",
			"enabled":true,
			"retries":3
		}`
		require.JSONEq(t, want, callToolText(t, result))
		requireStructuredJSONEq(t, want, result.StructuredContent)
	})

	t.Run("connector and task nested config", func(t *testing.T) {
		app := newRecordingConnectApp()
		app.connector.Config = cloneConnectConfig(aliases)
		app.tasks = []domaincluster.ConnectorTask{{
			ID: 0, State: "RUNNING", WorkerID: "node-a",
			Config: cloneConnectConfig(aliases),
		}}
		executor, _ := newConnectExecutor(t, app, false, false)

		connector := callConnectTool(t, executor, "getConnector", map[string]any{
			"clusterName": "prod", "connectName": "worker-a",
			"connectorName": "orders-sink",
		}, false)
		requireConnectNestedAliasesRedacted(t, connector, "config")

		tasks := callConnectTool(t, executor, "getConnectorTasks", map[string]any{
			"clusterName": "prod", "connectName": "worker-a",
			"connectorName": "orders-sink",
		}, false)
		requireConnectTaskAliasesRedacted(t, tasks)
	})

	t.Run("plugin defaults values recommendations and all remote errors", func(t *testing.T) {
		app := newRecordingConnectApp()
		app.validation.Configs = []domaincluster.PluginConfigEntry{
			{
				Definition: domaincluster.PluginConfigDef{
					Name: "api.key", Type: "STRING", Importance: "HIGH", Width: "MEDIUM",
					DefaultValue: "do-not-return-default",
				},
				Value: domaincluster.PluginConfigValue{
					Name: "api.key", Value: "do-not-return-value",
					RecommendedValues: []string{"do-not-return-recommendation"},
					Errors:            []string{"do-not-return-credential-error"},
				},
			},
			{
				Definition: domaincluster.PluginConfigDef{
					Name: "batch.size", Type: "INT", Importance: "MEDIUM", Width: "SHORT",
				},
				Value: domaincluster.PluginConfigValue{
					Name: "batch.size", Value: "100",
					Errors: []string{"remote error echoed do-not-return-safe-field-value"},
				},
			},
		}
		executor, _ := newConnectExecutor(t, app, false, false)
		result := callConnectTool(t, executor, "validateConnectorPluginConfig", map[string]any{
			"clusterName": "prod", "connectName": "worker-a",
			"pluginName": "example.Sink", "body": map[string]any{},
		}, false)
		for _, marker := range []string{
			"do-not-return-default",
			"do-not-return-value",
			"do-not-return-recommendation",
			"do-not-return-credential-error",
			"do-not-return-safe-field-value",
		} {
			require.NotContains(t, callToolText(t, result), marker)
			structured, err := json.Marshal(result.StructuredContent)
			require.NoError(t, err)
			require.NotContains(t, string(structured), marker)
		}
		require.Equal(t, 5, strings.Count(callToolText(t, result), `"[REDACTED]"`))
		var textPayload map[string]any
		require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &textPayload))
		var structuredPayload map[string]any
		require.NoError(t, remarshalConnectStructured(result.StructuredContent, &structuredPayload))
		require.Equal(t, textPayload, structuredPayload)
	})

	t.Run("configured address query", func(t *testing.T) {
		app := newRecordingConnectApp()
		app.connects = []domaincluster.ConnectCluster{{
			Name: "worker-a",
			Address: "https://worker.example.test:8083?" +
				"basic.auth.user.info=one&api.key=two&api_key=three&" +
				"apikey=four&authorization=five&region=west",
		}}
		executor, _ := newConnectExecutor(t, app, false, false)
		result := callConnectTool(t, executor, "getConnects", map[string]any{
			"clusterName": "prod",
		}, false)
		for _, structured := range []any{
			json.RawMessage(callToolText(t, result)),
			result.StructuredContent,
		} {
			address := connectAddressFromStructured(t, structured)
			parsed, err := url.Parse(address)
			require.NoError(t, err)
			for _, alias := range []string{
				"basic.auth.user.info", "api.key", "api_key", "apikey", "authorization",
			} {
				require.Equal(t, redactedValue, parsed.Query().Get(alias), alias)
			}
			require.Equal(t, "west", parsed.Query().Get("region"))
		}
	})
}

func TestConnectToolsBoundRawResultsBeforeLocalProcessing(t *testing.T) {
	t.Run("over-bound remote slices fail before conversion", func(t *testing.T) {
		tests := []struct {
			name   string
			tool   string
			input  map[string]any
			mutate func(*recordingConnectApp)
		}{
			{
				name: "connects", tool: "getConnects",
				input: map[string]any{"clusterName": "prod"},
				mutate: func(app *recordingConnectApp) {
					app.connects = make([]domaincluster.ConnectCluster, connectRawTestBound+1)
					app.connects[0] = domaincluster.ConnectCluster{Name: "", Address: "not-a-url"}
				},
			},
			{
				name: "connector names", tool: "getConnectors",
				input: map[string]any{"clusterName": "prod", "connectName": "worker-a"},
				mutate: func(app *recordingConnectApp) {
					app.connectors = make([]string, connectRawTestBound+1)
					app.connectors[0] = ""
				},
			},
			{
				name: "connector refs", tool: "getAllConnectors",
				input: map[string]any{"clusterName": "prod"},
				mutate: func(app *recordingConnectApp) {
					app.allConnectors = make([]domaincluster.ConnectorRef, connectRawTestBound+1)
					app.allConnectors[0] = domaincluster.ConnectorRef{TasksCount: -1}
				},
			},
			{
				name: "tasks", tool: "getConnectorTasks",
				input: map[string]any{
					"clusterName": "prod", "connectName": "worker-a",
					"connectorName": "orders-sink",
				},
				mutate: func(app *recordingConnectApp) {
					app.tasks = make([]domaincluster.ConnectorTask, connectRawTestBound+1)
					app.tasks[0] = domaincluster.ConnectorTask{ID: -1}
				},
			},
			{
				name: "plugins", tool: "getConnectorPlugins",
				input: map[string]any{"clusterName": "prod", "connectName": "worker-a"},
				mutate: func(app *recordingConnectApp) {
					app.plugins = make([]domaincluster.ConnectorPlugin, connectRawTestBound+1)
					app.plugins[0] = domaincluster.ConnectorPlugin{Class: ""}
				},
			},
			{
				name: "task ids", tool: "getConnector",
				input: map[string]any{
					"clusterName": "prod", "connectName": "worker-a",
					"connectorName": "orders-sink",
				},
				mutate: func(app *recordingConnectApp) {
					app.connector.TaskIDs = make([]int, connectRawTestBound+1)
					app.connector.TaskIDs[0] = -1
				},
			},
			{
				name: "topics", tool: "getConnector",
				input: map[string]any{
					"clusterName": "prod", "connectName": "worker-a",
					"connectorName": "orders-sink",
				},
				mutate: func(app *recordingConnectApp) {
					app.connector.Topics = make([]string, connectRawTestBound+1)
					app.connector.Topics[0] = ""
				},
			},
			{
				name: "plugin configs", tool: "validateConnectorPluginConfig",
				input: map[string]any{
					"clusterName": "prod", "connectName": "worker-a",
					"pluginName": "example.Sink", "body": map[string]any{},
				},
				mutate: func(app *recordingConnectApp) {
					app.validation.Configs = make([]domaincluster.PluginConfigEntry, maxMCPConnectorConfigKeys+1)
					app.validation.Configs[0].Definition.Name = ""
				},
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				app := newRecordingConnectApp()
				test.mutate(app)
				executor, _ := newConnectExecutor(t, app, false, false)
				result := callConnectTool(t, executor, test.tool, test.input, true)
				require.Equal(t, "result_too_large", callToolText(t, result))
			})
		}
	})

	t.Run("exact raw boundary still deterministically returns top five hundred", func(t *testing.T) {
		app := newRecordingConnectApp()
		app.connectors = make([]string, 0, connectRawTestBound)
		for index := connectRawTestBound - 1; index >= 0; index-- {
			app.connectors = append(app.connectors, fmt.Sprintf("connector-%04d", index))
		}
		executor, _ := newConnectExecutor(t, app, false, false)
		result := callConnectTool(t, executor, "getConnectors", map[string]any{
			"clusterName": "prod", "connectName": "worker-a",
		}, false)
		var envelope struct {
			Result []string `json:"result"`
		}
		require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &envelope))
		require.Len(t, envelope.Result, maxListItems)
		require.Equal(t, "connector-0000", envelope.Result[0])
		require.Equal(t, "connector-0499", envelope.Result[maxListItems-1])
	})

	t.Run("every raw slice accepts its exact independent boundary", func(t *testing.T) {
		tests := []struct {
			name   string
			tool   string
			input  map[string]any
			mutate func(*recordingConnectApp)
			assert func(*testing.T, *mcp.CallToolResult)
		}{
			{
				name: "connects", tool: "getConnects",
				input: map[string]any{"clusterName": "prod"},
				mutate: func(app *recordingConnectApp) {
					app.connects = make([]domaincluster.ConnectCluster, 0, connectRawTestBound)
					for index := 0; index < connectRawTestBound; index++ {
						app.connects = append(app.connects, domaincluster.ConnectCluster{
							Name: fmt.Sprintf("worker-%04d", index),
							Address: fmt.Sprintf(
								"https://worker-%04d.example.test", index,
							),
						})
					}
				},
				assert: requireConnectTopLevelResultLength(maxListItems),
			},
			{
				name: "refs", tool: "getAllConnectors",
				input: map[string]any{"clusterName": "prod"},
				mutate: func(app *recordingConnectApp) {
					app.allConnectors = make([]domaincluster.ConnectorRef, 0, connectRawTestBound)
					for index := 0; index < connectRawTestBound; index++ {
						app.allConnectors = append(app.allConnectors, domaincluster.ConnectorRef{
							ConnectName: "worker-a",
							Name:        fmt.Sprintf("connector-%04d", index),
							Type:        "sink",
							State:       "RUNNING",
						})
					}
				},
				assert: requireConnectTopLevelResultLength(maxListItems),
			},
			{
				name: "tasks", tool: "getConnectorTasks",
				input: map[string]any{
					"clusterName": "prod", "connectName": "worker-a",
					"connectorName": "orders-sink",
				},
				mutate: func(app *recordingConnectApp) {
					app.tasks = make([]domaincluster.ConnectorTask, 0, connectRawTestBound)
					for index := 0; index < connectRawTestBound; index++ {
						app.tasks = append(app.tasks, domaincluster.ConnectorTask{
							ID: index, State: "RUNNING", WorkerID: "worker-a",
						})
					}
				},
				assert: requireConnectTopLevelResultLength(maxListItems),
			},
			{
				name: "plugins", tool: "getConnectorPlugins",
				input: map[string]any{"clusterName": "prod", "connectName": "worker-a"},
				mutate: func(app *recordingConnectApp) {
					app.plugins = make([]domaincluster.ConnectorPlugin, 0, connectRawTestBound)
					for index := 0; index < connectRawTestBound; index++ {
						app.plugins = append(app.plugins, domaincluster.ConnectorPlugin{
							Class: fmt.Sprintf("example.Plugin%04d", index),
						})
					}
				},
				assert: requireConnectTopLevelResultLength(maxListItems),
			},
			{
				name: "task ids", tool: "getConnector",
				input: map[string]any{
					"clusterName": "prod", "connectName": "worker-a",
					"connectorName": "orders-sink",
				},
				mutate: func(app *recordingConnectApp) {
					app.connector.TaskIDs = make([]int, connectRawTestBound)
					for index := range app.connector.TaskIDs {
						app.connector.TaskIDs[index] = index
					}
				},
				assert: requireConnectNestedResultLength("tasks", maxListItems),
			},
			{
				name: "topics", tool: "getConnector",
				input: map[string]any{
					"clusterName": "prod", "connectName": "worker-a",
					"connectorName": "orders-sink",
				},
				mutate: func(app *recordingConnectApp) {
					app.connector.Topics = make([]string, connectRawTestBound)
					for index := range app.connector.Topics {
						app.connector.Topics[index] = fmt.Sprintf("topic-%04d", index)
					}
				},
				assert: requireConnectNestedResultLength("topics", maxListItems),
			},
			{
				name: "plugin configs", tool: "validateConnectorPluginConfig",
				input: map[string]any{
					"clusterName": "prod", "connectName": "worker-a",
					"pluginName": "example.Sink", "body": map[string]any{},
				},
				mutate: func(app *recordingConnectApp) {
					app.validation.Configs = make(
						[]domaincluster.PluginConfigEntry,
						maxMCPConnectorConfigKeys,
					)
					for index := range app.validation.Configs {
						name := fmt.Sprintf("config-%03d", index)
						app.validation.Configs[index] = domaincluster.PluginConfigEntry{
							Definition: domaincluster.PluginConfigDef{
								Name: name, Type: "STRING", Importance: "LOW", Width: "NONE",
							},
							Value: domaincluster.PluginConfigValue{Name: name},
						}
					}
				},
				assert: requireConnectNestedResultLength(
					"configs", maxMCPConnectorConfigKeys,
				),
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				app := newRecordingConnectApp()
				test.mutate(app)
				executor, _ := newConnectExecutor(t, app, false, false)
				result := callConnectTool(t, executor, test.tool, test.input, false)
				test.assert(t, result)
			})
		}
	})
}

func TestConnectToolsRejectWithStatsBeforeDelegation(t *testing.T) {
	for _, name := range []string{"getConnects", "getConnectsCsv"} {
		t.Run(name+" true", func(t *testing.T) {
			app := newRecordingConnectApp()
			executor, _ := newConnectExecutor(t, app, false, false)
			result := callConnectTool(t, executor, name, map[string]any{
				"clusterName": "prod", "query": map[string]any{"withStats": true},
			}, true)
			require.Equal(t, "invalid_request", callToolText(t, result))
			require.Empty(t, app.snapshot())
		})
		t.Run(name+" false", func(t *testing.T) {
			app := newRecordingConnectApp()
			executor, _ := newConnectExecutor(t, app, false, false)
			result := callConnectTool(t, executor, name, map[string]any{
				"clusterName": "prod", "query": map[string]any{"withStats": false},
			}, false)
			require.False(t, result.IsError)
			require.Equal(t, "listConnects", app.snapshot()[0].name)
		})
	}
}

func TestConnectToolsRejectMalformedPluginEnumsWithoutRawDetails(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*domaincluster.PluginConfigDef)
	}{
		{name: "type", mutate: func(def *domaincluster.PluginConfigDef) { def.Type = "FUTURE_SECRET_TYPE" }},
		{name: "importance", mutate: func(def *domaincluster.PluginConfigDef) { def.Importance = "FUTURE_IMPORTANCE" }},
		{name: "width", mutate: func(def *domaincluster.PluginConfigDef) { def.Width = "FUTURE_WIDTH" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newRecordingConnectApp()
			test.mutate(&app.validation.Configs[0].Definition)
			executor, _ := newConnectExecutor(t, app, false, false)
			result := callConnectTool(t, executor, "validateConnectorPluginConfig", map[string]any{
				"clusterName": "prod", "connectName": "worker-a",
				"pluginName": "example.Sink", "body": map[string]any{},
			}, true)
			require.Equal(t, "operation_failed", callToolText(t, result))
			require.NotContains(t, callToolText(t, result), "FUTURE")
		})
	}
}

func TestConnectToolsConfigBodyExactByteBoundary(t *testing.T) {
	payloadAtBoundary := strings.Repeat("a", maxMCPConnectorConfigBytes-len(`{"x":""}`))
	tests := []struct {
		name      string
		value     string
		wantError bool
		wantCalls int
	}{
		{name: "exact 256KiB", value: payloadAtBoundary, wantCalls: 1},
		{name: "one byte over", value: payloadAtBoundary + "a", wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newRecordingConnectApp()
			executor, _ := newConnectExecutor(t, app, false, false)
			result := callConnectTool(t, executor, "validateConnectorPluginConfig", map[string]any{
				"clusterName": "prod", "connectName": "worker-a",
				"pluginName": "example.Sink", "body": map[string]any{"x": test.value},
			}, test.wantError)
			if test.wantError {
				require.Equal(t, "invalid_request", callToolText(t, result))
			}
			require.Len(t, app.snapshot(), test.wantCalls)
		})
	}
}

func TestConnectAggregateToolsMapUnconfiguredConnectSafely(t *testing.T) {
	tests := []struct {
		name      string
		operation string
	}{
		{name: "getConnects", operation: "listConnects"},
		{name: "getConnectsCsv", operation: "listConnects"},
		{name: "getAllConnectors", operation: "allConnectors"},
		{name: "getAllConnectorsCsv", operation: "allConnectors"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newRecordingConnectApp()
			app.errs[test.operation] = fmt.Errorf(
				"%w: https://admin:do-not-return@worker.internal",
				appcluster.ErrUnknownConnect,
			)
			executor, _ := newConnectExecutor(t, app, false, false)
			result := callConnectTool(t, executor, test.name, map[string]any{
				"clusterName": "prod",
			}, true)
			require.Equal(t, "not_found", callToolText(t, result))
			require.NotContains(t, callToolText(t, result), "do-not-return")
			require.NotContains(t, callToolText(t, result), "worker.internal")
		})
	}
}

func TestConnectCSVCellNeutralizesEveryHandledFormulaPrefix(t *testing.T) {
	for _, prefix := range []string{"=", "+", "-", "@", "\t", "\r"} {
		t.Run(fmt.Sprintf("%q", prefix), func(t *testing.T) {
			require.Equal(t, "'"+prefix+"payload", connectCSVCell(prefix+"payload"))
		})
	}
	require.Equal(t, "safe", connectCSVCell("safe"))
	require.Empty(t, connectCSVCell(""))
}

func TestConnectToolsDoNotExposeRoutingOrExecutionInputs(t *testing.T) {
	for _, name := range []string{
		"getConnects", "getConnectsCsv", "getConnectors", "getConnector",
		"getAllConnectors", "getAllConnectorsCsv", "getConnectorConfig",
		"getConnectorTasks", "getConnectorPlugins", "validateConnectorPluginConfig",
		"createConnector", "deleteConnector", "setConnectorConfig",
		"updateConnectorState", "restartConnectorTask", "resetConnectorOffsets",
	} {
		t.Run(name, func(t *testing.T) {
			spec := requireCatalogSpec(t, name)
			executor, _ := newConnectExecutor(t, newRecordingConnectApp(), false, true)
			session := newSDKSession(t, spec, executor)
			listed, err := session.ListTools(context.Background(), nil)
			require.NoError(t, err)
			tool := findTool(t, listed.Tools, name)
			schema := fmt.Sprint(tool.InputSchema)
			for _, forbidden := range []string{"command", "executable", "environment", "connectUrl", "address", "filePath"} {
				require.NotContains(t, schema, forbidden)
			}
		})
	}
}

func newConnectExecutor(
	t *testing.T,
	app ConnectServicer,
	readOnly bool,
	allowWrites bool,
) (*Executor, *mcppolicy.Store) {
	t.Helper()
	store := mcppolicy.NewStore(filepath.Join(t.TempDir(), "mcp-policy.json"))
	require.NoError(t, store.Save(context.Background(), *policy(true, allowWrites)))
	executor, err := NewExecutor(Dependencies{
		Connects: app,
		Policy:   store,
		IsReadOnly: func(string) bool {
			return readOnly
		},
	})
	require.NoError(t, err)
	return executor, store
}

func callConnectTool(
	t *testing.T,
	executor *Executor,
	name string,
	input map[string]any,
	wantError bool,
) *mcp.CallToolResult {
	t.Helper()
	result, err := newSDKSession(t, requireCatalogSpec(t, name), executor).CallTool(
		context.Background(), &mcp.CallToolParams{Name: name, Arguments: input},
	)
	require.NoError(t, err)
	require.Equal(t, wantError, result.IsError, callToolText(t, result))
	return result
}

func jsonUnmarshalResult(result *mcp.CallToolResult, target any) error {
	return json.Unmarshal([]byte(callToolTextWithoutTest(result)), target)
}

func callToolTextWithoutTest(result *mcp.CallToolResult) string {
	if result == nil || len(result.Content) != 1 {
		return ""
	}
	text, _ := result.Content[0].(*mcp.TextContent)
	if text == nil {
		return ""
	}
	return strings.TrimSpace(text.Text)
}

func requireConnectNestedAliasesRedacted(
	t *testing.T,
	result *mcp.CallToolResult,
	field string,
) {
	t.Helper()
	for _, structured := range []any{
		json.RawMessage(callToolText(t, result)),
		result.StructuredContent,
	} {
		var payload map[string]any
		require.NoError(t, remarshalConnectStructured(structured, &payload))
		config, ok := payload[field].(map[string]any)
		require.True(t, ok, "field %q has type %T", field, payload[field])
		requireConnectAliasConfig(t, config)
	}
}

func requireConnectTaskAliasesRedacted(t *testing.T, result *mcp.CallToolResult) {
	t.Helper()
	for _, structured := range []any{
		json.RawMessage(callToolText(t, result)),
		result.StructuredContent,
	} {
		var payload map[string]any
		require.NoError(t, remarshalConnectStructured(structured, &payload))
		items, ok := payload["result"].([]any)
		require.True(t, ok)
		require.Len(t, items, 1)
		task, ok := items[0].(map[string]any)
		require.True(t, ok)
		config, ok := task["config"].(map[string]any)
		require.True(t, ok)
		requireConnectAliasConfig(t, config)
	}
}

func requireConnectAliasConfig(t *testing.T, config map[string]any) {
	t.Helper()
	for _, alias := range []string{
		"basic.auth.user.info", "api.key", "api_key", "apikey", "authorization",
	} {
		require.Equal(t, redactedValue, config[alias], alias)
	}
	require.Equal(t, true, config["enabled"])
	require.Equal(t, float64(3), config["retries"])
}

func connectAddressFromStructured(t *testing.T, structured any) string {
	t.Helper()
	var payload struct {
		Result []struct {
			Address string `json:"address"`
		} `json:"result"`
	}
	require.NoError(t, remarshalConnectStructured(structured, &payload))
	require.Len(t, payload.Result, 1)
	return payload.Result[0].Address
}

func remarshalConnectStructured(input any, target any) error {
	switch typed := input.(type) {
	case json.RawMessage:
		return json.Unmarshal(typed, target)
	case []byte:
		return json.Unmarshal(typed, target)
	default:
		raw, err := json.Marshal(input)
		if err != nil {
			return err
		}
		return json.Unmarshal(raw, target)
	}
}

func requireConnectTopLevelResultLength(
	want int,
) func(*testing.T, *mcp.CallToolResult) {
	return func(t *testing.T, result *mcp.CallToolResult) {
		t.Helper()
		var payload map[string]any
		require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &payload))
		items, ok := payload["result"].([]any)
		require.True(t, ok)
		require.Len(t, items, want)
	}
}

func requireConnectNestedResultLength(
	field string,
	want int,
) func(*testing.T, *mcp.CallToolResult) {
	return func(t *testing.T, result *mcp.CallToolResult) {
		t.Helper()
		var payload map[string]any
		require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &payload))
		items, ok := payload[field].([]any)
		require.True(t, ok, "field %q has type %T", field, payload[field])
		require.Len(t, items, want)
	}
}

func cloneConnectConfig(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func cloneConnector(input domaincluster.Connector) domaincluster.Connector {
	input.Config = cloneConnectConfig(input.Config)
	input.TaskIDs = append([]int(nil), input.TaskIDs...)
	input.Topics = append([]string(nil), input.Topics...)
	return input
}

func cloneConnectorTasks(input []domaincluster.ConnectorTask) []domaincluster.ConnectorTask {
	output := append([]domaincluster.ConnectorTask(nil), input...)
	for index := range output {
		output[index].Config = cloneConnectConfig(output[index].Config)
	}
	return output
}

var _ ConnectServicer = (*recordingConnectApp)(nil)
