package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	domaincluster "github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/mcppolicy"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

type recordingClusterBrokerApp struct {
	mu sync.Mutex

	snapshots []domaincluster.Snapshot
	state     domaincluster.RuntimeState
	stateOK   bool
	dirs      []domaincluster.BrokerLogDirs
	configs   []domaincluster.ConfigEntry

	refreshErr error
	logDirsErr error
	configsErr error
	alterErr   error
	moveErr    error

	calls []string
}

func (f *recordingClusterBrokerApp) List(context.Context) []domaincluster.Snapshot {
	f.record("states.list")
	return f.snapshots
}

func (f *recordingClusterBrokerApp) Get(_ context.Context, clusterName string) (domaincluster.RuntimeState, bool) {
	f.record("states.get(" + clusterName + ")")
	return f.state, f.stateOK
}

func (f *recordingClusterBrokerApp) Refresh(_ context.Context, clusterName string) (domaincluster.RuntimeState, error) {
	f.record("states.refresh(" + clusterName + ")")
	return f.state, f.refreshErr
}

func (f *recordingClusterBrokerApp) LogDirs(_ context.Context, clusterName string, brokers []int32) ([]domaincluster.BrokerLogDirs, error) {
	f.record(fmt.Sprintf("brokers.logdirs(%s,%v)", clusterName, brokers))
	return f.dirs, f.logDirsErr
}

func (f *recordingClusterBrokerApp) BrokerConfigs(_ context.Context, clusterName string, id int32) ([]domaincluster.ConfigEntry, error) {
	f.record(fmt.Sprintf("brokers.configs(%s,%d)", clusterName, id))
	return f.configs, f.configsErr
}

func (f *recordingClusterBrokerApp) AlterBrokerConfig(_ context.Context, clusterName string, id int32, name, value string) error {
	f.record(fmt.Sprintf("brokers.alter(%s,%d,%s,%s)", clusterName, id, name, value))
	return f.alterErr
}

func (f *recordingClusterBrokerApp) MoveReplicaLogDir(_ context.Context, clusterName string, id int32, topic string, partition int32, logDir string) error {
	f.record(fmt.Sprintf("brokers.move(%s,%d,%s,%d,%s)", clusterName, id, topic, partition, logDir))
	return f.moveErr
}

func TestGetAllBrokerLogDirsRejectsAggregateRawOverflowBeforeDTOAdaptation(t *testing.T) {
	app := newRecordingClusterBrokerApp()
	first := domaincluster.BrokerLogDirs{Broker: 1, Dir: "/data-1"}
	second := domaincluster.BrokerLogDirs{Broker: 2, Dir: "/data-2"}
	for index := 0; index <= maxBrokerLogDirTopicGroups/2; index++ {
		first.Topics = append(first.Topics, domaincluster.TopicLogDirs{
			Topic: fmt.Sprintf("first-%04d", index),
		})
		second.Topics = append(second.Topics, domaincluster.TopicLogDirs{
			Topic: fmt.Sprintf("second-%04d", index),
		})
	}
	app.dirs = []domaincluster.BrokerLogDirs{first, second}
	executor, _ := newClusterBrokerExecutor(t, app, &recordingReadOnlyResolver{}, false)

	result, err := newSDKSession(
		t,
		requireCatalogSpec(t, "getAllBrokersLogdirs"),
		executor,
	).CallTool(
		context.Background(),
		&mcp.CallToolParams{
			Name:      "getAllBrokersLogdirs",
			Arguments: map[string]any{"clusterName": "prod"},
		},
	)
	require.NoError(t, err)
	require.True(t, result.IsError)
	require.Equal(t, "result_too_large", callToolText(t, result))
}

func TestValidateBrokerLogDirsResultHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := validateBrokerLogDirsResult(
		ctx,
		[]domaincluster.BrokerLogDirs{{Broker: 1, Dir: "/data"}},
	)
	require.ErrorIs(t, err, context.Canceled)
}

func (f *recordingClusterBrokerApp) record(call string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call)
}

func (f *recordingClusterBrokerApp) recordedCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

type recordingReadOnlyResolver struct {
	mu       sync.Mutex
	readOnly bool
	clusters []string
}

func (r *recordingReadOnlyResolver) Resolve(clusterName string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clusters = append(r.clusters, clusterName)
	return r.readOnly
}

func (r *recordingReadOnlyResolver) recordedClusters() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.clusters...)
}

func TestClusterToolsDelegateAllElevenClusterAndBrokerOperationsThroughApplicationPorts(t *testing.T) {
	tests := []struct {
		name       string
		input      map[string]any
		wantCall   string
		wantAccess AccessClass
	}{
		{
			name:       "getClusters",
			input:      map[string]any{},
			wantCall:   "states.list",
			wantAccess: AccessReadOnly,
		},
		{
			name:       "getClusterMetrics",
			input:      map[string]any{"clusterName": "prod"},
			wantCall:   "states.get(prod)",
			wantAccess: AccessReadOnly,
		},
		{
			name:       "getClusterStats",
			input:      map[string]any{"clusterName": "prod"},
			wantCall:   "states.get(prod)",
			wantAccess: AccessReadOnly,
		},
		{
			name:       "updateClusterInfo",
			input:      map[string]any{"clusterName": "prod"},
			wantCall:   "states.refresh(prod)",
			wantAccess: AccessReadOnly,
		},
		{
			name:       "getBrokers",
			input:      map[string]any{"clusterName": "prod"},
			wantCall:   "states.get(prod)",
			wantAccess: AccessReadOnly,
		},
		{
			name:       "getBrokersCsv",
			input:      map[string]any{"clusterName": "prod"},
			wantCall:   "states.get(prod)",
			wantAccess: AccessReadOnly,
		},
		{
			name:       "getBrokersMetrics",
			input:      map[string]any{"clusterName": "prod", "id": float64(2)},
			wantCall:   "states.get(prod)",
			wantAccess: AccessReadOnly,
		},
		{
			name: "getAllBrokersLogdirs",
			input: map[string]any{
				"clusterName": "prod",
				"query":       map[string]any{"broker": []any{float64(2)}},
			},
			wantCall:   "brokers.logdirs(prod,[2])",
			wantAccess: AccessReadOnly,
		},
		{
			name:       "getBrokerConfig",
			input:      map[string]any{"clusterName": "prod", "id": float64(2)},
			wantCall:   "brokers.configs(prod,2)",
			wantAccess: AccessReadOnly,
		},
		{
			name: "updateBrokerTopicPartitionLogDir",
			input: map[string]any{
				"clusterName": "prod",
				"id":          float64(2),
				"topic":       "orders",
				"partition":   float64(3),
				"logDir":      "/data/kafka-2",
			},
			wantCall:   "brokers.move(prod,2,orders,3,/data/kafka-2)",
			wantAccess: AccessWrite,
		},
		{
			name: "updateBrokerConfigByName",
			input: map[string]any{
				"clusterName": "prod",
				"id":          float64(2),
				"name":        "compression.type",
				"value":       "producer",
			},
			wantCall:   "brokers.alter(prod,2,compression.type,producer)",
			wantAccess: AccessWrite,
		},
	}
	require.Len(t, tests, 11)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := newRecordingClusterBrokerApp()
			resolver := &recordingReadOnlyResolver{}
			executor, _ := newClusterBrokerExecutor(t, app, resolver, true)
			spec := requireCatalogSpec(t, tt.name)
			require.Equal(t, tt.wantAccess, spec.Meta.Access)

			result, err := newSDKSession(t, spec, executor).CallTool(
				context.Background(),
				&mcp.CallToolParams{Name: tt.name, Arguments: tt.input},
			)
			require.NoError(t, err)
			require.False(t, result.IsError, callToolText(t, result))
			require.Equal(t, []string{tt.wantCall}, app.recordedCalls())
			if tt.wantAccess == AccessWrite {
				require.Equal(t, []string{"prod"}, resolver.recordedClusters())
			} else {
				require.Empty(t, resolver.recordedClusters())
			}
		})
	}
}

func TestClusterToolsKeepCacheRefreshVisibleUnderReadOnlyPolicy(t *testing.T) {
	app := newRecordingClusterBrokerApp()
	resolver := &recordingReadOnlyResolver{}
	executor, store := newClusterBrokerExecutor(t, app, resolver, false)
	session := newSDKCatalogSession(t, VisibleCatalog(*mustLoadPolicy(t, store)), executor)

	listed, err := session.ListTools(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, listed.Tools, 51)
	require.NotNil(t, findTool(t, listed.Tools, "updateClusterInfo"))
	require.Nil(t, findToolOrNil(listed.Tools, "updateBrokerTopicPartitionLogDir"))
	require.Nil(t, findToolOrNil(listed.Tools, "updateBrokerConfigByName"))

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "updateClusterInfo",
		Arguments: map[string]any{"clusterName": "prod"},
	})
	require.NoError(t, err)
	require.False(t, result.IsError, callToolText(t, result))
	require.Equal(t, []string{"states.refresh(prod)"}, app.recordedCalls())
	require.Empty(t, resolver.recordedClusters())
}

func TestBrokerToolsCachedWriteIsDeniedAfterPolicyDowngrade(t *testing.T) {
	app := newRecordingClusterBrokerApp()
	resolver := &recordingReadOnlyResolver{}
	executor, store := newClusterBrokerExecutor(t, app, resolver, true)
	session := newSDKSession(t, requireCatalogSpec(t, "updateBrokerConfigByName"), executor)
	params := &mcp.CallToolParams{
		Name: "updateBrokerConfigByName",
		Arguments: map[string]any{
			"clusterName": "prod",
			"id":          float64(2),
			"name":        "compression.type",
			"value":       "producer",
		},
	}

	result, err := session.CallTool(context.Background(), params)
	require.NoError(t, err)
	require.False(t, result.IsError, callToolText(t, result))
	require.Equal(t, []string{"brokers.alter(prod,2,compression.type,producer)"}, app.recordedCalls())

	require.NoError(t, store.Save(context.Background(), *policy(true, false)))
	result, err = session.CallTool(context.Background(), params)
	require.NoError(t, err)
	require.True(t, result.IsError)
	require.Equal(t, "writes_disabled", callToolText(t, result))
	require.Equal(t, []string{"brokers.alter(prod,2,compression.type,producer)"}, app.recordedCalls())
}

func TestBrokerToolsBothWritesUseClusterSelectorAndReadOnlyGate(t *testing.T) {
	tests := []struct {
		name  string
		input map[string]any
	}{
		{
			name: "updateBrokerConfigByName",
			input: map[string]any{
				"clusterName": "prod",
				"id":          float64(2),
				"name":        "compression.type",
				"value":       "producer",
			},
		},
		{
			name: "updateBrokerTopicPartitionLogDir",
			input: map[string]any{
				"clusterName": "prod",
				"id":          float64(2),
				"topic":       "orders",
				"partition":   float64(3),
				"logDir":      "/data/kafka-2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := newRecordingClusterBrokerApp()
			resolver := &recordingReadOnlyResolver{readOnly: true}
			executor, _ := newClusterBrokerExecutor(t, app, resolver, true)

			result, err := newSDKSession(t, requireCatalogSpec(t, tt.name), executor).CallTool(
				context.Background(),
				&mcp.CallToolParams{Name: tt.name, Arguments: tt.input},
			)
			require.NoError(t, err)
			require.True(t, result.IsError)
			require.Equal(t, "cluster_read_only", callToolText(t, result))
			require.Empty(t, app.recordedCalls())
			require.Equal(t, []string{"prod"}, resolver.recordedClusters())
		})
	}
}

func TestBrokerToolsConfigRecursivelyRedactsCredentialValuesOnly(t *testing.T) {
	app := newRecordingClusterBrokerApp()
	app.configs = []domaincluster.ConfigEntry{
		{
			Name:        "sasl.jaas.config",
			Value:       "username=admin password=credential-marker",
			Source:      "DYNAMIC_BROKER_CONFIG",
			IsSensitive: true,
			Synonyms: []domaincluster.ConfigSynonym{
				{Name: "ssl.keystore.password", Value: "nested-credential", Source: "STATIC_BROKER_CONFIG"},
			},
		},
		{
			Name:   "monkey",
			Value:  "ordinary-key-field",
			Source: "DEFAULT_CONFIG",
			Synonyms: []domaincluster.ConfigSynonym{
				{Name: "ssl.keystore.password", Value: "nested-ordinary-parent-credential", Source: "STATIC_BROKER_CONFIG"},
				{Name: "message.key.field", Value: "order-42", Source: "DEFAULT_CONFIG"},
			},
		},
	}
	executor, _ := newClusterBrokerExecutor(t, app, &recordingReadOnlyResolver{}, false)

	result, err := newSDKSession(t, requireCatalogSpec(t, "getBrokerConfig"), executor).CallTool(
		context.Background(),
		&mcp.CallToolParams{
			Name:      "getBrokerConfig",
			Arguments: map[string]any{"clusterName": "prod", "id": float64(2)},
		},
	)
	require.NoError(t, err)
	require.False(t, result.IsError, callToolText(t, result))
	require.NotContains(t, callToolText(t, result), "credential-marker")
	require.NotContains(t, callToolText(t, result), "nested-credential")
	require.NotContains(t, callToolText(t, result), "nested-ordinary-parent-credential")

	var envelope struct {
		Result []struct {
			Name     string `json:"name"`
			Value    string `json:"value"`
			Synonyms []struct {
				Name  string `json:"name"`
				Value string `json:"value"`
			} `json:"synonyms"`
		} `json:"result"`
	}
	require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &envelope))
	require.Len(t, envelope.Result, 2)
	require.Equal(t, "sasl.jaas.config", envelope.Result[0].Name)
	require.Equal(t, redactedValue, envelope.Result[0].Value)
	require.Equal(t, redactedValue, envelope.Result[0].Synonyms[0].Value)
	require.Equal(t, "monkey", envelope.Result[1].Name)
	require.Equal(t, "ordinary-key-field", envelope.Result[1].Value)
	require.Equal(t, redactedValue, envelope.Result[1].Synonyms[0].Value)
	require.Equal(t, "order-42", envelope.Result[1].Synonyms[1].Value)
}

func TestBrokerToolsCsvHasFrozenHeaderSortedIDsAndOneMiBBound(t *testing.T) {
	app := newRecordingClusterBrokerApp()
	executor, _ := newClusterBrokerExecutor(t, app, &recordingReadOnlyResolver{}, false)
	spec := requireCatalogSpec(t, "getBrokersCsv")
	session := newSDKSession(t, spec, executor)
	params := &mcp.CallToolParams{
		Name:      "getBrokersCsv",
		Arguments: map[string]any{"clusterName": "prod"},
	}

	first, err := session.CallTool(context.Background(), params)
	require.NoError(t, err)
	require.False(t, first.IsError, callToolText(t, first))
	second, err := session.CallTool(context.Background(), params)
	require.NoError(t, err)
	require.False(t, second.IsError, callToolText(t, second))

	var firstEnvelope, secondEnvelope struct {
		Result string `json:"result"`
	}
	require.NoError(t, json.Unmarshal([]byte(callToolText(t, first)), &firstEnvelope))
	require.NoError(t, json.Unmarshal([]byte(callToolText(t, second)), &secondEnvelope))
	require.Equal(t, firstEnvelope.Result, secondEnvelope.Result)
	require.Equal(t, ""+
		"bytesInPerSec,bytesOutPerSec,host,id,inSyncPartitions,leadersSkew,partitions,partitionsLeader,partitionsSkew,port\n"+
		",,broker-one,1,8,,9,4,,9092\n"+
		",,\"broker,two\",2,7,,10,5,,9093\n",
		firstEnvelope.Result,
	)

	oversized := newRecordingClusterBrokerApp()
	oversized.state.Brokers[0].Host = strings.Repeat("x", maxCSVBytes)
	oversizedExecutor, _ := newClusterBrokerExecutor(t, oversized, &recordingReadOnlyResolver{}, false)
	tooLarge, err := newSDKSession(t, spec, oversizedExecutor).CallTool(context.Background(), params)
	require.NoError(t, err)
	require.True(t, tooLarge.IsError)
	require.Equal(t, "result_too_large", callToolText(t, tooLarge))

	bounded := newRecordingClusterBrokerApp()
	bounded.state.Brokers = make([]domaincluster.BrokerInfo, maxListItems+1)
	for index := range bounded.state.Brokers {
		bounded.state.Brokers[index] = domaincluster.BrokerInfo{ID: int32(maxListItems - index)}
	}
	boundedExecutor, _ := newClusterBrokerExecutor(t, bounded, &recordingReadOnlyResolver{}, false)
	boundedResult, err := newSDKSession(t, spec, boundedExecutor).CallTool(context.Background(), params)
	require.NoError(t, err)
	require.False(t, boundedResult.IsError, callToolText(t, boundedResult))
	var boundedEnvelope struct {
		Result string `json:"result"`
	}
	require.NoError(t, json.Unmarshal([]byte(callToolText(t, boundedResult)), &boundedEnvelope))
	require.Len(t, strings.Split(strings.TrimSpace(boundedEnvelope.Result), "\n"), maxListItems+1)
}

func TestClusterToolsUnknownCachedClusterReturnsStableNotFound(t *testing.T) {
	app := newRecordingClusterBrokerApp()
	app.stateOK = false
	executor, _ := newClusterBrokerExecutor(t, app, &recordingReadOnlyResolver{}, false)

	result, err := newSDKSession(t, requireCatalogSpec(t, "getClusterStats"), executor).CallTool(
		context.Background(),
		&mcp.CallToolParams{
			Name:      "getClusterStats",
			Arguments: map[string]any{"clusterName": "missing"},
		},
	)
	require.NoError(t, err)
	require.True(t, result.IsError)
	require.Equal(t, "cluster_not_found", callToolText(t, result))
	require.Equal(t, []string{"states.get(missing)"}, app.recordedCalls())
}

func TestBrokerToolsRejectInvalidAndUnboundedInputsBeforeServiceCalls(t *testing.T) {
	tests := []struct {
		name  string
		input map[string]any
	}{
		{
			name:  "getBrokers empty cluster",
			input: map[string]any{"clusterName": ""},
		},
		{
			name:  "getBrokersMetrics negative broker",
			input: map[string]any{"clusterName": "prod", "id": float64(-1)},
		},
		{
			name: "getAllBrokersLogdirs negative broker",
			input: map[string]any{
				"clusterName": "prod",
				"query":       map[string]any{"broker": []any{float64(-1)}},
			},
		},
		{
			name: "getAllBrokersLogdirs unbounded broker list",
			input: map[string]any{
				"clusterName": "prod",
				"query":       map[string]any{"broker": int32Values(maxListItems + 1)},
			},
		},
		{
			name: "updateBrokerConfigByName empty config name",
			input: map[string]any{
				"clusterName": "prod",
				"id":          float64(2),
				"name":        " ",
				"value":       "producer",
			},
		},
		{
			name: "updateBrokerConfigByName negative broker",
			input: map[string]any{
				"clusterName": "prod",
				"id":          float64(-1),
				"name":        "compression.type",
				"value":       "producer",
			},
		},
		{
			name: "updateBrokerConfigByName unbounded value",
			input: map[string]any{
				"clusterName": "prod",
				"id":          float64(2),
				"name":        "compression.type",
				"value":       strings.Repeat("x", maxResultBytes+1),
			},
		},
		{
			name: "updateBrokerTopicPartitionLogDir negative partition",
			input: map[string]any{
				"clusterName": "prod",
				"id":          float64(2),
				"topic":       "orders",
				"partition":   float64(-1),
				"logDir":      "/data/kafka-2",
			},
		},
		{
			name: "updateBrokerTopicPartitionLogDir negative broker",
			input: map[string]any{
				"clusterName": "prod",
				"id":          float64(-1),
				"topic":       "orders",
				"partition":   float64(3),
				"logDir":      "/data/kafka-2",
			},
		},
		{
			name: "updateBrokerTopicPartitionLogDir empty topic",
			input: map[string]any{
				"clusterName": "prod",
				"id":          float64(2),
				"topic":       "",
				"partition":   float64(3),
				"logDir":      "/data/kafka-2",
			},
		},
		{
			name: "updateBrokerTopicPartitionLogDir empty log dir",
			input: map[string]any{
				"clusterName": "prod",
				"id":          float64(2),
				"topic":       "orders",
				"partition":   float64(3),
				"logDir":      " ",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			toolName := strings.Split(tt.name, " ")[0]
			app := newRecordingClusterBrokerApp()
			executor, _ := newClusterBrokerExecutor(t, app, &recordingReadOnlyResolver{}, true)

			result, err := newSDKSession(t, requireCatalogSpec(t, toolName), executor).CallTool(
				context.Background(),
				&mcp.CallToolParams{Name: toolName, Arguments: tt.input},
			)
			require.NoError(t, err)
			require.True(t, result.IsError)
			require.Equal(t, "invalid_request", callToolText(t, result))
			require.Empty(t, app.recordedCalls())
		})
	}
}

func TestBrokerToolsAuthorizeBeforeValidatingWriteInput(t *testing.T) {
	app := newRecordingClusterBrokerApp()
	resolver := &recordingReadOnlyResolver{}
	executor, store := newClusterBrokerExecutor(t, app, resolver, true)
	session := newSDKSession(t, requireCatalogSpec(t, "updateBrokerTopicPartitionLogDir"), executor)
	require.NoError(t, store.Save(context.Background(), *policy(false, false)))

	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "updateBrokerTopicPartitionLogDir",
		Arguments: map[string]any{
			"clusterName": "prod",
			"id":          float64(-1),
			"topic":       "",
			"partition":   float64(-1),
			"logDir":      "",
		},
	})
	require.NoError(t, err)
	require.True(t, result.IsError)
	require.Equal(t, "policy_disabled", callToolText(t, result))
	require.Empty(t, app.recordedCalls())
	require.Empty(t, resolver.recordedClusters())
}

func TestBrokerToolsExposeFlatWriteInputSchemas(t *testing.T) {
	tools := listSDKCatalogTools(t)

	configSchema := requireSchemaObject(t, findTool(t, tools, "updateBrokerConfigByName").InputSchema)
	require.ElementsMatch(t,
		[]string{"clusterName", "id", "name", "value"},
		schemaPropertyNames(t, configSchema),
	)
	require.ElementsMatch(t,
		[]string{"clusterName", "id", "name", "value"},
		schemaRequired(t, configSchema),
	)

	moveSchema := requireSchemaObject(t, findTool(t, tools, "updateBrokerTopicPartitionLogDir").InputSchema)
	require.ElementsMatch(t,
		[]string{"clusterName", "id", "topic", "partition", "logDir"},
		schemaPropertyNames(t, moveSchema),
	)
	require.ElementsMatch(t,
		[]string{"clusterName", "id", "topic", "partition", "logDir"},
		schemaRequired(t, moveSchema),
	)
}

func TestClusterToolsAndBrokerToolsReturnHTTPCompatibleSDKResultFields(t *testing.T) {
	tests := []struct {
		name  string
		input map[string]any
		setup func(*recordingClusterBrokerApp)
		want  string
	}{
		{
			name:  "getClusters",
			input: map[string]any{},
			want: `{"result":[{
				"name":"prod",
				"readOnly":false,
				"status":"ONLINE",
				"brokerCount":2,
				"features":["KAFKA_ACL_VIEW","KAFKA_ACL_EDIT","TOPIC_DELETION"]
			}]}`,
		},
		{
			name:  "getClusterMetrics",
			input: map[string]any{"clusterName": "prod"},
			want: `{"items":[
				{"name":"broker_count","value":2},
				{"name":"topic_count","value":3},
				{"name":"kafka_topic_partitions","value":12,"labels":{"status":"online"}},
				{"name":"kafka_topic_partitions","value":1,"labels":{"status":"offline"}},
				{"name":"broker_bytes_disk","value":2048,"labels":{"broker":"2"}},
				{"name":"broker_bytes_disk","value":1024,"labels":{"broker":"1"}}
			]}`,
		},
		{
			name:  "getClusterStats",
			input: map[string]any{"clusterName": "prod"},
			setup: func(app *recordingClusterBrokerApp) {
				app.state.Controller = 7
			},
			want: `{
				"activeControllers":1,
				"brokerCount":2,
				"onlinePartitionCount":12,
				"offlinePartitionCount":1,
				"underReplicatedPartitionCount":2,
				"inSyncReplicasCount":20,
				"outOfSyncReplicasCount":3,
				"diskUsage":[
					{"brokerId":2,"segmentSize":2048,"segmentCount":20},
					{"brokerId":1,"segmentSize":1024,"segmentCount":10}
				],
				"version":"3.9.0"
			}`,
		},
		{
			name:  "getBrokers",
			input: map[string]any{"clusterName": "prod"},
			want: `{"result":[
				{"id":2,"host":"broker,two","port":9093,"partitionsLeader":5,"partitions":10,"inSyncPartitions":7},
				{"id":1,"host":"broker-one","port":9092,"partitionsLeader":4,"partitions":9,"inSyncPartitions":8}
			]}`,
		},
		{
			name:  "getBrokersMetrics",
			input: map[string]any{"clusterName": "prod", "id": float64(2)},
			want: `{
				"segmentCount":20,
				"segmentSize":2048,
				"metrics":[
					{"name":"partitions_leader","value":5},
					{"name":"partitions","value":10},
					{"name":"in_sync_partitions","value":7}
				]
			}`,
		},
		{
			name: "getAllBrokersLogdirs",
			input: map[string]any{
				"clusterName": "prod",
				"query":       map[string]any{"broker": []any{float64(2)}},
			},
			want: `{"result":[{
				"name":"/data/kafka-2",
				"topics":[{
					"name":"orders",
					"partitions":[{"broker":2,"partition":3,"size":42,"offsetLag":0}]
				}]
			}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := newRecordingClusterBrokerApp()
			if tt.setup != nil {
				tt.setup(app)
			}
			executor, _ := newClusterBrokerExecutor(t, app, &recordingReadOnlyResolver{}, false)
			result, err := newSDKSession(t, requireCatalogSpec(t, tt.name), executor).CallTool(
				context.Background(),
				&mcp.CallToolParams{Name: tt.name, Arguments: tt.input},
			)
			require.NoError(t, err)
			require.False(t, result.IsError, callToolText(t, result))
			require.JSONEq(t, tt.want, callToolText(t, result))
			requireStructuredJSONEq(t, tt.want, result.StructuredContent)
		})
	}
}

func TestClusterToolsClusterStatsCountsControllerPresence(t *testing.T) {
	tests := []struct {
		name       string
		controller int32
		want       int32
	}{
		{name: "broker ID seven is one active controller", controller: 7, want: 1},
		{name: "unknown controller is zero active controllers", controller: -1, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := newRecordingClusterBrokerApp()
			app.state.Controller = tt.controller
			executor, _ := newClusterBrokerExecutor(t, app, &recordingReadOnlyResolver{}, false)
			result, err := newSDKSession(t, requireCatalogSpec(t, "getClusterStats"), executor).CallTool(
				context.Background(),
				&mcp.CallToolParams{
					Name:      "getClusterStats",
					Arguments: map[string]any{"clusterName": "prod"},
				},
			)
			require.NoError(t, err)
			require.False(t, result.IsError, callToolText(t, result))
			var stats generated.ClusterStats
			require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &stats))
			require.NotNil(t, stats.ActiveControllers)
			require.Equal(t, tt.want, *stats.ActiveControllers)
		})
	}
}

func TestBrokerToolsTranslateRealKafkaClientMetricsConfigSource(t *testing.T) {
	app := newRecordingClusterBrokerApp()
	app.configs = []domaincluster.ConfigEntry{{
		Name:   "client.metrics.sample.window.ms",
		Value:  "30000",
		Source: "CLIENT_METRICS_CONFIG",
		Synonyms: []domaincluster.ConfigSynonym{{
			Name:   "client.metrics.sample.window.ms",
			Value:  "60000",
			Source: "CLIENT_METRICS_CONFIG",
		}},
	}}
	executor, _ := newClusterBrokerExecutor(t, app, &recordingReadOnlyResolver{}, false)

	result, err := newSDKSession(t, requireCatalogSpec(t, "getBrokerConfig"), executor).CallTool(
		context.Background(),
		&mcp.CallToolParams{
			Name:      "getBrokerConfig",
			Arguments: map[string]any{"clusterName": "prod", "id": float64(2)},
		},
	)
	require.NoError(t, err)
	require.False(t, result.IsError, callToolText(t, result))
	var envelope struct {
		Result []generated.BrokerConfig `json:"result"`
	}
	require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &envelope))
	require.Len(t, envelope.Result, 1)
	require.Equal(t, generated.ConfigSourceDYNAMICCLIENTMETRICSCONFIG, envelope.Result[0].Source)
	require.NotNil(t, envelope.Result[0].Synonyms)
	require.Len(t, *envelope.Result[0].Synonyms, 1)
	require.NotNil(t, (*envelope.Result[0].Synonyms)[0].Source)
	require.Equal(t,
		generated.ConfigSourceDYNAMICCLIENTMETRICSCONFIG,
		*(*envelope.Result[0].Synonyms)[0].Source,
	)
}

func TestClusterToolsAndBrokerToolsClampCacheCountsToInt32ContractBounds(t *testing.T) {
	if math.MaxInt == math.MaxInt32 {
		t.Skip("int cannot represent a value above int32 on this architecture")
	}
	overflow := int(int64(math.MaxInt32) + 1)
	app := newRecordingClusterBrokerApp()
	app.snapshots = []domaincluster.Snapshot{
		{
			Definition:  domaincluster.Definition{Name: "negative"},
			Status:      domaincluster.StatusOnline,
			BrokerCount: -1,
		},
		{
			Definition:  domaincluster.Definition{Name: "overflow"},
			Status:      domaincluster.StatusOnline,
			BrokerCount: overflow,
		},
	}
	app.state.Controller = -1
	app.state.Partitions = domaincluster.PartitionCounts{
		Online:          -1,
		Offline:         overflow,
		UnderReplicated: -1,
		InSync:          overflow,
		OutOfSync:       -1,
	}
	app.state.Disk[0].SegmentCount = overflow
	app.state.Disk[1].SegmentCount = -1
	app.state.Brokers[0].PartitionsLeader = -1
	app.state.Brokers[0].Partitions = overflow
	app.state.Brokers[0].InSyncPartitions = -1

	t.Run("cluster list broker counts", func(t *testing.T) {
		result := callClusterBrokerTool(t, app, "getClusters", map[string]any{}, false)
		var envelope struct {
			Result []generated.Cluster `json:"result"`
		}
		require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &envelope))
		require.Len(t, envelope.Result, 2)
		require.Equal(t, int32(0), *envelope.Result[0].BrokerCount)
		require.Equal(t, int32(math.MaxInt32), *envelope.Result[1].BrokerCount)
	})

	t.Run("cluster stats counts", func(t *testing.T) {
		result := callClusterBrokerTool(t, app, "getClusterStats", map[string]any{"clusterName": "prod"}, false)
		var stats generated.ClusterStats
		require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &stats))
		require.Equal(t, int32(0), *stats.OnlinePartitionCount)
		require.Equal(t, int32(math.MaxInt32), *stats.OfflinePartitionCount)
		require.Equal(t, int32(0), *stats.UnderReplicatedPartitionCount)
		require.Equal(t, int32(math.MaxInt32), *stats.InSyncReplicasCount)
		require.Equal(t, int32(0), *stats.OutOfSyncReplicasCount)
		require.Equal(t, int32(math.MaxInt32), *(*stats.DiskUsage)[0].SegmentCount)
		require.Equal(t, int32(0), *(*stats.DiskUsage)[1].SegmentCount)
	})

	t.Run("broker list counts", func(t *testing.T) {
		result := callClusterBrokerTool(t, app, "getBrokers", map[string]any{"clusterName": "prod"}, false)
		var envelope struct {
			Result []generated.Broker `json:"result"`
		}
		require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &envelope))
		require.Len(t, envelope.Result, 2)
		require.Equal(t, int32(0), *envelope.Result[0].PartitionsLeader)
		require.Equal(t, int32(math.MaxInt32), *envelope.Result[0].Partitions)
		require.Equal(t, int32(0), *envelope.Result[0].InSyncPartitions)
	})

	t.Run("broker metrics segment count", func(t *testing.T) {
		result := callClusterBrokerTool(t, app, "getBrokersMetrics", map[string]any{
			"clusterName": "prod",
			"id":          float64(2),
		}, false)
		var metrics generated.BrokerMetrics
		require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &metrics))
		require.NotNil(t, metrics.SegmentCount)
		require.Equal(t, int32(math.MaxInt32), *metrics.SegmentCount)
	})
}

func TestBrokerToolsSuccessfulWritesReturnStructuredNull(t *testing.T) {
	tests := []struct {
		name  string
		input map[string]any
	}{
		{
			name: "updateBrokerConfigByName",
			input: map[string]any{
				"clusterName": "prod",
				"id":          float64(2),
				"name":        "compression.type",
				"value":       "producer",
			},
		},
		{
			name: "updateBrokerTopicPartitionLogDir",
			input: map[string]any{
				"clusterName": "prod",
				"id":          float64(2),
				"topic":       "orders",
				"partition":   float64(3),
				"logDir":      "/data/kafka-2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := newRecordingClusterBrokerApp()
			result := callClusterBrokerTool(t, app, tt.name, tt.input, true)
			require.JSONEq(t, `{"result":null}`, callToolText(t, result))
			requireStructuredJSONEq(t, `{"result":null}`, result.StructuredContent)
		})
	}
}

func requireStructuredJSONEq(t *testing.T, want string, structured any) {
	t.Helper()
	raw, err := json.Marshal(structured)
	require.NoError(t, err)
	require.JSONEq(t, want, string(raw))
}

func callClusterBrokerTool(
	t *testing.T,
	app *recordingClusterBrokerApp,
	name string,
	input map[string]any,
	allowWrites bool,
) *mcp.CallToolResult {
	t.Helper()
	executor, _ := newClusterBrokerExecutor(t, app, &recordingReadOnlyResolver{}, allowWrites)
	result, err := newSDKSession(t, requireCatalogSpec(t, name), executor).CallTool(
		context.Background(),
		&mcp.CallToolParams{Name: name, Arguments: input},
	)
	require.NoError(t, err)
	require.False(t, result.IsError, callToolText(t, result))
	return result
}

func newRecordingClusterBrokerApp() *recordingClusterBrokerApp {
	state := domaincluster.RuntimeState{
		Definition: domaincluster.Definition{
			Name:     "prod",
			ReadOnly: false,
		},
		Status:     domaincluster.StatusOnline,
		Controller: 1,
		TopicCount: 3,
		Brokers: []domaincluster.BrokerInfo{
			{ID: 2, Host: "broker,two", Port: 9093, PartitionsLeader: 5, Partitions: 10, InSyncPartitions: 7},
			{ID: 1, Host: "broker-one", Port: 9092, PartitionsLeader: 4, Partitions: 9, InSyncPartitions: 8},
		},
		Partitions: domaincluster.PartitionCounts{
			Online: 12, Offline: 1, UnderReplicated: 2, InSync: 20, OutOfSync: 3,
		},
		Disk: []domaincluster.DiskUsage{
			{Broker: 2, SegmentSize: 2048, SegmentCount: 20},
			{Broker: 1, SegmentSize: 1024, SegmentCount: 10},
		},
		Version:              "3.9.0",
		TopicDeletionEnabled: true,
	}
	return &recordingClusterBrokerApp{
		snapshots: []domaincluster.Snapshot{state.Snapshot()},
		state:     state,
		stateOK:   true,
		dirs: []domaincluster.BrokerLogDirs{{
			Broker: 2,
			Dir:    "/data/kafka-2",
			Topics: []domaincluster.TopicLogDirs{{
				Topic:      "orders",
				Partitions: []domaincluster.PartitionLogDir{{Partition: 3, Size: 42, OffsetLag: 0}},
			}},
		}},
		configs: []domaincluster.ConfigEntry{{
			Name:   "compression.type",
			Value:  "producer",
			Source: "DYNAMIC_BROKER_CONFIG",
		}},
	}
}

func newClusterBrokerExecutor(
	t *testing.T,
	app *recordingClusterBrokerApp,
	resolver *recordingReadOnlyResolver,
	allowWrites bool,
) (*Executor, *mcppolicy.Store) {
	t.Helper()
	store := mcppolicy.NewStore(filepath.Join(t.TempDir(), "mcp-policy.json"))
	require.NoError(t, store.Save(context.Background(), *policy(true, allowWrites)))
	executor, err := NewExecutor(Dependencies{
		States:     app,
		Brokers:    app,
		Policy:     store,
		IsReadOnly: resolver.Resolve,
	})
	require.NoError(t, err)
	return executor, store
}

func newSDKCatalogSession(t *testing.T, specs []ToolSpec, executor *Executor) *mcp.ClientSession {
	t.Helper()
	server := mcp.NewServer(&mcp.Implementation{Name: "cluster-broker-test-server", Version: "test"}, nil)
	for _, spec := range specs {
		spec.Register(server, executor)
	}
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(context.Background(), serverTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, serverSession.Close()) })

	client := mcp.NewClient(&mcp.Implementation{Name: "cluster-broker-test-client", Version: "test"}, nil)
	clientSession, err := client.Connect(context.Background(), clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, clientSession.Close()) })
	return clientSession
}

func requireCatalogSpec(t *testing.T, name string) ToolSpec {
	t.Helper()
	for _, spec := range Catalog() {
		if spec.Meta.Name == name {
			return spec
		}
	}
	t.Fatalf("catalog tool %q not found", name)
	return ToolSpec{}
}

func findToolOrNil(tools []*mcp.Tool, name string) *mcp.Tool {
	for _, tool := range tools {
		if tool.Name == name {
			return tool
		}
	}
	return nil
}

func mustLoadPolicy(t *testing.T, store *mcppolicy.Store) *mcppolicy.Policy {
	t.Helper()
	loaded, err := store.Load()
	require.NoError(t, err)
	return &loaded
}

func int32Values(count int) []any {
	values := make([]any, count)
	for index := range values {
		values[index] = float64(index)
	}
	return values
}

var (
	_ ClusterStater  = (*recordingClusterBrokerApp)(nil)
	_ BrokerServicer = (*recordingClusterBrokerApp)(nil)
)
