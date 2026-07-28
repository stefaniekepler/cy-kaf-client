package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	domainanalysis "github.com/cy-kaf/cy-kaf-client/internal/domain/analysis"
	domaincluster "github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/mcppolicy"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

type recordingTopicAnalysisApp struct {
	mu sync.Mutex

	page      appcluster.TopicPage
	details   domaincluster.TopicState
	configs   []domaincluster.ConfigEntry
	acls      []domaincluster.AclBinding
	producers []domaincluster.ProducerState
	analysis  appcluster.AnalysisView
	found     bool

	listErr       error
	detailsErr    error
	configsErr    error
	aclsErr       error
	producersErr  error
	connectorsErr error
	analysisErr   error

	calls        []string
	queries      []appcluster.TopicListQuery
	createdSpecs []domaincluster.TopicSpec
	updates      []map[string]string
}

func (f *recordingTopicAnalysisApp) List(_ context.Context, clusterName string, query appcluster.TopicListQuery) (appcluster.TopicPage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "topics.list("+clusterName+")")
	f.queries = append(f.queries, query)
	return f.page, f.listErr
}

func (f *recordingTopicAnalysisApp) Details(_ context.Context, clusterName, topicName string) (domaincluster.TopicState, []domaincluster.ConfigEntry, error) {
	f.record("topics.details(" + clusterName + "," + topicName + ")")
	return f.details, f.configs, f.detailsErr
}

func (f *recordingTopicAnalysisApp) Configs(_ context.Context, clusterName, topicName string) ([]domaincluster.ConfigEntry, error) {
	f.record("topics.configs(" + clusterName + "," + topicName + ")")
	return f.configs, f.configsErr
}

func (f *recordingTopicAnalysisApp) Acls(_ context.Context, clusterName, topicName string) ([]domaincluster.AclBinding, error) {
	f.record("topics.acls(" + clusterName + "," + topicName + ")")
	return f.acls, f.aclsErr
}

func (f *recordingTopicAnalysisApp) ActiveProducers(_ context.Context, clusterName, topicName string) ([]domaincluster.ProducerState, error) {
	f.record("topics.producers(" + clusterName + "," + topicName + ")")
	return f.producers, f.producersErr
}

func (f *recordingTopicAnalysisApp) Connectors(_ context.Context, clusterName, topicName string) error {
	f.record("topics.connectors(" + clusterName + "," + topicName + ")")
	return f.connectorsErr
}

func (f *recordingTopicAnalysisApp) Create(_ context.Context, clusterName string, spec domaincluster.TopicSpec) (domaincluster.TopicState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "topics.create("+clusterName+","+spec.Name+")")
	f.createdSpecs = append(f.createdSpecs, spec)
	return topicState(spec.Name), nil
}

func (f *recordingTopicAnalysisApp) Delete(_ context.Context, clusterName, topicName string) error {
	f.record("topics.delete(" + clusterName + "," + topicName + ")")
	return nil
}

func (f *recordingTopicAnalysisApp) UpdateConfigs(_ context.Context, clusterName, topicName string, desired map[string]string) (domaincluster.TopicState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "topics.update("+clusterName+","+topicName+")")
	f.updates = append(f.updates, cloneStringMap(desired))
	return topicState(topicName), nil
}

func (f *recordingTopicAnalysisApp) Recreate(_ context.Context, clusterName, topicName string) (domaincluster.TopicState, error) {
	f.record("topics.recreate(" + clusterName + "," + topicName + ")")
	return topicState(topicName), nil
}

func (f *recordingTopicAnalysisApp) Clone(_ context.Context, clusterName, sourceTopic, newTopic string) (domaincluster.TopicState, error) {
	f.record("topics.clone(" + clusterName + "," + sourceTopic + "," + newTopic + ")")
	return topicState(newTopic), nil
}

func (f *recordingTopicAnalysisApp) IncreasePartitions(_ context.Context, clusterName, topicName string, total int32) error {
	f.record(fmt.Sprintf("topics.increase(%s,%s,%d)", clusterName, topicName, total))
	return nil
}

func (f *recordingTopicAnalysisApp) ChangeReplicationFactor(_ context.Context, clusterName, topicName string, target int16) error {
	f.record(fmt.Sprintf("topics.replication(%s,%s,%d)", clusterName, topicName, target))
	return nil
}

func (f *recordingTopicAnalysisApp) Analyze(_ context.Context, clusterName, topicName string) error {
	f.record("analysis.analyze(" + clusterName + "," + topicName + ")")
	return f.analysisErr
}

func (f *recordingTopicAnalysisApp) Get(clusterName, topicName string) (appcluster.AnalysisView, bool, error) {
	f.record("analysis.get(" + clusterName + "," + topicName + ")")
	return f.analysis, f.found, f.analysisErr
}

func (f *recordingTopicAnalysisApp) Cancel(_ context.Context, clusterName, topicName string) error {
	f.record("analysis.cancel(" + clusterName + "," + topicName + ")")
	return f.analysisErr
}

func (f *recordingTopicAnalysisApp) record(call string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call)
}

func (f *recordingTopicAnalysisApp) recordedCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *recordingTopicAnalysisApp) recordedQueries() []appcluster.TopicListQuery {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]appcluster.TopicListQuery(nil), f.queries...)
}

func (f *recordingTopicAnalysisApp) recordedSpecs() []domaincluster.TopicSpec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]domaincluster.TopicSpec(nil), f.createdSpecs...)
}

func (f *recordingTopicAnalysisApp) recordedUpdates() []map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]map[string]string, len(f.updates))
	for index := range f.updates {
		out[index] = cloneStringMap(f.updates[index])
	}
	return out
}

func TestTopicToolsDelegateAllSeventeenOperationsThroughApplicationPorts(t *testing.T) {
	tests := []struct {
		name       string
		input      map[string]any
		wantCall   string
		wantAccess AccessClass
		wantJSON   string
	}{
		{
			name: "getTopicConfigs", input: topicArguments(),
			wantCall: "topics.configs(prod,orders)", wantAccess: AccessReadOnly,
			wantJSON: `{
				"result":[{
					"name":"compression.type",
					"value":"producer",
					"source":"DYNAMIC_TOPIC_CONFIG",
					"isSensitive":false,
					"isReadOnly":false
				}]
			}`,
		},
		{
			name: "getTopicDetails", input: topicArguments(),
			wantCall: "topics.details(prod,orders)", wantAccess: AccessReadOnly,
			wantJSON: `{
				"name":"orders",
				"internal":false,
				"inSyncReplicas":2,
				"partitionCount":1,
				"partitions":[{
					"leader":1,
					"offsetMax":5,
					"offsetMin":0,
					"partition":0,
					"replicas":[
						{"broker":1,"inSync":true,"leader":true},
						{"broker":2,"inSync":true,"leader":false}
					]
				}],
				"replicas":2,
				"replicationFactor":2,
				"segmentCount":0,
				"segmentSize":0,
				"underReplicatedPartitions":0
			}`,
		},
		{
			name: "getTopics", input: map[string]any{"clusterName": "prod"},
			wantCall: "topics.list(prod)", wantAccess: AccessReadOnly,
			wantJSON: `{
				"items":[{
					"name":"orders",
					"internal":false,
					"messagesCount":5,
					"partitionCount":1,
					"replicas":2,
					"replicationFactor":2,
					"inSyncReplicas":2,
					"segmentCount":0,
					"segmentSize":0,
					"underReplicatedPartitions":0
				}],
				"page":1,
				"pageCount":1,
				"truncated":false
			}`,
		},
		{
			name: "getTopicsCsv", input: map[string]any{"clusterName": "prod"},
			wantCall: "topics.list(prod)", wantAccess: AccessReadOnly,
			wantJSON: `{
				"result":"bytesInPerSec,bytesOutPerSec,cleanUpPolicy,inSyncReplicas,internal,messagesCount,name,partitionCount,partitions,replicas,replicationFactor,segmentCount,segmentSize,underReplicatedPartitions\n,,,2,false,5,orders,1,,2,2,0,0,0\n"
			}`,
		},
		{
			name: "listTopicAcls", input: topicArguments(),
			wantCall: "topics.acls(prod,orders)", wantAccess: AccessReadOnly,
			wantJSON: `{
				"result":[{
					"host":"*",
					"namePatternType":"LITERAL",
					"operation":"READ",
					"permission":"ALLOW",
					"principal":"User:test",
					"resourceName":"orders",
					"resourceType":"TOPIC"
				}]
			}`,
		},
		{
			name: "analyzeTopic", input: topicArguments(),
			wantCall: "analysis.analyze(prod,orders)", wantAccess: AccessReadOnly,
			wantJSON: `{"result":null}`,
		},
		{
			name: "cancelTopicAnalysis", input: topicArguments(),
			wantCall: "analysis.cancel(prod,orders)", wantAccess: AccessReadOnly,
			wantJSON: `{"result":null}`,
		},
		{
			name: "getTopicAnalysis", input: topicArguments(),
			wantCall: "analysis.get(prod,orders)", wantAccess: AccessReadOnly,
			wantJSON: `{
				"progress":{
					"bytesScanned":99,
					"completenessPercent":37.5,
					"msgsScanned":3,
					"startedAt":100
				}
			}`,
		},
		{
			name: "getActiveProducerStates", input: topicArguments(),
			wantCall: "topics.producers(prod,orders)", wantAccess: AccessReadOnly,
			wantJSON: `{
				"result":[{
					"coordinatorEpoch":0,
					"currentTransactionStartOffset":0,
					"lastSequence":0,
					"lastTimestampMs":0,
					"partition":0,
					"producerEpoch":0,
					"producerId":42
				}]
			}`,
		},
		{
			name: "getTopicConnectors", input: topicArguments(),
			wantCall: "topics.connectors(prod,orders)", wantAccess: AccessReadOnly,
			wantJSON: `{"connectors":[]}`,
		},
		{
			name: "createTopic",
			input: map[string]any{"clusterName": "prod", "body": map[string]any{
				"name": "created", "partitions": float64(2), "replicationFactor": float64(2),
				"configs": map[string]any{"cleanup.policy": "delete"},
			}},
			wantCall: "topics.create(prod,created)", wantAccess: AccessWrite,
			wantJSON: `{
				"name":"created",
				"internal":false,
				"messagesCount":5,
				"partitionCount":1,
				"replicas":2,
				"replicationFactor":2,
				"inSyncReplicas":2,
				"segmentCount":0,
				"segmentSize":0,
				"underReplicatedPartitions":0
			}`,
		},
		{
			name: "recreateTopic", input: topicArguments(),
			wantCall: "topics.recreate(prod,orders)", wantAccess: AccessWrite,
			wantJSON: `{
				"name":"orders",
				"internal":false,
				"messagesCount":5,
				"partitionCount":1,
				"replicas":2,
				"replicationFactor":2,
				"inSyncReplicas":2,
				"segmentCount":0,
				"segmentSize":0,
				"underReplicatedPartitions":0
			}`,
		},
		{
			name: "cloneTopic",
			input: map[string]any{
				"clusterName": "prod", "topicName": "orders",
				"query": map[string]any{"newTopicName": "cloned"},
			},
			wantCall: "topics.clone(prod,orders,cloned)", wantAccess: AccessWrite,
			wantJSON: `{
				"name":"cloned",
				"internal":false,
				"messagesCount":5,
				"partitionCount":1,
				"replicas":2,
				"replicationFactor":2,
				"inSyncReplicas":2,
				"segmentCount":0,
				"segmentSize":0,
				"underReplicatedPartitions":0
			}`,
		},
		{
			name: "deleteTopic", input: topicArguments(),
			wantCall: "topics.delete(prod,orders)", wantAccess: AccessWrite,
			wantJSON: `{"result":null}`,
		},
		{
			name: "updateTopic",
			input: map[string]any{
				"clusterName": "prod", "topicName": "orders",
				"body": map[string]any{"configs": map[string]any{"retention.ms": "604800000"}},
			},
			wantCall: "topics.update(prod,orders)", wantAccess: AccessWrite,
			wantJSON: `{
				"name":"orders",
				"internal":false,
				"messagesCount":5,
				"partitionCount":1,
				"replicas":2,
				"replicationFactor":2,
				"inSyncReplicas":2,
				"segmentCount":0,
				"segmentSize":0,
				"underReplicatedPartitions":0
			}`,
		},
		{
			name: "increaseTopicPartitions",
			input: map[string]any{
				"clusterName": "prod", "topicName": "orders",
				"body": map[string]any{"totalPartitionsCount": float64(6)},
			},
			wantCall: "topics.increase(prod,orders,6)", wantAccess: AccessWrite,
			wantJSON: `{"topicName":"orders","totalPartitionsCount":6}`,
		},
		{
			name: "changeReplicationFactor",
			input: map[string]any{
				"clusterName": "prod", "topicName": "orders",
				"body": map[string]any{"totalReplicationFactor": float64(3)},
			},
			wantCall: "topics.replication(prod,orders,3)", wantAccess: AccessWrite,
			wantJSON: `{"topicName":"orders","totalReplicationFactor":3}`,
		},
	}
	require.Len(t, tests, 17)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newRecordingTopicAnalysisApp()
			executor, _ := newTopicExecutor(t, app, &recordingReadOnlyResolver{}, true)
			spec := requireCatalogSpec(t, test.name)
			require.Equal(t, test.wantAccess, spec.Meta.Access)
			if test.name == "analyzeTopic" || test.name == "cancelTopicAnalysis" {
				require.Equal(t, analysisTimeout, spec.Meta.Timeout)
			}

			result, err := newSDKSession(t, spec, executor).CallTool(
				context.Background(),
				&mcp.CallToolParams{Name: test.name, Arguments: test.input},
			)
			require.NoError(t, err)
			require.False(t, result.IsError, callToolText(t, result))
			require.JSONEq(t, test.wantJSON, callToolText(t, result))
			requireStructuredJSONEq(t, test.wantJSON, result.StructuredContent)
			require.Equal(t, []string{test.wantCall}, app.recordedCalls())
		})
	}
}

func TestTopicToolsKeepAnalysisStartAndCancelDefaultVisible(t *testing.T) {
	app := newRecordingTopicAnalysisApp()
	executor, store := newTopicExecutor(t, app, &recordingReadOnlyResolver{}, false)
	session := newSDKCatalogSession(t, VisibleCatalog(*mustLoadPolicy(t, store)), executor)

	listed, err := session.ListTools(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, listed.Tools, 51)
	require.NotNil(t, findTool(t, listed.Tools, "analyzeTopic"))
	require.NotNil(t, findTool(t, listed.Tools, "cancelTopicAnalysis"))
	require.Nil(t, findToolOrNil(listed.Tools, "createTopic"))
}

func TestTopicToolsPaginationDefaultsBoundsMetadataAndOrdering(t *testing.T) {
	t.Run("defaults and metadata", func(t *testing.T) {
		app := newRecordingTopicAnalysisApp()
		app.page = appcluster.TopicPage{
			Topics:    []domaincluster.TopicState{topicState("zeta"), topicState("alpha")},
			PageCount: 3,
		}
		result := callTopicTool(t, app, "getTopics", map[string]any{"clusterName": "prod"}, false)

		require.Equal(t, []appcluster.TopicListQuery{{Page: 1, PerPage: 25}}, app.recordedQueries())
		var output struct {
			Items     []generated.Topic `json:"items"`
			Page      int               `json:"page"`
			PageCount int               `json:"pageCount"`
			Truncated bool              `json:"truncated"`
		}
		require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &output))
		require.Equal(t, 1, output.Page)
		require.Equal(t, 3, output.PageCount)
		require.True(t, output.Truncated)
		require.Equal(t, []string{"alpha", "zeta"}, []string{output.Items[0].Name, output.Items[1].Name})
	})

	t.Run("empty result normalizes the requested page to one", func(t *testing.T) {
		app := newRecordingTopicAnalysisApp()
		app.page = appcluster.TopicPage{PageCount: 0}
		result := callTopicTool(t, app, "getTopics", map[string]any{
			"clusterName": "prod",
			"query":       map[string]any{"page": float64(7)},
		}, false)

		want := `{"items":[],"page":1,"pageCount":0,"truncated":false}`
		require.JSONEq(t, want, callToolText(t, result))
		requireStructuredJSONEq(t, want, result.StructuredContent)
	})

	t.Run("out of range request clamps to the last positive page", func(t *testing.T) {
		app := newRecordingTopicAnalysisApp()
		app.page = appcluster.TopicPage{
			Topics:    []domaincluster.TopicState{topicState("last")},
			PageCount: 2,
		}
		result := callTopicTool(t, app, "getTopics", map[string]any{
			"clusterName": "prod",
			"query":       map[string]any{"page": float64(7)},
		}, false)

		want := `{
			"items":[{
				"name":"last",
				"internal":false,
				"messagesCount":5,
				"partitionCount":1,
				"replicas":2,
				"inSyncReplicas":2,
				"replicationFactor":2,
				"segmentCount":0,
				"segmentSize":0,
				"underReplicatedPartitions":0
			}],
			"page":2,
			"pageCount":2,
			"truncated":false
		}`
		require.JSONEq(t, want, callToolText(t, result))
		requireStructuredJSONEq(t, want, result.StructuredContent)
	})

	for _, perPage := range []float64{0, 101} {
		t.Run(fmt.Sprintf("reject perPage %.0f", perPage), func(t *testing.T) {
			app := newRecordingTopicAnalysisApp()
			result, err := callTopicToolRaw(t, app, "getTopics", map[string]any{
				"clusterName": "prod",
				"query":       map[string]any{"perPage": perPage},
			}, false)
			require.NoError(t, err)
			require.True(t, result.IsError)
			require.Equal(t, "invalid_request", callToolText(t, result))
			require.Empty(t, app.recordedCalls())
		})
	}

	t.Run("accept perPage one hundred", func(t *testing.T) {
		app := newRecordingTopicAnalysisApp()
		callTopicTool(t, app, "getTopics", map[string]any{
			"clusterName": "prod",
			"query":       map[string]any{"page": float64(2), "perPage": float64(100)},
		}, false)
		require.Equal(t, 2, app.recordedQueries()[0].Page)
		require.Equal(t, 100, app.recordedQueries()[0].PerPage)
	})
}

func TestTopicToolsCsvUsesIndependentBoundAndDeterministicOrder(t *testing.T) {
	csvFromOrder := func(nameIndex func(int) int) (*recordingTopicAnalysisApp, string) {
		app := newRecordingTopicAnalysisApp()
		app.page.PageCount = 2
		app.page.Topics = make([]domaincluster.TopicState, maxListItems+1)
		for index := range app.page.Topics {
			app.page.Topics[index] = topicState(fmt.Sprintf("topic-%03d", nameIndex(index)))
		}
		result := callTopicTool(t, app, "getTopicsCsv", map[string]any{
			"clusterName": "prod",
			"query":       map[string]any{"orderBy": "TOTAL_PARTITIONS"},
		}, false)
		var envelope struct {
			Result string `json:"result"`
		}
		require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &envelope))
		return app, envelope.Result
	}

	reversedApp, reversed := csvFromOrder(func(index int) int {
		return maxListItems - index
	})
	shuffledApp, shuffled := csvFromOrder(func(index int) int {
		return (index * 2) % (maxListItems + 1)
	})
	wantQuery := []appcluster.TopicListQuery{{
		Page: 1, PerPage: maxListItems, OrderBy: "TOTAL_PARTITIONS",
	}}
	require.Equal(t, wantQuery, reversedApp.recordedQueries())
	require.Equal(t, wantQuery, shuffledApp.recordedQueries())
	require.Equal(t, reversed, shuffled)

	lines := strings.Split(strings.TrimSpace(reversed), "\n")
	require.Len(t, lines, maxListItems+1)
	require.Contains(t, lines[0], "messagesCount")
	require.Contains(t, lines[1], "topic-000")
	require.Contains(t, lines[len(lines)-1], "topic-499")
	require.NotContains(t, reversed, "topic-500")
}

func TestTopicToolsConnectorsAndAnalysisKeepKnownAbsenceDistinctFromUnknownCluster(t *testing.T) {
	t.Run("known connector stub is exact empty array", func(t *testing.T) {
		app := newRecordingTopicAnalysisApp()
		result := callTopicTool(t, app, "getTopicConnectors", topicArguments(), false)
		require.JSONEq(t, `{"connectors":[]}`, callToolText(t, result))
		requireStructuredJSONEq(t, `{"connectors":[]}`, result.StructuredContent)
	})

	t.Run("absent analysis is safe not found", func(t *testing.T) {
		app := newRecordingTopicAnalysisApp()
		app.found = false
		result, err := callTopicToolRaw(t, app, "getTopicAnalysis", topicArguments(), false)
		require.NoError(t, err)
		require.True(t, result.IsError)
		require.Equal(t, "not_found", callToolText(t, result))
	})

	t.Run("unknown analysis cluster is cluster not found", func(t *testing.T) {
		app := newRecordingTopicAnalysisApp()
		app.analysisErr = appcluster.ErrUnknownCluster
		result, err := callTopicToolRaw(t, app, "getTopicAnalysis", topicArguments(), false)
		require.NoError(t, err)
		require.True(t, result.IsError)
		require.Equal(t, "cluster_not_found", callToolText(t, result))
	})

	t.Run("unknown connector cluster is cluster not found", func(t *testing.T) {
		app := newRecordingTopicAnalysisApp()
		app.connectorsErr = appcluster.ErrUnknownCluster
		result, err := callTopicToolRaw(t, app, "getTopicConnectors", topicArguments(), false)
		require.NoError(t, err)
		require.True(t, result.IsError)
		require.Equal(t, "cluster_not_found", callToolText(t, result))
	})
}

func TestTopicToolsDoNotExposeRawTerminalAnalysisErrors(t *testing.T) {
	app := newRecordingTopicAnalysisApp()
	app.analysis = appcluster.AnalysisView{Result: &appcluster.AnalysisResult{
		StartedAt: 100, FinishedAt: 200,
		Error:      "SASL password=credential-marker broker failure",
		TotalStats: domainanalysis.Stats{},
	}}
	result := callTopicTool(t, app, "getTopicAnalysis", topicArguments(), false)
	require.NotContains(t, callToolText(t, result), "credential-marker")

	var output generated.TopicAnalysis
	require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &output))
	require.NotNil(t, output.Result)
	require.NotNil(t, output.Result.Error)
	require.Equal(t, "operation_failed", *output.Result.Error)
}

func TestTopicToolsBoundNestedDetailConfigAndAnalysisCollections(t *testing.T) {
	t.Run("topic detail partitions", func(t *testing.T) {
		app := newRecordingTopicAnalysisApp()
		app.details.Partitions = make([]domaincluster.PartitionState, maxListItems+1)
		for index := range app.details.Partitions {
			app.details.Partitions[index].ID = int32(maxListItems - index)
		}
		result := callTopicTool(t, app, "getTopicDetails", topicArguments(), false)

		var output generated.TopicDetails
		require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &output))
		require.NotNil(t, output.Partitions)
		require.Len(t, *output.Partitions, maxListItems)
		require.Equal(t, int32(0), (*output.Partitions)[0].Partition)
		require.Equal(t, int32(maxListItems-1), (*output.Partitions)[maxListItems-1].Partition)
	})

	t.Run("config synonyms", func(t *testing.T) {
		app := newRecordingTopicAnalysisApp()
		app.configs[0].Synonyms = make([]domaincluster.ConfigSynonym, maxListItems+1)
		for index := range app.configs[0].Synonyms {
			app.configs[0].Synonyms[index] = domaincluster.ConfigSynonym{
				Name: fmt.Sprintf("synonym.%03d", maxListItems-index), Value: "value",
			}
		}
		result := callTopicTool(t, app, "getTopicConfigs", topicArguments(), false)

		var envelope struct {
			Result []generated.TopicConfig `json:"result"`
		}
		require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &envelope))
		require.NotNil(t, envelope.Result[0].Synonyms)
		require.Len(t, *envelope.Result[0].Synonyms, maxListItems)
	})

	t.Run("analysis partitions and hourly buckets", func(t *testing.T) {
		app := newRecordingTopicAnalysisApp()
		stats := make([]domainanalysis.Stats, maxListItems+1)
		for index := range stats {
			partition := int32(maxListItems - index)
			stats[index] = domainanalysis.Stats{Partition: &partition}
		}
		stats[len(stats)-1].HasData = true
		stats[len(stats)-1].HourlyMsgCounts = make([]domainanalysis.HourCount, maxListItems+1)
		app.analysis = appcluster.AnalysisView{Result: &appcluster.AnalysisResult{
			StartedAt: 100, FinishedAt: 200, PartitionStats: stats,
		}}
		result := callTopicTool(t, app, "getTopicAnalysis", topicArguments(), false)

		var output generated.TopicAnalysis
		require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &output))
		require.NotNil(t, output.Result)
		require.NotNil(t, output.Result.PartitionStats)
		require.Len(t, *output.Result.PartitionStats, maxListItems)
		first := (*output.Result.PartitionStats)[0]
		require.NotNil(t, first.HourlyMsgCounts)
		require.Len(t, *first.HourlyMsgCounts, maxListItems)
	})
}

func TestTopicToolsAllSevenWritesUseActualClusterReadOnlyGate(t *testing.T) {
	tests := []struct {
		name  string
		input map[string]any
	}{
		{name: "createTopic", input: map[string]any{
			"clusterName": "prod", "body": map[string]any{"name": "created", "partitions": float64(1)},
		}},
		{name: "recreateTopic", input: topicArguments()},
		{name: "cloneTopic", input: map[string]any{
			"clusterName": "prod", "topicName": "orders",
			"query": map[string]any{"newTopicName": "cloned"},
		}},
		{name: "deleteTopic", input: topicArguments()},
		{name: "updateTopic", input: map[string]any{
			"clusterName": "prod", "topicName": "orders",
			"body": map[string]any{"configs": map[string]any{"retention.ms": "1000"}},
		}},
		{name: "increaseTopicPartitions", input: map[string]any{
			"clusterName": "prod", "topicName": "orders",
			"body": map[string]any{"totalPartitionsCount": float64(3)},
		}},
		{name: "changeReplicationFactor", input: map[string]any{
			"clusterName": "prod", "topicName": "orders",
			"body": map[string]any{"totalReplicationFactor": float64(2)},
		}},
	}
	require.Len(t, tests, 7)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newRecordingTopicAnalysisApp()
			resolver := &recordingReadOnlyResolver{readOnly: true}
			executor, _ := newTopicExecutor(t, app, resolver, true)
			result, err := newSDKSession(t, requireCatalogSpec(t, test.name), executor).CallTool(
				context.Background(),
				&mcp.CallToolParams{Name: test.name, Arguments: test.input},
			)
			require.NoError(t, err)
			require.True(t, result.IsError)
			require.Equal(t, "cluster_read_only", callToolText(t, result))
			require.Empty(t, app.recordedCalls())
			require.Equal(t, []string{"prod"}, resolver.recordedClusters())
		})
	}
}

func TestTopicToolsConvertContractBodiesAndLenientScalarConfigsBeforeService(t *testing.T) {
	app := newRecordingTopicAnalysisApp()
	executor, _ := newTopicExecutor(t, app, &recordingReadOnlyResolver{}, true)

	create, err := newSDKSession(t, requireCatalogSpec(t, "createTopic"), executor).CallTool(
		context.Background(),
		&mcp.CallToolParams{
			Name: "createTopic",
			Arguments: map[string]any{
				"clusterName": "prod",
				"body": map[string]any{
					"name": "created", "partitions": float64(4), "replicationFactor": float64(3),
					"configs": map[string]any{
						"retention.ms":  float64(604800000),
						"unclean.elect": true,
					},
				},
			},
		},
	)
	require.NoError(t, err)
	require.False(t, create.IsError, callToolText(t, create))
	require.Equal(t, []domaincluster.TopicSpec{{
		Name: "created", Partitions: 4, ReplicationFactor: 3,
		Configs: map[string]string{"retention.ms": "604800000", "unclean.elect": "true"},
	}}, app.recordedSpecs())

	update, err := newSDKSession(t, requireCatalogSpec(t, "updateTopic"), executor).CallTool(
		context.Background(),
		&mcp.CallToolParams{
			Name: "updateTopic",
			Arguments: map[string]any{
				"clusterName": "prod", "topicName": "orders",
				"body": map[string]any{"configs": map[string]any{
					"retention.bytes": float64(1048576),
					"compact":         false,
					"nullable":        nil,
				}},
			},
		},
	)
	require.NoError(t, err)
	require.False(t, update.IsError, callToolText(t, update))
	require.Equal(t, []map[string]string{{
		"retention.bytes": "1048576",
		"compact":         "false",
	}}, app.recordedUpdates())
}

func TestTopicToolsRejectInvalidNamesCountsAndConfigBoundsBeforeService(t *testing.T) {
	tooManyConfigs := make(map[string]any, maxListItems+1)
	for index := 0; index <= maxListItems; index++ {
		tooManyConfigs[fmt.Sprintf("config.%03d", index)] = "value"
	}
	tests := []struct {
		name  string
		tool  string
		input map[string]any
	}{
		{
			name: "invalid topic characters", tool: "deleteTopic",
			input: map[string]any{"clusterName": "prod", "topicName": "orders/invalid"},
		},
		{
			name: "zero create partitions", tool: "createTopic",
			input: map[string]any{
				"clusterName": "prod",
				"body":        map[string]any{"name": "created", "partitions": float64(0)},
			},
		},
		{
			name: "oversized create replication", tool: "createTopic",
			input: map[string]any{
				"clusterName": "prod",
				"body": map[string]any{
					"name": "created", "partitions": float64(1), "replicationFactor": float64(32768),
				},
			},
		},
		{
			name: "too many configs", tool: "updateTopic",
			input: map[string]any{
				"clusterName": "prod", "topicName": "orders",
				"body": map[string]any{"configs": tooManyConfigs},
			},
		},
		{
			name: "empty config name", tool: "updateTopic",
			input: map[string]any{
				"clusterName": "prod", "topicName": "orders",
				"body": map[string]any{"configs": map[string]any{" ": "value"}},
			},
		},
		{
			name: "oversized config value", tool: "updateTopic",
			input: map[string]any{
				"clusterName": "prod", "topicName": "orders",
				"body": map[string]any{"configs": map[string]any{
					"retention.ms": strings.Repeat("x", maxResultBytes+1),
				}},
			},
		},
		{
			name: "missing partition target", tool: "increaseTopicPartitions",
			input: map[string]any{
				"clusterName": "prod", "topicName": "orders", "body": map[string]any{},
			},
		},
		{
			name: "zero replication target", tool: "changeReplicationFactor",
			input: map[string]any{
				"clusterName": "prod", "topicName": "orders",
				"body": map[string]any{"totalReplicationFactor": float64(0)},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newRecordingTopicAnalysisApp()
			result, err := callTopicToolRaw(t, app, test.tool, test.input, true)
			require.NoError(t, err)
			require.True(t, result.IsError)
			require.Equal(t, "invalid_request", callToolText(t, result))
			require.Empty(t, app.recordedCalls())
		})
	}
}

func TestTopicToolsSortListLikeResultsAndRedactTopicConfigCredentials(t *testing.T) {
	app := newRecordingTopicAnalysisApp()
	app.configs = []domaincluster.ConfigEntry{
		{Name: "z.config", Value: "z"},
		{Name: "sasl.jaas.config", Value: "credential-marker", IsSensitive: true},
		{Name: "a.config", Value: "a"},
	}
	result := callTopicTool(t, app, "getTopicConfigs", topicArguments(), false)
	require.NotContains(t, callToolText(t, result), "credential-marker")

	var envelope struct {
		Result []generated.TopicConfig `json:"result"`
	}
	require.NoError(t, json.Unmarshal([]byte(callToolText(t, result)), &envelope))
	require.Equal(t, []string{"a.config", "sasl.jaas.config", "z.config"}, []string{
		envelope.Result[0].Name, envelope.Result[1].Name, envelope.Result[2].Name,
	})
	require.Equal(t, redactedValue, *envelope.Result[1].Value)
}

func TestTopicToolsRedactCredentialNamedDefaultSynonymWithoutHidingOrdinaryDefaults(t *testing.T) {
	app := newRecordingTopicAnalysisApp()
	app.configs = []domaincluster.ConfigEntry{
		{
			Name: "compression.type", Value: "producer", Source: "DYNAMIC_TOPIC_CONFIG",
			Synonyms: []domaincluster.ConfigSynonym{{
				Name: "sasl.password", Value: "credential-marker", Source: "DEFAULT_CONFIG",
			}},
		},
		{
			Name: "retention.ms", Value: "60000", Source: "DYNAMIC_TOPIC_CONFIG",
			Synonyms: []domaincluster.ConfigSynonym{{
				Name: "retention.ms", Value: "120000", Source: "DEFAULT_CONFIG",
			}},
		},
	}

	result := callTopicTool(t, app, "getTopicConfigs", topicArguments(), false)
	want := `{
		"result": [
			{
				"name": "compression.type",
				"value": "producer",
				"source": "DYNAMIC_TOPIC_CONFIG",
				"isSensitive": false,
				"isReadOnly": false,
				"defaultValue": "[REDACTED]",
				"synonyms": [
					{"name": "sasl.password", "value": "[REDACTED]", "source": "DEFAULT_CONFIG"}
				]
			},
			{
				"name": "retention.ms",
				"value": "60000",
				"source": "DYNAMIC_TOPIC_CONFIG",
				"isSensitive": false,
				"isReadOnly": false,
				"defaultValue": "120000",
				"synonyms": [
					{"name": "retention.ms", "value": "120000", "source": "DEFAULT_CONFIG"}
				]
			}
		]
	}`

	require.NotContains(t, callToolText(t, result), "credential-marker")
	require.JSONEq(t, want, callToolText(t, result))
	structured, err := json.Marshal(result.StructuredContent)
	require.NoError(t, err)
	require.NotContains(t, string(structured), "credential-marker")
	requireStructuredJSONEq(t, want, result.StructuredContent)
}

func newRecordingTopicAnalysisApp() *recordingTopicAnalysisApp {
	return &recordingTopicAnalysisApp{
		page: appcluster.TopicPage{
			Topics:    []domaincluster.TopicState{topicState("orders")},
			PageCount: 1,
		},
		details: topicState("orders"),
		configs: []domaincluster.ConfigEntry{{
			Name: "compression.type", Value: "producer", Source: "DYNAMIC_TOPIC_CONFIG",
		}},
		acls: []domaincluster.AclBinding{{
			Principal: "User:test", Host: "*", ResourceType: "TOPIC",
			ResourceName: "orders", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW",
		}},
		producers: []domaincluster.ProducerState{{Partition: 0, ProducerID: 42}},
		analysis: appcluster.AnalysisView{Progress: &appcluster.AnalysisProgress{
			StartedAt: 100, CompletenessPercent: 37.5, MsgsScanned: 3, BytesScanned: 99,
		}},
		found: true,
	}
}

func newTopicExecutor(
	t *testing.T,
	app *recordingTopicAnalysisApp,
	resolver *recordingReadOnlyResolver,
	allowWrites bool,
) (*Executor, *mcppolicy.Store) {
	t.Helper()
	store := mcppolicy.NewStore(filepath.Join(t.TempDir(), "mcp-policy.json"))
	require.NoError(t, store.Save(context.Background(), *policy(true, allowWrites)))
	executor, err := NewExecutor(Dependencies{
		Topics:     app,
		Analysis:   app,
		Policy:     store,
		IsReadOnly: resolver.Resolve,
	})
	require.NoError(t, err)
	return executor, store
}

func callTopicTool(
	t *testing.T,
	app *recordingTopicAnalysisApp,
	name string,
	input map[string]any,
	allowWrites bool,
) *mcp.CallToolResult {
	t.Helper()
	result, err := callTopicToolRaw(t, app, name, input, allowWrites)
	require.NoError(t, err)
	require.False(t, result.IsError, callToolText(t, result))
	return result
}

func callTopicToolRaw(
	t *testing.T,
	app *recordingTopicAnalysisApp,
	name string,
	input map[string]any,
	allowWrites bool,
) (*mcp.CallToolResult, error) {
	t.Helper()
	executor, _ := newTopicExecutor(t, app, &recordingReadOnlyResolver{}, allowWrites)
	return newSDKSession(t, requireCatalogSpec(t, name), executor).CallTool(
		context.Background(),
		&mcp.CallToolParams{Name: name, Arguments: input},
	)
}

func topicArguments() map[string]any {
	return map[string]any{"clusterName": "prod", "topicName": "orders"}
}

func topicState(name string) domaincluster.TopicState {
	return domaincluster.TopicState{
		Name: name, ReplicationFactor: 2,
		Partitions: []domaincluster.PartitionState{{
			ID: 0, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1, 2},
			StartOffset: 0, EndOffset: 5,
		}},
	}
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

var (
	_ TopicServicer    = (*recordingTopicAnalysisApp)(nil)
	_ AnalysisServicer = (*recordingTopicAnalysisApp)(nil)
)
