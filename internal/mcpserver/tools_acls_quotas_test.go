package mcpserver

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"strconv"
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

type aclQuotaCall struct {
	name        string
	cluster     string
	filter      domaincluster.AclFilter
	binding     domaincluster.AclBinding
	consumer    appcluster.ConsumerAclSpec
	producer    appcluster.ProducerAclSpec
	stream      appcluster.StreamAppAclSpec
	csv         string
	quota       domaincluster.ClientQuota
	formatCount int
}

type recordingAclQuotaApp struct {
	mu sync.Mutex

	acls        []domaincluster.AclBinding
	quotas      []domaincluster.ClientQuota
	deleteCount int
	formatText  string
	formatted   [][]domaincluster.AclBinding
	errs        map[string]error
	calls       []aclQuotaCall
}

func newRecordingAclQuotaApp() *recordingAclQuotaApp {
	return &recordingAclQuotaApp{
		acls: []domaincluster.AclBinding{
			{
				Principal: "User:bob", Host: "192.0.2.2", ResourceType: "GROUP",
				ResourceName: "payments", PatternType: "PREFIXED", Operation: "DESCRIBE",
				Permission: "DENY",
			},
			{
				Principal: "User:alice", Host: "*", ResourceType: "TOPIC",
				ResourceName: "orders", PatternType: "LITERAL", Operation: "READ",
				Permission: "ALLOW",
			},
		},
		quotas: []domaincluster.ClientQuota{
			{User: "bob", ClientID: "checkout", Quotas: map[string]float64{"consumer_byte_rate": 2048}},
			{User: "alice", Quotas: map[string]float64{"producer_byte_rate": 1024}},
		},
		deleteCount: 1,
		errs:        make(map[string]error),
	}
}

func (f *recordingAclQuotaApp) List(_ context.Context, cluster string, filter domaincluster.AclFilter) ([]domaincluster.AclBinding, error) {
	f.record(aclQuotaCall{name: "listAcls", cluster: cluster, filter: filter})
	return append([]domaincluster.AclBinding(nil), f.acls...), f.operationError("listAcls")
}

func (f *recordingAclQuotaApp) CreateAcl(_ context.Context, cluster string, binding domaincluster.AclBinding) error {
	f.record(aclQuotaCall{name: "createAcl", cluster: cluster, binding: binding})
	return f.operationError("createAcl")
}

func (f *recordingAclQuotaApp) DeleteAcl(_ context.Context, cluster string, binding domaincluster.AclBinding) (int, error) {
	f.record(aclQuotaCall{name: "deleteAcl", cluster: cluster, binding: binding})
	return f.deleteCount, f.operationError("deleteAcl")
}

func (f *recordingAclQuotaApp) CreateConsumerAcl(_ context.Context, cluster string, spec appcluster.ConsumerAclSpec) error {
	f.record(aclQuotaCall{name: "createConsumerAcl", cluster: cluster, consumer: spec})
	return f.operationError("createConsumerAcl")
}

func (f *recordingAclQuotaApp) CreateProducerAcl(_ context.Context, cluster string, spec appcluster.ProducerAclSpec) error {
	f.record(aclQuotaCall{name: "createProducerAcl", cluster: cluster, producer: spec})
	return f.operationError("createProducerAcl")
}

func (f *recordingAclQuotaApp) CreateStreamAppAcl(_ context.Context, cluster string, spec appcluster.StreamAppAclSpec) error {
	f.record(aclQuotaCall{name: "createStreamAppAcl", cluster: cluster, stream: spec})
	return f.operationError("createStreamAppAcl")
}

func (f *recordingAclQuotaApp) FormatAclCSV(bindings []domaincluster.AclBinding) (string, error) {
	f.record(aclQuotaCall{name: "formatAclCSV", formatCount: len(bindings)})
	f.mu.Lock()
	f.formatted = append(f.formatted, append([]domaincluster.AclBinding(nil), bindings...))
	f.mu.Unlock()
	if err := f.operationError("formatAclCSV"); err != nil {
		return "", err
	}
	if f.formatText != "" {
		return f.formatText, nil
	}
	return new(appcluster.AclService).FormatAclCSV(bindings)
}

func (f *recordingAclQuotaApp) SyncCSV(_ context.Context, cluster, csv string) error {
	f.record(aclQuotaCall{name: "syncAclsCsv", cluster: cluster, csv: csv})
	return f.operationError("syncAclsCsv")
}

func (f *recordingAclQuotaApp) ListQuotas(_ context.Context, cluster string) ([]domaincluster.ClientQuota, error) {
	f.record(aclQuotaCall{name: "listQuotas", cluster: cluster})
	return append([]domaincluster.ClientQuota(nil), f.quotas...), f.operationError("listQuotas")
}

func (f *recordingAclQuotaApp) UpsertQuotas(_ context.Context, cluster string, quota domaincluster.ClientQuota) error {
	f.record(aclQuotaCall{name: "upsertClientQuotas", cluster: cluster, quota: cloneClientQuota(quota)})
	return f.operationError("upsertClientQuotas")
}

func (f *recordingAclQuotaApp) record(call aclQuotaCall) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call)
}

func (f *recordingAclQuotaApp) operationError(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.errs[name]
}

func (f *recordingAclQuotaApp) snapshot() []aclQuotaCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]aclQuotaCall(nil), f.calls...)
}

func (f *recordingAclQuotaApp) formattedSnapshot() [][]domaincluster.AclBinding {
	f.mu.Lock()
	defer f.mu.Unlock()
	output := make([][]domaincluster.AclBinding, len(f.formatted))
	for index, bindings := range f.formatted {
		output[index] = append([]domaincluster.AclBinding(nil), bindings...)
	}
	return output
}

func TestAclToolsAndQuotaToolsDelegateAllTenOperationsWithExactSDKResults(t *testing.T) {
	aclBody := map[string]any{
		"principal": "User:alice", "host": "*", "resourceType": "TOPIC",
		"resourceName": "orders", "namePatternType": "LITERAL",
		"operation": "READ", "permission": "ALLOW",
	}
	csv := "Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\n" +
		"User:alice,TOPIC,LITERAL,orders,READ,ALLOW,*\n"
	tests := []struct {
		name       string
		input      map[string]any
		wantAccess AccessClass
		wantCalls  []aclQuotaCall
		wantJSON   string
	}{
		{
			name: "listAcls",
			input: map[string]any{"clusterName": "prod", "query": map[string]any{
				"resourceType": "TOPIC", "resourceName": "orders",
				"namePatternType": "MATCH", "search": "alice", "fts": true,
			}},
			wantAccess: AccessReadOnly,
			wantCalls: []aclQuotaCall{{name: "listAcls", cluster: "prod", filter: domaincluster.AclFilter{
				ResourceType: "TOPIC", ResourceName: "orders", PatternType: "MATCH",
				Search: "alice", Fts: true,
			}}},
			wantJSON: `{"result":[
				{"host":"*","namePatternType":"LITERAL","operation":"READ","permission":"ALLOW","principal":"User:alice","resourceName":"orders","resourceType":"TOPIC"},
				{"host":"192.0.2.2","namePatternType":"PREFIXED","operation":"DESCRIBE","permission":"DENY","principal":"User:bob","resourceName":"payments","resourceType":"GROUP"}
			]}`,
		},
		{
			name:       "getAclAsCsv",
			input:      map[string]any{"clusterName": "prod"},
			wantAccess: AccessReadOnly,
			wantCalls: []aclQuotaCall{
				{name: "listAcls", cluster: "prod"},
				{name: "formatAclCSV", formatCount: 2},
			},
			wantJSON: `{"result":"Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\nUser:alice,TOPIC,LITERAL,orders,READ,ALLOW,*\nUser:bob,GROUP,PREFIXED,payments,DESCRIBE,DENY,192.0.2.2\n"}`,
		},
		{
			name: "createAcl", input: map[string]any{"clusterName": "prod", "body": aclBody},
			wantAccess: AccessWrite,
			wantCalls: []aclQuotaCall{{name: "createAcl", cluster: "prod", binding: domaincluster.AclBinding{
				Principal: "User:alice", Host: "*", ResourceType: "TOPIC", ResourceName: "orders",
				PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW",
			}}},
			wantJSON: `{"result":null}`,
		},
		{
			name: "deleteAcl", input: map[string]any{"clusterName": "prod", "body": aclBody},
			wantAccess: AccessWrite,
			wantCalls: []aclQuotaCall{{name: "deleteAcl", cluster: "prod", binding: domaincluster.AclBinding{
				Principal: "User:alice", Host: "*", ResourceType: "TOPIC", ResourceName: "orders",
				PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW",
			}}},
			wantJSON: `{"result":null}`,
		},
		{
			name: "syncAclsCsv", input: map[string]any{"clusterName": "prod", "body": csv},
			wantAccess: AccessWrite,
			wantCalls:  []aclQuotaCall{{name: "syncAclsCsv", cluster: "prod", csv: csv}},
			wantJSON:   `{"result":null}`,
		},
		{
			name: "createConsumerAcl",
			input: map[string]any{"clusterName": "prod", "body": map[string]any{
				"principal": "User:alice", "topics": []any{"orders"}, "topicsPrefix": "in-",
				"consumerGroups": []any{"orders-cg"}, "consumerGroupsPrefix": "cg-",
			}},
			wantAccess: AccessWrite,
			wantCalls: []aclQuotaCall{{name: "createConsumerAcl", cluster: "prod", consumer: appcluster.ConsumerAclSpec{
				Principal: "User:alice", Host: "*", Topics: []string{"orders"}, TopicsPrefix: "in-",
				ConsumerGroups: []string{"orders-cg"}, ConsumerGroupsPrefix: "cg-",
			}}},
			wantJSON: `{"result":null}`,
		},
		{
			name: "createProducerAcl",
			input: map[string]any{"clusterName": "prod", "body": map[string]any{
				"principal": "User:alice", "host": "192.0.2.7", "topics": []any{"orders"},
				"topicsPrefix": "out-", "transactionalId": "txn", "transactionsIdPrefix": "txn-",
				"idempotent": true,
			}},
			wantAccess: AccessWrite,
			wantCalls: []aclQuotaCall{{name: "createProducerAcl", cluster: "prod", producer: appcluster.ProducerAclSpec{
				Principal: "User:alice", Host: "192.0.2.7", Topics: []string{"orders"}, TopicsPrefix: "out-",
				TransactionalID: "txn", TransactionsIDPrefix: "txn-", Idempotent: true,
			}}},
			wantJSON: `{"result":null}`,
		},
		{
			name: "createStreamAppAcl",
			input: map[string]any{"clusterName": "prod", "body": map[string]any{
				"principal": "User:alice", "inputTopics": []any{"orders"},
				"outputTopics": []any{"payments"}, "applicationId": "orders-app",
			}},
			wantAccess: AccessWrite,
			wantCalls: []aclQuotaCall{{name: "createStreamAppAcl", cluster: "prod", stream: appcluster.StreamAppAclSpec{
				Principal: "User:alice", Host: "*", InputTopics: []string{"orders"},
				OutputTopics: []string{"payments"}, ApplicationID: "orders-app",
			}}},
			wantJSON: `{"result":null}`,
		},
		{
			name: "listQuotas", input: map[string]any{"clusterName": "prod"},
			wantAccess: AccessReadOnly,
			wantCalls:  []aclQuotaCall{{name: "listQuotas", cluster: "prod"}},
			wantJSON: `{"result":[
				{"quotas":{"producer_byte_rate":1024},"user":"alice"},
				{"clientId":"checkout","quotas":{"consumer_byte_rate":2048},"user":"bob"}
			]}`,
		},
		{
			name: "upsertClientQuotas",
			input: map[string]any{"clusterName": "prod", "body": map[string]any{
				"user": "alice", "clientId": "checkout",
				"quotas": map[string]any{"producer_byte_rate": float64(1024)},
			}},
			wantAccess: AccessWrite,
			wantCalls: []aclQuotaCall{{name: "upsertClientQuotas", cluster: "prod", quota: domaincluster.ClientQuota{
				User: "alice", ClientID: "checkout", Quotas: map[string]float64{"producer_byte_rate": 1024},
			}}},
			wantJSON: `{"result":null}`,
		},
	}
	require.Len(t, tests, 10)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newRecordingAclQuotaApp()
			executor, _ := newAclQuotaExecutor(t, app, false, true)
			spec := requireCatalogSpec(t, test.name)
			require.Equal(t, test.wantAccess, spec.Meta.Access)

			result, err := newSDKSession(t, spec, executor).CallTool(
				context.Background(),
				&mcp.CallToolParams{Name: test.name, Arguments: test.input},
			)
			require.NoError(t, err)
			require.False(t, result.IsError, callToolText(t, result))
			require.JSONEq(t, test.wantJSON, callToolText(t, result))
			requireStructuredJSONEq(t, test.wantJSON, result.StructuredContent)
			require.Equal(t, test.wantCalls, app.snapshot())
		})
	}
}

func TestAclToolsAndQuotaToolsExposeThreeReadsAndGateSevenWrites(t *testing.T) {
	app := newRecordingAclQuotaApp()
	executor, store := newAclQuotaExecutor(t, app, false, false)
	session := newSDKCatalogSession(t, VisibleCatalog(*mustLoadPolicy(t, store)), executor)
	listed, err := session.ListTools(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, listed.Tools, 51)

	for _, name := range []string{"listAcls", "getAclAsCsv", "listQuotas"} {
		require.NotNil(t, findTool(t, listed.Tools, name))
	}
	for _, name := range []string{
		"createAcl", "deleteAcl", "syncAclsCsv", "createConsumerAcl",
		"createProducerAcl", "createStreamAppAcl", "upsertClientQuotas",
	} {
		require.Nil(t, findToolOrNil(listed.Tools, name))
	}
}

func newAclQuotaExecutor(
	t *testing.T,
	app *recordingAclQuotaApp,
	readOnly bool,
	allowWrites bool,
) (*Executor, *mcppolicy.Store) {
	t.Helper()
	store := mcppolicy.NewStore(filepath.Join(t.TempDir(), "mcp-policy.json"))
	require.NoError(t, store.Save(context.Background(), *policy(true, allowWrites)))
	executor, err := NewExecutor(Dependencies{
		Acls: app, Quotas: app, Policy: store,
		IsReadOnly: func(string) bool { return readOnly },
	})
	require.NoError(t, err)
	return executor, store
}

func callAclQuotaTool(
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

func cloneClientQuota(quota domaincluster.ClientQuota) domaincluster.ClientQuota {
	cloned := quota
	cloned.Quotas = make(map[string]float64, len(quota.Quotas))
	for name, value := range quota.Quotas {
		cloned.Quotas[name] = value
	}
	return cloned
}

func TestAclToolsRejectInvalidFiltersAndBindingsBeforeDelegation(t *testing.T) {
	validBody := func() map[string]any {
		return map[string]any{
			"principal": "User:alice", "host": "*", "resourceType": "TOPIC",
			"resourceName": "orders", "namePatternType": "LITERAL",
			"operation": "READ", "permission": "ALLOW",
		}
	}
	tests := []struct {
		name  string
		tool  string
		input map[string]any
	}{
		{name: "unknown filter resource type", tool: "listAcls", input: map[string]any{
			"clusterName": "prod", "query": map[string]any{"resourceType": "BROKER"},
		}},
		{name: "unknown filter pattern", tool: "getAclAsCsv", input: map[string]any{
			"clusterName": "prod", "query": map[string]any{"namePatternType": "ANY"},
		}},
		{name: "oversized filter resource", tool: "listAcls", input: map[string]any{
			"clusterName": "prod", "query": map[string]any{"resourceName": strings.Repeat("r", 1025)},
		}},
		{name: "oversized filter search", tool: "listAcls", input: map[string]any{
			"clusterName": "prod", "query": map[string]any{"search": strings.Repeat("s", 1025)},
		}},
		{name: "malformed principal", tool: "createAcl", input: map[string]any{
			"clusterName": "prod", "body": withAclField(validBody(), "principal", "alice"),
		}},
		{name: "oversized principal", tool: "createAcl", input: map[string]any{
			"clusterName": "prod", "body": withAclField(validBody(), "principal", "User:"+strings.Repeat("a", 1025)),
		}},
		{name: "invalid host syntax", tool: "createAcl", input: map[string]any{
			"clusterName": "prod", "body": withAclField(validBody(), "host", "broker.example.test"),
		}},
		{name: "oversized host", tool: "createAcl", input: map[string]any{
			"clusterName": "prod", "body": withAclField(validBody(), "host", strings.Repeat("1", 1025)),
		}},
		{name: "invalid resource type", tool: "createAcl", input: map[string]any{
			"clusterName": "prod", "body": withAclField(validBody(), "resourceType", "USER"),
		}},
		{name: "invalid create match pattern", tool: "createAcl", input: map[string]any{
			"clusterName": "prod", "body": withAclField(validBody(), "namePatternType", "MATCH"),
		}},
		{name: "invalid operation", tool: "createAcl", input: map[string]any{
			"clusterName": "prod", "body": withAclField(validBody(), "operation", "UNKNOWN"),
		}},
		{name: "invalid permission", tool: "createAcl", input: map[string]any{
			"clusterName": "prod", "body": withAclField(validBody(), "permission", "MAYBE"),
		}},
		{name: "blank resource name", tool: "deleteAcl", input: map[string]any{
			"clusterName": "prod", "body": withAclField(validBody(), "resourceName", " "),
		}},
		{name: "empty helper expansion", tool: "createConsumerAcl", input: map[string]any{
			"clusterName": "prod", "body": map[string]any{"principal": "User:alice"},
		}},
		{name: "duplicate helper topics", tool: "createProducerAcl", input: map[string]any{
			"clusterName": "prod", "body": map[string]any{
				"principal": "User:alice", "topics": []any{"orders", "orders"},
			},
		}},
		{name: "too many helper topics", tool: "createStreamAppAcl", input: map[string]any{
			"clusterName": "prod", "body": map[string]any{
				"principal": "User:alice", "inputTopics": stringValues(501),
				"applicationId": "app",
			},
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newRecordingAclQuotaApp()
			executor, _ := newAclQuotaExecutor(t, app, false, true)
			result := callAclQuotaTool(t, executor, test.tool, test.input, true)
			require.Equal(t, "invalid_request", callToolText(t, result))
			require.Empty(t, app.snapshot())
		})
	}
}

func TestAclToolsAcceptEverySupportedEnum(t *testing.T) {
	for _, resourceType := range []string{
		"UNKNOWN", "TOPIC", "GROUP", "CLUSTER", "TRANSACTIONAL_ID", "DELEGATION_TOKEN", "USER",
	} {
		t.Run("filter resource "+resourceType, func(t *testing.T) {
			app := newRecordingAclQuotaApp()
			executor, _ := newAclQuotaExecutor(t, app, false, false)
			result := callAclQuotaTool(t, executor, "listAcls", map[string]any{
				"clusterName": "prod", "query": map[string]any{"resourceType": resourceType},
			}, false)
			require.False(t, result.IsError)
			require.Len(t, app.snapshot(), 1)
		})
	}
	for _, pattern := range []string{"MATCH", "LITERAL", "PREFIXED"} {
		t.Run("filter pattern "+pattern, func(t *testing.T) {
			app := newRecordingAclQuotaApp()
			executor, _ := newAclQuotaExecutor(t, app, false, false)
			result := callAclQuotaTool(t, executor, "listAcls", map[string]any{
				"clusterName": "prod", "query": map[string]any{"namePatternType": pattern},
			}, false)
			require.False(t, result.IsError)
		})
	}

	for _, operation := range []string{
		"ALL", "READ", "WRITE", "CREATE", "DELETE", "ALTER", "DESCRIBE",
		"CLUSTER_ACTION", "DESCRIBE_CONFIGS", "ALTER_CONFIGS", "IDEMPOTENT_WRITE",
		"CREATE_TOKENS", "DESCRIBE_TOKENS",
	} {
		t.Run("write operation "+operation, func(t *testing.T) {
			app := newRecordingAclQuotaApp()
			executor, _ := newAclQuotaExecutor(t, app, false, true)
			body := map[string]any{
				"principal": "User:alice", "host": "*", "resourceType": "TOPIC",
				"resourceName": "orders", "namePatternType": "LITERAL",
				"operation": operation, "permission": "ALLOW",
			}
			result := callAclQuotaTool(t, executor, "createAcl", map[string]any{
				"clusterName": "prod", "body": body,
			}, false)
			require.False(t, result.IsError)
		})
	}
	for _, permission := range []string{"ALLOW", "DENY"} {
		t.Run("write permission "+permission, func(t *testing.T) {
			app := newRecordingAclQuotaApp()
			executor, _ := newAclQuotaExecutor(t, app, false, true)
			body := map[string]any{
				"principal": "User:alice", "host": "*", "resourceType": "TOPIC",
				"resourceName": "orders", "namePatternType": "LITERAL",
				"operation": "READ", "permission": permission,
			}
			result := callAclQuotaTool(t, executor, "createAcl", map[string]any{
				"clusterName": "prod", "body": body,
			}, false)
			require.False(t, result.IsError)
		})
	}
}

func TestAclToolsMapPresentEmptyOptionalDTOFieldsLikeHTTP(t *testing.T) {
	app := newRecordingAclQuotaApp()
	executor, _ := newAclQuotaExecutor(t, app, false, true)
	result := callAclQuotaTool(t, executor, "createProducerAcl", map[string]any{
		"clusterName": "prod",
		"body": map[string]any{
			"principal":            "User:alice",
			"topics":               []any{"orders"},
			"topicsPrefix":         "",
			"transactionalId":      "",
			"transactionsIdPrefix": "",
		},
	}, false)
	require.False(t, result.IsError, callToolText(t, result))
	require.Equal(t, []aclQuotaCall{{
		name: "createProducerAcl", cluster: "prod",
		producer: appcluster.ProducerAclSpec{
			Principal: "User:alice", Host: "*", Topics: []string{"orders"},
		},
	}}, app.snapshot())
}

func TestAclToolsCapExactHelperExpansionBeforeDelegation(t *testing.T) {
	t.Run("consumer exact five hundred expanded bindings delegates", func(t *testing.T) {
		app := newRecordingAclQuotaApp()
		executor, _ := newAclQuotaExecutor(t, app, false, true)
		result := callAclQuotaTool(t, executor, "createConsumerAcl", map[string]any{
			"clusterName": "prod",
			"body": map[string]any{
				"principal": "User:alice",
				"topics":    stringValues(250),
			},
		}, false)
		require.False(t, result.IsError, callToolText(t, result))
		calls := app.snapshot()
		require.Len(t, calls, 1)
		require.Len(t, calls[0].consumer.Topics, 250)
	})

	t.Run("consumer first resource beyond expansion bound is rejected", func(t *testing.T) {
		app := newRecordingAclQuotaApp()
		executor, _ := newAclQuotaExecutor(t, app, false, true)
		result := callAclQuotaTool(t, executor, "createConsumerAcl", map[string]any{
			"clusterName": "prod",
			"body": map[string]any{
				"principal": "User:alice",
				"topics":    stringValues(251),
			},
		}, true)
		require.Equal(t, "invalid_request", callToolText(t, result))
		require.Empty(t, app.snapshot())
	})

	t.Run("producer exact five hundred expanded bindings delegates", func(t *testing.T) {
		app := newRecordingAclQuotaApp()
		executor, _ := newAclQuotaExecutor(t, app, false, true)
		result := callAclQuotaTool(t, executor, "createProducerAcl", map[string]any{
			"clusterName": "prod",
			"body": map[string]any{
				"principal":       "User:alice",
				"topics":          stringValues(166),
				"transactionalId": "txn",
				"idempotent":      false,
			},
		}, false)
		require.False(t, result.IsError, callToolText(t, result))
		calls := app.snapshot()
		require.Len(t, calls, 1)
		require.Len(t, calls[0].producer.Topics, 166)
		require.Equal(t, "txn", calls[0].producer.TransactionalID)
		require.False(t, calls[0].producer.Idempotent)
	})

	t.Run("producer idempotent binding is exact plus one and rejected", func(t *testing.T) {
		app := newRecordingAclQuotaApp()
		executor, _ := newAclQuotaExecutor(t, app, false, true)
		result := callAclQuotaTool(t, executor, "createProducerAcl", map[string]any{
			"clusterName": "prod",
			"body": map[string]any{
				"principal":       "User:alice",
				"topics":          stringValues(166),
				"transactionalId": "txn",
				"idempotent":      true,
			},
		}, true)
		require.Equal(t, "invalid_request", callToolText(t, result))
		require.Empty(t, app.snapshot())
	})

	t.Run("stream exact five hundred expanded bindings delegates", func(t *testing.T) {
		app := newRecordingAclQuotaApp()
		executor, _ := newAclQuotaExecutor(t, app, false, true)
		result := callAclQuotaTool(t, executor, "createStreamAppAcl", map[string]any{
			"clusterName": "prod",
			"body": map[string]any{
				"principal":     "User:alice",
				"inputTopics":   stringValues(498),
				"applicationId": "stream-app",
			},
		}, false)
		require.False(t, result.IsError, callToolText(t, result))
		calls := app.snapshot()
		require.Len(t, calls, 1)
		require.Len(t, calls[0].stream.InputTopics, 498)
		require.Equal(t, "stream-app", calls[0].stream.ApplicationID)
	})

	t.Run("stream first output beyond expansion bound is rejected", func(t *testing.T) {
		app := newRecordingAclQuotaApp()
		executor, _ := newAclQuotaExecutor(t, app, false, true)
		result := callAclQuotaTool(t, executor, "createStreamAppAcl", map[string]any{
			"clusterName": "prod",
			"body": map[string]any{
				"principal":     "User:alice",
				"inputTopics":   stringValues(498),
				"outputTopics":  []any{"out"},
				"applicationId": "stream-app",
			},
		}, true)
		require.Equal(t, "invalid_request", callToolText(t, result))
		require.Empty(t, app.snapshot())
	})
}

func TestAclToolsBoundAndValidateRawRowsBeforeCopyOrSort(t *testing.T) {
	t.Run("raw ACL slice over bound fails closed", func(t *testing.T) {
		app := newRecordingAclQuotaApp()
		app.acls = make([]domaincluster.AclBinding, 2001)
		executor, _ := newAclQuotaExecutor(t, app, false, false)
		result := callAclQuotaTool(t, executor, "listAcls", map[string]any{
			"clusterName": "prod",
		}, true)
		require.Equal(t, "result_too_large", callToolText(t, result))
		require.Equal(t, []aclQuotaCall{{name: "listAcls", cluster: "prod"}}, app.snapshot())
	})

	t.Run("bad raw enum fails closed", func(t *testing.T) {
		app := newRecordingAclQuotaApp()
		app.acls = []domaincluster.AclBinding{{
			Principal: "User:alice", Host: "*", ResourceType: "BROKER",
			ResourceName: "orders", PatternType: "LITERAL", Operation: "READ", Permission: "ALLOW",
		}}
		executor, _ := newAclQuotaExecutor(t, app, false, false)
		result := callAclQuotaTool(t, executor, "listAcls", map[string]any{
			"clusterName": "prod",
		}, true)
		require.Equal(t, "operation_failed", callToolText(t, result))
	})

	t.Run("accepted raw rows are sorted then capped", func(t *testing.T) {
		app := newRecordingAclQuotaApp()
		app.acls = make([]domaincluster.AclBinding, 501)
		for index := 500; index >= 0; index-- {
			app.acls[500-index] = domaincluster.AclBinding{
				Principal: "User:" + fmt.Sprintf("%04d", index), Host: "*",
				ResourceType: "TOPIC", ResourceName: "topic", PatternType: "LITERAL",
				Operation: "READ", Permission: "ALLOW",
			}
		}
		executor, _ := newAclQuotaExecutor(t, app, false, false)
		result := callAclQuotaTool(t, executor, "listAcls", map[string]any{
			"clusterName": "prod",
		}, false)
		var output struct {
			Result []generated.KafkaAcl `json:"result"`
		}
		require.NoError(t, jsonUnmarshalResult(result, &output))
		require.Len(t, output.Result, 500)
		require.Equal(t, "User:0000", output.Result[0].Principal)
		require.Equal(t, "User:0499", output.Result[499].Principal)
	})
}

func TestAclToolsBoundCSVAndRejectUnsafeSyncBeforeDelegation(t *testing.T) {
	t.Run("formatted CSV over byte bound is rejected", func(t *testing.T) {
		app := newRecordingAclQuotaApp()
		app.formatText = strings.Repeat("x", maxCSVBytes+1)
		executor, _ := newAclQuotaExecutor(t, app, false, false)
		result := callAclQuotaTool(t, executor, "getAclAsCsv", map[string]any{
			"clusterName": "prod",
		}, true)
		require.Equal(t, "result_too_large", callToolText(t, result))
	})

	t.Run("CSV renderer safely quotes delimiters and quote characters", func(t *testing.T) {
		app := newRecordingAclQuotaApp()
		app.acls = []domaincluster.AclBinding{{
			Principal: "User:alice", Host: "*", ResourceType: "TOPIC",
			ResourceName: "orders,\"line-2", PatternType: "LITERAL",
			Operation: "READ", Permission: "ALLOW",
		}}
		executor, _ := newAclQuotaExecutor(t, app, false, false)
		result := callAclQuotaTool(t, executor, "getAclAsCsv", map[string]any{
			"clusterName": "prod",
		}, false)
		require.Contains(t, callToolText(t, result), `orders,\"\"line-2`)
	})

	header := "Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\n"
	for _, test := range []struct {
		name string
		body string
	}{
		{name: "empty", body: ""},
		{name: "malformed", body: "not,the,acl,header\n"},
		{name: "oversized", body: strings.Repeat("x", maxCSVBytes+1)},
		{name: "too many rows", body: header + aclCSVRows(501)},
		{name: "bad actor", body: header + "alice,TOPIC,LITERAL,orders,READ,ALLOW,*\n"},
	} {
		t.Run("sync "+test.name, func(t *testing.T) {
			app := newRecordingAclQuotaApp()
			executor, _ := newAclQuotaExecutor(t, app, false, true)
			result := callAclQuotaTool(t, executor, "syncAclsCsv", map[string]any{
				"clusterName": "prod", "body": test.body,
			}, true)
			require.Equal(t, "invalid_request", callToolText(t, result))
			require.Empty(t, app.snapshot())
		})
	}

	t.Run("exactly five hundred rows delegate", func(t *testing.T) {
		app := newRecordingAclQuotaApp()
		executor, _ := newAclQuotaExecutor(t, app, false, true)
		body := header + aclCSVRows(500)
		result := callAclQuotaTool(t, executor, "syncAclsCsv", map[string]any{
			"clusterName": "prod", "body": body,
		}, false)
		require.False(t, result.IsError)
		require.Equal(t, []aclQuotaCall{{name: "syncAclsCsv", cluster: "prod", csv: body}}, app.snapshot())
	})
}

func TestAclToolsFormulaSafeCopyNeutralizesEveryExportedCellAndPrefix(t *testing.T) {
	for _, prefix := range []string{"=", "+", "-", "@", "\t", "\r"} {
		t.Run(fmt.Sprintf("%q", prefix), func(t *testing.T) {
			value := prefix + "payload"
			original := domaincluster.AclBinding{
				Principal: value, Host: value, ResourceType: value,
				ResourceName: value, PatternType: value, Operation: value,
				Permission: value,
			}
			safe, err := formulaSafeMCPAcls([]domaincluster.AclBinding{original})
			require.NoError(t, err)
			require.Equal(t, []domaincluster.AclBinding{{
				Principal: "'" + value, Host: "'" + value, ResourceType: "'" + value,
				ResourceName: "'" + value, PatternType: "'" + value, Operation: "'" + value,
				Permission: "'" + value,
			}}, safe)
			require.Equal(t, value, original.Principal)
			require.Equal(t, value, original.ResourceName)
		})
	}

	original := domaincluster.AclBinding{
		Principal: "User:alice", Host: "*", ResourceType: "TOPIC",
		ResourceName: "orders", PatternType: "LITERAL", Operation: "READ",
		Permission: "ALLOW",
	}
	safe, err := formulaSafeMCPAcls([]domaincluster.AclBinding{original})
	require.NoError(t, err)
	require.Equal(t, []domaincluster.AclBinding{original}, safe)
}

func TestAclToolsCSVFormulaSafetyHasExactSDKTextAndStructuredContent(t *testing.T) {
	app := newRecordingAclQuotaApp()
	app.acls = []domaincluster.AclBinding{{
		Principal: "User:alice", Host: "*", ResourceType: "TOPIC",
		ResourceName: "=1+1", PatternType: "LITERAL", Operation: "READ",
		Permission: "ALLOW",
	}}
	executor, _ := newAclQuotaExecutor(t, app, false, false)
	result := callAclQuotaTool(t, executor, "getAclAsCsv", map[string]any{
		"clusterName": "prod",
	}, false)
	want := `{"result":"Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\nUser:alice,TOPIC,LITERAL,'=1+1,READ,ALLOW,*\n"}`
	require.JSONEq(t, want, callToolText(t, result))
	requireStructuredJSONEq(t, want, result.StructuredContent)
	require.Equal(t, "=1+1", app.acls[0].ResourceName)
	require.Equal(t, "'=1+1", app.formattedSnapshot()[0][0].ResourceName)
}

func TestAclToolsCountEveryRawCSVLogicalRowBeforeParseOrDelegation(t *testing.T) {
	header := "Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\n"
	duplicate := "User:alice,TOPIC,LITERAL,orders,READ,ALLOW,*\n"

	t.Run("five hundred duplicate rows remain within the raw bound", func(t *testing.T) {
		app := newRecordingAclQuotaApp()
		executor, _ := newAclQuotaExecutor(t, app, false, true)
		body := header + strings.Repeat(duplicate, 500)
		result := callAclQuotaTool(t, executor, "syncAclsCsv", map[string]any{
			"clusterName": "prod", "body": body,
		}, false)
		require.False(t, result.IsError, callToolText(t, result))
		require.Equal(t, []aclQuotaCall{{
			name: "syncAclsCsv", cluster: "prod", csv: body,
		}}, app.snapshot())
	})

	t.Run("five hundred and one duplicate rows fail before app delegation", func(t *testing.T) {
		app := newRecordingAclQuotaApp()
		executor, _ := newAclQuotaExecutor(t, app, false, true)
		body := header + strings.Repeat(duplicate, 501)
		result := callAclQuotaTool(t, executor, "syncAclsCsv", map[string]any{
			"clusterName": "prod", "body": body,
		}, true)
		require.Equal(t, "invalid_request", callToolText(t, result))
		require.Empty(t, app.snapshot())
	})
}

func TestAclToolsLogicalCSVRowCounterHandlesMultilineAndBlankRecords(t *testing.T) {
	header := "Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\n"
	regular := "User:alice,TOPIC,LITERAL,orders,READ,ALLOW,*\n"
	multiline := "User:alice,TOPIC,LITERAL,\"orders\ncontinued\",READ,ALLOW,*\n"

	t.Run("quoted multiline value is one logical record", func(t *testing.T) {
		body := header + multiline + strings.Repeat(regular, 499)
		require.NoError(t, validateMCPAClCSVLogicalRows(body, 500))
		require.ErrorIs(
			t,
			validateMCPAClCSVLogicalRows(body+regular, 500),
			errInvalidRequest,
		)
	})

	t.Run("physical blank lines do not consume the record budget", func(t *testing.T) {
		var body strings.Builder
		body.WriteString("\n \n")
		body.WriteString(header)
		for index := 0; index < 500; index++ {
			body.WriteString("\n")
			body.WriteString(regular)
			body.WriteString(" \n")
		}
		require.NoError(t, validateMCPAClCSVLogicalRows(body.String(), 500))
	})

	t.Run("malformed quoted record fails safely", func(t *testing.T) {
		require.ErrorIs(
			t,
			validateMCPAClCSVLogicalRows(header+"\"unterminated", 500),
			errInvalidRequest,
		)
	})
}

func TestQuotaToolsValidateEntityKeysValuesAndAmbiguity(t *testing.T) {
	tests := []struct {
		name string
		body map[string]any
	}{
		{name: "missing entity", body: map[string]any{"quotas": map[string]any{"producer_byte_rate": 1}}},
		{name: "blank entity", body: map[string]any{"user": " ", "quotas": map[string]any{"producer_byte_rate": 1}}},
		{name: "oversized entity", body: map[string]any{"user": strings.Repeat("u", 1025)}},
		{name: "unknown quota key", body: map[string]any{"user": "alice", "quotas": map[string]any{"password": 1}}},
		{name: "negative quota value", body: map[string]any{"user": "alice", "quotas": map[string]any{"producer_byte_rate": -1}}},
		{name: "too many quota keys", body: map[string]any{"user": "alice", "quotas": quotaValues(33)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newRecordingAclQuotaApp()
			executor, _ := newAclQuotaExecutor(t, app, false, true)
			result := callAclQuotaTool(t, executor, "upsertClientQuotas", map[string]any{
				"clusterName": "prod", "body": test.body,
			}, true)
			require.Equal(t, "invalid_request", callToolText(t, result))
			require.Empty(t, app.snapshot())
		})
	}

	for _, key := range []string{
		"producer_byte_rate", "consumer_byte_rate", "request_percentage",
		"controller_mutation_rate", "connection_creation_rate",
	} {
		t.Run("supported key "+key, func(t *testing.T) {
			app := newRecordingAclQuotaApp()
			executor, _ := newAclQuotaExecutor(t, app, false, true)
			result := callAclQuotaTool(t, executor, "upsertClientQuotas", map[string]any{
				"clusterName": "prod", "body": map[string]any{
					"user": "alice", "quotas": map[string]any{key: float64(1)},
				},
			}, false)
			require.False(t, result.IsError)
		})
	}

	t.Run("non-finite direct DTO value is rejected", func(t *testing.T) {
		user := "alice"
		values := map[string]float32{"producer_byte_rate": float32(math.Inf(1))}
		_, err := clientQuotaFromMCP(generated.ClientQuotas{User: &user, Quotas: &values})
		require.ErrorIs(t, err, errInvalidRequest)
	})
}

func TestQuotaToolsListCanonicalDefaultEntityLikeHTTP(t *testing.T) {
	tests := []struct {
		name     string
		quota    domaincluster.ClientQuota
		wantJSON string
	}{
		{
			name:     "default entity without quotas",
			quota:    domaincluster.ClientQuota{Quotas: map[string]float64{}},
			wantJSON: `{"result":[{}]}`,
		},
		{
			name: "default entity with quotas",
			quota: domaincluster.ClientQuota{
				Quotas: map[string]float64{"producer_byte_rate": 1024},
			},
			wantJSON: `{"result":[{"quotas":{"producer_byte_rate":1024}}]}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newRecordingAclQuotaApp()
			app.quotas = []domaincluster.ClientQuota{test.quota}
			executor, _ := newAclQuotaExecutor(t, app, false, false)
			result := callAclQuotaTool(t, executor, "listQuotas", map[string]any{
				"clusterName": "prod",
			}, false)
			require.JSONEq(t, test.wantJSON, callToolText(t, result))
			requireStructuredJSONEq(t, test.wantJSON, result.StructuredContent)
		})
	}

	t.Run("duplicate default identities fail closed", func(t *testing.T) {
		app := newRecordingAclQuotaApp()
		app.quotas = []domaincluster.ClientQuota{
			{Quotas: map[string]float64{"producer_byte_rate": 1}},
			{Quotas: map[string]float64{"consumer_byte_rate": 2}},
		}
		executor, _ := newAclQuotaExecutor(t, app, false, false)
		result := callAclQuotaTool(t, executor, "listQuotas", map[string]any{
			"clusterName": "prod",
		}, true)
		require.Equal(t, "operation_failed", callToolText(t, result))
	})
}

func TestQuotaToolsRejectEveryExplicitBlankDimensionBeforeDelegation(t *testing.T) {
	tests := []struct {
		name string
		body map[string]any
	}{
		{
			name: "empty client id alongside valid user",
			body: map[string]any{"user": "alice", "clientId": ""},
		},
		{
			name: "whitespace client id alongside valid user",
			body: map[string]any{"user": "alice", "clientId": " \t"},
		},
		{
			name: "empty ip alongside valid user",
			body: map[string]any{"user": "alice", "ip": ""},
		},
		{
			name: "whitespace ip alongside valid user",
			body: map[string]any{"user": "alice", "ip": " \t"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newRecordingAclQuotaApp()
			executor, _ := newAclQuotaExecutor(t, app, false, true)
			result := callAclQuotaTool(t, executor, "upsertClientQuotas", map[string]any{
				"clusterName": "prod", "body": test.body,
			}, true)
			require.Equal(t, "invalid_request", callToolText(t, result))
			require.Empty(t, app.snapshot())
		})
	}
}

func TestQuotaToolsBoundValidateAndSortRawResults(t *testing.T) {
	t.Run("raw quota slice over bound fails before processing", func(t *testing.T) {
		app := newRecordingAclQuotaApp()
		app.quotas = make([]domaincluster.ClientQuota, 2001)
		executor, _ := newAclQuotaExecutor(t, app, false, false)
		result := callAclQuotaTool(t, executor, "listQuotas", map[string]any{
			"clusterName": "prod",
		}, true)
		require.Equal(t, "result_too_large", callToolText(t, result))
	})

	t.Run("duplicate entity is ambiguous", func(t *testing.T) {
		app := newRecordingAclQuotaApp()
		app.quotas = []domaincluster.ClientQuota{
			{User: "alice", Quotas: map[string]float64{"producer_byte_rate": 1}},
			{User: "alice", Quotas: map[string]float64{"producer_byte_rate": 2}},
		}
		executor, _ := newAclQuotaExecutor(t, app, false, false)
		result := callAclQuotaTool(t, executor, "listQuotas", map[string]any{
			"clusterName": "prod",
		}, true)
		require.Equal(t, "operation_failed", callToolText(t, result))
	})

	for _, test := range []struct {
		name  string
		quota domaincluster.ClientQuota
	}{
		{name: "unknown raw key", quota: domaincluster.ClientQuota{User: "alice", Quotas: map[string]float64{"secret": 1}}},
		{name: "non-finite raw value", quota: domaincluster.ClientQuota{User: "alice", Quotas: map[string]float64{"producer_byte_rate": math.NaN()}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			app := newRecordingAclQuotaApp()
			app.quotas = []domaincluster.ClientQuota{test.quota}
			executor, _ := newAclQuotaExecutor(t, app, false, false)
			result := callAclQuotaTool(t, executor, "listQuotas", map[string]any{
				"clusterName": "prod",
			}, true)
			require.Equal(t, "operation_failed", callToolText(t, result))
		})
	}
}

func TestAclToolsAndQuotaToolsEnforceReadOnlyClusterForEveryWrite(t *testing.T) {
	validACL := map[string]any{
		"principal": "User:alice", "host": "*", "resourceType": "TOPIC",
		"resourceName": "orders", "namePatternType": "LITERAL",
		"operation": "READ", "permission": "ALLOW",
	}
	csv := "Principal,ResourceType,PatternType,ResourceName,Operation,PermissionType,Host\n" +
		"User:alice,TOPIC,LITERAL,orders,READ,ALLOW,*\n"
	tests := []struct {
		name  string
		input map[string]any
	}{
		{name: "createAcl", input: map[string]any{"clusterName": "prod", "body": validACL}},
		{name: "deleteAcl", input: map[string]any{"clusterName": "prod", "body": validACL}},
		{name: "syncAclsCsv", input: map[string]any{"clusterName": "prod", "body": csv}},
		{name: "createConsumerAcl", input: map[string]any{
			"clusterName": "prod", "body": map[string]any{"principal": "User:alice", "topics": []any{"orders"}},
		}},
		{name: "createProducerAcl", input: map[string]any{
			"clusterName": "prod", "body": map[string]any{"principal": "User:alice", "topics": []any{"orders"}},
		}},
		{name: "createStreamAppAcl", input: map[string]any{
			"clusterName": "prod", "body": map[string]any{"principal": "User:alice", "applicationId": "app"},
		}},
		{name: "upsertClientQuotas", input: map[string]any{
			"clusterName": "prod", "body": map[string]any{"user": "alice", "quotas": map[string]any{}},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			app := newRecordingAclQuotaApp()
			executor, _ := newAclQuotaExecutor(t, app, true, true)
			result := callAclQuotaTool(t, executor, test.name, test.input, true)
			require.Equal(t, "cluster_read_only", callToolText(t, result))
			require.Empty(t, app.snapshot())
		})
	}
}

func TestAclToolsAndQuotaToolsDoNotExposeFileURLCommandOrSecurityConfigInputs(t *testing.T) {
	for _, name := range []string{
		"listAcls", "getAclAsCsv", "createAcl", "deleteAcl", "syncAclsCsv",
		"createConsumerAcl", "createProducerAcl", "createStreamAppAcl",
		"listQuotas", "upsertClientQuotas",
	} {
		t.Run(name, func(t *testing.T) {
			executor, _ := newAclQuotaExecutor(t, newRecordingAclQuotaApp(), false, true)
			session := newSDKSession(t, requireCatalogSpec(t, name), executor)
			listed, err := session.ListTools(context.Background(), nil)
			require.NoError(t, err)
			schema := fmt.Sprint(findTool(t, listed.Tools, name).InputSchema)
			for _, forbidden := range []string{
				"filePath", "url", "command", "environment", "bootstrapServers",
				"sasl", "password", "securityProtocol",
			} {
				require.NotContains(t, schema, forbidden)
			}
		})
	}
}

func withAclField(body map[string]any, name string, value any) map[string]any {
	body[name] = value
	return body
}

func stringValues(count int) []any {
	values := make([]any, count)
	for index := range values {
		values[index] = fmt.Sprintf("value-%04d", index)
	}
	return values
}

func aclCSVRows(count int) string {
	var output strings.Builder
	for index := 0; index < count; index++ {
		output.WriteString("User:user-")
		output.WriteString(strconv.Itoa(index))
		output.WriteString(",TOPIC,LITERAL,topic-")
		output.WriteString(strconv.Itoa(index))
		output.WriteString(",READ,ALLOW,*\n")
	}
	return output.String()
}

func quotaValues(count int) map[string]any {
	values := make(map[string]any, count)
	for index := 0; index < count; index++ {
		values[fmt.Sprintf("key-%02d", index)] = float64(index)
	}
	return values
}
