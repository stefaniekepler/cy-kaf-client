package mcpserver

import (
	"context"
	"sort"
	"testing"

	"github.com/cy-kaf/cy-kaf-client/internal/mcppolicy"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

func TestCatalogMatchesFrozenKafbatV150Runtime(t *testing.T) {
	expectedNames := []string{
		"analyzeTopic",
		"cancelTopicAnalysis",
		"changeReplicationFactor",
		"checkSchemaCompatibility",
		"cloneTopic",
		"createAcl",
		"createConnector",
		"createConsumerAcl",
		"createNewSchema",
		"createProducerAcl",
		"createStreamAppAcl",
		"createTopic",
		"deleteAcl",
		"deleteConnector",
		"deleteConsumerGroup",
		"deleteConsumerGroupOffsets",
		"deleteLatestSchema",
		"deleteSchema",
		"deleteSchemaByVersion",
		"deleteTopic",
		"deleteTopicMessages",
		"executeKsql",
		"executeSmartFilterTest",
		"getAclAsCsv",
		"getActiveProducerStates",
		"getAllBrokersLogdirs",
		"getAllConnectors",
		"getAllConnectorsCsv",
		"getAllVersionsBySubject",
		"getBrokerConfig",
		"getBrokers",
		"getBrokersCsv",
		"getBrokersMetrics",
		"getClusterMetrics",
		"getClusterStats",
		"getClusters",
		"getConnector",
		"getConnectorConfig",
		"getConnectorPlugins",
		"getConnectorTasks",
		"getConnectors",
		"getConnects",
		"getConnectsCsv",
		"getConsumerGroup",
		"getConsumerGroupsCsv",
		"getConsumerGroupsLag",
		"getConsumerGroupsPage",
		"getGlobalSchemaCompatibilityLevel",
		"getLatestSchema",
		"getSchemaByVersion",
		"getSchemas",
		"getSerdes",
		"getTopicAnalysis",
		"getTopicConfigs",
		"getTopicConnectors",
		"getTopicConsumerGroups",
		"getTopicDetails",
		"getTopicMessagesV2",
		"getTopics",
		"getTopicsCsv",
		"increaseTopicPartitions",
		"listAcls",
		"listQuotas",
		"listStreams",
		"listTables",
		"listTopicAcls",
		"openKsqlResponsePipe",
		"recreateTopic",
		"registerFilter",
		"resetConnectorOffsets",
		"resetConsumerGroupOffsets",
		"restartConnectorTask",
		"sendTopicMessages",
		"setConnectorConfig",
		"syncAclsCsv",
		"updateBrokerConfigByName",
		"updateBrokerTopicPartitionLogDir",
		"updateClusterInfo",
		"updateConnectorState",
		"updateGlobalSchemaCompatibilityLevel",
		"updateSchemaCompatibilityLevel",
		"updateTopic",
		"upsertClientQuotas",
		"validateConnectorPluginConfig",
	}

	all := Catalog()
	readVisible := VisibleCatalog(mcppolicy.Policy{
		Version:     mcppolicy.CurrentVersion,
		Enabled:     true,
		AllowWrites: false,
	})
	writeOnly := make([]ToolSpec, 0)
	names := make([]string, 0, len(all))
	seen := make(map[string]struct{}, len(all))

	for _, spec := range all {
		require.NotEmpty(t, spec.Meta.Name)
		require.NotEmpty(t, spec.Meta.Title, spec.Meta.Name)
		require.NotEmpty(t, spec.Meta.Description, spec.Meta.Name)
		require.NotEmpty(t, spec.Meta.Subsystem, spec.Meta.Name)
		require.NotNil(t, spec.Register, spec.Meta.Name)
		require.Positive(t, spec.Meta.Timeout, spec.Meta.Name)
		require.Positive(t, spec.Meta.MaxBytes, spec.Meta.Name)
		require.NotContains(t, seen, spec.Meta.Name)
		seen[spec.Meta.Name] = struct{}{}
		names = append(names, spec.Meta.Name)
		if spec.Meta.Access == AccessWrite {
			writeOnly = append(writeOnly, spec)
		}
	}
	sort.Strings(names)

	require.Len(t, all, 84)
	require.Len(t, readVisible, 51)
	require.Len(t, writeOnly, 33)
	require.NotContains(t, names, "getTopicMessages")
	require.Contains(t, names, "getTopicMessagesV2")
	require.Equal(t, expectedNames, names)
}

func TestSDKCatalogEmitsTypedSchemasAndExplicitAnnotations(t *testing.T) {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "catalog-test-server",
		Version: "test",
	}, nil)
	specByName := make(map[string]ToolSpec, len(Catalog()))
	for _, spec := range Catalog() {
		spec.Register(server, &Executor{})
		specByName[spec.Meta.Name] = spec
	}

	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(context.Background(), serverTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, serverSession.Close()) })

	client := mcp.NewClient(&mcp.Implementation{
		Name:    "catalog-test-client",
		Version: "test",
	}, nil)
	clientSession, err := client.Connect(context.Background(), clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, clientSession.Close()) })

	result, err := clientSession.ListTools(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, result.Tools, 84)

	for _, tool := range result.Tools {
		spec, ok := specByName[tool.Name]
		require.True(t, ok, tool.Name)
		require.NotNil(t, tool.InputSchema, tool.Name)
		require.NotNil(t, tool.Annotations, tool.Name)
		require.Equal(t, spec.Meta.Access == AccessReadOnly, tool.Annotations.ReadOnlyHint, tool.Name)
		require.NotNil(t, tool.Annotations.DestructiveHint, tool.Name)
		require.Equal(t, spec.Meta.Destructive, *tool.Annotations.DestructiveHint, tool.Name)
		require.Equal(t, spec.Meta.Idempotent, tool.Annotations.IdempotentHint, tool.Name)
		require.NotNil(t, tool.Annotations.OpenWorldHint, tool.Name)
		require.False(t, *tool.Annotations.OpenWorldHint, tool.Name)
	}

	executeKsql := findTool(t, result.Tools, "executeKsql")
	require.False(t, executeKsql.Annotations.ReadOnlyHint)
}

func TestSDKCatalogEmitsOperationAppropriateInputSchemas(t *testing.T) {
	tools := listSDKCatalogTools(t)

	tests := []struct {
		name           string
		properties     []string
		required       []string
		bodyProperties []string
		bodyRequired   []string
	}{
		{
			name:       "getClusterMetrics",
			properties: []string{"clusterName"},
			required:   []string{"clusterName"},
		},
		{
			name:       "getTopicDetails",
			properties: []string{"clusterName", "topicName"},
			required:   []string{"clusterName", "topicName"},
		},
		{
			name:       "getConnector",
			properties: []string{"clusterName", "connectName", "connectorName"},
			required:   []string{"clusterName", "connectName", "connectorName"},
		},
		{
			name:           "executeKsql",
			properties:     []string{"body", "clusterName"},
			required:       []string{"body", "clusterName"},
			bodyProperties: []string{"ksql", "streamsProperties"},
			bodyRequired:   []string{"ksql"},
		},
		{
			name:           "createTopic",
			properties:     []string{"body", "clusterName"},
			required:       []string{"body", "clusterName"},
			bodyProperties: []string{"configs", "name", "partitions", "replicationFactor"},
			bodyRequired:   []string{"name", "partitions"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := requireSchemaObject(t, findTool(t, tools, tt.name).InputSchema)
			require.ElementsMatch(t, tt.properties, schemaPropertyNames(t, schema))
			require.ElementsMatch(t, tt.required, schemaRequired(t, schema))
			if len(tt.bodyProperties) == 0 {
				return
			}
			body := requireSchemaObject(t, schemaProperty(t, schema, "body"))
			require.ElementsMatch(t, tt.bodyProperties, schemaPropertyNames(t, body))
			require.ElementsMatch(t, tt.bodyRequired, schemaRequired(t, body))
		})
	}
}

func listSDKCatalogTools(t *testing.T) []*mcp.Tool {
	t.Helper()
	server := mcp.NewServer(&mcp.Implementation{Name: "schema-test-server", Version: "test"}, nil)
	for _, spec := range Catalog() {
		spec.Register(server, &Executor{})
	}
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(context.Background(), serverTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, serverSession.Close()) })

	client := mcp.NewClient(&mcp.Implementation{Name: "schema-test-client", Version: "test"}, nil)
	clientSession, err := client.Connect(context.Background(), clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, clientSession.Close()) })
	result, err := clientSession.ListTools(context.Background(), nil)
	require.NoError(t, err)
	return result.Tools
}

func requireSchemaObject(t *testing.T, value any) map[string]any {
	t.Helper()
	schema, ok := value.(map[string]any)
	require.True(t, ok, "schema has type %T", value)
	return schema
}

func schemaPropertyNames(t *testing.T, schema map[string]any) []string {
	t.Helper()
	properties, ok := schema["properties"].(map[string]any)
	require.True(t, ok, "schema properties have type %T", schema["properties"])
	names := make([]string, 0, len(properties))
	for name := range properties {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func schemaRequired(t *testing.T, schema map[string]any) []string {
	t.Helper()
	raw, ok := schema["required"]
	if !ok {
		return nil
	}
	values, ok := raw.([]any)
	require.True(t, ok, "schema required has type %T", raw)
	required := make([]string, 0, len(values))
	for _, value := range values {
		name, ok := value.(string)
		require.True(t, ok, "required entry has type %T", value)
		required = append(required, name)
	}
	sort.Strings(required)
	return required
}

func schemaProperty(t *testing.T, schema map[string]any, name string) any {
	t.Helper()
	properties, ok := schema["properties"].(map[string]any)
	require.True(t, ok)
	property, ok := properties[name]
	require.True(t, ok, "schema property %q is absent", name)
	if propertySchema, ok := property.(map[string]any); ok {
		if ref, ok := propertySchema["$ref"].(string); ok {
			return resolveSchemaRef(t, schema, ref)
		}
	}
	return property
}

func resolveSchemaRef(t *testing.T, root map[string]any, ref string) map[string]any {
	t.Helper()
	const prefix = "#/$defs/"
	require.Contains(t, ref, prefix)
	definitions, ok := root["$defs"].(map[string]any)
	require.True(t, ok, "schema definitions have type %T", root["$defs"])
	resolved, ok := definitions[ref[len(prefix):]].(map[string]any)
	require.True(t, ok, "schema definition %q is absent", ref)
	return resolved
}

func findTool(t *testing.T, tools []*mcp.Tool, name string) *mcp.Tool {
	t.Helper()
	for _, tool := range tools {
		if tool.Name == name {
			return tool
		}
	}
	t.Fatalf("tool %q not found", name)
	return nil
}
