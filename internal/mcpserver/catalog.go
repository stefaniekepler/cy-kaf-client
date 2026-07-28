package mcpserver

import (
	"context"
	"fmt"
	"time"

	"github.com/cy-kaf/cy-kaf-client/internal/mcppolicy"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	defaultToolTimeout = 15 * time.Second
	analysisTimeout    = 30 * time.Second
	maxListItems       = 500
	maxMessageItems    = 100
	maxKsqlRows        = 500
	maxResultBytes     = 1 << 20
	maxCSVBytes        = 1 << 20
)

type AccessClass uint8

const (
	AccessReadOnly AccessClass = iota
	AccessWrite
	AccessConditionalKSQL
)

type ToolMeta struct {
	Name        string
	Title       string
	Description string
	Subsystem   string
	Access      AccessClass
	Destructive bool
	Idempotent  bool
	Timeout     time.Duration
	MaxItems    int
	MaxBytes    int
}

func (m ToolMeta) tool() *mcp.Tool {
	openWorld := false
	destructive := m.Destructive
	return &mcp.Tool{
		Name:        m.Name,
		Title:       m.Title,
		Description: m.Description,
		Annotations: &mcp.ToolAnnotations{
			Title:           m.Title,
			ReadOnlyHint:    m.Access == AccessReadOnly,
			DestructiveHint: &destructive,
			IdempotentHint:  m.Idempotent,
			OpenWorldHint:   &openWorld,
		},
	}
}

type ToolSpec struct {
	Meta     ToolMeta
	Register func(*mcp.Server, *Executor)
}

type clusterSelector[In any] func(In) string
type toolCall[In any] func(context.Context, *Executor, In) (any, error)
type accessSelector[In any] func(context.Context, *Executor, In) (AccessClass, error)

func newTool[In any](
	meta ToolMeta,
	cluster clusterSelector[In],
	access accessSelector[In],
	call toolCall[In],
) ToolSpec {
	return newToolWithInputSchema(meta, nil, cluster, access, call)
}

func newToolWithInputSchema[In any](
	meta ToolMeta,
	inputSchema any,
	cluster clusterSelector[In],
	access accessSelector[In],
	call toolCall[In],
) ToolSpec {
	return ToolSpec{
		Meta: meta,
		Register: func(server *mcp.Server, executor *Executor) {
			tool := meta.tool()
			tool.InputSchema = inputSchema
			mcp.AddTool(server, tool,
				func(ctx context.Context, _ *mcp.CallToolRequest, input In) (*mcp.CallToolResult, any, error) {
					ctx, cancel := context.WithTimeout(ctx, meta.Timeout)
					defer cancel()

					if err := executor.authorize(ctx, meta, "", AccessReadOnly); err != nil {
						return nil, nil, safeToolError(meta.Name, err)
					}
					clusterName := ""
					if cluster != nil {
						clusterName = cluster(input)
					}
					effective := meta.Access
					if access != nil {
						var err error
						effective, err = access(ctx, executor, input)
						if err != nil {
							return nil, nil, safeToolError(meta.Name, err)
						}
					}
					if err := executor.authorize(ctx, meta, clusterName, effective); err != nil {
						return nil, nil, safeToolError(meta.Name, err)
					}
					output, err := call(ctx, executor, input)
					if err != nil {
						return nil, nil, safeToolError(meta.Name, err)
					}
					// Cancellation is cooperative: a callback already in progress
					// cannot be forcibly stopped. Never accept its late success.
					if err := ctx.Err(); err != nil {
						return nil, nil, safeToolError(meta.Name, err)
					}
					return executor.safeResult(meta, output)
				})
		},
	}
}

type catalogDefinition struct {
	name        string
	subsystem   string
	access      AccessClass
	destructive bool
	idempotent  bool
	timeout     time.Duration
	maxItems    int
}

var frozenCatalog = []catalogDefinition{
	{name: "getClusters", subsystem: "Clusters", access: AccessReadOnly, idempotent: true},
	{name: "getClusterMetrics", subsystem: "Clusters", access: AccessReadOnly, idempotent: true},
	{name: "getClusterStats", subsystem: "Clusters", access: AccessReadOnly, idempotent: true},
	{name: "updateClusterInfo", subsystem: "Clusters", access: AccessReadOnly, idempotent: true},

	{name: "getBrokers", subsystem: "Brokers", access: AccessReadOnly, idempotent: true},
	{name: "getBrokersCsv", subsystem: "Brokers", access: AccessReadOnly, idempotent: true},
	{name: "getBrokersMetrics", subsystem: "Brokers", access: AccessReadOnly, idempotent: true},
	{name: "getAllBrokersLogdirs", subsystem: "Brokers", access: AccessReadOnly, idempotent: true},
	{name: "getBrokerConfig", subsystem: "Brokers", access: AccessReadOnly, idempotent: true},
	{name: "updateBrokerTopicPartitionLogDir", subsystem: "Brokers", access: AccessWrite, destructive: true, idempotent: true},
	{name: "updateBrokerConfigByName", subsystem: "Brokers", access: AccessWrite, destructive: true, idempotent: true},

	{name: "getTopicConfigs", subsystem: "Topics", access: AccessReadOnly, idempotent: true},
	{name: "getTopicDetails", subsystem: "Topics", access: AccessReadOnly, idempotent: true},
	{name: "getTopics", subsystem: "Topics", access: AccessReadOnly, idempotent: true},
	{name: "getTopicsCsv", subsystem: "Topics", access: AccessReadOnly, idempotent: true},
	{name: "listTopicAcls", subsystem: "Topics", access: AccessReadOnly, idempotent: true},
	{name: "analyzeTopic", subsystem: "Topics", access: AccessReadOnly, timeout: analysisTimeout},
	{name: "cancelTopicAnalysis", subsystem: "Topics", access: AccessReadOnly, idempotent: true, timeout: analysisTimeout},
	{name: "getTopicAnalysis", subsystem: "Topics", access: AccessReadOnly, idempotent: true},
	{name: "getActiveProducerStates", subsystem: "Topics", access: AccessReadOnly, idempotent: true},
	{name: "getTopicConnectors", subsystem: "Topics", access: AccessReadOnly, idempotent: true},
	{name: "createTopic", subsystem: "Topics", access: AccessWrite},
	{name: "recreateTopic", subsystem: "Topics", access: AccessWrite, destructive: true},
	{name: "cloneTopic", subsystem: "Topics", access: AccessWrite},
	{name: "deleteTopic", subsystem: "Topics", access: AccessWrite, destructive: true, idempotent: true},
	{name: "updateTopic", subsystem: "Topics", access: AccessWrite, destructive: true, idempotent: true},
	{name: "increaseTopicPartitions", subsystem: "Topics", access: AccessWrite},
	{name: "changeReplicationFactor", subsystem: "Topics", access: AccessWrite, destructive: true, idempotent: true},

	{name: "executeSmartFilterTest", subsystem: "Messages", access: AccessReadOnly, idempotent: true, maxItems: maxMessageItems},
	{name: "getTopicMessagesV2", subsystem: "Messages", access: AccessReadOnly, maxItems: maxMessageItems},
	{name: "getSerdes", subsystem: "Messages", access: AccessReadOnly, idempotent: true},
	{name: "registerFilter", subsystem: "Messages", access: AccessReadOnly},
	{name: "deleteTopicMessages", subsystem: "Messages", access: AccessWrite, destructive: true, idempotent: true},
	{name: "sendTopicMessages", subsystem: "Messages", access: AccessWrite, maxItems: maxMessageItems},

	{name: "getConsumerGroup", subsystem: "Consumer Groups", access: AccessReadOnly, idempotent: true},
	{name: "getConsumerGroupsLag", subsystem: "Consumer Groups", access: AccessReadOnly, idempotent: true},
	{name: "getTopicConsumerGroups", subsystem: "Consumer Groups", access: AccessReadOnly, idempotent: true},
	{name: "getConsumerGroupsPage", subsystem: "Consumer Groups", access: AccessReadOnly, idempotent: true},
	{name: "getConsumerGroupsCsv", subsystem: "Consumer Groups", access: AccessReadOnly, idempotent: true},
	{name: "deleteConsumerGroup", subsystem: "Consumer Groups", access: AccessWrite, destructive: true, idempotent: true},
	{name: "deleteConsumerGroupOffsets", subsystem: "Consumer Groups", access: AccessWrite, destructive: true, idempotent: true},
	{name: "resetConsumerGroupOffsets", subsystem: "Consumer Groups", access: AccessWrite, destructive: true},

	{name: "checkSchemaCompatibility", subsystem: "Schemas", access: AccessReadOnly, idempotent: true},
	{name: "getAllVersionsBySubject", subsystem: "Schemas", access: AccessReadOnly, idempotent: true},
	{name: "getGlobalSchemaCompatibilityLevel", subsystem: "Schemas", access: AccessReadOnly, idempotent: true},
	{name: "getLatestSchema", subsystem: "Schemas", access: AccessReadOnly, idempotent: true},
	{name: "getSchemaByVersion", subsystem: "Schemas", access: AccessReadOnly, idempotent: true},
	{name: "getSchemas", subsystem: "Schemas", access: AccessReadOnly, idempotent: true},
	{name: "createNewSchema", subsystem: "Schemas", access: AccessWrite},
	{name: "deleteLatestSchema", subsystem: "Schemas", access: AccessWrite, destructive: true, idempotent: true},
	{name: "deleteSchema", subsystem: "Schemas", access: AccessWrite, destructive: true, idempotent: true},
	{name: "deleteSchemaByVersion", subsystem: "Schemas", access: AccessWrite, destructive: true, idempotent: true},
	{name: "updateGlobalSchemaCompatibilityLevel", subsystem: "Schemas", access: AccessWrite, destructive: true, idempotent: true},
	{name: "updateSchemaCompatibilityLevel", subsystem: "Schemas", access: AccessWrite, destructive: true, idempotent: true},

	{name: "getConnects", subsystem: "Kafka Connect", access: AccessReadOnly, idempotent: true},
	{name: "getConnectsCsv", subsystem: "Kafka Connect", access: AccessReadOnly, idempotent: true},
	{name: "getConnectors", subsystem: "Kafka Connect", access: AccessReadOnly, idempotent: true},
	{name: "getConnector", subsystem: "Kafka Connect", access: AccessReadOnly, idempotent: true},
	{name: "getAllConnectors", subsystem: "Kafka Connect", access: AccessReadOnly, idempotent: true},
	{name: "getAllConnectorsCsv", subsystem: "Kafka Connect", access: AccessReadOnly, idempotent: true},
	{name: "getConnectorConfig", subsystem: "Kafka Connect", access: AccessReadOnly, idempotent: true},
	{name: "getConnectorTasks", subsystem: "Kafka Connect", access: AccessReadOnly, idempotent: true},
	{name: "getConnectorPlugins", subsystem: "Kafka Connect", access: AccessReadOnly, idempotent: true},
	{name: "validateConnectorPluginConfig", subsystem: "Kafka Connect", access: AccessReadOnly, idempotent: true},
	{name: "createConnector", subsystem: "Kafka Connect", access: AccessWrite},
	{name: "deleteConnector", subsystem: "Kafka Connect", access: AccessWrite, destructive: true, idempotent: true},
	{name: "setConnectorConfig", subsystem: "Kafka Connect", access: AccessWrite, destructive: true, idempotent: true},
	{name: "updateConnectorState", subsystem: "Kafka Connect", access: AccessWrite},
	{name: "restartConnectorTask", subsystem: "Kafka Connect", access: AccessWrite},
	{name: "resetConnectorOffsets", subsystem: "Kafka Connect", access: AccessWrite, destructive: true},

	{name: "listAcls", subsystem: "ACL", access: AccessReadOnly, idempotent: true},
	{name: "getAclAsCsv", subsystem: "ACL", access: AccessReadOnly, idempotent: true},
	{name: "createAcl", subsystem: "ACL", access: AccessWrite},
	{name: "deleteAcl", subsystem: "ACL", access: AccessWrite, destructive: true, idempotent: true},
	{name: "syncAclsCsv", subsystem: "ACL", access: AccessWrite, destructive: true},
	{name: "createConsumerAcl", subsystem: "ACL", access: AccessWrite},
	{name: "createProducerAcl", subsystem: "ACL", access: AccessWrite},
	{name: "createStreamAppAcl", subsystem: "ACL", access: AccessWrite},

	{name: "listQuotas", subsystem: "Client Quotas", access: AccessReadOnly, idempotent: true},
	{name: "upsertClientQuotas", subsystem: "Client Quotas", access: AccessWrite, destructive: true, idempotent: true},

	{name: "executeKsql", subsystem: "KSQL", access: AccessConditionalKSQL, destructive: true, maxItems: maxKsqlRows},
	{name: "openKsqlResponsePipe", subsystem: "KSQL", access: AccessReadOnly, maxItems: maxKsqlRows},
	{name: "listStreams", subsystem: "KSQL", access: AccessReadOnly, idempotent: true},
	{name: "listTables", subsystem: "KSQL", access: AccessReadOnly, idempotent: true},
}

func Catalog() []ToolSpec {
	specs := make([]ToolSpec, 0, len(frozenCatalog))
	for _, definition := range frozenCatalog {
		timeout := definition.timeout
		if timeout <= 0 {
			timeout = defaultToolTimeout
		}
		maxItems := definition.maxItems
		if maxItems <= 0 {
			maxItems = maxListItems
		}
		meta := ToolMeta{
			Name:        definition.name,
			Title:       definition.name,
			Description: fmt.Sprintf("Kafbat %s operation %s.", definition.subsystem, definition.name),
			Subsystem:   definition.subsystem,
			Access:      definition.access,
			Destructive: definition.destructive,
			Idempotent:  definition.idempotent,
			Timeout:     timeout,
			MaxItems:    maxItems,
			MaxBytes:    maxResultBytes,
		}
		specs = append(specs, catalogTool(meta))
	}
	return specs
}

func catalogTool(meta ToolMeta) ToolSpec {
	switch meta.Name {
	case "getClusters", "getClusterMetrics", "getClusterStats", "updateClusterInfo",
		"getBrokers", "getBrokersCsv", "getBrokersMetrics",
		"getAllBrokersLogdirs", "getBrokerConfig",
		"updateBrokerTopicPartitionLogDir", "updateBrokerConfigByName":
		return clusterBrokerTool(meta)

	case "getTopicConfigs", "getTopicDetails", "listTopicAcls",
		"analyzeTopic", "cancelTopicAnalysis", "getTopicAnalysis",
		"getActiveProducerStates", "getTopicConnectors",
		"getTopics", "getTopicsCsv",
		"createTopic", "recreateTopic", "cloneTopic", "deleteTopic",
		"updateTopic", "increaseTopicPartitions", "changeReplicationFactor":
		return topicTool(meta)

	case "executeSmartFilterTest", "getTopicMessagesV2", "getSerdes",
		"registerFilter", "deleteTopicMessages", "sendTopicMessages":
		return messageTool(meta)

	case "getConsumerGroup", "getConsumerGroupsLag", "getTopicConsumerGroups",
		"getConsumerGroupsPage", "getConsumerGroupsCsv",
		"deleteConsumerGroup", "deleteConsumerGroupOffsets", "resetConsumerGroupOffsets":
		return consumerGroupTool(meta)

	case "checkSchemaCompatibility", "getAllVersionsBySubject",
		"getGlobalSchemaCompatibilityLevel", "getLatestSchema",
		"getSchemaByVersion", "getSchemas",
		"createNewSchema", "deleteLatestSchema", "deleteSchema",
		"deleteSchemaByVersion", "updateGlobalSchemaCompatibilityLevel",
		"updateSchemaCompatibilityLevel":
		return schemaTool(meta)

	case "getConnects", "getConnectsCsv", "getConnectors", "getConnector",
		"getAllConnectors", "getAllConnectorsCsv", "getConnectorConfig",
		"getConnectorTasks", "getConnectorPlugins", "validateConnectorPluginConfig",
		"createConnector", "deleteConnector", "setConnectorConfig",
		"updateConnectorState", "restartConnectorTask", "resetConnectorOffsets":
		return connectTool(meta)

	case "listAcls", "getAclAsCsv", "createAcl", "deleteAcl", "syncAclsCsv",
		"createConsumerAcl", "createProducerAcl", "createStreamAppAcl",
		"listQuotas", "upsertClientQuotas":
		return aclQuotaTool(meta)

	case "executeKsql", "openKsqlResponsePipe", "listStreams", "listTables":
		return ksqlTool(meta)
	default:
		panic(fmt.Sprintf("MCP catalog tool %q has no typed input", meta.Name))
	}
}

func VisibleCatalog(policy mcppolicy.Policy) []ToolSpec {
	if !policy.Enabled {
		return nil
	}
	all := Catalog()
	if policy.AllowWrites {
		return all
	}
	visible := make([]ToolSpec, 0, len(all))
	for _, spec := range all {
		if spec.Meta.Access != AccessWrite {
			visible = append(visible, spec)
		}
	}
	return visible
}
