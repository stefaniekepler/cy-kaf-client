package mcpserver

import (
	"encoding/json"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
)

type noInput struct{}

type clusterInput struct {
	ClusterName string `json:"clusterName" jsonschema:"Kafka cluster name"`
}

type clusterOptionalQueryInput[Query any] struct {
	ClusterName string `json:"clusterName" jsonschema:"Kafka cluster name"`
	Query       *Query `json:"query,omitempty"`
}

type clusterQueryInput[Query any] struct {
	ClusterName string `json:"clusterName" jsonschema:"Kafka cluster name"`
	Query       Query  `json:"query"`
}

type clusterBodyInput[Body any] struct {
	ClusterName string `json:"clusterName" jsonschema:"Kafka cluster name"`
	Body        Body   `json:"body"`
}

type topicInput struct {
	ClusterName string `json:"clusterName" jsonschema:"Kafka cluster name"`
	TopicName   string `json:"topicName" jsonschema:"Kafka topic name"`
}

type topicOptionalQueryInput[Query any] struct {
	ClusterName string `json:"clusterName" jsonschema:"Kafka cluster name"`
	TopicName   string `json:"topicName" jsonschema:"Kafka topic name"`
	Query       *Query `json:"query,omitempty"`
}

type topicQueryInput[Query any] struct {
	ClusterName string `json:"clusterName" jsonschema:"Kafka cluster name"`
	TopicName   string `json:"topicName" jsonschema:"Kafka topic name"`
	Query       Query  `json:"query"`
}

type topicBodyInput[Body any] struct {
	ClusterName string `json:"clusterName" jsonschema:"Kafka cluster name"`
	TopicName   string `json:"topicName" jsonschema:"Kafka topic name"`
	Body        Body   `json:"body"`
}

// topicCreationLenientInput and topicUpdateLenientInput retain the generated
// contract field names and scalar fields, while keeping config values as raw
// JSON until the adapter applies the HTTP endpoint's string/number/boolean
// coercion. Their tool schemas are supplied explicitly by tools_topics.go so
// only those scalar config values are wider than the generated string schema.
type topicCreationLenientInput struct {
	Name              string                     `json:"name"`
	Partitions        int32                      `json:"partitions"`
	ReplicationFactor *int32                     `json:"replicationFactor,omitempty"`
	Configs           map[string]json.RawMessage `json:"configs,omitempty"`
}

type topicUpdateLenientInput struct {
	Configs map[string]json.RawMessage `json:"configs,omitempty"`
}

type brokerInput struct {
	ClusterName string `json:"clusterName" jsonschema:"Kafka cluster name"`
	ID          int32  `json:"id" jsonschema:"Kafka broker ID"`
}

type updateBrokerConfigInput struct {
	ClusterName string `json:"clusterName" jsonschema:"Kafka cluster name"`
	ID          int32  `json:"id" jsonschema:"Kafka broker ID"`
	Name        string `json:"name" jsonschema:"Broker configuration name"`
	Value       string `json:"value" jsonschema:"Broker configuration value"`
}

type moveLogDirInput struct {
	ClusterName string `json:"clusterName" jsonschema:"Kafka cluster name"`
	ID          int32  `json:"id" jsonschema:"Kafka broker ID"`
	Topic       string `json:"topic" jsonschema:"Kafka topic name"`
	Partition   int32  `json:"partition" jsonschema:"Kafka partition ID"`
	LogDir      string `json:"logDir" jsonschema:"Destination broker log directory"`
}

type connectInput struct {
	ClusterName string `json:"clusterName"`
	ConnectName string `json:"connectName"`
}

type connectBodyInput[Body any] struct {
	ClusterName string `json:"clusterName"`
	ConnectName string `json:"connectName"`
	Body        Body   `json:"body"`
}

type connectorInput struct {
	ClusterName   string `json:"clusterName"`
	ConnectName   string `json:"connectName"`
	ConnectorName string `json:"connectorName"`
}

type connectorBodyInput[Body any] struct {
	ClusterName   string `json:"clusterName"`
	ConnectName   string `json:"connectName"`
	ConnectorName string `json:"connectorName"`
	Body          Body   `json:"body"`
}

type connectorPluginBodyInput struct {
	ClusterName string                    `json:"clusterName"`
	ConnectName string                    `json:"connectName"`
	PluginName  string                    `json:"pluginName"`
	Body        generated.ConnectorConfig `json:"body"`
}

type connectorActionInput struct {
	ClusterName   string                    `json:"clusterName"`
	ConnectName   string                    `json:"connectName"`
	ConnectorName string                    `json:"connectorName"`
	Action        generated.ConnectorAction `json:"action"`
}

type connectorTaskInput struct {
	ClusterName   string `json:"clusterName"`
	ConnectName   string `json:"connectName"`
	ConnectorName string `json:"connectorName"`
	TaskID        int32  `json:"taskId"`
}

type consumerGroupInput struct {
	ClusterName string `json:"clusterName"`
	ID          string `json:"id"`
}

type consumerGroupTopicInput struct {
	ClusterName string `json:"clusterName"`
	ID          string `json:"id"`
	TopicName   string `json:"topicName"`
}

type consumerGroupBodyInput[Body any] struct {
	ClusterName string `json:"clusterName"`
	ID          string `json:"id"`
	Body        Body   `json:"body"`
}

type schemaSubjectInput struct {
	ClusterName string `json:"clusterName"`
	Subject     string `json:"subject"`
}

type schemaVersionInput struct {
	ClusterName string `json:"clusterName"`
	Subject     string `json:"subject"`
	Version     string `json:"version"`
}

type schemaSubjectBodyInput[Body any] struct {
	ClusterName string `json:"clusterName"`
	Subject     string `json:"subject"`
	Body        Body   `json:"body"`
}

type globalBodyInput[Body any] struct {
	Body Body `json:"body"`
}
