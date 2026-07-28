package mcpserver

import (
	"context"

	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	domaincluster "github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/serde"
	"github.com/cy-kaf/cy-kaf-client/internal/mcppolicy"
)

type ClusterStater interface {
	List(context.Context) []domaincluster.Snapshot
	Get(context.Context, string) (domaincluster.RuntimeState, bool)
	Refresh(context.Context, string) (domaincluster.RuntimeState, error)
}

type BrokerServicer interface {
	LogDirs(context.Context, string, []int32) ([]domaincluster.BrokerLogDirs, error)
	BrokerConfigs(context.Context, string, int32) ([]domaincluster.ConfigEntry, error)
	AlterBrokerConfig(context.Context, string, int32, string, string) error
	MoveReplicaLogDir(context.Context, string, int32, string, int32, string) error
}

type TopicServicer interface {
	List(context.Context, string, appcluster.TopicListQuery) (appcluster.TopicPage, error)
	Details(context.Context, string, string) (domaincluster.TopicState, []domaincluster.ConfigEntry, error)
	Configs(context.Context, string, string) ([]domaincluster.ConfigEntry, error)
	Acls(context.Context, string, string) ([]domaincluster.AclBinding, error)
	ActiveProducers(context.Context, string, string) ([]domaincluster.ProducerState, error)
	Connectors(context.Context, string, string) error
	Create(context.Context, string, domaincluster.TopicSpec) (domaincluster.TopicState, error)
	Delete(context.Context, string, string) error
	UpdateConfigs(context.Context, string, string, map[string]string) (domaincluster.TopicState, error)
	Recreate(context.Context, string, string) (domaincluster.TopicState, error)
	Clone(context.Context, string, string, string) (domaincluster.TopicState, error)
	IncreasePartitions(context.Context, string, string, int32) error
	ChangeReplicationFactor(context.Context, string, string, int16) error
}

type GroupServicer interface {
	Page(context.Context, string, appcluster.GroupPageQuery) (appcluster.GroupPage, error)
	Get(context.Context, string, string) (domaincluster.GroupState, error)
	Lag(context.Context, string, []string) ([]domaincluster.GroupState, error)
	ForTopic(context.Context, string, string) ([]domaincluster.GroupState, error)
	Reset(context.Context, string, string, domaincluster.ResetSpec) error
	Delete(context.Context, string, string) error
	DeleteOffsets(context.Context, string, string, string) error
}

type SerdeServicer interface {
	Suggest(context.Context, string, string, serde.Usage) (serde.Suggestion, error)
}

type SchemaServicer interface {
	ListSchemas(context.Context, string, appcluster.SchemaListQuery) (appcluster.SchemaPage, error)
	LatestSchema(context.Context, string, string) (domaincluster.SchemaVersion, error)
	SchemaByVersion(context.Context, string, string, string) (domaincluster.SchemaVersion, error)
	GlobalCompat(context.Context, string) (string, error)
	SetGlobalCompat(context.Context, string, string) error
	SetSubjectCompat(context.Context, string, string, string) error
	CheckCompat(context.Context, string, string, domaincluster.NewSchema) (bool, error)
	Register(context.Context, string, string, domaincluster.NewSchema) (domaincluster.SchemaVersion, error)
	DeleteSubject(context.Context, string, string) ([]int, error)
	DeleteVersion(context.Context, string, string, string) (int, error)
	AllVersions(context.Context, string, string) ([]domaincluster.SchemaVersion, error)
}

type ConnectServicer interface {
	ListConnects(context.Context, string) ([]domaincluster.ConnectCluster, error)
	Plugins(context.Context, string, string) ([]domaincluster.ConnectorPlugin, error)
	ValidatePlugin(context.Context, string, string, string, map[string]any) (domaincluster.PluginValidation, error)
	AllConnectors(context.Context, string) ([]domaincluster.ConnectorRef, error)
	Connectors(context.Context, string, string) ([]string, error)
	Connector(context.Context, string, string, string) (domaincluster.Connector, error)
	ConnectorConfig(context.Context, string, string, string) (map[string]any, error)
	ConnectorTasks(context.Context, string, string, string) ([]domaincluster.ConnectorTask, error)
	CreateConnector(context.Context, string, string, string, map[string]any) (domaincluster.Connector, error)
	DeleteConnector(context.Context, string, string, string) error
	SetConnectorConfig(context.Context, string, string, string, map[string]any) (domaincluster.Connector, error)
	UpdateConnectorState(context.Context, string, string, string, string) error
	ResetConnectorOffsets(context.Context, string, string, string) error
	RestartConnectorTask(context.Context, string, string, string, int) error
}

type AclServicer interface {
	List(context.Context, string, domaincluster.AclFilter) ([]domaincluster.AclBinding, error)
	CreateAcl(context.Context, string, domaincluster.AclBinding) error
	DeleteAcl(context.Context, string, domaincluster.AclBinding) (int, error)
	CreateConsumerAcl(context.Context, string, appcluster.ConsumerAclSpec) error
	CreateProducerAcl(context.Context, string, appcluster.ProducerAclSpec) error
	CreateStreamAppAcl(context.Context, string, appcluster.StreamAppAclSpec) error
	FormatAclCSV([]domaincluster.AclBinding) (string, error)
	SyncCSV(context.Context, string, string) error
}

type QuotaServicer interface {
	ListQuotas(context.Context, string) ([]domaincluster.ClientQuota, error)
	UpsertQuotas(context.Context, string, domaincluster.ClientQuota) error
}

type KSQLServicer interface {
	Register(context.Context, string, string, map[string]string) (string, error)
	OpenAuthorized(context.Context, string, string, appcluster.KsqlAuthorize, func(domaincluster.KsqlTable) error) error
	ListStreams(context.Context, string) ([]domaincluster.KsqlStreamDescription, error)
	ListTables(context.Context, string) ([]domaincluster.KsqlTableDescription, error)
}

type MessageServicer interface {
	Browse(context.Context, string, string, appcluster.BrowseSpec, func(appcluster.BrowseEvent) error) error
	Send(context.Context, string, string, appcluster.SendSpec) error
	Delete(context.Context, string, string, []int32) error
}

type AnalysisServicer interface {
	Analyze(context.Context, string, string) error
	Get(string, string) (appcluster.AnalysisView, bool, error)
	Cancel(context.Context, string, string) error
}

type SmartFilterServicer interface {
	Register(string) (string, error)
	Test(appcluster.SmartFilterTest) (bool, string, error)
}

type Dependencies struct {
	States         ClusterStater
	Brokers        BrokerServicer
	Topics         TopicServicer
	Groups         GroupServicer
	Serdes         SerdeServicer
	Schemas        SchemaServicer
	Connects       ConnectServicer
	Acls           AclServicer
	Quotas         QuotaServicer
	KSQL           KSQLServicer
	KSQLClassifier appcluster.KsqlClassifier
	Messages       MessageServicer
	Analysis       AnalysisServicer
	SmartFilters   SmartFilterServicer
	IsReadOnly     func(string) bool
	Policy         *mcppolicy.Store
}
