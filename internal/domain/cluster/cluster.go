// Package cluster holds the cluster domain model. It must stay free of
// application, infrastructure and third-party SDK imports (enforced by lint).
package cluster

import (
	"context"
	"errors"
	"time"
)

var ErrResultTooLarge = errors.New("result_too_large")

// ErrInvalidConfig marks a configuration document that parses but violates the
// config schema (missing name/bootstrapServers, duplicate cluster names, or an
// unrecognized enum value). Callers (the import handler) map it to a 400.
var ErrInvalidConfig = errors.New("invalid config")

type ConnectionSpec struct {
	BootstrapServers []string
	Security         map[string]string
}

// SRAuth is a Schema Registry connection's optional HTTP basic auth
// credentials (contract's schemaRegistryAuth.username/password — the oauth
// sub-object is out of scope for P2a's franz-go pkg/sr client, which only
// supports basic auth + SSL).
type SRAuth struct{ Username, Password string }

// SRSSL is a Schema Registry connection's optional custom TLS material.
// Keystore fields mirror the contract's schemaRegistrySsl (client cert, for
// mutual TLS); Truststore fields follow the same convention already used
// elsewhere in this package's config-sourced SSL material (e.g. Kafka
// broker SSL) for verifying the SR server's own certificate.
type SRSSL struct{ KeystoreLocation, KeystorePassword, TruststoreLocation, TruststorePassword string }

// KsqlAuth is an optional HTTP basic-auth credential for a ksqlDB server.
// A nil Definition.KsqlAuth means the cluster configuration did not provide
// an auth block; an allocated value preserves a deliberately configured empty
// credential pair for the infrastructure adapter.
type KsqlAuth struct {
	Username string
	Password string
}

// KsqlSSL is optional TLS material for a ksqlDB server.  The infrastructure
// adapter currently consumes the truststore fields; keystore values are kept
// in the domain so the configuration contract is lossless and can be used by
// a later mTLS implementation without changing the config shape.
type KsqlSSL struct {
	TruststoreLocation string
	TruststorePassword string
	KeystoreLocation   string
	KeystorePassword   string
}

// SchemaRegistrySpec is one cluster's Schema Registry connection: its base
// URL plus optional basic auth and/or custom TLS material. Auth/SSL nil
// means "not configured" — distinct from a configured-but-empty value.
type SchemaRegistrySpec struct {
	URL  string
	Auth *SRAuth // nil = 无 basic auth
	SSL  *SRSSL  // nil = 无自定义 TLS 材料
}

// ConnectAuth is a Kafka Connect worker connection's optional HTTP basic
// auth credentials (contract's kafkaConnect[].username/password).
type ConnectAuth struct{ Username, Password string }

// ConnectSSL is a Kafka Connect worker connection's optional custom TLS
// material. Keystore fields mirror the contract's kafkaConnect[].
// keystoreLocation/keystorePassword (client cert, for mutual TLS);
// Truststore fields follow the same convention already used elsewhere in
// this package's config-sourced SSL material (e.g. SchemaRegistrySpec's
// SRSSL) for verifying the Connect worker's own certificate.
type ConnectSSL struct{ KeystoreLocation, KeystorePassword, TruststoreLocation, TruststorePassword string }

// ConnectSpec is one Kafka Connect worker cluster's connection: its name
// (contract's kafkaConnect[].name, used to address it in later per-
// connector endpoints), base address, plus optional basic auth and/or
// custom TLS material. Auth/SSL nil means "not configured" — distinct from
// a configured-but-empty value.
type ConnectSpec struct {
	Name    string
	Address string
	Auth    *ConnectAuth // nil = 无 basic auth
	SSL     *ConnectSSL  // nil = 无自定义 TLS 材料
}

type Definition struct {
	Name           string
	ReadOnly       bool
	Conn           ConnectionSpec
	SchemaRegistry SchemaRegistrySpec
	Connects       []ConnectSpec
	KsqlURL        string
	KsqlAuth       *KsqlAuth
	KsqlSSL        *KsqlSSL

	// P1c Task 1: per-cluster serde/masking/defaults/throttle config, sourced
	// from infra/config's YAML clusters[] item (contract-shaped: serde,
	// defaultKeySerde/defaultValueSerde, masking, pollingThrottleRate) —
	// consumed by later P1c tasks (serde suggestion, masking) rather than by
	// anything in this task itself.
	SerdeConfigs        []SerdeConfig
	DefaultKeySerde     string
	DefaultValueSerde   string
	Maskings            []MaskingRule
	PollingThrottleRate int64 // 每秒字节数上限；0 = 不限速
}

// MaskingType is one masking strategy applied to matched fields (contract's
// masking[].type enum).
type MaskingType int

const (
	MaskRemove  MaskingType = iota // 契约枚举 "REMOVE"
	MaskMask                       // "MASK"
	MaskReplace                    // "REPLACE"
)

// MaskingRule is one configured data-masking policy: which fields (by exact
// name or by FieldsNamePattern regex) get masked, using which strategy-
// specific parameters, scoped to topics matching TopicKeysPattern/
// TopicValuesPattern.
type MaskingRule struct {
	Type                    MaskingType
	Fields                  []string // 精确字段名列表
	FieldsNamePattern       string   // 正则（字段名匹配）
	MaskingCharsReplacement []string // MASK 策略用：字符类替换表
	Replacement             string   // REPLACE 策略用：整体替换串
	TopicKeysPattern        string   // 正则：绑定到哪些 topic 的 key
	TopicValuesPattern      string   // 正则：绑定到哪些 topic 的 value
}

// SerdeConfig is one configured serde's binding: which topics' keys/values it
// is the preferred serde for (via the two pattern fields), plus its own
// properties.
type SerdeConfig struct {
	Name               string
	TopicKeysPattern   string // 正则：该 serde 对哪些 topic 的 key 为 preferred
	TopicValuesPattern string
	Properties         map[string]any
}

type Status string

const (
	StatusOnline  Status = "ONLINE"
	StatusOffline Status = "OFFLINE"
)

type Feature string

const (
	FeatureSchemaRegistry Feature = "SCHEMA_REGISTRY"
	FeatureKafkaConnect   Feature = "KAFKA_CONNECT"
	FeatureKsqlDB         Feature = "KSQL_DB"
	FeatureKafkaACLView   Feature = "KAFKA_ACL_VIEW"
	FeatureKafkaACLEdit   Feature = "KAFKA_ACL_EDIT"
	// FeatureTopicDeletion is runtime-derived (unlike the three above, which
	// are config-driven): it reflects the live cluster's delete.topic.enable
	// broker config, scraped into RuntimeState.TopicDeletionEnabled by
	// FetchState — see (RuntimeState).Features().
	FeatureTopicDeletion Feature = "TOPIC_DELETION"
)

type Snapshot struct {
	Definition  Definition
	Status      Status
	BrokerCount int
	Features    []Feature
}

// Features derives the config-driven subset of enabled UI features from
// configured integrations, mirroring upstream FeatureService semantics.
// P1b Task 5 adds a runtime-derived feature (TOPIC_DELETION) that this
// Definition-only method cannot see (it needs a live scrape) — callers that
// have a RuntimeState should call (RuntimeState).Features() instead, which
// wraps this method and merges in the runtime part. This method stays
// exported (rather than folding into RuntimeState alone) because it's also
// the only signal available before a cluster's first scrape completes.
func (d Definition) Features() []Feature {
	fs := []Feature{}
	if d.SchemaRegistry.URL != "" {
		fs = append(fs, FeatureSchemaRegistry)
	}
	if len(d.Connects) > 0 {
		fs = append(fs, FeatureKafkaConnect)
	}
	if d.KsqlURL != "" {
		fs = append(fs, FeatureKsqlDB)
	}
	// Authentication is permanently disabled for this single-user desktop
	// client, so every configured cluster has ACL view permission. Editing is
	// advertised only when the cluster itself is not read-only; the API's
	// readOnlyGuard remains the enforcement boundary.
	fs = append(fs, FeatureKafkaACLView)
	if !d.ReadOnly {
		fs = append(fs, FeatureKafkaACLEdit)
	}
	return fs
}

// Features merges Definition's config-driven features with the runtime-
// derived ones (currently just TOPIC_DELETION, from TopicDeletionEnabled —
// scraped by FetchState from the live cluster's delete.topic.enable broker
// config). This is the method Snapshot() and every other RuntimeState-holding
// caller should use; Definition.Features() alone is only for the "no
// RuntimeState yet" pre-first-scrape case.
func (s RuntimeState) Features() []Feature {
	fs := s.Definition.Features()
	if s.TopicDeletionEnabled {
		fs = append(fs, FeatureTopicDeletion)
	}
	return fs
}

// BrokerInfo is one broker as reported by the cluster's own metadata.
type BrokerInfo struct {
	ID   int32
	Host string
	Port int32
	Rack string

	// Per-broker partition tallies, filled in by tallyPartitions from the same
	// metadata scrape that produces the cluster-wide PartitionCounts.
	PartitionsLeader int // partitions this broker leads
	Partitions       int // partitions this broker replicates (leader or follower)
	InSyncPartitions int // partitions this broker is an in-sync replica for
}

// PartitionCounts tallies partition health across all topics in a cluster.
type PartitionCounts struct{ Online, Offline, UnderReplicated, InSync, OutOfSync int }

// DiskUsage is the aggregate log segment footprint for one broker.
type DiskUsage struct {
	Broker       int32
	SegmentSize  int64
	SegmentCount int
}

// RuntimeState is a point-in-time snapshot of a cluster's live status, as
// scraped from the cluster itself (P1a subset of upstream ScrapedClusterState).
type RuntimeState struct {
	Definition  Definition
	Status      Status
	Brokers     []BrokerInfo
	Controller  int32
	TopicCount  int
	Partitions  PartitionCounts
	Disk        []DiskUsage
	Topics      []TopicState // P1b Task 4: per-topic state, filled by FetchState/tallyPartitions
	Version     string       // 取不到时留空（契约字段可空）
	RefreshedAt time.Time
	Err         string // 最近一次刷新失败原因（Status=OFFLINE 时非空）

	// TopicDeletionEnabled is P1b Task 5's TOPIC_DELETION feature flag,
	// scraped from any one broker's delete.topic.enable config. FetchState
	// defaults this to true (KRaft's own default) whenever the config can't
	// be read, so a describe failure never silently reports the feature as
	// disabled — see (RuntimeState).Features() and infra/kafka/state.go.
	TopicDeletionEnabled bool
}

// PartitionState is one topic-partition's cluster-reported placement and
// offsets, as scraped by FetchState (Metadata for leader/replicas/ISR, plus
// ListStartOffsets/ListEndOffsets for the low/high watermarks). Offsets are
// -1 when the corresponding offsets fetch failed or hasn't been attempted —
// that failure is non-fatal to the overall scrape (see infra/kafka/state.go).
type PartitionState struct {
	ID                     int32
	Leader                 int32
	Replicas               []int32
	ISR                    []int32
	StartOffset, EndOffset int64
}

// TopicState is one topic's cluster-reported shape: its partitions
// (placement + offsets) plus the disk footprint logdirs attributes to it.
// SegmentSize/SegmentCount are 0 when the logdirs describe failed (same
// non-fatal-degrade convention as RuntimeState.Disk).
type TopicState struct {
	Name              string
	Internal          bool
	Partitions        []PartitionState
	ReplicationFactor int
	SegmentSize       int64 // 来自 logdirs 聚合（无数据时 0）
	SegmentCount      int
}

// MessagesCount returns the sum of known per-partition offset spans. Partitions
// with an unknown start or end offset are skipped; the boolean is false only
// when no partition has a complete offset pair.
func (t TopicState) MessagesCount() (int64, bool) {
	var total int64
	known := false
	for _, partition := range t.Partitions {
		if partition.StartOffset < 0 || partition.EndOffset < 0 {
			continue
		}
		known = true
		if delta := partition.EndOffset - partition.StartOffset; delta > 0 {
			total += delta
		}
	}
	return total, known
}

// AclBinding is one Kafka ACL entry bound to a resource (in Task 4's scope,
// always a topic resource, since TopicAdminPort.TopicAcls filters by topic
// name). Unlike ConfigEntry.Source (a raw driver string mapped to the
// contract's enum later, in api's sourceToGenerated), these four fields are
// already contract-enum-shaped strings — infra/kafka/topics.go maps kmsg's
// ACLResourceType/ACLResourcePatternType/ACLOperation/ACLPermissionType
// through a dedicated table (aclXxxToContract) before they ever reach
// domain, since the underlying kmsg enum values happen to already spell the
// same literals the contract uses (confirmed by reading kmsg@v1.13.1's
// generated String() methods, not assumed).
type AclBinding struct {
	Principal, Host, ResourceName                    string
	ResourceType, PatternType, Operation, Permission string
}

// AclFilter 是 listAcls/getAclAsCsv 的查询条件（契约 ListAclsParams/GetAclAsCsvParams
// 的 domain 形态）：ResourceType/PatternType 为契约枚举文本（"" = 不限），ResourceName
// 为资源名前缀/精确（"" = 不限），Search 为 principal 子串（fts=false 时按 principal 子串，
// fts=true 时按全字段子串），Fts 打开全文匹配。
type AclFilter struct {
	ResourceType string
	ResourceName string
	PatternType  string
	Search       string
	Fts          bool
}

// ClientQuota 是一条 client-quota 记录（契约 ClientQuotas）：User/ClientID/IP 三个
// entity 维度（"" = 该维度不参与），Quotas 是配额键→值的完整期望态（缺失键 = 该限额
// 应被删除，见 QuotaPort.UpsertQuotas 的 diff 语义）。
type ClientQuota struct {
	User     string
	ClientID string
	IP       string
	Quotas   map[string]float64
}

// AclAdminPort 是 ACL 读/写面（P2c Task 2），由 infra/kafka 的 Pool 实现。与
// TopicAdminPort 一样每次调用直连 live kadm 客户端（无缓存）；AclService
// (app/cluster/acl.go) 是唯一调用入口。四枚举字段进出都用契约文本形态，
// 契约文本↔kadm 枚举的翻译在 infra 侧完成（domain 不 import kadm）。
type AclAdminPort interface {
	// ListAcls 用一次广查 DescribeACLs（AnyResource + 按 filter 下推资源/模式）
	// 列出匹配 filter 的全部 ACL；principal 子串/fts 的本地过滤与稳定排序由
	// AclService 完成，本方法只做资源维度下推 + kadm→契约映射。
	ListAcls(ctx context.Context, def Definition, filter AclFilter) ([]AclBinding, error)
	// CreateAcls 逐条创建 bindings（每条一个单值 ACLBuilder，避免 kadm builder 的
	// principals×hosts×resources×operations 交叉相乘把异构 binding 拼错）。
	CreateAcls(ctx context.Context, def Definition, bindings []AclBinding) error
	// DeleteAcls 按 binding 精确匹配删除，返回实际删除条数（0 → 调用方判 404）。
	DeleteAcls(ctx context.Context, def Definition, binding AclBinding) (deleted int, err error)
}

// QuotaPort 是 client-quota 读/写面（P2c Task 3），由 infra/kafka 的 Pool 实现。
type QuotaPort interface {
	// ListQuotas 描述全部 client quotas（DescribeClientQuotas 全量）。
	ListQuotas(ctx context.Context, def Definition) ([]ClientQuota, error)
	// UpsertQuotas 对单个 entity 做「完整期望态」写：先描述该 entity 现存键，
	// 请求里有的键 Set、请求里缺而现存的键 Remove（对齐上游 full-replace 语义）。
	UpsertQuotas(ctx context.Context, def Definition, quota ClientQuota) error
}

// ProducerState is one active (transactional) producer's last-known state
// for a topic-partition, as reported by kadm's DescribeProducers.
type ProducerState struct {
	Partition                     int32
	ProducerID                    int64
	ProducerEpoch                 int32
	LastSequence                  int32
	LastTimestamp                 int64
	CoordinatorEpoch              int32
	CurrentTransactionStartOffset int64
}

// TopicSpec is the desired shape of a topic to create (CreateTopic) or to
// re-create with (TopicService.Recreate/Clone derive one from an existing
// topic's observed shape). Partitions/ReplicationFactor use -1 to mean
// "let the cluster choose its own default" (Kafka's own create-topic
// sentinel, passed straight through to kadm — see infra/kafka/topics.go).
// Configs is the full set of dynamic config overrides to apply at creation
// time; incremental modification of an *existing* topic's configs goes
// through AlterTopicConfig instead, never through a second CreateTopic call.
type TopicSpec struct {
	Name              string
	Partitions        int32             // -1 = 集群默认
	ReplicationFactor int16             // -1 = 集群默认
	Configs           map[string]string // 创建时全量；增量修改走 AlterTopicConfig
}

// TopicAdminPort performs topic-scoped admin operations against a live
// cluster connection: configs, ACLs, active producers (P1b Task 4, read-
// only) plus create/delete/alter-config/partition operations (P1b Task 5).
// Like BrokerAdminPort, every call goes through the live kadm client (no
// caching) — TopicService (app/cluster/topic.go) is the single place
// callers reach these through.
type TopicAdminPort interface {
	// TopicConfigs reports one topic's configuration entries (dynamic/
	// static/default, with synonyms).
	TopicConfigs(ctx context.Context, def Definition, topic string) ([]ConfigEntry, error)
	// TopicAcls reports every ACL binding matching topic (literal resource
	// name match — prefixed/wildcard ACLs that would also apply to this
	// topic are out of scope for P1b Task 4).
	TopicAcls(ctx context.Context, def Definition, topic string) ([]AclBinding, error)
	// ActiveProducers reports topic's active (in-flight transactional)
	// producers, per partition.
	ActiveProducers(ctx context.Context, def Definition, topic string) ([]ProducerState, error)

	// CreateTopic creates one topic with spec's shape (partitions/
	// replication factor/initial dynamic configs).
	CreateTopic(ctx context.Context, def Definition, spec TopicSpec) error
	// DeleteTopic deletes topic. Callers (TopicService.Delete) are
	// responsible for checking RuntimeState.TopicDeletionEnabled first —
	// this port method itself has no feature-flag awareness, it always
	// attempts the delete.
	DeleteTopic(ctx context.Context, def Definition, topic string) error
	// AlterTopicConfig incrementally alters a single topic config key: a
	// non-empty value sets it, an empty value unsets it (reverts to
	// whatever lower-priority source then applies, e.g. a cluster default —
	// the kadm.DeleteConfig incremental op, not a literal "set to empty
	// string"). Implementations must use kadm's incremental
	// AlterTopicConfigs, never the full-state-replace AlterTopicConfigsState
	// (ADR-0003 §6.1's same-family trap that already bit AlterBrokerConfig).
	AlterTopicConfig(ctx context.Context, def Definition, topic, name, value string) error
	// CreatePartitions sets topic's *total* partition count to total (not an
	// incremental add — total is the contract's PartitionsIncrease.
	// totalPartitionsCount semantics; see infra/kafka/topics.go's doc comment
	// for the kadm-level CreatePartitions/UpdatePartitions naming trap this
	// implies).
	CreatePartitions(ctx context.Context, def Definition, topic string, total int32) error
	// AlterPartitionAssignments reassigns topic's replicas per partition
	// (partition ID -> ordered broker ID list, leader-first by convention)
	// — used by TopicService.ChangeReplicationFactor's reassignForFactor
	// output.
	AlterPartitionAssignments(ctx context.Context, def Definition, topic string, assignment map[int32][]int32) error
}

// TopicPartitions is one topic and a set of its partition numbers — used by
// GroupMember.Assignments (what a consumer group member is currently
// assigned to consume).
type TopicPartitions struct {
	Topic      string
	Partitions []int32
}

// GroupMember is one consumer group member as reported by a describe-groups
// call. Assignments is only ever populated for the classic "consumer"
// protocol type (kmsg.ConsumerMemberAssignment) — a "connect" or otherwise
// unrecognized protocol member degrades to an empty Assignments, same
// "no data source for this case" convention used elsewhere in this package.
type GroupMember struct {
	MemberID, ClientID, Host string
	Assignments              []TopicPartitions
}

// GroupOffset is one topic-partition's committed-vs-end offset pair within a
// consumer group. Committed is always >= 0 as currently produced by
// infra/kafka/groups.go's DescribeGroup (only partitions the group has
// actually committed against are included there — see that file's doc
// comment for the deliberate scope limit); the -1 "no commit" sentinel is
// still part of this type's contract for Lag's sake (and for a future
// widening that also surfaces assigned-but-never-committed partitions).
type GroupOffset struct {
	Topic     string
	Partition int32
	Committed int64 // -1 = no commit ever made for this partition
	End       int64 // partition's end (high-watermark) offset — the lag denominator; -1 = unknown
}

// GroupState is one consumer group's state. ListGroups (list-level) fills
// only ID/State/Coordinator/CoordinatorID; DescribeGroup/GroupsForTopic
// (detail-level) additionally fill Members/Offsets/Protocol — see
// GroupAdminPort's doc comment for why the two are deliberately split.
type GroupState struct {
	ID, State     string // State: contract's ConsumerGroupState enum spelling (infra/kafka/groups.go's groupStateToGenerated table maps kadm's raw state string onto it)
	Coordinator   string // coordinator broker's host; "" at list level (ListedGroup carries no host, only a node ID)
	CoordinatorID int32
	Members       []GroupMember
	Offsets       []GroupOffset
	Protocol      string // partition assignor strategy; "" at list level
}

// Lag sums max(End-Committed, 0) across every offset entry whose Committed
// is known (>= 0). An entry with Committed == -1 (no commit) contributes 0,
// not End: the contract's ConsumerGroup.consumerLag field description
// ("null if consumer group has no offsets committed") frames "no commit" as
// an absent-lag case rather than a worst-case "hasn't consumed anything at
// all" case, so this mirrors upstream kafka-ui's own semantic — a partition
// nobody has ever committed against isn't counted as this consumer's
// backlog. Callers that need to distinguish "genuinely zero lag" from "no
// commits at all" (the contract's null case) do so themselves by inspecting
// Offsets (see internal/api/handlers_group.go's groupHasAnyCommit), since
// that distinction is a serialization concern, not this pure function's.
func (g GroupState) Lag() int64 {
	var total int64
	for _, o := range g.Offsets {
		if o.Committed < 0 {
			continue
		}
		if lag := o.End - o.Committed; lag > 0 {
			total += lag
		}
	}
	return total
}

// TopicCount reports the number of distinct topics g has committed offsets
// against. List-level GroupState (empty Offsets) always reports 0 — see
// GroupAdminPort.ListGroups' doc comment.
func (g GroupState) TopicCount() int {
	seen := map[string]struct{}{}
	for _, o := range g.Offsets {
		seen[o.Topic] = struct{}{}
	}
	return len(seen)
}

// ResetSpec is resetConsumerGroupOffsets' request shape: which topic to
// reset, the reset strategy (contract's ConsumerGroupOffsetsResetType enum:
// EARLIEST/LATEST/OFFSET/TIMESTAMP), and strategy-specific parameters.
type ResetSpec struct {
	Topic             string
	ResetType         string
	Partitions        []int32         // empty = every partition of Topic (EARLIEST/LATEST/TIMESTAMP only)
	PartitionsOffsets map[int32]int64 // OFFSET reset type: explicit partition -> target offset
	Timestamp         int64           // TIMESTAMP reset type: milliseconds since epoch
}

// GroupAdminPort performs consumer-group-scoped admin operations against a
// live cluster connection (P1b Task 6) — same "no caching, every call hits
// the live kadm client" shape as TopicAdminPort/BrokerAdminPort.
//
// ListGroups and DescribeGroup/GroupsForTopic deliberately split cheap-list
// from expensive-detail: describing every group's members and fetching its
// offsets (plus listing the end offsets those need for a lag denominator)
// is an O(groups) fan-out of extra live calls that only makes sense once a
// caller actually wants one group's (or an explicitly requested set's)
// detail — never for a paginated "how many groups exist" list.
//
// Known divergence (P1b final review, deferred to P1c): upstream kafka-ui's
// getConsumerGroupsPage enriches (describes) the filtered page BEFORE sorting,
// so its MEMBERS/TOPIC_NUM/MESSAGES_BEHIND columns sort on real values;
// GroupService.Page sorts this thin ListGroups result directly (those fields
// are zero-value here), so ordering on those three columns degrades to a
// stable no-op. Closing the gap needs the same O(groups) describe fan-out
// upstream pays — its own P1c task with a size/perf check, not a bug here.
// See ADR-0005 §8.
type GroupAdminPort interface {
	// ListGroups reports every group's identity/state/coordinator only — no
	// members, no offsets (Members/Offsets/Protocol are always zero-value on
	// the returned GroupState; Coordinator, the host string, is also always
	// "" here — only CoordinatorID is available at this level).
	ListGroups(ctx context.Context, def Definition) ([]GroupState, error)
	// DescribeGroup reports one group's full detail: members (with their
	// partition assignments) and, for every topic-partition the group has
	// committed offsets against, the committed/end offset pair.
	DescribeGroup(ctx context.Context, def Definition, id string) (GroupState, error)
	// GroupsForTopic reports every group (detail-level, same shape as
	// DescribeGroup) that has committed offsets against topic.
	GroupsForTopic(ctx context.Context, def Definition, topic string) ([]GroupState, error)
	// ResetOffsets resets group id's committed offsets for spec.Topic per
	// spec's reset strategy.
	ResetOffsets(ctx context.Context, def Definition, id string, spec ResetSpec) error
	// DeleteGroup deletes consumer group id entirely.
	DeleteGroup(ctx context.Context, def Definition, id string) error
	// DeleteGroupOffsets deletes group id's committed offsets for topic only
	// (every partition the group has committed against that topic), leaving
	// the group and its other topics' offsets untouched.
	DeleteGroupOffsets(ctx context.Context, def Definition, id, topic string) error
}

// PartitionLogDir is one partition's on-disk footprint within a single log
// directory: its segment size and how far its local log lags the log end
// offset (or the future log, if this replica is mid-move).
type PartitionLogDir struct {
	Partition int32
	Size      int64
	OffsetLag int64
}

// TopicLogDirs groups a topic's partitions within one broker's log directory.
type TopicLogDirs struct {
	Topic      string
	Partitions []PartitionLogDir
}

// BrokerLogDirs is one (broker, directory) pair's log dir report: every
// topic-partition replica that broker stores under that directory. Error is
// set instead of Topics when the broker failed to describe that directory.
type BrokerLogDirs struct {
	Broker int32
	Dir    string
	Error  string
	Topics []TopicLogDirs
}

// ConfigSynonym is one fallback value for a ConfigEntry: lower-priority
// sources (e.g. a cluster-default) that would apply if the entry itself
// weren't dynamically overridden.
type ConfigSynonym struct{ Name, Value, Source string }

// ConfigEntry is one broker (or topic) configuration key as reported by the
// cluster. Source is the raw string form of the underlying driver's config
// source (e.g. kadm/kmsg's ConfigSource.String(), such as
// "DYNAMIC_BROKER_CONFIG") — it is *not* yet the contract's ConfigSource
// enum; that mapping (including the two vocabularies' naming mismatches) is
// the api layer's job (sourceToGenerated), not domain's.
type ConfigEntry struct {
	Name, Value, Source     string
	IsSensitive, IsReadOnly bool
	Synonyms                []ConfigSynonym
}

// StateScraper scrapes a cluster's runtime state from the cluster itself.
type StateScraper interface {
	FetchState(ctx context.Context, def Definition) (RuntimeState, error)
}

// BrokerAdminPort performs broker-scoped admin operations against a live
// cluster connection: log directory inspection/migration and configuration
// read/write. Unlike StateScraper (fed into a periodically-refreshed cache),
// callers go through this port on every request.
type BrokerAdminPort interface {
	// LogDirs reports per-broker log directory usage. brokers filters the
	// result to that set of broker IDs; empty/nil means all brokers.
	LogDirs(ctx context.Context, def Definition, brokers []int32) ([]BrokerLogDirs, error)
	// BrokerConfigs reports one broker's configuration entries.
	BrokerConfigs(ctx context.Context, def Definition, broker int32) ([]ConfigEntry, error)
	// AlterBrokerConfig incrementally sets a single broker config key,
	// leaving every other key untouched (ADR-0003 §6.1: implementations must
	// never use a full-state-replace alter API here — doing so would wipe
	// every other dynamic config on that broker).
	AlterBrokerConfig(ctx context.Context, def Definition, broker int32, name, value string) error
	// MoveReplicaLogDir migrates one topic-partition replica on broker to a
	// different log directory.
	MoveReplicaLogDir(ctx context.Context, def Definition, broker int32, topic string, partition int32, dir string) error
}

// ClientLifecycle manages the lifetime of a cluster's underlying long-lived
// client connection.
type ClientLifecycle interface {
	Invalidate(name string) // 关闭并丢弃该集群的底层客户端
	Close()
}

// OffsetRange is one partition's low/high watermark. End is the
// high-water-mark (the next offset that will be written, i.e. one past the
// last record — the same "end offset" convention ListEndOffsets/
// PartitionState.EndOffset already use elsewhere in this package), not the
// last written offset.
type OffsetRange struct{ Start, End int64 }

// RawRecord is one message as read off a partition, still in raw wire bytes
// — deserialization (via the configured serde) happens in the app layer
// (P1c Task 8), never here. KeySize/ValueSize/HeadersSize are the record's
// byte footprint (len(Key), len(Value), and the summed key+value byte length
// across Headers respectively), reported alongside the raw bytes so callers
// that only need sizes (e.g. throttling, list views) don't have to
// recompute them from Key/Value/Headers themselves.
type RawRecord struct {
	Partition   int32
	Offset      int64
	TimestampMs int64
	Key, Value  []byte // 原始字节；反序列化在 app 层（Task 8）
	Headers     map[string]string
	// Control marks a Kafka transaction marker (commit/abort). Control
	// records advance a reader's offset but are not user messages and must not
	// be included in message statistics or browse output. The regular reader
	// path leaves these records filtered; the analysis reader opts in when it
	// needs markers to prove progress across transactional tails.
	Control bool

	KeySize, ValueSize, HeadersSize int
}

// ReaderSession is one open, per-partition-seeked consume session (created by
// MessageReaderPort.Open). Poll fetches one batch; an empty, error-free
// result (nil, nil) means this round produced no records. A non-empty batch
// may accompany a non-nil error: callers must process the batch before
// handling the error because the underlying consumer may already have
// advanced its cursor. RawRecord.Control entries (when an analysis-capable
// reader opts into transaction markers) advance offsets but are not user
// messages. Close releases the underlying client; implementations must
// tolerate Close being called without a prior error and must not panic if
// called after the session's ctx is already done.
type ReaderSession interface {
	Poll(ctx context.Context) ([]RawRecord, error)
	Close()
}

// MessageReaderPort reads raw (undeserialized) records from a live cluster
// connection: partition watermarks, timestamp-to-offset lookups, and an
// open-a-session-then-poll consume path seeked to caller-chosen per-partition
// starting offsets. This is the bottom layer of the P1c message engine —
// Task 7's emitter (forward/backward/tailing) is the only consumer of Open/
// ReaderSession; PartitionRanges/OffsetsForTimestamp also back the app
// layer's own offset-resolution logic (e.g. turning a UI-selected "from this
// timestamp" into concrete per-partition starts before calling Open).
type MessageReaderPort interface {
	// PartitionRanges reports topic's low/high watermark per partition.
	// partitions filters the result to that set; empty/nil means every
	// partition of topic.
	PartitionRanges(ctx context.Context, def Definition, topic string, partitions []int32) (map[int32]OffsetRange, error)
	// OffsetsForTimestamp reports, per partition, the offset of the first
	// record with a timestamp >= tsMs (kadm's own ListOffsetsAfterMilli
	// semantics — a partition with no such record reports its own end
	// offset). partitions filters the same way as PartitionRanges.
	OffsetsForTimestamp(ctx context.Context, def Definition, topic string, partitions []int32, tsMs int64) (map[int32]int64, error)
	// Open starts a dedicated consume session against topic, seeked to
	// starts (partition -> starting offset) — one partition per map entry,
	// nothing implicit about "every partition" here (unlike PartitionRanges/
	// OffsetsForTimestamp's empty-means-all convention: Open always consumes
	// exactly the partitions starts names). The returned ReaderSession owns a
	// brand-new client (never the shared pooled one), so its Close never
	// affects any other caller.
	Open(ctx context.Context, def Definition, topic string, starts map[int32]int64) (ReaderSession, error)
}

// ProduceRecord is one record to write to topic's Partition (an explicit,
// caller-chosen partition -- there is no "let the broker/partitioner pick"
// option here, see MessageWriterPort.Produce's doc comment on why). Key/
// Value nil (as opposed to a non-nil empty []byte) means "don't send that
// side of the record at all" -- a genuinely absent Kafka key or value, not
// an empty-string one; app layer's Send (Task 11) maps its SendSpec.Key/
// Value *string nil-ness onto this same convention one-for-one.
type ProduceRecord struct {
	Partition int32
	Key       []byte
	Value     []byte
	Headers   map[string]string
}

// MessageWriterPort is the write-side counterpart to MessageReaderPort
// above: producing one record to a live cluster connection, and purging
// records (deleteRecords) from one or more partitions. Both are genuine
// cluster-scoped writes (P1c Task 11) -- unlike MessageReaderPort, which is
// read-only.
type MessageWriterPort interface {
	// Produce writes rec to topic, landing on rec.Partition exactly --
	// implementations MUST NOT let a hash/sticky partitioner re-route it to
	// a different partition based on rec.Key (kgo's default partitioner
	// does exactly that, silently, unless overridden -- see the infra
	// implementation's doc comment for the verified mechanism).
	Produce(ctx context.Context, def Definition, topic string, rec ProduceRecord) error
	// DeleteRecords purges every record up to (and including) each named
	// partition's current end offset -- i.e. empties it, kadm's
	// DeleteRecords "delete to this offset" semantics applied with that
	// offset set to the partition's own end (there is no "delete up to an
	// arbitrary offset" exposed here, only "delete everything currently
	// there"). partitions empty/nil means every partition of topic (same
	// empty-means-all convention as PartitionRanges/OffsetsForTimestamp).
	DeleteRecords(ctx context.Context, def Definition, topic string, partitions []int32) error
}

// ConfigSnapshot is the config wizard's view of the running configuration
// (P1c Task 13): Raw is the complete config.yaml deserialized into a generic
// tree, kept verbatim so a later Save (Task 14) can merge edits back without
// dropping fields this codebase doesn't model (auth/rbac/webclient/...);
// Clusters is that same config's kafka.clusters[] already parsed to domain
// Definitions, which Validate probes and Save rewrites.
type ConfigSnapshot struct {
	Raw      map[string]any
	Clusters []Definition
}

// PropertyValidation is one probed subsystem's verdict (mirrors the contract's
// ApplicationPropertyValidation): Error true with a human ErrorMessage when the
// probe failed, both zero when it passed.
type PropertyValidation struct {
	Error        bool
	ErrorMessage string
}

// ClusterValidation is one cluster's per-subsystem validation. Kafka is always
// probed (and so is a value, not a pointer). The rest are P2 subsystems --
// nil means "not configured / not probed", distinct from a configured-but-
// failing &PropertyValidation{Error:true} (P1c leaves them nil; see
// ConfigStorePort.Validate).
type ClusterValidation struct {
	Kafka             PropertyValidation
	SchemaRegistry    *PropertyValidation
	Ksqldb            *PropertyValidation
	PrometheusStorage *PropertyValidation
	KafkaConnects     map[string]PropertyValidation
}

// ConfigValidation is the whole config's validation, keyed by cluster name
// (mirrors the contract's ApplicationConfigValidation.clusters).
type ConfigValidation struct {
	Clusters map[string]ClusterValidation
}

// ConfigStorePort is the config wizard's persistence + probe port. P1c Task 13
// implements the read side (Current) and probe side (Validate); the write side
// (Save/SaveRelatedFile) is declared here so the port is stable, but its infra
// implementation lands in Task 14 (until then those two report a not-yet-
// implemented error).
type ConfigStorePort interface {
	// Current reads the running configuration into a ConfigSnapshot.
	Current() (ConfigSnapshot, error)
	// Validate probes each of snap.Clusters for reachability, returning a
	// per-cluster verdict. P1c probes Kafka connectivity only; other
	// subsystems stay nil.
	Validate(ctx context.Context, snap ConfigSnapshot) (ConfigValidation, error)
	// Save merges snap's edits back into config.yaml (Task 14).
	Save(ctx context.Context, snap ConfigSnapshot) error
	// Parse decodes data as a config document and validates its schema
	// (cluster name/bootstrapServers required, no duplicate names, known
	// masking enums), returning the snapshot for Save/Reload. A schema
	// violation wraps ErrInvalidConfig.
	Parse(data []byte) (ConfigSnapshot, error)
	// Backup copies the existing config file to a sibling timestamped .bak
	// and returns that path; when no config file exists it returns "".
	Backup() (string, error)
	// SaveRelatedFile stores an uploaded truststore/keystore/etc. and returns
	// its on-disk location (Task 14).
	SaveRelatedFile(ctx context.Context, name string, content []byte) (location string, err error)
}

// SchemaReference is one schema's reference to another registered schema
// (contract's SchemaReference / sr.SchemaReference): Name is the reference
// name used inside the referencing schema's own definition (e.g. an Avro/
// Protobuf import alias), Subject/Version pin the referenced schema.
type SchemaReference struct {
	Name    string
	Subject string
	Version int
}

// SchemaVersion is one registered version of a subject's schema (contract's
// SchemaSubject, read side): ID is the registry-global schema ID (shared
// across subjects when the same schema text is registered more than once),
// Version is the subject-local version number, SchemaType is the contract's
// SchemaType enum spelling ("AVRO"/"JSON"/"PROTOBUF" -- empty/omitted on the
// wire defaults to AVRO, mirrored here by sr.SchemaType's own zero value),
// CompatLevel is the subject's *effective* compatibility level (its own
// override if set, else the registry's global default -- contract's
// SchemaSubject.compatibilityLevel is required, so the infra implementation
// resolves this fallback itself rather than leaving it to callers).
type SchemaVersion struct {
	ID          int
	Subject     string
	Version     int
	Schema      string
	SchemaType  string
	CompatLevel string
	References  []SchemaReference
}

// RawSchema is a schema fetched by its registry-global ID (contract's
// SchemaSubject minus subject/version/compatibility -- SchemaByID has no
// notion of "which subject", a schema ID can be shared by several). This is
// the shape the serde layer (P2a Task 6) pulls to decode Confluent-wire-
// format Avro/JSON payloads.
type RawSchema struct {
	Schema     string
	SchemaType string
}

// NewSchema is a schema submitted for registration (contract's
// NewSchemaSubject) or compatibility checking: References follow the same
// semantics as SchemaVersion.References above.
type NewSchema struct {
	Schema     string
	SchemaType string
	References []SchemaReference
}

// SchemaRegistryPort is the read/write/compatibility surface over one
// cluster's Schema Registry connection (P2a Task 2), implemented by
// infra/schemaregistry's Pool over franz-go's pkg/sr client. All methods
// take the owning cluster's Definition so the infra side can resolve (and
// cache) the right *sr.Client for def.SchemaRegistry -- there is no separate
// "open" step, mirroring the read-side ports elsewhere in this file
// (StateScraper et al.) rather than the connection-scoped MessageReaderPort/
// MessageWriterPort pair.
//
// version parameters are strings, not ints, throughout: "latest" is a
// distinct sentinel from any numeric version (matches the contract's own
// {version} path parameter, which accepts either), and implementations MUST
// treat it as such rather than trying to parse it as a number.
type SchemaRegistryPort interface {
	// Subjects lists every registered subject name.
	Subjects(ctx context.Context, def Definition) ([]string, error)
	// SchemaByVersion fetches one subject's schema at version (a numeric
	// string, or "latest").
	SchemaByVersion(ctx context.Context, def Definition, subject, version string) (SchemaVersion, error)
	// SchemaByID fetches a schema by its registry-global ID, independent of
	// subject/version.
	SchemaByID(ctx context.Context, def Definition, id int) (RawSchema, error)
	// Versions lists a subject's registered version numbers.
	Versions(ctx context.Context, def Definition, subject string) ([]int, error)
	// Register submits a new schema (or returns the existing ID, if an
	// identical schema is already registered under subject -- registry-
	// native idempotency, not something the caller needs to check for)
	// under subject, returning its registry-global ID.
	Register(ctx context.Context, def Definition, subject string, s NewSchema) (int, error)
	// DeleteSubject removes every version of subject (soft delete by
	// default; permanent=true additionally purges the soft-deleted marker,
	// letting the subject be recreated from scratch), returning the
	// version numbers that were deleted.
	DeleteSubject(ctx context.Context, def Definition, subject string, permanent bool) ([]int, error)
	// DeleteVersion removes one version of subject (again numeric-string-
	// or-"latest"), returning the concrete version number that was
	// resolved and deleted (never -1, even when version=="latest").
	DeleteVersion(ctx context.Context, def Definition, subject, version string, permanent bool) (int, error)
	// GlobalCompat reads the registry-wide default compatibility level.
	GlobalCompat(ctx context.Context, def Definition) (string, error)
	// SetGlobalCompat writes the registry-wide default compatibility level.
	SetGlobalCompat(ctx context.Context, def Definition, level string) error
	// SubjectCompat reads subject's effective compatibility level (its own
	// override if set, else the registry's global default).
	SubjectCompat(ctx context.Context, def Definition, subject string) (string, error)
	// SetSubjectCompat writes subject's own compatibility-level override.
	SetSubjectCompat(ctx context.Context, def Definition, subject, level string) error
	// CheckCompat reports whether s would be compatible with subject's
	// latest registered version, under subject's effective compatibility
	// rule -- a dry run; it never registers s.
	CheckCompat(ctx context.Context, def Definition, subject string, s NewSchema) (bool, error)
}

// ConnectCluster is one configured Connect worker's reachability probe result
// (contract's Connect, address/identity-facing subset). Name/Address are
// copied straight from the matching ConnectSpec -- no REST call is needed
// for those -- but the aggregating Connects port method only includes an
// entry here once a cheap REST probe (GET /connectors) against that worker
// has actually succeeded; an unreachable Connect is skipped entirely by that
// aggregation (P2b-D3), never reported with some "offline" placeholder value.
// The contract's richer stats fields (connectorsCount, version, commit, ...)
// need additional per-connector fan-out this port method does not perform,
// and are left to a later task's aggregation.
type ConnectCluster struct {
	Name    string
	Address string
}

// ConnectorRef names one connector as seen while aggregating across every
// Connect cluster configured on a Definition (AllConnectors): which Connect
// it lives on (ConnectName), its own Name, plus its status (State/WorkerID),
// Type and task counts (TasksCount/FailedTasksCount) -- all populated from
// Kafka Connect's bulk KIP-465 `GET /connectors?expand=status&expand=info`
// endpoint (one REST call per configured Connect worker, the same call count
// as a plain `GET /connectors` -- NOT an N+1 fan-out per connector found).
// Fuller per-connector detail still only fetched one at a time via Connector
// once a caller actually wants it: the full Config map, per-task IDs, and
// Topics (Kafka Connect's bulk endpoint doesn't carry those).
type ConnectorRef struct {
	ConnectName string
	Name        string
	Type        string
	State       string
	WorkerID    string

	TasksCount       int
	FailedTasksCount int
}

// ConnectorPlugin is one connector class a Connect worker has available to
// create connectors from (contract's ConnectorPlugin -- the contract models
// only the class name, even though the underlying Connect REST response
// carries more, e.g. type/version).
type ConnectorPlugin struct {
	Class string
}

// Connector is one connector's assembled detail (contract's Connector),
// gathered from three separate Connect REST calls -- GET /connectors/{name}
// (name/type/task IDs), GET /connectors/{name}/status (state/trace/
// workerId), GET /connectors/{name}/config (its full config) -- the Connect
// REST API has no single "everything" response for a connector, so
// implementations of Connector (the port method) must assemble it from all
// three. TaskIDs is the set of task indices currently assigned (contract's
// Connector.Tasks []TaskId, minus the redundant per-entry connector name
// TaskId.Connector always repeats); full per-task detail (state/trace/
// config) is fetched separately via ConnectorTasks, not carried here.
type Connector struct {
	Name        string
	ConnectName string
	Type        string
	State       string
	Trace       string
	WorkerID    string
	Config      map[string]any
	TaskIDs     []int
	Topics      []string
}

// ConnectorTask is one connector task's assembled detail (contract's Task +
// nested TaskStatus), gathered from GET /connectors/{name}/tasks (ID +
// config) and GET /connectors/{name}/status (state/trace/workerId per task)
// -- again no single Connect REST response carries both.
type ConnectorTask struct {
	ID       int
	State    string
	Trace    string
	WorkerID string
	Config   map[string]any
}

// PluginConfigDef is one connector plugin config key's static definition, as
// returned within a plugin config validation response (contract's
// ConnectorPluginConfigDefinition).
type PluginConfigDef struct {
	Name          string
	Type          string
	Required      bool
	DefaultValue  string
	Importance    string
	Documentation string
	Group         string
	Width         string
	DisplayName   string
	Order         int
	Dependents    []string
}

// PluginConfigValue is one connector plugin config key's validated value, as
// returned within a plugin config validation response (contract's
// ConnectorPluginConfigValue).
type PluginConfigValue struct {
	Name              string
	Value             string
	RecommendedValues []string
	Errors            []string
	Visible           bool
}

// PluginConfigEntry pairs one config key's static definition with its
// validated value (contract's ConnectorPluginConfig).
type PluginConfigEntry struct {
	Definition PluginConfigDef
	Value      PluginConfigValue
}

// PluginValidation is a connector plugin config dry-run validation's result
// (contract's ConnectorPluginConfigValidationResponse) -- a dry run against
// the plugin's own config definitions, never registering a connector.
type PluginValidation struct {
	Name       string
	ErrorCount int
	Groups     []string
	Configs    []PluginConfigEntry
}

// KafkaConnectPort is the read/write/action surface over every Connect
// worker cluster configured on a Definition (def.Connects) -- P2b Task 2,
// implemented by infra/connect's Pool over net/http. Like SchemaRegistryPort,
// every method takes the owning cluster's Definition so the infra side can
// resolve (and cache) the right *http.Client for the (Definition,
// connectName) pair; there is no separate "open" step.
//
// Connects and AllConnectors are the two aggregating methods -- they fan out
// across every def.Connects entry and, per P2b-D3, skip (not fail on) any
// single Connect cluster whose REST call errors, returning the rest. Every
// other method addresses one connectName and propagates that Connect's
// error verbatim -- a caller that already named a specific Connect gets no
// such best-effort treatment.
//
// connectName not matching any def.Connects[].Name is reported as
// ErrUnknownConnect (infra/connect's sentinel) by every single-Connect
// method's client resolution -- this port's doc comment names it since
// callers (app layer) match on it the same way they match ErrUnknownCluster
// against Resolver.Lookup.
type KafkaConnectPort interface {
	// Connects probes every configured Connect cluster and reports the ones
	// that answered (P2b-D3: unreachable ones are skipped, not failed).
	Connects(ctx context.Context, def Definition) ([]ConnectCluster, error)
	// Plugins lists connectName's available connector plugin classes.
	Plugins(ctx context.Context, def Definition, connectName string) ([]ConnectorPlugin, error)
	// ValidatePlugin dry-runs cfg against pluginName's config definitions on
	// connectName, without creating or altering any connector.
	ValidatePlugin(ctx context.Context, def Definition, connectName, pluginName string, cfg map[string]any) (PluginValidation, error)
	// AllConnectors lists every connector across every configured Connect
	// cluster (P2b-D3: unreachable Connects are skipped, not failed).
	AllConnectors(ctx context.Context, def Definition) ([]ConnectorRef, error)
	// Connectors lists connectName's connector names.
	Connectors(ctx context.Context, def Definition, connectName string) ([]string, error)
	// Connector fetches name's full assembled detail on connectName.
	Connector(ctx context.Context, def Definition, connectName, name string) (Connector, error)
	// ConnectorConfig fetches name's current config on connectName.
	ConnectorConfig(ctx context.Context, def Definition, connectName, name string) (map[string]any, error)
	// ConnectorTasks fetches name's tasks (assembled config + status) on
	// connectName.
	ConnectorTasks(ctx context.Context, def Definition, connectName, name string) ([]ConnectorTask, error)
	// CreateConnector creates a new connector named name on connectName with
	// cfg, returning its assembled detail.
	CreateConnector(ctx context.Context, def Definition, connectName string, name string, cfg map[string]any) (Connector, error)
	// DeleteConnector deletes name from connectName.
	DeleteConnector(ctx context.Context, def Definition, connectName, name string) error
	// SetConnectorConfig replaces name's whole config on connectName,
	// returning its assembled detail (Connect's own PUT .../config semantics
	// -- a full replace, not an incremental merge; unlike TopicAdminPort/
	// BrokerAdminPort's incremental-only convention elsewhere in this file,
	// this is what the Connect REST API itself offers).
	SetConnectorConfig(ctx context.Context, def Definition, connectName, name string, cfg map[string]any) (Connector, error)
	// UpdateConnectorState applies action (the contract's ConnectorAction
	// enum string: PAUSE/RESUME/STOP/RESTART/RESTART_ALL_TASKS/
	// RESTART_FAILED_TASKS -- P2b-D4) to name on connectName.
	UpdateConnectorState(ctx context.Context, def Definition, connectName, name, action string) error
	// ResetConnectorOffsets resets name's committed offsets on connectName
	// (Connect 3.6+, and only while name is STOPPED -- this port method does
	// not itself gate on either precondition, an unmet one surfaces as
	// whatever error Connect itself returns).
	ResetConnectorOffsets(ctx context.Context, def Definition, connectName, name string) error
	// RestartConnectorTask restarts one task (by index) of name on
	// connectName.
	RestartConnectorTask(ctx context.Context, def Definition, connectName, name string, taskID int) error
}

// Snapshot 把运行时状态压缩为列表视图条目（/api/clusters 用）。
func (s RuntimeState) Snapshot() Snapshot {
	return Snapshot{
		Definition:  s.Definition,
		Status:      s.Status,
		BrokerCount: len(s.Brokers),
		Features:    s.Features(),
	}
}
