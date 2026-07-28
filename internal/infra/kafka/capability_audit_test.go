package kafka

// P1-P3 所需 kadm 能力的编译期审计：任何一行编译失败即说明该能力
// 在当前 franz-go(v1.21.5)/kadm(v1.18.0) 版本缺失，须在 ADR-0003 记录替代
// （kmsg 直发或版本升级）。核查过程与结论见 docs/adr/0003-franz-go-capability.md。
//
// 本文件相对 brief 原始 17 行清单的改动（均为 go vet 编译验证，非猜测）：
//   - ListOffsets（brief 猜测名）不存在，实名拆成 "List<Kind>Offsets" 一族，逐一列出。
//   - 新增 AlterBrokerReplicaLogDirs / AlterAllReplicaLogDirs：brief 预告 AlterReplicaLogDirs
//     "可能需 kmsg 层"，但该猜测名虽不存在，kadm 下有对应的真实方法（改名而非缺失）。
//   - 新增 DescribeConsumerGroups：与 DescribeGroups 并存的独立真实方法（更丰富的分组详情，
//     含成员分配），"groups 全家" 的一部分。
//   - AlterBrokerConfigsState、DescribeGroups：brief 特别标注"可能实名不同"，经核查两者均为
//     kadm 的真实导出方法，原名直接命中，无需改名。
//
// Task 6 追加（DescribeBrokerConfigs/AlterBrokerConfigs）：两者均直接命中真实导出方法，源码见
// $(go env GOMODCACHE)/github.com/twmb/franz-go/pkg/kadm@v1.18.0/configs.go:92,255；无需改名。
// AlterBrokerConfigs 是增量修改（底层 IncrementalAlterConfigs），与上面已入审计表多时的
// AlterBrokerConfigsState（全量替换，"All prior configuration is lost."）是同族但语义互斥的两个
// 真实方法——ADR-0003 §6.1 已记录选错的后果，本任务的 Pool.AlterBrokerConfig 只调用前者，严禁改用
// 后者（state_integration_test.go 的 TestAlterBrokerConfigIsIncremental 把这条锁死为回归测试）。
//
// Task 4 追加（DescribeTopicConfigs）：与 DescribeBrokerConfigs 同一文件（configs.go:77），brief
// 猜测名直接命中真实导出方法，无需改名；ListStartOffsets/ListEndOffsets/DescribeACLs/
// DescribeProducers 在本文件中已因更早任务入表，Task 4 直接复用、未新增。
//
// Task 5 追加（Topics 写面）：
//   - DeleteTopics/AlterTopicConfigs：brief 猜测名直接命中真实导出方法（topics.go:317,
//     configs.go:228），无需改名。
//   - CreatePartitions 名称虽命中，但语义是**增量**（"adding \"add\" partitions"，
//     topics.go:568）——契约 PartitionsIncrease.totalPartitionsCount 是**目标总数**语义
//     （见同名 openapi schema），与 CreatePartitions 的 add 参数语义不符；kadm 源码同一文件
//     里的 UpdatePartitions（topics.go:585，"setting the final partition count to \"set\""）
//     才是目标总数语义，本任务 Pool.CreatePartitions 实际调用它，不调用 kadm.CreatePartitions
//     （审计表两者都登记，避免日后误选前者）。
//   - kadm.NewACLs/ACLBuilder 链式方法/kadm.ACLPatternLiteral/kadm.TopicsSet.Add：补记
//     Task 4 遗漏的审计项（topics.go 的 TopicAcls/ActiveProducers 已经在用，只是当时没有
//     一并入表）——本任务借 Step 1 一并补齐，审计表letter-complete。
//
// Task 6 追加（P1b Consumer Groups 全组）：
//   - DeleteGroup（单数）/DeleteOffsets：brief 猜测名均直接命中真实导出方法（groups.go:485,
//     1351），无需改名。DeleteGroup 内部就是 `DeleteGroups(ctx, group)` 的单组包装，第二返回值
//     已经是该组的 `.Err`（源码 `return g, g.Err`），调用方不需要再单独查一次 map。
//   - **重要发现（brief 未预期，ADR-0003 §6 追加第 6 条记录）：DescribeConsumerGroups 不能用于
//     本任务。** 已入表多时的 DescribeConsumerGroups 是 KIP-848 "next generation" 消费组协议
//     专用方法（源码原文："This is the 'next generation' equivalent of DescribeGroups and is
//     specifically for consumer groups using the new consumer group protocol"），底层走
//     `kmsg.ConsumerGroupDescribeRequest`（协议 ConsumerGroupDescribe API），对使用经典协议
//     （`subscribe()`/join-group、绝大多数现网部署，包括 T7 用的 testcontainers 默认镜像）的
//     消费组会失败或返回空。本任务 DescribeGroup/GroupsForTopic 改用经典 DescribeGroups
//     （本表已有，未新增），其 `DescribedGroup{Coordinator, State, ProtocolType, Protocol,
//     Members []DescribedGroupMember{Join, Assigned GroupMemberAssignment}}` 形状也与本任务
//     domain GroupMember/TopicPartitions 的字段（MemberID/ClientID/Host/Assignments）逐字对应，
//     不需要额外转换层。
import (
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var (
	// Clusters: 集群级元数据快照（FetchState/StateCache 用，Task 2）
	_ = (*kadm.Client).Metadata

	// Brokers: log dirs 查（Describe）+ 改（Alter）
	_ = (*kadm.Client).DescribeAllLogDirs
	_ = (*kadm.Client).DescribeBrokerLogDirs     // T5: per-broker 过滤查询候选（实现最终选用 DescribeAllLogDirs+客户端过滤，见 state.go LogDirs）
	_ = (*kadm.Client).AlterBrokerReplicaLogDirs // T6: MoveReplicaLogDir 用（见 state.go）
	_ = (*kadm.Client).AlterAllReplicaLogDirs
	_ = (*kadm.Client).AlterBrokerConfigsState // Brokers: 配置修改（状态化 API，全量替换——严禁用于 T6 AlterBrokerConfig）

	// Brokers: 配置查（Describe）+ 改（Alter，增量）——T6
	_ = (*kadm.Client).DescribeBrokerConfigs
	_ = (*kadm.Client).AlterBrokerConfigs // 增量修改（IncrementalAlterConfigs）；不是上面的 AlterBrokerConfigsState

	// Topics: 分区重分配查（List）+ 改（Alter）
	_ = (*kadm.Client).AlterPartitionAssignments
	_ = (*kadm.Client).ListPartitionReassignments
	_ = (*kadm.Client).DescribeProducers    // Topics: active producers
	_ = (*kadm.Client).CreateTopics         // Topics: T5 集成测试建 topic 用（真实分区/ISR 计数验证）
	_ = (*kadm.Client).DescribeTopicConfigs // Topics: T4 topic 配置读（实名与 brief 猜测一致，直接命中）

	// Quotas 查改
	_ = (*kadm.Client).DescribeClientQuotas
	_ = (*kadm.Client).AlterClientQuotas

	_ = (*kadm.Client).DeleteRecords // Messages: purge

	// Topics: 写面（T5）
	_ = (*kadm.Client).DeleteTopics           // deleteTopic/Recreate 用
	_ = (*kadm.Client).AlterTopicConfigs      // 增量修改（IncrementalAlterConfigs）；严禁改用 AlterTopicConfigsState
	_ = (*kadm.Client).CreatePartitions       // 审计对照：增量语义（add），本任务不使用，见上方注释
	_ = (*kadm.Client).UpdatePartitions       // 目标总数语义（set）——Pool.CreatePartitions 实际调用这个
	_ = (*kadm.Client).AlterTopicConfigsState // 审计对照：全量替换，严禁用于 T5 UpdateConfigs/AlterTopicConfig

	// ACLs 构造链（Task 4 遗漏补记，T5 一并入表）
	_                 = kadm.NewACLs
	_                 = (*kadm.ACLBuilder).Topics
	_                 = (*kadm.ACLBuilder).ResourcePatternType
	_                 = (*kadm.ACLBuilder).Allow
	_                 = (*kadm.ACLBuilder).AllowHosts
	_                 = (*kadm.ACLBuilder).Deny
	_                 = (*kadm.ACLBuilder).DenyHosts
	_                 = (*kadm.ACLBuilder).Operations
	_                 = (*kadm.TopicsSet).Add
	_ kadm.ACLPattern = kadm.ACLPatternLiteral

	// Offsets 全家（ListOffsets 猜测名不存在，实名按 "List<Kind>Offsets" 拆分）
	_ = (*kadm.Client).ListStartOffsets
	_ = (*kadm.Client).ListEndOffsets
	_ = (*kadm.Client).ListCommittedOffsets
	_ = (*kadm.Client).ListOffsetsAfterMilli
	_ = (*kadm.Client).ListMaxTimestampOffsets
	_ = (*kadm.Client).ListLatestRemoteOffsets
	_ = (*kadm.Client).ListLocalLogStartOffsets

	// ConsumerGroups 全家
	_ = (*kadm.Client).ListGroups
	_ = (*kadm.Client).DescribeGroups
	_ = (*kadm.Client).DescribeConsumerGroups
	_ = (*kadm.Client).FetchOffsets
	_ = (*kadm.Client).CommitOffsets // ConsumerGroups: reset
	_ = (*kadm.Client).DeleteGroups
	_ = (*kadm.Client).DeleteGroup   // T6: 单数版，DeleteConsumerGroup 用它而非批量 DeleteGroups（见 groups.go）
	_ = (*kadm.Client).DeleteOffsets // T6: OffsetDelete API，deleteConsumerGroupOffsets 用（见 groups.go）

	// ACLs 全家
	_ = (*kadm.Client).DescribeACLs
	_ = (*kadm.Client).CreateACLs
	_ = (*kadm.Client).DeleteACLs
)

// P1c Task 6 追加（Messages: MessageReaderPort + kgo 消费适配器）——不要与上面
// 标注 "T6" 的 P1b Task 6（Consumer Groups）混淆，那是 P1b 阶段的任务编号。
// ListStartOffsets/ListEndOffsets/ListOffsetsAfterMilli 已在上面的「Offsets 全家」
// 表中入表（P1b），本任务直接复用、未新增；本任务新增的是 kgo（消费端，此前
// capability audit 只覆盖过 kadm/kmsg，本文件首次引入 kgo 符号）的四个符号，
// 全部直接命中真实导出符号，无需改名：
//   - kgo.ConsumePartitions（consumer.go 的 ConsumerOpt，逐 partition 指定起始 Offset）
//   - kgo.NewOffset（返回 Offset 值类型，.At(n) 链式设置绝对位点）
//   - (*kgo.Client).PollFetches / (*kgo.Client).Close（既有 groups_integration_test.go
//     已用过这两个方法验证过存在，此前未入审计表——本任务补记）
var (
	_ = kgo.ConsumePartitions
	_ = kgo.NewOffset
	_ = (*kgo.Client).PollFetches
	_ = (*kgo.Client).Close
)

// P1c Task 11 追加（Messages: MessageWriterPort — produce + deleteRecords）：
//   - DeleteRecords 已在上面「Offsets 全家」上方早入表（"Messages: purge" 注释，P1b 时代占位，
//     未被任何调用方使用过）——本任务是它的第一个真实调用方，签名核对：
//     `(cl *Client) DeleteRecords(ctx context.Context, os Offsets) (DeleteRecordsResponses, error)`
//     （topics.go:461，源码见 kadm@v1.18.0/topics.go）。`os[topic][partition].At` 是"删至此 offset
//     （不含）"的语义——设 `At=end offset`（ListEndOffsets 的值）即可清空该分区全部记录，这正是
//     brief 要的"删至各分区 end（全清）"。`kadm.Offsets`（`map[string]map[int32]Offset`）与
//     `(*Offsets).Add(Offset)` 均直接命中真实导出符号，无需改名；`ListedOffsets.Offsets()`
//     （metadata.go:372）把 `ListEndOffsets` 的返回值直接转成 `kadm.Offsets`（`At: listedOffset.
//     Offset`）——比手动拼 `Offsets{}`+`Add` 更省一层, 但本任务的 `DeleteRecords` 实现仍手动
//     `Add` 逐分区过滤（先应用 `partitionSet` 排除未请求的分区), 未直接用 `.Offsets()`，因为后者
//     会把 topic 的全部分区都转进去, 不满足"只删调用方点名的分区"这个前提。
//     `DeleteRecordsResponses.Error()`（topics.go:438）用作 first-error 提取, 直接命中。
//   - Produce 的「固定分区」陷阱验证结论（brief §「固定分区」全文照抄的疑点, 本任务源码核实）：
//     kgo 默认 partitioner（StickyKeyPartitioner）确实按 key 重新散列、忽略 `Record.Partition`
//     （partitioner.go 的 `stickyKeyTopicPartitioner.Partition` 实现）；`kgo.ManualPartitioner()`
//     （partitioner.go:118）的文档原文："simply returns the Partition field that is already set on
//     any record" —— 与 `kgo.Record.Partition` 字段自己的文档（record_and_fetch.go:100 附近）
//     "If you use the ManualPartitioner, the value of this field is always the partition chosen
//     when producing" 互相印证, 两处源码合起来就是 brief 推荐做法的确切依据。`kgo.RecordPartitioner`
//     （config.go:1318, 签名 `func RecordPartitioner(partitioner Partitioner) ProducerOpt`）是挂载
//     该 partitioner 的 client 级选项——brief 推荐的"专用 producer client"做法（`kgo.NewClient
//     (append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner())))`）经此核实成立, 本任务照办,
//     未发现需要向 ADR-0003 回写的偏差（全部符号名与签名首试命中）。
//   - `(*kgo.Client).ProduceSync`（producer.go:352, 签名 `ProduceSync(ctx context.Context, rs
//     ...*Record) ProduceResults`）与 `ProduceResults.FirstErr() error`（producer.go:326）：
//     first-error 提取, 直接命中, 与 messages_integration_test.go（Task 6 既有集成测试）里已经
//     在用的写法一致。`kgo.Record{Topic, Partition, Key, Value, Headers}` 与
//     `kgo.RecordHeader{Key string, Value []byte}` 字段名均直接命中, 无需改名。
var (
	_ = (*kgo.Client).ProduceSync
	_ = kgo.RecordPartitioner
	_ = kgo.ManualPartitioner
	_ = (*kadm.Client).DeleteRecords
	_ = (*kadm.Offsets).Add
	_ kadm.DeleteRecordsResponses
)

// P1c Task 13 追加（config wizard 连通性探测 ProbeConnectivity）：
//   - `(*kgo.Client).Ping(ctx context.Context) error`（client.go:633，franz-go@v1.21.5）：
//     强制向全部 seed broker 建连并做一次心跳，任一 seed 不可达即返回非 nil——正是
//     validateConfig 要的"这个集群的 Kafka 连得通吗"最轻探测，无需 kadm 全量 Metadata。
//     直接命中真实导出符号，签名首试通过，无需回写 ADR-0003。
var (
	_ = (*kgo.Client).Ping
)

// Quorum: kadm 无对应方法（(*kadm.Client).DescribeMetadataQuorum 编译失败，已核实）。
// 回退方案是 kmsg 协议层直发：kmsg.DescribeQuorumRequest（协议 API key 55，KIP-595），
// 经 kgo.Client.Request(ctx, req) 或 req.RequestWith(ctx, cl) 发送。以下编译期证明该
// 回退类型真实存在且满足 kmsg.Request 接口（Key/MaxVersion/AppendTo/ReadFrom 等）。
var (
	_ kmsg.Request = (*kmsg.DescribeQuorumRequest)(nil)
	_              = kmsg.NewPtrDescribeQuorumRequest
)

// P2c Task 2/3 追加（ACL 写面 + Quota 读写面）。CreateACLs/DeleteACLs/DescribeACLs、
// NewACLs、ACLBuilder.{ResourcePatternType,Allow,AllowHosts,Deny,DenyHosts,Operations,Topics}、
// ACLPatternLiteral、DescribeClientQuotas/AlterClientQuotas 已在前面表内，本块只补新符号。
var (
	// ACL：反向构造需要的资源维度设置器 + 非 Literal 模式常量
	_ = (*kadm.ACLBuilder).AnyResource
	_ = (*kadm.ACLBuilder).Groups
	_ = (*kadm.ACLBuilder).Clusters
	_ = (*kadm.ACLBuilder).TransactionalIDs
	_ = (*kadm.ACLBuilder).DelegationTokens
	_ kadm.ACLPattern = kadm.ACLPatternPrefixed
	_ kadm.ACLPattern = kadm.ACLPatternMatch
	_ kadm.ACLPattern = kadm.ACLPatternAny
	// ACL：契约文本→kadm operation 的反向解析（归一化：去下划线/破折号、小写）
	_ = kmsg.ParseACLOperation

	// Quota：DescribeClientQuotas / AlterClientQuotas 的输入输出类型族
	_ kadm.DescribeClientQuotaComponent
	_ kadm.DescribedClientQuota
	_ kadm.DescribedClientQuotas
	_ kadm.ClientQuotaEntity
	_ kadm.ClientQuotaEntityComponent
	_ kadm.ClientQuotaValue
	_ kadm.ClientQuotaValues
	_ kadm.AlterClientQuotaEntry
	_ kadm.AlterClientQuotaOp
	_ kadm.AlteredClientQuota
	_ kadm.AlteredClientQuotas
)
