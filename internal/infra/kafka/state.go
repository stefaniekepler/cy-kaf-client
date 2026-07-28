package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// FetchState 一次抓取集群运行时快照：元数据（brokers/controller/topics/partitions）
// + log dirs 磁盘用量。上游语义对应 ScrapedClusterState 的 P1a 子集。
func (p *Pool) FetchState(ctx context.Context, def cluster.Definition) (cluster.RuntimeState, error) {
	c, err := p.clientFor(def)
	if err != nil {
		return offlineState(def, err), err
	}
	meta, err := c.adm.Metadata(ctx)
	if err != nil {
		return offlineState(def, err), fmt.Errorf("fetch metadata: %w", err)
	}
	st := cluster.RuntimeState{
		Definition:  def,
		Status:      cluster.StatusOnline,
		Controller:  meta.Controller,
		RefreshedAt: time.Now(),
	}
	for _, b := range meta.Brokers {
		bi := cluster.BrokerInfo{ID: b.NodeID, Host: b.Host, Port: b.Port}
		if b.Rack != nil {
			bi.Rack = *b.Rack
		}
		st.Brokers = append(st.Brokers, bi)
	}
	perBroker := map[int32]*cluster.BrokerInfo{}
	for i := range st.Brokers {
		perBroker[st.Brokers[i].ID] = &st.Brokers[i]
	}
	st.TopicCount, st.Partitions, st.Topics = tallyPartitions(meta, perBroker)
	topicIdx := map[string]*cluster.TopicState{}
	for i := range st.Topics {
		topicIdx[st.Topics[i].Name] = &st.Topics[i]
	}

	// Offsets：每种失败独立不致命（低/高水位各拉一次，互不依赖），保留
	// tallyPartitions 已经置好的 -1 占位（见其分区循环里的 StartOffset/EndOffset 初始化）。
	if starts, err := c.adm.ListStartOffsets(ctx); err == nil {
		applyOffsets(st.Topics, starts, func(ps *cluster.PartitionState) *int64 { return &ps.StartOffset })
	} else {
		slog.Debug("list start offsets failed", "cluster", def.Name, "err", err)
	}
	if ends, err := c.adm.ListEndOffsets(ctx); err == nil {
		applyOffsets(st.Topics, ends, func(ps *cluster.PartitionState) *int64 { return &ps.EndOffset })
	} else {
		slog.Debug("list end offsets failed", "cluster", def.Name, "err", err)
	}

	// 磁盘用量：logdirs 失败不致命（如权限受限），留空并继续
	if dirs, err := c.adm.DescribeAllLogDirs(ctx, nil); err == nil {
		usage := map[int32]*cluster.DiskUsage{}
		dirs.Each(func(d kadm.DescribedLogDir) {
			u, ok := usage[d.Broker]
			if !ok {
				u = &cluster.DiskUsage{Broker: d.Broker}
				usage[d.Broker] = u
			}
			d.Topics.Each(func(pt kadm.DescribedLogDirPartition) {
				u.SegmentSize += pt.Size
				u.SegmentCount++
				if ts, ok := topicIdx[pt.Topic]; ok {
					ts.SegmentSize += pt.Size
					ts.SegmentCount++
				}
			})
		})
		for _, u := range usage {
			st.Disk = append(st.Disk, *u)
		}
	} else {
		// 不致命：st.Disk 留空、这一轮快照仍然返回 online——只在 Debug 级别留痕，
		// 不提升为 Warn（不同于 refreshOne 的整体刷新失败）。
		slog.Debug("logdirs describe failed", "cluster", def.Name, "err", err)
	}

	// TOPIC_DELETION feature 动态化（P1b Task 5）：从任一 broker 的
	// delete.topic.enable 配置读取，读不到（无 broker / describe 失败 / 键缺失 /
	// 解析失败）一律默认 true（KRaft 自身默认开启）——非致命，同 logdirs/offsets 的
	// 降级约定，不能让一次 describe 失败被误读成"该集群已禁用删除"。
	st.TopicDeletionEnabled = true
	var cfgs []cluster.ConfigEntry
	if len(st.Brokers) > 0 {
		if cfgs, err = p.BrokerConfigs(ctx, def, st.Brokers[0].ID); err == nil {
			st.TopicDeletionEnabled = topicDeletionEnabledFrom(cfgs)
		} else {
			slog.Debug("delete.topic.enable read failed", "cluster", def.Name, "err", err)
		}
	}
	if version, err := detectKafkaVersion(ctx, cfgs, func(ctx context.Context) (string, error) {
		return guessKafkaVersion(ctx, c.adm)
	}); err == nil {
		st.Version = version
	} else {
		slog.Debug("broker version detection failed", "cluster", def.Name, "err", err)
	}
	return st, nil
}

func normalizeKafkaVersion(raw string) string {
	v := strings.TrimSpace(raw)
	if strings.HasPrefix(v, "at least v") {
		return "at least " + strings.TrimPrefix(v, "at least v")
	}
	v = strings.TrimPrefix(v, "v")
	if base, _, ok := strings.Cut(v, "-IV"); ok {
		return base
	}
	return v
}

func detectKafkaVersion(
	ctx context.Context,
	cfgs []cluster.ConfigEntry,
	guess func(context.Context) (string, error),
) (string, error) {
	for _, cfg := range cfgs {
		if cfg.Name == "inter.broker.protocol.version" {
			if version := normalizeKafkaVersion(cfg.Value); version != "" {
				return version, nil
			}
		}
	}
	version, err := guess(ctx)
	if err != nil {
		return "", err
	}
	version = normalizeKafkaVersion(version)
	if version == "" {
		return "", errors.New("empty Kafka version guess")
	}
	return version, nil
}

func guessKafkaVersion(ctx context.Context, adm *kadm.Client) (string, error) {
	versions, err := adm.ApiVersions(ctx)
	if err != nil {
		return "", fmt.Errorf("fetch API versions: %w", err)
	}
	for _, broker := range versions.Sorted() {
		if broker.Err != nil {
			continue
		}
		if version := broker.VersionGuess(); version != "" {
			return version, nil
		}
	}
	return "", errors.New("no usable broker API versions")
}

// topicDeletionEnabledFrom extracts delete.topic.enable from a broker's
// config entries — a pure helper so the true/false/missing-key/unparseable-
// value cases are unit-testable without a live cluster (state_test.go).
// Defaults to true (KRaft's own default) whenever the key is absent or its
// value doesn't parse as a bool.
func topicDeletionEnabledFrom(cfgs []cluster.ConfigEntry) bool {
	for _, c := range cfgs {
		if c.Name != "delete.topic.enable" {
			continue
		}
		if v, err := strconv.ParseBool(c.Value); err == nil {
			return v
		}
		break
	}
	return true
}

// applyOffsets writes one side (start or end, picked by field) of a
// ListedOffsets result onto every partition of topics it has an entry for,
// leaving tallyPartitions' -1 placeholder untouched for anything not found
// (unknown topic/partition, or a per-partition Err — kadm still reports -1
// itself in that case, so looking it up and copying through is enough; no
// separate Err check needed here).
func applyOffsets(topics []cluster.TopicState, offsets kadm.ListedOffsets, field func(*cluster.PartitionState) *int64) {
	for i := range topics {
		for j := range topics[i].Partitions {
			ps := &topics[i].Partitions[j]
			if lo, ok := offsets.Lookup(topics[i].Name, ps.ID); ok {
				*field(ps) = lo.Offset
			}
		}
	}
}

// tallyPartitions 纯函数：统计 topic 数、分区健康计数，并把每个分区的 leader/replica/ISR
// 归属累加进 perBroker（key=broker ID，value=该 broker 在 st.Brokers 中的元素指针；nil 或未
// 命中的 broker ID 静默跳过——分区可能引用当前已从 metadata 消失的 broker），同时产出每个
// topic 的 TopicState（P1b Task 4：FetchState 随后拿它去补 offsets/segment 用量）。抽出以便
// 无网络单测（门禁保护）；配套单测（同包 state_test.go）用手工构造的 kadm.Metadata 字面量覆盖
// online/offline/under-replicated 三态与 per-broker 归属。
func tallyPartitions(meta kadm.Metadata, perBroker map[int32]*cluster.BrokerInfo) (topics int, pc cluster.PartitionCounts, topicStates []cluster.TopicState) {
	for _, tp := range meta.Topics.Sorted() {
		topics++
		ts := cluster.TopicState{
			Name:              tp.Topic,
			Internal:          tp.IsInternal, // 源码确认字段（kadm metadata.go TopicDetail.IsInternal），不用 "_" 前缀猜测
			ReplicationFactor: tp.Partitions.NumReplicas(),
		}
		for _, pd := range tp.Partitions.Sorted() {
			if pd.Leader < 0 {
				pc.Offline++
			} else {
				pc.Online++
			}
			pc.InSync += len(pd.ISR)
			pc.OutOfSync += len(pd.Replicas) - len(pd.ISR)
			if len(pd.ISR) < len(pd.Replicas) {
				pc.UnderReplicated++
			}
			if b, ok := perBroker[pd.Leader]; ok {
				b.PartitionsLeader++
			}
			for _, rep := range pd.Replicas {
				if b, ok := perBroker[rep]; ok {
					b.Partitions++
				}
			}
			for _, isr := range pd.ISR {
				if b, ok := perBroker[isr]; ok {
					b.InSyncPartitions++
				}
			}
			// Offsets 占位 -1：FetchState 随后用 ListStartOffsets/ListEndOffsets 覆盖，
			// 失败时（非致命）保留这个占位值，而不是 0（0 是一个合法的真实 offset）。
			ts.Partitions = append(ts.Partitions, cluster.PartitionState{
				ID: pd.Partition, Leader: pd.Leader, Replicas: pd.Replicas, ISR: pd.ISR,
				StartOffset: -1, EndOffset: -1,
			})
		}
		topicStates = append(topicStates, ts)
	}
	return topics, pc, topicStates
}

func offlineState(def cluster.Definition, err error) cluster.RuntimeState {
	// TopicDeletionEnabled 同样默认 true：连接失败并不能证明删除被禁用，保持与
	// FetchState 成功路径一致的"读不到默认 true"约定。
	return cluster.RuntimeState{Definition: def, Status: cluster.StatusOffline,
		RefreshedAt: time.Now(), Err: err.Error(), TopicDeletionEnabled: true}
}

// LogDirs 报告每个（broker, 目录）的分区落盘用量：DescribeAllLogDirs 一次拿到全部 broker 的
// 数据，brokers 非空时按 ID 客户端过滤（DescribeBrokerLogDirs 可按单个 broker 查询，但要覆盖
// 多 broker 过滤集合仍需多次调用+合并，不如一次全量查询后过滤直接——已在 capability_audit_test.go
// 审计过两者均编译通过）。
func (p *Pool) LogDirs(ctx context.Context, def cluster.Definition, brokers []int32) ([]cluster.BrokerLogDirs, error) {
	c, err := p.clientFor(def)
	if err != nil {
		return nil, err
	}
	described, err := describeLogDirs(ctx, c.adm, brokers)
	if err != nil {
		return nil, fmt.Errorf("describe log dirs: %w", err)
	}
	out, err := buildBrokerLogDirs(ctx, described, brokers, defaultLogDirBudget)
	if err != nil {
		return nil, err
	}
	return out, nil
}

const maxFilteredLogDirBrokers = 256

type logDirDescriber interface {
	DescribeAllLogDirs(context.Context, kadm.TopicsSet) (kadm.DescribedAllLogDirs, error)
	DescribeBrokerLogDirs(context.Context, int32, kadm.TopicsSet) (kadm.DescribedLogDirs, error)
}

func describeLogDirs(
	ctx context.Context,
	describer logDirDescriber,
	brokers []int32,
) (kadm.DescribedAllLogDirs, error) {
	if ctx == nil || describer == nil {
		return nil, context.Canceled
	}
	if len(brokers) == 0 {
		return describer.DescribeAllLogDirs(ctx, nil)
	}
	unique := make([]int32, 0, min(len(brokers), maxFilteredLogDirBrokers))
	seen := make(map[int32]struct{}, min(len(brokers), maxFilteredLogDirBrokers))
	for _, broker := range brokers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, duplicate := seen[broker]; duplicate {
			continue
		}
		seen[broker] = struct{}{}
		if len(seen) > maxFilteredLogDirBrokers {
			return nil, cluster.ErrResultTooLarge
		}
		unique = append(unique, broker)
	}
	output := make(kadm.DescribedAllLogDirs, len(unique))
	for _, broker := range unique {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		directories, err := describer.DescribeBrokerLogDirs(ctx, broker, nil)
		if err != nil {
			return nil, err
		}
		output[broker] = directories
	}
	return output, nil
}

type logDirBudget struct {
	brokers, directories, topicGroups, partitions, bytes int
}

var defaultLogDirBudget = logDirBudget{
	brokers: 256, directories: 2048, topicGroups: 8192,
	partitions: 65536, bytes: 8 << 20,
}

type logDirUsage struct {
	brokers, directories, topicGroups, partitions, bytes int
}

func buildBrokerLogDirs(
	ctx context.Context,
	described kadm.DescribedAllLogDirs,
	brokers []int32,
	budget logDirBudget,
) ([]cluster.BrokerLogDirs, error) {
	var out []cluster.BrokerLogDirs
	usage := logDirUsage{}
	want := make(map[int32]struct{}, min(len(brokers), budget.brokers))
	for _, broker := range brokers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		want[broker] = struct{}{}
		if len(want) > budget.brokers {
			return nil, cluster.ErrResultTooLarge
		}
	}
	selectedBrokers := 0
	for broker := range described {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(want) == 0 {
			selectedBrokers++
		} else if _, selected := want[broker]; selected {
			selectedBrokers++
		}
		if selectedBrokers > budget.brokers {
			return nil, cluster.ErrResultTooLarge
		}
	}
	brokerIDs := make([]int32, 0, selectedBrokers)
	for broker := range described {
		if len(want) == 0 {
			brokerIDs = append(brokerIDs, broker)
		} else if _, selected := want[broker]; selected {
			brokerIDs = append(brokerIDs, broker)
		}
	}
	sort.Slice(brokerIDs, func(left, right int) bool { return brokerIDs[left] < brokerIDs[right] })
	for _, broker := range brokerIDs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		usage.brokers++
		if usage.brokers > budget.brokers {
			return nil, cluster.ErrResultTooLarge
		}
		directories := described[broker]
		if len(directories) > budget.directories-usage.directories {
			return nil, cluster.ErrResultTooLarge
		}
		directoryNames := make([]string, 0, len(directories))
		for directory := range directories {
			directoryNames = append(directoryNames, directory)
		}
		sort.Strings(directoryNames)
		for _, directory := range directoryNames {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			entry := directories[directory]
			usage.directories++
			if usage.directories > budget.directories ||
				!consumeLogDirBytes(&usage, budget, 64+len(entry.Dir)) {
				return nil, cluster.ErrResultTooLarge
			}
			built := cluster.BrokerLogDirs{Broker: entry.Broker, Dir: entry.Dir}
			if entry.Err != nil {
				built.Error = entry.Err.Error()
				if !consumeLogDirBytes(&usage, budget, len(built.Error)) {
					return nil, cluster.ErrResultTooLarge
				}
			}
			if len(entry.Topics) > budget.topicGroups-usage.topicGroups {
				return nil, cluster.ErrResultTooLarge
			}
			topicNames := make([]string, 0, len(entry.Topics))
			for topic := range entry.Topics {
				topicNames = append(topicNames, topic)
			}
			sort.Strings(topicNames)
			for _, topic := range topicNames {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				usage.topicGroups++
				if usage.topicGroups > budget.topicGroups ||
					!consumeLogDirBytes(&usage, budget, 32+len(topic)) {
					return nil, cluster.ErrResultTooLarge
				}
				partitions := entry.Topics[topic]
				if len(partitions) > budget.partitions-usage.partitions {
					return nil, cluster.ErrResultTooLarge
				}
				partitionIDs := make([]int32, 0, len(partitions))
				for partition := range partitions {
					partitionIDs = append(partitionIDs, partition)
				}
				sort.Slice(partitionIDs, func(left, right int) bool {
					return partitionIDs[left] < partitionIDs[right]
				})
				builtTopic := cluster.TopicLogDirs{Topic: topic}
				for _, partition := range partitionIDs {
					if err := ctx.Err(); err != nil {
						return nil, err
					}
					usage.partitions++
					if usage.partitions > budget.partitions ||
						!consumeLogDirBytes(&usage, budget, 48) {
						return nil, cluster.ErrResultTooLarge
					}
					value := partitions[partition]
					builtTopic.Partitions = append(
						builtTopic.Partitions,
						cluster.PartitionLogDir{
							Partition: value.Partition,
							Size:      value.Size,
							OffsetLag: value.OffsetLag,
						},
					)
				}
				built.Topics = append(built.Topics, builtTopic)
			}
			out = append(out, built)
		}
	}
	return out, nil
}

func consumeLogDirBytes(usage *logDirUsage, budget logDirBudget, size int) bool {
	if size < 0 || usage.bytes > budget.bytes-size {
		return false
	}
	usage.bytes += size
	return true
}

// BrokerConfigs reports one broker's configuration entries (dynamic +
// static + defaults, with synonyms — kadm's IncludeSynonyms is always on).
func (p *Pool) BrokerConfigs(ctx context.Context, def cluster.Definition, broker int32) ([]cluster.ConfigEntry, error) {
	c, err := p.clientFor(def)
	if err != nil {
		return nil, err
	}
	rcs, err := c.adm.DescribeBrokerConfigs(ctx, broker)
	if err != nil {
		return nil, fmt.Errorf("describe broker configs: %w", err)
	}
	return resourceConfigsToEntries(rcs)
}

// resourceConfigsToEntries maps a kadm.ResourceConfigs result (one entry per
// requested resource — a single broker for BrokerConfigs, a single topic for
// TopicConfigs/topics.go) onto domain ConfigEntry, shared by both so the
// Config->ConfigEntry field mapping (including per-entry Synonyms) has one
// implementation.
func resourceConfigsToEntries(rcs kadm.ResourceConfigs) ([]cluster.ConfigEntry, error) {
	var out []cluster.ConfigEntry
	for _, rc := range rcs { // 每资源一项，broker/topic 单个查询恰一项
		if rc.Err != nil {
			return nil, rc.Err
		}
		for _, cfg := range rc.Configs {
			e := cluster.ConfigEntry{Name: cfg.Key, Source: cfg.Source.String(),
				IsSensitive: cfg.Sensitive}
			if cfg.Value != nil {
				e.Value = *cfg.Value
			}
			for _, syn := range cfg.Synonyms {
				s := cluster.ConfigSynonym{Name: syn.Key, Source: syn.Source.String()}
				if syn.Value != nil {
					s.Value = *syn.Value
				}
				e.Synonyms = append(e.Synonyms, s)
			}
			out = append(out, e)
		}
	}
	return out, nil
}

// AlterBrokerConfig incrementally sets a single broker config key via
// IncrementalAlterConfigs (kadm.AlterBrokerConfigs). It must never call
// AlterBrokerConfigsState: that alters the *entire* config state and drops
// every other dynamic key on the broker (ADR-0003 §6.1;
// state_integration_test.go's TestAlterBrokerConfigIsIncremental pins this
// down as a regression test).
func (p *Pool) AlterBrokerConfig(ctx context.Context, def cluster.Definition, broker int32, name, value string) error {
	c, err := p.clientFor(def)
	if err != nil {
		return err
	}
	// 增量 SET 单键：底层 IncrementalAlterConfigs。绝不使用 AlterBrokerConfigsState。
	resps, err := c.adm.AlterBrokerConfigs(ctx,
		[]kadm.AlterConfig{{Op: kadm.SetConfig, Name: name, Value: &value}}, broker)
	if err != nil {
		return fmt.Errorf("alter broker config: %w", err)
	}
	for _, r := range resps {
		if r.Err != nil {
			return r.Err
		}
	}
	return nil
}

// MoveReplicaLogDir migrates one topic-partition replica on broker to dir
// via AlterBrokerReplicaLogDirs. The request shape is a dir->TopicsSet map
// (kadm.AlterReplicaLogDirsReq): we build a single-topic-partition set and
// attach it to the one target directory.
func (p *Pool) MoveReplicaLogDir(ctx context.Context, def cluster.Definition, broker int32, topic string, partition int32, dir string) error {
	c, err := p.clientFor(def)
	if err != nil {
		return err
	}
	var ts kadm.TopicsSet
	ts.Add(topic, partition)
	var req kadm.AlterReplicaLogDirsReq
	req.Add(dir, ts)
	resps, err := c.adm.AlterBrokerReplicaLogDirs(ctx, broker, req)
	if err != nil {
		return fmt.Errorf("move replica log dir: %w", err)
	}
	// 与 AlterBrokerConfig 一致的 first-error 聚合：发现第一个 Err 即返回，不用
	// 最后一个覆盖前面的（resps 底层是嵌套 map，Each 的遍历顺序不确定，因此用
	// Sorted() 取得确定顺序——first-error 才是可复现的"第一个"）。
	for _, r := range resps.Sorted() {
		if r.Err != nil {
			return r.Err
		}
	}
	return nil
}
