//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

func TestFetchStateAgainstRealKafka(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.8.0")
	testcontainers.CleanupContainer(t, kc)
	require.NoError(t, err)
	brokers, err := kc.Brokers(ctx)
	require.NoError(t, err)

	pool := NewPool()
	defer pool.Close()
	def := cluster.Definition{Name: "it", Conn: cluster.ConnectionSpec{BootstrapServers: brokers}}
	st, err := pool.FetchState(ctx, def)
	require.NoError(t, err)
	require.Equal(t, cluster.StatusOnline, st.Status)
	require.Len(t, st.Brokers, 1)
	require.NotEmpty(t, st.Brokers[0].Host)
	require.GreaterOrEqual(t, st.Controller, int32(0))

	// Real topic → per-broker partition tally must go non-zero (T5): 2
	// partitions, replication factor 1, on a single-broker cluster means that
	// one broker leads (and replicates) both.
	p2, err := pool.clientFor(def)
	require.NoError(t, err)
	_, err = p2.adm.CreateTopics(ctx, 2, 1, nil, "p1a-it-topic") // 2 分区 1 副本
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		st, err := pool.FetchState(ctx, def)
		return err == nil && st.TopicCount >= 1 && st.Brokers[0].PartitionsLeader >= 2
	}, 30*time.Second, time.Second)

	// T4 Step 2: st.Topics carries the same topic, with 2 partitions and a
	// real (non-negative) end offset once ListEndOffsets has succeeded —
	// locks down the offsets-fetch addition to FetchState/tallyPartitions.
	st, err = pool.FetchState(ctx, def)
	require.NoError(t, err)
	var it *cluster.TopicState
	for i := range st.Topics {
		if st.Topics[i].Name == "p1a-it-topic" {
			it = &st.Topics[i]
			break
		}
	}
	require.NotNil(t, it, "expected p1a-it-topic in st.Topics")
	require.Len(t, it.Partitions, 2)
	for _, p := range it.Partitions {
		require.GreaterOrEqual(t, p.EndOffset, int64(0))
	}

	dirs, err := pool.LogDirs(ctx, def, nil)
	require.NoError(t, err)
	require.NotEmpty(t, dirs)

	// Filter branch: real broker ID matches non-empty; non-existent ID filters to empty
	filtered, err := pool.LogDirs(ctx, def, []int32{st.Brokers[0].ID})
	require.NoError(t, err)
	require.NotEmpty(t, filtered)
	none, err := pool.LogDirs(ctx, def, []int32{99999})
	require.NoError(t, err)
	require.Empty(t, none)
}

// TestAlterBrokerConfigIsIncremental is this task's soul test: it pins down
// ADR-0003 §6.1 as a real-container regression guard. AlterBrokerConfig must
// use kadm's incremental IncrementalAlterConfigs (kadm.AlterBrokerConfigs),
// never the full-state-replace kadm.AlterBrokerConfigsState — the latter
// would silently wipe out every previously-set dynamic key each time a new
// one is set, which is exactly what this test would catch: setting
// log.cleaner.threads then background.threads, and asserting *both* survive.
func TestAlterBrokerConfigIsIncremental(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.8.0")
	testcontainers.CleanupContainer(t, kc)
	require.NoError(t, err)
	brokers, err := kc.Brokers(ctx)
	require.NoError(t, err)

	pool := NewPool()
	defer pool.Close()
	def := cluster.Definition{Name: "it-broker-config", Conn: cluster.ConnectionSpec{BootstrapServers: brokers}}
	st, err := pool.FetchState(ctx, def)
	require.NoError(t, err)
	require.Len(t, st.Brokers, 1)
	brokerID := st.Brokers[0].ID

	// ① 先设第一个动态配置
	require.NoError(t, pool.AlterBrokerConfig(ctx, def, brokerID, "log.cleaner.threads", "2"))
	// ② 再设第二个动态配置
	require.NoError(t, pool.AlterBrokerConfig(ctx, def, brokerID, "background.threads", "12"))
	// ③ 断言两者共存——若误用全量替换 API，①会在②时丢失。
	// 用 require.Eventually 轮询（同款手法见上面 TestFetchStateAgainstRealKafka 的分区计数
	// 断言）：AlterBrokerConfigs 的响应在写入被接受后即返回，但 broker 侧动态配置真正生效
	// （KRaft metadata log apply）有短暂异步延迟——实测直接同步断言曾观察到 background.threads
	// 读回默认值"10"而非刚写入的"12"（log.cleaner.threads 因为写入更早、传播时间更充分而读到了
	// 正确值"2"）。这是传播延迟，不是 AlterBrokerConfigs/AlterBrokerConfigsState 选错：选错的
	// 特征是先写入的键被后写入的操作清空，而不是刚写入的键读到默认值。
	var byName map[string]cluster.ConfigEntry
	require.Eventually(t, func() bool {
		cfgs, err := pool.BrokerConfigs(ctx, def, brokerID)
		if err != nil {
			return false
		}
		byName = map[string]cluster.ConfigEntry{}
		for _, c := range cfgs {
			byName[c.Name] = c
		}
		return byName["log.cleaner.threads"].Value == "2" && byName["background.threads"].Value == "12"
	}, 30*time.Second, time.Second)
	require.Equal(t, "2", byName["log.cleaner.threads"].Value)
	require.Equal(t, "12", byName["background.threads"].Value)
	require.Contains(t, byName["log.cleaner.threads"].Source, "DYNAMIC")
}

// dirA/dirB are the two log directories TestMoveReplicaLogDirAgainstRealKafka
// configures via KAFKA_LOG_DIRS. Confirmed by directly inspecting the
// confluentinc/confluent-local:7.8.0 image before writing this test (not
// assumed): /etc/confluent/docker/configure only defaults KAFKA_LOG_DIRS to
// "/var/lib/kafka/data" when the env var is *unset*
// (`if [[ -z "${KAFKA_LOG_DIRS-}" ]]; then ... fi`), and
// /etc/confluent/docker/kafka-propertiesSpec.json's generic
// `"prefixes": {"KAFKA": false}` rule strips the "KAFKA_" prefix and
// lowercases/dot-joins the rest for any KAFKA_* env var not explicitly
// excluded (KAFKA_LOG_DIRS isn't in that spec's "excludes" list) — so
// KAFKA_LOG_DIRS maps straight onto kafka.properties' log.dirs, and
// testcontainers' own starter script's `kafka-storage format` formats every
// directory named there. This is exactly the fixture the P1a Task 6 review
// judged feasible (progress.md) and this test now proves it end-to-end.
const (
	dirA = "/tmp/kafka-logs-a"
	dirB = "/tmp/kafka-logs-b"
)

// TestMoveReplicaLogDirAgainstRealKafka clears the last of P1a's four
// mandatory items (the other three — readOnly write guard, refresh
// freshness guard, deep-route white-screen fix — were cleared in P1b Tasks
// 2/3): MoveReplicaLogDir's success path needs a real multi-log-dir broker,
// which the single-log-dir container every other integration test in this
// package uses can't exercise (there is nowhere else to move a replica
// *to*). Deliberately not downgraded to a single-dir "no-op" test — see
// this task's report for the full fixture investigation.
func TestMoveReplicaLogDirAgainstRealKafka(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.8.0",
		testcontainers.WithEnv(map[string]string{
			"KAFKA_LOG_DIRS": dirA + "," + dirB,
		}))
	testcontainers.CleanupContainer(t, kc)
	require.NoError(t, err)
	brokers, err := kc.Brokers(ctx)
	require.NoError(t, err)

	pool := NewPool()
	defer pool.Close()
	def := cluster.Definition{Name: "it-multilogdir", Conn: cluster.ConnectionSpec{BootstrapServers: brokers}}
	st, err := pool.FetchState(ctx, def)
	require.NoError(t, err)
	require.Len(t, st.Brokers, 1)
	brokerID := st.Brokers[0].ID

	const topic = "mv-topic"
	require.NoError(t, pool.CreateTopic(ctx, def, cluster.TopicSpec{Name: topic, Partitions: 1, ReplicationFactor: 1}))

	// Log dir assignment for a freshly-created partition happens
	// asynchronously (LogManager picks a dir once it processes the
	// partition's LeaderAndIsr, not synchronously with CreateTopics
	// returning) — same "Eventually" convergence convention as
	// TestFetchStateAgainstRealKafka's partition-tally assertion above.
	var from string
	require.Eventually(t, func() bool {
		dirs, err := pool.LogDirs(ctx, def, nil)
		if err != nil {
			return false
		}
		from = dirOf(dirs, topic)
		return from == dirA || from == dirB
	}, 30*time.Second, time.Second, "expected mv-topic's replica to land in one of the two configured log dirs")

	to := otherDir(from)
	require.NoError(t, pool.MoveReplicaLogDir(ctx, def, brokerID, topic, 0, to))
	require.Eventually(t, func() bool {
		dirs, err := pool.LogDirs(ctx, def, nil)
		return err == nil && dirOf(dirs, topic) == to
	}, 60*time.Second, 2*time.Second, "expected mv-topic's replica to have moved to %q", to)
}

// dirOf finds which of dirs' (broker, directory) entries currently holds
// topic's (single-partition, single-replica) replica, or "" if none does
// yet.
func dirOf(dirs []cluster.BrokerLogDirs, topic string) string {
	for _, d := range dirs {
		for _, tl := range d.Topics {
			if tl.Topic == topic {
				return d.Dir
			}
		}
	}
	return ""
}

// otherDir flips between the two fixture log dirs (dirA/dirB above).
func otherDir(from string) string {
	if from == dirA {
		return dirB
	}
	return dirA
}
