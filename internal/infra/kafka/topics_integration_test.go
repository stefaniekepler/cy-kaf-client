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

// TestTopicsWriteRoundTripAgainstRealKafka locks down P1b Task 5's full
// Topics write surface against a real broker, one container reused across
// every step (brief: "一个容器串联" — amortize container startup cost). Its
// centerpiece is this task's Topics-version of T6's broker soul test
// (TestAlterBrokerConfigIsIncremental in state_integration_test.go): setting
// two topic config keys in succession and asserting BOTH coexist with
// DYNAMIC_TOPIC_CONFIG source pins down that AlterTopicConfig really does use
// kadm's incremental AlterTopicConfigs, never the full-state-replace
// AlterTopicConfigsState (ADR-0003 §6.1's trap, same family as
// AlterBrokerConfig's).
func TestTopicsWriteRoundTripAgainstRealKafka(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.8.0")
	testcontainers.CleanupContainer(t, kc)
	require.NoError(t, err)
	brokers, err := kc.Brokers(ctx)
	require.NoError(t, err)

	pool := NewPool()
	defer pool.Close()
	def := cluster.Definition{Name: "it-topics-write", Conn: cluster.ConnectionSpec{BootstrapServers: brokers}}

	st, err := pool.FetchState(ctx, def)
	require.NoError(t, err)
	require.Len(t, st.Brokers, 1)
	brokerID := st.Brokers[0].ID

	const topic = "rw-topic"

	// Create: 2 partitions, 1 replica, one initial dynamic config.
	require.NoError(t, pool.CreateTopic(ctx, def, cluster.TopicSpec{
		Name: topic, Partitions: 2, ReplicationFactor: 1,
		Configs: map[string]string{"retention.ms": "3600000"},
	}))

	cfgs, err := pool.TopicConfigs(ctx, def, topic)
	require.NoError(t, err)
	require.Equal(t, "3600000", configsByName(cfgs)["retention.ms"].Value)

	// Soul test #2 (topic version of TestAlterBrokerConfigIsIncremental):
	// set two keys in succession, assert BOTH survive. If AlterTopicConfig
	// ever regressed to AlterTopicConfigsState, setting cleanup.policy here
	// would silently wipe out max.message.bytes.
	require.NoError(t, pool.AlterTopicConfig(ctx, def, topic, "max.message.bytes", "2000000"))
	require.NoError(t, pool.AlterTopicConfig(ctx, def, topic, "cleanup.policy", "compact"))
	var byName map[string]cluster.ConfigEntry
	require.Eventually(t, func() bool {
		cfgs, err := pool.TopicConfigs(ctx, def, topic)
		if err != nil {
			return false
		}
		byName = configsByName(cfgs)
		return byName["max.message.bytes"].Value == "2000000" && byName["cleanup.policy"].Value == "compact"
	}, 30*time.Second, time.Second, "expected both max.message.bytes and cleanup.policy to coexist")
	require.Equal(t, "DYNAMIC_TOPIC_CONFIG", byName["max.message.bytes"].Source)
	require.Equal(t, "DYNAMIC_TOPIC_CONFIG", byName["cleanup.policy"].Source)

	// CreatePartitions: 2 -> 4 (a *total*, not an incremental add — see
	// topics.go's doc comment on the kadm CreatePartitions/UpdatePartitions
	// naming trap Pool.CreatePartitions avoids).
	require.NoError(t, pool.CreatePartitions(ctx, def, topic, 4))
	require.Eventually(t, func() bool {
		st, err := pool.FetchState(ctx, def)
		return err == nil && partitionCountOf(st, topic) == 4
	}, 30*time.Second, time.Second, "expected topic to have 4 partitions after CreatePartitions")

	// Replication factor: TopicService.ChangeReplicationFactor's own
	// decision algorithm (reassignForFactor: shrink/grow/rotation) is
	// unit-tested against no live cluster at all in Task 5
	// (topic_internal_test.go) — that's where its correctness is proven, not
	// here. What a single-broker integration container CAN uniquely prove is
	// that Pool.AlterPartitionAssignments' error surfacing (the first-error
	// `resps.Sorted()` loop in topics.go) reacts to a REAL broker response
	// rather than only a mocked one: an identity reassignment (target ==
	// current replication factor) is a genuine no-op success, while naming a
	// replica ID that isn't a registered broker on this cluster is a genuine
	// rejection. (reassignForFactor's own target>len(brokers) guard is what
	// stops production code ever reaching the broker with an unsatisfiable
	// target in the first place — this just proves the underlying kadm call
	// itself doesn't silently accept nonsense.)
	require.NoError(t, pool.AlterPartitionAssignments(ctx, def, topic, map[int32][]int32{0: {brokerID}}))
	err = pool.AlterPartitionAssignments(ctx, def, topic, map[int32][]int32{0: {brokerID, brokerID + 1000}})
	require.Error(t, err, "expected a real broker rejection when naming a nonexistent replica broker ID")
	t.Logf("AlterPartitionAssignments naming a nonexistent broker ID returned: %v", err)

	// configOps diff path: TopicService.UpdateConfigs' "the caller's desired
	// config map no longer includes a previously-set key" case unsets it via
	// AlterTopicConfig(topic, key, "") — deleting cleanup.policy must leave
	// max.message.bytes (set moments ago, above) completely untouched.
	require.NoError(t, pool.AlterTopicConfig(ctx, def, topic, "cleanup.policy", ""))
	require.Eventually(t, func() bool {
		cfgs, err := pool.TopicConfigs(ctx, def, topic)
		if err != nil {
			return false
		}
		byName = configsByName(cfgs)
		c, ok := byName["cleanup.policy"]
		return ok && c.Source != "DYNAMIC_TOPIC_CONFIG"
	}, 30*time.Second, time.Second, "expected cleanup.policy to revert off DYNAMIC_TOPIC_CONFIG once unset")
	t.Logf("cleanup.policy reverted to source=%q value=%q", byName["cleanup.policy"].Source, byName["cleanup.policy"].Value)
	require.Equal(t, "2000000", byName["max.message.bytes"].Value, "unsetting cleanup.policy must not disturb max.message.bytes")
	require.Equal(t, "DYNAMIC_TOPIC_CONFIG", byName["max.message.bytes"].Source)

	// DeleteTopic -> Eventually disappears from FetchState.
	require.NoError(t, pool.DeleteTopic(ctx, def, topic))
	require.Eventually(t, func() bool {
		st, err := pool.FetchState(ctx, def)
		return err == nil && partitionCountOf(st, topic) == -1
	}, 30*time.Second, time.Second, "expected rw-topic to disappear after DeleteTopic")
}

// configsByName indexes cfgs by Name for convenient test lookups.
func configsByName(cfgs []cluster.ConfigEntry) map[string]cluster.ConfigEntry {
	out := make(map[string]cluster.ConfigEntry, len(cfgs))
	for _, c := range cfgs {
		out[c.Name] = c
	}
	return out
}

// partitionCountOf returns topic's partition count in st.Topics, or -1 if
// topic isn't present at all (used both for the CreatePartitions convergence
// check and DeleteTopic's disappearance check).
func partitionCountOf(st cluster.RuntimeState, topic string) int {
	for _, ts := range st.Topics {
		if ts.Name == topic {
			return len(ts.Partitions)
		}
	}
	return -1
}
