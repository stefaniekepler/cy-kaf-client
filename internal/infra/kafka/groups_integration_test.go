//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// TestGroupsRealRoundTripAgainstRealKafka locks down P1b Task 6's Groups
// write surface with a *real* consumer group: a kgo producer seeds records
// and a kgo consumer group actually joins, consumes and commits, so
// DescribeGroup's Committed/End offsets are genuine broker state rather than
// something this repo's own ResetOffsets manufactured. Both fixture topics
// are single-partition so every offset assertion below is an exact scalar
// rather than a sum across a round-robin partition split.
//
// The group deliberately commits against *two* topics (topic/topic2), not
// one. First cut of this test used only one topic, and its final DeleteGroup
// call failed with a real GROUP_ID_NOT_FOUND: once DeleteGroupOffsets strips
// the last topic an already-Empty group has committed against, the broker
// doesn't keep the now-fully-empty group around for a subsequent explicit
// DeleteGroup to find (an Empty group with zero retained offsets and zero
// members has nothing left worth persisting). Giving the group a second
// topic it never resets/deletes keeps it genuinely alive through the
// DeleteGroupOffsets(topic) step, which as a bonus is a strictly better test
// of that method's own documented contract ("leaving the group and its other
// topics' offsets untouched", groups.go) — a single-topic group could never
// have exercised the "other topics" half of that sentence at all.
//
// This test also settles a real question Task 6 (unit-tests-only) left open
// in its own doc comment (app/cluster/group.go's GroupService.Reset):
// whether resetting a group's offsets while it still has an active member is
// accepted or rejected by a real broker. Empirically (verified against this
// exact container/kadm/kgo combination before writing the final assertion
// below, not assumed): kadm's CommitOffsets-based admin reset — which
// carries no member ID / generation (it isn't the group's own member
// committing, it's an out-of-band admin commit) — is rejected with
// UNKNOWN_MEMBER_ID as long as the group has a real joined member, and only
// succeeds once that member leaves and the group goes Empty.
//
// UPDATE (Task 8e, P1b final-review verdict②): this test calls
// Pool.ResetOffsets directly — the raw GroupAdminPort implementation —
// deliberately bypassing GroupService's own pre-check, since this test's job
// is to pin down the *broker's* raw enforcement, not the app layer's. That
// raw UNKNOWN_MEMBER_ID rejection asserted below is exactly the broker-level
// fact that motivated Task 8e: GroupService.Reset used to have "no
// pre-check, just pass the broker's rejection through as an ordinary 500" —
// which meant a real caller hitting this exact path (the single most common
// failure mode: forgetting to stop a consumer before resetting) got an
// uninformative 500 (reads as an app bug) instead of a precise, actionable
// 400 ("stop your consumers first"). GroupService.Reset now describes the
// group first (the same DescribeGroup call Get uses) and rejects any
// non-EMPTY/DEAD state with ErrGroupNotInactive *before* ever calling
// ResetOffsets, so in normal application use this broker rejection is
// preempted and never actually reached. This test's direct pool-level call
// remains valuable precisely because it still proves the broker really
// would reject the reset if asked — it's now the empirical justification
// *for* the app-layer guard, not a justification for omitting one.
func TestGroupsRealRoundTripAgainstRealKafka(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.8.0")
	testcontainers.CleanupContainer(t, kc)
	require.NoError(t, err)
	brokers, err := kc.Brokers(ctx)
	require.NoError(t, err)

	pool := NewPool()
	defer pool.Close()
	def := cluster.Definition{Name: "it-groups-write", Conn: cluster.ConnectionSpec{BootstrapServers: brokers}}

	const (
		topic   = "grp-topic"   // the topic every Lag/Reset assertion below targets
		topic2  = "grp-topic-2" // committed-once, never reset/deleted -- the "other topics untouched" control
		groupID = "grp-real"
	)
	require.NoError(t, pool.CreateTopic(ctx, def, cluster.TopicSpec{Name: topic, Partitions: 1, ReplicationFactor: 1}))
	require.NoError(t, pool.CreateTopic(ctx, def, cluster.TopicSpec{Name: topic2, Partitions: 1, ReplicationFactor: 1}))

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer producer.Close()

	produce := func(produceTopic string, n int) {
		recs := make([]*kgo.Record, n)
		for i := range recs {
			recs[i] = &kgo.Record{Topic: produceTopic, Value: []byte("v")}
		}
		results := producer.ProduceSync(ctx, recs...)
		require.NoError(t, results.FirstErr())
	}

	offsetFor := func(gs cluster.GroupState, forTopic string, partition int32) (cluster.GroupOffset, bool) {
		for _, o := range gs.Offsets {
			if o.Topic == forTopic && o.Partition == partition {
				return o, true
			}
		}
		return cluster.GroupOffset{}, false
	}

	// --- seed topic2's control offset + topic's first 10, then consume both + commit ---
	produce(topic2, 2)
	produce(topic, 10)

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic, topic2),
		kgo.DisableAutoCommit(),
	)
	require.NoError(t, err)

	consumeAtLeast := func(n int) {
		got := 0
		deadline := time.Now().Add(60 * time.Second)
		for got < n && time.Now().Before(deadline) {
			fetches := consumer.PollFetches(ctx)
			require.Empty(t, fetches.Errors())
			got += fetches.NumRecords()
		}
		require.GreaterOrEqual(t, got, n, "expected to consume at least %d records before the deadline", n)
	}
	consumeAtLeast(12) // 10 on topic + 2 on topic2
	require.NoError(t, consumer.CommitUncommittedOffsets(ctx))

	var gs cluster.GroupState
	require.Eventually(t, func() bool {
		gs, err = pool.DescribeGroup(ctx, def, groupID)
		if err != nil {
			return false
		}
		o, ok := offsetFor(gs, topic, 0)
		o2, ok2 := offsetFor(gs, topic2, 0)
		return ok && o.Committed == 10 && o.End == 10 && ok2 && o2.Committed == 2 && o2.End == 2
	}, 30*time.Second, time.Second, "expected Committed=10,End=10 on topic and Committed=2,End=2 on topic2 after consuming+committing both")
	require.Equal(t, int64(0), gs.Lag())

	// --- produce 5 more on topic (pure admin-side describe, no consumer action) -> Lag=5 ---
	// midTS is captured strictly between the two produce batches (with a
	// margin either side, since ListOffsetsAfterMilli's precision is
	// millisecond-grained CreateTime) so the later TIMESTAMP reset below
	// lands exactly at the batch boundary (offset 10), not "somewhere" mid
	// stream.
	time.Sleep(1500 * time.Millisecond)
	midTS := time.Now()
	time.Sleep(1500 * time.Millisecond)
	produce(topic, 5)

	require.Eventually(t, func() bool {
		gs, err = pool.DescribeGroup(ctx, def, groupID)
		if err != nil {
			return false
		}
		o, ok := offsetFor(gs, topic, 0)
		return ok && o.End == 15
	}, 30*time.Second, time.Second, "expected End to reach 15 once the 5 new records are visible")
	require.Equal(t, int64(5), gs.Lag())

	// --- active-group reset: the consumer is still a live, joined member
	// (its background heartbeats keep it in the group even though we've
	// stopped polling it) -- this calls Pool.ResetOffsets directly (the raw
	// port), deliberately bypassing GroupService.Reset's Task 8e pre-check,
	// to assert the real broker rejection that pre-check now exists to keep
	// callers from ever hitting blind (see this test's doc comment above).
	activeResetErr := pool.ResetOffsets(ctx, def, groupID, cluster.ResetSpec{Topic: topic, ResetType: "EARLIEST"})
	require.ErrorIs(t, activeResetErr, kerr.UnknownMemberID, "expected a real UNKNOWN_MEMBER_ID rejection when resetting a group with an active member")
	gs, err = pool.DescribeGroup(ctx, def, groupID)
	require.NoError(t, err)
	o, ok := offsetFor(gs, topic, 0)
	require.True(t, ok)
	require.Equal(t, int64(10), o.Committed, "a rejected reset must leave the committed offset unchanged")

	// --- consumer closes -> group transitions to Empty ---
	consumer.Close()
	require.Eventually(t, func() bool {
		gs, err := pool.DescribeGroup(ctx, def, groupID)
		return err == nil && gs.State == "EMPTY"
	}, 30*time.Second, time.Second, "expected group to reach EMPTY after the consumer closes")

	// --- ResetOffsets EARLIEST (now inactive) -> Committed back to 0, Lag=15 ---
	require.NoError(t, pool.ResetOffsets(ctx, def, groupID, cluster.ResetSpec{Topic: topic, ResetType: "EARLIEST"}))
	require.Eventually(t, func() bool {
		gs, err = pool.DescribeGroup(ctx, def, groupID)
		if err != nil {
			return false
		}
		o, ok := offsetFor(gs, topic, 0)
		return ok && o.Committed == 0
	}, 30*time.Second, time.Second, "expected Committed=0 after ResetOffsets EARLIEST")
	require.Equal(t, int64(15), gs.Lag())

	// --- ResetOffsets TIMESTAMP(mid-stream) -> Committed lands at the batch boundary (10) ---
	require.NoError(t, pool.ResetOffsets(ctx, def, groupID, cluster.ResetSpec{
		Topic: topic, ResetType: "TIMESTAMP", Timestamp: midTS.UnixMilli(),
	}))
	require.Eventually(t, func() bool {
		gs, err = pool.DescribeGroup(ctx, def, groupID)
		if err != nil {
			return false
		}
		o, ok := offsetFor(gs, topic, 0)
		return ok && o.Committed == 10
	}, 30*time.Second, time.Second, "expected Committed=10 (the batch boundary) after ResetOffsets TIMESTAMP")

	// --- DeleteGroupOffsets(topic) -> topic's offset gone, topic2's untouched ---
	require.NoError(t, pool.DeleteGroupOffsets(ctx, def, groupID, topic))
	require.Eventually(t, func() bool {
		gs, err := pool.DescribeGroup(ctx, def, groupID)
		if err != nil {
			return false
		}
		_, ok := offsetFor(gs, topic, 0)
		return !ok
	}, 30*time.Second, time.Second, "expected no committed offset for topic after DeleteGroupOffsets")
	gs, err = pool.DescribeGroup(ctx, def, groupID)
	require.NoError(t, err)
	o2, ok2 := offsetFor(gs, topic2, 0)
	require.True(t, ok2, "expected topic2's committed offset to survive DeleteGroupOffsets(topic) untouched")
	require.Equal(t, int64(2), o2.Committed)
	require.Equal(t, int64(2), o2.End)

	// --- DeleteGroup -> disappears from ListGroups. The group still retains
	// topic2's committed offset at this point, so this is a real deletion of
	// a genuinely still-existing group, not a no-op against something the
	// broker already reaped (see this test's doc comment above).
	require.NoError(t, pool.DeleteGroup(ctx, def, groupID))
	require.Eventually(t, func() bool {
		groups, err := pool.ListGroups(ctx, def)
		if err != nil {
			return false
		}
		for _, g := range groups {
			if g.ID == groupID {
				return false
			}
		}
		return true
	}, 30*time.Second, time.Second, "expected group to disappear from ListGroups after DeleteGroup")
}
