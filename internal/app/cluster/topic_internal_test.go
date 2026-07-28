package cluster

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// This file is package cluster (white-box), not cluster_test: filterSortPage
// and sortTopics are unexported pure functions, so their table-driven
// coverage lives here directly rather than routing every case through
// TopicService.List (which would need a StateCache/Resolver per case just
// to reach the same logic) — TopicService.List's own behaviour (unknown
// cluster -> ErrUnknownCluster, known cluster -> delegates here) is covered
// separately in topic_test.go (cluster_test package).

// partitionsOfCount builds n placeholder partitions — filterSortPage's
// TOTAL_PARTITIONS ordering only cares about len(Partitions), not their
// contents.
func partitionsOfCount(n int) []cluster.PartitionState {
	ps := make([]cluster.PartitionState, n)
	for i := range ps {
		ps[i] = cluster.PartitionState{ID: int32(i), StartOffset: -1, EndOffset: -1}
	}
	return ps
}

func topicWithCount(name string, count int64) cluster.TopicState {
	return cluster.TopicState{
		Name: name,
		Partitions: []cluster.PartitionState{
			{ID: 0, StartOffset: 0, EndOffset: count},
		},
	}
}

func topicWithUnknownCount(name string) cluster.TopicState {
	return cluster.TopicState{
		Name: name,
		Partitions: []cluster.PartitionState{
			{ID: 0, StartOffset: -1, EndOffset: -1},
		},
	}
}

func TestFilterSortPage(t *testing.T) {
	cases := []struct {
		name      string
		in        []cluster.TopicState
		query     TopicListQuery
		wantNames []string
		wantCount int
	}{
		{
			name:      "empty input yields an empty page and zero pageCount",
			in:        nil,
			query:     TopicListQuery{},
			wantNames: []string{},
			wantCount: 0,
		},
		{
			name: "search is case-insensitive contains",
			in: []cluster.TopicState{
				{Name: "OrdersArchive"}, {Name: "payments"}, {Name: "orders-eu"},
			},
			query:     TopicListQuery{Search: "ORDER"},
			wantNames: []string{"OrdersArchive", "orders-eu"}, // NAME asc, byte-wise: 'O' < 'o'
			wantCount: 1,
		},
		{
			name: "showInternal false (default) excludes internal topics",
			in: []cluster.TopicState{
				{Name: "a", Internal: false}, {Name: "_internal", Internal: true}, {Name: "b", Internal: false},
			},
			query:     TopicListQuery{},
			wantNames: []string{"a", "b"},
			wantCount: 1,
		},
		{
			name: "showInternal true includes internal topics",
			in: []cluster.TopicState{
				{Name: "a", Internal: false}, {Name: "_internal", Internal: true},
			},
			query:     TopicListQuery{ShowInternal: true},
			wantNames: []string{"_internal", "a"}, // '_' (0x5F) < 'a' (0x61)
			wantCount: 1,
		},
		{
			name: "orderBy NAME ascending",
			in: []cluster.TopicState{
				{Name: "charlie"}, {Name: "alpha"}, {Name: "bravo"},
			},
			query:     TopicListQuery{OrderBy: "NAME"},
			wantNames: []string{"alpha", "bravo", "charlie"},
			wantCount: 1,
		},
		{
			name: "orderBy TOTAL_PARTITIONS ascending",
			in: []cluster.TopicState{
				{Name: "three", Partitions: partitionsOfCount(3)},
				{Name: "one", Partitions: partitionsOfCount(1)},
				{Name: "two", Partitions: partitionsOfCount(2)},
			},
			query:     TopicListQuery{OrderBy: "TOTAL_PARTITIONS"},
			wantNames: []string{"one", "two", "three"},
			wantCount: 1,
		},
		{
			name: "orderBy REPLICATION_FACTOR ascending",
			in: []cluster.TopicState{
				{Name: "rf3", ReplicationFactor: 3},
				{Name: "rf1", ReplicationFactor: 1},
				{Name: "rf2", ReplicationFactor: 2},
			},
			query:     TopicListQuery{OrderBy: "REPLICATION_FACTOR"},
			wantNames: []string{"rf1", "rf2", "rf3"},
			wantCount: 1,
		},
		{
			name: "orderBy SIZE ascending",
			in: []cluster.TopicState{
				{Name: "big", SegmentSize: 900},
				{Name: "small", SegmentSize: 100},
				{Name: "medium", SegmentSize: 500},
			},
			query:     TopicListQuery{OrderBy: "SIZE"},
			wantNames: []string{"small", "medium", "big"},
			wantCount: 1,
		},
		{
			name: "sortOrder DESC reverses the ordering",
			in: []cluster.TopicState{
				{Name: "alpha"}, {Name: "bravo"}, {Name: "charlie"},
			},
			query:     TopicListQuery{OrderBy: "NAME", SortOrder: "DESC"},
			wantNames: []string{"charlie", "bravo", "alpha"},
			wantCount: 1,
		},
		{
			name: "messages descending sorts globally before pagination",
			in: []cluster.TopicState{
				topicWithCount("ten-b", 10), topicWithUnknownCount("unknown"),
				topicWithCount("thirty", 30), topicWithCount("ten-a", 10),
			},
			query:     TopicListQuery{OrderBy: "MESSAGES_COUNT", SortOrder: "DESC", Page: 1, PerPage: 2},
			wantNames: []string{"thirty", "ten-a"},
			wantCount: 2,
		},
		{
			name: "messages ascending keeps unknown last",
			in: []cluster.TopicState{
				topicWithUnknownCount("a-unknown"), topicWithCount("ten", 10), topicWithCount("zero", 0),
			},
			query:     TopicListQuery{OrderBy: "MESSAGES_COUNT", SortOrder: "ASC"},
			wantNames: []string{"zero", "ten", "a-unknown"},
			wantCount: 1,
		},
		{
			name: "pagination slice boundary: page 2 of perPage 2 over 5 items",
			in: []cluster.TopicState{
				{Name: "t1"}, {Name: "t2"}, {Name: "t3"}, {Name: "t4"}, {Name: "t5"},
			},
			query:     TopicListQuery{Page: 2, PerPage: 2},
			wantNames: []string{"t3", "t4"},
			wantCount: 3, // ceil(5/2)
		},
		{
			name: "out-of-range page clamps to the last real page",
			in: []cluster.TopicState{
				{Name: "t1"}, {Name: "t2"}, {Name: "t3"},
			},
			query:     TopicListQuery{Page: 99, PerPage: 2},
			wantNames: []string{"t3"}, // last page (2 of 2) holds just t3
			wantCount: 2,              // ceil(3/2)
		},
		{
			name: "zero Page/PerPage default to 1/25",
			in: []cluster.TopicState{
				{Name: "only"},
			},
			query:     TopicListQuery{},
			wantNames: []string{"only"},
			wantCount: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := filterSortPage(tc.in, tc.query)
			names := make([]string, len(got.Topics))
			for i, ts := range got.Topics {
				names[i] = ts.Name
			}
			require.Equal(t, tc.wantNames, names)
			require.Equal(t, tc.wantCount, got.PageCount)
		})
	}
}

// --- reassignForFactor (P1b Task 5's pure algorithmic core) ---

func TestReassignForFactorIncreasesRotatingOntoUnheldBrokers(t *testing.T) {
	current := []cluster.PartitionState{
		{ID: 0, Leader: 1, Replicas: []int32{1, 2}},
		{ID: 1, Leader: 2, Replicas: []int32{2, 3}},
	}
	got, err := reassignForFactor(current, []int32{1, 2, 3, 4}, 3)
	require.NoError(t, err)
	require.Equal(t, map[int32][]int32{
		0: {1, 2, 3},
		1: {2, 3, 4},
	}, got)
}

func TestReassignForFactorDecreaseKeepsLeaderFirst(t *testing.T) {
	current := []cluster.PartitionState{
		{ID: 0, Leader: 2, Replicas: []int32{1, 2, 3}},
		{ID: 1, Leader: 1, Replicas: []int32{1, 3, 4}},
	}
	got, err := reassignForFactor(current, []int32{1, 2, 3, 4}, 1)
	require.NoError(t, err)
	require.Equal(t, map[int32][]int32{
		0: {2}, // leader (2) survives, even though it wasn't first in Replicas
		1: {1}, // leader (1) already first
	}, got)
}

func TestReassignForFactorNoOpWhenTargetMatchesCurrent(t *testing.T) {
	current := []cluster.PartitionState{
		{ID: 0, Leader: 1, Replicas: []int32{1, 2}},
	}
	got, err := reassignForFactor(current, []int32{1, 2, 3}, 2)
	require.NoError(t, err)
	require.Equal(t, map[int32][]int32{0: {1, 2}}, got)
}

// TestReassignForFactorDecreaseWithNoLeaderFallsBackToFirstReplicas covers a
// partition with no current leader (Leader == -1 — e.g. offline/under-
// replicated). Placing p.Leader first unconditionally would put broker ID -1
// into the assignment, which a real cluster rejects; shrinkReplicas must
// instead fall back to just the first `target` entries of Replicas in their
// original order.
func TestReassignForFactorDecreaseWithNoLeaderFallsBackToFirstReplicas(t *testing.T) {
	current := []cluster.PartitionState{
		{ID: 0, Leader: -1, Replicas: []int32{3, 1, 2}},
	}
	got, err := reassignForFactor(current, []int32{1, 2, 3}, 2)
	require.NoError(t, err)
	require.Equal(t, map[int32][]int32{0: {3, 1}}, got) // no leader to place first; first 2 of Replicas kept, in original order
	for _, replicas := range got {
		for _, b := range replicas {
			require.GreaterOrEqual(t, b, int32(0), "shrinkReplicas must never place a leaderless broker ID (-1) into the assignment")
		}
	}
}

func TestReassignForFactorErrorsWhenTargetExceedsBrokerCount(t *testing.T) {
	current := []cluster.PartitionState{{ID: 0, Leader: 1, Replicas: []int32{1}}}
	_, err := reassignForFactor(current, []int32{1, 2}, 5)
	require.Error(t, err)
}

func TestReassignForFactorErrorsWhenTargetBelowOne(t *testing.T) {
	current := []cluster.PartitionState{{ID: 0, Leader: 1, Replicas: []int32{1}}}
	_, err := reassignForFactor(current, []int32{1, 2}, 0)
	require.Error(t, err)
	_, err = reassignForFactor(current, []int32{1, 2}, -1)
	require.Error(t, err)
}

// TestReassignForFactorBalancesAddedReplicasAcrossPartitions is the brief's
// "多分区分布均衡性" soft assertion: 4 partitions, each currently a single
// replica on a distinct broker (a symmetric worst case where every broker is
// already a leader for exactly one partition), growing 1->2 replicas. Every
// broker should end up with the same total replica count (leader slot +
// however many times it was picked as a new replica for another partition) —
// a difference of at most 1 across brokers, asserted generically rather than
// pinned to one specific rotation outcome.
func TestReassignForFactorBalancesAddedReplicasAcrossPartitions(t *testing.T) {
	current := []cluster.PartitionState{
		{ID: 0, Leader: 1, Replicas: []int32{1}},
		{ID: 1, Leader: 2, Replicas: []int32{2}},
		{ID: 2, Leader: 3, Replicas: []int32{3}},
		{ID: 3, Leader: 4, Replicas: []int32{4}},
	}
	brokers := []int32{1, 2, 3, 4}
	got, err := reassignForFactor(current, brokers, 2)
	require.NoError(t, err)
	require.Len(t, got, 4)

	counts := map[int32]int{}
	for _, replicas := range got {
		require.Len(t, replicas, 2) // grew from 1 to target 2
		for _, b := range replicas {
			counts[b]++
		}
	}
	minC, maxC := -1, -1
	for _, b := range brokers {
		c := counts[b]
		if minC == -1 || c < minC {
			minC = c
		}
		if maxC == -1 || c > maxC {
			maxC = c
		}
	}
	require.LessOrEqual(t, maxC-minC, 1, "counts=%v", counts)
}

// --- configOps (P1b Task 5's incremental config diff, three-state) ---

func TestConfigOpsThreeStates(t *testing.T) {
	current := []cluster.ConfigEntry{
		{Name: "cleanup.policy", Value: "delete", Source: "DYNAMIC_TOPIC_CONFIG"}, // will be unset (absent from desired)
		{Name: "retention.ms", Value: "60000", Source: "DYNAMIC_TOPIC_CONFIG"},    // unchanged (same value in desired)
		{Name: "segment.bytes", Value: "1073741824", Source: "DEFAULT_CONFIG"},    // static/default, absent from desired -> no unset (never dynamically overridden)
	}
	desired := map[string]string{
		"retention.ms":      "60000",   // unchanged
		"max.message.bytes": "2000000", // new key -> set
	}
	got := configOps(current, desired)
	require.Equal(t, []configOp{
		{Name: "cleanup.policy", Unset: true},
		{Name: "max.message.bytes", Value: "2000000"},
	}, got)
}

func TestConfigOpsSetOverwritesExistingDynamicValue(t *testing.T) {
	current := []cluster.ConfigEntry{{Name: "cleanup.policy", Value: "delete", Source: "DYNAMIC_TOPIC_CONFIG"}}
	desired := map[string]string{"cleanup.policy": "compact"}
	got := configOps(current, desired)
	require.Equal(t, []configOp{{Name: "cleanup.policy", Value: "compact"}}, got)
}

func TestConfigOpsEmptyDesiredUnsetsAllDynamicKeys(t *testing.T) {
	current := []cluster.ConfigEntry{
		{Name: "cleanup.policy", Value: "delete", Source: "DYNAMIC_TOPIC_CONFIG"},
		{Name: "retention.ms", Value: "1000", Source: "DYNAMIC_TOPIC_CONFIG"},
	}
	got := configOps(current, nil)
	require.Equal(t, []configOp{
		{Name: "cleanup.policy", Unset: true},
		{Name: "retention.ms", Unset: true},
	}, got)
}

// --- refreshAfterWrite (Task 8b: post-write cache refresh helper) ---

// TestRefreshAfterWriteLogsWarnAndSwallowsRefreshError directly exercises
// refreshAfterWrite's defensive err != nil branch. In production this branch
// is (per its doc comment) near-unreachable: every write method's own
// earlier Resolver.Lookup(name) call already proved name known before
// refreshAfterWrite is ever reached, so StateCache.Refresh's only error case
// (its own Resolver.Lookup failing) can't actually happen on that same
// name/Resolver pair. Rather than contriving an unreachable-in-practice race
// through the public write methods, this calls the unexported method
// directly with a name the StateCache's Resolver was never configured with,
// proving the branch logs and swallows the error (doesn't panic, doesn't
// propagate anything to a caller that has no error to receive) instead of
// silently doing nothing.
func TestRefreshAfterWriteLogsWarnAndSwallowsRefreshError(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(prev)

	res := NewResolver([]cluster.Definition{{Name: "prod"}})
	fs := &fakeScraper{}
	states := NewStateCache(res, fs, fs, time.Hour)
	svc := NewTopicService(res, states, nil)

	svc.refreshAfterWrite(context.Background(), "unknown-cluster", "TestOp")

	require.Contains(t, buf.String(), "post-write cache refresh failed")
	require.Contains(t, buf.String(), "TestOp")
	require.Contains(t, buf.String(), "unknown-cluster")
}
