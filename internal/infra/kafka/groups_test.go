package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// --- error paths (TDD core for this step; real success-path behaviour
// against a live cluster is locked down by T7's integration tests, per the
// task brief) — same "eager security rejection" + "unreachable broker"
// shape as topics_test.go/state_test.go's existing method pairs. ---

func TestListGroupsRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "bad",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"},
			Security: map[string]string{"security.protocol": "SASL_SSL"}}}

	groups, err := p.ListGroups(context.Background(), def)
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
	require.Nil(t, groups)
}

func TestListGroupsReturnsErrorWhenBrokerUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "unreachable", Conn: cluster.ConnectionSpec{BootstrapServers: []string{addr}}}

	groups, err := p.ListGroups(ctx, def)
	require.ErrorContains(t, err, "list groups")
	require.Nil(t, groups)
}

func TestDescribeGroupRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "bad",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"},
			Security: map[string]string{"security.protocol": "SASL_SSL"}}}

	gs, err := p.DescribeGroup(context.Background(), def, "g1")
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
	require.Equal(t, cluster.GroupState{}, gs)
}

func TestDescribeGroupReturnsErrorWhenBrokerUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "unreachable", Conn: cluster.ConnectionSpec{BootstrapServers: []string{addr}}}

	gs, err := p.DescribeGroup(ctx, def, "g1")
	require.ErrorContains(t, err, "describe consumer group")
	require.Equal(t, cluster.GroupState{}, gs)
}

func TestGroupsForTopicRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "bad",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"},
			Security: map[string]string{"security.protocol": "SASL_SSL"}}}

	groups, err := p.GroupsForTopic(context.Background(), def, "t1")
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
	require.Nil(t, groups)
}

func TestGroupsForTopicReturnsErrorWhenBrokerUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "unreachable", Conn: cluster.ConnectionSpec{BootstrapServers: []string{addr}}}

	groups, err := p.GroupsForTopic(ctx, def, "t1")
	require.ErrorContains(t, err, "list groups")
	require.Nil(t, groups)
}

type fakeGroupAdminClient struct {
	listed              kadm.ListedGroups
	fetched             kadm.FetchOffsetsResponses
	described           kadm.DescribedGroups
	oneOffsets          map[string]kadm.OffsetResponses
	endOffsets          kadm.ListedOffsets
	listCalls           int
	fetchManyCalls      int
	describeCalls       int
	fetchOneCalls       int
	listEndOffsetsCalls int
	fetchManyArgs       [][]string
	describeArgs        [][]string
	fetchOneArgs        []string
	listEndOffsetsArgs  [][]string
}

func (f *fakeGroupAdminClient) ListGroups(context.Context, ...string) (kadm.ListedGroups, error) {
	f.listCalls++
	return f.listed, nil
}

func (f *fakeGroupAdminClient) FetchManyOffsets(_ context.Context, groups ...string) kadm.FetchOffsetsResponses {
	f.fetchManyCalls++
	f.fetchManyArgs = append(f.fetchManyArgs, append([]string(nil), groups...))
	return f.fetched
}

func (f *fakeGroupAdminClient) DescribeGroups(_ context.Context, groups ...string) (kadm.DescribedGroups, error) {
	f.describeCalls++
	f.describeArgs = append(f.describeArgs, append([]string(nil), groups...))
	return f.described, nil
}

func (f *fakeGroupAdminClient) FetchOffsets(_ context.Context, group string) (kadm.OffsetResponses, error) {
	f.fetchOneCalls++
	f.fetchOneArgs = append(f.fetchOneArgs, group)
	return f.oneOffsets[group], nil
}

func (f *fakeGroupAdminClient) ListEndOffsets(_ context.Context, topics ...string) (kadm.ListedOffsets, error) {
	f.listEndOffsetsCalls++
	f.listEndOffsetsArgs = append(f.listEndOffsetsArgs, append([]string(nil), topics...))
	return f.endOffsets, nil
}

func TestGroupsForTopicPrefiltersAllNonMatchingGroupsWithBoundedOffsetFetches(t *testing.T) {
	fake := &fakeGroupAdminClient{
		listed:     make(kadm.ListedGroups, 169),
		fetched:    make(kadm.FetchOffsetsResponses, 169),
		oneOffsets: make(map[string]kadm.OffsetResponses),
	}
	wantNames := make([]string, 0, 169)
	for i := range 169 {
		id := fmt.Sprintf("group-%03d", i)
		wantNames = append(wantNames, id)
		fake.listed[id] = kadm.ListedGroup{Group: id}
		fake.fetched[id] = kadm.FetchOffsetsResponse{
			Group: id,
			Fetched: kadm.OffsetResponses{
				"another-topic": {
					0: {Offset: kadm.Offset{Topic: "another-topic", Partition: 0, At: 1}},
				},
			},
		}
	}

	got, err := groupsForTopic(context.Background(), fake, "requested-topic")

	require.NoError(t, err)
	require.Empty(t, got)
	require.Equal(t, 1, fake.listCalls)
	require.Equal(t, 6, fake.fetchManyCalls)
	require.Len(t, fake.fetchManyArgs, 6)
	var gotNames []string
	var gotBatchSizes []int
	for _, batch := range fake.fetchManyArgs {
		gotNames = append(gotNames, batch...)
		gotBatchSizes = append(gotBatchSizes, len(batch))
	}
	require.Equal(t, []int{32, 32, 32, 32, 32, 9}, gotBatchSizes)
	require.Equal(t, wantNames, gotNames)
	require.Zero(t, fake.describeCalls)
	require.Zero(t, fake.fetchOneCalls)
	require.Zero(t, fake.listEndOffsetsCalls)
}

func TestGroupsForTopicDescribesOnlyMatchingGroups(t *testing.T) {
	targetOffsets := kadm.OffsetResponses{
		"requested-topic": {
			0: {Offset: kadm.Offset{Topic: "requested-topic", Partition: 0, At: 4}},
		},
	}
	fake := &fakeGroupAdminClient{
		listed: kadm.ListedGroups{
			"matching-group": {Group: "matching-group"},
			"other-group":    {Group: "other-group"},
			"failed-group":   {Group: "failed-group"},
		},
		fetched: kadm.FetchOffsetsResponses{
			"matching-group": {Group: "matching-group", Fetched: targetOffsets},
			"other-group": {
				Group: "other-group",
				Fetched: kadm.OffsetResponses{
					"another-topic": {
						0: {Offset: kadm.Offset{Topic: "another-topic", Partition: 0, At: 2}},
					},
				},
			},
			"failed-group": {Group: "failed-group", Err: errors.New("offset fetch denied")},
		},
		described: kadm.DescribedGroups{
			"matching-group": {Group: "matching-group", State: "Stable"},
		},
		oneOffsets: map[string]kadm.OffsetResponses{
			"matching-group": targetOffsets,
		},
		endOffsets: kadm.ListedOffsets{
			"requested-topic": {
				0: {Topic: "requested-topic", Partition: 0, Offset: 10},
			},
		},
	}

	got, err := groupsForTopic(context.Background(), fake, "requested-topic")

	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "matching-group", got[0].ID)
	require.Equal(t, 1, fake.describeCalls)
	require.Equal(t, 1, fake.fetchOneCalls)
	require.Equal(t, 1, fake.listEndOffsetsCalls)
	require.Equal(t, [][]string{{"failed-group", "matching-group", "other-group"}}, fake.fetchManyArgs)
	require.Equal(t, [][]string{{"matching-group"}}, fake.describeArgs)
	require.Equal(t, []string{"matching-group"}, fake.fetchOneArgs)
	require.Equal(t, [][]string{{"requested-topic"}}, fake.listEndOffsetsArgs)
}

func TestGroupsForTopicRechecksTopicAfterDetailFetch(t *testing.T) {
	prefilterOffsets := kadm.OffsetResponses{
		"requested-topic": {
			0: {Offset: kadm.Offset{Topic: "requested-topic", Partition: 0, At: 4}},
		},
	}
	detailOffsets := kadm.OffsetResponses{
		"another-topic": {
			0: {Offset: kadm.Offset{Topic: "another-topic", Partition: 0, At: 2}},
		},
	}
	fake := &fakeGroupAdminClient{
		listed: kadm.ListedGroups{
			"changed-group": {Group: "changed-group"},
		},
		fetched: kadm.FetchOffsetsResponses{
			"changed-group": {Group: "changed-group", Fetched: prefilterOffsets},
		},
		described: kadm.DescribedGroups{
			"changed-group": {Group: "changed-group", State: "Stable"},
		},
		oneOffsets: map[string]kadm.OffsetResponses{
			"changed-group": detailOffsets,
		},
		endOffsets: kadm.ListedOffsets{
			"another-topic": {
				0: {Topic: "another-topic", Partition: 0, Offset: 10},
			},
		},
	}

	got, err := groupsForTopic(context.Background(), fake, "requested-topic")

	require.NoError(t, err)
	require.Empty(t, got)
}

func TestResetOffsetsRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "bad",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"},
			Security: map[string]string{"security.protocol": "SASL_SSL"}}}

	err := p.ResetOffsets(context.Background(), def, "g1", cluster.ResetSpec{Topic: "t1", ResetType: "OFFSET"})
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
}

// TestResetOffsetsReturnsErrorWhenBrokerUnreachable uses the EARLIEST reset
// type deliberately, not OFFSET: EARLIEST/LATEST/TIMESTAMP all resolve
// broker-side offsets via a List*Offsets call before ever reaching
// CommitOffsets, and that call fails with a plain top-level dial error
// against an unreachable broker (confirmed empirically) — the same shape
// every other *ReturnsErrorWhenBrokerUnreachable test in this package
// asserts. OFFSET skips straight to CommitOffsets, which behaves
// differently against this exact "closed port" unreachable simulation: its
// top-level call succeeds and the failure surfaces as a synthetic
// per-partition UNKNOWN_TOPIC_OR_PARTITION inside the response instead (kadm
// resolving the group's coordinator/topic IDs degrades that way rather than
// erroring outright) — real-broker behavior for OFFSET is exercised by T7's
// integration tests instead, consistent with this task being unit-tests-only.
func TestResetOffsetsReturnsErrorWhenBrokerUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "unreachable", Conn: cluster.ConnectionSpec{BootstrapServers: []string{addr}}}

	err = p.ResetOffsets(ctx, def, "g1", cluster.ResetSpec{Topic: "t1", ResetType: "EARLIEST"})
	require.ErrorContains(t, err, "list start offsets")
}

// TestResetOffsetsRejectsUnknownResetType covers resetOffsetsFor's default
// branch: an unrecognized ResetType is rejected before any network call is
// attempted (the switch runs before CommitOffsets), so this needs no live
// broker at all — a perfectly ordinary (if made-up) bootstrap address is
// enough, since ResetOffsets never reaches it for this input.
func TestResetOffsetsRejectsUnknownResetType(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "prod", Conn: cluster.ConnectionSpec{BootstrapServers: []string{"127.0.0.1:1"}}}

	err := p.ResetOffsets(context.Background(), def, "g1", cluster.ResetSpec{Topic: "t1", ResetType: "BOGUS"})
	require.ErrorContains(t, err, "unknown reset type")
}

func TestDeleteGroupRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "bad",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"},
			Security: map[string]string{"security.protocol": "SASL_SSL"}}}

	err := p.DeleteGroup(context.Background(), def, "g1")
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
}

func TestDeleteGroupReturnsErrorWhenBrokerUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "unreachable", Conn: cluster.ConnectionSpec{BootstrapServers: []string{addr}}}

	err = p.DeleteGroup(ctx, def, "g1")
	require.ErrorContains(t, err, "delete consumer group")
}

func TestDeleteGroupOffsetsRejectsUnsupportedSecurityEagerly(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "bad",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"k:9092"},
			Security: map[string]string{"security.protocol": "SASL_SSL"}}}

	err := p.DeleteGroupOffsets(context.Background(), def, "g1", "t1")
	require.ErrorIs(t, err, ErrUnsupportedSecurity)
}

func TestDeleteGroupOffsetsReturnsErrorWhenBrokerUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p := NewPool()
	defer p.Close()
	def := cluster.Definition{Name: "unreachable", Conn: cluster.ConnectionSpec{BootstrapServers: []string{addr}}}

	err = p.DeleteGroupOffsets(ctx, def, "g1", "t1")
	require.ErrorContains(t, err, "fetch offsets for delete")
}

// --- pure mapping function table tests ---

// TestGroupStateToGenerated locks the kadm raw state string -> contract
// ConsumerGroupState enum spelling table brief Step 2 calls for, including
// the "unrecognized -> UNKNOWN" default (never leak a raw driver string).
func TestGroupStateToGenerated(t *testing.T) {
	cases := []struct{ in, want string }{
		{"Empty", "EMPTY"},
		{"Stable", "STABLE"},
		{"Dead", "DEAD"},
		{"PreparingRebalance", "PREPARING_REBALANCE"},
		{"CompletingRebalance", "COMPLETING_REBALANCE"},
		{"", "UNKNOWN"},
		{"SomethingUnexpected", "UNKNOWN"},
	}
	for _, c := range cases {
		require.Equal(t, c.want, groupStateToGenerated(c.in), "in=%q", c.in)
	}
}

// TestGroupTouchesTopic covers GroupsForTopic's per-group filter predicate:
// true iff gs has at least one offset entry for topic, regardless of any
// other topics it also touches.
func TestGroupTouchesTopic(t *testing.T) {
	gs := cluster.GroupState{Offsets: []cluster.GroupOffset{
		{Topic: "orders", Partition: 0}, {Topic: "payments", Partition: 0},
	}}
	require.True(t, groupTouchesTopic(gs, "orders"))
	require.True(t, groupTouchesTopic(gs, "payments"))
	require.False(t, groupTouchesTopic(gs, "shipments"))
	require.False(t, groupTouchesTopic(cluster.GroupState{}, "orders"))
}

// TestFilterOffsetsByPartitions covers resetOffsetsFor's partition-filter
// helper: an empty/nil partitions filter keeps every partition of the named
// topic (and only that topic, if the input Offsets happens to carry more
// than one — it never does in practice since callers always list a single
// topic, but the filter itself doesn't assume that); a non-empty filter
// keeps only the requested partition numbers.
func TestFilterOffsetsByPartitions(t *testing.T) {
	all := make(kadm.Offsets)
	all.Add(kadm.Offset{Topic: "t1", Partition: 0, At: 10})
	all.Add(kadm.Offset{Topic: "t1", Partition: 1, At: 20})
	all.Add(kadm.Offset{Topic: "t1", Partition: 2, At: 30})

	t.Run("empty filter keeps every partition", func(t *testing.T) {
		got := filterOffsetsByPartitions(all, "t1", nil)
		require.Len(t, got["t1"], 3)
	})

	t.Run("non-empty filter keeps only requested partitions", func(t *testing.T) {
		got := filterOffsetsByPartitions(all, "t1", []int32{0, 2})
		require.Len(t, got["t1"], 2)
		require.Contains(t, got["t1"], int32(0))
		require.Contains(t, got["t1"], int32(2))
		require.NotContains(t, got["t1"], int32(1))
	})

	t.Run("filter naming a partition absent from the input yields nothing for it", func(t *testing.T) {
		got := filterOffsetsByPartitions(all, "t1", []int32{99})
		require.Empty(t, got["t1"])
	})
}
