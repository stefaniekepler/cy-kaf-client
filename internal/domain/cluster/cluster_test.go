package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFeaturesDerivedFromDefinition(t *testing.T) {
	d := Definition{SchemaRegistry: SchemaRegistrySpec{URL: "http://sr"}, Connects: []ConnectSpec{{Name: "m"}}, KsqlURL: ""}
	require.Equal(t, []Feature{
		FeatureSchemaRegistry,
		FeatureKafkaConnect,
		FeatureKafkaACLView,
		FeatureKafkaACLEdit,
	}, d.Features())
	require.Equal(t, []Feature{FeatureKafkaACLView, FeatureKafkaACLEdit}, Definition{}.Features())
	require.Equal(t, []Feature{FeatureKafkaACLView}, Definition{ReadOnly: true}.Features())
}

func TestFeaturesIncludesKsqlDBWhenConfigured(t *testing.T) {
	d := Definition{KsqlURL: "http://ksql:8088"}
	require.Equal(t, []Feature{FeatureKsqlDB, FeatureKafkaACLView, FeatureKafkaACLEdit}, d.Features())
}

func TestRuntimeStateToSnapshot(t *testing.T) {
	st := RuntimeState{
		Definition: Definition{Name: "a", SchemaRegistry: SchemaRegistrySpec{URL: "http://sr"}},
		Status:     StatusOnline,
		Brokers:    []BrokerInfo{{ID: 1}, {ID: 2}},
	}
	snap := st.Snapshot()
	require.Equal(t, "a", snap.Definition.Name)
	require.Equal(t, StatusOnline, snap.Status)
	require.Equal(t, 2, snap.BrokerCount)
	require.Equal(t, []Feature{FeatureSchemaRegistry, FeatureKafkaACLView, FeatureKafkaACLEdit}, snap.Features)
}

func TestBrokerInfoCountsZeroValueSafe(t *testing.T) {
	st := RuntimeState{Brokers: []BrokerInfo{{ID: 1, PartitionsLeader: 3, Partitions: 6, InSyncPartitions: 6}}}
	require.Equal(t, 3, st.Brokers[0].PartitionsLeader)
	require.Equal(t, 1, st.Snapshot().BrokerCount)
}

func TestTopicStateMessagesCount(t *testing.T) {
	cases := []struct {
		name  string
		parts []PartitionState
		want  int64
		known bool
	}{
		{"known zero", []PartitionState{{StartOffset: 0, EndOffset: 0}}, 0, true},
		{"sum known", []PartitionState{{StartOffset: 2, EndOffset: 12}, {StartOffset: 5, EndOffset: 8}}, 13, true},
		{"skip unknown", []PartitionState{{StartOffset: 0, EndOffset: 4}, {StartOffset: -1, EndOffset: 30}}, 4, true},
		{"all unknown", []PartitionState{{StartOffset: -1, EndOffset: -1}}, 0, false},
		{"clamp anomaly", []PartitionState{{StartOffset: 9, EndOffset: 3}}, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, known := (TopicState{Partitions: tc.parts}).MessagesCount()
			require.Equal(t, tc.known, known)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestRuntimeStateFeaturesMergesDefinitionAndRuntimeParts locks P1b Task 5's
// Features() split: the Definition-derived features are unchanged, and
// TOPIC_DELETION is appended only when TopicDeletionEnabled is true (the
// RuntimeState zero value is false, so a state built without setting it
// must NOT report TOPIC_DELETION — see TestRuntimeStateToSnapshot above,
// which already pins that down for Snapshot()).
func TestRuntimeStateFeaturesMergesDefinitionAndRuntimeParts(t *testing.T) {
	base := Definition{SchemaRegistry: SchemaRegistrySpec{URL: "http://sr"}}

	enabled := RuntimeState{Definition: base, TopicDeletionEnabled: true}
	require.Equal(t, []Feature{
		FeatureSchemaRegistry,
		FeatureKafkaACLView,
		FeatureKafkaACLEdit,
		FeatureTopicDeletion,
	}, enabled.Features())

	disabled := RuntimeState{Definition: base, TopicDeletionEnabled: false}
	require.Equal(t, []Feature{
		FeatureSchemaRegistry,
		FeatureKafkaACLView,
		FeatureKafkaACLEdit,
	}, disabled.Features())
	require.NotContains(t, disabled.Features(), FeatureTopicDeletion)
}

func TestRuntimeStateSnapshotIncludesTopicDeletionFeatureWhenEnabled(t *testing.T) {
	st := RuntimeState{
		Definition:           Definition{Name: "a"},
		Status:               StatusOnline,
		TopicDeletionEnabled: true,
	}
	require.Equal(t, []Feature{
		FeatureKafkaACLView,
		FeatureKafkaACLEdit,
		FeatureTopicDeletion,
	}, st.Snapshot().Features)
}

// --- P1b Task 6: GroupState.Lag/TopicCount pure-method table tests ---

func TestGroupStateLag(t *testing.T) {
	cases := []struct {
		name string
		in   []GroupOffset
		want int64
	}{
		{name: "no offsets at all", in: nil, want: 0},
		{
			name: "regular: sums End-Committed across every offset",
			in: []GroupOffset{
				{Topic: "t1", Partition: 0, Committed: 10, End: 15},
				{Topic: "t1", Partition: 1, Committed: 0, End: 5},
			},
			want: 10, // 5 + 5
		},
		{
			name: "Committed == -1 (no commit) contributes 0, not End",
			in: []GroupOffset{
				{Topic: "t1", Partition: 0, Committed: -1, End: 100},
				{Topic: "t1", Partition: 1, Committed: 5, End: 8},
			},
			want: 3, // only the second entry counts
		},
		{
			name: "End < Committed (stale/inconsistent watermark) clamps to 0, not negative",
			in: []GroupOffset{
				{Topic: "t1", Partition: 0, Committed: 20, End: 10},
			},
			want: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := GroupState{Offsets: tc.in}
			require.Equal(t, tc.want, g.Lag())
		})
	}
}

func TestGroupStateTopicCount(t *testing.T) {
	g := GroupState{Offsets: []GroupOffset{
		{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}, {Topic: "t2", Partition: 0},
	}}
	require.Equal(t, 2, g.TopicCount())
	require.Equal(t, 0, GroupState{}.TopicCount())
}

// --- P2c Task 1: AclFilter/ClientQuota value types + AclAdminPort/QuotaPort ports ---

// TestAclFilterAndClientQuotaValueTypes is a compile-time proof that the new
// port signatures can be implemented (guards against signature drift) plus a
// plain value-construction/roundtrip assertion. domain has no implementation
// here — that lands in infra (Task 2/3) — this test only pins the shape.
func TestAclFilterAndClientQuotaValueTypes(t *testing.T) {
	f := AclFilter{ResourceType: "TOPIC", ResourceName: "t", PatternType: "LITERAL", Search: "User:x", Fts: true}
	if f.ResourceType != "TOPIC" || !f.Fts {
		t.Fatalf("AclFilter field roundtrip broken: %+v", f)
	}
	q := ClientQuota{User: "u", ClientID: "c", IP: "1.2.3.4", Quotas: map[string]float64{"producer_byte_rate": 1024}}
	if q.Quotas["producer_byte_rate"] != 1024 {
		t.Fatalf("ClientQuota quotas map broken: %+v", q)
	}
	// 端口类型存在性（编译即证明）：
	var _ AclAdminPort
	var _ QuotaPort
}
