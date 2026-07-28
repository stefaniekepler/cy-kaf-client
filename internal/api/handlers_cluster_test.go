package api

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/api/generated"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// Whitebox unit tests for the pure mapping functions behind the cluster
// handlers (package api, not api_test): snapshotToGenerated/inferredMetrics/
// errorResponse are unexported, so these tests exercise them directly rather
// than only indirectly through an HTTP round trip.

func TestSnapshotToGeneratedOnline(t *testing.T) {
	sn := cluster.Snapshot{
		Definition:  cluster.Definition{Name: "prod", ReadOnly: true},
		Status:      cluster.StatusOnline,
		BrokerCount: 3,
		Features:    []cluster.Feature{cluster.FeatureSchemaRegistry},
	}
	c := snapshotToGenerated(sn)
	require.Equal(t, "prod", c.Name)
	require.Equal(t, generated.ONLINE, c.Status)
	require.NotNil(t, c.BrokerCount)
	require.Equal(t, int32(3), *c.BrokerCount)
	require.NotNil(t, c.ReadOnly)
	require.True(t, *c.ReadOnly)
	require.NotNil(t, c.Features)
	require.Equal(t, []generated.ClusterFeatures{generated.ClusterFeatures(cluster.FeatureSchemaRegistry)}, *c.Features)
}

func TestSnapshotToGeneratedOfflineOmitsBrokerCount(t *testing.T) {
	sn := cluster.Snapshot{Definition: cluster.Definition{Name: "dev"}, Status: cluster.StatusOffline}
	c := snapshotToGenerated(sn)
	require.Equal(t, generated.OFFLINE, c.Status)
	require.Nil(t, c.BrokerCount) // offline → 不报 broker 数（既有 P0 语义）
	require.NotNil(t, c.Features)
	require.Empty(t, *c.Features) // 非 nil 空切片：避免 JSON null（契约 features 非 nullable）
}

func TestInferredMetricsIncludesFixedAndPerBrokerEntries(t *testing.T) {
	st := cluster.RuntimeState{
		Brokers:    []cluster.BrokerInfo{{ID: 1}, {ID: 2}},
		TopicCount: 5,
		Partitions: cluster.PartitionCounts{Online: 10, Offline: 2},
		Disk: []cluster.DiskUsage{
			{Broker: 1, SegmentSize: 1024, SegmentCount: 4},
			{Broker: 2, SegmentSize: 2048, SegmentCount: 8},
		},
	}
	items := inferredMetrics(st)
	require.NotNil(t, items) // 契约 items 为必填非空切片，绝不能是 nil（会序列化成 null）

	byName := map[string][]generated.Metric{}
	for _, m := range items {
		byName[*m.Name] = append(byName[*m.Name], m)
	}
	require.Len(t, byName["broker_count"], 1)
	require.Equal(t, float32(2), *byName["broker_count"][0].Value)
	require.Len(t, byName["topic_count"], 1)
	require.Equal(t, float32(5), *byName["topic_count"][0].Value)
	require.Len(t, byName["kafka_topic_partitions"], 2) // online + offline
	require.Len(t, byName["broker_bytes_disk"], 2)      // one per broker
	for _, m := range byName["broker_bytes_disk"] {
		require.Contains(t, *m.Labels, "broker")
	}
}

func TestInferredMetricsNeverNilEvenWithoutDiskData(t *testing.T) {
	items := inferredMetrics(cluster.RuntimeState{})
	require.NotEmpty(t, items) // broker_count/topic_count/partitions 四条固定条目恒在
}

func TestErrorResponsePopulatesAllRequiredFields(t *testing.T) {
	er := errorResponse(404, "cluster not found")
	require.Equal(t, int32(404), er.Code)
	require.Equal(t, "cluster not found", er.Message)
	require.NotEmpty(t, er.RequestId)
	require.Greater(t, er.Timestamp, float32(0))
}
