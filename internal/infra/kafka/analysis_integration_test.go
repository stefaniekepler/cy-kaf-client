//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kgo"

	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	domainanalysis "github.com/cy-kaf/cy-kaf-client/internal/domain/analysis"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// TestTopicAnalysisRealRoundTrip proves the complete P2d path against a real
// broker: explicit partition production, pooled watermark discovery, the
// dedicated reader session, and app-layer aggregation into total and
// partition snapshots. The fixture deliberately includes duplicate and nil
// keys/values so the HLL union and null/size semantics are exercised together.
func TestTopicAnalysisRealRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.8.0")
	testcontainers.CleanupContainer(t, kc)
	require.NoError(t, err)
	brokers, err := kc.Brokers(ctx)
	require.NoError(t, err)

	pool := NewPool()
	defer pool.Close()
	def := cluster.Definition{
		Name: "it-topic-analysis",
		Conn: cluster.ConnectionSpec{BootstrapServers: brokers},
	}
	const topic = "topic-analysis-real-round-trip"
	require.NoError(t, pool.CreateTopic(ctx, def, cluster.TopicSpec{
		Name: topic, Partitions: 2, ReplicationFactor: 1,
	}))

	baseHour := time.Unix(1_700_000_000, 0).UTC().Truncate(time.Hour).UnixMilli()
	hourMillis := int64(time.Hour / time.Millisecond)
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	require.NoError(t, err)
	defer producer.Close()

	records := []*kgo.Record{
		{Topic: topic, Partition: 0, Key: []byte("alpha"), Value: []byte("one"), Timestamp: time.UnixMilli(baseHour + 1_000), Headers: []kgo.RecordHeader{{Key: "h", Value: []byte("x")}}},
		{Topic: topic, Partition: 0, Key: []byte("beta"), Value: nil, Timestamp: time.UnixMilli(baseHour + 2_000), Headers: []kgo.RecordHeader{{Key: "h", Value: []byte("yy")}}},
		{Topic: topic, Partition: 0, Key: nil, Value: []byte("three"), Timestamp: time.UnixMilli(baseHour + hourMillis + 3_000), Headers: []kgo.RecordHeader{{Key: "h", Value: []byte("z")}}},
		{Topic: topic, Partition: 1, Key: []byte("alpha"), Value: []byte("four"), Timestamp: time.UnixMilli(baseHour + 4_000), Headers: []kgo.RecordHeader{{Key: "h", Value: []byte("xx")}}},
		{Topic: topic, Partition: 1, Key: []byte("delta"), Value: []byte("five"), Timestamp: time.UnixMilli(baseHour + hourMillis + 5_000)},
		{Topic: topic, Partition: 1, Key: nil, Value: nil, Timestamp: time.UnixMilli(baseHour + hourMillis + 6_000), Headers: []kgo.RecordHeader{{Key: "h", Value: []byte("zzz")}}},
	}
	results := producer.ProduceSync(ctx, records...)
	require.NoError(t, results.FirstErr())

	resolver := appcluster.NewResolver([]cluster.Definition{def})
	service := appcluster.NewAnalysisService(ctx, resolver, pool)
	require.NoError(t, service.Analyze(context.Background(), def.Name, topic))

	result := waitForRealAnalysisResult(t, service, def.Name, topic)
	require.Empty(t, result.Error)
	require.GreaterOrEqual(t, result.FinishedAt, result.StartedAt)

	total := result.TotalStats
	require.Nil(t, total.Partition)
	require.True(t, total.HasData)
	require.Equal(t, int64(6), total.TotalMsgs)
	require.Equal(t, int64(0), total.MinOffset)
	require.Equal(t, int64(2), total.MaxOffset)
	require.Equal(t, baseHour+1_000, total.MinTimestamp)
	require.Equal(t, baseHour+hourMillis+6_000, total.MaxTimestamp)
	require.Equal(t, int64(2), total.NullKeys)
	require.Equal(t, int64(2), total.NullValues)
	require.InDelta(t, 3, total.ApproxUniqKeys, 1)
	require.InDelta(t, 4, total.ApproxUniqValues, 1)
	require.Equal(t, int64(19), total.KeySize.Sum)
	require.Equal(t, int64(0), total.KeySize.Min)
	require.Equal(t, int64(5), total.KeySize.Max)
	require.Equal(t, int64(3), total.KeySize.Avg)
	require.Equal(t, int64(16), total.ValueSize.Sum)
	require.Equal(t, int64(0), total.ValueSize.Min)
	require.Equal(t, int64(5), total.ValueSize.Max)
	require.Equal(t, int64(2), total.ValueSize.Avg)
	assertApproxSizeQuantiles(t, total.KeySize, []int64{4, 5, 5, 5, 5})
	assertApproxSizeQuantiles(t, total.ValueSize, []int64{3, 4, 5, 5, 5})
	require.Equal(t, []int64{baseHour, baseHour + hourMillis}, hourStarts(total.HourlyMsgCounts))
	require.Equal(t, []int64{3, 3}, hourCounts(total.HourlyMsgCounts))

	require.Len(t, result.PartitionStats, 2)
	assertRealPartitionStats(t, result.PartitionStats[0], 0, baseHour+1_000, baseHour+hourMillis+3_000, 3, 1, 1, 2, 1, 9, 8, 5, 5, 3, 2, 2, 2)
	assertRealPartitionStats(t, result.PartitionStats[1], 1, baseHour+4_000, baseHour+hourMillis+6_000, 3, 1, 1, 1, 2, 10, 8, 5, 4, 3, 2, 2, 2)

	// A committed transaction appends a control marker after its user record.
	// The ordinary browse reader intentionally filters that marker, while the
	// analysis-specific reader retains it as advance-only progress. This
	// regression proves a marker-only tail cannot leave the analysis goroutine
	// spinning after the user record has been counted.
	const markerTopic = "topic-analysis-transaction-marker-tail"
	require.NoError(t, pool.CreateTopic(ctx, def, cluster.TopicSpec{
		Name: markerTopic, Partitions: 1, ReplicationFactor: 1,
	}))
	txProducer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.TransactionalID("topic-analysis-marker-tail-producer"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	require.NoError(t, err)
	defer txProducer.Close()
	require.NoError(t, txProducer.BeginTransaction())
	require.NoError(t, txProducer.ProduceSync(ctx, &kgo.Record{
		Topic: markerTopic, Partition: 0, Key: []byte("transactional"), Value: []byte("value"),
	}).FirstErr())
	require.NoError(t, txProducer.EndTransaction(ctx, kgo.TransactionEndTry(true)))

	markerService := appcluster.NewAnalysisService(ctx, resolver, pool)
	require.NoError(t, markerService.Analyze(context.Background(), def.Name, markerTopic))
	markerResult := waitForRealAnalysisResult(t, markerService, def.Name, markerTopic)
	require.Empty(t, markerResult.Error)
	require.Equal(t, int64(1), markerResult.TotalStats.TotalMsgs)
}

func waitForRealAnalysisResult(t *testing.T, service *appcluster.AnalysisService, clusterName, topic string) appcluster.AnalysisResult {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		view, found, err := service.Get(clusterName, topic)
		if err == nil && found && view.Result != nil {
			return *view.Result
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("topic analysis did not reach a terminal result before the deadline")
	return appcluster.AnalysisResult{}
}

func assertRealPartitionStats(t *testing.T, stats domainanalysis.Stats, partition int32, minTimestamp, maxTimestamp, totalMsgs, nullKeys, nullValues, hour0, hour1, keySum, valueSum, keyMax, valueMax, keyAvg, valueAvg, wantUniqueKeys, wantUniqueValues int64) {
	t.Helper()
	require.NotNil(t, stats.Partition)
	require.Equal(t, partition, *stats.Partition)
	require.True(t, stats.HasData)
	require.Equal(t, totalMsgs, stats.TotalMsgs)
	require.Equal(t, int64(0), stats.MinOffset)
	require.Equal(t, int64(2), stats.MaxOffset)
	require.Equal(t, minTimestamp, stats.MinTimestamp)
	require.Equal(t, maxTimestamp, stats.MaxTimestamp)
	require.Equal(t, nullKeys, stats.NullKeys)
	require.Equal(t, nullValues, stats.NullValues)
	require.InDelta(t, wantUniqueKeys, stats.ApproxUniqKeys, 1)
	require.InDelta(t, wantUniqueValues, stats.ApproxUniqValues, 1)
	require.Equal(t, keySum, stats.KeySize.Sum)
	require.Equal(t, int64(0), stats.KeySize.Min)
	require.Equal(t, keyMax, stats.KeySize.Max)
	require.Equal(t, keyAvg, stats.KeySize.Avg)
	require.Equal(t, valueSum, stats.ValueSize.Sum)
	require.Equal(t, int64(0), stats.ValueSize.Min)
	require.Equal(t, valueMax, stats.ValueSize.Max)
	require.Equal(t, valueAvg, stats.ValueSize.Avg)
	require.Equal(t, []int64{hour0, hour1}, hourCounts(stats.HourlyMsgCounts))
}

func assertApproxSizeQuantiles(t *testing.T, stats domainanalysis.SizeStats, wants []int64) {
	t.Helper()
	got := []int64{stats.Prctl50, stats.Prctl75, stats.Prctl95, stats.Prctl99, stats.Prctl999}
	require.Len(t, wants, len(got))
	for i := range got {
		delta := float64(wants[i]) * 0.03
		if delta < 1 {
			delta = 1
		}
		require.InDelta(t, wants[i], got[i], delta)
	}
}

func hourStarts(hours []domainanalysis.HourCount) []int64 {
	out := make([]int64, 0, len(hours))
	for _, hour := range hours {
		out = append(out, hour.HourStart)
	}
	return out
}

func hourCounts(hours []domainanalysis.HourCount) []int64 {
	out := make([]int64, 0, len(hours))
	for _, hour := range hours {
		out = append(out, hour.Count)
	}
	return out
}
