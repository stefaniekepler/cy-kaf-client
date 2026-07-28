package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

type concurrentOffsetLister struct {
	startCalled chan struct{}
	endCalled   chan struct{}
}

func newConcurrentOffsetLister() *concurrentOffsetLister {
	return &concurrentOffsetLister{
		startCalled: make(chan struct{}),
		endCalled:   make(chan struct{}),
	}
}

func (f *concurrentOffsetLister) ListStartOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error) {
	close(f.startCalled)
	select {
	case <-f.endCalled:
		return kadm.ListedOffsets{topics[0]: {
			0: {Topic: topics[0], Partition: 0, Offset: 4},
		}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *concurrentOffsetLister) ListEndOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error) {
	close(f.endCalled)
	select {
	case <-f.startCalled:
		return kadm.ListedOffsets{topics[0]: {
			0: {Topic: topics[0], Partition: 0, Offset: 8},
		}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestListPartitionRangesStartsWatermarkRequestsConcurrently(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	got, err := listPartitionRanges(ctx, newConcurrentOffsetLister(), "orders", nil)

	require.NoError(t, err)
	require.Equal(t, map[int32]cluster.OffsetRange{
		0: {Start: 4, End: 8},
	}, got)
}

// TestRawRecordFromMapsEveryField pins down rawRecordFrom's kgo.Record ->
// cluster.RawRecord mapping field by field, with no live cluster (pure
// transform). TimestampMs must come from Timestamp.UnixMilli(); KeySize/
// ValueSize are len(); HeadersSize is the summed byte length of every
// header's key+value.
func TestRawRecordFromMapsEveryField(t *testing.T) {
	ts := time.UnixMilli(1_700_000_000_123)
	rec := &kgo.Record{
		Partition: 7,
		Offset:    42,
		Timestamp: ts,
		Key:       []byte("the-key"),
		Value:     []byte("the-value-bytes"),
		Headers: []kgo.RecordHeader{
			{Key: "h1", Value: []byte("v1")},
			{Key: "content-type", Value: []byte("application/json")},
		},
	}

	got := rawRecordFrom(rec)

	require.Equal(t, int32(7), got.Partition)
	require.Equal(t, int64(42), got.Offset)
	require.Equal(t, ts.UnixMilli(), got.TimestampMs)
	require.Equal(t, []byte("the-key"), got.Key)
	require.Equal(t, []byte("the-value-bytes"), got.Value)
	require.Equal(t, map[string]string{"h1": "v1", "content-type": "application/json"}, got.Headers)
	require.False(t, got.Control)
	require.Equal(t, len("the-key"), got.KeySize)
	require.Equal(t, len("the-value-bytes"), got.ValueSize)
	// HeadersSize sums key+value bytes across BOTH headers:
	// len("h1")+len("v1") + len("content-type")+len("application/json").
	require.Equal(t, (len("h1")+len("v1"))+(len("content-type")+len("application/json")), got.HeadersSize)
}

// TestRawRecordFromDuplicateHeaderKey documents an intentional, known
// divergence: Kafka records may carry MULTIPLE headers with the same key, but
// RawRecord.Headers is a map[string]string (brief-mandated, mirroring upstream
// kafka-ui's Map<String,String> header DTO), so the last occurrence wins in
// the map. HeadersSize, computed before the map collapse, still counts every
// occurrence's bytes. This test locks that behavior so the collapse can't
// regress silently into (say) counting only the surviving header or dropping
// the size of the shadowed one.
func TestRawRecordFromDuplicateHeaderKey(t *testing.T) {
	rec := &kgo.Record{
		Headers: []kgo.RecordHeader{
			{Key: "dup", Value: []byte("first")},
			{Key: "dup", Value: []byte("second-longer")},
		},
	}

	got := rawRecordFrom(rec)

	// Map keeps last-write-wins: only the second value survives.
	require.Equal(t, map[string]string{"dup": "second-longer"}, got.Headers)
	// HeadersSize counts BOTH occurrences' bytes, not just the survivor's.
	wantSize := (len("dup") + len("first")) + (len("dup") + len("second-longer"))
	require.Equal(t, wantSize, got.HeadersSize)
}

// TestRawRecordFromEmptyHeaders confirms the no-header case: an empty
// (non-nil) map and zero HeadersSize, with nil Key/Value preserved as
// zero-length.
func TestRawRecordFromEmptyHeaders(t *testing.T) {
	got := rawRecordFrom(&kgo.Record{Partition: 0, Offset: 0, Timestamp: time.UnixMilli(0)})

	require.Equal(t, map[string]string{}, got.Headers)
	require.Equal(t, 0, got.HeadersSize)
	require.Equal(t, 0, got.KeySize)
	require.Equal(t, 0, got.ValueSize)
	require.Equal(t, int64(0), got.TimestampMs)
}

func TestFlattenFetchesKeepsRecordsAlongsidePartitionError(t *testing.T) {
	boom := errors.New("partition unavailable")
	fetches := kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic: "orders",
			Partitions: []kgo.FetchPartition{
				{Partition: 0, Records: []*kgo.Record{{Partition: 0, Offset: 7, Timestamp: time.UnixMilli(1)}}},
				{Partition: 1, Err: boom},
			},
		}},
	}}

	got, err := flattenFetches(fetches)
	require.Len(t, got, 1)
	require.Equal(t, int64(7), got[0].Offset)
	require.ErrorIs(t, err, boom)
}

func TestFlattenFetchesReturnsEmptyRoundWithoutError(t *testing.T) {
	got, err := flattenFetches(kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic:      "orders",
			Partitions: []kgo.FetchPartition{{Partition: 0}},
		}},
	}})
	require.Nil(t, got)
	require.NoError(t, err)
}

func TestOpenReaderKeepsControlOnlyForAnalysis(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{
		Name: "reader-options",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"127.0.0.1:1"}},
	}

	ordinary, err := p.openReader(def, "orders", map[int32]int64{0: 0}, false)
	require.NoError(t, err)
	ordinarySession := ordinary.(*readerSession)
	require.False(t, ordinarySession.cl.OptValue(kgo.KeepControlRecords).(bool))
	ordinarySession.Close()

	analysis, err := p.openReader(def, "orders", map[int32]int64{0: 0}, true)
	require.NoError(t, err)
	analysisSession := analysis.(*readerSession)
	require.True(t, analysisSession.cl.OptValue(kgo.KeepControlRecords).(bool))
	analysisSession.Close()
}

func TestOpenReaderReusesIdleClientForSameAssignment(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{
		Name: "reader-reuse",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"127.0.0.1:1"}},
	}

	first, err := p.Open(context.Background(), def, "orders", map[int32]int64{0: 10})
	require.NoError(t, err)
	firstClient := first.(*readerSession).cl
	first.Close()

	second, err := p.Open(context.Background(), def, "orders", map[int32]int64{0: 20})
	require.NoError(t, err)
	secondClient := second.(*readerSession).cl
	second.Close()

	require.Same(t, firstClient, secondClient)
}

func TestReaderCloseIsIdempotent(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{
		Name: "reader-idempotent-close",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"127.0.0.1:1"}},
	}

	session, err := p.Open(context.Background(), def, "orders", map[int32]int64{0: 10})
	require.NoError(t, err)
	session.Close()
	session.Close()

	p.mu.Lock()
	defer p.mu.Unlock()
	require.Equal(t, 1, p.idleReaderCount)
}

func TestFailedReaderIsNotReused(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{
		Name: "reader-failure",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"127.0.0.1:1"}},
	}

	first, err := p.Open(context.Background(), def, "orders", map[int32]int64{0: 10})
	require.NoError(t, err)
	firstSession := first.(*readerSession)
	firstClient := firstSession.cl
	firstSession.reusable.Store(false)
	first.Close()

	second, err := p.Open(context.Background(), def, "orders", map[int32]int64{0: 20})
	require.NoError(t, err)
	secondClient := second.(*readerSession).cl
	second.Close()

	require.NotSame(t, firstClient, secondClient)
}

func TestPoolClosePreventsCheckedOutReaderFromReturningIdle(t *testing.T) {
	p := NewPool()
	def := cluster.Definition{
		Name: "reader-close-generation",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"127.0.0.1:1"}},
	}

	session, err := p.Open(context.Background(), def, "orders", map[int32]int64{0: 10})
	require.NoError(t, err)

	p.Close()
	session.Close()

	p.mu.Lock()
	defer p.mu.Unlock()
	require.Zero(t, p.idleReaderCount)
	require.Empty(t, p.idleReaders)
}

func TestInvalidatePreventsCheckedOutReaderFromReturningIdle(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{
		Name: "reader-invalidate-generation",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"127.0.0.1:1"}},
	}

	first, err := p.Open(context.Background(), def, "orders", map[int32]int64{0: 10})
	require.NoError(t, err)
	firstClient := first.(*readerSession).cl

	p.Invalidate(def.Name)
	first.Close()

	second, err := p.Open(context.Background(), def, "orders", map[int32]int64{0: 20})
	require.NoError(t, err)
	secondClient := second.(*readerSession).cl
	second.Close()

	require.NotSame(t, firstClient, secondClient)
}

func TestReaderIdlePoolHasGlobalBound(t *testing.T) {
	p := NewPool()
	defer p.Close()
	def := cluster.Definition{
		Name: "reader-idle-bound",
		Conn: cluster.ConnectionSpec{BootstrapServers: []string{"127.0.0.1:1"}},
	}

	sessions := make([]cluster.ReaderSession, 0, maxIdleReaderClients+2)
	for i := 0; i < maxIdleReaderClients+2; i++ {
		session, err := p.Open(
			context.Background(),
			def,
			fmt.Sprintf("orders-%d", i),
			map[int32]int64{0: 10},
		)
		require.NoError(t, err)
		sessions = append(sessions, session)
	}
	for _, session := range sessions {
		session.Close()
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	require.Equal(t, maxIdleReaderClients, p.idleReaderCount)
}

func TestPartitionRangesFromOffsetsRejectsPartitionError(t *testing.T) {
	boom := errors.New("offline")
	starts := kadm.ListedOffsets{"orders": {
		0: {Topic: "orders", Partition: 0, Offset: 4},
	}}
	ends := kadm.ListedOffsets{"orders": {
		0: {Topic: "orders", Partition: 0, Offset: 8, Err: boom},
	}}

	_, err := partitionRangesFromOffsets("orders", starts, ends, nil)
	require.ErrorIs(t, err, boom)
}

func TestPartitionRangesFromOffsetsRejectsInvalidRange(t *testing.T) {
	starts := kadm.ListedOffsets{"orders": {
		0: {Topic: "orders", Partition: 0, Offset: 9},
	}}
	ends := kadm.ListedOffsets{"orders": {
		0: {Topic: "orders", Partition: 0, Offset: 8},
	}}

	_, err := partitionRangesFromOffsets("orders", starts, ends, nil)
	require.Error(t, err)
}

func TestPartitionRangesFromOffsetsKeepsUnknownTopicSentinelAsEmpty(t *testing.T) {
	starts := kadm.ListedOffsets{"orders": {
		-1: {Topic: "orders", Partition: -1, Offset: -1, Err: errors.New("unknown topic")},
	}}
	ends := kadm.ListedOffsets{"orders": {
		-1: {Topic: "orders", Partition: -1, Offset: -1, Err: errors.New("unknown topic")},
	}}

	got, err := partitionRangesFromOffsets("orders", starts, ends, nil)
	require.NoError(t, err)
	require.Empty(t, got)
}

// TestPartitionSet covers the partition-filter helper's two modes: nil/empty
// means "no filter" (returns nil, which callers read as match-all), and an
// explicit list builds a membership set containing exactly those IDs.
func TestPartitionSet(t *testing.T) {
	require.Nil(t, partitionSet(nil), "nil input means no filter (all partitions)")
	require.Nil(t, partitionSet([]int32{}), "empty input means no filter (all partitions)")

	set := partitionSet([]int32{0, 2, 5})
	require.Equal(t, map[int32]bool{0: true, 2: true, 5: true}, set)
	require.True(t, set[0])
	require.True(t, set[2])
	require.True(t, set[5])
	require.False(t, set[1], "an unlisted partition is not a member")
	require.False(t, set[3])
}
