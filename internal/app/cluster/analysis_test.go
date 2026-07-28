package cluster

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	domaincluster "github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

type analysisReader struct {
	mu            sync.Mutex
	ranges        map[int32]domaincluster.OffsetRange
	records       []domaincluster.RawRecord
	firstPoll     []domaincluster.RawRecord
	firstPollErr  error
	openCalls     int
	closeCalls    int
	rangesErr     error
	openErr       error
	pollErr       error
	pollErrAfter  int
	gate          <-chan struct{}
	opened        chan struct{}
	rangesGate    <-chan struct{}
	rangesStarted chan struct{}
}

func newAnalysisReader() *analysisReader {
	return &analysisReader{
		ranges:       make(map[int32]domaincluster.OffsetRange),
		opened:       make(chan struct{}, 1),
		pollErrAfter: -1,
	}
}

func (r *analysisReader) PartitionRanges(ctx context.Context, _ domaincluster.Definition, _ string, _ []int32) (map[int32]domaincluster.OffsetRange, error) {
	if r.rangesStarted != nil {
		select {
		case r.rangesStarted <- struct{}{}:
		default:
		}
	}
	if r.rangesGate != nil {
		select {
		case <-r.rangesGate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if r.rangesErr != nil {
		return nil, r.rangesErr
	}
	out := make(map[int32]domaincluster.OffsetRange, len(r.ranges))
	for p, v := range r.ranges {
		out[p] = v
	}
	return out, nil
}

func (r *analysisReader) OffsetsForTimestamp(context.Context, domaincluster.Definition, string, []int32, int64) (map[int32]int64, error) {
	return nil, nil
}

func (r *analysisReader) Open(_ context.Context, _ domaincluster.Definition, _ string, starts map[int32]int64) (domaincluster.ReaderSession, error) {
	r.mu.Lock()
	r.openCalls++
	r.mu.Unlock()
	if r.openErr != nil {
		return nil, r.openErr
	}
	ordered := append([]domaincluster.RawRecord(nil), r.records...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Partition != ordered[j].Partition {
			return ordered[i].Partition < ordered[j].Partition
		}
		return ordered[i].Offset < ordered[j].Offset
	})
	current := make(map[int32]int64, len(starts))
	for p, start := range starts {
		current[p] = start
	}
	select {
	case r.opened <- struct{}{}:
	default:
	}
	return &analysisSession{reader: r, records: ordered, current: current}, nil
}

type analysisSession struct {
	reader  *analysisReader
	records []domaincluster.RawRecord
	current map[int32]int64
	polls   int
	closed  bool
}

func (s *analysisSession) Poll(ctx context.Context) ([]domaincluster.RawRecord, error) {
	if s.polls == 0 && s.reader.gate != nil {
		select {
		case <-s.reader.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.reader.pollErrAfter >= 0 && s.polls >= s.reader.pollErrAfter {
		return nil, s.reader.pollErr
	}
	s.polls++
	if s.polls == 1 && s.reader.firstPoll != nil {
		return append([]domaincluster.RawRecord(nil), s.reader.firstPoll...), s.reader.firstPollErr
	}
	var out []domaincluster.RawRecord
	for _, record := range s.records {
		if start, ok := s.current[record.Partition]; !ok || record.Offset < start {
			continue
		}
		out = append(out, record)
		s.current[record.Partition] = record.Offset + 1
	}
	return out, nil
}

func (s *analysisSession) Close() {
	if s.closed {
		return
	}
	s.closed = true
	s.reader.mu.Lock()
	s.reader.closeCalls++
	s.reader.mu.Unlock()
}

func analysisDefinition() domaincluster.Definition {
	return domaincluster.Definition{Name: "c1"}
}

func newAnalysisServiceForTest(t *testing.T, reader *analysisReader) (*AnalysisService, context.CancelFunc) {
	t.Helper()
	root, cancel := context.WithCancel(context.Background())
	resolver := NewResolver([]domaincluster.Definition{analysisDefinition()})
	return NewAnalysisService(root, resolver, reader), cancel
}

func waitForAnalysisResult(t *testing.T, service *AnalysisService) AnalysisResult {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		view, found, err := service.Get("c1", "orders")
		if err == nil && found && view.Result != nil {
			return *view.Result
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("analysis did not reach a terminal result")
	return AnalysisResult{}
}

func TestAnalysisServiceRejectsUnknownCluster(t *testing.T) {
	service, cancel := newAnalysisServiceForTest(t, newAnalysisReader())
	defer cancel()
	ctx := context.Background()
	require.ErrorIs(t, service.Analyze(ctx, "missing", "orders"), ErrUnknownCluster)
	_, _, err := service.Get("missing", "orders")
	require.ErrorIs(t, err, ErrUnknownCluster)
	require.ErrorIs(t, service.Cancel(ctx, "missing", "orders"), ErrUnknownCluster)
}

func TestAnalysisServiceRejectsUnknownTopicWithoutOpening(t *testing.T) {
	reader := newAnalysisReader()
	service, cancel := newAnalysisServiceForTest(t, reader)
	defer cancel()
	require.ErrorIs(t, service.Analyze(context.Background(), "c1", "orders"), ErrAnalysisTopicNotFound)
	require.Equal(t, 0, reader.openCalls)
}

func TestAnalysisServiceReturnsRangeErrorBeforeRegistering(t *testing.T) {
	reader := newAnalysisReader()
	reader.rangesErr = errors.New("range failed")
	service, cancel := newAnalysisServiceForTest(t, reader)
	defer cancel()
	require.EqualError(t, service.Analyze(context.Background(), "c1", "orders"), "range failed")
	_, found, err := service.Get("c1", "orders")
	require.NoError(t, err)
	require.False(t, found)
}

// TestAnalysisServiceCancelDoesNotWaitForRangeLookup guards the lifecycle
// contract that DELETE removes an analysis immediately. The initial
// PartitionRanges call may be slow, but it must not hold the per-topic lock
// while waiting for the broker.
func TestAnalysisServiceCancelDoesNotWaitForRangeLookup(t *testing.T) {
	reader := newAnalysisReader()
	reader.ranges[0] = domaincluster.OffsetRange{Start: 0, End: 1}
	rangesGate := make(chan struct{})
	reader.rangesGate = rangesGate
	reader.rangesStarted = make(chan struct{}, 1)
	service, cancelRoot := newAnalysisServiceForTest(t, reader)
	defer cancelRoot()

	analyzeDone := make(chan error, 1)
	go func() { analyzeDone <- service.Analyze(context.Background(), "c1", "orders") }()
	select {
	case <-reader.rangesStarted:
	case <-time.After(time.Second):
		t.Fatal("range lookup did not start")
	}

	cancelDone := make(chan error, 1)
	go func() { cancelDone <- service.Cancel(context.Background(), "c1", "orders") }()
	select {
	case err := <-cancelDone:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Cancel waited for the in-flight range lookup")
	}

	select {
	case err := <-analyzeDone:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Cancel did not abort the in-flight range lookup")
	}
	_, found, err := service.Get("c1", "orders")
	require.NoError(t, err)
	require.False(t, found)
}

func TestAnalysisServiceRootCancelDuringRangeLookupDoesNotRegisterRun(t *testing.T) {
	reader := newAnalysisReader()
	rangesGate := make(chan struct{})
	reader.rangesGate = rangesGate
	reader.rangesStarted = make(chan struct{}, 1)
	service, cancelRoot := newAnalysisServiceForTest(t, reader)

	analyzeDone := make(chan error, 1)
	go func() { analyzeDone <- service.Analyze(context.Background(), "c1", "orders") }()
	select {
	case <-reader.rangesStarted:
	case <-time.After(time.Second):
		t.Fatal("range lookup did not start")
	}
	cancelRoot()
	select {
	case err := <-analyzeDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("root cancellation did not abort the in-flight range lookup")
	}
	_, found, err := service.Get("c1", "orders")
	require.NoError(t, err)
	require.False(t, found)
}

func TestAnalysisServiceOpenErrorPublishesTerminalError(t *testing.T) {
	reader := newAnalysisReader()
	reader.ranges[0] = domaincluster.OffsetRange{Start: 0, End: 1}
	reader.openErr = errors.New("open failed")
	service, cancel := newAnalysisServiceForTest(t, reader)
	defer cancel()
	require.NoError(t, service.Analyze(context.Background(), "c1", "orders"))
	result := waitForAnalysisResult(t, service)
	require.EqualError(t, errors.New(result.Error), "open failed")
	require.Zero(t, result.TotalStats.TotalMsgs)
}

func TestAnalysisServiceActiveAnalyzeIsIdempotent(t *testing.T) {
	reader := newAnalysisReader()
	reader.ranges[0] = domaincluster.OffsetRange{Start: 0, End: 1}
	reader.records = []domaincluster.RawRecord{{Partition: 0, Offset: 0, TimestampMs: 1, Value: []byte("v"), ValueSize: 1}}
	gate := make(chan struct{})
	reader.gate = gate
	service, cancel := newAnalysisServiceForTest(t, reader)
	defer cancel()

	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- service.Analyze(context.Background(), "c1", "orders")
		}()
	}
	select {
	case <-reader.opened:
	case <-time.After(time.Second):
		t.Fatal("analysis did not open a reader")
	}
	wg.Wait()
	close(gate)
	for i := 0; i < 20; i++ {
		require.NoError(t, <-errs)
	}
	require.Equal(t, 1, reader.openCalls)
	_ = waitForAnalysisResult(t, service)
}

func TestAnalysisServicePublishesProgressAndResult(t *testing.T) {
	reader := newAnalysisReader()
	reader.ranges[0] = domaincluster.OffsetRange{Start: 0, End: 2}
	reader.records = []domaincluster.RawRecord{
		{Partition: 0, Offset: 0, TimestampMs: 1, Key: []byte("k"), Value: []byte("v"), KeySize: 1, ValueSize: 1, HeadersSize: 2},
		{Partition: 0, Offset: 1, TimestampMs: 2, Value: []byte("vv"), ValueSize: 2},
	}
	gate := make(chan struct{})
	reader.gate = gate
	service, cancel := newAnalysisServiceForTest(t, reader)
	defer cancel()
	require.NoError(t, service.Analyze(context.Background(), "c1", "orders"))
	select {
	case <-reader.opened:
	case <-time.After(time.Second):
		t.Fatal("analysis did not open a reader")
	}
	view, found, err := service.Get("c1", "orders")
	require.NoError(t, err)
	require.True(t, found)
	require.NotNil(t, view.Progress)
	require.Nil(t, view.Result)
	require.Equal(t, int64(0), view.Progress.MsgsScanned)
	require.Equal(t, float32(0), view.Progress.CompletenessPercent)
	close(gate)
	result := waitForAnalysisResult(t, service)
	require.Empty(t, result.Error)
	require.Equal(t, int64(2), result.TotalStats.TotalMsgs)
	require.Equal(t, int64(1), result.TotalStats.KeySize.Sum)
	require.Equal(t, int64(3), result.TotalStats.ValueSize.Sum)
	require.GreaterOrEqual(t, result.FinishedAt, result.StartedAt)
}

func TestAnalysisServiceGetDeepCopiesHourlyStats(t *testing.T) {
	reader := newAnalysisReader()
	reader.ranges[0] = domaincluster.OffsetRange{Start: 0, End: 1}
	reader.records = []domaincluster.RawRecord{{Partition: 0, Offset: 0, TimestampMs: 3_600_001}}
	service, cancel := newAnalysisServiceForTest(t, reader)
	defer cancel()
	require.NoError(t, service.Analyze(context.Background(), "c1", "orders"))
	_ = waitForAnalysisResult(t, service)

	first, found, err := service.Get("c1", "orders")
	require.NoError(t, err)
	require.True(t, found)
	require.NotNil(t, first.Result)
	first.Result.TotalStats.HourlyMsgCounts[0].Count = 99
	first.Result.PartitionStats[0].HourlyMsgCounts[0].Count = 99

	second, found, err := service.Get("c1", "orders")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(1), second.Result.TotalStats.HourlyMsgCounts[0].Count)
	require.Equal(t, int64(1), second.Result.PartitionStats[0].HourlyMsgCounts[0].Count)
}

func TestAnalysisServiceUsesRootContextAfterRequestCancellation(t *testing.T) {
	reader := newAnalysisReader()
	reader.ranges[0] = domaincluster.OffsetRange{Start: 0, End: 1}
	reader.records = []domaincluster.RawRecord{{Partition: 0, Offset: 0, TimestampMs: 1}}
	service, cancel := newAnalysisServiceForTest(t, reader)
	defer cancel()
	requestCtx, requestCancel := context.WithCancel(context.Background())
	require.NoError(t, service.Analyze(requestCtx, "c1", "orders"))
	requestCancel()
	result := waitForAnalysisResult(t, service)
	require.Empty(t, result.Error)
}

func TestAnalysisServiceRestartsCompletedRun(t *testing.T) {
	reader := newAnalysisReader()
	reader.ranges[0] = domaincluster.OffsetRange{Start: 0, End: 1}
	reader.records = []domaincluster.RawRecord{{Partition: 0, Offset: 0, TimestampMs: 1}}
	service, cancel := newAnalysisServiceForTest(t, reader)
	defer cancel()
	first := waitAfterAnalyze(t, service, reader)
	second := waitAfterAnalyze(t, service, reader)
	require.Equal(t, int64(1), first.TotalStats.TotalMsgs)
	require.Equal(t, int64(1), second.TotalStats.TotalMsgs)
	require.Equal(t, 2, reader.openCalls)
}

func waitAfterAnalyze(t *testing.T, service *AnalysisService, reader *analysisReader) AnalysisResult {
	t.Helper()
	require.NoError(t, service.Analyze(context.Background(), "c1", "orders"))
	return waitForAnalysisResult(t, service)
}

func TestAnalysisServicePollErrorPublishesPartialResult(t *testing.T) {
	reader := newAnalysisReader()
	reader.ranges[0] = domaincluster.OffsetRange{Start: 0, End: 2}
	reader.records = []domaincluster.RawRecord{{Partition: 0, Offset: 0, TimestampMs: 1}}
	reader.pollErr = errors.New("poll failed")
	reader.pollErrAfter = 1
	service, cancel := newAnalysisServiceForTest(t, reader)
	defer cancel()
	require.NoError(t, service.Analyze(context.Background(), "c1", "orders"))
	result := waitForAnalysisResult(t, service)
	require.EqualError(t, errors.New(result.Error), "poll failed")
	require.Equal(t, int64(1), result.TotalStats.TotalMsgs)
}

func TestAnalysisServiceCancelRemovesStateAndUnblocksReader(t *testing.T) {
	reader := newAnalysisReader()
	reader.ranges[0] = domaincluster.OffsetRange{Start: 0, End: 1}
	reader.records = []domaincluster.RawRecord{{Partition: 0, Offset: 0, TimestampMs: 1}}
	gate := make(chan struct{})
	reader.gate = gate
	service, cancel := newAnalysisServiceForTest(t, reader)
	defer cancel()
	require.NoError(t, service.Analyze(context.Background(), "c1", "orders"))
	select {
	case <-reader.opened:
	case <-time.After(time.Second):
		t.Fatal("analysis did not open a reader")
	}
	require.NoError(t, service.Cancel(context.Background(), "c1", "orders"))
	_, found, err := service.Get("c1", "orders")
	require.NoError(t, err)
	require.False(t, found)
	require.NoError(t, service.Cancel(context.Background(), "c1", "orders"))
	close(gate)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		reader.mu.Lock()
		closed := reader.closeCalls
		reader.mu.Unlock()
		if closed > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("reader was not closed after cancellation")
}

func TestAnalysisServiceBoundsScanToInitialRange(t *testing.T) {
	reader := newAnalysisReader()
	reader.ranges[0] = domaincluster.OffsetRange{Start: 0, End: 1}
	reader.records = []domaincluster.RawRecord{
		{Partition: 0, Offset: 0, TimestampMs: 1},
		{Partition: 0, Offset: 1, TimestampMs: 2},
	}
	service, cancel := newAnalysisServiceForTest(t, reader)
	defer cancel()
	require.NoError(t, service.Analyze(context.Background(), "c1", "orders"))
	result := waitForAnalysisResult(t, service)
	require.Equal(t, int64(1), result.TotalStats.TotalMsgs)
	require.GreaterOrEqual(t, result.FinishedAt, result.StartedAt)
}

func TestAnalysisServiceSkipsControlRecords(t *testing.T) {
	reader := newAnalysisReader()
	reader.ranges[0] = domaincluster.OffsetRange{Start: 0, End: 2}
	reader.records = []domaincluster.RawRecord{
		{Partition: 0, Offset: 0, Control: true},
		{Partition: 0, Offset: 1, TimestampMs: 1, Value: []byte("v"), ValueSize: 1},
	}
	service, cancel := newAnalysisServiceForTest(t, reader)
	defer cancel()

	require.NoError(t, service.Analyze(context.Background(), "c1", "orders"))
	result := waitForAnalysisResult(t, service)
	require.Empty(t, result.Error)
	require.Equal(t, int64(1), result.TotalStats.TotalMsgs)
}

func TestAnalysisServiceKeepsRecordsWhenPollAlsoReturnsError(t *testing.T) {
	reader := newAnalysisReader()
	reader.ranges[0] = domaincluster.OffsetRange{Start: 0, End: 1}
	reader.firstPoll = []domaincluster.RawRecord{{Partition: 0, Offset: 0, TimestampMs: 1}}
	reader.firstPollErr = errors.New("partition failed")
	service, cancel := newAnalysisServiceForTest(t, reader)
	defer cancel()

	require.NoError(t, service.Analyze(context.Background(), "c1", "orders"))
	result := waitForAnalysisResult(t, service)
	require.EqualError(t, errors.New(result.Error), "partition failed")
	require.Equal(t, int64(1), result.TotalStats.TotalMsgs)
}

func TestAnalysisServiceTerminatesAfterNoProgress(t *testing.T) {
	reader := newAnalysisReader()
	reader.ranges[0] = domaincluster.OffsetRange{Start: 0, End: 2}
	reader.records = []domaincluster.RawRecord{{Partition: 0, Offset: 0, TimestampMs: 1}}
	service, cancel := newAnalysisServiceForTest(t, reader)
	defer cancel()
	service.noProgressTimeout = 10 * time.Millisecond

	require.NoError(t, service.Analyze(context.Background(), "c1", "orders"))
	result := waitForAnalysisResult(t, service)
	require.Contains(t, result.Error, "no progress")
	require.Equal(t, int64(1), result.TotalStats.TotalMsgs)
}

var _ domaincluster.MessageReaderPort = (*analysisReader)(nil)
