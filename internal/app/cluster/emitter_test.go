package cluster

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// This file is package cluster (white-box), not cluster_test: emitForward/
// emitBackward/emitTailing/emitSpec/throttle/newThrottle are unexported
// (Task 8's MessageService, living in this same package, is their only
// production caller) -- same "internal function needs a same-package test"
// rationale as topic_internal_test.go/group_internal_test.go.

var errEmitBoom = errors.New("emit boom")

// --- fake MessageReaderPort ---

// fakeReader is a deterministic, in-memory cluster.MessageReaderPort double:
// preset per-partition offset ranges and record fixtures, served through
// fakeSession in ascending-offset batches of at most `batch` records. No
// Docker/real cluster involved anywhere in this file.
type fakeReader struct {
	mu      sync.Mutex
	ranges  map[int32]cluster.OffsetRange
	records map[int32][]cluster.RawRecord // ascending by offset

	batch int // max records returned per Poll call, across all of a session's partitions

	rangesErr error
	openErr   error

	// openErrByPartition/pollErrByPartition inject a failure whenever the
	// corresponding partition is included in a session. This supports both
	// single-partition fixtures and emitBackward's multi-partition session
	// without changing the blanket openErr/pollErr used by other emitters.
	openErrByPartition      map[int32]error
	pollErrByPartition      map[int32]error
	pollBatchByPartition    map[int32][]cluster.RawRecord
	pollBatchErrByPartition map[int32]error

	openCalls  int
	lastStarts map[int32]int64 // defensive copy of the most recent Open call's starts
	closeCalls int
}

func newFakeReader(batch int) *fakeReader {
	if batch <= 0 {
		batch = 2
	}
	return &fakeReader{
		ranges:  map[int32]cluster.OffsetRange{},
		records: map[int32][]cluster.RawRecord{},
		batch:   batch,
	}
}

func (f *fakeReader) withRange(partition int32, start, end int64) *fakeReader {
	f.ranges[partition] = cluster.OffsetRange{Start: start, End: end}
	return f
}

func (f *fakeReader) withRecords(partition int32, recs ...cluster.RawRecord) *fakeReader {
	f.records[partition] = append([]cluster.RawRecord(nil), recs...)
	return f
}

// withOpenErrForPartition makes Open fail with err whenever the partition
// requested (as one of possibly several keys in Open's starts map) is p.
func (f *fakeReader) withOpenErrForPartition(p int32, err error) *fakeReader {
	if f.openErrByPartition == nil {
		f.openErrByPartition = map[int32]error{}
	}
	f.openErrByPartition[p] = err
	return f
}

// withPollErrForPartition makes the session returned by Open fail every
// Poll with err whenever partition p is among that session's partitions.
func (f *fakeReader) withPollErrForPartition(p int32, err error) *fakeReader {
	if f.pollErrByPartition == nil {
		f.pollErrByPartition = map[int32]error{}
	}
	f.pollErrByPartition[p] = err
	return f
}

// withPollBatchErrForPartition makes the first Poll for partition p return
// both the supplied records and the supplied error. This is the regression
// shape exposed by a real franz-go fetch when one partition has healthy
// records alongside another partition's fatal error.
func (f *fakeReader) withPollBatchErrForPartition(p int32, batch []cluster.RawRecord, err error) *fakeReader {
	if f.pollBatchByPartition == nil {
		f.pollBatchByPartition = map[int32][]cluster.RawRecord{}
	}
	if f.pollBatchErrByPartition == nil {
		f.pollBatchErrByPartition = map[int32]error{}
	}
	f.pollBatchByPartition[p] = append([]cluster.RawRecord(nil), batch...)
	f.pollBatchErrByPartition[p] = err
	return f
}

func (f *fakeReader) PartitionRanges(_ context.Context, _ cluster.Definition, _ string, partitions []int32) (map[int32]cluster.OffsetRange, error) {
	if f.rangesErr != nil {
		return nil, f.rangesErr
	}
	if len(partitions) == 0 {
		out := make(map[int32]cluster.OffsetRange, len(f.ranges))
		for p, r := range f.ranges {
			out[p] = r
		}
		return out, nil
	}
	out := make(map[int32]cluster.OffsetRange, len(partitions))
	for _, p := range partitions {
		out[p] = f.ranges[p]
	}
	return out, nil
}

// OffsetsForTimestamp is unused by the Task 7 emitters -- present only to
// satisfy cluster.MessageReaderPort.
func (f *fakeReader) OffsetsForTimestamp(context.Context, cluster.Definition, string, []int32, int64) (map[int32]int64, error) {
	return nil, nil
}

func (f *fakeReader) Open(_ context.Context, _ cluster.Definition, _ string, starts map[int32]int64) (cluster.ReaderSession, error) {
	f.mu.Lock()
	f.openCalls++
	cp := make(map[int32]int64, len(starts))
	for p, o := range starts {
		cp[p] = o
	}
	f.lastStarts = cp
	f.mu.Unlock()

	if f.openErr != nil {
		return nil, f.openErr
	}
	for p := range starts {
		if err, ok := f.openErrByPartition[p]; ok && err != nil {
			return nil, err
		}
	}

	order := make([]int32, 0, len(starts))
	for p := range starts {
		order = append(order, p)
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })

	cur := make(map[int32]int64, len(starts))
	for p, o := range starts {
		cur[p] = o
	}

	var pollErr error
	var pollBatch []cluster.RawRecord
	var pollBatchErr error
	for _, p := range order {
		if err, ok := f.pollErrByPartition[p]; ok && err != nil {
			pollErr = err
			break
		}
		if batch, ok := f.pollBatchByPartition[p]; ok {
			pollBatch = append([]cluster.RawRecord(nil), batch...)
			pollBatchErr = f.pollBatchErrByPartition[p]
			break
		}
	}

	return &fakeSession{reader: f, order: order, cur: cur, pollErr: pollErr, firstBatch: pollBatch, firstErr: pollBatchErr}, nil
}

// fakeSession is fakeReader.Open's cluster.ReaderSession: serves records for
// its seeked partitions in ascending-offset batches of at most
// reader.batch, and reports (nil, nil) once every partition is drained --
// the exact "this round produced no records" contract ReaderSession.Poll's
// domain doc comment describes.
type fakeSession struct {
	reader     *fakeReader
	order      []int32
	cur        map[int32]int64
	pollErr    error
	firstBatch []cluster.RawRecord
	firstErr   error
	firstDone  bool
	closed     bool
}

func (s *fakeSession) Poll(ctx context.Context) ([]cluster.RawRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.pollErr != nil {
		return nil, s.pollErr
	}
	if !s.firstDone && (len(s.firstBatch) > 0 || s.firstErr != nil) {
		s.firstDone = true
		return append([]cluster.RawRecord(nil), s.firstBatch...), s.firstErr
	}
	var out []cluster.RawRecord
outer:
	for _, p := range s.order {
		for _, rec := range s.reader.records[p] {
			if rec.Offset < s.cur[p] {
				continue
			}
			if len(out) >= s.reader.batch {
				break outer
			}
			out = append(out, rec)
			s.cur[p] = rec.Offset + 1
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func (s *fakeSession) Close() {
	s.closed = true
	s.reader.mu.Lock()
	s.reader.closeCalls++
	s.reader.mu.Unlock()
}

// rr builds one fixture RawRecord: byteSize is split evenly across
// KeySize/ValueSize purely so tests have a nonzero, predictable byte
// footprint to reason about if they want it; the five emitter tests below
// don't assert on it (they run with newThrottle(0), which never blocks).
func rr(partition int32, offset, ts int64) cluster.RawRecord {
	return cluster.RawRecord{
		Partition:   partition,
		Offset:      offset,
		TimestampMs: ts,
		Key:         []byte("k"),
		Value:       []byte("v"),
		KeySize:     1,
		ValueSize:   1,
	}
}

// --- ① emitForward ---

func TestEmitForwardStopsAtLimitAndReportsNextCursor(t *testing.T) {
	r := newFakeReader(2).
		withRange(0, 0, 5).
		withRecords(0, rr(0, 0, 10), rr(0, 1, 20), rr(0, 2, 30), rr(0, 3, 40), rr(0, 4, 50))

	var got []int64
	spec := emitSpec{limit: 3, starts: map[int32]int64{0: 0}}
	next, err := emitForward(context.Background(), r, cluster.Definition{}, "t", spec, newThrottle(0), func(rec cluster.RawRecord) error {
		got = append(got, rec.Offset)
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2}, got)
	require.Equal(t, map[int32]int64{0: 3}, next)
}

// TestEmitForwardSinksBatchBeforePollError locks the shared ReaderSession
// contract on the forward path: a healthy record in a batch that also carries
// an error must advance the cursor and reach the sink before the error is
// returned.
func TestEmitForwardSinksBatchBeforePollError(t *testing.T) {
	r := newFakeReader(4).
		withRange(0, 0, 3).
		withPollBatchErrForPartition(0, []cluster.RawRecord{rr(0, 0, 10)}, errEmitBoom)

	var got []int64
	next, err := emitForward(context.Background(), r, cluster.Definition{}, "t", emitSpec{starts: map[int32]int64{0: 0}}, newThrottle(0), func(rec cluster.RawRecord) error {
		got = append(got, rec.Offset)
		return nil
	})

	require.ErrorIs(t, err, errEmitBoom)
	require.Equal(t, []int64{0}, got)
	require.Equal(t, map[int32]int64{0: 1}, next)
}

// --- ② emitBackward budgets and session count ---

func TestBackwardBudgetsStayWithinLimitAndRedistributeEmptyCapacity(t *testing.T) {
	partitions := []int32{0, 1, 2}
	ranges := map[int32]cluster.OffsetRange{
		0: {Start: 0, End: 0},
		1: {Start: 0, End: 10},
		2: {Start: 0, End: 10},
	}
	ends := map[int32]int64{0: 0, 1: 10, 2: 10}

	got := backwardBudgets(partitions, ranges, ends, 5)

	require.Equal(t, map[int32]int64{0: 0, 1: 3, 2: 2}, got)
	var total int64
	for _, budget := range got {
		total += budget
	}
	require.LessOrEqual(t, total, int64(5))
}

func TestEmitBackwardUsesOneSessionAndTotalPageBudget(t *testing.T) {
	r := newFakeReader(10).
		withRange(0, 0, 10).
		withRange(1, 0, 10).
		withRecords(0, rr(0, 7, 70), rr(0, 8, 80), rr(0, 9, 90)).
		withRecords(1, rr(1, 8, 85), rr(1, 9, 95))

	var got []cluster.RawRecord
	_, err := emitBackward(
		context.Background(), r, cluster.Definition{}, "t",
		emitSpec{limit: 5}, newThrottle(0),
		func(rec cluster.RawRecord) error {
			got = append(got, rec)
			return nil
		},
	)

	require.NoError(t, err)
	require.Equal(t, 1, r.openCalls)
	require.Equal(t, 1, r.closeCalls)
	require.Equal(t, map[int32]int64{0: 7, 1: 8}, r.lastStarts)
	require.Len(t, got, 5)
}

// --- ③ emitBackward (single partition) ---

func TestEmitBackwardReturnsLastLimitRecordsDescending(t *testing.T) {
	r := newFakeReader(2).
		withRange(0, 0, 5).
		withRecords(0, rr(0, 0, 10), rr(0, 1, 20), rr(0, 2, 30), rr(0, 3, 40), rr(0, 4, 50))

	var got []int64
	spec := emitSpec{limit: 2, ends: map[int32]int64{0: 5}}
	_, err := emitBackward(context.Background(), r, cluster.Definition{}, "t", spec, newThrottle(0), func(rec cluster.RawRecord) error {
		got = append(got, rec.Offset)
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, []int64{4, 3}, got)
}

// --- ④ emitBackward (multi-partition merge) ---

// TestEmitBackwardMergesAcrossPartitionsDescendingByTimestamp is the soul
// test for backward's cross-partition merge: records from the shared session
// are sorted descending by (TimestampMs, Partition, Offset) and truncated to
// exactly limit -- proving the truncation is a genuine cross-partition merge.
func TestEmitBackwardMergesAcrossPartitionsDescendingByTimestamp(t *testing.T) {
	r := newFakeReader(4).
		withRange(0, 0, 2).
		withRange(1, 0, 1).
		withRecords(0, rr(0, 0, 10), rr(0, 1, 20)).
		withRecords(1, rr(1, 0, 15))

	type seen struct {
		partition int32
		ts        int64
	}
	var got []seen
	spec := emitSpec{limit: 2, ends: map[int32]int64{0: 2, 1: 1}}
	_, err := emitBackward(context.Background(), r, cluster.Definition{}, "t", spec, newThrottle(0), func(rec cluster.RawRecord) error {
		got = append(got, seen{partition: rec.Partition, ts: rec.TimestampMs})
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, []seen{{partition: 0, ts: 20}, {partition: 1, ts: 15}}, got, "ts=10 must be truncated away by the cross-partition limit")
}

// --- backward out-of-range filter ---

// TestEmitBackwardFiltersOutOffsetsAtOrAboveEnd is the guard for a *second*
// backward page: ends[p] here (3) is deliberately below the partition's true
// high-water mark (range End=5) -- exactly the shape a caller gets by
// re-using a prior page's returned lower cursor as the next page's end. The
// fake session happily serves offsets 3 and 4 in the same poll batch (it
// knows nothing about "end"), so fetchBackwardPartition's `rec.Offset >= end`
// filter is the only thing keeping them out. This pins the `>=` (not `>`)
// boundary: offset 3 == end must be excluded, not just offset 4 > end.
func TestEmitBackwardFiltersOutOffsetsAtOrAboveEnd(t *testing.T) {
	r := newFakeReader(10). // batch big enough that one poll returns all 5 records
				withRange(0, 0, 5).
				withRecords(0, rr(0, 0, 10), rr(0, 1, 20), rr(0, 2, 30), rr(0, 3, 40), rr(0, 4, 50))

	var got []int64
	spec := emitSpec{limit: 10, ends: map[int32]int64{0: 3}}
	_, err := emitBackward(context.Background(), r, cluster.Definition{}, "t", spec, newThrottle(0), func(rec cluster.RawRecord) error {
		got = append(got, rec.Offset)
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, []int64{2, 1, 0}, got, "offsets >= end(3) must be filtered out, remainder in descending order")
}

// --- backward same-timestamp cross-partition tie-break ---

// TestEmitBackwardTieBreaksByPartitionThenOffsetDescending exercises the
// sort.Slice comparator's second and third clauses, which every other
// backward fixture leaves dead code because they all use distinct
// TimestampMs values: with every record sharing TimestampMs=100, ordering
// can only come from "Partition desc, then Offset desc". Repeated a few
// times to demonstrate the merge is deterministic, not an accidental input
// or map iteration ordering.
func TestEmitBackwardTieBreaksByPartitionThenOffsetDescending(t *testing.T) {
	type pOff struct {
		partition int32
		offset    int64
	}
	want := []pOff{{1, 1}, {1, 0}, {0, 1}, {0, 0}}

	for i := 0; i < 3; i++ {
		r := newFakeReader(8).
			withRange(0, 0, 2).
			withRange(1, 0, 2).
			withRecords(0, rr(0, 0, 100), rr(0, 1, 100)).
			withRecords(1, rr(1, 0, 100), rr(1, 1, 100))

		var got []pOff
		spec := emitSpec{limit: 4, ends: map[int32]int64{0: 2, 1: 2}}
		_, err := emitBackward(context.Background(), r, cluster.Definition{}, "t", spec, newThrottle(0), func(rec cluster.RawRecord) error {
			got = append(got, pOff{rec.Partition, rec.Offset})
			return nil
		})

		require.NoError(t, err)
		require.Equal(t, want, got, "iteration %d: equal TimestampMs must tie-break Partition desc then Offset desc", i)
	}
}

// --- backward error propagation ---

// TestEmitBackwardPropagatesOpenErrorNoRecords verifies that a failure while
// opening the one all-partition session emits nothing and has no session to
// close.
func TestEmitBackwardPropagatesOpenErrorNoRecords(t *testing.T) {
	r := newFakeReader(4).
		withRange(0, 0, 5).
		withRange(1, 0, 3).
		withRecords(0, rr(0, 0, 10), rr(0, 1, 20), rr(0, 2, 30), rr(0, 3, 40), rr(0, 4, 50)).
		withRecords(1, rr(1, 0, 15), rr(1, 1, 25)).
		withOpenErrForPartition(0, errEmitBoom)

	var got []cluster.RawRecord
	spec := emitSpec{limit: 2, ends: map[int32]int64{0: 5, 1: 3}}
	next, err := emitBackward(context.Background(), r, cluster.Definition{}, "t", spec, newThrottle(0), func(rec cluster.RawRecord) error {
		got = append(got, rec)
		return nil
	})

	require.ErrorIs(t, err, errEmitBoom)
	require.Nil(t, next)
	require.Empty(t, got, "no records must be sunk when the shared Open fails")
	require.Equal(t, 1, r.openCalls)
	require.Equal(t, 0, r.closeCalls, "a failed Open returns no session to close")
	require.Equal(t, map[int32]int64{0: 4, 1: 2}, r.lastStarts)
}

// TestEmitBackwardPropagatesPollErrorNoRecordsAndClosesSession fails Poll
// after the one all-partition Open succeeds, so that session must be closed.
func TestEmitBackwardPropagatesPollErrorNoRecordsAndClosesSession(t *testing.T) {
	r := newFakeReader(4).
		withRange(0, 0, 5).
		withRange(1, 0, 3).
		withRecords(0, rr(0, 0, 10), rr(0, 1, 20), rr(0, 2, 30), rr(0, 3, 40), rr(0, 4, 50)).
		withRecords(1, rr(1, 0, 15), rr(1, 1, 25)).
		withPollErrForPartition(0, errEmitBoom)

	var got []cluster.RawRecord
	spec := emitSpec{limit: 2, ends: map[int32]int64{0: 5, 1: 3}}
	next, err := emitBackward(context.Background(), r, cluster.Definition{}, "t", spec, newThrottle(0), func(rec cluster.RawRecord) error {
		got = append(got, rec)
		return nil
	})

	require.ErrorIs(t, err, errEmitBoom)
	require.Nil(t, next)
	require.Empty(t, got)
	require.Equal(t, 1, r.openCalls)
	require.Equal(t, 1, r.closeCalls)
}

// TestEmitBackwardSinksBatchBeforePollError verifies the ReaderSession
// contract for the backward path: records returned in the same Poll batch as
// a partition error are merged and delivered before emitBackward returns the
// error. Without the partial-result plumbing this data was silently dropped.
func TestEmitBackwardSinksBatchBeforePollError(t *testing.T) {
	r := newFakeReader(4).
		withRange(0, 0, 3).
		withPollBatchErrForPartition(0, []cluster.RawRecord{rr(0, 1, 10)}, errEmitBoom)

	var got []int64
	spec := emitSpec{limit: 2, ends: map[int32]int64{0: 3}}
	next, err := emitBackward(context.Background(), r, cluster.Definition{}, "t", spec, newThrottle(0), func(rec cluster.RawRecord) error {
		got = append(got, rec.Offset)
		return nil
	})

	require.ErrorIs(t, err, errEmitBoom)
	require.Equal(t, []int64{1}, got)
	require.Equal(t, map[int32]int64{0: 1}, next)
}

// TestEmitBackwardPartialBatchKeepsStartsForAllOpenedPartitions verifies that
// a shared session's partial batch is delivered before its Poll error and the
// cursor retains the start for every partition that was successfully opened.
func TestEmitBackwardPartialBatchKeepsStartsForAllOpenedPartitions(t *testing.T) {
	r := newFakeReader(4).
		withRange(0, 0, 3).
		withRange(1, 0, 3).
		withPollBatchErrForPartition(1, []cluster.RawRecord{rr(1, 1, 10)}, errEmitBoom)

	var got []int64
	next, err := emitBackward(context.Background(), r, cluster.Definition{}, "t", emitSpec{limit: 4, ends: map[int32]int64{0: 3, 1: 3}}, newThrottle(0), func(rec cluster.RawRecord) error {
		got = append(got, rec.Offset)
		return nil
	})

	require.ErrorIs(t, err, errEmitBoom)
	require.Equal(t, []int64{1}, got)
	require.Equal(t, map[int32]int64{0: 1, 1: 1}, next)
	require.Equal(t, 1, r.openCalls)
	require.Equal(t, 1, r.closeCalls)
}

// TestEmitBackwardReturnsFirstErrorAmongMultiplePartitions pins fakeReader's
// deterministic ascending-partition selection when multiple members of the
// shared session are configured with Poll errors.
func TestEmitBackwardReturnsFirstErrorAmongMultiplePartitions(t *testing.T) {
	errEmitBoom2 := errors.New("emit boom 2")

	for i := 0; i < 3; i++ {
		r := newFakeReader(4).
			withRange(0, 0, 5).
			withRange(1, 0, 3).
			withRecords(0, rr(0, 0, 10), rr(0, 1, 20)).
			withRecords(1, rr(1, 0, 15), rr(1, 1, 25)).
			withPollErrForPartition(0, errEmitBoom).
			withPollErrForPartition(1, errEmitBoom2)

		spec := emitSpec{limit: 2, ends: map[int32]int64{0: 5, 1: 3}}
		_, err := emitBackward(context.Background(), r, cluster.Definition{}, "t", spec, newThrottle(0), func(cluster.RawRecord) error {
			return nil
		})

		require.ErrorIs(t, err, errEmitBoom, "iteration %d: partition 0 (lowest index) must win deterministically", i)
		require.NotErrorIs(t, err, errEmitBoom2)
		require.Equal(t, 1, r.openCalls)
		require.Equal(t, 1, r.closeCalls)
	}
}

// --- ④ emitTailing ---

// TestEmitTailingReturnsNilOnContextCancel locks the "no dead loop" contract:
// emitTailing must return (nil) promptly once ctx is cancelled, having
// already delivered whatever it saw before that.
func TestEmitTailingReturnsNilOnContextCancel(t *testing.T) {
	recs := make([]cluster.RawRecord, 0, 50)
	for i := int64(0); i < 50; i++ {
		recs = append(recs, rr(0, i, i*10))
	}
	// emitTailing now always consults PartitionRanges (finding 4's clamp
	// fix), so this fixture needs a range even though spec.starts is
	// non-empty and within it -- previously PartitionRanges was skipped
	// whenever spec.starts was already populated.
	r := newFakeReader(2).withRange(0, 0, 50).withRecords(0, recs...)

	seen := make(chan cluster.RawRecord, 64)
	ctx, cancel := context.WithCancel(context.Background())
	spec := emitSpec{starts: map[int32]int64{0: 0}}

	errCh := make(chan error, 1)
	go func() {
		errCh <- emitTailing(ctx, r, cluster.Definition{}, "t", spec, newThrottle(0), func(rec cluster.RawRecord) error {
			seen <- rec
			return nil
		})
	}()

	for i := 0; i < 3; i++ {
		select {
		case <-seen:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for emitTailing to emit records")
		}
	}
	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("emitTailing did not return after context cancellation")
	}
}

// TestEmitTailingSinksBatchBeforePollError is the live-stream counterpart to
// the forward/backward regressions above. A non-cancellation Poll error still
// must not erase records already present in that same batch.
func TestEmitTailingSinksBatchBeforePollError(t *testing.T) {
	r := newFakeReader(4).
		withRange(0, 0, 3).
		withPollBatchErrForPartition(0, []cluster.RawRecord{rr(0, 0, 10)}, errEmitBoom)

	var got []int64
	err := emitTailing(context.Background(), r, cluster.Definition{}, "t", emitSpec{starts: map[int32]int64{0: 0}}, newThrottle(0), func(rec cluster.RawRecord) error {
		got = append(got, rec.Offset)
		return nil
	})

	require.ErrorIs(t, err, errEmitBoom)
	require.Equal(t, []int64{0}, got)
}

// TestEmitTailingDefaultsStartsToPartitionEnd locks the "starts 缺省=各分区
// End" rule: an empty spec.starts must resolve (via PartitionRanges) to each
// target partition's current high-water mark, not offset 0 -- otherwise a
// fresh tail would replay the whole partition instead of only new messages.
func TestEmitTailingDefaultsStartsToPartitionEnd(t *testing.T) {
	r := newFakeReader(2).
		withRange(0, 0, 5).
		withRecords(0, rr(0, 0, 10), rr(0, 1, 20), rr(0, 2, 30), rr(0, 3, 40), rr(0, 4, 50), rr(0, 5, 60))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := make(chan int64, 8)
	errCh := make(chan error, 1)
	go func() {
		errCh <- emitTailing(ctx, r, cluster.Definition{}, "t", emitSpec{}, newThrottle(0), func(rec cluster.RawRecord) error {
			seen <- rec.Offset
			return nil
		})
	}()

	select {
	case off := <-seen:
		require.Equal(t, int64(5), off, "must start from the high-water mark (End=5), skipping the 5 pre-existing records")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for emitTailing to emit the tail record")
	}
	cancel()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("emitTailing did not return after context cancellation")
	}
}

// --- ⑤ out-of-range start clamp ---

func TestEmitForwardClampsOutOfRangeStartWithoutPanicOrOverread(t *testing.T) {
	r := newFakeReader(2).
		withRange(0, 0, 5).
		withRecords(0, rr(0, 0, 10), rr(0, 1, 20), rr(0, 2, 30), rr(0, 3, 40), rr(0, 4, 50))

	var got []cluster.RawRecord
	spec := emitSpec{limit: 3, starts: map[int32]int64{0: 99}}
	next, err := emitForward(context.Background(), r, cluster.Definition{}, "t", spec, newThrottle(0), func(rec cluster.RawRecord) error {
		got = append(got, rec)
		return nil
	})

	require.NoError(t, err)
	require.Empty(t, got)
	require.Equal(t, map[int32]int64{0: 5}, next)
	require.Equal(t, map[int32]int64{0: 5}, r.lastStarts, "Open must receive the clamped start, never the raw out-of-range 99")
}

// TestEmitTailingClampsOutOfRangeCallerSuppliedStart locks finding 4's fix:
// a caller-supplied spec.starts entry past the partition's End must be
// clamped before Open, exactly like forward's clamp above -- previously
// emitTailing only clamped (via defaulting to End) when spec.starts was
// empty, and passed a non-empty caller start straight through unclamped.
func TestEmitTailingClampsOutOfRangeCallerSuppliedStart(t *testing.T) {
	r := newFakeReader(2).
		withRange(0, 0, 5).
		withRecords(0, rr(0, 0, 10), rr(0, 1, 20), rr(0, 2, 30), rr(0, 3, 40), rr(0, 4, 50))

	ctx, cancel := context.WithCancel(context.Background())
	spec := emitSpec{starts: map[int32]int64{0: 99}} // caller start far past End=5

	errCh := make(chan error, 1)
	go func() {
		errCh <- emitTailing(ctx, r, cluster.Definition{}, "t", spec, newThrottle(0), func(cluster.RawRecord) error {
			return nil
		})
	}()

	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.openCalls > 0
	}, 2*time.Second, 10*time.Millisecond, "emitTailing must reach Open")

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("emitTailing did not return after context cancellation")
	}

	r.mu.Lock()
	lastStarts := r.lastStarts
	r.mu.Unlock()
	require.Equal(t, map[int32]int64{0: 5}, lastStarts, "Open must receive the clamped start (End=5), never the raw out-of-range 99")
}

// --- error propagation ---

func TestEmitForwardPropagatesSinkError(t *testing.T) {
	r := newFakeReader(2).
		withRange(0, 0, 5).
		withRecords(0, rr(0, 0, 10), rr(0, 1, 20))
	spec := emitSpec{limit: 5, starts: map[int32]int64{0: 0}}
	_, err := emitForward(context.Background(), r, cluster.Definition{}, "t", spec, newThrottle(0), func(cluster.RawRecord) error {
		return errEmitBoom
	})
	require.ErrorIs(t, err, errEmitBoom)
}

func TestEmitForwardPropagatesPartitionRangesError(t *testing.T) {
	r := newFakeReader(2)
	r.rangesErr = errEmitBoom
	spec := emitSpec{limit: 1, starts: map[int32]int64{0: 0}}
	_, err := emitForward(context.Background(), r, cluster.Definition{}, "t", spec, newThrottle(0), func(cluster.RawRecord) error {
		return nil
	})
	require.ErrorIs(t, err, errEmitBoom)
}

// --- throttle sanity ---

func TestThrottleZeroRateReturnsImmediately(t *testing.T) {
	thr := newThrottle(0)
	require.NoError(t, thr.wait(context.Background(), 1_000_000))
}

func TestThrottleWaitRespectsAlreadyCanceledContext(t *testing.T) {
	thr := newThrottle(1) // 1 byte/sec bucket -- any nontrivial request must wait
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := thr.wait(ctx, 1_000)
	require.ErrorIs(t, err, context.Canceled)
}
