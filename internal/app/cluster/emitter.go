// emitter.go implements P1c Task 7's app-layer message engine core: three
// browse-mode emitters (forward/backward/tailing) over the domain
// cluster.MessageReaderPort delivered by Task 6, plus a hand-rolled stdlib
// token-bucket throttle. Task 8's MessageService (not yet built) is the sole
// production caller of everything unexported here -- these are internal
// building blocks, not a public API of this package.
package cluster

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// emitSpec is one emitter call's parameters: which partitions to target
// (empty = every partition of the topic, resolved via PartitionRanges),
// forward/backward's record cap (ignored by tailing), and the per-partition
// starting/ending offsets forward+tailing / backward respectively key off.
type emitSpec struct {
	partitions []int32
	limit      int
	starts     map[int32]int64 // forward/tailing: each partition's starting offset
	ends       map[int32]int64 // backward: each partition's upper bound (its high-water mark)
}

// batchBytes sums a poll batch's byte footprint the way throttling accounts
// for it: KeySize+ValueSize+HeadersSize per record (Task 6's RawRecord doc
// comment reports these fields specifically so callers like this one never
// have to recompute them from Key/Value/Headers).
func batchBytes(batch []cluster.RawRecord) int {
	total := 0
	for _, rec := range batch {
		if rec.Control {
			continue
		}
		total += rec.KeySize + rec.ValueSize + rec.HeadersSize
	}
	return total
}

// clampOffset forces v into [lo, hi] -- forward's (and, defensively, a
// caller-supplied tailing start's) "越界 clamp" rule: a stale or bogus
// cursor must never reach Open as an out-of-range offset.
func clampOffset(v, lo, hi int64) int64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// emitForward reads spec.starts forward (ascending offset) across spec's
// target partitions, sinking up to spec.limit records, and stops as soon as
// either the limit is hit or every target partition has caught up to its own
// high-water mark (from PartitionRanges, snapshotted once up front). It
// returns each partition's next unread offset -- the cursor a caller saves
// (CursorCache.Save) to resume a later page.
//
// Every out-of-range spec.starts entry (missing, below Start, or above End)
// is clamped to the partition's own [Start, End] before Open is ever called,
// so a stale/bogus resume cursor can never reach the live reader as an
// out-of-bounds seek.
func emitForward(ctx context.Context, reader cluster.MessageReaderPort, def cluster.Definition, topic string, spec emitSpec, thr *throttle, sink func(cluster.RawRecord) error) (map[int32]int64, error) {
	ranges, err := reader.PartitionRanges(ctx, def, topic, spec.partitions)
	if err != nil {
		return nil, err
	}

	starts := make(map[int32]int64, len(ranges))
	for p, r := range ranges {
		s, ok := spec.starts[p]
		if !ok {
			s = r.Start
		}
		starts[p] = clampOffset(s, r.Start, r.End)
	}

	session, err := reader.Open(ctx, def, topic, starts)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	next := make(map[int32]int64, len(starts))
	for p, s := range starts {
		next[p] = s
	}

	count := 0
	for {
		if err := ctx.Err(); err != nil {
			return next, err
		}
		if spec.limit > 0 && count >= spec.limit {
			break
		}
		if forwardExhausted(next, ranges) {
			break
		}

		batch, err := session.Poll(ctx)
		if len(batch) > 0 {
			if waitErr := thr.wait(ctx, batchBytes(batch)); waitErr != nil {
				return next, waitErr
			}
			for _, rec := range batch {
				if rec.Control {
					// Transaction markers advance the reader cursor but are
					// never user-visible browse messages.
					if rec.Offset+1 > next[rec.Partition] {
						next[rec.Partition] = rec.Offset + 1
					}
					continue
				}
				if spec.limit > 0 && count >= spec.limit {
					break
				}
				if err := sink(rec); err != nil {
					return next, err
				}
				next[rec.Partition] = rec.Offset + 1
				count++
			}
		}
		if err != nil {
			// Poll may return healthy records and a fatal error from another
			// partition in the same fetch. The batch above must be delivered
			// before surfacing that error.
			return next, err
		}
		if len(batch) == 0 {
			continue // "this round produced no records" -- not yet caught up, keep polling
		}
	}
	return next, nil
}

// forwardExhausted reports whether every partition ranges names has already
// been read up to its own high-water mark (next[p] >= r.End) -- the "caught
// up" stop condition emitForward's loop checks alongside the record limit.
func forwardExhausted(next map[int32]int64, ranges map[int32]cluster.OffsetRange) bool {
	for p, r := range ranges {
		if next[p] < r.End {
			return false
		}
	}
	return true
}

// backwardBudgets fairly divides one total page limit across partitions with
// readable capacity. Empty or short partitions return their unused share to
// the remaining partitions, and deterministic partition order decides where
// a non-even remainder goes.
func backwardBudgets(partitions []int32, ranges map[int32]cluster.OffsetRange, ends map[int32]int64, limit int) map[int32]int64 {
	budgets := make(map[int32]int64, len(partitions))
	active := make([]int32, 0, len(partitions))
	capacities := make(map[int32]int64, len(partitions))
	for _, partition := range partitions {
		budgets[partition] = 0
		r := ranges[partition]
		end := clampOffset(ends[partition], r.Start, r.End)
		capacity := end - r.Start
		capacities[partition] = capacity
		if capacity > 0 {
			active = append(active, partition)
		}
	}
	if limit <= 0 {
		for _, partition := range partitions {
			budgets[partition] = capacities[partition]
		}
		return budgets
	}

	remaining := int64(limit)
	for remaining > 0 && len(active) > 0 {
		share := (remaining + int64(len(active)) - 1) / int64(len(active))
		next := make([]int32, 0, len(active))
		for _, partition := range active {
			room := capacities[partition] - budgets[partition]
			take := min(share, room, remaining)
			budgets[partition] += take
			remaining -= take
			if budgets[partition] < capacities[partition] {
				next = append(next, partition)
			}
		}
		active = next
	}
	return budgets
}

// emitBackward assigns each target partition a fair share of spec.limit,
// opens one multi-partition reader at the resulting lower bounds, and merges
// the returned records with an explicit sort.Slice descending by
// (TimestampMs, Partition, Offset). It truncates to exactly spec.limit and
// sinks sequentially in that order (sink is not assumed goroutine-safe).
// Each returned cursor is the minimum delivered offset for its partition,
// falling back to that partition's Open lower bound when no record survives.
func emitBackward(ctx context.Context, reader cluster.MessageReaderPort, def cluster.Definition, topic string, spec emitSpec, thr *throttle, sink func(cluster.RawRecord) error) (map[int32]int64, error) {
	ranges, err := reader.PartitionRanges(ctx, def, topic, spec.partitions)
	if err != nil {
		return nil, err
	}

	partitions := make([]int32, 0, len(ranges))
	for p := range ranges {
		partitions = append(partitions, p)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })

	ends := make(map[int32]int64, len(partitions))
	for _, p := range partitions {
		r := ranges[p]
		end := r.End
		if requested, ok := spec.ends[p]; ok {
			end = requested
		}
		ends[p] = clampOffset(end, r.Start, r.End)
	}

	budgets := backwardBudgets(partitions, ranges, ends, spec.limit)
	starts := make(map[int32]int64, len(partitions))
	next := make(map[int32]int64, len(partitions))
	for _, p := range partitions {
		starts[p] = ends[p] - budgets[p]
		next[p] = starts[p]
	}
	if len(partitions) == 0 {
		return next, nil
	}

	session, err := reader.Open(ctx, def, topic, starts)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	observed := make(map[int32]int64, len(starts))
	for p, start := range starts {
		observed[p] = start
	}

	var (
		merged  []cluster.RawRecord
		readErr error
	)
	for {
		if err := ctx.Err(); err != nil {
			readErr = err
			break
		}

		exhausted := true
		for _, p := range partitions {
			if observed[p] < ends[p] {
				exhausted = false
				break
			}
		}
		if exhausted {
			break
		}

		batch, pollErr := session.Poll(ctx)
		if len(batch) > 0 {
			if waitErr := thr.wait(ctx, batchBytes(batch)); waitErr != nil {
				readErr = waitErr
				break
			}
			for _, rec := range batch {
				start, targeted := starts[rec.Partition]
				if !targeted {
					continue
				}
				if rec.Offset+1 > observed[rec.Partition] {
					observed[rec.Partition] = rec.Offset + 1
				}
				if rec.Offset < start || rec.Offset >= ends[rec.Partition] || rec.Control {
					continue
				}
				merged = append(merged, rec)
			}
		}
		if pollErr != nil {
			readErr = pollErr
			break
		}
		if len(batch) == 0 {
			break
		}
	}

	if readErr != nil && len(merged) == 0 {
		return nil, readErr
	}

	sort.Slice(merged, func(i, j int) bool {
		a, b := merged[i], merged[j]
		if a.TimestampMs != b.TimestampMs {
			return a.TimestampMs > b.TimestampMs
		}
		if a.Partition != b.Partition {
			return a.Partition > b.Partition
		}
		return a.Offset > b.Offset
	})
	if spec.limit > 0 && len(merged) > spec.limit {
		merged = merged[:spec.limit]
	}

	seen := make(map[int32]bool, len(next))
	for _, rec := range merged {
		if !seen[rec.Partition] || rec.Offset < next[rec.Partition] {
			next[rec.Partition] = rec.Offset
			seen[rec.Partition] = true
		}
	}

	for _, rec := range merged {
		if err := sink(rec); err != nil {
			return next, err
		}
	}
	if readErr != nil {
		return next, readErr
	}
	return next, nil
}

// emitTailing polls spec.starts (defaulting, per partition, to that
// partition's current high-water mark when spec.starts is empty -- so a
// fresh tail only ever sees new messages, never a replay) forward forever,
// sinking every record it sees, until ctx is cancelled. Cancellation is the
// normal, expected way this returns (a live SSE stream client disconnecting)
// so it reports nil rather than ctx.Err() -- never a dead loop, and never an
// error for what is, from this function's point of view, a clean stop.
//
// spec.starts always goes through PartitionRanges and clampOffset, same as
// emitForward -- a caller-supplied tailing start that is stale or bogus
// (below the partition's Start, or past its current End) is clamped to
// [Start, End] before Open is ever called, matching the general "越界 clamp"
// rule (plan §7.2) rather than just the empty-starts default case.
func emitTailing(ctx context.Context, reader cluster.MessageReaderPort, def cluster.Definition, topic string, spec emitSpec, thr *throttle, sink func(cluster.RawRecord) error) error {
	ranges, err := reader.PartitionRanges(ctx, def, topic, spec.partitions)
	if err != nil {
		return err
	}

	starts := make(map[int32]int64, len(ranges))
	for p, r := range ranges {
		s, ok := spec.starts[p]
		if !ok {
			s = r.End
		}
		starts[p] = clampOffset(s, r.Start, r.End)
	}

	session, err := reader.Open(ctx, def, topic, starts)
	if err != nil {
		return err
	}
	defer session.Close()

	for {
		if ctx.Err() != nil {
			return nil
		}

		batch, err := session.Poll(ctx)
		if len(batch) > 0 {
			if waitErr := thr.wait(ctx, batchBytes(batch)); waitErr != nil {
				if ctx.Err() != nil {
					return nil
				}
				return waitErr
			}
			for _, rec := range batch {
				if rec.Control {
					continue
				}
				if sinkErr := sink(rec); sinkErr != nil {
					return sinkErr
				}
			}
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if len(batch) == 0 {
			continue
		}
	}
}

// throttle is a hand-rolled stdlib token bucket (time+sync only -- never
// golang.org/x/time/rate, which app-only-domain's depguard rule forbids
// anyway). Its capacity is exactly one second's worth of bytesPerSec, so a
// caller that has been idle can burst up to a full second's allowance before
// wait starts blocking.
type throttle struct {
	mu          sync.Mutex
	bytesPerSec int64
	tokens      float64
	last        time.Time
}

// newThrottle builds a throttle capped at bytesPerSec bytes/second.
// bytesPerSec <= 0 means unlimited: wait always returns immediately without
// even touching the mutex.
func newThrottle(bytesPerSec int64) *throttle {
	return &throttle{bytesPerSec: bytesPerSec, tokens: float64(bytesPerSec), last: time.Now()}
}

// wait blocks until bytes tokens are available (refilling at bytesPerSec/s
// since the last call), or ctx is done first (returning ctx.Err()). A
// disabled throttle (bytesPerSec <= 0) or a non-positive request always
// returns nil immediately.
func (t *throttle) wait(ctx context.Context, bytes int) error {
	if t == nil || t.bytesPerSec <= 0 || bytes <= 0 {
		return nil
	}
	for {
		t.mu.Lock()
		now := time.Now()
		if elapsed := now.Sub(t.last).Seconds(); elapsed > 0 {
			bucketCap := float64(t.bytesPerSec)
			t.tokens += elapsed * bucketCap
			if t.tokens > bucketCap {
				t.tokens = bucketCap
			}
			t.last = now
		}
		if t.tokens >= float64(bytes) {
			t.tokens -= float64(bytes)
			t.mu.Unlock()
			return nil
		}
		deficit := float64(bytes) - t.tokens
		waitFor := time.Duration(deficit / float64(t.bytesPerSec) * float64(time.Second))
		t.mu.Unlock()

		timer := time.NewTimer(waitFor)
		select {
		case <-timer.C:
			// refill and re-check: another concurrent caller may have
			// consumed tokens meanwhile, so loop rather than assume this
			// caller alone paid the full deficit.
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}
