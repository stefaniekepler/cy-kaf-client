package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	domainanalysis "github.com/cy-kaf/cy-kaf-client/internal/domain/analysis"
	domaincluster "github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

var ErrAnalysisTopicNotFound = errors.New("analysis topic not found")

// ErrAnalysisNoProgress is published in a terminal result when the reader
// keeps returning empty rounds for the bounded snapshot without advancing a
// partition. Kafka can legitimately return an empty fetch for a compacted
// tail, but franz-go does not expose that fetch's high-watermark to the
// ReaderSession wrapper; terminating with an explicit partial-result error is
// safer than claiming complete statistics or leaving a goroutine forever.
var ErrAnalysisNoProgress = errors.New("analysis made no progress")

const defaultAnalysisNoProgressTimeout = 30 * time.Second

type AnalysisProgress struct {
	StartedAt           int64
	CompletenessPercent float32
	MsgsScanned         int64
	BytesScanned        int64
}

type AnalysisResult struct {
	StartedAt      int64
	FinishedAt     int64
	Error          string
	TotalStats     domainanalysis.Stats
	PartitionStats []domainanalysis.Stats
}

type AnalysisView struct {
	Progress *AnalysisProgress
	Result   *AnalysisResult
}

type analysisIdentity struct {
	cluster string
	topic   string
}

// analysisStart reserves an identity while the synchronous watermark lookup
// is in flight. Keeping this reservation separate from runs lets Cancel remove
// it without waiting for a potentially slow broker call, while concurrent POST
// requests still remain idempotent. cancel interrupts the lookup context when
// the underlying reader honors context cancellation.
type analysisStart struct {
	cancel context.CancelFunc
}

type analysisRun struct {
	ctx         context.Context
	cancel      context.CancelFunc
	startedAt   int64
	totalToScan int64

	mu       sync.RWMutex
	progress AnalysisProgress
	result   *AnalysisResult
	done     bool
}

type AnalysisService struct {
	root   context.Context
	res    *Resolver
	reader domaincluster.MessageReaderPort
	// noProgressTimeout bounds an otherwise-unbounded sequence of empty polls
	// while scanning a fixed offset snapshot. It is a field (rather than a
	// package global) so deterministic tests can use a short deadline without
	// changing production behavior.
	noProgressTimeout time.Duration

	mu     sync.RWMutex
	runs   map[analysisIdentity]*analysisRun
	starts map[analysisIdentity]*analysisStart
	keys   keyedLocker[analysisIdentity]
}

func NewAnalysisService(root context.Context, res *Resolver, reader domaincluster.MessageReaderPort) *AnalysisService {
	if root == nil {
		root = context.Background()
	}
	return &AnalysisService{
		root:              root,
		res:               res,
		reader:            reader,
		noProgressTimeout: defaultAnalysisNoProgressTimeout,
		runs:              make(map[analysisIdentity]*analysisRun),
		starts:            make(map[analysisIdentity]*analysisStart),
	}
}

// analysisReaderPort is an optional capability implemented by the Kafka
// adapter. The normal Open path deliberately filters transaction markers for
// browse/emit callers; analysis opts in to markers so it can advance across a
// control-only tail without changing existing UI message semantics. Test and
// third-party readers that do not provide the capability safely fall back to
// the ordinary Open method.
type analysisReaderPort interface {
	OpenAnalysis(context.Context, domaincluster.Definition, string, map[int32]int64) (domaincluster.ReaderSession, error)
}

func (s *AnalysisService) openAnalysis(ctx context.Context, def domaincluster.Definition, topic string, starts map[int32]int64) (domaincluster.ReaderSession, error) {
	if reader, ok := s.reader.(analysisReaderPort); ok {
		return reader.OpenAnalysis(ctx, def, topic, starts)
	}
	return s.reader.Open(ctx, def, topic, starts)
}

func (s *AnalysisService) Analyze(ctx context.Context, name, topic string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	def, err := s.lookup(name)
	if err != nil {
		return err
	}
	identity := analysisIdentity{cluster: name, topic: topic}
	unlock := s.keys.lock(identity)

	s.mu.Lock()
	current := s.runs[identity]
	if current != nil && !current.isDone() {
		s.mu.Unlock()
		unlock()
		return nil
	}
	if _, starting := s.starts[identity]; starting {
		s.mu.Unlock()
		unlock()
		return nil
	}
	lookupCtx, cancelLookup := context.WithCancel(ctx)
	stopRoot := context.AfterFunc(s.root, cancelLookup)
	lookupCancel := func() {
		stopRoot()
		cancelLookup()
	}
	reservation := &analysisStart{cancel: lookupCancel}
	s.starts[identity] = reservation
	s.mu.Unlock()
	unlock()

	// Do not hold the keyed lock across the synchronous broker lookup. A
	// concurrent DELETE can now remove the reservation and return immediately.
	ranges, rangeErr := s.reader.PartitionRanges(lookupCtx, def, topic, nil)
	lookupCancel()

	unlock = s.keys.lock(identity)
	defer unlock()
	s.mu.Lock()
	if currentReservation, ok := s.starts[identity]; !ok || currentReservation != reservation {
		s.mu.Unlock()
		return nil
	}
	delete(s.starts, identity)
	if rangeErr != nil {
		s.mu.Unlock()
		return rangeErr
	}
	if current = s.runs[identity]; current != nil && !current.isDone() {
		s.mu.Unlock()
		return nil
	}
	if s.root.Err() != nil {
		s.mu.Unlock()
		return nil
	}

	if len(ranges) == 0 {
		s.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrAnalysisTopicNotFound, topic)
	}
	ranges = copyRanges(ranges)
	var totalToScan int64
	for _, r := range ranges {
		if r.End > r.Start {
			totalToScan += r.End - r.Start
		}
	}

	runCtx, cancel := context.WithCancel(s.root)
	startedAt := time.Now().UnixMilli()
	run := &analysisRun{
		ctx:         runCtx,
		cancel:      cancel,
		startedAt:   startedAt,
		totalToScan: totalToScan,
		progress: AnalysisProgress{
			StartedAt: startedAt,
		},
	}

	s.runs[identity] = run
	s.mu.Unlock()

	if totalToScan == 0 {
		run.finish(nil, domainanalysis.NewAccumulator())
		return nil
	}
	go s.scan(run, def, topic, ranges)
	return nil
}

func (s *AnalysisService) Get(name, topic string) (AnalysisView, bool, error) {
	if _, err := s.lookup(name); err != nil {
		return AnalysisView{}, false, err
	}
	identity := analysisIdentity{cluster: name, topic: topic}
	s.mu.RLock()
	run := s.runs[identity]
	s.mu.RUnlock()
	if run == nil {
		return AnalysisView{}, false, nil
	}
	return run.view(), true, nil
}

func (s *AnalysisService) Cancel(_ context.Context, name, topic string) error {
	_, err := s.lookup(name)
	if err != nil {
		return err
	}
	identity := analysisIdentity{cluster: name, topic: topic}
	unlock := s.keys.lock(identity)
	defer unlock()

	s.mu.Lock()
	run := s.runs[identity]
	delete(s.runs, identity)
	// A POST may still be resolving its initial watermarks. Remove that
	// reservation too; the POST will observe the token mismatch after the
	// lookup and will not register a run.
	start := s.starts[identity]
	delete(s.starts, identity)
	s.mu.Unlock()
	if start != nil && start.cancel != nil {
		start.cancel()
	}
	if run != nil {
		run.cancel()
	}
	return nil
}

func (s *AnalysisService) lookup(name string) (domaincluster.Definition, error) {
	if s == nil || s.res == nil {
		return domaincluster.Definition{}, fmt.Errorf("%w: %q", ErrUnknownCluster, name)
	}
	return s.res.Lookup(name)
}

func (s *AnalysisService) scan(run *analysisRun, def domaincluster.Definition, topic string, ranges map[int32]domaincluster.OffsetRange) {
	acc := domainanalysis.NewAccumulator()
	starts := make(map[int32]int64, len(ranges))
	next := make(map[int32]int64, len(ranges))
	for partition, r := range ranges {
		starts[partition] = r.Start
		next[partition] = r.Start
	}
	session, err := s.openAnalysis(run.ctx, def, topic, starts)
	if err != nil {
		if run.ctx.Err() == nil {
			run.finish(err, acc)
		}
		return
	}
	defer session.Close()
	idleSince := time.Now()

	for {
		if run.ctx.Err() != nil {
			return
		}
		if forwardAnalysisExhausted(next, ranges) {
			run.finish(nil, acc)
			return
		}

		pollCtx := run.ctx
		var pollCancel context.CancelFunc
		if s.noProgressTimeout > 0 {
			remaining := s.noProgressTimeout - time.Since(idleSince)
			if remaining <= 0 {
				run.finish(fmt.Errorf("%w after %s", ErrAnalysisNoProgress, s.noProgressTimeout), acc)
				return
			}
			pollCtx, pollCancel = context.WithTimeout(run.ctx, remaining)
		}
		batch, pollErr := session.Poll(pollCtx)
		if pollCancel != nil {
			pollCancel()
		}

		progressed := false
		for _, record := range batch {
			r, ok := ranges[record.Partition]
			if !ok {
				continue
			}
			if record.Offset < r.Start {
				continue
			}
			// Kafka offsets are ordered within a partition. Seeing the first
			// record at/after the snapshotted end proves this partition has
			// crossed the boundary, even when producers appended meanwhile;
			// appended records themselves remain excluded from the stats.
			if record.Offset >= r.End {
				if next[record.Partition] < r.End {
					next[record.Partition] = r.End
					progressed = true
				}
				continue
			}
			if record.Offset+1 > next[record.Partition] {
				next[record.Partition] = record.Offset + 1
				progressed = true
			}
			if record.Control {
				continue
			}
			acc.Observe(record.Partition, record.Offset, record.TimestampMs, record.Key, record.Value)
			run.recordProgress(int64(record.KeySize) + int64(record.ValueSize) + int64(record.HeadersSize))
		}

		if progressed {
			idleSince = time.Now()
		}
		if pollErr != nil {
			if run.ctx.Err() == nil {
				if errors.Is(pollErr, context.DeadlineExceeded) && s.noProgressTimeout > 0 && time.Since(idleSince) >= s.noProgressTimeout {
					run.finish(fmt.Errorf("%w after %s", ErrAnalysisNoProgress, s.noProgressTimeout), acc)
				} else {
					run.finish(pollErr, acc)
				}
			}
			return
		}
		if !progressed && s.noProgressTimeout > 0 && time.Since(idleSince) >= s.noProgressTimeout {
			run.finish(fmt.Errorf("%w after %s", ErrAnalysisNoProgress, s.noProgressTimeout), acc)
			return
		}
	}
}

func forwardAnalysisExhausted(next map[int32]int64, ranges map[int32]domaincluster.OffsetRange) bool {
	for partition, r := range ranges {
		if next[partition] < r.End {
			return false
		}
	}
	return true
}

func copyRanges(in map[int32]domaincluster.OffsetRange) map[int32]domaincluster.OffsetRange {
	out := make(map[int32]domaincluster.OffsetRange, len(in))
	for partition, r := range in {
		out[partition] = r
	}
	return out
}

func (r *analysisRun) isDone() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.done
}

func (r *analysisRun) recordProgress(bytes int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.progress.MsgsScanned++
	r.progress.BytesScanned += bytes
	if r.totalToScan > 0 {
		percent := 100 * float32(r.progress.MsgsScanned) / float32(r.totalToScan)
		if percent > 100 {
			percent = 100
		}
		r.progress.CompletenessPercent = percent
	}
}

func (r *analysisRun) finish(scanErr error, acc *domainanalysis.Accumulator) {
	if r.ctx.Err() != nil {
		return
	}
	snapshot := acc.Snapshot()
	result := &AnalysisResult{
		StartedAt:      r.startedAt,
		FinishedAt:     time.Now().UnixMilli(),
		TotalStats:     snapshot.Total,
		PartitionStats: snapshot.Partitions,
	}
	if scanErr != nil {
		result.Error = scanErr.Error()
	}
	r.mu.Lock()
	r.result = result
	r.done = true
	r.mu.Unlock()
}

func (r *analysisRun) view() AnalysisView {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.result != nil {
		result := *r.result
		result.TotalStats = cloneAnalysisStats(r.result.TotalStats)
		result.PartitionStats = make([]domainanalysis.Stats, len(r.result.PartitionStats))
		for i, stats := range r.result.PartitionStats {
			result.PartitionStats[i] = cloneAnalysisStats(stats)
		}
		return AnalysisView{Result: &result}
	}
	progress := r.progress
	return AnalysisView{Progress: &progress}
}

func cloneAnalysisStats(stats domainanalysis.Stats) domainanalysis.Stats {
	if stats.Partition != nil {
		partition := *stats.Partition
		stats.Partition = &partition
	}
	stats.HourlyMsgCounts = append([]domainanalysis.HourCount(nil), stats.HourlyMsgCounts...)
	return stats
}
