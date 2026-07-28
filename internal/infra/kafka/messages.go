// messages.go implements MessageReaderPort (P1c Task 6): partition watermark/
// timestamp lookups via the pooled admin client, plus an exclusively leased
// kgo consume client seeked to caller-chosen per-partition starting offsets.
// Healthy consume clients return to a bounded idle pool between requests.
// This is the bottom layer Task 7's emitter (forward/backward/tailing) builds
// on. P1c Task 11 adds MessageWriterPort (Produce/DeleteRecords) at the bottom
// of this file -- the write-side counterpart.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// partitionSet turns a MessageReaderPort partition filter into a lookup set:
// nil means "no filter" (every partition passes), matching PartitionRanges/
// OffsetsForTimestamp's documented empty-means-all convention.
func partitionSet(partitions []int32) map[int32]bool {
	if len(partitions) == 0 {
		return nil
	}
	set := make(map[int32]bool, len(partitions))
	for _, p := range partitions {
		set[p] = true
	}
	return set
}

// PartitionRanges reports topic's low/high watermark per partition, using
// the pooled admin client's ListStartOffsets/ListEndOffsets (P1b-audited).
// The -1 partition ListEndOffsets/ListStartOffsets synthesize when topic
// doesn't exist at all (kadm's documented sentinel, carrying
// kerr.UnknownTopicOrPartition) is filtered out here — it is never a real
// partition to report.
func (p *Pool) PartitionRanges(ctx context.Context, def cluster.Definition, topic string, partitions []int32) (map[int32]cluster.OffsetRange, error) {
	c, err := p.clientFor(def)
	if err != nil {
		return nil, err
	}
	return listPartitionRanges(ctx, c.adm, topic, partitions)
}

type offsetLister interface {
	ListStartOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
	ListEndOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
}

type listedOffsetsResult struct {
	offsets kadm.ListedOffsets
	err     error
}

// listPartitionRanges starts the independent low/high watermark requests
// together. A high-volume message browse needs both snapshots before it can
// choose a safe window, but serializing two broker round trips adds avoidable
// latency to every page. Buffered channels let either call finish first.
func listPartitionRanges(ctx context.Context, lister offsetLister, topic string, partitions []int32) (map[int32]cluster.OffsetRange, error) {
	startsResult := make(chan listedOffsetsResult, 1)
	endsResult := make(chan listedOffsetsResult, 1)
	go func() {
		offsets, err := lister.ListStartOffsets(ctx, topic)
		startsResult <- listedOffsetsResult{offsets: offsets, err: err}
	}()
	go func() {
		offsets, err := lister.ListEndOffsets(ctx, topic)
		endsResult <- listedOffsetsResult{offsets: offsets, err: err}
	}()

	starts := <-startsResult
	ends := <-endsResult
	if starts.err != nil {
		return nil, fmt.Errorf("list start offsets: %w", starts.err)
	}
	if ends.err != nil {
		return nil, fmt.Errorf("list end offsets: %w", ends.err)
	}

	want := partitionSet(partitions)
	out, err := partitionRangesFromOffsets(topic, starts.offsets, ends.offsets, want)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// partitionRangesFromOffsets validates per-partition kadm results before
// exposing them to the app layer. kadm can return a nil top-level error while
// carrying an error (or -1 offset) on just one offline partition; silently
// turning that into [0,-1) makes a reader seek an invalid offset and can leave
// an analysis run spinning forever.
func partitionRangesFromOffsets(topic string, starts, ends kadm.ListedOffsets, want map[int32]bool) (map[int32]cluster.OffsetRange, error) {
	out := make(map[int32]cluster.OffsetRange)
	for partition, end := range ends[topic] {
		// kadm uses a synthetic negative partition for a topic that does not
		// exist. Let the app layer translate the resulting empty map into its
		// declared "topic not found" response.
		if partition < 0 || (want != nil && !want[partition]) {
			continue
		}
		if end.Err != nil {
			return nil, fmt.Errorf("list end offset for topic %q partition %d: %w", topic, partition, end.Err)
		}
		if end.Offset < 0 {
			return nil, fmt.Errorf("list end offset for topic %q partition %d returned %d", topic, partition, end.Offset)
		}
		start, ok := starts[topic][partition]
		if !ok {
			return nil, fmt.Errorf("list start offset for topic %q partition %d was missing", topic, partition)
		}
		if start.Err != nil {
			return nil, fmt.Errorf("list start offset for topic %q partition %d: %w", topic, partition, start.Err)
		}
		if start.Offset < 0 {
			return nil, fmt.Errorf("list start offset for topic %q partition %d returned %d", topic, partition, start.Offset)
		}
		if end.Offset < start.Offset {
			return nil, fmt.Errorf("invalid offset range for topic %q partition %d: start %d > end %d", topic, partition, start.Offset, end.Offset)
		}
		out[partition] = cluster.OffsetRange{Start: start.Offset, End: end.Offset}
	}
	return out, nil
}

// OffsetsForTimestamp reports, per partition, the offset of the first record
// with a timestamp >= tsMs (kadm's ListOffsetsAfterMilli, P1b-audited).
func (p *Pool) OffsetsForTimestamp(ctx context.Context, def cluster.Definition, topic string, partitions []int32, tsMs int64) (map[int32]int64, error) {
	c, err := p.clientFor(def)
	if err != nil {
		return nil, err
	}
	listed, err := c.adm.ListOffsetsAfterMilli(ctx, tsMs, topic)
	if err != nil {
		return nil, fmt.Errorf("list offsets after timestamp: %w", err)
	}

	want := partitionSet(partitions)
	out := map[int32]int64{}
	for partition, lo := range listed[topic] {
		if partition < 0 || (want != nil && !want[partition]) {
			continue
		}
		if lo.Err != nil {
			return nil, fmt.Errorf("list offset after timestamp for topic %q partition %d: %w", topic, partition, lo.Err)
		}
		if lo.Offset < 0 {
			return nil, fmt.Errorf("list offset after timestamp for topic %q partition %d returned %d", topic, partition, lo.Offset)
		}
		out[partition] = lo.Offset
	}
	return out, nil
}

// Open leases an exclusive kgo consume client seeked to starts, one
// kgo.Offset per partition. A healthy session may reuse an idle client with
// the same cluster/topic/partition assignment after resetting its offsets.
// Unlike PartitionRanges/OffsetsForTimestamp, starts names exactly the
// partitions to consume — there is no "empty means all" here.
//
// ctx is intentionally unused: kgo.NewClient is synchronous and not
// context-aware (it only wires up config, it doesn't dial), matching the
// existing codebase precedent (buildOpts/clientFor take no ctx either) — a
// caller cancellation during Open has no effect. The session's ctx-honouring
// work happens later, in ReaderSession.Poll (PollFetches(ctx)).
func (p *Pool) Open(_ context.Context, def cluster.Definition, topic string, starts map[int32]int64) (cluster.ReaderSession, error) {
	return p.openReader(def, topic, starts, false)
}

// OpenAnalysis leases the same exclusive reader shape as Open, but retains
// Kafka transaction markers as advance-only RawRecords. Topic analysis needs
// those markers to move across control-only tails; ordinary browse/emit
// callers intentionally keep the historical marker-filtered behavior.
func (p *Pool) OpenAnalysis(_ context.Context, def cluster.Definition, topic string, starts map[int32]int64) (cluster.ReaderSession, error) {
	return p.openReader(def, topic, starts, true)
}

const maxIdleReaderClients = 8

type readerPoolKey struct {
	clusterName string
	topic       string
	assignment  string
	keepControl bool
}

func sortedReaderPartitions(starts map[int32]int64) []int32 {
	partitions := make([]int32, 0, len(starts))
	for partition := range starts {
		partitions = append(partitions, partition)
	}
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i] < partitions[j]
	})
	return partitions
}

func readerAssignmentKey(partitions []int32) string {
	key := make([]byte, 0, len(partitions)*4)
	for _, partition := range partitions {
		key = strconv.AppendInt(key, int64(partition), 10)
		key = append(key, ',')
	}
	return string(key)
}

func readerEpochOffsets(topic string, starts map[int32]int64) map[string]map[int32]kgo.EpochOffset {
	offsets := make(map[int32]kgo.EpochOffset, len(starts))
	for partition, at := range starts {
		offsets[partition] = kgo.EpochOffset{Epoch: -1, Offset: at}
	}
	return map[string]map[int32]kgo.EpochOffset{topic: offsets}
}

func readerTopicPartitions(topic string, partitions []int32) map[string][]int32 {
	return map[string][]int32{topic: partitions}
}

func (p *Pool) openReader(def cluster.Definition, topic string, starts map[int32]int64, keepControl bool) (cluster.ReaderSession, error) {
	partitions := sortedReaderPartitions(starts)
	key := readerPoolKey{
		clusterName: def.Name,
		topic:       topic,
		assignment:  readerAssignmentKey(partitions),
		keepControl: keepControl,
	}

	p.mu.Lock()
	generation := p.readerGenerations[def.Name]
	p.readerGenerations[def.Name] = generation
	readers := p.idleReaders[key]
	if len(readers) > 0 {
		cl := readers[len(readers)-1]
		if len(readers) == 1 {
			delete(p.idleReaders, key)
		} else {
			p.idleReaders[key] = readers[:len(readers)-1]
		}
		p.idleReaderCount--
		p.mu.Unlock()

		cl.SetOffsets(readerEpochOffsets(topic, starts))
		cl.ResumeFetchPartitions(readerTopicPartitions(topic, partitions))
		return newReaderSession(p, key, generation, partitions, cl), nil
	}
	p.mu.Unlock()

	opts, err := buildOpts(def.Conn)
	if err != nil {
		return nil, err
	}
	offsets := make(map[int32]kgo.Offset, len(starts))
	for partition, at := range starts {
		offsets[partition] = kgo.NewOffset().At(at)
	}
	opts = append(opts, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: offsets}))
	if keepControl {
		opts = append(opts, kgo.KeepControlRecords())
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("open message reader: %w", err)
	}
	return newReaderSession(p, key, generation, partitions, cl), nil
}

func newReaderSession(pool *Pool, key readerPoolKey, generation uint64, partitions []int32, cl *kgo.Client) *readerSession {
	session := &readerSession{
		cl:         cl,
		pool:       pool,
		key:        key,
		generation: generation,
		partitions: partitions,
	}
	session.reusable.Store(true)
	return session
}

func (p *Pool) releaseReader(key readerPoolKey, generation uint64, partitions []int32, cl *kgo.Client, reusable bool) {
	if !reusable {
		cl.Close()
		return
	}

	cl.PauseFetchPartitions(readerTopicPartitions(key.topic, partitions))

	p.mu.Lock()
	if p.readerGenerations[key.clusterName] != generation ||
		p.idleReaderCount >= maxIdleReaderClients {
		p.mu.Unlock()
		cl.Close()
		return
	}
	p.idleReaders[key] = append(p.idleReaders[key], cl)
	p.idleReaderCount++
	p.mu.Unlock()
}

// readerSession is MessageReaderPort.Open's ReaderSession implementation: a
// thin wrapper over one dedicated *kgo.Client.
type readerSession struct {
	cl         *kgo.Client
	pool       *Pool
	key        readerPoolKey
	generation uint64
	partitions []int32
	reusable   atomic.Bool
	closeOnce  sync.Once
}

// Poll fetches one batch and flattens it to RawRecord.
//
// Error/record ordering is deliberate and load-bearing: franz-go can return a
// single Fetches that mixes real records from healthy partitions with a
// per-partition or injected error on another, and once PollFetches returns kgo
// has already advanced its internal position — so any recovered record dropped
// here is lost forever. Therefore records are extracted FIRST and always win:
//   - records present ⇒ return (out, error) when a coexisting partition
//     error is present. Callers must consume out before handling the error;
//     kgo has already advanced its internal position when PollFetches returns.
//     This preserves healthy-partition data while still surfacing fatal
//     errors that would otherwise disappear if another partition kept
//     returning records.
//   - no records, but an error present ⇒ return (nil, first error) with the
//     "poll fetches" wrapping.
//   - no records, no error ⇒ (nil, nil): an ordinary empty round ("nothing
//     new since last poll, but the broker still answered within its own
//     fetch-wait"), per ReaderSession's doc comment.
func (s *readerSession) Poll(ctx context.Context) ([]cluster.RawRecord, error) {
	records, err := flattenFetches(s.cl.PollFetches(ctx))
	if err != nil &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) {
		s.reusable.Store(false)
	}
	return records, err
}

// flattenFetches keeps every returned record even when another partition in
// the same poll carries an error. franz-go advances the successful partition
// cursors before returning Fetches, so dropping the records while surfacing
// the error would permanently lose them. Callers process the returned batch
// first and then handle the non-nil error.
func flattenFetches(fetches kgo.Fetches) ([]cluster.RawRecord, error) {
	records := fetches.Records()
	if len(records) == 0 {
		if errs := fetches.Errors(); len(errs) > 0 {
			return nil, fmt.Errorf("poll fetches: %w", errs[0].Err)
		}
		// Keep the ReaderSession contract's empty-round shape: callers use a
		// nil batch to distinguish "nothing arrived" from a real batch.
		return nil, nil
	}
	out := make([]cluster.RawRecord, 0, len(records))
	for _, r := range records {
		out = append(out, rawRecordFrom(r))
	}
	if errs := fetches.Errors(); len(errs) > 0 {
		// Preserve the long-standing error prefix for callers that surface or
		// classify reader failures; the records+error ordering is the semantic
		// change here, not the public error text.
		return out, fmt.Errorf("poll fetches: %w", errs[0].Err)
	}
	return out, nil
}

// Close pauses the assignment and returns a healthy client to the bounded
// idle pool. Reuse preserves its broker connections while SetOffsets on the
// next lease discards any buffered position and seeks to that request's exact
// window. Failed clients and stale configuration generations are closed.
func (s *readerSession) Close() {
	s.closeOnce.Do(func() {
		if s.pool == nil {
			s.cl.Close()
			return
		}
		s.pool.releaseReader(
			s.key,
			s.generation,
			s.partitions,
			s.cl,
			s.reusable.Load(),
		)
	})
}

// rawRecordFrom maps one kgo.Record onto domain RawRecord, keeping Key/Value
// as the raw bytes Kafka stored (deserialization is Task 8's job, in the app
// layer). HeadersSize sums each header's key+value byte length — the same
// "byte footprint" reading as KeySize/ValueSize, just summed across every
// header rather than reported as a single field's length.
func rawRecordFrom(r *kgo.Record) cluster.RawRecord {
	headers := make(map[string]string, len(r.Headers))
	headersSize := 0
	for _, h := range r.Headers {
		headers[h.Key] = string(h.Value)
		headersSize += len(h.Key) + len(h.Value)
	}
	return cluster.RawRecord{
		Partition:   r.Partition,
		Offset:      r.Offset,
		TimestampMs: r.Timestamp.UnixMilli(),
		Key:         r.Key,
		Value:       r.Value,
		Headers:     headers,
		Control:     r.Attrs.IsControl(),
		KeySize:     len(r.Key),
		ValueSize:   len(r.Value),
		HeadersSize: headersSize,
	}
}

var _ cluster.MessageReaderPort = (*Pool)(nil)

// Produce writes rec to topic on a dedicated (never pooled) kgo producer
// client, then closes it -- send is a low-frequency UI action, so paying a
// fresh connection per call remains acceptable even though the higher-volume
// browse path now reuses a bounded set of idle reader connections.
//
// ⚠️ FIXED-PARTITION TRAP (verified against franz-go v1.21.5/kadm v1.18.0
// source, not guessed -- see capability_audit_test.go's Task 11 entry for
// the full source citation): kgo's DEFAULT partitioner (StickyKeyPartitioner)
// ignores Record.Partition entirely and re-hashes by Record.Key, so a plain
// `pool.clientFor(def).cl.ProduceSync(ctx, &kgo.Record{Partition: p, ...})`
// against the shared pooled client would NOT reliably land on partition p --
// a silent correctness bug. The fix is kgo.RecordPartitioner(kgo.
// ManualPartitioner()), a ProducerOpt that can only be set at client
// construction (not per-record), which is exactly why this method builds its
// own dedicated client rather than reusing the shared pooled one (that
// shared client backs every other caller's default-partitioner produces too,
// if it ever needs any -- forcing ManualPartitioner onto it globally would be
// wrong). kgo.ManualPartitioner()'s own doc comment: "simply returns the
// Partition field that is already set on any record" -- which is exactly
// rec.Partition here.
func (p *Pool) Produce(ctx context.Context, def cluster.Definition, topic string, rec cluster.ProduceRecord) error {
	opts, err := buildOpts(def.Conn)
	if err != nil {
		return err
	}
	opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("open message producer: %w", err)
	}
	defer cl.Close()

	var headers []kgo.RecordHeader
	if len(rec.Headers) > 0 {
		headers = make([]kgo.RecordHeader, 0, len(rec.Headers))
		for k, v := range rec.Headers {
			headers = append(headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
	}

	kr := &kgo.Record{
		Topic:     topic,
		Partition: rec.Partition,
		Key:       rec.Key,
		Value:     rec.Value,
		Headers:   headers,
	}
	if err := cl.ProduceSync(ctx, kr).FirstErr(); err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	return nil
}

// DeleteRecords purges partitions (or, when empty, every partition of
// topic -- same empty-means-all convention as PartitionRanges/
// OffsetsForTimestamp) by asking the pooled admin client to delete each
// named partition's records up to its own current end offset (kadm's
// DeleteRecords "delete before this offset" semantics, applied with that
// offset set to ListEndOffsets' own value) -- i.e. empties it entirely.
// Unlike PartitionRanges/OffsetsForTimestamp this does NOT reuse
// partitionSet's convenience the same way ListedOffsets.Offsets() would
// (that helper would carry every partition of topic straight into
// kadm.Offsets, which is wrong here when the caller named only some
// partitions) -- it builds the kadm.Offsets map manually, filtering to
// exactly the requested set first.
func (p *Pool) DeleteRecords(ctx context.Context, def cluster.Definition, topic string, partitions []int32) error {
	c, err := p.clientFor(def)
	if err != nil {
		return err
	}
	ends, err := c.adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return fmt.Errorf("list end offsets: %w", err)
	}

	want := partitionSet(partitions)
	offsets := make(kadm.Offsets)
	for partition, end := range ends[topic] {
		if partition < 0 || (want != nil && !want[partition]) {
			continue
		}
		offsets.Add(kadm.Offset{Topic: topic, Partition: partition, At: end.Offset})
	}

	resp, err := c.adm.DeleteRecords(ctx, offsets)
	if err != nil {
		return fmt.Errorf("delete records: %w", err)
	}
	if err := resp.Error(); err != nil {
		return fmt.Errorf("delete records: %w", err)
	}
	return nil
}

var _ cluster.MessageWriterPort = (*Pool)(nil)
