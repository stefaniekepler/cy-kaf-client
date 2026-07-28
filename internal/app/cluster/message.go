// message.go implements P1c Task 8's MessageService: the orchestration
// layer that opens one of Task 7's three emitters (forward/backward/
// tailing), and for every cluster.RawRecord it yields, deserializes it with
// Task 3's serde.Provider, masks it with Task 5's masking.Masker, filters it
// with a plain substring check and/or Task 4's filter.Engine, and hands the
// result to a caller-supplied emit callback as a BrowseEvent. Task 9 (not
// yet built) is this package's sole production caller by way of the api
// layer's Deps.Messages seam -- MessageService itself never touches HTTP or
// SSE framing, just the pipeline.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/filter"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/masking"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/serde"
)

// BrowseMode is the app-layer mirror of the contract's PollingMode enum
// (api's Task 9 handler maps generated.PollingMode onto this -- app must
// never import internal/api/generated, depguard's app-only-domain rule
// forbids it). Values and ordering are the plan's, verbatim.
type BrowseMode int

const (
	ModeLatest        BrowseMode = iota // 契约 LATEST
	ModeEarliest                        // EARLIEST
	ModeTailing                         // TAILING
	ModeFromOffset                      // FROM_OFFSET
	ModeToOffset                        // TO_OFFSET
	ModeFromTimestamp                   // FROM_TIMESTAMP
	ModeToTimestamp                     // TO_TIMESTAMP
)

// BrowseSpec is one Browse call's full request: which mode/partitions/limit
// to read, the from-offset/from-timestamp value that mode consults (Offset/
// TimestampMs -- only one is meaningful per mode, see the mode->emitter
// table on Browse), the two independent filters (plain substring and/or a
// CEL predicate), an explicit key/value serde override (falling back to
// each's suggested-preferred serde when empty), and a resume Cursor from a
// previous page's BrowseEvent.CursorID.
//
// The CEL predicate comes from exactly one of two mutually-exclusive
// sources: SmartFilterID (a previously filters.Register'd id -- Task 8's
// original design, driven by v2/getTopicMessagesV2's smartFilterId param)
// or FilterCode (raw CEL source, added by P1c Task 10's brief as a small,
// deliberate deviation from that task's plan file list: v1's
// getTopicMessages supports an inline `q`+filterQueryType=CEL_SCRIPT
// pair, but RegisterFilter isn't exposed over HTTP until Task 12, so v1's
// handler has nowhere else to compile that raw CEL through). When both are
// set, SmartFilterID wins and FilterCode is never even consulted -- see
// Browse's predicate-resolution comment for exactly where.
type BrowseSpec struct {
	Mode          BrowseMode
	Partitions    []int32
	Limit         int
	Offset        int64
	TimestampMs   int64
	StringFilter  string
	SmartFilterID string
	FilterCode    string
	KeySerde      string
	ValueSerde    string
	Cursor        string
}

// BrowseEventKind is BrowseEvent's discriminant. Exactly these four --
// there is no separate "cursor" event kind: a resume cursor is a field
// (CursorID) riding on a EventDone event, not its own event.
type BrowseEventKind int

const (
	EventPhase BrowseEventKind = iota
	EventConsuming
	EventMessage
	EventDone
)

// DecodedMessage is one fully-processed record, ready to hand to the UI:
// already deserialized (Key/Value are text, not raw bytes), already masked.
//
// TimestampType is always "CREATE_TIME": cluster.RawRecord (Task 6's
// MessageReaderPort) does not transmit Kafka's real per-record timestamp
// type (CreateTime vs LogAppendTime) -- the port would need extending to
// carry it, which is out of this task's scope (P2/if-needed, per this
// task's brief).
type DecodedMessage struct {
	Partition                       int32
	Offset                          int64
	TimestampMs                     int64
	TimestampType                   string
	Key, Value                      string
	Headers                         map[string]string
	KeySize, ValueSize, HeadersSize int64
	KeySerde, ValueSerde            string
}

// ConsumingStats is a browse's running or final progress snapshot: how much
// has actually been scanned off the cluster (BytesConsumed/
// MessagesConsumed count every record the emitter handed to sink, whether
// or not it passed the string/CEL filter -- these report scan progress, not
// filter hit count), how many CEL evaluations errored (FilterApplyErrors,
// never fatal -- see Browse's sink), and whether the browse stopped because
// its context was cancelled (IsCancelled -- only ever set on tailing's
// final snapshot; forward/backward's normal completion always reports
// false).
type ConsumingStats struct {
	BytesConsumed, ElapsedMs            int64
	MessagesConsumed, FilterApplyErrors int32
	IsCancelled                         bool
}

// BrowseEvent is one item Browse's emit callback receives. Only the field
// matching Kind is meaningful: EventPhase carries Phase, EventMessage
// carries Message, EventConsuming carries Consuming, EventDone carries
// CursorID (set only when a resume cursor exists, i.e. forward/backward's
// emitter returned a non-empty next map -- tailing's completion never sets
// this, nor does it emit EventDone at all, see Browse).
type BrowseEvent struct {
	Kind      BrowseEventKind
	Phase     string
	Message   *DecodedMessage
	Consuming *ConsumingStats
	CursorID  string
}

// fallbackSerdeName is what DecodedMessage.KeySerde/ValueSerde record when
// no serde could be resolved at all (e.g. an
// explicit spec.KeySerde/ValueSerde name Lookup doesn't recognize) or the
// resolved serde's Deserialize call itself errored on a particular record's
// bytes -- either way, decodeField still renders *something* (the raw bytes
// as a Go string) rather than dropping the record, and marks it so the UI
// can show its "fallback" affordance.
const fallbackSerdeName = "Fallback"

// consumingEventEvery is Browse's EventConsuming cadence: after every this-
// many processed records (whether or not they passed the filter), sink
// emits a running snapshot, in addition to the one unconditional final
// snapshot Browse always emits right before EventDone (or, for tailing,
// right before returning). The plan does not pin an exact frequency (its
// own words: "计划未钉频率，测试未强断") -- periodic-every-N plus a
// guaranteed final summary covers both a long-lived tailing stream (where
// "final" may be minutes away) and a short forward/backward page (where the
// final snapshot is effectively the only one that ever fires, since N=100
// well exceeds a typical page's Limit).
const consumingEventEvery = 100

// MessageService orchestrates Task 6's MessageReaderPort, Task 3's
// serde.Provider, Task 4's filter.Engine and Task 7's emitters/CursorCache
// into the single Browse entry point Task 9's SSE handler will call. It
// caches one masking.Masker per cluster name (masking.New precompiles every
// configured rule's regex, which would otherwise happen on every single
// Browse call).
type MessageService struct {
	res       *Resolver
	reader    cluster.MessageReaderPort
	writer    cluster.MessageWriterPort
	serdes    serde.Provider
	filters   filter.Engine
	cursors   *CursorCache
	refresher stateRefresher

	maskersMu sync.Mutex
	maskers   map[string]*masking.Masker
}

// stateRefresher lets Send/Delete nudge a cluster's cached topic counts to catch
// up after a produce/delete (read-your-writes) using the existing connection --
// *StateCache.RefreshWithoutInvalidate satisfies it. Optional: a nil refresher
// simply skips the nudge (e.g. tests that don't exercise count freshness). A
// narrow app-internal interface rather than a *StateCache field so it stays
// testable with a fake and doesn't drag the whole cache into every Send test.
type stateRefresher interface {
	RefreshWithoutInvalidate(ctx context.Context, name string) (cluster.RuntimeState, error)
}

// NewMessageService builds a MessageService resolving cluster names through
// res, reading raw records through reader (production: the shared
// infra/kafka pool, which satisfies cluster.MessageReaderPort), producing/
// purging records through writer (production: the same infra/kafka pool,
// which also satisfies cluster.MessageWriterPort -- P1c Task 11's Send/
// Delete), choosing/running serdes through serdes and CEL predicates through
// filters (production: infra/serde.NewProvider and infra/filter.NewEngine
// respectively), saving/resuming paging cursors through cursors, and nudging
// cached topic counts after a write through refresher (production: the shared
// *StateCache -- P1c Task 16's refresh-without-invalidate; a nil refresher
// skips that nudge).
func NewMessageService(res *Resolver, reader cluster.MessageReaderPort, writer cluster.MessageWriterPort, serdes serde.Provider, filters filter.Engine, cursors *CursorCache, refresher stateRefresher) *MessageService {
	return &MessageService{
		res:       res,
		reader:    reader,
		writer:    writer,
		serdes:    serdes,
		filters:   filters,
		cursors:   cursors,
		refresher: refresher,
		maskers:   map[string]*masking.Masker{},
	}
}

// maskerFor returns def's cached *masking.Masker, building and caching one
// (keyed by def.Name) on first use. A malformed masking rule -- New's only
// error case -- is a cluster configuration error, so it's returned as-is
// rather than falling back to "no masking"; the caller (Browse) surfaces it
// to whatever called Browse, same as any other setup error.
func (s *MessageService) maskerFor(def cluster.Definition) (*masking.Masker, error) {
	s.maskersMu.Lock()
	defer s.maskersMu.Unlock()
	if m, ok := s.maskers[def.Name]; ok {
		return m, nil
	}
	m, err := masking.New(def.Maskings)
	if err != nil {
		return nil, err
	}
	s.maskers[def.Name] = m
	return m, nil
}

// resolveSerdeFrom picks an explicit serde name or scans one already-fetched
// target description list for its preferred candidate, then resolves the
// selected name through Lookup. Either path failing reports nil.
func (s *MessageService) resolveSerdeFrom(def cluster.Definition, explicit string, descs []serde.Description) serde.Serde {
	name := explicit
	if name == "" {
		for _, desc := range descs {
			if desc.Preferred {
				name = desc.Name
				break
			}
		}
	}
	if name == "" {
		return nil
	}
	sd, ok := s.serdes.Lookup(def, name)
	if !ok {
		return nil
	}
	return sd
}

// resolveSerde picks the serde.Serde a single-target caller runs through:
// explicit's Lookup when non-empty, else whichever candidate one Suggest
// call marks Preferred for target (Task 3 guarantees exactly one).
//
// Send (use=UsageSerialize, P1c Task 11) treats a nil result as ErrSerialize
// -- see serializeField's doc comment.
func (s *MessageService) resolveSerde(def cluster.Definition, topic string, target serde.Target, explicit string, use serde.Usage) serde.Serde {
	var descs []serde.Description
	if explicit == "" {
		sug := s.serdes.Suggest(def, topic, use)
		descs = sug.Key
		if target == serde.TargetValue {
			descs = sug.Value
		}
	}
	return s.resolveSerdeFrom(def, explicit, descs)
}

// resolveBrowseSerdes resolves key and value defaults from one consistent
// suggestion snapshot. Explicit names bypass suggestion for that target, and
// when both names are explicit no Suggest call is made.
func (s *MessageService) resolveBrowseSerdes(def cluster.Definition, topic string, spec BrowseSpec) (serde.Serde, serde.Serde) {
	var suggestion serde.Suggestion
	if spec.KeySerde == "" || spec.ValueSerde == "" {
		suggestion = s.serdes.Suggest(def, topic, serde.UsageDeserialize)
	}
	return s.resolveSerdeFrom(def, spec.KeySerde, suggestion.Key),
		s.resolveSerdeFrom(def, spec.ValueSerde, suggestion.Value)
}

// decodeField renders data as text via sd (a resolveSerde result), falling
// back to the raw bytes verbatim under fallbackSerdeName when sd is nil (no
// serde resolved at all) or sd.Deserialize errors on this particular
// record's bytes (e.g. a numeric codec fed the wrong byte width) -- a
// per-record fallback, independent of every other record's own success.
func decodeField(sd serde.Serde, topic string, target serde.Target, data []byte) (text, serdeName string) {
	if sd != nil {
		if t, err := sd.Deserialize(topic, target, data); err == nil {
			return t, sd.Name()
		}
	}
	return string(data), fallbackSerdeName
}

// offsetsForPartitions builds a starts/ends map applying offset uniformly
// to every partition in partitions -- ModeFromOffset/ModeToOffset's "seek
// to this offset" only makes sense scoped to caller-named partition(s)
// (unlike timestamp-based modes, an offset has no cross-partition meaning),
// so this task expects spec.Partitions to be non-empty whenever one of
// those two modes is used; an empty partitions reports a nil map (no
// override), which falls back to the emitter's own per-mode default -- an
// edge case this task's tests don't exercise.
func offsetsForPartitions(partitions []int32, offset int64) map[int32]int64 {
	if len(partitions) == 0 {
		return nil
	}
	out := make(map[int32]int64, len(partitions))
	for _, p := range partitions {
		out[p] = offset
	}
	return out
}

// buildEmitSpec turns spec into the emitSpec Task 7's emitters consume, per
// the mode->emitter table in Browse's doc comment: which of starts/ends
// gets populated (and how) depends on spec.Mode; spec.Cursor, when it
// resolves via cursors.Load, then overrides that mode's starting point
// (forward modes override starts, backward modes override ends -- the
// direction always follows the original mode, never the cursor). A Cursor
// that fails to Load (unknown/expired id) is treated exactly like no cursor
// at all -- Browse still runs from the mode's own default start, per this
// task's brief.
func (s *MessageService) buildEmitSpec(ctx context.Context, def cluster.Definition, topic string, spec BrowseSpec) (emitSpec, error) {
	eSpec := emitSpec{partitions: spec.Partitions, limit: spec.Limit}

	switch spec.Mode {
	case ModeEarliest:
		// starts left nil: emitForward defaults every partition to its own Start.
	case ModeLatest:
		// ends left nil: emitBackward defaults every partition to its own End.
	case ModeTailing:
		// starts left nil: emitTailing defaults every partition to its own End.
	case ModeFromOffset:
		eSpec.starts = offsetsForPartitions(spec.Partitions, spec.Offset)
	case ModeToOffset:
		eSpec.ends = offsetsForPartitions(spec.Partitions, spec.Offset)
	case ModeFromTimestamp:
		offs, err := s.reader.OffsetsForTimestamp(ctx, def, topic, spec.Partitions, spec.TimestampMs)
		if err != nil {
			return emitSpec{}, err
		}
		eSpec.starts = offs
	case ModeToTimestamp:
		offs, err := s.reader.OffsetsForTimestamp(ctx, def, topic, spec.Partitions, spec.TimestampMs)
		if err != nil {
			return emitSpec{}, err
		}
		eSpec.ends = offs
	default:
		return emitSpec{}, fmt.Errorf("cluster: unknown browse mode %d", spec.Mode)
	}

	if spec.Cursor != "" {
		if pos, ok := s.cursors.Load(spec.Cursor); ok {
			switch spec.Mode {
			case ModeLatest, ModeToOffset, ModeToTimestamp:
				eSpec.ends = pos
			default: // ModeEarliest, ModeTailing, ModeFromOffset, ModeFromTimestamp
				eSpec.starts = pos
			}
		}
	}

	return eSpec, nil
}

// Browse resolves name to a cluster.Definition, opens the emitter Mode maps
// to, and for every record it yields runs the pipeline deserialize -> mask
// -> filter -> emit, in this order:
//
//	ModeEarliest       -> emitForward,  starts default (partition Start)
//	ModeLatest         -> emitBackward, ends default (partition End)
//	ModeTailing        -> emitTailing,  starts default (partition End)
//	ModeFromOffset     -> emitForward,  starts = {p: spec.Offset}
//	ModeToOffset       -> emitBackward, ends   = {p: spec.Offset}
//	ModeFromTimestamp  -> emitForward,  starts = OffsetsForTimestamp(...)
//	ModeToTimestamp    -> emitBackward, ends   = OffsetsForTimestamp(...)
//
// spec.Cursor, when it resolves, overrides that starting point (see
// buildEmitSpec). Event sequence: one EventPhase{"Consuming"} first; then,
// interleaved as records are processed, EventMessage per filter-matching
// record plus a periodic EventConsuming (see consumingEventEvery); one
// final EventConsuming right before completion; and -- forward/backward
// only -- a closing EventDone carrying CursorID when the emitter reported a
// resumable next position. Tailing has no "done" (it only stops via ctx
// cancellation or emit erroring), so it never emits EventDone; its final
// EventConsuming instead reports IsCancelled.
//
// emit returning a non-nil error (the SSE client disconnected) stops
// Browse immediately -- no further processing, no panic -- and that error
// propagates out of Browse.
//
// PARITY FLAG (see this task's report): this pipeline filters on
// *masked* text (deserialize -> mask -> filter, per the plan's stated
// order). Upstream kafka-ui may instead filter on the *unmasked* original
// text (masking meant only for display) -- left for the final P1c holistic
// parity review, not resolved here. This task's own tests don't distinguish
// the two orders (masking rule and filter terms never overlap).
func (s *MessageService) Browse(ctx context.Context, name, topic string, spec BrowseSpec, emit func(BrowseEvent) error) error {
	def, err := s.res.Lookup(name)
	if err != nil {
		return err
	}

	masker, err := s.maskerFor(def)
	if err != nil {
		return err
	}

	keySerde, valSerde := s.resolveBrowseSerdes(def, topic, spec)

	// SmartFilterID naming an id filters.Predicate doesn't recognize (never
	// Register'd, or evicted by the infra Engine's own TTL/capacity) is
	// treated as "no CEL filter for this request" rather than an error --
	// a stale/racy id shouldn't fail an otherwise-valid browse outright.
	// This task's tests don't cover that edge case.
	//
	// FilterCode (P1c Task 10's inline-CEL hook, see BrowseSpec's doc
	// comment) is consulted only when SmartFilterID is empty -- the two are
	// mutually exclusive by construction here, not just by convention.
	// Unlike an unrecognized SmartFilterID, a FilterCode compile error IS
	// fatal and returned immediately: it happens right here, strictly
	// before buildEmitSpec below and the unconditional EventPhase emit that
	// follows it, so it's always a pre-stream error -- api's v1 handler can
	// still turn it into an ordinary error response instead of a broken/
	// half-open SSE stream, exactly like every other pre-stream error this
	// method can return (res.Lookup, maskerFor, buildEmitSpec's own
	// OffsetsForTimestamp call).
	var pred filter.Predicate
	havePred := false
	switch {
	case spec.SmartFilterID != "":
		pred, havePred = s.filters.Predicate(spec.SmartFilterID)
	case spec.FilterCode != "":
		var compileErr error
		pred, compileErr = s.filters.Compile(spec.FilterCode)
		if compileErr != nil {
			return compileErr
		}
		havePred = true
	}

	eSpec, err := s.buildEmitSpec(ctx, def, topic, spec)
	if err != nil {
		return err
	}

	thr := newThrottle(def.PollingThrottleRate)

	if err := emit(BrowseEvent{Kind: EventPhase, Phase: "Consuming"}); err != nil {
		return err
	}

	stats := ConsumingStats{}
	start := time.Now()

	sink := func(rec cluster.RawRecord) error {
		stats.BytesConsumed += int64(rec.KeySize + rec.ValueSize + rec.HeadersSize)
		stats.MessagesConsumed++

		keyText, keyName := decodeField(keySerde, topic, serde.TargetKey, rec.Key)
		valText, valName := decodeField(valSerde, topic, serde.TargetValue, rec.Value)

		// A masking Apply error (only ever the re-marshal step -- see
		// masking.Masker.Apply's doc comment; not reachable with any of
		// this task's fixtures) falls back to that field's unmasked text
		// rather than aborting the whole browse over one record: dropping
		// a single record's masking is judged less disruptive than killing
		// an otherwise-healthy long-lived stream. ConsumingStats has no
		// dedicated counter for this (only FilterApplyErrors is in the
		// plan's struct) -- a deliberate judgment call, flagged in this
		// task's report.
		maskedKey, err := masker.Apply(topic, masking.TargetKey, keyText)
		if err != nil {
			maskedKey = keyText
		}
		maskedVal, err := masker.Apply(topic, masking.TargetValue, valText)
		if err != nil {
			maskedVal = valText
		}

		matched := spec.StringFilter == "" || strings.Contains(maskedKey, spec.StringFilter) || strings.Contains(maskedVal, spec.StringFilter)
		// NOTE (dual-review clarification, no behavior change): `matched &&
		// havePred` short-circuits pred.Eval whenever StringFilter is also
		// configured and this record already failed it -- both filters are
		// AND'd together, so there is no need to evaluate the second once
		// the first has already failed. This is intentional, not an
		// oversight: it means FilterApplyErrors can slightly undercount CEL
		// eval errors on records that would also have failed StringFilter,
		// since pred.Eval (and its error, if any) simply never runs for
		// them. Left as a documented choice for the final P1c holistic
		// review, not fixed here.
		if matched && havePred {
			hit, evalErr := pred.Eval(filter.Record{
				Key: maskedKey, Value: maskedVal, Headers: rec.Headers,
				Partition: rec.Partition, Offset: rec.Offset, TimestampMs: rec.TimestampMs,
			})
			switch {
			case evalErr != nil:
				stats.FilterApplyErrors++
				matched = false
			case !hit:
				matched = false
			}
		}

		if matched {
			msg := &DecodedMessage{
				Partition:     rec.Partition,
				Offset:        rec.Offset,
				TimestampMs:   rec.TimestampMs,
				TimestampType: "CREATE_TIME", // RawRecord doesn't transmit it -- see DecodedMessage's doc comment
				Key:           maskedKey,
				Value:         maskedVal,
				Headers:       rec.Headers,
				KeySize:       int64(rec.KeySize),
				ValueSize:     int64(rec.ValueSize),
				HeadersSize:   int64(rec.HeadersSize),
				KeySerde:      keyName,
				ValueSerde:    valName,
			}
			if err := emit(BrowseEvent{Kind: EventMessage, Message: msg}); err != nil {
				return err
			}
		}

		if stats.MessagesConsumed%consumingEventEvery == 0 {
			snap := stats
			snap.ElapsedMs = time.Since(start).Milliseconds()
			if err := emit(BrowseEvent{Kind: EventConsuming, Consuming: &snap}); err != nil {
				return err
			}
		}
		return nil
	}

	var next map[int32]int64
	switch spec.Mode {
	case ModeEarliest, ModeFromOffset, ModeFromTimestamp:
		next, err = emitForward(ctx, s.reader, def, topic, eSpec, thr, sink)
	case ModeLatest, ModeToOffset, ModeToTimestamp:
		next, err = emitBackward(ctx, s.reader, def, topic, eSpec, thr, sink)
	case ModeTailing:
		err = emitTailing(ctx, s.reader, def, topic, eSpec, thr, sink)
	default:
		return fmt.Errorf("cluster: unknown browse mode %d", spec.Mode)
	}
	if err != nil {
		return err
	}

	final := stats
	final.ElapsedMs = time.Since(start).Milliseconds()
	if spec.Mode == ModeTailing {
		final.IsCancelled = ctx.Err() != nil
	}
	if err := emit(BrowseEvent{Kind: EventConsuming, Consuming: &final}); err != nil {
		return err
	}

	if spec.Mode == ModeTailing {
		// Tailing has no "next page" (it only stops on cancellation/emit
		// error) -- no EventDone, no cursor, per this task's brief.
		return nil
	}

	done := BrowseEvent{Kind: EventDone}
	if len(next) > 0 {
		done.CursorID = s.cursors.Save(next)
	}
	return emit(done)
}

// ErrSerialize is Send's sentinel for a serialize-direction failure: no
// serde could be resolved at all (resolveSerde returned nil -- an explicit
// spec.KeySerde/ValueSerde name Lookup doesn't recognize, or no candidate is
// marked Preferred), or the resolved serde.Serde.Serialize call itself
// errored on the caller-supplied text (e.g. non-numeric text handed to an
// Int64 codec). Either way this is a client input error, not a server one
// -- the api handler maps it to 400 (same "app sentinel -> api status code"
// shape as ErrGroupNotInactive/ErrTopicDeletionDisabled elsewhere in this
// package).
var ErrSerialize = errors.New("failed to serialize message")

// SendSpec is one sendTopicMessages call's full request: which partition to
// produce to (explicit, no "let the partitioner choose" option -- see
// cluster.MessageWriterPort.Produce's doc comment on why fixed-partition is
// the only mode), the key/value text to serialize (nil = don't send that
// side of the record at all, one-for-one with cluster.ProduceRecord.Key/
// Value's own nil-means-absent convention -- an explicit empty string *is*
// still serialized and sent, it is not the same as nil), any headers to
// attach verbatim, and an explicit key/value serde override (falling back
// to each's suggested-preferred serialize serde when empty, via the same
// selection rules Browse uses for the deserialize direction).
//
// KeySerdeProps/ValueSerdeProps are accepted (the contract's
// CreateTopicMessage carries them) but not consulted by anything in this
// task: every built-in serde this task's serde.Provider resolves to ignores
// per-call properties entirely. They exist for a schema-registry-backed
// serde (P2, not built yet) that would need them; threading them through
// SendSpec now is forward-compatible plumbing, not currently load-bearing.
type SendSpec struct {
	Partition       int32
	Key, Value      *string
	Headers         map[string]string
	KeySerde        string
	ValueSerde      string
	KeySerdeProps   map[string]any
	ValueSerdeProps map[string]any
}

// serializeField converts *text into wire bytes via whichever serde
// resolveSerde picks for target (explicit name, or the Suggest-preferred
// UsageSerialize candidate) -- text == nil unconditionally reports (nil,
// nil) without even resolving a serde, matching cluster.ProduceRecord's
// "don't send this side" convention.
//
// Both "no serde resolved at all" and "resolved serde's Serialize call
// itself errored" report ErrSerialize. This is the mirror image of
// decodeField's fallback-to-raw-bytes behavior on the deserialize side, and
// deliberately does NOT mirror it: decodeField always has real bytes off
// the wire to fall back to showing verbatim, but serializeField has only
// caller-supplied text that -- by construction, since a serde failed or
// couldn't be found -- cannot be turned into wire bytes at all. There is no
// safe substitute to send instead, so this is a genuine client-facing
// error, not a display fallback.
func (s *MessageService) serializeField(def cluster.Definition, topic string, target serde.Target, explicit string, text *string) ([]byte, error) {
	if text == nil {
		return nil, nil
	}
	sd := s.resolveSerde(def, topic, target, explicit, serde.UsageSerialize)
	if sd == nil {
		return nil, fmt.Errorf("%w: no serde resolved for %s", ErrSerialize, topic)
	}
	data, err := sd.Serialize(topic, target, *text)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSerialize, err)
	}
	return data, nil
}

// Send resolves name to a cluster.Definition, serializes spec's key/value
// text (see serializeField) and produces one record to topic's
// spec.Partition through the MessageWriterPort.
//
// Deliberately does NOT call refreshAfterWrite (topic.go's read-your-writes
// helper, wired after all 6 of Topics' write methods) -- ADR-0005 §3 names
// this exact task and rules it out, for two independent reasons: (1) Browse
// (the read path Send's data would need to be "fresh" for) always reads a
// live cluster connection directly, never StateCache -- there is nothing
// stale on that read path for a produce to fix. (2) The one thing
// StateCache *does* track that a produce changes -- a topic's
// messagesCount -- is a UI-only nice-to-have that already catches up within
// StateCache's own 30s periodic refresh; paying refreshAfterWrite's
// Invalidate+reconnect+scrape cost on every single message send (a
// high-frequency UI action, unlike topic create/delete/config-change) was
// judged not worth it.
func (s *MessageService) Send(ctx context.Context, name, topic string, spec SendSpec) error {
	def, err := s.res.Lookup(name)
	if err != nil {
		return err
	}

	key, err := s.serializeField(def, topic, serde.TargetKey, spec.KeySerde, spec.Key)
	if err != nil {
		return err
	}
	value, err := s.serializeField(def, topic, serde.TargetValue, spec.ValueSerde, spec.Value)
	if err != nil {
		return err
	}

	rec := cluster.ProduceRecord{
		Partition: spec.Partition,
		Key:       key,
		Value:     value,
		Headers:   spec.Headers,
	}
	if err := s.writer.Produce(ctx, def, topic, rec); err != nil {
		return err
	}
	s.refreshAfterWrite(ctx, name)
	return nil
}

// Delete resolves name to a cluster.Definition and purges partitions (empty
// = every partition of topic, cluster.MessageWriterPort.DeleteRecords' own
// empty-means-all convention) through the MessageWriterPort.
//
// Same refreshAfterWrite-not-wired ruling as Send -- see its doc comment;
// ADR-0005 §3 covers both of this task's methods together, not just Send.
func (s *MessageService) Delete(ctx context.Context, name, topic string, partitions []int32) error {
	def, err := s.res.Lookup(name)
	if err != nil {
		return err
	}
	if err := s.writer.DeleteRecords(ctx, def, topic, partitions); err != nil {
		return err
	}
	s.refreshAfterWrite(ctx, name)
	return nil
}

// refreshAfterWrite nudges the state cache to re-scrape name so a produce/delete
// is reflected in the topic's messagesCount without waiting for the periodic
// refresh (read-your-writes) -- best-effort: it never fails the write (which
// already succeeded) and uses RefreshWithoutInvalidate so it does NOT pay a
// reconnect per message (ADR-0005 §3 / P1c Task 16). A nil refresher (some
// tests) makes this a no-op.
func (s *MessageService) refreshAfterWrite(ctx context.Context, name string) {
	if s.refresher == nil {
		return
	}
	if _, err := s.refresher.RefreshWithoutInvalidate(ctx, name); err != nil {
		slog.Warn("message write cache refresh failed", "cluster", name, "err", err)
	}
}
