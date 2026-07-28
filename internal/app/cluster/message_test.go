package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/filter"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/serde"
)

// message_test.go is package cluster (white-box, same as emitter_test.go):
// it reuses that file's fakeReader/newFakeReader helpers directly (no
// redefinition needed).
//
// DEPGUARD DEVIATION (see this task's report for the full rationale): the
// plan's Step 1 literally calls for "real serde.NewProvider / real
// filter.NewEngine" here, but both live in internal/infra, and
// .golangci.yml's app-test depguard rule allows only $gostd + domain + app
// + testify for internal/app/**/*_test.go -- importing infra here would be
// a lint failure, not a style nit. serde.Provider and filter.Engine are
// both domain interfaces, so this file fakes them directly; masking.New is
// domain (no such restriction) and is used for real below, with one REMOVE
// rule, giving genuine masking coverage. Real serde/CEL correctness is
// already locked down by infra/serde's and infra/filter's own test suites
// (Task 2/3, Task 4) -- this file's job is to prove MessageService's own
// orchestration wiring, which only needs domain-interface-shaped doubles.

// --- fake serde.Provider ---

// fakeSerde is a fake serde.Serde. Its zero value (fakeSerde{}) mirrors the
// original hardcoded double exactly: Name()=="FakeString", Deserialize is
// literally string(data) (the real built-in String codec's actual
// behavior, always succeeds) -- every pre-existing test in this file that
// builds fakeSerde{} is unaffected by the fields below.
//
// name/deserialize/serialize are injectable (all optional, zero value = the
// original behavior) added by this task's dual-review fix pass (deserialize)
// and by P1c Task 11 (serialize): they let
// TestBrowseExplicitSerdeOverride*/TestBrowseSerdeDeserializeError*/
// TestBrowseSuggestPreferredNotAtIndexZero* and TestSend* (below) build
// additional named serdes with a distinguishable transform or a forced
// Deserialize/Serialize error, without perturbing any test that only ever
// used the original always-"FakeString"/always-succeeds double.
type fakeSerde struct {
	name        string
	deserialize func(data []byte) (string, error)  // nil = string(data), nil (original behavior)
	serialize   func(input string) ([]byte, error) // nil = []byte(input), nil (original behavior)
}

func (s fakeSerde) Name() string {
	if s.name == "" {
		return "FakeString"
	}
	return s.name
}
func (fakeSerde) Description() string                        { return "fake string codec for message_test.go" }
func (fakeSerde) CanDeserialize(string, serde.Target) bool   { return true }
func (fakeSerde) CanSerialize(string, serde.Target) bool     { return true }
func (fakeSerde) Schema(string, serde.Target) (string, bool) { return "", false }
func (s fakeSerde) Serialize(_ string, _ serde.Target, input string) ([]byte, error) {
	if s.serialize != nil {
		return s.serialize(input)
	}
	return []byte(input), nil
}
func (s fakeSerde) Deserialize(_ string, _ serde.Target, data []byte) (string, error) {
	if s.deserialize != nil {
		return s.deserialize(data)
	}
	return string(data), nil
}

// fakeSerdeProvider is a deterministic fake of domain/serde.Provider. Its
// zero value (fakeSerdeProvider{}) mirrors the original hardcoded double
// exactly: Suggest always marks "FakeString" Preferred for both key and
// value (Task 3 guarantees exactly one Preferred candidate in the real
// provider; this fake mirrors that invariant), Lookup only recognizes that
// one name -- every pre-existing test in this file is unaffected.
//
// suggestKey/suggestValue/lookup are injectable (all optional, nil = fall
// back to the original behavior) added by this task's dual-review fix
// pass, so tests can drive resolveSerde's other branches: an explicit
// spec.KeySerde/ValueSerde naming a serde Lookup *does* recognize (the
// explicit-override branch), naming one it doesn't (the "explicit ->
// Lookup fails -> nil -> fallback" branch), and a Suggest fixture whose
// Preferred candidate isn't the slice's first element (guards a
// regression that reads descs[0] instead of scanning for Preferred).
type fakeSerdeProvider struct {
	suggestKey   []serde.Description
	suggestValue []serde.Description
	lookup       map[string]serde.Serde
	suggestCalls *int
}

func (p fakeSerdeProvider) Suggest(_ cluster.Definition, _ string, _ serde.Usage) serde.Suggestion {
	if p.suggestCalls != nil {
		(*p.suggestCalls)++
	}
	key, val := p.suggestKey, p.suggestValue
	if key == nil {
		key = []serde.Description{{Name: "FakeString", Description: "fake", Preferred: true}}
	}
	if val == nil {
		val = []serde.Description{{Name: "FakeString", Description: "fake", Preferred: true}}
	}
	return serde.Suggestion{Key: key, Value: val}
}

func (p fakeSerdeProvider) Lookup(_ cluster.Definition, name string) (serde.Serde, bool) {
	if p.lookup != nil {
		sd, ok := p.lookup[name]
		return sd, ok
	}
	if name != "FakeString" {
		return nil, false
	}
	return fakeSerde{}, true
}

// --- fake filter.Engine ---

// fakeFilterEngine is a deterministic fake of domain/filter.Engine: Browse
// calls Predicate (SmartFilterID path) and, as of P1c Task 10's inline-CEL
// hook, Compile (FilterCode path); Register is unused by Browse either way
// and stubbed just to satisfy the interface.
//
// compilePred/compileErr (added by Task 10) are the caller-injectable
// Compile result: a test sets one or the other (never both) to drive
// FilterCode's "compiled predicate applies like any other" and "compile
// error aborts Browse before its first EventPhase send" cases respectively,
// without this file ever importing infra/filter.NewEngine (real CEL
// correctness is Task 4's own infra/filter test suite's job, not this
// package's -- see this file's DEPGUARD DEVIATION note above).
type fakeFilterEngine struct {
	preds       map[string]filter.Predicate
	compilePred filter.Predicate
	compileErr  error
	registerID  string // Task 12: SmartFilterService.Register passthrough (zero value keeps Browse's callers untouched)
	registerErr error
}

func newFakeFilterEngine() *fakeFilterEngine {
	return &fakeFilterEngine{preds: map[string]filter.Predicate{}}
}

func (f *fakeFilterEngine) Register(string) (string, error) { return f.registerID, f.registerErr }
func (f *fakeFilterEngine) Predicate(id string) (filter.Predicate, bool) {
	p, ok := f.preds[id]
	return p, ok
}
func (f *fakeFilterEngine) Compile(string) (filter.Predicate, error) {
	return f.compilePred, f.compileErr
}

// containsXOrBoomPredicate implements the brief's "record.value.contains('x')"
// CEL predicate as a plain Go fake: a Value containing the sentinel "boom"
// simulates a filter expression that errors during Eval (e.g. a runtime
// type mismatch), everything else matches iff Value contains "x". The
// "boom"/"x" sentinels are deliberately disjoint from the masking rule's
// "secret" field used across this file's fixtures, per this task's brief
// ("masking 规则与 filter 词不重叠，两种顺序都过测").
type containsXOrBoomPredicate struct{}

var errPredicateBoom = errors.New("predicate eval boom")

func (containsXOrBoomPredicate) Eval(rec filter.Record) (bool, error) {
	if strings.Contains(rec.Value, "boom") {
		return false, errPredicateBoom
	}
	return strings.Contains(rec.Value, "x"), nil
}

// --- shared fixtures ---
//
// Every fixture record's value below is a JSON object with a "secret" field
// (masked away by newTestMessageService's REMOVE rule) and a "msg" field
// (left untouched, and where filter-relevant substrings like "hit"/"x"/
// "boom" live -- never inside "secret", so masking-then-filter and
// filter-then-masking would both see the same substrings; a real
// masking.Masker -- domain, no depguard issue -- runs for real, via
// newTestMessageService's Definition.Maskings, giving genuine masking
// coverage rather than a fake).

// msgRec builds one fixture cluster.RawRecord whose Value is a JSON object
// {"secret":"shh","msg":msg} -- msg carries whatever filter-relevant
// substring a given test needs, Key is a plain "k<offset>" string.
func msgRec(partition int32, offset, ts int64, msg string) cluster.RawRecord {
	key := []byte("k" + msgIndex(offset))
	val := []byte(`{"secret":"shh","msg":"` + msg + `"}`)
	return cluster.RawRecord{
		Partition: partition, Offset: offset, TimestampMs: ts,
		Key: key, Value: val,
		KeySize: len(key), ValueSize: len(val),
	}
}

func msgIndex(offset int64) string {
	digits := "0123456789"
	if offset < 10 {
		return string(digits[offset])
	}
	return "N"
}

// newTestMessageService wires a MessageService the way this file's tests
// need: a Resolver over a single "test" cluster.Definition carrying the
// REMOVE-secret masking rule, the fake serde provider/filter engine above,
// and a fresh CursorCache. writer is left nil (a MessageWriterPort this
// suite's Browse-only tests never touch -- see message_test.go's own P1c
// Task 11 section for the dedicated newTestMessageServiceForWrite helper
// Send/Delete tests use instead).
func newTestMessageService(t *testing.T, reader cluster.MessageReaderPort, engine *fakeFilterEngine) *MessageService {
	t.Helper()
	def := cluster.Definition{
		Name: "test",
		Maskings: []cluster.MaskingRule{
			{Type: cluster.MaskRemove, Fields: []string{"secret"}, TopicValuesPattern: ".*"},
		},
	}
	res := NewResolver([]cluster.Definition{def})
	if engine == nil {
		engine = newFakeFilterEngine()
	}
	cursors := NewCursorCache(time.Minute, 0) // capacity<=0=unbounded; ttl must be > 0 -- Load treats a ttl<=0 entry as already expired
	return NewMessageService(res, reader, nil, fakeSerdeProvider{}, engine, cursors, nil)
}

// newTestMessageServiceWithProvider is newTestMessageService with an
// injectable serde.Provider (added by this task's dual-review fix pass):
// the serde explicit-override/fallback/preferred-index tests below need a
// fakeSerdeProvider with a custom lookup/suggestKey/suggestValue fixture,
// which the plain newTestMessageService (hardcoded fakeSerdeProvider{})
// can't provide. writer is nil, same rationale as newTestMessageService.
func newTestMessageServiceWithProvider(t *testing.T, reader cluster.MessageReaderPort, engine *fakeFilterEngine, provider serde.Provider) *MessageService {
	t.Helper()
	def := cluster.Definition{
		Name: "test",
		Maskings: []cluster.MaskingRule{
			{Type: cluster.MaskRemove, Fields: []string{"secret"}, TopicValuesPattern: ".*"},
		},
	}
	res := NewResolver([]cluster.Definition{def})
	if engine == nil {
		engine = newFakeFilterEngine()
	}
	cursors := NewCursorCache(time.Minute, 0)
	return NewMessageService(res, reader, nil, provider, engine, cursors, nil)
}

// rawRec builds one fixture cluster.RawRecord from caller-supplied literal
// key/value text, unlike msgRec (which always wraps value in the shared
// {"secret":...,"msg":...} shape) -- used by the serde-selection tests
// below, which need precise control over the exact bytes a serde's
// Deserialize sees and exact control over whether the post-deserialize text
// is a JSON object the masking REMOVE rule can pass through untouched.
func rawRec(partition int32, offset, ts int64, key, val string) cluster.RawRecord {
	return cluster.RawRecord{
		Partition: partition, Offset: offset, TimestampMs: ts,
		Key: []byte(key), Value: []byte(val),
		KeySize: len(key), ValueSize: len(val),
	}
}

// offsetsOf extracts, in order, the Offset of every DecodedMessage in msgs
// (a messageEvents(...) result) -- used by the mode-dispatch tests below to
// assert both which offsets came through and in what order (ascending
// forward vs descending backward) with one concise assertion.
func offsetsOf(msgs []BrowseEvent) []int64 {
	out := make([]int64, len(msgs))
	for i, m := range msgs {
		out[i] = m.Message.Offset
	}
	return out
}

// collectingEmit returns an emit callback that appends every BrowseEvent it
// receives to *events, plus a way to make it fail starting from the Nth
// call (0 = never fails) to simulate an SSE client disconnecting mid-stream.
func collectingEmit(events *[]BrowseEvent, failAtMessage int) func(BrowseEvent) error {
	seen := 0
	return func(e BrowseEvent) error {
		if e.Kind == EventMessage {
			seen++
			if failAtMessage > 0 && seen == failAtMessage {
				*events = append(*events, e) // the disconnect happens *after* this event is already on the wire
				return errBrowseDisconnect
			}
		}
		*events = append(*events, e)
		return nil
	}
}

var errBrowseDisconnect = errors.New("simulated client disconnect")

func messageEvents(events []BrowseEvent) []BrowseEvent {
	var out []BrowseEvent
	for _, e := range events {
		if e.Kind == EventMessage {
			out = append(out, e)
		}
	}
	return out
}

// --- ① EARLIEST full browse: event sequence + masking ---

func TestBrowseEarliestEmitsPhaseThenMaskedMessagesThenDone(t *testing.T) {
	r := newFakeReader(10).
		withRange(0, 0, 3).
		withRecords(0, msgRec(0, 0, 100, "m0"), msgRec(0, 1, 200, "m1"), msgRec(0, 2, 300, "m2"))
	svc := newTestMessageService(t, r, nil)

	var events []BrowseEvent
	spec := BrowseSpec{Mode: ModeEarliest, Limit: 10}
	err := svc.Browse(context.Background(), "test", "t", spec, collectingEmit(&events, 0))
	require.NoError(t, err)

	require.NotEmpty(t, events)
	require.Equal(t, EventPhase, events[0].Kind)
	require.Equal(t, "Consuming", events[0].Phase)
	require.Equal(t, EventDone, events[len(events)-1].Kind)

	msgs := messageEvents(events)
	require.Len(t, msgs, 3)
	require.Equal(t, `{"msg":"m0"}`, msgs[0].Message.Value, "secret field must be masked away, msg field left as-is")
	require.Equal(t, `{"msg":"m1"}`, msgs[1].Message.Value)
	require.Equal(t, `{"msg":"m2"}`, msgs[2].Message.Value)
	require.Equal(t, "FakeString", msgs[0].Message.ValueSerde)
	require.Equal(t, "FakeString", msgs[0].Message.KeySerde, "default (non-explicit) key serde resolution must also record the resolved name")
	require.Equal(t, int32(0), msgs[0].Message.Partition)
	require.Equal(t, int64(0), msgs[0].Message.Offset)
}

// --- ② StringFilter: case-sensitive substring on masked text ---

func TestBrowseStringFilterIsCaseSensitiveSubstringOnMaskedText(t *testing.T) {
	r := newFakeReader(10).
		withRange(0, 0, 5).
		withRecords(0,
			msgRec(0, 0, 100, "hit-blue"),  // matches
			msgRec(0, 1, 200, "nothing"),   // no match
			msgRec(0, 2, 300, "Hit-caps"),  // capital H must NOT match (case-sensitive)
			msgRec(0, 3, 400, "hit-again"), // matches
			msgRec(0, 4, 500, "nope"),      // no match
		)
	svc := newTestMessageService(t, r, nil)

	var events []BrowseEvent
	spec := BrowseSpec{Mode: ModeEarliest, Limit: 10, StringFilter: "hit"}
	err := svc.Browse(context.Background(), "test", "t", spec, collectingEmit(&events, 0))
	require.NoError(t, err)

	msgs := messageEvents(events)
	require.Len(t, msgs, 2)
	require.Equal(t, int64(0), msgs[0].Message.Offset)
	require.Equal(t, int64(3), msgs[1].Message.Offset)
}

// --- ③ SmartFilterID: CEL predicate hit + eval-error path ---

func TestBrowseSmartFilterMatchesAndCountsEvalErrorsWithoutAborting(t *testing.T) {
	r := newFakeReader(10).
		withRange(0, 0, 4).
		withRecords(0,
			msgRec(0, 0, 100, "has-x"),  // matches predicate
			msgRec(0, 1, 200, "no-hit"), // no match
			msgRec(0, 2, 300, "boom"),   // Eval errors -- must count, not abort
			msgRec(0, 3, 400, "also-x"), // matches predicate
		)
	engine := newFakeFilterEngine()
	engine.preds["pred1"] = containsXOrBoomPredicate{}
	svc := newTestMessageService(t, r, engine)

	var events []BrowseEvent
	spec := BrowseSpec{Mode: ModeEarliest, Limit: 10, SmartFilterID: "pred1"}
	err := svc.Browse(context.Background(), "test", "t", spec, collectingEmit(&events, 0))
	require.NoError(t, err)

	msgs := messageEvents(events)
	require.Len(t, msgs, 2, "only offsets 0 and 3 contain 'x'; offset 2's eval error must exclude it, not abort the browse")
	require.Equal(t, int64(0), msgs[0].Message.Offset)
	require.Equal(t, int64(3), msgs[1].Message.Offset)

	var lastConsuming *ConsumingStats
	for _, e := range events {
		if e.Kind == EventConsuming {
			lastConsuming = e.Consuming
		}
	}
	require.NotNil(t, lastConsuming)
	require.Equal(t, int32(1), lastConsuming.FilterApplyErrors)
	require.Equal(t, int32(4), lastConsuming.MessagesConsumed, "all 4 scanned records count, filtered out or not")
}

// --- ④ cursor pagination: second page continues, no repeat/gap ---

func TestBrowseCursorResumesFromWhereThePreviousPageLeftOff(t *testing.T) {
	r := newFakeReader(10).
		withRange(0, 0, 5).
		withRecords(0,
			msgRec(0, 0, 100, "a"),
			msgRec(0, 1, 200, "b"),
			msgRec(0, 2, 300, "c"),
			msgRec(0, 3, 400, "d"),
			msgRec(0, 4, 500, "e"),
		)
	svc := newTestMessageService(t, r, nil)

	var firstEvents []BrowseEvent
	firstSpec := BrowseSpec{Mode: ModeEarliest, Limit: 2}
	err := svc.Browse(context.Background(), "test", "t", firstSpec, collectingEmit(&firstEvents, 0))
	require.NoError(t, err)

	firstMsgs := messageEvents(firstEvents)
	require.Len(t, firstMsgs, 2)
	require.Equal(t, int64(0), firstMsgs[0].Message.Offset)
	require.Equal(t, int64(1), firstMsgs[1].Message.Offset)

	done := firstEvents[len(firstEvents)-1]
	require.Equal(t, EventDone, done.Kind)
	require.NotEmpty(t, done.CursorID)

	var secondEvents []BrowseEvent
	secondSpec := BrowseSpec{Mode: ModeEarliest, Limit: 10, Cursor: done.CursorID}
	err = svc.Browse(context.Background(), "test", "t", secondSpec, collectingEmit(&secondEvents, 0))
	require.NoError(t, err)

	secondMsgs := messageEvents(secondEvents)
	require.Len(t, secondMsgs, 3, "must continue with exactly the remaining 3 records, no repeat/gap")
	require.Equal(t, int64(2), secondMsgs[0].Message.Offset)
	require.Equal(t, int64(3), secondMsgs[1].Message.Offset)
	require.Equal(t, int64(4), secondMsgs[2].Message.Offset)
}

// --- ⑤ emit error aborts Browse early, no panic ---

func TestBrowseStopsEarlyWithoutPanicWhenEmitErrors(t *testing.T) {
	r := newFakeReader(10).
		withRange(0, 0, 3).
		withRecords(0, msgRec(0, 0, 100, "m0"), msgRec(0, 1, 200, "m1"), msgRec(0, 2, 300, "m2"))
	svc := newTestMessageService(t, r, nil)

	var events []BrowseEvent
	spec := BrowseSpec{Mode: ModeEarliest, Limit: 10}

	require.NotPanics(t, func() {
		_ = svc.Browse(context.Background(), "test", "t", spec, collectingEmit(&events, 1))
	})

	msgs := messageEvents(events)
	require.Len(t, msgs, 1, "must stop after the first message event once emit errors -- no further messages, no EventDone")
	for _, e := range events {
		require.NotEqual(t, EventDone, e.Kind, "a disconnect must never reach EventDone")
	}
}

// --- Lookup(unknown cluster) propagates ErrUnknownCluster ---

func TestBrowseUnknownClusterReturnsErrUnknownCluster(t *testing.T) {
	r := newFakeReader(10)
	svc := newTestMessageService(t, r, nil)

	err := svc.Browse(context.Background(), "does-not-exist", "t", BrowseSpec{Mode: ModeEarliest}, func(BrowseEvent) error { return nil })
	require.ErrorIs(t, err, ErrUnknownCluster)
}

// --- mode->emitter mapping: the brief's own explicit risk flag ---
//
// The 5 tests above (mirroring the plan's Step 1 literally) only exercise
// ModeEarliest. Browse's doc comment table is this task's riskiest single
// piece of logic (the brief calls it out by name as one of the things to
// double-check before reporting done), so the tests below independently
// pin every mode's starts/ends population (buildEmitSpec, white-box/
// unexported -- fine, this file is package cluster) and, at the full-Browse
// level, that ModeLatest genuinely drives the backward emitter and
// ModeTailing genuinely never emits EventDone.

// fakeTimestampReader is a minimal cluster.MessageReaderPort double used
// only by TestBuildEmitSpecMapsEachModeToStartsOrEnds' FromTimestamp/
// ToTimestamp cases, where buildEmitSpec's only reader call is
// OffsetsForTimestamp -- PartitionRanges/Open are never reached from
// buildEmitSpec itself, so they're stubbed to zero values.
type fakeTimestampReader struct {
	offsets map[int32]int64
}

func (f fakeTimestampReader) PartitionRanges(context.Context, cluster.Definition, string, []int32) (map[int32]cluster.OffsetRange, error) {
	return nil, nil
}
func (f fakeTimestampReader) OffsetsForTimestamp(context.Context, cluster.Definition, string, []int32, int64) (map[int32]int64, error) {
	return f.offsets, nil
}
func (f fakeTimestampReader) Open(context.Context, cluster.Definition, string, map[int32]int64) (cluster.ReaderSession, error) {
	return nil, nil
}

func TestBuildEmitSpecMapsEachModeToStartsOrEnds(t *testing.T) {
	tsReader := fakeTimestampReader{offsets: map[int32]int64{0: 42}}

	cases := []struct {
		name       string
		reader     cluster.MessageReaderPort
		spec       BrowseSpec
		wantStarts map[int32]int64
		wantEnds   map[int32]int64
	}{
		{
			name:   "Earliest defaults starts to nil (forward, partition Start)",
			reader: newFakeReader(2),
			spec:   BrowseSpec{Mode: ModeEarliest},
		},
		{
			name:   "Latest defaults ends to nil (backward, partition End)",
			reader: newFakeReader(2),
			spec:   BrowseSpec{Mode: ModeLatest},
		},
		{
			name:   "Tailing defaults starts to nil (tailing, partition End)",
			reader: newFakeReader(2),
			spec:   BrowseSpec{Mode: ModeTailing},
		},
		{
			name:       "FromOffset fills starts={p:Offset} for every target partition",
			reader:     newFakeReader(2),
			spec:       BrowseSpec{Mode: ModeFromOffset, Partitions: []int32{0, 1}, Offset: 7},
			wantStarts: map[int32]int64{0: 7, 1: 7},
		},
		{
			name:     "ToOffset fills ends={p:Offset} for every target partition",
			reader:   newFakeReader(2),
			spec:     BrowseSpec{Mode: ModeToOffset, Partitions: []int32{0, 1}, Offset: 9},
			wantEnds: map[int32]int64{0: 9, 1: 9},
		},
		{
			name:       "FromTimestamp fills starts from reader.OffsetsForTimestamp",
			reader:     tsReader,
			spec:       BrowseSpec{Mode: ModeFromTimestamp, TimestampMs: 12345},
			wantStarts: map[int32]int64{0: 42},
		},
		{
			name:     "ToTimestamp fills ends from reader.OffsetsForTimestamp",
			reader:   tsReader,
			spec:     BrowseSpec{Mode: ModeToTimestamp, TimestampMs: 12345},
			wantEnds: map[int32]int64{0: 42},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := newTestMessageService(t, tc.reader, nil)
			eSpec, err := svc.buildEmitSpec(context.Background(), cluster.Definition{Name: "test"}, "t", tc.spec)
			require.NoError(t, err)
			require.Equal(t, tc.wantStarts, eSpec.starts)
			require.Equal(t, tc.wantEnds, eSpec.ends)
		})
	}
}

// TestBuildEmitSpecCursorOverridesFollowOriginalModeDirection pins the
// "覆盖该 mode 的起点（方向仍依原 mode）" rule: a resolved cursor overrides
// starts for forward-shaped modes (Earliest/Tailing/FromOffset/
// FromTimestamp) and ends for backward-shaped modes (Latest/ToOffset/
// ToTimestamp) -- never the other way around, regardless of which map that
// mode's own default step happened to populate. An unresolvable cursor
// (Load ok=false) must leave the mode's own default untouched.
func TestBuildEmitSpecCursorOverridesFollowOriginalModeDirection(t *testing.T) {
	svc := newTestMessageService(t, newFakeReader(2), nil)
	cursorPos := map[int32]int64{0: 99}
	id := svc.cursors.Save(cursorPos)

	forward, err := svc.buildEmitSpec(context.Background(), cluster.Definition{Name: "test"}, "t", BrowseSpec{Mode: ModeEarliest, Cursor: id})
	require.NoError(t, err)
	require.Equal(t, cursorPos, forward.starts, "forward-shaped mode: cursor overrides starts")
	require.Nil(t, forward.ends)

	backward, err := svc.buildEmitSpec(context.Background(), cluster.Definition{Name: "test"}, "t", BrowseSpec{Mode: ModeLatest, Cursor: id})
	require.NoError(t, err)
	require.Equal(t, cursorPos, backward.ends, "backward-shaped mode: cursor overrides ends")
	require.Nil(t, backward.starts)

	unresolved, err := svc.buildEmitSpec(context.Background(), cluster.Definition{Name: "test"}, "t", BrowseSpec{Mode: ModeEarliest, Cursor: "no-such-cursor-id"})
	require.NoError(t, err)
	require.Nil(t, unresolved.starts, "an unresolvable cursor must fall back to the mode's own default, not zero-value-override it")
}

// TestBrowseLatestUsesBackwardEmitterDescendingOrder proves ModeLatest
// genuinely drives emitBackward (not forward): with 3 records the messages
// must arrive in descending-offset order, the exact shape only
// emitBackward's merge produces (see TestEmitBackwardReturnsLastLimitRecordsDescending
// in emitter_test.go for the same assertion at the emitter layer).
func TestBrowseLatestUsesBackwardEmitterDescendingOrder(t *testing.T) {
	r := newFakeReader(10).
		withRange(0, 0, 3).
		withRecords(0, msgRec(0, 0, 100, "m0"), msgRec(0, 1, 200, "m1"), msgRec(0, 2, 300, "m2"))
	svc := newTestMessageService(t, r, nil)

	var events []BrowseEvent
	spec := BrowseSpec{Mode: ModeLatest, Limit: 10}
	err := svc.Browse(context.Background(), "test", "t", spec, collectingEmit(&events, 0))
	require.NoError(t, err)

	msgs := messageEvents(events)
	require.Len(t, msgs, 3)
	require.Equal(t, []int64{2, 1, 0}, []int64{msgs[0].Message.Offset, msgs[1].Message.Offset, msgs[2].Message.Offset})
	require.Equal(t, `{"msg":"m2"}`, msgs[0].Message.Value, "masking must still apply along the backward path")
}

// TestBrowseTailingNeverEmitsDoneAndReportsCancelledOnStop proves
// ModeTailing's documented divergence from forward/backward: it stops on
// ctx cancellation (not exhaustion), and its completion is a final
// EventConsuming with IsCancelled=true -- never an EventDone (no next page,
// no cursor, per this task's brief).
func TestBrowseTailingNeverEmitsDoneAndReportsCancelledOnStop(t *testing.T) {
	r := newFakeReader(2).
		withRange(0, 0, 0). // End=0: tailing's default start is the partition's End, so the one fixture
		// record (at offset 0, below) must sit *at* that End for it to ever be seen --
		// this mirrors "a fresh tail only ever sees new messages" (emitTailing's own doc comment).
		withRecords(0, msgRec(0, 0, 100, "m0"))
	svc := newTestMessageService(t, r, nil)

	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	var events []BrowseEvent
	emit := func(e BrowseEvent) error {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
		if e.Kind == EventMessage {
			cancel() // stop the tail right after it delivers the one record it has
		}
		return nil
	}

	spec := BrowseSpec{Mode: ModeTailing}
	errCh := make(chan error, 1)
	go func() { errCh <- svc.Browse(ctx, "test", "t", spec, emit) }()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Browse(ModeTailing) did not return after context cancellation")
	}

	mu.Lock()
	defer mu.Unlock()
	for _, e := range events {
		require.NotEqual(t, EventDone, e.Kind, "tailing must never emit EventDone")
	}
	last := events[len(events)-1]
	require.Equal(t, EventConsuming, last.Kind)
	require.True(t, last.Consuming.IsCancelled)
}

// --- serde selection and decodeField branches ---
//
// The original tests only exercised the "no explicit name,
// Suggest's one-and-only Preferred candidate sits at descs[0]" path, and
// decodeField's "sd resolved, Deserialize succeeds" path (fakeSerde{}
// always succeeds, fakeSerdeProvider{} always suggests exactly one
// candidate). The tests below pin single-snapshot Browse resolution plus
// explicit override, per-record Deserialize fallback, unknown-name fallback,
// and a Preferred candidate that is not first in its slice.

func TestBrowseResolvesDefaultKeyAndValueFromOneSuggestion(t *testing.T) {
	calls := 0
	provider := fakeSerdeProvider{suggestCalls: &calls}
	r := newFakeReader(10).
		withRange(0, 0, 1).
		withRecords(0, rawRec(0, 0, 100, "key", `{"msg":"value"}`))
	svc := newTestMessageServiceWithProvider(t, r, nil, provider)

	var events []BrowseEvent
	err := svc.Browse(
		context.Background(), "test", "t",
		BrowseSpec{Mode: ModeEarliest, Limit: 10},
		collectingEmit(&events, 0),
	)

	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.Len(t, messageEvents(events), 1)
}

// TestBrowseExplicitSerdeOverrideUsesNamedSerde pins the
// `explicit != ""` branch: spec.KeySerde/ValueSerde naming a serde
// Lookup *does* recognize must be used verbatim, in preference to
// whatever Suggest would otherwise have picked -- proven by using a
// distinguishable transform (uppercase) so the decoded text itself, not
// just the recorded serde name, shows the named serde actually ran.
func TestBrowseExplicitSerdeOverrideUsesNamedSerde(t *testing.T) {
	upper := fakeSerde{name: "FakeUpper", deserialize: func(data []byte) (string, error) {
		return strings.ToUpper(string(data)), nil
	}}
	provider := fakeSerdeProvider{lookup: map[string]serde.Serde{
		"FakeString": fakeSerde{},
		"FakeUpper":  upper,
	}}
	r := newFakeReader(10).
		withRange(0, 0, 1).
		withRecords(0, rawRec(0, 0, 100, "k0", `{"msg":"foo"}`))
	svc := newTestMessageServiceWithProvider(t, r, nil, provider)

	var events []BrowseEvent
	spec := BrowseSpec{Mode: ModeEarliest, Limit: 10, KeySerde: "FakeUpper", ValueSerde: "FakeUpper"}
	err := svc.Browse(context.Background(), "test", "t", spec, collectingEmit(&events, 0))
	require.NoError(t, err)

	msgs := messageEvents(events)
	require.Len(t, msgs, 1)
	require.Equal(t, "FakeUpper", msgs[0].Message.KeySerde, "explicit spec.KeySerde must override Suggest's preferred FakeString")
	require.Equal(t, "FakeUpper", msgs[0].Message.ValueSerde, "explicit spec.ValueSerde must override Suggest's preferred FakeString")
	require.Equal(t, "K0", msgs[0].Message.Key, "key text must show FakeUpper's transform actually ran, not just its name being recorded")
	require.Equal(t, `{"MSG":"FOO"}`, msgs[0].Message.Value, "value text must show FakeUpper's transform actually ran (no \"secret\" field, so masking passes it through unchanged)")
}

// errFakeSerdeDeserializeFailed is the forced error a fakeSerde built with
// a failing deserialize func reports -- simulates a serde that resolves
// fine (Lookup finds it) but chokes on one particular record's bytes (e.g.
// a numeric codec fed the wrong byte width).
var errFakeSerdeDeserializeFailed = errors.New("fake serde: deserialize failed")

// TestBrowseSerdeDeserializeErrorFallsBackToRawBytesAsFallback pins
// decodeField's per-record fallback branch: a resolved (non-nil) serde
// whose Deserialize call itself errors must render the field as its raw
// bytes verbatim and record KeySerde/ValueSerde as "Fallback" -- distinct
// from TestBrowseExplicitUnknownSerdeNameFallsBackToRawBytes below, which
// hits the same fallback via a nil serde (Lookup never even finding a
// serde to try).
func TestBrowseSerdeDeserializeErrorFallsBackToRawBytesAsFallback(t *testing.T) {
	failing := fakeSerde{name: "AlwaysFailsDeserialize", deserialize: func([]byte) (string, error) {
		return "", errFakeSerdeDeserializeFailed
	}}
	provider := fakeSerdeProvider{lookup: map[string]serde.Serde{
		"FakeString":             fakeSerde{},
		"AlwaysFailsDeserialize": failing,
	}}
	r := newFakeReader(10).
		withRange(0, 0, 1).
		withRecords(0, rawRec(0, 0, 100, "raw-key-bytes", `{"msg":"foo"}`))
	svc := newTestMessageServiceWithProvider(t, r, nil, provider)

	var events []BrowseEvent
	spec := BrowseSpec{Mode: ModeEarliest, Limit: 10, KeySerde: "AlwaysFailsDeserialize"}
	err := svc.Browse(context.Background(), "test", "t", spec, collectingEmit(&events, 0))
	require.NoError(t, err)

	msgs := messageEvents(events)
	require.Len(t, msgs, 1)
	require.Equal(t, fallbackSerdeName, msgs[0].Message.KeySerde, "a resolved serde whose Deserialize errors must still fall back and be marked Fallback")
	require.Equal(t, "raw-key-bytes", msgs[0].Message.Key, "fallback text must be the raw bytes verbatim, not an empty/garbled string")
}

// TestBrowseExplicitUnknownSerdeNameFallsBackToRawBytes pins the
// "explicit set but Lookup doesn't recognize it -> nil" branch (an
// explicit spec.KeySerde/ValueSerde naming something the provider's
// Lookup has never heard of), and confirms decodeField treats that nil
// serde exactly like a Deserialize error: raw bytes verbatim, marked
// "Fallback".
func TestBrowseExplicitUnknownSerdeNameFallsBackToRawBytes(t *testing.T) {
	r := newFakeReader(10).
		withRange(0, 0, 1).
		withRecords(0, rawRec(0, 0, 100, "raw-key-bytes", `{"msg":"foo"}`))
	svc := newTestMessageService(t, r, nil) // default fakeSerdeProvider{}: Lookup only recognizes "FakeString"

	var events []BrowseEvent
	spec := BrowseSpec{Mode: ModeEarliest, Limit: 10, KeySerde: "NoSuchSerde"}
	err := svc.Browse(context.Background(), "test", "t", spec, collectingEmit(&events, 0))
	require.NoError(t, err)

	msgs := messageEvents(events)
	require.Len(t, msgs, 1)
	require.Equal(t, fallbackSerdeName, msgs[0].Message.KeySerde, "an explicit name Lookup doesn't recognize must fall back, exactly like a Deserialize error")
	require.Equal(t, "raw-key-bytes", msgs[0].Message.Key, "fallback text must be the raw bytes verbatim")
}

// TestBrowseSuggestPreferredNotAtIndexZeroIsSelected guards against a
// `descs[0]` regression in serde selection: every pre-existing test's
// fakeSerdeProvider Suggest fixture has exactly one candidate (so index 0
// and "the Preferred one" always coincide, hiding a regression that reads
// descs[0] instead of scanning for Preferred==true). This test's Suggest
// fixture puts a non-preferred candidate first and the Preferred one
// second; if selection ever regressed to descs[0], the record would
// come back as {"msg":"WRONG"} (NotPreferred's fixed transform) instead of
// the fixture's real {"msg":"plain"} passthrough.
func TestBrowseSuggestPreferredNotAtIndexZeroIsSelected(t *testing.T) {
	notPreferred := fakeSerde{name: "NotPreferred", deserialize: func([]byte) (string, error) {
		return `{"msg":"WRONG"}`, nil
	}}
	actuallyPreferred := fakeSerde{name: "ActuallyPreferred"} // default deserialize = string(data), i.e. passthrough
	provider := fakeSerdeProvider{
		suggestValue: []serde.Description{
			{Name: "NotPreferred", Description: "first in the slice, but not preferred", Preferred: false},
			{Name: "ActuallyPreferred", Description: "second in the slice, and IS preferred", Preferred: true},
		},
		lookup: map[string]serde.Serde{
			"FakeString":        fakeSerde{},
			"NotPreferred":      notPreferred,
			"ActuallyPreferred": actuallyPreferred,
		},
	}
	r := newFakeReader(10).
		withRange(0, 0, 1).
		withRecords(0, rawRec(0, 0, 100, "k0", `{"msg":"plain"}`))
	svc := newTestMessageServiceWithProvider(t, r, nil, provider)

	var events []BrowseEvent
	spec := BrowseSpec{Mode: ModeEarliest, Limit: 10} // no explicit ValueSerde: forces selection through Suggest
	err := svc.Browse(context.Background(), "test", "t", spec, collectingEmit(&events, 0))
	require.NoError(t, err)

	msgs := messageEvents(events)
	require.Len(t, msgs, 1)
	require.Equal(t, "ActuallyPreferred", msgs[0].Message.ValueSerde, "must select the Preferred candidate regardless of its slice position")
	require.Equal(t, `{"msg":"plain"}`, msgs[0].Message.Value, "must be ActuallyPreferred's real passthrough, not NotPreferred's WRONG sentinel")
}

// --- dual-review fix pass: full-Browse dispatch for the 4 remaining modes ---
//
// buildEmitSpec is table-tested (TestBuildEmitSpecMapsEachModeToStartsOrEnds
// above) for all 7 modes, but only ModeEarliest/ModeLatest/ModeTailing are
// ever driven through Browse's own emitter-dispatch switch end-to-end
// (TestBrowseEarliestEmitsPhaseThenMaskedMessagesThenDone,
// TestBrowseLatestUsesBackwardEmitterDescendingOrder,
// TestBrowseTailingNeverEmitsDoneAndReportsCancelledOnStop) -- Go's shared
// `case ModeEarliest, ModeFromOffset, ModeFromTimestamp:` (and the backward
// equivalent) block means per-branch coverage tools can't tell whether
// ModeFromOffset/ModeToOffset/ModeFromTimestamp/ModeToTimestamp actually
// reach that switch at all, only that *some* case sharing that block does.
// The four tests below close that gap: same 5-record fixture (offsets
// 0..4) for all four, with a from/to boundary strictly inside the range
// (not the partition's own Start=0/End=5 default) so a wiring regression
// that silently fell back to ModeEarliest/ModeLatest's defaults would
// produce a visibly different offset set/order, not just accidentally
// match.

// fiveOffsetFixture builds the shared 5-record (offsets 0..4, partition 0,
// range [0,5)) fakeReader the four mode-dispatch tests below all start
// from.
func fiveOffsetFixture() *fakeReader {
	return newFakeReader(10).
		withRange(0, 0, 5).
		withRecords(0,
			msgRec(0, 0, 100, "m0"), msgRec(0, 1, 200, "m1"), msgRec(0, 2, 300, "m2"),
			msgRec(0, 3, 400, "m3"), msgRec(0, 4, 500, "m4"))
}

// offsetsForTimestampReader wraps a *fakeReader, overriding only
// OffsetsForTimestamp to return a fixed fixture map. Unlike
// TestBuildEmitSpecMapsEachModeToStartsOrEnds' fakeTimestampReader (which
// only needs to satisfy buildEmitSpec's single OffsetsForTimestamp call),
// the ModeFromTimestamp/ModeToTimestamp tests below run a real end-to-end
// Browse, which also needs PartitionRanges/Open/Poll to actually serve
// records -- embedding *fakeReader supplies those for free, promoted
// unchanged; only OffsetsForTimestamp needs shadowing.
type offsetsForTimestampReader struct {
	*fakeReader
	offsets map[int32]int64
}

func (r offsetsForTimestampReader) OffsetsForTimestamp(context.Context, cluster.Definition, string, []int32, int64) (map[int32]int64, error) {
	return r.offsets, nil
}

// TestBrowseFromOffsetDrivesForwardEmitterFromGivenOffset proves
// ModeFromOffset is wired to emitForward starting at spec.Offset (2), not
// the partition's default Start (0): only offsets 2..4 must come through,
// ascending.
func TestBrowseFromOffsetDrivesForwardEmitterFromGivenOffset(t *testing.T) {
	svc := newTestMessageService(t, fiveOffsetFixture(), nil)

	var events []BrowseEvent
	spec := BrowseSpec{Mode: ModeFromOffset, Partitions: []int32{0}, Offset: 2, Limit: 10}
	err := svc.Browse(context.Background(), "test", "t", spec, collectingEmit(&events, 0))
	require.NoError(t, err)

	msgs := messageEvents(events)
	require.Len(t, msgs, 3, "must start at offset 2 (spec.Offset), not the partition's default Start=0")
	require.Equal(t, []int64{2, 3, 4}, offsetsOf(msgs), "forward direction: ascending offsets")
}

// TestBrowseToOffsetDrivesBackwardEmitterUpToGivenOffset proves
// ModeToOffset is wired to emitBackward with ends=spec.Offset (3, an
// exclusive upper bound per emitBackward's own semantics), not the
// partition's default End (5): only offsets 0..2 must come through,
// descending.
func TestBrowseToOffsetDrivesBackwardEmitterUpToGivenOffset(t *testing.T) {
	svc := newTestMessageService(t, fiveOffsetFixture(), nil)

	var events []BrowseEvent
	spec := BrowseSpec{Mode: ModeToOffset, Partitions: []int32{0}, Offset: 3, Limit: 10}
	err := svc.Browse(context.Background(), "test", "t", spec, collectingEmit(&events, 0))
	require.NoError(t, err)

	msgs := messageEvents(events)
	require.Len(t, msgs, 3, "must stop at offset 3 exclusive (spec.Offset), not the partition's default End=5")
	require.Equal(t, []int64{2, 1, 0}, offsetsOf(msgs), "backward direction: descending offsets")
}

// TestBrowseFromTimestampDrivesForwardEmitterFromResolvedOffset proves
// ModeFromTimestamp is wired to emitForward starting at whatever
// reader.OffsetsForTimestamp resolves (fixture: 2), not the partition's
// default Start (0) -- i.e. buildEmitSpec's OffsetsForTimestamp call
// actually reaches the forward emitter's starts, not just buildEmitSpec's
// own return value (already covered by
// TestBuildEmitSpecMapsEachModeToStartsOrEnds).
func TestBrowseFromTimestampDrivesForwardEmitterFromResolvedOffset(t *testing.T) {
	r := offsetsForTimestampReader{fakeReader: fiveOffsetFixture(), offsets: map[int32]int64{0: 2}}
	svc := newTestMessageService(t, r, nil)

	var events []BrowseEvent
	spec := BrowseSpec{Mode: ModeFromTimestamp, Partitions: []int32{0}, TimestampMs: 250, Limit: 10}
	err := svc.Browse(context.Background(), "test", "t", spec, collectingEmit(&events, 0))
	require.NoError(t, err)

	msgs := messageEvents(events)
	require.Len(t, msgs, 3, "must start at OffsetsForTimestamp's resolved offset 2, not the partition's default Start=0")
	require.Equal(t, []int64{2, 3, 4}, offsetsOf(msgs), "forward direction: ascending offsets")
}

// TestBrowseToTimestampDrivesBackwardEmitterUpToResolvedOffset proves
// ModeToTimestamp is wired to emitBackward with ends=whatever
// reader.OffsetsForTimestamp resolves (fixture: 3), not the partition's
// default End (5).
func TestBrowseToTimestampDrivesBackwardEmitterUpToResolvedOffset(t *testing.T) {
	r := offsetsForTimestampReader{fakeReader: fiveOffsetFixture(), offsets: map[int32]int64{0: 3}}
	svc := newTestMessageService(t, r, nil)

	var events []BrowseEvent
	spec := BrowseSpec{Mode: ModeToTimestamp, Partitions: []int32{0}, TimestampMs: 350, Limit: 10}
	err := svc.Browse(context.Background(), "test", "t", spec, collectingEmit(&events, 0))
	require.NoError(t, err)

	msgs := messageEvents(events)
	require.Len(t, msgs, 3, "must stop at OffsetsForTimestamp's resolved offset 3 exclusive, not the partition's default End=5")
	require.Equal(t, []int64{2, 1, 0}, offsetsOf(msgs), "backward direction: descending offsets")
}

// --- P1c Task 10: BrowseSpec.FilterCode inline-CEL hook ---
//
// v1's getTopicMessages (api's thin seek adapter, Task 10) has no
// RegisterFilter endpoint to lean on yet (that's Task 12) -- so a raw
// CEL_SCRIPT `q` value is compiled inline via filters.Compile and fed into
// the exact same filter step SmartFilterID already uses. The three tests
// below pin: the compiled predicate applies identically to the
// SmartFilterID path (reusing ③'s containsXOrBoomPredicate fixture,
// including its eval-error-doesn't-abort behavior), a Compile error is
// surfaced *before* Browse's first EventPhase emit (so api's handler can
// still turn it into an ordinary pre-stream error response, never a
// half-open SSE stream), and SmartFilterID takes precedence whenever both
// are somehow set (mutual exclusivity, per this task's brief).

// TestBrowseInlineCELFilterCodeAppliesCompiledPredicate mirrors
// TestBrowseSmartFilterMatchesAndCountsEvalErrorsWithoutAborting's fixture
// exactly, but drives the predicate through spec.FilterCode (engine.Compile)
// instead of spec.SmartFilterID (engine.Predicate) -- same fixture, same
// expected outcome, proving the two paths feed the same downstream filter
// step.
func TestBrowseInlineCELFilterCodeAppliesCompiledPredicate(t *testing.T) {
	r := newFakeReader(10).
		withRange(0, 0, 4).
		withRecords(0,
			msgRec(0, 0, 100, "has-x"),  // matches predicate
			msgRec(0, 1, 200, "no-hit"), // no match
			msgRec(0, 2, 300, "boom"),   // Eval errors -- must count, not abort
			msgRec(0, 3, 400, "also-x"), // matches predicate
		)
	engine := newFakeFilterEngine()
	engine.compilePred = containsXOrBoomPredicate{}
	svc := newTestMessageService(t, r, engine)

	var events []BrowseEvent
	spec := BrowseSpec{Mode: ModeEarliest, Limit: 10, FilterCode: "record.value.contains('x')"}
	err := svc.Browse(context.Background(), "test", "t", spec, collectingEmit(&events, 0))
	require.NoError(t, err)

	msgs := messageEvents(events)
	require.Len(t, msgs, 2, "only offsets 0 and 3 contain 'x'; offset 2's eval error must exclude it, not abort the browse")
	require.Equal(t, int64(0), msgs[0].Message.Offset)
	require.Equal(t, int64(3), msgs[1].Message.Offset)

	var lastConsuming *ConsumingStats
	for _, e := range events {
		if e.Kind == EventConsuming {
			lastConsuming = e.Consuming
		}
	}
	require.NotNil(t, lastConsuming)
	require.Equal(t, int32(1), lastConsuming.FilterApplyErrors)
}

// TestBrowseInlineCELCompileErrorIsPreStreamError proves a FilterCode
// compile failure is returned before Browse's first EventPhase send: emit
// must never be called at all (not even once), matching the SmartFilterID
// resolution block's placement -- both run strictly before buildEmitSpec and
// the unconditional EventPhase emit that follows it.
func TestBrowseInlineCELCompileErrorIsPreStreamError(t *testing.T) {
	r := newFakeReader(10).withRange(0, 0, 1).withRecords(0, msgRec(0, 0, 100, "m0"))
	engine := newFakeFilterEngine()
	compileErr := errors.New("bad CEL syntax")
	engine.compileErr = compileErr
	svc := newTestMessageService(t, r, engine)

	emitCalled := false
	spec := BrowseSpec{Mode: ModeEarliest, Limit: 10, FilterCode: "not valid cel($$$"}
	err := svc.Browse(context.Background(), "test", "t", spec, func(BrowseEvent) error {
		emitCalled = true
		return nil
	})

	require.ErrorIs(t, err, compileErr)
	require.False(t, emitCalled, "a Compile error must be returned before the first EventPhase send -- no send may occur")
}

// TestBrowseSmartFilterIDTakesPrecedenceOverFilterCode locks the mutual-
// exclusivity rule the brief states explicitly: when a caller somehow sets
// both, SmartFilterID wins and FilterCode's Compile path is never even
// consulted -- proven by rigging engine.compileErr to a value that would
// fail the whole Browse if Compile were ever called, and asserting Browse
// still succeeds using the registered SmartFilterID predicate instead.
func TestBrowseSmartFilterIDTakesPrecedenceOverFilterCode(t *testing.T) {
	r := newFakeReader(10).
		withRange(0, 0, 2).
		withRecords(0, msgRec(0, 0, 100, "has-x"), msgRec(0, 1, 200, "no-hit"))
	engine := newFakeFilterEngine()
	engine.preds["pred1"] = containsXOrBoomPredicate{}
	engine.compileErr = errors.New("must not be reached: FilterCode must be ignored when SmartFilterID is set")
	svc := newTestMessageService(t, r, engine)

	var events []BrowseEvent
	spec := BrowseSpec{Mode: ModeEarliest, Limit: 10, SmartFilterID: "pred1", FilterCode: "ignored"}
	err := svc.Browse(context.Background(), "test", "t", spec, collectingEmit(&events, 0))
	require.NoError(t, err)

	msgs := messageEvents(events)
	require.Len(t, msgs, 1)
	require.Equal(t, int64(0), msgs[0].Message.Offset)
}

// --- P1c Task 11: Send/Delete (fake MessageWriterPort) ---

// fakeWriter is a deterministic fake of domain/cluster.MessageWriterPort:
// Produce/DeleteRecords each record their call's arguments (so tests can
// assert exactly what MessageService.Send/Delete handed the port) and
// report produceErr/deleteErr when non-nil (both nil = always succeed).
type fakeWriter struct {
	produceErr error
	deleteErr  error

	produceCalled    bool
	lastProduceDef   cluster.Definition
	lastProduceTopic string
	lastProduceRec   cluster.ProduceRecord

	deleteCalled         bool
	lastDeleteDef        cluster.Definition
	lastDeleteTopic      string
	lastDeletePartitions []int32
}

func (f *fakeWriter) Produce(_ context.Context, def cluster.Definition, topic string, rec cluster.ProduceRecord) error {
	f.produceCalled = true
	f.lastProduceDef = def
	f.lastProduceTopic = topic
	f.lastProduceRec = rec
	return f.produceErr
}

func (f *fakeWriter) DeleteRecords(_ context.Context, def cluster.Definition, topic string, partitions []int32) error {
	f.deleteCalled = true
	f.lastDeleteDef = def
	f.lastDeleteTopic = topic
	f.lastDeletePartitions = partitions
	return f.deleteErr
}

// newTestMessageServiceForWrite wires a minimal MessageService for Send/
// Delete tests: only res/writer/serdes are populated -- Send/Delete never
// touch reader/filters/cursors/maskers (see their own doc comments), so
// routing through the full newTestMessageService/NewMessageService
// constructor (which demands a reader this suite has no use for) would add
// nothing.
func newTestMessageServiceForWrite(writer cluster.MessageWriterPort, provider serde.Provider) *MessageService {
	def := cluster.Definition{Name: "test"}
	res := NewResolver([]cluster.Definition{def})
	return &MessageService{res: res, writer: writer, serdes: provider}
}

// TestSendSerializesKeyAndValueAndPassesPartitionThrough proves Send's happy
// path end to end: both Key/Value text run through the (Suggest-preferred,
// since SendSpec.KeySerde/ValueSerde are left empty here) fake serde's
// Serialize, and the resulting bytes -- along with Partition and Headers
// verbatim -- reach the MessageWriterPort's Produce call unchanged.
func TestSendSerializesKeyAndValueAndPassesPartitionThrough(t *testing.T) {
	fw := &fakeWriter{}
	svc := newTestMessageServiceForWrite(fw, fakeSerdeProvider{})

	spec := SendSpec{
		Partition: 2,
		Key:       strPtr("k1"),
		Value:     strPtr("v1"),
		Headers:   map[string]string{"h": "hv"},
	}
	err := svc.Send(context.Background(), "test", "t", spec)
	require.NoError(t, err)

	require.True(t, fw.produceCalled)
	require.Equal(t, "t", fw.lastProduceTopic)
	require.Equal(t, int32(2), fw.lastProduceRec.Partition)
	require.Equal(t, []byte("k1"), fw.lastProduceRec.Key)
	require.Equal(t, []byte("v1"), fw.lastProduceRec.Value)
	require.Equal(t, map[string]string{"h": "hv"}, fw.lastProduceRec.Headers)
}

// TestSendInt64ValueSerdeProducesBigEndianBytes proves Send actually runs
// the resolved serde's Serialize -- not just wraps the text as raw bytes --
// by resolving to a fake "Int64" serde whose Serialize mirrors a real
// fixed-width numeric codec (8-byte big-endian), and asserting the exact
// wire bytes Produce receives.
func TestSendInt64ValueSerdeProducesBigEndianBytes(t *testing.T) {
	int64Serde := fakeSerde{
		name: "Int64",
		serialize: func(input string) ([]byte, error) {
			n, err := strconv.ParseInt(input, 10, 64)
			if err != nil {
				return nil, err
			}
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, uint64(n))
			return b, nil
		},
	}
	provider := fakeSerdeProvider{
		suggestValue: []serde.Description{{Name: "Int64", Description: "fake int64", Preferred: true}},
		lookup:       map[string]serde.Serde{"Int64": int64Serde},
	}
	fw := &fakeWriter{}
	svc := newTestMessageServiceForWrite(fw, provider)

	spec := SendSpec{Partition: 0, Value: strPtr("42")}
	err := svc.Send(context.Background(), "test", "t", spec)
	require.NoError(t, err)

	require.True(t, fw.produceCalled)
	want := make([]byte, 8)
	binary.BigEndian.PutUint64(want, 42)
	require.Equal(t, want, fw.lastProduceRec.Value)
}

// TestSendKeyNilDoesNotSerializeOrSend proves SendSpec.Key == nil ("don't
// send this side of the record at all") reaches Produce as a genuinely nil
// ProduceRecord.Key -- not an empty-but-non-nil []byte{}, and without ever
// resolving or invoking a serde for it (proven by rigging the key serde's
// Serialize to always fail: if it were ever called, this test would fail).
func TestSendKeyNilDoesNotSerializeOrSend(t *testing.T) {
	provider := fakeSerdeProvider{
		suggestKey: []serde.Description{{Name: "AlwaysFails", Description: "must never run", Preferred: true}},
		lookup: map[string]serde.Serde{
			"AlwaysFails": fakeSerde{name: "AlwaysFails", serialize: func(string) ([]byte, error) {
				return nil, errors.New("must never be called: Key is nil")
			}},
			// Value's suggestValue defaults to "FakeString" (fakeSerdeProvider.Suggest's
			// own nil-fallback) -- since a non-nil lookup map replaces the
			// Lookup method's default fallback entirely (not just extends it),
			// "FakeString" must be registered here too or Value's resolveSerde
			// would itself fail to resolve.
			"FakeString": fakeSerde{},
		},
	}
	fw := &fakeWriter{}
	svc := newTestMessageServiceForWrite(fw, provider)

	spec := SendSpec{Partition: 0, Key: nil, Value: strPtr("v1")}
	err := svc.Send(context.Background(), "test", "t", spec)
	require.NoError(t, err)

	require.True(t, fw.produceCalled)
	require.Nil(t, fw.lastProduceRec.Key)
	require.Equal(t, []byte("v1"), fw.lastProduceRec.Value)
}

// TestSendSerializeFailureReturnsErrSerialize covers both ways
// serializeField can fail -- an explicit serde name Lookup doesn't
// recognize (no serde resolved at all), and a resolved serde whose
// Serialize call itself errors -- asserting both report ErrSerialize via
// errors.Is (not just a similarly-worded plain error) and, in each case,
// that Produce is never called: a serialize failure must abort Send before
// it ever reaches the MessageWriterPort.
func TestSendSerializeFailureReturnsErrSerialize(t *testing.T) {
	t.Run("explicit serde name not found", func(t *testing.T) {
		fw := &fakeWriter{}
		svc := newTestMessageServiceForWrite(fw, fakeSerdeProvider{})

		spec := SendSpec{Partition: 0, Value: strPtr("v1"), ValueSerde: "NoSuchSerde"}
		err := svc.Send(context.Background(), "test", "t", spec)
		require.ErrorIs(t, err, ErrSerialize)
		require.False(t, fw.produceCalled)
	})

	t.Run("resolved serde Serialize errors", func(t *testing.T) {
		boom := errors.New("boom: not a valid int64")
		provider := fakeSerdeProvider{
			suggestValue: []serde.Description{{Name: "Int64", Description: "fake int64", Preferred: true}},
			lookup: map[string]serde.Serde{
				"Int64": fakeSerde{name: "Int64", serialize: func(string) ([]byte, error) { return nil, boom }},
			},
		}
		fw := &fakeWriter{}
		svc := newTestMessageServiceForWrite(fw, provider)

		spec := SendSpec{Partition: 0, Value: strPtr("not-a-number")}
		err := svc.Send(context.Background(), "test", "t", spec)
		require.ErrorIs(t, err, ErrSerialize)
		require.False(t, fw.produceCalled)
	})
}

// TestSendUnknownClusterReturnsErrUnknownCluster proves Send resolves the
// cluster name before touching serde/writer at all -- an unknown name
// reports ErrUnknownCluster (via errors.Is, Resolver.Lookup's own sentinel)
// and never calls Produce.
func TestSendUnknownClusterReturnsErrUnknownCluster(t *testing.T) {
	fw := &fakeWriter{}
	svc := newTestMessageServiceForWrite(fw, fakeSerdeProvider{})

	spec := SendSpec{Partition: 0, Value: strPtr("v1")}
	err := svc.Send(context.Background(), "nope", "t", spec)
	require.ErrorIs(t, err, ErrUnknownCluster)
	require.False(t, fw.produceCalled)
}

// TestSendWriterErrorPropagates proves a MessageWriterPort.Produce failure
// (e.g. a real broker-side produce error) propagates out of Send unchanged
// -- Send neither swallows nor rewraps it into ErrSerialize or anything
// else.
func TestSendWriterErrorPropagates(t *testing.T) {
	boom := errors.New("boom: broker rejected produce")
	fw := &fakeWriter{produceErr: boom}
	svc := newTestMessageServiceForWrite(fw, fakeSerdeProvider{})

	spec := SendSpec{Partition: 0, Value: strPtr("v1")}
	err := svc.Send(context.Background(), "test", "t", spec)
	require.ErrorIs(t, err, boom)
}

// TestDeletePassesThroughPartitions proves Delete hands topic/partitions to
// the MessageWriterPort's DeleteRecords unchanged, including the
// empty-means-all convention (a nil/empty partitions slice passed straight
// through, not defaulted to something else here -- MessageWriterPort's own
// doc comment is where "empty means all" is actually implemented).
func TestDeletePassesThroughPartitions(t *testing.T) {
	fw := &fakeWriter{}
	svc := newTestMessageServiceForWrite(fw, fakeSerdeProvider{})

	err := svc.Delete(context.Background(), "test", "t", []int32{0, 2})
	require.NoError(t, err)

	require.True(t, fw.deleteCalled)
	require.Equal(t, "t", fw.lastDeleteTopic)
	require.Equal(t, []int32{0, 2}, fw.lastDeletePartitions)
}

// TestDeleteUnknownClusterReturnsErrUnknownCluster mirrors
// TestSendUnknownClusterReturnsErrUnknownCluster for Delete.
func TestDeleteUnknownClusterReturnsErrUnknownCluster(t *testing.T) {
	fw := &fakeWriter{}
	svc := newTestMessageServiceForWrite(fw, fakeSerdeProvider{})

	err := svc.Delete(context.Background(), "nope", "t", []int32{0})
	require.ErrorIs(t, err, ErrUnknownCluster)
	require.False(t, fw.deleteCalled)
}

// TestDeleteWriterErrorPropagates mirrors TestSendWriterErrorPropagates for
// Delete.
func TestDeleteWriterErrorPropagates(t *testing.T) {
	boom := errors.New("boom: broker rejected delete records")
	fw := &fakeWriter{deleteErr: boom}
	svc := newTestMessageServiceForWrite(fw, fakeSerdeProvider{})

	err := svc.Delete(context.Background(), "test", "t", nil)
	require.ErrorIs(t, err, boom)
}

// strPtr is a tiny *string literal helper -- SendSpec.Key/Value are *string
// (nil vs pointer-to-empty-string is load-bearing, see SendSpec's doc
// comment), and Go has no address-of-a-literal syntax.
func strPtr(s string) *string { return &s }

// fakeRefresher records the cluster names Send/Delete asked to re-scrape.
type fakeRefresher struct {
	names []string
	err   error
}

func (f *fakeRefresher) RefreshWithoutInvalidate(_ context.Context, name string) (cluster.RuntimeState, error) {
	f.names = append(f.names, name)
	return cluster.RuntimeState{}, f.err
}

func newTestMessageServiceForWriteWithRefresher(writer cluster.MessageWriterPort, provider serde.Provider, refresher stateRefresher) *MessageService {
	res := NewResolver([]cluster.Definition{{Name: "test"}})
	return &MessageService{res: res, writer: writer, serdes: provider, refresher: refresher}
}

// TestSendRefreshesStateWithoutInvalidateAfterProduce proves a successful produce
// nudges the state cache so the topic's messagesCount catches up (read-your-
// writes), P1c Task 16's refresh-without-invalidate fix.
func TestSendRefreshesStateWithoutInvalidateAfterProduce(t *testing.T) {
	fr := &fakeRefresher{}
	svc := newTestMessageServiceForWriteWithRefresher(&fakeWriter{}, fakeSerdeProvider{}, fr)

	require.NoError(t, svc.Send(context.Background(), "test", "t", SendSpec{Partition: 0, Value: strPtr("v")}))
	require.Equal(t, []string{"test"}, fr.names, "a successful produce must refresh the cluster's cached counts")
}

// TestSendDoesNotRefreshWhenProduceFails proves the refresh only fires on
// success -- a failed produce leaves the cache untouched.
func TestSendDoesNotRefreshWhenProduceFails(t *testing.T) {
	fr := &fakeRefresher{}
	svc := newTestMessageServiceForWriteWithRefresher(&fakeWriter{produceErr: errors.New("boom")}, fakeSerdeProvider{}, fr)

	require.Error(t, svc.Send(context.Background(), "test", "t", SendSpec{Partition: 0, Value: strPtr("v")}))
	require.Empty(t, fr.names, "a failed produce must not refresh")
}

// TestDeleteRefreshesStateWithoutInvalidateAfterPurge is Delete's counterpart.
func TestDeleteRefreshesStateWithoutInvalidateAfterPurge(t *testing.T) {
	fr := &fakeRefresher{}
	svc := newTestMessageServiceForWriteWithRefresher(&fakeWriter{}, fakeSerdeProvider{}, fr)

	require.NoError(t, svc.Delete(context.Background(), "test", "t", []int32{0}))
	require.Equal(t, []string{"test"}, fr.names)
}

// TestSendRefreshErrorDoesNotFailTheProduce proves a refresh failure is
// swallowed (best-effort) -- the produce already succeeded, so Send still
// returns nil.
func TestSendRefreshErrorDoesNotFailTheProduce(t *testing.T) {
	fr := &fakeRefresher{err: errors.New("scrape boom")}
	svc := newTestMessageServiceForWriteWithRefresher(&fakeWriter{}, fakeSerdeProvider{}, fr)

	require.NoError(t, svc.Send(context.Background(), "test", "t", SendSpec{Partition: 0, Value: strPtr("v")}),
		"a refresh failure must not fail a produce that already succeeded")
}
