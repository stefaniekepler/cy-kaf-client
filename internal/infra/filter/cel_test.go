package filter

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	domainfilter "github.com/cy-kaf/cy-kaf-client/internal/domain/filter"
)

// idOf independently computes the brief's id derivation (sha256(code)'s
// first 8 hex characters) so tests assert against a computation that
// doesn't just echo Register's own output.
func idOf(code string) string {
	sum := sha256.Sum256([]byte(code))
	return hex.EncodeToString(sum[:])[:8]
}

// TestRegisterIDIsStableSha256Prefix covers brief case 1: Register returns
// an 8-hex-digit id equal to sha256(code)[:8], and registering the same
// code twice returns the identical id -- idempotent, not just "some" stable
// value. The frontend's register -> id -> smartFilterId round trip depends
// on this determinism.
func TestRegisterIDIsStableSha256Prefix(t *testing.T) {
	eng := NewEngine()
	code := "record.value == 'hit'"

	id1, err := eng.Register(code)
	require.NoError(t, err)
	require.Len(t, id1, 8)
	require.Equal(t, idOf(code), id1)

	id2, err := eng.Register(code)
	require.NoError(t, err)
	require.Equal(t, id1, id2, "registering identical code twice must be idempotent")
}

// TestRegisteredPredicateEvaluatesValueEquality covers brief case 2: the
// predicate registered for `record.value == 'hit'` matches a record whose
// Value is "hit" and rejects one whose Value is "miss".
func TestRegisteredPredicateEvaluatesValueEquality(t *testing.T) {
	eng := NewEngine()
	id, err := eng.Register("record.value == 'hit'")
	require.NoError(t, err)

	pred, ok := eng.Predicate(id)
	require.True(t, ok)

	got, err := pred.Eval(domainfilter.Record{Value: "hit"})
	require.NoError(t, err)
	require.True(t, got)

	got, err = pred.Eval(domainfilter.Record{Value: "miss"})
	require.NoError(t, err)
	require.False(t, got)
}

// TestHeadersMapAccess covers brief case 3: record.headers['k'] indexes the
// Headers map.
func TestHeadersMapAccess(t *testing.T) {
	eng := NewEngine()
	pred, err := eng.Compile("record.headers['k'] == 'v'")
	require.NoError(t, err)

	got, err := pred.Eval(domainfilter.Record{Headers: map[string]string{"k": "v"}})
	require.NoError(t, err)
	require.True(t, got)
}

// TestNumericFieldsPartitionAndOffset covers brief case 4: partition/offset
// are usable as CEL ints with comparison/boolean operators.
func TestNumericFieldsPartitionAndOffset(t *testing.T) {
	eng := NewEngine()
	pred, err := eng.Compile("record.partition == 0 && record.offset > 5")
	require.NoError(t, err)

	got, err := pred.Eval(domainfilter.Record{Partition: 0, Offset: 6})
	require.NoError(t, err)
	require.True(t, got)

	got, err = pred.Eval(domainfilter.Record{Partition: 0, Offset: 3})
	require.NoError(t, err)
	require.False(t, got)
}

// TestStringsLibContains covers brief case 5: .contains(...) resolves and
// evaluates correctly against a string field, exercising the string
// operator surface the strings extension is meant to align with upstream
// (see NewEngine's doc comment: as of cel-go v0.29.1, contains itself
// already ships in CEL's base standard library too, so this specific
// overload isn't proof ext.Strings() is the *only* thing making it work --
// but the env does enable it, and this locks the observable behavior
// either way).
func TestStringsLibContains(t *testing.T) {
	eng := NewEngine()
	pred, err := eng.Compile("record.value.contains('ell')")
	require.NoError(t, err)

	got, err := pred.Eval(domainfilter.Record{Value: "hello"})
	require.NoError(t, err)
	require.True(t, got)
}

// TestCompileErrorsAreReturnedNotPanicked covers brief case 6: a syntax
// error from both Register and Compile comes back as an error, never a
// panic; a failed Register must not leave anything reachable under the
// code's id (nothing partially cached on failure).
func TestCompileErrorsAreReturnedNotPanicked(t *testing.T) {
	const badCode = "record.value ==="

	eng := NewEngine()

	require.NotPanics(t, func() {
		_, err := eng.Register(badCode)
		require.Error(t, err)
	})

	require.NotPanics(t, func() {
		_, err := eng.Compile(badCode)
		require.Error(t, err)
	})

	_, ok := eng.Predicate(idOf(badCode))
	require.False(t, ok, "a failed Register must not cache anything under the code's id")
}

// TestPredicateUnknownID covers brief case 7.
func TestPredicateUnknownID(t *testing.T) {
	eng := NewEngine()

	pred, ok := eng.Predicate("deadbeef")
	require.False(t, ok)
	require.Nil(t, pred)
}

// TestCompileDoesNotPopulateRegisterCache covers brief case 8: Compile is a
// one-shot -- its result is never reachable via Predicate(id), even though
// the id it would have used is computed identically to Register's.
func TestCompileDoesNotPopulateRegisterCache(t *testing.T) {
	code := "record.value == 'x'"
	eng := NewEngine()

	_, err := eng.Compile(code)
	require.NoError(t, err)

	_, ok := eng.Predicate(idOf(code))
	require.False(t, ok)
}

// TestEvalNonBoolResultErrors covers brief case 9: an expression whose
// result type isn't bool (e.g. bare record.offset, an int) must surface as
// an Eval error, not a silent truthiness coercion.
func TestEvalNonBoolResultErrors(t *testing.T) {
	eng := NewEngine()
	pred, err := eng.Compile("record.offset")
	require.NoError(t, err)

	_, err = pred.Eval(domainfilter.Record{Offset: 42})
	require.Error(t, err)
}

// TestEvalRuntimeErrorMissingHeaderKey covers Eval's *other* error path --
// a genuine CEL runtime evaluation error (p.prg.Eval itself returning err),
// distinct from case 9's "evaluated fine but the result type isn't bool"
// path. Indexing a CEL map with an absent key is a runtime error in CEL
// (unlike a plain Go map read, which would silently zero-value), so a
// filter comparing record.headers['x'] against the empty string must
// itself error when the record has no "x" header, not evaluate to true
// (or false). Worth locking down:
// Task 8/12 callers need to know an absent-header filter errors rather
// than silently matching/not-matching.
func TestEvalRuntimeErrorMissingHeaderKey(t *testing.T) {
	eng := NewEngine()
	pred, err := eng.Compile("record.headers['missing'] == ''")
	require.NoError(t, err)

	_, err = pred.Eval(domainfilter.Record{Headers: map[string]string{"present": "x"}})
	require.Error(t, err)
}

// fakeClock is a manually-advanced clock, injected into a CELEngine's
// unexported `now` field (reachable because this test file shares package
// filter with cel.go) so the two cache-lifecycle tests below don't need
// real sleeps to observe TTL expiry deterministically.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// TestCacheTTLExpiryEvictsAndSelfHeals exercises the Register cache's TTL
// branch (brief's "TTL + 容量上限防泄漏" requirement, not one of the 9
// numbered cases but still a cache branch the global constraints call out
// as needing coverage): a Register'd predicate is reachable via Predicate
// before its TTL elapses, unreachable once it has, and -- because the
// cache key is a pure function of the code, not a caller-chosen name --
// re-registering identical code after expiry self-heals: it recompiles and
// re-caches under that same id rather than erroring.
func TestCacheTTLExpiryEvictsAndSelfHeals(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	eng := NewEngine(WithCacheTTL(time.Minute))
	eng.now = clock.now

	const code = "record.value == 'x'"
	id, err := eng.Register(code)
	require.NoError(t, err)

	_, ok := eng.Predicate(id)
	require.True(t, ok, "must be found before its TTL elapses")

	clock.advance(2 * time.Minute)

	_, ok = eng.Predicate(id)
	require.False(t, ok, "must be evicted once its TTL has elapsed")

	id2, err := eng.Register(code)
	require.NoError(t, err)
	require.Equal(t, id, id2, "re-registering identical code must self-heal under the same id")

	_, ok = eng.Predicate(id2)
	require.True(t, ok, "re-registering after expiry must repopulate the cache")
}

// TestCacheCapacityEvictsOldestWhenFull exercises the Register cache's
// capacity branch: once at capacity, Register evicts the single
// soonest-to-expire (== oldest inserted, since ttl is engine-wide/uniform)
// entry to make room, never erroring the new registration and never
// evicting a newer entry instead.
func TestCacheCapacityEvictsOldestWhenFull(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	eng := NewEngine(WithCacheCapacity(2))
	eng.now = clock.now

	id1, err := eng.Register("record.value == 'a'")
	require.NoError(t, err)
	clock.advance(time.Second)
	id2, err := eng.Register("record.value == 'b'")
	require.NoError(t, err)
	clock.advance(time.Second)
	id3, err := eng.Register("record.value == 'c'")
	require.NoError(t, err)

	_, ok := eng.Predicate(id1)
	require.False(t, ok, "oldest entry must be evicted once capacity is exceeded")
	_, ok = eng.Predicate(id2)
	require.True(t, ok, "newer entries must survive the eviction")
	_, ok = eng.Predicate(id3)
	require.True(t, ok, "the just-registered entry must always survive")
}

// TestRegisterSweepsExpiredEntriesOnNextRegister exercises
// evictExpiredLocked's proactive sweep specifically -- as opposed to
// Predicate's own separate lazy per-lookup expiry check (already covered by
// TestCacheTTLExpiryEvictsAndSelfHeals). Registering a second, different
// code after the first one's TTL has lapsed must shrink the cache back
// down to just the new entry, proving the sweep actually deleted the
// expired one during Register rather than leaving it to linger until some
// later Predicate call happens to notice.
func TestRegisterSweepsExpiredEntriesOnNextRegister(t *testing.T) {
	clock := &fakeClock{t: time.Now()}
	eng := NewEngine(WithCacheTTL(time.Minute))
	eng.now = clock.now

	idA, err := eng.Register("record.value == 'a'")
	require.NoError(t, err)

	clock.advance(2 * time.Minute)

	idB, err := eng.Register("record.value == 'b'")
	require.NoError(t, err)

	require.Len(t, eng.cache, 1, "evictExpiredLocked must sweep the expired entry during Register, not just leave it for a later lazy check")

	_, ok := eng.Predicate(idA)
	require.False(t, ok)
	_, ok = eng.Predicate(idB)
	require.True(t, ok)
}

// TestEvalNavigatesJSONValueFields proves record.value is exposed as navigable
// parsed JSON (upstream kafka-ui's smart-filter model), so a filter can reach
// nested fields -- the vendored frontend generates exactly this shape
// (record.value.value.internalValue == N) against a JSON message.
func TestEvalNavigatesJSONValueFields(t *testing.T) {
	eng := NewEngine()
	pred, err := eng.Compile("record.value.value.internalValue == 2")
	require.NoError(t, err)

	hit, err := pred.Eval(domainfilter.Record{Value: `{"name":"Name","value":{"internalValue":2}}`})
	require.NoError(t, err)
	require.True(t, hit, "a JSON value's nested int field must be navigable and comparable")

	miss, err := pred.Eval(domainfilter.Record{Value: `{"name":"Name","value":{"internalValue":3}}`})
	require.NoError(t, err)
	require.False(t, miss)
}

// TestEvalValueAsTextExposesRawString proves valueAsText/keyAsText carry the raw
// deserialized text, so substring filters (.contains) work regardless of whether
// the value parses as JSON.
func TestEvalValueAsTextExposesRawString(t *testing.T) {
	eng := NewEngine()
	pred, err := eng.Compile(`record.valueAsText.contains("internalValue")`)
	require.NoError(t, err)

	hit, err := pred.Eval(domainfilter.Record{Value: `{"value":{"internalValue":2}}`})
	require.NoError(t, err)
	require.True(t, hit, "valueAsText must be the raw JSON string, so contains() sees the field name")
}

// TestEvalNonJSONValueStaysString proves a value that isn't JSON still behaves as
// a plain string for equality/contains -- the existing record.value == 'hit'
// filters must not regress.
func TestEvalNonJSONValueStaysString(t *testing.T) {
	eng := NewEngine()
	pred, err := eng.Compile("record.value == 'hit'")
	require.NoError(t, err)

	hit, err := pred.Eval(domainfilter.Record{Value: "hit"})
	require.NoError(t, err)
	require.True(t, hit, "a non-JSON value must stay a comparable string")
}

// TestEvalJSONScalarValueStaysString proves only structured JSON (objects and
// arrays) is exposed as navigable -- a value that happens to be a bare JSON
// scalar (number/bool/null/quoted-string) must stay the raw string so a
// pre-existing record.value == 'X' filter doesn't silently change type and stop
// matching (code-review finding, P1c Task 17).
func TestEvalJSONScalarValueStaysString(t *testing.T) {
	eng := NewEngine()
	cases := []struct{ code, value string }{
		{"record.value == '42'", "42"},       // bare number must not become double
		{"record.value == 'true'", "true"},   // bare bool must not become bool
		{"record.value == 'null'", "null"},   // bare null must not become CEL null
		{`record.value == '"foo"'`, `"foo"`}, // quoted string keeps its quotes
	}
	for _, tc := range cases {
		pred, err := eng.Compile(tc.code)
		require.NoError(t, err, tc.code)
		hit, err := pred.Eval(domainfilter.Record{Value: tc.value})
		require.NoError(t, err, tc.code)
		require.True(t, hit, "scalar %q must stay a comparable string", tc.value)
	}
}
