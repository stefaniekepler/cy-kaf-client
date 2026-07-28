// Package filter is the cel-go-backed implementation of
// internal/domain/filter.Engine: it owns the CEL *cel.Env (a single
// variable `record` filter expressions are evaluated against), compiles
// CEL source into filter.Predicate values, and keeps a bounded,
// TTL-expiring cache of compiled predicates keyed by the registerFilter id
// (sha256(filterCode)[:8]) so a frontend can register a filter once and
// replay its id against a live message stream later (Task 8/12 consume
// this). cel-go is a third-party dependency -- that's why this lives in
// internal/infra rather than internal/domain/filter (depguard's
// domain-purity rule forbids it there), same split as internal/infra/serde
// vs internal/domain/serde.
package filter

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/filter"
)

// Defaults for the Register cache's leak-prevention bounds (spec §7.9):
// content-addressed caching (the key is sha256(code), not a caller-chosen
// name) means an expired or evicted entry is always safe to lose --
// re-registering identical code just recompiles and re-caches under the
// same id, idempotently -- so these defaults favor a generous, rarely-hit
// ceiling over precise tuning.
const (
	defaultCacheTTL      = time.Hour
	defaultCacheCapacity = 1000
)

// Option configures a CELEngine's Register cache. Compile never touches
// that cache, so these only affect the registerFilter/smartFilterId round
// trip's memory footprint, not one-shot executeSmartFilterTest calls.
type Option func(*CELEngine)

// WithCacheTTL overrides the default 1-hour TTL each Register'd predicate
// stays reachable via Predicate before lazy expiry reclaims it.
func WithCacheTTL(ttl time.Duration) Option {
	return func(e *CELEngine) { e.ttl = ttl }
}

// WithCacheCapacity overrides the default 1000-entry cache capacity. Once
// full, Register evicts one entry to make room for a new id -- see
// evictOldestLocked's doc comment for which one and why.
func WithCacheCapacity(capacity int) Option {
	return func(e *CELEngine) { e.capacity = capacity }
}

// cacheEntry is one Register'd predicate plus its expiry.
type cacheEntry struct {
	pred      *celPredicate
	expiresAt time.Time
}

// CELEngine is the cel-go-backed filter.Engine.
type CELEngine struct {
	env      *cel.Env
	ttl      time.Duration
	capacity int
	now      func() time.Time // overridable only by tests in this package (no exported clock knob -- YAGNI beyond that)

	mu    sync.Mutex
	cache map[string]cacheEntry
}

var _ filter.Engine = (*CELEngine)(nil)

// NewEngine builds a CELEngine: one shared cel.Env declaring the `record`
// variable filter expressions are evaluated against, plus an empty,
// TTL/capacity-bounded compiled-predicate cache.
//
// record is declared as map(string, dyn) --
// cel.Variable("record", cel.MapType(cel.StringType, cel.DynType)) --
// rather than a fixed struct/proto type: CEL defines dot-select syntax on a
// string-keyed map (record.key, record.partition, ...) to be equivalent to
// index syntax (record['key']), so this single declaration gives every
// field (key/value/headers/partition/offset/timestampMs) both dot- and
// index-access for free, with the concrete field values supplied per-call
// by Eval's activation (recordActivation), not fixed at env-construction
// time. This is the brief's suggested modeling, confirmed against cel-go
// v0.29.1 by this package's own test suite.
//
// ext.Strings() and ext.Encoders() are enabled so filter expressions can
// use the upstream-aligned string/encoding surface (contains, startsWith,
// matches, base64, ...) beyond bare CEL operators. Note: as of cel-go
// v0.29.1, contains/startsWith/endsWith already ship in CEL's base
// standard library (common/stdlib), so ext.Strings() isn't the sole
// gatekeeper for those three specifically -- but it remains required for
// the rest of the upstream-aligned surface (lowerAscii/upperAscii/split/
// join/replace/trim/...), so both extensions stay enabled per the brief.
func NewEngine(opts ...Option) *CELEngine {
	env, err := cel.NewEnv(
		cel.Variable("record", cel.MapType(cel.StringType, cel.DynType)),
		ext.Strings(),
		ext.Encoders(),
	)
	if err != nil {
		// The env's declarations are fixed, compile-time constants -- no
		// user input is involved in building them. A failure here means
		// this package's own construction is broken (a programming bug),
		// not that any particular filter is bad, so it panics the same
		// way a package-level regexp.MustCompile would rather than
		// forcing every caller to handle an error that can't legitimately
		// occur once this package compiles and its tests pass.
		panic(fmt.Sprintf("filter: building the CEL record env: %v", err))
	}
	e := &CELEngine{
		env:      env,
		ttl:      defaultCacheTTL,
		capacity: defaultCacheCapacity,
		now:      time.Now,
		cache:    make(map[string]cacheEntry),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// computeID derives the brief's stable registerFilter id: the first 8 hex
// characters of sha256(filterCode). Deterministic and content-addressed --
// registering identical code always yields the identical id, and the id
// alone (without the cache) is enough to prove whether two filter strings
// are the same.
func computeID(filterCode string) string {
	sum := sha256.Sum256([]byte(filterCode))
	return hex.EncodeToString(sum[:])[:8]
}

// Register compiles filterCode and caches it under computeID(filterCode),
// returning that id. Re-registering identical code while the cache entry
// is still live is idempotent and cheap: it returns the existing id
// without recompiling. A compile error is returned as-is (never a panic)
// and nothing is cached in that case.
func (e *CELEngine) Register(filterCode string) (string, error) {
	id := computeID(filterCode)

	e.mu.Lock()
	defer e.mu.Unlock()

	if entry, ok := e.cache[id]; ok && e.now().Before(entry.expiresAt) {
		return id, nil
	}

	pred, err := e.compile(filterCode)
	if err != nil {
		return "", err
	}

	e.evictExpiredLocked()
	if e.capacity > 0 && len(e.cache) >= e.capacity {
		e.evictOldestLocked()
	}
	e.cache[id] = cacheEntry{pred: pred, expiresAt: e.now().Add(e.ttl)}
	return id, nil
}

// Predicate looks up a Register'd id. An entry whose TTL has lapsed is
// evicted on the spot and reported as not found, exactly like an id that
// was never registered (or only ever went through Compile, which never
// populates this cache).
func (e *CELEngine) Predicate(id string) (filter.Predicate, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	entry, ok := e.cache[id]
	if !ok {
		return nil, false
	}
	if !e.now().Before(entry.expiresAt) {
		delete(e.cache, id)
		return nil, false
	}
	return entry.pred, true
}

// Compile compiles filterCode for immediate one-shot use (e.g.
// executeSmartFilterTest) without ever touching Register's id cache.
func (e *CELEngine) Compile(filterCode string) (filter.Predicate, error) {
	pred, err := e.compile(filterCode)
	if err != nil {
		return nil, err
	}
	return pred, nil
}

// compile is Register/Compile's shared core: parse+check filterCode
// against e.env, then plan the checked AST into an evaluable cel.Program.
// Both failure modes (checker Issues, program planning) come back as a
// plain error, never a panic.
func (e *CELEngine) compile(filterCode string) (*celPredicate, error) {
	ast, iss := e.env.Compile(filterCode)
	if err := iss.Err(); err != nil {
		return nil, fmt.Errorf("filter: compiling %q: %w", filterCode, err)
	}
	prg, err := e.env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("filter: planning program for %q: %w", filterCode, err)
	}
	return &celPredicate{prg: prg}, nil
}

// evictExpiredLocked drops every cache entry whose TTL has already lapsed.
// Called with e.mu held.
func (e *CELEngine) evictExpiredLocked() {
	now := e.now()
	for id, entry := range e.cache {
		if !now.Before(entry.expiresAt) {
			delete(e.cache, id)
		}
	}
}

// evictOldestLocked drops the single soonest-to-expire entry, making room
// for one new id once the cache is at capacity. Every entry shares the
// same engine-wide ttl (set once, at construction/Option time, never
// per-entry), so "soonest to expire" and "oldest inserted" name the same
// entry -- a plain linear scan (bounded by capacity, so cheap) needs no
// separate insertion-order bookkeeping to get FIFO-under-capacity
// eviction. Called with e.mu held.
func (e *CELEngine) evictOldestLocked() {
	var oldestID string
	var oldestAt time.Time
	found := false
	for id, entry := range e.cache {
		if !found || entry.expiresAt.Before(oldestAt) {
			oldestID, oldestAt, found = id, entry.expiresAt, true
		}
	}
	if found {
		delete(e.cache, oldestID)
	}
}

// celPredicate is the filter.Predicate backed by one compiled cel.Program.
type celPredicate struct {
	prg cel.Program
}

var _ filter.Predicate = (*celPredicate)(nil)

// Eval activates rec as the `record` variable and runs the compiled
// program. A non-bool result (e.g. a filter expression that's just
// `record.offset`) is reported as an error rather than silently coerced --
// the brief requires filter expressions to resolve to an actual boolean.
func (p *celPredicate) Eval(rec filter.Record) (bool, error) {
	out, _, err := p.prg.Eval(recordActivation(rec))
	if err != nil {
		return false, fmt.Errorf("filter: evaluating: %w", err)
	}
	b, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("filter: expression result is %s, not bool", out.Type().TypeName())
	}
	return b, nil
}

// recordActivation maps one domain Record onto the `record` map(string,dyn)
// activation Eval's compiled program expects. Partition/Offset/TimestampMs
// are widened to int64 explicitly (CEL's int type is always 64-bit) rather
// than relying on cel-go's reflective native-value adapter to do it for us.
//
// key/value are exposed as PARSED JSON when the deserialized text is valid
// JSON (an object/array/number/...), else as the raw string -- this matches
// upstream kafka-ui's smart-filter model, where a JSON message's fields are
// navigable (record.value.some.nested.field). keyAsText/valueAsText always
// carry the raw string, so substring filters (record.valueAsText.contains(..))
// work regardless of whether the value happened to parse as JSON.
func recordActivation(rec filter.Record) map[string]any {
	return map[string]any{
		"record": map[string]any{
			"key":         jsonOrString(rec.Key),
			"value":       jsonOrString(rec.Value),
			"keyAsText":   rec.Key,
			"valueAsText": rec.Value,
			"headers":     rec.Headers,
			"partition":   int64(rec.Partition),
			"offset":      rec.Offset,
			"timestampMs": rec.TimestampMs,
		},
	}
}

// jsonOrString returns s parsed as JSON only when it is a JSON object or array
// (so CEL can navigate its structure, e.g. record.value.a.b), else s itself.
// Crucially, a JSON *scalar* -- a bare number/bool/null/quoted-string like "42"
// or "null" -- is NOT unwrapped: it stays the raw string, so a pre-existing
// `record.value == '42'` filter keeps comparing strings instead of silently
// getting a CEL double/bool/null and no longer matching. A bare word like "hit"
// isn't valid JSON at all and likewise stays a string. Filters that want the
// raw text regardless use record.valueAsText/keyAsText.
func jsonOrString(s string) any {
	var v any
	if err := json.Unmarshal([]byte(s), &v); err == nil {
		switch v.(type) {
		case map[string]any, []any:
			return v
		}
	}
	return s
}
