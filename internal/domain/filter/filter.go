// Package filter defines the pure CEL-filter abstraction: a Predicate that
// evaluates one Kafka message Record to a bool, and an Engine port that
// compiles user-authored CEL filter code into Predicates. Zero third-party
// imports -- enforced by lint (depguard's domain-purity rule), same
// convention as internal/domain/serde: cel-go is a third-party dependency,
// so the Engine implementation that actually embeds it lives in
// internal/infra/filter.
//
// This underpins the smart-filter register/replay round trip: a frontend
// posts CEL source once, gets back a stable id, then replays that id as the
// `smartFilterId` query parameter against a live message stream (Task 8
// consumes Predicate/Engine that way; Task 12 exposes registerFilter /
// executeSmartFilterTest over HTTP).
package filter

// Record is the CEL single-variable `record`'s field model (aligned with
// upstream kafka-ui's smart-filter record shape). Key/Value are already
// deserialized text (whatever the chosen serde.Deserialize produced), not
// raw bytes -- a filter expression never sees the wire format.
type Record struct {
	Key         string
	Value       string
	Headers     map[string]string
	Partition   int32
	Offset      int64
	TimestampMs int64
}

// Predicate is one compiled filter, ready to evaluate against records.
type Predicate interface {
	// Eval reports whether rec matches this predicate. An error return
	// (never a panic) covers any evaluation failure, including the filter
	// expression's result not being a bool.
	Eval(rec Record) (bool, error)
}

// Engine compiles CEL filter code into Predicates, in two flavors: Register
// (compile once, cache the result under a stable id for later Predicate
// lookups -- the registerFilter/smartFilterId round trip) and Compile
// (compile once, use immediately, never cached -- the
// executeSmartFilterTest path, which tries out a filter without polluting
// the id cache).
type Engine interface {
	// Register compiles filterCode and caches the compiled predicate under
	// an id derived deterministically from filterCode (sha256(filterCode)'s
	// first 8 hex characters) -- registering the same code twice returns
	// the same id, idempotently, without recompiling. A compile error is
	// returned (never a panic); nothing is cached in that case.
	Register(filterCode string) (id string, err error)
	// Predicate looks up an id previously returned by Register. ok is false
	// when id is unknown -- including ids that only ever went through
	// Compile, which never populates this cache, and ids evicted by the
	// implementation's cache lifecycle (TTL/capacity).
	Predicate(id string) (Predicate, bool)
	// Compile compiles filterCode once for immediate use (e.g.
	// executeSmartFilterTest) without caching it under Register's id ->
	// Predicate lookup table.
	Compile(filterCode string) (Predicate, error)
}
