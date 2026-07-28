// Package serde defines the pure serde abstraction: the strategy interface
// that every codec (built-in, and later custom/schema-registry-backed)
// implements, plus the small value types used to describe and suggest them
// to callers. Zero third-party imports -- enforced by lint (depguard's
// domain-purity rule), same convention as internal/domain/cluster. This
// package does import internal/domain/cluster (for Provider's
// cluster.Definition parameter) -- domain->domain, same layer, allowed by
// depguard's domain-purity rule.
package serde

import "github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"

// Target is which part of a Kafka record a serde is being asked to act on.
type Target int

const (
	TargetKey Target = iota
	TargetValue
)

// Usage is which direction a serde is being asked to act in.
type Usage int

const (
	UsageSerialize Usage = iota
	UsageDeserialize
)

// Serde is a pure (I/O-free) strategy for converting between a Kafka
// record's raw bytes and a human-editable text representation. Built-in
// serdes (internal/infra/serde) can attempt to render/parse any bytes for
// any topic, so their CanDeserialize/CanSerialize are always true and their
// Schema is always ("", false); a schema-registry-backed serde (later work)
// would instead consult topic/t to answer those. Which serde is
// *preferred* for a given topic/target is computed from per-cluster config
// by the Provider port below.
type Serde interface {
	// Name is the serde's stable identifier (e.g. "String", "Int32") --
	// used as the registry lookup key and surfaced to callers/UI as-is.
	Name() string
	// Description is a short, human-readable summary of what this serde
	// does.
	Description() string
	// CanDeserialize reports whether this serde can render topic's t bytes
	// as text.
	CanDeserialize(topic string, t Target) bool
	// CanSerialize reports whether this serde can turn text back into
	// topic's t bytes.
	CanSerialize(topic string, t Target) bool
	// Schema reports a schema description for topic's t, when this serde
	// has one (e.g. an Avro/Protobuf schema fetched from a registry).
	// Built-in serdes never have one: ("", false).
	Schema(topic string, t Target) (string, bool)
	// Serialize converts input text into topic's t raw bytes.
	Serialize(topic string, t Target, input string) ([]byte, error)
	// Deserialize converts topic's t raw bytes into text. Implementations
	// must return an error (never panic) when data's shape doesn't match
	// what this serde expects -- e.g. the wrong byte length for a
	// fixed-width numeric or UUID codec.
	Deserialize(topic string, t Target, data []byte) (string, error)
}

// Param is one configurable property a serde exposes (e.g. a schema
// registry URL), for describing/soliciting it in the UI.
type Param struct {
	Name, VisibleName string
	AllowedValues     []string
}

// Description is one serde's self-description, as surfaced by a
// getSerdes-style suggestion: its name, a human summary, whether it's the
// preferred choice for the topic/target being asked about, its schema (if
// any), and any configurable params.
type Description struct {
	Name, Description string
	Preferred         bool
	Schema            *string
	Params            []Param
}

// Suggestion is the full set of serde suggestions for one topic: which
// serdes can handle its key, and which can handle its value.
type Suggestion struct {
	Key, Value []Description
}

// Provider computes getSerdes-style suggestions and does by-name lookups,
// both scoped to one cluster's config (def). infra/serde.Provider is the
// concrete, built-in-registry-backed implementation; app/cluster.
// SerdeService is this port's sole consumer (resolve a cluster name to a
// Definition, then delegate straight through).
type Provider interface {
	// Suggest reports every candidate serde for topic's key and value, with
	// whichever one candidate cluster config prefers (or, failing that, a
	// fixed fallback) marked Preferred.
	Suggest(def cluster.Definition, topic string, use Usage) Suggestion
	// Lookup finds a serde by its Name(), for later serialize/deserialize
	// calls (Task 8/11) -- ok is false when name isn't known.
	Lookup(def cluster.Definition, name string) (Serde, bool)
}
