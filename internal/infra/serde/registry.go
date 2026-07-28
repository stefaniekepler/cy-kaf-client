// Package serde provides the 8 built-in serde.Serde codecs (string, signed/
// unsigned 32/64-bit big-endian integers, base64, hex, and a 16-byte UUID)
// plus a Registry that looks them up by name. All 8 are pure stdlib --
// see uuid.go's doc comment for why UUIDBinary is handwritten instead of
// pulling in a UUID-parsing dependency.
package serde

import "github.com/cy-kaf/cy-kaf-client/internal/domain/serde"

// Registry is the fixed set of built-in serde codecs. Construction is
// explicit (not reflection-discovered): NewRegistry's own literal list is
// both the source of truth for what's registered and, via order, the
// display order callers (e.g. a future getSerdes suggestion list) should
// offer them in.
type Registry struct {
	byName map[string]serde.Serde
	order  []serde.Serde
}

// NewRegistry builds the registry with the 8 built-in codecs, registered in
// the order the brief lists them -- that registration order is also All's
// (and therefore the UI's) display order.
func NewRegistry() *Registry {
	order := []serde.Serde{
		String{},
		Int32{},
		Int64{},
		UInt32{},
		UInt64{},
		Base64{},
		Hex{},
		UUIDBinary{},
	}
	byName := make(map[string]serde.Serde, len(order))
	for _, s := range order {
		byName[s.Name()] = s
	}
	return &Registry{byName: byName, order: order}
}

// Get looks up a registered serde by its Name(). ok is false, and the
// returned serde.Serde nil, when name isn't registered.
func (r *Registry) Get(name string) (serde.Serde, bool) {
	s, ok := r.byName[name]
	return s, ok
}

// All returns every registered serde in registration (display) order.
func (r *Registry) All() []serde.Serde {
	return r.order
}
