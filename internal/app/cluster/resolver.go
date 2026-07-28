package cluster

import (
	"errors"
	"fmt"
	"sync"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// ErrUnknownCluster is returned by Resolver.Lookup (and by everything that
// forwards its result: StateCache.Refresh, BrokerService's four methods) when
// a cluster name isn't present in the configured Definitions.
var ErrUnknownCluster = errors.New("unknown cluster")

// Resolver is the repo's single name->Definition lookup implementation.
// StateCache and BrokerService (and P1b's TopicService/GroupService) resolve
// a cluster name through it instead of each re-walking their own defs slice,
// so "unknown cluster" has exactly one place to get it right.
//
// defs is guarded by mu so the config-wizard reload (Replace, P1c Task 14) can
// swap the whole set while in-flight requests read it concurrently.
type Resolver struct {
	mu   sync.RWMutex
	defs []cluster.Definition
}

func NewResolver(defs []cluster.Definition) *Resolver {
	return &Resolver{defs: defs}
}

// Lookup finds the configured Definition for name, or a wrapped
// ErrUnknownCluster if no Definition is configured under that name.
func (r *Resolver) Lookup(name string) (cluster.Definition, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, d := range r.defs {
		if d.Name == name {
			return d, nil
		}
	}
	return cluster.Definition{}, fmt.Errorf("%w: %q", ErrUnknownCluster, name)
}

// Definitions returns the configured Definitions in their original (config)
// order, e.g. for StateCache's Start/List to iterate in a stable order. The
// returned slice is a fresh copy, so a concurrent Replace can never mutate what
// a caller is iterating.
func (r *Resolver) Definitions() []cluster.Definition {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]cluster.Definition, len(r.defs))
	copy(out, r.defs)
	return out
}

// IsReadOnly reports whether name is a configured cluster with ReadOnly set.
// Unknown clusters report false — this is api's readOnlyGuard middleware's
// data source, and the guard doesn't own 404 semantics; that stays the
// handler layer's job via Lookup/ErrUnknownCluster. Delegates to Lookup for
// its read lock rather than taking its own (RWMutex.RLock is not reentrant).
func (r *Resolver) IsReadOnly(name string) bool {
	d, err := r.Lookup(name)
	return err == nil && d.ReadOnly
}

// Replace swaps the configured Definitions wholesale under the write lock --
// the config-wizard reload's runtime effect (P1c Task 14). After it returns,
// every subsequent Lookup/IsReadOnly/Definitions observes the new set; the
// Reloader pairs this with lifecycle.Invalidate on changed/removed clusters so
// their pooled connections rebuild against the new config.
func (r *Resolver) Replace(defs []cluster.Definition) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.defs = defs
}
