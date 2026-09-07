// reload.go is the config wizard's in-process smooth-reload orchestrator (P1c
// Task 14). It lives entirely in the app layer and touches only domain ports
// (cluster.ClientLifecycle, cluster.ConfigStorePort) plus app components
// (*Resolver, *StateCache) -- never infra.Pool directly. A reload swaps the
// Resolver's defs, invalidates changed/removed clusters' pooled connections
// (they rebuild lazily against the new config on next use), and realigns the
// StateCache scrape set. Because every service/Deps holds the same *Resolver /
// *StateCache / ClientLifecycle pointers throughout, none of them need
// rebuilding and there is no atomic Deps swap.
package cluster

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// Reloader orchestrates a smooth reload.
type Reloader struct {
	res       *Resolver
	lifecycle cluster.ClientLifecycle
	states    *StateCache
	store     cluster.ConfigStorePort

	// applyMu serializes Apply: RestartWithConfig calls it directly on the
	// request goroutine with no serialization, so two overlapping PUT /api/config
	// requests would otherwise interleave the Save/Replace/Invalidate/Reload
	// steps (and race StateCache.Reload's check-then-launch). Reload is a
	// rare admin action, so a plain mutex around the whole apply is the simplest
	// correct guard.
	applyMu sync.Mutex
}

// NewReloader wires the reload participants: the name resolver whose defs get
// swapped, the client lifecycle whose changed/removed connections get dropped,
// the state cache whose scrape set gets realigned, and the config store that
// persists the submitted config before anything is applied.
func NewReloader(res *Resolver, lifecycle cluster.ClientLifecycle, states *StateCache, store cluster.ConfigStorePort) *Reloader {
	return &Reloader{res: res, lifecycle: lifecycle, states: states, store: store}
}

// Apply applies snap as the new running configuration, or rolls back entirely.
// Order matters:
//  1. Persist snap to disk without probing Kafka connectivity. Submit is a
//     persistence action: offline new/changed clusters are valid saved
//     configurations, while the explicit Validate action owns connectivity
//     diagnostics. A save failure aborts before any runtime
//     change, so disk and runtime never diverge -- both stay on the old config.
//  2. Swap the Resolver defs (every in-flight request now resolves the new set).
//  3. Invalidate exactly the clusters whose connection config changed or that
//     were removed, so their pooled clients rebuild against the new config (or
//     are simply dropped). Unchanged and newly-added clusters are left alone --
//     added ones have no stale connection to drop.
//  4. Realign the state cache scrape set (start added, stop removed).
func (rl *Reloader) Apply(ctx context.Context, snap cluster.ConfigSnapshot) error {
	rl.applyMu.Lock()
	defer rl.applyMu.Unlock()

	return rl.applyLocked(ctx, snap)
}

// applyLocked requires applyMu, shared by normal saves and reviewed imports.
func (rl *Reloader) applyLocked(ctx context.Context, snap cluster.ConfigSnapshot) error {
	if err := rl.store.Save(ctx, snap); err != nil {
		return fmt.Errorf("persist new config: %w", err)
	}

	stale := changedOrRemoved(rl.res.Definitions(), snap.Clusters)

	rl.res.Replace(snap.Clusters)
	for _, name := range stale {
		rl.lifecycle.Invalidate(name)
	}
	rl.states.Reload(ctx)
	return nil
}

// changedOrRemoved returns the names of clusters whose connection config
// changed between old and next, plus those present in old but gone from next.
// These are exactly the clusters whose pooled connection must be dropped;
// connection identity is ConnectionSpec, so a readOnly/serde/masking-only edit
// (same Conn) is deliberately NOT treated as stale -- no needless reconnect.
func changedOrRemoved(old, next []cluster.Definition) []string {
	nextByName := make(map[string]cluster.Definition, len(next))
	for _, d := range next {
		nextByName[d.Name] = d
	}
	var stale []string
	for _, o := range old {
		n, ok := nextByName[o.Name]
		if !ok || !reflect.DeepEqual(o.Conn, n.Conn) {
			stale = append(stale, o.Name)
		}
	}
	return stale
}
