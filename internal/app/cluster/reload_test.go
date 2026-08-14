package cluster_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// recordingLifecycle is a cluster.ClientLifecycle that records every Invalidate
// name, so the Reloader tests can assert exactly which clusters' connections
// were dropped.
type recordingLifecycle struct {
	mu          sync.Mutex
	invalidated []string
}

func (r *recordingLifecycle) Invalidate(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.invalidated = append(r.invalidated, name)
}
func (r *recordingLifecycle) Close() {}
func (r *recordingLifecycle) names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.invalidated...)
}

// stubConfigStore is a cluster.ConfigStorePort whose Validate returns a preset
// verdict/error and whose Save records its call/errors; Current/SaveRelatedFile
// are unused by the Reloader.
type stubConfigStore struct {
	validation  cluster.ConfigValidation
	validateErr error
	saveErr     error

	mu         sync.Mutex
	saveCalled bool
	savedSnap  cluster.ConfigSnapshot
}

func (s *stubConfigStore) Current() (cluster.ConfigSnapshot, error) {
	return cluster.ConfigSnapshot{}, nil
}
func (s *stubConfigStore) Validate(context.Context, cluster.ConfigSnapshot) (cluster.ConfigValidation, error) {
	return s.validation, s.validateErr
}
func (s *stubConfigStore) Save(_ context.Context, snap cluster.ConfigSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveCalled = true
	s.savedSnap = snap
	return s.saveErr
}
func (s *stubConfigStore) SaveRelatedFile(context.Context, string, []byte) (string, error) {
	return "", nil
}
func (s *stubConfigStore) Parse([]byte) (cluster.ConfigSnapshot, error) {
	return cluster.ConfigSnapshot{}, nil
}
func (s *stubConfigStore) Backup() (string, error) { return "", nil }
func (s *stubConfigStore) sawSave() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveCalled
}

func conn(bootstrap string) cluster.ConnectionSpec {
	return cluster.ConnectionSpec{BootstrapServers: []string{bootstrap}}
}

// TestReloaderApplySwapsDefsInvalidatesChangedAndRealignsCache proves the happy
// path: Validate passes -> Resolver holds the new defs, exactly the changed +
// removed clusters are Invalidate'd (unchanged/added are not), and the state
// cache realigns (added scraped, removed dropped).
func TestReloaderApplySwapsDefsInvalidatesChangedAndRealignsCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := appcluster.NewResolver([]cluster.Definition{
		{Name: "keep", Conn: conn("keep:9092")},
		{Name: "changeme", Conn: conn("old:9092")},
		{Name: "removeme", Conn: conn("rm:9092")},
	})
	life := &recordingLifecycle{}
	sc := appcluster.NewStateCache(res, &fakeState{}, life, time.Hour)
	sc.Start(ctx)
	store := &stubConfigStore{} // empty verdict => passes
	rl := appcluster.NewReloader(res, life, sc, store)

	newDefs := []cluster.Definition{
		{Name: "keep", Conn: conn("keep:9092")},    // unchanged
		{Name: "changeme", Conn: conn("new:9092")}, // conn changed
		{Name: "added", Conn: conn("added:9092")},  // new
		// removeme dropped
	}
	require.NoError(t, rl.Apply(ctx, cluster.ConfigSnapshot{Raw: map[string]any{"kafka": map[string]any{}}, Clusters: newDefs}))

	require.True(t, store.sawSave(), "a successful reload must persist the new config")

	d, err := res.Lookup("changeme")
	require.NoError(t, err)
	require.Equal(t, []string{"new:9092"}, d.Conn.BootstrapServers)
	_, err = res.Lookup("removeme")
	require.Error(t, err)
	_, err = res.Lookup("added")
	require.NoError(t, err)

	require.ElementsMatch(t, []string{"changeme", "removeme"}, life.names(),
		"only changed + removed clusters get their connection invalidated")

	require.Eventually(t, func() bool { _, ok := sc.Get(ctx, "added"); return ok },
		time.Second, 5*time.Millisecond, "added cluster must start being scraped")
	require.Eventually(t, func() bool { _, ok := sc.Get(ctx, "removeme"); return !ok },
		time.Second, 5*time.Millisecond, "removed cluster must drop from cache")
}

// TestReloaderApplyUpdatesCachedDefinitionFeaturesImmediately proves an
// ecosystem-only edit (same Kafka connection) is reflected in /api/clusters
// without waiting for the next periodic Kafka scrape.
func TestReloaderApplyUpdatesCachedDefinitionFeaturesImmediately(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := appcluster.NewResolver([]cluster.Definition{{Name: "prod", Conn: conn("prod:9092")}})
	life := &recordingLifecycle{}
	sc := appcluster.NewStateCache(res, &fakeState{}, life, time.Hour)
	sc.Start(ctx)
	require.Eventually(t, func() bool { _, ok := sc.Get(ctx, "prod"); return ok },
		time.Second, 5*time.Millisecond)

	rl := appcluster.NewReloader(res, life, sc, &stubConfigStore{})
	next := cluster.Definition{
		Name:           "prod",
		Conn:           conn("prod:9092"),
		SchemaRegistry: cluster.SchemaRegistrySpec{URL: "http://sr:8081"},
	}
	require.NoError(t, rl.Apply(ctx, cluster.ConfigSnapshot{
		Raw:      map[string]any{"kafka": map[string]any{}},
		Clusters: []cluster.Definition{next},
	}))

	snaps := sc.List(ctx)
	require.Len(t, snaps, 1)
	require.Contains(t, snaps[0].Features, cluster.FeatureSchemaRegistry)
	require.Empty(t, life.names(), "unchanged Kafka connection should not be rebuilt")
}

// TestReloaderApplyRollsBackWhenValidationFails proves an unreachable new config
// leaves the running state completely untouched: defs unchanged, nothing
// invalidated, error returned.
func TestReloaderApplyRollsBackWhenValidationFails(t *testing.T) {
	res := appcluster.NewResolver([]cluster.Definition{{Name: "prod", Conn: conn("prod:9092")}})
	life := &recordingLifecycle{}
	sc := appcluster.NewStateCache(res, &fakeState{}, life, time.Hour)
	store := &stubConfigStore{validation: cluster.ConfigValidation{
		Clusters: map[string]cluster.ClusterValidation{
			"prod": {Kafka: cluster.PropertyValidation{Error: true, ErrorMessage: "dial tcp: unreachable"}},
		},
	}}
	rl := appcluster.NewReloader(res, life, sc, store)

	err := rl.Apply(context.Background(), cluster.ConfigSnapshot{
		Clusters: []cluster.Definition{{Name: "prod", Conn: conn("broken:9092")}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unreachable")

	d, _ := res.Lookup("prod")
	require.Equal(t, []string{"prod:9092"}, d.Conn.BootstrapServers, "rollback: defs must not change")
	require.Empty(t, life.names(), "rollback: no connection may be invalidated")
	require.False(t, store.sawSave(), "rollback: a rejected config must never be persisted")
}

// TestReloaderApplyRollsBackWhenSaveFails proves that a persist failure (after
// validation passed) also leaves the running state untouched -- disk and
// runtime stay consistent, both on the old config.
func TestReloaderApplyRollsBackWhenSaveFails(t *testing.T) {
	res := appcluster.NewResolver([]cluster.Definition{{Name: "prod", Conn: conn("prod:9092")}})
	life := &recordingLifecycle{}
	sc := appcluster.NewStateCache(res, &fakeState{}, life, time.Hour)
	store := &stubConfigStore{saveErr: errors.New("disk full")} // validation passes, Save fails
	rl := appcluster.NewReloader(res, life, sc, store)

	err := rl.Apply(context.Background(), cluster.ConfigSnapshot{
		Raw:      map[string]any{"kafka": map[string]any{}},
		Clusters: []cluster.Definition{{Name: "prod", Conn: conn("new:9092")}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "disk full")

	d, _ := res.Lookup("prod")
	require.Equal(t, []string{"prod:9092"}, d.Conn.BootstrapServers, "save failure must not swap defs")
	require.Empty(t, life.names(), "save failure must not invalidate any connection")
}

// TestReloaderApplyPropagatesValidateError proves an infrastructure error from
// Validate (not a per-cluster verdict) also rolls back.
func TestReloaderApplyPropagatesValidateError(t *testing.T) {
	res := appcluster.NewResolver([]cluster.Definition{{Name: "prod"}})
	life := &recordingLifecycle{}
	sc := appcluster.NewStateCache(res, &fakeState{}, life, time.Hour)
	store := &stubConfigStore{validateErr: errors.New("probe wiring boom")}
	rl := appcluster.NewReloader(res, life, sc, store)

	err := rl.Apply(context.Background(), cluster.ConfigSnapshot{
		Clusters: []cluster.Definition{{Name: "prod2"}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "boom")
	_, err = res.Lookup("prod2")
	require.Error(t, err, "validate error must not swap defs")
	require.Empty(t, life.names())
}

// TestReloaderApplyIsSerializedUnderConcurrency fires many concurrent Apply
// calls (RestartWithConfig runs them on unsynchronized request goroutines),
// half adding cluster "added" and half not, and asserts no data race / panic
// and a consistent final state. With -race this pins the applyMu serialization
// and StateCache.launch idempotency (code-review finding, P1c Task 17): before
// those, two overlapping Applies could double-launch "added"'s scrape goroutine.
func TestReloaderApplyIsSerializedUnderConcurrency(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := appcluster.NewResolver([]cluster.Definition{{Name: "keep", Conn: conn("keep:9092")}})
	life := &recordingLifecycle{}
	sc := appcluster.NewStateCache(res, &fakeState{}, life, time.Hour)
	sc.Start(ctx)
	rl := appcluster.NewReloader(res, life, sc, &stubConfigStore{})

	withAdded := cluster.ConfigSnapshot{Raw: map[string]any{"k": "v"}, Clusters: []cluster.Definition{
		{Name: "keep", Conn: conn("keep:9092")},
		{Name: "added", Conn: conn("added:9092")},
	}}
	onlyKeep := cluster.ConfigSnapshot{Raw: map[string]any{"k": "v"}, Clusters: []cluster.Definition{
		{Name: "keep", Conn: conn("keep:9092")},
	}}

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		snap := withAdded
		if i%2 == 1 {
			snap = onlyKeep
		}
		go func(s cluster.ConfigSnapshot) {
			defer wg.Done()
			require.NoError(t, rl.Apply(ctx, s))
		}(snap)
	}
	wg.Wait()

	// "keep" is always present; the final resolver state is internally consistent
	// (Lookup either finds "added" or not, never a corrupt slice).
	_, err := res.Lookup("keep")
	require.NoError(t, err)
	require.Eventually(t, func() bool { _, ok := sc.Get(ctx, "keep"); return ok },
		time.Second, 5*time.Millisecond)
}
