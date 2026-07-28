package cluster_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

type fakeState struct {
	calls atomic.Int32
	fail  bool

	// custom/useCustom (Task 8b: write-path cache refresh tests) let a test
	// control what FetchState returns instead of the fixed online/one-broker
	// default below — e.g. to simulate "the cluster now has the topic a
	// Create call just added" for the next forced Refresh to observe,
	// without needing a live cluster. Definition is overwritten with the
	// FetchState call's own def either way, same as the default branch.
	custom    cluster.RuntimeState
	useCustom bool
}

func (f *fakeState) FetchState(_ context.Context, def cluster.Definition) (cluster.RuntimeState, error) {
	f.calls.Add(1)
	if f.fail {
		return cluster.RuntimeState{Definition: def, Status: cluster.StatusOffline,
			Err: "boom"}, errors.New("boom")
	}
	if f.useCustom {
		st := f.custom
		st.Definition = def
		return st, nil
	}
	return cluster.RuntimeState{Definition: def, Status: cluster.StatusOnline,
		Brokers: []cluster.BrokerInfo{{ID: 1}}}, nil
}
func (f *fakeState) Invalidate(string) {}
func (f *fakeState) Close()            {}

func TestStateCacheServesFromCacheAndRefreshes(t *testing.T) {
	fs := &fakeState{}
	res := appcluster.NewResolver([]cluster.Definition{{Name: "a", Conn: cluster.ConnectionSpec{BootstrapServers: []string{"a:1"}}}})
	sc := appcluster.NewStateCache(res, fs, fs, time.Hour) // 长间隔：本测试只吃首轮 + 显式 Refresh
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sc.Start(ctx)

	require.Eventually(t, func() bool { // 首轮后台刷新完成
		_, ok := sc.Get(ctx, "a")
		return ok
	}, 2*time.Second, 10*time.Millisecond)
	before := fs.calls.Load()
	_ = sc.List(ctx)
	_ = sc.List(ctx)
	require.Equal(t, before, fs.calls.Load()) // List 纯读缓存，不触发抓取

	_, err := sc.Refresh(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, before+1, fs.calls.Load()) // Refresh 强制抓取

	_, err = sc.Refresh(ctx, "nope")
	require.Error(t, err) // 未知集群明确报错
}

func TestStateCacheKeepsServingWhenRefreshFails(t *testing.T) {
	fs := &fakeState{fail: true}
	res := appcluster.NewResolver([]cluster.Definition{{Name: "b", Conn: cluster.ConnectionSpec{BootstrapServers: []string{"b:1"}}}})
	sc := appcluster.NewStateCache(res, fs, fs, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sc.Start(ctx)
	require.Eventually(t, func() bool {
		st, ok := sc.Get(ctx, "b")
		return ok && st.Status == cluster.StatusOffline && st.Err == "boom"
	}, 2*time.Second, 10*time.Millisecond)
	snaps := sc.List(ctx)
	require.Len(t, snaps, 1)
	require.Equal(t, cluster.StatusOffline, snaps[0].Status)
}

// syncBuffer is a mutex-guarded bytes.Buffer: slog's own handler serializes
// concurrent Handle calls internally, but this test's goroutine reads the
// buffer while StateCache's background refresh goroutine (started by
// sc.Start) is still writing to it via slog — an unguarded bytes.Buffer would
// race under `go test -race`.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestStateCacheLogsWarnOnRefreshFailure is the one representative slog
// assertion for the hygiene batch's three logging additions (refreshOne's
// Warn, FetchState's logdirs Debug, api's 500 Error) — see task-3-brief.md
// step 3.4: one captured case is enough, not a point-by-point assertion of
// every call site. Captures refreshOne's failed-state-written-back Warn via
// slog.SetDefault, and restores the previous default logger afterwards so
// this test can't leak logging behaviour into any other test in the package.
func TestStateCacheLogsWarnOnRefreshFailure(t *testing.T) {
	var buf syncBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(prev)

	fs := &fakeState{fail: true}
	res := appcluster.NewResolver([]cluster.Definition{{Name: "e", Conn: cluster.ConnectionSpec{BootstrapServers: []string{"e:1"}}}})
	sc := appcluster.NewStateCache(res, fs, fs, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sc.Start(ctx)

	require.Eventually(t, func() bool {
		return strings.Contains(buf.String(), "cluster state refresh failed")
	}, 2*time.Second, 10*time.Millisecond)
	require.Contains(t, buf.String(), "cluster=e")
	require.Contains(t, buf.String(), "err=boom")
}

// blockingState never returns from FetchState until release is closed, so
// tests can deterministically observe cache state *before* the first
// background refresh completes.
type blockingState struct{ release chan struct{} }

func (b *blockingState) FetchState(ctx context.Context, def cluster.Definition) (cluster.RuntimeState, error) {
	select {
	case <-b.release:
	case <-ctx.Done():
	}
	return cluster.RuntimeState{Definition: def, Status: cluster.StatusOnline}, nil
}
func (b *blockingState) Invalidate(string) {}
func (b *blockingState) Close()            {}

// TestStateCacheListDefaultsToOfflineBeforeFirstRefresh covers List's "first
// round not finished yet" branch: an unknown cluster must present as OFFLINE
// rather than being omitted, so /api/clusters never silently drops a row.
func TestStateCacheListDefaultsToOfflineBeforeFirstRefresh(t *testing.T) {
	bs := &blockingState{release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := appcluster.NewResolver([]cluster.Definition{{Name: "c", Conn: cluster.ConnectionSpec{BootstrapServers: []string{"c:1"}}}})
	sc := appcluster.NewStateCache(res, bs, bs, time.Hour)
	sc.Start(ctx)
	t.Cleanup(func() { close(bs.release) }) // unblock the background goroutine so it doesn't leak past the test

	snaps := sc.List(ctx)
	require.Len(t, snaps, 1)
	require.Equal(t, cluster.StatusOffline, snaps[0].Status)
	require.Equal(t, "c", snaps[0].Definition.Name)
}

// TestStateCacheListPreservesConfigOrder locks in List's ordering contract
// (P1a review T2 minor): rows come back in configured Definition order, not
// Go's randomized map-iteration order, regardless of which cluster's
// background refresh happens to land first. "z" is deliberately configured
// before "a" (non-alphabetical) so a coincidental match against alphabetical
// or map-random order can't pass this test by accident.
func TestStateCacheListPreservesConfigOrder(t *testing.T) {
	fs := &fakeState{}
	res := appcluster.NewResolver([]cluster.Definition{
		{Name: "z", Conn: cluster.ConnectionSpec{BootstrapServers: []string{"z:1"}}},
		{Name: "a", Conn: cluster.ConnectionSpec{BootstrapServers: []string{"a:1"}}},
	})
	sc := appcluster.NewStateCache(res, fs, fs, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sc.Start(ctx)

	require.Eventually(t, func() bool { // 两集群首轮刷新都完成
		_, okZ := sc.Get(ctx, "z")
		_, okA := sc.Get(ctx, "a")
		return okZ && okA
	}, 2*time.Second, 10*time.Millisecond)

	snaps := sc.List(ctx)
	require.Len(t, snaps, 2)
	require.Equal(t, "z", snaps[0].Definition.Name)
	require.Equal(t, "a", snaps[1].Definition.Name)
}

// TestStateCacheRefreshesPeriodically covers Start's ticker branch: the given
// TestStateCacheServesFromCacheAndRefreshes/KeepsServingWhenRefreshFails cases
// use a 1-hour interval and only ever exercise the first (pre-loop) refresh,
// never the `case <-t.C` branch. A short interval here proves the background
// loop really does keep refreshing on its own, not just once at startup.
func TestStateCacheRefreshesPeriodically(t *testing.T) {
	fs := &fakeState{}
	res := appcluster.NewResolver([]cluster.Definition{{Name: "d", Conn: cluster.ConnectionSpec{BootstrapServers: []string{"d:1"}}}})
	sc := appcluster.NewStateCache(res, fs, fs, 20*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sc.Start(ctx)

	require.Eventually(t, func() bool {
		return fs.calls.Load() >= 3 // 首轮 + 至少 2 次周期性刷新
	}, 2*time.Second, 5*time.Millisecond)
}

// TestStateCacheReloadStartsAddedClusterAndDropsRemoved proves Reload realigns
// the per-cluster scrape set to the Resolver's current defs: a newly-added
// cluster starts being scraped, and a removed one stops and drops from cache.
func TestStateCacheReloadStartsAddedClusterAndDropsRemoved(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := appcluster.NewResolver([]cluster.Definition{{Name: "a"}})
	fs := &fakeState{}
	sc := appcluster.NewStateCache(res, fs, fs, 15*time.Millisecond)
	sc.Start(ctx)

	require.Eventually(t, func() bool { _, ok := sc.Get(ctx, "a"); return ok },
		time.Second, 5*time.Millisecond, "cluster a must be scraped after Start")

	// Add b, reload -> b starts being scraped.
	res.Replace([]cluster.Definition{{Name: "a"}, {Name: "b"}})
	sc.Reload(ctx)
	require.Eventually(t, func() bool { _, ok := sc.Get(ctx, "b"); return ok },
		time.Second, 5*time.Millisecond, "added cluster b must start being scraped")

	// Remove a, reload -> a stops and drops from cache, stays dropped.
	res.Replace([]cluster.Definition{{Name: "b"}})
	sc.Reload(ctx)
	require.Eventually(t, func() bool { _, ok := sc.Get(ctx, "a"); return !ok },
		time.Second, 5*time.Millisecond, "removed cluster a must drop from cache")
	require.Never(t, func() bool { _, ok := sc.Get(ctx, "a"); return ok },
		150*time.Millisecond, 15*time.Millisecond, "removed cluster a must not reappear on later ticks")
}

// TestStateCacheRefreshWithoutInvalidateReScrapesButKeepsConnection proves the
// light write-after refresh re-scrapes the cluster (so produce/delete counts
// catch up) WITHOUT dropping the pooled connection -- the contrast is the plain
// Refresh, which does invalidate. This is ADR-0005 §3's refresh-without-
// invalidate, wired after message produce/delete (P1c Task 16 fix).
func TestStateCacheRefreshWithoutInvalidateReScrapesButKeepsConnection(t *testing.T) {
	ctx := context.Background()
	res := appcluster.NewResolver([]cluster.Definition{{Name: "prod"}})
	fs := &fakeState{}
	life := &recordingLifecycle{}
	sc := appcluster.NewStateCache(res, fs, life, time.Hour)

	st, err := sc.RefreshWithoutInvalidate(ctx, "prod")
	require.NoError(t, err)
	require.Equal(t, cluster.StatusOnline, st.Status)
	require.GreaterOrEqual(t, int(fs.calls.Load()), 1, "must re-scrape the cluster")
	require.Empty(t, life.names(), "must NOT invalidate the pooled connection (unlike Refresh)")

	_, err = sc.Refresh(ctx, "prod")
	require.NoError(t, err)
	require.Equal(t, []string{"prod"}, life.names(), "plain Refresh, by contrast, does invalidate")
}

// TestStateCacheRefreshWithoutInvalidateUnknownClusterErrors mirrors Refresh's
// unknown-cluster contract.
func TestStateCacheRefreshWithoutInvalidateUnknownClusterErrors(t *testing.T) {
	sc := appcluster.NewStateCache(appcluster.NewResolver([]cluster.Definition{{Name: "prod"}}), &fakeState{}, &recordingLifecycle{}, time.Hour)
	_, err := sc.RefreshWithoutInvalidate(context.Background(), "nope")
	require.ErrorIs(t, err, appcluster.ErrUnknownCluster)
}
