package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cy-kaf/cy-kaf-client/internal/domain/cluster"
)

// This file is package cluster (white-box), not cluster_test: refreshOne's
// freshness guard (Task 2) needs to seed/read StateCache.states directly and
// to invoke the unexported refreshOne deterministically, without exporting
// test-only methods from the production type in state.go. Keeping that
// surface here means it only ever exists in the test binary — production
// code (and even cluster_test's black-box tests) never sees it.

// defA is the shared single-cluster fixture for the tests below.
var defA = cluster.Definition{Name: "a", Conn: cluster.ConnectionSpec{BootstrapServers: []string{"a:1"}}}

// fakeScraper is a more programmable StateScraper double than state_test.go's
// (cluster_test package) fakeState: `next` controls exactly what FetchState
// returns on its next call — including RefreshedAt, which the freshness
// guard compares against — and blockUntilCancel makes FetchState hang until
// ctx is done and then report ctx.Err(), so the "ctx cancel must not
// pollute the cache" case doesn't have to race real timing.
type fakeScraper struct {
	next             cluster.RuntimeState
	blockUntilCancel bool
}

func (f *fakeScraper) FetchState(ctx context.Context, def cluster.Definition) (cluster.RuntimeState, error) {
	if f.blockUntilCancel {
		<-ctx.Done()
		return cluster.RuntimeState{Definition: def, Status: cluster.StatusOffline, Err: "canceled"}, ctx.Err()
	}
	st := f.next
	st.Definition = def
	return st, nil
}
func (f *fakeScraper) Invalidate(string) {}
func (f *fakeScraper) Close()            {}

// newCacheForTest wires fs as both the StateScraper and ClientLifecycle
// (mirrors state_test.go's fakeState doing double duty) behind a
// single-cluster (defA) Resolver, with a 1-hour refresh interval so Start's
// background ticker never fires mid-test — only an explicit
// RefreshOneForTest call exercises refreshOne.
func newCacheForTest(fs *fakeScraper) *StateCache {
	res := NewResolver([]cluster.Definition{defA})
	return NewStateCache(res, fs, fs, time.Hour)
}

// SetForTest directly seeds c's cache for name, bypassing refreshOne.
func (c *StateCache) SetForTest(name string, st cluster.RuntimeState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.states[name] = st
}

// RefreshOneForTest exposes the unexported refreshOne to this file's tests.
func (c *StateCache) RefreshOneForTest(ctx context.Context, d cluster.Definition) cluster.RuntimeState {
	return c.refreshOne(ctx, d)
}

// TestRefreshOneRejectsStaleWrite: a slow, old refresh (its result stamped
// with an earlier RefreshedAt) must not clobber a newer state already in
// place — e.g. a background tick that was in flight when an explicit
// POST /cache Refresh already landed a fresher result.
func TestRefreshOneRejectsStaleWrite(t *testing.T) {
	fs := &fakeScraper{}
	sc := newCacheForTest(fs)
	fresh := cluster.RuntimeState{Definition: defA, Status: cluster.StatusOnline,
		RefreshedAt: time.Now()}
	stale := cluster.RuntimeState{Definition: defA, Status: cluster.StatusOffline,
		RefreshedAt: fresh.RefreshedAt.Add(-time.Minute), Err: "stale probe"}
	sc.SetForTest(defA.Name, fresh) // 已就位的新状态
	fs.next = stale
	sc.RefreshOneForTest(context.Background(), defA) // 触发一次刷新（返回 stale）
	got, _ := sc.Get(context.Background(), defA.Name)
	require.Equal(t, cluster.StatusOnline, got.Status) // 旧结果被拒
}

// TestRefreshOneSkipsCacheOnContextCancel: when the caller's ctx is already
// canceled (process shutdown, request aborted), refreshOne's own FetchState
// error is not trustworthy signal about the cluster itself — it must not
// overwrite the cache with a bogus OFFLINE entry.
func TestRefreshOneSkipsCacheOnContextCancel(t *testing.T) {
	fs := &fakeScraper{blockUntilCancel: true}
	sc := newCacheForTest(fs)
	seed := cluster.RuntimeState{Definition: defA, Status: cluster.StatusOnline, RefreshedAt: time.Now()}
	sc.SetForTest(defA.Name, seed)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sc.RefreshOneForTest(ctx, defA)
	got, _ := sc.Get(context.Background(), defA.Name)
	require.Equal(t, cluster.StatusOnline, got.Status) // 取消不产生 OFFLINE 污染
}
