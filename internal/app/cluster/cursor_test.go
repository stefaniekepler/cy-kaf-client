package cluster_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	appcluster "github.com/cy-kaf/cy-kaf-client/internal/app/cluster"
)

// CursorCache is exported (Task 8's MessageService will call it from outside
// this package), so its tests live in the ordinary black-box cluster_test
// package -- same convention as broker_test.go/serde_test.go/group_test.go.

func TestCursorCacheSaveLoadRoundTripReturnsDefensiveCopies(t *testing.T) {
	c := appcluster.NewCursorCache(time.Minute, 10)
	next := map[int32]int64{0: 5, 1: 9}
	id := c.Save(next)

	// Mutating the caller's original map after Save must not affect what's
	// stored.
	next[0] = 999

	got, ok := c.Load(id)
	require.True(t, ok)
	require.Equal(t, map[int32]int64{0: 5, 1: 9}, got)

	// Mutating the map returned by Load must not affect a later Load.
	got[0] = 111
	got2, ok := c.Load(id)
	require.True(t, ok)
	require.Equal(t, map[int32]int64{0: 5, 1: 9}, got2)
}

func TestCursorCacheLoadMissReturnsFalse(t *testing.T) {
	c := appcluster.NewCursorCache(time.Minute, 10)
	_, ok := c.Load("nonexistent")
	require.False(t, ok)
}

// TestCursorCacheExpiresAfterTTL uses a deliberately short TTL (tens of
// milliseconds) so the test stays fast, per the brief's guidance against a
// long time.Sleep.
func TestCursorCacheExpiresAfterTTL(t *testing.T) {
	c := appcluster.NewCursorCache(20*time.Millisecond, 10)
	id := c.Save(map[int32]int64{0: 1})

	_, ok := c.Load(id)
	require.True(t, ok, "must still be present before TTL elapses")

	time.Sleep(60 * time.Millisecond)
	_, ok = c.Load(id)
	require.False(t, ok, "must be treated as a miss once TTL has elapsed")
}

// TestCursorCacheEvictsOldestOverCapacity writes capacity+1 entries and
// asserts the very first (oldest) one is gone while the most recent
// `capacity` entries all remain, with their original offsets intact.
func TestCursorCacheEvictsOldestOverCapacity(t *testing.T) {
	c := appcluster.NewCursorCache(time.Minute, 3)
	ids := make([]string, 4)
	for i := 0; i < 4; i++ {
		ids[i] = c.Save(map[int32]int64{0: int64(i)})
	}

	_, ok := c.Load(ids[0])
	require.False(t, ok, "oldest entry must be evicted once capacity is exceeded")

	for i := 1; i < 4; i++ {
		got, ok := c.Load(ids[i])
		require.True(t, ok, "entry %d must still be present", i)
		require.Equal(t, map[int32]int64{0: int64(i)}, got)
	}
}

func TestCursorCacheSaveGeneratesDistinctIDs(t *testing.T) {
	c := appcluster.NewCursorCache(time.Minute, 10)
	id1 := c.Save(map[int32]int64{0: 1})
	id2 := c.Save(map[int32]int64{0: 2})
	require.NotEqual(t, id1, id2)
	require.NotEmpty(t, id1)
	require.NotEmpty(t, id2)
}
