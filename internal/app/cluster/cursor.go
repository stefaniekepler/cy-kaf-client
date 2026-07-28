// cursor.go implements P1c Task 7's paging cursor cache: a bounded, TTL-
// expiring in-memory store for emitForward/emitBackward's returned "next"
// offset maps, keyed by an opaque random id that a caller (Task 8's
// MessageService) hands back to the client and later resolves through Load
// to resume a forward/backward browse from where the previous page left off.
package cluster

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// cursorEntry is one saved cursor's stored state: a defensive copy of the
// offsets map it was Saved with, plus the wall-clock instant after which
// Load must treat it as gone.
type cursorEntry struct {
	offsets  map[int32]int64
	expireAt time.Time
}

// CursorCache is a bounded, TTL-expiring in-memory map from opaque cursor id
// to a per-partition offsets snapshot. Bounded (capacity, oldest-evicted)
// and TTL-expiring so a long-running process serving many browse sessions
// can't leak memory from cursors a client saves but never resumes.
type CursorCache struct {
	mu       sync.Mutex
	ttl      time.Duration
	capacity int

	entries map[string]cursorEntry
	order   []string // insertion order, oldest first -- drives capacity eviction
}

// NewCursorCache builds a CursorCache whose entries live for ttl and whose
// total entry count never exceeds capacity (capacity <= 0 means unbounded).
func NewCursorCache(ttl time.Duration, capacity int) *CursorCache {
	return &CursorCache{ttl: ttl, capacity: capacity, entries: map[string]cursorEntry{}}
}

// Save stores a defensive copy of next under a freshly generated random id
// (crypto/rand, never math/rand) with this cache's TTL, evicting the oldest
// entry if capacity is now exceeded, and returns the id.
func (c *CursorCache) Save(next map[int32]int64) string {
	id := newCursorID()
	cp := make(map[int32]int64, len(next))
	for p, o := range next {
		cp[p] = o
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[id] = cursorEntry{offsets: cp, expireAt: time.Now().Add(c.ttl)}
	c.order = append(c.order, id)
	if c.capacity > 0 {
		for len(c.order) > c.capacity {
			oldest := c.order[0]
			c.order = c.order[1:]
			delete(c.entries, oldest)
		}
	}
	return id
}

// Load resolves id to a defensive copy of its saved offsets map. A missing
// or TTL-expired id reports (nil, false) -- an expired entry is evicted on
// this Load rather than left for a background sweep, since Load is already
// the only place that reads it.
func (c *CursorCache) Load(id string) (map[int32]int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.entries[id]
	if !ok {
		return nil, false
	}
	if time.Now().After(e.expireAt) {
		delete(c.entries, id)
		c.removeFromOrder(id)
		return nil, false
	}

	cp := make(map[int32]int64, len(e.offsets))
	for p, o := range e.offsets {
		cp[p] = o
	}
	return cp, true
}

// removeFromOrder drops id from c.order, keeping capacity eviction accurate
// after an expired entry is reclaimed early by Load (otherwise a stale id
// would linger in order, making a later Save evict a still-live entry
// instead of the one that's actually oldest).
func (c *CursorCache) removeFromOrder(id string) {
	for i, v := range c.order {
		if v == id {
			c.order = append(c.order[:i], c.order[i+1:]...)
			return
		}
	}
}

// newCursorID generates a 16-byte crypto/rand id, hex-encoded. crypto/rand.Read
// failing is effectively unheard of on a real OS; if it ever does, Save must
// still not crash a live request over an id-generation hiccup, so this falls
// back to a timestamp-derived id rather than panicking.
func newCursorID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return hex.EncodeToString([]byte(time.Now().Format(time.RFC3339Nano)))
	}
	return hex.EncodeToString(b)
}
