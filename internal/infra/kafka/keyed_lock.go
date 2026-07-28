package kafka

import "sync"

// keyedLocker serializes work per comparable key and reclaims entries after
// the holder and all queued waiters leave.
type keyedLocker[K comparable] struct {
	mu      sync.Mutex
	entries map[K]*keyedLockEntry
}

type keyedLockEntry struct {
	mu   sync.Mutex
	refs int
}

func (l *keyedLocker[K]) lock(key K) func() {
	l.mu.Lock()
	if l.entries == nil {
		l.entries = make(map[K]*keyedLockEntry)
	}
	entry := l.entries[key]
	if entry == nil {
		entry = &keyedLockEntry{}
		l.entries[key] = entry
	}
	entry.refs++
	l.mu.Unlock()

	entry.mu.Lock()
	var once sync.Once
	return func() {
		once.Do(func() {
			entry.mu.Unlock()
			l.mu.Lock()
			entry.refs--
			if entry.refs == 0 {
				delete(l.entries, key)
			}
			l.mu.Unlock()
		})
	}
}
