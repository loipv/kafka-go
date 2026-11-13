package kafka

import (
	"sync"
	"time"
)

// idempotencyStore remembers processed idempotency keys for one TTL. It is
// per-process memory only: it is lost on restart or when a partition is
// rebalanced to another instance, a failed batch is redelivered whole and
// unfiltered, and memory grows with the number of distinct keys seen within
// one TTL.
//
// ponytail: unbounded map, one write lock per sweep — cap with an LRU or move to an external store if key volume per TTL gets large
type idempotencyStore struct {
	store     map[string]time.Time
	ttl       time.Duration
	mu        sync.RWMutex
	done      chan struct{}
	ticker    *time.Ticker
	closeOnce sync.Once
}

// newIdempotencyStore creates a new idempotency store
func newIdempotencyStore(ttl time.Duration) *idempotencyStore {
	if ttl <= 0 { // time.NewTicker(0) panics — clamp instead
		ttl = defaultIdempotencyTTL
	}
	s := &idempotencyStore{
		store:  make(map[string]time.Time),
		ttl:    ttl,
		done:   make(chan struct{}),
		ticker: time.NewTicker(ttl / 10), // Cleanup every 1/10 of TTL
	}

	go s.cleanup()

	return s
}

// add adds a key to the store
func (s *idempotencyStore) add(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[key] = time.Now()
}

// isDuplicate checks if a key is a duplicate
func (s *idempotencyStore) isDuplicate(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if ts, ok := s.store[key]; ok {
		if time.Since(ts) < s.ttl {
			return true
		}
	}
	return false
}

// close closes the idempotency store. Safe to call more than once.
func (s *idempotencyStore) close() {
	s.closeOnce.Do(func() {
		close(s.done)
		s.ticker.Stop()
	})
}

// cleanup periodically removes expired entries
func (s *idempotencyStore) cleanup() {
	for {
		select {
		case <-s.done:
			return
		case <-s.ticker.C:
			s.mu.Lock()
			now := time.Now()
			for key, ts := range s.store {
				if now.Sub(ts) >= s.ttl {
					delete(s.store, key)
				}
			}
			s.mu.Unlock()
		}
	}
}
