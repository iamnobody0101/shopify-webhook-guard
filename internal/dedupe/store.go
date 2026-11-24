// Package dedupe detects duplicate webhook deliveries.
//
// Shopify delivers at-least-once: timeouts and retries mean the same delivery
// (same X-Shopify-Webhook-Id) can arrive more than once.
package dedupe

import (
	"context"
	"sync"
	"time"
)

// Store records keys for a TTL. Implementations must be safe for concurrent use.
type Store interface {
	// MarkIfNew atomically records key and reports true if it had not been
	// seen within the TTL. It reports false for a duplicate.
	MarkIfNew(key string) bool
	// Forget removes key, e.g. when a delivery is refused after being marked
	// so that Shopify's retry is not mistaken for a duplicate.
	Forget(key string)
}

// Memory is an in-process Store. State is lost on restart and not shared
// between replicas; a Redis-backed Store is the path to horizontal scaling.
type Memory struct {
	mu   sync.Mutex
	ttl  time.Duration
	now  func() time.Time
	seen map[string]time.Time // key -> expiry
}

// NewMemory returns an in-memory Store whose entries expire after ttl.
func NewMemory(ttl time.Duration) *Memory {
	return &Memory{ttl: ttl, now: time.Now, seen: make(map[string]time.Time)}
}

func (m *Memory) MarkIfNew(key string) bool {
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if exp, ok := m.seen[key]; ok && now.Before(exp) {
		return false
	}
	m.seen[key] = now.Add(m.ttl)
	return true
}

func (m *Memory) Forget(key string) {
	m.mu.Lock()
	delete(m.seen, key)
	m.mu.Unlock()
}

// Len returns the number of stored keys, including expired ones not yet swept.
func (m *Memory) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.seen)
}

// Sweep deletes expired keys and returns how many were removed.
func (m *Memory) Sweep() int {
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for k, exp := range m.seen {
		if !now.Before(exp) {
			delete(m.seen, k)
			n++
		}
	}
	return n
}

// RunJanitor sweeps every interval until ctx is cancelled.
func (m *Memory) RunJanitor(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.Sweep()
		}
	}
}
