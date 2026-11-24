// Package ratelimit provides a per-key token-bucket rate limiter.
//
// The guard keys buckets by shop domain so one very busy (or misbehaving)
// store cannot starve deliveries for every other store.
package ratelimit

import (
	"context"
	"math"
	"sync"
	"time"
)

// Limiter is a set of token buckets, one per key, created lazily.
type Limiter struct {
	mu      sync.Mutex
	rate    float64 // tokens added per second; <= 0 disables limiting
	burst   float64
	now     func() time.Time
	buckets map[string]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

// New returns a Limiter allowing ratePerSecond sustained requests per key with
// bursts of up to burst. A rate <= 0 disables limiting.
func New(ratePerSecond float64, burst int) *Limiter {
	if burst < 1 {
		burst = 1
	}
	return &Limiter{
		rate:    ratePerSecond,
		burst:   float64(burst),
		now:     time.Now,
		buckets: make(map[string]*bucket),
	}
}

// Allow consumes one token for key and reports whether it was available.
func (l *Limiter) Allow(key string) bool {
	if l.rate <= 0 {
		return true
	}
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	} else if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens = math.Min(l.burst, b.tokens+elapsed*l.rate)
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Sweep removes buckets that have refilled completely. Removing a full
// bucket is lossless (a new one starts full), so memory stays bounded by the
// number of recently active keys.
func (l *Limiter) Sweep() int {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for k, b := range l.buckets {
		if b.tokens+now.Sub(b.last).Seconds()*l.rate >= l.burst {
			delete(l.buckets, k)
			n++
		}
	}
	return n
}

// Len returns the number of tracked buckets.
func (l *Limiter) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}

// RunJanitor sweeps every interval until ctx is cancelled.
func (l *Limiter) RunJanitor(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			l.Sweep()
		}
	}
}
