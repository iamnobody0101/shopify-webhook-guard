package ratelimit

import (
	"context"
	"testing"
	"time"
)

func withClock(l *Limiter) *time.Time {
	now := time.Unix(0, 0)
	l.now = func() time.Time { return now }
	return &now
}

func TestAllowBurstThenRefill(t *testing.T) {
	l := New(2, 3) // 2 tokens/s, burst 3
	now := withClock(l)

	for i := 0; i < 3; i++ {
		if !l.Allow("shop-a") {
			t.Fatalf("request %d within burst was denied", i+1)
		}
	}
	if l.Allow("shop-a") {
		t.Fatal("request beyond burst was allowed")
	}
	if !l.Allow("shop-b") {
		t.Fatal("a different shop must have its own bucket")
	}

	*now = now.Add(500 * time.Millisecond) // +1 token
	if !l.Allow("shop-a") {
		t.Fatal("refilled token was not granted")
	}
	if l.Allow("shop-a") {
		t.Fatal("only one token should have refilled")
	}

	*now = now.Add(time.Hour) // refill is capped at burst
	for i := 0; i < 3; i++ {
		if !l.Allow("shop-a") {
			t.Fatalf("request %d after long idle was denied", i+1)
		}
	}
	if l.Allow("shop-a") {
		t.Fatal("tokens exceeded burst after long idle")
	}
}

func TestDisabled(t *testing.T) {
	l := New(0, 1)
	for i := 0; i < 1000; i++ {
		if !l.Allow("shop") {
			t.Fatal("disabled limiter denied a request")
		}
	}
	if l.Len() != 0 {
		t.Fatal("disabled limiter should not track buckets")
	}
}

func TestBurstClampedToOne(t *testing.T) {
	l := New(1, 0)
	withClock(l)
	if !l.Allow("shop") {
		t.Fatal("first request denied with burst clamped to 1")
	}
	if l.Allow("shop") {
		t.Fatal("second immediate request allowed with burst 1")
	}
}

func TestSweepRemovesOnlyFullBuckets(t *testing.T) {
	l := New(2, 3)
	now := withClock(l)
	l.Allow("light") // 2 tokens left
	for i := 0; i < 3; i++ {
		l.Allow("heavy") // 0 tokens left
	}
	*now = now.Add(600 * time.Millisecond) // +1.2 tokens: light full, heavy not

	if n := l.Sweep(); n != 1 {
		t.Fatalf("Sweep() = %d, want 1", n)
	}
	if l.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", l.Len())
	}
}

func TestRunJanitorStops(t *testing.T) {
	l := New(1000, 1)
	l.Allow("shop")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { l.RunJanitor(ctx, time.Millisecond); close(done) }()

	deadline := time.Now().Add(2 * time.Second)
	for l.Len() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("janitor never swept the refilled bucket")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
}
