package dedupe

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fakeClock(m *Memory, start time.Time) *time.Time {
	now := start
	m.now = func() time.Time { return now }
	return &now
}

func TestMarkIfNew(t *testing.T) {
	m := NewMemory(time.Minute)
	now := fakeClock(m, time.Unix(1000, 0))

	if !m.MarkIfNew("a") {
		t.Fatal("first sighting should be new")
	}
	if m.MarkIfNew("a") {
		t.Fatal("second sighting should be a duplicate")
	}
	if !m.MarkIfNew("b") {
		t.Fatal("different key should be new")
	}
	*now = now.Add(59 * time.Second)
	if m.MarkIfNew("a") {
		t.Fatal("still within TTL, should be a duplicate")
	}
	*now = now.Add(time.Second)
	if !m.MarkIfNew("a") {
		t.Fatal("expired key should be new again")
	}
}

func TestForget(t *testing.T) {
	m := NewMemory(time.Hour)
	m.MarkIfNew("a")
	m.Forget("a")
	if !m.MarkIfNew("a") {
		t.Fatal("forgotten key should be new")
	}
}

func TestSweep(t *testing.T) {
	m := NewMemory(time.Minute)
	now := fakeClock(m, time.Unix(0, 0))
	m.MarkIfNew("a")
	m.MarkIfNew("b")
	*now = now.Add(30 * time.Second)
	m.MarkIfNew("c")
	*now = now.Add(31 * time.Second)

	if n := m.Sweep(); n != 2 {
		t.Fatalf("Sweep() = %d, want 2", n)
	}
	if m.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", m.Len())
	}
}

func TestMarkIfNewConcurrent(t *testing.T) {
	m := NewMemory(time.Hour)
	var wins atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if m.MarkIfNew("same-id") {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("%d goroutines won, want exactly 1", wins.Load())
	}
}

func TestRunJanitor(t *testing.T) {
	m := NewMemory(time.Millisecond)
	m.MarkIfNew("a")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.RunJanitor(ctx, time.Millisecond); close(done) }()

	deadline := time.Now().Add(2 * time.Second)
	for m.Len() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("janitor never swept the expired key")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
}
