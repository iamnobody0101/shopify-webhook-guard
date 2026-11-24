package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type memDLQ struct {
	mu   sync.Mutex
	recs []DeadLetter
}

func (m *memDLQ) Write(r DeadLetter) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recs = append(m.recs, r)
	return nil
}

func (m *memDLQ) all() []DeadLetter {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]DeadLetter(nil), m.recs...)
}

type counter struct {
	mu sync.Mutex
	m  map[string]int
}

func newCounter() *counter { return &counter{m: map[string]int{}} }

func (c *counter) Inc(s string) {
	c.mu.Lock()
	c.m[s]++
	c.mu.Unlock()
}

func (c *counter) get(s string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[s]
}

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func newTestForwarder(url string, attempts int) (*Forwarder, *memDLQ, *counter) {
	dlq, c := &memDLQ{}, newCounter()
	f := New(Config{
		UpstreamURL: url,
		Workers:     2,
		QueueSize:   10,
		MaxAttempts: attempts,
		BaseBackoff: time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
		Timeout:     2 * time.Second,
	}, dlq, c, discard)
	return f, dlq, c
}

func delivery(id string) Delivery {
	return Delivery{
		WebhookID: id,
		Shop:      "demo-store.myshopify.com",
		Topic:     "orders/create",
		Header: http.Header{
			"Content-Type":    {"application/json"},
			"X-Shopify-Topic": {"orders/create"},
		},
		Body: []byte(`{"id":1}`),
	}
}

func drain(t *testing.T, f *Forwarder) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := f.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestForwardSuccess(t *testing.T) {
	var mu sync.Mutex
	var gotBody []byte
	var gotTopic string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		gotBody, gotTopic = b, r.Header.Get("X-Shopify-Topic")
		mu.Unlock()
	}))
	defer srv.Close()

	f, dlq, c := newTestForwarder(srv.URL, 3)
	f.Start()
	if !f.Enqueue(delivery("wh-1")) {
		t.Fatal("Enqueue failed")
	}
	drain(t, f)

	mu.Lock()
	defer mu.Unlock()
	if string(gotBody) != `{"id":1}` || gotTopic != "orders/create" {
		t.Fatalf("upstream got body=%q topic=%q", gotBody, gotTopic)
	}
	if c.get(MetricForwarded) != 1 || len(dlq.all()) != 0 {
		t.Fatalf("forwarded=%d dlq=%d", c.get(MetricForwarded), len(dlq.all()))
	}
}

func TestForwardRetryBehaviour(t *testing.T) {
	tests := []struct {
		name        string
		responses   []int // status per call; last one repeats
		attempts    int
		wantCalls   int32
		wantDLQ     bool
		wantRetries int
	}{
		{"recovers after 5xx", []int{503, 502, 200}, 5, 3, false, 2},
		{"429 is retried", []int{429, 200}, 5, 2, false, 1},
		{"gives up after max attempts", []int{500}, 3, 3, true, 2},
		{"non-retryable 4xx stops immediately", []int{400}, 5, 1, true, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := int(calls.Add(1)) - 1
				if n >= len(tt.responses) {
					n = len(tt.responses) - 1
				}
				w.WriteHeader(tt.responses[n])
			}))
			defer srv.Close()

			f, dlq, c := newTestForwarder(srv.URL, tt.attempts)
			f.Start()
			f.Enqueue(delivery("wh-1"))
			drain(t, f)

			if calls.Load() != tt.wantCalls {
				t.Errorf("upstream calls = %d, want %d", calls.Load(), tt.wantCalls)
			}
			if got := c.get(MetricRetries); got != tt.wantRetries {
				t.Errorf("retries = %d, want %d", got, tt.wantRetries)
			}
			recs := dlq.all()
			if (len(recs) == 1) != tt.wantDLQ {
				t.Fatalf("dead letters = %d, want dlq=%v", len(recs), tt.wantDLQ)
			}
			if tt.wantDLQ {
				if recs[0].Attempts != int(tt.wantCalls) || !strings.Contains(recs[0].Error, "status") {
					t.Errorf("dead letter = %+v", recs[0])
				}
				if c.get(MetricDeadLettered) != 1 {
					t.Errorf("dead_lettered metric = %d", c.get(MetricDeadLettered))
				}
			}
		})
	}
}

func TestEnqueueWhenFull(t *testing.T) {
	f := New(Config{UpstreamURL: "http://upstream.invalid", QueueSize: 1}, &memDLQ{}, newCounter(), discard)
	// Not started, so nothing drains the queue.
	if !f.Enqueue(delivery("a")) {
		t.Fatal("first Enqueue should succeed")
	}
	if f.Enqueue(delivery("b")) {
		t.Fatal("Enqueue on a full queue should fail fast, not block")
	}
	if f.QueueLen() != 1 {
		t.Fatalf("QueueLen() = %d, want 1", f.QueueLen())
	}
}

func TestEnqueueAfterShutdown(t *testing.T) {
	f, _, _ := newTestForwarder("http://upstream.invalid", 1)
	f.Start()
	drain(t, f)
	if f.Enqueue(delivery("late")) {
		t.Fatal("Enqueue after Shutdown should fail")
	}
	drain(t, f) // second Shutdown must not panic
}

func TestShutdownDeadlineDeadLettersInFlight(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	dlq, c := &memDLQ{}, newCounter()
	f := New(Config{
		UpstreamURL: srv.URL, Workers: 1, QueueSize: 10, MaxAttempts: 1000,
		BaseBackoff: 50 * time.Millisecond, MaxBackoff: 50 * time.Millisecond,
	}, dlq, c, discard)
	f.Start()
	f.Enqueue(delivery("a"))
	f.Enqueue(delivery("b"))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := f.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want DeadlineExceeded", err)
	}
	if got := len(dlq.all()); got != 2 {
		t.Fatalf("dead letters = %d, want 2 (nothing acknowledged may be lost)", got)
	}
}

func TestBackoffBounds(t *testing.T) {
	f := New(Config{BaseBackoff: 10 * time.Millisecond, MaxBackoff: 100 * time.Millisecond}, nil, nil, discard)
	for n := 1; n <= 64; n++ {
		if d := f.backoff(n); d < 0 || d > 100*time.Millisecond {
			t.Fatalf("backoff(%d) = %v, out of [0, 100ms]", n, d)
		}
	}
}

func TestStatusErrorRetryable(t *testing.T) {
	for code, want := range map[int]bool{400: false, 401: false, 404: false, 408: true, 429: true, 500: true, 503: true} {
		if got := (&StatusError{Code: code}).Retryable(); got != want {
			t.Errorf("Retryable(%d) = %v, want %v", code, got, want)
		}
	}
}

func TestFileDLQRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dlq.jsonl")
	q, err := OpenFileDLQ(path)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("{\"note\":\"exact bytes \\u00e9 preserved\"}\n")
	for _, id := range []string{"wh-1", "wh-2"} {
		d := delivery(id)
		d.Body = body
		if err := q.Write(DeadLetter{Delivery: d, Error: "boom", Attempts: 3}); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	fh, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()
	var ids []string
	sc := bufio.NewScanner(fh)
	for sc.Scan() {
		var rec DeadLetter
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(rec.Body, body) || rec.Attempts != 3 {
			t.Fatalf("round trip mismatch: %+v", rec)
		}
		ids = append(ids, rec.WebhookID)
	}
	if strings.Join(ids, ",") != "wh-1,wh-2" {
		t.Fatalf("ids = %v", ids)
	}
}

func TestOpenFileDLQError(t *testing.T) {
	if _, err := OpenFileDLQ(filepath.Join(t.TempDir(), "missing-dir", "dlq.jsonl")); err == nil {
		t.Fatal("expected error for missing directory")
	}
}
