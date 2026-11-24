package server

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iamnobody0101/shopify-webhook-guard/internal/dedupe"
	"github.com/iamnobody0101/shopify-webhook-guard/internal/proxy"
	"github.com/iamnobody0101/shopify-webhook-guard/internal/ratelimit"
	"github.com/iamnobody0101/shopify-webhook-guard/internal/verify"
)

const secret = "test-app-secret"

var fixedNow = time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

type fakeQueue struct {
	mu    sync.Mutex
	items []proxy.Delivery
	full  bool
}

func (q *fakeQueue) Enqueue(d proxy.Delivery) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.full {
		return false
	}
	q.items = append(q.items, d)
	return true
}

type fakeCounter struct {
	mu sync.Mutex
	m  map[string]int
}

func (c *fakeCounter) Inc(s string) { c.mu.Lock(); c.m[s]++; c.mu.Unlock() }
func (c *fakeCounter) get(s string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[s]
}

type fixture struct {
	h     *Handler
	queue *fakeQueue
	count *fakeCounter
	store *dedupe.Memory
}

func newFixture(t *testing.T, mod func(*Config)) *fixture {
	t.Helper()
	cfg := Config{MaxBodyBytes: 1024, MaxAge: 5 * time.Minute, FutureSkew: time.Minute}
	if mod != nil {
		mod(&cfg)
	}
	v, err := verify.New([]string{secret})
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{queue: &fakeQueue{}, count: &fakeCounter{m: map[string]int{}}, store: dedupe.NewMemory(time.Hour)}
	f.h = NewHandler(cfg, v, f.store, ratelimit.New(0, 1), f.queue, f.count,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	f.h.now = func() time.Time { return fixedNow }
	return f
}

func (f *fixture) serve(r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	f.h.ServeHTTP(w, r)
	return w
}

// signed builds a request exactly like Shopify would send it.
func signed(body string, mutate func(*http.Request)) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/webhooks", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Shopify-Hmac-Sha256", verify.Sign([]byte(body), secret))
	r.Header.Set("X-Shopify-Shop-Domain", "demo-store.myshopify.com")
	r.Header.Set("X-Shopify-Topic", "orders/create")
	r.Header.Set("X-Shopify-Webhook-Id", "wh-1")
	r.Header.Set("X-Shopify-Triggered-At", fixedNow.Add(-time.Minute).Format(time.RFC3339Nano))
	if mutate != nil {
		mutate(r)
	}
	return r
}

func setHeader(k, v string) func(*http.Request) {
	return func(r *http.Request) { r.Header.Set(k, v) }
}

func delHeader(k string) func(*http.Request) {
	return func(r *http.Request) { r.Header.Del(k) }
}

func TestRejections(t *testing.T) {
	tests := []struct {
		name   string
		req    *http.Request
		status int
		reason string
	}{
		{"wrong method", httptest.NewRequest(http.MethodGet, "/webhooks", nil), 405, "method_not_allowed"},
		{"body too large", signed(strings.Repeat("a", 2048), nil), 413, "body_too_large"},
		{"missing signature", signed(`{}`, delHeader("X-Shopify-Hmac-Sha256")), 401, "missing_signature"},
		{"malformed signature", signed(`{}`, setHeader("X-Shopify-Hmac-Sha256", "not-base64!!")), 401, "malformed_signature"},
		{"wrong secret", signed(`{}`, setHeader("X-Shopify-Hmac-Sha256", verify.Sign([]byte(`{}`), "guess"))), 401, "invalid_signature"},
		{"tampered body", signed(`{"total":"1.00"}`, setHeader("X-Shopify-Hmac-Sha256", verify.Sign([]byte(`{"total":"999.00"}`), secret))), 401, "invalid_signature"},
		{"non-shopify shop domain", signed(`{}`, setHeader("X-Shopify-Shop-Domain", "evil.example.com")), 400, "invalid_shop_domain"},
		{"missing webhook id", signed(`{}`, delHeader("X-Shopify-Webhook-Id")), 400, "invalid_webhook_id"},
		{"oversized webhook id", signed(`{}`, setHeader("X-Shopify-Webhook-Id", strings.Repeat("x", 300))), 400, "invalid_webhook_id"},
		{"missing timestamp", signed(`{}`, delHeader("X-Shopify-Triggered-At")), 400, "invalid_timestamp"},
		{"garbage timestamp", signed(`{}`, setHeader("X-Shopify-Triggered-At", "yesterday")), 400, "invalid_timestamp"},
		{"stale timestamp", signed(`{}`, setHeader("X-Shopify-Triggered-At", fixedNow.Add(-10*time.Minute).Format(time.RFC3339))), 401, "stale_timestamp"},
		{"future timestamp", signed(`{}`, setHeader("X-Shopify-Triggered-At", fixedNow.Add(5*time.Minute).Format(time.RFC3339))), 401, "stale_timestamp"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, nil)
			w := f.serve(tt.req)
			if w.Code != tt.status {
				t.Fatalf("status = %d, want %d", w.Code, tt.status)
			}
			if got := f.count.get(RejectedSeries(tt.reason)); got != 1 {
				t.Fatalf("rejected{reason=%s} = %d, want 1", tt.reason, got)
			}
			if len(f.queue.items) != 0 {
				t.Fatal("rejected delivery reached the queue")
			}
		})
	}
}

func TestAcceptsAndForwardsOnlyShopifyHeaders(t *testing.T) {
	f := newFixture(t, nil)
	body := `{"id":820982911946154508}`
	w := f.serve(signed(body, setHeader("Cookie", "session=secret")))

	if w.Code != 200 || w.Body.String() != "ok" {
		t.Fatalf("got %d %q", w.Code, w.Body.String())
	}
	if len(f.queue.items) != 1 {
		t.Fatalf("queued %d deliveries, want 1", len(f.queue.items))
	}
	d := f.queue.items[0]
	if d.WebhookID != "wh-1" || d.Shop != "demo-store.myshopify.com" || d.Topic != "orders/create" || string(d.Body) != body {
		t.Fatalf("unexpected delivery: %+v", d)
	}
	if d.Header.Get("X-Shopify-Hmac-Sha256") == "" || d.Header.Get("Content-Type") == "" {
		t.Fatal("Shopify headers / Content-Type were not forwarded")
	}
	if d.Header.Get("Cookie") != "" {
		t.Fatal("non-Shopify header leaked to upstream")
	}
	if f.count.get(MetricAccepted) != 1 {
		t.Fatal("accepted metric not incremented")
	}
}

// A real delivery generated and signed by Shopify (see testdata/README.md),
// with the exact headers Shopify sent. It must pass every check and be queued
// byte-for-byte.
func TestAcceptsRealShopifyDelivery(t *testing.T) {
	body, err := os.ReadFile("../../testdata/shopify_orders_create_2026-10.json")
	if err != nil {
		t.Fatal(err)
	}
	triggeredAt, _ := time.Parse(time.RFC3339Nano, "2026-11-24T22:09:32.655934808Z")

	v, err := verify.New([]string{"local-test-secret"})
	if err != nil {
		t.Fatal(err)
	}
	q, c := &fakeQueue{}, &fakeCounter{m: map[string]int{}}
	h := NewHandler(Config{MaxBodyBytes: 5 << 20, MaxAge: 6 * time.Hour, FutureSkew: 2 * time.Minute},
		v, dedupe.NewMemory(time.Hour), ratelimit.New(0, 1), q, c, slog.New(slog.NewTextHandler(io.Discard, nil)))
	h.now = func() time.Time { return triggeredAt.Add(time.Second) }

	r := httptest.NewRequest(http.MethodPost, "/webhooks", strings.NewReader(string(body)))
	for k, val := range map[string]string{
		"Content-Type":           "application/json",
		"X-Shopify-Api-Version":  "2026-10",
		"X-Shopify-Hmac-Sha256":  "/WGG5GIepPtQJVZF3fdeUBzDrzhd0QmX9J+aERuu5mc=",
		"X-Shopify-Shop-Domain":  "shop.myshopify.com",
		"X-Shopify-Test":         "true",
		"X-Shopify-Topic":        "orders/create",
		"X-Shopify-Triggered-At": triggeredAt.Format(time.RFC3339Nano),
		"X-Shopify-Webhook-Id":   "a1cf3871-d038-4e02-9717-4ee98d8d21ae",
	} {
		r.Header.Set(k, val)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != 200 || w.Body.String() != "ok" {
		t.Fatalf("real Shopify delivery: %d %q", w.Code, w.Body.String())
	}
	if len(q.items) != 1 || string(q.items[0].Body) != string(body) {
		t.Fatal("real delivery was not queued byte-for-byte")
	}
	if q.items[0].Header.Get("X-Shopify-Test") != "true" {
		t.Fatal("X-Shopify-Test header was not forwarded")
	}
}

func TestAcceptsUnprefixedEventsHeaders(t *testing.T) {
	f := newFixture(t, nil)
	r := signed(`{}`, func(r *http.Request) {
		for _, name := range []string{"Hmac-Sha256", "Shop-Domain", "Topic", "Webhook-Id", "Triggered-At"} {
			r.Header.Set("Shopify-"+name, r.Header.Get("X-Shopify-"+name))
			r.Header.Del("X-Shopify-" + name)
		}
	})
	if w := f.serve(r); w.Code != 200 {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestDuplicateWebhookID(t *testing.T) {
	f := newFixture(t, nil)
	first := f.serve(signed(`{"id":1}`, nil))
	second := f.serve(signed(`{"id":1}`, nil))

	if first.Code != 200 || second.Code != 200 || second.Body.String() != "duplicate" {
		t.Fatalf("got %d/%q then %d/%q", first.Code, first.Body.String(), second.Code, second.Body.String())
	}
	if len(f.queue.items) != 1 || f.count.get(MetricDuplicates) != 1 {
		t.Fatalf("queued=%d duplicates=%d", len(f.queue.items), f.count.get(MetricDuplicates))
	}
}

func TestDedupeByBody(t *testing.T) {
	replay := setHeader("X-Shopify-Webhook-Id", "wh-forged-new-id")

	t.Run("enabled drops replayed body under a new id", func(t *testing.T) {
		f := newFixture(t, func(c *Config) { c.DedupeByBody = true })
		f.serve(signed(`{"id":1}`, nil))
		w := f.serve(signed(`{"id":1}`, replay))
		if w.Body.String() != "duplicate" || len(f.queue.items) != 1 {
			t.Fatalf("body replay not caught: %q, queued=%d", w.Body.String(), len(f.queue.items))
		}
		// The new id must not stay marked, since the delivery was dropped.
		if !f.store.MarkIfNew("id:wh-forged-new-id") {
			t.Fatal("id of dropped delivery was left marked")
		}
	})

	t.Run("disabled forwards both", func(t *testing.T) {
		f := newFixture(t, nil)
		f.serve(signed(`{"id":1}`, nil))
		f.serve(signed(`{"id":1}`, replay))
		if len(f.queue.items) != 2 {
			t.Fatalf("queued=%d, want 2", len(f.queue.items))
		}
	})
}

func TestRateLimitDoesNotMarkSeen(t *testing.T) {
	f := newFixture(t, nil)
	f.h.limiter = ratelimit.New(0.001, 1) // one request, then empty for a long time

	if w := f.serve(signed(`{}`, nil)); w.Code != 200 {
		t.Fatalf("first request: %d", w.Code)
	}
	w := f.serve(signed(`{}`, setHeader("X-Shopify-Webhook-Id", "wh-2")))
	if w.Code != 429 || w.Header().Get("Retry-After") == "" {
		t.Fatalf("second request: %d, Retry-After=%q", w.Code, w.Header().Get("Retry-After"))
	}
	if !f.store.MarkIfNew("id:wh-2") {
		t.Fatal("rate-limited delivery was marked as seen; Shopify's retry would be dropped")
	}
}

func TestQueueFullAllowsRetry(t *testing.T) {
	f := newFixture(t, func(c *Config) { c.DedupeByBody = true })
	f.queue.full = true
	if w := f.serve(signed(`{}`, nil)); w.Code != 503 {
		t.Fatalf("status = %d, want 503", w.Code)
	}
	f.queue.full = false
	if w := f.serve(signed(`{}`, nil)); w.Code != 200 || w.Body.String() != "ok" {
		t.Fatalf("retry after 503: %d %q (should not be treated as duplicate)", w.Code, w.Body.String())
	}
}

func TestMux(t *testing.T) {
	f := newFixture(t, nil)
	metricsHit := false
	mux := NewMux("/webhooks", f.h, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		metricsHit = true
	}))

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if w.Code != 200 {
		t.Fatalf("/healthz = %d", w.Code)
	}
	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !metricsHit {
		t.Fatal("/metrics not routed")
	}
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, signed(`{}`, nil))
	if w.Code != 200 {
		t.Fatalf("/webhooks = %d", w.Code)
	}
}
