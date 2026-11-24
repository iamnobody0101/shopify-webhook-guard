// Package server contains the HTTP handler that screens incoming webhooks.
//
// Pipeline, cheapest and most security-critical checks first:
//
//  1. method + body size limit (bounded memory before any crypto)
//  2. HMAC-SHA256 over the raw body (nothing below runs for forged requests)
//  3. header sanity: shop domain, webhook id, timestamp freshness
//  4. per-shop rate limit -> 429 so Shopify backs off and retries
//  5. dedupe by X-Shopify-Webhook-Id (optionally also by body hash)
//  6. enqueue for async forwarding and ACK immediately
package server

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/iamnobody0101/shopify-webhook-guard/internal/dedupe"
	"github.com/iamnobody0101/shopify-webhook-guard/internal/metrics"
	"github.com/iamnobody0101/shopify-webhook-guard/internal/proxy"
	"github.com/iamnobody0101/shopify-webhook-guard/internal/ratelimit"
	"github.com/iamnobody0101/shopify-webhook-guard/internal/verify"
)

// Metric series emitted by the handler.
const (
	MetricAccepted   = "webhook_guard_accepted_total"
	MetricDuplicates = "webhook_guard_duplicates_total"
	metricRejected   = "webhook_guard_rejected_total"
)

// RejectedSeries returns the rejection counter name for a reason.
func RejectedSeries(reason string) string { return metrics.Labeled(metricRejected, "reason", reason) }

// Enqueuer accepts verified deliveries for forwarding.
type Enqueuer interface{ Enqueue(proxy.Delivery) bool }

// Counter records metrics.
type Counter interface{ Inc(series string) }

// Config holds the handler's policy knobs.
type Config struct {
	MaxBodyBytes int64
	MaxAge       time.Duration // reject deliveries triggered longer ago than this
	FutureSkew   time.Duration // tolerated clock skew for timestamps in the future
	DedupeByBody bool          // also drop identical bodies arriving under a new webhook id
}

// Handler implements the webhook screening pipeline.
type Handler struct {
	cfg      Config
	verifier *verify.Verifier
	dedupe   dedupe.Store
	limiter  *ratelimit.Limiter
	queue    Enqueuer
	metrics  Counter
	log      *slog.Logger
	now      func() time.Time
}

func NewHandler(cfg Config, v *verify.Verifier, d dedupe.Store, l *ratelimit.Limiter,
	q Enqueuer, m Counter, log *slog.Logger) *Handler {
	return &Handler{cfg: cfg, verifier: v, dedupe: d, limiter: l, queue: q, metrics: m, log: log, now: time.Now}
}

// Shop domains look like "my-store.myshopify.com".
var shopDomainRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,61}\.myshopify\.com$`)

const maxWebhookIDLen = 256

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		h.reject(w, r, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.cfg.MaxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			h.reject(w, r, http.StatusRequestEntityTooLarge, "body_too_large")
		} else {
			h.reject(w, r, http.StatusBadRequest, "read_error")
		}
		return
	}

	if err := h.verifier.Verify(body, shopifyHeader(r.Header, "Hmac-Sha256")); err != nil {
		h.reject(w, r, http.StatusUnauthorized, "invalid_signature")
		return
	}

	// NOTE: Shopify's HMAC covers only the body. The headers below are not
	// signed, so they are validated for sanity, not trusted for authenticity.
	shop := shopifyHeader(r.Header, "Shop-Domain")
	if !shopDomainRe.MatchString(shop) {
		h.reject(w, r, http.StatusBadRequest, "invalid_shop_domain")
		return
	}
	webhookID := shopifyHeader(r.Header, "Webhook-Id")
	if webhookID == "" || len(webhookID) > maxWebhookIDLen {
		h.reject(w, r, http.StatusBadRequest, "invalid_webhook_id")
		return
	}
	triggeredAt, err := time.Parse(time.RFC3339Nano, shopifyHeader(r.Header, "Triggered-At"))
	if err != nil {
		h.reject(w, r, http.StatusBadRequest, "invalid_timestamp")
		return
	}
	if age := h.now().Sub(triggeredAt); age > h.cfg.MaxAge || age < -h.cfg.FutureSkew {
		h.reject(w, r, http.StatusUnauthorized, "stale_timestamp")
		return
	}

	// Rate-limit BEFORE marking as seen: a 429'd delivery will be retried by
	// Shopify and must not then be mistaken for a duplicate.
	if !h.limiter.Allow(shop) {
		w.Header().Set("Retry-After", "1")
		h.reject(w, r, http.StatusTooManyRequests, "rate_limited")
		return
	}

	keys := []string{"id:" + webhookID}
	if h.cfg.DedupeByBody {
		sum := sha256.Sum256(body)
		keys = append(keys, "body:"+hex.EncodeToString(sum[:]))
	}
	marked := make([]string, 0, len(keys))
	for _, k := range keys {
		if !h.dedupe.MarkIfNew(k) {
			h.forget(marked)
			h.metrics.Inc(MetricDuplicates)
			h.log.Info("duplicate delivery dropped",
				"shop", shop, "webhook_id", webhookID, "matched", strings.SplitN(k, ":", 2)[0])
			// 200, not 4xx: Shopify should stop retrying something we already have.
			writeText(w, http.StatusOK, "duplicate")
			return
		}
		marked = append(marked, k)
	}

	d := proxy.Delivery{
		WebhookID:  webhookID,
		Shop:       shop,
		Topic:      shopifyHeader(r.Header, "Topic"),
		Header:     forwardHeaders(r.Header),
		Body:       body,
		ReceivedAt: h.now().UTC(),
	}
	if !h.queue.Enqueue(d) {
		// Un-mark so Shopify's retry is processed rather than dropped as a dupe.
		h.forget(marked)
		w.Header().Set("Retry-After", "5")
		h.reject(w, r, http.StatusServiceUnavailable, "queue_full")
		return
	}
	h.metrics.Inc(MetricAccepted)
	writeText(w, http.StatusOK, "ok")
}

func (h *Handler) forget(keys []string) {
	for _, k := range keys {
		h.dedupe.Forget(k)
	}
}

func (h *Handler) reject(w http.ResponseWriter, r *http.Request, status int, reason string) {
	h.metrics.Inc(RejectedSeries(reason))
	h.log.Warn("delivery rejected",
		"reason", reason,
		"status", status,
		"remote", r.RemoteAddr,
		"shop_unverified", shopifyHeader(r.Header, "Shop-Domain"),
		"webhook_id_unverified", shopifyHeader(r.Header, "Webhook-Id"),
	)
	// Generic body: don't tell an attacker which check failed.
	writeText(w, status, http.StatusText(status))
}

// shopifyHeader reads a Shopify header. Classic webhooks use an "X-Shopify-"
// prefix; the newer Events deliveries use "Shopify-". Lookups are
// case-insensitive (HTTP/2 lowercases header names).
func shopifyHeader(h http.Header, name string) string {
	if v := h.Get("X-Shopify-" + name); v != "" {
		return v
	}
	return h.Get("Shopify-" + name)
}

// forwardHeaders keeps only Shopify metadata and Content-Type, so arbitrary
// client headers (cookies, auth, hop-by-hop) never reach the upstream app.
// The original HMAC header is forwarded so the app can re-verify
// (defense in depth).
func forwardHeaders(in http.Header) http.Header {
	out := make(http.Header)
	for k, vs := range in {
		ck := http.CanonicalHeaderKey(k)
		if strings.HasPrefix(ck, "X-Shopify-") || strings.HasPrefix(ck, "Shopify-") || ck == "Content-Type" {
			out[ck] = append([]string(nil), vs...)
		}
	}
	return out
}

func writeText(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, msg)
}

// NewMux wires the webhook endpoint, health check, and metrics.
func NewMux(webhookPath string, webhook, metricsHandler http.Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle(webhookPath, webhook)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeText(w, http.StatusOK, "ok")
	})
	mux.Handle("/metrics", metricsHandler)
	return mux
}
