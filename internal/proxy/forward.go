// Package proxy delivers verified webhooks to the upstream app asynchronously.
//
// Shopify expects a response within a few seconds and retries otherwise, so
// the guard acknowledges as soon as a delivery is verified and queued, and a
// worker pool forwards it in the background with retries. Deliveries that
// still fail are written to a dead-letter queue instead of being dropped.
package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"
)

// Metric series emitted by the forwarder.
const (
	MetricForwarded    = "webhook_guard_forwarded_total"
	MetricRetries      = "webhook_guard_forward_retries_total"
	MetricDeadLettered = "webhook_guard_dead_lettered_total"
)

// Delivery is a verified webhook waiting to be forwarded.
type Delivery struct {
	WebhookID  string      `json:"webhook_id"`
	Shop       string      `json:"shop"`
	Topic      string      `json:"topic"`
	Header     http.Header `json:"header"`
	Body       []byte      `json:"body"` // base64 in JSON, preserving exact bytes
	ReceivedAt time.Time   `json:"received_at"`
}

// Config controls forwarding behaviour. Zero values get sensible defaults.
type Config struct {
	UpstreamURL string
	Workers     int
	QueueSize   int
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	Timeout     time.Duration // per attempt
}

// Counter is the subset of the metrics registry the forwarder needs.
type Counter interface{ Inc(series string) }

// StatusError is returned when the upstream answers with a non-2xx status.
type StatusError struct{ Code int }

func (e *StatusError) Error() string { return fmt.Sprintf("upstream returned status %d", e.Code) }

// Retryable reports whether retrying could plausibly succeed. Other 4xx
// responses mean the upstream rejected the payload; retrying won't help.
func (e *StatusError) Retryable() bool {
	return e.Code == http.StatusRequestTimeout || e.Code == http.StatusTooManyRequests || e.Code >= 500
}

// Forwarder is a bounded queue drained by a fixed pool of workers.
type Forwarder struct {
	cfg     Config
	client  *http.Client
	dlq     DeadLetterWriter
	metrics Counter
	log     *slog.Logger

	mu     sync.RWMutex // guards closed and close(queue)
	closed bool
	queue  chan Delivery
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// New creates a Forwarder. Call Start to begin processing.
func New(cfg Config, dlq DeadLetterWriter, metrics Counter, log *slog.Logger) *Forwarder {
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.QueueSize < 1 {
		cfg.QueueSize = 1
	}
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 500 * time.Millisecond
	}
	if cfg.MaxBackoff < cfg.BaseBackoff {
		cfg.MaxBackoff = 32 * cfg.BaseBackoff
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	// Go's default transport keeps only 2 idle connections per host. With N
	// workers that forces constant TCP (and TLS) handshakes to the upstream,
	// which benchmarking showed was the forwarding bottleneck.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = cfg.Workers
	transport.MaxIdleConnsPerHost = cfg.Workers
	return &Forwarder{
		cfg:     cfg,
		client:  &http.Client{Timeout: cfg.Timeout, Transport: transport},
		dlq:     dlq,
		metrics: metrics,
		log:     log,
		queue:   make(chan Delivery, cfg.QueueSize),
	}
}

// Start launches the worker pool.
func (f *Forwarder) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	for i := 0; i < f.cfg.Workers; i++ {
		f.wg.Add(1)
		go func() {
			defer f.wg.Done()
			for d := range f.queue {
				f.deliver(ctx, d)
			}
		}()
	}
}

// Enqueue adds d to the queue without blocking. It returns false if the
// queue is full or shut down; the caller should then answer non-2xx so that
// Shopify retries later (backpressure instead of silent loss).
func (f *Forwarder) Enqueue(d Delivery) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.closed {
		return false
	}
	select {
	case f.queue <- d:
		return true
	default:
		return false
	}
}

// QueueLen returns the number of deliveries waiting for a worker.
func (f *Forwarder) QueueLen() int { return len(f.queue) }

// Shutdown stops accepting deliveries and waits for the queue to drain. If
// ctx expires first, in-flight retries are aborted and every remaining
// delivery is dead-lettered, so nothing that was acknowledged is lost
// silently on a graceful shutdown.
func (f *Forwarder) Shutdown(ctx context.Context) error {
	f.mu.Lock()
	if !f.closed {
		f.closed = true
		close(f.queue)
	}
	f.mu.Unlock()

	done := make(chan struct{})
	go func() { f.wg.Wait(); close(done) }()

	var err error
	select {
	case <-done:
	case <-ctx.Done():
		err = ctx.Err()
		if f.cancel != nil {
			f.cancel()
		}
		<-done
	}
	if f.cancel != nil {
		f.cancel()
	}
	return err
}

func (f *Forwarder) deliver(ctx context.Context, d Delivery) {
	var lastErr error
	attempts := 0
	for attempts < f.cfg.MaxAttempts {
		if attempts > 0 {
			f.metrics.Inc(MetricRetries)
			if err := sleepCtx(ctx, f.backoff(attempts)); err != nil {
				lastErr = err
				break
			}
		}
		attempts++
		err := f.send(ctx, d)
		if err == nil {
			f.metrics.Inc(MetricForwarded)
			return
		}
		lastErr = err
		var se *StatusError
		if errors.As(err, &se) && !se.Retryable() {
			break
		}
		if ctx.Err() != nil {
			break
		}
	}

	f.metrics.Inc(MetricDeadLettered)
	rec := DeadLetter{Delivery: d, Error: lastErr.Error(), Attempts: attempts, FailedAt: time.Now().UTC()}
	if err := f.dlq.Write(rec); err != nil {
		f.log.Error("dead-letter write failed; delivery lost",
			"webhook_id", d.WebhookID, "shop", d.Shop, "err", err)
		return
	}
	f.log.Warn("delivery dead-lettered",
		"webhook_id", d.WebhookID, "shop", d.Shop, "topic", d.Topic,
		"attempts", attempts, "err", lastErr)
}

func (f *Forwarder) send(ctx context.Context, d Delivery) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.cfg.UpstreamURL, bytes.NewReader(d.Body))
	if err != nil {
		return err
	}
	for k, vs := range d.Header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		// Drain a little so the keep-alive connection can be reused.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		_ = resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return &StatusError{Code: resp.StatusCode}
	}
	return nil
}

// backoff returns the delay before retry number n (n >= 1): exponential
// growth capped at MaxBackoff, with full jitter so that many deliveries
// failing together don't retry in lockstep against a recovering upstream.
func (f *Forwarder) backoff(n int) time.Duration {
	d := f.cfg.MaxBackoff
	if n >= 1 && n <= 30 {
		if exp := f.cfg.BaseBackoff << (n - 1); exp > 0 && exp < d {
			d = exp
		}
	}
	return time.Duration(rand.Int64N(int64(d) + 1))
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
