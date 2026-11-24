// Command guard runs the Shopify webhook security gateway.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/iamnobody0101/shopify-webhook-guard/internal/config"
	"github.com/iamnobody0101/shopify-webhook-guard/internal/dedupe"
	"github.com/iamnobody0101/shopify-webhook-guard/internal/metrics"
	"github.com/iamnobody0101/shopify-webhook-guard/internal/proxy"
	"github.com/iamnobody0101/shopify-webhook-guard/internal/ratelimit"
	"github.com/iamnobody0101/shopify-webhook-guard/internal/server"
	"github.com/iamnobody0101/shopify-webhook-guard/internal/verify"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "webhook-guard:", err)
		os.Exit(1)
	}
}

func run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return fmt.Errorf("invalid configuration:\n%w", err)
	}
	verifier, err := verify.New(cfg.Secrets)
	if err != nil {
		return err
	}
	dlq, err := proxy.OpenFileDLQ(cfg.DLQPath)
	if err != nil {
		return fmt.Errorf("open dead-letter queue: %w", err)
	}
	defer dlq.Close()

	reg := metrics.New()
	store := dedupe.NewMemory(cfg.DedupeTTL)
	limiter := ratelimit.New(cfg.RatePerShop, cfg.RateBurst)
	fwd := proxy.New(proxy.Config{
		UpstreamURL: cfg.UpstreamURL,
		Workers:     cfg.Workers,
		QueueSize:   cfg.QueueSize,
		MaxAttempts: cfg.MaxAttempts,
		Timeout:     cfg.ForwardTimeout,
	}, dlq, reg, logger)
	reg.SetGauge("webhook_guard_queue_depth", func() float64 { return float64(fwd.QueueLen()) })
	reg.SetGauge("webhook_guard_dedupe_entries", func() float64 { return float64(store.Len()) })
	reg.SetGauge("webhook_guard_ratelimit_buckets", func() float64 { return float64(limiter.Len()) })

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go store.RunJanitor(ctx, time.Minute)
	go limiter.RunJanitor(ctx, time.Minute)
	fwd.Start()

	handler := server.NewHandler(server.Config{
		MaxBodyBytes: cfg.MaxBodyBytes,
		MaxAge:       cfg.MaxAge,
		FutureSkew:   cfg.FutureSkew,
		DedupeByBody: cfg.DedupeByBody,
	}, verifier, store, limiter, fwd, reg, logger)

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           server.NewMux(cfg.WebhookPath, handler, reg),
		ReadHeaderTimeout: 5 * time.Second, // slowloris protection
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("webhook-guard listening",
			"addr", cfg.ListenAddr, "path", cfg.WebhookPath,
			"secrets", len(cfg.Secrets), "dedupe_by_body", cfg.DedupeByBody)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
	}

	logger.Info("shutting down: draining HTTP connections and forward queue")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown", "err", err)
	}
	if err := fwd.Shutdown(shutdownCtx); err != nil {
		logger.Error("forward queue not fully drained; remaining deliveries were dead-lettered", "err", err)
	}
	return nil
}
