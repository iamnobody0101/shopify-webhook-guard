// Package config loads guard settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ListenAddr     string
	WebhookPath    string
	Secrets        []string
	UpstreamURL    string
	MaxBodyBytes   int64
	MaxAge         time.Duration
	FutureSkew     time.Duration
	DedupeTTL      time.Duration
	DedupeByBody   bool
	RatePerShop    float64
	RateBurst      int
	Workers        int
	QueueSize      int
	MaxAttempts    int
	ForwardTimeout time.Duration
	DLQPath        string
}

// Load reads configuration using getenv (os.Getenv in production) and
// returns every validation problem at once.
func Load(getenv func(string) string) (Config, error) {
	p := &parser{getenv: getenv}
	c := Config{
		ListenAddr:     ":8080",
		WebhookPath:    p.str("WEBHOOK_PATH", "/webhooks"),
		UpstreamURL:    p.str("UPSTREAM_URL", ""),
		MaxBodyBytes:   p.int64("MAX_BODY_BYTES", 5<<20),
		MaxAge:         p.dur("MAX_AGE", 6*time.Hour),
		FutureSkew:     p.dur("FUTURE_SKEW", 2*time.Minute),
		DedupeByBody:   p.bool("DEDUPE_BY_BODY", false),
		RatePerShop:    p.float("RATE_PER_SHOP", 50),
		RateBurst:      p.int("RATE_BURST", 100),
		Workers:        p.int("WORKERS", 8),
		QueueSize:      p.int("QUEUE_SIZE", 10000),
		MaxAttempts:    p.int("MAX_ATTEMPTS", 5),
		ForwardTimeout: p.dur("FORWARD_TIMEOUT", 10*time.Second),
		DLQPath:        p.str("DLQ_PATH", "dead-letter.jsonl"),
	}
	c.DedupeTTL = p.dur("DEDUPE_TTL", c.MaxAge+c.FutureSkew)
	if port := getenv("PORT"); port != "" { // PaaS convention (Render, Fly, Heroku)
		c.ListenAddr = ":" + port
	}
	c.ListenAddr = p.str("LISTEN_ADDR", c.ListenAddr)

	for _, s := range strings.Split(getenv("SHOPIFY_SECRETS"), ",") {
		if s = strings.TrimSpace(s); s != "" {
			c.Secrets = append(c.Secrets, s)
		}
	}

	p.check(len(c.Secrets) > 0, "SHOPIFY_SECRETS is required (comma-separated app client secrets)")
	if u, err := url.Parse(c.UpstreamURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		p.fail("UPSTREAM_URL must be an absolute http(s) URL")
	}
	p.check(strings.HasPrefix(c.WebhookPath, "/"), "WEBHOOK_PATH must start with /")
	p.check(c.MaxBodyBytes > 0, "MAX_BODY_BYTES must be > 0")
	p.check(c.MaxAge > 0, "MAX_AGE must be > 0")
	p.check(c.FutureSkew >= 0, "FUTURE_SKEW must be >= 0")
	// Invariant: any verbatim replay is either still remembered (duplicate)
	// or old enough to fail the freshness check (stale). Never neither.
	p.check(c.DedupeTTL >= c.MaxAge+c.FutureSkew, "DEDUPE_TTL must be >= MAX_AGE + FUTURE_SKEW")
	p.check(c.RatePerShop >= 0, "RATE_PER_SHOP must be >= 0 (0 disables)")
	p.check(c.RateBurst >= 1, "RATE_BURST must be >= 1")
	p.check(c.Workers >= 1, "WORKERS must be >= 1")
	p.check(c.QueueSize >= 1, "QUEUE_SIZE must be >= 1")
	p.check(c.MaxAttempts >= 1, "MAX_ATTEMPTS must be >= 1")
	p.check(c.ForwardTimeout > 0, "FORWARD_TIMEOUT must be > 0")
	p.check(c.DLQPath != "", "DLQ_PATH must not be empty")

	return c, errors.Join(p.errs...)
}

type parser struct {
	getenv func(string) string
	errs   []error
}

func (p *parser) fail(format string, args ...any) {
	p.errs = append(p.errs, fmt.Errorf(format, args...))
}

func (p *parser) check(ok bool, msg string) {
	if !ok {
		p.errs = append(p.errs, errors.New(msg))
	}
}

func (p *parser) str(key, def string) string {
	if v := p.getenv(key); v != "" {
		return v
	}
	return def
}

func (p *parser) dur(key string, def time.Duration) time.Duration {
	v := p.getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		p.fail("%s: invalid duration %q", key, v)
		return def
	}
	return d
}

func (p *parser) int(key string, def int) int {
	v := p.getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		p.fail("%s: invalid integer %q", key, v)
		return def
	}
	return n
}

func (p *parser) int64(key string, def int64) int64 {
	v := p.getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		p.fail("%s: invalid integer %q", key, v)
		return def
	}
	return n
}

func (p *parser) float(key string, def float64) float64 {
	v := p.getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		p.fail("%s: invalid number %q", key, v)
		return def
	}
	return f
}

func (p *parser) bool(key string, def bool) bool {
	v := p.getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		p.fail("%s: invalid boolean %q", key, v)
		return def
	}
	return b
}
