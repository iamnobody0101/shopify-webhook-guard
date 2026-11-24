package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func minimal() map[string]string {
	return map[string]string{
		"SHOPIFY_SECRETS": " new-secret, old-secret ,",
		"UPSTREAM_URL":    "http://localhost:9090/webhooks",
	}
}

func TestLoadDefaults(t *testing.T) {
	c, err := Load(env(minimal()))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(c.Secrets, "|") != "new-secret|old-secret" {
		t.Errorf("Secrets = %q", c.Secrets)
	}
	if c.ListenAddr != ":8080" || c.WebhookPath != "/webhooks" {
		t.Errorf("ListenAddr=%q WebhookPath=%q", c.ListenAddr, c.WebhookPath)
	}
	if c.DedupeTTL != c.MaxAge+c.FutureSkew {
		t.Errorf("DedupeTTL = %v, want MaxAge+FutureSkew", c.DedupeTTL)
	}
	if c.Workers != 8 || c.MaxAttempts != 5 || c.DedupeByBody {
		t.Errorf("unexpected defaults: %+v", c)
	}
}

func TestLoadOverrides(t *testing.T) {
	m := minimal()
	for k, v := range map[string]string{
		"PORT": "3000", "WEBHOOK_PATH": "/hooks", "MAX_BODY_BYTES": "1024",
		"MAX_AGE": "1h", "FUTURE_SKEW": "30s", "DEDUPE_TTL": "2h", "DEDUPE_BY_BODY": "true",
		"RATE_PER_SHOP": "2.5", "RATE_BURST": "10", "WORKERS": "4", "QUEUE_SIZE": "50",
		"MAX_ATTEMPTS": "3", "FORWARD_TIMEOUT": "3s", "DLQ_PATH": "/tmp/dlq.jsonl",
	} {
		m[k] = v
	}
	c, err := Load(env(m))
	if err != nil {
		t.Fatal(err)
	}
	if c.ListenAddr != ":3000" || c.WebhookPath != "/hooks" || c.MaxBodyBytes != 1024 ||
		c.MaxAge != time.Hour || c.FutureSkew != 30*time.Second || c.DedupeTTL != 2*time.Hour ||
		!c.DedupeByBody || c.RatePerShop != 2.5 || c.RateBurst != 10 || c.Workers != 4 ||
		c.QueueSize != 50 || c.MaxAttempts != 3 || c.ForwardTimeout != 3*time.Second ||
		c.DLQPath != "/tmp/dlq.jsonl" {
		t.Fatalf("overrides not applied: %+v", c)
	}

	m["LISTEN_ADDR"] = "127.0.0.1:9999"
	if c, _ := Load(env(m)); c.ListenAddr != "127.0.0.1:9999" {
		t.Fatalf("LISTEN_ADDR should win over PORT, got %q", c.ListenAddr)
	}
}

func TestLoadErrors(t *testing.T) {
	tests := []struct {
		name    string
		set     map[string]string
		wantMsg string
	}{
		{"missing secrets", map[string]string{"SHOPIFY_SECRETS": " , "}, "SHOPIFY_SECRETS"},
		{"missing upstream", map[string]string{"UPSTREAM_URL": ""}, "UPSTREAM_URL"},
		{"non-http upstream", map[string]string{"UPSTREAM_URL": "ftp://example.com"}, "UPSTREAM_URL"},
		{"relative path", map[string]string{"WEBHOOK_PATH": "hooks"}, "WEBHOOK_PATH"},
		{"bad duration", map[string]string{"MAX_AGE": "soon"}, "MAX_AGE"},
		{"bad int", map[string]string{"WORKERS": "many"}, "WORKERS"},
		{"bad int64", map[string]string{"MAX_BODY_BYTES": "big"}, "MAX_BODY_BYTES"},
		{"bad float", map[string]string{"RATE_PER_SHOP": "fast"}, "RATE_PER_SHOP"},
		{"bad bool", map[string]string{"DEDUPE_BY_BODY": "maybe"}, "DEDUPE_BY_BODY"},
		{"zero workers", map[string]string{"WORKERS": "0"}, "WORKERS"},
		{"negative rate", map[string]string{"RATE_PER_SHOP": "-1"}, "RATE_PER_SHOP"},
		{"ttl shorter than freshness window", map[string]string{"MAX_AGE": "1h", "DEDUPE_TTL": "30m"}, "DEDUPE_TTL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := minimal()
			for k, v := range tt.set {
				m[k] = v
			}
			_, err := Load(env(m))
			if err == nil || !strings.Contains(err.Error(), tt.wantMsg) {
				t.Fatalf("err = %v, want mention of %s", err, tt.wantMsg)
			}
		})
	}
}
