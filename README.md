# shopify-webhook-guard

A drop-in security gateway for Shopify webhooks, written in Go with **zero third-party dependencies**.

It sits in front of a Shopify app and makes sure that only authentic, fresh, non-duplicate webhooks reach it. It acknowledges Shopify within milliseconds and delivers to the app asynchronously, with retries and a dead-letter queue.

**Live demo:** https://shopify-webhook-guard.onrender.com (deployed on Render's free tier from this repo's Dockerfile)
- [`/healthz`](https://shopify-webhook-guard.onrender.com/healthz) shows liveness, and [`/metrics`](https://shopify-webhook-guard.onrender.com/metrics) shows live counters.
- `POST /webhooks` needs a valid HMAC from a private demo secret, so unsigned requests get `401` (that's the point). To try the full flow, run it locally (see Quickstart).
- The demo forwards to `httpbin.org/post`. The free instance sleeps when idle, so the first request may take 30–60 s. That is far beyond Shopify's 5 s timeout, so this is a demo, not a production deployment. In production, run it on an always-on instance.

```
Shopify ──HTTPS──▶  webhook-guard  ──▶  your app (UPSTREAM_URL)
                     │ 1. method + body-size limit
                     │ 2. HMAC-SHA256 over raw body (constant-time, multi-secret rotation)
                     │ 3. shop domain / webhook id / timestamp freshness checks
                     │ 4. per-shop token-bucket rate limit  → 429 (Shopify retries)
                     │ 5. dedupe by X-Shopify-Webhook-Id (+ optional body hash)
                     │ 6. enqueue → 200 OK immediately
                     ▼
               worker pool ── retry w/ exponential backoff + full jitter ──▶ app
                     └── gives up → dead-letter queue (JSONL)
               /healthz   /metrics (Prometheus text format)
```

## Why

Every Shopify app has to handle webhooks, and the details are easy to get wrong:

| Common bug | What goes wrong | What the guard does |
|---|---|---|
| Comparing signatures with `==` | Timing side channel | `hmac.Equal`, checks every secret without exiting early |
| Verifying `JSON.stringify(parsed)` instead of raw bytes | Valid webhooks fail, or apps disable verification | Verifies the exact wire bytes; unit test covers this pitfall |
| No deduplication | Shopify delivers *at least once*, so retries double-process orders | Atomic check-and-set on `X-Shopify-Webhook-Id` |
| Slow handler | Shopify times out and retries, which adds load to an already slow app (a retry storm) | ACK after verify + enqueue; process asynchronously |
| Secret rotation means downtime | Single hard-coded secret | `SHOPIFY_SECRETS=new,old` |
| Failures disappear | Webhooks lost on app outage | Retries, then dead-letter file for replay |

## Quickstart

Requires Go 1.26+.

```powershell
# terminal 1: a stand-in app
go run ./cmd/mockapp

# terminal 2: the guard
$env:SHOPIFY_SECRETS = "s3cret"
$env:UPSTREAM_URL   = "http://localhost:9090/webhooks"
go run ./cmd/guard

# terminal 3: send signed fake webhooks
go run ./cmd/sendtest -secret s3cret                 # 200 ok -> forwarded
go run ./cmd/sendtest -secret s3cret -n 3 -id same   # 1x ok, 2x duplicate
go run ./cmd/sendtest -secret s3cret -tamper         # 401
go run ./cmd/sendtest -secret s3cret -age 7h         # 401 (stale)
go run ./cmd/sendtest -secret wrong                  # 401
curl http://localhost:8080/metrics
```

Retries and the dead-letter queue: restart mockapp with `-fail-rate 1`, send a webhook, and watch `dead-letter.jsonl`.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `SHOPIFY_SECRETS` | *(required)* | Comma-separated app client secrets (first = current) |
| `UPSTREAM_URL` | *(required)* | Where verified webhooks are forwarded |
| `LISTEN_ADDR` / `PORT` | `:8080` | Listen address (`PORT` for PaaS) |
| `WEBHOOK_PATH` | `/webhooks` | Endpoint path |
| `MAX_BODY_BYTES` | `5242880` | Request body limit |
| `MAX_AGE` | `6h` | Reject deliveries triggered longer ago than this |
| `FUTURE_SKEW` | `2m` | Tolerated clock skew |
| `DEDUPE_TTL` | `MAX_AGE+FUTURE_SKEW` | Must be ≥ that (enforced, see below) |
| `DEDUPE_BY_BODY` | `false` | Also drop identical bodies under a new webhook id |
| `RATE_PER_SHOP` / `RATE_BURST` | `50` / `100` | Token bucket per shop (`0` disables) |
| `WORKERS` / `QUEUE_SIZE` | `8` / `10000` | Forwarding concurrency and buffer |
| `MAX_ATTEMPTS` / `FORWARD_TIMEOUT` | `5` / `10s` | Delivery retries and per-attempt timeout |
| `DLQ_PATH` | `dead-letter.jsonl` | Dead-letter file (0600; contains payloads) |

## Design decisions

- **Order of checks.** The body-size limit runs first so memory is bounded before any crypto. HMAC runs next, so a forged request never touches the dedupe store or the rate limiter and can't fill them.
- **Rate limit before dedupe.** If a delivery that gets a 429 were marked as seen, Shopify's retry would then be dropped as a "duplicate" and the event lost. The same applies to `503 queue_full`, which un-marks its keys. Both cases have tests.
- **Duplicates return `200`, not `4xx`.** Shopify should stop retrying something we already have.
- **429/503 are not free.** Backpressure makes Shopify retry later, but each `429 rate_limited` or `503 queue_full` counts as a failed delivery. Eight in a row and Shopify deletes the subscription. Size `RATE_PER_SHOP`/`RATE_BURST` (default 50/s, burst 100) and `QUEUE_SIZE` (default 10,000) for peak traffic such as flash sales, so backpressure is a last resort. Alert on `webhook_guard_rejected_total{reason="rate_limited"}` and `{reason="queue_full"}`.
- **`DEDUPE_TTL ≥ MAX_AGE + FUTURE_SKEW`** is enforced at startup. A byte-for-byte replay is then always either still remembered (duplicate) or too old (stale), never neither.
- **ACK-then-forward** trades durability for latency. The queue is in memory, so a crash, as opposed to a graceful shutdown, loses queued deliveries. Graceful shutdown drains the queue or dead-letters what's left. See the roadmap.
- **Full-jitter exponential backoff**, so many deliveries failing together don't all retry at the same moment against a recovering app.
- **Header allow-list on forward.** Only `X-Shopify-*`/`Shopify-*` and `Content-Type` headers reach the app. The original HMAC header is forwarded so the app can verify again (defense in depth).
- **Standard library only**, so there are no transitive dependencies to audit (`govulncheck` runs in CI).

## Threat model

**Defends against:** forged webhooks (no secret), tampered bodies, verbatim replays, duplicate deliveries, oversized bodies, slowloris-style header attacks (server timeouts), one shop flooding everyone else, and arbitrary headers leaking to the app.

**Does not defend against:**
- **Unsigned headers.** Shopify's HMAC covers only the body, so `Webhook-Id`, `Triggered-At`, `Shop-Domain`, and `Topic` can be changed by anyone holding a captured request. A captured body could be replayed with a new id and a fresh timestamp. `DEDUPE_BY_BODY=true` closes this gap within the TTL. Beyond the TTL, the app must process webhooks idempotently (for example, by comparing the resource's `updated_at`). This is a limit of the signing scheme, not something a proxy can fully fix.
- **Secret compromise.** Rotate using `SHOPIFY_SECRETS`.
- **Volumetric DDoS.** That belongs to the CDN or load balancer layer.
- **Multiple replicas.** Dedupe state is per process; use the Redis store on the roadmap.

### Shopify delivery facts this design relies on

From [Shopify's webhook docs](https://shopify.dev/docs/apps/build/webhooks/verify-deliveries):
- **Shopify retries a failed delivery 8 times over 4 hours.** The default `MAX_AGE=6h` accepts every legitimate retry. A shorter value would reject real retries as stale.
- **Shopify allows 1 s to connect and 5 s for the whole request.** Any non-2xx response, including 3xx, counts as a failure. This is why the guard acknowledges after verify + enqueue and forwards in the background.
- **After 8 consecutive failures, Shopify deletes the subscription** (if it was created through the Admin API). See "429/503 are not free" above.
- **Deduplicate on `X-Shopify-Webhook-Id`.** It is unique per delivery. `X-Shopify-Event-Id` is shared by every subscription's copy of the same event.

## Testing

```powershell
go test -cover ./...          # Windows (the race detector needs cgo)
go test -race -cover ./...    # Linux/macOS/CI
```

CI runs gofmt, `go vet`, tests with `-race` and coverage, staticcheck, govulncheck, and a Docker build.

## Benchmarks

Measured on 2026-11-24 on a laptop (Intel i5-13500H, 16 threads, Windows 11) over loopback. The load generator, guard, and mock app all ran on the same machine. Settings: `WORKERS=32`, `RATE_PER_SHOP=0` (all requests come from one shop), 64 concurrent senders, 20,000 signed requests per run, 3 runs after a warm-up.

| | Throughput | p50 | p99 | Rejected | Forwarded end-to-end |
|---|---|---|---|---|---|
| Before connection-pool fix | 6.8k–16.2k req/s | 0.5–0.7 ms | 43–73 ms | 20–25% `503 queue_full` | queue backed up |
| **After** | **28.3k–29.5k req/s** | **1.1 ms** | **6.6–8.1 ms** | **0** | **62,000 / 62,000** |

**What the benchmark found.** The first run showed ingest outpacing forwarding: the queue filled and backpressure (`503`) kicked in. The cause was Go's default `http.Transport`, which keeps only **2 idle connections per host**, so 30 of the 32 workers opened a new TCP connection for almost every forward. Sizing the connection pool to the worker count (`internal/proxy/forward.go`) removed the bottleneck.

Reproduce: `go run ./cmd/mockapp -quiet`, run the guard with the settings above, then `go run ./cmd/sendtest -secret s3cret -n 20000 -c 64`.

## Verified against real Shopify webhooks

On 2026-11-24, real webhooks **generated and signed by Shopify** (Shopify CLI 5.1.0, `shopify app webhook trigger`, topic `orders/create`, API version `2026-10`) were sent to the guard.

| Where | Signed with | Result |
|---|---|---|
| Live Render deployment | the deployed secret | `accepted 1`, `forwarded 1`, and the payload with all `X-Shopify-*` headers arrived at the upstream |
| Local guard | the configured secret | accepted; the 15,048-byte body was forwarded byte-for-byte |
| Local guard | a different secret | rejected `401` (`invalid_signature`) |

The captured delivery is now a regression test (`testdata/`, used by `internal/verify` and `internal/server`). The suite therefore checks a signature computed by **Shopify**, not only ones computed by our own `Sign()`, and it confirms that Shopify's real header formats pass every check: nanosecond `Triggered-At`, UUID `Webhook-Id`, `shop.myshopify.com`.

**What the real test caught.** The first live attempt was rejected as `invalid_signature`, and the metrics couldn't say why: a missing header, a malformed one, and a wrong secret all reported the same reason. Diagnosis showed it was a secret mismatch between the sender and the deployment. The code was correct. The guard now reports three reasons, `missing_signature`, `malformed_signature` and `invalid_signature`, so `/metrics` alone answers that question next time. The HTTP response is still a generic `401` that reveals nothing to an attacker.

## Roadmap

- [ ] Redis dedupe store (same `dedupe.Store` interface) for horizontal scaling
- [ ] Durable queue (Redis Streams or SQLite WAL) so ACKed deliveries survive crashes
- [ ] `go test -fuzz` on header parsing and verification
- [ ] DLQ replay command
- [ ] Example Rails app as the upstream
