// Command sendtest signs and sends fake Shopify webhooks to the guard.
//
// It doubles as a demo tool and a simple load generator:
//
//	sendtest -secret s3cret                     # one valid delivery
//	sendtest -secret s3cret -n 2 -id fixed      # duplicate detection
//	sendtest -secret s3cret -tamper             # tampered body -> 401
//	sendtest -secret s3cret -age 7h             # stale timestamp -> 401
//	sendtest -secret s3cret -n 20000 -c 64      # throughput + latency
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/iamnobody0101/shopify-webhook-guard/internal/verify"
)

const sampleBody = `{"id":820982911946154508,"email":"jon@example.com","total_price":"199.00","currency":"CAD","line_items":[{"id":866550311766439020,"title":"IPod Nano - 8GB","quantity":1,"price":"199.00"}]}`

func main() {
	url := flag.String("url", "http://localhost:8080/webhooks", "guard endpoint")
	secret := flag.String("secret", os.Getenv("SHOPIFY_SECRET"), "app client secret used to sign (or env SHOPIFY_SECRET)")
	shop := flag.String("shop", "demo-store.myshopify.com", "X-Shopify-Shop-Domain")
	topic := flag.String("topic", "orders/create", "X-Shopify-Topic")
	body := flag.String("body", sampleBody, "JSON payload")
	n := flag.Int("n", 1, "number of requests")
	c := flag.Int("c", 1, "concurrent senders")
	id := flag.String("id", "", "fixed X-Shopify-Webhook-Id (default: random per request)")
	tamper := flag.Bool("tamper", false, "modify the body after signing")
	age := flag.Duration("age", 0, "backdate X-Shopify-Triggered-At by this much")
	flag.Parse()

	if *secret == "" {
		fmt.Fprintln(os.Stderr, "sendtest: -secret or SHOPIFY_SECRET is required")
		os.Exit(2)
	}
	if *c < 1 {
		*c = 1
	}

	payload := []byte(*body)
	sig := verify.Sign(payload, *secret)
	if *tamper && len(payload) > 0 {
		payload = append([]byte(nil), payload...)
		payload[len(payload)-2] ^= 0x01 // flip a bit inside the JSON
	}

	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: *c},
	}

	var (
		mu        sync.Mutex
		statuses  = map[string]int{}
		latencies = make([]time.Duration, 0, *n)
		wg        sync.WaitGroup
		jobs      = make(chan struct{})
	)
	start := time.Now()
	for i := 0; i < *c; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				webhookID := *id
				if webhookID == "" {
					webhookID = randomID()
				}
				req, err := http.NewRequest(http.MethodPost, *url, bytes.NewReader(payload))
				if err != nil {
					panic(err)
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("X-Shopify-Hmac-Sha256", sig)
				req.Header.Set("X-Shopify-Topic", *topic)
				req.Header.Set("X-Shopify-Shop-Domain", *shop)
				req.Header.Set("X-Shopify-Api-Version", "2026-10")
				req.Header.Set("X-Shopify-Webhook-Id", webhookID)
				req.Header.Set("X-Shopify-Event-Id", randomID())
				req.Header.Set("X-Shopify-Triggered-At", time.Now().Add(-*age).UTC().Format(time.RFC3339Nano))

				t0 := time.Now()
				resp, err := client.Do(req)
				lat := time.Since(t0)
				key := "error"
				if err == nil {
					respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
					_ = resp.Body.Close()
					key = fmt.Sprintf("%d %s", resp.StatusCode, bytes.TrimSpace(respBody))
				} else if *n == 1 {
					fmt.Fprintln(os.Stderr, "request failed:", err)
				}
				mu.Lock()
				statuses[key]++
				latencies = append(latencies, lat)
				mu.Unlock()
			}
		}()
	}
	for i := 0; i < *n; i++ {
		jobs <- struct{}{}
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(start)

	fmt.Println("responses:")
	for k, v := range statuses {
		fmt.Printf("  %-24s %d\n", k, v)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	fmt.Printf("requests: %d in %v (%.0f req/s)\n", *n, elapsed.Round(time.Millisecond), float64(*n)/elapsed.Seconds())
	fmt.Printf("latency: p50=%v p95=%v p99=%v max=%v\n",
		pct(latencies, 0.50), pct(latencies, 0.95), pct(latencies, 0.99), pct(latencies, 1))
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p*float64(len(sorted))) - 1
	if i < 0 {
		i = 0
	}
	return sorted[i].Round(time.Microsecond)
}

func randomID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
