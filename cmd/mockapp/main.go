// Command mockapp is a stand-in for a Shopify app behind the guard. It logs
// each forwarded webhook and can simulate failures to demonstrate retries and
// the dead-letter queue.
package main

import (
	"flag"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"sync/atomic"
	"time"
)

func main() {
	addr := flag.String("addr", ":9090", "listen address")
	failRate := flag.Float64("fail-rate", 0, "fraction of requests answered with 500 (0..1)")
	quiet := flag.Bool("quiet", false, "don't log each request (for load tests)")
	flag.Parse()

	var count atomic.Int64
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 10<<20))
		n := count.Add(1)
		if rand.Float64() < *failRate {
			if !*quiet {
				log.Printf("#%d simulated failure -> 500", n)
			}
			http.Error(w, "simulated failure", http.StatusInternalServerError)
			return
		}
		if !*quiet {
			log.Printf("#%d topic=%s shop=%s webhook_id=%s bytes=%d",
				n, r.Header.Get("X-Shopify-Topic"), r.Header.Get("X-Shopify-Shop-Domain"),
				r.Header.Get("X-Shopify-Webhook-Id"), len(body))
		}
		w.WriteHeader(http.StatusOK)
	})

	log.Printf("mockapp listening on %s (fail-rate=%.2f)", *addr, *failRate)
	srv := &http.Server{Addr: *addr, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
