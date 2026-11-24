package metrics

import (
	"net/http/httptest"
	"sync"
	"testing"
)

func TestRegistry(t *testing.T) {
	r := New()
	series := Labeled("rejected_total", "reason", "invalid_signature")
	if series != `rejected_total{reason="invalid_signature"}` {
		t.Fatalf("Labeled() = %s", series)
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); r.Inc(series) }()
	}
	wg.Wait()
	r.Inc("accepted_total")
	r.SetGauge("queue_depth", func() float64 { return 7 })

	if r.Value(series) != 50 {
		t.Fatalf("Value() = %d, want 50", r.Value(series))
	}

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	want := "accepted_total 1\nqueue_depth 7\n" + series + " 50\n"
	if got := w.Body.String(); got != want {
		t.Fatalf("exposition:\n%s\nwant:\n%s", got, want)
	}
}
