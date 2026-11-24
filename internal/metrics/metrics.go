// Package metrics is a tiny dependency-free registry that serves counters and
// gauges in the Prometheus text exposition format.
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
)

// Registry holds named series such as `requests_total{reason="x"}`.
type Registry struct {
	mu       sync.RWMutex
	counters map[string]*atomic.Uint64
	gauges   map[string]func() float64
}

func New() *Registry {
	return &Registry{
		counters: make(map[string]*atomic.Uint64),
		gauges:   make(map[string]func() float64),
	}
}

// Labeled builds a series name with a single label.
func Labeled(name, label, value string) string {
	return fmt.Sprintf("%s{%s=%q}", name, label, value)
}

func (r *Registry) counter(series string) *atomic.Uint64 {
	r.mu.RLock()
	c, ok := r.counters[series]
	r.mu.RUnlock()
	if ok {
		return c
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok = r.counters[series]; !ok {
		c = new(atomic.Uint64)
		r.counters[series] = c
	}
	return c
}

// Inc increments a counter, creating it on first use.
func (r *Registry) Inc(series string) { r.counter(series).Add(1) }

// Value returns a counter's current value.
func (r *Registry) Value(series string) uint64 { return r.counter(series).Load() }

// SetGauge registers a function evaluated at scrape time.
func (r *Registry) SetGauge(series string, fn func() float64) {
	r.mu.Lock()
	r.gauges[series] = fn
	r.mu.Unlock()
}

// ServeHTTP writes all series, sorted, in Prometheus text format.
func (r *Registry) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	r.mu.RLock()
	lines := make([]string, 0, len(r.counters)+len(r.gauges))
	for name, c := range r.counters {
		lines = append(lines, fmt.Sprintf("%s %d", name, c.Load()))
	}
	for name, fn := range r.gauges {
		lines = append(lines, fmt.Sprintf("%s %g", name, fn()))
	}
	r.mu.RUnlock()
	sort.Strings(lines)

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	for _, l := range lines {
		fmt.Fprintln(w, l)
	}
}
