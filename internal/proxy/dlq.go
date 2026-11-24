package proxy

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// DeadLetter is a delivery that could not be forwarded, with the reason.
type DeadLetter struct {
	Delivery
	Error    string    `json:"error"`
	Attempts int       `json:"attempts"`
	FailedAt time.Time `json:"failed_at"`
}

// DeadLetterWriter persists failed deliveries for inspection and replay.
type DeadLetterWriter interface {
	Write(DeadLetter) error
}

// FileDLQ appends dead letters to a JSON Lines file.
//
// The file contains full webhook payloads (customer PII for many topics), so
// it is created with 0600 permissions.
type FileDLQ struct {
	mu  sync.Mutex
	f   *os.File
	enc *json.Encoder
}

// OpenFileDLQ opens (or creates) path for appending.
func OpenFileDLQ(path string) (*FileDLQ, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	return &FileDLQ{f: f, enc: json.NewEncoder(f)}, nil
}

func (q *FileDLQ) Write(rec DeadLetter) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.enc.Encode(rec)
}

func (q *FileDLQ) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.f.Close()
}
