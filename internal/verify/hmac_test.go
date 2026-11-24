package verify

import (
	"encoding/base64"
	"encoding/hex"
	"errors"
	"testing"
)

func TestVerify(t *testing.T) {
	body := []byte(`{"id":820982911946154508,"total_price":"199.00"}`)
	v, err := New([]string{"new-secret", "old-secret"})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		body []byte
		sig  string
		want error
	}{
		{"valid with current secret", body, Sign(body, "new-secret"), nil},
		{"valid with rotated-out secret", body, Sign(body, "old-secret"), nil},
		{"valid empty body", []byte{}, Sign(nil, "new-secret"), nil},
		{"wrong secret", body, Sign(body, "attacker-guess"), ErrInvalidSignature},
		{"tampered body", []byte(`{"id":820982911946154508,"total_price":"1.00"}`), Sign(body, "new-secret"), ErrInvalidSignature},
		{"missing signature", body, "", ErrMissingSignature},
		{"not base64", body, "%%%not-base64%%%", ErrMalformedSignature},
		{"wrong digest length", body, base64.StdEncoding.EncodeToString([]byte("short")), ErrMalformedSignature},
		{"hex instead of base64", body, hex.EncodeToString(compute(body, []byte("new-secret"))), ErrMalformedSignature},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := v.Verify(tt.body, tt.sig); !errors.Is(got, tt.want) {
				t.Fatalf("Verify() = %v, want %v", got, tt.want)
			}
		})
	}
}

// A classic integration bug: verifying a re-serialized body instead of the
// raw bytes. Semantically identical JSON must NOT verify.
func TestVerifyRequiresRawBytes(t *testing.T) {
	v, _ := New([]string{"s"})
	raw := []byte(`{"id": 1, "note": "hi"}`)
	reserialized := []byte(`{"id":1,"note":"hi"}`)
	if err := v.Verify(reserialized, Sign(raw, "s")); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("re-serialized body verified: %v", err)
	}
}

func TestNew(t *testing.T) {
	if _, err := New(nil); !errors.Is(err, ErrNoSecrets) {
		t.Fatalf("New(nil) = %v, want ErrNoSecrets", err)
	}
	if _, err := New([]string{"ok", ""}); !errors.Is(err, ErrEmptySecret) {
		t.Fatalf("New with empty secret = %v, want ErrEmptySecret", err)
	}
}
