package verify

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
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

// Verifies a signature computed by Shopify itself, not by our own Sign(), so
// a shared bug in Sign and Verify couldn't make this pass. See testdata/README.md.
func TestVerifyRealShopifySignature(t *testing.T) {
	body, err := os.ReadFile("../../testdata/shopify_orders_create_2026-10.json")
	if err != nil {
		t.Fatal(err)
	}
	const shopifySig = "/WGG5GIepPtQJVZF3fdeUBzDrzhd0QmX9J+aERuu5mc="
	v, _ := New([]string{"local-test-secret"})

	if err := v.Verify(body, shopifySig); err != nil {
		t.Fatalf("real Shopify signature rejected: %v", err)
	}

	// One flipped bit anywhere must fail.
	tampered := bytes.Clone(body)
	tampered[len(tampered)/2] ^= 0x01
	if err := v.Verify(tampered, shopifySig); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("tampered real body: %v", err)
	}

	// Shopify's body is pretty-printed; the same JSON compacted must fail.
	var compact bytes.Buffer
	if err := json.Compact(&compact, body); err != nil {
		t.Fatal(err)
	}
	if err := v.Verify(compact.Bytes(), shopifySig); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("re-serialized real body verified: %v", err)
	}

	// Wrong secret must fail.
	other, _ := New([]string{"some-other-secret"})
	if err := other.Verify(body, shopifySig); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("wrong secret: %v", err)
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
