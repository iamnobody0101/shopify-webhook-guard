// Package verify implements Shopify webhook signature verification.
//
// Shopify signs every HTTPS webhook with HMAC-SHA256 over the *raw* request
// body using the app's client secret, and sends the base64-encoded digest in
// the X-Shopify-Hmac-Sha256 header.
package verify

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
)

var (
	ErrNoSecrets          = errors.New("verify: at least one secret is required")
	ErrEmptySecret        = errors.New("verify: empty secret")
	ErrMissingSignature   = errors.New("verify: missing signature")
	ErrMalformedSignature = errors.New("verify: malformed signature")
	ErrInvalidSignature   = errors.New("verify: invalid signature")
)

// Verifier checks signatures against one or more secrets. Accepting several
// secrets allows zero-downtime rotation: deploy with "new,old", rotate the
// secret in Shopify, then drop "old".
type Verifier struct {
	secrets [][]byte
}

// New returns a Verifier for the given secrets.
func New(secrets []string) (*Verifier, error) {
	if len(secrets) == 0 {
		return nil, ErrNoSecrets
	}
	v := &Verifier{}
	for _, s := range secrets {
		if s == "" {
			return nil, ErrEmptySecret
		}
		v.secrets = append(v.secrets, []byte(s))
	}
	return v, nil
}

// Verify reports whether signature is a valid Shopify HMAC for body.
//
// body must be the exact bytes received on the wire. Parsing and re-encoding
// the JSON first changes whitespace/key order and breaks verification.
func (v *Verifier) Verify(body []byte, signature string) error {
	if signature == "" {
		return ErrMissingSignature
	}
	got, err := base64.StdEncoding.DecodeString(signature)
	if err != nil || len(got) != sha256.Size {
		return ErrMalformedSignature
	}
	// Check every secret (no early exit) and compare in constant time so
	// response timing doesn't reveal how many bytes of a forged MAC matched.
	matched := false
	for _, s := range v.secrets {
		if hmac.Equal(compute(body, s), got) {
			matched = true
		}
	}
	if !matched {
		return ErrInvalidSignature
	}
	return nil
}

// Sign returns the base64 HMAC-SHA256 of body, exactly as Shopify computes it.
// It is used by tests and the sendtest tool.
func Sign(body []byte, secret string) string {
	return base64.StdEncoding.EncodeToString(compute(body, []byte(secret)))
}

func compute(body, secret []byte) []byte {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	return mac.Sum(nil)
}
