// Package ids generates the ephemeral identifiers used by lazysusan:
//   - client id: 26-char lowercase base32 (16 bytes of entropy)
//   - request id: 13-char lowercase base32 (8 bytes of entropy)
//   - token: base64url, 32 bytes of entropy, "stk_" prefix
package ids

import (
	"crypto/rand"
	"encoding/base32"
	"encoding/base64"
)

const lowercaseBase32 = "abcdefghijklmnopqrstuvwxyz234567"

var b32 = base32.NewEncoding(lowercaseBase32).WithPadding(base32.NoPadding)

// NewClientID returns a fresh 26-char client id.
func NewClientID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return b32.EncodeToString(buf[:]), nil
}

// NewRequestID returns a fresh 13-char request id.
func NewRequestID() (string, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return b32.EncodeToString(buf[:]), nil
}

// NewToken returns a fresh stk_-prefixed token.
func NewToken() (string, error) {
	var buf [32]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return "stk_" + base64.RawURLEncoding.EncodeToString(buf[:]), nil
}
