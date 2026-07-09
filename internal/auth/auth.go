// Package auth holds the constant-time token comparison used to validate the
// call secret from X-Susan-Permit against the value registered by the connected
// client.
package auth

import "crypto/subtle"

// padSize must be larger than any token we generate.
// 32 raw bytes -> 43 base64url chars; "stk_" prefix -> 47 chars max.
// 64 gives ample headroom while staying tiny.
const padSize = 64

// MaxTokenLen is the longest value Equal compares without truncating. Generated
// tokens are well under this; callers accepting operator-supplied secrets (e.g.
// SUSAN_RELAY_KEY) must reject longer values so the comparison stays exact.
const MaxTokenLen = padSize

func padTo(s string, n int) []byte {
	buf := make([]byte, n)
	if len(s) > n {
		copy(buf, s[:n])
		return buf
	}
	copy(buf, s)
	return buf
}

// Equal returns true iff a and b are byte-equal. It compares in constant time
// AND is constant-time with respect to lengths so an attacker cannot learn the
// stored token's length by timing the comparison.
func Equal(a, b string) bool {
	la := int32(len(a))
	lb := int32(len(b))
	pa := padTo(a, padSize)
	pb := padTo(b, padSize)
	return subtle.ConstantTimeEq(la, lb)&subtle.ConstantTimeCompare(pa, pb) == 1
}
