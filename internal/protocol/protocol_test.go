package protocol

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRoundTrip_EndHeader(t *testing.T) {
	in := EndHeader{Type: TypeEndHeader, ReqID: "r_abc", Exit: 7, DurationMs: 41, TimedOut: false, Truncated: false, SizeBytes: 128}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out EndHeader
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("roundtrip mismatch: %#v vs %#v", out, in)
	}
}

func TestValidateHeaderField(t *testing.T) {
	cases := []struct {
		s    string
		max  int
		want bool
	}{
		{"abc123", 16, true},
		{"abc\r\n", 16, false},
		{"abc\x00", 16, false},
		{"  ok  ", 16, true},
		{"toolong", 4, false},
		{strings.Repeat("a", 1000), 64, false},
	}
	for _, c := range cases {
		if got := ValidateHeaderField(c.s, c.max); got != c.want {
			t.Errorf("ValidateHeaderField(%q,%d)=%v want %v", c.s, c.max, got, c.want)
		}
	}
}
