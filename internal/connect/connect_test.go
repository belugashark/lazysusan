package connect

import (
	"strings"
	"testing"
)

func TestParseConnect(t *testing.T) {
	cases := []struct {
		name     string
		in       string
		relayURL string
		relayKey string
	}{
		{"gated", "susan://secret123@relay.example.com", "wss://relay.example.com", "secret123"},
		{"open", "susan://relay.example.com", "wss://relay.example.com", ""},
		{"insecure gated", "susan+insecure://secret@localhost:8199", "ws://localhost:8199", "secret"},
		{"insecure open", "susan+insecure://localhost:8199", "ws://localhost:8199", ""},
		{"trailing slash ok", "susan://key@relay.example.com/", "wss://relay.example.com", "key"},
		{"ws passthrough", "ws://localhost:8199", "ws://localhost:8199", ""},
		{"wss passthrough", "wss://relay.example.com", "wss://relay.example.com", ""},
		{"http passthrough", "http://localhost:8199", "http://localhost:8199", ""},
		{"https passthrough", "https://relay.example.com", "https://relay.example.com", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ParseConnect(c.in)
			if err != nil {
				t.Fatalf("ParseConnect(%q): %v", c.in, err)
			}
			if got.RelayURL != c.relayURL || got.RelayKey != c.relayKey {
				t.Fatalf("ParseConnect(%q) = %q/%q, want %q/%q", c.in, got.RelayURL, got.RelayKey, c.relayURL, c.relayKey)
			}
		})
	}
}

func TestParseConnectRejectsPermitShapedUserinfo(t *testing.T) {
	_, err := ParseConnect("susan://iwyxhpsk:stk_abc@relay.example.com")
	if err == nil {
		t.Fatal("want error for id:token userinfo in a connect string")
	}
	if !strings.Contains(err.Error(), "permit") {
		t.Fatalf("error should point at the permit confusion, got: %v", err)
	}
}

func TestParseConnectRejectsBadInput(t *testing.T) {
	for _, in := range []string{
		"", "ftp://relay.example.com", "relay.example.com", "susan://",
		"http://",                                  // passthrough with empty host
		"susan://key@relay.example.com?x=1",        // query silently changing meaning
		"susan://key@relay.example.com#frag",       // fragment likewise
		"susan://key@relay.example.com/base",       // relays are not served under a path
		"wss://iwyxhpsk:stk_abc@relay.example.com", // permit-shaped userinfo in passthrough
	} {
		if _, err := ParseConnect(in); err == nil {
			t.Fatalf("ParseConnect(%q): want error", in)
		}
	}
}
