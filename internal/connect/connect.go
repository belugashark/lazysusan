// Package connect parses the susan:// connect string a box uses to join a relay:
//
//	susan://[relay-key@]host          TLS (wss)
//	susan+insecure://[relay-key@]host plaintext, local dev (ws)
//
// The caller-side credential (the permit, <machine-id>:<call-secret> carried in
// the X-Susan-Permit header) is a plain string with no grammar to parse; it is
// assembled by cmd/susan and split by cmd/tray.
package connect

import (
	"errors"
	"fmt"
	"net/url"
)

// Connect is what a box needs to join a relay.
type Connect struct {
	RelayURL string // ws(s):// or http(s):// URL, as accepted by --relay today
	RelayKey string // admits the box to a gated relay; empty for open ones
}

// ParseConnect parses a connect string. Plain ws(s)/http(s) URLs pass through
// unchanged with an empty key.
func ParseConnect(s string) (Connect, error) {
	u, err := url.Parse(s)
	if err != nil {
		return Connect{}, fmt.Errorf("bad connect string: %w", err)
	}
	var wsScheme string
	switch u.Scheme {
	case "susan":
		wsScheme = "wss"
	case "susan+insecure":
		wsScheme = "ws"
	case "ws", "wss", "http", "https":
		wsScheme = "" // taken verbatim; validated below
	default:
		return Connect{}, fmt.Errorf("bad connect string: unsupported scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return Connect{}, errors.New("bad connect string: missing relay host")
	}
	if _, hasColon := u.User.Password(); hasColon {
		return Connect{}, errors.New("bad connect string: unexpected ':' in userinfo — a permit is a caller credential, not a connect string")
	}
	if wsScheme == "" {
		return Connect{RelayURL: s}, nil
	}
	// Reject rather than silently drop: a stray path, ?query, or #fragment is
	// most likely a paste error, and the wss URL built below would not carry it.
	if (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return Connect{}, errors.New("bad connect string: expected susan://[relay-key@]host with no path, query, or fragment")
	}
	relay := &url.URL{Scheme: wsScheme, Host: u.Host}
	return Connect{RelayURL: relay.String(), RelayKey: u.User.Username()}, nil
}
