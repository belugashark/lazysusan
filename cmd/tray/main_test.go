package main

import (
	"strings"
	"testing"
)

func TestLandingPage(t *testing.T) {
	s := landingPage("https://relay.example", "owner/repo")
	for _, want := range []string{
		"curl -fsSL https://relay.example/get | sh",
		"curl https://relay.example \\",
		"-H 'X-Susan-Permit: <permit>' \\",
		"more: https://github.com/owner/repo#readme",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("landing page missing %q:\n%s", want, s)
		}
	}
}

func TestLandingPageCommentAlignment(t *testing.T) {
	// The three # comments must start in the same column regardless of the
	// public URL's length.
	for _, url := range []string{"https://lazysusan.sh", "https://a-much-longer-relay-hostname.example.com"} {
		cols := map[int]bool{}
		for _, line := range strings.Split(landingPage(url, "owner/repo"), "\n") {
			if i := strings.Index(line, "# "); i >= 0 {
				cols[i] = true
			}
		}
		if len(cols) != 1 {
			t.Errorf("url %s: comment columns not aligned: %v", url, cols)
		}
	}
}
