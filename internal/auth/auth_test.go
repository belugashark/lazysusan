package auth

import "testing"

func TestEqual(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"", "", true},
		{"stk_abc", "stk_abc", true},
		{"stk_abc", "stk_abd", false},
		{"short", "longer-token", false},
		{"stk_aaaa", "stk_aaab", false},
	}
	for _, c := range cases {
		if got := Equal(c.a, c.b); got != c.want {
			t.Errorf("Equal(%q,%q)=%v want %v", c.a, c.b, got, c.want)
		}
	}
}
