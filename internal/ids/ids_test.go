package ids

import "testing"

func TestNewClientID_Shape(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id, err := NewClientID()
		if err != nil {
			t.Fatal(err)
		}
		if len(id) != 26 {
			t.Fatalf("len=%d want 26", len(id))
		}
		if seen[id] {
			t.Fatalf("duplicate id: %q", id)
		}
		seen[id] = true
	}
}

func TestNewRequestID_Shape(t *testing.T) {
	for i := 0; i < 1000; i++ {
		r, err := NewRequestID()
		if err != nil {
			t.Fatal(err)
		}
		if len(r) != 13 {
			t.Fatalf("len=%d want 13", len(r))
		}
	}
}

func TestNewToken_Prefix(t *testing.T) {
	tok, err := NewToken()
	if err != nil {
		t.Fatal(err)
	}
	if len(tok) < 10 || tok[:4] != "stk_" {
		t.Fatalf("bad token: %q", tok)
	}
}
