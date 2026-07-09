package clock

import (
	"testing"
	"time"
)

func TestFakeAdvance(t *testing.T) {
	f := NewFake(time.Unix(0, 0))
	if !f.Now().Equal(time.Unix(0, 0)) {
		t.Fatalf("Now=%v", f.Now())
	}
	f.Advance(2 * time.Second)
	if !f.Now().Equal(time.Unix(2, 0)) {
		t.Fatalf("after Advance Now=%v", f.Now())
	}
}

func TestRealNowMonotonic(t *testing.T) {
	r := Real()
	a := r.Now()
	for i := 0; i < 1000 && !r.Now().After(a); i++ {
	}
	b := r.Now()
	if !b.After(a) {
		t.Fatalf("now did not advance: %v -> %v", a, b)
	}
}
