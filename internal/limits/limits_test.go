package limits

import (
	"testing"
	"time"

	"github.com/belugashark/lazysusan/internal/clock"
)

func TestSlidingWindow_DisabledIsAlways(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	w := NewSlidingWindow(0, time.Minute, clk)
	for i := 0; i < 10_000; i++ {
		ok, _ := w.Allow("k")
		if !ok {
			t.Fatal("disabled limiter should always allow")
		}
	}
}

func TestSlidingWindow_RejectsAndWaits(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	w := NewSlidingWindow(3, time.Minute, clk)
	for i := 0; i < 3; i++ {
		ok, _ := w.Allow("k")
		if !ok {
			t.Fatalf("hit %d unexpectedly rejected", i)
		}
	}
	ok, wait := w.Allow("k")
	if ok {
		t.Fatal("4th should be rejected")
	}
	if wait <= 0 || wait > time.Minute {
		t.Fatalf("wait should be (0, 60s], got %v", wait)
	}
	clk.Advance(time.Minute)
	ok, _ = w.Allow("k")
	if !ok {
		t.Fatal("after window, should allow")
	}
}

func TestSlidingWindow_PerKeyIndependent(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	w := NewSlidingWindow(1, time.Minute, clk)
	if ok, _ := w.Allow("a"); !ok {
		t.Fatal("a first should pass")
	}
	if ok, _ := w.Allow("b"); !ok {
		t.Fatal("b first should pass")
	}
	if ok, _ := w.Allow("a"); ok {
		t.Fatal("a second should be rejected")
	}
}

func TestGlobalCounter(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	g := NewGlobalCounter(2, time.Minute, clk)
	if !g.Allow() || !g.Allow() {
		t.Fatal("first two should pass")
	}
	if g.Allow() {
		t.Fatal("third should be rejected")
	}
	if g.Count() != 2 {
		t.Fatalf("count=%d want 2", g.Count())
	}
	clk.Advance(time.Minute)
	if g.Count() != 0 {
		t.Fatalf("post-window count=%d want 0", g.Count())
	}
}

func TestFloodLimiter_AcceptsModerate(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	f := NewFloodLimiter(clk)
	// 100 small frames at start should fit in initial bucket (100 tokens).
	for i := 0; i < 100; i++ {
		if !f.Allow(1024) {
			t.Fatalf("frame %d rejected unexpectedly", i)
		}
	}
	// 101st: out of frame tokens -> reject.
	if f.Allow(1) {
		t.Fatal("101st frame should be rejected")
	}
}
