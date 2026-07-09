// Package clock provides a Now() abstraction so production code can use the
// standard time package and tests can inject deterministic time.
package clock

import (
	"sync"
	"time"
)

// Clock is the interface used everywhere we need time-related operations.
type Clock interface {
	Now() time.Time
}

// Real returns a Clock backed by the standard time package.
func Real() Clock { return realClock{} }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Fake is a deterministic clock for tests. Time advances only via Advance.
type Fake struct {
	mu  sync.Mutex
	now time.Time
}

// NewFake returns a Fake clock starting at t.
func NewFake(t time.Time) *Fake { return &Fake{now: t} }

func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Advance moves time forward by d.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	f.mu.Unlock()
}
