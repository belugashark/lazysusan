// Package limits implements the rate, miss, unauth, and WS flood limiters.
// All limiters take a clock.Clock for deterministic tests.
package limits

import (
	"hash/fnv"
	"sync"
	"time"

	"github.com/belugashark/lazysusan/internal/clock"
)

const numShards = 16

func shardFor(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32() % numShards
}

// pruneExpired drops the expired prefix of hits; hits are appended in time
// order, so everything at or before cutoff sits at the front.
func pruneExpired(hits []time.Time, cutoff time.Time) []time.Time {
	i := 0
	for i < len(hits) && !hits[i].After(cutoff) {
		i++
	}
	return hits[i:]
}

// SlidingWindow is a fixed-budget-per-minute sliding-window limiter. limit==0
// disables the check entirely (Allow returns true, RetryAfter returns 0).
type SlidingWindow struct {
	limit  int
	window time.Duration
	clock  clock.Clock
	shards [numShards]struct {
		mu  sync.Mutex
		hit map[string][]time.Time
	}
}

// NewSlidingWindow returns a per-key limiter with the given budget per window.
func NewSlidingWindow(limit int, window time.Duration, c clock.Clock) *SlidingWindow {
	s := &SlidingWindow{limit: limit, window: window, clock: c}
	for i := range s.shards {
		s.shards[i].hit = make(map[string][]time.Time)
	}
	return s
}

// Allow consumes one slot for key, returning true on admission and the wait
// duration until the next free slot if rejected.
func (s *SlidingWindow) Allow(key string) (bool, time.Duration) {
	if s.limit == 0 {
		return true, 0
	}
	now := s.clock.Now()
	cutoff := now.Add(-s.window)
	sh := &s.shards[shardFor(key)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	hits := pruneExpired(sh.hit[key], cutoff)
	if len(hits) >= s.limit {
		// next slot frees when the oldest in window expires
		wait := hits[0].Add(s.window).Sub(now)
		if wait < 0 {
			wait = 0
		}
		sh.hit[key] = hits
		return false, wait
	}
	hits = append(hits, now)
	sh.hit[key] = hits
	return true, 0
}

// GC walks one shard and drops entries with no recent hits. Call periodically.
func (s *SlidingWindow) GC(shard int) {
	if s.limit == 0 {
		return
	}
	if shard < 0 || shard >= numShards {
		return
	}
	cutoff := s.clock.Now().Add(-s.window)
	sh := &s.shards[shard]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	for k, hits := range sh.hit {
		hits = pruneExpired(hits, cutoff)
		if len(hits) == 0 {
			delete(sh.hit, k)
		} else {
			sh.hit[k] = hits
		}
	}
}

// GlobalCounter is a single-key sliding-window counter. limit==0 disables it.
type GlobalCounter struct {
	limit  int
	window time.Duration
	clock  clock.Clock
	mu     sync.Mutex
	hits   []time.Time
}

// NewGlobalCounter returns a single-bucket sliding-window counter.
func NewGlobalCounter(limit int, window time.Duration, c clock.Clock) *GlobalCounter {
	return &GlobalCounter{limit: limit, window: window, clock: c}
}

// Allow returns true if a slot was consumed and false if the limit is hit.
func (g *GlobalCounter) Allow() bool {
	if g.limit == 0 {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.clock.Now()
	g.hits = pruneExpired(g.hits, now.Add(-g.window))
	if len(g.hits) >= g.limit {
		return false
	}
	g.hits = append(g.hits, now)
	return true
}

// Count returns the count of hits within the current window.
func (g *GlobalCounter) Count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.hits = pruneExpired(g.hits, g.clock.Now().Add(-g.window))
	return len(g.hits)
}

// FloodLimiter enforces a per-connection frame-rate + byte-rate cap. Not
// configurable: 100 fps, 5 MiB/s sustained.
type FloodLimiter struct {
	clock        clock.Clock
	framesPerSec int
	bytesPerSec  int

	mu          sync.Mutex
	frameTokens float64
	byteTokens  float64
	lastRefill  time.Time
}

// NewFloodLimiter returns a token-bucket limiter at the spec'd rates.
func NewFloodLimiter(c clock.Clock) *FloodLimiter {
	return &FloodLimiter{
		clock:        c,
		framesPerSec: 100,
		bytesPerSec:  5 * 1024 * 1024,
		frameTokens:  100,
		byteTokens:   5 * 1024 * 1024,
		lastRefill:   c.Now(),
	}
}

func (f *FloodLimiter) refillLocked(now time.Time) {
	delta := now.Sub(f.lastRefill).Seconds()
	if delta <= 0 {
		return
	}
	f.lastRefill = now
	f.frameTokens += delta * float64(f.framesPerSec)
	if f.frameTokens > float64(f.framesPerSec) {
		f.frameTokens = float64(f.framesPerSec)
	}
	f.byteTokens += delta * float64(f.bytesPerSec)
	if f.byteTokens > float64(f.bytesPerSec) {
		f.byteTokens = float64(f.bytesPerSec)
	}
}

// Allow accounts for one frame of n bytes and returns false if either bucket
// would be exhausted.
func (f *FloodLimiter) Allow(n int) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.clock.Now()
	f.refillLocked(now)
	if f.frameTokens < 1 {
		return false
	}
	if f.byteTokens < float64(n) {
		return false
	}
	f.frameTokens--
	f.byteTokens -= float64(n)
	return true
}
