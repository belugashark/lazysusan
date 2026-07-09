package router

import (
	"context"
	"errors"
	"testing"
)

// newConn builds a ClientConn with a nil ws/flood: the revive/retention paths
// under test never dereference either.
func newConn(id, token, ip string) *ClientConn {
	ctx, cancel := context.WithCancel(context.Background())
	return New(ctx, cancel, id, token, "v1", ip, nil, nil)
}

func TestReviveRebindsSamePointer(t *testing.T) {
	m := NewMap()
	cc := newConn("id1", "tok", "1.1.1.1")
	m.Add(cc)
	cc.MarkDead(errors.New("drop"))
	if cc.DeadReason() == nil {
		t.Fatal("conn should be dead after MarkDead")
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	revived := m.Resume("id1", func(prev *ClientConn) bool {
		return prev.Revive(ctx2, cancel2, "v2", "2.2.2.2", nil)
	})
	if revived != cc {
		t.Fatal("Resume should revive and return the same pointer")
	}
	if cc.DeadReason() != nil {
		t.Fatal("conn should be alive after revive")
	}
	if cc.ClientVersion != "v2" || cc.RegIP != "2.2.2.2" {
		t.Fatalf("revive did not refresh fields: version=%q regip=%q", cc.ClientVersion, cc.RegIP)
	}
	if cc.Epoch() != 1 {
		t.Fatalf("epoch = %d, want 1 after one revive", cc.Epoch())
	}
}

func TestReviveRefusesLiveConn(t *testing.T) {
	cc := newConn("id", "tok", "ip")
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	if cc.Revive(ctx2, cancel2, "v2", "ip2", nil) {
		t.Fatal("Revive must refuse a conn that is not dead")
	}
}

func TestSnapshotSkipsDeadButLookupFindsIt(t *testing.T) {
	m := NewMap()
	live := newConn("live", "t", "ip")
	dead := newConn("dead", "t", "ip")
	m.Add(live)
	m.Add(dead)
	dead.MarkDead(errors.New("x"))

	count, _, conns := m.Snapshot()
	if count != 1 || len(conns) != 1 || conns[0] != live {
		t.Fatalf("Snapshot should return only the live conn, got count=%d", count)
	}
	if m.Lookup("dead") != dead {
		t.Fatal("Lookup must still find a retained dead conn (for resume / 503)")
	}
}

func TestRemoveIfStaleEpochGuard(t *testing.T) {
	m := NewMap()
	cc := newConn("id", "t", "ip")
	m.Add(cc)

	// First death schedules a reaper that captures e0.
	cc.MarkDead(errors.New("drop1"))
	e0 := cc.Epoch()

	// A resume revives it (epoch bumps), then it dies again.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	m.Resume("id", func(p *ClientConn) bool { return p.Revive(ctx2, cancel2, "v", "ip", nil) })
	cc.MarkDead(errors.New("drop2"))

	// The first death's reaper (e0) must not evict the conn that has since been
	// revived and re-died under a newer epoch.
	if m.RemoveIfStale(cc, e0) {
		t.Fatal("reaper with a stale epoch must not evict")
	}
	if m.Lookup("id") != cc {
		t.Fatal("conn was wrongly evicted by a stale-epoch reaper")
	}

	// The current death's reaper (current epoch) does evict.
	if !m.RemoveIfStale(cc, cc.Epoch()) {
		t.Fatal("reaper with the current epoch should evict a dead conn")
	}
	if m.Lookup("id") != nil {
		t.Fatal("conn should be gone after eviction")
	}
}

func TestRemoveIfStaleLeavesLiveConn(t *testing.T) {
	m := NewMap()
	cc := newConn("id", "t", "ip")
	m.Add(cc)
	// A live conn is never stale, whatever epoch a reaper captured.
	if m.RemoveIfStale(cc, cc.Epoch()) {
		t.Fatal("RemoveIfStale must not evict a live conn")
	}
	if m.Lookup("id") != cc {
		t.Fatal("live conn wrongly evicted")
	}
}

func TestResumeMissAndDeclineReturnNil(t *testing.T) {
	m := NewMap()
	if got := m.Resume("nope", func(*ClientConn) bool { return true }); got != nil {
		t.Fatal("Resume on an unknown id should return nil")
	}
	cc := newConn("id", "t", "ip")
	m.Add(cc)
	if got := m.Resume("id", func(*ClientConn) bool { return false }); got != nil {
		t.Fatal("Resume should return nil when adopt declines (e.g. token mismatch)")
	}
}
