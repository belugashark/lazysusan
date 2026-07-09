// Package router holds the in-memory id -> *ClientConn map and the per-conn
// busy/respCh state machine.
package router

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/belugashark/lazysusan/internal/limits"
	"github.com/coder/websocket"
)

var (
	// ErrConnDead means the client disconnected; holders of a stale
	// *ClientConn get this instead of a panic.
	ErrConnDead = errors.New("conn dead")
	// ErrConnBusy means the client is serving another request; one command
	// runs at a time per machine.
	ErrConnBusy = errors.New("conn busy")
)

// Response is delivered to the HTTP handler via the channel returned by Reserve.
type Response struct {
	Err        error
	WSClosed   bool
	ProtoError bool

	ReqID      string
	Exit       int
	DurationMs int64
	TimedOut   bool
	Truncated  bool
	Output     []byte
}

// ClientConn is one connected client. The pointer-stable struct outlives the
// map entry so a stale holder sees ErrConnDead instead of a panic, and so a
// reconnecting client can Revive the same entry (keeping its machine id, hence
// its permit) instead of registering a fresh one.
type ClientConn struct {
	ID    string
	Token string
	// RegIP is the source IP of the (re)registration; used to key the per-IP
	// retain budget when this conn is held dead-but-resumable.
	RegIP string

	ClientVersion string

	ws    *websocket.Conn
	flood *limits.FloodLimiter

	mu          sync.Mutex
	dead        bool
	deadReason  error
	epoch       uint64
	busy        bool
	activeReq   string
	respCh      chan Response
	reqDeadline time.Time

	connCtx    context.Context
	cancelConn context.CancelFunc
}

// New makes a ClientConn. connCtx must be derived from the WS goroutine's
// context so cancelling everything tears the conn down.
func New(connCtx context.Context, cancel context.CancelFunc, id, token, version, regIP string, ws *websocket.Conn, flood *limits.FloodLimiter) *ClientConn {
	return &ClientConn{
		ID:            id,
		Token:         token,
		RegIP:         regIP,
		ClientVersion: version,
		ws:            ws,
		flood:         flood,
		connCtx:       connCtx,
		cancelConn:    cancel,
	}
}

// WS returns the underlying websocket connection.
func (c *ClientConn) WS() *websocket.Conn { return c.ws }

// Ctx returns the connection's lifetime context.
func (c *ClientConn) Ctx() context.Context { return c.connCtx }

// Flood returns the conn's flood limiter.
func (c *ClientConn) Flood() *limits.FloodLimiter { return c.flood }

// MarkDead fails any in-flight respCh and cancels the conn context. Idempotent.
func (c *ClientConn) MarkDead(reason error) {
	c.mu.Lock()
	if c.dead {
		c.mu.Unlock()
		return
	}
	c.dead = true
	c.deadReason = reason
	ch := c.respCh
	c.respCh = nil
	c.busy = false
	active := c.activeReq
	c.activeReq = ""
	c.mu.Unlock()
	if ch != nil {
		select {
		case ch <- Response{Err: reason, WSClosed: true, ReqID: active}:
		default:
		}
	}
	c.cancelConn()
}

// DeadReason returns the reason from the first MarkDead call, nil while alive.
func (c *ClientConn) DeadReason() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deadReason
}

// Epoch returns the current revive generation. A retained-conn reaper captures
// this at disconnect and only evicts if it is unchanged when the timer fires,
// so a conn that was revived and then died again is not evicted early by its
// first death's timer.
func (c *ClientConn) Epoch() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epoch
}

// Revive rebinds a dead conn to a fresh WebSocket and context, clearing the
// dead flag so callers can reach it again and bumping the epoch. It returns
// false if the conn is not currently dead (e.g. the still-live original is
// racing a duplicate resume), in which case the caller must not adopt it.
// MarkDead already reset busy/respCh/activeReq, so the revived conn starts idle.
func (c *ClientConn) Revive(connCtx context.Context, cancel context.CancelFunc, version, regIP string, ws *websocket.Conn) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.dead {
		return false
	}
	c.dead = false
	c.deadReason = nil
	c.epoch++
	c.ws = ws
	c.connCtx = connCtx
	c.cancelConn = cancel
	c.ClientVersion = version
	c.RegIP = regIP
	return true
}

// Reserve atomically marks this conn busy for reqID and installs a respCh.
func (c *ClientConn) Reserve(reqID string, reqDeadline time.Time) (chan Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dead {
		return nil, ErrConnDead
	}
	if c.busy {
		return nil, ErrConnBusy
	}
	c.busy = true
	c.activeReq = reqID
	c.respCh = make(chan Response, 1)
	c.reqDeadline = reqDeadline
	return c.respCh, nil
}

// Release frees the busy slot iff activeReq matches reqID. Idempotent.
func (c *ClientConn) Release(reqID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.activeReq != reqID {
		return
	}
	c.busy = false
	c.activeReq = ""
	c.respCh = nil
}

// ActiveReq returns (reqID, deadline, isBusy).
func (c *ClientConn) ActiveReq() (string, time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.activeReq, c.reqDeadline, c.busy
}

// ActiveReqID returns the currently-active request id, "" if idle.
func (c *ClientConn) ActiveReqID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.activeReq
}

// Deliver forwards r to whoever is waiting on respCh. No-op if the channel has
// already been retired (a competing failure path got there first).
func (c *ClientConn) Deliver(r Response) {
	c.mu.Lock()
	if c.activeReq != r.ReqID {
		c.mu.Unlock()
		return
	}
	ch := c.respCh
	c.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- r:
	default:
	}
}

// IsBusy reports whether a request is currently in flight.
func (c *ClientConn) IsBusy() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.busy
}

// Map is the top-level id -> *ClientConn registry.
type Map struct {
	mu    sync.RWMutex
	conns map[string]*ClientConn
}

// NewMap returns an empty registry.
func NewMap() *Map { return &Map{conns: make(map[string]*ClientConn)} }

// Add inserts c under c.ID. Returns false on id collision.
func (m *Map) Add(c *ClientConn) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.conns[c.ID]; ok {
		return false
	}
	m.conns[c.ID] = c
	return true
}

// Lookup returns the conn for id, or nil if absent.
func (m *Map) Lookup(id string) *ClientConn {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.conns[id]
}

// Resume runs adopt against the conn mapped at id, holding the map lock for the
// whole call so a concurrent RemoveIfStale cannot evict it mid-adoption. adopt
// (the caller's token check + Revive) reports whether it took ownership; on true
// Resume returns the still-mapped conn, else nil. The pointer is never removed
// or re-added, so there is no swap to race.
func (m *Map) Resume(id string, adopt func(*ClientConn) bool) *ClientConn {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.conns[id]
	if !ok {
		return nil
	}
	if !adopt(cur) {
		return nil
	}
	return cur
}

// Remove deletes id from the map iff the stored pointer matches c.
func (m *Map) Remove(c *ClientConn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cur, ok := m.conns[c.ID]; ok && cur == c {
		delete(m.conns, c.ID)
	}
}

// RemoveIfStale evicts a retained dead conn iff it is still the mapped pointer,
// still dead, and has not been revived since epoch was captured. It returns
// true if it evicted, so the caller can release the retain budget exactly once.
func (m *Map) RemoveIfStale(c *ClientConn, epoch uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.conns[c.ID]
	if !ok || cur != c {
		return false
	}
	c.mu.Lock()
	stale := c.dead && c.epoch == epoch
	c.mu.Unlock()
	if !stale {
		return false
	}
	delete(m.conns, c.ID)
	return true
}

// Snapshot returns the live conns only (a retained dead conn awaiting resume is
// neither counted nor returned), plus their in-flight count. Lookup still finds
// dead conns so resume and the reconnecting-503 path can reach them; callers
// that act on live clients (stats, drain) use Snapshot.
func (m *Map) Snapshot() (count, inFlight int, conns []*ClientConn) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	conns = make([]*ClientConn, 0, len(m.conns))
	for _, c := range m.conns {
		if c.DeadReason() != nil {
			continue
		}
		conns = append(conns, c)
		if c.IsBusy() {
			inFlight++
		}
	}
	count = len(conns)
	return
}
