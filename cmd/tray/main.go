// Command tray is the lazysusan relay binary.
//
// It accepts WebSocket registrations on /ws from susan clients and POSTs on /
// (authorized by an X-Susan-Permit header) from curl-like callers, multiplexing
// each call onto the matching client's WebSocket.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/belugashark/lazysusan/internal/auth"
	"github.com/belugashark/lazysusan/internal/clock"
	"github.com/belugashark/lazysusan/internal/ids"
	"github.com/belugashark/lazysusan/internal/install"
	"github.com/belugashark/lazysusan/internal/limits"
	"github.com/belugashark/lazysusan/internal/logging"
	"github.com/belugashark/lazysusan/internal/protocol"
	"github.com/belugashark/lazysusan/internal/router"
)

var (
	version  = "dev"
	buildSha = "unknown"
)

type config struct {
	port               string
	publicURL          string
	insecure           bool
	trustXFF           bool
	maxResponseBytes   int64
	rateLimitPerMin    int
	ipMissLimitPerMin  int
	globalUnauthPerMin int
	maxUnregWSPerIP    int
	maxUnregWSTotal    int
	maxCmdBytes        int64
	maxTimeoutSecs     int
	logLevel           string
	repo               string
	relayKey           string
}

const (
	wsReadLimitBytes  = 1 << 20
	registerTimeout   = 5 * time.Second
	keepalivePeriod   = 25 * time.Second
	keepaliveTimeout  = 60 * time.Second
	binaryIdleTimeout = 10 * time.Second
	// resumeGraceWindow is how long a dropped client keeps its machine id (and
	// thus its permit) claimable by a reconnect. A reconnect fires within
	// seconds of the drop, so this only needs to cover a brief network blip; a
	// few minutes is ample and keeps retained-conn memory trivially bounded.
	resumeGraceWindow = 2 * time.Minute
	// maxRetainTotal / maxRetainPerIP bound how many dead-but-resumable conns
	// are held at once, so connect/register/disconnect churn cannot pin
	// unbounded memory. Over budget, a disconnect drops the conn immediately
	// (no resume) rather than growing the map.
	maxRetainTotal = 256
	maxRetainPerIP = 3
	// wsWriteTimeout bounds a single write to a client WS so a wedged (slow or
	// stuck) reader cannot pin the request goroutine; the request frame is tiny,
	// so 10s is ample even on a slow link.
	wsWriteTimeout = 10 * time.Second
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "--version", "-v":
			fmt.Printf("tray %s %s %s\n", version, buildSha, time.Now().UTC().Format("2006-01-02"))
			return
		case "--help", "-h":
			printHelp()
			return
		}
	}
	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	logger := logging.New(cfg.logLevel)
	if err := run(cfg, logger); err != nil {
		logger.Error("tray exited with error", "err", err.Error())
		os.Exit(1)
	}
}

func printHelp() {
	fmt.Println(`tray — relay for the lazysusan reverse-tunneling tool

usage: tray

env vars:
  PORT                          HTTP listen port (default 8080)
  SUSAN_PUBLIC_URL               required, public https:// URL clients see
  SUSAN_INSECURE                 1 to allow non-https SUSAN_PUBLIC_URL (dev only)
  SUSAN_TRUST_XFF                1 to honor X-Forwarded-For for rate-limit keying
  SUSAN_MAX_RESPONSE_BYTES       default 10485760; 0 = unbounded
  SUSAN_RATE_LIMIT_PER_MIN       default 60; 0 = disabled
  SUSAN_IP_MISS_LIMIT_PER_MIN    default 30; 0 = disabled
  SUSAN_GLOBAL_UNAUTH_PER_MIN    default 1000; 0 = disabled
  SUSAN_MAX_UNREG_WS_PER_IP      default 5; 0 = disabled
  SUSAN_MAX_UNREG_WS_TOTAL       default 500; 0 = disabled
  SUSAN_MAX_CMD_BYTES            default 65536 (hard cap)
  SUSAN_MAX_TIMEOUT_SECONDS      default 60; hard-capped at 290
  SUSAN_LOG_LEVEL                debug|info|warn|error
  SUSAN_REPO                     owner/repo for the /get install script
  SUSAN_RELAY_KEY                relay key: if set, clients must present it to register
                                and /stats requires it as a Bearer token; unset = open relay`)
}

func loadConfig() (*config, error) {
	flag.CommandLine = flag.NewFlagSet("tray", flag.ContinueOnError)
	flag.CommandLine.SetOutput(io.Discard)
	_ = flag.CommandLine.Parse(os.Args[1:])

	cfg := &config{
		port:               envOr("PORT", "8080"),
		publicURL:          os.Getenv("SUSAN_PUBLIC_URL"),
		insecure:           os.Getenv("SUSAN_INSECURE") == "1",
		trustXFF:           os.Getenv("SUSAN_TRUST_XFF") == "1",
		maxResponseBytes:   envInt64("SUSAN_MAX_RESPONSE_BYTES", 10*1024*1024),
		rateLimitPerMin:    envInt("SUSAN_RATE_LIMIT_PER_MIN", 60),
		ipMissLimitPerMin:  envInt("SUSAN_IP_MISS_LIMIT_PER_MIN", 30),
		globalUnauthPerMin: envInt("SUSAN_GLOBAL_UNAUTH_PER_MIN", 1000),
		maxUnregWSPerIP:    envInt("SUSAN_MAX_UNREG_WS_PER_IP", 5),
		maxUnregWSTotal:    envInt("SUSAN_MAX_UNREG_WS_TOTAL", 500),
		maxCmdBytes:        envInt64("SUSAN_MAX_CMD_BYTES", 65536),
		maxTimeoutSecs:     envInt("SUSAN_MAX_TIMEOUT_SECONDS", 60),
		logLevel:           envOr("SUSAN_LOG_LEVEL", "info"),
		repo:               envOr("SUSAN_REPO", "belugashark/lazysusan"),
		relayKey:           os.Getenv("SUSAN_RELAY_KEY"),
	}
	if cfg.publicURL == "" {
		return nil, errors.New("tray: SUSAN_PUBLIC_URL is required (set to your public https:// URL)")
	}
	if len(cfg.relayKey) > auth.MaxTokenLen {
		return nil, fmt.Errorf("tray: SUSAN_RELAY_KEY must be at most %d characters", auth.MaxTokenLen)
	}
	if !strings.HasPrefix(cfg.publicURL, "https://") && !cfg.insecure {
		return nil, errors.New("tray: SUSAN_PUBLIC_URL must be https:// (set SUSAN_INSECURE=1 to override for local dev)")
	}
	if cfg.maxTimeoutSecs > protocol.HardMaxTimeoutSecs {
		cfg.maxTimeoutSecs = protocol.HardMaxTimeoutSecs
	}
	if cfg.maxTimeoutSecs < 1 {
		cfg.maxTimeoutSecs = 1
	}
	return cfg, nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
func envInt64(k string, def int64) int64 {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

type server struct {
	cfg    *config
	log    *slog.Logger
	clk    clock.Clock
	startT time.Time

	rmap         *router.Map
	rateLimiter  *limits.SlidingWindow
	missLimiter  *limits.SlidingWindow
	unauthGlobal *limits.GlobalCounter

	unregPerIPMu sync.Mutex
	unregPerIP   map[string]int
	unregTotal   int64

	retainMu    sync.Mutex
	retainPerIP map[string]int
	retainTotal int

	requestsWindow    *limits.GlobalCounter
	disconnectsWindow *limits.GlobalCounter

	draining atomic.Bool
}

func run(cfg *config, log *slog.Logger) error {
	clk := clock.Real()
	// requestsWindow and disconnectsWindow are counter-only; the huge limit
	// means Allow never trips so Count() reflects the rolling minute.
	srv := &server{
		cfg:               cfg,
		log:               log,
		clk:               clk,
		startT:            clk.Now(),
		rmap:              router.NewMap(),
		rateLimiter:       limits.NewSlidingWindow(cfg.rateLimitPerMin, time.Minute, clk),
		missLimiter:       limits.NewSlidingWindow(cfg.ipMissLimitPerMin, time.Minute, clk),
		unauthGlobal:      limits.NewGlobalCounter(cfg.globalUnauthPerMin, time.Minute, clk),
		unregPerIP:        make(map[string]int),
		retainPerIP:       make(map[string]int),
		requestsWindow:    limits.NewGlobalCounter(1<<30, time.Minute, clk),
		disconnectsWindow: limits.NewGlobalCounter(1<<30, time.Minute, clk),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", srv.handleHealthz)
	mux.HandleFunc("/stats", srv.handleStats)
	mux.HandleFunc("/get", srv.handleGet)
	mux.HandleFunc("/ws", srv.handleWS)
	mux.HandleFunc("/", srv.handleRoot)

	addr := ":" + cfg.port
	// WriteTimeout intentionally 0; per-request ctx enforces the deadline.
	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	fmt.Fprintf(os.Stdout, "tray %s %s listening %s public_url=%s\n", version, buildSha, addr, cfg.publicURL)
	log.Info("tray starting", "version", version, "build_sha", buildSha, "addr", addr, "public_url", cfg.publicURL)

	stopGC := make(chan struct{})
	go srv.gcLoop(stopGC)

	errCh := make(chan error, 1)
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
	case err := <-errCh:
		close(stopGC)
		return err
	}
	stop()
	log.Info("tray drain start")
	srv.draining.Store(true)
	srv.drain()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	close(stopGC)
	log.Info("tray drain complete")
	return nil
}

// gcLoop sweeps one limiter shard each tick: 16 shards, swept every 60s/16.
func (s *server) gcLoop(stop <-chan struct{}) {
	t := time.NewTicker(time.Minute / 16)
	defer t.Stop()
	shard := 0
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			s.rateLimiter.GC(shard)
			s.missLimiter.GC(shard)
			shard = (shard + 1) % 16
		}
	}
}

// clientIP honors X-Forwarded-For only when SUSAN_TRUST_XFF=1; otherwise an
// attacker reaching tray directly could spoof the header to evade rate limits.
func (s *server) clientIP(r *http.Request) string {
	if s.cfg.trustXFF {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			if i := strings.Index(xff, ","); i >= 0 {
				return strings.TrimSpace(xff[:i])
			}
			return strings.TrimSpace(xff)
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (s *server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	if r.Method == http.MethodPost {
		s.handleClientPost(w, r)
		return
	}
	body := landingPage(strings.TrimRight(s.cfg.publicURL, "/"), s.cfg.repo)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	_, _ = io.WriteString(w, body)
}

// landingPage renders the plain-text page served on GET /: banner, install
// and usage instructions, and the endpoint list. There is no separate /help —
// this page is the whole story; anything more lives in the README.
func landingPage(publicURL, repo string) string {
	// Right-pad so the trailing # comments stay aligned however long the
	// relay's public URL is. The curl example is the longest commented line.
	col := len(`  curl `+publicURL+` \`) + 2
	if col < 39 {
		col = 39
	}
	pad := func(s string) string {
		return s + strings.Repeat(" ", col-len(s))
	}
	lines := []string{
		`        .-""-.   .-""-.   .-""-.`,
		`       ( curl ) (  ls  ) ( echo )`,
		"  .-----`----'---`----'---`----'-----.",
		`   \    l  a  z  y  s  u  s  a  n   /`,
		"    `-----------.________.---------'",
		``,
		`your shell, dished out over https —`,
		`run commands on your machine from anywhere, with curl.`,
		``,
		`install the client:`,
		``,
		`  curl -fsSL ` + publicURL + `/get | sh`,
		``,
		`use it:`,
		``,
		pad(`  susan`) + `# on the machine; prints a ready-made curl with permit`,
		``,
		pad(`  curl `+publicURL+` \`) + `# from anywhere`,
		`    -H 'X-Susan-Permit: <permit>' \`,
		pad(`    -d 'uptime'`) + `# the command you want to run`,
		``,
		`endpoints:`,
		``,
		`  GET  /            this page`,
		`  GET  /get         install script`,
		`  POST /            run a command (X-Susan-Permit header required)`,
		`  GET  /healthz     relay health`,
		`  GET  /stats       relay stats`,
		``,
		`more: https://github.com/` + repo + `#readme`,
	}
	return strings.Join(lines, "\n") + "\n"
}

func (s *server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		// Report unhealthy while draining so the orchestrator's healthcheck
		// stops routing to a replica that is shutting down; new POSTs 503 anyway.
		writeEmpty(w, http.StatusServiceUnavailable, "")
		return
	}
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Length", "2")
	_, _ = io.WriteString(w, "ok")
}

// statsAuthorized gates /stats when SUSAN_RELAY_KEY is set: the operator
// must present it as a Bearer token. With no token (open relay), /stats is public.
func (s *server) statsAuthorized(r *http.Request) bool {
	if s.cfg.relayKey == "" {
		return true
	}
	const prefix = "Bearer "
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, prefix) {
		return false
	}
	return auth.Equal(s.cfg.relayKey, strings.TrimPrefix(h, prefix))
}

func (s *server) handleStats(w http.ResponseWriter, r *http.Request) {
	if !s.statsAuthorized(r) {
		writeEmpty(w, http.StatusUnauthorized, "")
		return
	}
	count, inFlight, _ := s.rmap.Snapshot()
	payload := map[string]any{
		"connected_clients":       count,
		"in_flight":               inFlight,
		"requests_last_min":       s.requestsWindow.Count(),
		"ws_disconnects_last_min": s.disconnectsWindow.Count(),
		"version":                 version,
		"build_sha":               buildSha,
		"uptime_seconds":          int64(time.Since(s.startT).Seconds()),
	}
	b, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, "marshal failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	_, _ = w.Write(b)
}

func (s *server) handleGet(w http.ResponseWriter, _ *http.Request) {
	// A dev build (no release version stamped in, e.g. a from-source Docker
	// build) has no matching GitHub release to pin, so serve the latest one.
	v := "latest"
	if version != "dev" {
		v = "v" + version
	}
	script := install.Render(install.Config{
		Repo:    s.cfg.repo,
		Version: v,
	})
	w.Header().Set("Content-Type", "text/x-shellscript")
	w.Header().Set("Content-Length", strconv.Itoa(len(script)))
	_, _ = io.WriteString(w, script)
}

func (s *server) handleWS(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		http.Error(w, "draining", http.StatusServiceUnavailable)
		return
	}
	if r.Header.Get("Origin") != "" {
		http.Error(w, "origin not permitted", http.StatusForbidden)
		return
	}
	ip := s.clientIP(r)
	if !s.reserveUnreg(ip) {
		http.Error(w, "too many unregistered ws", http.StatusServiceUnavailable)
		return
	}
	// Tracks whether this goroutine still owns the pre-register slot.
	released := false
	defer func() {
		if !released {
			s.releaseUnreg(ip)
		}
	}()

	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: false})
	if err != nil {
		s.log.Warn("ws accept failed", "err", err.Error())
		return
	}
	c.SetReadLimit(wsReadLimitBytes)

	connCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	regCtx, regCancel := context.WithTimeout(connCtx, registerTimeout)
	var reg protocol.Register
	err = wsjson.Read(regCtx, c, &reg)
	regCancel()
	if err != nil {
		c.Close(protocol.CloseProtocolError, "register timeout")
		return
	}
	if reg.Type != protocol.TypeRegister || reg.Token == "" {
		errFrame := protocol.ErrorFrame{Type: protocol.TypeError, Code: protocol.ErrBadRegister, Message: "first frame must be register"}
		_ = wsjson.Write(connCtx, c, errFrame)
		c.Close(protocol.CloseProtocolError, "bad register")
		return
	}
	if s.cfg.relayKey != "" && !auth.Equal(s.cfg.relayKey, reg.RelayKey) {
		errFrame := protocol.ErrorFrame{Type: protocol.TypeError, Code: protocol.ErrUnauthorized, Message: "relay key required"}
		_ = wsjson.Write(connCtx, c, errFrame)
		c.Close(protocol.CloseProtocolError, "unauthorized")
		s.log.Warn("register rejected: bad relay key", "src_ip", ip)
		return
	}
	if s.cfg.insecure {
		s.log.Warn("tray: WARNING — running in insecure mode; tokens transit in cleartext")
	}

	// A ResumeID asks to rebind to a prior machine id so the reconnecting client
	// keeps its permit. Honor it only if the token matches the retained conn.
	// The lookup + token check + revive run under the map lock (via Resume) so
	// the retain reaper cannot evict the conn between lookup and revive; on
	// success we hand back the retain budget the disconnect path reserved.
	// Unknown id, expired, or wrong token all fall through to a fresh id, which
	// reveals nothing an ordinary registration wouldn't.
	var cc *router.ClientConn
	id := reg.ResumeID
	resumed := false
	if reg.ResumeID != "" {
		var oldIP string
		if revived := s.rmap.Resume(reg.ResumeID, func(prev *router.ClientConn) bool {
			if !auth.Equal(prev.Token, reg.Token) {
				return false
			}
			oldIP = prev.RegIP
			return prev.Revive(connCtx, cancel, reg.ClientVersion, ip, c)
		}); revived != nil {
			cc = revived
			resumed = true
			s.releaseRetain(oldIP)
		}
	}
	if cc == nil {
		newID, err := ids.NewClientID()
		if err != nil {
			c.Close(protocol.CloseProtocolError, "id alloc failed")
			return
		}
		id = newID
		flood := limits.NewFloodLimiter(s.clk)
		cc = router.New(connCtx, cancel, id, reg.Token, reg.ClientVersion, ip, c, flood)
		if !s.rmap.Add(cc) {
			c.Close(protocol.CloseProtocolError, "id collision")
			return
		}
	}
	s.releaseUnreg(ip)
	released = true
	s.log.Info("register", "client_id", id, "client_version", reg.ClientVersion, "src_ip", ip, "resumed", resumed)

	welcome := protocol.Welcome{Type: protocol.TypeWelcome, ID: id, PublicURL: strings.TrimRight(s.cfg.publicURL, "/")}
	if err := wsjson.Write(connCtx, c, welcome); err != nil {
		s.rmap.Remove(cc)
		cc.MarkDead(err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); s.keepaliveLoop(connCtx, cc) }()
	go func() { defer wg.Done(); s.readerLoop(connCtx, cc) }()
	wg.Wait()

	cc.MarkDead(errors.New("connection closed"))
	s.disconnectsWindow.Allow()

	// Retain the dead conn (in the map, unreachable to Snapshot but findable by
	// Lookup) so a reconnecting client can resume its permit within the grace
	// window. Budget-gate it so churn can't pin unbounded memory; over budget or
	// while draining we drop it immediately, degrading to no-resume. A per-conn
	// reaper evicts it when the window lapses, unless a revive bumped the epoch
	// first — see RemoveIfStale.
	resumable := !s.draining.Load() && s.reserveRetain(cc.RegIP)
	if resumable {
		epoch := cc.Epoch()
		retainIP := cc.RegIP
		time.AfterFunc(resumeGraceWindow, func() {
			if s.rmap.RemoveIfStale(cc, epoch) {
				s.releaseRetain(retainIP)
			}
		})
	} else {
		s.rmap.Remove(cc)
	}
	// MarkDead is first-reason-wins, so this reports what actually killed the
	// conn (read error, keepalive failure, protocol violation), not the
	// catch-all above.
	s.log.Info("disconnect", "client_id", id, "reason", cc.DeadReason().Error(), "resumable", resumable)
}

func (s *server) reserveUnreg(ip string) bool {
	s.unregPerIPMu.Lock()
	defer s.unregPerIPMu.Unlock()
	if s.cfg.maxUnregWSTotal > 0 && s.unregTotal >= int64(s.cfg.maxUnregWSTotal) {
		return false
	}
	if s.cfg.maxUnregWSPerIP > 0 && s.unregPerIP[ip] >= s.cfg.maxUnregWSPerIP {
		return false
	}
	s.unregPerIP[ip]++
	s.unregTotal++
	return true
}

func (s *server) releaseUnreg(ip string) {
	s.unregPerIPMu.Lock()
	defer s.unregPerIPMu.Unlock()
	if n, ok := s.unregPerIP[ip]; ok && n > 0 {
		s.unregPerIP[ip] = n - 1
		if s.unregPerIP[ip] == 0 {
			delete(s.unregPerIP, ip)
		}
		s.unregTotal--
	}
}

// reserveRetain / releaseRetain bound the pool of dead-but-resumable conns,
// mirroring the unreg budget. Each held conn is paired with exactly one
// release: either the resume path (revive) or the reaper (eviction), never
// both, guarded by the conn's epoch.
func (s *server) reserveRetain(ip string) bool {
	s.retainMu.Lock()
	defer s.retainMu.Unlock()
	if s.retainTotal >= maxRetainTotal {
		return false
	}
	if s.retainPerIP[ip] >= maxRetainPerIP {
		return false
	}
	s.retainPerIP[ip]++
	s.retainTotal++
	return true
}

func (s *server) releaseRetain(ip string) {
	s.retainMu.Lock()
	defer s.retainMu.Unlock()
	if n, ok := s.retainPerIP[ip]; ok && n > 0 {
		s.retainPerIP[ip] = n - 1
		if s.retainPerIP[ip] == 0 {
			delete(s.retainPerIP, ip)
		}
		s.retainTotal--
	}
}

func (s *server) keepaliveLoop(ctx context.Context, cc *router.ClientConn) {
	t := time.NewTicker(keepalivePeriod)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			pctx, cancel := context.WithTimeout(ctx, keepaliveTimeout)
			err := cc.WS().Ping(pctx)
			cancel()
			if err != nil {
				cc.WS().Close(protocol.CloseKeepaliveTimeout, "keepalive timeout")
				cc.MarkDead(err)
				return
			}
		}
	}
}

// reqStream is the in-flight response state. The reader goroutine is the sole
// owner; idleTimer's callback only touches the client conn, not these fields.
type reqStream struct {
	reqID      string
	exit       int
	durationMs int64
	timedOut   bool
	truncated  bool
	sizeBytes  int64
	bytesGot   int64
	chunks     int64
	output     []byte
	idleTimer  *time.Timer
}

func (s *server) readerLoop(ctx context.Context, cc *router.ClientConn) {
	var st *reqStream
	for {
		typ, data, err := cc.WS().Read(ctx)
		if err != nil {
			cc.MarkDead(err)
			return
		}
		var ok, solicited bool
		switch typ {
		case websocket.MessageText:
			ok, solicited = s.dispatchText(cc, data, &st)
		case websocket.MessageBinary:
			ok, solicited = s.dispatchBinary(cc, data, st)
		}
		if !ok {
			return
		}
		// Solicited frames — the response stream for the request the relay is
		// itself waiting on — are already budgeted by their own contract
		// (declared size ≤ maxResponseBytes, overrun/mismatch checks, idle
		// timer, chunk budget). Only unsolicited traffic pays the flood bucket.
		if !solicited && !cc.Flood().Allow(len(data)) {
			cc.WS().Close(protocol.CloseFlooding, "flooding")
			cc.MarkDead(errors.New("flooding"))
			return
		}
	}
}

// dispatchText returns (keep-going, solicited): solicited is true only for
// frames that belong to the response the relay is currently waiting on, which
// exempts them from the flood bucket in readerLoop.
func (s *server) dispatchText(cc *router.ClientConn, data []byte, st **reqStream) (bool, bool) {
	var env protocol.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		cc.WS().Close(protocol.CloseProtocolError, "protocol error")
		cc.MarkDead(err)
		return false, false
	}
	switch env.Type {
	case protocol.TypeRegister:
		cc.WS().Close(protocol.CloseProtocolError, "duplicate register")
		cc.MarkDead(errors.New("duplicate register"))
		return false, false
	case protocol.TypeEndHeader:
		return s.handleEndHeader(cc, data, st)
	case protocol.TypeEndFooter:
		return s.handleEndFooter(cc, data, st)
	case protocol.TypeError:
		var ef protocol.ErrorFrame
		_ = json.Unmarshal(data, &ef)
		active := cc.ActiveReqID()
		if ef.ReqID != "" && active != "" && ef.ReqID == active {
			cc.Deliver(router.Response{ReqID: active, ProtoError: true, Err: errors.New("client error: " + ef.Code)})
			return true, true
		}
		s.log.Warn("client error frame", "code", ef.Code, "msg", ef.Message)
		return true, false
	default:
		cc.WS().Close(protocol.CloseProtocolError, "protocol error")
		cc.MarkDead(errors.New("wrong-direction or unknown frame: " + env.Type))
		return false, false
	}
}

func (s *server) handleEndHeader(cc *router.ClientConn, data []byte, st **reqStream) (bool, bool) {
	var eh protocol.EndHeader
	if err := json.Unmarshal(data, &eh); err != nil {
		cc.WS().Close(protocol.CloseProtocolError, "protocol error")
		cc.MarkDead(err)
		return false, false
	}
	active := cc.ActiveReqID()
	if active == "" || eh.ReqID != active {
		s.log.Warn("end_header for inactive req", "got", eh.ReqID, "active", active)
		return true, false
	}
	if !protocol.ValidateHeaderField(eh.ReqID, 64) {
		cc.Deliver(router.Response{ReqID: active, ProtoError: true, Err: errors.New("bad req_id")})
		cc.WS().Close(protocol.CloseProtocolError, "protocol error")
		cc.MarkDead(errors.New("header inj"))
		return false, false
	}
	if s.cfg.maxResponseBytes > 0 && eh.SizeBytes > s.cfg.maxResponseBytes {
		cc.Deliver(router.Response{ReqID: active, ProtoError: true, Err: errors.New("size_bytes > cap")})
		cc.WS().Close(protocol.CloseProtocolError, "protocol error")
		cc.MarkDead(errors.New("size_bytes > cap"))
		return false, false
	}
	cur := &reqStream{
		reqID:      eh.ReqID,
		exit:       eh.Exit,
		durationMs: eh.DurationMs,
		timedOut:   eh.TimedOut,
		truncated:  eh.Truncated,
		sizeBytes:  eh.SizeBytes,
		output:     make([]byte, 0, eh.SizeBytes),
	}
	if eh.SizeBytes > 0 {
		reqID := cur.reqID
		cur.idleTimer = time.AfterFunc(binaryIdleTimeout, func() {
			cc.Deliver(router.Response{ReqID: reqID, ProtoError: true, Err: errors.New("binary idle")})
			cc.WS().Close(protocol.CloseProtocolError, "protocol error")
			cc.MarkDead(errors.New("binary idle"))
		})
	}
	*st = cur
	return true, true
}

func (s *server) dispatchBinary(cc *router.ClientConn, data []byte, st *reqStream) (bool, bool) {
	if st == nil {
		s.log.Warn("binary frame before end_header — discarding")
		return true, false
	}
	// Chunk budget: the real client emits 256 KiB chunks, so a conforming
	// response never needs more than size/64Ki (4× slack) + a few short tail
	// frames. Enforced so the flood exemption can't be gamed with tiny frames.
	st.chunks++
	if st.chunks > st.sizeBytes/(64<<10)+8 {
		cc.Deliver(router.Response{ReqID: st.reqID, ProtoError: true, Err: errors.New("chunk overrun")})
		cc.WS().Close(protocol.CloseFlooding, "flooding")
		cc.MarkDead(errors.New("chunk overrun"))
		return false, false
	}
	st.bytesGot += int64(len(data))
	if st.bytesGot > st.sizeBytes {
		cc.Deliver(router.Response{ReqID: st.reqID, ProtoError: true, Err: errors.New("binary overrun")})
		cc.WS().Close(protocol.CloseProtocolError, "protocol error")
		cc.MarkDead(errors.New("binary overrun"))
		return false, false
	}
	st.output = append(st.output, data...)
	if st.idleTimer != nil {
		st.idleTimer.Reset(binaryIdleTimeout)
	}
	return true, true
}

func (s *server) handleEndFooter(cc *router.ClientConn, data []byte, st **reqStream) (bool, bool) {
	var ef protocol.EndFooter
	if err := json.Unmarshal(data, &ef); err != nil {
		cc.WS().Close(protocol.CloseProtocolError, "protocol error")
		cc.MarkDead(err)
		return false, false
	}
	cur := *st
	active := cc.ActiveReqID()
	if cur == nil || active == "" || ef.ReqID != active || ef.ReqID != cur.reqID {
		s.log.Warn("end_footer for inactive req", "got", ef.ReqID, "active", active)
		return true, false
	}
	if cur.idleTimer != nil {
		cur.idleTimer.Stop()
	}
	if cur.bytesGot != cur.sizeBytes {
		cc.Deliver(router.Response{ReqID: cur.reqID, ProtoError: true, Err: errors.New("size mismatch")})
		cc.WS().Close(protocol.CloseProtocolError, "protocol error")
		cc.MarkDead(errors.New("size mismatch"))
		return false, false
	}
	cc.Deliver(router.Response{
		ReqID:      cur.reqID,
		Exit:       cur.exit,
		DurationMs: cur.durationMs,
		TimedOut:   cur.timedOut,
		Truncated:  cur.truncated,
		Output:     cur.output,
	})
	*st = nil
	return true, true
}

// handleClientPost handles POST / with an X-Susan-Permit header
// (<machine-id>:<call-secret>). Per-IP miss and global unauth budgets are only
// debited on a real miss (lookup-fail or token-fail), not on authenticated
// traffic. A missing or malformed permit is a 401 (bad credential); a
// well-formed permit naming an unknown machine is a 404.
func (s *server) handleClientPost(w http.ResponseWriter, r *http.Request) {
	rid, _ := ids.NewRequestID()
	w.Header().Set("X-Susan-Request-Id", rid)
	ip := s.clientIP(r)

	id, tok, ok := strings.Cut(r.Header.Get("X-Susan-Permit"), ":")
	if !ok || id == "" || tok == "" {
		s.miss(w, rid, "", "", ip, http.StatusUnauthorized)
		return
	}
	if s.draining.Load() {
		writeEmpty(w, http.StatusServiceUnavailable, "")
		s.logReq(rid, "", "", ip, http.StatusServiceUnavailable, 0, 0, 0)
		return
	}

	cc := s.rmap.Lookup(id)
	if cc == nil {
		s.miss(w, rid, "", "", ip, http.StatusNotFound)
		return
	}
	if !auth.Equal(cc.Token, tok) {
		s.miss(w, rid, cc.ID, cc.ClientVersion, ip, http.StatusUnauthorized)
		return
	}

	if ok, wait := s.rateLimiter.Allow(id); !ok {
		w.Header().Set("X-Susan-Retry-After-Ms", strconv.FormatInt(wait.Milliseconds(), 10))
		w.Header().Set("Retry-After", strconv.Itoa(int(wait.Seconds()+0.999)))
		writeEmpty(w, http.StatusTooManyRequests, "")
		s.logReq(rid, cc.ID, cc.ClientVersion, ip, http.StatusTooManyRequests, 0, 0, 0)
		return
	}

	ceiling := s.cfg.maxTimeoutSecs
	timeoutSecs := ceiling
	if raw, has := r.Header["X-Susan-Timeout"]; has {
		v := ""
		if len(raw) > 0 {
			v = raw[0]
		}
		if v == "" {
			writeEmpty(w, http.StatusBadRequest, "")
			s.logReq(rid, cc.ID, cc.ClientVersion, ip, http.StatusBadRequest, 0, 0, 0)
			return
		}
		n, perr := strconv.ParseInt(v, 10, 64)
		if perr != nil || n <= 0 || n > 1<<30 {
			writeEmpty(w, http.StatusBadRequest, "")
			s.logReq(rid, cc.ID, cc.ClientVersion, ip, http.StatusBadRequest, 0, 0, 0)
			return
		}
		if int(n) < timeoutSecs {
			timeoutSecs = int(n)
		}
	}
	if timeoutSecs < 1 {
		timeoutSecs = 1
	}
	if timeoutSecs > ceiling {
		timeoutSecs = ceiling
	}
	if timeoutSecs > protocol.HardMaxTimeoutSecs {
		timeoutSecs = protocol.HardMaxTimeoutSecs
	}
	timeoutMs := int64(timeoutSecs) * 1000

	deadline := s.clk.Now().Add(time.Duration(timeoutSecs+5) * time.Second)
	respCh, rerr := cc.Reserve(rid, deadline)
	if errors.Is(rerr, router.ErrConnBusy) {
		_, activeDL, _ := cc.ActiveReq()
		wait := time.Until(activeDL)
		if wait < 0 {
			wait = 0
		}
		w.Header().Set("X-Susan-Busy", "true")
		w.Header().Set("X-Susan-Retry-After-Ms", strconv.FormatInt(wait.Milliseconds(), 10))
		w.Header().Set("Retry-After", strconv.Itoa(int(wait.Seconds()+0.999)))
		writeEmpty(w, http.StatusConflict, "")
		s.logReq(rid, cc.ID, cc.ClientVersion, ip, http.StatusConflict, 0, 0, 0)
		return
	}
	if rerr != nil {
		// The conn is dead but still mapped: its client dropped and is within
		// its resume window. Tell the caller to retry rather than 404 — the same
		// permit should work again once the client reconnects.
		w.Header().Set("X-Susan-Retry-After-Ms", "1000")
		w.Header().Set("Retry-After", "1")
		writeEmpty(w, http.StatusServiceUnavailable, protocol.ReasonReconnecting)
		s.logReq(rid, cc.ID, cc.ClientVersion, ip, http.StatusServiceUnavailable, 0, 0, 0)
		return
	}
	defer cc.Release(rid)

	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.maxCmdBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeEmpty(w, http.StatusBadRequest, "")
		s.logReq(rid, cc.ID, cc.ClientVersion, ip, http.StatusBadRequest, 0, 0, 0)
		return
	}

	req := protocol.Request{
		Type:      protocol.TypeRequest,
		ReqID:     rid,
		Body:      string(body),
		TimeoutMs: timeoutMs,
	}
	wctx, wcancel := context.WithTimeout(cc.Ctx(), wsWriteTimeout)
	err = wsjson.Write(wctx, cc.WS(), req)
	wcancel()
	if err != nil {
		writeBadGateway(w, protocol.ReasonWSClientDisconnected)
		s.logReq(rid, cc.ID, cc.ClientVersion, ip, http.StatusBadGateway, 0, -1, 0)
		return
	}

	s.requestsWindow.Allow()

	timer := time.NewTimer(time.Duration(timeoutSecs+5) * time.Second)
	defer timer.Stop()

	select {
	case resp := <-respCh:
		if resp.WSClosed {
			writeBadGateway(w, protocol.ReasonWSClientDisconnected)
			s.logReq(rid, cc.ID, cc.ClientVersion, ip, http.StatusBadGateway, 0, -1, 0)
			return
		}
		if resp.ProtoError {
			writeBadGateway(w, protocol.ReasonProtocolError)
			s.logReq(rid, cc.ID, cc.ClientVersion, ip, http.StatusBadGateway, 0, -1, 0)
			return
		}
		status, exit := writeCommandResult(w, resp)
		s.logReq(rid, cc.ID, cc.ClientVersion, ip, status, resp.DurationMs, exit, len(resp.Output))
	case <-timer.C:
		w.Header().Set("X-Susan-Exit", "-1")
		w.Header().Set("X-Susan-Timed-Out", "true")
		w.Header().Set("X-Susan-Reason", protocol.ReasonDeadlineExceeded)
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusGatewayTimeout)
		cc.WS().Close(protocol.CloseProtocolError, "deadline exceeded")
		cc.MarkDead(errors.New("relay deadline exceeded"))
		s.logReq(rid, cc.ID, cc.ClientVersion, ip, http.StatusGatewayTimeout, 0, -1, 0)
	}
}

// miss serves a 401/404 — but only after debiting the per-IP miss budget and
// the global unauth budget. Over per-IP: silent 404. Over global: 503. Logs
// the status it actually wrote (may differ from `status` if a budget tripped).
func (s *server) miss(w http.ResponseWriter, rid, cid, cversion, ip string, status int) {
	written := status
	if ok, _ := s.missLimiter.Allow("p:" + ip); !ok {
		written = http.StatusNotFound
	} else if !s.unauthGlobal.Allow() {
		written = http.StatusServiceUnavailable
	}
	writeEmpty(w, written, "")
	s.logReq(rid, cid, cversion, ip, written, 0, 0, 0)
}

func writeEmpty(w http.ResponseWriter, status int, reason string) {
	if reason != "" {
		w.Header().Set("X-Susan-Reason", reason)
	}
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(status)
}

// writeBadGateway reports a request that failed before producing a command
// result; reason distinguishes a client disconnect from a protocol violation.
func writeBadGateway(w http.ResponseWriter, reason string) {
	w.Header().Set("X-Susan-Exit", "-1")
	w.Header().Set("X-Susan-Reason", reason)
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusBadGateway)
}

// writeCommandResult maps a completed command onto the HTTP contract: 200 for
// exit 0, 504 for a command timeout (normalized to exit -1), 502 otherwise.
// It returns the status and exit code it wrote, for the request log.
func writeCommandResult(w http.ResponseWriter, resp router.Response) (status, exit int) {
	status, exit = http.StatusOK, resp.Exit
	reason := ""
	switch {
	case resp.TimedOut:
		status, exit, reason = http.StatusGatewayTimeout, -1, protocol.ReasonCommandTimeout
	case resp.Exit != 0:
		status, reason = http.StatusBadGateway, protocol.ReasonCommandFailed
	}
	w.Header().Set("X-Susan-Exit", strconv.Itoa(exit))
	w.Header().Set("X-Susan-Duration-Ms", strconv.FormatInt(resp.DurationMs, 10))
	w.Header().Set("X-Susan-Timed-Out", strconv.FormatBool(resp.TimedOut))
	w.Header().Set("X-Susan-Truncated", strconv.FormatBool(resp.Truncated))
	if reason != "" {
		w.Header().Set("X-Susan-Reason", reason)
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(resp.Output)))
	w.WriteHeader(status)
	_, _ = w.Write(resp.Output)
	return status, exit
}

func (s *server) logReq(rid, cid, cversion, ip string, status int, durMs int64, exit, bytes int) {
	s.log.Info("request",
		"request_id", rid,
		"client_id", cid,
		"client_version", cversion,
		"src_ip", ip,
		"route", "POST /",
		"status", status,
		"duration_ms", durMs,
		"exit_code", exit,
		"bytes", bytes,
	)
}

func (s *server) drain() {
	count, _, conns := s.rmap.Snapshot()
	s.log.Info("draining", "connected", count)
	var wg sync.WaitGroup
	for _, cc := range conns {
		wg.Add(1)
		go func(c *router.ClientConn) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(c.Ctx(), 2*time.Second)
			defer cancel()
			_ = wsjson.Write(ctx, c.WS(), protocol.Bye{Type: protocol.TypeBye, Reason: "relay draining"})
		}(cc)
	}
	wg.Wait()
	deadline := time.Now().Add(time.Duration(min(s.cfg.maxTimeoutSecs, 30)) * time.Second)
	for time.Now().Before(deadline) {
		_, inFlight, _ := s.rmap.Snapshot()
		if inFlight == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	_, _, conns2 := s.rmap.Snapshot()
	for _, cc := range conns2 {
		cc.WS().Close(protocol.CloseRelayDraining, "relay draining")
		cc.MarkDead(errors.New("drain close"))
	}
}
