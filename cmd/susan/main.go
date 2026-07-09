// Command susan is the lazysusan client binary. It opens a persistent
// WebSocket to a relay, registers an in-memory token, and runs each request's
// command body in $SHELL -c, returning the captured output to the relay.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/belugashark/lazysusan/internal/connect"
	pexec "github.com/belugashark/lazysusan/internal/exec"
	"github.com/belugashark/lazysusan/internal/ids"
	"github.com/belugashark/lazysusan/internal/protocol"
)

var (
	version  = "dev"
	buildSha = "unknown"
	// defaultRelay is where a bare `susan` (no connect string, no --relay/$SUSAN_RELAY)
	// dials — the hosted relay, so the tool works out of the box like sshx's
	// https://sshx.io default. It must stay the wss:// twin of the relay's
	// SUSAN_PUBLIC_URL; when a custom domain is attached, update both together.
	defaultRelay = "wss://lazysusan.sh"
)

const (
	binaryEmitChunk    = 256 << 10
	wsReadLimitBytes   = 1 << 20
	maxCmdBytes        = 64 * 1024
	auditPreviewMaxLen = 80
	// maxCaptureBytes caps captured command output; it matches the relay's
	// default SUSAN_MAX_RESPONSE_BYTES so a full capture is never rejected at
	// end_header.
	maxCaptureBytes = 10 * 1024 * 1024
	// wsWriteTimeout bounds a single write to the relay so a relay that stops
	// reading cannot pin a response write (and thus every future request);
	// generous since the largest write is one binaryEmitChunk.
	wsWriteTimeout = 10 * time.Second

	// Reconnect tuning. After a transport drop the client re-dials with
	// exponential backoff, resuming its id so the permit survives; it gives up
	// after maxReconnectAttempts so a truly dead relay doesn't hang the caller.
	dialTimeout          = 30 * time.Second
	reconnectBaseBackoff = 500 * time.Millisecond
	reconnectMaxBackoff  = 30 * time.Second
	maxReconnectAttempts = 8
)

func main() {
	if err := mainErr(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func mainErr() error {
	fs := flag.NewFlagSet("susan", flag.ContinueOnError)
	relay := fs.String("relay", "", "relay URL (default: built-in, override $SUSAN_RELAY)")
	timeout := fs.Duration("timeout", 0, "command timeout ceiling (default 60s)")
	relayKey := fs.String("relay-key", "", "relay key for a gated relay (override $SUSAN_RELAY_KEY)")
	jsonOut := fs.Bool("json", false, "print the permit and endpoint as one JSON line")
	help := fs.Bool("help", false, "print help")
	helpH := fs.Bool("h", false, "print help (alias)")
	ver := fs.Bool("version", false, "print version")
	verV := fs.Bool("v", false, "print version (alias)")
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	if *help || *helpH {
		printHelp()
		return nil
	}
	if *ver || *verV {
		fmt.Printf("susan %s %s %s\n", version, buildSha, time.Now().UTC().Format("2006-01-02"))
		return nil
	}

	resolvedRelay := *relay
	if resolvedRelay == "" {
		resolvedRelay = os.Getenv("SUSAN_RELAY")
	}
	if resolvedRelay == "" {
		resolvedRelay = defaultRelay
	}
	ceiling := *timeout
	if ceiling == 0 {
		if v := os.Getenv("SUSAN_TIMEOUT"); v != "" {
			if d, err := time.ParseDuration(v); err == nil && d > 0 {
				ceiling = d
			}
		}
	}
	if ceiling == 0 {
		ceiling = 60 * time.Second
	}
	resolvedRelayKey := *relayKey
	if resolvedRelayKey == "" {
		resolvedRelayKey = os.Getenv("SUSAN_RELAY_KEY")
	}
	if fs.NArg() > 1 {
		return errors.New("susan: unexpected extra arguments — usage: susan [flags] [connect-string]")
	}
	// A connect string is the complete relay spec: it sets both the relay URL
	// and the relay key, so a keyless one connects keyless even when
	// --relay-key/$SUSAN_RELAY_KEY are set.
	if cs := fs.Arg(0); cs != "" {
		conn, err := connect.ParseConnect(cs)
		if err != nil {
			return fmt.Errorf("susan: %w", err)
		}
		resolvedRelay = conn.RelayURL
		resolvedRelayKey = conn.RelayKey
	}

	tok, err := ids.NewToken()
	if err != nil {
		return fmt.Errorf("susan: token generation failed: %w", err)
	}
	wsURL, err := normalizeWS(resolvedRelay)
	if err != nil {
		return fmt.Errorf("susan: bad relay URL: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cwd, err := os.Getwd()
	if err != nil {
		cwd, _ = os.UserHomeDir()
	}
	shell := os.Getenv("SHELL")
	if shell == "" {
		shell = "/bin/sh"
	}

	cl := &client{
		ctx:     ctx,
		shell:   shell,
		cwd:     cwd,
		env:     os.Environ(),
		ceiling: ceiling,
	}
	defer func() {
		if ws := cl.getWS(); ws != nil {
			ws.Close(websocket.StatusNormalClosure, "bye")
		}
	}()

	// First connect is single-shot so a bad relay or key fails fast instead of
	// after a backoff loop. Resume only makes sense once we hold an id, so the
	// reconnect loop is armed only after this succeeds.
	welcome, err := cl.connect(wsURL, tok, resolvedRelayKey, "")
	if err != nil {
		return fmt.Errorf("susan: %w", err)
	}
	currentID := welcome.ID
	printPermit(welcome, tok, *jsonOut)

	go cl.statusLineLoop()

	for {
		derr := cl.runLoop()
		if ctx.Err() != nil {
			return nil // interrupted — clean shutdown
		}
		if !derr.reconnect {
			return fmt.Errorf("susan: %s", derr.Error())
		}
		// Transport dropped. Kill the now-orphaned subprocess and close the dead
		// conn, then try to resume the same id within the relay's grace window
		// so the caller's permit keeps working.
		cl.killActive()
		if old := cl.getWS(); old != nil {
			old.Close(websocket.StatusNormalClosure, "reconnecting")
		}
		w, ok := cl.reconnect(wsURL, tok, resolvedRelayKey, currentID)
		if ctx.Err() != nil {
			return nil
		}
		if !ok {
			return fmt.Errorf("susan: giving up after %d reconnect attempts", maxReconnectAttempts)
		}
		if w.ID != currentID {
			// The grace window lapsed (or the relay restarted): we got a fresh
			// id, so the old permit is dead. Surface the new one.
			currentID = w.ID
			fmt.Fprintln(os.Stderr, "\n⚠ permit changed — the previous permit is no longer valid")
			printPermit(w, tok, *jsonOut)
		} else {
			fmt.Fprintln(os.Stderr, "\n✓ reconnected — permit unchanged")
		}
	}
}

// connect dials the relay, registers (optionally asking to resume resumeID),
// and on success stores the live conn on cl and returns the welcome. All steps
// are bounded by dialTimeout so a stalled relay cannot wedge the reconnect loop.
func (cl *client) connect(wsURL, token, relayKey, resumeID string) (protocol.Welcome, error) {
	dialCtx, cancel := context.WithTimeout(cl.ctx, dialTimeout)
	defer cancel()
	c, _, err := websocket.Dial(dialCtx, wsURL, nil)
	if err != nil {
		return protocol.Welcome{}, fmt.Errorf("cannot reach relay: %w", err)
	}
	c.SetReadLimit(wsReadLimitBytes)

	reg := protocol.Register{Type: protocol.TypeRegister, Token: token, ClientVersion: version, RelayKey: relayKey, ResumeID: resumeID}
	if err := wsjson.Write(dialCtx, c, reg); err != nil {
		c.Close(websocket.StatusInternalError, "register failed")
		return protocol.Welcome{}, fmt.Errorf("register failed: %w", err)
	}
	// The relay replies with a welcome on success, or an error frame (then close)
	// when it rejects the registration — surface that reason rather than a close code.
	_, wdata, err := c.Read(dialCtx)
	if err != nil {
		c.Close(websocket.StatusInternalError, "welcome failed")
		return protocol.Welcome{}, fmt.Errorf("welcome failed: %w", err)
	}
	var env protocol.Envelope
	if json.Unmarshal(wdata, &env) == nil && env.Type == protocol.TypeError {
		var ef protocol.ErrorFrame
		_ = json.Unmarshal(wdata, &ef)
		msg := ef.Code
		if ef.Message != "" {
			msg += ": " + ef.Message
		}
		c.Close(websocket.StatusNormalClosure, "rejected")
		return protocol.Welcome{}, fmt.Errorf("relay rejected registration: %s", msg)
	}
	var welcome protocol.Welcome
	if json.Unmarshal(wdata, &welcome) != nil || welcome.Type != protocol.TypeWelcome || welcome.ID == "" {
		c.Close(protocol.CloseProtocolError, "malformed welcome")
		return protocol.Welcome{}, errors.New("malformed welcome")
	}
	cl.setWS(c)
	return welcome, nil
}

// reconnect retries connect with exponential backoff, resuming resumeID. It
// returns the welcome and true on success, or false once attempts are exhausted
// or ctx is cancelled (the caller distinguishes via ctx.Err()).
func (cl *client) reconnect(wsURL, token, relayKey, resumeID string) (protocol.Welcome, bool) {
	backoff := reconnectBaseBackoff
	for attempt := 1; attempt <= maxReconnectAttempts; attempt++ {
		if !sleepCtx(cl.ctx, backoff) {
			return protocol.Welcome{}, false
		}
		welcome, err := cl.connect(wsURL, token, relayKey, resumeID)
		if err == nil {
			return welcome, true
		}
		fmt.Fprintf(os.Stderr, "… reconnect %d/%d failed: %v\n", attempt, maxReconnectAttempts, err)
		backoff *= 2
		if backoff > reconnectMaxBackoff {
			backoff = reconnectMaxBackoff
		}
	}
	return protocol.Welcome{}, false
}

// sleepCtx waits d or until ctx is cancelled; it returns false if cancelled.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func printPermit(w protocol.Welcome, token string, jsonOut bool) {
	permit := w.ID + ":" + token
	curl := fmt.Sprintf("curl %s -H 'X-Susan-Permit: %s' -d 'uptime'", w.PublicURL, permit)
	if jsonOut {
		doc := struct {
			Relay  string `json:"relay"`
			Permit string `json:"permit"`
			Curl   string `json:"curl"`
		}{
			Relay:  w.PublicURL,
			Permit: permit,
			Curl:   curl,
		}
		b, _ := json.Marshal(doc)
		fmt.Println(string(b))
		return
	}
	fmt.Printf("✓ connected\n\n  relay:   %s\n  permit:  %s\n\n  run:     %s\n", w.PublicURL, permit, curl)
}

func normalizeWS(s string) (string, error) {
	u, err := url.Parse(s)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/ws"
	}
	return u.String(), nil
}

func printHelp() {
	fmt.Println(`susan — open a tunnel for remote shell commands via curl

usage: susan [flags] [connect-string]

connect-string:
  susan://[relay-key@]host          join a relay (TLS); relay key only for gated relays
  susan+insecure://[relay-key@]host same, plaintext (local dev)

on connect, susan prints a permit — <machine-id>:<call-secret> — the one credential
a caller sends as the X-Susan-Permit header on POST <relay-url>.

flags:
  --relay <url>           relay URL (default: built-in, override $SUSAN_RELAY)
  --timeout <dur>         command timeout ceiling (default 60s)
  --relay-key <key>       relay key for a gated relay (override $SUSAN_RELAY_KEY)
  --json                  print the permit and endpoint as one JSON line`)
}

type client struct {
	// ws is swapped on every reconnect; hold it behind an atomic pointer so an
	// in-flight request goroutine writing to the old conn races cleanly with the
	// swap (a write to a closed conn just errors) instead of a data race.
	ws      atomic.Pointer[websocket.Conn]
	ctx     context.Context
	shell   string
	cwd     string
	env     []string
	ceiling time.Duration

	mu           sync.Mutex
	busy         bool
	activeCancel context.CancelFunc
	serving      atomic.Int64
}

func (cl *client) getWS() *websocket.Conn  { return cl.ws.Load() }
func (cl *client) setWS(c *websocket.Conn) { cl.ws.Store(c) }

// runLoop reads relay frames until the connection ends. It returns a
// disconnectErr whose reconnect flag says whether re-dialing makes sense: a
// transport drop or keepalive timeout is reconnectable; a protocol-level
// rejection (the relay closing us for a protocol/flood violation, or an
// out-of-contract frame) is fatal.
func (cl *client) runLoop() *disconnectErr {
	ws := cl.getWS()
	for {
		_, data, err := ws.Read(cl.ctx)
		if err != nil {
			status := int(websocket.CloseStatus(err))
			// No close frame means the transport died (RST/EOF); the raw error
			// is the only diagnostic there is, so surface it as the reason.
			reason := err.Error()
			if cs, ok := errAsCloseError(err); ok {
				reason = cs.Reason
				if cs.Code != 0 {
					status = int(cs.Code)
				}
			}
			fmt.Fprintf(os.Stderr, "\n✕ disconnected: %d/%s\n", status, reason)
			return &disconnectErr{
				reconnect: reconnectableClose(status),
				msg:       fmt.Sprintf("disconnected: %d/%s", status, reason),
			}
		}
		var env protocol.Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			continue
		}
		switch env.Type {
		case protocol.TypeRequest:
			var req protocol.Request
			if err := json.Unmarshal(data, &req); err != nil {
				continue
			}
			go cl.handleRequest(req)
		case protocol.TypeBye:
			var b protocol.Bye
			_ = json.Unmarshal(data, &b)
			fmt.Fprintf(os.Stderr, "\n✕ disconnected: 1001/%s\n", b.Reason)
			cl.killActive()
			// A drain means this relay instance is going away; a fresh one may
			// replace it, so reconnect (the id likely won't survive, but the
			// backoff loop handles a changed permit).
			return &disconnectErr{reconnect: true, msg: "relay draining: " + b.Reason}
		case protocol.TypeError:
			var ef protocol.ErrorFrame
			_ = json.Unmarshal(data, &ef)
			fmt.Fprintf(os.Stderr, "susan: relay error: %s %s\n", ef.Code, ef.Message)
		default:
			ws.Close(protocol.CloseProtocolError, "protocol error")
			return &disconnectErr{reconnect: false, msg: "protocol error"}
		}
	}
}

// disconnectErr is why runLoop stopped; reconnect says whether to re-dial.
type disconnectErr struct {
	reconnect bool
	msg       string
}

func (e *disconnectErr) Error() string { return e.msg }

// reconnectableClose decides, from a close status, whether re-dialing is worth
// it. 1002 (protocol error) and 1008 (flooding) are the relay rejecting how we
// speak — reconnecting would just repeat the offense — so they are fatal.
// Everything else (abnormal transport closure, keepalive timeout, etc.) is a
// transient drop worth resuming.
func reconnectableClose(status int) bool {
	switch status {
	case protocol.CloseProtocolError, protocol.CloseFlooding:
		return false
	default:
		return true
	}
}

func errAsCloseError(err error) (websocket.CloseError, bool) {
	var ce websocket.CloseError
	if errors.As(err, &ce) {
		return ce, true
	}
	return websocket.CloseError{}, false
}

// killActive cancels the in-flight subprocess context (if any) so the child's
// process group receives SIGTERM via os/exec's cmd.Cancel hook.
func (cl *client) killActive() {
	cl.mu.Lock()
	cancel := cl.activeCancel
	cl.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// writeFrame and writeBinary send to the relay under a per-write deadline so a
// relay that stops reading cannot block the request-serving goroutine forever.
func (cl *client) writeFrame(v any) error {
	wctx, cancel := context.WithTimeout(cl.ctx, wsWriteTimeout)
	defer cancel()
	return wsjson.Write(wctx, cl.getWS(), v)
}

func (cl *client) writeBinary(b []byte) error {
	wctx, cancel := context.WithTimeout(cl.ctx, wsWriteTimeout)
	defer cancel()
	return cl.getWS().Write(wctx, websocket.MessageBinary, b)
}

func (cl *client) handleRequest(req protocol.Request) {
	reqCtx, cancel := context.WithCancel(cl.ctx)
	defer cancel()

	cl.mu.Lock()
	if cl.busy {
		cl.mu.Unlock()
		// defense-in-depth: relay 409s the 2nd caller so this should never fire.
		_ = cl.writeFrame(protocol.ErrorFrame{Type: protocol.TypeError, ReqID: req.ReqID, Code: protocol.ErrConcurrentRequest})
		return
	}
	cl.busy = true
	cl.activeCancel = cancel
	cl.mu.Unlock()

	defer func() {
		cl.mu.Lock()
		cl.busy = false
		cl.activeCancel = nil
		cl.mu.Unlock()
	}()

	if len(req.Body) > maxCmdBytes {
		_ = cl.writeFrame(protocol.ErrorFrame{Type: protocol.TypeError, ReqID: req.ReqID, Code: protocol.ErrCmdTooLarge})
		return
	}
	if req.TimeoutMs < 1 || req.TimeoutMs > protocol.HardMaxTimeoutSecs*1000 {
		_ = cl.writeFrame(protocol.ErrorFrame{Type: protocol.TypeError, ReqID: req.ReqID, Code: protocol.ErrBadTimeout})
		return
	}
	if cl.ceiling > 0 {
		if ceilingMs := cl.ceiling.Milliseconds(); req.TimeoutMs > ceilingMs {
			req.TimeoutMs = ceilingMs
		}
	}

	cl.serving.Add(1)

	auditStart := time.Now()
	res := pexec.Run(reqCtx, cl.shell, req.Body, cl.cwd, cl.env, time.Duration(req.TimeoutMs)*time.Millisecond, maxCaptureBytes)

	eh := protocol.EndHeader{
		Type:       protocol.TypeEndHeader,
		ReqID:      req.ReqID,
		Exit:       res.Exit,
		DurationMs: res.DurationMs,
		TimedOut:   res.TimedOut,
		Truncated:  res.Truncated,
		SizeBytes:  int64(len(res.Output)),
	}
	if err := cl.writeFrame(eh); err != nil {
		return
	}
	for off := 0; off < len(res.Output); {
		end := off + binaryEmitChunk
		if end > len(res.Output) {
			end = len(res.Output)
		}
		if err := cl.writeBinary(res.Output[off:end]); err != nil {
			return
		}
		off = end
	}
	_ = cl.writeFrame(protocol.EndFooter{Type: protocol.TypeEndFooter, ReqID: req.ReqID})

	preview := req.Body
	if len(preview) > auditPreviewMaxLen {
		preview = preview[:auditPreviewMaxLen] + "…"
	}
	preview = strings.ReplaceAll(preview, "\n", " ")
	fmt.Fprintf(os.Stderr, "[%s] · %s · %d · %dms\n",
		auditStart.Format("15:04:05"),
		preview,
		res.Exit,
		res.DurationMs,
	)
}

func (cl *client) statusLineLoop() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-cl.ctx.Done():
			return
		case <-t.C:
			cl.mu.Lock()
			busy := cl.busy
			cl.mu.Unlock()
			n := cl.serving.Load()
			label := "idle"
			if busy {
				label = "busy"
			}
			fmt.Fprintf(os.Stderr, "\r[%s] %s — %d requests served    ",
				time.Now().Format("15:04:05"),
				label,
				n,
			)
		}
	}
}
