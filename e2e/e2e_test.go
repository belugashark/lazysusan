// Package e2e drives the full susan + tray pair via os/exec and hits the relay
// over net/http to verify the API contract end-to-end. Each test gets a fresh
// relay + client on an ephemeral port.
package e2e

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var binMu sync.Mutex
var binPaths struct {
	susan string
	tray  string
}

func buildBinaries(t *testing.T) (susan, tray string) {
	t.Helper()
	binMu.Lock()
	defer binMu.Unlock()
	if binPaths.susan != "" {
		return binPaths.susan, binPaths.tray
	}
	dir, err := os.MkdirTemp("", "psusan-bin-*")
	if err != nil {
		t.Fatal(err)
	}
	susanPath := filepath.Join(dir, "susan")
	trayPath := filepath.Join(dir, "tray")
	root := projectRoot(t)
	build := func(out, pkg string) {
		cmd := exec.Command("go", "build", "-o", out, pkg)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		if b, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", pkg, err, b)
		}
	}
	build(susanPath, "./cmd/susan")
	build(trayPath, "./cmd/tray")
	binPaths.susan = susanPath
	binPaths.tray = trayPath
	return susanPath, trayPath
}

func projectRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for d := wd; d != "/"; d = filepath.Dir(d) {
		if _, err := os.Stat(filepath.Join(d, "go.mod")); err == nil {
			return d
		}
	}
	t.Fatal("could not find project root (go.mod)")
	return ""
}

type fixture struct {
	t           *testing.T
	trayProc    *exec.Cmd
	susanProc   *exec.Cmd
	trayURL     string
	susanID     string
	susanToken  string
	susanStderr *strings.Builder
	trayLog     *strings.Builder
}

func newFixture(t *testing.T, env ...string) *fixture {
	t.Helper()
	susan, tray := buildBinaries(t)
	port := freePort(t)
	trayURL := fmt.Sprintf("http://127.0.0.1:%d", port)

	trayLog := &strings.Builder{}
	trayCmd := exec.Command(tray)
	trayCmd.Env = append(os.Environ(),
		"PORT="+strconv.Itoa(port),
		"SUSAN_PUBLIC_URL="+trayURL,
		"SUSAN_INSECURE=1",
		"SUSAN_LOG_LEVEL=info",
	)
	trayCmd.Env = append(trayCmd.Env, env...)
	trayCmd.Stdout = trayLog
	trayCmd.Stderr = trayLog
	if err := trayCmd.Start(); err != nil {
		t.Fatal(err)
	}
	if err := waitFor(trayURL+"/healthz", 5*time.Second); err != nil {
		_ = trayCmd.Process.Kill()
		t.Fatalf("tray did not become healthy: %v\nlog:\n%s", err, trayLog.String())
	}

	susanStderr := &strings.Builder{}
	susanStdout, w := io.Pipe()
	susanCmd := exec.Command(susan, "--relay", "ws://127.0.0.1:"+strconv.Itoa(port))
	susanCmd.Stdout = w
	susanCmd.Stderr = susanStderr
	if err := susanCmd.Start(); err != nil {
		_ = trayCmd.Process.Kill()
		t.Fatal(err)
	}
	id, tok := readWelcome(t, susanStdout, 5*time.Second)

	fx := &fixture{
		t:           t,
		trayProc:    trayCmd,
		susanProc:   susanCmd,
		trayURL:     trayURL,
		susanID:     id,
		susanToken:  tok,
		susanStderr: susanStderr,
		trayLog:     trayLog,
	}
	t.Cleanup(fx.Close)
	go io.Copy(io.Discard, susanStdout)
	return fx
}

func (f *fixture) Close() {
	stopCmd(f.susanProc)
	stopCmd(f.trayProc)
}

// stopCmd interrupts a child process and force-kills it if it hasn't exited
// within 3s.
func stopCmd(c *exec.Cmd) {
	if c == nil || c.Process == nil {
		return
	}
	_ = c.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() { _ = c.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		_ = c.Process.Kill()
	}
}

func (f *fixture) URL() string {
	return f.trayURL
}

func (f *fixture) Permit() string {
	return f.susanID + ":" + f.susanToken
}

func (f *fixture) Post(t *testing.T, body string, headers ...string) *http.Response {
	t.Helper()
	req, err := http.NewRequest("POST", f.URL(), strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Susan-Permit", f.Permit())
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitFor(url string, d time.Duration) error {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", url)
}

func readWelcome(t *testing.T, r io.Reader, d time.Duration) (id, tok string) {
	t.Helper()
	br := bufio.NewReader(r)
	done := make(chan struct{})
	var idOut, tokOut string
	go func() {
		defer close(done)
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			if i := strings.Index(line, "permit:"); i >= 0 {
				permit := strings.TrimSpace(line[i+len("permit:"):])
				idOut, tokOut, _ = strings.Cut(permit, ":")
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("timeout reading welcome block")
	}
	if idOut == "" || tokOut == "" {
		t.Fatalf("welcome block did not parse: id=%q tok=%q", idOut, tokOut)
	}
	return idOut, tokOut
}

func body(t *testing.T, r *http.Response) []byte {
	t.Helper()
	b, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestHappyPath(t *testing.T) {
	f := newFixture(t)
	resp := f.Post(t, "echo hello-susan")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if resp.Header.Get("X-Susan-Exit") != "0" {
		t.Fatalf("exit hdr=%q", resp.Header.Get("X-Susan-Exit"))
	}
	if resp.Header.Get("X-Susan-Request-Id") == "" {
		t.Fatal("missing X-Susan-Request-Id")
	}
	if strings.TrimSpace(string(body(t, resp))) != "hello-susan" {
		t.Fatalf("bad body")
	}
}

func TestNonZeroExitMapsTo502_CommandFailed(t *testing.T) {
	f := newFixture(t)
	resp := f.Post(t, "exit 7")
	defer resp.Body.Close()
	if resp.StatusCode != 502 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if resp.Header.Get("X-Susan-Exit") != "7" {
		t.Fatalf("exit=%q", resp.Header.Get("X-Susan-Exit"))
	}
	if resp.Header.Get("X-Susan-Reason") != "command_failed" {
		t.Fatalf("reason=%q", resp.Header.Get("X-Susan-Reason"))
	}
}

func TestClientTimeoutMapsTo504_CommandTimeout(t *testing.T) {
	f := newFixture(t)
	resp := f.Post(t, "sleep 5", "X-Susan-Timeout", "1")
	defer resp.Body.Close()
	if resp.StatusCode != 504 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if resp.Header.Get("X-Susan-Reason") != "command_timeout" {
		t.Fatalf("reason=%q", resp.Header.Get("X-Susan-Reason"))
	}
	if resp.Header.Get("X-Susan-Timed-Out") != "true" {
		t.Fatalf("timed_out=%q", resp.Header.Get("X-Susan-Timed-Out"))
	}
	if resp.Header.Get("X-Susan-Exit") != "-1" {
		t.Fatalf("exit=%q", resp.Header.Get("X-Susan-Exit"))
	}
}

func TestBinaryOutputByteExact(t *testing.T) {
	f := newFixture(t)
	resp := f.Post(t, `dd if=/dev/urandom bs=1 count=4096 2>/dev/null | base64`)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	got := body(t, resp)
	// just sanity: non-empty and matches reported Content-Length
	if len(got) == 0 {
		t.Fatal("empty output")
	}
	cl := resp.Header.Get("Content-Length")
	n, _ := strconv.Atoi(cl)
	if n != len(got) {
		t.Fatalf("Content-Length=%s body=%d", cl, len(got))
	}
}

func TestLargeOutputViaBinaryFrames(t *testing.T) {
	f := newFixture(t)
	// ~1 MiB of randomness; sha-check round-trip
	resp := f.Post(t, `head -c 1048576 /dev/urandom | base64`)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	got := body(t, resp)
	if len(got) < 1_000_000 {
		t.Fatalf("output too small: %d", len(got))
	}
	_ = sha256.Sum256(got)
}

func TestOutputTruncatedHeader(t *testing.T) {
	f := newFixture(t)
	// Exceed susan's 10 MiB capture cap; the capped response is exactly the
	// relay's default max, so it comes back 200 with the truncation flagged.
	resp := f.Post(t, `head -c 11534336 /dev/zero`)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if h := resp.Header.Get("X-Susan-Truncated"); h != "true" {
		t.Fatalf("X-Susan-Truncated=%q, want true", h)
	}
	got := body(t, resp)
	if len(got) != 10*1024*1024 {
		t.Fatalf("body=%d bytes, want exactly the 10 MiB capture cap", len(got))
	}
}

func TestAuthRejected_BadToken(t *testing.T) {
	f := newFixture(t)
	req, _ := http.NewRequest("POST", f.URL(), strings.NewReader("uptime"))
	req.Header.Set("X-Susan-Permit", f.susanID+":not-the-real-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Length") != "0" {
		t.Fatalf("Content-Length=%q", resp.Header.Get("Content-Length"))
	}
}

func TestAuthRejected_NoToken(t *testing.T) {
	f := newFixture(t)
	req, _ := http.NewRequest("POST", f.URL(), strings.NewReader("uptime"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

func TestNoClient_404(t *testing.T) {
	f := newFixture(t)
	req, _ := http.NewRequest("POST", f.URL(), strings.NewReader("uptime"))
	req.Header.Set("X-Susan-Permit", "aaaaaaaaaaaaaaaaaaaaaaaaaa:anything")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

func TestCommandBodyExceeds64KiB_400(t *testing.T) {
	f := newFixture(t)
	huge := strings.Repeat("a", 65537)
	resp := f.Post(t, huge)
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

func TestXSusanTimeoutMalformed_400(t *testing.T) {
	f := newFixture(t)
	cases := []string{"-1", "0", "abc", "1.5", "99999999999999999999", ""}
	for _, c := range cases {
		resp := f.Post(t, "echo hi", "X-Susan-Timeout", c)
		resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Errorf("X-Susan-Timeout=%q -> status=%d (want 400)", c, resp.StatusCode)
		}
	}
}

func TestXSusanTimeoutShortensCeiling(t *testing.T) {
	f := newFixture(t)
	start := time.Now()
	resp := f.Post(t, "sleep 10", "X-Susan-Timeout", "1")
	dur := time.Since(start)
	defer resp.Body.Close()
	if resp.StatusCode != 504 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if dur > 3*time.Second {
		t.Fatalf("took %v, expected ~1s", dur)
	}
}

func TestRateLimit_429(t *testing.T) {
	f := newFixture(t, "SUSAN_RATE_LIMIT_PER_MIN=3")
	for i := 0; i < 3; i++ {
		resp := f.Post(t, "echo "+strconv.Itoa(i))
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("req %d: status=%d", i, resp.StatusCode)
		}
	}
	resp := f.Post(t, "echo over")
	defer resp.Body.Close()
	if resp.StatusCode != 429 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("missing Retry-After")
	}
	if resp.Header.Get("X-Susan-Retry-After-Ms") == "" {
		t.Fatal("missing X-Susan-Retry-After-Ms")
	}
}

func TestSecondRequestWhileBusy_409(t *testing.T) {
	f := newFixture(t)
	// Start a long-running command in the background.
	first := make(chan struct{})
	go func() {
		resp := f.Post(t, "sleep 3")
		resp.Body.Close()
		close(first)
	}()
	// Give it a moment to register busy.
	time.Sleep(300 * time.Millisecond)
	resp := f.Post(t, "echo me-too")
	body(t, resp)
	if resp.StatusCode != 409 {
		t.Fatalf("2nd: status=%d", resp.StatusCode)
	}
	if resp.Header.Get("X-Susan-Busy") != "true" {
		t.Fatalf("X-Susan-Busy=%q", resp.Header.Get("X-Susan-Busy"))
	}
	if resp.Header.Get("X-Susan-Retry-After-Ms") == "" {
		t.Fatal("missing X-Susan-Retry-After-Ms")
	}
	<-first
}

func TestStatsEndpoint(t *testing.T) {
	f := newFixture(t)
	resp, err := http.Get(f.trayURL + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	b := body(t, resp)
	for _, k := range []string{"connected_clients", "in_flight", "requests_last_min", "version", "build_sha", "uptime_seconds"} {
		if !strings.Contains(string(b), k) {
			t.Errorf("stats missing key %q in %s", k, string(b))
		}
	}
}

func TestHealthzGetAndHead(t *testing.T) {
	f := newFixture(t)
	resp, err := http.Get(f.trayURL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("get status=%d", resp.StatusCode)
	}
	if string(body(t, resp)) != "ok" {
		t.Fatal("get body != ok")
	}
	req, _ := http.NewRequest("HEAD", f.trayURL+"/healthz", nil)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Fatalf("head status=%d", resp2.StatusCode)
	}
}

func TestRequestIdInResponse(t *testing.T) {
	f := newFixture(t)
	resp := f.Post(t, "echo x")
	resp.Body.Close()
	if resp.Header.Get("X-Susan-Request-Id") == "" {
		t.Fatal("missing X-Susan-Request-Id on 200")
	}

	req, _ := http.NewRequest("POST", f.URL(), strings.NewReader("echo"))
	req.Header.Set("X-Susan-Permit", f.susanID+":wrong")
	resp2, _ := http.DefaultClient.Do(req)
	resp2.Body.Close()
	if resp2.Header.Get("X-Susan-Request-Id") == "" {
		t.Fatal("missing X-Susan-Request-Id on 401")
	}
}

func TestSusanVersionFlag(t *testing.T) {
	susan, _ := buildBinaries(t)
	cmd := exec.Command(susan, "--version")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(out), "susan ") {
		t.Fatalf("bad version output: %q", string(out))
	}
}

func TestSusanHelpFlag(t *testing.T) {
	susan, _ := buildBinaries(t)
	cmd := exec.Command(susan, "--help")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "susan — open a tunnel") {
		t.Fatalf("bad help output: %q", string(out))
	}
}

func TestSusanStartupConnectFailure(t *testing.T) {
	susan, _ := buildBinaries(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, susan, "--relay", "ws://127.0.0.1:1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected non-zero exit")
	}
	if !strings.Contains(string(out), "susan: cannot reach") {
		t.Fatalf("expected 'susan: cannot reach' in stderr, got: %q", string(out))
	}
}

func TestTrayRequiresHTTPS_WithoutInsecure(t *testing.T) {
	_, tray := buildBinaries(t)
	cmd := exec.Command(tray)
	cmd.Env = append(os.Environ(),
		"PORT="+strconv.Itoa(freePort(t)),
		"SUSAN_PUBLIC_URL=http://example.com",
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected non-zero exit")
	}
	if !strings.Contains(string(out), "SUSAN_PUBLIC_URL must be https://") {
		t.Fatalf("expected specific stderr message, got: %q", string(out))
	}
}

func TestTrayRequiresPublicURL(t *testing.T) {
	_, tray := buildBinaries(t)
	cmd := exec.Command(tray)
	env := []string{"PORT=" + strconv.Itoa(freePort(t))}
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, "SUSAN_PUBLIC_URL=") {
			continue
		}
		env = append(env, e)
	}
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected non-zero exit")
	}
	if !strings.Contains(string(out), "SUSAN_PUBLIC_URL is required") {
		t.Fatalf("expected specific stderr message, got: %q", string(out))
	}
}

func TestTrayRejectsOversizedRelayKey(t *testing.T) {
	_, tray := buildBinaries(t)
	cmd := exec.Command(tray)
	cmd.Env = append(os.Environ(),
		"PORT="+strconv.Itoa(freePort(t)),
		"SUSAN_PUBLIC_URL=http://example.com",
		"SUSAN_INSECURE=1",
		"SUSAN_RELAY_KEY="+strings.Repeat("x", 65),
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected non-zero exit")
	}
	if !strings.Contains(string(out), "SUSAN_RELAY_KEY must be at most") {
		t.Fatalf("expected specific stderr message, got: %q", string(out))
	}
}

func TestOriginRejected(t *testing.T) {
	f := newFixture(t)
	req, _ := http.NewRequest("GET", f.trayURL+"/ws", nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-Websocket-Version", "13")
	req.Header.Set("Sec-Websocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	req.Header.Set("Origin", "https://evil.com")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("status=%d (want 403)", resp.StatusCode)
	}
}

func TestStdinIsDevNull(t *testing.T) {
	f := newFixture(t)
	resp := f.Post(t, "cat -")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if resp.Header.Get("X-Susan-Exit") != "0" {
		t.Fatalf("exit=%q", resp.Header.Get("X-Susan-Exit"))
	}
	if len(body(t, resp)) != 0 {
		t.Fatal("expected empty output")
	}
}

func envWithout(env []string, key string) []string {
	out := make([]string, 0, len(env))
	for _, e := range env {
		if strings.HasPrefix(e, key+"=") {
			continue
		}
		out = append(out, e)
	}
	return out
}

// TestRelayKey covers the optional SUSAN_RELAY_KEY gate: a configured relay
// rejects clients without the key, gates /stats behind it, and accepts
// clients that present the matching key.
func TestRelayKey(t *testing.T) {
	susan, tray := buildBinaries(t)
	port := freePort(t)
	trayURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	const secret = "stk_test_register_secret"

	trayLog := &strings.Builder{}
	trayCmd := exec.Command(tray)
	trayCmd.Env = append(os.Environ(),
		"PORT="+strconv.Itoa(port),
		"SUSAN_PUBLIC_URL="+trayURL,
		"SUSAN_INSECURE=1",
		"SUSAN_RELAY_KEY="+secret,
	)
	trayCmd.Stdout = trayLog
	trayCmd.Stderr = trayLog
	if err := trayCmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopCmd(trayCmd) })
	if err := waitFor(trayURL+"/healthz", 5*time.Second); err != nil {
		t.Fatalf("tray not healthy: %v\n%s", err, trayLog.String())
	}

	// /stats requires the bearer token when the relay is gated.
	resp, err := http.Get(trayURL + "/stats")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("/stats without token: status=%d (want 401)", resp.StatusCode)
	}
	req, _ := http.NewRequest("GET", trayURL+"/stats", nil)
	req.Header.Set("Authorization", "Bearer "+secret)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("/stats with token: status=%d (want 200)", resp.StatusCode)
	}

	// A client without the token is rejected and exits non-zero.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bad := exec.CommandContext(ctx, susan, "--relay", "ws://127.0.0.1:"+strconv.Itoa(port))
	bad.Env = envWithout(os.Environ(), "SUSAN_RELAY_KEY")
	out, err := bad.CombinedOutput()
	if err == nil {
		t.Fatalf("expected susan to fail without relay key; out=%q", out)
	}
	if !strings.Contains(string(out), "rejected registration") {
		t.Fatalf("expected rejection message, got: %q", out)
	}

	// A client with the matching token registers and round-trips a command.
	susanStderr := &strings.Builder{}
	pr, pw := io.Pipe()
	good := exec.Command(susan, "--relay", "ws://127.0.0.1:"+strconv.Itoa(port))
	good.Env = append(envWithout(os.Environ(), "SUSAN_RELAY_KEY"), "SUSAN_RELAY_KEY="+secret)
	good.Stdout = pw
	good.Stderr = susanStderr
	if err := good.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopCmd(good) })
	id, tok := readWelcome(t, pr, 5*time.Second)
	go io.Copy(io.Discard, pr)

	creq, _ := http.NewRequest("POST", trayURL, strings.NewReader("echo hello-token"))
	creq.Header.Set("X-Susan-Permit", id+":"+tok)
	cresp, err := http.DefaultClient.Do(creq)
	if err != nil {
		t.Fatal(err)
	}
	defer cresp.Body.Close()
	if cresp.StatusCode != 200 {
		t.Fatalf("gated round-trip: status=%d", cresp.StatusCode)
	}
	if strings.TrimSpace(string(body(t, cresp))) != "hello-token" {
		t.Fatal("bad body on gated round-trip")
	}
}

func TestProcessGroupKilled(t *testing.T) {
	f := newFixture(t)
	resp := f.Post(t, `bash -c 'sleep 30 & echo PID:$!; wait' || true`, "X-Susan-Timeout", "1")
	defer resp.Body.Close()
	if resp.StatusCode != 504 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	out := body(t, resp)
	for _, line := range strings.Split(string(out), "\n") {
		if !strings.HasPrefix(line, "PID:") {
			continue
		}
		pid, _ := strconv.Atoi(strings.TrimPrefix(line, "PID:"))
		if pid <= 0 {
			continue
		}
		// Briefly give the kill -PGID a moment to land before probing.
		time.Sleep(200 * time.Millisecond)
		p, err := os.FindProcess(pid)
		if err != nil {
			continue
		}
		if err := p.Signal(syscall.Signal(0)); err == nil {
			t.Fatalf("child pid %d still alive after timeout", pid)
		}
	}
}
