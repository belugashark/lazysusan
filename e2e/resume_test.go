package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/belugashark/lazysusan/internal/protocol"
)

// registerRaw opens a WebSocket and registers, optionally asking to resume
// resumeID, returning the live conn and the welcome.
func registerRaw(t *testing.T, wsURL, token, resumeID string) (*websocket.Conn, protocol.Welcome) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c.SetReadLimit(1 << 20)
	if err := wsjson.Write(ctx, c, protocol.Register{
		Type: protocol.TypeRegister, Token: token, ClientVersion: "test", ResumeID: resumeID,
	}); err != nil {
		t.Fatalf("register write: %v", err)
	}
	_, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("welcome read: %v", err)
	}
	var w protocol.Welcome
	if err := json.Unmarshal(data, &w); err != nil || w.Type != protocol.TypeWelcome || w.ID == "" {
		t.Fatalf("bad welcome: %s", data)
	}
	return c, w
}

// serveOneRequest answers exactly one relayed request with out, mimicking the
// client's response protocol (end_header, binary, end_footer).
func serveOneRequest(c *websocket.Conn, out []byte) {
	ctx := context.Background()
	for {
		typ, data, err := c.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		var env protocol.Envelope
		if json.Unmarshal(data, &env) != nil || env.Type != protocol.TypeRequest {
			continue
		}
		var req protocol.Request
		if json.Unmarshal(data, &req) != nil {
			return
		}
		wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_ = wsjson.Write(wctx, c, protocol.EndHeader{
			Type: protocol.TypeEndHeader, ReqID: req.ReqID, Exit: 0, SizeBytes: int64(len(out)),
		})
		_ = c.Write(wctx, websocket.MessageBinary, out)
		_ = wsjson.Write(wctx, c, protocol.EndFooter{Type: protocol.TypeEndFooter, ReqID: req.ReqID})
		cancel()
		return
	}
}

// wsFromURL turns an http://host:port relay URL into its ws://host:port/ws twin.
func wsFromURL(url string) string {
	return "ws" + strings.TrimPrefix(url, "http") + "/ws"
}

// TestResumeKeepsPermit verifies the resume contract end-to-end against the real
// relay: after an abrupt drop, the permit 503s (retryable) during the grace
// window, a resume reclaims the same machine id, and the permit then serves.
func TestResumeKeepsPermit(t *testing.T) {
	url, _ := startTray(t, "")
	wsURL := wsFromURL(url)
	token := "stk_" + strings.Repeat("a", 32)

	c1, w1 := registerRaw(t, wsURL, token, "")
	permit := w1.ID + ":" + token

	// Sever the transport without a close handshake — the -1/unknown case.
	if err := c1.CloseNow(); err != nil {
		t.Fatalf("CloseNow: %v", err)
	}

	// Within the grace window the relay retains the dead conn, so the permit
	// gets a retryable 503 (reconnecting), never a terminal 404.
	got503 := false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp := postPermit(t, url, permit, "echo hi")
		resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			t.Fatal("got 404 during grace window — dead conn was not retained")
		}
		if resp.StatusCode == http.StatusServiceUnavailable &&
			resp.Header.Get("X-Susan-Reason") == protocol.ReasonReconnecting {
			got503 = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !got503 {
		t.Fatal("expected a 503 reconnecting during the grace window")
	}

	// Resume reclaims the same id (so the same permit stays valid).
	c2, w2 := registerRaw(t, wsURL, token, w1.ID)
	if w2.ID != w1.ID {
		t.Fatalf("resume changed the machine id: %s != %s", w2.ID, w1.ID)
	}

	// The resumed conn serves the original permit.
	go serveOneRequest(c2, []byte("hi\n"))
	resp := postPermit(t, url, permit, "echo hi")
	b := body(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("post after resume: status %d body %q", resp.StatusCode, b)
	}
	if string(b) != "hi\n" {
		t.Fatalf("post after resume body = %q, want %q", b, "hi\n")
	}
	_ = c2.Close(websocket.StatusNormalClosure, "done")
}

// startTrayOnPort launches the relay bound to a fixed port and waits until it
// is healthy, returning the process so the caller can restart it. Unlike
// startTray it does not register a cleanup, since the test controls its life.
func startTrayOnPort(t *testing.T, port int) *exec.Cmd {
	t.Helper()
	_, tray := buildBinaries(t)
	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	log := &strings.Builder{}
	cmd := exec.Command(tray)
	cmd.Env = append(envWithout(os.Environ(), "SUSAN_RELAY_KEY"),
		"PORT="+strconv.Itoa(port),
		"SUSAN_PUBLIC_URL="+url,
		"SUSAN_INSECURE=1",
	)
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if err := waitFor(url+"/healthz", 5*time.Second); err != nil {
		t.Fatalf("tray did not become healthy: %v\nlog:\n%s", err, log.String())
	}
	return cmd
}

// TestClientReconnectsAfterRelayRestart drives the real susan binary: when the
// relay restarts (the Railway-redeploy case), the client re-dials, prints a new
// permit because the fresh relay can't honor the resume, and keeps serving.
func TestClientReconnectsAfterRelayRestart(t *testing.T) {
	susan, _ := buildBinaries(t)
	port := freePort(t)
	url := fmt.Sprintf("http://127.0.0.1:%d", port)

	p1 := startTrayOnPort(t, port)
	relayUp := true
	t.Cleanup(func() {
		if relayUp {
			stopCmd(p1)
		}
	})

	stdout, w := io.Pipe()
	stderr := &strings.Builder{}
	susanCmd := exec.Command(susan, "--relay", "ws://127.0.0.1:"+strconv.Itoa(port))
	susanCmd.Env = envWithout(os.Environ(), "SUSAN_RELAY_KEY")
	susanCmd.Stdout = w
	susanCmd.Stderr = stderr
	if err := susanCmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopCmd(susanCmd) })

	// A persistent scanner so the second permit is read from where the first
	// left off rather than losing buffered bytes.
	br := bufio.NewReader(stdout)
	nextPermit := func(d time.Duration) (id, tok string) {
		type res struct{ id, tok string }
		ch := make(chan res, 1)
		go func() {
			for {
				line, err := br.ReadString('\n')
				if err != nil {
					return
				}
				if i := strings.Index(line, "permit:"); i >= 0 {
					id, tok, _ := strings.Cut(strings.TrimSpace(line[i+len("permit:"):]), ":")
					ch <- res{id, tok}
					return
				}
			}
		}()
		select {
		case r := <-ch:
			return r.id, r.tok
		case <-time.After(d):
			t.Fatalf("timed out waiting for a permit; client stderr:\n%s", stderr.String())
			return "", ""
		}
	}

	id1, tok := nextPermit(10 * time.Second)
	if resp := postPermit(t, url, id1+":"+tok, "echo one"); string(body(t, resp)) != "one\n" {
		t.Fatalf("first permit did not serve: status %d", resp.StatusCode)
	}

	// Restart the relay on the same port. The client's socket drops; its
	// reconnect loop should re-register and, against the memory-less new relay,
	// receive a fresh id and print the new permit.
	stopCmd(p1)
	relayUp = false
	p2 := startTrayOnPort(t, port)
	t.Cleanup(func() { stopCmd(p2) })

	id2, tok2 := nextPermit(30 * time.Second)
	if id2 == id1 {
		t.Fatalf("expected a new id after relay restart, still %s", id1)
	}
	if resp := postPermit(t, url, id2+":"+tok2, "echo two"); string(body(t, resp)) != "two\n" {
		t.Fatalf("new permit did not serve after reconnect: status %d", resp.StatusCode)
	}
}

// TestResumeWrongTokenGetsFreshID verifies that a resume presenting the wrong
// token never reclaims the id: it silently falls back to a fresh registration
// (and leaves the retained conn alone for its real owner).
func TestResumeWrongTokenGetsFreshID(t *testing.T) {
	url, _ := startTray(t, "")
	wsURL := wsFromURL(url)
	token := "stk_" + strings.Repeat("a", 32)

	c1, w1 := registerRaw(t, wsURL, token, "")
	if err := c1.CloseNow(); err != nil {
		t.Fatalf("CloseNow: %v", err)
	}

	// An attacker who knows the (public) id but not the token asks to resume it.
	c2, w2 := registerRaw(t, wsURL, "stk_"+strings.Repeat("b", 32), w1.ID)
	if w2.ID == w1.ID {
		t.Fatal("resume with a wrong token must not reclaim the id")
	}
	_ = c2.Close(websocket.StatusNormalClosure, "done")

	// The real owner can still resume its id.
	c3, w3 := registerRaw(t, wsURL, token, w1.ID)
	if w3.ID != w1.ID {
		t.Fatalf("owner resume after a wrong-token attempt failed: %s != %s", w3.ID, w1.ID)
	}
	_ = c3.Close(websocket.StatusNormalClosure, "done")
}
