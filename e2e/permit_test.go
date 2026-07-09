package e2e

import (
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
)

// startTray starts a relay; relayKey gates it, "" leaves it open.
func startTray(t *testing.T, relayKey string) (trayURL string, port int) {
	t.Helper()
	_, tray := buildBinaries(t)
	port = freePort(t)
	trayURL = fmt.Sprintf("http://127.0.0.1:%d", port)
	log := &strings.Builder{}
	cmd := exec.Command(tray)
	cmd.Env = append(envWithout(os.Environ(), "SUSAN_RELAY_KEY"),
		"PORT="+strconv.Itoa(port),
		"SUSAN_PUBLIC_URL="+trayURL,
		"SUSAN_INSECURE=1",
	)
	if relayKey != "" {
		cmd.Env = append(cmd.Env, "SUSAN_RELAY_KEY="+relayKey)
	}
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopCmd(cmd) })
	if err := waitFor(trayURL+"/healthz", 5*time.Second); err != nil {
		t.Fatalf("tray did not become healthy: %v\nlog:\n%s", err, log.String())
	}
	return trayURL, port
}

// startSusan starts susan with the given args and returns its stdout reader.
func startSusan(t *testing.T, args ...string) io.Reader {
	t.Helper()
	susan, _ := buildBinaries(t)
	stdout, w := io.Pipe()
	stderr := &strings.Builder{}
	cmd := exec.Command(susan, args...)
	cmd.Env = envWithout(os.Environ(), "SUSAN_RELAY_KEY")
	cmd.Stdout = w
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopCmd(cmd) })
	return stdout
}

func postPermit(t *testing.T, url, permit, cmd string) *http.Response {
	t.Helper()
	req, err := http.NewRequest("POST", url, strings.NewReader(cmd))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Susan-Permit", permit)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// TestConnectString: `susan susan+insecure://<key>@host:port` joins a gated relay,
// prints relay + permit, and the permit authorizes a command via POST /.
func TestConnectString(t *testing.T) {
	trayURL, port := startTray(t, "sekrit-key")
	stdout := startSusan(t, fmt.Sprintf("susan+insecure://sekrit-key@127.0.0.1:%d", port))

	relayLine, permit := readConnectBlock(t, stdout, 5*time.Second)
	if relayLine != trayURL {
		t.Fatalf("relay = %q, want %q", relayLine, trayURL)
	}
	if id, tok, ok := strings.Cut(permit, ":"); !ok || id == "" || tok == "" {
		t.Fatalf("permit %q is not <machine-id>:<call-secret>", permit)
	}

	resp := postPermit(t, trayURL, permit, "echo via-connect-string")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	if strings.TrimSpace(string(b)) != "via-connect-string" {
		t.Fatalf("body=%q", b)
	}
}

// TestOpenRelayConnectString: a keyless connect string joins an open relay
// (SUSAN_RELAY_KEY unset) — the v1 default deployment shape.
func TestOpenRelayConnectString(t *testing.T) {
	trayURL, port := startTray(t, "")
	stdout := startSusan(t, fmt.Sprintf("susan+insecure://127.0.0.1:%d", port))

	_, permit := readConnectBlock(t, stdout, 5*time.Second)
	resp := postPermit(t, trayURL, permit, "echo via-open-relay")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	if strings.TrimSpace(string(b)) != "via-open-relay" {
		t.Fatalf("body=%q", b)
	}
}

// TestConnectStringBadKey: wrong relay key in the connect string is rejected
// with the relay's unauthorized error.
func TestConnectStringBadKey(t *testing.T) {
	_, port := startTray(t, "right-key")
	susan, _ := buildBinaries(t)
	cmd := exec.Command(susan, fmt.Sprintf("susan+insecure://wrong-key@127.0.0.1:%d", port))
	cmd.Env = envWithout(os.Environ(), "SUSAN_RELAY_KEY")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("want non-zero exit, got success:\n%s", out)
	}
	if !strings.Contains(string(out), "unauthorized") {
		t.Fatalf("want unauthorized in output, got:\n%s", out)
	}
}

// TestJSONOutput: --json emits one JSON object on stdout with relay, permit,
// and a ready-made curl; the permit works against the relay.
func TestJSONOutput(t *testing.T) {
	trayURL, port := startTray(t, "sekrit-key")
	stdout := startSusan(t, "--json", fmt.Sprintf("susan+insecure://sekrit-key@127.0.0.1:%d", port))

	br := newLineScanner(stdout)
	var doc struct {
		Relay  string `json:"relay"`
		Permit string `json:"permit"`
		Curl   string `json:"curl"`
	}
	select {
	case line := <-br:
		if err := json.Unmarshal([]byte(line), &doc); err != nil {
			t.Fatalf("stdout line is not JSON: %v\nline: %s", err, line)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for JSON line")
	}
	if doc.Relay != trayURL {
		t.Fatalf("relay = %q, want %q", doc.Relay, trayURL)
	}
	if id, tok, ok := strings.Cut(doc.Permit, ":"); !ok || id == "" || tok == "" {
		t.Fatalf("permit %q is not <machine-id>:<call-secret>", doc.Permit)
	}
	if !strings.Contains(doc.Curl, doc.Permit) || !strings.Contains(doc.Curl, "X-Susan-Permit") {
		t.Fatalf("curl field doesn't carry the permit header: %q", doc.Curl)
	}

	resp := postPermit(t, doc.Relay, doc.Permit, "echo via-json")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	if strings.TrimSpace(string(b)) != "via-json" {
		t.Fatalf("body=%q", b)
	}
}

// TestConnectStringIsCompleteSpec: a positional connect string defines BOTH the
// relay and the key — a keyless connect string must not fall back to a relay
// key inherited from the environment.
func TestConnectStringIsCompleteSpec(t *testing.T) {
	_, port := startTray(t, "right-key")
	susan, _ := buildBinaries(t)
	cmd := exec.Command(susan, fmt.Sprintf("susan+insecure://127.0.0.1:%d", port))
	cmd.Env = append(envWithout(os.Environ(), "SUSAN_RELAY_KEY"), "SUSAN_RELAY_KEY=right-key")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("keyless connect string must ignore env key and be rejected, got success:\n%s", out)
	}
	if !strings.Contains(string(out), "unauthorized") {
		t.Fatalf("want unauthorized in output, got:\n%s", out)
	}
}

// TestMalformedPermit401: a permit without the id:token shape is rejected as
// unauthorized, not treated as a lookup.
func TestMalformedPermit401(t *testing.T) {
	trayURL, _ := startTray(t, "")
	resp := postPermit(t, trayURL, "no-colon-here", "uptime")
	defer resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("status=%d, want 401", resp.StatusCode)
	}
}

// readConnectBlock scrapes the startup output for the relay and permit lines.
func readConnectBlock(t *testing.T, r io.Reader, d time.Duration) (relay, permit string) {
	t.Helper()
	br := newLineScanner(r)
	deadline := time.After(d)
	for relay == "" || permit == "" {
		select {
		case line := <-br:
			if i := strings.Index(line, "relay:"); i >= 0 {
				relay = strings.TrimSpace(line[i+len("relay:"):])
			} else if i := strings.Index(line, "permit:"); i >= 0 {
				permit = strings.TrimSpace(line[i+len("permit:"):])
			}
		case <-deadline:
			t.Fatalf("timeout; got relay=%q permit=%q", relay, permit)
		}
	}
	return relay, permit
}

// newLineScanner returns a channel of stdout lines.
func newLineScanner(r io.Reader) <-chan string {
	ch := make(chan string, 64)
	go func() {
		defer close(ch)
		buf := make([]byte, 4096)
		acc := ""
		for {
			n, err := r.Read(buf)
			if n > 0 {
				acc += string(buf[:n])
				for {
					i := strings.IndexByte(acc, '\n')
					if i < 0 {
						break
					}
					ch <- acc[:i]
					acc = acc[i+1:]
				}
			}
			if err != nil {
				if acc != "" {
					ch <- acc
				}
				return
			}
		}
	}()
	return ch
}
