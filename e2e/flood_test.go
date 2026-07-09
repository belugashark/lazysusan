// Flood-limiter regression tests: solicited response traffic (the answer to
// the one request the relay is waiting on) must never trip the per-connection
// flood limiter, while unsolicited abuse still does.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/belugashark/lazysusan/internal/protocol"
)

// A single response larger than the limiter's 5 MiB/s byte bucket must be
// delivered intact, and the client must survive to serve the next request.
func TestLargeOutputDoesNotTripFloodLimiter(t *testing.T) {
	f := newFixture(t)
	resp := f.Post(t, `head -c 6291456 /dev/zero`, "X-Susan-Timeout", "30")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d reason=%s", resp.StatusCode, resp.Header.Get("X-Susan-Reason"))
	}
	got := body(t, resp)
	if len(got) != 6291456 {
		t.Fatalf("body len=%d want 6291456", len(got))
	}

	resp2 := f.Post(t, `echo alive`)
	defer resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Fatalf("client did not survive large output: status=%d reason=%s",
			resp2.StatusCode, resp2.Header.Get("X-Susan-Reason"))
	}
}

// A rapid burst of small requests (an agent in a tight tool-call loop) costs
// ~3 frames each; it must not exhaust the 100-frame bucket and kill the client.
func TestRapidRequestBurstDoesNotTripFloodLimiter(t *testing.T) {
	f := newFixture(t, "SUSAN_RATE_LIMIT_PER_MIN=100000")
	for i := 0; i < 50; i++ {
		resp := f.Post(t, fmt.Sprintf("echo burst-%d", i))
		got := body(t, resp)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("request %d: status=%d reason=%s", i, resp.StatusCode, resp.Header.Get("X-Susan-Reason"))
		}
		if strings.TrimSpace(string(got)) != fmt.Sprintf("burst-%d", i) {
			t.Fatalf("request %d: body=%q", i, got)
		}
	}
}

// A client that declares a size then drips it in tiny frames is gaming the
// solicited-traffic exemption; the chunk budget must kick it.
func TestTinyChunkSpamStillKicked(t *testing.T) {
	url, _ := startTray(t, "")
	wsURL := wsFromURL(url)
	token := "stk_" + strings.Repeat("a", 32)

	c, w := registerRaw(t, wsURL, token, "")
	defer c.CloseNow()
	permit := w.ID + ":" + token

	postDone := make(chan int, 1)
	go func() {
		req, _ := http.NewRequest("POST", url, strings.NewReader("whatever"))
		req.Header.Set("X-Susan-Permit", permit)
		req.Header.Set("X-Susan-Timeout", "10")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			postDone <- -1
			return
		}
		resp.Body.Close()
		postDone <- resp.StatusCode
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Read the relayed request, then answer with 1-byte binary frames far past
	// any plausible chunk count for the declared size.
	var reqID string
	for {
		typ, data, err := c.Read(ctx)
		if err != nil {
			t.Fatalf("read request: %v", err)
		}
		if typ != websocket.MessageText {
			continue
		}
		var env protocol.Envelope
		if json.Unmarshal(data, &env) != nil || env.Type != protocol.TypeRequest {
			continue
		}
		var req protocol.Request
		if err := json.Unmarshal(data, &req); err != nil {
			t.Fatalf("unmarshal request: %v", err)
		}
		reqID = req.ReqID
		break
	}
	if err := wsjson.Write(ctx, c, protocol.EndHeader{
		Type: protocol.TypeEndHeader, ReqID: reqID, Exit: 0, SizeBytes: 1 << 20,
	}); err != nil {
		t.Fatalf("end_header: %v", err)
	}
	kicked := false
	for i := 0; i < 2000; i++ {
		if err := c.Write(ctx, websocket.MessageBinary, []byte{'x'}); err != nil {
			kicked = true
			break
		}
	}
	if !kicked {
		// Writes can outrun the close frame; the read surfaces it.
		if _, _, err := c.Read(ctx); err == nil {
			t.Fatal("relay accepted 2000 tiny chunks without kicking")
		}
	}

	if code := <-postDone; code != 502 {
		t.Fatalf("caller got %d, want 502", code)
	}
}
