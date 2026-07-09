package exec

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestRun_Success(t *testing.T) {
	r := Run(context.Background(), "/bin/sh", "echo hello && echo bye", "/", os.Environ(), 5*time.Second, 0)
	if r.Exit != 0 {
		t.Fatalf("exit=%d", r.Exit)
	}
	if !strings.Contains(string(r.Output), "hello") || !strings.Contains(string(r.Output), "bye") {
		t.Fatalf("output=%q", string(r.Output))
	}
	if r.TimedOut {
		t.Fatal("should not be timed out")
	}
}

func TestRun_NonZeroExit(t *testing.T) {
	r := Run(context.Background(), "/bin/sh", "exit 7", "/", os.Environ(), 5*time.Second, 0)
	if r.Exit != 7 {
		t.Fatalf("exit=%d want 7", r.Exit)
	}
}

func TestRun_Timeout(t *testing.T) {
	r := Run(context.Background(), "/bin/sh", "sleep 5", "/", os.Environ(), 200*time.Millisecond, 0)
	if !r.TimedOut {
		t.Fatal("expected timed_out")
	}
	if r.Exit != -1 {
		t.Fatalf("exit=%d want -1", r.Exit)
	}
	if r.DurationMs > 5000 {
		t.Fatalf("took too long: %d", r.DurationMs)
	}
}

func TestRun_ProcessGroupKilled(t *testing.T) {
	// Spawn a child that backgrounds another process; both should be killed on timeout.
	r := Run(context.Background(), "/bin/sh", `sh -c 'sleep 100 & echo PID:$!; wait'`, "/", os.Environ(), 300*time.Millisecond, 0)
	if !r.TimedOut {
		t.Fatal("expected timeout")
	}
	for _, l := range strings.Split(string(r.Output), "\n") {
		if strings.HasPrefix(l, "PID:") {
			return
		}
	}
}

func TestRun_TruncationCap(t *testing.T) {
	r := Run(context.Background(), "/bin/sh", `yes a | head -c 200000`, "/", os.Environ(), 5*time.Second, 1024)
	if !r.Truncated {
		t.Fatal("expected truncated")
	}
	if int64(len(r.Output)) != 1024 {
		t.Fatalf("output len=%d want 1024", len(r.Output))
	}
}

func TestRun_UnboundedWhenCapZero(t *testing.T) {
	r := Run(context.Background(), "/bin/sh", `yes a | head -c 50000`, "/", os.Environ(), 5*time.Second, 0)
	if r.Truncated {
		t.Fatal("should not be truncated")
	}
	if len(r.Output) < 50000 {
		t.Fatalf("len=%d want >=50000", len(r.Output))
	}
}

func TestRun_StdinIsClosed(t *testing.T) {
	r := Run(context.Background(), "/bin/sh", "cat -", "/", os.Environ(), 2*time.Second, 0)
	if r.Exit != 0 {
		t.Fatalf("exit=%d", r.Exit)
	}
	if len(r.Output) != 0 {
		t.Fatalf("expected empty output, got %q", string(r.Output))
	}
}

func TestRun_BackgroundChildHoldsPipe(t *testing.T) {
	// A background child inherits the output pipe and outlives the shell. The
	// shell exits 0 immediately; after WaitDelay the run must report the
	// shell's real exit code, not a pipe-collection failure.
	r := Run(context.Background(), "/bin/sh", "sleep 30 & echo hi", "/", os.Environ(), 10*time.Second, 0)
	if r.Exit != 0 {
		t.Fatalf("exit=%d want 0", r.Exit)
	}
	if r.TimedOut {
		t.Fatal("should not be timed out")
	}
	if !strings.Contains(string(r.Output), "hi") {
		t.Fatalf("output=%q", string(r.Output))
	}
}
