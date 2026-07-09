// Package exec runs a single shell command in its own process group, captures
// combined stdout+stderr up to a byte cap, and propagates timeout/kill
// semantics.
package exec

import (
	"bytes"
	"context"
	"errors"
	"os"
	osexec "os/exec"
	"sync"
	"syscall"
	"time"
)

// Result is the outcome of a single Run.
type Result struct {
	Exit       int
	DurationMs int64
	TimedOut   bool
	Truncated  bool
	Output     []byte
}

// safeBuf is a mutex-wrapped *bytes.Buffer. Wrapping the same pointer in both
// Stdout and Stderr lets os/exec see them as equal interface values — the
// workaround for the bug where independent writers can deadlock.
type safeBuf struct {
	mu    sync.Mutex
	buf   *bytes.Buffer
	cap   int64
	used  int64
	trunc bool
}

func (b *safeBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cap > 0 {
		remain := b.cap - b.used
		if remain <= 0 {
			b.trunc = true
			return len(p), nil
		}
		if int64(len(p)) > remain {
			b.buf.Write(p[:remain])
			b.used += remain
			b.trunc = true
			return len(p), nil
		}
	}
	n, _ := b.buf.Write(p)
	b.used += int64(n)
	return len(p), nil
}

// Run spawns shell -c body, enforces the timeout via cmd.Cancel override
// (SIGTERM the process group; SIGKILL the parent after WaitDelay), and returns
// the buffered output.
//
// shell is the resolved shell (caller has already done the $SHELL/sh fallback).
// cwd, env are passed straight through.
// maxBytes==0 means unbounded capture.
func Run(parentCtx context.Context, shell, body, cwd string, env []string, timeout time.Duration, maxBytes int64) Result {
	ctx, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()

	cmd := osexec.CommandContext(ctx, shell, "-c", body)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Dir = cwd
	cmd.Env = env
	cmd.Stdin = nil

	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return os.ErrProcessDone
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	}
	cmd.WaitDelay = 2 * time.Second

	sb := &safeBuf{buf: &bytes.Buffer{}, cap: maxBytes}
	cmd.Stdout = sb
	cmd.Stderr = sb

	start := time.Now()
	err := cmd.Run()
	dur := time.Since(start)

	res := Result{
		DurationMs: dur.Milliseconds(),
		Truncated:  sb.trunc,
		Output:     sb.buf.Bytes(),
	}

	// Belt-and-suspenders: kill any orphaned group members.
	if cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}

	if ctx.Err() == context.DeadlineExceeded {
		res.TimedOut = true
		res.Exit = -1
		return res
	}

	if err == nil {
		res.Exit = 0
		return res
	}

	var ee *osexec.ExitError
	if errors.As(err, &ee) {
		res.Exit = ee.ExitCode()
		// negative exit code means terminated by signal with no exit value
		if res.Exit < 0 {
			res.Exit = -1
		}
		return res
	}

	// The process exited cleanly but a background child kept the output pipe
	// open past WaitDelay (e.g. `server & echo started`). The command itself
	// succeeded — report its real exit status, not a collection failure.
	if errors.Is(err, osexec.ErrWaitDelay) && cmd.ProcessState != nil {
		res.Exit = cmd.ProcessState.ExitCode()
		if res.Exit < 0 {
			res.Exit = -1
		}
		return res
	}

	// non-exit failure (e.g., couldn't exec the shell)
	res.Exit = -1
	return res
}
