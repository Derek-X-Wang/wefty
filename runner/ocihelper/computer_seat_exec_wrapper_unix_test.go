//go:build linux || darwin

package ocihelper

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func seatWrapperCommand(t *testing.T, ctx context.Context, script string, args ...string) *exec.Cmd {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	argv := []string{"-I", "-S", "-c", seatCommandWrapper, python, "-I", "-S", "-c", script}
	cmd := exec.CommandContext(ctx, python, append(argv, args...)...)
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	return cmd
}
func TestSeatCommandWrapperOutputExitAndSignal(t *testing.T) {
	for _, row := range []struct {
		script       string
		code, signal int
	}{
		{`import os;os.write(1,b'x'*200000);os.write(2,b'err');raise SystemExit(23)`, 23, 0},
		{`import os,signal;os.kill(os.getpid(),signal.SIGTERM)`, 0, 15},
	} {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		var out, errout bytes.Buffer
		var exit *SeatCommandExit
		stream := newSeatCommandStream(ctx, func(e SeatCommandEvent) error {
			if e.Exit != nil {
				exit = e.Exit
			} else if e.Stream == "stdout" {
				out.Write(e.Data)
			} else {
				errout.Write(e.Data)
			}
			return nil
		})
		cmd := seatWrapperCommand(t, ctx, row.script)
		cmd.Stdout = stream
		if err := cmd.Run(); err != nil {
			t.Fatal(err)
		}
		if err := stream.finish(); err != nil {
			t.Fatal(err)
		}
		cancel()
		if exit == nil || exit.Signal != row.signal || (row.signal == 0 && (exit.Code == nil || *exit.Code != row.code)) {
			t.Fatalf("exit %+v", exit)
		}
		if row.signal == 0 && (out.Len() != 200000 || errout.String() != "err") {
			t.Fatalf("lost output: %d %q", out.Len(), errout.String())
		}
	}
}
func TestSeatCommandWrapperCancelTreeKeepsDetachedProgram(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	root := t.TempDir()
	ready := filepath.Join(root, "ready")
	script := `import os,sys,time
root=sys.argv[1]
if os.fork()==0:
 os.setsid()
 if os.fork(): os._exit(0)
 open(root+'/background','w').write(str(os.getpid()))
 while True: time.sleep(1)
if os.fork()==0:
 open(root+'/child','w').write(str(os.getpid()))
 while True: time.sleep(1)
while not os.path.exists(root+'/child') or not os.path.exists(root+'/background'): time.sleep(.01)
open(root+'/ready','w').write(str(os.getpid()))
while True: time.sleep(1)`
	cmd := seatWrapperCommand(t, ctx, script, root)
	var exit *SeatCommandExit
	cmd.Stdout = newSeatCommandStream(ctx, func(e SeatCommandEvent) error {
		if e.Exit != nil {
			exit = e.Exit
		}
		return nil
	})
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	})
	// Real process integration: wait for file evidence, never infer readiness
	// from elapsed time. No lifecycle policy uses this wall clock.
	waitSeatCommandFile(t, ctx, ready)
	readPID := func(name string) int {
		b, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
		if err != nil {
			t.Fatal(err)
		}
		return pid
	}
	leader, child, background := readPID("ready"), readPID("child"), readPID("background")
	defer syscall.Kill(background, syscall.SIGKILL)
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if exit == nil || exit.Signal != 9 {
		t.Fatalf("cancel exit %+v", exit)
	}
	for _, pid := range []int{leader, child} {
		// On Linux an unreaped zombie is stopped but still answers kill(0).
		if output, err := exec.Command("ps", "-o", "stat=", "-p", strconv.Itoa(pid)).Output(); err == nil && len(bytes.TrimSpace(output)) > 0 && !bytes.HasPrefix(bytes.TrimSpace(output), []byte("Z")) {
			t.Fatalf("command process %d survived: %s", pid, output)
		}
	}
	if err := syscall.Kill(background, 0); err != nil {
		t.Fatalf("detached program stopped: %v", err)
	}
}
func TestSeatCommandWrapperBackgroundDoesNotHoldOutputOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	root := t.TempDir()
	script := `import os,sys,time
if os.fork(): os._exit(0)
os.setsid()
if os.fork(): os._exit(0)
open(sys.argv[1]+'/pid','w').write(str(os.getpid()))
while True: time.sleep(1)`
	stream := newSeatCommandStream(ctx, nil)
	cmd := seatWrapperCommand(t, ctx, script, root)
	cmd.Stdout = stream
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	if err := stream.finish(); err != nil {
		t.Fatal(err)
	}
	waitSeatCommandFile(t, ctx, filepath.Join(root, "pid"))
	b, err := os.ReadFile(filepath.Join(root, "pid"))
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(b))
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Kill(pid, syscall.SIGKILL)
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatal(err)
	}
}
func waitSeatCommandFile(t *testing.T, ctx context.Context, path string) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if b, err := os.ReadFile(path); err == nil && len(b) > 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("file did not arrive: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}
