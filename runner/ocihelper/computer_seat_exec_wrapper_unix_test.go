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
	cmd.Stderr = os.Stderr
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
	script := `import os,sys,time,signal
signal.signal(signal.SIGTERM,lambda *_:None)
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

func TestSeatWrapperPlainBackgroundKeepsWritingAfterCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	root := t.TempDir()
	// The shell's plain '&' child retains stdout/stderr and its process group.
	script := `import subprocess,sys
code="import os,time;open('pid','w').write(str(os.getpid()));[(print('background',flush=True),time.sleep(.1)) for _ in range(30)];open('survived','w').write('yes')"
subprocess.run(['/bin/sh','-c',sys.executable+" -u -c "+__import__('shlex').quote(code)+" &"],check=True)`
	cmd := seatWrapperCommand(t, ctx, script)
	cmd.Dir = root
	cmd.Stdout = newSeatCommandStream(ctx, nil)
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	waitSeatCommandFile(t, ctx, filepath.Join(root, "pid"))
	b, _ := os.ReadFile(filepath.Join(root, "pid"))
	pid, _ := strconv.Atoi(string(b))
	defer syscall.Kill(pid, syscall.SIGKILL)
	waitSeatCommandFile(t, ctx, filepath.Join(root, "survived"))
}
func TestSeatWrapperTermAllowsCommandCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	root := t.TempDir()
	script := `import signal,sys,time
from pathlib import Path
def term(sig,frame):
 Path('cleaned').write_text('yes')
 # Keep the command alive after cleanup to exercise the forced-kill edge.
signal.signal(signal.SIGTERM,term)
Path('ready').write_text('yes')
while True: time.sleep(.05)`
	cmd := seatWrapperCommand(t, ctx, script)
	cmd.Dir = root
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
	defer cmd.Process.Kill()
	waitSeatCommandFile(t, ctx, filepath.Join(root, "ready"))
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "cleaned")); err != nil {
		t.Fatalf("TERM cleanup never ran: %v", err)
	}
	if exit == nil || exit.Signal != 9 {
		t.Fatalf("cleanup/escalation exit lost: %+v", exit)
	}
}
func TestSeatWrapperLaunchPermissionDeniedReturns126(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "not-executable")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0600); err != nil {
		t.Fatal(err)
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, python, "-I", "-S", "-c", seatCommandWrapper, path)
	var exit *SeatCommandExit
	cmd.Stdout = newSeatCommandStream(ctx, func(e SeatCommandEvent) error {
		if e.Exit != nil {
			exit = e.Exit
		}
		return nil
	})
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	if exit == nil || exit.Code == nil || *exit.Code != 126 {
		t.Fatalf("unrunnable executable exit: %+v", exit)
	}
}
func TestSeatWrapperCancelGraceUsesInjectedClock(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	harness := `import signal,sys,types
ns={'__name__':'wrapper_test'}
try: exec(sys.argv[1],ns)
except SystemExit: raise AssertionError('wrapper has no importable cancellation policy')
now=[10.0];calls=[]
ns['clock']=lambda: now[0]
ns['child']=types.SimpleNamespace(pid=1234,returncode=None)
ns['os'].killpg=lambda pid,sig:calls.append((pid,sig))
ns['stop'](signal.SIGTERM,None)
assert calls==[(1234,signal.SIGTERM)],calls
now[0]=14.99
assert not ns['escalate_cancel']()
assert len(calls)==1,calls
now[0]=15.0
assert ns['escalate_cancel']()
assert calls==[(1234,signal.SIGTERM),(1234,signal.SIGKILL)],calls`
	if out, err := exec.CommandContext(t.Context(), python, "-I", "-S", "-c", harness, seatCommandWrapper).CombinedOutput(); err != nil {
		t.Fatalf("grace policy: %v %s", err, out)
	}
}
