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

func TestSeatWrapperCancelReportsExitForZombieLeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	root := t.TempDir()
	cmd := seatWrapperCommand(t, ctx, `import time
open('ready','w').write('yes')
while True: time.sleep(1)`)
	cmd.Dir = root
	var exit *SeatCommandExit
	stream := newSeatCommandStream(ctx, func(e SeatCommandEvent) error {
		if e.Exit != nil {
			exit = e.Exit
		}
		return nil
	})
	cmd.Stdout = stream
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()
	waitSeatCommandFile(t, ctx, filepath.Join(root, "ready"))
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("zombie leader lost command exit: %v", err)
	}
	if err := stream.finish(); err != nil {
		t.Fatal(err)
	}
	if exit == nil || exit.Signal != int(syscall.SIGTERM) {
		t.Fatalf("cancel exit: %+v", exit)
	}
}

func TestSeatWrapperPermissionDeniedRequiresExitedLeader(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	harness := `import errno,signal,sys,types
source=sys.argv[1]
for state in ('alive','zombie','reaped'):
 ns={'__name__':'wrapper_test'}
 exec(source,ns)
 calls=[]
 def denied(pid,sig):
  calls.append((pid,sig))
  raise PermissionError(errno.EPERM,'fixture group signal refused')
 def poll():
  if state=='alive': return None
  child.returncode=23
  return 23
 child=types.SimpleNamespace(pid=1234,returncode=23 if state=='reaped' else None,poll=poll)
 ns['child']=child
 original=ns['os'].killpg
 ns['os'].killpg=denied
 try:
  try: ns['signal_command_group'](signal.SIGTERM)
  except PermissionError:
   assert state=='alive',state
  else:
   assert state!='alive','live leader permission failure was hidden'
   # Reaping a zombie permits PID reuse. Never signal that group again.
   ns['signal_command_group'](signal.SIGKILL)
   assert len(calls)==1,calls
 finally: ns['os'].killpg=original`
	if out, err := exec.CommandContext(t.Context(), python, "-I", "-S", "-c", harness, seatCommandWrapper).CombinedOutput(); err != nil {
		t.Fatalf("permission policy: %v %s", err, out)
	}
}

func TestSeatWrapperDiscardReaderClosesStdioBeforeSetup(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	harness := `import os,selectors,signal,sys
ns={'__name__':'wrapper_test'}
exec(sys.argv[1],ns)
r,w=os.pipe();os.close(w)
reader=os.fdopen(r,'rb')
selector=selectors.DefaultSelector()
selector.register(reader,selectors.EVENT_READ,'stdout')
original=selectors.DefaultSelector
# Stop the forked reader before it can rebuild its poller. The command's
# output must already be detached, even if this process never resumes.
def paused_selector():
 open(sys.argv[2]+'/reader.pid','w').write(str(os.getpid()))
 os.kill(os.getpid(),signal.SIGSTOP)
 return original()
selectors.DefaultSelector=paused_selector
ns['retain_background_readers'](selector)
ns['emit']({'exit':{'code':0}})`
	cmd := exec.CommandContext(ctx, python, "-I", "-S", "-c", harness, seatCommandWrapper, root)
	cmd.WaitDelay = time.Second
	var exit *SeatCommandExit
	cmd.Stdout = newSeatCommandStream(ctx, func(e SeatCommandEvent) error {
		if e.Exit != nil {
			exit = e.Exit
		}
		return nil
	})
	cmd.Stderr = os.Stderr
	runErr := cmd.Run()
	waitSeatCommandFile(t, ctx, filepath.Join(root, "reader.pid"))
	b, err := os.ReadFile(filepath.Join(root, "reader.pid"))
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(b))
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Kill(pid, syscall.SIGKILL)
	if runErr != nil || exit == nil || exit.Code == nil || *exit.Code != 0 {
		t.Fatalf("stopped discard reader retained command stdio: err=%v exit=%+v", runErr, exit)
	}
}

func TestSeatWrapperDiscardReaderUsesBlockingSelect(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	harness := `import os,selectors,sys
ns={'__name__':'wrapper_test'}
exec(sys.argv[1],ns)
r,w=os.pipe();os.close(w)
reader=os.fdopen(r,'rb')
selector=selectors.DefaultSelector()
selector.register(reader,selectors.EVENT_READ,'stdout')
original=selectors.DefaultSelector
class RecordedSelector(original):
 def select(self,timeout=None):
  open(sys.argv[2]+'/timeout','w').write(repr(timeout))
  return super().select(timeout)
selectors.DefaultSelector=RecordedSelector
ns['retain_background_readers'](selector)`
	if out, err := exec.CommandContext(ctx, python, "-I", "-S", "-c", harness, seatCommandWrapper, root).CombinedOutput(); err != nil {
		t.Fatalf("discard reader: %v %s", err, out)
	}
	waitSeatCommandFile(t, ctx, filepath.Join(root, "timeout"))
	b, err := os.ReadFile(filepath.Join(root, "timeout"))
	if err != nil || string(b) != "None" {
		t.Fatalf("idle discard reader polls: timeout=%q err=%v", b, err)
	}
}
