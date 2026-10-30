//go:build unix

package ocihelper

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/seatcopy"
)

func TestSeatCopyProgramRoundTripAndFailedWrite(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	source := filepath.Join(base, "input")
	if err = os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(source, ".env"), []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	var archive, stderr bytes.Buffer
	pack := exec.Command(python, "-I", "-S", "-c", seatCopyProgram, "out", source)
	pack.Stdout = &archive
	pack.Stderr = &stderr
	if err = pack.Run(); err != nil {
		t.Fatalf("pack: %v %s", err, stderr.String())
	}
	dest := filepath.Join(base, "result")
	size, err := seatcopy.Receive(bytes.NewReader(archive.Bytes()), dest)
	if err != nil || size != 7 {
		t.Fatalf("receive: %d %v", size, err)
	}
	in := exec.Command(python, "-I", "-S", "-c", seatCopyProgram, "in", "relative")
	in.Dir = base
	in.Stdin = bytes.NewReader(append(archive.Bytes(), []byte("\nWEFTY_COPY_COMMIT_V1\n")...))
	stderr.Reset()
	in.Stderr = &stderr
	if err = in.Run(); err != nil {
		t.Fatalf("copy in: %v %s", err, stderr.String())
	}
	got, err := os.ReadFile(filepath.Join(base, "relative", ".env"))
	if err != nil || string(got) != "private" {
		t.Fatalf("copied=%q err=%v", got, err)
	}
	failed := exec.Command(python, "-I", "-S", "-c", "import resource; resource.setrlimit(resource.RLIMIT_FSIZE,(1,1))\n"+seatCopyProgram, "in", "failed")
	failed.Dir = base
	failed.Stdin = bytes.NewReader(append(archive.Bytes(), []byte("\nWEFTY_COPY_COMMIT_V1\n")...))
	stderr.Reset()
	failed.Stderr = &stderr
	if err = failed.Run(); err == nil {
		t.Fatal("full destination accepted")
	}
	if _, err = os.Stat(filepath.Join(base, "failed")); !os.IsNotExist(err) {
		t.Fatalf("partial destination: %v", err)
	}
	entries, _ := os.ReadDir(base)
	for _, entry := range entries {
		if len(entry.Name()) > 12 && entry.Name()[:12] == ".wefty-copy-" {
			t.Fatalf("staging remained: %s", entry.Name())
		}
	}
}

func TestSeatCopyProgramParentLinkCancellationAndDeepPack(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	base, parent := t.TempDir(), t.TempDir()
	link := filepath.Join(base, "parent")
	if err = os.Symlink(parent, link); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "file")
	if err = os.WriteFile(source, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	if _, err = seatcopy.Pack(&archive, source); err != nil {
		t.Fatal(err)
	}
	in := exec.Command(python, "-I", "-S", "-c", seatCopyProgram, "in", filepath.Join(link, "result"))
	in.Stdin = io.MultiReader(bytes.NewReader(archive.Bytes()), strings.NewReader("\nWEFTY_COPY_COMMIT_V1\n"))
	if out, err := in.CombinedOutput(); err != nil {
		t.Fatalf("linked parent: %v %s", err, out)
	}
	// The real child blocks on input after opening a staged file. No sleeps.
	prefix := seatCopyStackDump + "import os\nreal_open=os.open\ndef observed(path,*args,**kwargs):\n fd=real_open(path,*args,**kwargs)\n if path=='item': os.write(1,b'STAGED\\n')\n return fd\nos.open=observed\n"
	child := exec.Command(python, "-I", "-S", "-c", prefix+seatCopyProgram, "in", filepath.Join(parent, "cancelled"))
	input, err := child.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	child.Stderr = &stderr
	deadline := time.Now().Add(seatCopyHandshakeBound)
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	defer child.Process.Kill()
	defer input.Close()
	var partial bytes.Buffer
	tw := tar.NewWriter(&partial)
	if err = tw.WriteHeader(&tar.Header{Name: "item", Mode: 0600, Size: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	// tarfile's streaming reader asks for a whole block before it opens output.
	if _, err = input.Write(partial.Bytes()); err != nil {
		t.Fatal(err)
	}
	readSeatCopyHandshake(t, child, output, "STAGED\n", deadline, &stderr)
	terminateSeatCopyChild(t, child, &stderr)
	requireNoSeatCopyStaging(t, parent)
	deep := filepath.Join(base, "deep")
	if err = os.Mkdir(deep, 0700); err != nil {
		t.Fatal(err)
	}
	leaf := deep
	for range 150 {
		leaf = filepath.Join(leaf, "x")
		if err = os.Mkdir(leaf, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err = os.WriteFile(filepath.Join(leaf, "file"), []byte("deep"), 0600); err != nil {
		t.Fatal(err)
	}
	pack := exec.Command(python, "-I", "-S", "-c", "import tarfile,sys;sys.setrecursionlimit(120)\n"+seatCopyProgram, "out", deep)
	pack.Stdout = io.Discard
	if err := pack.Run(); err != nil {
		t.Fatalf("deep pack failed: %v", err)
	}
}

// A SIGTERM that Python's main thread does not take inside its blocking read
// must still end the copy and remove the staging folder (#1283). Blocking
// SIGTERM in the main thread sends it to a helper thread, so the main thread
// gets no EINTR. The test signals only after the main thread is parked.
func TestSeatCopyProgramSignalWhileMainThreadBlocked(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	parent := t.TempDir()
	prefix := seatCopyStackDump + `import os, signal, sys, threading, time
main, staged = threading.get_ident(), threading.Event()
real_open = os.open
def observed(path, *args, **kwargs):
    fd = real_open(path, *args, **kwargs)
    if path == 'item':
        staged.set()
    return fd
os.open = observed
def at_stdin_read(frame):
    # The fixed program waits in its Input.read, called by tarfile's stream
    # reader; the old program waits in that tarfile frame itself.
    if frame.f_code.co_name == 'read' and frame.f_code.co_filename == '<string>':
        frame = frame.f_back
    return frame.f_code.co_name == '__read' and frame.f_code.co_filename.endswith('tarfile.py')
def watch():
    staged.wait()
    last, still = None, 0
    while still < 5:
        time.sleep(0.02)
        frame = sys._current_frames()[main]
        where = (id(frame), frame.f_lasti) if at_stdin_read(frame) else None
        still, last = (still + 1 if where is not None and where == last else 0), where
    os.write(1, b'PARKED\n')
    threading.Event().wait()
threading.Thread(target=watch, daemon=True).start()
signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM})
`
	child := exec.Command(python, "-I", "-S", "-c", prefix+seatCopyProgram, "in", filepath.Join(parent, "cancelled"))
	input, err := child.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	child.Stderr = &stderr
	deadline := time.Now().Add(seatCopyHandshakeBound)
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	defer child.Process.Kill()
	defer input.Close()
	var partial bytes.Buffer
	if err = tar.NewWriter(&partial).WriteHeader(&tar.Header{Name: "item", Mode: 0600, Size: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	if _, err = input.Write(partial.Bytes()); err != nil {
		t.Fatal(err)
	}
	readSeatCopyHandshake(t, child, output, "PARKED\n", deadline, &stderr)
	terminateSeatCopyChild(t, child, &stderr)
	if code := child.ProcessState.ExitCode(); code != 1 {
		t.Fatalf("cancelled copy exit=%d %s", code, stderr.String())
	}
	if _, err = os.Lstat(filepath.Join(parent, "cancelled")); !os.IsNotExist(err) {
		t.Fatalf("cancelled destination: %v", err)
	}
	requireNoSeatCopyStaging(t, parent)
}

// A full stdout pipe must not hide SIGTERM from the packing thread (#1287).
// The watcher inherits an unblocked mask before the main thread blocks SIGTERM.
func TestSeatCopyProgramSignalWhileStdoutBlocked(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "file")
	if err = os.WriteFile(source, make([]byte, 1<<20), 0600); err != nil {
		t.Fatal(err)
	}
	prefix := seatCopyStackDump + `import os, signal, sys, threading, time
main = threading.get_ident()
def at_stdout_write(frame):
    # The fixed program waits in Output.write; the old program waits in
    # tarfile's stream writer itself, inside the buffered stdout write.
    if frame.f_code.co_name == 'write' and frame.f_code.co_filename == '<string>':
        frame = frame.f_back
    return frame.f_code.co_name == '__write' and frame.f_code.co_filename.endswith('tarfile.py')
def watch():
    last, still = None, 0
    while still < 5:
        time.sleep(0.02)
        frame = sys._current_frames()[main]
        where = (id(frame), frame.f_lasti) if at_stdout_write(frame) else None
        still, last = (still + 1 if where is not None and where == last else 0), where
    os.write(3, b'PARKED\n')
    threading.Event().wait()
threading.Thread(target=watch, daemon=True).start()
signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM})
`
	child := exec.Command(python, "-I", "-S", "-c", prefix+seatCopyProgram, "out", source)
	output, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close() // Keep stdout open without reading, so its pipe fills.
	handshake, ready, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer handshake.Close()
	defer ready.Close()
	child.ExtraFiles = []*os.File{ready}
	var stderr bytes.Buffer
	child.Stderr = &stderr
	deadline := time.Now().Add(seatCopyHandshakeBound)
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	defer child.Process.Kill()
	if err = ready.Close(); err != nil {
		t.Fatal(err)
	}
	readSeatCopyHandshake(t, child, handshake, "PARKED\n", deadline, &stderr)
	terminateSeatCopyChild(t, child, &stderr)
	if code := child.ProcessState.ExitCode(); code != 1 {
		t.Fatalf("cancelled pack exit=%d %s", code, stderr.String())
	}
}

func TestSeatCopyProgramPackShortWrites(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "file")
	if err = os.WriteFile(source, bytes.Repeat([]byte("short writes\n"), 4096), 0600); err != nil {
		t.Fatal(err)
	}
	var want, got, stderr bytes.Buffer
	pack := exec.Command(python, "-I", "-S", "-c", seatCopyProgram, "out", source)
	pack.Stdout, pack.Stderr = &want, &stderr
	if err = pack.Run(); err != nil {
		t.Fatalf("pack: %v %s", err, stderr.String())
	}
	// Force both partial writes and a full pipe after a writable select (#1287).
	prefix := `import os
real_write, writes = os.write, 0
def short_write(fd, data):
    global writes
    if fd == 1:
        writes += 1
        if writes == 1:
            raise BlockingIOError()
        data = data[:37]
    return real_write(fd, data)
os.write = short_write
`
	pack = exec.Command(python, "-I", "-S", "-c", prefix+seatCopyProgram+"\nassert writes > 1\n", "out", source)
	stderr.Reset()
	pack.Stdout, pack.Stderr = &got, &stderr
	if err = pack.Run(); err != nil {
		t.Fatalf("pack with short writes: %v %s", err, stderr.String())
	}
	if !bytes.Equal(got.Bytes(), want.Bytes()) {
		t.Fatalf("short writes changed tar stream: got %d bytes, want %d", got.Len(), want.Len())
	}
}

// seatCopyHandshakeBound covers child startup and its handshake line.
const seatCopyHandshakeBound = 30 * time.Second

// readSeatCopyHandshake requires the child's handshake line before deadline.
// A child that never reaches that point is killed and fails the test instead
// of hanging the package until its timeout (#1283).
func readSeatCopyHandshake(t *testing.T, child *exec.Cmd, output io.Reader, want string, deadline time.Time, stderr *bytes.Buffer) {
	t.Helper()
	type handshake struct {
		line string
		err  error
	}
	got := make(chan handshake, 1)
	go func() {
		line, err := bufio.NewReader(output).ReadString('\n')
		got <- handshake{line, err}
	}()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case h := <-got:
		if h.err == nil && h.line == want {
			return
		}
		_ = child.Process.Kill()
		_ = child.Wait()
		t.Fatalf("copy child handshake=%q %v, want %q (#1283); stderr=%s", h.line, h.err, want, stderr.String())
	case <-timer.C:
		_ = child.Process.Kill()
		_ = child.Wait()
		t.Fatalf("copy child did not print %q within %s of start (#1283); stderr=%s", strings.TrimSpace(want), seatCopyHandshakeBound, stderr.String())
	}
}

// seatCopyStackDump lets a stuck test child print every Python thread's stack
// on SIGUSR1, so a bound failure shows where the child waited. The chained
// Python handler adds the main thread's pending and blocked signals and how
// often the program's SIGTERM handler ran: evidence the stack alone lacks
// (#1283). On macOS an already pending SIGTERM handler runs before it.
// seatCopyTerminateBound is generous because a heavily swapping host can
// stall a healthy child for seconds; it still fails far below the package timeout.
const seatCopyTerminateBound = 30 * time.Second

const seatCopyStackDump = `import faulthandler, signal, sys
_wefty_term_runs = []
_wefty_signal = signal.signal
def _wefty_mark(sig, handler):
    # Counts SIGTERM handler entries for the report below.
    if sig == signal.SIGTERM and callable(handler):
        inner = handler
        def handler(*args):
            _wefty_term_runs.append(1)
            return inner(*args)
    return _wefty_signal(sig, handler)
signal.signal = _wefty_mark
def _wefty_names(signals):
    return sorted(getattr(s, 'name', str(s)) for s in signals)
def _wefty_signal_state(*_):
    sys.stderr.write('signals: pending=%s blocked=%s sigterm_handler_runs=%d\n' % (
        _wefty_names(signal.sigpending()), _wefty_names(signal.pthread_sigmask(signal.SIG_BLOCK, [])), len(_wefty_term_runs)))
_wefty_signal(signal.SIGUSR1, _wefty_signal_state)
faulthandler.register(signal.SIGUSR1, all_threads=True, chain=True)
`

// terminateSeatCopyChild sends SIGTERM and requires a failed exit within a
// short bound, so a missed signal fails in seconds instead of hanging the
// package until its timeout. On timeout it asks the child for its stacks.
func terminateSeatCopyChild(t *testing.T, child *exec.Cmd, stderr *bytes.Buffer) {
	t.Helper()
	if err := child.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled copy succeeded")
		}
	case <-time.After(seatCopyTerminateBound):
		_ = child.Process.Signal(syscall.SIGUSR1)
		// Record whether the child exits in the diagnostic window; that alone
		// does not say why.
		after := "killed"
		select {
		case err := <-done:
			after = fmt.Sprintf("exited after SIGUSR1: %v", err)
		case <-time.After(5 * time.Second):
			_ = child.Process.Kill()
			<-done
		}
		t.Fatalf("copy child did not exit within %s of SIGTERM (#1283); %s; stderr=%s", seatCopyTerminateBound, after, stderr.String())
	}
}

func requireNoSeatCopyStaging(t *testing.T, parent string) {
	t.Helper()
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".wefty-copy-") {
			t.Fatalf("cancel left %s", entry.Name())
		}
	}
}

// The shim retains a stdin writer after cio finishes copying. Model that
// handle with a pipe which only the explicit CloseIO callback can close.
func TestSeatCopyProgramClosesRetainedStdin(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	source := filepath.Join(base, ".env")
	if err := os.WriteFile(source, []byte("ignored secret"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, python, "-I", "-S", "-c", seatCopyProgram, "in", filepath.Join(base, "result"))
	child.WaitDelay = time.Second
	retained, err := child.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer retained.Close()
	var output bytes.Buffer
	child.Stdout = &output
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	input, writer := io.Pipe()
	defer input.Close()
	packed := make(chan error, 1)
	go func() { _, err := seatcopy.Pack(writer, source); writer.CloseWithError(err); packed <- err }()
	copied := make(chan error, 1)
	go func() {
		_, err := io.Copy(retained, &seatCommandInput{
			reader:     io.MultiReader(input, strings.NewReader("\nWEFTY_COPY_COMMIT_V1\n")),
			closeStdin: retained.Close,
		})
		copied <- err
	}()
	if err := child.Wait(); err != nil {
		t.Fatalf("copy with retained runtime stdin: %v; context=%v; output=%s", err, ctx.Err(), output.String())
	}
	if err := <-copied; err != nil {
		t.Fatal(err)
	}
	if err := <-packed; err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(base, "result"))
	if err != nil || string(got) != "ignored secret" {
		t.Fatalf("copy=%q err=%v", got, err)
	}
}
