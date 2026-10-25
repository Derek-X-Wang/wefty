//go:build unix

package ocihelper

import (
	"archive/tar"
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A SIGTERM that Python's main thread does not take inside its blocking read
// must still end a working-copy sync, keep the last applied manifest and leave
// no stage folder (#1283 for the copy program). Blocking SIGTERM in the main
// thread sends it to a helper thread, so the main thread gets no EINTR.
func TestSeatSyncProgramSignalWhileMainThreadBlocked(t *testing.T) {
	base := t.TempDir()
	if out, err := runSyncFixture(t, base, syncFixtureArchive(t, syncFixtureFile{"src/file", "caller", "", 0644})); err != nil {
		t.Fatalf("initial sync: %v %s", err, out)
	}
	manifest := filepath.Join(base, ".wefty", "work-manifest.json")
	before, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	program, err := os.ReadFile("computer_seat_sync.py")
	if err != nil {
		t.Fatal(err)
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	// hashlib loads before the mask: importing it after the mask stops this
	// forced case from routing SIGTERM to the helper thread on some builds.
	prefix := seatCopyStackDump + `import hashlib, os, signal, sys, threading, time
watched_thread = threading.get_ident()
def at_stdin_read(frame):
    if frame.f_code.co_name == 'read' and frame.f_code.co_filename == '<string>':
        frame = frame.f_back
    return frame.f_code.co_name == '__read' and frame.f_code.co_filename.endswith('tarfile.py')
def watch():
    last, still = None, 0
    while still < 5:
        time.sleep(0.02)
        frame = sys._current_frames()[watched_thread]
        where = (id(frame), frame.f_lasti) if at_stdin_read(frame) else None
        still, last = (still + 1 if where is not None and where == last else 0), where
    os.write(1, b'PARKED\n')
    threading.Event().wait()
threading.Thread(target=watch, daemon=True).start()
signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM})
`
	child := exec.Command(python, "-I", "-S", "-c", prefix+string(program))
	child.Dir = base
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
	if err = tar.NewWriter(&partial).WriteHeader(&tar.Header{Name: "src/file", Mode: 0644, Size: 1 << 20, Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err = input.Write(partial.Bytes()); err != nil {
		t.Fatal(err)
	}
	readSeatCopyHandshake(t, child, output, "PARKED\n", deadline, &stderr)
	terminateSeatCopyChild(t, child, &stderr)
	if code := child.ProcessState.ExitCode(); code != 1 {
		t.Fatalf("cancelled sync exit=%d %s", code, stderr.String())
	}
	after, err := os.ReadFile(manifest)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("manifest changed by a cancelled sync: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(base, "work", "src", "file")); err != nil || string(got) != "caller" {
		t.Fatalf("working copy file=%q %v", got, err)
	}
	entries, err := os.ReadDir(filepath.Join(base, ".wefty"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "sync-") {
			t.Fatalf("stage folder remained: %s", entry.Name())
		}
	}
}
