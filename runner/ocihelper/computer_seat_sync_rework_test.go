//go:build unix

package ocihelper

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestSeatSyncAdoptsNewCallerPaths(t *testing.T) {
	for _, matching := range []bool{true, false} {
		t.Run(map[bool]string{true: "matching", false: "different"}[matching], func(t *testing.T) {
			base := t.TempDir()
			if err := os.Mkdir(filepath.Join(base, "work"), 0700); err != nil {
				t.Fatal(err)
			}
			name := filepath.Join(base, "work/package-lock.json")
			content := "caller"
			mode := os.FileMode(0644)
			if !matching {
				content = "Seat"
				mode = 0600
			}
			if err := os.WriteFile(name, []byte(content), mode); err != nil {
				t.Fatal(err)
			}
			old := time.Unix(100, 0)
			if err := os.Chtimes(name, old, old); err != nil {
				t.Fatal(err)
			}
			out, err := runSyncFixture(t, base, syncFixtureArchive(t, syncFixtureFile{"package-lock.json", "caller", "", 0644}))
			if err != nil {
				t.Fatalf("new caller path: %v %s", err, out)
			}
			got, _ := os.ReadFile(name)
			info, _ := os.Stat(name)
			if string(got) != "caller" || info.Mode().Perm() != 0644 {
				t.Fatalf("caller lost: %q %v", got, info)
			}
			if matching && (!info.ModTime().Equal(old) || !bytes.Contains(out, []byte(`"files_written": 0`))) {
				t.Fatalf("matching adoption wrote file: %s", out)
			}
			if out, err = runSyncFixture(t, base, syncFixtureArchive(t)); err != nil {
				t.Fatalf("delete adopted path: %v %s", err, out)
			}
			if _, err := os.Stat(name); !os.IsNotExist(err) {
				t.Fatal("new path was not adopted into manifest")
			}
		})
	}
}

func TestSeatSyncRefusalNamesPathAndFix(t *testing.T) {
	base, outside := t.TempDir(), t.TempDir()
	if err := os.Mkdir(filepath.Join(base, "work"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(base, "work/blocked")); err != nil {
		t.Fatal(err)
	}
	out, err := runSyncFixture(t, base, syncFixtureArchive(t, syncFixtureFile{"blocked/file", "caller", "", 0644}))
	if err == nil || !strings.Contains(string(out), "blocked") || !strings.Contains(string(out), "retry") {
		t.Fatalf("unhelpful refusal: %v %s", err, out)
	}
}

func TestSeatSyncTamperedManifestsStayWithinWork(t *testing.T) {
	for _, manifest := range []string{"work-manifest.json", "work-pending.json"} {
		for _, absolute := range []bool{false, true} {
			t.Run(manifest+map[bool]string{true: "/absolute", false: "/dotdot"}[absolute], func(t *testing.T) {
				base := t.TempDir()
				if out, err := runSyncFixture(t, base, syncFixtureArchive(t, syncFixtureFile{"tracked", "old", "", 0644})); err != nil {
					t.Fatalf("setup: %v %s", err, out)
				}
				victim := filepath.Join(base, "victim")
				if err := os.WriteFile(victim, []byte("untouched"), 0600); err != nil {
					t.Fatal(err)
				}
				unsafe := "../victim"
				if absolute {
					unsafe = victim
				}
				raw, _ := json.Marshal(map[string]any{unsafe: map[string]any{"kind": "file", "mode": 0644, "size": 9, "hash": "not-authority"}})
				name := filepath.Join(base, ".wefty", manifest)
				if err := os.WriteFile(name, raw, 0600); err != nil {
					t.Fatal(err)
				}
				if out, err := runSyncFixture(t, base, syncFixtureArchive(t, syncFixtureFile{"tracked", "new", "", 0644})); err == nil {
					t.Fatalf("tampered manifest accepted: %s", out)
				}
				got, _ := os.ReadFile(victim)
				if string(got) != "untouched" {
					t.Fatalf("outside victim=%q", got)
				}
				got, _ = os.ReadFile(filepath.Join(base, "work/tracked"))
				if string(got) != "old" {
					t.Fatal("invalid state published files")
				}
				got, _ = os.ReadFile(name)
				if !bytes.Equal(got, raw) {
					t.Fatal("refusal replaced tampered state")
				}
			})
		}
	}
}

func TestSeatSyncManagedDirectoryReplacedByOutsideSymlink(t *testing.T) {
	base, outside := t.TempDir(), t.TempDir()
	if out, err := runSyncFixture(t, base, syncFixtureArchive(t, syncFixtureFile{"nested/tracked", "old", "", 0644})); err != nil {
		t.Fatalf("setup: %v %s", err, out)
	}
	victim := filepath.Join(outside, "tracked")
	if err := os.WriteFile(victim, []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(base, "work/nested/tracked")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(base, "work/nested")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(base, "work/nested")); err != nil {
		t.Fatal(err)
	}
	if out, err := runSyncFixture(t, base, syncFixtureArchive(t, syncFixtureFile{"nested/tracked", "new", "", 0644})); err != nil {
		t.Fatalf("repair: %v %s", err, out)
	}
	got, _ := os.ReadFile(victim)
	if string(got) != "outside" {
		t.Fatalf("sync escaped: %q", got)
	}
	got, _ = os.ReadFile(filepath.Join(base, "work/nested/tracked"))
	if string(got) != "new" {
		t.Fatal("managed directory was not repaired")
	}
}

func TestSeatSyncCleansStaleStagesWithoutFollowingLinks(t *testing.T) {
	base, outside := t.TempDir(), t.TempDir()
	state := filepath.Join(base, ".wefty")
	stage := filepath.Join(state, "sync-0123456789abcdef0123456789abcdef")
	if err := os.MkdirAll(filepath.Join(stage, "nested"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stage, "nested/partial"), []byte("allocated"), 0600); err != nil {
		t.Fatal(err)
	}
	victim := filepath.Join(outside, "keep")
	if err := os.WriteFile(victim, []byte("safe"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(stage, "outside")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(state, "sync-link")); err != nil {
		t.Fatal(err)
	}
	// A SIGKILL between atomic_json's create and rename leaves its temporary.
	temporary := filepath.Join(state, ".sync-0123456789abcdef0123456789abcdef")
	if err := os.WriteFile(temporary, []byte(`{"partial":`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victim, filepath.Join(state, ".sync-link")); err != nil {
		t.Fatal(err)
	}
	if out, err := runSyncFixture(t, base, syncFixtureArchive(t, syncFixtureFile{"x", "ready", "", 0644})); err != nil {
		t.Fatalf("sync: %v %s", err, out)
	}
	for _, name := range []string{stage, filepath.Join(state, "sync-link"), temporary, filepath.Join(state, ".sync-link")} {
		if _, err := os.Lstat(name); !os.IsNotExist(err) {
			t.Fatalf("stale stage remained: %s %v", name, err)
		}
	}
	got, _ := os.ReadFile(victim)
	if string(got) != "safe" {
		t.Fatal("stage cleanup followed link")
	}
}

func TestSeatSyncWaitsForLock(t *testing.T) {
	base := t.TempDir()
	state := filepath.Join(base, ".wefty")
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	lock, err := os.OpenFile(filepath.Join(state, "work.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	prefix := `import fcntl,os
real_flock=fcntl.flock
announced=False
def observed(fd,operation):
 global announced
 try: return real_flock(fd,operation)
 except BlockingIOError:
  if not announced: os.write(1,b'WAITING\n'); announced=True
  raise
fcntl.flock=observed
`
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, python, "-I", "-S", "-c", prefix+seatSyncProgram)
	cmd.Dir = base
	cmd.WaitDelay = time.Second
	cmd.Stdin = bytes.NewReader(syncFixtureArchive(t, syncFixtureFile{"x", "ready", "", 0644}))
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var diagnostic bytes.Buffer
	cmd.Stderr = &diagnostic
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()
	reader := bufio.NewReader(output)
	line, err := reader.ReadString('\n')
	if err != nil || line != "WAITING\n" {
		cmd.Process.Kill()
		cmd.Wait()
		t.Fatalf("lock handshake: %q %v %s", line, err, &diagnostic)
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); err != nil {
		cmd.Process.Kill()
		cmd.Wait()
		t.Fatal(err)
	}
	_, readErr := io.ReadAll(reader)
	err = cmd.Wait()
	if err != nil || readErr != nil {
		t.Fatalf("queued sync: %v %v %s", err, readErr, &diagnostic)
	}
	got, _ := os.ReadFile(filepath.Join(base, "work/x"))
	if string(got) != "ready" {
		t.Fatal("queued sync did not apply")
	}
}

func TestSeatSyncLockWaitIsBounded(t *testing.T) {
	prefix := `import errno,fcntl,time
elapsed=-15
def clock():
 global elapsed
 elapsed+=15
 return elapsed
def blocked(*args): raise BlockingIOError(errno.EAGAIN,'busy')
time.monotonic=clock
time.sleep=lambda _:None
fcntl.flock=blocked
`
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, python, "-I", "-S", "-c", prefix+seatSyncProgram)
	cmd.Dir = t.TempDir()
	cmd.WaitDelay = time.Second
	cmd.Stdin = bytes.NewReader(syncFixtureArchive(t))
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatal("bounded lock used wall time instead of injected clock")
	}
	if err == nil || !strings.Contains(string(out), "another working-copy update of this Seat is running; try again") || strings.Contains(string(out), "Errno") {
		t.Fatalf("bounded lock refusal: %v %s", err, out)
	}
}
