//go:build unix

package ocihelper

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

type syncFixtureFile struct {
	name, data, link string
	mode             int64
}

func syncFixtureArchive(t *testing.T, files ...syncFixtureFile) []byte {
	t.Helper()
	var b bytes.Buffer
	tw := tar.NewWriter(&b)
	for _, f := range files {
		h := &tar.Header{Name: f.name, Mode: f.mode, Size: int64(len(f.data)), Typeflag: tar.TypeReg}
		if f.link != "" {
			h.Typeflag = tar.TypeSymlink
			h.Linkname = f.link
			h.Size = 0
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if f.link == "" {
			if _, err := tw.Write([]byte(f.data)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return append(b.Bytes(), []byte("\nWEFTY_COPY_COMMIT_V1\n")...)
}
func runSyncFixture(t *testing.T, base string, input []byte) ([]byte, error) {
	t.Helper()
	program, err := os.ReadFile("computer_seat_sync.py")
	if err != nil {
		t.Fatal(err)
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, python, "-I", "-S", "-c", string(program))
	cmd.WaitDelay = time.Second
	cmd.Dir = base
	cmd.Stdin = bytes.NewReader(input)
	var diagnostic bytes.Buffer
	cmd.Stderr = &diagnostic
	out, err := cmd.Output()
	if ctx.Err() != nil {
		t.Fatalf("sync hung: %s", diagnostic.String())
	}
	if err != nil {
		out = append(out, diagnostic.Bytes()...)
	}
	return out, err
}
func TestSeatSyncProgramWorkingCopy(t *testing.T) {
	base := t.TempDir()
	initial := syncFixtureArchive(t, syncFixtureFile{"src/file", "caller", "", 0644}, syncFixtureFile{"deleted", "gone", "", 0644}, syncFixtureFile{"became-ignored", "gone", "", 0644}, syncFixtureFile{"bin", "run", "", 0644})
	if out, err := runSyncFixture(t, base, initial); err != nil {
		t.Fatalf("new: %v %s", err, out)
	}
	file := filepath.Join(base, "work/src/file")
	old := time.Unix(100, 0)
	if err := os.Chtimes(file, old, old); err != nil {
		t.Fatal(err)
	}
	if out, err := runSyncFixture(t, base, initial); err != nil {
		t.Fatalf("unchanged: %v %s", err, out)
	}
	info, _ := os.Stat(file)
	if !info.ModTime().Equal(old) {
		t.Fatal("unchanged mtime changed")
	}
	if err := os.WriteFile(file, []byte("Seat!!"), 0644); err != nil {
		t.Fatal(err)
	} // same size; mtime alone is insufficient
	if err := os.Chtimes(file, old, old); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(base, "work/node_modules"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "work/node_modules/dependency"), []byte("installed"), 0600); err != nil {
		t.Fatal(err)
	}
	next := syncFixtureArchive(t, syncFixtureFile{"src/file", "caller", "", 0644}, syncFixtureFile{"bin", "run", "", 0755}, syncFixtureFile{"new", "changed", "", 0644})
	out, err := runSyncFixture(t, base, next)
	if err != nil {
		t.Fatalf("update: %v %s", err, out)
	}
	var stats struct{ FilesWritten, FilesRemoved int }
	var wire map[string]int
	if err = json.Unmarshal(out, &wire); err != nil {
		t.Fatal(err)
	}
	stats.FilesWritten = wire["files_written"]
	stats.FilesRemoved = wire["files_removed"]
	if stats.FilesWritten != 3 || stats.FilesRemoved != 2 {
		t.Fatalf("counts: %s", out)
	}
	got, _ := os.ReadFile(file)
	if string(got) != "caller" {
		t.Fatalf("Seat changed file not restored: %q", got)
	}
	for _, name := range []string{"deleted", "became-ignored"} {
		if _, err := os.Stat(filepath.Join(base, "work", name)); !os.IsNotExist(err) {
			t.Fatalf("%s remained", name)
		}
	}
	got, _ = os.ReadFile(filepath.Join(base, "work/node_modules/dependency"))
	if string(got) != "installed" {
		t.Fatal("dependency lost")
	}
	info, _ = os.Stat(filepath.Join(base, "work/bin"))
	if info.Mode().Perm() != 0755 {
		t.Fatalf("executable mode: %v", info.Mode())
	}
}
func TestSeatSyncProgramSymlinksAndPartialInput(t *testing.T) {
	base, outside := t.TempDir(), t.TempDir()
	first := syncFixtureArchive(t, syncFixtureFile{"tracked", "old", "", 0644}, syncFixtureFile{"link", "", outside, 0777})
	if out, err := runSyncFixture(t, base, first); err != nil {
		t.Fatalf("symlink capture: %v %s", err, out)
	}
	manifest := filepath.Join(base, ".wefty/work-manifest.json")
	before, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	// Incomplete producer input must never publish a partial file or manifest.
	partial := syncFixtureArchive(t, syncFixtureFile{"tracked", "new", "", 0644})
	partial = partial[:len(partial)-10]
	if out, err := runSyncFixture(t, base, partial); err == nil {
		t.Fatalf("partial accepted: %s", out)
	}
	after, _ := os.ReadFile(manifest)
	if !bytes.Equal(before, after) {
		t.Fatal("partial changed manifest")
	}
	got, _ := os.ReadFile(filepath.Join(base, "work/tracked"))
	if string(got) != "old" {
		t.Fatalf("partial file published: %q", got)
	}
	var short bytes.Buffer
	shortTar := tar.NewWriter(&short)
	if err := shortTar.WriteHeader(&tar.Header{Name: "tracked", Mode: 0644, Size: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	if _, err := shortTar.Write([]byte("half")); err != nil {
		t.Fatal(err)
	}
	if out, err := runSyncFixture(t, base, short.Bytes()); err == nil {
		t.Fatalf("truncated payload accepted: %s", out)
	}
	after, _ = os.ReadFile(manifest)
	got, _ = os.ReadFile(filepath.Join(base, "work/tracked"))
	if !bytes.Equal(before, after) || string(got) != "old" {
		t.Fatal("truncated payload changed committed state")
	}
	next := syncFixtureArchive(t, syncFixtureFile{"link/escape", "bad", "", 0644})
	if _, err := runSyncFixture(t, base, next); err != nil { /* safely refusing a parent link is allowed */
	}
	if _, err := os.Stat(filepath.Join(outside, "escape")); !os.IsNotExist(err) {
		t.Fatal("write escaped through symlink")
	}
	// An unowned path must not be replaced, even to create a caller directory.
	if err := os.Symlink(outside, filepath.Join(base, "work/unowned")); err != nil {
		t.Fatal(err)
	}
	if out, err := runSyncFixture(t, base, syncFixtureArchive(t, syncFixtureFile{"unowned/escape", "bad", "", 0644})); err == nil {
		t.Fatalf("unowned parent overwritten: %s", out)
	}
	if _, err := os.Stat(filepath.Join(outside, "escape")); !os.IsNotExist(err) {
		t.Fatal("unowned link followed")
	}
}
func TestSeatSyncProgramChangedFileEmptyDirectoriesAndRecovery(t *testing.T) {
	base := t.TempDir()
	first := syncFixtureArchive(t, syncFixtureFile{"empty-after-delete/file", "old", "", 0644}, syncFixtureFile{"tracked", "old", "", 0644})
	if out, err := runSyncFixture(t, base, first); err != nil {
		t.Fatalf("first: %v %s", err, out)
	}
	next := syncFixtureArchive(t, syncFixtureFile{"tracked", "new content", "", 0644})
	if out, err := runSyncFixture(t, base, next); err != nil {
		t.Fatalf("changed: %v %s", err, out)
	}
	got, _ := os.ReadFile(filepath.Join(base, "work/tracked"))
	if string(got) != "new content" {
		t.Fatalf("changed bytes=%q", got)
	}
	if _, err := os.Stat(filepath.Join(base, "work/empty-after-delete")); !os.IsNotExist(err) {
		t.Fatal("empty managed directory remained")
	}
	manifest := filepath.Join(base, ".wefty/work-manifest.json")
	before, _ := os.ReadFile(manifest)
	// Force an apply failure after a new path was published. The following
	// snapshot drops that path again: a pending journal must remove it too.
	program, err := os.ReadFile("computer_seat_sync.py")
	if err != nil {
		t.Fatal(err)
	}
	prefix := `import os
real_rename=os.rename
def fail_manifest(source,destination,*args,**kwargs):
 if destination=='work-manifest.json': raise OSError('injected manifest failure')
 return real_rename(source,destination,*args,**kwargs)
os.rename=fail_manifest
`
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, python, "-I", "-S", "-c", prefix+string(program))
	child.WaitDelay = time.Second
	child.Dir = base
	child.Stdin = bytes.NewReader(syncFixtureArchive(t, syncFixtureFile{"new-in-failed-apply", "complete", "", 0644}, syncFixtureFile{"tracked", "pending", "", 0644}))
	if out, err := child.CombinedOutput(); err == nil {
		t.Fatalf("injected failure accepted: %s", out)
	}
	if ctx.Err() != nil {
		t.Fatal("failed apply hung")
	}
	after, _ := os.ReadFile(manifest)
	if !bytes.Equal(before, after) {
		t.Fatal("failed apply changed old manifest")
	}
	got, _ = os.ReadFile(filepath.Join(base, "work/new-in-failed-apply"))
	if string(got) != "complete" {
		t.Fatalf("half file published: %q", got)
	}
	if out, err := runSyncFixture(t, base, next); err != nil {
		t.Fatalf("repair: %v %s", err, out)
	}
	if _, err := os.Stat(filepath.Join(base, "work/new-in-failed-apply")); !os.IsNotExist(err) {
		t.Fatal("failed-apply path not retired")
	}
	got, _ = os.ReadFile(filepath.Join(base, "work/tracked"))
	if string(got) != "new content" {
		t.Fatalf("failed apply not repaired: %q", got)
	}
}

func TestSeatSyncProgramRefusesUnsafeInputAndAdoptsCallerFile(t *testing.T) {
	for _, name := range []string{"../escape", "/escape", "a/../escape", "a//escape", ".git/config", "pkg/.git/config"} {
		t.Run(name, func(t *testing.T) {
			base := t.TempDir()
			if out, err := runSyncFixture(t, base, syncFixtureArchive(t, syncFixtureFile{"setup", "ready", "", 0644})); err != nil {
				t.Fatalf("valid setup: %v %s", err, out)
			}
			if out, err := runSyncFixture(t, base, syncFixtureArchive(t, syncFixtureFile{name, "bad", "", 0644})); err == nil {
				t.Fatalf("unsafe archive accepted: %s", out)
			}
		})
	}
	base := t.TempDir()
	if out, err := runSyncFixture(t, base, syncFixtureArchive(t, syncFixtureFile{"setup", "ready", "", 0644})); err != nil {
		t.Fatalf("valid setup: %v %s", err, out)
	}
	dest := filepath.Join(base, "work/unowned")
	if err := os.WriteFile(dest, []byte("installed"), 0600); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if out, err := runSyncFixture(t, base, syncFixtureArchive(t, syncFixtureFile{"unowned", "caller", "", 0644})); err != nil {
			t.Fatalf("caller path refused: %v %s", err, out)
		}
	}
	got, _ := os.ReadFile(dest)
	if string(got) != "caller" {
		t.Fatal("caller file did not win")
	}
}

func TestSeatSyncProgramPreservesCapturedPathBytes(t *testing.T) {
	base := t.TempDir()
	input := syncFixtureArchive(t, syncFixtureFile{"directory\\name/file\nname", "exact bytes", "", 0644})
	if out, err := runSyncFixture(t, base, input); err != nil {
		t.Fatalf("captured path rejected: %v %s", err, out)
	}
	got, err := os.ReadFile(filepath.Join(base, "work", "directory\\name", "file\nname"))
	if err != nil || string(got) != "exact bytes" {
		t.Fatalf("captured path=%q %v", got, err)
	}
}
