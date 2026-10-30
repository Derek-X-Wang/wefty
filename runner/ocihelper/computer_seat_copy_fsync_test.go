//go:build unix

package ocihelper

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// largeSeatFile is written into a Seat by the copy and sync programs below.
// It is larger than two sync intervals, so a bounded program syncs it at
// least three times.
const largeSeatFile = 130 << 20

// fsyncRecordingPrefix replaces os.fsync for the program that follows it and
// writes the size of every large regular file it syncs to FSYNC_LOG on exit.
// The tests check when the programs sync, so the disk sync itself is skipped.
const fsyncRecordingPrefix = `import atexit, os, stat
synced = []
def recording_fsync(fd):
    info = os.fstat(fd)
    if stat.S_ISREG(info.st_mode) and info.st_size > (1 << 20):
        synced.append(info.st_size)
os.fsync = recording_fsync
atexit.register(lambda: open(os.environ['FSYNC_LOG'], 'w').write(' '.join(map(str, synced))))
`

// runRecordingFsync runs program with the fsync recorder and returns the sizes
// at which it synced the large file.
func runRecordingFsync(t *testing.T, dir, program string, input []byte, args ...string) []int64 {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	log := filepath.Join(t.TempDir(), "fsync.log")
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, python, append([]string{"-I", "-S", "-c", fsyncRecordingPrefix + program}, args...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "FSYNC_LOG="+log)
	cmd.Stdin = bytes.NewReader(input)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("program: %v %s", err, stderr.String())
	}
	data, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	var sizes []int64
	for _, field := range strings.Fields(string(data)) {
		size, err := strconv.ParseInt(field, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		sizes = append(sizes, size)
	}
	return sizes
}

// requireBoundedSyncs checks that no stretch of a written file waited for one
// fsync longer than the sync interval plus one read (#1261).
func requireBoundedSyncs(t *testing.T, sizes []int64) {
	t.Helper()
	const bound = 64<<20 + 32<<10
	if len(sizes) < 3 || sizes[len(sizes)-1] != largeSeatFile {
		t.Fatalf("large file synced at sizes %v, want at least three syncs ending at %d", sizes, largeSeatFile)
	}
	var previous int64
	for _, size := range sizes {
		if size-previous > bound {
			t.Fatalf("large file synced at sizes %v: %d bytes waited for one fsync", sizes, size-previous)
		}
		previous = size
	}
}

func TestSeatCopyProgramSyncsLargeFileInBoundedSteps(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "large")
	if err := os.WriteFile(source, make([]byte, largeSeatFile), 0600); err != nil {
		t.Fatal(err)
	}
	var archive, stderr bytes.Buffer
	pack := exec.Command(python, "-I", "-S", "-c", seatCopyProgram, "out", source)
	pack.Stdout, pack.Stderr = &archive, &stderr
	if err := pack.Run(); err != nil {
		t.Fatalf("pack: %v %s", err, stderr.String())
	}
	base := t.TempDir()
	input := append(archive.Bytes(), []byte("\nWEFTY_COPY_COMMIT_V1\n")...)
	requireBoundedSyncs(t, runRecordingFsync(t, base, seatCopyProgram, input, "in", "large"))
	if info, err := os.Stat(filepath.Join(base, "large")); err != nil || info.Size() != largeSeatFile {
		t.Fatalf("copied file: %v %v", info, err)
	}
}

func TestSeatSyncProgramSyncsLargeFileInBoundedSteps(t *testing.T) {
	program, err := os.ReadFile("computer_seat_sync.py")
	if err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	input := syncFixtureArchive(t, syncFixtureFile{"large", string(make([]byte, largeSeatFile)), "", 0644})
	requireBoundedSyncs(t, runRecordingFsync(t, base, string(program), input))
	if info, err := os.Stat(filepath.Join(base, "work", "large")); err != nil || info.Size() != largeSeatFile {
		t.Fatalf("synced file: %v %v", info, err)
	}
}
