package scripts

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// #1625: `go build ./cmd/<name>` writes its binary to the repository root, and
// a `git add -A` once committed a 62 MB one. Every such path stays ignored,
// while the command's own source stays tracked.
func TestRootBuildBinariesAreIgnored(t *testing.T) {
	entries, err := os.ReadDir("../cmd")
	if err != nil {
		t.Fatal(err)
	}
	commands := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		commands++
		name := entry.Name()
		if !gitIgnores(t, name) {
			t.Errorf(".gitignore does not ignore the root build output /%s", name)
		}
		if source := "cmd/" + name + "/main.go"; gitIgnores(t, source) {
			t.Errorf(".gitignore ignores %s; anchor the entry to the root", source)
		}
	}
	if commands == 0 {
		t.Fatal("no commands under cmd/")
	}
}

// No tracked file is a build artifact. The largest source file is well under
// this cap; a compiled binary is far over it.
func TestNoLargeTrackedFiles(t *testing.T) {
	const limit = 1 << 20
	output, err := exec.Command("git", "-C", "..", "ls-files", "-z").Output()
	if err != nil {
		t.Fatal(err)
	}
	paths := bytes.Split(bytes.TrimSuffix(output, []byte{0}), []byte{0})
	if len(paths) < 2 {
		t.Fatalf("git ls-files listed %d paths", len(paths))
	}
	for _, path := range paths {
		info, err := os.Lstat(filepath.Join("..", string(path)))
		if errors.Is(err, fs.ErrNotExist) {
			continue // deleted in the working tree, not yet staged
		}
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() > limit {
			t.Errorf("tracked file %s is %d bytes (limit %d); is it a build artifact?", path, info.Size(), limit)
		}
	}
}

func gitIgnores(t *testing.T, path string) bool {
	t.Helper()
	err := exec.Command("git", "-C", "..", "check-ignore", "-q", "--no-index", path).Run()
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() == 1 {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return true
}
