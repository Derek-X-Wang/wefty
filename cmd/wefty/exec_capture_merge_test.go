package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func execTestUnmergedRepo(t *testing.T, paths ...string) string {
	t.Helper()
	repo := execTestRepo(t)
	for _, path := range paths {
		writeExecFile(t, filepath.Join(repo, path), "base\n")
	}
	execTestGit(t, repo, "add", ".")
	execTestGit(t, repo, "commit", "-qm", "merge base")
	execTestGit(t, repo, "checkout", "-qb", "side")
	for _, path := range paths {
		writeExecFile(t, filepath.Join(repo, path), "side\n")
	}
	execTestGit(t, repo, "commit", "-qam", "side")
	execTestGit(t, repo, "checkout", "-q", "-")
	for _, path := range paths {
		writeExecFile(t, filepath.Join(repo, path), "main\n")
	}
	execTestGit(t, repo, "commit", "-qam", "main")
	cmd := exec.Command("git", "merge", "side")
	cmd.Dir, cmd.Env = repo, execTestGitEnv()
	out, err := cmd.CombinedOutput()
	if !execGitExited(err, 1) || execTestGit(t, repo, "ls-files", "-u") == "" {
		t.Fatalf("expected an unmerged index: err=%v output=%s", err, out)
	}
	return repo
}

func TestExecCaptureUnmergedWorkingFiles(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		t.Run(fmt.Sprintf("deleted=%t", deleted), func(t *testing.T) {
			execTestStorageRoot(t)
			const path = "sub/resolved file\tname"
			repo := execTestUnmergedRepo(t, path)
			// Marker-like text inside a line, a heading underline, and binary bytes
			// are resolved content.
			const resolved = "resolved working content\n inline <<<<<<< ======= >>>>>>>\nTitle\n==========\nbin\x00ary\n"
			if deleted {
				if err := os.Remove(filepath.Join(repo, path)); err != nil {
					t.Fatal(err)
				}
			} else {
				writeExecFile(t, filepath.Join(repo, path), resolved)
			}
			indexPath := filepath.Join(repo, ".git", "index")
			before, err := os.ReadFile(indexPath)
			if err != nil {
				t.Fatal(err)
			}
			unmerged := execTestGit(t, repo, "ls-files", "-u", "-z")
			capture, err := captureExec(t.Context(), repo)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cleanupExecStorage(capture.Storage) })
			after, err := os.ReadFile(indexPath)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("caller index changed: err=%v", err)
			}
			if got := execTestGit(t, repo, "ls-files", "-u", "-z"); got != unmerged {
				t.Fatalf("caller merge stages changed: before=%q after=%q", unmerged, got)
			}
			private := filepath.Join(capture.Storage, "snapshot.git")
			stages, err := execGit(t.Context(), repo, private, nil, "ls-files", "-u")
			if err != nil || stages != "" {
				t.Fatalf("merge stages leaked into private index: %q %v", stages, err)
			}
			if deleted {
				entry, err := execGit(t.Context(), repo, private, nil, "ls-tree", capture.Tree, "--", path)
				if err != nil || entry != "" {
					t.Fatalf("deleted file captured: %q %v", entry, err)
				}
			} else {
				got, err := execGit(t.Context(), repo, private, nil, "show", capture.Tree+":"+path)
				if err != nil || got != resolved {
					t.Fatalf("snapshot content=%q want=%q err=%v", got, resolved, err)
				}
			}
		})
	}
}

func TestExecCaptureUnmergedRefusals(t *testing.T) {
	for _, content := range []string{"<<<<<<< ours\n", "resolved\n=======\n", ">>>>>>> theirs", "||||||| base\r\n", "bin\x00ary\n<<<<<<<<<< ours\n"} {
		t.Run(fmt.Sprintf("%q", content), func(t *testing.T) {
			repo := execTestUnmergedRepo(t, "sub/conflicted")
			writeExecFile(t, filepath.Join(repo, "sub/conflicted"), content)
			_, err := captureExec(t.Context(), repo)
			var usage usageError
			if !errors.As(err, &usage) || !strings.Contains(err.Error(), "sub/conflicted") {
				t.Fatalf("want usage error naming path, got %v", err)
			}
			if !strings.Contains(err.Error(), "resolve the markers") {
				t.Fatalf("want marker refusal, got %v", err)
			}
		})
	}
}

func TestExecCaptureUnmergedMarkerPathsCapped(t *testing.T) {
	repo := execTestUnmergedRepo(t, "a.go", "b.go", "c.go", "d.go", "e.go", "f.go", "g.go")
	_, err := captureExec(t.Context(), repo)
	const want = "unresolved merge: conflict markers remain in a.go, b.go, c.go, d.go, e.go and 2 more; resolve the markers, or finish or abort the merge"
	var usage usageError
	if !errors.As(err, &usage) || err.Error() != want {
		t.Fatalf("want %q, got %v", want, err)
	}
}
