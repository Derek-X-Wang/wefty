package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestExecCheckoutGitStateFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, state := range []string{"clean", "dirty"} {
		for _, folder := range []string{".", "sub"} {
			t.Run(state+"/"+folder, func(t *testing.T) {
				stack := newExecStack(t, execRealBinaryIdleLimit)
				repo := execTestRepo(t)
				writeExecFile(t, filepath.Join(repo, "sub", "deleted"), "delete me\n")
				writeExecFile(t, filepath.Join(repo, ".gitattributes"), "sub/attributes ident text eol=crlf filter=unused\n")
				writeExecFile(t, filepath.Join(repo, "sub", "attributes"), "$Id$\nexact\n")
				execTestGit(t, repo, "add", ".")
				execTestGit(t, repo, "commit", "-qm", "Git state fixture")
				base := execTestGit(t, repo, "rev-parse", "HEAD")
				wantStatus := ""
				if state == "dirty" {
					writeExecFile(t, filepath.Join(repo, "sub", "tracked"), "local change\n")
					// Staging is deliberately discarded inside the run.
					execTestGit(t, repo, "add", "sub/tracked")
					writeExecFile(t, filepath.Join(repo, "sub", "new"), "untracked bytes\n")
					if err := os.Remove(filepath.Join(repo, "sub", "deleted")); err != nil {
						t.Fatal(err)
					}
					wantStatus = " D sub/deleted\n M sub/tracked\n?? sub/new\n"
				}
				tracked, err := os.ReadFile(filepath.Join(repo, "sub", "tracked"))
				if err != nil {
					t.Fatal(err)
				}
				attributes, err := os.ReadFile(filepath.Join(repo, "sub", "attributes"))
				if err != nil {
					t.Fatal(err)
				}
				t.Chdir(filepath.Join(repo, folder))
				script := `set -e
git rev-parse HEAD
printf '\000'
git status --porcelain=v1
printf '\000'
root=$(git rev-parse --show-toplevel)
cat "$root/sub/tracked"
printf '\000'
cat "$root/sub/attributes"
printf '\000'
git show HEAD:sub/tracked
printf '\000'
if [ -e "$root/sub/new" ]; then
  test ! -e "$root/sub/deleted"
  cat "$root/sub/new"
else
  cat "$root/sub/deleted"
fi
`
				code, stdout, stderr := runPolishBinary(t, binary,
					"--l1="+stack.l1Address, "--l3="+stack.l3Address,
					"--node-config="+stack.clients.nodeConfigPath, "exec", "--", "/bin/sh", "-c", script)
				if code != 0 {
					t.Fatalf("exit=%d stdout=%q stderr=%s", code, stdout, stderr)
				}
				parts := bytes.Split(stdout, []byte{0})
				if len(parts) != 6 {
					t.Fatalf("unexpected command output: %q", stdout)
				}
				if string(parts[0]) != base {
					t.Errorf("HEAD=%q, want caller HEAD %q", parts[0], base)
				}
				if string(parts[1]) != wantStatus {
					t.Errorf("status=%q, want %q", parts[1], wantStatus)
				}
				if !bytes.Equal(parts[2], tracked) || !bytes.Equal(parts[3], attributes) {
					t.Errorf("working bytes changed: tracked=%q attributes=%q", parts[2], parts[3])
				}
				if string(parts[4]) != "base" {
					t.Errorf("HEAD:sub/tracked=%q, want base contents", parts[4])
				}
				lastPath := "deleted"
				if state == "dirty" {
					lastPath = "new"
				}
				last, err := os.ReadFile(filepath.Join(repo, "sub", lastPath))
				if err != nil || !bytes.Equal(parts[5], last) {
					t.Errorf("%s bytes=%q, want %q (read error=%v)", lastPath, parts[5], last, err)
				}
			})
		}
	}
}

func TestExecCheckoutUsageFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, fixture := range []struct {
		name, message string
		setup         func(*testing.T, string)
	}{
		{"outside-checkout", "wefty exec runs from inside a Git checkout; cd into the project first", nil},
		{"unborn-head", "wefty exec needs at least one commit in this checkout; commit first", func(t *testing.T, dir string) {
			execTestGit(t, dir, "init", "-q")
		}},
		{"sparse-checkout", "sparse checkout is not supported by exec; disable sparse checkout or use a full clone", func(t *testing.T, dir string) {
			execTestGit(t, dir, "init", "-q")
			execTestGit(t, dir, "commit", "--allow-empty", "-qm", "base")
			execTestGit(t, dir, "config", "core.sparseCheckout", "true")
		}},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			stack := newExecStack(t, execRealBinaryIdleLimit)
			dir := t.TempDir()
			if fixture.setup != nil {
				fixture.setup(t, dir)
			}
			t.Chdir(dir)
			for _, jsonOutput := range []bool{false, true} {
				args := []string{"--l1=" + stack.l1Address, "--l3=" + stack.l3Address,
					"--node-config=" + stack.clients.nodeConfigPath, "exec"}
				if jsonOutput {
					args = append(args, "--json")
				}
				args = append(args, "--", "true")
				code, stdout, stderr := runPolishBinary(t, binary, args...)
				if code != exitUsage || len(stdout) != 0 {
					t.Errorf("json=%v exit=%d stdout=%s stderr=%s", jsonOutput, code, stdout, stderr)
				}
				if jsonOutput {
					var envelope contract.ErrorResponse
					if err := json.Unmarshal(stderr, &envelope); err != nil || envelope.Error.Code != contract.ErrorInvalidRequest || envelope.Error.Message != fixture.message || envelope.Error.Retryable {
						t.Errorf("JSON refusal=%s decode=%v", stderr, err)
					}
				} else if string(stderr) != "wefty: "+fixture.message+"\n" {
					t.Errorf("plain refusal=%q", stderr)
				}
			}
			page, err := stack.clients.listRuns(t.Context(), "", 10)
			if err != nil || len(page.Runs) != 0 {
				t.Fatalf("refusal created runs: %+v err=%v", page, err)
			}
		})
	}
}

func TestExecCheckoutGitFailuresFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, kind := range []string{"broken-git-directory", "missing-head-object", "unreadable-head-object", "git-missing"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "unreadable-head-object" && os.Geteuid() == 0 {
				t.Skip("root ignores the file mode this test relies on")
			}
			stack := newExecStack(t, execRealBinaryIdleLimit)
			dir := t.TempDir()
			switch kind {
			case "broken-git-directory":
				if err := os.Mkdir(filepath.Join(dir, ".git"), 0700); err != nil {
					t.Fatal(err)
				}
			case "missing-head-object", "unreadable-head-object":
				dir = execTestRepo(t)
				head := strings.TrimSpace(execTestGit(t, dir, "rev-parse", "HEAD"))
				object := filepath.Join(dir, ".git", "objects", head[:2], head[2:])
				if kind == "missing-head-object" {
					if err := os.Remove(object); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := os.Chmod(object, 0000); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = os.Chmod(object, 0600) })
					if _, err := os.ReadFile(object); !os.IsPermission(err) {
						t.Fatalf("fixture must refuse reads: %v", err)
					}
				}
			case "git-missing":
				t.Setenv("PATH", t.TempDir())
			}
			t.Chdir(dir)
			code, stdout, stderr := runPolishBinary(t, binary,
				"--l1="+stack.l1Address, "--l3="+stack.l3Address,
				"--node-config="+stack.clients.nodeConfigPath, "exec", "--json", "--", "true")
			var envelope contract.ErrorResponse
			if err := json.Unmarshal(stderr, &envelope); err != nil || code != exitFailure || len(stdout) != 0 || envelope.Error.Code != contract.ErrorInternal || !strings.Contains(envelope.Error.Message, "git") {
				t.Fatalf("exit=%d stdout=%s stderr=%s decode=%v", code, stdout, stderr, err)
			}
		})
	}
}
