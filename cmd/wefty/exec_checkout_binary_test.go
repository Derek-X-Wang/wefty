package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestExecCheckoutUsageFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, fixture := range []struct {
		name, message string
		unborn        bool
	}{
		{"outside-checkout", "wefty exec runs from inside a Git checkout; cd into the project first", false},
		{"unborn-head", "wefty exec needs at least one commit in this checkout; commit first", true},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			stack := newExecStack(t, execRealBinaryIdleLimit)
			dir := t.TempDir()
			if fixture.unborn {
				execTestGit(t, dir, "init", "-q")
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
