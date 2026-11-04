package main

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestExecIgnoredDependencyNoticeFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, jsonOutput := range []bool{false, true} {
		for _, fail := range []bool{false, true} {
			t.Run(fmt.Sprintf("json=%v/fail=%v", jsonOutput, fail), func(t *testing.T) {
				stack := newExecStack(t, execRealBinaryIdleLimit)
				repo := execTestRepo(t)
				writeExecFile(t, filepath.Join(repo, ".gitignore"), "node_modules/\n")
				writeExecFile(t, filepath.Join(repo, "node_modules/dependency"), "installed")
				t.Chdir(repo)
				args := []string{"--l1=" + stack.l1Address, "--l3=" + stack.l3Address, "--node-config=" + stack.clients.nodeConfigPath, "exec"}
				if jsonOutput {
					args = append(args, "--json")
				}
				command := "test ! -e node_modules || exit 42; printf dependency-test"
				wantCode, wantOutcome := 0, "pass"
				if fail {
					command += "; exit 1"
					wantCode, wantOutcome = exitRunFailed, "fail"
				}
				code, stdout, stderr := runPolishBinary(t, binary, append(args, "--", "/bin/sh", "-c", command)...)
				if code != wantCode || strings.Count(string(stderr), "wefty: ignored dependency folders") != 1 {
					t.Fatalf("exit=%d want=%d stdout=%s stderr=%s", code, wantCode, stdout, stderr)
				}
				if strings.Contains(string(stderr), "this checkout's node_modules") {
					t.Fatalf("failure repeated the dependency advisory: %s", stderr)
				}
				if jsonOutput {
					events := execModeEvents(t, string(stdout))
					finished := events[len(events)-1]
					if finished["outcome"] != wantOutcome {
						t.Fatalf("finished=%v stdout=%s", finished, stdout)
					}
				} else if string(stdout) != "dependency-test" {
					t.Fatalf("stdout=%q", stdout)
				}
				if strings.Contains(string(stdout), "ignored dependency folders") {
					t.Fatalf("notice leaked into stdout: %s", stdout)
				}
			})
		}
	}
}
