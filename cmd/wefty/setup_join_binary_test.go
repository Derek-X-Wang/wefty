package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestSetupJoinFlagsExitsAndReportsFromRealBinary(t *testing.T) {
	binary := buildFleetFixtureBinary(t)
	code := joinCodeForTest(t, "opaque-l1", "opaque-l3", "")
	keys := "single-use-node\nsingle-use-client\n"
	command := func(home, input string, env []string, args ...string) (int, string, string) {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), binary, args...)
		cmd.Env = append([]string{"HOME=" + home, "PATH=/usr/bin:/bin", "WEFTY_FIXTURE_LOGIN_URL=https://login.invalid/one-use"}, env...)
		cmd.Stdin = strings.NewReader(input)
		var out, diagnostics bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &diagnostics
		err := cmd.Run()
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return exit.ExitCode(), out.String(), diagnostics.String()
		}
		if err != nil {
			t.Fatal(err)
		}
		return 0, out.String(), diagnostics.String()
	}
	for _, person := range []bool{false, true} {
		home := newFleetTestHome(t)
		for _, args := range [][]string{{"--json", "setup", "--role", "join"}, {"setup", "--role", "join", "--join-code", "broken"}, {"setup", "--remove", "--join-code="}, {"setup", "--remove", "--with-person=false"}, {"setup", "--with-person"}} {
			exit, out, diagnostics := command(home, "", nil, args...)
			if exit != 2 || out != "" || strings.Contains(diagnostics, "Single-use join key") {
				t.Fatalf("usage %v: %d %s %s", args, exit, out, diagnostics)
			}
		}
		args := []string{"setup", "--role", "join", "--join-code", code, "--node-id", "studio-0123abcd", "--json"}
		if person {
			args = append(args, "--with-person")
		}
		exit, out, diagnostics := command(home, keys, nil, args...)
		if exit != 0 || strings.Count(diagnostics, "Single-use join key") != 2 {
			t.Fatalf("join: %d %s %s", exit, out, diagnostics)
		}
		var report setupReport
		if json.Unmarshal([]byte(out), &report) != nil || report.Role != "join" || len(report.Services) != 2 || len(report.Readiness) != 6 || len(report.Participants) != 2+boolInt(person) {
			t.Fatalf("report: %s", out)
		}
		assertJoinArtifactsPrivate(t, home, diagnostics, report, "single-use-node", "single-use-client", "https://login.invalid/one-use")
		exit, out, diagnostics = command(home, "", nil, "--json", "setup", "--skip-smoke-run")
		if exit != 0 || strings.Contains(diagnostics, "Single-use") || !strings.Contains(out, "skipped by --skip-smoke-run") {
			t.Fatalf("rerun: %d %s %s", exit, out, diagnostics)
		}
		exit, out, diagnostics = command(home, "", nil, "--json", "setup", "--role", "join", "--join-code", joinCodeForTest(t, "other-l1", "opaque-l3", ""))
		if exit != 1 || out != "" || strings.Contains(diagnostics, "Single-use") {
			t.Fatalf("code conflict: %d %s %s", exit, out, diagnostics)
		}
		exit, out, diagnostics = command(home, "", []string{"WEFTY_FIXTURE_NOT_ADMITTED=1"}, "--json", "setup")
		if exit != 12 || !strings.Contains(out, "set-claims") || !strings.Contains(out, `"FAILED"`) {
			t.Fatalf("admission: %d %s %s", exit, out, diagnostics)
		}
		if err := os.RemoveAll(filepath.Join(home, setupFolders.data, "fabric", "client")); err != nil {
			t.Fatal(err)
		}
		exit, out, diagnostics = command(home, "", nil, "--json", "setup")
		if exit != 3 || out != "" || !strings.Contains(diagnostics, "machine_identity_required") {
			t.Fatalf("lost state: %d %s %s", exit, out, diagnostics)
		}
	}
	home := newFleetTestHome(t)
	exit, out, diagnostics := command(home, keys, []string{"WEFTY_FIXTURE_BAD_PEERS=1"}, "--json", "setup", "--role", "join", "--join-code", code, "--node-id", "studio-0123abcd")
	if exit != 1 || out != "" || !strings.Contains(diagnostics, "l1:role_mismatch") {
		t.Fatalf("verification: %d %s %s", exit, out, diagnostics)
	}
	exit, out, diagnostics = command(home, "", nil, "--json", "setup", "--role", "join", "--join-code", code)
	if exit != 0 || strings.Contains(diagnostics, "Single-use") {
		t.Fatalf("verification repair: %d %s %s", exit, out, diagnostics)
	}
}
func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
