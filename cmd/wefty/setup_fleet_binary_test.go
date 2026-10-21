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

func TestSetupControlPlaneFlagAndReportFromRealBinary(t *testing.T) {
	binary := buildFleetFixtureBinary(t)
	home := newFleetTestHome(t)
	command := func(input string, extraEnv []string, args ...string) (int, string, string) {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), binary, args...)
		cmd.Env = append([]string{"HOME=" + home, "PATH=/usr/bin:/bin"}, extraEnv...)
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
	for _, flag := range []string{"--role=control-plane", "--role=", "--coordinator-url=https://coordinator.example.test", "--coordinator-url=", "--skip-smoke-run", "--skip-smoke-run=false"} {
		code, out, diagnostics := command("", nil, "setup", "--remove", flag)
		if code != 2 || !strings.Contains(diagnostics, "--remove cannot use --role, --coordinator-url or --skip-smoke-run") || out != "" {
			t.Errorf("remove with %s: %d %s %s", flag, code, out, diagnostics)
		}
	}
	if code, out, diagnostics := command("", nil, "setup", "unexpected"); code != 2 || !strings.Contains(diagnostics, "[--role control-plane]") || out != "" {
		t.Errorf("usage omitted role: %d %s %s", code, out, diagnostics)
	}
	keys := "single-use-control\nsingle-use-ledger\nsingle-use-agent\nsingle-use-client\n"
	if code, out, diagnostics := command(keys, []string{"TS_CONTROL_URL=https://other.example.test"}, "--control-url", "https://coordinator.example.test", "--json", "setup", "--role", "control-plane"); code != 1 || out != "" || !strings.Contains(diagnostics, "coordinator") || strings.Contains(diagnostics, "Single-use join key") {
		t.Fatalf("ambient coordinator conflict was not preflighted: %d %s %s", code, out, diagnostics)
	}
	code, out, diagnostics := command(keys, nil, "--json", "setup", "--role", "control-plane", "--node-id", "studio-0123abcd")
	if code != 0 || strings.Count(diagnostics, "Single-use join key") != 4 {
		t.Fatalf("setup exit=%d stdout=%s stderr=%s", code, out, diagnostics)
	}
	var report setupReport
	if err := json.Unmarshal([]byte(out), &report); err != nil || report.Role != "control-plane" || len(report.Readiness) != 9 {
		t.Fatalf("report shape: %s %v", out, err)
	}
	for _, key := range strings.Fields(keys) {
		if strings.Contains(out+diagnostics, key) {
			t.Fatal("secret printed by real CLI")
		}
	}
	code, out, diagnostics = command("", nil, "setup", "--skip-smoke-run")
	if code != 0 || strings.Contains(diagnostics, "join key") || !strings.Contains(out, "smoke run: NOT-RUN (skipped by --skip-smoke-run)") {
		t.Fatalf("completed rerun: %d %s %s", code, out, diagnostics)
	}
	code, out, diagnostics = command("", nil, "setup", "--role", "unknown")
	if code != 2 {
		t.Fatalf("unknown role exit=%d %s %s", code, out, diagnostics)
	}
	code, out, diagnostics = command("", []string{"WEFTY_FIXTURE_NOT_READY=1"}, "--json", "setup")
	if code != 12 || !strings.Contains(out, `"FAILED"`) || strings.Contains(diagnostics, `"error"`) {
		t.Fatalf("readiness exit/report=%d %s %s", code, out, diagnostics)
	}

	code, out, diagnostics = command("", nil, "--json", "setup", "--role", "join")
	if code != 1 || !strings.Contains(diagnostics, "recorded fleet role") || out != "" {
		t.Fatalf("role conflict exit=%d %s %s", code, out, diagnostics)
	}
	if err := os.RemoveAll(filepath.Join(home, setupFolders.data, "fabric", "client")); err != nil {
		t.Fatal(err)
	}
	code, out, diagnostics = command("", nil, "--json", "setup")
	if code != 3 || !strings.Contains(diagnostics, "machine_identity_required") || strings.Contains(diagnostics, "Single-use join key") || out != "" {
		t.Fatalf("missing saved state exit=%d %s %s", code, out, diagnostics)
	}
	if err := filepath.WalkDir(home, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, key := range strings.Fields(keys) {
				if bytes.Contains(b, []byte(key)) {
					t.Errorf("secret persisted at %s", path)
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func buildFleetFixtureBinary(t *testing.T) string {
	t.Helper()
	buildDir := t.TempDir()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	overlay := filepath.Join(buildDir, "overlay.json")
	payload, _ := json.Marshal(map[string]any{"Replace": map[string]string{filepath.Join(cwd, "setup_host.go"): filepath.Join(cwd, "testdata", "setup_host.go")}})
	if err := os.WriteFile(overlay, payload, 0600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(buildDir, "wefty")
	build := exec.Command("go", "build", "-overlay", overlay, "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build fixture binary: %v\n%s", err, output)
	}
	for _, name := range []string{"wefty-l1", "wefty-l3", "wefty-agent"} {
		if err := os.WriteFile(filepath.Join(buildDir, name), []byte("#!/bin/sh\necho dev\n"), 0755); err != nil {
			t.Fatal(err)
		}
	}

	return binary
}
