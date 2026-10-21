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

	"github.com/Derek-X-Wang/wefty/l1"
)

func TestSetupConvertPlainFlagRequiresJoinFromRealBinary(t *testing.T) {
	binary := buildFleetFixtureBinary(t)
	for _, args := range [][]string{
		{"--json", "setup", "--convert-plain"},
		{"setup", "--role", "control-plane", "--convert-plain"},
	} {
		cmd := exec.CommandContext(t.Context(), binary, args...)
		cmd.Env = []string{"HOME=" + newFleetTestHome(t), "PATH=/usr/bin:/bin"}
		var out, diagnostics bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &diagnostics
		err := cmd.Run()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 2 || out.Len() != 0 || !strings.Contains(diagnostics.String(), "--convert-plain requires --role join") {
			t.Fatalf("conversion flag did not enforce role: exit=%v stdout=%s stderr=%s", err, &out, &diagnostics)
		}
	}
}

func TestSetupConvertPlainReportAndExitsFromRealBinary(t *testing.T) {
	binary := buildFleetFixtureBinary(t)
	home := newFleetTestHome(t)
	t.Cleanup(func() {
		_ = filepath.WalkDir(home, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return os.Chmod(path, 0700)
			}
			return nil
		})
	})
	command := func(input string, env []string, args ...string) (int, string, string) {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), binary, args...)
		cmd.Env = append([]string{"HOME=" + home, "PATH=/usr/bin:/bin"}, env...)
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
	data := filepath.Join(home, setupFolders.data)
	if err := os.MkdirAll(filepath.Join(data, "setup-ready"), 0700); err != nil {
		t.Fatal(err)
	}
	if code, out, diagnostics := command("", nil, "setup", "--node-id", "studio-0123abcd"); code != 0 {
		t.Fatalf("plain fixture: %d %s %s", code, out, diagnostics)
	}
	for _, name := range []string{"l1.sqlite", "l1.sqlite-wal", "l1.sqlite-shm", "l3.sqlite", "l3.sqlite-wal", "l3.sqlite-shm"} {
		if err := os.WriteFile(filepath.Join(data, name), []byte("closed "+name), 0600); err != nil {
			t.Fatal(err)
		}
	}
	join := joinCodeForTest(t, "opaque-l1", "opaque-l3", "")
	for _, role := range []string{"join", "control-plane"} {
		if code, out, diagnostics := command("", nil, "--json", "setup", "--role", role, "--join-code", join); code != 1 || out != "" || !strings.Contains(diagnostics, "existing plain installation") {
			t.Fatalf("implicit role change: %d %s %s", code, out, diagnostics)
		}
	}
	for _, busy := range []string{"one-shot", "service", "computer"} {
		code, out, diagnostics := command("", []string{"WEFTY_FIXTURE_BUSY=" + busy}, "--json", "setup", "--role", "join", "--join-code", join, "--convert-plain")
		if code != 5 || out != "" || !strings.Contains(diagnostics, "retry later") {
			t.Fatalf("busy conversion: %d %s %s", code, out, diagnostics)
		}
		var node l1.Node
		b, err := os.ReadFile(filepath.Join(home, "fixture-drain.json"))
		if err != nil || json.Unmarshal(b, &node) != nil || !node.ClaimsEnabled {
			t.Fatal("binary refusal did not restore claims")
		}
	}
	keys := "single-use-node\nsingle-use-client\n"
	code, out, diagnostics := command(keys, []string{"WEFTY_FIXTURE_BAD_PEERS=1"}, "--json", "setup", "--role", "join", "--join-code", join, "--convert-plain")
	var report setupReport
	if code != 1 || json.Unmarshal([]byte(out), &report) != nil || report.History == nil || report.JoinResult != "incomplete" || !strings.Contains(diagnostics, "peer verification failed") {
		t.Fatalf("incomplete report: %d %s %s", code, out, diagnostics)
	}
	code, out, diagnostics = command("", nil, "--json", "setup", "--role", "join", "--join-code", "invalid-code", "--convert-plain")
	if code != 2 || json.Unmarshal([]byte(out), &report) != nil || report.History == nil || report.JoinResult != "incomplete" || !strings.Contains(out, "#901") {
		t.Fatalf("resume validation lost history report: %d %s %s", code, out, diagnostics)
	}
	code, out, diagnostics = command("", nil, "--json", "setup", "--role", "join", "--join-code", join, "--convert-plain")
	if code != 0 || json.Unmarshal([]byte(out), &report) != nil || report.JoinResult != "joined" || report.History == nil || !report.History.ReadOnly || len(report.Services) != 2 || !strings.Contains(out, "#901") {
		t.Fatalf("binary conversion: %d %s %s", code, out, diagnostics)
	}
	assertJoinArtifactsPrivate(t, home, out+diagnostics, report, "single-use-node", "single-use-client")
	code, out, diagnostics = command("", nil, "setup")
	if code != 0 || !strings.Contains(out, "Join result: joined") || !strings.Contains(out, "Read-only; never restored (ADR-0003)") || strings.Contains(diagnostics, "Single-use join key") {
		t.Fatalf("committed report: %d %s %s", code, out, diagnostics)
	}
}

func TestSetupConvertRound2AbandonFlagFromRealBinary(t *testing.T) {
	binary := buildFleetFixtureBinary(t)
	for _, args := range [][]string{{"setup", "--abandon-conversion"}, {"setup", "--remove", "--convert-plain"}} {
		command := exec.CommandContext(t.Context(), binary, args...)
		command.Env = []string{"HOME=" + newFleetTestHome(t), "PATH=/usr/bin:/bin"}
		out, err := command.CombinedOutput()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 2 || !strings.Contains(string(out), "--convert-plain") {
			t.Fatalf("flag usage: %s %v", out, err)
		}
	}
	f := newConversionFixture(t)
	r := f.request(t)
	r.afterFleetStep = func(step string) error {
		if step == "convert:inventory" {
			panic("fixture crash")
		}
		return nil
	}
	func() { defer func() { _ = recover() }(); _, _ = runSetup(t.Context(), f.host, r) }()
	command := exec.CommandContext(t.Context(), binary, "--json", "setup", "--role", "join", "--convert-plain", "--abandon-conversion")
	command.Env = []string{"HOME=" + f.home, "PATH=/usr/bin:/bin"}
	out, err := command.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "Conversion abandoned") || strings.Contains(string(out), "Single-use join key") {
		t.Fatalf("binary abandon: %s %v", out, err)
	}
	paths, _ := resolveSetupPaths(f.home)
	if _, err := os.Stat(filepath.Join(paths.config, plainConversionName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("binary abandon retained record")
	}
}
