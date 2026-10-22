package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestFleetDoctorPolicyFlagRealBinary(t *testing.T) {
	binary := buildFleetFixtureBinary(t)
	home := newFleetTestHome(t)
	for _, args := range [][]string{{"doctor", "--print-policy"}, {"node", "doctor", "--print-policy"}, {"--json", "doctor", "--print-policy"}} {
		cmd := exec.CommandContext(t.Context(), binary, args...)
		cmd.Env = []string{"HOME=" + home, "PATH=/usr/bin:/bin"}
		var out, diagnostics bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &diagnostics
		if err := cmd.Run(); err != nil {
			t.Fatalf("%v: %v stdout=%s stderr=%s", args, err, &out, &diagnostics)
		}
		if !strings.Contains(out.String(), "Keep the Owner person identity on the owner's laptop.") ||
			!strings.Contains(out.String(), "Do not sign the control-plane machine's network app") ||
			strings.Contains(out.String(), "until L1 pins the person device") {
			t.Fatalf("obsolete person-device policy guidance: %s", &out)
		}
		if strings.Contains(strings.Join(args, " "), "--json") {
			var report struct {
				Snippet      string   `json:"snippet"`
				Requirements []string `json:"requirements"`
			}
			if err := json.Unmarshal(out.Bytes(), &report); err != nil || report.Snippet == "" || len(report.Requirements) != 5 {
				t.Fatalf("policy JSON=%s err=%v", &out, err)
			}
		} else if !strings.Contains(out.String(), "Private network policy") || !strings.Contains(out.String(), "tagOwners") {
			t.Fatalf("policy text=%s", &out)
		}
	}
}

func TestFleetDoctorReportsAndExitsRealBinary(t *testing.T) {
	binary := buildFleetFixtureBinary(t)
	home := newFleetTestHome(t)
	tools := filepath.Join(home, "tools")
	if err := os.MkdirAll(tools, 0700); err != nil {
		t.Fatal(err)
	}
	for _, tool := range []string{"go", "git", "node", "gh", "claude", "codex"} {
		if err := os.WriteFile(filepath.Join(tools, tool), []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	call := func(env []string, input string, args ...string) (int, string, string) {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), binary, args...)
		cmd.Env = append([]string{"HOME=" + home, "PATH=/usr/bin:/bin", "WEFTY_FIXTURE_LOGIN_PATH=" + tools}, env...)
		cmd.Stdin = strings.NewReader(input)
		var out, diagnostics bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &diagnostics
		err := cmd.Run()
		code := 0
		if err != nil {
			exit, ok := err.(*exec.ExitError)
			if !ok {
				t.Fatal(err)
			}
			code = exit.ExitCode()
		}
		return code, out.String(), diagnostics.String()
	}
	code, out, diagnostics := call(nil, "single-use-l1\nsingle-use-l3\nsingle-use-agent\nsingle-use-client\n", "--json", "setup", "--role", "control-plane", "--node-id", "studio-0123abcd", "--skip-smoke-run")
	if code != 0 {
		t.Fatalf("setup: %d %s %s", code, out, diagnostics)
	}
	var setup map[string]any
	if json.Unmarshal([]byte(out), &setup) != nil || setup["private_network_policy"] == nil {
		t.Fatalf("setup did not publish policy: %s", out)
	}
	code, out, diagnostics = call(nil, "", "setup", "--skip-smoke-run")
	if code != 0 || !strings.Contains(out, "Private network policy\n") || !strings.Contains(out, "tagOwners") {
		t.Fatalf("setup text policy: %d %s %s", code, out, diagnostics)
	}
	// Host advice must not change a healthy real-binary verdict.
	for _, tc := range []struct {
		env    []string
		want   int
		detail string
	}{
		{nil, 0, "private network policy"},
		{[]string{"WEFTY_FIXTURE_PEER_NOT_READY=1"}, 12, "FAILED"},
		{[]string{"WEFTY_FIXTURE_PEER_UNAVAILABLE=1"}, 0, peerProbeUnavailable},
		{[]string{"WEFTY_FIXTURE_FILEVAULT_ON=1"}, 0, "FileVault is on"},
		{[]string{"WEFTY_FIXTURE_IDENTITY_REFUSED=1"}, 3, "REFUSED"},
		{[]string{"WEFTY_FIXTURE_NOT_READY=1"}, 13, "NOT-RUN"},
		{[]string{"WEFTY_FIXTURE_NOT_ADMITTED=1"}, 12, "not alive and admitted"},
	} {
		for _, jsonMode := range []bool{false, true} {
			args := []string{"doctor"}
			if jsonMode {
				args = append([]string{"--json"}, args...)
			}
			code, out, diagnostics = call(tc.env, "", args...)
			if code != tc.want {
				t.Fatalf("doctor exit=%d want=%d stdout=%s stderr=%s", code, tc.want, out, diagnostics)
			}
			if jsonMode {
				var report plainDoctorReport
				if err := json.Unmarshal([]byte(out), &report); err != nil || report.Install != "fleet-process" || report.Role != "control-plane" || len(report.PolicyEvidence) != 9 || diagnostics != "" {
					t.Fatalf("doctor JSON report: %s stderr=%s err=%v", out, diagnostics, err)
				}
				if tc.detail == peerProbeUnavailable {
					assertFleetPeerChecks(t, report.PolicyEvidence, "NOT-RUN")
				} else if tc.want == 0 {
					assertFleetPeerChecks(t, report.PolicyEvidence, "VERIFIED")
				} else if strings.Contains(strings.Join(tc.env, " "), "WEFTY_FIXTURE_PEER_NOT_READY") {
					assertFleetPeerChecks(t, report.PolicyEvidence, "FAILED")
				}
				if tc.want == 0 && !report.Healthy {
					t.Fatalf("advisory host warning made doctor unhealthy: %s", out)
				}
				for _, e := range report.PolicyEvidence {
					if strings.Contains(e.Check, "denied") && e.Status != "NOT-RUN" {
						t.Fatal("deny proof overclaimed")
					}
				}
			} else if !strings.Contains(out, tc.detail) {
				t.Fatalf("missing %s in %s", tc.detail, out)
			}
		}
	}
	for _, args := range [][]string{{"doctor", "--print-policy=bad"}, {"doctor", "extra"}, {"doctor", "--unknown"}} {
		code, _, _ = call(nil, "", args...)
		if code != 2 {
			t.Fatalf("usage %v exit=%d", args, code)
		}
	}
	code, out, diagnostics = call(nil, "", "doctor", "--help")
	if code != 0 || !strings.Contains(out, "print-policy") || diagnostics != "" {
		t.Fatalf("doctor help: %d %s %s", code, out, diagnostics)
	}
}
