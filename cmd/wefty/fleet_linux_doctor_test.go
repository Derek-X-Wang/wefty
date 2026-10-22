package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
)

func TestFleetLinuxDoctorChecklistAndLingering(t *testing.T) {
	for _, role := range []string{"control-plane", "join"} {
		for _, linger := range []string{"yes", "no", "unknown", "failure"} {
			t.Run(role+"/"+linger, func(t *testing.T) {
				f, _, _, s := newLinuxFleetFixture(t, role)
				tools := filepath.Join(f.home, "tools")
				if err := os.MkdirAll(tools, 0700); err != nil {
					t.Fatal(err)
				}
				for _, tool := range []string{"go", "git", "node", "gh", "claude", "codex"} {
					if err := os.WriteFile(filepath.Join(tools, tool), []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
						t.Fatal(err)
					}
				}
				f.loginPATH = tools
				if _, err := runSetup(t.Context(), f.host, linuxSetupRequest(t, role)); err != nil {
					t.Fatal(err)
				}
				var probes [][]string
				f.host.doctorCommand = func(_ context.Context, name string, args ...string) ([]byte, error) {
					probes = append(probes, append([]string{name}, args...))
					if name != "/usr/bin/loginctl" || !slices.Equal(args, []string{"show-user", "owner", "-p", "Linger"}) {
						t.Fatalf("unexpected Linux host check: %s %v", name, args)
					}
					if linger == "failure" {
						return nil, errors.New("logind unavailable")
					}
					return []byte("Linger=" + linger + "\n"), nil
				}
				s.calls = nil
				paths, _ := resolvePlatformMaintenancePaths(f.home, "linux", "")
				release := holdDoctorParticipantLocks(t, f.profile(t))
				defer release()
				before := snapshotLinuxHome(t, f.home)
				report, err := runFleetDoctor(t.Context(), f.host, globalOptions{}, paths, serviceplan.Role(role))
				if err != nil || !report.Healthy {
					t.Fatalf("Linux doctor: %+v %v", report, err)
				}
				if len(probes) != 1 || len(s.mutations()) > 0 {
					t.Fatal("doctor changed services or host", probes, s.calls)
				}
				seen := map[string]plainDoctorFinding{}
				for _, finding := range report.Findings {
					seen[finding.Check] = finding
				}
				for _, name := range []string{"dev.wefty.node-agent.service", "dev.wefty.client-service.service", "connection-profile", "node-config", "node-agent-PATH", "participant:client", "participant:node-agent", "local-node", "client → L1", "client → L3"} {
					if seen[name].Severity != "ok" {
						t.Fatalf("missing or non-ok check %q: %+v", name, seen[name])
					}
				}
				if _, ok := seen["system-sleep"]; ok {
					t.Fatal("Mac sleep check on Linux")
				}
				if _, ok := seen["FileVault"]; ok {
					t.Fatal("FileVault check on Linux")
				}
				finding := seen["lingering"]
				switch linger {
				case "yes":
					if finding.Severity != "ok" || !strings.Contains(finding.Detail, "survive logout") {
						t.Fatal(finding)
					}
				case "no":
					if finding.Severity != "warning" || !strings.Contains(finding.Fix, "loginctl enable-linger 'owner'") || !strings.Contains(finding.Detail, "do not survive") {
						t.Fatal(finding)
					}
				default:
					if !strings.Contains(finding.Detail, "unverified") {
						t.Fatal(finding)
					}
				}
				if !equalFleetFiles(before, snapshotLinuxHome(t, f.home)) {
					t.Fatal("doctor changed files")
				}
				for _, policy := range report.PolicyEvidence[:5] {
					if policy.Status != "VERIFIED" && policy.Check != "node agent → L1/L3" {
						t.Fatal(policy)
					}
				}
				var out bytes.Buffer
				handled, err := maybeExecuteFleetDoctor(t.Context(), f.host, globalOptions{jsonOutput: true}, &out)
				if !handled || err != nil || json.Unmarshal(out.Bytes(), new(plainDoctorReport)) != nil {
					t.Fatal("Linux fleet dispatch", handled, err, out.String())
				}
			})
		}
	}
}
func TestFleetLinuxDoctorServicesDownRetainsFileChecklist(t *testing.T) {
	f, _, _, s := newLinuxFleetFixture(t, "join")
	if _, err := runSetup(t.Context(), f.host, linuxSetupRequest(t, "join")); err != nil {
		t.Fatal(err)
	}
	s.running = map[string]bool{}
	paths, _ := resolvePlatformMaintenancePaths(f.home, "linux", "")
	report, err := runFleetDoctor(t.Context(), f.host, globalOptions{}, paths, serviceplan.Join)
	if err == nil || report.Healthy {
		t.Fatal("stopped units healthy")
	}
	for _, name := range []string{"connection-profile", "node-config", "participant:node-agent", "participant:client", "node-agent-PATH", "local-node"} {
		if !hasDoctorFinding(report, name) {
			t.Fatal("missing independent check", name)
		}
	}
	for _, finding := range report.Findings {
		if strings.HasPrefix(finding.Check, "dev.wefty.") && finding.Severity != "error" {
			t.Fatal(finding)
		}
	}
}
