package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
)

// advanceUpgradeMain adds one commit on top of from at ref in the fixture's
// bare remote and returns it.
func advanceUpgradeMain(t *testing.T, f *upgradeFixture, ref, from, skill string) string {
	t.Helper()
	stream := fmt.Sprintf("commit %s\ncommitter Fixture <fixture@example.invalid> 1700000001 +0000\ndata 23\nUpgrade fixture source\n\nfrom %s\nM 100644 inline skills/wefty/SKILL.md\ndata %d\n%s\n", ref, from, len(skill), skill)
	command := exec.Command("git", "--git-dir="+f.operations.remote, "fast-import", "--quiet", "--force")
	command.Stdin = strings.NewReader(stream)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("advance %s: %v %s", ref, err, output)
	}
	output, err := exec.Command("git", "--git-dir="+f.operations.remote, "rev-parse", ref).Output()
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(output))
}

func TestUpgradeCommitPinInstallsExactlyThatCommitOnMain(t *testing.T) {
	f := newUpgradeFixture(t)
	newer := advanceUpgradeMain(t, f, "refs/heads/main", f.commit, "upgraded skill from newer main\n")
	f.operations.pin = f.commit
	report, err := runUpgrade(t.Context(), f.host, true, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if report.Version != f.commit || report.Version == newer {
		t.Fatalf("pinned upgrade installed %s, want %s", report.Version, f.commit)
	}
	if receipt := readSetupReceipt(t, f.home); receipt.InstalledVersion != f.commit {
		t.Fatalf("receipt installed_version = %s, want %s", receipt.InstalledVersion, f.commit)
	}
	paths, _ := resolveSetupPaths(f.home)
	for _, target := range paths.skillTargets {
		skill, err := os.ReadFile(filepath.Join(f.home, target, "SKILL.md"))
		if err != nil || string(skill) != "upgraded skill from main\n" {
			t.Fatalf("skill is not from the pinned commit: %q %v", skill, err)
		}
	}
	assertNoUpgradeStage(t, f)
}

func TestUpgradeCommitPinRefusesCommitNotOnMain(t *testing.T) {
	cases := map[string]func(t *testing.T, f *upgradeFixture) string{
		"absent": func(*testing.T, *upgradeFixture) string { return strings.Repeat("0123456789", 4) },
		"other-branch": func(t *testing.T, f *upgradeFixture) string {
			return advanceUpgradeMain(t, f, "refs/heads/side", f.commit, "upgraded skill from a side branch\n")
		},
		// The owned clone already holds the commit from an earlier fetch, but a
		// rewritten main no longer reaches it.
		"dropped-from-main": func(t *testing.T, f *upgradeFixture) string {
			dropped := advanceUpgradeMain(t, f, "refs/heads/main", f.commit, "upgraded skill from dropped main\n")
			paths, _ := resolveSetupPaths(f.home)
			if _, err := fetchUpgradeSource(t.Context(), upgradeOperations(f.host), paths, t.TempDir(), f.loginPATH); err != nil {
				t.Fatal(err)
			}
			if output, err := exec.Command("git", "--git-dir="+f.operations.remote, "update-ref", "refs/heads/main", f.commit).CombinedOutput(); err != nil {
				t.Fatalf("rewrite main: %v %s", err, output)
			}
			return dropped
		},
	}
	for name, pinFor := range cases {
		t.Run(name, func(t *testing.T) {
			f := newUpgradeFixture(t)
			f.operations.pin = pinFor(t, f)
			before := installedUpgradeFiles(t, f)
			_, err := runUpgrade(t.Context(), f.host, true, io.Discard)
			var pin *upgradePinError
			if !errors.As(err, &pin) || pin.Commit != f.operations.pin || pin.Main != f.commit {
				t.Fatalf("expected not-on-main refusal: %v", err)
			}
			if slices.Contains(f.events, "build") || !reflect.DeepEqual(before, installedUpgradeFiles(t, f)) || len(f.launchd.mutating()) != 0 {
				t.Fatalf("refusal built or changed the installation: %v", f.events)
			}
			assertNoUpgradeStage(t, f)
			// main reports this decision directly: conflict exit and typed code.
			report := decideCommandReport(err)
			var stderr bytes.Buffer
			report.writeError(&stderr, true)
			var envelope contract.ErrorResponse
			if json.Unmarshal(stderr.Bytes(), &envelope) != nil || report.exitCode != exitConflict || envelope.Error.Code != upgradeErrorCommitNotOnMain || envelope.Error.Retryable || !strings.Contains(envelope.Error.Message, f.operations.pin) {
				t.Fatalf("report: exit %d %s", report.exitCode, stderr.String())
			}
		})
	}
}

// Fleet roles share the pinned fetch: the pin installs that commit, and a pin
// off main refuses before any build.
func TestFleetUpgradeCommitPin(t *testing.T) {
	f := newFleetUpgradeFixture(t, serviceplan.Join)
	advanceUpgradeMain(t, f, "refs/heads/main", f.commit, "upgraded skill from newer main\n")
	f.operations.pin = strings.Repeat("0123456789", 4)
	var pin *upgradePinError
	if _, err := runUpgrade(t.Context(), f.host, false, io.Discard); !errors.As(err, &pin) || slices.Contains(f.events, "built:wefty") {
		t.Fatalf("fleet pin off main: %v %v", err, f.events)
	}
	f.operations.pin = f.commit
	report, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	if err != nil || report.Version != f.commit {
		t.Fatalf("fleet pinned upgrade: %+v %v", report, err)
	}
}
