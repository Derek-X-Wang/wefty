package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestUpgradeDoubleRunStopsNothing(t *testing.T) {
	f := newUpgradeFixture(t)
	if _, err := runUpgrade(t.Context(), f.host, true, io.Discard); err != nil {
		t.Fatal(err)
	}
	before := installedUpgradeFiles(t, f)
	f.launchd.commands = nil
	originalRunner := f.host.launchctl
	f.host.launchctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if args[0] == "bootstrap" && strings.Contains(args[len(args)-1], setupL3Label) {
			return f.launchd.run(ctx, name, args...)
		}
		return originalRunner(ctx, name, args...)
	}
	var out bytes.Buffer
	report, err := runUpgrade(t.Context(), f.host, false, &out)
	if err != nil {
		t.Fatalf("second run failed: %v", err)
	}
	if report.Version != f.commit || !strings.Contains(out.String(), "already at "+f.commit) {
		t.Fatalf("no no-op message: %+v %s", report, out.String())
	}
	if len(f.launchd.mutating()) != 0 {
		t.Fatalf("second run restarted services: %v", f.launchd.mutating())
	}
	after := installedUpgradeFiles(t, f)
	for name, value := range before {
		if after[name] != value {
			t.Fatalf("no-op changed %s", name)
		}
	}
}

func TestUpgradeUnchangedNodeAgentNeedsNoNewBoot(t *testing.T) {
	f := newUpgradeFixture(t)
	// Stage the exact future node binary/plist but leave the control services old.
	originalBuild := f.operations.build
	stage := t.TempDir()
	paths, _ := resolveSetupPaths(f.home)
	if err := originalBuild(t.Context(), localSkillSource(t), stage, f.commit, f.loginPATH+":"+paths.bin); err != nil {
		t.Fatal(err)
	}
	digest, err := executableDigest(filepath.Join(stage, "wefty-agent"))
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := os.ReadFile(filepath.Join(stage, "wefty-agent"))
	if err := os.WriteFile(filepath.Join(paths.bin, "wefty-agent"), payload, 0755); err != nil {
		t.Fatal(err)
	}
	digests := map[string]string{}
	for _, name := range setupBinaries {
		digests[name], _ = executableDigest(filepath.Join(paths.bin, name))
	}
	digests["wefty-agent"] = digest
	services, err := setupServices(paths, f.profile(t), digests, f.loginPATH+":"+paths.bin)
	if err != nil {
		t.Fatal(err)
	}
	plist, err := launchagent.Render(services[2])
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(paths.launchAgents, setupNodeAgentLabel+".plist"), plist, 0644); err != nil {
		t.Fatal(err)
	}
	// The fixture's extra old-binary assertion is only for actual replacements.
	originalRunner := f.host.launchctl
	f.host.launchctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if args[0] == "bootstrap" && strings.Contains(args[len(args)-1], setupL3Label) {
			f.events = append(f.events, "restart:"+setupL3Label)
			f.now = f.now.Add(50 * time.Millisecond)
			f.restarted = true
			return f.launchd.run(ctx, name, args...)
		}
		return originalRunner(ctx, name, args...)
	}
	if _, err := runUpgrade(t.Context(), f.host, true, io.Discard); err != nil {
		t.Fatalf("unchanged node agent was awaited: %v", err)
	}
	for _, cmd := range f.launchd.mutating() {
		if strings.Contains(strings.Join(cmd, " "), setupNodeAgentLabel) {
			t.Fatalf("node agent changed: %v", cmd)
		}
	}
}

func localSkillSource(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	folder := filepath.Join(root, "skills", "wefty")
	if err := os.MkdirAll(folder, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(folder, "SKILL.md"), []byte("upgraded skill"), 0600); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestUpgradeRecoveryPreservesNodeAndDoesNotPublishVersion(t *testing.T) {
	for _, check := range []string{"forward-recovery", "keep-node", "version", "rerun"} {
		t.Run(check, func(t *testing.T) {
			f := newUpgradeFixture(t)
			f.failure = "schema-l1"
			_, err := runUpgrade(t.Context(), f.host, true, io.Discard)
			if err == nil {
				t.Fatal("expected activation failure")
			}
			paths, _ := resolveSetupPaths(f.home)
			switch check {
			case "forward-recovery":
				if !strings.Contains(err.Error(), "~/.local/bin/wefty setup") {
					t.Fatalf("missing forward recovery: %v", err)
				}
			case "keep-node":
				if !f.launchd.loaded[setupNodeAgentLabel] || f.launchd.loaded[setupL1Label] || f.launchd.loaded[setupL3Label] {
					t.Fatalf("wrong quiesced services: %v", f.launchd.loaded)
				}
			case "version":
				if got := installedStackVersion(paths.nodeConfig); got == f.commit {
					t.Fatalf("failed activation published %s", got)
				}
			case "rerun":
				f.operations.observe = func(context.Context, connprofile.Profile) (l1.Node, error) {
					return l1.Node{}, errors.New("connection refused")
				}
				_, err = runUpgrade(t.Context(), f.host, true, io.Discard)
				if err == nil || !strings.Contains(err.Error(), "L1 is down") || !strings.Contains(err.Error(), "~/.local/bin/wefty setup") {
					t.Fatalf("rerun dead end: %v", err)
				}
			}
		})
	}
}

func TestUpgradeRefusedRollbackAfterNodeStageUnloadsNode(t *testing.T) {
	f := newUpgradeFixture(t)
	f.failure = "replacement"
	observe := f.operations.observe
	changed := false
	f.operations.observe = func(ctx context.Context, p connprofile.Profile) (l1.Node, error) {
		if f.replaced && !changed {
			changed = true
			if _, err := f.databases[0].Exec("CREATE TABLE stage2_format(id INTEGER)"); err != nil {
				t.Fatal(err)
			}
		}
		return observe(ctx, p)
	}
	_, err := runUpgrade(t.Context(), f.host, true, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "~/.local/bin/wefty setup") {
		t.Fatalf("missing recovery after stage2: %v", err)
	}
	for _, label := range []string{setupL1Label, setupL3Label, setupNodeAgentLabel} {
		if f.launchd.loaded[label] {
			t.Fatalf("stage2 writer still loaded: %s", label)
		}
	}
	paths, _ := resolveSetupPaths(f.home)
	if installedStackVersion(paths.nodeConfig) == f.commit {
		t.Fatal("failed stage2 published completed version")
	}
}

func TestUpgradeBuildUsesHostEnvironment(t *testing.T) {
	t.Setenv("GOOS", "plan9")
	t.Setenv("GOARCH", "386")
	t.Setenv("GOFLAGS", "-race")
	t.Setenv("CGO_ENABLED", "1")
	f := newUpgradeFixture(t)
	tools := t.TempDir()
	goPath := filepath.Join(tools, "go")
	if err := os.WriteFile(goPath, []byte("#!/bin/sh\nexit 99\n"), 0755); err != nil {
		t.Fatal(err)
	}
	paths, _ := resolveSetupPaths(f.home)
	plist := strings.ReplaceAll(f.plist(t, setupNodeAgentLabel), f.loginPATH, tools+":"+f.loginPATH)
	if err := os.WriteFile(filepath.Join(paths.launchAgents, setupNodeAgentLabel+".plist"), []byte(plist), 0644); err != nil {
		t.Fatal(err)
	}
	orig := f.operations.command
	called := false
	f.operations.build = nil
	f.operations.command = func(ctx context.Context, dir string, env []string, name string, args ...string) ([]byte, error) {
		if name == goPath {
			called = true
			values := map[string]string{}
			for _, v := range env {
				p := strings.SplitN(v, "=", 2)
				values[p[0]] = p[1]
			}
			for key, want := range map[string]string{"GOOS": runtime.GOOS, "GOARCH": runtime.GOARCH, "GOFLAGS": "", "CGO_ENABLED": "0"} {
				if values[key] != want {
					t.Errorf("%s=%q want %q", key, values[key], want)
				}
			}
			return nil, errors.New("stop after inspecting build")
		}
		return orig(ctx, dir, env, name, args...)
	}
	_, _ = runUpgrade(t.Context(), f.host, true, io.Discard)
	if !called {
		t.Fatal("build not reached")
	}
}

func TestUpgradeExclusiveSetupAndUpgrade(t *testing.T) {
	f := newUpgradeFixture(t)
	original := f.operations.build
	inside := false
	f.operations.build = func(ctx context.Context, src, dst, commit, path string) error {
		if !inside {
			inside = true
			if _, err := runSetup(ctx, f.host, setupRequest{}); err == nil || !strings.Contains(err.Error(), "another setup or upgrade is in progress") {
				t.Errorf("overlapping setup: %v", err)
			}
			if _, err := runSetupRemove(ctx, f.host, true); err == nil || !strings.Contains(err.Error(), "another setup or upgrade is in progress") {
				t.Errorf("overlapping removal: %v", err)
			}
			if _, err := runUpgrade(ctx, f.host, true, io.Discard); err == nil || !strings.Contains(err.Error(), "another setup or upgrade is in progress") {
				t.Errorf("overlapping upgrade: %v", err)
			}
		}
		return original(ctx, src, dst, commit, path)
	}
	if _, err := runUpgrade(t.Context(), f.host, true, io.Discard); err != nil {
		t.Fatal(err)
	}
}

func TestUpgradeCleansStaleStageAndRepairsOwnedClone(t *testing.T) {
	for _, invalid := range []string{"valid", "partial", "wrong-repository"} {
		t.Run(invalid, func(t *testing.T) {
			f := newUpgradeFixture(t)
			paths, _ := resolveSetupPaths(f.home)
			stale := filepath.Join(paths.data, ".upgrade-abandoned")
			if err := os.Mkdir(stale, 0700); err != nil {
				t.Fatal(err)
			}
			clone := filepath.Join(paths.data, upgradeSourceName)
			if invalid != "valid" {
				receiptPath := filepath.Join(paths.config, setupManifestName)
				payload, err := os.ReadFile(receiptPath)
				if err != nil {
					t.Fatal(err)
				}
				var receipt setupManifest
				if err := json.Unmarshal(payload, &receipt); err != nil {
					t.Fatal(err)
				}
				receipt.UpgradeSource = true
				payload, err = json.Marshal(receipt)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(receiptPath, payload, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(clone, 0700); err != nil {
					t.Fatal(err)
				}
			}
			if invalid == "wrong-repository" {
				if err := os.WriteFile(filepath.Join(clone, "HEAD"), []byte("garbage"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			var out bytes.Buffer
			if _, err := runUpgrade(t.Context(), f.host, true, &out); err != nil {
				t.Fatal(err)
			}
			if invalid != "valid" && !strings.Contains(out.String(), "re-cloning") {
				t.Fatalf("missing clone repair message: %s", out.String())
			}
			assertNoUpgradeStage(t, f)
		})
	}
}

func TestUpgradeJSONReportIncludesWorkAndRenewal(t *testing.T) {
	f := newUpgradeFixture(t)
	f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "long-job", AttemptID: "long-attempt", LeaseExpiresAt: f.now.Add(30 * time.Second)}}
	for _, yes := range []bool{false, true} {
		report, err := runUpgrade(t.Context(), f.host, yes, io.Discard)
		if yes && err != nil {
			t.Fatal(err)
		}
		data, e := json.Marshal(report)
		if e != nil {
			t.Fatal(e)
		}
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(data, &fields)
		if !bytes.Contains(fields["running_work"], []byte("long-job")) {
			t.Fatalf("JSON omitted running work: %s", data)
		}
		if yes && string(fields["lease_renewal_ns"]) != "200000000" {
			t.Fatalf("JSON omitted renewal: %s", data)
		}
	}
}

func TestUpgradePreservesOwnerSkillAndReceiptDigest(t *testing.T) {
	for _, failure := range []string{"", "schema-l1", "replacement"} {
		t.Run("failure="+failure, func(t *testing.T) {
			f := newUpgradeFixture(t)
			f.failure = failure
			paths, _ := resolveSetupPaths(f.home)
			skill := filepath.Join(paths.skillTargets[0], "SKILL.md")
			receiptPath := filepath.Join(paths.config, setupManifestName)
			data, _ := os.ReadFile(receiptPath)
			var before setupManifest
			_ = json.Unmarshal(data, &before)
			edited := []byte("owner's custom skill\n")
			if err := os.WriteFile(filepath.Join(f.home, skill), edited, 0644); err != nil {
				t.Fatal(err)
			}
			report, err := runUpgrade(t.Context(), f.host, true, io.Discard)
			if failure == "" && err != nil {
				t.Fatal(err)
			}
			got, _ := os.ReadFile(filepath.Join(f.home, skill))
			if !bytes.Equal(got, edited) {
				t.Fatalf("owner's skill overwritten: %q", got)
			}
			data, _ = os.ReadFile(receiptPath)
			var after setupManifest
			_ = json.Unmarshal(data, &after)
			if after.Files[skill] != before.Files[skill] {
				t.Fatalf("receipt adopted owner edit: %s", after.Files[skill])
			}
			if failure == "" && !strings.Contains(strings.Join(report.Setup.Notes, " "), "edited") {
				t.Fatalf("no warning: %+v", report)
			}
		})
	}
}

var homeBeforeTestMain = os.Getenv("HOME")

func TestPackageTestsKeepTheBuildHome(t *testing.T) {
	if os.Getenv("HOME") != homeBeforeTestMain {
		t.Fatalf("package-wide HOME override changes Go build caches: got %q want %q", os.Getenv("HOME"), homeBeforeTestMain)
	}
}

func TestUpgradeSetupAlsoLocksUpgrade(t *testing.T) {
	f := newUpgradeFixture(t)
	original := f.host.loginShell
	checked := false
	f.host.loginShell = func(ctx context.Context, shell string, args ...string) ([]byte, error) {
		if !checked {
			checked = true
			if _, err := runUpgrade(ctx, f.host, true, io.Discard); err == nil || !strings.Contains(err.Error(), "another setup or upgrade is in progress") {
				t.Errorf("upgrade overlapped setup: %v", err)
			}
		}
		return original(ctx, shell, args...)
	}
	if _, err := runSetup(t.Context(), f.host, setupRequest{}); err != nil {
		t.Fatal(err)
	}
}
