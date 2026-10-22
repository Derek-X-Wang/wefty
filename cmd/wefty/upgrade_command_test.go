package main

import (
	"bytes"
	"context"
	"database/sql"
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
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

type upgradeFixture struct {
	*setupFixture
	operations   *upgradeHost
	commit       string
	now          time.Time
	node         l1.Node
	restarted    bool
	replaced     bool
	events       []string
	databases    []*sql.DB
	failure      string
	observeCalls int
}

func newUpgradeFixture(t *testing.T) *upgradeFixture {
	t.Helper()
	f := &upgradeFixture{setupFixture: newSetupFixture(t), now: time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)}
	git, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	f.loginPATH = filepath.Dir(git) + ":/usr/bin:/bin"
	installed := f.setup(t, setupRequest{})
	f.node = readyNode(installed.NodeID)
	f.node.BootSessionID, f.node.AuthorityGeneration = "old-boot", 1
	f.node.LastHeartbeatAt = f.now.Add(-time.Second)
	for _, name := range []string{"l1.sqlite", "l3.sqlite"} {
		db, err := sql.Open("sqlite", filepath.Join(installed.DataDir, name))
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		if _, err := db.Exec("PRAGMA journal_mode=WAL; CREATE TABLE authority(fence INTEGER); INSERT INTO authority VALUES (41)"); err != nil {
			t.Fatal(err)
		}
		f.databases = append(f.databases, db)
		t.Cleanup(func() { db.Close() })
	}
	remote := t.TempDir()
	command := exec.Command(git, "init", "--bare", remote)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("local bare source: %v %s", err, output)
	}
	// A local source fixture; no network and no commits in the worker checkout.
	payload := "upgraded skill from main\n"
	stream := fmt.Sprintf("commit refs/heads/main\ncommitter Fixture <fixture@example.invalid> 1700000000 +0000\ndata 23\nUpgrade fixture source\n\nM 100644 inline skills/wefty/SKILL.md\ndata %d\n%s\n", len(payload), payload)
	command = exec.Command(git, "--git-dir="+remote, "fast-import", "--quiet")
	command.Stdin = strings.NewReader(stream)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("import local source: %v %s", err, output)
	}
	command = exec.Command(git, "--git-dir="+remote, "rev-parse", "main")
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	f.commit = strings.TrimSpace(string(output))
	f.operations = &upgradeHost{
		remote: remote,
		now:    func() time.Time { return f.now },
		wait: func(ctx context.Context, d time.Duration) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			f.now = f.now.Add(d)
			return nil
		},
		build: func(ctx context.Context, source, destination, commit, loginPATH string) error {
			f.events = append(f.events, "build")
			if f.failure == "build" {
				return errors.New("injected build failure")
			}
			if loginPATH != filepath.Dir(git)+":/usr/bin:/bin:"+filepath.Join(f.home, ".local", "bin") {
				t.Fatalf("build did not use recorded PATH: %q", loginPATH)
			}
			if skill, err := os.ReadFile(filepath.Join(source, "skills", "wefty", "SKILL.md")); err != nil || !bytes.Contains(skill, []byte("upgraded skill")) {
				t.Fatalf("build did not get fetched main: %s %v", skill, err)
			}
			for _, name := range setupBinaries {
				if err := os.WriteFile(filepath.Join(destination, name), []byte("#!/bin/sh\necho "+commit+"\n"), 0o755); err != nil {
					return err
				}
			}
			return nil
		},
	}
	f.operations.setClaims = func(ctx context.Context, p connprofile.Profile, request l1.NodeIntentRequest) (l1.Node, error) {
		if ctx.Err() != nil {
			return l1.Node{}, ctx.Err()
		}
		if !request.ClaimsEnabled && (!slices.Contains(f.events, "preflight:wefty-l1") || !slices.Contains(f.events, "preflight:wefty-l3")) {
			t.Fatal("claims disabled before successful build/version/state preflight")
		}
		if request.IntentRevision != f.node.IntentRevision {
			return l1.Node{}, &apiResponseError{APIError: contract.APIError{Code: contract.ErrorStaleIntentRevision, Message: "intent changed"}}
		}
		f.node.ClaimsEnabled = request.ClaimsEnabled
		f.node.IntentRevision++
		f.node.IntentReason = request.Reason
		return f.node, nil
	}
	f.operations.command = func(ctx context.Context, directory string, environment []string, name string, args ...string) ([]byte, error) {
		if slices.Contains(args, "fetch") && f.failure == "fetch" {
			return nil, errors.New("injected fetch failure")
		}
		if slices.Contains(args, "--version") {
			f.events = append(f.events, "version:"+filepath.Base(name))
			if f.failure == "version" {
				return []byte("wrong-commit\n"), nil
			}
		}
		if slices.Contains(args, "--state-preflight") {
			f.events = append(f.events, "preflight:"+filepath.Base(name))
			if f.failure == "preflight-"+filepath.Base(name) {
				return nil, errors.New("injected preflight failure")
			}
			path := strings.TrimPrefix(args[1], "--db=")
			if !strings.Contains(path, ".upgrade-") || filepath.Dir(path) == installed.DataDir {
				t.Fatalf("preflight received live DB: %s", path)
			}
			db, err := sql.Open("sqlite", path)
			if err != nil {
				return nil, err
			}
			defer db.Close()
			var fence int
			if err := db.QueryRow("SELECT fence FROM authority").Scan(&fence); err != nil || fence != 41 {
				t.Fatalf("preflight omitted WAL data: fence=%d %v", fence, err)
			}
			_, err = db.Exec("CREATE TABLE staged_migration (id INTEGER)")
			return nil, err
		}
		return runUpgradeCommand(ctx, directory, environment, name, args...)
	}
	f.operations.observe = func(ctx context.Context, _ connprofile.Profile) (l1.Node, error) {
		f.observeCalls++
		if ctx.Err() != nil {
			return l1.Node{}, ctx.Err()
		}
		node := f.node
		if f.restarted && f.failure != "renewal" && !strings.HasPrefix(f.failure, "schema-") && f.failure != "fingerprint" && f.failure != "stop" {
			node.LastHeartbeatAt = f.now
			node.ActiveAttempts = slices.Clone(node.ActiveAttempts)
			for index := range node.ActiveAttempts {
				node.ActiveAttempts[index].LeaseExpiresAt = f.now.Add(l1.DefaultLeaseDuration)
			}
		}
		if f.replaced {
			node.BootSessionID = "new-boot"
			node.AgentVersion = f.commit
			if f.failure == "replacement-version" {
				node.AgentVersion = "old-version"
			}
			node.LastHeartbeatAt = f.now
			if f.failure == "replacement" {
				node.BootSessionID = "old-boot"
			}
		}
		return node, nil
	}
	f.operations.readNode = func(ctx context.Context, p connprofile.Profile) (l1.Node, error) { return f.operations.observe(ctx, p) }
	f.host.upgrade = f.operations
	f.host.launchctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if args[0] == "bootstrap" {
			label := strings.TrimSuffix(filepath.Base(args[2]), ".plist")
			f.events = append(f.events, "restart:"+label)
			f.now = f.now.Add(50 * time.Millisecond)
			if label == setupL3Label {
				f.restarted = true
				old, err := os.ReadFile(filepath.Join(f.home, ".local", "bin", "wefty-agent"))
				if err != nil || !bytes.Contains(old, []byte("v1")) {
					t.Fatalf("node binary replaced before lease renewal: %q %v", old, err)
				}
				switch f.failure {
				case "schema-l1":
					_, err = f.databases[0].Exec("CREATE TABLE new_format(id INTEGER)")
				case "schema-l3":
					_, err = f.databases[1].Exec("CREATE TABLE new_format(id INTEGER)")
				case "schema-version":
					_, err = f.databases[0].Exec("PRAGMA user_version=2")
				case "fingerprint":
					err = os.Rename(filepath.Join(installed.DataDir, "l1.sqlite"), filepath.Join(installed.DataDir, "gone.sqlite"))
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if label == setupNodeAgentLabel {
				f.replaced = true
			}
		}
		if args[0] == "bootout" && f.restarted && f.failure == "stop" {
			return nil, errors.New("injected stop refusal")
		}
		return f.launchd.run(ctx, name, args...)
	}
	f.launchd.commands = nil
	return f
}

func installedUpgradeFiles(t *testing.T, f *upgradeFixture) map[string]string {
	t.Helper()
	paths, err := resolveSetupPaths(f.home)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := saveUpgradeInstall(paths, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := map[string]string{}
	for _, file := range saved {
		result[filepath.Join(file.folder, file.name)] = fmt.Sprintf("%o:%s", file.mode, file.payload)
	}
	return result
}

func assertNoUpgradeStage(t *testing.T, f *upgradeFixture) {
	t.Helper()
	paths, _ := resolveSetupPaths(f.home)
	matches, err := filepath.Glob(filepath.Join(paths.data, ".upgrade-*"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("staging left behind: %v %v", matches, err)
	}
}

func TestUpgradeConvergesInLeaseOrderAndRefreshesReceiptAndStatus(t *testing.T) {
	f := newUpgradeFixture(t)
	original := installedUpgradeFiles(t, f)
	f.loginPATH += "/refresh" // Setup captures the new login PATH; build still uses the recorded one.
	var out bytes.Buffer
	report, err := runUpgrade(t.Context(), f.host, true, &out)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"build", "version:wefty", "version:wefty-l1", "version:wefty-l3", "version:wefty-agent", "preflight:wefty-l1", "preflight:wefty-l3", "restart:" + setupL1Label, "restart:" + setupL3Label, "restart:" + setupNodeAgentLabel}
	if !reflect.DeepEqual(f.events, want) {
		t.Fatalf("activation order: %v", f.events)
	}
	if report.Version != f.commit || report.LeaseRenewal != 200*time.Millisecond || !strings.Contains(out.String(), "200ms (window "+upgradeRenewalWindow.String()+")") {
		t.Fatalf("renewal measurement: %+v %s", report, out.String())
	}
	if !f.replaced {
		t.Fatal("node agent not replaced")
	}
	paths, _ := resolveSetupPaths(f.home)
	var manifest setupManifest
	payload, err := os.ReadFile(filepath.Join(paths.config, setupManifestName))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(payload, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.InstalledVersion != f.commit || !manifest.UpgradeSource {
		t.Fatalf("receipt: %+v", manifest)
	}
	for name, digest := range manifest.Files {
		payload, err := os.ReadFile(filepath.Join(f.home, name))
		if err != nil || payloadDigest(payload) != digest {
			t.Fatalf("stale receipt for %s: %v", name, err)
		}
	}
	for _, target := range paths.skillTargets {
		skill, err := os.ReadFile(filepath.Join(f.home, target, "SKILL.md"))
		if err != nil || string(skill) != "upgraded skill from main\n" {
			t.Fatalf("skill not refreshed: %s %v", skill, err)
		}
	}
	savedPATH, err := recordedNodePATH([]byte(f.plist(t, setupNodeAgentLabel)))
	if err != nil || !strings.Contains(savedPATH, "/refresh") {
		t.Fatalf("PATH not refreshed: %q %v", savedPATH, err)
	}
	if reflect.DeepEqual(original, installedUpgradeFiles(t, f)) {
		t.Fatal("install unchanged")
	}
	// Exercise status's public response and text path using the upgraded receipt.
	h := newStatusHarness(t)
	h.nodes = l1.NodeList{Nodes: []l1.Node{readyNode(report.Setup.NodeID)}}
	clients := h.clients()
	clients.nodeConfigPath = paths.nodeConfig
	for _, jsonMode := range []bool{false, true} {
		var statusOut bytes.Buffer
		if err := executeStatus(t.Context(), clients, jsonMode, nil, &statusOut, io.Discard); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(statusOut.String(), f.commit) {
			t.Fatalf("status omitted installed commit: %s", statusOut.String())
		}
	}
	// Removal and purge consume the refreshed receipt, including the source clone.
	removed, err := runSetupRemove(t.Context(), f.host, true)
	if err != nil || len(removed.Warnings) != 0 {
		t.Fatalf("remove upgraded installation: %+v %v", removed, err)
	}
	if _, err := os.Stat(filepath.Join(paths.data, upgradeSourceName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("purge kept owned source clone: %v", err)
	}
	assertNoUpgradeStage(t, f)
}

func TestUpgradePreparationFailuresPreserveInstall(t *testing.T) {
	for _, failure := range []string{"fetch", "build", "version", "preflight-wefty-l1", "preflight-wefty-l3", "snapshot"} {
		t.Run(failure, func(t *testing.T) {
			f := newUpgradeFixture(t)
			f.failure = failure
			f.operations.setClaims = func(context.Context, connprofile.Profile, l1.NodeIntentRequest) (l1.Node, error) {
				t.Fatal("preparation failure mutated intent")
				return l1.Node{}, nil
			}
			before := installedUpgradeFiles(t, f)
			if failure == "snapshot" {
				paths, _ := resolveSetupPaths(f.home)
				if err := os.Rename(filepath.Join(paths.data, "l1.sqlite"), filepath.Join(paths.data, "missing.sqlite")); err != nil {
					t.Fatal(err)
				}
			}
			_, err := runUpgrade(t.Context(), f.host, true, io.Discard)
			phase := failure
			if strings.HasPrefix(failure, "preflight") {
				phase = "preflight"
			}
			if err == nil || !strings.Contains(err.Error(), phase) {
				t.Fatalf("expected %s refusal: %v", phase, err)
			}
			if !reflect.DeepEqual(before, installedUpgradeFiles(t, f)) || len(f.launchd.mutating()) != 0 {
				t.Fatal("preparation failure changed installed stack")
			}
			for _, label := range []string{setupL1Label, setupL3Label, setupNodeAgentLabel} {
				if !f.launchd.loaded[label] {
					t.Fatalf("old service stopped: %s", label)
				}
			}
			assertNoUpgradeStage(t, f)
		})
	}
}

func TestUpgradeMissingRenewalRollsBackOnlyUnchangedFormat(t *testing.T) {
	for _, failure := range []string{"renewal", "schema-l1", "schema-l3", "schema-version", "fingerprint", "stop", "replacement", "replacement-version"} {
		t.Run(failure, func(t *testing.T) {
			f := newUpgradeFixture(t)
			f.failure = failure
			// Rollback must retain all authority writes made since preflight.
			before := installedUpgradeFiles(t, f)
			originalObserve := f.operations.observe
			written := false
			f.operations.observe = func(ctx context.Context, p connprofile.Profile) (l1.Node, error) {
				if f.restarted && !written && failure != "fingerprint" {
					if _, err := f.databases[0].Exec("UPDATE authority SET fence=42"); err != nil {
						t.Fatal(err)
					}
					written = true
				}
				return originalObserve(ctx, p)
			}
			_, err := runUpgrade(t.Context(), f.host, true, io.Discard)
			if err == nil || !strings.Contains(err.Error(), "activation failed") {
				t.Fatalf("failure not reported: %v", err)
			}
			unchanged := failure == "renewal" || strings.HasPrefix(failure, "replacement")
			if unchanged {
				if !strings.Contains(err.Error(), "fingerprints unchanged") || !reflect.DeepEqual(before, installedUpgradeFiles(t, f)) {
					t.Fatalf("safe rollback omitted: %v", err)
				}
				for _, label := range []string{setupL1Label, setupL3Label, setupNodeAgentLabel} {
					if !f.launchd.loaded[label] {
						t.Fatalf("rollback did not reload %s", label)
					}
				}
			} else {
				if !strings.Contains(err.Error(), "not restored") && !strings.Contains(err.Error(), "rollback refused") {
					t.Fatalf("unsafe rollback not refused: %v", err)
				}
				if reflect.DeepEqual(before, installedUpgradeFiles(t, f)) {
					t.Fatal("old binaries restored after unknown or changed format")
				}
				if failure != "stop" {
					for _, label := range []string{setupL1Label, setupL3Label} {
						if f.launchd.loaded[label] {
							t.Fatalf("new database writer left loaded: %s", label)
						}
					}
					if !f.launchd.loaded[setupNodeAgentLabel] {
						t.Fatal("old node agent stopped before replacement began")
					}
				}
			}
			if failure != "fingerprint" {
				var fence int
				paths, _ := resolveSetupPaths(f.home)
				liveDB, err := sql.Open("sqlite", filepath.Join(paths.data, "l1.sqlite"))
				if err != nil {
					t.Fatal(err)
				}
				defer liveDB.Close()
				if err := liveDB.QueryRow("SELECT fence FROM authority").Scan(&fence); err != nil || fence != 42 {
					t.Fatalf("authority rewound: %d %v", fence, err)
				}
			}
			if !strings.HasPrefix(failure, "replacement") && failure != "stop" && len(f.events) > 0 {
				for _, event := range f.events {
					if event == "restart:"+setupNodeAgentLabel && !unchanged {
						t.Fatal("node agent replaced before successful renewal")
					}
				}
			}
			assertNoUpgradeStage(t, f)
		})
	}
}

func TestUpgradeRenewalRequiresHeartbeatAndAttemptLeaseAfterRestarts(t *testing.T) {
	for _, failure := range []string{"heartbeat", "lease", "boot", "generation", "slow-restart", "healthy"} {
		t.Run(failure, func(t *testing.T) {
			started := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
			now := started.Add(100 * time.Millisecond)
			restarted := now
			baseline := readyNode("this-node")
			baseline.BootSessionID = "boot"
			baseline.AuthorityGeneration = 1
			baseline.ActiveAttempts = []l1.NodeActiveAttempt{{AttemptID: "attempt", LeaseExpiresAt: started.Add(l1.DefaultLeaseDuration)}}
			node := baseline
			node.ActiveAttempts = slices.Clone(baseline.ActiveAttempts)
			operations := upgradeHost{now: func() time.Time { return now }, wait: func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil }, observe: func(_ context.Context, _ connprofile.Profile) (l1.Node, error) {
				node.LastHeartbeatAt = now
				node.ActiveAttempts[0].LeaseExpiresAt = now.Add(l1.DefaultLeaseDuration)
				switch failure {
				case "heartbeat":
					node.LastHeartbeatAt = started
				case "lease":
					node.ActiveAttempts[0].LeaseExpiresAt = baseline.ActiveAttempts[0].LeaseExpiresAt
				case "boot":
					node.BootSessionID = "other"
				case "generation":
					node.AuthorityGeneration = 2
				}
				return node, nil
			}}
			if failure == "slow-restart" {
				now = started.Add(upgradeRenewalWindow)
			}
			elapsed, err := waitUpgradeRenewal(t.Context(), operations, connprofile.Profile{}, baseline, started, restarted)
			if failure == "healthy" {
				if err != nil || elapsed != 200*time.Millisecond {
					t.Fatalf("healthy renewal rejected: %s %v", elapsed, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "within "+upgradeRenewalWindow.String()) {
				t.Fatalf("accepted stale authority or missing renewal: %s %v", elapsed, err)
			}
		})
	}
}

func TestUpgradeUsesRecordedGoAndCommitBuildFlags(t *testing.T) {
	f := newUpgradeFixture(t)
	toolRoot := t.TempDir()
	goPath := filepath.Join(toolRoot, "go")
	if err := os.WriteFile(goPath, []byte("#!/bin/sh\nexit 99\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	paths, _ := resolveSetupPaths(f.home)
	// Recorded PATH finds our owner's go first. The invoking process cannot choose it.
	for _, label := range []string{setupNodeAgentLabel} {
		payload := []byte(f.plist(t, label))
		payload = bytes.ReplaceAll(payload, []byte(f.loginPATH), []byte(toolRoot+":"+f.loginPATH))
		if err := os.WriteFile(filepath.Join(paths.launchAgents, label+".plist"), payload, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	original := f.operations.command
	fakeBuild := f.operations.build
	f.operations.build = nil
	called := false
	f.operations.command = func(ctx context.Context, directory string, environment []string, name string, args ...string) ([]byte, error) {
		if name == goPath {
			called = true
			if !slices.Contains(args, "-ldflags=-X main.version="+f.commit) || !slices.Contains(args, "./cmd/wefty-agent") || !slices.Contains(args, "./cmd/wefty-l3") || !slices.Contains(args, "./cmd/wefty-l1") || !slices.Contains(args, "./cmd/wefty") {
				t.Fatalf("wrong build flags: %v", args)
			}
			if !slices.Contains(environment, "PATH="+toolRoot+":"+f.loginPATH+":"+paths.bin) {
				t.Fatal("build did not receive the recorded PATH")
			}
			return nil, fakeBuild(ctx, directory, args[4], f.commit, f.loginPATH+":"+paths.bin)
		}
		return original(ctx, directory, environment, name, args...)
	}
	_, err := runUpgrade(t.Context(), f.host, true, io.Discard)
	if err != nil || !called {
		t.Fatalf("recorded go not used: %v called=%t", err, called)
	}
}

func TestUpgradeRefusesUnsupportedInstallBeforeAnyWork(t *testing.T) {
	for _, kind := range []string{"root", "platform", "source-link", "database-link", "identity", "shell"} {
		t.Run(kind, func(t *testing.T) {
			f := newUpgradeFixture(t)
			paths, _ := resolveSetupPaths(f.home)
			expected := map[string]string{"root": "never as root", "platform": "macOS and Linux fleet installs", "source-link": "real folder", "database-link": "snapshot", "identity": "matching connection profile", "shell": "login-shell PATH"}[kind]
			switch kind {
			case "root":
				f.host.euid = 0
			case "platform":
				f.host.goos = "freebsd"
			case "source-link":
				if err := os.Symlink(t.TempDir(), filepath.Join(paths.data, upgradeSourceName)); err != nil {
					t.Fatal(err)
				}
			case "database-link":
				target := filepath.Join(paths.data, "kept.sqlite")
				if err := os.Rename(filepath.Join(paths.data, "l1.sqlite"), target); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, filepath.Join(paths.data, "l1.sqlite")); err != nil {
					t.Fatal(err)
				}
			case "identity":
				profile := f.profile(t)
				profile.Identities.Node = "another-node"
				payload, err := connprofile.Encode(profile)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(paths.profile, payload, 0o600); err != nil {
					t.Fatal(err)
				}
			case "shell":
				f.host.loginShell = func(context.Context, string, ...string) ([]byte, error) { return nil, errors.New("shell failed") }
			}
			before := installedUpgradeFiles(t, f)
			_, err := runUpgrade(t.Context(), f.host, true, io.Discard)
			if err == nil || !strings.Contains(err.Error(), expected) {
				t.Fatalf("%s refusal: %v", kind, err)
			}
			if !reflect.DeepEqual(before, installedUpgradeFiles(t, f)) || len(f.launchd.mutating()) != 0 {
				t.Fatal("unsupported installation was changed")
			}
			assertNoUpgradeStage(t, f)
		})
	}
}

func TestUpgradeFetchesNewMainThroughOwnedClone(t *testing.T) {
	f := newUpgradeFixture(t)
	paths, _ := resolveSetupPaths(f.home)
	operations := upgradeOperations(f.host)
	first := t.TempDir()
	original, err := fetchUpgradeSource(t.Context(), operations, paths, first, f.loginPATH)
	if err != nil || original != f.commit {
		t.Fatalf("initial main: %s %v", original, err)
	}
	payload := "upgraded skill from next main\n"
	stream := fmt.Sprintf("commit refs/heads/main\ncommitter Fixture <fixture@example.invalid> 1700000001 +0000\ndata 23\nUpgrade fixture source\n\nfrom %s\nM 100644 inline skills/wefty/SKILL.md\ndata %d\n%s\n", original, len(payload), payload)
	command := exec.Command("git", "--git-dir="+f.operations.remote, "fast-import", "--quiet")
	command.Stdin = strings.NewReader(stream)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("advance local main: %v %s", err, output)
	}
	next := t.TempDir()
	commit, err := fetchUpgradeSource(t.Context(), operations, paths, next, f.loginPATH)
	if err != nil || commit == original {
		t.Fatalf("fetch did not advance main: %s %v", commit, err)
	}
	skill, err := os.ReadFile(filepath.Join(next, "skills", "wefty", "SKILL.md"))
	if err != nil || string(skill) != payload {
		t.Fatalf("source not pinned to latest main: %s %v", skill, err)
	}
	if info, err := os.Stat(filepath.Join(paths.data, upgradeSourceName)); err != nil || !info.IsDir() {
		t.Fatalf("missing owned clone: %v", err)
	}
}
