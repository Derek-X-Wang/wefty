//go:build unix

package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/internal/serviceready"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Reuse the fleet upgrade's local source/build/client seams, with the Linux
// setup filesystem and systemctl adapter. No installed services are accessed.
func newLinuxUpgradeFixture(t *testing.T, role serviceplan.Role) (*upgradeFixture, *fakeFleetSystemd) {
	t.Helper()
	f := newFleetUpgradeFixture(t, role)
	old, _ := resolveSetupPaths(f.home)
	f.host.goos, f.host.euid = "linux", os.Geteuid()
	f.host.linuxFiles, _ = linuxTestFiles(t)
	paths, issues := resolvePlatformMaintenancePaths(f.home, "linux", "")
	if len(issues) > 0 {
		t.Fatal(issues)
	}
	for _, db := range f.databases {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Dir(paths.data), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(old.data, paths.data); err != nil {
		t.Fatal(err)
	}
	f.databases = nil
	for _, name := range []string{"l1.sqlite", "l3.sqlite"} {
		db, err := sql.Open("sqlite", filepath.Join(paths.data, name))
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		f.databases = append(f.databases, db)
		t.Cleanup(func() { db.Close() })
	}
	for _, path := range []string{paths.logs, paths.launchAgents} {
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	p := f.profile(t)
	for r, part := range p.Participants {
		part.StateDirectory = filepath.Join(paths.data, "fabric", r)
		p.Participants[r] = part
	}
	payload, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.profile, payload, 0600); err != nil {
		t.Fatal(err)
	}
	var receipt setupManifest
	b, _ := os.ReadFile(filepath.Join(paths.config, setupManifestName))
	if err := json.Unmarshal(b, &receipt); err != nil {
		t.Fatal(err)
	}
	receipt.LinuxLogRoot = paths.logs
	receipt.Files[filepath.Join(paths.configRel, "profile.json")] = payloadDigest(payload)
	b, _ = json.Marshal(receipt)
	if err := os.WriteFile(filepath.Join(paths.config, setupManifestName), b, 0600); err != nil {
		t.Fatal(err)
	}
	record, err := readFleetSetupRecord(paths)
	if err != nil {
		t.Fatal(err)
	}
	record.LinuxLogRoot = paths.logs
	b, _ = json.Marshal(record)
	if err := os.WriteFile(filepath.Join(paths.config, fleetSetupRecordName), b, 0600); err != nil {
		t.Fatal(err)
	}
	plist, _ := os.ReadFile(filepath.Join(old.launchAgents, setupNodeAgentLabel+".plist"))
	loginPATH, err := recordedNodePATH(plist)
	if err != nil {
		t.Fatal(err)
	}
	digests := map[string]string{}
	for _, name := range setupBinaries {
		digests[name], err = executableDigest(filepath.Join(paths.bin, name))
		if err != nil {
			t.Fatal(err)
		}
	}
	services, err := setupRoleServices(paths, p, digests, loginPATH, role)
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeFleetSystemd{known: map[string]bool{}, running: map[string]bool{}, enabled: map[string]bool{}, unitFolder: paths.launchAgents}
	manager := fleetServiceManager{host: f.host}
	cached := map[string]string{}
	previous := ""
	for _, service := range services {
		payload, err := manager.render(service, previous)
		if err != nil {
			t.Fatal(err)
		}
		previous = service.Label
		unit := service.Label + ".service"
		if err := os.WriteFile(filepath.Join(paths.launchAgents, unit), payload, 0644); err != nil {
			t.Fatal(err)
		}
		s.known[unit], s.running[unit], s.enabled[unit], cached[unit] = true, true, true, string(payload)
	}
	f.host.launchctl = func(context.Context, string, ...string) ([]byte, error) {
		t.Error("Linux invoked launchctl")
		return nil, fmt.Errorf("unexpected launchctl")
	}
	s.beforeStart = func(unit string) error {
		b, err := os.ReadFile(filepath.Join(paths.launchAgents, unit))
		if err != nil {
			return err
		}
		if cached[unit] != string(b) {
			return fmt.Errorf("unit started without reloading its published definition: %s", unit)
		}
		label := strings.TrimSuffix(unit, ".service")
		f.events = append(f.events, "restart:"+label)
		f.now = f.now.Add(50 * time.Millisecond)
		if label == setupL3Label {
			f.restarted = true
			agent, _ := os.ReadFile(filepath.Join(paths.bin, "wefty-agent"))
			if !bytes.Contains(agent, []byte("v1")) && !f.replaced {
				t.Fatal("agent replaced before control-plane renewal")
			}
		}
		if label == setupNodeAgentLabel {
			f.replaced = true
		}
		return nil
	}
	f.host.systemctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if len(args) > 1 && args[1] == "daemon-reload" {
			for unit := range s.known {
				b, err := os.ReadFile(filepath.Join(paths.launchAgents, unit))
				if err != nil {
					return nil, err
				}
				cached[unit] = string(b)
			}
		}
		b, err := s.run(ctx, name, args...)
		if len(args) > 2 && args[1] == "show" && s.known[args[2]] {
			current, _ := os.ReadFile(filepath.Join(paths.launchAgents, args[2]))
			if cached[args[2]] != string(current) {
				b = bytes.ReplaceAll(b, []byte("NeedDaemonReload=no"), []byte("NeedDaemonReload=yes"))
			}
		}
		return b, err
	}
	f.events = nil
	return f, s
}

func TestLinuxFleetUpgradeRolePlan(t *testing.T) {
	for _, role := range []serviceplan.Role{serviceplan.Join, serviceplan.ControlPlane} {
		t.Run(string(role), func(t *testing.T) {
			f, s := newLinuxUpgradeFixture(t, role)
			paths, _ := resolvePlatformMaintenancePaths(f.home, "linux", "")
			// Existing Linux records pin logs even if the login environment changes.
			f.host.xdgStateHome = filepath.Join(t.TempDir(), "unused-state")
			if role == serviceplan.Join {
				for _, db := range f.databases {
					if err := db.Close(); err != nil {
						t.Fatal(err)
					}
				}
				for _, name := range []string{"wefty-l1", "wefty-l3"} {
					if err := os.Remove(filepath.Join(paths.bin, name)); err != nil {
						t.Fatal(err)
					}
				}
				for _, name := range []string{"l1.sqlite", "l3.sqlite"} {
					files, err := filepath.Glob(filepath.Join(paths.data, name+"*"))
					if err != nil {
						t.Fatal(err)
					}
					for _, path := range files {
						if err := os.Remove(path); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			stray := filepath.Join(paths.launchAgents, "dev.wefty.unrelated.service")
			if err := os.WriteFile(stray, []byte("owner unit\n"), 0644); err != nil {
				t.Fatal(err)
			}
			report, err := runUpgrade(t.Context(), f.host, false, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			if report.Version != f.commit || !f.node.ClaimsEnabled {
				t.Fatalf("incomplete upgrade: %+v", report)
			}
			want := []string{setupNodeAgentLabel, "dev.wefty.client-service"}
			built := []string{"built:wefty", "built:wefty-agent"}
			if role == serviceplan.ControlPlane {
				want = append([]string{setupL1Label, setupL3Label}, want...)
				built = []string{"built:wefty", "built:wefty-l1", "built:wefty-l3", "built:wefty-agent"}
			}
			var got, gotBuilt []string
			for _, event := range f.events {
				if strings.HasPrefix(event, "restart:") {
					got = append(got, strings.TrimPrefix(event, "restart:"))
				}
				if strings.HasPrefix(event, "built:") {
					gotBuilt = append(gotBuilt, event)
				}
			}
			if !slices.Equal(got, want) || !slices.Equal(gotBuilt, built) {
				t.Fatal("wrong restart/build plan", got, gotBuilt)
			}
			var mutations []string
			for _, c := range s.mutations() {
				mutations = append(mutations, strings.Join(c[2:], " "))
			}
			var expected []string
			for i, label := range want {
				unit := label + ".service"
				expected = append(expected, "stop "+unit, "daemon-reload", "enable "+unit, "start "+unit)
				service := report.Setup.Services[i]
				if service.Label != unit || service.Unit != filepath.Join(paths.launchAgents, unit) || service.Plist != "" {
					t.Fatal("wrong Linux service report", service)
				}
			}
			if !slices.Equal(mutations, expected) {
				t.Fatalf("systemctl plan: %v want %v", mutations, expected)
			}
			b, _ := os.ReadFile(filepath.Join(paths.config, setupManifestName))
			var receipt setupManifest
			if json.Unmarshal(b, &receipt) != nil || receipt.InstalledVersion != f.commit || receipt.LinuxLogRoot != paths.logs {
				t.Fatal("incomplete receipt", string(b))
			}
			if role == serviceplan.Join {
				for _, path := range []string{filepath.Join(paths.bin, "wefty-l1"), filepath.Join(paths.bin, "wefty-l3"), filepath.Join(paths.data, "l1.sqlite"), filepath.Join(paths.data, "l3.sqlite")} {
					if _, err := os.Stat(path); !os.IsNotExist(err) {
						t.Fatal("join created control-plane input", filepath.Base(path), err)
					}
				}
			}
			b, _ = os.ReadFile(stray)
			if string(b) != "owner unit\n" {
				t.Fatal("stray unit changed")
			}
			matches, _ := filepath.Glob(filepath.Join(paths.data, ".upgrade-*"))
			if len(matches) > 0 {
				t.Fatal("staging left behind", matches)
			}
		})
	}
}

func TestLinuxFleetUpgradeRollbackRestoresUnitsBeforeRestart(t *testing.T) {
	for _, role := range []serviceplan.Role{serviceplan.Join, serviceplan.ControlPlane} {
		t.Run(string(role), func(t *testing.T) {
			f, s := newLinuxUpgradeFixture(t, role)
			before := snapshotLinuxUpgradeHome(t, f.home)
			// Do not rely on NeedDaemonReload noticing a quickly restored definition.
			// The recovery path must explicitly reload after restoring the old bytes.
			run := f.host.systemctl
			f.host.systemctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
				b, err := run(ctx, name, args...)
				return bytes.ReplaceAll(b, []byte("NeedDaemonReload=yes"), []byte("NeedDaemonReload=no")), err
			}
			f.failure = "replacement"
			_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
			if err == nil || commandExitCode(err) != 1 || !strings.Contains(err.Error(), "old installation files restored") {
				t.Fatalf("rollback failed: %v", err)
			}
			after := snapshotLinuxUpgradeHome(t, f.home)
			for name, want := range before {
				if after[name] != want {
					t.Errorf("rollback did not restore %s", filepath.Base(name))
				}
			}
			if !f.node.ClaimsEnabled {
				t.Fatal("rollback did not restore claims")
			}
			var got []string
			for _, c := range s.calls {
				if c[2] == "start" {
					got = append(got, c[3])
				}
			}
			want := []string{setupNodeAgentLabel + ".service", "dev.wefty.client-service.service"}
			if role == serviceplan.ControlPlane {
				want = append([]string{setupL1Label + ".service", setupL3Label + ".service"}, want...)
			}
			if !slices.Equal(got, append(slices.Clone(want), want...)) {
				t.Fatal("wrong recovery restart order", got)
			}
			for _, unit := range want {
				if !s.running[unit] {
					t.Fatal("old service left stopped", unit)
				}
			}
		})
	}
}

func TestLinuxFleetUpgradeImmediateHeartbeat(t *testing.T) {
	for _, role := range []serviceplan.Role{serviceplan.Join, serviceplan.ControlPlane} {
		t.Run(string(role), func(t *testing.T) {
			f, s := newLinuxUpgradeFixture(t, role)
			paths, _ := resolvePlatformMaintenancePaths(f.home, "linux", "")
			readyDir := filepath.Join(paths.data, "setup-ready")
			if err := os.MkdirAll(readyDir, 0700); err != nil {
				t.Fatal(err)
			}
			f.host.bootTime = func() (time.Time, error) { return f.now.Add(-24 * time.Hour), nil }
			start := s.beforeStart
			s.beforeStart = func(unit string) error {
				if err := start(unit); err != nil {
					return err
				}
				if unit == setupNodeAgentLabel+".service" {
					payload, _ := json.Marshal(serviceready.Metadata{PID: 4242, ReadyAt: f.now, NodeID: f.node.NodeID, BootSessionID: "new-boot", ImmediateHeartbeat: true})
					return os.WriteFile(filepath.Join(readyDir, "node-agent.json"), payload, 0600)
				}
				return nil
			}
			signaled := false
			observe := f.operations.observe
			f.operations.observe = func(ctx context.Context, p connprofile.Profile) (l1.Node, error) {
				node, err := observe(ctx, p)
				if f.replaced && !signaled {
					node.LastHeartbeatAt = f.now.Add(-time.Hour)
				}
				return node, err
			}
			run := f.host.systemctl
			f.host.systemctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if len(args) > 1 && args[1] == "kill" {
					want := []string{"--user", "kill", "--kill-who=main", "--signal=SIGUSR1", setupNodeAgentLabel + ".service"}
					if name != "/usr/bin/systemctl" || !slices.Equal(args, want) {
						t.Fatalf("wrong heartbeat command: %s %v", name, args)
					}
					signaled = true
					return nil, nil
				}
				return run(ctx, name, args...)
			}
			_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
			if err != nil || !signaled {
				t.Fatalf("no immediate Linux heartbeat: signaled=%v err=%v", signaled, err)
			}
		})
	}
}

func TestLinuxFleetUpgradeRefusalsBeforePreparation(t *testing.T) {
	for _, kind := range []string{"staged", "conversion", "receipt-profile", "missing-systemctl", "unsafe-log-parent", "unit-drop-in", "external-unit-folder", "missing-log-record", "bus-down", "foreign-fragment"} {
		t.Run(kind, func(t *testing.T) {
			f, s := newLinuxUpgradeFixture(t, serviceplan.Join)
			paths, _ := resolvePlatformMaintenancePaths(f.home, "linux", "")
			want := ""
			drainWrites, interruptions := 0, 0
			f.operations.writeDrain = func(_ context.Context, config string, receipt *upgradeDrainReceipt) error {
				drainWrites++
				return writeUpgradeDrain(config, receipt)
			}
			if kind == "bus-down" || kind == "foreign-fragment" {
				attempt := l1.NodeActiveAttempt{JobID: "repeat", AttemptID: "old-repeat", BootSessionID: f.node.BootSessionID, Class: contract.JobClassOneShot, Restartable: true}
				f.node.ActiveAttempts = []l1.NodeActiveAttempt{attempt}
				settled := l1.Job{JobID: attempt.JobID, State: contract.JobQueued, Attempts: []l1.Attempt{{AttemptID: attempt.AttemptID, State: contract.AttemptInterrupted}}}
				f.operations.interruptAttempt = func(context.Context, connprofile.Profile, string, l1.InterruptRestartableAttemptRequest) (l1.Job, error) {
					interruptions++
					f.node.ActiveAttempts = nil
					return settled, nil
				}
				f.operations.readJob = func(context.Context, connprofile.Profile, string) (l1.Job, error) { return settled, nil }
			}
			switch kind {
			case "staged":
				record, err := readFleetSetupRecord(paths)
				if err != nil {
					t.Fatal(err)
				}
				record.Committed = false
				payload, _ := json.Marshal(record)
				if err := os.WriteFile(filepath.Join(paths.config, fleetSetupRecordName), payload, 0600); err != nil {
					t.Fatal(err)
				}
				want = "upgrade refuses staged fleet setup"
			case "conversion":
				// A private, incomplete conversion is never interpreted as upgrade authority.
				p := f.profile(t)
				p.Fabric = "plain"
				p.PlainFabricID = "plain-fixture"
				p.Participants = nil
				r := plainConversion{Version: 1, NodeID: p.Identities.Node, Phase: "prepared", Profile: p, History: plainHistory{Directory: filepath.Join(paths.data, plainHistoryFolder, p.Identities.Node), NeverRestore: true}}
				for _, label := range []string{setupL1Label, setupL3Label, setupNodeAgentLabel} {
					r.Services = append(r.Services, plainConversionService{Label: label, Plist: []byte("saved plist")})
				}
				if err := savePlainConversion(paths, &r); err != nil {
					t.Fatal(err)
				}
				want = "upgrade refuses a plain-to-join conversion"
			case "receipt-profile":
				b, err := os.ReadFile(paths.profile)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(paths.profile, append(b, '\n'), 0600); err != nil {
					t.Fatal(err)
				}
				want = "fleet profile that disagrees with its receipt"
			case "bus-down":
				run := f.host.systemctl
				f.host.systemctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
					if slices.Equal(args, []string{"--version"}) {
						return run(ctx, name, args...)
					}
					s.calls = append(s.calls, append([]string{name}, args...))
					return nil, fmt.Errorf("injected user bus unavailable")
				}
				want = "user service manager is unreachable from this shell"
			case "foreign-fragment":
				foreign := filepath.Join(f.home, "foreign-client.service")
				if err := os.WriteFile(foreign, []byte("foreign definition"), 0644); err != nil {
					t.Fatal(err)
				}
				run := f.host.systemctl
				f.host.systemctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
					b, err := run(ctx, name, args...)
					if len(args) > 2 && args[1] == "show" && args[2] == "dev.wefty.client-service.service" {
						b = bytes.ReplaceAll(b, []byte("FragmentPath="+filepath.Join(paths.launchAgents, args[2])), []byte("FragmentPath="+foreign))
					}
					return b, err
				}
				want = "unit definition is not the one setup wrote"
			case "missing-systemctl":
				files, mirror := linuxTestFiles(t)
				f.host.linuxFiles = files
				if err := os.Remove(filepath.Join(mirror, "usr/bin/systemctl")); err != nil {
					t.Fatal(err)
				}
				want = "systemctl not found at /usr/bin or /bin"
			case "unsafe-log-parent":
				if err := os.Chmod(filepath.Dir(paths.logs), 0777); err != nil {
					t.Fatal(err)
				}
				want = "Linux log parent is group- or world-writable"
			case "unit-drop-in":
				dir := filepath.Join(paths.launchAgents, setupNodeAgentLabel+".service.d")
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "override.conf"), []byte("[Service]\n"), 0600); err != nil {
					t.Fatal(err)
				}
				want = "foreign user unit fragment or unsafe drop-ins"
			case "external-unit-folder":
				if err := os.RemoveAll(paths.launchAgents); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(t.TempDir(), paths.launchAgents); err != nil {
					t.Fatal(err)
				}
				want = "outside the home folder"
			case "missing-log-record":
				record, err := readFleetSetupRecord(paths)
				if err != nil {
					t.Fatal(err)
				}
				record.LinuxLogRoot = ""
				b, _ := json.Marshal(record)
				if err := os.WriteFile(filepath.Join(paths.config, fleetSetupRecordName), b, 0600); err != nil {
					t.Fatal(err)
				}
				b, _ = os.ReadFile(filepath.Join(paths.config, setupManifestName))
				var receipt setupManifest
				if err := json.Unmarshal(b, &receipt); err != nil {
					t.Fatal(err)
				}
				receipt.LinuxLogRoot = ""
				b, _ = json.Marshal(receipt)
				if err := os.WriteFile(filepath.Join(paths.config, setupManifestName), b, 0600); err != nil {
					t.Fatal(err)
				}
				want = "existing Linux fleet records have no log root"
			}
			before := snapshotLinuxUpgradeHome(t, f.home)
			_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
			if err == nil || commandExitCode(err) != 1 || !strings.Contains(err.Error(), want) {
				t.Errorf("wrong refusal: %v want %q", err, want)
			}
			if len(f.events) > 0 || len(s.mutations()) > 0 || !f.node.ClaimsEnabled || drainWrites != 0 || interruptions != 0 {
				t.Errorf("refusal reached preparation or mutation: events=%v commands=%v drain_writes=%d interruptions=%d", f.events, s.mutations(), drainWrites, interruptions)
			}
			after := snapshotLinuxUpgradeHome(t, f.home)
			for name, digest := range before {
				if after[name] != digest {
					t.Errorf("refusal changed %s", filepath.Base(name))
				}
			}
			for name := range after {
				if (strings.HasPrefix(name, paths.bin+string(filepath.Separator)) || strings.HasPrefix(name, paths.launchAgents+string(filepath.Separator))) && before[name] == "" {
					t.Errorf("refusal created installed file %s", filepath.Base(name))
				}
			}
			if _, err := readUpgradeDrain(paths.config); !os.IsNotExist(err) {
				t.Errorf("refusal left a drain receipt: %v", err)
			}
		})
	}
}

func snapshotLinuxUpgradeHome(t *testing.T, home string) map[string]string {
	t.Helper()
	result := map[string]string{}
	err := filepath.WalkDir(home, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		var payload []byte
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			payload = []byte(target)
		} else {
			payload, err = os.ReadFile(path)
			if err != nil {
				return err
			}
		}
		result[path] = fmt.Sprintf("%o:%s", info.Mode(), payloadDigest(payload))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestLinuxFleetUpgradeSharedDrain(t *testing.T) {
	for _, role := range []serviceplan.Role{serviceplan.Join, serviceplan.ControlPlane} {
		t.Run(string(role), func(t *testing.T) {
			f, s := newLinuxUpgradeFixture(t, role)
			restart := l1.NodeActiveAttempt{JobID: "repeat", AttemptID: "old-repeat", BootSessionID: f.node.BootSessionID, Class: contract.JobClassOneShot, Restartable: true}
			ordinary := l1.NodeActiveAttempt{JobID: "finish", AttemptID: "old-finish", BootSessionID: f.node.BootSessionID, Class: contract.JobClassOneShot}
			f.node.ActiveAttempts = []l1.NodeActiveAttempt{restart, ordinary}
			gaps, waits, interrupts := 0, 0, 0
			settled := false
			wait := f.operations.wait
			f.operations.wait = func(ctx context.Context, d time.Duration) error {
				if d == upgradeDrainPoll {
					if f.node.ClaimsEnabled {
						gaps++
						if interrupts != 0 || f.replaced {
							t.Fatal("replacement/interruption before ordinary run finished")
						}
						f.node.ActiveAttempts = []l1.NodeActiveAttempt{restart}
					} else {
						waits++
						if interrupts != 1 {
							t.Fatal("drain waited before interrupting restartable Attempt")
						}
						settled = true
						f.node.ActiveAttempts = nil
					}
				}
				return wait(ctx, d)
			}
			f.operations.interruptAttempt = func(_ context.Context, _ connprofile.Profile, id string, r l1.InterruptRestartableAttemptRequest) (l1.Job, error) {
				if id != restart.AttemptID || r.JobID != restart.JobID || r.BootSessionID != restart.BootSessionID || r.IntentRevision != f.node.IntentRevision || r.IdempotencyKey == "" || r.Reason != l1.InterruptionUpgrade || f.node.ClaimsEnabled || gaps != 1 {
					t.Fatalf("wrong interruption: %+v", r)
				}
				interrupts++
				return l1.Job{State: contract.JobRunning}, nil
			}
			f.operations.readJob = func(_ context.Context, _ connprofile.Profile, id string) (l1.Job, error) {
				if id != restart.JobID {
					t.Fatal("ordinary run was interrupted", id)
				}
				state, jobState := contract.AttemptRunning, contract.JobRunning
				if settled {
					state, jobState = contract.AttemptInterrupted, contract.JobQueued
				}
				return l1.Job{JobID: id, State: jobState, Attempts: []l1.Attempt{{AttemptID: restart.AttemptID, State: state}}}, nil
			}
			start := s.beforeStart
			s.beforeStart = func(unit string) error {
				if !settled {
					t.Fatal("service restarted before exact-Attempt settlement")
				}
				return start(unit)
			}
			var out bytes.Buffer
			report, err := runUpgrade(t.Context(), f.host, false, &out)
			if err != nil {
				t.Fatal(err)
			}
			if gaps != 1 || waits != 1 || interrupts != 1 || report.RestartPending != 1 || report.WaitingNonrestartable != 0 || report.StoppingRestartable != 0 || len(report.RunningWork) != 2 || !f.node.ClaimsEnabled {
				t.Fatalf("wrong drain: gaps=%d waits=%d interrupts=%d report=%+v", gaps, waits, interrupts, report)
			}
			for _, want := range []string{"waiting_nonrestartable=1 stopping_restartable=0 restart_pending=0; claims remain open", "waiting_nonrestartable=0 stopping_restartable=1 restart_pending=0", "waiting_nonrestartable=0 stopping_restartable=0 restart_pending=1"} {
				if !strings.Contains(out.String(), want) {
					t.Fatal("missing shared progress shape", want, out.String())
				}
			}
			payload, _ := json.Marshal(report)
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(payload, &fields); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"waiting_nonrestartable", "stopping_restartable", "restart_pending", "running_work"} {
				if _, ok := fields[name]; !ok {
					t.Fatal("missing shared JSON field", name)
				}
			}
		})
	}
}

func TestLinuxFleetUpgradeInterruptedDrainRecovery(t *testing.T) {
	for _, operatorDisabled := range []bool{false, true} {
		t.Run(fmt.Sprint(operatorDisabled), func(t *testing.T) {
			f, s := newLinuxUpgradeFixture(t, serviceplan.Join)
			paths, _ := resolvePlatformMaintenancePaths(f.home, "linux", "")
			f.node.ClaimsEnabled = false
			f.node.IntentRevision = 3
			f.node.IntentReason = "wefty upgrade interrupted"
			receipt := &upgradeDrainReceipt{Version: 1, Home: f.home, NodeID: f.node.NodeID, Revision: 3, Reason: f.node.IntentReason, RestoreClaims: true, OldBoot: "pre-crash-boot", Services: []l1.NodeActiveAttempt{{JobID: "service", AttemptID: "old-service", Class: contract.JobClassService}}}
			if err := writeUpgradeDrain(paths.config, receipt); err != nil {
				t.Fatal(err)
			}
			if operatorDisabled {
				f.node.IntentRevision++
				f.node.IntentReason = "newer operator disable"
			}
			restarted := 0
			f.operations.readJob = func(context.Context, connprofile.Profile, string) (l1.Job, error) {
				return l1.Job{JobID: "service", State: contract.JobFailed, CurrentAttemptID: "old-service", ServiceJob: &l1.ServiceJob{DesiredState: contract.ServiceDesiredRunning, BoundNodeID: f.node.NodeID}}, nil
			}
			f.operations.restartService = func(_ context.Context, _ connprofile.Profile, id, key string) (l1.Job, error) {
				if id != "service" || key != receipt.Reason+":old-service" {
					t.Fatal("changed recovery identity", id, key)
				}
				restarted++
				return l1.Job{}, nil
			}
			command := f.operations.command
			f.operations.command = func(ctx context.Context, dir string, env []string, name string, args ...string) ([]byte, error) {
				if slices.Contains(args, "fetch") {
					if restarted != 1 || f.node.ClaimsEnabled == operatorDisabled {
						t.Fatal("preparation before recovery")
					}
					return nil, fmt.Errorf("injected fetch failure after recovery")
				}
				return command(ctx, dir, env, name, args...)
			}
			var out bytes.Buffer
			_, err := runUpgrade(t.Context(), f.host, false, &out)
			if err == nil || !strings.Contains(err.Error(), "injected fetch failure after recovery") || !strings.Contains(out.String(), "Restored intent from interrupted upgrade.") {
				t.Fatal("recovery not completed before fetch", err, out.String())
			}
			if restarted != 1 || f.node.ClaimsEnabled == operatorDisabled || len(s.mutations()) != 0 {
				t.Fatal("wrong recovery", restarted, f.node.ClaimsEnabled, s.mutations())
			}
			if _, err := readUpgradeDrain(paths.config); !os.IsNotExist(err) {
				t.Fatal("settled receipt retained", err)
			}
		})
	}
}

func TestLinuxFleetUpgradeUnchangedAgentReport(t *testing.T) {
	f, s := newLinuxUpgradeFixture(t, serviceplan.Join)
	if _, err := runUpgrade(t.Context(), f.host, false, io.Discard); err != nil {
		t.Fatal(err)
	}
	paths, _ := resolvePlatformMaintenancePaths(f.home, "linux", "")
	// A partial receipt needs completion even though the binaries/units match.
	payload, err := os.ReadFile(filepath.Join(paths.config, setupManifestName))
	if err != nil {
		t.Fatal(err)
	}
	var receipt setupManifest
	if err := json.Unmarshal(payload, &receipt); err != nil {
		t.Fatal(err)
	}
	receipt.InstalledVersion = ""
	payload, _ = json.Marshal(receipt)
	if err := os.WriteFile(filepath.Join(paths.config, setupManifestName), payload, 0600); err != nil {
		t.Fatal(err)
	}
	s.calls = nil
	var out, stderr bytes.Buffer
	if err := executeUpgradeWithHost(t.Context(), f.host, globalOptions{}, false, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Node agent unchanged.\n") || len(s.mutations()) != 0 {
		t.Fatal("unchanged Linux node misreported", out.String(), s.mutations())
	}
}

func TestLinuxFleetUpgradeRecoversDrainBeforePreflightRefusal(t *testing.T) {
	for _, cause := range []string{"unit-drop-in", "unsafe-log-parent", "bus-down"} {
		t.Run(cause, func(t *testing.T) {
			f, s := newLinuxUpgradeFixture(t, serviceplan.Join)
			paths, _ := resolvePlatformMaintenancePaths(f.home, "linux", "")
			f.node.ClaimsEnabled = false
			f.node.IntentRevision = 3
			f.node.IntentReason = "wefty upgrade interrupted"
			receipt := &upgradeDrainReceipt{Version: 1, Home: f.home, NodeID: f.node.NodeID, Revision: 3, Reason: f.node.IntentReason, RestoreClaims: true}
			if err := writeUpgradeDrain(paths.config, receipt); err != nil {
				t.Fatal(err)
			}
			switch cause {
			case "unit-drop-in":
				dir := filepath.Join(paths.launchAgents, setupNodeAgentLabel+".service.d")
				if err := os.Mkdir(dir, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "override.conf"), []byte("[Service]\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "unsafe-log-parent":
				if err := os.Chmod(filepath.Dir(paths.logs), 0777); err != nil {
					t.Fatal(err)
				}
			case "bus-down":
				run := f.host.systemctl
				f.host.systemctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
					if slices.Equal(args, []string{"--version"}) {
						return run(ctx, name, args...)
					}
					return nil, fmt.Errorf("injected user bus unavailable")
				}
			}
			before := snapshotLinuxUpgradeHome(t, f.home)
			var out bytes.Buffer
			_, err := runUpgrade(t.Context(), f.host, false, &out)
			if err == nil || commandExitCode(err) != 1 {
				t.Fatalf("expected preflight refusal: %v", err)
			}
			if !f.node.ClaimsEnabled || !strings.Contains(out.String(), "Restored intent from interrupted upgrade.") {
				t.Errorf("preflight blocked drain recovery: claims=%v output=%s err=%v", f.node.ClaimsEnabled, &out, err)
			}
			if _, err := readUpgradeDrain(paths.config); !os.IsNotExist(err) {
				t.Errorf("recovered drain receipt retained: %v", err)
			}
			if len(f.events) > 0 || len(s.mutations()) > 0 {
				t.Errorf("preflight refusal reached preparation or services: %v %v", f.events, s.mutations())
			}
			after := snapshotLinuxUpgradeHome(t, f.home)
			for name, digest := range before {
				if name != filepath.Join(paths.config, upgradeDrainFile) && after[name] != digest {
					t.Errorf("preflight refusal changed %s", filepath.Base(name))
				}
			}
		})
	}
}
