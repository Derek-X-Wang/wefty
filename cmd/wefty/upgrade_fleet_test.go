package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/internal/serviceready"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Keep the plain fixture and its assertions unchanged. This fixture adds only
// explicit fleet metadata and uses the same confined build/launchctl seams.
func newFleetUpgradeFixture(t *testing.T, role serviceplan.Role) *upgradeFixture {
	t.Helper()
	f := newUpgradeFixture(t)
	// A fleet socket needs a short private home. Close the fake databases before
	// moving the plain fixture; all files remain disposable under the Go cache.
	for _, db := range f.databases {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	oldHome := f.home
	home := newFleetTestHome(t)
	entries, err := os.ReadDir(oldHome)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if err := os.Rename(filepath.Join(oldHome, entry.Name()), filepath.Join(home, entry.Name())); err != nil {
			t.Fatal(err)
		}
	}
	f.home, f.host.home = home, home
	if err := filepath.WalkDir(home, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".plist") {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			return os.WriteFile(path, bytes.ReplaceAll(b, []byte(oldHome), []byte(home)), 0644)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	f.databases = nil
	for _, name := range []string{"l1.sqlite", "l3.sqlite"} {
		db, err := sql.Open("sqlite", filepath.Join(home, setupFolders.data, name))
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		f.databases = append(f.databases, db)
		t.Cleanup(func() { db.Close() })
	}

	paths, _ := resolveSetupPaths(f.home)
	p := f.profile(t)
	p.Fabric, p.PlainFabricID = "tsnet", ""
	p.L1Address, p.L3Address = "opaque-l1", "opaque-l3"
	p.Participants = map[string]connprofile.Participant{}
	roles := []string{"node-agent", "client"}
	if role == serviceplan.ControlPlane {
		roles = append([]string{"l1", "l3"}, roles...)
		p.AutomaticNodeAdmission = true
	} else {
		p.NodeAdmission = nil
	}
	for _, r := range roles {
		name, _, _ := setupParticipantPolicy(r)
		if strings.HasSuffix(name, "/") {
			name += p.Identities.Node
		}
		dir := filepath.Join(paths.data, "fabric", r)
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "fixture-state"), []byte("saved"), 0600); err != nil {
			t.Fatal(err)
		}
		p.Participants[r] = connprofile.Participant{Name: name, StateDirectory: dir, NodeID: "opaque-" + r, NetworkName: "wefty-" + r}
	}
	if err := serviceplan.ValidateProfile(role, p); err != nil {
		t.Fatal(err)
	}
	payload, err := connprofile.Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.profile, payload, 0600); err != nil {
		t.Fatal(err)
	}
	var manifest setupManifest
	receiptPath := filepath.Join(paths.config, setupManifestName)
	old, _ := os.ReadFile(receiptPath)
	if err := json.Unmarshal(old, &manifest); err != nil {
		t.Fatal(err)
	}
	// Today's installed fleet binaries predate the maintenance flag. Keep
	// their real exit-2 behavior instead of synthesizing compatibility replies.
	for _, name := range setupBinaries {
		payload := []byte("#!/bin/sh\nif [ \"$1\" = --version ]; then echo v1; exit 0; fi\necho 'flag provided but not defined: -profile-preflight' >&2\nexit 2\n")
		if err := os.WriteFile(filepath.Join(paths.bin, name), payload, 0755); err != nil {
			t.Fatal(err)
		}
		manifest.Files[filepath.Join(paths.binRel, name)] = payloadDigest(payload)
	}
	manifest.Role = role
	manifest.Files[filepath.Join(paths.configRel, connprofile.FileName)] = payloadDigest(payload)
	old, _ = json.Marshal(manifest)
	if err := os.WriteFile(receiptPath, old, 0600); err != nil {
		t.Fatal(err)
	}
	record := fleetSetupRecord{Version: 1, Role: role, NodeID: p.Identities.Node, StagedProfile: filepath.Join(paths.config, fleetSetupProfileName), Committed: true, ProfilePublished: true}
	if role == serviceplan.Join {
		record.JoinCode = joinCodeForTest(t, p.L1Address, p.L3Address, "")
	}
	old, _ = json.Marshal(record)
	if err := os.WriteFile(filepath.Join(paths.config, fleetSetupRecordName), old, 0600); err != nil {
		t.Fatal(err)
	}
	// Test the selected build command by forcing the real build planner through
	// the command seam. No Go compiler, network or real service is invoked.
	f.operations.build = nil
	command := f.operations.command
	f.operations.command = func(ctx context.Context, dir string, env []string, name string, args ...string) ([]byte, error) {
		if slices.Contains(args, "build") {
			f.events = append(f.events, "build")
			out := args[slices.Index(args, "-o")+1]
			for _, arg := range args {
				if strings.HasPrefix(arg, "./cmd/") {
					binary := strings.TrimPrefix(arg, "./cmd/")
					f.events = append(f.events, "built:"+binary)
					if err := os.WriteFile(filepath.Join(out, binary), []byte("#!/bin/sh\nif [ \"$1\" = --profile-preflight ]; then echo profile-readable "+string(role)+"; exit 0; fi\necho "+f.commit+"\n"), 0755); err != nil {
						return nil, err
					}
				}
			}
			return nil, nil
		}
		if slices.Contains(args, "--profile-preflight") {
			f.events = append(f.events, "profile:"+filepath.Base(name))
		}
		return command(ctx, dir, env, name, args...)
	}
	// Supply a harmless Go executable solely for findUpgradeTool.
	goPath := filepath.Join(f.home, "tools")
	if err := os.Mkdir(goPath, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(goPath, "go"), []byte("#!/bin/sh\nexit 99\n"), 0755); err != nil {
		t.Fatal(err)
	}
	plistPath := filepath.Join(paths.launchAgents, setupNodeAgentLabel+".plist")
	plist, _ := os.ReadFile(plistPath)
	plist = bytes.ReplaceAll(plist, []byte(f.loginPATH+":"+paths.bin), []byte(goPath+":"+f.loginPATH+":"+paths.bin))
	if err := os.WriteFile(plistPath, plist, 0644); err != nil {
		t.Fatal(err)
	}
	f.host.loginShell = func(context.Context, string, ...string) ([]byte, error) {
		return []byte(setupPATHBegin + "\n" + goPath + ":" + f.loginPATH + ":" + paths.bin + "\n" + setupPATHEnd + "\n"), nil
	}
	if role == serviceplan.Join {
		f.operations.setClaims = func(ctx context.Context, _ connprofile.Profile, r l1.NodeIntentRequest) (l1.Node, error) {
			if r.IntentRevision != f.node.IntentRevision {
				t.Fatal("unfenced claims mutation")
			}
			f.node.ClaimsEnabled = r.ClaimsEnabled
			f.node.IntentRevision++
			f.node.IntentReason = r.Reason
			return f.node, nil
		}
	}
	digests := map[string]string{}
	for _, name := range setupBinaries {
		digest, err := executableDigest(filepath.Join(paths.bin, name))
		if err != nil {
			t.Fatal(err)
		}
		digests[name] = digest
	}
	services, err := setupRoleServices(paths, p, digests, goPath+":"+f.loginPATH+":"+paths.bin, role)
	if err != nil {
		t.Fatal(err)
	}
	for _, service := range services {
		payload, err := launchagent.Render(service)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(paths.launchAgents, service.Label+".plist"), payload, 0644); err != nil {
			t.Fatal(err)
		}
		f.launchd.loaded[service.Label] = true
	}
	f.events = nil
	f.launchd.commands = nil
	return f
}

func TestFleetUpgradeRolePlan(t *testing.T) {
	for _, role := range []serviceplan.Role{serviceplan.Join, serviceplan.ControlPlane} {
		t.Run(string(role), func(t *testing.T) {
			f := newFleetUpgradeFixture(t, role)
			paths, _ := resolveSetupPaths(f.home)
			beforeProfile, _ := os.ReadFile(paths.profile)
			history := filepath.Join(paths.data, "plain-history")
			if err := os.Mkdir(history, 0700); err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(history, "l1.sqlite")
			if err := os.WriteFile(marker, []byte("history is never an authority"), 0400); err != nil {
				t.Fatal(err)
			}
			report, err := runUpgrade(t.Context(), f.host, false, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			if report.Version != f.commit || !f.node.ClaimsEnabled {
				t.Fatalf("incomplete upgrade: %+v", report)
			}
			want := []string{"restart:" + setupNodeAgentLabel, "restart:dev.wefty.client-service"}
			built := []string{"built:wefty", "built:wefty-agent"}
			if role == serviceplan.ControlPlane {
				want = append([]string{"restart:" + setupL1Label, "restart:" + setupL3Label}, want...)
				built = []string{"built:wefty", "built:wefty-l1", "built:wefty-l3", "built:wefty-agent"}
			}
			var got, gotBuilt []string
			for _, e := range f.events {
				if strings.HasPrefix(e, "restart:") {
					got = append(got, e)
				}
				if strings.HasPrefix(e, "built:") {
					gotBuilt = append(gotBuilt, e)
				}
			}
			if !slices.Equal(got, want) || !slices.Equal(gotBuilt, built) {
				t.Fatalf("services %v want %v; builds %v want %v", got, want, gotBuilt, built)
			}
			if role == serviceplan.Join && (slices.Contains(f.events, "preflight:wefty-l1") || slices.Contains(f.events, "preflight:wefty-l3")) {
				t.Fatal("join preflighted authority")
			}
			afterProfile, _ := os.ReadFile(paths.profile)
			if !bytes.Equal(beforeProfile, afterProfile) {
				t.Fatal("upgrade rewrote participant profile")
			}
			after, _ := os.ReadFile(marker)
			if string(after) != "history is never an authority" {
				t.Fatal("history changed")
			}
		})
	}
}

func TestStatusVersionMismatchFromFakeL1(t *testing.T) {
	for _, fleet := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "fleet"}[fleet], func(t *testing.T) {
			h := newStatusHarness(t)
			a, b := readyNode("node-a"), readyNode("node-b")
			a.AgentVersion, b.AgentVersion = "build-a", "build-b"
			h.nodes = l1.NodeList{Nodes: []l1.Node{b, a}}
			clients := h.clients()
			if fleet {
				clients.clientIdentityID = "saved-client"
				clients.localPersonSignIn = "not_signed_in"
			}
			var out, stderr bytes.Buffer
			if err := executeStatus(t.Context(), clients, true, nil, &out, &stderr); err != nil {
				t.Fatal(err)
			}
			var status map[string]json.RawMessage
			if err := json.Unmarshal(out.Bytes(), &status); err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains(status["version_warnings"], []byte("node-a")) || !bytes.Contains(status["version_warnings"], []byte("node-b")) {
				t.Fatalf("missing mismatched Nodes: %s", out.String())
			}
			out.Reset()
			if err := executeStatus(t.Context(), clients, false, nil, &out, &stderr); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), "build-a") || !strings.Contains(out.String(), "build-b") || !strings.Contains(out.String(), "version") {
				t.Fatalf("missing human version warning: %s", &out)
			}
		})
	}
}

func TestFleetUpgradeRefusalsPrecedeMutation(t *testing.T) {
	for _, kind := range []string{"staged", "conversion", "receipt-profile", "record-node", "role"} {
		t.Run(kind, func(t *testing.T) {
			f := newFleetUpgradeFixture(t, serviceplan.Join)
			paths, _ := resolveSetupPaths(f.home)
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
			case "conversion":
				if err := os.WriteFile(filepath.Join(paths.config, plainConversionName), []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			case "receipt-profile":
				p := f.profile(t)
				p.L1Address = "changed-peer"
				payload, _ := connprofile.Encode(p)
				if err := os.WriteFile(paths.profile, payload, 0600); err != nil {
					t.Fatal(err)
				}
			case "record-node":
				record, err := readFleetSetupRecord(paths)
				if err != nil {
					t.Fatal(err)
				}
				record.NodeID = "other"
				payload, _ := json.Marshal(record)
				if err := os.WriteFile(filepath.Join(paths.config, fleetSetupRecordName), payload, 0600); err != nil {
					t.Fatal(err)
				}
			case "role":
				receipt := readSetupReceipt(t, f.home)
				receipt.Role = serviceplan.ControlPlane
				payload, _ := json.Marshal(receipt)
				if err := os.WriteFile(filepath.Join(paths.config, setupManifestName), payload, 0600); err != nil {
					t.Fatal(err)
				}
			}
			before := installedUpgradeFiles(t, f)
			f.events = nil
			f.launchd.commands = nil
			if _, err := runUpgrade(t.Context(), f.host, false, io.Discard); err == nil {
				t.Fatal("unsafe upgrade accepted")
			}
			if len(f.events) > 0 || len(f.launchd.commands) > 0 {
				t.Fatalf("refusal mutated: %v %v", f.events, f.launchd.commands)
			}
			if !equalFleetFiles(before, installedUpgradeFiles(t, f)) {
				t.Fatal("refusal changed install")
			}
		})
	}
}

func TestFleetUpgradeCandidateAndRollbackSecondGate(t *testing.T) {
	for _, gate := range []string{"candidate", "rollback"} {
		t.Run(gate, func(t *testing.T) {
			f := newFleetUpgradeFixture(t, serviceplan.ControlPlane)
			paths, _ := resolveSetupPaths(f.home)
			if gate == "rollback" {
				// These bytes are not covered by the receipt and must pass the gate.
				if err := os.WriteFile(filepath.Join(paths.bin, "wefty"), []byte("#!/bin/sh\nexit 2\n"), 0755); err != nil {
					t.Fatal(err)
				}
			}
			previous, _ := os.ReadFile(filepath.Join(paths.bin, "wefty"))
			command := f.operations.command
			f.operations.command = func(ctx context.Context, dir string, env []string, name string, args ...string) ([]byte, error) {
				isPrevious := strings.Contains(name, "previous-binaries")
				if slices.Contains(args, "--profile-preflight") && ((gate == "rollback" && isPrevious) || (gate == "candidate" && !isPrevious)) {
					// This fake old executable rejects the participants field, as the old
					// strict decoder did. Run the actual subprocess through the production
					// command seam; no successful stub can mask this failure.
					script := "#!/bin/sh\nfor arg in \"$@\"; do case \"$arg\" in --profile=*) profile=${arg#--profile=};; esac; done\nif /usr/bin/grep -q '\"participants\"' \"$profile\"; then echo 'json: unknown field participants' >&2; exit 1; fi\nexit 0\n"
					if err := os.WriteFile(name, []byte(script), 0755); err != nil {
						t.Fatal(err)
					}
					return runUpgradeCommand(ctx, dir, env, name, args...)
				}
				return command(ctx, dir, env, name, args...)
			}
			if gate == "rollback" {
				f.failure = "renewal"
			}
			_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
			if err == nil || !strings.Contains(err.Error(), "cannot read the active fleet profile") {
				t.Fatalf("missing compatibility gate: %v", err)
			}
			current, _ := os.ReadFile(filepath.Join(paths.bin, "wefty"))
			if !bytes.Equal(current, previous) || slices.Contains(f.events, "restart:"+setupL1Label) {
				t.Fatal("incompatible candidate or rollback path reached activation")
			}
			p := f.profile(t)
			if len(p.Participants) != 4 {
				t.Fatal("participant records stripped")
			}
		})
	}
}

func TestFleetUpgradeRollbackPreservesLiveAuthority(t *testing.T) {
	for _, schemaChange := range []bool{false, true} {
		t.Run(map[bool]string{false: "unchanged-format", true: "new-format"}[schemaChange], func(t *testing.T) {
			f := newFleetUpgradeFixture(t, serviceplan.ControlPlane)
			paths, _ := resolveSetupPaths(f.home)
			previous, _ := os.ReadFile(filepath.Join(paths.bin, "wefty"))
			f.failure = "renewal"
			runner := f.host.launchctl
			changed := false
			f.host.launchctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
				out, err := runner(ctx, name, args...)
				if err == nil && !changed && args[0] == "bootstrap" && strings.HasSuffix(args[2], setupL3Label+".plist") {
					changed = true
					if _, err := f.databases[0].Exec("UPDATE authority SET fence=42"); err != nil {
						t.Fatal(err)
					}
					if schemaChange {
						if _, err := f.databases[0].Exec("CREATE TABLE changed_format(id INTEGER)"); err != nil {
							t.Fatal(err)
						}
					}
				}
				return out, err
			}
			_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
			if err == nil {
				t.Fatal("renewal failure accepted")
			}
			current, _ := os.ReadFile(filepath.Join(paths.bin, "wefty"))
			if bytes.Equal(current, previous) == schemaChange {
				t.Fatalf("wrong rollback decision: %v", err)
			}
			var fence int
			if err := f.databases[0].QueryRow("SELECT fence FROM authority").Scan(&fence); err != nil || fence != 42 {
				t.Fatalf("authority restored: %d %v", fence, err)
			}
			if schemaChange && readSetupReceipt(t, f.home).InstalledVersion != "" {
				t.Fatal("partial receipt claims complete version")
			}
		})
	}
}

func TestFleetUpgradeImmediateHeartbeatReceiver(t *testing.T) {
	for _, kind := range []string{"supported", "legacy", "pid-mismatch", "boot-mismatch", "previous-system-boot"} {
		t.Run(kind, func(t *testing.T) {
			f := newFleetUpgradeFixture(t, serviceplan.ControlPlane)
			paths, _ := resolveSetupPaths(f.home)
			dir := filepath.Join(paths.data, "setup-ready")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			ready := serviceready.Metadata{PID: 4242, ReadyAt: f.now.Add(-time.Hour), NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ImmediateHeartbeat: kind != "legacy"}
			f.host.bootTime = func() (time.Time, error) { return f.now.Add(-24 * time.Hour), nil }
			switch kind {
			case "pid-mismatch":
				ready.PID++
			case "boot-mismatch":
				ready.BootSessionID = "other-boot"
			case "previous-system-boot":
				ready.ReadyAt = f.now.Add(-48 * time.Hour)
			}
			payload, _ := json.Marshal(ready)
			if err := os.WriteFile(filepath.Join(dir, "node-agent.json"), payload, 0600); err != nil {
				t.Fatal(err)
			}
			signaled := false
			f.host.launchctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
				if args[0] == "kill" {
					if len(args) != 3 || args[1] != "SIGUSR1" || !strings.HasSuffix(args[2], "/"+setupNodeAgentLabel) {
						t.Fatalf("wrong heartbeat target: %v", args)
					}
					signaled = true
					f.now = f.now.Add(10 * time.Millisecond)
					f.node.LastHeartbeatAt = f.now
					return nil, nil
				}
				return f.launchd.run(ctx, name, args...)
			}
			err := requestFleetUpgradeHeartbeat(t.Context(), f.host, paths, f.node)
			if (kind == "supported" || kind == "legacy") != (err == nil) {
				t.Fatalf("receiver result: %v", err)
			}
			if signaled != (kind == "supported") {
				t.Fatalf("unsafe or missing signal: %t", signaled)
			}
			if kind == "supported" {
				op := upgradeOperations(f.host)
				started := f.now.Add(-10 * time.Millisecond)
				elapsed, err := waitFleetUpgradeRenewal(t.Context(), op, f.profile(t), f.node, started, started, func(ctx context.Context, _ l1.Node) {
					if err := requestFleetUpgradeHeartbeat(ctx, f.host, paths, f.node); err != nil {
						t.Fatal(err)
					}
				})
				if err != nil || elapsed >= time.Second {
					t.Fatalf("renewal waited for 15 s phase: %s %v", elapsed, err)
				}
			}
		})
	}
}

func TestFleetUpgradeKeepsDrainIntentAndServiceSafeguards(t *testing.T) {
	for _, kind := range []string{"drain-timeout", "newer-intent", "late-oneshot", "pre-disabled", "service-restart", "recovery-receipt"} {
		t.Run(kind, func(t *testing.T) {
			f := newFleetUpgradeFixture(t, serviceplan.Join)
			paths, _ := resolveSetupPaths(f.home)
			switch kind {
			case "drain-timeout":
				f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "busy", AttemptID: "busy-attempt", Class: contract.JobClassOneShot}}
			case "late-oneshot", "newer-intent":
				observe := f.operations.observe
				f.operations.observe = func(ctx context.Context, p connprofile.Profile) (l1.Node, error) {
					node, err := observe(ctx, p)
					if f.observeCalls == 3 {
						if kind == "newer-intent" {
							f.node.IntentRevision++
							f.node.IntentReason = "operator kept closed"
							node = f.node
						} else {
							node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "late", AttemptID: "late-attempt", Class: contract.JobClassOneShot}}
						}
					}
					return node, err
				}
			case "pre-disabled":
				f.node.ClaimsEnabled = false
				f.node.IntentReason = "operator kept closed"
			case "service-restart":
				f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "app", AttemptID: "old-attempt", Class: contract.JobClassService}}
				f.operations.readJob = func(context.Context, connprofile.Profile, string) (l1.Job, error) {
					return l1.Job{JobID: "app", CurrentAttemptID: "old-attempt", ServiceJob: &l1.ServiceJob{BoundNodeID: f.node.NodeID, DesiredState: contract.ServiceDesiredRunning}}, nil
				}
				f.operations.restartService = func(_ context.Context, _ connprofile.Profile, id, key string) (l1.Job, error) {
					if id != "app" || !strings.Contains(key, "old-attempt") || f.node.ClaimsEnabled {
						t.Fatal("unfenced or premature service recovery")
					}
					return l1.Job{}, nil
				}
			case "recovery-receipt":
				set := f.operations.setClaims
				f.operations.setClaims = func(ctx context.Context, p connprofile.Profile, r l1.NodeIntentRequest) (l1.Node, error) {
					if r.ClaimsEnabled {
						return l1.Node{}, errors.New("injected unavailable restoration")
					}
					return set(ctx, p, r)
				}
			}
			report, err := runUpgrade(t.Context(), f.host, false, io.Discard, 10*time.Second)
			failed := kind == "drain-timeout" || kind == "newer-intent" || kind == "late-oneshot" || kind == "recovery-receipt"
			if failed != (err != nil) {
				t.Fatalf("wrong result: %+v %v", report, err)
			}
			switch kind {
			case "drain-timeout", "newer-intent", "late-oneshot":
				if f.replaced {
					t.Fatal("unsafe node replacement")
				}
				if kind == "newer-intent" && f.node.ClaimsEnabled {
					t.Fatal("newer intent overwritten")
				}
			case "pre-disabled":
				if !report.ClaimsDisabled || f.node.ClaimsEnabled {
					t.Fatal("operator disable cleared")
				}
			case "service-restart":
				if !slices.Equal(report.RestartedServices, []string{"app"}) {
					t.Fatalf("lost service recovery: %+v", report)
				}
			case "recovery-receipt":
				if _, err := readUpgradeDrain(paths.config); err != nil {
					t.Fatalf("recovery evidence lost: %v", err)
				}
				op := fleetUpgradeOperations(f.host, paths, f.profile(t), mustFleetPlan(t, paths, serviceplan.Join))
				op.setClaims = func(_ context.Context, _ connprofile.Profile, r l1.NodeIntentRequest) (l1.Node, error) {
					if r.IntentRevision != f.node.IntentRevision {
						t.Fatal("unfenced recovery")
					}
					f.node.ClaimsEnabled = r.ClaimsEnabled
					f.node.IntentRevision++
					f.node.IntentReason = r.Reason
					return f.node, nil
				}
				if err := recoverUpgradeDrain(t.Context(), op, paths, f.profile(t), io.Discard); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func mustFleetPlan(t *testing.T, paths setupPaths, role serviceplan.Role) serviceplan.Plan {
	t.Helper()
	plan, err := setupRolePlan(paths, role)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}
