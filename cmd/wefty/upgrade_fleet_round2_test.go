package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/Derek-X-Wang/wefty/contract"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/internal/serviceready"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestFleetUpgradeRecordedBaseBinariesCanRollBack(t *testing.T) {
	for _, role := range []serviceplan.Role{serviceplan.ControlPlane, serviceplan.Join} {
		for _, failure := range []string{"renewal", "replacement"} {
			t.Run(string(role)+"/"+failure, func(t *testing.T) {
				f := newFleetUpgradeFixture(t, role)
				paths, _ := resolveSetupPaths(f.home)
				plan := mustFleetPlan(t, paths, role)
				before := map[string][]byte{}
				for _, name := range fleetUpgradeBinaries(plan) {
					payload, err := os.ReadFile(filepath.Join(paths.bin, name))
					if err != nil {
						t.Fatal(err)
					}
					before[name] = payload
				}
				f.failure = failure
				if role == serviceplan.Join && failure == "renewal" {
					observe := f.operations.observe
					f.operations.observe = func(ctx context.Context, p connprofile.Profile) (l1.Node, error) {
						node, err := observe(ctx, p)
						if f.replaced {
							node.LastHeartbeatAt = f.now.Add(-time.Hour)
						}
						return node, err
					}
				}
				_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
				if err == nil || !strings.Contains(err.Error(), "old installation files restored") {
					t.Fatalf("base-like recorded binaries prevented rollback: %v", err)
				}
				for _, service := range plan.Services {
					label := "dev.wefty." + service.Name
					if !f.launchd.loaded[label] || f.launchd.stopped[label] {
						t.Errorf("rollback left service down: %s", label)
					}
				}
				for name, want := range before {
					got, err := os.ReadFile(filepath.Join(paths.bin, name))
					if err != nil || !bytes.Equal(got, want) {
						t.Errorf("previous binary not restored: %s %v", name, err)
					}
				}
			})
		}
	}
}

func TestFleetUpgradeOptionalHeartbeatFailureDoesNotRejectHealthyNode(t *testing.T) {
	for _, role := range []serviceplan.Role{serviceplan.ControlPlane, serviceplan.Join} {
		for _, failure := range []string{"verification", "signal"} {
			t.Run(string(role)+"/"+failure, func(t *testing.T) {
				f := newFleetUpgradeFixture(t, role)
				paths, _ := resolveSetupPaths(f.home)
				dir := filepath.Join(paths.data, "setup-ready")
				if err := os.MkdirAll(dir, 0700); err != nil {
					t.Fatal(err)
				}
				writeReady := func(boot string) {
					payload := []byte("invalid")
					if failure == "signal" {
						payload, _ = json.Marshal(serviceready.Metadata{PID: 4242, ReadyAt: f.now.Add(-time.Hour), NodeID: f.node.NodeID, BootSessionID: boot, ImmediateHeartbeat: true})
					}
					if err := os.WriteFile(filepath.Join(dir, "node-agent.json"), payload, 0600); err != nil {
						t.Fatal(err)
					}
				}
				writeReady(f.node.BootSessionID)
				f.host.bootTime = func() (time.Time, error) { return f.now.Add(-24 * time.Hour), nil }
				kills := 0
				launch := f.host.launchctl
				f.host.launchctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
					if args[0] == "kill" {
						kills++
						return nil, errors.New("injected optional signal failure\nwith extra detail")
					}
					out, err := launch(ctx, name, args...)
					if args[0] == "bootstrap" && strings.HasSuffix(args[len(args)-1], setupNodeAgentLabel+".plist") {
						writeReady("new-boot")
					}
					return out, err
				}
				var warnings bytes.Buffer
				if _, err := runUpgrade(t.Context(), f.host, false, &warnings); err != nil {
					t.Fatalf("optional signal failed healthy upgrade: %v", err)
				}
				if strings.Count(warnings.String(), "normal renewal window") != 1 || strings.Contains(warnings.String(), "\nwith extra detail") {
					t.Fatalf("missing or repeated fallback notice: %s", &warnings)
				}
				if failure == "verification" && kills != 0 || failure == "signal" && kills == 0 {
					t.Fatalf("unexpected signal attempts: %d", kills)
				}
			})
		}
	}
}

func TestFleetUpgradeFirstObservationHasClientAndL1Hint(t *testing.T) {
	f := newFleetUpgradeFixture(t, serviceplan.Join)
	f.operations.observe = func(context.Context, connprofile.Profile) (l1.Node, error) {
		return l1.Node{}, errors.New("observation unavailable")
	}
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "client service") || !strings.Contains(err.Error(), "L1") {
		t.Fatalf("missing observation advice: %v", err)
	}
}

func TestStatusVersionWarningsIgnoreDeadNodes(t *testing.T) {
	for _, fleet := range []bool{false, true} {
		h := newStatusHarness(t)
		a, b := readyNode("live-node"), readyNode("dead-node")
		a.AgentVersion, b.AgentVersion, b.State = "current", "old", contract.NodeDead
		h.nodes = l1.NodeList{Nodes: []l1.Node{a, b}}
		clients := h.clients()
		if fleet {
			clients.clientIdentityID = "saved-client"
		}
		var out, stderr bytes.Buffer
		if err := executeStatus(t.Context(), clients, true, nil, &out, &stderr); err != nil {
			t.Fatal(err)
		}
		var report struct {
			VersionWarnings []statusReason `json:"version_warnings"`
		}
		if err := json.Unmarshal(out.Bytes(), &report); err != nil {
			t.Fatal(err)
		}
		if len(report.VersionWarnings) != 0 {
			t.Fatalf("dead node produced mismatch: %+v", report.VersionWarnings)
		}
	}
	f := newFleetUpgradeFixture(t, serviceplan.Join)
	paths, _ := resolveSetupPaths(f.home)
	f.host.fleetClients = func(context.Context, setupPaths, connprofile.Profile) (*apiClients, error) {
		h := newStatusHarness(t)
		n := readyNode("dead-node")
		n.State, n.AgentVersion = contract.NodeDead, "old"
		h.nodes = l1.NodeList{Nodes: []l1.Node{n}}
		return h.clients(), nil
	}
	warnings, err := fleetUpgradeVersionWarnings(t.Context(), f.host, paths, f.profile(t), f.commit)
	if err != nil || len(warnings) != 0 {
		t.Fatalf("dead node needs upgrade: %+v %v", warnings, err)
	}
}

func TestFleetUpgradeSignalsOnlyAfterL1Observation(t *testing.T) {
	f := newFleetUpgradeFixture(t, serviceplan.ControlPlane)
	paths, _ := resolveSetupPaths(f.home)
	dir := filepath.Join(paths.data, "setup-ready")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	ready := serviceready.Metadata{PID: 4242, ReadyAt: f.now.Add(-time.Hour), NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ImmediateHeartbeat: true}
	payload, _ := json.Marshal(ready)
	if err := os.WriteFile(filepath.Join(dir, "node-agent.json"), payload, 0600); err != nil {
		t.Fatal(err)
	}
	f.host.bootTime = func() (time.Time, error) { return f.now.Add(-24 * time.Hour), nil }
	restarted, observed, signaled, failed := false, false, false, false
	var restartAt time.Time
	launch := f.host.launchctl
	f.host.launchctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if args[0] == "kill" {
			if !observed {
				t.Error("signal sent before L1 was observed reachable")
			}
			signaled = true
			return nil, nil
		}
		out, err := launch(ctx, name, args...)
		if args[0] == "bootstrap" && strings.HasSuffix(args[len(args)-1], setupL3Label+".plist") {
			restarted = true
			restartAt = f.now
		}
		return out, err
	}
	observe := f.operations.observe
	f.operations.observe = func(ctx context.Context, p connprofile.Profile) (l1.Node, error) {
		n, err := observe(ctx, p)
		if restarted && !f.replaced {
			if !failed {
				failed = true
				return l1.Node{}, errors.New("L1 restarting")
			}
			observed = true
			if !signaled {
				n.LastHeartbeatAt = restartAt.Add(-time.Second)
			}
		}
		return n, err
	}
	if _, err := runUpgrade(t.Context(), f.host, false, io.Discard); err != nil {
		t.Fatal(err)
	}
	if !failed || !signaled || f.now.Sub(restartAt) >= time.Second {
		t.Fatalf("heartbeat missed reachable poll: failed=%t signal=%t elapsed=%s", failed, signaled, f.now.Sub(restartAt))
	}
}
