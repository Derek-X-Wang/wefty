//go:build darwin || linux

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

func round2DoctorFixture(t *testing.T, nodes *l1.NodeList) (*setupFixture, setupPaths) {
	t.Helper()
	f := newPlainBinaryFixture(t, "").setup
	network := plain.NewNetwork()
	participant := network.NewFabric(fabric.Identity{NodeID: "round2-doctor-control"})
	listener, err := participant.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("diagnostic mutation: %s", r.Method)
		}
		switch r.URL.Path {
		case "/v1/nodes":
			_ = json.NewEncoder(w).Encode(nodes)
		case "/v1/nodes/test-mac":
			_ = json.NewEncoder(w).Encode(nodes.Nodes[0])
		default:
			t.Errorf("unexpected diagnostic read %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	server.Listener.Close()
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	f.host.l1Address = strings.TrimPrefix(server.URL, "http://")
	f.host.l3Address = f.host.l1Address
	f.setup(t, setupRequest{nodeID: "test-mac"})
	paths, _ := resolveSetupPaths(f.home)
	return f, paths
}
func round2Nodes(t *testing.T, enrolled bool) l1.NodeList {
	t.Helper()
	nodes := generationStatusFixture(time.Now(), 0)
	n := &nodes.Nodes[0]
	n.NodeID = "test-mac"
	tags, err := pinnedImageTags(nil, n.NodeID, true)
	if err != nil {
		t.Fatal(err)
	}
	n.AuthoritativeTags = tags
	if !enrolled {
		n.AgentGenerationID = ""
		n.Generations = nil
	}
	return nodes
}
func TestRound2PendingMaintenance(t *testing.T) {
	for _, kind := range []string{"remove", "convert"} {
		for _, catalogPresent := range []bool{false, true} {
			t.Run(kind+map[bool]string{false: "/catalog-absent", true: "/catalog-present"}[catalogPresent], func(t *testing.T) {
				nodes := round2Nodes(t, true)
				f, paths := round2DoctorFixture(t, &nodes)
				id := strings.Repeat("a", 32)
				nodes.Nodes[0].AgentGenerationID = id
				nodes.Nodes[0].Generations[0].AgentGenerationID = id
				root := filepath.Join(paths.data, agentinstall.Directory)
				label := "dev.wefty.node-agent.g-" + id
				c := agentinstall.Catalog{Version: 1, NodeID: "test-mac", Revision: 1, Primary: id, Generations: map[string]agentinstall.Generation{id: {ID: id, Role: "primary", Version: "v1", Digest: strings.Repeat("c", 64), Label: label, Executable: filepath.Join(root, id, "wefty-agent"), Plist: filepath.Join(paths.launchAgents, label+".plist"), CreatedAt: time.Now().Add(-time.Hour), StartBootID: rollingTestBoot, CandidateBootSessionID: strings.Repeat("d", 32), ProtocolVersions: []int{1}}}}
				if catalogPresent {
					if err := os.MkdirAll(root, 0700); err != nil {
						t.Fatal(err)
					}
					b, _ := json.Marshal(c)
					if err := os.WriteFile(filepath.Join(root, "catalog.json"), b, 0600); err != nil {
						t.Fatal(err)
					}
				}
				m := agentinstall.Maintenance{Version: 1, Operation: "pending-operation", Kind: kind, Catalog: c, Device: 1, Inode: 1}
				b, _ := json.Marshal(m)
				if err := os.WriteFile(filepath.Join(paths.data, agentinstall.MaintenanceFile), b, 0600); err != nil {
					t.Fatal(err)
				}
				receipt := upgradeRollingReceipt{Version: 1, OperationID: "finished-upgrade", NodeID: "test-mac", Candidate: agentinstall.Generation{ID: id}}
				if err := writeUpgradeRolling(paths, &receipt, "complete"); err != nil {
					t.Fatal(err)
				}
				want := "maintenance in progress: " + map[string]string{"remove": "removal", "convert": "conversion"}[kind] + " pending-operation; re-run the same wefty setup command to finish or restore"
				if catalogPresent {
					plist, err := os.ReadFile(filepath.Join(paths.launchAgents, setupNodeAgentLabel+".plist"))
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(c.Generations[id].Plist, plist, 0644); err != nil {
						t.Fatal(err)
					}
					f.launchd.loaded[label] = true
				}
				before := diagnosticTree(t, f.home)
				mutations := len(f.launchd.mutating())
				t.Run("status", func(t *testing.T) {
					h := newStatusHarness(t)
					h.nodes = nodes
					clients := h.clients()
					clients.nodeConfigPath = paths.nodeConfig
					status := collectStatus(t.Context(), clients)
					if status.Ready {
						t.Error("pending maintenance became ready")
					}
					reviewDiagnosticOutputs(t, status.rollingStatusFields, want, "")
				})
				t.Run("doctor", func(t *testing.T) {
					report := runFixturePlainDoctor(t, f.host, globalOptions{nodeConfigPath: paths.nodeConfig, profilePath: paths.profile})
					if report.Healthy {
						t.Error("pending maintenance became healthy")
					}
					reviewDiagnosticOutputs(t, report.rollingStatusFields, want, "")
				})
				if diagnosticTree(t, f.home) != before || len(f.launchd.mutating()) != mutations {
					t.Fatal("maintenance diagnosis mutated local evidence or launchd")
				}

			})
		}
	}
}
func TestRound2LegacyActiveAttemptErrors(t *testing.T) {
	for _, enrolled := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "enrolled"}[enrolled], func(t *testing.T) {
			nodes := round2Nodes(t, enrolled)
			nodes.Nodes[0].ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "job-broken", AttemptID: "attempt-broken", ObservationError: "unreadable retained Job spec: unexpected end of JSON input"}}
			nodes.Nodes = append(nodes.Nodes, readyNode("other-node"))
			f, paths := round2DoctorFixture(t, &nodes)
			t.Run("status", func(t *testing.T) {
				h := newStatusHarness(t)
				h.nodes = nodes
				status := collectStatus(t.Context(), h.clients())
				if status.Ready || len(status.Nodes) != 2 {
					t.Errorf("lost error or unrelated Node: %+v", status)
				}
				reviewDiagnosticOutputs(t, status.rollingStatusFields, "attempt-broken", "")
			})
			t.Run("doctor", func(t *testing.T) {
				report := runFixturePlainDoctor(t, f.host, globalOptions{nodeConfigPath: paths.nodeConfig, profilePath: paths.profile})
				if report.Healthy {
					t.Error("legacy active-Attempt observation error became healthy")
				}
				reviewDiagnosticOutputs(t, report.rollingStatusFields, "attempt-broken", "")
			})
		})
	}
}
func TestRound2EffectiveEndpointGuard(t *testing.T) {
	f := newPlainBinaryFixture(t, "")
	marker := filepath.Join(t.TempDir(), "executed")
	sentinel := filepath.Join(t.TempDir(), "sentinel")
	if err := os.WriteFile(sentinel, []byte("#!/bin/sh\nprintf ran > \"$WEFTY_GUARD_MARKER\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	f.binary = sentinel
	f.environment = append(f.environment, "WEFTY_GUARD_MARKER="+marker)
	f.setup.setup(t, setupRequest{})
	alternate := f.setup.profile(t)
	alternate.L1Address = defaultSetupL1Address
	badProfile := filepath.Join(t.TempDir(), "bad.json")
	if err := connprofile.Write(badProfile, alternate); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"--l1", defaultSetupL1Address, "node", "doctor"},
		{"--l3", defaultSetupL3Address, "status"},
		{"--profile", badProfile, "node", "doctor"},
		{"--profile=" + badProfile, "node", "doctor"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			if err := f.diagnosticEndpointError(args); err == nil {
				f.run(t, args...) // A harmless sentinel, never the real CLI or network.
				if _, err := os.Stat(marker); err == nil {
					t.Fatal("effective default-port endpoint bypassed guard and executed the sentinel")
				}
				t.Fatal("effective default-port endpoint passed the pre-execution guard")
			}
		})
	}
	// Existing equals/env forms must remain rejected too.
	if err := f.diagnosticEndpointError([]string{"--l1=" + defaultSetupL1Address, "doctor"}); err == nil {
		t.Fatal("equals default passed")
	}
	f.environment = append(f.environment, "WEFTY_L1_ADDR="+defaultSetupL1Address)
	if err := f.diagnosticEndpointError([]string{"doctor"}); err == nil {
		t.Fatal("environment default passed")
	}
}

func TestRound2UnreadableMaintenanceWithoutCatalog(t *testing.T) {
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	paths, _ := resolveSetupPaths(home)
	if err := os.MkdirAll(paths.data, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(paths.data, agentinstall.MaintenanceFile), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	got := collectRollingStatus(paths.nodeConfig, l1.NodeList{}, true, time.Now())
	if len(got.findings()) == 0 || !strings.Contains(strings.Join(got.EvidenceErrors, " "), "maintenance") {
		t.Fatalf("unreadable maintenance disappeared: %+v", got)
	}
	reviewDiagnosticOutputs(t, got, "unexpected end of JSON input", "")
}

func TestRound2EffectiveEndpointPrecedence(t *testing.T) {
	f := newPlainBinaryFixture(t, "")
	f.setup.setup(t, setupRequest{})
	safe := f.setup.host.l1Address
	safeProfile := f.setup.profile(t)
	safePath := filepath.Join(t.TempDir(), "safe.json")
	if err := connprofile.Write(safePath, safeProfile); err != nil {
		t.Fatal(err)
	}
	bad := safeProfile
	bad.L1Address = defaultSetupL1Address
	badPath := filepath.Join(t.TempDir(), "bad.json")
	if err := connprofile.Write(badPath, bad); err != nil {
		t.Fatal(err)
	}
	f.environment = append(f.environment, "WEFTY_L1_ADDR="+defaultSetupL1Address)
	for _, args := range [][]string{{"--l1", safe, "doctor"}, {"--l1=" + safe, "--profile", badPath, "node", "doctor"}, {"-l1", safe, "-profile", safePath, "status"}} {
		if err := f.diagnosticEndpointError(args); err != nil {
			t.Errorf("explicit safe override rejected: %v: %v", args, err)
		}
	}
	f.environment = f.environment[:len(f.environment)-1]
	f.environment = append(f.environment, connprofile.EnvironmentVariable+"="+badPath)
	if err := f.diagnosticEndpointError([]string{"doctor"}); err == nil {
		t.Fatal("selected environment profile escaped guard")
	}
	if err := f.diagnosticEndpointError([]string{"--profile", safePath, "doctor"}); err != nil {
		t.Fatal("safe selected profile did not override environment profile", err)
	}
}
