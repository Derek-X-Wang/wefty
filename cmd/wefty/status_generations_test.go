package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/l1"
)

func generationStatusFixture(now time.Time, retiring int) l1.NodeList {
	node := readyNode("node")
	node.AgentGenerationID = "primary"
	node.Generations = []l1.NodeGenerationObservation{{AgentGenerationID: "primary", Role: l1.NodeGenerationPrimary, Version: "new", CreatedAt: now.Add(-time.Minute), Attempts: []l1.NodeGenerationAttempt{}}}
	for i := 0; i < retiring; i++ {
		at := now.Add(-time.Second)
		retired := now.Add(-time.Minute)
		node.Generations = append(node.Generations, l1.NodeGenerationObservation{AgentGenerationID: fmt.Sprintf("old-%d", i), Role: l1.NodeGenerationRetiring, Version: "old", CreatedAt: now.Add(-time.Hour), RetirementStartedAt: &retired, SelectedProtocol: 1, Attempts: []l1.NodeGenerationAttempt{{JobID: fmt.Sprintf("job-%d", i), AttemptID: fmt.Sprintf("attempt-%d", i), RunID: fmt.Sprintf("run-%d", i), State: generationAttemptState(contract.AttemptRunning), LastRenewedAt: &at, LeaseExpiresAt: generationDeadline(now.Add(time.Minute)), LeaseLive: generationLeaseLive(true)}}})
	}
	return l1.NodeList{Nodes: []l1.Node{node}}
}

func generationLeaseLive(live bool) *bool { return &live }

func TestGenerationLeaseStatusIgnoresCLIClockSkew(t *testing.T) {
	l1Now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		skew time.Duration
		live bool
		want string
	}{
		{"ahead-live", time.Hour, true, "healthy"},
		{"behind-expired", -time.Hour, false, "degraded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var a l1.NodeGenerationAttempt
			payload := fmt.Sprintf(`{"job_id":"job","attempt_id":"attempt","state":"running","lease_expires_at":%q,"lease_live":%t}`, l1Now.Add(time.Minute).Format(time.RFC3339Nano), tc.live)
			if err := json.Unmarshal([]byte(payload), &a); err != nil {
				t.Fatal(err)
			}
			g := projectGeneration(readyNode("node"), l1.NodeGenerationObservation{Role: l1.NodeGenerationRetiring, Attempts: []l1.NodeGenerationAttempt{a}}, l1Now.Add(tc.skew))
			if g.RenewalHealth != tc.want {
				t.Fatalf("L1 lease_live=%t, CLI skew=%s: health=%s want=%s", tc.live, tc.skew, g.RenewalHealth, tc.want)
			}
		})
	}
}

func TestFreshAttemptRenewalStatusAndDoctor(t *testing.T) {
	for _, tc := range []struct {
		name   string
		state  contract.AttemptState
		lease  *time.Time
		health string
		code   string
	}{
		{name: "inside-first-lease", state: contract.AttemptRunning, lease: generationDeadline(time.Now().Add(time.Hour)), health: "healthy"},
		{name: "expired-first-lease", state: contract.AttemptRunning, lease: generationDeadline(time.Now().Add(-time.Hour)), health: "degraded", code: "retiring_lease_stale"},
		{name: "missing-lease", state: contract.AttemptRunning, health: "unverified", code: "generation_renewal_unverified"},
		{name: "claimed-without-renewal", state: contract.AttemptClaimed, lease: generationDeadline(time.Now().Add(time.Hour)), health: "healthy"},
		{name: "claimed-expired-first-lease", state: contract.AttemptClaimed, lease: generationDeadline(time.Now().Add(-time.Hour)), health: "degraded", code: "retiring_lease_stale"},
		{name: "claimed-missing-lease", state: contract.AttemptClaimed, health: "unverified", code: "generation_renewal_unverified"},
		{name: "awaiting-input-without-renewal", state: contract.AttemptAwaitingInput, lease: generationDeadline(time.Now().Add(time.Hour)), health: "healthy"},
		{name: "awaiting-input-expired-first-lease", state: contract.AttemptAwaitingInput, lease: generationDeadline(time.Now().Add(-time.Hour)), health: "degraded", code: "retiring_lease_stale"},
		{name: "awaiting-input-missing-lease", state: contract.AttemptAwaitingInput, health: "unverified", code: "generation_renewal_unverified"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, entry := range []string{"status", "doctor"} {
				t.Run(entry, func(t *testing.T) {
					nodes := round2Nodes(t, true)
					nodes.Nodes[0].Generations[0].Attempts = []l1.NodeGenerationAttempt{{
						JobID: "fresh-job", AttemptID: "fresh-attempt",
						State: generationAttemptState(tc.state), LeaseExpiresAt: tc.lease,
					}}
					if tc.lease != nil {
						nodes.Nodes[0].Generations[0].Attempts[0].LeaseLive = generationLeaseLive(tc.health == "healthy")
					}
					wantHealthy := tc.health == "healthy"
					if entry == "status" {
						h := newStatusHarness(t)
						h.nodes = nodes
						var out, stderr bytes.Buffer
						err := executeStatus(t.Context(), h.clients(), true, nil, &out, &stderr)
						var status clusterStatus
						if decodeErr := json.Unmarshal(out.Bytes(), &status); decodeErr != nil {
							t.Fatal(decodeErr)
						}
						if (err == nil) != wantHealthy || status.Ready != wantHealthy || len(status.Generations) != 1 || status.Generations[0].RenewalHealth != tc.health {
							t.Fatalf("first lease status: err=%v status=%s", err, out.String())
						}
						if wantHealthy {
							if status.Verdict != "ready" || len(status.Reasons) != 0 {
								t.Fatalf("healthy first lease verdict: %s", out.String())
							}
						} else if !slices.ContainsFunc(status.Reasons, func(r statusReason) bool { return r.Code == tc.code }) {
							t.Fatalf("missing %s: %s", tc.code, out.String())
						}
						return
					}
					f, paths := round2DoctorFixture(t, &nodes)
					report := runFixturePlainDoctor(t, f.host, globalOptions{nodeConfigPath: paths.nodeConfig, profilePath: paths.profile})
					if report.Healthy != wantHealthy || len(report.Generations) != 1 || report.Generations[0].RenewalHealth != tc.health {
						t.Fatalf("first lease doctor: %+v", report)
					}
					check, severity := tc.code, "warning"
					if wantHealthy {
						check, severity = "rolling-upgrade", "ok"
					} else if tc.health == "degraded" {
						severity = "error"
					}
					if !slices.ContainsFunc(report.Findings, func(f plainDoctorFinding) bool { return f.Check == check && f.Severity == severity }) {
						t.Fatalf("doctor omitted %s %s: %+v", check, severity, report.Findings)
					}
				})
			}
		})
	}
}

func TestPrimaryGenerationReregistrationRecoversStatus(t *testing.T) {
	clock := &nodeCLIClock{now: time.Now().UTC()}
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "rejoin.sqlite"), l1.StoreOptions{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registration := contract.NodeRegistration{NodeID: "node", BootSessionID: "boot", AgentGenerationID: "generation", OS: "darwin", Architecture: "arm64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true, "upgrade:retirement:v1": true}}
	register := func() {
		t.Helper()
		if _, err := store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, registration, l1.NodePolicy{MaxOneshotSlots: 1}, true); err != nil {
			t.Fatal(err)
		}
	}
	register()
	clock.Advance(l1.DefaultNodeDeadAfter)
	if _, err := store.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, rejoin := range []bool{false, true} {
		if rejoin {
			register()
		}
		nodes, err := store.ListNodes(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		h := newStatusHarness(t)
		h.nodes = l1.NodeList{Nodes: nodes}
		var out, stderr bytes.Buffer
		err = executeStatus(t.Context(), h.clients(), true, nil, &out, &stderr)
		if rejoin && err != nil {
			t.Fatal(err)
		}
		var status clusterStatus
		if err := json.Unmarshal(out.Bytes(), &status); err != nil {
			t.Fatal(err)
		}
		if status.Ready != rejoin || len(status.Nodes) != 1 || status.Nodes[0].AcceptsOneshot != rejoin {
			t.Fatalf("rejoin=%t status=%s", rejoin, out.String())
		}
		if rejoin && (len(status.Generations) != 1 || status.Generations[0].ID != "generation" || status.Generations[0].RenewalHealth != "healthy" || len(status.Generations[0].Problems) != 0) {
			t.Fatalf("generation health after rejoin=%s", out.String())
		}
	}
}

func TestRollingStatusClosedGenerationSummary(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		mutate    func(*rollingStatusFields)
		summary   int
		detailIDs []string
	}{
		{name: "healthy", summary: 3, detailIDs: []string{"primary"}},
		{name: "problem-with-healthy-renewal", summary: 2, detailIDs: []string{"primary", "old-0"}, mutate: func(r *rollingStatusFields) {
			r.Generations[1].Problems = []statusReason{{Code: "retiring_attempt_problem", Detail: "Run run-0: interrupted"}}
		}},
		{name: "degraded-renewal", summary: 2, detailIDs: []string{"primary", "old-0"}, mutate: func(r *rollingStatusFields) {
			r.Generations[1].RenewalHealth = "degraded"
		}},
		{name: "unverified-renewal", summary: 2, detailIDs: []string{"primary", "old-0"}, mutate: func(r *rollingStatusFields) {
			r.Generations[1].RenewalHealth = "unverified"
		}},
		{name: "staged", summary: 2, detailIDs: []string{"primary", "old-0"}, mutate: func(r *rollingStatusFields) {
			r.Generations[1].Role = "staged"
		}},
		{name: "retiring", summary: 2, detailIDs: []string{"primary", "old-0"}, mutate: func(r *rollingStatusFields) {
			r.Generations[1].Role = "retiring"
		}},
		{name: "history", summary: 2, detailIDs: []string{"primary", "old-0"}, mutate: func(r *rollingStatusFields) {
			r.Generations[1].Role = "history"
		}},
		{name: "no-closed-generations", detailIDs: []string{"primary"}, mutate: func(r *rollingStatusFields) {
			r.Generations = r.Generations[:1]
		}},
		{name: "no-healthy-closed-generations", detailIDs: []string{"primary", "old-0", "old-1", "old-2"}, mutate: func(r *rollingStatusFields) {
			for i := 1; i < len(r.Generations); i++ {
				r.Generations[i].RenewalHealth = "unverified"
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodes := generationStatusFixture(now, 3)
			for i := 1; i < len(nodes.Nodes[0].Generations); i++ {
				nodes.Nodes[0].Generations[i].Role = l1.NodeGenerationClosed
				nodes.Nodes[0].Generations[i].Attempts[0].State = generationAttemptState(contract.AttemptSucceeded)
			}
			r := collectRollingStatus(t.Context(), "", nodes, true, now)
			if tc.mutate != nil {
				tc.mutate(&r)
			}
			for _, command := range []string{"status", "doctor"} {
				for _, asJSON := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/json=%t", command, asJSON), func(t *testing.T) {
						var out bytes.Buffer
						var err error
						if command == "status" {
							err = writeStatusResult(&out, clusterStatus{rollingStatusFields: r, Ready: true, Verdict: "ready"}, asJSON)
						} else {
							err = writeDoctorReport(&out, plainDoctorReport{rollingStatusFields: r, Healthy: true}, asJSON)
						}
						if err != nil {
							t.Fatal(err)
						}
						if asJSON {
							var decoded rollingStatusFields
							if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
								t.Fatal(err)
							}
							if !reflect.DeepEqual(decoded.Generations, r.Generations) {
								t.Fatalf("JSON lost generation detail: %s", out.String())
							}
							return
						}
						for _, g := range r.Generations {
							line := "generation " + g.ID + " " + g.Role + ": version "
							if strings.Contains(out.String(), line) != slices.Contains(tc.detailIDs, g.ID) {
								t.Fatalf("wrong generation detail for %s: %s", g.ID, out.String())
							}
							for _, problem := range g.Problems {
								if !strings.Contains(out.String(), problem.Detail) {
									t.Fatalf("hidden problem: %s", out.String())
								}
							}
						}
						if tc.summary == 0 {
							if strings.Contains(out.String(), "closed generations:") {
								t.Fatalf("unexpected summary: %s", out.String())
							}
						} else if strings.Count(out.String(), "closed generations:") != 1 ||
							!strings.Contains(out.String(), fmt.Sprintf("\nclosed generations: %d (healthy)\n", tc.summary)) {
							t.Fatalf("wrong healthy closed-generation summary: %s", out.String())
						}
					})
				}
			}
		})
	}
}

func TestRollingStatusTextAndJSONFixtures(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		retiring  int
		mutate    func(*l1.NodeList)
		reachable bool
		state     string
		problem   bool
		contains  string
	}{
		{name: "legacy", reachable: true},
		{name: "primary-only", reachable: true, state: "complete", contains: "upgrade complete"},
		{name: "healthy-retirement", retiring: 1, reachable: true, state: "complete", contains: "older generations finishing 1 Runs"},
		{name: "completed-resident-does-not-stale-live-renewal", retiring: 1, reachable: true, state: "complete", contains: "renewal healthy; last renewal 2026-10-24T11:59:59Z; deadline 2026-10-24T12:01:00Z", mutate: func(n *l1.NodeList) {
			old := now.Add(-2 * time.Hour)
			n.Nodes[0].Generations[1].Attempts = append(n.Nodes[0].Generations[1].Attempts, l1.NodeGenerationAttempt{JobID: "done", AttemptID: "done", RunID: "run-done", State: generationAttemptState(contract.AttemptSucceeded), LastRenewedAt: &old, LeaseExpiresAt: generationDeadline(now.Add(-time.Hour))})
		}},
		{name: "two-retiring", retiring: 2, reachable: true, state: "complete", contains: "older generations finishing 2 Runs"},
		{name: "three-retiring", retiring: 3, reachable: true, state: "complete", contains: "older generations finishing 3 Runs"},
		{name: "distinct-runs", retiring: 3, reachable: true, state: "complete", contains: "older generations finishing 1 Runs", mutate: func(n *l1.NodeList) {
			for i := 1; i < len(n.Nodes[0].Generations); i++ {
				n.Nodes[0].Generations[i].Attempts[0].RunID = "one-run"
			}
		}},
		{name: "stale-retiree", retiring: 1, reachable: true, state: "complete", problem: true, contains: "Run run-0 (Attempt attempt-0): stale renewal", mutate: func(n *l1.NodeList) {
			n.Nodes[0].Generations[1].Attempts[0].LeaseExpiresAt = generationDeadline(now.Add(-time.Second))
			n.Nodes[0].Generations[1].Attempts[0].LeaseLive = generationLeaseLive(false)
		}},
		{name: "interrupted-retiree", retiring: 1, reachable: true, state: "complete", problem: true, contains: "Run run-0 (Attempt attempt-0): interrupted", mutate: func(n *l1.NodeList) {
			n.Nodes[0].Generations[1].Attempts[0].State = generationAttemptState(contract.AttemptInterrupted)
		}},
		{name: "prior-boot-retiree", retiring: 1, reachable: true, state: "complete", problem: true, contains: "Run run-0 (Attempt attempt-0): interrupted after host reboot", mutate: func(n *l1.NodeList) { n.Nodes[0].Generations[1].PreviousSystemBoot = true }},
		{name: "lost-retiree", retiring: 1, reachable: true, state: "complete", problem: true, contains: "Run run-0 (Attempt attempt-0): lost", mutate: func(n *l1.NodeList) {
			n.Nodes[0].Generations[1].Attempts[0].State = generationAttemptState(contract.AttemptLost)
		}},
		{name: "failed-retiree", retiring: 1, reachable: true, state: "complete", problem: true, contains: "Run run-0 (Attempt attempt-0): failed", mutate: func(n *l1.NodeList) {
			n.Nodes[0].Generations[1].Attempts[0].State = generationAttemptState(contract.AttemptFailed)
		}},
		{name: "lease-unknown", retiring: 1, reachable: true, state: "complete", problem: true, contains: "renewal evidence unknown", mutate: func(n *l1.NodeList) { n.Nodes[0].Generations[1].Attempts[0].LeaseExpiresAt = nil }},
		{name: "attempt-unknown", retiring: 1, reachable: true, state: "complete", problem: true, contains: "renewal evidence unknown", mutate: func(n *l1.NodeList) { n.Nodes[0].Generations[1].Attempts[0].State = nil }},
		{name: "no-run", retiring: 1, reachable: true, state: "complete", contains: "Attempts without an L3 Run: attempt-0", mutate: func(n *l1.NodeList) { n.Nodes[0].Generations[1].Attempts[0].RunID = "" }},
		{name: "unreachable", retiring: 1, reachable: false, state: "unverified", problem: true, contains: "unverified"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodes := generationStatusFixture(now, tc.retiring)
			if tc.name == "legacy" {
				nodes.Nodes[0].Generations = nil
				nodes.Nodes[0].AgentGenerationID = ""
			}
			if tc.mutate != nil {
				tc.mutate(&nodes)
			}
			r := collectRollingStatus(t.Context(), "", nodes, tc.reachable, now)
			if r.UpgradeState != tc.state || (len(r.findings()) > 0) != tc.problem {
				t.Fatalf("projection=%+v findings=%+v", r, r.findings())
			}
			if len(r.Generations) != len(nodes.Nodes[0].Generations) {
				t.Fatal("truncated generation inventory")
			}
			for _, asJSON := range []bool{false, true} {
				var out bytes.Buffer
				status := clusterStatus{rollingStatusFields: r, Ready: !tc.problem, Verdict: "ready", Capacity: map[string]int{}}
				err := writeStatusResult(&out, status, asJSON)
				if (err != nil) != tc.problem {
					t.Fatalf("status err=%v", err)
				}
				if asJSON {
					var decoded clusterStatus
					if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
						t.Fatal(err)
					}
					if decoded.UpgradeState != r.UpgradeState || len(decoded.Generations) != len(r.Generations) {
						t.Fatal(out.String())
					}
				} else if !strings.Contains(out.String(), tc.contains) {
					t.Fatal(out.String())
				}
				out.Reset()
				report := plainDoctorReport{rollingStatusFields: r, Healthy: !tc.problem, Findings: []plainDoctorFinding{}}
				if err := writeDoctorReport(&out, report, asJSON); err != nil {
					t.Fatal(err)
				}
				if asJSON {
					var decoded plainDoctorReport
					if err := json.Unmarshal(out.Bytes(), &decoded); err != nil {
						t.Fatal(err)
					}
					if decoded.Healthy == tc.problem || decoded.UpgradeState != r.UpgradeState {
						t.Fatal(out.String())
					}
				} else if !strings.Contains(out.String(), tc.contains) {
					t.Fatal(out.String())
				}
			}
		})
	}
}

func TestRollingStatusReceiptPhasesAndReadOnly(t *testing.T) {
	for _, phase := range []string{"prepared", "candidate_loading", "candidate_ready", "promotion_requested", "promotion_committed", "retirement_acknowledged", "installation_complete", "rolling_back", "complete", "rolled_back", "reboot_reconciled"} {
		for _, active := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/active=%t", phase, active), func(t *testing.T) {
				home := t.TempDir()
				home, _ = filepath.EvalSymlinks(home)
				paths, _ := resolveSetupPaths(home)
				if err := os.MkdirAll(paths.config, 0700); err != nil {
					t.Fatal(err)
				}
				r := upgradeRollingReceipt{Version: 1, OperationID: "operation", NodeID: "node", Candidate: agentinstall.Generation{ID: "primary", Version: "new"}}
				if err := writeUpgradeRolling(paths, &r, phase); err != nil {
					t.Fatal(err)
				}
				if !active {
					release, err := lockSetup(paths, true)
					if err != nil {
						t.Fatal(err)
					}
					release()
				}
				if active {
					unlock, err := lockSetup(paths, true)
					if err != nil {
						t.Fatal(err)
					}
					defer unlock()
				}
				before := diagnosticTree(t, home)
				status := collectRollingStatus(t.Context(), paths.nodeConfig, generationStatusFixture(time.Now(), 1), true, time.Now())
				after := diagnosticTree(t, home)
				if before != after {
					t.Fatal("diagnostic wrote local files")
				}
				want := "interrupted"
				if active {
					want = "cutover"
				}
				if phase == "complete" || phase == "rolled_back" || phase == "reboot_reconciled" {
					want = "complete"
				}
				if status.UpgradeState != want {
					t.Fatalf("got %+v want %s", status, want)
				}
				for _, jsonOutput := range []bool{false, true} {
					var out bytes.Buffer
					if jsonOutput {
						if err := writeJSON(&out, status); err != nil {
							t.Fatal(err)
						}
						if !strings.Contains(out.String(), `"upgrade_state": "`+want+`"`) {
							t.Fatal(out.String())
						}
					} else {
						if err := writeRollingStatus(&out, status); err != nil {
							t.Fatal(err)
						}
						if want == "interrupted" && !strings.Contains(out.String(), "wefty upgrade --rolling") {
							t.Fatal(out.String())
						}
					}
				}
			})
		}
	}
}
func diagnosticTree(t *testing.T, root string) string {
	t.Helper()
	var result strings.Builder
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		fmt.Fprintf(&result, "%s %s %d %s\n", path, info.Mode(), info.Size(), info.ModTime())
		if !d.IsDir() {
			payload, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			result.Write(payload)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return result.String()
}
func TestRollingStatusUnreadableEvidenceContinues(t *testing.T) {
	for _, kind := range []string{"invalid-json", "unreadable", "invalid-catalog"} {
		t.Run(kind, func(t *testing.T) {
			home := t.TempDir()
			home, _ = filepath.EvalSymlinks(home)
			paths, _ := resolveSetupPaths(home)
			if err := os.MkdirAll(paths.config, 0700); err != nil {
				t.Fatal(err)
			}
			name := filepath.Join(paths.config, upgradeRollingFile)
			payload := "{"
			mode := os.FileMode(0600)
			if kind == "unreadable" {
				payload = `{"version":1,"operation_id":"op","node_id":"node","phase":"complete"}`
				mode = 0000
			}
			if kind == "invalid-catalog" {
				name = filepath.Join(paths.data, agentinstall.Directory, "catalog.json")
				if err := os.MkdirAll(filepath.Dir(name), 0700); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(name, []byte(payload), mode); err != nil {
				t.Fatal(err)
			}
			got := collectRollingStatus(t.Context(), paths.nodeConfig, generationStatusFixture(time.Now(), 3), true, time.Now())
			if got.UpgradeState != "unverified" || len(got.EvidenceErrors) != 1 || len(got.Generations) != 4 {
				t.Fatalf("%+v", got)
			}
			for _, jsonOutput := range []bool{false, true} {
				var out bytes.Buffer
				var err error
				if jsonOutput {
					err = writeJSON(&out, got)
				} else {
					err = writeRollingStatus(&out, got)
				}
				if err != nil || !strings.Contains(out.String(), got.EvidenceErrors[0]) {
					t.Fatalf("%s %v", out.String(), err)
				}
			}
		})
	}
}

func TestRollingStatusHealthyPrimaryCannotHideRetiringProblems(t *testing.T) {
	for _, cutover := range []bool{false, true} {
		t.Run(fmt.Sprint(cutover), func(t *testing.T) {
			h := newStatusHarness(t)
			h.nodes = generationStatusFixture(time.Now(), 1)
			h.nodes.Nodes[0].Generations[1].Attempts[0].State = generationAttemptState(contract.AttemptInterrupted)
			clients := h.clients()
			if cutover {
				home, _ := filepath.EvalSymlinks(t.TempDir())
				paths, _ := resolveMaintenancePaths(home)
				if err := os.MkdirAll(paths.config, 0700); err != nil {
					t.Fatal(err)
				}
				receipt := upgradeRollingReceipt{Version: 1, OperationID: "active", NodeID: "node"}
				if err := writeUpgradeRolling(paths, &receipt, "promotion_requested"); err != nil {
					t.Fatal(err)
				}
				release, err := lockSetup(paths, true)
				if err != nil {
					t.Fatal(err)
				}
				defer release()
				clients.nodeConfigPath = paths.nodeConfig
			}
			got := collectStatus(t.Context(), clients)
			if got.Ready {
				t.Fatalf("healthy primary hid interrupted retiree: %+v", got)
			}
			var found bool
			for _, r := range got.Reasons {
				found = found || strings.Contains(r.Detail, "Run run-0")
			}
			if !found {
				t.Fatalf("missing Run ID: %+v", got)
			}
			for _, asJSON := range []bool{false, true} {
				var out bytes.Buffer
				err := writeStatusResult(&out, got, asJSON)
				if commandExitCode(err) != exitNotReady || !strings.Contains(out.String(), "run-0") {
					t.Fatalf("exit %v output %s", err, out.String())
				}
			}
		})
	}
}

func TestRollingStatusDoctorReadsWithoutMutation(t *testing.T) {
	f := newSetupFixture(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer server.Close()
	f.host.l1Address = strings.TrimPrefix(server.URL, "http://")
	f.host.l3Address = f.host.l1Address
	f.setup(t, setupRequest{})
	paths, _ := resolveSetupPaths(f.home)
	receipt := upgradeRollingReceipt{Version: 1, OperationID: "interrupted", NodeID: "node"}
	if err := writeUpgradeRolling(paths, &receipt, "candidate_loading"); err != nil {
		t.Fatal(err)
	}
	before := diagnosticTree(t, f.home)
	mutations := len(f.launchd.mutating())
	report := runFixturePlainDoctor(t, f.host, globalOptions{nodeConfigPath: paths.nodeConfig, profilePath: paths.profile})
	after := diagnosticTree(t, f.home)
	if before != after || len(f.launchd.mutating()) != mutations {
		t.Fatal("doctor mutated files or launchd")
	}
	if report.UpgradeState != "unverified" || report.Healthy {
		t.Fatalf("unreachable authority was called healthy: %+v", report)
	}
}

// JSON captures come from ea087ec4's unchanged report struct definitions.
// Existing TestFleetPlainHumanCompatibility also pins the full legacy text.
func TestRollingStatusLegacyJSONBaseBytes(t *testing.T) {
	for _, kind := range []string{"status", "doctor"} {
		t.Run(kind, func(t *testing.T) {
			var out bytes.Buffer
			if kind == "status" {
				r := clusterStatus{Ready: true, Verdict: "ready", Identity: fabricStatus{Kind: identityMachine, Detail: "machine principal"}, Services: []serviceStatus{}, Nodes: []nodeStatus{}, Capacity: map[string]int{"process": 4, "oci": 0}, CheckedAt: time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)}
				if err := writeStatusResult(&out, r, true); err != nil {
					t.Fatal(err)
				}
			} else {
				r := plainDoctorReport{Version: 1, Install: "plain-process", Healthy: true, Findings: []plainDoctorFinding{{"local-node", "ok", "verified", ""}}}
				if err := writeDoctorReport(&out, r, true); err != nil {
					t.Fatal(err)
				}
			}
			assertPlainBaseBytes(t, "plain-"+kind+"-json-ea087ec4.json", out.String())
		})
	}
}

func generationDeadline(at time.Time) *time.Time { return &at }

func TestRollingStatusCatalogAndUnreadableRetirement(t *testing.T) {
	home, _ := filepath.EvalSymlinks(t.TempDir())
	paths, _ := resolveMaintenancePaths(home)
	root := filepath.Join(paths.data, agentinstall.Directory)
	for _, path := range []string{root, paths.launchAgents} {
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC()
	nodes := generationStatusFixture(now, 1)
	primary, retiring := strings.Repeat("a", 32), strings.Repeat("b", 32)
	nodes.Nodes[0].AgentGenerationID = primary
	nodes.Nodes[0].Generations[0].AgentGenerationID = primary
	nodes.Nodes[0].Generations[1].AgentGenerationID = retiring
	catalog := agentinstall.Catalog{Version: 1, NodeID: "node", Revision: 1, Primary: primary, Generations: map[string]agentinstall.Generation{}}
	for _, id := range []string{primary, retiring} {
		role := "primary"
		if id == retiring {
			role = "retiring"
		}
		label := "dev.wefty.node-agent.g-" + id
		catalog.Generations[id] = agentinstall.Generation{ID: id, Role: role, Version: "installed", Digest: strings.Repeat("c", 64), Label: label, Executable: filepath.Join(root, id, "wefty-agent"), Plist: filepath.Join(paths.launchAgents, label+".plist"), CreatedAt: now.Add(-time.Hour), StartBootID: rollingTestBoot, CandidateBootSessionID: strings.Repeat("d", 32), ProtocolVersions: []int{1}}
		if err := os.MkdirAll(filepath.Join(root, id), 0700); err != nil {
			t.Fatal(err)
		}
	}
	payload, _ := json.Marshal(catalog)
	if err := os.WriteFile(filepath.Join(root, "catalog.json"), payload, 0600); err != nil {
		t.Fatal(err)
	}
	got := collectRollingStatus(t.Context(), paths.nodeConfig, nodes, true, now)
	if got.UpgradeState != "complete" || len(got.findings()) != 0 || len(got.Generations) != 2 {
		t.Fatalf("healthy catalog: %+v", got)
	}
	// Missing authoritative reads must retain both local generations as unknown.
	unknown := collectRollingStatus(t.Context(), paths.nodeConfig, l1.NodeList{}, false, now)
	if unknown.UpgradeState != "unverified" || len(unknown.Generations) != 2 {
		t.Fatalf("%+v", unknown)
	}
	for _, g := range unknown.Generations {
		if g.Capacity != nil || g.RenewalHealth != "unverified" || g.RemainingRunIDs != nil {
			t.Fatalf("invented healthy evidence: %+v", g)
		}
	}
	name := filepath.Join(root, retiring, "retirement-ack.json")
	if err := os.WriteFile(name, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	before := diagnosticTree(t, home)
	got = collectRollingStatus(t.Context(), paths.nodeConfig, nodes, true, now)
	if got.UpgradeState != "unverified" || len(got.Generations) != 2 || len(got.EvidenceErrors) != 1 || !strings.Contains(got.EvidenceErrors[0], "retirement acknowledgment") {
		t.Fatalf("%+v", got)
	}
	if after := diagnosticTree(t, home); after != before {
		t.Fatal("read mutated generation state")
	}
}

func generationAttemptState(state contract.AttemptState) *contract.AttemptState { return &state }

func TestRollingStatusCountsOnlyVerifiedNonterminalRuns(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state contract.RunState
		fail  bool
		count int
	}{
		{"running", contract.RunRunning, false, 1},
		{"succeeded-before-attempt-teardown", contract.RunSucceeded, false, 0},
		{"failed-before-attempt-teardown", contract.RunFailed, false, 0},
		{"unreachable-ledger", "", true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/v1/runs/run-0" {
					t.Errorf("unexpected read %s %s", r.Method, r.URL.Path)
				}
				calls++
				if tc.fail {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				_ = json.NewEncoder(w).Encode(contract.RunRecord{RunID: "run-0", Status: tc.state})
			}))
			defer server.Close()
			clients := &apiClients{l3: statusClient(t, "L3", "wefty://run-ledger", server)}
			nodes := generationStatusFixture(time.Now(), 2)
			nodes.Nodes[0].Generations[2].Attempts[0].RunID = "run-0"
			got := collectRollingStatus(t.Context(), "", nodes, true, time.Now())
			verifyRollingRuns(t.Context(), clients, &got)
			if calls != 1 {
				t.Fatalf("duplicate Run read %d", calls)
			}
			if tc.fail {
				if got.UpgradeState != "unverified" || got.Generations[1].RemainingRunIDs != nil || len(got.findings()) == 0 {
					t.Fatalf("%+v", got)
				}
			} else if len(got.Generations[1].RemainingRunIDs) != tc.count || !strings.Contains(got.UpgradeDetail, fmt.Sprintf("finishing %d Runs", tc.count)) {
				t.Fatalf("%+v", got)
			}
			// A terminal Run does not erase its still-resident Attempt or capacity.
			if len(got.Generations[1].RemainingAttemptIDs) != 1 || got.Generations[1].Capacity == nil {
				t.Fatal("teardown Attempt disappeared")
			}
			for _, asJSON := range []bool{false, true} {
				var out bytes.Buffer
				status := clusterStatus{rollingStatusFields: got, Ready: !tc.fail, Verdict: "ready"}
				err := writeStatusResult(&out, status, asJSON)
				if (err != nil) != tc.fail {
					t.Fatal(err)
				}
				var doc bytes.Buffer
				if err := writeDoctorReport(&doc, plainDoctorReport{rollingStatusFields: got, Healthy: !tc.fail}, asJSON); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestRollingStatusCutoverPreservesOwnerDisable(t *testing.T) {
	h := newStatusHarness(t)
	h.nodes = generationStatusFixture(time.Now(), 0)
	h.nodes.Nodes[0].ClaimsEnabled = false
	h.nodes.Nodes[0].IntentReason = "owner maintenance"
	home, _ := filepath.EvalSymlinks(t.TempDir())
	paths, _ := resolveSetupPaths(home)
	if err := os.MkdirAll(paths.config, 0700); err != nil {
		t.Fatal(err)
	}
	receipt := upgradeRollingReceipt{Version: 1, OperationID: "active", NodeID: "node"}
	if err := writeUpgradeRolling(paths, &receipt, "promotion_requested"); err != nil {
		t.Fatal(err)
	}
	release, err := lockSetup(paths, true)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	clients := h.clients()
	clients.nodeConfigPath = paths.nodeConfig
	got := collectStatus(t.Context(), clients)
	if got.UpgradeState != "cutover" || got.Ready {
		t.Fatalf("cutover hid owner admission: %+v", got)
	}
	found := false
	for _, reason := range got.Reasons {
		found = found || strings.Contains(reason.Detail, "claims disabled")
	}
	if !found {
		t.Fatalf("owner disable reason missing: %+v", got.Reasons)
	}
}

func TestRollingStatusCompletedReceiptRequiresWinnerEvidence(t *testing.T) {
	for _, kind := range []string{"missing-candidate", "wrong-primary", "missing-commit"} {
		t.Run(kind, func(t *testing.T) {
			home, _ := filepath.EvalSymlinks(t.TempDir())
			paths, _ := resolveSetupPaths(home)
			if err := os.MkdirAll(paths.config, 0700); err != nil {
				t.Fatal(err)
			}
			receipt := upgradeRollingReceipt{Version: 1, OperationID: "operation", NodeID: "node"}
			if kind == "wrong-primary" {
				receipt.Candidate.ID = "other"
			}
			if kind == "missing-commit" {
				receipt.Candidate.ID = "primary"
				receipt.Promotion = &l1.PromoteNodeGenerationRequest{OperationID: "operation", CandidateGenerationID: "primary"}
			}
			if err := writeUpgradeRolling(paths, &receipt, "complete"); err != nil {
				t.Fatal(err)
			}
			got := collectRollingStatus(t.Context(), paths.nodeConfig, generationStatusFixture(time.Now(), 3), true, time.Now())
			if got.UpgradeState != "unverified" || len(got.EvidenceErrors) == 0 || len(got.Generations) != 4 {
				t.Fatalf("%+v", got)
			}
			for _, asJSON := range []bool{false, true} {
				var out bytes.Buffer
				if asJSON {
					if err := writeJSON(&out, got); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := writeRollingStatus(&out, got); err != nil {
						t.Fatal(err)
					}
				}
				if !strings.Contains(out.String(), "unverified") {
					t.Fatal(out.String())
				}
			}
		})
	}
}

func TestRollingStatusDoctorUsesPrimaryGenerationLabelAndPATH(t *testing.T) {
	for _, mode := range []string{"healthy", "interrupted", "missing-observations"} {
		t.Run(mode, func(t *testing.T) {
			interrupted := mode == "interrupted"
			f := newPlainBinaryFixture(t, "").setup
			now := time.Now().UTC()
			nodes := generationStatusFixture(now, 1)
			primary, retiring := strings.Repeat("a", 32), strings.Repeat("b", 32)
			node := &nodes.Nodes[0]
			node.NodeID = "test-mac"
			node.AgentGenerationID = primary
			tags, err := pinnedImageTags(nil, node.NodeID, true)
			if err != nil {
				t.Fatal(err)
			}
			node.AuthoritativeTags = tags
			node.Generations[0].AgentGenerationID = primary
			node.Generations[1].AgentGenerationID = retiring
			if interrupted {
				node.Generations[1].Attempts[0].State = generationAttemptState(contract.AttemptInterrupted)
			}
			network := plain.NewNetwork()
			participant := network.NewFabric(fabric.Identity{NodeID: "doctor-test-control"})
			listener, err := participant.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("diagnostic mutation: %s %s", r.Method, r.URL.Path)
				}
				switch r.URL.Path {
				case "/v1/nodes":
					_ = json.NewEncoder(w).Encode(nodes)
				case "/v1/nodes/test-mac":
					_ = json.NewEncoder(w).Encode(node)
				case "/v1/runs/run-0":
					_ = json.NewEncoder(w).Encode(contract.RunRecord{RunID: "run-0", Status: contract.RunRunning})
				default:
					t.Errorf("unexpected diagnostic read: %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			server.Listener.Close()
			server.Listener = listener
			server.Start()
			defer server.Close()
			f.host.l1Address = strings.TrimPrefix(server.URL, "http://")
			f.host.l3Address = f.host.l1Address
			f.setup(t, setupRequest{nodeID: "test-mac"})
			paths, _ := resolveSetupPaths(f.home)
			if mode == "missing-observations" {
				node.Generations = nil
				report := runFixturePlainDoctor(t, f.host, globalOptions{nodeConfigPath: paths.nodeConfig, profilePath: paths.profile})
				if report.Healthy || report.UpgradeState != "unverified" {
					t.Fatalf("enrolled Node without catalog or observations became healthy: %+v", report)
				}
				reviewDiagnosticOutputs(t, report.rollingStatusFields, "generation observations unavailable", "")
				return
			}
			root := filepath.Join(paths.data, agentinstall.Directory)
			if err := os.MkdirAll(root, 0700); err != nil {
				t.Fatal(err)
			}
			catalog := agentinstall.Catalog{Version: 1, NodeID: "test-mac", Revision: 1, Primary: primary, Generations: map[string]agentinstall.Generation{}}
			for _, id := range []string{primary, retiring} {
				role := "primary"
				if id == retiring {
					role = "retiring"
				}
				label := "dev.wefty.node-agent.g-" + id
				catalog.Generations[id] = agentinstall.Generation{ID: id, Role: role, Version: "v1", Digest: strings.Repeat("c", 64), Label: label, Executable: filepath.Join(root, id, "wefty-agent"), Plist: filepath.Join(paths.launchAgents, label+".plist"), CreatedAt: now.Add(-time.Hour), StartBootID: rollingTestBoot, CandidateBootSessionID: strings.Repeat("d", 32), ProtocolVersions: []int{1}}
				if err := os.MkdirAll(filepath.Join(root, id), 0700); err != nil {
					t.Fatal(err)
				}
			}
			payload, _ := json.Marshal(catalog)
			if err := os.WriteFile(filepath.Join(root, "catalog.json"), payload, 0600); err != nil {
				t.Fatal(err)
			}
			legacy := filepath.Join(paths.launchAgents, setupNodeAgentLabel+".plist")
			plist, err := os.ReadFile(legacy)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(catalog.Generations[primary].Plist, plist, 0644); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(legacy); err != nil {
				t.Fatal(err)
			}
			delete(f.launchd.loaded, setupNodeAgentLabel)
			f.launchd.loaded[catalog.Generations[primary].Label] = true
			before := diagnosticTree(t, f.home)
			mutations := len(f.launchd.mutating())
			report := runFixturePlainDoctor(t, f.host, globalOptions{nodeConfigPath: paths.nodeConfig, profilePath: paths.profile})
			if report.Healthy == interrupted || report.UpgradeState != "complete" {
				t.Fatalf("doctor=%+v", report)
			}
			if len(f.launchd.mutating()) != mutations || diagnosticTree(t, f.home) != before {
				t.Fatal("doctor mutated installed fixture")
			}
			for _, finding := range report.Findings {
				if finding.Check == setupNodeAgentLabel {
					t.Fatalf("probed removed legacy agent: %+v", finding)
				}
			}
			for _, asJSON := range []bool{false, true} {
				var out bytes.Buffer
				if err := writeDoctorReport(&out, report, asJSON); err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(out.String(), primary) || !strings.Contains(out.String(), retiring) {
					t.Fatal(out.String())
				}
			}
		})
	}
}
