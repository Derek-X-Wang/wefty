package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Use wire input so this regression also runs against the pre-field CLI.
func cancellationStatusNodes(t *testing.T, outcome string, extra contract.AttemptState) l1.NodeList {
	t.Helper()
	nodes := generationStatusFixture(time.Now(), 1)
	node := &nodes.Nodes[0]
	node.NodeID = "test-mac"
	tags, err := pinnedImageTags(nil, node.NodeID, true)
	if err != nil {
		t.Fatal(err)
	}
	node.AuthoritativeTags = tags
	a := &node.Generations[1].Attempts[0]
	a.State = generationAttemptState(contract.AttemptFailed)
	// Retained terminal Lease expiry must not turn cancellation into stale work.
	a.LeaseExpiresAt = generationDeadline(time.Now().Add(-time.Hour))
	if extra != "" {
		node.Generations[1].Attempts = append(node.Generations[1].Attempts, l1.NodeGenerationAttempt{JobID: "job-extra", AttemptID: "attempt-extra", RunID: "run-extra", State: generationAttemptState(extra)})
	}
	payload, err := json.Marshal(nodes)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(payload, &wire); err != nil {
		t.Fatal(err)
	}
	attempts := wire["nodes"].([]any)[0].(map[string]any)["generations"].([]any)[1].(map[string]any)["attempts"].([]any)
	attempts[0].(map[string]any)["job_outcome"] = outcome
	payload, err = json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(payload, &nodes); err != nil {
		t.Fatal(err)
	}
	return nodes
}

func TestCancelledRetiringRunDiagnostics(t *testing.T) {
	binary := buildPlainInstallCLI(t)
	for _, tc := range []struct {
		name, outcome string
		extra         contract.AttemptState
		problems      int
	}{
		{"cancelled-and-lost", "canceled", contract.AttemptLost, 1},
		{"cancelled-alone", "canceled", "", 0},
		{"failed-by-itself", "", "", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodes := cancellationStatusNodes(t, tc.outcome, tc.extra)
			f := newPlainBinaryFixture(t, binary)
			network := plain.NewNetwork()
			participant := network.NewFabric(fabric.Identity{NodeID: "cancel-test-control"})
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
				case "/v1/whoami":
					w.WriteHeader(http.StatusForbidden)
					_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorPersonIdentityRequired, Message: "machine principal"}})
				case "/v1/seat-approval-windows":
					_ = json.NewEncoder(w).Encode([]l1.SeatApprovalWindow{})
				case "/v1/runs":
					_ = json.NewEncoder(w).Encode(map[string]any{"runs": []any{}})
				default:
					t.Errorf("unexpected read: %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			server.Listener.Close()
			server.Listener = listener
			server.Start()
			defer server.Close()
			f.setup.host.l1Address = strings.TrimPrefix(server.URL, "http://")
			f.setup.host.l3Address = f.setup.host.l1Address
			f.setup.setup(t, setupRequest{nodeID: "test-mac"})
			before := diagnosticTree(t, f.setup.home)
			for _, command := range []string{"status", "doctor"} {
				for _, asJSON := range []bool{true, false} {
					t.Run(fmt.Sprintf("%s/json=%t", command, asJSON), func(t *testing.T) {
						args := []string{command}
						if asJSON {
							args = append([]string{"--json"}, args...)
						}
						// f.run applies #1181's effective-endpoint guard before execution.
						code, out, stderr := f.run(t, args...)
						wantCode := 0
						if tc.problems > 0 {
							wantCode = exitNotReady
						}
						if code != wantCode {
							t.Errorf("exit=%d want=%d stderr=%s output=%s", code, wantCode, stderr, out)
						}
						if asJSON {
							var report struct {
								Ready       bool                 `json:"ready"`
								Healthy     bool                 `json:"healthy"`
								Generations []generationStatus   `json:"generations"`
								Reasons     []statusReason       `json:"reasons"`
								Findings    []plainDoctorFinding `json:"findings"`
							}
							if err := json.Unmarshal([]byte(out), &report); err != nil {
								t.Fatal(err)
							}
							healthy := report.Ready
							if command == "doctor" {
								healthy = report.Healthy
							}
							if healthy != (tc.problems == 0) {
								t.Errorf("healthy=%t output=%s", healthy, out)
							}
							problems := 0
							for _, g := range report.Generations {
								problems += len(g.Problems)
								if tc.problems == 0 && g.RenewalHealth != "healthy" {
									t.Errorf("settled cancellation degraded health: %+v", g)
								}
								if tc.outcome == "canceled" {
									for _, p := range g.Problems {
										if !strings.Contains(p.Detail, "Run run-extra (Attempt attempt-extra): lost") {
											t.Errorf("problem is not the lost Attempt: %+v", p)
										}
									}
								}
							}
							if problems != tc.problems {
								t.Errorf("problems=%d want=%d output=%s", problems, tc.problems, out)
							}
							findings := 0
							for _, r := range report.Reasons {
								if r.Code == "retiring_attempt_problem" {
									findings++
								}
							}
							for _, f := range report.Findings {
								if f.Check == "retiring_attempt_problem" {
									findings++
								}
							}
							if findings != tc.problems {
								t.Errorf("findings=%d want=%d output=%s", findings, tc.problems, out)
							}
							if tc.outcome == "canceled" && !strings.Contains(out, `"job_outcome": "canceled"`) {
								t.Errorf("missing canceled outcome: %s", out)
							}
						} else {
							if tc.outcome == "canceled" {
								if !strings.Contains(out, "Run run-0 (Attempt attempt-0): canceled") || strings.Contains(out, "Run run-0 (Attempt attempt-0): failed") {
									t.Errorf("cancellation not settled: %s", out)
								}
							} else if !strings.Contains(out, "Run run-0 (Attempt attempt-0): failed") {
								t.Errorf("failure disappeared: %s", out)
							}
							if tc.extra == contract.AttemptLost && !strings.Contains(out, "Run run-extra (Attempt attempt-extra): lost") {
								t.Errorf("loss disappeared: %s", out)
							}
						}
					})
				}
			}
			if diagnosticTree(t, f.setup.home) != before {
				t.Error("diagnostics mutated installation")
			}
		})
	}
}

func TestCancelledRetiringRunPreservesAttemptProblems(t *testing.T) {
	for _, tc := range []struct {
		name             string
		state            contract.AttemptState
		outcome          string
		stale            bool
		observationError string
		problem          string
	}{
		{name: "settled-failure", state: contract.AttemptFailed, outcome: "canceled"},
		{name: "settled-success", state: contract.AttemptSucceeded, outcome: "canceled"},
		{name: "pending-live-cancellation", state: contract.AttemptRunning, outcome: "canceled"},
		{name: "lost-after-cancellation", state: contract.AttemptLost, outcome: "canceled", problem: "retiring_attempt_problem"},
		{name: "interrupted-after-cancellation", state: contract.AttemptInterrupted, outcome: "canceled", problem: "retiring_attempt_problem"},
		{name: "expired-after-cancellation", state: contract.AttemptRunning, outcome: "canceled", stale: true, problem: "retiring_lease_stale"},
		{name: "self-failure", state: contract.AttemptFailed, problem: "retiring_attempt_problem"},
		{name: "unverified-cancellation", state: contract.AttemptFailed, outcome: "canceled", observationError: "unreadable retained Attempt", problem: "generation_observation_unverified"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			nodes := cancellationStatusNodes(t, tc.outcome, "")
			a := &nodes.Nodes[0].Generations[1].Attempts[0]
			a.State = generationAttemptState(tc.state)
			a.ObservationError = tc.observationError
			if !tc.stale {
				a.LeaseExpiresAt = generationDeadline(now.Add(time.Minute))
			}
			got := projectGeneration(nodes.Nodes[0], nodes.Nodes[0].Generations[1], now)
			wantProblems := 0
			if tc.problem != "" {
				wantProblems = 1
			}
			if len(got.Problems) != wantProblems {
				t.Fatalf("problems=%+v", got.Problems)
			}
			if wantProblems > 0 && (got.Problems[0].Code != tc.problem || got.RenewalHealth == "healthy") {
				t.Fatalf("problem hidden: %+v", got)
			}
			if liveGenerationAttempt(tc.state) && len(got.RemainingAttemptIDs) != 1 {
				t.Fatalf("pending cancellation settled a live Attempt: %+v", got)
			}
		})
	}
}
