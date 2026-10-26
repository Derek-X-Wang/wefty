package l1

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestNodeGenerationObservationsRetainLossAndAcceptedRenewal(t *testing.T) {
	f := newGenerationFixture(t)
	a := f.claim(t, "old")
	spec := a.Job.Spec
	spec.Labels = map[string]string{contract.LabelRunID: "run-old"}
	payload, _ := json.Marshal(spec)
	if _, err := f.h.store.db.Exec(`UPDATE jobs SET spec_json=? WHERE job_id=?`, payload, a.Job.JobID); err != nil {
		t.Fatal(err)
	}
	f.h.clock.Advance(time.Second)
	accepted := f.h.clock.Now()
	if _, err := f.h.store.RenewLease(t.Context(), "agent", a.Job.JobID, a.Lease.AttemptID, a.Lease.FencingToken); err != nil {
		t.Fatal(err)
	}
	f.promote(t, "g2")
	f.intent(t, true)
	f.claim(t, "new")
	f.promote(t, "g3")
	f.intent(t, true)
	f.claim(t, "newest")
	f.promote(t, "g4")
	before, err := f.h.store.ListNodes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(before[0].Generations) != 4 {
		t.Fatalf("truncated %+v", before)
	}
	var old NodeGenerationObservation
	for _, g := range before[0].Generations {
		if g.AgentGenerationID == "g1" {
			old = g
		}
	}
	if len(old.Attempts) != 1 || old.Attempts[0].RunID != "run-old" || old.Attempts[0].LastRenewedAt == nil || !old.Attempts[0].LastRenewedAt.Equal(accepted) {
		t.Fatalf("%+v", old)
	}
	// A later log/result/state timestamp cannot replace accepted renewal time.
	if _, err := f.h.store.db.Exec(`UPDATE attempts SET state='interrupted',interruption_reason='lease_expired',updated_ns=? WHERE attempt_id=?`, accepted.Add(time.Minute).UnixNano(), a.Lease.AttemptID); err != nil {
		t.Fatal(err)
	}
	nodes, err := f.h.store.ListNodes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range nodes[0].Generations {
		if g.AgentGenerationID == "g1" {
			if g.Attempts[0].State == nil || *g.Attempts[0].State != contract.AttemptInterrupted || !g.Attempts[0].LastRenewedAt.Equal(accepted) {
				t.Fatalf("%+v", g)
			}
		}
	}
	public, _ := json.Marshal(nodes)
	for _, secret := range []string{"fencing_token", "promotion_request", "boot-g1"} {
		if strings.Contains(string(public), secret) {
			t.Fatalf("authority leaked: %s", secret)
		}
	}
}

func TestNodeGenerationObservationReadsDoNotTakeWriterLock(t *testing.T) {
	f := generationObservationStoreFixture(t)
	f.claim(t, "resident")
	tx, err := f.h.store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// This writer owns BEGIN IMMEDIATE. ReadOnly snapshot must still complete.
	nodes, err := f.h.store.ListNodes(t.Context())
	if err != nil || len(nodes) != 1 || len(nodes[0].Generations) != 1 {
		t.Fatalf("nodes=%+v err=%v", nodes, err)
	}
}

func TestNodeGenerationObservationPriorBootWithoutRenewalIsNotSurvival(t *testing.T) {
	f := newGenerationFixture(t)
	f.claim(t, "prior-boot")
	f.promote(t, "g2")
	for id, boot := range map[string]string{"g1": "darwin:11111111-1111-1111-1111-111111111111", "g2": "darwin:22222222-2222-2222-2222-222222222222"} {
		if _, err := f.h.store.db.Exec(`UPDATE node_generations SET registration_json=json_set(CAST(registration_json AS TEXT),'$.system_boot_id',?) WHERE generation_id=?`, boot, id); err != nil {
			t.Fatal(err)
		}
	}
	node, err := f.h.store.GetNode(t.Context(), "node")
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range node.Generations {
		if g.AgentGenerationID == "g1" && (!g.PreviousSystemBoot || len(g.Attempts) != 1 || g.Attempts[0].LastRenewedAt != nil) {
			t.Fatalf("%+v", g)
		}
		if g.AgentGenerationID == "g2" && g.PreviousSystemBoot {
			t.Fatal("primary marked prior boot")
		}
	}
}

func TestReviewMalformedJobObservationPreservesOtherRecords(t *testing.T) {
	f := newGenerationFixture(t)
	broken := f.claim(t, "broken")
	good := f.claim(t, "readable")
	f.promote(t, "g2")
	registration := generationRegistration(f.h, "other-generation")
	registration.NodeID = "other-node"
	if _, err := f.h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "other-agent"}, registration, NodePolicy{MaxOneshotSlots: 1}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := f.h.store.db.Exec(`UPDATE jobs SET spec_json=? WHERE job_id=?`, []byte("{"), broken.Job.JobID); err != nil {
		t.Fatal(err)
	}
	nodes, err := f.h.store.ListNodes(t.Context())
	if err != nil {
		t.Fatalf("one malformed retained Job suppressed all Nodes: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("lost unrelated Node: %+v", nodes)
	}
	if _, err := getNode(t.Context(), f.h.store.db, "node"); err == nil {
		t.Fatal("private Node projection accepted malformed Job")
	}
	for _, n := range nodes {
		for _, a := range n.ActiveAttempts {
			if a.AttemptID == broken.Lease.AttemptID && a.ObservationError == "" {
				t.Fatal("active-Attempt projection lost record cause")
			}
		}
	}
	foundBroken, foundGood := false, false
	for _, n := range nodes {
		for _, g := range n.Generations {
			for _, a := range g.Attempts {
				if a.AttemptID == broken.Lease.AttemptID {
					payload, _ := json.Marshal(a)
					var record map[string]any
					_ = json.Unmarshal(payload, &record)
					if a.State != nil || record["observation_error"] == nil {
						t.Fatalf("malformed record lacks unknown state and cause: %s", payload)
					}
					foundBroken = true
				}
				if a.AttemptID == good.Lease.AttemptID && a.State != nil {
					foundGood = true
				}
			}
		}
	}
	if !foundBroken || !foundGood {
		t.Fatalf("lost records broken=%t readable=%t", foundBroken, foundGood)
	}
}

func TestReviewMalformedGenerationObservationPreservesOtherRecords(t *testing.T) {
	for _, column := range []string{"registration_json", "protocols_json", "selected_protocol"} {
		t.Run(column, func(t *testing.T) {
			f := newGenerationFixture(t)
			f.claim(t, "resident")
			f.promote(t, "g2")
			if _, err := f.h.store.db.Exec(`UPDATE node_generations SET `+column+`=? WHERE generation_id='g1'`, []byte("{")); err != nil {
				t.Fatal(err)
			}
			nodes, err := f.h.store.ListNodes(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if len(nodes) != 1 || len(nodes[0].Generations) != 2 {
				t.Fatalf("%+v", nodes)
			}
			for _, g := range nodes[0].Generations {
				if g.AgentGenerationID == "g1" && (g.ObservationError == "" || len(g.Attempts) != 1) {
					t.Fatalf("lost identity, cause or resident: %+v", g)
				}
				if g.AgentGenerationID == "g2" && g.ObservationError != "" {
					t.Fatalf("unrelated primary became unknown: %+v", g)
				}
			}
			// Diagnostic tolerance must not broaden the private authority reader.
			if _, err := f.h.store.NodeGenerations(t.Context(), "agent", "node"); err == nil {
				t.Fatal("private authority read accepted malformed generation")
			}
		})
	}
}

func TestReviewMissingRetainedAttemptHasIdentityAndCause(t *testing.T) {
	f := newGenerationFixture(t)
	a := f.claim(t, "missing-retained")
	f.promote(t, "g2")
	if _, err := f.h.store.db.Exec(`DELETE FROM attempts WHERE attempt_id=?`, a.Lease.AttemptID); err != nil {
		t.Fatal(err)
	}
	node, err := f.h.store.GetNode(t.Context(), "node")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, g := range node.Generations {
		for _, observed := range g.Attempts {
			if observed.AttemptID == a.Lease.AttemptID {
				found = true
				if observed.State != nil || observed.JobID != a.Job.JobID || observed.ObservationError == "" {
					t.Fatalf("%+v", observed)
				}
			}
		}
	}
	if !found {
		t.Fatal("frozen membership disappeared with its Attempt row")
	}
}

func TestRound2ActiveAttemptConversionPreservesOtherNodes(t *testing.T) {
	f := newGenerationFixture(t)
	broken := f.claim(t, "bad-expiry")
	registration := generationRegistration(f.h, "")
	registration.AgentVersion = "other-version"
	registration.NodeID = "other-node"
	if _, err := f.h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "other-agent"}, registration, NodePolicy{MaxOneshotSlots: 1}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := f.h.store.db.Exec(`UPDATE attempts SET lease_expires_ns='not-a-number' WHERE attempt_id=?`, broken.Lease.AttemptID); err != nil {
		t.Fatal(err)
	}
	nodes, err := f.h.store.ListNodes(t.Context())
	if err != nil {
		t.Fatalf("record-specific conversion suppressed all Nodes: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("lost unrelated Node: %+v", nodes)
	}
	found := false
	for _, n := range nodes {
		for _, a := range n.ActiveAttempts {
			if a.AttemptID == broken.Lease.AttemptID {
				found = true
				if a.JobID != broken.Job.JobID || !strings.Contains(a.ObservationError, "lease_expires_ns") {
					t.Fatalf("identity or cause lost: %+v", a)
				}
			}
		}
	}
	if !found {
		t.Fatal("malformed active Attempt disappeared")
	}
	if _, err := getNode(t.Context(), f.h.store.db, "node"); err == nil {
		t.Fatal("private projection accepted invalid lease expiry")
	}
}

func TestNodeGenerationObservationJobOutcome(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "failed-by-itself"
		if canceled {
			name = "owner-canceled"
		}
		t.Run(name, func(t *testing.T) {
			f := newGenerationFixture(t)
			a := f.claim(t, name)
			f.promote(t, "g2")
			if canceled {
				if _, err := f.h.store.CancelJob(t.Context(), a.Job.JobID, JobCancelCaller{Submitter: "operator"}); err != nil {
					t.Fatal(err)
				}
			}
			exit := 143 // Deliberately identical: process evidence is not cancellation intent.
			if _, err := f.h.store.CompleteAttempt(t.Context(), "agent", a.Job.JobID, a.Lease.AttemptID, CompletionRequest{FencingToken: a.Lease.FencingToken, IdempotencyKey: name, Result: ProcessResult{ExitCode: &exit}}); err != nil {
				t.Fatal(err)
			}
			nodes, err := f.h.store.ListNodes(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			node, err := f.h.store.GetNode(t.Context(), "node")
			if err != nil {
				t.Fatal(err)
			}
			for _, n := range []Node{nodes[0], node} {
				found := false
				for _, g := range n.Generations {
					for _, observed := range g.Attempts {
						if observed.AttemptID != a.Lease.AttemptID {
							continue
						}
						found = true
						if observed.State == nil || *observed.State != contract.AttemptFailed {
							t.Fatalf("Attempt state lost: %+v", observed)
						}
						payload, err := json.Marshal(observed)
						if err != nil {
							t.Fatal(err)
						}
						var fields map[string]any
						if err := json.Unmarshal(payload, &fields); err != nil {
							t.Fatal(err)
						}
						if canceled {
							if fields["job_outcome"] != "canceled" {
								t.Errorf("missing cancellation intent: %s", payload)
							}
						} else if _, present := fields["job_outcome"]; present {
							t.Errorf("invented outcome from exit 143: %s", payload)
						}
					}
				}
				if !found {
					t.Fatal("retiring resident disappeared")
				}
			}
		})
	}
}

func TestNodeGenerationObservationPayloadExit(t *testing.T) {
	for _, tc := range []struct {
		name, result, lateResult string
		wantExit                 any
		wantError                bool
		initiator                contract.TerminationCause
		canceled                 bool
	}{
		{name: "payload-exit-17", result: `{"exit_code":17}`, wantExit: float64(17)},
		{name: "agent-initiated-exit-17", result: `{"exit_code":17}`, wantExit: float64(17), initiator: contract.TerminationCauseAgent},
		{name: "guardian-initiated-exit-17", result: `{"exit_code":17}`, wantExit: float64(17), initiator: contract.TerminationCauseGuardian},
		{name: "owner-canceled-exit-17", result: `{"exit_code":17}`, wantExit: float64(17), initiator: contract.TerminationCauseAgent, canceled: true},
		{name: "payload-exit-zero", result: `{"exit_code":0}`, wantExit: float64(0)},
		{name: "no-result"},
		{name: "infrastructure-failure", result: `{"output_error":"cannot retain output"}`},
		{name: "late-exit-is-not-accepted", lateResult: `{"result":{"exit_code":17}}`},
		{name: "unreadable-result", result: `{`, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := generationObservationStoreFixture(t)
			a := f.claim(t, tc.name)
			f.promote(t, "g2")
			if tc.canceled {
				if _, err := f.h.store.CancelJob(t.Context(), a.Job.JobID, JobCancelCaller{Submitter: "operator"}); err != nil {
					t.Fatal(err)
				}
			}
			// Ordinary exits enter through the accepted completion API. Corrupt,
			// absent and late evidence fixtures exercise conservative snapshot reads.
			if tc.wantExit != nil {
				var result ProcessResult
				if err := json.Unmarshal([]byte(tc.result), &result); err != nil {
					t.Fatal(err)
				}
				if _, err := f.h.store.CompleteAttempt(t.Context(), "agent", a.Job.JobID, a.Lease.AttemptID, CompletionRequest{FencingToken: a.Lease.FencingToken, IdempotencyKey: tc.name, Result: result, TerminationInitiator: tc.initiator}); err != nil {
					t.Fatal(err)
				}
			}
			var result, lateResult any
			if tc.result != "" {
				result = []byte(tc.result)
			}
			if tc.lateResult != "" {
				lateResult = []byte(tc.lateResult)
			}
			if tc.wantExit == nil {
				if _, err := f.h.store.db.Exec(`UPDATE attempts SET state='failed',result_json=?,late_result_json=? WHERE attempt_id=?`, result, lateResult, a.Lease.AttemptID); err != nil {
					t.Fatal(err)
				}
			}
			nodes, err := f.h.store.ListNodes(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			node, err := f.h.store.GetNode(t.Context(), "node")
			if err != nil {
				t.Fatal(err)
			}
			for _, n := range []Node{nodes[0], node} {
				found := false
				for _, g := range n.Generations {
					for _, observed := range g.Attempts {
						if observed.AttemptID != a.Lease.AttemptID {
							continue
						}
						found = true
						payload, err := json.Marshal(observed)
						if err != nil {
							t.Fatal(err)
						}
						var fields map[string]any
						if err := json.Unmarshal(payload, &fields); err != nil {
							t.Fatal(err)
						}
						if tc.initiator != "" && (observed.State == nil || *observed.State != contract.AttemptFailed) {
							t.Errorf("agent/guardian completion is not failed: %s", payload)
						}
						if tc.canceled && fields["job_outcome"] != "canceled" {
							t.Errorf("owner cancellation intent missing: %s", payload)
						}
						if tc.initiator != "" && fields["termination_initiator"] != string(tc.initiator) {
							t.Errorf("accepted termination provenance missing: %s", payload)
						}
						if fields["exit_code"] != tc.wantExit || (observed.ObservationError != "") != tc.wantError {
							t.Errorf("observation=%s want exit=%v error=%t", payload, tc.wantExit, tc.wantError)
						}
						if tc.wantExit == nil {
							if _, present := fields["exit_code"]; present {
								t.Errorf("invented exit evidence: %s", payload)
							}
						}
					}
				}
				if !found {
					t.Fatal("retiring resident disappeared")
				}
			}
		})
	}
}

// Snapshot/completion tests need a Store, not an HTTP server or background reconciler.
func generationObservationStoreFixture(t *testing.T) *generationFixture {
	t.Helper()
	clock := &fakeClock{now: time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)}
	store, err := OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), StoreOptions{Clock: clock, LeaseDuration: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	h := &integrationHarness{t: t, store: store, clock: clock}
	node, err := store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, generationRegistration(h, "g1"), NodePolicy{MaxOneshotSlots: 3, MaxServiceSlots: 2}, true)
	if err != nil {
		t.Fatal(err)
	}
	f := &generationFixture{h: h, node: node}
	f.refresh(t)
	return f
}
