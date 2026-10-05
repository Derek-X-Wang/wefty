package l1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestCancelActiveProcessContract(t *testing.T) {
	for _, state := range []contract.JobState{contract.JobClaimed, contract.JobRunning, contract.JobAwaitingInput} {
		t.Run(string(state), func(t *testing.T) {
			h, client, agent, node := credentialHarness(t)
			job := h.submit(client, "active-cancel", []string{"linux"})
			claim := claimClass(t, h, agent, node, contract.JobClassOneShot)
			if state != contract.JobClaimed {
				if _, err := h.store.RenewLease(t.Context(), "node-1", job.JobID, claim.Lease.AttemptID, claim.Lease.FencingToken); err != nil {
					t.Fatal(err)
				}
			}
			if state == contract.JobAwaitingInput {
				if _, err := h.store.db.Exec(`UPDATE jobs SET state='awaiting-input' WHERE job_id=?`, job.JobID); err != nil {
					t.Fatal(err)
				}
				if _, err := h.store.db.Exec(`UPDATE attempts SET state='awaiting-input' WHERE attempt_id=?`, claim.Lease.AttemptID); err != nil {
					t.Fatal(err)
				}
			}
			status, _, body := h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/cancel", nil)
			if status != http.StatusOK {
				t.Fatalf("cancel=%d %s", status, body)
			}
			pending := decodeJob(t, body)
			if pending.Outcome != "canceled" || pending.State != state {
				t.Fatalf("pending=%s", body)
			}
			h.clock.Advance(time.Second)
			status, _, body = h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/cancel", nil)
			if status != http.StatusOK || !reflect.DeepEqual(pending, decodeJob(t, body)) {
				t.Fatalf("retry changed intent=%s", body)
			}
			lease, err := h.store.RenewLease(t.Context(), "node-1", job.JobID, claim.Lease.AttemptID, claim.Lease.FencingToken)
			if err != nil || string(lease.Directive) != "cancel" {
				t.Fatalf("renew=%+v %v", lease, err)
			}
			after, err := h.store.GetJob(t.Context(), job.JobID)
			if err != nil || after.State != state {
				t.Fatalf("cancellation acknowledged start=%+v %v", after, err)
			}
			status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/"+node.NodeID+"/heartbeat", heartbeatRequestForNode(node))
			var heartbeat struct {
				Cancels []struct {
					JobID     string `json:"job_id"`
					AttemptID string `json:"attempt_id"`
					Fence     string `json:"fencing_token"`
				} `json:"one_shot_cancel_directives"`
			}
			if err := json.Unmarshal(body, &heartbeat); err != nil {
				t.Fatal(err)
			}
			if status != http.StatusOK || len(heartbeat.Cancels) != 1 || heartbeat.Cancels[0].JobID != job.JobID || heartbeat.Cancels[0].AttemptID != claim.Lease.AttemptID || heartbeat.Cancels[0].Fence != claim.Lease.FencingToken {
				t.Fatalf("heartbeat=%d %s", status, body)
			}
			zero := 0
			done, err := h.store.CompleteAttempt(t.Context(), "node-1", job.JobID, claim.Lease.AttemptID, CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "canceled-zero", Result: ProcessResult{ExitCode: &zero}, TerminationInitiator: contract.TerminationCauseAgent})
			if err != nil || done.State != contract.JobFailed || done.Outcome != "canceled" {
				t.Fatalf("cancel overwritten=%+v %v", done, err)
			}
			attempts, err := h.store.ListJobAttempts(t.Context(), job.JobID)
			if err != nil || len(attempts) != 1 || attempts[0].Result == nil || attempts[0].Result.ExitCode == nil || *attempts[0].Result.ExitCode != 0 {
				t.Fatalf("real result lost=%+v %v", attempts, err)
			}
		})
	}
}

func TestCancelSettlementDeadlineReopenAndEvidence(t *testing.T) {
	h, client, agent, node := credentialHarness(t)
	job := h.submit(client, "cancel-deadline", []string{"linux"})
	claim := claimClass(t, h, agent, node, contract.JobClassOneShot)
	canceledAt := h.clock.Now()
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/cancel", nil)
	if status != http.StatusOK {
		t.Fatalf("cancel=%d %s", status, body)
	}
	// Reopen the durable database with a different Store, not the original's
	// configuration or any in-memory cancellation state.
	var seq int
	var name, path string
	if err := h.store.db.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(path, StoreOptions{Clock: h.clock, LeaseDuration: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for i := 0; i < 3; i++ {
		h.clock.Advance(9 * time.Second)
		lease, err := reopened.RenewLease(t.Context(), "node-1", job.JobID, claim.Lease.AttemptID, claim.Lease.FencingToken)
		if err != nil || !lease.LeaseExpires.Equal(canceledAt.Add(30*time.Second)) {
			t.Fatalf("deadline extended=%+v %v", lease, err)
		}
	}
	h.clock.Advance(3 * time.Second)
	result, err := reopened.Reconcile(t.Context())
	if err != nil || result.ExpiredAttempts != 1 {
		t.Fatalf("settle=%+v %v", result, err)
	}
	done, err := reopened.GetJob(t.Context(), job.JobID)
	if err != nil || done.State != contract.JobFailed || done.Outcome != "canceled" {
		t.Fatalf("silent node=%+v %v", done, err)
	}
	attempts, err := reopened.ListJobAttempts(t.Context(), job.JobID)
	if err != nil || len(attempts) != 1 || attempts[0].State != contract.AttemptLost || attempts[0].Result != nil {
		t.Fatalf("invented termination=%+v %v", attempts, err)
	}
	// A logically terminal cancellation still owes stop delivery to a returning
	// node, and preserves the existing provenance-only upload rules.
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/"+node.NodeID+"/heartbeat", heartbeatRequestForNode(node))
	var heartbeat map[string]json.RawMessage
	_ = json.Unmarshal(body, &heartbeat)
	if status != http.StatusOK || string(heartbeat["one_shot_cancel_directives"]) == "[]" || len(heartbeat["one_shot_cancel_directives"]) == 0 {
		t.Fatalf("stop delivery lost=%d %s", status, body)
	}
	_, err = reopened.AppendLogs(t.Context(), "node-1", job.JobID, claim.Lease.AttemptID, AppendLogsRequest{FencingToken: claim.Lease.FencingToken, Events: []contract.LogEvent{logEvent(claim.Lease.AttemptID, contract.LogStdout, 0, []byte("late log"))}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = reopened.SetAttemptResult(t.Context(), "node-1", job.JobID, claim.Lease.AttemptID, AttemptResultRequest{FencingToken: claim.Lease.FencingToken, Document: []byte(`{"late":true}`)})
	if err != nil {
		t.Fatal(err)
	}
	zero := 0
	_, err = reopened.CompleteAttempt(t.Context(), "node-1", job.JobID, claim.Lease.AttemptID, CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "late", Result: ProcessResult{ExitCode: &zero}})
	if errorCode(err) != contract.ErrorLeaseExpired {
		t.Fatalf("late completion=%v", err)
	}
	done, err = reopened.GetJob(t.Context(), job.JobID)
	if err != nil || done.State != contract.JobFailed || done.Outcome != "canceled" {
		t.Fatalf("late success overwrote cancel=%+v %v", done, err)
	}
}

func TestCancelOrderedCompletionExpiryAndChildRaces(t *testing.T) {
	for _, first := range []string{"cancel", "completion", "expiry", "child"} {
		t.Run(first, func(t *testing.T) {
			h, client, agent, node := credentialHarness(t)
			job := h.submit(client, "cancel-order", []string{"linux"})
			claim := claimClass(t, h, agent, node, contract.JobClassOneShot)
			// Resolve middleware authority before cancel. The later creation must
			// recheck intent inside its own committing transaction.
			scope, err := h.store.ResolveAttemptCredential(t.Context(), claim.AttemptToken, "node-1")
			if err != nil {
				t.Fatal(err)
			}
			create := func() (Job, error) {
				child, _, err := h.store.CreateJobAs(t.Context(), validJobSpec("ordered-child", []string{"linux"}), JobOrigin{Parent: &scope})
				return child, err
			}
			zero := 0
			complete := func() (Job, error) {
				return h.store.CompleteAttempt(t.Context(), "node-1", job.JobID, claim.Lease.AttemptID, CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "ordered-completion", Result: ProcessResult{ExitCode: &zero}})
			}
			var child Job
			switch first {
			case "completion":
				if _, err := complete(); err != nil {
					t.Fatal(err)
				}
			case "expiry":
				h.clock.Advance(time.Minute)
				if _, err := h.store.Reconcile(t.Context()); err != nil {
					t.Fatal(err)
				}
			case "child":
				child, err = create()
				if err != nil {
					t.Fatal(err)
				}
			}
			status, _, body := h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/cancel", nil)
			if status != http.StatusOK {
				t.Fatalf("cancel=%d %s", status, body)
			}
			after := decodeJob(t, body)
			if first == "completion" || first == "expiry" {
				if after.Outcome != "" {
					t.Fatalf("prior terminal changed=%s", body)
				}
				return
			}
			if after.Outcome != "canceled" {
				t.Fatalf("cancel intent missing=%s", body)
			}
			if first == "child" {
				got, err := h.store.GetJob(t.Context(), child.JobID)
				if err != nil || got.State != contract.JobQueued || got.Outcome != "" {
					t.Fatalf("cascade=%+v %v", got, err)
				}
			}
			if _, err := create(); errorCode(err) != contract.ErrorUnauthorized {
				t.Fatalf("stale creation authority=%v", err)
			}
			if first == "cancel" {
				h.clock.Advance(time.Minute)
				if _, err := h.store.Reconcile(t.Context()); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := complete(); err != nil {
					t.Fatal(err)
				}
			}
			got, err := h.store.GetJob(context.Background(), job.JobID)
			if err != nil || got.Outcome != "canceled" || got.State != contract.JobFailed {
				t.Fatalf("cancel reservation lost=%+v %v", got, err)
			}
		})
	}
}

func TestCancelRemovedServiceAndComputerRefusals(t *testing.T) {
	h, client, _, _ := credentialHarness(t)
	job := submitRestartService(t, h, client, "removed-cancel-service", nil, nil)
	if _, err := h.store.RemoveService(t.Context(), job.JobID); err != nil {
		t.Fatal(err)
	}
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/cancel", nil)
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorCancelService)
	status, _, body = h.do(client, http.MethodPost, "/v1/computers", CreateComputerRequest{Name: "cancel-computer", Spec: computerCapabilityJobSpec("cancel-computer"), Actor: "test"})
	if status != http.StatusCreated {
		t.Fatalf("computer=%d %s", status, body)
	}
	var computer Computer
	if err := json.Unmarshal(body, &computer); err != nil {
		t.Fatal(err)
	}
	admin := h.client(fabric.Identity{NodeID: "cancel-admin", UserID: "cancel-admin-person", DeviceID: "cancel-admin-device"})
	challenge, err := h.store.InitiateAdminBootstrap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(admin, http.MethodPost, "/v1/admin-bootstrap", BootstrapAdminRequest{Nonce: challenge.Nonce})
	if status != http.StatusCreated {
		t.Fatalf("bootstrap=%d %s", status, body)
	}
	status, _, body = h.do(admin, http.MethodPost, "/v1/jobs/"+computer.CurrentJobID+"/cancel", nil)
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorCancelService)
	var refusal contract.ErrorResponse
	if err := json.Unmarshal(body, &refusal); err != nil {
		t.Fatal(err)
	}
	if refusal.Error.Details["computer_id"] != computer.ComputerID || refusal.Error.Details["desired_state_path"] != "/v1/computers/"+computer.ComputerID+"/desired-state" || refusal.Error.Details["remove_path"] != "/v1/computers/"+computer.ComputerID+"/remove" {
		t.Fatalf("Computer routes=%s", body)
	}
}

// Block the first operation's clock read while its immediate writer transaction
// is held. Launch the competing operation before releasing it, so both commit
// orders are deterministic instead of depending on scheduler luck.
func TestCancelDeterministicTransactionRaces(t *testing.T) {
	for _, operation := range []string{"claim", "renew", "logs", "child", "completion", "expiry"} {
		for _, cancelFirst := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/cancel-first=%t", operation, cancelFirst), func(t *testing.T) {
				h := newIntegrationHarnessWithReconcileInterval(t, StoreOptions{}, map[string]NodePolicy{"node-1": DefaultNodePolicy()}, true, time.Hour)
				client := h.client(fabric.Identity{NodeID: "race-submitter", Tags: []string{DefaultClientPrincipalTag}})
				agent := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
				node := h.register(agent, "node-1")
				job := h.submit(client, "transaction-race", nil)
				var claim *Claim
				var scope AttemptCredentialScope
				var err error
				if operation != "claim" {
					won := claimClass(t, h, agent, node, contract.JobClassOneShot)
					claim = &won
					scope, err = h.store.ResolveAttemptCredential(t.Context(), claim.AttemptToken, "node-1")
					if err != nil {
						t.Fatal(err)
					}
				}
				if operation == "expiry" {
					h.clock.Advance(30 * time.Second)
				}
				now := h.clock.Now()
				entered, release := make(chan struct{}), make(chan struct{})
				var reads atomic.Int32
				h.store.clock = ClockFunc(func() time.Time {
					if reads.Add(1) == 1 {
						close(entered)
						<-release
					}
					return now
				})
				var canceled Job
				var cancelErr, rivalErr error
				var renewed AttemptLease
				var child Job
				cancel := func() {
					canceled, cancelErr = h.store.CancelJob(t.Context(), job.JobID, JobCancelCaller{Submitter: "race-submitter"})
				}
				rival := func() {
					switch operation {
					case "claim":
						claim, rivalErr = h.store.ClaimJob(t.Context(), "node-1", node.NodeID, node.BootSessionID, contract.JobClassOneShot)
					case "renew", "expiry":
						renewed, rivalErr = h.store.RenewLease(t.Context(), "node-1", job.JobID, claim.Lease.AttemptID, claim.Lease.FencingToken)
					case "logs":
						_, rivalErr = h.store.AppendLogs(t.Context(), "node-1", job.JobID, claim.Lease.AttemptID, AppendLogsRequest{FencingToken: claim.Lease.FencingToken, Events: []contract.LogEvent{logEvent(claim.Lease.AttemptID, contract.LogStdout, 0, []byte("acknowledgement"))}})
					case "child":
						child, _, rivalErr = h.store.CreateJobAs(t.Context(), validJobSpec("race-child", nil), JobOrigin{Parent: &scope})
					case "completion":
						zero := 0
						_, rivalErr = h.store.CompleteAttempt(t.Context(), "node-1", job.JobID, claim.Lease.AttemptID, CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "race-complete", Result: ProcessResult{ExitCode: &zero}})
					}
				}
				first, second := cancel, rival
				if !cancelFirst {
					first, second = rival, cancel
				}
				firstDone, secondDone := make(chan struct{}), make(chan struct{})
				go func() { defer close(firstDone); first() }()
				<-entered
				go func() { defer close(secondDone); second() }()
				close(release)
				<-firstDone
				<-secondDone
				h.store.clock = h.clock
				if cancelErr != nil {
					t.Fatal(cancelErr)
				}
				if operation == "child" && cancelFirst {
					if errorCode(rivalErr) != contract.ErrorUnauthorized {
						t.Fatalf("child committed after cancel=%+v %v", child, rivalErr)
					}
				} else if operation == "expiry" {
					if errorCode(rivalErr) != contract.ErrorLeaseExpired {
						t.Fatalf("expiry error=%v", rivalErr)
					}
				} else if rivalErr != nil {
					t.Fatal(rivalErr)
				}
				got, err := h.store.GetJob(t.Context(), job.JobID)
				if err != nil {
					t.Fatal(err)
				}
				if (operation == "completion" || operation == "expiry") && !cancelFirst {
					want := contract.JobSucceeded
					if operation == "expiry" {
						want = contract.JobFailed
					}
					if got.State != want || got.Outcome != "" || canceled.Outcome != "" {
						t.Fatalf("earlier completion lost=%+v", got)
					}
					return
				}
				if got.Outcome != "canceled" {
					t.Fatalf("reservation lost=%+v", got)
				}
				switch operation {
				case "claim":
					if cancelFirst && claim != nil {
						t.Fatalf("claim after cancel=%+v", claim)
					}
				case "renew", "logs":
					want := contract.JobClaimed
					if !cancelFirst {
						want = contract.JobRunning
					}
					if got.State != want {
						t.Fatalf("start arbitration=%+v", got)
					}
					if operation == "renew" && cancelFirst && string(renewed.Directive) != "cancel" {
						t.Fatalf("cancel delivery=%+v", renewed)
					}
				case "child":
					if !cancelFirst {
						got, err := h.store.GetJob(t.Context(), child.JobID)
						if err != nil || got.State != contract.JobQueued || got.Outcome != "" {
							t.Fatalf("child cascaded=%+v %v", got, err)
						}
					}
				case "completion":
					if got.State != contract.JobFailed {
						t.Fatalf("completion overwrote intent=%+v", got)
					}
				}
			})
		}
	}
}

func TestCancelActiveOCIStillRefused(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {}})
	registerOCIFixtureNode(t, h)
	client := h.client(fabric.Identity{NodeID: runLedgerOrigin.OriginatingSubmitter, Tags: []string{DefaultClientPrincipalTag}})
	spec := contract.JobSpec{SchemaVersion: 1, DispatchKey: "active-oci-cancel", Kind: "oci", Class: "one-shot", RuntimeHandler: "io.containerd.runc.v2", Labels: map[string]string{contract.LabelRunID: "run-active-oci-cancel"}, Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: "ghcr.io/example/tool:latest"}}}}
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusCreated {
		t.Fatalf("submit=%d %s", status, body)
	}
	job := decodeJob(t, body)
	claimOCIFixture(t, h, contract.JobClassOneShot)
	before, err := h.store.GetJob(t.Context(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/cancel", nil)
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorCancelNotQueued)
	after, err := h.store.GetJob(t.Context(), job.JobID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("OCI cancel mutated=%+v %v", after, err)
	}
}
