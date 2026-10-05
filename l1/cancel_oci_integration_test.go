package l1

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// Ordinary app submission: no L3 labels or privileged submission origin.
func directCancelOCI(t *testing.T) (*integrationHarness, Job, *Claim) {
	t.Helper()
	h := newIntegrationHarness(t, map[string][]string{"node-1": {}})
	registerOCIFixtureNode(t, h)
	key := "cancel-instance"
	spec := contract.JobSpec{SchemaVersion: 1, DispatchKey: "oci-cancel", Kind: "oci", Class: "one-shot", InstanceKey: &key,
		RuntimeHandler: "io.containerd.runc.v2", Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: "ghcr.io/example/tool:latest"}}}}
	client := h.client(fabric.Identity{NodeID: "ordinary-app", Tags: []string{DefaultClientPrincipalTag}})
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusCreated {
		t.Fatalf("direct submit=%d %s", status, body)
	}
	job := decodeJob(t, body)
	return h, job, claimOCIFixture(t, h, contract.JobClassOneShot)
}

func TestCancelOCISettlementAndCompletionReplay(t *testing.T) {
	for _, phase := range []string{"image_preparation", "helper_admission", "started", "completion_first", "silent_node", "lease_expiry"} {
		t.Run(phase, func(t *testing.T) {
			h, job, claim := directCancelOCI(t)
			ctx := t.Context()
			start := func() error {
				_, err := h.store.ObserveAttemptImage(ctx, "agent", job.JobID, claim.Lease.AttemptID, testImageObservation(claim.Lease.FencingToken))
				if err != nil {
					return err
				}
				_, err = h.store.StartAttempt(ctx, "agent", job.JobID, claim.Lease.AttemptID, StartedRequest{FencingToken: claim.Lease.FencingToken})
				return err
			}
			request := CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "cancel-completion",
				Result: ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureRuntimeUnavailable, Message: "helper unavailable before Started"}}}
			if phase == "started" || phase == "completion_first" || phase == "silent_node" {
				if err := start(); err != nil {
					t.Fatal(err)
				}
				zero := 0
				request.Result = ProcessResult{ExitCode: &zero}
			}
			if phase == "helper_admission" {
				if _, err := h.store.ObserveAttemptImage(ctx, "agent", job.JobID, claim.Lease.AttemptID, testImageObservation(claim.Lease.FencingToken)); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "completion_first" {
				if _, err := h.store.CompleteAttempt(ctx, "agent", job.JobID, claim.Lease.AttemptID, request); err != nil {
					t.Fatal(err)
				}
			}
			canceledAt := h.clock.Now()
			pending, err := h.store.CancelJob(ctx, job.JobID, JobCancelCaller{Submitter: "ordinary-app"})
			if err != nil {
				t.Fatal(err)
			}
			if phase == "completion_first" {
				if pending.State != contract.JobSucceeded || pending.Outcome != "" {
					t.Fatalf("earlier success=%+v", pending)
				}
				return
			}
			if pending.Outcome != "canceled" || pending.State == contract.JobFailed {
				t.Fatalf("pending=%+v", pending)
			}
			// Pending cancellation keeps the instance key until terminal settlement.
			next := job.Spec
			next.DispatchKey = "successor"
			client := h.client(fabric.Identity{NodeID: "ordinary-app", Tags: []string{DefaultClientPrincipalTag}})
			status, _, body := h.do(client, http.MethodPost, "/v1/jobs", next)
			assertAPIError(t, status, body, http.StatusConflict, contract.ErrorInstanceKeyConflict)
			replay, err := h.store.CancelJob(ctx, job.JobID, JobCancelCaller{Submitter: "ordinary-app"})
			if err != nil || !reflect.DeepEqual(pending, replay) {
				t.Fatalf("cancel retry=%+v %v", replay, err)
			}
			lease, err := h.store.RenewLease(ctx, "agent", job.JobID, claim.Lease.AttemptID, claim.Lease.FencingToken)
			if err != nil || lease.Directive != AttemptDirectiveCancel || lease.LeaseExpires.After(canceledAt.Add(CancelSettlementTimeout)) {
				t.Fatalf("renew=%+v %v", lease, err)
			}
			directives, err := h.store.ListNodeCancelDirectives(ctx, "agent", "node-1", "boot-node-1")
			if err != nil || len(directives) != 1 || directives[0].AttemptID != claim.Lease.AttemptID || directives[0].FencingToken != claim.Lease.FencingToken {
				t.Fatalf("directives=%+v %v", directives, err)
			}
			if phase == "started" || phase == "silent_node" {
				if _, err := h.store.StartAttempt(ctx, "agent", job.JobID, claim.Lease.AttemptID, StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
					t.Fatalf("Started replay=%v", err)
				}
			} else if phase == "helper_admission" {
				if _, err := h.store.StartAttempt(ctx, "agent", job.JobID, claim.Lease.AttemptID, StartedRequest{FencingToken: claim.Lease.FencingToken}); errorCode(err) != contract.ErrorConflict {
					t.Fatalf("Started after cancel=%v", err)
				}
			}
			if phase == "silent_node" || phase == "lease_expiry" {
				if phase == "silent_node" {
					h.clock.Advance(CancelSettlementTimeout)
				} else {
					h.clock.Advance(DefaultLeaseDuration)
				}
				if _, err := h.store.Reconcile(ctx); err != nil {
					t.Fatal(err)
				}
				if _, err := h.store.CompleteAttempt(ctx, "agent", job.JobID, claim.Lease.AttemptID, request); errorCode(err) != contract.ErrorLeaseExpired {
					t.Fatalf("late completion=%v", err)
				}
			} else {
				// Runtime-unavailable must never restore queued after cancellation, including replay.
				for i := 0; i < 2; i++ {
					done, err := h.store.CompleteAttempt(ctx, "agent", job.JobID, claim.Lease.AttemptID, request)
					if err != nil || done.State != contract.JobFailed || done.Outcome != "canceled" {
						t.Fatalf("completion/replay=%+v %v", done, err)
					}
				}
			}
			done, err := h.store.GetJob(ctx, job.JobID)
			if err != nil || done.State != contract.JobFailed || done.Outcome != "canceled" {
				t.Fatalf("settled=%+v %v", done, err)
			}
			status, _, body = h.do(client, http.MethodPost, "/v1/jobs", next)
			if status != http.StatusCreated {
				t.Fatalf("settlement did not release key=%d %s", status, body)
			}
			// Late result upload survives terminal cancellation under the exact fence.
			agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
			status, _, body = h.do(agent, http.MethodPost, fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/result", job.JobID, claim.Lease.AttemptID), AttemptResultRequest{FencingToken: claim.Lease.FencingToken, Document: json.RawMessage(`{"canceled":true}`)})
			if status != http.StatusOK {
				t.Fatalf("result upload=%d %s", status, body)
			}
			var result JobResult
			status, _, body = h.do(client, http.MethodGet, "/v1/jobs/"+job.JobID+"/result", nil)
			if err := json.Unmarshal(body, &result); err != nil || status != http.StatusOK || string(result.Document) != `{"canceled":true}` {
				t.Fatalf("result=%d %s %v", status, body, err)
			}
		})
	}
}

func TestCancelParentAllowsStoredChildReplayAndLogsStayClaimed(t *testing.T) {
	h, client, agent, node := credentialHarness(t)
	job := h.submit(client, "parent-replay", []string{"linux"})
	claim := claimClass(t, h, agent, node, contract.JobClassOneShot)
	spec := validJobSpec("child-replay", nil)
	status, body := h.credentialRequest(agent, http.MethodPost, "/v1/jobs", claim.AttemptToken, spec)
	if status != http.StatusCreated {
		t.Fatalf("child=%d %s", status, body)
	}
	child := decodeJob(t, body)
	status, _, body = h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/cancel", nil)
	if status != http.StatusOK {
		t.Fatalf("cancel=%d %s", status, body)
	}
	status, body = h.credentialRequest(agent, http.MethodPost, "/v1/jobs", claim.AttemptToken, spec)
	if status != http.StatusOK || decodeJob(t, body).JobID != child.JobID {
		t.Fatalf("stored child replay=%d %s", status, body)
	}
	response, err := h.store.AppendLogs(t.Context(), "node-1", job.JobID, claim.Lease.AttemptID, AppendLogsRequest{FencingToken: claim.Lease.FencingToken, Events: []contract.LogEvent{logEvent(claim.Lease.AttemptID, contract.LogStdout, 0, []byte("late log"))}})
	if err != nil || response.AttemptState != contract.AttemptClaimed {
		t.Fatalf("log state=%+v %v", response, err)
	}
}

func TestCancelProvenanceLessTombstoneRequiresAdmin(t *testing.T) {
	h, client, _, _ := credentialHarness(t)
	job := submitRestartService(t, h, client, "legacy-tombstone-cancel", nil, nil)
	if _, err := h.store.RemoveService(t.Context(), job.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec(`UPDATE service_tombstones SET originating_submitter='', parent_job_id='' WHERE job_id=?`, job.JobID); err != nil {
		t.Fatal(err)
	}
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/cancel", nil)
	assertAPIError(t, status, body, http.StatusNotFound, contract.ErrorNotFound)
	admin := h.client(fabric.Identity{NodeID: "legacy-cancel-admin", UserID: "legacy-cancel-person", DeviceID: "legacy-cancel-device"})
	challenge, err := h.store.InitiateAdminBootstrap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(admin, http.MethodPost, "/v1/admin-bootstrap", BootstrapAdminRequest{Nonce: challenge.Nonce})
	if status != http.StatusCreated {
		t.Fatalf("bootstrap=%d %s", status, body)
	}
	status, _, body = h.do(admin, http.MethodPost, "/v1/jobs/"+job.JobID+"/cancel", nil)
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorCancelService)
}

func TestCancelOCIRequeuedCompletionEvidenceReplay(t *testing.T) {
	h, job, claim := directCancelOCI(t)
	request := CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "runtime-requeue-first", Result: ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureRuntimeUnavailable, Message: "runtime unavailable"}}}
	queued, err := h.store.CompleteAttempt(t.Context(), "agent", job.JobID, claim.Lease.AttemptID, request)
	if err != nil || queued.State != contract.JobQueued {
		t.Fatalf("runtime requeue=%+v %v", queued, err)
	}
	canceled, err := h.store.CancelJob(t.Context(), job.JobID, JobCancelCaller{Submitter: "ordinary-app"})
	if err != nil || canceled.State != contract.JobFailed || canceled.Outcome != "canceled" {
		t.Fatalf("queued cancel=%+v %v", canceled, err)
	}
	replay, err := h.store.CompleteAttempt(t.Context(), "agent", job.JobID, claim.Lease.AttemptID, request)
	if err != nil || replay.State != contract.JobFailed || replay.Outcome != "canceled" {
		t.Fatalf("requeued evidence replay=%+v %v", replay, err)
	}
}
