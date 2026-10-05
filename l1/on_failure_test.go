package l1

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func policyStopJSON(t *testing.T, job Job) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	return fields["policy_stop"]
}

func TestOnFailurePublicLifecycle(t *testing.T) {
	for _, action := range []string{"start", "restart", "stop"} {
		t.Run(action, func(t *testing.T) {
			h := newIntegrationHarnessWithOptions(t, StoreOptions{Jitter: func(d time.Duration) time.Duration { return d }}, map[string]NodePolicy{"service-node": DefaultNodePolicy("service")})
			client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
			agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
			node := h.register(agent, "service-node")
			if _, err := h.store.db.Exec("UPDATE nodes SET max_service_slots=1 WHERE node_id=?", node.NodeID); err != nil {
				t.Fatal(err)
			}
			spec := validJobSpec("on-failure-"+action, []string{"service"})
			spec.Class = "service"
			spec.Restart = "on-failure"
			spec.Execution.HandoffDirectory = ""
			port := 18080
			spec.PublishedPort = &port
			status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
			if status != http.StatusCreated {
				t.Fatalf("submit = %d %s", status, body)
			}
			var job Job
			if err := json.Unmarshal(body, &job); err != nil {
				t.Fatal(err)
			}
			claim := claimRestartService(t, h, agent, node)
			publicationPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/publication", job.JobID, claim.Lease.AttemptID)
			status, _, body = h.do(agent, http.MethodPut, publicationPath, PublicationRequest{FencingToken: claim.Lease.FencingToken, Ready: boolPointer(true)})
			if status != http.StatusOK {
				t.Fatalf("publication = %d %s", status, body)
			}
			if _, err := h.store.db.Exec("UPDATE service_jobs SET restart_streak=3, lifetime_restart_count=4 WHERE job_id=?", job.JobID); err != nil {
				t.Fatal(err)
			}
			zero := 0
			completion := CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "clean", Result: ProcessResult{ExitCode: &zero}, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt}
			path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, claim.Lease.AttemptID)
			status, _, body = h.do(agent, http.MethodPost, path, completion)
			if status != http.StatusOK {
				t.Fatalf("complete = %d %s", status, body)
			}
			if err := json.Unmarshal(body, &job); err != nil {
				t.Fatal(err)
			}
			if job.State != contract.JobStopped || job.DesiredState != contract.ServiceDesiredRunning || job.HoldsSlot(job.State) || job.PublishedPort != nil || job.NextRestartAt != nil || job.RestartStreak != 3 || job.LifetimeRestartCount != 4 || len(policyStopJSON(t, job)) == 0 {
				t.Fatalf("policy stop = %s", body)
			}
			var cause ProcessResult
			if err := json.Unmarshal(policyStopJSON(t, job), &cause); err != nil || cause.ExitCode == nil || *cause.ExitCode != 0 {
				t.Fatalf("cause = %+v %v", cause, err)
			}
			status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, Class: "service"})
			if status != http.StatusNoContent {
				t.Fatalf("policy stop claimed = %d %s", status, body)
			}
			status, _, body = h.do(agent, http.MethodPost, path, completion)
			if status != http.StatusOK {
				t.Fatalf("replay = %d %s", status, body)
			}
			var seq int
			var name, databasePath string
			if err := h.store.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &databasePath); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenStore(databasePath, StoreOptions{Clock: h.clock})
			if err != nil {
				t.Fatal(err)
			}
			persisted, err := reopened.GetJob(t.Context(), job.JobID)
			reopened.Close()
			if err != nil || persisted.State != contract.JobStopped || len(policyStopJSON(t, persisted)) == 0 {
				t.Fatalf("persisted stop = %+v %v", persisted, err)
			}
			if action == "start" {
				peer := submitRestartService(t, h, client, "capacity-peer", []string{"service"}, nil)
				peerClaim := claimRestartService(t, h, agent, node)
				if peerClaim.Job.JobID != peer.JobID {
					t.Fatal("stopped capacity was not released")
				}
				refused, _, refusalBody := h.do(client, http.MethodPut, serviceMutationPath(job.JobID, "desired-state"), ServiceDesiredStateRequest{DesiredState: contract.ServiceDesiredRunning})
				if refused != http.StatusConflict {
					t.Fatalf("start must reacquire capacity = %d %s", refused, refusalBody)
				}
				if got := getRestartService(t, h, job.JobID); got.State != contract.JobStopped || len(policyStopJSON(t, got)) == 0 {
					t.Fatal("refused start cleared policy stop")
				}
				if _, err := h.store.SetServiceDesiredState(t.Context(), peer.JobID, contract.ServiceDesiredStopped); err != nil {
					t.Fatal(err)
				}
				if _, err := h.store.CompleteAttempt(t.Context(), "agent", peer.JobID, peerClaim.Lease.AttemptID, CompletionRequest{FencingToken: peerClaim.Lease.FencingToken, IdempotencyKey: "stop-peer", Result: ProcessResult{ExitCode: &zero}, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt}); err != nil {
					t.Fatal(err)
				}
			}
			if action == "restart" {
				status, _, body = h.do(client, http.MethodPost, serviceMutationPath(job.JobID, "restart"), ServiceRestartRequest{IdempotencyKey: "resume"})
			} else {
				desired := contract.ServiceDesiredRunning
				if action == "stop" {
					desired = contract.ServiceDesiredStopped
				}
				status, _, body = h.do(client, http.MethodPut, serviceMutationPath(job.JobID, "desired-state"), ServiceDesiredStateRequest{DesiredState: desired})
			}
			if status != http.StatusAccepted {
				t.Fatalf("%s = %d %s", action, status, body)
			}
			job = Job{}
			if err := json.Unmarshal(body, &job); err != nil {
				t.Fatal(err)
			}
			if action == "stop" {
				if job.State != contract.JobStopped || job.DesiredState != contract.ServiceDesiredStopped {
					t.Fatalf("operator stop = %s", body)
				}
				return
			}
			if job.State != contract.JobQueued || len(policyStopJSON(t, job)) != 0 {
				t.Fatalf("resume = %s", body)
			}
			second := claimRestartService(t, h, agent, node)
			if second.Lease.AttemptID == claim.Lease.AttemptID {
				t.Fatal("resume reused attempt")
			}
		})
	}
}

func TestOnFailureExplicitRestartWinsCleanExit(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{Jitter: func(d time.Duration) time.Duration { return d }}, map[string]NodePolicy{"service-node": DefaultNodePolicy("service")})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "service-node")
	spec := validJobSpec("restart-zero", []string{"service"})
	spec.Class = "service"
	spec.Restart = "on-failure"
	spec.Execution.HandoffDirectory = ""
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusCreated {
		t.Fatalf("submit = %d %s", status, body)
	}
	var job Job
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatal(err)
	}
	first := claimRestartService(t, h, agent, node)
	if _, _, err := h.store.RestartService(t.Context(), job.JobID, ServiceRestartRequest{IdempotencyKey: "restart"}); err != nil {
		t.Fatal(err)
	}
	zero := 0
	completed, err := h.store.CompleteAttempt(t.Context(), "agent", job.JobID, first.Lease.AttemptID, CompletionRequest{FencingToken: first.Lease.FencingToken, IdempotencyKey: "term-zero", Result: ProcessResult{ExitCode: &zero}, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt})
	if err != nil || completed.State != contract.JobQueued || len(policyStopJSON(t, completed)) != 0 {
		t.Fatalf("restart completion = %+v %v", completed, err)
	}
	h.clock.Advance(time.Second)
	second := claimRestartService(t, h, agent, node)
	if second.Lease.AttemptID == first.Lease.AttemptID {
		t.Fatal("restart reused attempt")
	}
	completed, err = h.store.CompleteAttempt(t.Context(), "agent", job.JobID, second.Lease.AttemptID, CompletionRequest{FencingToken: second.Lease.FencingToken, IdempotencyKey: "natural-zero", Result: ProcessResult{ExitCode: &zero}, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt})
	if err != nil || completed.State != contract.JobStopped {
		t.Fatalf("old restart leaked = %+v %v", completed, err)
	}
}

func TestOnFailureCompletionPrecedence(t *testing.T) {
	zero, one := 0, 1
	tests := []struct {
		name             string
		result           ProcessResult
		want             contract.JobState
		streak, lifetime int
	}{
		{"clean", ProcessResult{ExitCode: &zero}, contract.JobStopped, 2, 5},
		{"incomplete logs", ProcessResult{ExitCode: &zero, LogEvidenceIncomplete: true}, contract.JobStopped, 2, 5},
		{"crash", ProcessResult{ExitCode: &one}, contract.JobQueued, 3, 6},
		{"output latch", ProcessResult{ExitCode: &zero, OutputError: "disk write failed"}, contract.JobFailed, 2, 5},
		{"spawn latch", ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureImageUnavailable, Message: "missing image"}}, contract.JobFailed, 2, 5},
		{"spontaneous", ProcessResult{Signal: "killed", TerminationCause: contract.TerminationCauseSpontaneous}, contract.JobQueued, 3, 6},
		{"agent interruption", ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}, contract.JobQueued, 2, 6},
		{"guardian interruption", ProcessResult{Signal: "killed", TerminationCause: contract.TerminationCauseGuardian}, contract.JobQueued, 2, 6},
	}
	s := &Store{leaseDuration: 30 * time.Second, restartJitter: func(d time.Duration) time.Duration { return d }}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := Job{State: contract.JobRunning, Spec: contract.JobSpec{Restart: "on-failure"}, ServiceJob: &ServiceJob{DesiredState: contract.ServiceDesiredRunning, RestartStreak: 2, LifetimeRestartCount: 5}}
			raw, _ := json.Marshal(tc.result)
			// Use completion through the store for restart precedence; this classifier
			// row tests that terminal arms still precede a clean exit.
			p := s.classifyServiceCompletion(job, tc.result, RuntimeQuiescenceAttempt, raw, time.Unix(100, 0))
			if p.jobState != tc.want || p.restartStreak != tc.streak || p.lifetimeRestartCount != tc.lifetime {
				t.Fatalf("policy = %+v", p)
			}
			if tc.want == contract.JobQueued && (p.nextRestartNS == nil || *p.nextRestartNS != time.Unix(100, 0).Add(serviceRestartDelay(tc.streak, 30*time.Second, s.restartJitter)).UnixNano()) && tc.streak == 3 {
				t.Fatalf("failure backoff changed: %+v", p)
			}
		})
	}
}

func TestOnFailureLatchesWin(t *testing.T) {
	for _, mode := range []string{"image", "removal"} {
		t.Run(mode, func(t *testing.T) {
			h := newIntegrationHarness(t, map[string][]string{"service-node": {"service"}})
			client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
			agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
			node := h.register(agent, "service-node")
			spec := validJobSpec("policy-latch-"+mode, []string{"service"})
			spec.Class = "service"
			spec.Restart = "on-failure"
			spec.Execution.HandoffDirectory = ""
			status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
			if status != http.StatusCreated {
				t.Fatalf("submit = %d %s", status, body)
			}
			var job Job
			if err := json.Unmarshal(body, &job); err != nil {
				t.Fatal(err)
			}
			claim := claimRestartService(t, h, agent, node)
			zero := 0
			completion := CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "clean", Result: ProcessResult{ExitCode: &zero}, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt}
			if mode == "image" {
				if _, err := h.store.CompleteAttempt(t.Context(), "agent", job.JobID, claim.Lease.AttemptID, completion); err != nil {
					t.Fatal(err)
				}
				latched, err := h.store.LatchServiceImageReconciliationFailure(t.Context(), "agent", job.JobID, ServiceImageReconciliationFailureRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, Failure: contract.SpawnFailure{Code: contract.SpawnFailureImageUnavailable, Message: "pin unavailable"}})
				if err != nil || latched.State != contract.JobFailed || len(policyStopJSON(t, latched)) != 0 || latched.NextRestartAt != nil {
					t.Fatalf("image latch = %+v %v", latched, err)
				}
				if _, err := h.store.SetServiceDesiredState(t.Context(), job.JobID, contract.ServiceDesiredRunning); err == nil {
					t.Fatal("start bypassed image latch")
				}
			} else {
				if _, err := h.store.RemoveService(t.Context(), job.JobID); err != nil {
					t.Fatal(err)
				}
				_, _ = h.store.CompleteAttempt(t.Context(), "agent", job.JobID, claim.Lease.AttemptID, completion)
				removed, err := h.store.GetJob(t.Context(), job.JobID)
				if err != nil {
					t.Fatal(err)
				}
				if removed.State != contract.JobRemovalPending || removed.Removal == nil || removed.Removal.RemovalDesiredState != contract.ServiceDesiredRemoved || len(policyStopJSON(t, removed)) != 0 {
					t.Fatalf("removal overwritten = %+v", removed)
				}
				if _, err := h.store.SetServiceDesiredState(t.Context(), job.JobID, contract.ServiceDesiredRunning); err == nil {
					t.Fatal("start bypassed removal")
				}
			}
		})
	}
}

func TestOnFailureOCICompletion(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{Jitter: func(d time.Duration) time.Duration { return d }}, map[string]NodePolicy{"service-node": DefaultNodePolicy("service")})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.registerWithCapabilities(agent, "service-node", map[string]bool{"kind:oci": true})
	spec := capabilityJobSpec("oci-policy-clean", contract.JobKindOCI, contract.JobClassService, "", nil)
	spec.Restart = "on-failure"
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusCreated {
		t.Fatalf("submit = %d %s", status, body)
	}
	var job Job
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatal(err)
	}
	claim := claimRestartService(t, h, agent, node)
	if _, err := h.store.ObserveAttemptImage(t.Context(), "agent", job.JobID, claim.Lease.AttemptID, testImageObservation(claim.Lease.FencingToken)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.StartAttempt(t.Context(), "agent", job.JobID, claim.Lease.AttemptID, StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
		t.Fatal(err)
	}
	zero := 0
	completed, err := h.store.CompleteAttempt(t.Context(), "agent", job.JobID, claim.Lease.AttemptID, CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "oci-clean", Result: ProcessResult{ExitCode: &zero}, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt})
	if err != nil || completed.State != contract.JobStopped || completed.DesiredState != contract.ServiceDesiredRunning || completed.RestartStreak != 0 || len(policyStopJSON(t, completed)) == 0 {
		t.Fatalf("OCI stop = %+v %v", completed, err)
	}
}
