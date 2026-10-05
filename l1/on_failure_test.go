package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
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
		initiator        contract.TerminationCause
		restart          string
		want             contract.JobState
		streak, lifetime int
	}{
		{"clean", ProcessResult{ExitCode: &zero}, "", "", contract.JobStopped, 2, 5},
		{"incomplete logs", ProcessResult{ExitCode: &zero, LogEvidenceIncomplete: true}, "", "", contract.JobStopped, 2, 5},
		{"crash", ProcessResult{ExitCode: &one}, "", "", contract.JobQueued, 3, 6},
		{"output latch", ProcessResult{ExitCode: &zero, OutputError: "disk write failed"}, "", "", contract.JobFailed, 2, 5},
		{"spawn latch", ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureImageUnavailable, Message: "missing image"}}, "", "", contract.JobFailed, 2, 5},
		{"spontaneous", ProcessResult{Signal: "killed", TerminationCause: contract.TerminationCauseSpontaneous}, "", "", contract.JobQueued, 3, 6},
		{"agent interruption", ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}, "", "", contract.JobQueued, 2, 6},
		{"guardian interruption", ProcessResult{Signal: "killed", TerminationCause: contract.TerminationCauseGuardian}, "", "", contract.JobQueued, 2, 6},
		// A TERM handler answers the agent's or guardian's request with an exit
		// code of its own. Whatever the code, that is an interruption: never a
		// policy stop, never a payload failure, and never a streak step.
		{"agent-requested clean exit", ProcessResult{ExitCode: &zero}, contract.TerminationCauseAgent, "", contract.JobQueued, 2, 6},
		{"guardian-requested clean exit", ProcessResult{ExitCode: &zero}, contract.TerminationCauseGuardian, "", contract.JobQueued, 2, 6},
		{"guardian-requested nonzero exit", ProcessResult{ExitCode: &one}, contract.TerminationCauseGuardian, "", contract.JobQueued, 2, 6},
		{"always guardian-requested clean exit", ProcessResult{ExitCode: &zero}, contract.TerminationCauseGuardian, contract.RestartAlways, contract.JobQueued, 2, 6},
	}
	s := &Store{leaseDuration: 30 * time.Second, restartJitter: func(d time.Duration) time.Duration { return d }}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			restart := tc.restart
			if restart == "" {
				restart = contract.RestartOnFailure
			}
			job := Job{State: contract.JobRunning, Spec: contract.JobSpec{Restart: restart}, ServiceJob: &ServiceJob{DesiredState: contract.ServiceDesiredRunning, RestartStreak: 2, LifetimeRestartCount: 5}}
			raw, _ := json.Marshal(tc.result)
			// Use completion through the store for restart precedence; this classifier
			// row tests that terminal arms still precede a clean exit.
			completion := CompletionRequest{Result: tc.result, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt, TerminationInitiator: tc.initiator}
			p := s.classifyServiceCompletion(job, completion, raw, time.Unix(100, 0), false)
			if p.jobState != tc.want || p.restartStreak != tc.streak || p.lifetimeRestartCount != tc.lifetime {
				t.Fatalf("policy = %+v", p)
			}
			if tc.want == contract.JobQueued && (p.nextRestartNS == nil || *p.nextRestartNS != time.Unix(100, 0).Add(serviceRestartDelay(tc.streak, 30*time.Second, s.restartJitter)).UnixNano()) && tc.streak == 3 {
				t.Fatalf("failure backoff changed: %+v", p)
			}
			if tc.initiator != "" && (p.policyStop != nil || p.updateLastFailure || p.nextRestartNS == nil ||
				*p.nextRestartNS != time.Unix(100, 0).Add(prestartRetryDelay(tc.lifetime, s.restartJitter)).UnixNano()) {
				t.Fatalf("requested termination was not an infrastructure interruption: %+v", p)
			}
		})
	}
}

// A payload that handles TERM must come back after the agent or its guardian
// ends it, however cleanly it exits. Only a clean exit nobody asked for stops
// an on-failure service.
func TestOnFailureRequestedTerminationRequeues(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{Jitter: func(d time.Duration) time.Duration { return d }}, map[string]NodePolicy{"service-node": DefaultNodePolicy("service")})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "service-node")
	spec := validJobSpec("requested-termination", []string{"service"})
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
	zero := 0
	complete := func(claim Claim, key string, initiator contract.TerminationCause) Job {
		t.Helper()
		completion := CompletionRequest{
			FencingToken: claim.Lease.FencingToken, IdempotencyKey: key, Result: ProcessResult{ExitCode: &zero},
			RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt, TerminationInitiator: initiator,
		}
		path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, claim.Lease.AttemptID)
		status, _, body := h.do(agent, http.MethodPost, path, completion)
		if status != http.StatusOK {
			t.Fatalf("complete = %d %s", status, body)
		}
		replayStatus, _, replayBody := h.do(agent, http.MethodPost, path, completion)
		if replayStatus != http.StatusOK {
			t.Fatalf("replay = %d %s", replayStatus, replayBody)
		}
		var completed Job
		if err := json.Unmarshal(body, &completed); err != nil {
			t.Fatal(err)
		}
		return completed
	}
	previous := ""
	for _, initiator := range []contract.TerminationCause{contract.TerminationCauseAgent, contract.TerminationCauseGuardian} {
		claim := claimRestartService(t, h, agent, node)
		if claim.Lease.AttemptID == previous {
			t.Fatal("requeue reused attempt")
		}
		previous = claim.Lease.AttemptID
		completed := complete(claim, "requested-"+string(initiator), initiator)
		if completed.State != contract.JobQueued || completed.DesiredState != contract.ServiceDesiredRunning ||
			len(policyStopJSON(t, completed)) != 0 || completed.RestartStreak != 0 || len(completed.LastFailure) != 0 || completed.NextRestartAt == nil {
			t.Fatalf("%s-requested exit zero was not requeued: %+v", initiator, completed)
		}
		h.clock.Advance(prestartRetryDelay(completed.LifetimeRestartCount, func(d time.Duration) time.Duration { return d }))
	}
	final := claimRestartService(t, h, agent, node)
	stopped := complete(final, "self-exit", "")
	if stopped.State != contract.JobStopped || stopped.DesiredState != contract.ServiceDesiredRunning || len(policyStopJSON(t, stopped)) == 0 {
		t.Fatalf("self-exit did not record a policy stop: %+v", stopped)
	}
}

// termination_initiator names who asked for an exit-code termination. A signal
// carries its own initiator, and a spontaneous exit names none.
func TestCompletionRefusesIncoherentTerminationInitiator(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{}, map[string]NodePolicy{"service-node": DefaultNodePolicy("service")})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "service-node")
	job := submitRestartService(t, h, client, "incoherent-initiator", []string{"service"}, nil)
	claim := claimRestartService(t, h, agent, node)
	zero := 0
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, claim.Lease.AttemptID)
	for name, completion := range map[string]CompletionRequest{
		"signal":      {Result: ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}, TerminationInitiator: contract.TerminationCauseAgent},
		"spawn error": {Result: ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureProcessSpawn, Message: "missing"}}, TerminationInitiator: contract.TerminationCauseGuardian},
		"spontaneous": {Result: ProcessResult{ExitCode: &zero}, TerminationInitiator: contract.TerminationCauseSpontaneous},
		"unknown":     {Result: ProcessResult{ExitCode: &zero}, TerminationInitiator: "operator"},
	} {
		completion.FencingToken, completion.IdempotencyKey = claim.Lease.FencingToken, "incoherent-"+name
		completion.RuntimeQuiescenceEvidence = RuntimeQuiescenceAttempt
		status, _, body := h.do(agent, http.MethodPost, path, completion)
		assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
	}
	if got := getRestartService(t, h, job.JobID); got.State != contract.JobClaimed || got.CurrentAttemptID != claim.Lease.AttemptID {
		t.Fatalf("refused completion changed the job: %+v", got)
	}
}

// A repeat stop of a stopped service is a validated no-op. The first stop of a
// policy-stopped service is the one stop that has intent left to record.
func TestOnFailureRepeatStopIsNoOp(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{Jitter: func(d time.Duration) time.Duration { return d }}, map[string]NodePolicy{"service-node": DefaultNodePolicy("service")})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "service-node")
	spec := validJobSpec("repeat-stop", []string{"service"})
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
	if _, err := h.store.CompleteAttempt(t.Context(), "agent", job.JobID, claim.Lease.AttemptID, CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "self-exit", Result: ProcessResult{ExitCode: &zero}, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt}); err != nil {
		t.Fatal(err)
	}
	stop := func() Job {
		t.Helper()
		h.clock.Advance(time.Second)
		status, _, body := h.do(client, http.MethodPut, serviceMutationPath(job.JobID, "desired-state"), ServiceDesiredStateRequest{DesiredState: contract.ServiceDesiredStopped})
		if status != http.StatusAccepted {
			t.Fatalf("stop = %d %s", status, body)
		}
		var stopped Job
		if err := json.Unmarshal(body, &stopped); err != nil {
			t.Fatal(err)
		}
		return stopped
	}
	policyStopped := getRestartService(t, h, job.JobID)
	first := stop()
	if first.State != contract.JobStopped || first.DesiredState != contract.ServiceDesiredStopped || len(policyStopJSON(t, first)) == 0 || !first.UpdatedAt.After(policyStopped.UpdatedAt) {
		t.Fatalf("stop of a policy-stopped service = %+v", first)
	}
	repeat := stop()
	if repeat.State != contract.JobStopped || repeat.DesiredState != contract.ServiceDesiredStopped || !repeat.UpdatedAt.Equal(first.UpdatedAt) || len(policyStopJSON(t, repeat)) == 0 {
		t.Fatalf("repeat stop was not a no-op: first updated %s, repeat %+v", first.UpdatedAt, repeat)
	}
}

// A database from before restart: on-failure has no policy_stop_json column.
// Opening it adds the column, and the services already in it read and run as
// they did before.
func TestStoreAddsPolicyStopToExistingServiceJobs(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "pre-policy-stop.sqlite")
	clock := &fakeClock{now: time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)}
	options := StoreOptions{Clock: clock, LeaseDuration: 30 * time.Second, Jitter: func(d time.Duration) time.Duration { return d }}
	store, err := OpenStore(path, options)
	if err != nil {
		t.Fatal(err)
	}
	identity := fabric.Identity{NodeID: "agent"}
	policy := DefaultNodePolicy("service")
	registration := contract.NodeRegistration{
		NodeID: "service-node", BootSessionID: "boot-service-node", OS: "linux", Architecture: "arm64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true}, CapabilityRevision: 1, CapabilityObservedAt: clock.Now(), MissingCapabilities: []string{},
	}
	node, err := store.RegisterNode(ctx, identity, registration, policy, true)
	if err != nil {
		t.Fatal(err)
	}
	spec := validJobSpec("pre-policy-stop", []string{"service"})
	spec.Class = contract.JobClassService
	spec.Restart = contract.RestartAlways
	spec.Execution.HandoffDirectory = ""
	existing, _, err := store.CreateJob(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimJob(ctx, identity.NodeID, node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil || claim.Job.JobID != existing.JobID {
		t.Fatalf("claim = %+v %v", claim, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.Exec(`ALTER TABLE service_jobs DROP COLUMN policy_stop_json`)
	closeErr := database.Close()
	if err != nil || closeErr != nil {
		t.Fatal(errors.Join(err, closeErr))
	}

	store, err = OpenStore(path, options)
	if err != nil {
		t.Fatalf("open pre-policy-stop database: %v", err)
	}
	defer store.Close()
	var columns int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('service_jobs') WHERE name='policy_stop_json'`).Scan(&columns); err != nil || columns != 1 {
		t.Fatalf("policy_stop_json columns = %d %v", columns, err)
	}
	reopened, err := store.GetJob(ctx, existing.JobID)
	if err != nil || reopened.State != contract.JobClaimed || reopened.DesiredState != contract.ServiceDesiredRunning || reopened.PolicyStop != nil {
		t.Fatalf("existing service after migration = %+v %v", reopened, err)
	}
	// The existing always service behaves as it did: a clean exit restarts it.
	zero := 0
	completed, err := store.CompleteAttempt(ctx, identity.NodeID, existing.JobID, claim.Lease.AttemptID, CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "after-migration", Result: ProcessResult{ExitCode: &zero}, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt,
	})
	if err != nil || completed.State != contract.JobQueued || completed.RestartStreak != 1 || completed.PolicyStop != nil {
		t.Fatalf("existing service completion after migration = %+v %v", completed, err)
	}
	var stored sql.NullString
	if err := store.db.QueryRow(`SELECT policy_stop_json FROM service_jobs WHERE job_id=?`, existing.JobID).Scan(&stored); err != nil || stored.Valid {
		t.Fatalf("migrated policy stop = %+v %v", stored, err)
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
