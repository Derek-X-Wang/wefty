package l1

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestServiceRestartableCompletionRequeuesWithPersistedBackoff(t *testing.T) {
	assertServiceRestartableCompletionRequeuesWithPersistedBackoff(t)
}

func TestServiceCompletionClassification(t *testing.T) {
	assertServiceCompletionClassification(t)
}

func TestPublishedListenerFailureRequeuesWithoutLatching(t *testing.T) {
	assertPublishedListenerFailureRequeuesWithoutLatching(t)
}

func TestOrdinaryOCIServiceOOMEvidenceKeepsExitRestartPolicy(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{Jitter: func(delay time.Duration) time.Duration { return delay }}, map[string]NodePolicy{
		"service-node": DefaultNodePolicy("service"),
	})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.registerWithCapabilities(agent, "service-node", map[string]bool{"kind:oci": true})
	spec := capabilityJobSpec("ordinary-oci-oom-exit-zero", contract.JobKindOCI, contract.JobClassService, "", nil)
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusCreated {
		t.Fatalf("submit ordinary OCI service status = %d body=%s", status, body)
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
	exitZero := 0
	completed, err := h.store.CompleteAttempt(t.Context(), "agent", job.JobID, claim.Lease.AttemptID, CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "ordinary-oci-oom-exit-zero",
		Result: ProcessResult{ExitCode: &exitZero, OOM: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != contract.JobQueued || completed.RestartStreak != 1 || completed.NextRestartAt == nil {
		t.Fatalf("ordinary OCI OOM evidence latched resource failure: %+v", completed)
	}
	if len(completed.LastFailure) == 0 || bytes.Contains(completed.LastFailure, []byte(contract.SpawnFailureInsufficientMemory)) {
		t.Fatalf("ordinary OCI last_failure synthesized Computer latch: %s", completed.LastFailure)
	}
}

func assertPublishedListenerFailureRequeuesWithoutLatching(t *testing.T) {
	t.Helper()
	maximumOne := 1
	h := newIntegrationHarnessWithOptions(t, StoreOptions{
		Jitter: func(delay time.Duration) time.Duration { return delay },
	}, map[string]NodePolicy{"service-node": DefaultNodePolicy("service")})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "service-node")
	job := submitRestartService(t, h, client, "published-listener-failure", []string{"service"}, &maximumOne)
	first := claimRestartService(t, h, agent, node)

	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, first.Lease.AttemptID)
	status, _, body := h.do(agent, http.MethodPost, path, CompletionRequest{
		FencingToken:   first.Lease.FencingToken,
		IdempotencyKey: "published-listener-failure",
		Result: ProcessResult{SpawnError: &contract.SpawnFailure{
			Code: contract.SpawnFailurePublishedListener, Message: "Fabric published listener stopped accepting connections",
		}},
	})
	if status != http.StatusOK {
		t.Fatalf("front-door completion status = %d body=%s", status, body)
	}

	requeued := getRestartService(t, h, job.JobID)
	wantRestart := h.clock.Now().Add(time.Second)
	if requeued.State != contract.JobQueued || requeued.RestartStreak != 0 || requeued.LifetimeRestartCount != 1 ||
		requeued.NextRestartAt == nil || !requeued.NextRestartAt.Equal(wantRestart) {
		t.Fatalf("front-door failure service state/streak/lifetime/backoff = %q/%d/%d/%v, want queued/0/1/%s", requeued.State, requeued.RestartStreak, requeued.LifetimeRestartCount, requeued.NextRestartAt, wantRestart)
	}
	h.clock.Advance(time.Second)
	second := claimRestartService(t, h, agent, node)
	if second.Lease.AttemptID == first.Lease.AttemptID || second.Lease.FencingToken == first.Lease.FencingToken {
		t.Fatalf("front-door recovery reused attempt authority: first=%#v second=%#v", first.Lease, second.Lease)
	}
}

func assertServiceCompletionClassification(t *testing.T) {
	t.Helper()
	exitZero := 0
	exitOne := 1
	maximumOne := 1
	tests := []struct {
		name                 string
		result               ProcessResult
		maximum              *int
		wantJob              contract.JobState
		wantAttempt          contract.AttemptState
		wantStreak           int
		wantLifetimeRestarts int
		wantBackoff          bool
		wantFailure          bool
		wantFailureCode      contract.SpawnFailureCode
		wantResourceFacts    bool
	}{
		{name: "exit zero is a successful attempt but unexpected service termination", result: ProcessResult{ExitCode: &exitZero}, wantJob: contract.JobQueued, wantAttempt: contract.AttemptSucceeded, wantStreak: 1, wantLifetimeRestarts: 1, wantBackoff: true, wantFailure: true},
		{name: "nonzero exit restarts", result: ProcessResult{ExitCode: &exitOne}, wantJob: contract.JobQueued, wantAttempt: contract.AttemptFailed, wantStreak: 1, wantLifetimeRestarts: 1, wantBackoff: true, wantFailure: true},
		{name: "spontaneous signal restarts", result: ProcessResult{Signal: "killed", TerminationCause: contract.TerminationCauseSpontaneous}, wantJob: contract.JobQueued, wantAttempt: contract.AttemptFailed, wantStreak: 1, wantLifetimeRestarts: 1, wantBackoff: true, wantFailure: true},
		{name: "startup readiness timeout is whitelisted", result: ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureStartupReadinessTimeout, Message: "backend never accepted"}}, wantJob: contract.JobQueued, wantAttempt: contract.AttemptFailed, wantStreak: 1, wantLifetimeRestarts: 1, wantBackoff: true, wantFailure: true, wantFailureCode: contract.SpawnFailureStartupReadinessTimeout},
		{name: "published listener failure is infrastructure", result: ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailurePublishedListener, Message: "listener failed"}}, wantJob: contract.JobQueued, wantAttempt: contract.AttemptFailed, wantLifetimeRestarts: 1, wantBackoff: true},
		{name: "process runtime unavailable is terminal", result: ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureRuntimeUnavailable, Message: "unexpected process code"}}, wantJob: contract.JobFailed, wantAttempt: contract.AttemptFailed, wantFailure: true, wantFailureCode: contract.SpawnFailureRuntimeUnavailable},
		{name: "process runtime failure is terminal", result: ProcessResult{RuntimeFailure: &contract.RuntimeFailure{Code: contract.RuntimeFailureUnavailable, Message: "unexpected process runtime arm"}}, wantJob: contract.JobFailed, wantAttempt: contract.AttemptFailed, wantFailure: true},
		{name: "deterministic spawn failure latches", result: ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureProcessSpawn, Message: "executable missing"}}, wantJob: contract.JobFailed, wantAttempt: contract.AttemptFailed, wantFailure: true, wantFailureCode: contract.SpawnFailureProcessSpawn},
		{name: "unknown spawn failure defaults terminal", result: ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureCode("future_spawn_failure"), Message: "unknown"}}, wantJob: contract.JobFailed, wantAttempt: contract.AttemptFailed, wantFailure: true, wantFailureCode: contract.SpawnFailureCode("future_spawn_failure")},
		{name: "published port occupied latches", result: ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailurePublishedPortOccupied, Message: "port 8080 occupied"}}, wantJob: contract.JobFailed, wantAttempt: contract.AttemptFailed, wantFailure: true, wantFailureCode: contract.SpawnFailurePublishedPortOccupied},
		{name: "insufficient disk latches without restart accounting", result: ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureInsufficientDisk, Message: "allocation failed", RequestedBytes: 8 << 30, ObservedAvailableBytes: 2 << 30}}, wantJob: contract.JobFailed, wantAttempt: contract.AttemptFailed, wantFailure: true, wantFailureCode: contract.SpawnFailureInsufficientDisk, wantResourceFacts: true},
		{name: "insufficient memory latches without restart accounting", result: ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureInsufficientMemory, Message: "cap sum exceeded", RequestedBytes: 8 << 30, ObservedAvailableBytes: 2 << 30}}, wantJob: contract.JobFailed, wantAttempt: contract.AttemptFailed, wantFailure: true, wantFailureCode: contract.SpawnFailureInsufficientMemory, wantResourceFacts: true},
		{name: "genuine output error latches", result: ProcessResult{OutputError: "SQLite corruption"}, wantJob: contract.JobFailed, wantAttempt: contract.AttemptFailed, wantFailure: true},
		{name: "guardian termination is infrastructure", result: ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseGuardian}, wantJob: contract.JobQueued, wantAttempt: contract.AttemptFailed, wantLifetimeRestarts: 1, wantBackoff: true},
		{name: "configured streak maximum latches on first countable termination", result: ProcessResult{ExitCode: &exitOne}, maximum: &maximumOne, wantJob: contract.JobFailed, wantAttempt: contract.AttemptFailed, wantStreak: 1, wantFailure: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newIntegrationHarnessWithOptions(t, StoreOptions{
				Jitter: func(delay time.Duration) time.Duration { return delay },
			}, map[string]NodePolicy{"service-node": DefaultNodePolicy("service")})
			client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
			agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
			node := h.register(agent, "service-node")
			job := submitRestartService(t, h, client, "classification", []string{"service"}, test.maximum)
			claim := claimRestartService(t, h, agent, node)

			path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, claim.Lease.AttemptID)
			status, _, body := h.do(agent, http.MethodPost, path, CompletionRequest{
				FencingToken:   claim.Lease.FencingToken,
				IdempotencyKey: "classification-completion",
				Result:         test.result,
			})
			if status != http.StatusOK {
				t.Fatalf("complete status = %d body=%s", status, body)
			}

			completed := getRestartService(t, h, job.JobID)
			if completed.State != test.wantJob || completed.RestartStreak != test.wantStreak || completed.LifetimeRestartCount != test.wantLifetimeRestarts {
				t.Fatalf("service state/streak/lifetime = %q/%d/%d, want %q/%d/%d", completed.State, completed.RestartStreak, completed.LifetimeRestartCount, test.wantJob, test.wantStreak, test.wantLifetimeRestarts)
			}
			if (completed.NextRestartAt != nil) != test.wantBackoff {
				t.Fatalf("next_restart_at presence = %t, want %t", completed.NextRestartAt != nil, test.wantBackoff)
			}
			if (len(completed.LastFailure) != 0) != test.wantFailure {
				t.Fatalf("last_failure presence = %t, want %t (%s)", len(completed.LastFailure) != 0, test.wantFailure, completed.LastFailure)
			}
			if test.wantFailureCode != "" {
				var failure contract.SpawnFailure
				if err := json.Unmarshal(completed.LastFailure, &failure); err != nil {
					t.Fatalf("decode last_failure: %v", err)
				}
				if failure.Code != test.wantFailureCode {
					t.Fatalf("last_failure.code = %q, want %q", failure.Code, test.wantFailureCode)
				}
				if test.wantResourceFacts && (failure.NodeID != node.NodeID || failure.RequestedBytes != 8<<30 || failure.ObservedAvailableBytes != 2<<30) {
					t.Fatalf("last_failure resource facts = %+v", failure)
				}
			}
			assertAttemptState(t, h, claim.Lease.AttemptID, test.wantAttempt)
		})
	}
}

func TestServiceBackoffUsesPinnedSequenceAndEffectiveLeaseCap(t *testing.T) {
	assertServiceBackoffUsesPinnedSequenceAndEffectiveLeaseCap(t)
}

func assertServiceBackoffUsesPinnedSequenceAndEffectiveLeaseCap(t *testing.T) {
	t.Helper()
	h := newIntegrationHarnessWithOptions(t, StoreOptions{
		LeaseDuration:  30 * time.Second,
		NodeStaleAfter: time.Hour,
		NodeDeadAfter:  2 * time.Hour,
		Jitter:         func(delay time.Duration) time.Duration { return delay },
	}, map[string]NodePolicy{"service-node": DefaultNodePolicy("service")})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "service-node")
	job := submitRestartService(t, h, client, "backoff-sequence", []string{"service"}, nil)

	wantDelays := []time.Duration{
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		30 * time.Second,
		30 * time.Second,
	}
	previousAttemptID := ""
	previousFence := ""
	for index, wantDelay := range wantDelays {
		claim := claimRestartService(t, h, agent, node)
		if claim.Job.JobID != job.JobID {
			t.Fatalf("claim %d job = %q, want %q", index+1, claim.Job.JobID, job.JobID)
		}
		if claim.Lease.AttemptID == previousAttemptID || claim.Lease.FencingToken == previousFence {
			t.Fatalf("claim %d reused restart identity attempt=%q fence=%q", index+1, claim.Lease.AttemptID, claim.Lease.FencingToken)
		}
		previousAttemptID = claim.Lease.AttemptID
		previousFence = claim.Lease.FencingToken

		exitCode := index + 1
		path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, claim.Lease.AttemptID)
		status, _, body := h.do(agent, http.MethodPost, path, CompletionRequest{
			FencingToken:   claim.Lease.FencingToken,
			IdempotencyKey: fmt.Sprintf("backoff-%d", index+1),
			Result:         ProcessResult{ExitCode: &exitCode},
		})
		if status != http.StatusOK {
			t.Fatalf("completion %d status = %d body=%s", index+1, status, body)
		}
		requeued := getRestartService(t, h, job.JobID)
		wantRestart := h.clock.Now().Add(wantDelay)
		if requeued.NextRestartAt == nil || !requeued.NextRestartAt.Equal(wantRestart) {
			t.Fatalf("completion %d next_restart_at = %v, want %s", index+1, requeued.NextRestartAt, wantRestart)
		}
		h.clock.Advance(wantDelay)
	}

	if got := serviceRestartDelay(6, 3*time.Second, func(delay time.Duration) time.Duration { return delay }); got != 3*time.Second {
		t.Fatalf("effective lease cap delay = %s, want 3s", got)
	}
}

func TestServiceBackoffDrawsJitterOnceAndAppliesFloorAfterCap(t *testing.T) {
	t.Run("one draw survives completion replay", func(t *testing.T) {
		draws := 0
		h := newIntegrationHarnessWithOptions(t, StoreOptions{
			LeaseDuration: 10 * time.Second,
			Jitter: func(delay time.Duration) time.Duration {
				draws++
				return delay * 12 / 10
			},
		}, map[string]NodePolicy{"service-node": DefaultNodePolicy("service")})
		client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
		agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
		node := h.register(agent, "service-node")
		job := submitRestartService(t, h, client, "jitter-once", []string{"service"}, nil)
		claim := claimRestartService(t, h, agent, node)
		exitCode := 1
		path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, claim.Lease.AttemptID)
		request := CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "jitter-once", Result: ProcessResult{ExitCode: &exitCode}}
		for range 2 {
			status, _, body := h.do(agent, http.MethodPost, path, request)
			if status != http.StatusOK {
				t.Fatalf("completion status = %d body=%s", status, body)
			}
		}
		job = getRestartService(t, h, job.JobID)
		want := h.clock.Now().Add(1200 * time.Millisecond)
		if draws != 1 || job.NextRestartAt == nil || !job.NextRestartAt.Equal(want) {
			t.Fatalf("jitter draws/next_restart_at = %d/%v, want 1/%s", draws, job.NextRestartAt, want)
		}
	})

	t.Run("tiny effective lease still has anti-spin floor", func(t *testing.T) {
		h := newIntegrationHarnessWithOptions(t, StoreOptions{
			LeaseDuration: 200 * time.Millisecond,
			Jitter:        func(delay time.Duration) time.Duration { return delay },
		}, map[string]NodePolicy{"service-node": DefaultNodePolicy("service")})
		client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
		agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
		node := h.register(agent, "service-node")
		job := submitRestartService(t, h, client, "restart-floor", []string{"service"}, nil)
		claim := claimRestartService(t, h, agent, node)
		exitCode := 1
		path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, claim.Lease.AttemptID)
		status, _, body := h.do(agent, http.MethodPost, path, CompletionRequest{
			FencingToken:   claim.Lease.FencingToken,
			IdempotencyKey: "restart-floor",
			Result:         ProcessResult{ExitCode: &exitCode},
		})
		if status != http.StatusOK {
			t.Fatalf("completion status = %d body=%s", status, body)
		}
		job = getRestartService(t, h, job.JobID)
		want := h.clock.Now().Add(500 * time.Millisecond)
		if job.NextRestartAt == nil || !job.NextRestartAt.Equal(want) {
			t.Fatalf("next_restart_at = %v, want anti-spin floor %s", job.NextRestartAt, want)
		}
	})
}

func TestServiceLeaseExpiryRequeuesWithoutConsumingRestartBudget(t *testing.T) {
	assertServiceLeaseExpiryRequeuesWithoutConsumingRestartBudget(t)
}

func assertServiceLeaseExpiryRequeuesWithoutConsumingRestartBudget(t *testing.T) {
	t.Helper()
	tests := []struct {
		name   string
		expire func(*testing.T, *integrationHarness, *http.Client, Job, Claim)
	}{
		{
			name: "periodic reconciler",
			expire: func(t *testing.T, h *integrationHarness, _ *http.Client, _ Job, _ Claim) {
				t.Helper()
				result, err := h.store.Reconcile(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if result.ExpiredAttempts != 1 {
					t.Fatalf("expired attempts = %d, want 1", result.ExpiredAttempts)
				}
			},
		},
		{
			name: "inline renewal expiry",
			expire: func(t *testing.T, h *integrationHarness, agent *http.Client, job Job, claim Claim) {
				t.Helper()
				path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/lease", job.JobID, claim.Lease.AttemptID)
				status, _, body := h.do(agent, http.MethodPost, path, RenewalRequest{FencingToken: claim.Lease.FencingToken})
				assertAPIError(t, status, body, http.StatusConflict, contract.ErrorLeaseExpired)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newIntegrationHarnessWithOptions(t, StoreOptions{
				LeaseDuration: 3 * time.Second,
				Jitter:        func(delay time.Duration) time.Duration { return delay },
			}, map[string]NodePolicy{"service-node": DefaultNodePolicy("service")})
			client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
			agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
			node := h.register(agent, "service-node")
			job := submitRestartService(t, h, client, "expiry-"+test.name, []string{"service"}, nil)
			claim := claimRestartService(t, h, agent, node)
			lastFailure := []byte(`{"code":"prior_failure"}`)
			if _, err := h.store.db.Exec(`UPDATE service_jobs SET bound_node_id=?, restart_streak=2,
				lifetime_restart_count=7, last_failure=?, healthy_since_ns=?, published_attempt_id=? WHERE job_id=?`,
				node.NodeID, lastFailure, h.clock.Now().UnixNano(), claim.Lease.AttemptID, job.JobID); err != nil {
				t.Fatal(err)
			}

			h.clock.Advance(3 * time.Second)
			test.expire(t, h, agent, job, claim)
			requeued := getRestartService(t, h, job.JobID)
			if requeued.State != contract.JobQueued || requeued.RestartStreak != 2 || requeued.LifetimeRestartCount != 7 {
				t.Fatalf("expired service state/streak/lifetime = %q/%d/%d, want queued/2/7", requeued.State, requeued.RestartStreak, requeued.LifetimeRestartCount)
			}
			if requeued.NextRestartAt != nil || requeued.HealthySinceAt != nil || requeued.PublishedAttemptID != "" {
				t.Fatalf("expired service retained restart/readiness state: %#v", requeued.ServiceJob)
			}
			if !bytes.Equal(requeued.LastFailure, lastFailure) {
				t.Fatalf("expired service changed prior failure evidence = %s, want %s", requeued.LastFailure, lastFailure)
			}
			if requeued.CurrentAttemptID != claim.Lease.AttemptID {
				t.Fatalf("expired current attempt = %q, want replayable %q", requeued.CurrentAttemptID, claim.Lease.AttemptID)
			}
			assertAttemptState(t, h, claim.Lease.AttemptID, contract.AttemptLost)

			fresh := claimRestartService(t, h, agent, node)
			if fresh.Job.JobID != job.JobID || fresh.Lease.AttemptID == claim.Lease.AttemptID || fresh.Lease.FencingToken == claim.Lease.FencingToken {
				t.Fatalf("fresh restart identity = job:%q attempt:%q fence:%q; first=%#v", fresh.Job.JobID, fresh.Lease.AttemptID, fresh.Lease.FencingToken, claim.Lease)
			}
		})
	}
}

func TestStoppedServiceLeaseExpiryLatchesWhenQuiescenceIsUnconfirmed(t *testing.T) {
	assertStoppedServiceLeaseExpiryLatchesWhenQuiescenceIsUnconfirmed(t)
}

func assertStoppedServiceLeaseExpiryLatchesWhenQuiescenceIsUnconfirmed(t *testing.T) {
	t.Helper()
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
		"service-node": DefaultNodePolicy("service"),
	})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "service-node")
	job := submitRestartService(t, h, client, "stopping-expiry", []string{"service"}, nil)
	claim := claimRestartService(t, h, agent, node)
	if _, err := h.store.db.Exec("UPDATE service_jobs SET desired_state=?, bound_node_id=? WHERE job_id=?", contract.ServiceDesiredStopped, node.NodeID, job.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec("UPDATE jobs SET state=? WHERE job_id=?", contract.JobStopping, job.JobID); err != nil {
		t.Fatal(err)
	}
	h.clock.Advance(3 * time.Second)
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/lease", job.JobID, claim.Lease.AttemptID)
	status, _, body := h.do(agent, http.MethodPost, path, RenewalRequest{FencingToken: claim.Lease.FencingToken})
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorLeaseExpired)
	failed := getRestartService(t, h, job.JobID)
	if failed.State != contract.JobFailed || failed.RestartStreak != 0 || failed.NextRestartAt != nil {
		t.Fatalf("stopping expiry service = %#v", failed)
	}
	assertAttemptState(t, h, claim.Lease.AttemptID, contract.AttemptLost)
}

func TestPlannedServiceStopCompletesWithoutRestart(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
		"service-node": DefaultNodePolicy("service"),
	})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "service-node")
	job := submitRestartService(t, h, client, "planned-stop", []string{"service"}, nil)
	claim := claimRestartService(t, h, agent, node)
	if _, err := h.store.db.Exec("UPDATE service_jobs SET desired_state=?, bound_node_id=? WHERE job_id=?", contract.ServiceDesiredStopped, node.NodeID, job.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec("UPDATE jobs SET state=? WHERE job_id=?", contract.JobStopping, job.JobID); err != nil {
		t.Fatal(err)
	}
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, claim.Lease.AttemptID)
	status, _, body := h.do(agent, http.MethodPost, path, CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "planned-stop",
		Result:                    ProcessResult{OutputError: "upload logs: object store unavailable"},
		RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt,
	})
	if status != http.StatusOK {
		t.Fatalf("planned stop completion status = %d body=%s", status, body)
	}
	stopped := getRestartService(t, h, job.JobID)
	if stopped.State != contract.JobStopped || stopped.DesiredState != contract.ServiceDesiredStopped || stopped.RestartStreak != 0 || stopped.LifetimeRestartCount != 0 || stopped.NextRestartAt != nil {
		t.Fatalf("planned stop service = %#v", stopped)
	}
	if stopped.CurrentAttemptID != claim.Lease.AttemptID {
		t.Fatalf("stopped current attempt = %q, want replayable %q", stopped.CurrentAttemptID, claim.Lease.AttemptID)
	}
}

func TestPlannedServiceStopLatchesWhenRuntimeQuiescenceFails(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
		"service-node": DefaultNodePolicy("service"),
	})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.registerWithCapabilities(agent, "service-node", map[string]bool{"kind:oci": true})
	spec := capabilityJobSpec("planned-stop-unverified", contract.JobKindOCI, contract.JobClassService, "", nil)
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusCreated {
		t.Fatalf("submit OCI stop-latch service status = %d body=%s", status, body)
	}
	var job Job
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatal(err)
	}
	claim := claimRestartService(t, h, agent, node)
	if _, err := h.store.ObserveAttemptImage(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, testImageObservation(claim.Lease.FencingToken)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.StartAttempt(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec("UPDATE service_jobs SET desired_state=?, bound_node_id=? WHERE job_id=?", contract.ServiceDesiredStopped, node.NodeID, job.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec("UPDATE jobs SET state=? WHERE job_id=?", contract.JobStopping, job.JobID); err != nil {
		t.Fatal(err)
	}
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, claim.Lease.AttemptID)
	status, _, body = h.do(agent, http.MethodPost, path, CompletionRequest{
		FencingToken:   claim.Lease.FencingToken,
		IdempotencyKey: "planned-stop-unverified",
		Result:         ProcessResult{OutputError: "reap and verify OCI runtime: helper sweep unavailable"},
	})
	if status != http.StatusOK {
		t.Fatalf("unverified stop completion status = %d body=%s", status, body)
	}
	failed := getRestartService(t, h, job.JobID)
	if failed.State != contract.JobFailed || failed.DesiredState != contract.ServiceDesiredStopped || failed.HoldsSlot(failed.State) || failed.LastFailure == nil {
		t.Fatalf("unverified stop service = %#v", failed)
	}
	if failed.BoundNodeID != node.NodeID || failed.Spec.Execution.OCI == nil || failed.Spec.Execution.OCI.Image.Digest == nil || *failed.Spec.Execution.OCI.Image.Digest != testTopDigest {
		t.Fatalf("unverified stop lost binding or digest pin = %#v", failed)
	}
	assertAttemptState(t, h, claim.Lease.AttemptID, contract.AttemptFailed)
}

func TestServiceStabilityWindowUsesDurableTimestampAndAliveNode(t *testing.T) {
	assertServiceStabilityWindowUsesDurableTimestampAndAliveNode(t)
}

func assertServiceStabilityWindowUsesDurableTimestampAndAliveNode(t *testing.T) {
	t.Helper()
	t.Run("continuous stability resets despite lease renewal timestamps", func(t *testing.T) {
		h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Minute}, map[string]NodePolicy{
			"service-node": DefaultNodePolicy("service"),
		})
		client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
		agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
		node := h.register(agent, "service-node")
		job := submitRestartService(t, h, client, "stability-reset", []string{"service"}, nil)
		claim := claimRestartService(t, h, agent, node)
		if _, err := h.store.RenewLease(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, claim.Lease.FencingToken); err != nil {
			t.Fatal(err)
		}
		stableSince := h.clock.Now()
		if _, err := h.store.db.Exec(`UPDATE service_jobs SET bound_node_id=?, restart_streak=4, healthy_since_ns=? WHERE job_id=?`,
			node.NodeID, stableSince.UnixNano(), job.JobID); err != nil {
			t.Fatal(err)
		}

		h.clock.Advance(time.Minute)
		if _, err := h.store.RenewLease(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, claim.Lease.FencingToken); err != nil {
			t.Fatal(err)
		}
		h.clock.Advance(time.Minute)
		if _, err := h.store.HeartbeatNode(context.Background(), "agent", node.NodeID, node.BootSessionID); err != nil {
			t.Fatal(err)
		}
		result, err := h.store.Reconcile(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if result.RestartStreakResets != 1 {
			t.Fatalf("restart streak resets = %d, want 1", result.RestartStreakResets)
		}
		stable := getRestartService(t, h, job.JobID)
		if stable.RestartStreak != 0 || stable.HealthySinceAt == nil || !stable.HealthySinceAt.Equal(stableSince) {
			t.Fatalf("stable service = %#v", stable.ServiceJob)
		}
	})

	t.Run("node liveness transition wins at the equal two minute boundary", func(t *testing.T) {
		h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Minute}, map[string]NodePolicy{
			"service-node": DefaultNodePolicy("service"),
		})
		client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
		agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
		node := h.register(agent, "service-node")
		job := submitRestartService(t, h, client, "stability-liveness-order", []string{"service"}, nil)
		claim := claimRestartService(t, h, agent, node)
		if _, err := h.store.RenewLease(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, claim.Lease.FencingToken); err != nil {
			t.Fatal(err)
		}
		if _, err := h.store.db.Exec(`UPDATE service_jobs SET bound_node_id=?, restart_streak=4, healthy_since_ns=? WHERE job_id=?`,
			node.NodeID, h.clock.Now().UnixNano(), job.JobID); err != nil {
			t.Fatal(err)
		}

		h.clock.Advance(DefaultNodeDeadAfter)
		result, err := h.store.Reconcile(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if result.DeadNodes != 1 || result.RestartStreakResets != 0 {
			t.Fatalf("dead nodes/restart resets = %d/%d, want 1/0", result.DeadNodes, result.RestartStreakResets)
		}
		service := getRestartService(t, h, job.JobID)
		if service.RestartStreak != 4 {
			t.Fatalf("dead-node stability reset streak = %d, want frozen 4", service.RestartStreak)
		}
		var state contract.NodeState
		if err := h.store.db.QueryRow("SELECT state FROM nodes WHERE node_id=?", node.NodeID).Scan(&state); err != nil {
			t.Fatal(err)
		}
		if state != contract.NodeDead {
			t.Fatalf("node state = %q, want dead", state)
		}
	})

	t.Run("stale time interrupts health without changing the streak", func(t *testing.T) {
		h := newIntegrationHarnessWithOptions(t, StoreOptions{
			LeaseDuration:          5 * time.Minute,
			NodeStaleAfter:         time.Minute,
			NodeDeadAfter:          10 * time.Minute,
			ServiceStabilityWindow: 30 * time.Second,
		}, map[string]NodePolicy{"service-node": DefaultNodePolicy("service")})
		client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
		agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
		node := h.register(agent, "service-node")
		job := submitRestartService(t, h, client, "stability-stale-suppression", []string{"service"}, nil)
		claim := claimRestartService(t, h, agent, node)
		if _, err := h.store.RenewLease(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, claim.Lease.FencingToken); err != nil {
			t.Fatal(err)
		}
		if _, err := h.store.db.Exec("UPDATE service_jobs SET bound_node_id=?, restart_streak=3 WHERE job_id=?", node.NodeID, job.JobID); err != nil {
			t.Fatal(err)
		}

		h.clock.Advance(time.Minute)
		result, err := h.store.Reconcile(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		interrupted := getRestartService(t, h, job.JobID)
		if result.StaleNodes != 1 || result.RestartStreakResets != 0 || interrupted.RestartStreak != 3 || interrupted.HealthySinceAt != nil {
			t.Fatalf("stale suppression result/service = %#v/%#v", result, interrupted.ServiceJob)
		}

		if _, err := h.store.HeartbeatNode(context.Background(), "agent", node.NodeID, node.BootSessionID); err != nil {
			t.Fatal(err)
		}
		if _, err := h.store.RenewLease(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, claim.Lease.FencingToken); err != nil {
			t.Fatal(err)
		}
		recoveredSince := h.clock.Now()
		h.clock.Advance(29 * time.Second)
		if _, err := h.store.HeartbeatNode(context.Background(), "agent", node.NodeID, node.BootSessionID); err != nil {
			t.Fatal(err)
		}
		result, err = h.store.Reconcile(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if result.RestartStreakResets != 0 || getRestartService(t, h, job.JobID).RestartStreak != 3 {
			t.Fatal("suppressed time counted toward the replacement stability window")
		}

		h.clock.Advance(time.Second)
		if _, err := h.store.HeartbeatNode(context.Background(), "agent", node.NodeID, node.BootSessionID); err != nil {
			t.Fatal(err)
		}
		result, err = h.store.Reconcile(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		recovered := getRestartService(t, h, job.JobID)
		if result.RestartStreakResets != 1 || recovered.RestartStreak != 0 || recovered.HealthySinceAt == nil || !recovered.HealthySinceAt.Equal(recoveredSince) {
			t.Fatalf("replacement stability window result/service = %#v/%#v", result, recovered.ServiceJob)
		}
	})
}

func TestPortlessServiceExecutionAcknowledgementStartsStabilityWindow(t *testing.T) {
	assertPortlessServiceExecutionAcknowledgementStartsStabilityWindow(t)
}

func assertPortlessServiceExecutionAcknowledgementStartsStabilityWindow(t *testing.T) {
	t.Helper()
	tests := []struct {
		name      string
		port      *int
		wantSince bool
	}{
		{name: "portless service", wantSince: true},
		{name: "portful service waits for publication evidence", port: func() *int { value := 8080; return &value }()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Minute}, map[string]NodePolicy{
				"service-node": DefaultNodePolicy("service"),
			})
			client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
			agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
			node := h.register(agent, "service-node")
			spec := validJobSpec("stability-start-"+test.name, []string{"service"})
			spec.Class = contract.JobClassService
			spec.Execution.HandoffDirectory = ""
			spec.Restart = contract.RestartAlways
			spec.PublishedPort = test.port
			status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
			if status != http.StatusCreated {
				t.Fatalf("submit status = %d body=%s", status, body)
			}
			var job Job
			if err := json.Unmarshal(body, &job); err != nil {
				t.Fatal(err)
			}
			claim := claimRestartService(t, h, agent, node)
			if _, err := h.store.RenewLease(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, claim.Lease.FencingToken); err != nil {
				t.Fatal(err)
			}
			job = getRestartService(t, h, job.JobID)
			if (job.HealthySinceAt != nil) != test.wantSince {
				t.Fatalf("healthy_since_at presence = %t, want %t", job.HealthySinceAt != nil, test.wantSince)
			}
			if test.wantSince && !job.HealthySinceAt.Equal(h.clock.Now()) {
				t.Fatalf("healthy_since_at = %s, want %s", job.HealthySinceAt, h.clock.Now())
			}
		})
	}
}

func assertServiceRestartableCompletionRequeuesWithPersistedBackoff(t *testing.T) {
	t.Helper()
	h := newIntegrationHarnessWithOptions(t, StoreOptions{
		LeaseDuration: 3 * time.Second,
		Jitter:        func(delay time.Duration) time.Duration { return delay },
	}, map[string]NodePolicy{
		"service-node": DefaultNodePolicy("service"),
	})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "service-agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "service-node")
	job := submitRestartService(t, h, client, "restartable-completion", []string{"service"}, nil)
	first := claimRestartService(t, h, agent, node)
	if _, err := h.store.db.Exec("UPDATE service_jobs SET bound_node_id=? WHERE job_id=?", node.NodeID, job.JobID); err != nil {
		t.Fatal(err)
	}

	exitCode := 17
	completionPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, first.Lease.AttemptID)
	request := CompletionRequest{
		FencingToken:   first.Lease.FencingToken,
		IdempotencyKey: "restartable-completion-1",
		Result:         ProcessResult{ExitCode: &exitCode},
	}
	status, _, body := h.do(agent, http.MethodPost, completionPath, request)
	if status != http.StatusOK {
		t.Fatalf("complete restartable service status = %d body=%s", status, body)
	}

	requeued := getRestartService(t, h, job.JobID)
	if requeued.State != contract.JobQueued || requeued.RestartStreak != 1 || requeued.LifetimeRestartCount != 1 {
		t.Fatalf("requeued service state/streak/lifetime = %q/%d/%d, want queued/1/1", requeued.State, requeued.RestartStreak, requeued.LifetimeRestartCount)
	}
	wantRestart := h.clock.Now().Add(time.Second)
	if requeued.NextRestartAt == nil || !requeued.NextRestartAt.Equal(wantRestart) {
		t.Fatalf("next_restart_at = %v, want %s", requeued.NextRestartAt, wantRestart)
	}
	if !requeued.RestartPending(requeued.State, h.clock.Now()) {
		t.Fatal("restartable completion must project restart-pending before its persisted due time")
	}
	if !json.Valid(requeued.LastFailure) || !bytes.Contains(requeued.LastFailure, []byte(`"exit_code":17`)) {
		t.Fatalf("last_failure = %s, want the accepted process result", requeued.LastFailure)
	}
	assertAttemptState(t, h, first.Lease.AttemptID, contract.AttemptFailed)

	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, Class: contract.JobClassService})
	if status != http.StatusNoContent {
		t.Fatalf("pre-due restart claim status = %d body=%s, want 204", status, body)
	}

	// Completion replay returns the previously committed transition without a
	// second streak increment, jitter draw, or next_restart_at recomputation.
	status, _, body = h.do(agent, http.MethodPost, completionPath, request)
	if status != http.StatusOK {
		t.Fatalf("completion replay status = %d body=%s", status, body)
	}
	replayed := getRestartService(t, h, job.JobID)
	if replayed.RestartStreak != 1 || replayed.LifetimeRestartCount != 1 || replayed.NextRestartAt == nil || !replayed.NextRestartAt.Equal(wantRestart) {
		t.Fatalf("completion replay changed restart policy state: %#v", replayed.ServiceJob)
	}

	h.clock.Advance(time.Second)
	second := claimRestartService(t, h, agent, node)
	if second.Job.JobID != job.JobID {
		t.Fatalf("restart job ID = %q, want stable %q", second.Job.JobID, job.JobID)
	}
	if second.Lease.AttemptID == first.Lease.AttemptID {
		t.Fatalf("restart reused attempt ID %q", second.Lease.AttemptID)
	}
	if second.Lease.FencingToken == first.Lease.FencingToken {
		t.Fatalf("restart reused fencing token %q", second.Lease.FencingToken)
	}
	claimed := getRestartService(t, h, job.JobID)
	if claimed.NextRestartAt != nil {
		t.Fatalf("claimed restart retained next_restart_at = %s", claimed.NextRestartAt)
	}
}

func submitRestartService(t *testing.T, h *integrationHarness, client *http.Client, dispatchKey string, tags []string, maxRestartStreak *int) Job {
	t.Helper()
	spec := validJobSpec(dispatchKey, tags)
	spec.Class = contract.JobClassService
	spec.Execution.HandoffDirectory = ""
	spec.Restart = contract.RestartAlways
	spec.MaxRestartStreak = maxRestartStreak
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusCreated {
		t.Fatalf("submit restart service status = %d body=%s", status, body)
	}
	var job Job
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatal(err)
	}
	return job
}

func claimRestartService(t *testing.T, h *integrationHarness, agent *http.Client, node Node) Claim {
	t.Helper()
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, Class: contract.JobClassService})
	if status != http.StatusOK {
		t.Fatalf("claim restart service status = %d body=%s", status, body)
	}
	var claim Claim
	if err := json.Unmarshal(body, &claim); err != nil {
		t.Fatal(err)
	}
	return claim
}

func getRestartService(t *testing.T, h *integrationHarness, jobID string) Job {
	t.Helper()
	job, err := h.store.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatal(err)
	}
	if job.ServiceJob == nil {
		t.Fatalf("job %q has no service metadata", jobID)
	}
	return job
}

func assertAttemptState(t *testing.T, h *integrationHarness, attemptID string, want contract.AttemptState) {
	t.Helper()
	var got contract.AttemptState
	if err := h.store.db.QueryRow("SELECT state FROM attempts WHERE attempt_id=?", attemptID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("attempt %q state = %q, want %q", attemptID, got, want)
	}
}
