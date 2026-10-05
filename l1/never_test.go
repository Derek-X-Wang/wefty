package l1

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func neverFixture(t *testing.T, kind string) (*integrationHarness, *http.Client, *http.Client, Node, Job, Claim) {
	t.Helper()
	h := newIntegrationHarnessWithOptions(t, StoreOptions{Jitter: func(d time.Duration) time.Duration { return d }}, map[string]NodePolicy{"service-node": DefaultNodePolicy("service")})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.registerWithCapabilities(agent, "service-node", map[string]bool{"kind:" + kind: true})
	spec := capabilityJobSpec("never", kind, contract.JobClassService, "", nil)
	spec.Restart = "never"
	spec.RoutingTags = []string{"service"}
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusCreated {
		t.Fatalf("submit never = %d %s", status, body)
	}
	var job Job
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatal(err)
	}
	claim := claimRestartService(t, h, agent, node)
	return h, client, agent, node, job, claim
}

func neverStarted(t *testing.T, h *integrationHarness, agent *http.Client, job Job, claim Claim) {
	t.Helper()
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s", job.JobID, claim.Lease.AttemptID)
	if job.Spec.Kind == contract.JobKindOCI {
		status, _, body := h.do(agent, http.MethodPut, path+"/image", testImageObservation(claim.Lease.FencingToken))
		if status != http.StatusOK {
			t.Fatalf("image = %d %s", status, body)
		}
	}
	status, _, body := h.do(agent, http.MethodPost, path+"/started", StartedRequest{FencingToken: claim.Lease.FencingToken})
	if status != http.StatusOK {
		t.Fatalf("Started = %d %s", status, body)
	}
}

func neverComplete(t *testing.T, h *integrationHarness, agent *http.Client, job Job, claim Claim, completion CompletionRequest) Job {
	t.Helper()
	completion.FencingToken = claim.Lease.FencingToken
	completion.IdempotencyKey = "complete"
	completion.RuntimeQuiescenceEvidence = RuntimeQuiescenceAttempt
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, claim.Lease.AttemptID)
	status, _, body := h.do(agent, http.MethodPost, path, completion)
	if status != http.StatusOK {
		t.Fatalf("completion = %d %s", status, body)
	}
	var completed Job
	if err := json.Unmarshal(body, &completed); err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(agent, http.MethodPost, path, completion)
	if status != http.StatusOK {
		t.Fatalf("completion replay = %d %s", status, body)
	}
	return completed
}

func TestNeverCompletionMatrix(t *testing.T) {
	zero, one := 0, 1
	for _, kind := range []string{"process", "oci"} {
		for _, tc := range []struct {
			name            string
			completion      CompletionRequest
			want            contract.JobState
			policy, failure bool
		}{
			{"clean", CompletionRequest{Result: ProcessResult{ExitCode: &zero}}, contract.JobStopped, true, false},
			{"clean incomplete logs", CompletionRequest{Result: ProcessResult{ExitCode: &zero, LogEvidenceIncomplete: true}}, contract.JobStopped, true, false},
			{"crash", CompletionRequest{Result: ProcessResult{ExitCode: &one}}, contract.JobFailed, true, true},
			{"spontaneous signal", CompletionRequest{Result: ProcessResult{Signal: "killed", TerminationCause: contract.TerminationCauseSpontaneous}}, contract.JobFailed, true, true},
			{"agent signal", CompletionRequest{Result: ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}}, contract.JobFailed, false, false},
			{"guardian signal", CompletionRequest{Result: ProcessResult{Signal: "killed", TerminationCause: contract.TerminationCauseGuardian}}, contract.JobFailed, false, false},
			{"agent clean interruption", CompletionRequest{Result: ProcessResult{ExitCode: &zero}, TerminationInitiator: contract.TerminationCauseAgent}, contract.JobFailed, false, false},
			{"guardian clean interruption", CompletionRequest{Result: ProcessResult{ExitCode: &zero}, TerminationInitiator: contract.TerminationCauseGuardian}, contract.JobFailed, false, false},
			{"guardian nonzero interruption", CompletionRequest{Result: ProcessResult{ExitCode: &one}, TerminationInitiator: contract.TerminationCauseGuardian}, contract.JobFailed, false, false},
			{"output latch", CompletionRequest{Result: ProcessResult{OutputError: "spool failed"}}, contract.JobFailed, false, true},
		} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				h, _, agent, node, job, claim := neverFixture(t, kind)
				neverStarted(t, h, agent, job, claim)
				if _, err := h.store.db.Exec("UPDATE service_jobs SET restart_streak=2, lifetime_restart_count=5, lease_loss_count=3 WHERE job_id=?", job.JobID); err != nil {
					t.Fatal(err)
				}
				got := neverComplete(t, h, agent, job, claim, tc.completion)
				if got.State != tc.want || (got.PolicyStop != nil) != tc.policy || (len(got.LastFailure) > 0) != tc.failure || got.NextRestartAt != nil || got.RestartStreak != 2 || got.LifetimeRestartCount != 5 || got.LeaseLossCount != 3 || got.DesiredState != contract.ServiceDesiredRunning || got.HoldsSlot(got.State) {
					t.Fatalf("completion = %+v service=%+v", got, got.ServiceJob)
				}
				projected, err := h.store.projectServiceJob(t.Context(), got)
				if err != nil {
					t.Fatal(err)
				}
				if tc.policy && !strings.Contains(projected.RestartSuppressed, "policy stop:") {
					t.Fatal("policy stop missing from service status")
				}
				h.clock.Advance(time.Minute)
				if _, err := h.store.HeartbeatNode(t.Context(), "agent", node.NodeID, node.BootSessionID); err != nil {
					t.Fatal(err)
				}
				status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, Class: "service"})
				if status != http.StatusNoContent {
					t.Fatalf("automatically claimed = %d %s", status, body)
				}
			})
		}
	}
}

func TestNeverExplicitResume(t *testing.T) {
	for _, kind := range []string{"process", "oci"} {
		for _, code := range []int{0, 1} {
			for _, action := range []string{"start", "restart"} {
				t.Run(fmt.Sprintf("%s/%d/%s", kind, code, action), func(t *testing.T) {
					h, client, agent, _, job, claim := neverFixture(t, kind)
					neverStarted(t, h, agent, job, claim)
					neverComplete(t, h, agent, job, claim, CompletionRequest{Result: ProcessResult{ExitCode: &code}})
					var status int
					var body []byte
					if action == "start" {
						status, _, body = h.do(client, http.MethodPut, serviceMutationPath(job.JobID, "desired-state"), ServiceDesiredStateRequest{DesiredState: contract.ServiceDesiredRunning})
					} else {
						status, _, body = h.do(client, http.MethodPost, serviceMutationPath(job.JobID, "restart"), ServiceRestartRequest{IdempotencyKey: "resume"})
					}
					if status != http.StatusAccepted {
						t.Fatalf("resume = %d %s", status, body)
					}
					got := getRestartService(t, h, job.JobID)
					if got.State != contract.JobQueued || got.PolicyStop != nil || got.NextRestartAt != nil {
						t.Fatalf("resume = %+v", got)
					}
					// A replay of the old completion must not reinstate suppression.
					neverComplete(t, h, agent, job, claim, CompletionRequest{Result: ProcessResult{ExitCode: &code}})
					if got := getRestartService(t, h, job.JobID); got.State != contract.JobQueued || got.PolicyStop != nil {
						t.Fatalf("replay reapplied policy: %+v", got)
					}
				})
			}
		}
	}
}

func TestNeverLeaseExpiryMatrix(t *testing.T) {
	for _, kind := range []string{"process", "oci"} {
		for _, phase := range []string{"claimed", "renewed", "logged", "started", "started renewed", "started restart", "started stop"} {
			t.Run(kind+"/"+phase, func(t *testing.T) {
				h, _, agent, node, job, claim := neverFixture(t, kind)
				started := strings.HasPrefix(phase, "started")
				if strings.Contains(phase, "renewed") {
					if _, err := h.store.RenewLease(t.Context(), "agent", job.JobID, claim.Lease.AttemptID, claim.Lease.FencingToken); err != nil {
						t.Fatal(err)
					}
				}
				if phase == "logged" {
					_, err := h.store.AppendLogs(t.Context(), "agent", job.JobID, claim.Lease.AttemptID, AppendLogsRequest{FencingToken: claim.Lease.FencingToken, Events: []contract.LogEvent{{AttemptID: claim.Lease.AttemptID, Stream: contract.LogStdout, Sequence: 0, Timestamp: h.clock.Now(), Bytes: []byte("output")}}})
					if err != nil {
						t.Fatal(err)
					}
				}
				if started {
					neverStarted(t, h, agent, job, claim)
					if strings.Contains(phase, "renewed") {
						if _, err := h.store.RenewLease(t.Context(), "agent", job.JobID, claim.Lease.AttemptID, claim.Lease.FencingToken); err != nil {
							t.Fatal(err)
						}
					}
				}
				if strings.Contains(phase, "restart") {
					if _, _, err := h.store.RestartService(t.Context(), job.JobID, ServiceRestartRequest{IdempotencyKey: "restart"}); err != nil {
						t.Fatal(err)
					}
				}
				if strings.Contains(phase, "stop") {
					if _, err := h.store.SetServiceDesiredState(t.Context(), job.JobID, contract.ServiceDesiredStopped); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := h.store.db.Exec("UPDATE service_jobs SET restart_streak=2,lifetime_restart_count=5 WHERE job_id=?", job.JobID); err != nil {
					t.Fatal(err)
				}
				h.clock.Advance(DefaultLeaseDuration + time.Second)
				if _, err := h.store.Reconcile(t.Context()); err != nil {
					t.Fatal(err)
				}
				if _, err := h.store.Reconcile(t.Context()); err != nil {
					t.Fatal(err)
				}
				got := getRestartService(t, h, job.JobID)
				want := contract.JobQueued
				if started && !strings.Contains(phase, "restart") {
					want = contract.JobFailed
				}
				if got.State != want || got.PolicyStop != nil || got.LeaseLossCount != 1 || got.RestartStreak != 2 || got.LifetimeRestartCount != 5 || len(got.LastFailure) != 0 || (got.NextRestartAt != nil) != (want == contract.JobQueued) {
					t.Fatalf("expiry = %+v service=%+v", got, got.ServiceJob)
				}
				attempts, err := h.store.ListJobAttempts(t.Context(), job.JobID)
				if err != nil || len(attempts) != 1 || attempts[0].State != contract.AttemptLost || attempts[0].Result != nil {
					t.Fatalf("lost attempt = %+v %v", attempts, err)
				}
				if want == contract.JobFailed {
					// Node return cannot start the service; only a new explicit restart can.
					if _, err := h.store.HeartbeatNode(t.Context(), "agent", node.NodeID, node.BootSessionID); err != nil {
						t.Fatal(err)
					}
					status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, Class: "service"})
					if status != http.StatusNoContent {
						t.Fatalf("lost service reclaimed = %d %s", status, body)
					}
					if !strings.Contains(phase, "stop") {
						if _, _, err := h.store.RestartService(t.Context(), job.JobID, ServiceRestartRequest{IdempotencyKey: "resume-after-loss"}); err != nil {
							t.Fatal(err)
						}
						if got := getRestartService(t, h, job.JobID); got.State != contract.JobQueued {
							t.Fatalf("restart lost service = %+v", got)
						}
					}
				}
			})
		}
	}
}

func TestNeverClassifierWithoutValidation(t *testing.T) {
	s := &Store{leaseDuration: 30 * time.Second, restartJitter: func(d time.Duration) time.Duration { return d }}
	zero, one := 0, 1
	for _, code := range []int{zero, one} {
		job := Job{State: contract.JobRunning, Spec: contract.JobSpec{Restart: "never"}, ServiceJob: &ServiceJob{DesiredState: contract.ServiceDesiredRunning}}
		result := ProcessResult{ExitCode: &code}
		raw, _ := json.Marshal(result)
		got := s.classifyServiceCompletion(job, CompletionRequest{Result: result}, raw, time.Unix(100, 0), false)
		want := contract.JobFailed
		if code == 0 {
			want = contract.JobStopped
		}
		if got.jobState != want || len(got.policyStop) == 0 || got.nextRestartNS != nil || got.restartStreak != 0 || got.lifetimeRestartCount != 0 {
			t.Fatalf("never exit %d = %+v", code, got)
		}
	}
}

func TestNeverExplicitRestartCompletionMatrix(t *testing.T) {
	zero, one := 0, 1
	for _, kind := range []string{"process", "oci"} {
		for _, tc := range []struct {
			name           string
			completion     CompletionRequest
			infrastructure bool
		}{
			{"clean", CompletionRequest{Result: ProcessResult{ExitCode: &zero}}, false},
			{"crash", CompletionRequest{Result: ProcessResult{ExitCode: &one}}, false},
			{"spontaneous", CompletionRequest{Result: ProcessResult{Signal: "killed", TerminationCause: contract.TerminationCauseSpontaneous}}, false},
			{"agent", CompletionRequest{Result: ProcessResult{ExitCode: &zero}, TerminationInitiator: contract.TerminationCauseAgent}, true},
			{"guardian", CompletionRequest{Result: ProcessResult{Signal: "killed", TerminationCause: contract.TerminationCauseGuardian}}, true},
		} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				h, _, agent, node, job, claim := neverFixture(t, kind)
				neverStarted(t, h, agent, job, claim)
				if _, _, err := h.store.RestartService(t.Context(), job.JobID, ServiceRestartRequest{IdempotencyKey: "restart"}); err != nil {
					t.Fatal(err)
				}
				got := neverComplete(t, h, agent, job, claim, tc.completion)
				streak := 1
				if tc.infrastructure {
					streak = 0
				}
				if got.State != contract.JobQueued || got.PolicyStop != nil || got.NextRestartAt == nil || got.RestartStreak != streak || got.LifetimeRestartCount != 1 || got.LeaseLossCount != 0 {
					t.Fatalf("restart completion = %+v service=%+v", got, got.ServiceJob)
				}
				h.clock.Advance(time.Second)
				next := claimRestartService(t, h, agent, node)
				neverStarted(t, h, agent, job, next)
				stopped := neverComplete(t, h, agent, job, next, CompletionRequest{Result: ProcessResult{ExitCode: &zero}})
				if stopped.State != contract.JobStopped || stopped.PolicyStop == nil {
					t.Fatalf("restart leaked to new attempt = %+v", stopped)
				}
			})
		}
	}
}

func TestNeverPrestartAndLatchPrecedence(t *testing.T) {
	for _, kind := range []string{"process", "oci"} {
		for _, tc := range []struct {
			name   string
			code   contract.SpawnFailureCode
			want   contract.JobState
			policy bool
		}{
			{"terminal spawn", contract.SpawnFailureImageUnavailable, contract.JobFailed, false},
			{"listener infrastructure", contract.SpawnFailurePublishedListener, contract.JobQueued, false},
			{"readiness failure", contract.SpawnFailureStartupReadinessTimeout, contract.JobFailed, true},
			{"runtime infrastructure", contract.SpawnFailureRuntimeUnavailable, contract.JobQueued, false},
		} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				h, _, agent, _, job, claim := neverFixture(t, kind)
				want := tc.want
				if kind == "process" && tc.code == contract.SpawnFailureRuntimeUnavailable {
					want = contract.JobFailed
				}
				got := neverComplete(t, h, agent, job, claim, CompletionRequest{Result: ProcessResult{SpawnError: &contract.SpawnFailure{Code: tc.code, Message: "fixture"}}})
				if got.State != want || (got.PolicyStop != nil) != tc.policy || got.RestartStreak != 0 || (got.LifetimeRestartCount == 1) != (want == contract.JobQueued) {
					t.Fatalf("prestart completion = %+v service=%+v", got, got.ServiceJob)
				}
			})
		}
	}
	for _, latch := range []string{"output", "image", "removal"} {
		t.Run(latch, func(t *testing.T) {
			h, client, agent, node, job, claim := neverFixture(t, "process")
			neverStarted(t, h, agent, job, claim)
			zero := 0
			result := ProcessResult{ExitCode: &zero}
			if latch == "output" {
				result = ProcessResult{OutputError: "corrupt spool"}
			}
			neverComplete(t, h, agent, job, claim, CompletionRequest{Result: result})
			if latch == "image" {
				_, err := h.store.LatchServiceImageReconciliationFailure(t.Context(), "agent", job.JobID, ServiceImageReconciliationFailureRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, Failure: contract.SpawnFailure{Code: contract.SpawnFailureImageUnavailable, Message: "pin missing"}})
				if err != nil {
					t.Fatal(err)
				}
			}
			if latch == "removal" {
				status, _, body := h.do(client, http.MethodPost, serviceMutationPath(job.JobID, "remove"), nil)
				if status != http.StatusAccepted {
					t.Fatalf("remove = %d %s", status, body)
				}
			}
			status, _, body := h.do(client, http.MethodPut, serviceMutationPath(job.JobID, "desired-state"), ServiceDesiredStateRequest{DesiredState: contract.ServiceDesiredRunning})
			if status != http.StatusConflict && !(latch == "removal" && status == http.StatusNotFound) {
				t.Fatalf("start bypassed %s latch = %d %s", latch, status, body)
			}
		})
	}
}

func TestNeverRuntimeLossAndRestartLatch(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprint(restart), func(t *testing.T) {
			h, _, agent, _, job, claim := neverFixture(t, "oci")
			neverStarted(t, h, agent, job, claim)
			if restart {
				if _, _, err := h.store.RestartService(t.Context(), job.JobID, ServiceRestartRequest{IdempotencyKey: "restart"}); err != nil {
					t.Fatal(err)
				}
			}
			got := neverComplete(t, h, agent, job, claim, CompletionRequest{Result: ProcessResult{RuntimeFailure: &contract.RuntimeFailure{Code: contract.RuntimeFailureUnavailable, Message: "engine lost"}}})
			want := contract.JobFailed
			count := 0
			if restart {
				want = contract.JobQueued
				count = 1
			}
			if got.State != want || got.PolicyStop != nil || len(got.LastFailure) != 0 || got.RestartStreak != 0 || got.LifetimeRestartCount != count || got.LeaseLossCount != 0 {
				t.Fatalf("runtime loss = %+v service=%+v", got, got.ServiceJob)
			}
		})
	}
	for _, kind := range []string{"process", "oci"} {
		t.Run(kind+"/restart output latch", func(t *testing.T) {
			h, _, agent, _, job, claim := neverFixture(t, kind)
			neverStarted(t, h, agent, job, claim)
			if _, _, err := h.store.RestartService(t.Context(), job.JobID, ServiceRestartRequest{IdempotencyKey: "restart"}); err != nil {
				t.Fatal(err)
			}
			got := neverComplete(t, h, agent, job, claim, CompletionRequest{Result: ProcessResult{OutputError: "spool corrupt"}})
			if got.State != contract.JobFailed || got.PolicyStop != nil || got.NextRestartAt != nil || got.RestartStreak != 0 || got.LifetimeRestartCount != 0 {
				t.Fatalf("restart bypassed output latch = %+v", got)
			}
		})
	}
}

func TestNeverStartAcknowledgementSurvivesReopen(t *testing.T) {
	for _, kind := range []string{"process", "oci"} {
		t.Run(kind, func(t *testing.T) {
			h, _, agent, _, job, claim := neverFixture(t, kind)
			neverStarted(t, h, agent, job, claim)
			var seq int
			var name, path string
			if err := h.store.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenStore(path, StoreOptions{Clock: h.clock})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			h.clock.Advance(DefaultLeaseDuration + time.Second)
			if _, err := reopened.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			got, err := reopened.GetJob(t.Context(), job.JobID)
			if err != nil || got.State != contract.JobFailed || got.LeaseLossCount != 1 || got.NextRestartAt != nil {
				t.Fatalf("reopened loss = %+v %v", got, err)
			}
		})
	}
}
