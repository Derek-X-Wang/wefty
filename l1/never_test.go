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

func neverFixture(t *testing.T, kind string, ports ...int) (*integrationHarness, *http.Client, *http.Client, Node, Job, Claim) {
	t.Helper()
	h := newIntegrationHarnessWithOptions(t, StoreOptions{Jitter: func(d time.Duration) time.Duration { return d }}, map[string]NodePolicy{"service-node": DefaultNodePolicy("service")})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.registerWithCapabilities(agent, "service-node", map[string]bool{"kind:" + kind: true})
	spec := capabilityJobSpec("never", kind, contract.JobClassService, "", nil)
	spec.Restart = "never"
	if len(ports) > 0 {
		spec.PublishedPort = &ports[0]
	}
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
			{"agent signal", CompletionRequest{Result: ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}}, contract.JobFailed, false, true},
			{"guardian signal", CompletionRequest{Result: ProcessResult{Signal: "killed", TerminationCause: contract.TerminationCauseGuardian}}, contract.JobFailed, false, true},
			{"agent clean interruption", CompletionRequest{Result: ProcessResult{ExitCode: &zero}, TerminationInitiator: contract.TerminationCauseAgent}, contract.JobFailed, false, true},
			{"guardian clean interruption", CompletionRequest{Result: ProcessResult{ExitCode: &zero}, TerminationInitiator: contract.TerminationCauseGuardian}, contract.JobFailed, false, true},
			{"guardian nonzero interruption", CompletionRequest{Result: ProcessResult{ExitCode: &one}, TerminationInitiator: contract.TerminationCauseGuardian}, contract.JobFailed, false, true},
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
					// Node return cannot start the service; explicit start/restart is required.
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
		got := s.classifyServiceCompletion(job, CompletionRequest{Result: result}, raw, raw, time.Unix(100, 0), true, false)
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
				result := ProcessResult{SpawnError: &contract.SpawnFailure{Code: tc.code, Message: "fixture"}}
				got := neverComplete(t, h, agent, job, claim, CompletionRequest{Result: result})
				if got.State != want || (got.PolicyStop != nil) != tc.policy || got.RestartStreak != 0 || (got.LifetimeRestartCount == 1) != (want == contract.JobQueued) {
					t.Fatalf("prestart completion = %+v service=%+v", got, got.ServiceJob)
				}
				if tc.policy {
					if got.PolicyStop.SpawnError == nil || *got.PolicyStop.SpawnError != *result.SpawnError {
						t.Fatalf("policy stop lost spawn result: %+v", got.PolicyStop)
					}
					if err := validateProcessResult(*got.PolicyStop); err != nil {
						t.Fatalf("invalid policy stop: %v", err)
					}
				}
			})
		}
	}
	for _, latch := range []string{"spawn", "output", "image", "image infrastructure", "removal"} {
		t.Run(latch, func(t *testing.T) {
			h, client, agent, node, job, claim := neverFixture(t, "process")
			neverStarted(t, h, agent, job, claim)
			zero := 0
			result := ProcessResult{ExitCode: &zero}
			if latch == "spawn" {
				result = ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureImageUnavailable, Message: "spawn failed"}}
			}
			if latch == "output" {
				result = ProcessResult{OutputError: "corrupt spool"}
			}
			neverComplete(t, h, agent, job, claim, CompletionRequest{Result: result})
			if strings.HasPrefix(latch, "image") {
				code := contract.SpawnFailureImageUnavailable
				if latch == "image infrastructure" {
					code = contract.SpawnFailurePublishedListener
				}
				_, err := h.store.LatchServiceImageReconciliationFailure(t.Context(), "agent", job.JobID, ServiceImageReconciliationFailureRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, Failure: contract.SpawnFailure{Code: code, Message: "pin missing"}})
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
			if got.State != want || got.PolicyStop != nil || (len(got.LastFailure) > 0) != !restart || got.RestartStreak != 0 || got.LifetimeRestartCount != count || got.LeaseLossCount != 0 {
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

// A listener can fail after publication. Its spawn_error arm does not undo
// durable Started, and must not authorize a new attempt under never.
func TestNeverAcknowledgedPublishedListenerFailure(t *testing.T) {
	h, client, agent, node, job, claim := neverFixture(t, "process", 8080)
	neverStarted(t, h, agent, job, claim)
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/publication", job.JobID, claim.Lease.AttemptID)
	status, _, body := h.do(agent, http.MethodPut, path, PublicationRequest{FencingToken: claim.Lease.FencingToken, Ready: boolPointer(true)})
	if status != http.StatusOK {
		t.Fatalf("publication = %d %s", status, body)
	}
	failure := contract.SpawnFailure{Code: contract.SpawnFailurePublishedListener, Message: "Fabric listener stopped"}
	got := neverComplete(t, h, agent, job, claim, CompletionRequest{Result: ProcessResult{SpawnError: &failure}})
	if got.State != contract.JobFailed || got.PolicyStop != nil || got.NextRestartAt != nil || got.RestartStreak != 0 || got.LifetimeRestartCount != 0 || got.LeaseLossCount != 0 || got.PublishedPort != nil || got.HoldsSlot(got.State) {
		t.Fatalf("acknowledged listener failure requeued: %+v service=%+v", got, got.ServiceJob)
	}
	var last contract.SpawnFailure
	if err := json.Unmarshal(got.LastFailure, &last); err != nil || last != failure {
		t.Fatalf("listener failure evidence = %s, %v", got.LastFailure, err)
	}
	h.clock.Advance(time.Minute)
	if _, err := h.store.HeartbeatNode(t.Context(), "agent", node.NodeID, node.BootSessionID); err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, Class: "service"})
	if status != http.StatusNoContent {
		t.Fatalf("automatic claim = %d %s", status, body)
	}
	projected, err := h.store.projectServiceJob(t.Context(), getRestartService(t, h, job.JobID))
	if err != nil || !strings.Contains(projected.RestartSuppressed, "published_listener") {
		t.Fatalf("suppression = %+v %v", projected.ServiceJob, err)
	}
	status, _, body = h.do(client, http.MethodPut, serviceMutationPath(job.JobID, "desired-state"), ServiceDesiredStateRequest{DesiredState: contract.ServiceDesiredRunning})
	if status != http.StatusAccepted {
		t.Fatalf("explicit start = %d %s", status, body)
	}
}

// A failed start ack cancels the payload without setting started_ns. Even if
// renewal or logs promoted running, its interruption keeps pre-start retry.
func TestNeverUnacknowledgedInterruptionRetries(t *testing.T) {
	zero := 0
	for _, kind := range []string{"process"} {
		for _, phase := range []string{"claimed", "renewed", "logged"} {
			for _, completion := range []CompletionRequest{
				{Result: ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}},
				{Result: ProcessResult{ExitCode: &zero}, TerminationInitiator: contract.TerminationCauseAgent},
			} {
				t.Run(kind+"/"+phase+"/"+fmt.Sprint(completion.Result.ExitCode != nil), func(t *testing.T) {
					h, _, agent, node, job, claim := neverFixture(t, kind)
					if phase == "renewed" {
						if _, err := h.store.RenewLease(t.Context(), "agent", job.JobID, claim.Lease.AttemptID, claim.Lease.FencingToken); err != nil {
							t.Fatal(err)
						}
					}
					if phase == "logged" {
						if _, err := h.store.AppendLogs(t.Context(), "agent", job.JobID, claim.Lease.AttemptID, AppendLogsRequest{FencingToken: claim.Lease.FencingToken, Events: []contract.LogEvent{{AttemptID: claim.Lease.AttemptID, Stream: contract.LogStdout, Timestamp: h.clock.Now(), Bytes: []byte("output")}}}); err != nil {
							t.Fatal(err)
						}
					}
					var started bool
					if err := h.store.db.QueryRow("SELECT started_ns IS NOT NULL FROM attempts WHERE attempt_id=?", claim.Lease.AttemptID).Scan(&started); err != nil || started {
						t.Fatalf("start marker = %t %v", started, err)
					}
					got := neverComplete(t, h, agent, job, claim, completion)
					if got.State != contract.JobQueued || got.PolicyStop != nil || len(got.LastFailure) != 0 || got.NextRestartAt == nil || !got.NextRestartAt.Equal(h.clock.Now().Add(time.Second)) || got.RestartStreak != 0 || got.LifetimeRestartCount != 1 || got.LeaseLossCount != 0 {
						t.Fatalf("unacknowledged interruption = %+v service=%+v", got, got.ServiceJob)
					}
					h.clock.Advance(time.Second)
					next := claimRestartService(t, h, agent, node)
					if next.Lease.AttemptID == claim.Lease.AttemptID {
						t.Fatal("retry reused attempt")
					}
				})
			}
		}
	}
}

func TestNeverStartAfterAutomaticFailure(t *testing.T) {
	zero := 0
	for _, kind := range []string{"process", "oci"} {
		for _, cause := range []string{"lease loss", "agent", "guardian", "requested exit", "runtime"} {
			if kind == "process" && cause == "runtime" {
				continue
			}
			t.Run(kind+"/"+cause, func(t *testing.T) {
				h, client, agent, node, job, claim := neverFixture(t, kind)
				neverStarted(t, h, agent, job, claim)
				// Historical failure is evidence, not a latch on this attempt.
				if _, err := h.store.db.Exec("UPDATE service_jobs SET last_failure=? WHERE job_id=?", []byte(`{"output_error":"earlier failure"}`), job.JobID); err != nil {
					t.Fatal(err)
				}
				if cause == "lease loss" {
					h.clock.Advance(DefaultLeaseDuration + time.Second)
					if _, err := h.store.Reconcile(t.Context()); err != nil {
						t.Fatal(err)
					}
				} else {
					completion := CompletionRequest{Result: ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCause(cause)}}
					if cause == "requested exit" {
						completion = CompletionRequest{Result: ProcessResult{ExitCode: &zero}, TerminationInitiator: contract.TerminationCauseAgent}
					}
					if cause == "runtime" {
						completion = CompletionRequest{Result: ProcessResult{RuntimeFailure: &contract.RuntimeFailure{Code: contract.RuntimeFailureUnavailable, Message: "runtime lost"}}}
					}
					neverComplete(t, h, agent, job, claim, completion)
				}
				got, err := h.store.projectServiceJob(t.Context(), getRestartService(t, h, job.JobID))
				if err != nil || got.State != contract.JobFailed || got.PolicyStop != nil || !strings.Contains(got.RestartSuppressed, "use start or restart") || (cause == "lease loss" && !strings.Contains(got.RestartSuppressed, "lease lost")) {
					t.Errorf("automatic failure cause = %+v %v", got.ServiceJob, err)
				}
				if cause != "lease loss" {
					var last ProcessResult
					if err := json.Unmarshal(got.LastFailure, &last); err != nil || validateProcessResult(last) != nil {
						t.Errorf("interruption evidence = %s %v", got.LastFailure, err)
					}
				}
				if _, err := h.store.HeartbeatNode(t.Context(), "agent", node.NodeID, node.BootSessionID); err != nil {
					t.Fatal(err)
				}
				status, _, body := h.do(client, http.MethodPut, serviceMutationPath(job.JobID, "desired-state"), ServiceDesiredStateRequest{DesiredState: contract.ServiceDesiredRunning})
				if status != http.StatusAccepted {
					t.Fatalf("start after %s = %d %s", cause, status, body)
				}
				resumed := getRestartService(t, h, job.JobID)
				if resumed.State != contract.JobQueued || resumed.PolicyStop != nil || resumed.NextRestartAt != nil || resumed.RestartStreak != got.RestartStreak || resumed.LifetimeRestartCount != got.LifetimeRestartCount || resumed.LeaseLossCount != got.LeaseLossCount {
					t.Fatalf("resume changed accounting = %+v", resumed.ServiceJob)
				}
				if next := claimRestartService(t, h, agent, node); next.Lease.AttemptID == claim.Lease.AttemptID {
					t.Fatal("start reused failed attempt")
				}
			})
		}
	}
}
