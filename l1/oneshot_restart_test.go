package l1

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

type oneshotFixture struct {
	h      *integrationHarness
	node   Node
	job    Job
	client *http.Client
}

func newOneshotFixture(t *testing.T, restartable bool) *oneshotFixture {
	t.Helper()
	return newOneshotFixtureWithSpec(t, restartable, validJobSpec("restart-test", nil))
}
func newOneshotFixtureWithSpec(t *testing.T, restartable bool, spec contract.JobSpec) *oneshotFixture {
	t.Helper()
	h := newIntegrationHarnessWithReconcileInterval(t, StoreOptions{Jitter: func(d time.Duration) time.Duration { return d }}, map[string]NodePolicy{"node": {MaxOneshotSlots: 4, MaxServiceSlots: 2}}, true, time.Hour)
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.registerWithCapabilities(agent, "node", map[string]bool{"kind:process": true, CapabilityOneshotRestartable: true})
	spec.Restartable = restartable
	job, _, err := h.store.CreateJobAs(t.Context(), spec, JobOrigin{OriginatingSubmitter: "operator", SubmittedByRunLedger: true})
	if err != nil {
		t.Fatal(err)
	}
	return &oneshotFixture{h: h, node: node, job: job, client: h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})}
}
func (f *oneshotFixture) claim(t *testing.T) *Claim {
	t.Helper()
	c, err := f.h.store.ClaimJob(t.Context(), "agent", "node", f.node.BootSessionID, contract.JobClassOneShot)
	if err != nil || c == nil {
		t.Fatalf("claim=%v err=%v", c, err)
	}
	return c
}
func (f *oneshotFixture) disable(t *testing.T) {
	t.Helper()
	status, _, body := f.h.do(f.client, http.MethodPost, "/v1/nodes/node/claims", NodeIntentRequest{ClaimsEnabled: false, IntentRevision: f.node.IntentRevision, Reason: "upgrade"})
	if status != 200 {
		t.Fatalf("disable=%d %s", status, body)
	}
	if err := json.Unmarshal(body, &f.node); err != nil {
		t.Fatal(err)
	}
}
func (f *oneshotFixture) enable(t *testing.T) {
	t.Helper()
	status, _, body := f.h.do(f.client, http.MethodPost, "/v1/nodes/node/claims", NodeIntentRequest{ClaimsEnabled: true, IntentRevision: f.node.IntentRevision, Reason: "upgrade finished"})
	if status != 200 {
		t.Fatalf("enable=%d %s", status, body)
	}
	if err := json.Unmarshal(body, &f.node); err != nil {
		t.Fatal(err)
	}
}
func (f *oneshotFixture) request(c *Claim) InterruptRestartableAttemptRequest {
	return InterruptRestartableAttemptRequest{JobID: f.job.JobID, BootSessionID: f.node.BootSessionID, IntentRevision: f.node.IntentRevision, IdempotencyKey: "interrupt-" + c.Lease.AttemptID, Reason: InterruptionUpgrade}
}
func (f *oneshotFixture) interrupt(t *testing.T, c *Claim) CompletionRequest {
	t.Helper()
	req := f.request(c)
	job, replay, err := f.h.store.InterruptRestartableAttempt(t.Context(), "node", c.Lease.AttemptID, req)
	if err != nil || replay || job.JobID != f.job.JobID {
		t.Fatalf("interrupt=%+v replay=%v err=%v", job, replay, err)
	}
	lease, err := f.h.store.RenewLease(t.Context(), "agent", f.job.JobID, c.Lease.AttemptID, c.Lease.FencingToken)
	if err != nil || lease.Directive != AttemptDirectiveRestart || lease.InterruptionReason != InterruptionUpgrade || lease.InterruptionRequestKey != req.IdempotencyKey {
		t.Fatalf("directive=%+v err=%v", lease, err)
	}
	return CompletionRequest{FencingToken: c.Lease.FencingToken, IdempotencyKey: "complete-" + c.Lease.AttemptID, Result: ProcessResult{Signal: "SIGTERM", TerminationCause: contract.TerminationCauseAgent}, InterruptionReason: InterruptionUpgrade, InterruptionRequestKey: req.IdempotencyKey, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt}
}
func (f *oneshotFixture) get(t *testing.T) Job {
	t.Helper()
	job, err := f.h.store.GetJob(t.Context(), f.job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func TestOneshotRestartDefaultAndEligibility(t *testing.T) {
	spec := validJobSpec("default", nil)
	if spec.Restartable || slices.Contains(RequiredCapabilities(spec), CapabilityOneshotRestartable) {
		t.Fatal("ordinary one-shot opted into recovery")
	}
	var decoded contract.JobSpec
	encoded, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil || decoded.Restartable {
		t.Fatalf("default wire=%v err=%v", decoded.Restartable, err)
	}
	f := newOneshotFixture(t, true)
	var capability string
	if err := f.h.store.db.QueryRow("SELECT capability FROM job_required_capabilities WHERE job_id=? AND capability=?", f.job.JobID, CapabilityOneshotRestartable).Scan(&capability); err != nil {
		t.Fatal(err)
	}
	// A process-capable agent without the complete restart implementation cannot claim.
	if _, err := f.h.store.db.Exec(`UPDATE nodes SET capabilities_json='{"kind:process":true}' WHERE node_id='node'`); err != nil {
		t.Fatal(err)
	}
	c, err := f.h.store.ClaimJob(t.Context(), "agent", "node", f.node.BootSessionID, contract.JobClassOneShot)
	if err != nil || c != nil {
		t.Fatalf("unsupported claim=%v err=%v", c, err)
	}
	for _, tc := range []struct{ kind, class string }{{"process", "service"}, {"oci", "one-shot"}, {"oci", "service"}} {
		spec := validJobSpec(tc.kind+tc.class, nil)
		spec.Restartable = true
		spec.Kind = tc.kind
		spec.Class = tc.class
		if _, _, err := f.h.store.CreateJob(t.Context(), spec); errorCode(err) != contract.ErrorInvalidRequest {
			t.Fatalf("%s/%s accepted: %v", tc.kind, tc.class, err)
		}
	}
}

func TestOneshotRestartControlledAndReplay(t *testing.T) {
	f := newOneshotFixture(t, true)
	first := f.claim(t)
	f.disable(t)
	interruptRequest := f.request(first)
	request := f.interrupt(t, first)
	job, err := f.h.store.CompleteAttempt(t.Context(), "agent", f.job.JobID, first.Lease.AttemptID, request)
	if err != nil || job.State != contract.JobQueued || job.RestartRecovery.Count != 0 || !job.RestartRecovery.Pending || !job.RestartRecovery.NextRestartAt.Equal(f.h.clock.Now()) {
		t.Fatalf("recovery=%+v err=%v", job, err)
	}
	if job.CurrentAttemptID != "" || job.SecretsScrubbedAt != nil {
		t.Fatalf("recovery detached/retained=%+v", job)
	}
	blocked, err := f.h.store.ClaimJob(t.Context(), "agent", "node", f.node.BootSessionID, contract.JobClassOneShot)
	if err != nil || blocked != nil {
		t.Fatalf("disabled Node claimed: %v %v", blocked, err)
	}
	f.enable(t)
	second := f.claim(t)
	if second.Job.RestartRecovery.Count != 1 || second.Lease.AttemptID == first.Lease.AttemptID || second.Lease.FencingToken == first.Lease.FencingToken || second.AttemptToken == first.AttemptToken {
		t.Fatalf("fresh successor: first=%+v second=%+v", first.Lease, second.Lease)
	}
	// No predecessor request or credential grants authority to the successor.
	lease, err := f.h.store.RenewLease(t.Context(), "agent", f.job.JobID, second.Lease.AttemptID, second.Lease.FencingToken)
	if err != nil || lease.Directive != "" || lease.InterruptionReason != "" || lease.InterruptionRequestKey != "" {
		t.Fatalf("successor inherited directive: %+v err=%v", lease, err)
	}
	if _, err := f.h.store.ResolveAttemptCredential(t.Context(), first.AttemptToken, "agent"); err == nil {
		t.Fatal("predecessor credential still live")
	}
	if _, err := f.h.store.ResolveAttemptCredential(t.Context(), second.AttemptToken, "agent"); err != nil {
		t.Fatalf("successor credential invalid: %v", err)
	}
	// Accepted completion stays replayable after its successor and boot replacement.
	if _, err := f.h.store.db.Exec("UPDATE nodes SET authority_generation=authority_generation+1 WHERE node_id='node'"); err != nil {
		t.Fatal(err)
	}
	outcome, err := f.h.store.CompleteAttemptOutcome(t.Context(), "agent", f.job.JobID, first.Lease.AttemptID, request)
	if err != nil || !outcome.Replayed || outcome.Job.CurrentAttemptID != second.Lease.AttemptID || outcome.Job.RestartRecovery.Count != 1 {
		t.Fatalf("completion replay=%+v err=%v", outcome, err)
	}
	_, replayed, err := f.h.store.InterruptRestartableAttempt(t.Context(), "node", first.Lease.AttemptID, interruptRequest)
	if err != nil || !replayed {
		t.Fatalf("interruption replay=%v err=%v", replayed, err)
	}
	changed := request
	changed.IdempotencyKey = "different"
	if _, err := f.h.store.CompleteAttempt(t.Context(), "agent", f.job.JobID, first.Lease.AttemptID, changed); errorCode(err) != contract.ErrorIdempotencyConflict {
		t.Fatalf("changed replay=%v", err)
	}
	attempts, err := f.h.store.ListJobAttempts(t.Context(), f.job.JobID)
	if err != nil || len(attempts) != 2 || attempts[0].State != contract.AttemptInterrupted || attempts[0].InterruptionReason != InterruptionUpgrade {
		t.Fatalf("attempts=%+v err=%v", attempts, err)
	}
}

func TestOneshotRestartNaturalExitAndCancellation(t *testing.T) {
	for _, scenario := range []string{"exit-zero", "exit-one", "limit", "cancel", "no-evidence", "wrong-request", "unacknowledged", "preparation-failure", "incomplete-interruption", "interruption-exit-zero", "shutdown"} {
		t.Run(scenario, func(t *testing.T) {
			f := newOneshotFixture(t, true)
			c := f.claim(t)
			f.disable(t)
			request := f.interrupt(t, c)
			want := contract.JobFailed
			switch scenario {
			case "exit-zero", "exit-one":
				value := 0
				if scenario == "exit-one" {
					value = 1
				}
				request.Result = ProcessResult{ExitCode: &value}
				if value == 0 {
					want = contract.JobSucceeded
				}
			case "limit":
				request.Result.LimitReached = contract.LimitReachedMaxRuntime
			case "cancel":
				if _, err := f.h.store.CancelJob(t.Context(), f.job.JobID, JobCancelCaller{Submitter: "operator"}); err != nil {
					t.Fatal(err)
				}
				lease, err := f.h.store.RenewLease(t.Context(), "agent", f.job.JobID, c.Lease.AttemptID, c.Lease.FencingToken)
				if err != nil || lease.Directive != AttemptDirectiveCancel || lease.InterruptionReason != "" {
					t.Fatalf("cancel directive=%+v err=%v", lease, err)
				}
			case "no-evidence":
				request.RuntimeQuiescenceEvidence = ""
			case "wrong-request":
				request.InterruptionRequestKey = "different"
			case "unacknowledged":
				request.InterruptionReason = ""
				request.InterruptionRequestKey = ""
			case "preparation-failure":
				request.Result = ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureRuntimeUnavailable, Message: "preparation failed"}}
			case "interruption-exit-zero":
				value := 0
				request.Result = ProcessResult{ExitCode: &value}
				request.TerminationInitiator = contract.TerminationCauseAgent
				want = contract.JobQueued
			case "incomplete-interruption":
				request.Result.LogEvidenceIncomplete = true
				want = contract.JobQueued
			case "shutdown":
				request.InterruptionReason = InterruptionAgentShutdown
				request.InterruptionRequestKey = ""
				want = contract.JobQueued
			}
			job, err := f.h.store.CompleteAttempt(t.Context(), "agent", f.job.JobID, c.Lease.AttemptID, request)
			if scenario == "no-evidence" || scenario == "wrong-request" {
				if err == nil {
					t.Fatal("accepted missing/incorrect proof")
				}
				return
			}
			if err != nil || job.State != want {
				t.Fatalf("completion state=%s want=%s err=%v", job.State, want, err)
			}
			if want != contract.JobQueued && job.RestartRecovery.Pending {
				t.Fatal("terminal Job pending restart")
			}
		})
	}
}

func TestOneshotRestartLeaseExpiryBoundary(t *testing.T) {
	for _, restartable := range []bool{false, true} {
		t.Run(fmt.Sprint(restartable), func(t *testing.T) {
			f := newOneshotFixture(t, restartable)
			c := f.claim(t)
			f.h.clock.Advance(DefaultLeaseDuration - time.Nanosecond)
			if _, err := f.h.store.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			if f.get(t).State != contract.JobClaimed {
				t.Fatal("expired before boundary")
			}
			f.h.clock.Advance(time.Nanosecond)
			if _, err := f.h.store.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			job := f.get(t)
			want := contract.JobFailed
			if restartable {
				want = contract.JobQueued
			}
			if job.State != want {
				t.Fatalf("expired state=%s want=%s", job.State, want)
			}
			attempts, err := f.h.store.ListJobAttempts(t.Context(), f.job.JobID)
			if err != nil || attempts[0].State != contract.AttemptLost {
				t.Fatalf("attempts=%+v err=%v", attempts, err)
			}
			if !restartable {
				if job.SecretsScrubbedAt == nil {
					t.Fatal("ordinary failed Job not scrubbed")
				}
				return
			}
			if job.RestartRecovery.Count != 0 || (job.RestartRecovery.LastReason == nil || *job.RestartRecovery.LastReason != InterruptionLeaseExpired) || !job.RestartRecovery.NextRestartAt.Equal(f.h.clock.Now().Add(time.Second)) {
				t.Fatalf("backoff=%+v", job.RestartRecovery)
			}
			before, err := f.h.store.ClaimJob(t.Context(), "agent", "node", f.node.BootSessionID, contract.JobClassOneShot)
			if err != nil || before != nil {
				t.Fatalf("claim before due=%v %v", before, err)
			}
			f.h.clock.Advance(time.Second)
			next := f.claim(t)
			if next.Job.RestartRecovery.Count != 1 || next.Lease.FencingToken == c.Lease.FencingToken || next.AttemptToken == c.AttemptToken {
				t.Fatal("successor authority/counter not renewed")
			}
		})
	}
}

func TestOneshotRestartLimitAndPersistedBackoff(t *testing.T) {
	for _, reason := range []InterruptionReason{InterruptionAgentShutdown, InterruptionLeaseExpired} {
		t.Run(string(reason), func(t *testing.T) {
			f := newOneshotFixture(t, true)
			for n := 0; n <= MaximumOneshotRestarts; n++ {
				// Keep Node heartbeat live independently of the Attempt Lease.
				if _, err := f.h.store.db.Exec("UPDATE nodes SET last_heartbeat_ns=?,state='alive' WHERE node_id='node'", f.h.clock.Now().UnixNano()); err != nil {
					t.Fatal(err)
				}
				c := f.claim(t)
				if c.Job.RestartRecovery.Count != n {
					t.Fatalf("count=%d want=%d", c.Job.RestartRecovery.Count, n)
				}
				if reason == InterruptionLeaseExpired {
					f.h.clock.Advance(DefaultLeaseDuration)
					if _, err := f.h.store.Reconcile(t.Context()); err != nil {
						t.Fatal(err)
					}
				} else {
					req := CompletionRequest{FencingToken: c.Lease.FencingToken, IdempotencyKey: "shutdown-" + c.Lease.AttemptID, Result: ProcessResult{Signal: "SIGTERM", TerminationCause: contract.TerminationCauseAgent}, InterruptionReason: reason, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt}
					if _, err := f.h.store.CompleteAttempt(t.Context(), "agent", f.job.JobID, c.Lease.AttemptID, req); err != nil {
						t.Fatal(err)
					}
				}
				job := f.get(t)
				if n == MaximumOneshotRestarts {
					if job.State != contract.JobFailed || (job.RestartRecovery.TerminalReason == nil || *job.RestartRecovery.TerminalReason != RestartLimitReached) || job.FailureReason != "restart limit reached (10 restarts)" || job.SecretsScrubbedAt == nil {
						t.Fatalf("limit=%+v", job)
					}
					break
				}
				if job.State != contract.JobQueued || job.RestartRecovery.Count != n || job.SecretsScrubbedAt != nil {
					t.Fatalf("pending=%+v", job)
				}
				delay := time.Duration(0)
				if reason == InterruptionLeaseExpired {
					delay = oneshotRestartDelay(n+1, func(d time.Duration) time.Duration { return d })
				}
				want := f.h.clock.Now().Add(delay)
				if !job.RestartRecovery.NextRestartAt.Equal(want) {
					t.Fatalf("due=%s want=%s", job.RestartRecovery.NextRestartAt, want)
				}
				// Reconciliation and observation cannot redraw or advance persisted jitter.
				f.h.store.restartJitter = func(time.Duration) time.Duration { return time.Minute }
				if _, err := f.h.store.Reconcile(t.Context()); err != nil {
					t.Fatal(err)
				}
				if !f.get(t).RestartRecovery.NextRestartAt.Equal(want) {
					t.Fatal("backoff redrawn")
				}
				f.h.store.restartJitter = func(d time.Duration) time.Duration { return d }
				f.h.clock.Advance(delay)
			}
		})
	}
	for _, test := range []struct {
		restart int
		want    time.Duration
	}{{1, time.Second}, {2, 2 * time.Second}, {6, 32 * time.Second}, {7, time.Minute}, {10, time.Minute}} {
		if got := oneshotRestartDelay(test.restart, func(d time.Duration) time.Duration { return d }); got != test.want {
			t.Fatalf("delay(%d)=%s", test.restart, got)
		}
	}
	if oneshotRestartDelay(2, func(time.Duration) time.Duration { return 3 * time.Second }) != 3*time.Second {
		t.Fatal("jitter not injected")
	}
}

func TestOneshotRestartEndpointExactRequest(t *testing.T) {
	f := newOneshotFixture(t, true)
	c := f.claim(t)
	path := "/v1/nodes/node/restartable-attempts/" + c.Lease.AttemptID + "/interrupt"
	req := f.request(c)
	status, _, _ := f.h.do(f.client, http.MethodPost, path, req)
	if status != 409 {
		t.Fatalf("enabled admission=%d", status)
	}
	f.disable(t)
	req = f.request(c)
	stale := req
	stale.IntentRevision--
	status, _, _ = f.h.do(f.client, http.MethodPost, path, stale)
	if status != 409 {
		t.Fatalf("stale intent=%d", status)
	}
	other := req
	other.BootSessionID = "old-boot"
	status, _, _ = f.h.do(f.client, http.MethodPost, path, other)
	if status != 409 {
		t.Fatalf("stale boot=%d", status)
	}
	status, _, body := f.h.do(f.client, http.MethodPost, path, req)
	if status != 200 {
		t.Fatalf("request=%d %s", status, body)
	}
	status, headers, body := f.h.do(f.client, http.MethodPost, path, req)
	if status != 200 || headers.Get("Idempotent-Replay") != "true" {
		t.Fatalf("replay=%d %s", status, body)
	}
	req.IdempotencyKey = "different"
	status, _, _ = f.h.do(f.client, http.MethodPost, path, req)
	if status != 409 {
		t.Fatalf("conflict=%d", status)
	}
}

func TestOneshotRestartInlineExpiryAndCancelPrecedence(t *testing.T) {
	for _, mode := range []string{"renewal", "completion", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			f := newOneshotFixture(t, true)
			c := f.claim(t)
			if mode == "canceled" {
				if _, err := f.h.store.CancelJob(t.Context(), f.job.JobID, JobCancelCaller{Submitter: "operator"}); err != nil {
					t.Fatal(err)
				}
			}
			f.h.clock.Advance(DefaultLeaseDuration)
			var err error
			if mode == "completion" {
				exit := 0
				_, err = f.h.store.CompleteAttempt(t.Context(), "agent", f.job.JobID, c.Lease.AttemptID, CompletionRequest{FencingToken: c.Lease.FencingToken, IdempotencyKey: "late-natural-exit", Result: ProcessResult{ExitCode: &exit}})
			} else {
				_, err = f.h.store.RenewLease(t.Context(), "agent", f.job.JobID, c.Lease.AttemptID, c.Lease.FencingToken)
			}
			if errorCode(err) != contract.ErrorLeaseExpired {
				t.Fatalf("inline expiry=%v", err)
			}
			got := f.get(t)
			want := contract.JobQueued
			if mode == "canceled" {
				want = contract.JobFailed
			}
			if got.State != want || got.RestartRecovery.Pending != (mode != "canceled") {
				t.Fatalf("inline expiry Job=%+v", got)
			}
			attempts, err := f.h.store.ListJobAttempts(t.Context(), f.job.JobID)
			if err != nil || len(attempts) != 1 || attempts[0].State != contract.AttemptLost {
				t.Fatalf("inline expiry Attempts=%+v err=%v", attempts, err)
			}
			if mode == "completion" && attempts[0].LateResult == nil {
				t.Fatal("late completion lost evidence")
			}
		})
	}
}

func TestOneshotRestartPendingCancellationStaysTerminal(t *testing.T) {
	f := newOneshotFixture(t, true)
	c := f.claim(t)
	f.disable(t)
	request := f.interrupt(t, c)
	if _, err := f.h.store.CompleteAttempt(t.Context(), "agent", f.job.JobID, c.Lease.AttemptID, request); err != nil {
		t.Fatal(err)
	}
	job, err := f.h.store.CancelJob(t.Context(), f.job.JobID, JobCancelCaller{Submitter: "operator"})
	if err != nil || job.State != contract.JobFailed || job.Outcome != contract.JobOutcomeCanceled || job.RestartRecovery.Pending || job.RestartRecovery.NextRestartAt != nil || job.RestartRecovery.Count != 0 {
		t.Fatalf("pending cancellation=%+v err=%v", job, err)
	}
	replay, err := f.h.store.CompleteAttemptOutcome(t.Context(), "agent", f.job.JobID, c.Lease.AttemptID, request)
	if err != nil || !replay.Replayed || replay.Job.State != contract.JobFailed {
		t.Fatalf("canceled replay=%+v err=%v", replay, err)
	}
	f.enable(t)
	next, err := f.h.store.ClaimJob(t.Context(), "agent", "node", f.node.BootSessionID, contract.JobClassOneShot)
	if err != nil || next != nil {
		t.Fatalf("canceled successor claimed=%v err=%v", next, err)
	}
}
