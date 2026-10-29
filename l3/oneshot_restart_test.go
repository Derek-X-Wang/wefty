package l3

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestRestartableExecPolicyValidation(t *testing.T) {
	request := execCaptureRunRequest(strings.Repeat("a", 64), 1)
	setPolicy := func(r CreateRunRequest, value bool) CreateRunRequest {
		var params map[string]any
		_ = json.Unmarshal(r.Params, &params)
		params["restartable"] = value
		r.Params, _ = json.Marshal(params)
		return r
	}
	valid := setPolicy(request, true)
	valid.Restartable = true
	for _, tt := range []struct {
		name    string
		request CreateRunRequest
		allowed bool
	}{
		{"default", request, true}, {"captured restart", valid, true},
		{"typed mismatch", func() CreateRunRequest { r := request; r.Restartable = true; return r }(), false},
		{"params mismatch", setPolicy(request, true), false},
		{"dispatch authority", func() CreateRunRequest { r := valid; r.DispatchAuthority = true; return r }(), false},
		{"version one", func() CreateRunRequest {
			r := valid
			r.Params = json.RawMessage(`{"exec_version":1,"restartable":true}`)
			return r
		}(), false},
		{"workflow", func() CreateRunRequest { r := valid; r.InlineScript = nil; r.WorkflowRef = "test"; return r }(), false},
		{"OCI", func() CreateRunRequest { r := valid; r.Image = &contract.ImageProgram{}; return r }(), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := validateExecCaptureRun(CreateRunInput{FabricClient: true, Request: tt.request})
			if (err == nil) != tt.allowed {
				t.Fatalf("validation=%v allowed=%t", err, tt.allowed)
			}
		})
	}
}

func TestRestartableExecPersistsAndDispatches(t *testing.T) {
	h := newIntegrationHarness(t)
	payload := []byte("capture")
	hash := uploadExecFixture(t, h, payload)
	request := execCaptureRunRequest(hash, len(payload))
	request.Restartable = true
	var params map[string]any
	_ = json.Unmarshal(request.Params, &params)
	params["restartable"] = true
	request.Params, _ = json.Marshal(params)
	accepted := h.submit(request, "restartable")
	record, err := h.l3Store.GetRun(t.Context(), accepted.RunID)
	if err != nil || !record.Restartable {
		t.Fatalf("persisted %+v %v", record, err)
	}
	intents, err := h.l3Store.pendingDispatches(t.Context())
	if err != nil || len(intents) != 1 {
		t.Fatalf("dispatch %+v %v", intents, err)
	}
	spec := intents[0].jobSpec("token")
	if !spec.Restartable || spec.Kind != contract.JobKindProcess || spec.Labels[contract.LabelDispatchAuthority] != "" {
		t.Fatalf("spec %+v", spec)
	}
}

func TestRestartableRunMissedRunningPoll(t *testing.T) {
	for _, tc := range []struct {
		name          string
		started       bool
		renewed       bool
		interruptions int
		want          contract.RunState
	}{
		{"recorded start", true, false, 1, contract.RunRunning},
		{"earlier recorded start", true, false, 2, contract.RunRunning},
		{"interrupted before start", false, false, 1, contract.RunQueued},
		{"renewal alone", false, true, 1, contract.RunQueued},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newIntegrationHarnessWithL1Options(t, l1.StoreOptions{Clock: l1.ClockFunc(func() time.Time { return protocolTestTime })})
			agent := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{l1.DefaultAgentPrincipalTag}}, DefaultL1Address)
			status, _, body := h.do(agent, http.MethodPost, "/v1/agent/nodes/register", contract.NodeRegistration{
				NodeID: "node-1", BootSessionID: "boot-1", OS: "linux", Architecture: "arm64", AgentVersion: "test",
				Capabilities:       map[string]bool{"kind:process": true, l1.CapabilityOneshotRestartable: true},
				CapabilityRevision: 1, CapabilityObservedAt: protocolTestTime, MissingCapabilities: []string{},
			}, nil)
			if status != http.StatusOK {
				t.Fatalf("register=%d %s", status, body)
			}
			payload := []byte("capture")
			request := execCaptureRunRequest(uploadExecFixture(t, h, payload), len(payload))
			request.Restartable = true
			var params map[string]any
			if err := json.Unmarshal(request.Params, &params); err != nil {
				t.Fatal(err)
			}
			params["restartable"] = true
			request.Params, _ = json.Marshal(params)
			accepted := h.submit(request, "missed-running")
			reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if err := reconciler.ReconcileOnce(t.Context()); err != nil {
				t.Fatal(err)
			}
			record, err := h.l3Store.GetRun(t.Context(), accepted.RunID)
			if err != nil || record.Status != contract.RunQueued {
				t.Fatalf("before claim: Run=%+v err=%v", record, err)
			}
			// L3 makes no observations while these Attempts are active.
			for i := 0; i < tc.interruptions; i++ {
				claim, err := h.l1Store.ClaimJob(t.Context(), "node-1", "node-1", "boot-1", contract.JobClassOneShot)
				if err != nil || claim == nil {
					t.Fatalf("claim=%+v err=%v", claim, err)
				}
				if tc.started && i == 0 {
					if _, err := h.l1Store.StartAttempt(t.Context(), "node-1", claim.Job.JobID, claim.Lease.AttemptID, l1.StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
						t.Fatal(err)
					}
				}
				if tc.renewed {
					if _, err := h.l1Store.RenewLease(t.Context(), "node-1", claim.Job.JobID, claim.Lease.AttemptID, claim.Lease.FencingToken); err != nil {
						t.Fatal(err)
					}
				}
				job, err := h.l1Store.CompleteAttempt(t.Context(), "node-1", claim.Job.JobID, claim.Lease.AttemptID, l1.CompletionRequest{
					FencingToken: claim.Lease.FencingToken, IdempotencyKey: "interrupt-" + claim.Lease.AttemptID,
					Result:             l1.ProcessResult{Signal: "SIGTERM", TerminationCause: contract.TerminationCauseAgent},
					InterruptionReason: l1.InterruptionAgentShutdown, RuntimeQuiescenceEvidence: l1.RuntimeQuiescenceAttempt,
				})
				if err != nil || job.State != contract.JobQueued || job.RestartRecovery == nil || !job.RestartRecovery.Pending {
					t.Fatalf("requeued Job=%+v err=%v", job, err)
				}
			}
			if err := reconciler.ReconcileOnce(t.Context()); err != nil {
				t.Fatal(err)
			}
			record, err = h.l3Store.GetRun(t.Context(), accepted.RunID)
			if err != nil || record.Status != tc.want {
				t.Fatalf("after missed running poll: status=%s, want %s; err=%v", record.Status, tc.want, err)
			}
		})
	}
}

func TestNonRestartableRunQueuedJobWithRecordedStartStaysQueued(t *testing.T) {
	h := newIntegrationHarnessWithL1Options(t, l1.StoreOptions{Clock: l1.ClockFunc(func() time.Time { return protocolTestTime })})
	accepted := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "ordinary-run")
	jobs := &recordingJobClient{}
	reconciler, err := NewReconciler(h.l3Store, jobs, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	record, err := h.l3Store.GetRun(t.Context(), accepted.RunID)
	if err != nil {
		t.Fatal(err)
	}
	job := jobs.jobs[record.L1JobID]
	started := protocolTestTime
	job.Attempts = []l1.Attempt{{AttemptID: "old", State: contract.AttemptInterrupted, StartedAt: &started}}
	jobs.jobs[record.L1JobID] = job
	if err := reconciler.ReconcileOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	record, err = h.l3Store.GetRun(t.Context(), accepted.RunID)
	if err != nil || record.Status != contract.RunQueued {
		t.Fatalf("ordinary queued Job: Run=%+v err=%v", record, err)
	}
}
