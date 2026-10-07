package l3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestCancelRunStates(t *testing.T) {
	for _, state := range []string{"pending", "queued", "claimed", "running", "succeeded-unprojected", "failed-unprojected", "succeeded", "failed"} {
		t.Run(state, func(t *testing.T) {
			h := newIntegrationHarness(t)
			ctx := context.Background()
			request := inlineRunRequest("#!/bin/sh\nexit 0\n")
			request.Tags = append(request.Tags, contract.StableNodeTagPrefix+"node-1")
			run := h.submit(request, "cancel-"+state)
			reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			reconcile := func() {
				t.Helper()
				if err := reconciler.ReconcileOnce(ctx); err != nil {
					t.Fatal(err)
				}
			}
			var agent *http.Client
			var claim l1.Claim
			var completionPath string
			if state != "pending" {
				reconcile()
			}
			if state != "pending" && state != "queued" {
				agent = h.agent()
				status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", l1.ClaimRequest{NodeID: "node-1", BootSessionID: "boot-1", Class: contract.JobClassOneShot}, nil)
				if status != http.StatusOK {
					t.Fatalf("claim = %d %s", status, body)
				}
				if err := json.Unmarshal(body, &claim); err != nil {
					t.Fatal(err)
				}
				completionPath = fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", claim.Job.JobID, claim.Lease.AttemptID)
				if state != "claimed" {
					status, _, body = h.do(agent, http.MethodPost, fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/started", claim.Job.JobID, claim.Lease.AttemptID), l1.StartedRequest{FencingToken: claim.Lease.FencingToken}, nil)
					if status != http.StatusOK {
						t.Fatalf("started = %d %s", status, body)
					}
					if state == "running" {
						reconcile()
					} else {
						exit := 0
						if state == "failed" || state == "failed-unprojected" {
							exit = 17
						}
						status, _, body = h.do(agent, http.MethodPost, completionPath, l1.CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "before-cancel", Result: l1.ProcessResult{ExitCode: &exit}}, nil)
						if status != http.StatusOK {
							t.Fatalf("complete = %d %s", status, body)
						}
						if state == "succeeded" || state == "failed" {
							reconcile()
							reconcile()
						}
					}
				}
			}
			before, err := h.l3Store.GetRun(ctx, run.RunID)
			if err != nil {
				t.Fatal(err)
			}
			cancel := func() contract.RunRecord {
				t.Helper()
				status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
				if status != http.StatusOK {
					t.Fatalf("cancel = %d %s", status, body)
				}
				var record contract.RunRecord
				if err := json.Unmarshal(body, &record); err != nil {
					t.Fatal(err)
				}
				return record
			}
			got := cancel()
			if state == "claimed" || state == "running" {
				if got.FinishedAt != nil {
					t.Fatalf("settled before job: %+v", got)
				}
				job, err := h.l1Store.GetJob(ctx, claim.Job.JobID)
				if err != nil || job.Outcome != contract.JobOutcomeCanceled {
					t.Fatalf("intent: %+v %v", job, err)
				}
				cancel()
				exit := 0
				status, _, body := h.do(agent, http.MethodPost, completionPath, l1.CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "after-cancel", Result: l1.ProcessResult{ExitCode: &exit}}, nil)
				if status != http.StatusOK {
					t.Fatalf("settle = %d %s", status, body)
				}
				reconcile()
				got = cancel()
			}
			want := contract.RunFailed
			reason := "the L1 job was canceled"
			if state == "pending" {
				reason = "the run was canceled before dispatch"
			}
			if state == "succeeded" || state == "succeeded-unprojected" {
				want = contract.RunSucceeded
				reason = ""
			}
			if state == "failed" || state == "failed-unprojected" {
				reason = "exit 17"
			}
			if got.Status != want || got.FailureReason != reason || got.FinishedAt == nil {
				t.Fatalf("cancel = %+v want %s / %q", got, want, reason)
			}
			if state == "succeeded" || state == "failed" {
				if !reflect.DeepEqual(before, got) {
					t.Fatalf("rewrote terminal run: before=%+v after=%+v", before, got)
				}
			}
			repeated := cancel()
			if !reflect.DeepEqual(got, repeated) {
				t.Fatalf("retry rewrote run: before=%+v after=%+v", got, repeated)
			}
			reconcile()
			if state == "pending" {
				record, err := h.l3Store.GetRun(ctx, run.RunID)
				if err != nil || record.L1JobID != "" {
					t.Fatalf("canceled run dispatched: %+v %v", record, err)
				}
			}
			status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/rerun", nil, http.Header{"Idempotency-Key": []string{"rerun-after-cancel"}})
			if status != http.StatusCreated {
				t.Fatalf("rerun = %d %s", status, body)
			}
			var rerun RunAccepted
			if err := json.Unmarshal(body, &rerun); err != nil {
				t.Fatal(err)
			}
			reconcile()
			fresh, err := h.l3Store.GetRun(ctx, rerun.RunID)
			if err != nil || fresh.Status != contract.RunQueued || fresh.L1JobID == "" || fresh.L1JobID == got.L1JobID {
				t.Fatalf("rerun inherited cancellation: %+v %v", fresh, err)
			}

		})
	}
}

func TestCancelRunAuthorization(t *testing.T) {
	h := newIntegrationHarness(t)
	run := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "cancel-auth")
	stranger := h.client(fabric.Identity{NodeID: "stranger", UserID: "bob", Tags: []string{DefaultCallerPrincipalTag}}, DefaultL3Address)
	status, _, body := h.do(stranger, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)
	status, _, body = h.do(h.caller, http.MethodPost, "/v1/runs/run_missing/cancel", nil, nil)
	assertAPIError(t, status, body, http.StatusNotFound, contract.ErrorNotFound)
	record, err := h.l3Store.GetRun(context.Background(), run.RunID)
	if err != nil || record.Status != contract.RunPending {
		t.Fatalf("unauthorized mutation: %+v %v", record, err)
	}
}

func TestCancelRunAmbiguousDispatch(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	run := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "cancel-ambiguous")
	intents, err := h.l3Store.pendingDispatches(ctx)
	if err != nil {
		t.Fatal(err)
	}
	token, err := h.l3Store.beginDispatch(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	job, err := h.l1Client.SubmitJob(ctx, intents[0].jobSpec(token))
	if err != nil {
		t.Fatal(err)
	}
	// L1 committed, but its acknowledgement was lost before the job ID was stored.
	status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("cancel = %d %s", status, body)
	}
	reopened, err := OpenStore(h.l3Path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reconciler, err := NewReconciler(reopened, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	record, err := reopened.GetRun(ctx, run.RunID)
	if err != nil || record.Status != contract.RunFailed || record.L1JobID != job.JobID || record.FailureReason != "the L1 job was canceled" {
		t.Fatalf("recovered cancellation: %+v %v", record, err)
	}
}

func TestCancelRunDispatchFence(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	run := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "cancel-inflight")
	intents, err := h.l3Store.pendingDispatches(ctx)
	if err != nil {
		t.Fatal(err)
	}
	token, err := h.l3Store.beginDispatch(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("cancel = %d %s", status, body)
	}
	if _, err := h.l3Store.beginDispatch(ctx, run.RunID); !errors.Is(err, errDispatchAbandoned) {
		t.Fatalf("began a submit after cancel: %v", err)
	}
	pending, err := h.l3Store.pendingDispatches(ctx)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending after cancel=%+v %v", pending, err)
	}
	reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	record, err := h.l3Store.GetRun(ctx, run.RunID)
	if err != nil || record.FinishedAt != nil || record.L1JobID != "" {
		t.Fatalf("premature absence settlement: %+v %v", record, err)
	}
	// The already-started request can still arrive after the first lookup.
	job, err := h.l1Client.SubmitJob(ctx, intents[0].jobSpec(token))
	if err != nil {
		t.Fatal(err)
	}
	if err := h.l3Store.completeDispatch(ctx, run.RunID, job.JobID); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	record, err = h.l3Store.GetRun(ctx, run.RunID)
	if err != nil || record.Status != contract.RunFailed || record.L1JobID != job.JobID || record.FailureReason != "the L1 job was canceled" {
		t.Fatalf("late ack cancellation: %+v %v", record, err)
	}
}

func TestCancelRunAbsentDispatchSettlement(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	run := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "cancel-absent")
	if _, err := h.l3Store.beginDispatch(ctx, run.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.l3Store.db.Exec(`UPDATE runs SET dispatch_attempt_ns=dispatch_attempt_ns-? WHERE run_id=?`, int64(unrecordedDispatchSettleHorizon+time.Minute), run.RunID); err != nil {
		t.Fatal(err)
	}
	status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("cancel = %d %s", status, body)
	}
	var record contract.RunRecord
	if err := json.Unmarshal(body, &record); err != nil {
		t.Fatal(err)
	}
	if record.Status != contract.RunFailed || record.FinishedAt == nil || record.L1JobID != "" || record.FailureReason != "the run was canceled before dispatch" {
		t.Fatalf("absent cancellation=%+v", record)
	}
}

type unavailableCancelClient struct{ *L1Client }

func (c unavailableCancelClient) CancelJob(context.Context, string) (l1.Job, error) {
	return l1.Job{}, &Error{Code: contract.ErrorInternal, Message: "injected cancel outage", Retryable: true}
}

func TestCancelRunDeliverySurvivesRestart(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	run := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "cancel-delivery")
	reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	h.l3Server.jobs = unavailableCancelClient{h.l1Client}
	status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
	assertAPIError(t, status, body, http.StatusServiceUnavailable, contract.ErrorInternal)
	reopened, err := OpenStore(h.l3Path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reconciler, err = NewReconciler(reopened, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	record, err := reopened.GetRun(ctx, run.RunID)
	if err != nil || record.Status != contract.RunFailed || record.FailureReason != "the L1 job was canceled" {
		t.Fatalf("delivery after restart=%+v %v", record, err)
	}
}
