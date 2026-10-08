package l3

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

type reviewCancelClient struct {
	*L1Client
	err   error
	calls int
}

func (c *reviewCancelClient) CancelJob(ctx context.Context, id string) (l1.Job, error) {
	c.calls++
	if c.err != nil {
		// This alternate client supplies an affirmative complete wire refusal,
		// including its operation and response provenance, just like L1Client.
		if e, ok := c.err.(*Error); ok {
			status := http.StatusConflict
			switch e.Code {
			case contract.ErrorNotFound:
				status = 404
			case contract.ErrorForbidden:
				status = 403
			case contract.ErrorUnauthorized:
				status = 401
			case contract.ErrorInternal:
				status = 500
			}
			return l1.Job{}, &l1ResponseError{status: status, method: http.MethodPost, path: "/v1/jobs/" + url.PathEscape(id) + "/cancel", validEnvelope: true, protocol: e}
		}
		return l1.Job{}, c.err
	}
	return c.L1Client.CancelJob(ctx, id)
}

func TestCancelReviewPermanentRefusal(t *testing.T) {
	for _, missing := range []bool{true, false} {
		t.Run(map[bool]string{true: "lost-job", false: "wrong-submitter"}[missing], func(t *testing.T) {
			h := newIntegrationHarness(t)
			ctx := context.Background()
			run := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "permanent-refusal")
			r, _ := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
			if err := r.ReconcileOnce(ctx); err != nil {
				t.Fatal(err)
			}
			if missing {
				// Reproduce a regressed L1 without changing its immutable job records.
				if _, err := h.l3Store.db.Exec(`UPDATE runs SET l1_job_id='missing'; UPDATE dispatch_outbox SET job_id='missing'`); err != nil {
					t.Fatal(err)
				}
			}
			client := &reviewCancelClient{L1Client: h.l1Client, err: &Error{Code: contract.ErrorNotFound, Message: "job was not found"}}
			if missing {
				client.err = nil
			}
			h.l3Server.jobs = client
			status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
			if status != http.StatusOK {
				t.Fatalf("cancel refusal = %d %s", status, body)
			}
			r, _ = NewReconciler(h.l3Store, client, ReconcilerConfig{})
			for i := 0; i < 3; i++ {
				_ = r.ReconcileOnce(ctx)
			}
			status, _, body = h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
			if status != http.StatusOK {
				t.Fatalf("repeat cancel = %d %s", status, body)
			}
			if client.calls != 1 {
				t.Fatalf("permanent refusal retried %d times", client.calls)
			}
			record, err := h.l3Store.GetRun(ctx, run.RunID)
			if err != nil {
				t.Fatal(err)
			}
			want := contract.RunQueued
			if missing {
				want = contract.RunFailed
			}
			if record.Status != want {
				t.Fatalf("state=%s want=%s", record.Status, want)
			}
			// The refusal is durable evidence, rather than a retrying delivery row.
			var reason string
			if err := h.l3Store.db.QueryRow(`SELECT last_error FROM run_cancellations WHERE run_id=? AND completed_ns IS NOT NULL`, run.RunID).Scan(&reason); err != nil || reason == "" {
				t.Fatalf("refusal evidence=%q %v", reason, err)
			}
		})
	}
}

func TestCancelReviewTerminalLiveJob(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	run := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "terminal-live-job")
	r, _ := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err := r.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.l3Store.rejectProtocolWrite(ctx, run.RunID, "envelope", "rejected", []byte(`{}`), "hash", "invalid envelope", errors.New("invalid envelope")); err != nil {
		t.Fatal(err)
	}
	before, _ := h.l3Store.GetRun(ctx, run.RunID)
	status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("cancel=%d %s", status, body)
	}
	job, err := h.l1Client.GetJob(ctx, before.L1JobID)
	if err != nil || job.Outcome != contract.JobOutcomeCanceled {
		t.Fatalf("terminal run job still live: %+v %v", job, err)
	}
	after, _ := h.l3Store.GetRun(ctx, run.RunID)
	if before.Status != after.Status || before.FailureReason != after.FailureReason || !reflect.DeepEqual(before.FinishedAt, after.FinishedAt) || !before.UpdatedAt.Equal(after.UpdatedAt) {
		t.Fatalf("terminal outcome changed: %+v -> %+v", before, after)
	}
}

func TestCancelReviewRootSubmitter(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	root := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "cancel-root")
	parent := root.RunID
	for depth := 0; depth < 2; depth++ {
		request := inlineRunRequest("#!/bin/sh\nexit 0\n")
		request.ParentRunID = parent
		child, _, err := h.l3Store.CreateRun(ctx, CreateRunInput{IdempotencyKey: "cancel-child-" + parent, Actor: "run:" + parent, Request: request})
		if err != nil {
			t.Fatal(err)
		}
		parent = child.RunID
	}
	request := inlineRunRequest("exit 0\n")
	request.ParentRunID = parent
	descendant, _, err := h.l3Store.CreateRun(ctx, CreateRunInput{IdempotencyKey: "cancel-descendant", Actor: "run:" + parent, Request: request})
	if err != nil {
		t.Fatal(err)
	}
	stranger := h.client(fabric.Identity{NodeID: "stranger", UserID: "bob", Tags: []string{DefaultCallerPrincipalTag}}, DefaultL3Address)
	status, _, body := h.do(stranger, http.MethodPost, "/v1/runs/"+parent+"/cancel", nil, nil)
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)
	status, _, body = h.do(h.caller, http.MethodPost, "/v1/runs/"+parent+"/cancel", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("root submitter cancel=%d %s", status, body)
	}
	var got contract.RunRecord
	if err := json.Unmarshal(body, &got); err != nil || got.Status != contract.RunFailed {
		t.Fatalf("child cancel=%+v %v", got, err)
	}
	descendantRecord, _ := h.l3Store.GetRun(ctx, descendant.RunID)
	if descendantRecord.Status != contract.RunPending {
		t.Fatalf("cancel cascaded to descendant: %+v", descendantRecord)
	}
	rootRecord, _ := h.l3Store.GetRun(ctx, root.RunID)
	if rootRecord.Status != contract.RunPending {
		t.Fatalf("cancel escaped target: %+v", rootRecord)
	}
}

func TestCancelReviewTransientBackoff(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	clock := &mutableClock{now: time.Now().UTC()}
	h.l3Store.clock = clock
	run := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "cancel-backoff")
	r, _ := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err := r.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	client := &reviewCancelClient{L1Client: h.l1Client, err: errors.New("transport outage")}
	h.l3Server.jobs = client
	_, _, _ = h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
	reopened, err := OpenStore(h.l3Path, StoreOptions{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	r, _ = NewReconciler(reopened, client, ReconcilerConfig{})
	_ = r.ReconcileOnce(ctx)
	if client.calls != 1 {
		t.Fatalf("retried before backoff: %d calls", client.calls)
	}
	clock.now = clock.now.Add(unrecordedDispatchRetryBase)
	_ = r.ReconcileOnce(ctx)
	if client.calls != 2 {
		t.Fatalf("did not retry due cancellation: %d", client.calls)
	}
	clock.now = clock.now.Add(unrecordedDispatchRetryBase)
	_ = r.ReconcileOnce(ctx)
	if client.calls != 2 {
		t.Fatalf("second failure did not double backoff: %d", client.calls)
	}
	client.err = nil
	clock.now = clock.now.Add(unrecordedDispatchRetryBase)
	if err := r.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	got, _ := h.l3Store.GetRun(ctx, run.RunID)
	if got.Status != contract.RunFailed {
		t.Fatalf("retry did not settle cancellation: %+v", got)
	}
}

func TestCancelReviewLookupBudgetAndBackoff(t *testing.T) {
	s, _, clock := recoveryStore(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		key := time.Duration(i).String()
		run, _, err := s.CreateRun(ctx, CreateRunInput{IdempotencyKey: key, Actor: "test", Request: inlineRunRequest("exit 0\n")})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.beginDispatch(ctx, run.RunID); err != nil {
			t.Fatal(err)
		}
		// A terminal canceled dispatch must not be looked up a second time by recovery.
		if i == 0 {
			if err := s.rejectProtocolWrite(ctx, run.RunID, "envelope", key, []byte(`{}`), "hash", "invalid", errors.New("invalid")); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.requestRunCancellation(ctx, run.RunID, "test"); err != nil {
			t.Fatal(err)
		}
	}
	lookup := &unavailableLookupClient{hang: true}
	const budget = 50 * time.Millisecond
	r, _ := NewReconciler(s, &recordingJobClient{}, ReconcilerConfig{DispatchLookup: lookup, DispatchRecoveryBudget: budget})
	started := time.Now()
	_ = r.ReconcileOnce(ctx)
	if elapsed := time.Since(started); elapsed > budget+2*time.Second {
		t.Fatalf("cancel lookups escaped budget: %v", elapsed)
	}
	if len(lookup.looked) != 1 {
		t.Fatalf("lookups=%v want one", lookup.looked)
	}
	lookup.hang = false
	_ = r.ReconcileOnce(ctx)
	if len(lookup.looked) != 3 {
		t.Fatalf("backed-off row retried or peers starved: %v", lookup.looked)
	}
	_ = r.ReconcileOnce(ctx)
	if len(lookup.looked) != 3 {
		t.Fatalf("lookups ignored backoff: %v", lookup.looked)
	}
	clock.now = clock.now.Add(unrecordedDispatchRetryBase)
	_ = r.ReconcileOnce(ctx)
	if len(lookup.looked) != 6 {
		t.Fatalf("due lookups=%v", lookup.looked)
	}
}

func TestCancelReviewTerminalOutageReturnsRealState(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	run := h.submit(inlineRunRequest("exit 0\n"), "terminal-outage")
	r, _ := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err := r.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.l3Store.rejectProtocolWrite(ctx, run.RunID, "envelope", "rejected", []byte(`{}`), "hash", "invalid", errors.New("invalid")); err != nil {
		t.Fatal(err)
	}
	before, _ := h.l3Store.GetRun(ctx, run.RunID)
	client := &reviewCancelClient{L1Client: h.l1Client, err: &Error{Code: contract.ErrorInternal, Retryable: true, Message: "outage"}}
	h.l3Server.jobs = client
	for i := 0; i < 2; i++ {
		status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
		if status != http.StatusOK {
			t.Fatalf("terminal cancel=%d %s", status, body)
		}
		var got contract.RunRecord
		err := json.Unmarshal(body, &got)
		got.CancelStatus, got.CancelReason = "", ""
		if err != nil || !reflect.DeepEqual(before, got) {
			t.Fatalf("terminal response changed: %+v %v", got, err)
		}
	}
	if client.calls != 2 {
		t.Fatalf("terminal delivery calls=%d want explicit repeat delivery", client.calls)
	}
}

type absentReviewLookup struct{ calls int }

func (c *absentReviewLookup) LookupJobByDispatchKey(_ context.Context, key string) (l1.Job, error) {
	c.calls++
	return l1.Job{}, &DispatchNotFoundError{DispatchKey: key}
}

func TestCancelReviewProvisionalAbsenceBackoff(t *testing.T) {
	s, _, clock := recoveryStore(t)
	ctx := context.Background()
	run, _, err := s.CreateRun(ctx, CreateRunInput{IdempotencyKey: "provisional-cancel", Actor: "test", Request: inlineRunRequest("exit 0\n")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.beginDispatch(ctx, run.RunID); err != nil {
		t.Fatal(err)
	}
	if err := s.requestRunCancellation(ctx, run.RunID, "test"); err != nil {
		t.Fatal(err)
	}
	lookup := &absentReviewLookup{}
	r, _ := NewReconciler(s, &recordingJobClient{}, ReconcilerConfig{DispatchLookup: lookup})
	for i := 0; i < 3; i++ {
		if err := r.ReconcileOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if lookup.calls != 1 {
		t.Fatalf("provisional absence looked up %d times", lookup.calls)
	}
	record, _ := s.GetRun(ctx, run.RunID)
	if record.FinishedAt != nil {
		t.Fatalf("absence settled before horizon: %+v", record)
	}
	clock.now = clock.now.Add(unrecordedDispatchSettleHorizon)
	if err := r.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	record, _ = s.GetRun(ctx, run.RunID)
	if lookup.calls != 2 || record.Status != contract.RunFailed || record.FailureReason != "the run was canceled before dispatch" {
		t.Fatalf("horizon settlement=%+v calls=%d", record, lookup.calls)
	}
}
