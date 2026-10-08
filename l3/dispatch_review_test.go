package l3

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestDispatchReviewUnknownAnswerEvidence(t *testing.T) {
	for _, body := range []string{
		`{"error":{"code":"quota_exceeded","message":"sensitive-body","retryable":false,"request_id":"l1-review-request","details":{"secret":"sensitive-details"}}}`,
		`{"error":{"code":"quota_exceeded","retryable":false,"request_id":"l1-review-request"}}`,
		`{"error":{"code":"quota_exceeded"}}`,
	} {
		t.Run(body, func(t *testing.T) {
			h := newHoldHTTPHarness(t)
			ctx := context.Background()
			run := h.submit(inlineRunRequest("exit 0\n"), "unknown-answer")
			calls := 0
			h.l1Client.client.Transport = recoveryRoundTripper(func(req *http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: 409, Header: http.Header{"X-Request-Id": {"l1-review-request"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
			})
			var err error
			reconciler, createErr := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{OnError: func(reconcileErr error) { err = reconcileErr }})
			if createErr != nil {
				t.Fatal(createErr)
			}
			// Exercise the error callback used by the background reconcile log.
			reconciler.reconcileAndReport(ctx)
			execution, readErr := h.l3Store.GetRunExecution(ctx, run.RunID)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if execution.DispatchError == nil || execution.DispatchError.Details["l1_code"] != "quota_exceeded" || execution.DispatchError.Details["l1_status"] != float64(409) || execution.DispatchError.RequestID != "l1-review-request" {
				t.Fatalf("dispatch_error lost L1 evidence: %+v", execution.DispatchError)
			}
			_, hasRetryable := execution.DispatchError.Details["l1_retryable"]
			if hasRetryable != strings.Contains(body, `"retryable"`) || (hasRetryable && execution.DispatchError.Details["l1_retryable"] != false) {
				t.Fatalf("retryable presence lost: %+v", execution.DispatchError)
			}
			if hasRetryable && !strings.Contains(err.Error(), "retryable=false") {
				t.Errorf("log lost retryable: %v", err)
			}
			for _, want := range []string{"409", "quota_exceeded", "l1-review-request"} {
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Errorf("reconcile log missing %q: %v", want, err)
				}
			}
			payload, _ := json.Marshal(execution.DispatchError)
			if strings.Contains(string(payload), "sensitive") || strings.Contains(err.Error(), "sensitive") {
				t.Fatalf("sensitive body leaked: %s %v", payload, err)
			}
			record, _ := h.l3Store.GetRun(ctx, run.RunID)
			if record.Status != contract.RunDispatching || record.DispatchHold != nil {
				t.Fatalf("unknown answer changed outcome: %+v", record)
			}
			_ = h.reconcile()
			if calls != 1 {
				t.Fatalf("unknown answer did not back off: %d", calls)
			}
			w := httptest.NewRecorder()
			writeError(w, err)
			if strings.Contains(w.Body.String(), "quota_exceeded") || strings.Contains(w.Body.String(), "invalid control plane") || !strings.Contains(w.Body.String(), "internal server error") {
				t.Fatalf("caller error not scrubbed: %s", w.Body)
			}
		})
	}
}

func TestDispatchReviewRepeatedHoldDoesNotWrite(t *testing.T) {
	s, _, clock := recoveryStore(t)
	ctx := context.Background()
	if err := s.holdDispatch(ctx, "principal_forbidden"); err != nil {
		t.Fatal(err)
	}
	before, _ := s.DispatchHealth(ctx)
	if before.DispatchHold == nil || before.DispatchHold.Generation < 1 {
		t.Fatalf("hold generation violates wire contract: %+v", before)
	}
	// A redundant SQL UPDATE is itself a regression, even if its values match.
	if _, err := s.db.Exec(`CREATE TRIGGER reject_redundant_hold BEFORE UPDATE ON dispatch_hold WHEN OLD.since_ns IS NOT NULL AND OLD.reason=NEW.reason AND OLD.probe_id='' BEGIN SELECT RAISE(ABORT,'redundant hold write'); END`); err != nil {
		t.Fatal(err)
	}
	if err := s.holdDispatch(ctx, "principal_forbidden"); err != nil {
		t.Fatalf("repeated hold wrote: %v", err)
	}
	after, _ := s.DispatchHealth(ctx)
	if *before.DispatchHold != *after.DispatchHold {
		t.Fatal("repeated refusal changed hold")
	}
	if _, err := s.db.Exec(`DROP TRIGGER reject_redundant_hold`); err != nil {
		t.Fatal(err)
	}
	if err := s.holdDispatch(ctx, "identity_unverifiable"); err != nil {
		t.Fatal(err)
	}
	changed, _ := s.DispatchHealth(ctx)
	if changed.DispatchHold.Generation != before.DispatchHold.Generation {
		t.Fatal("generation advanced without an in-flight probe")
	}
	clock.now = clock.now.Add(time.Second)
	p, err := s.reserveDispatchProbe(ctx, time.Second)
	if err != nil || p == nil {
		t.Fatalf("reserve: %v %v", p, err)
	}
	if err := s.holdDispatch(ctx, "identity_unverifiable"); err != nil {
		t.Fatal(err)
	}
	if err := s.finishDispatchProbe(ctx, p, true); err != nil {
		t.Fatal(err)
	}
	fenced, _ := s.DispatchHealth(ctx)
	if fenced.DispatchHold == nil || fenced.DispatchHold.Generation != p.generation+1 {
		t.Fatalf("refusal did not fence in-flight probe: %+v", fenced)
	}
}

func TestDispatchReviewIdleProbeDoesNotLock(t *testing.T) {
	for _, state := range []string{"no hold", "not due", "in flight"} {
		t.Run(state, func(t *testing.T) {
			s, _, clock := recoveryStore(t)
			ctx := context.Background()
			if state != "no hold" {
				if err := s.holdDispatch(ctx, "principal_forbidden"); err != nil {
					t.Fatal(err)
				}
			}
			if state == "in flight" {
				clock.now = clock.now.Add(time.Second)
				if p, err := s.reserveDispatchProbe(ctx, time.Second); err != nil || p == nil {
					t.Fatalf("reserve: %v %v", p, err)
				}
			}
			// WAL readers proceed beside a held writer; BEGIN IMMEDIATE cannot.
			tx, err := s.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			readCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
			defer cancel()
			if p, err := s.reserveDispatchProbe(readCtx, time.Second); err != nil || p != nil {
				t.Fatalf("idle probe took write lock: %v %v", p, err)
			}
		})
	}
}

func TestDispatchReviewProbeKeyIsNotBearer(t *testing.T) {
	s, _, clock := recoveryStore(t)
	ctx := context.Background()
	if err := s.holdDispatch(ctx, "principal_forbidden"); err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(time.Second)
	p, err := s.reserveDispatchProbe(ctx, time.Second)
	if err != nil || p == nil {
		t.Fatalf("reserve: %v %v", p, err)
	}
	if strings.Contains(p.key, "wrun_") || strings.Contains(p.id, "wrun_") {
		t.Fatalf("admission probe looks like a bearer: %s", p.key)
	}
}

func TestDispatchReviewHealthRefusesBearersWithoutL1(t *testing.T) {
	for _, kind := range []string{"run", "Computer"} {
		t.Run(kind, func(t *testing.T) {
			h := newHoldHTTPHarness(t)
			ctx := context.Background()
			var token string
			if kind == "run" {
				run := h.submit(inlineRunRequest("exit 0\n"), "health-token")
				var err error
				token, err = h.l3Store.ensureRunToken(ctx, run.RunID)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				grant, err := h.l3Store.MintComputerToken(ctx, testComputerScope())
				if err != nil {
					t.Fatal(err)
				}
				token = grant.Token
			}
			calls := 0
			h.l1Client.client.Transport = recoveryRoundTripper(func(*http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: 401, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":{"code":"unauthorized","message":"not admitted","retryable":false}}`))}, nil
			})
			host := h.client(fabric.Identity{NodeID: "node-1"}, DefaultL3Address)
			status, _, body := h.do(host, "GET", "/v1/health", nil, http.Header{"Authorization": {"Bearer " + token}})
			if status != 403 || calls != 0 || !strings.Contains(string(body), "ledger health") || strings.Contains(string(body), "workflow administration") {
				t.Fatalf("health bearer: status=%d calls=%d body=%s", status, calls, body)
			}
			health, err := h.l3Store.DispatchHealth(ctx)
			if err != nil || health.DispatchHold != nil {
				t.Fatalf("health refusal started hold: %+v %v", health, err)
			}
		})
	}
}

func TestDispatchReviewRetryCleanup(t *testing.T) {
	for _, end := range []string{"acknowledged", "refused", "canceled", "projected"} {
		t.Run(end, func(t *testing.T) {
			h := newHoldHTTPHarness(t)
			s := h.l3Store
			ctx := context.Background()
			run := h.submit(inlineRunRequest("exit 0\n"), "cleanup")
			if _, err := s.beginDispatch(ctx, run.RunID); err != nil {
				t.Fatal(err)
			}
			if err := s.recordDispatchError(ctx, run.RunID, errors.New("transient")); err != nil {
				t.Fatal(err)
			}
			var err error
			switch end {
			case "acknowledged":
				err = s.completeDispatch(ctx, run.RunID, "job-review")
			case "refused":
				err = s.failDispatch(ctx, run.RunID, protocolError(contract.ErrorInvalidRequest, "refused"))
			case "canceled":
				_, err = s.db.Exec(`UPDATE runs SET dispatch_attempt_ns=NULL WHERE run_id=?`, run.RunID)
				if err == nil {
					err = s.requestRunCancellation(ctx, run.RunID, "alice")
				}
			case "projected":
				err = s.completeDispatch(ctx, run.RunID, "job-review")
				if err == nil {
					// A legacy row present when the job becomes terminal is cleaned too.
					_, err = s.db.Exec(`INSERT INTO dispatch_retry(run_id,failures,retry_ns) VALUES(?,1,0) ON CONFLICT(run_id) DO NOTHING`, run.RunID)
				}
				if err == nil {
					err = s.projectJobState(ctx, projectedRun{RunID: run.RunID, JobID: "job-review", State: contract.RunQueued}, contract.JobFailed)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			var n int
			if err := s.db.QueryRow(`SELECT COUNT(*) FROM dispatch_retry WHERE run_id=?`, run.RunID).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n != 0 {
				t.Fatalf("%s retained dispatch retry row", end)
			}
			// A late failed submit cannot resurrect retry state for a finished dispatch.
			if err := s.recordDispatchError(ctx, run.RunID, errors.New("late transient")); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow(`SELECT COUNT(*) FROM dispatch_retry WHERE run_id=?`, run.RunID).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n != 0 {
				t.Fatal("late submit error resurrected retry row")
			}
		})
	}
}
