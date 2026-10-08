package l3

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestCancelFollowupRefusalVisibleOnRead(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	run := h.submit(inlineRunRequest("exit 0\n"), "visible-refusal")
	r, _ := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err := r.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	client := &reviewCancelClient{L1Client: h.l1Client, err: &Error{Code: contract.ErrorCancelNotQueued, Message: "only one-shots support active cancellation"}}
	h.l3Server.jobs = client
	status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("cancel = %d %s", status, body)
	}
	status, _, body = h.do(h.caller, http.MethodGet, "/v1/runs/"+run.RunID, nil, nil)
	var wire struct {
		Status       contract.RunState `json:"status"`
		CancelStatus string            `json:"cancel_status"`
		CancelReason string            `json:"cancel_reason"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		t.Fatal(err)
	}
	if status != http.StatusOK || wire.Status != contract.RunQueued || wire.CancelStatus != "refused" || wire.CancelReason != client.err.Error() {
		t.Fatalf("run read = %d %+v", status, wire)
	}
	// Repeats preserve the refusal, including after reopening the ledger.
	reopened, err := OpenStore(h.l3Path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	record, err := reopened.GetRun(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(record)
	var persisted map[string]any
	_ = json.Unmarshal(raw, &persisted)
	if persisted["cancel_status"] != "refused" || persisted["cancel_reason"] != wire.CancelReason {
		t.Fatalf("reopened read = %s", raw)
	}
}

func TestCancelFollowupIdentityFailuresRetry(t *testing.T) {
	for _, code := range []contract.ErrorCode{contract.ErrorUnauthorized, contract.ErrorPersonIdentityRequired, contract.ErrorInternal} {
		t.Run(string(code), func(t *testing.T) {
			s, _, clock := recoveryStore(t)
			ctx := context.Background()
			run, _ := dispatchedRecoveryRun(t, s, "identity")
			if err := s.requestRunCancellation(ctx, run.RunID, "test"); err != nil {
				t.Fatal(err)
			}
			calls := 0
			client := &L1Client{operationTimeout: time.Second, client: &http.Client{Transport: recoveryRoundTripper(func(req *http.Request) (*http.Response, error) {
				if req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/cancel") {
					calls++
				}
				status := http.StatusUnauthorized
				body := `{"error":{"code":"` + string(code) + `","message":"fabric identity could not be authenticated","retryable":false}}`
				if calls > 1 {
					status = http.StatusOK
					raw, _ := json.Marshal(l1.Job{JobID: run.JobID, State: contract.JobFailed, Outcome: contract.JobOutcomeCanceled})
					body = string(raw)
				}
				return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
			})}}
			r, _ := NewReconciler(s, client, ReconcilerConfig{})
			pending, err := s.pendingRunCancellations(ctx, run.RunID)
			if err != nil || len(pending) != 1 {
				t.Fatalf("pending = %+v %v", pending, err)
			}
			err = r.cancelRunJob(ctx, pending[0])
			var protocol *Error
			if !errors.As(err, &protocol) || !protocol.Retryable {
				t.Errorf("identity failure must be retryable: %v", err)
			}
			var completed sql.NullInt64
			var retry int64
			if err := s.db.QueryRow(`SELECT completed_ns,retry_ns FROM run_cancellations WHERE run_id=?`, run.RunID).Scan(&completed, &retry); err != nil {
				t.Fatal(err)
			}
			if completed.Valid || retry <= clock.now.UnixNano() {
				t.Fatalf("identity failure settled: completed=%v retry=%d", completed, retry)
			}
			clock.now = clock.now.Add(unrecordedDispatchRetryBase)
			pending, err = s.pendingRunCancellations(ctx, run.RunID)
			if err != nil || len(pending) != 1 {
				t.Fatalf("due = %+v %v", pending, err)
			}
			if err := r.cancelRunJob(ctx, pending[0]); err != nil {
				t.Fatal(err)
			}
			if calls != 2 {
				t.Fatalf("retry calls=%d", calls)
			}
			record, err := s.GetRun(ctx, run.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if record.CancelStatus != "settled" || record.CancelReason != "" {
				t.Fatalf("retry retained stale error: %+v", record)
			}
		})
	}
}

func TestCancelFollowupLegacyOutboxAcknowledgement(t *testing.T) {
	for _, existingIntent := range []bool{false, true} {
		t.Run(map[bool]string{false: "explicit-cancel", true: "existing-intent"}[existingIntent], func(t *testing.T) {
			h := newIntegrationHarness(t)
			ctx := context.Background()
			run := h.submit(inlineRunRequest("exit 0\n"), "legacy-cancel")
			crashing := &loseSubmitResponseClient{JobClient: h.l1Client}
			r, _ := NewReconciler(h.l3Store, crashing, ReconcilerConfig{})
			if err := r.ReconcileOnce(ctx); err == nil || crashing.submitted.JobID == "" {
				t.Fatalf("expected accepted job with lost response: %+v %v", crashing.submitted, err)
			}
			job := crashing.submitted
			failRunAfterDispatchAttempt(t, h.l3Store, run.RunID)
			recordOutboxOnlyAcknowledgement(t, h.l3Store, run.RunID, job.JobID)
			before := snapshotTerminalRun(t, h.l3Store, run.RunID)
			if existingIntent {
				if _, err := h.l3Store.db.Exec(`INSERT INTO run_cancellations(run_id, requested_ns) VALUES(?, ?)`, run.RunID, h.l3Store.clock.Now().UnixNano()); err != nil {
					t.Fatal(err)
				}
				recovery, _ := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
				if err := recovery.ReconcileOnce(ctx); err != nil {
					t.Fatal(err)
				}
			} else {
				status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
				if status != http.StatusOK {
					t.Fatalf("cancel = %d %s", status, body)
				}
			}
			record, err := h.l3Store.GetRun(ctx, run.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if record.L1JobID != job.JobID {
				t.Fatalf("legacy job was not linked: %+v", record)
			}
			observed, err := h.l1Client.GetJob(ctx, job.JobID)
			if err != nil || observed.Outcome != contract.JobOutcomeCanceled {
				t.Fatalf("legacy job = %+v %v", observed, err)
			}
			var completed sql.NullInt64
			if err := h.l3Store.db.QueryRow(`SELECT completed_ns FROM run_cancellations WHERE run_id=?`, run.RunID).Scan(&completed); err != nil || !completed.Valid {
				t.Fatalf("legacy cancel not settled: %v %v", completed, err)
			}
			if after := snapshotTerminalRun(t, h.l3Store, run.RunID); !reflect.DeepEqual(before, after) {
				t.Fatalf("terminal outcome changed: %+v -> %+v", before, after)
			}

		})
	}
}

func TestCancelFollowupRepeatResetsBackoff(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	run := h.submit(inlineRunRequest("exit 0\n"), "repeat-backoff")
	r, _ := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err := r.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	client := &reviewCancelClient{L1Client: h.l1Client, err: errors.New("outage")}
	h.l3Server.jobs = client
	_, _, _ = h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
	if _, err := h.l3Store.db.Exec(`UPDATE run_cancellations SET failures=20,retry_ns=? WHERE run_id=?`, time.Now().Add(30*time.Minute).UnixNano(), run.RunID); err != nil {
		t.Fatal(err)
	}
	client.err = nil
	status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
	if status != http.StatusOK || client.calls != 2 {
		t.Fatalf("repeat = %d %s, calls=%d", status, body, client.calls)
	}
	var failures int
	var completed sql.NullInt64
	if err := h.l3Store.db.QueryRow(`SELECT failures,completed_ns FROM run_cancellations WHERE run_id=?`, run.RunID).Scan(&failures, &completed); err != nil || failures != 0 || !completed.Valid {
		t.Fatalf("repeat not reset/settled: failures=%d completed=%v err=%v", failures, completed, err)
	}
}

func TestCancelFollowupLocalSettlementVisible(t *testing.T) {
	s, _, _ := recoveryStore(t)
	ctx := context.Background()
	run, _, err := s.CreateRun(ctx, CreateRunInput{IdempotencyKey: "local-cancel-status", Actor: "test", Request: inlineRunRequest("exit 0\n")})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.requestRunCancellation(ctx, run.RunID, "test"); err != nil {
		t.Fatal(err)
	}
	record, err := s.GetRun(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.CancelStatus != "settled" || record.CancelReason != "" || record.Status != contract.RunFailed {
		t.Fatalf("local cancellation = %+v", record)
	}
}

func TestCancelFollowupLegacyCompletedEvidence(t *testing.T) {
	s, path, clock := recoveryStore(t)
	ctx := context.Background()
	run, _ := dispatchedRecoveryRun(t, s, "legacy-completed")
	// Reopen a pre-followup cancellation table to exercise the additive migration.
	var markerColumns int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('run_cancellations') WHERE name='refused'`).Scan(&markerColumns); err != nil {
		t.Fatal(err)
	}
	if markerColumns != 0 {
		if _, err := s.db.Exec(`ALTER TABLE run_cancellations DROP COLUMN refused`); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(path, StoreOptions{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	s = reopened
	// Pre-followup code retained last_error after both successful delivery and
	// refusal. Even retryability cannot establish which outcome won.
	for _, retryable := range []bool{true, false} {
		reason, err := json.Marshal(contract.APIError{Code: contract.ErrorInternal, Message: "old delivery error", Retryable: retryable})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec(`INSERT INTO run_cancellations(run_id,requested_ns,completed_ns,last_error) VALUES(?,?,?,?) ON CONFLICT(run_id) DO UPDATE SET last_error=excluded.last_error`, run.RunID, clock.now.UnixNano(), clock.now.UnixNano(), string(reason)); err != nil {
			t.Fatal(err)
		}
		record, err := s.GetRun(ctx, run.RunID)
		if err != nil {
			t.Fatal(err)
		}
		if record.CancelStatus != "completed" || record.CancelReason != "old delivery error" {
			t.Fatalf("legacy evidence invented a refusal: %+v", record)
		}
	}
}
