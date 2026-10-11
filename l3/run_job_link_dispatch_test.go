package l3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestDispatchDeliversExactRunEnvironmentAndStoresTokenHash(t *testing.T) {
	h := newIntegrationHarness(t)
	accepted := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "execution-env")
	reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	agent := h.agent()
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", l1.ClaimRequest{NodeID: "node-1", BootSessionID: "boot-1", Class: contract.JobClassOneShot}, nil)
	if status != http.StatusOK {
		t.Fatalf("claim status = %d body=%s", status, body)
	}
	var claim l1.Claim
	if err := json.Unmarshal(body, &claim); err != nil {
		t.Fatal(err)
	}
	handoff := filepath.Join(DefaultHandoffRoot, accepted.RunID)
	wantPublic := map[string]string{
		contract.EnvRunID: accepted.RunID, contract.EnvL3Endpoint: DefaultL3Address,
		contract.EnvHandoffDir: handoff,
	}
	if !reflect.DeepEqual(claim.Job.Spec.Execution.Env, wantPublic) {
		t.Fatalf("public env = %#v, want %#v", claim.Job.Spec.Execution.Env, wantPublic)
	}
	if len(claim.Job.Spec.Execution.SensitiveEnv) != 1 || claim.Job.Spec.Execution.SensitiveEnv[contract.EnvRunToken] == "" {
		t.Fatalf("sensitive env = %#v, want only %s", claim.Job.Spec.Execution.SensitiveEnv, contract.EnvRunToken)
	}
	token := claim.Job.Spec.Execution.SensitiveEnv[contract.EnvRunToken]
	if claim.Job.Spec.Execution.HandoffDirectory != handoff {
		t.Fatalf("handoff directory = %q, want %q", claim.Job.Spec.Execution.HandoffDirectory, handoff)
	}

	var storedHash []byte
	var attemptID string
	if err := h.l3Store.db.QueryRow(`SELECT attempt_id, token_hash FROM run_tokens WHERE run_id=?`, accepted.RunID).Scan(&attemptID, &storedHash); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(token))
	if attemptID == "" || !bytes.Equal(storedHash, digest[:]) || bytes.Contains(storedHash, []byte(token)) {
		t.Fatalf("stored token binding/hash = %q/%x", attemptID, storedHash)
	}
	var staged *string
	if err := inspectTestOutbox(h.l3Store, accepted.RunID, func(v dispatchOutboxRecord) {
		if v.delivery.Valid {
			staged = &v.delivery.String
		} else {
			staged = nil
		}
	}); err != nil {
		t.Fatal(err)
	}
	if staged != nil {
		t.Fatalf("token delivery remained staged after dispatch: %q", *staged)
	}
}

func TestTerminalRunTokenGraceAndExpiry(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	clock := &mutableClock{now: now}
	store, err := OpenStore(filepath.Join(t.TempDir(), "tokens.sqlite"), StoreOptions{Clock: clock, RunTokenGrace: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	request := inlineRunRequest("#!/bin/sh\nexit 0\n")
	record, _, err := store.CreateRun(context.Background(), CreateRunInput{IdempotencyKey: "grace", Actor: "test", Request: request})
	if err != nil {
		t.Fatal(err)
	}
	token, err := store.ensureRunToken(context.Background(), record.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.beginDispatch(context.Background(), record.RunID); err != nil {
		t.Fatal(err)
	}
	if err := store.projectJobState(context.Background(), projectedRun{RunID: record.RunID, State: contract.RunDispatching}, contract.JobSucceeded); err != nil {
		t.Fatal(err)
	}
	scope, err := store.AuthenticateRunToken(context.Background(), token)
	if err != nil || scope.RunID != record.RunID || scope.AttemptID == "" {
		t.Fatalf("authenticate during grace = (%#v, %v)", scope, err)
	}
	clock.now = now.Add(time.Minute - time.Nanosecond)
	if _, err := store.AuthenticateRunToken(context.Background(), token); err != nil {
		t.Fatalf("authenticate before grace expiry: %v", err)
	}
	clock.now = now.Add(time.Minute)
	if _, err := store.AuthenticateRunToken(context.Background(), token); err == nil {
		t.Fatal("authenticate at expiry succeeded, want unauthorized")
	} else {
		var protocolErr *Error
		if !errors.As(err, &protocolErr) || protocolErr.Code != contract.ErrorUnauthorized {
			t.Fatalf("authenticate at expiry = %v, want unauthorized", err)
		}
	}
}

func TestTerminalRunTokenGraceRejectsNewChildDispatch(t *testing.T) {
	h := newIntegrationHarness(t)
	parent := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "terminal-parent")
	token, err := h.l3Store.ensureRunToken(context.Background(), parent.RunID)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := h.l3Store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := failRunTx(context.Background(), tx, parent.RunID, time.Now().UTC(), time.Minute, "test failure"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := h.l3Store.AuthenticateRunToken(context.Background(), token); err != nil {
		t.Fatalf("terminal token should remain valid for final reads and writes: %v", err)
	}
	child := inlineRunRequest("#!/bin/sh\nexit 0\n")
	child.ParentRunID = parent.RunID
	workflow := h.client(fabric.Identity{NodeID: "workflow-node"}, DefaultL3Address)
	status, _, body := h.do(workflow, http.MethodPost, "/v1/runs", child, http.Header{
		"Authorization":   []string{"Bearer " + token},
		"Idempotency-Key": []string{"terminal-child"},
	})
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorConflict)
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
				err = resetLegacyUnattemptedDispatch(t, s, run.RunID)
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

// A submit error that L3 records after the run settled must not replace the
// settled dispatch_not_found diagnostic.
func TestLateSubmitErrorDoesNotClobberASettledDiagnostic(t *testing.T) {
	s, _, clock := recoveryStore(t)
	key := endedUnrecordedRuns(t, s, clock, 1)[0]
	runID := runIDForDispatchKey(t, s, key)
	clock.now = clock.now.Add(unrecordedDispatchSettleHorizon)
	reconciler, err := NewReconciler(s, &recordingJobClient{}, ReconcilerConfig{DispatchLookup: &appearingLookupClient{}})
	if err != nil {
		t.Fatal(err)
	}
	_ = reconciler.ReconcileOnce(context.Background())
	if !jobLinkSettled(t, s, runID) {
		t.Fatal("fixture did not settle")
	}
	ctx := context.Background()
	if err := s.recordDispatchError(ctx, runID, &Error{Code: contract.ErrorConflict, Message: "late transport error", Retryable: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.failDispatch(ctx, runID, &Error{Code: contract.ErrorInvalidRequest, Message: "late refusal"}); err != nil {
		t.Fatal(err)
	}
	execution, err := s.GetRunExecution(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if execution.DispatchError == nil || execution.DispatchError.Details["reason"] != dispatchNotFoundReason {
		t.Fatalf("late submit error replaced the settled diagnostic: %+v", execution.DispatchError)
	}
}

func TestPermanentRefusalKeepsItsDiagnosticWhenNoJobIsFound(t *testing.T) {
	h := newIntegrationHarness(t)
	run := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "permanent-refusal")
	refusal := &permanentSubmitErrorClient{JobClient: h.l1Client, runID: run.RunID}
	lookup := &countingDispatchRecoveryClient{JobClient: refusal, JobDispatchLookupClient: h.l1Client}
	reconciler, err := NewReconciler(h.l3Store, lookup, ReconcilerConfig{DispatchLookup: lookup})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err == nil {
		t.Fatal("permanent dispatch refusal was not reported")
	}
	execution, err := h.l3Store.GetRunExecution(context.Background(), run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if execution.DispatchError == nil || execution.DispatchError.Code != contract.ErrorDispatchKeyConflict || execution.DispatchError.Message != "dispatch key conflict" {
		t.Fatalf("permanent refusal was replaced: %+v", execution.DispatchError)
	}
	if lookup.lookups != 1 {
		t.Fatalf("lookup calls = %d, want 1", lookup.lookups)
	}
	// Inside the settle horizon the absence only backs the run off; past it
	// the absence settles and the refusal stays the recorded diagnostic.
	ageDispatchAttemptPastSettleHorizon(t, h.l3Store, run.RunID)
	if err := reconciler.ReconcileOnce(context.Background()); err == nil {
		t.Fatal("settled absence was not reported")
	}
	execution, err = h.l3Store.GetRunExecution(context.Background(), run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if lookup.lookups != 2 || execution.DispatchError == nil || execution.DispatchError.Code != contract.ErrorDispatchKeyConflict {
		t.Fatalf("settled refusal: lookups=%d diagnostic=%+v", lookup.lookups, execution.DispatchError)
	}
	if !jobLinkSettled(t, h.l3Store, run.RunID) {
		t.Fatal("refusal not settled")
	}

	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if lookup.lookups != 2 {
		t.Fatalf("settled refusal looked up again: %d", lookup.lookups)
	}
}

// L1 accepted the job, L3 crashed before recording the dispatch, and the run
// then failed on its own protocol write. It is never dispatched again, so its
// staged run-token bearer must not stay in the outbox (#52).
func TestTerminalRunDropsItsStagedTokenDelivery(t *testing.T) {
	s, _, _ := recoveryStore(t)
	ctx := context.Background()
	record, _, err := s.CreateRun(ctx, CreateRunInput{IdempotencyKey: "staged-token", Actor: "test", Request: inlineRunRequest("#!/bin/sh\nexit 0\n")})
	if err != nil {
		t.Fatal(err)
	}
	token, err := s.ensureRunToken(ctx, record.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.beginDispatch(ctx, record.RunID); err != nil {
		t.Fatal(err)
	}
	var staged sql.NullString
	if err := inspectTestOutbox(s, record.RunID, func(v dispatchOutboxRecord) { staged = v.delivery }); err != nil {
		t.Fatal(err)
	}
	if staged.String != token {
		t.Fatalf("fixture error: staged delivery = %v, want the minted bearer", staged)
	}
	if err := s.rejectProtocolWrite(ctx, record.RunID, "envelope", "rejected", []byte(`{}`), "hash", "invalid envelope", errors.New("invalid envelope")); err != nil {
		t.Fatal(err)
	}
	failed, err := s.GetRun(ctx, record.RunID)
	if err != nil {
		t.Fatal(err)
	}
	status := failed.Status
	staged = linkTestOutbox(t, s, record.RunID).delivery
	if status != contract.RunFailed || staged.Valid {
		t.Fatalf("failed run status=%s staged delivery=%v, want failed with no bearer", status, staged)
	}
}
