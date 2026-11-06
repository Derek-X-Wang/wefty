package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// A committed L1 mutation whose post-change read expired (#1511) recovers
// with exactly one bounded step that the mutation names: a keyed request is
// resent once, an unkeyed one is read back, and otherwise the answer stays
// exit 13 but says the change was applied. Every check decides by the
// requests the stub saw; no wall-clock assertion decides an outcome.

func writeCommittedReadUnavailable(w http.ResponseWriter, retryable bool) {
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
		Code: contract.ErrorUnavailable, Message: "job change committed but its post-change read is unavailable", Retryable: retryable,
		Details: map[string]any{"reason": "read_snapshot_post_change_failed", "mutation_applied": true,
			"job_id": "job_1", "read_reason": "read_snapshot_expired"},
	}})
}

type postChangeRequest struct{ method, path, body string }

// postChangeStub answers the first request with a committed-but-unread
// answer and every later one with answer, and records each request.
func postChangeStub(t *testing.T, answer func(w http.ResponseWriter)) (*apiClients, func() []postChangeRequest) {
	t.Helper()
	var mu sync.Mutex
	var seen []postChangeRequest
	clients, _ := readRetryStub(t, func(requests int32, r *http.Request, w http.ResponseWriter) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		seen = append(seen, postChangeRequest{r.Method, r.URL.RequestURI(), string(body)})
		mu.Unlock()
		if requests == 1 {
			writeCommittedReadUnavailable(w, true)
			return
		}
		answer(w)
	})
	withRetryWait(t, func(context.Context, time.Duration) error { return nil })
	return clients, func() []postChangeRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]postChangeRequest(nil), seen...)
	}
}

func writeJob(w http.ResponseWriter, status int, job l1.Job) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(job)
}

// TestKeyedMutationResendsOnceAfterCommittedReadUnavailable: the identical
// keyed request is sent once more; L1's replay is the success (no error, so exit 0).
func TestKeyedMutationResendsOnceAfterCommittedReadUnavailable(t *testing.T) {
	clients, seen := postChangeStub(t, func(w http.ResponseWriter) {
		w.Header().Set("Idempotent-Replay", "true")
		writeJob(w, http.StatusOK, l1.Job{JobID: "job_1", State: contract.JobQueued})
	})
	spec := contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: "create-key"}
	job, err := clients.createService(t.Context(), spec)
	requests := seen()
	if err != nil || job.JobID != "job_1" || len(requests) != 2 ||
		requests[0] != requests[1] || requests[1].method != http.MethodPost || requests[1].path != "/v1/jobs" ||
		!strings.Contains(requests[1].body, `"dispatch_key":"create-key"`) {
		t.Fatalf("keyed resend: job=%+v err=%v requests=%+v", job, err, requests)
	}
}

// TestKeyedResendKeepsTheFirstReceipt: L1 wrote this command's receipt
// (applied, not a replay) before its post-change read expired. The resend is
// a replay, but the command reports what it did, with the replay's operation.
func TestKeyedResendKeepsTheFirstReceipt(t *testing.T) {
	clients, requests := readRetryStub(t, func(requests int32, r *http.Request, w http.ResponseWriter) {
		if requests == 1 {
			w.Header().Set("Mutation-Applied", "true")
			w.Header().Set("Idempotent-Replay", "false")
			writeCommittedReadUnavailable(w, true)
			return
		}
		w.Header().Set("Mutation-Applied", "false")
		w.Header().Set("Idempotent-Replay", "true")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: "computer_1", IntentRevision: 4})
	})
	withRetryWait(t, func(context.Context, time.Duration) error { return nil })
	computer, receipt, err := clients.restartComputer(t.Context(), "computer_1", l1.ComputerRestartRequest{IdempotencyKey: "restart-key"})
	if err != nil || computer.IntentRevision != 4 || requests.Load() != 2 || !receipt.Applied || receipt.Replay {
		t.Fatalf("keyed resend receipt: computer=%+v receipt=%+v err=%v requests=%d", computer, receipt, err, requests.Load())
	}
}

// TestKeyedResendKeepsAReplayReceiptAndOperationHeaders: a first answer that
// was itself a replay stays one, and the resend's other headers survive.
func TestKeyedResendKeepsAReplayReceiptAndOperationHeaders(t *testing.T) {
	for _, test := range []struct {
		name         string
		firstReplay  string
		wantReplay   bool
		wantApplied  bool
		firstApplied string
	}{
		{"first answer was a replay", "true", true, false, "false"},
		{"first answer omits the replay header", "", false, true, "true"},
	} {
		t.Run(test.name, func(t *testing.T) {
			clients, requests := readRetryStub(t, func(requests int32, r *http.Request, w http.ResponseWriter) {
				if requests == 1 {
					w.Header().Set("Mutation-Applied", test.firstApplied)
					if test.firstReplay != "" {
						w.Header().Set("Idempotent-Replay", test.firstReplay)
					}
					writeCommittedReadUnavailable(w, true)
					return
				}
				w.Header().Set("Mutation-Applied", "false")
				w.Header().Set("Idempotent-Replay", "true")
				w.Header().Set("X-Operation", "kept")
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: "computer_1"})
			})
			withRetryWait(t, func(context.Context, time.Duration) error { return nil })
			headers, err := clients.l1.mutate(t.Context(), resendKeepingReceipt, http.MethodPost, "/v1/computers/computer_1/restart", nil, &l1.Computer{}, http.StatusOK)
			receipt := mutationReceiptFromHeaders(headers)
			if err != nil || requests.Load() != 2 || receipt.Replay != test.wantReplay || receipt.Applied != test.wantApplied ||
				headers.Get("X-Operation") != "kept" {
				t.Fatalf("receipt=%+v headers=%v err=%v requests=%d", receipt, headers, err, requests.Load())
			}
		})
	}
}

// TestNoResendCallSitesSendOneMutation pins the client methods whose plan is
// not to resend: each sends one mutation, keeps exit 13 and names the same
// idempotency key.
func TestNoResendCallSitesSendOneMutation(t *testing.T) {
	for _, test := range []struct {
		name string
		call func(context.Context, *apiClients) error
	}{
		{"reimage", func(ctx context.Context, c *apiClients) error {
			_, _, err := c.reimageComputer(ctx, "computer_1", l1.ComputerReimageRequest{IdempotencyKey: "k"})
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			// L1 sends reimage's receipt before the read, so only the
			// plan keeps it from resending.
			clients, requests := readRetryStub(t, func(requests int32, r *http.Request, w http.ResponseWriter) {
				if requests > 1 {
					t.Error("a no-resend mutation sent a second request")
				}
				w.Header().Set("Mutation-Applied", "true")
				w.Header().Set("Idempotent-Replay", "false")
				writeCommittedReadUnavailable(w, true)
			})
			withRetryWait(t, func(context.Context, time.Duration) error { return nil })
			err := test.call(t.Context(), clients)
			var refusal *apiResponseError
			if commandExitCode(err) != exitUnavailable || !errors.As(err, &refusal) || requests.Load() != 1 ||
				!strings.Contains(refusal.APIError.Message, "the change was applied") ||
				!strings.Contains(refusal.APIError.Message, "same idempotency key") {
				t.Fatalf("exit=%d requests=%d err=%v", commandExitCode(err), requests.Load(), err)
			}
		})
	}
}

// TestUpgradeRestartReadsBackInsteadOfResending: an upgrade restart can be a
// no-op that records no key, so it is read back, never sent again.
func TestUpgradeRestartReadsBackInsteadOfResending(t *testing.T) {
	clients, seen := postChangeStub(t, func(w http.ResponseWriter) {
		writeJob(w, http.StatusOK, l1.Job{JobID: "job_1", State: contract.JobRunning})
	})
	job, err := clients.restartServiceRequest(t.Context(), "job_1", l1.ServiceRestartRequest{
		IdempotencyKey: "upgrade-key", Upgrade: &l1.ServiceUpgradeRecovery{}})
	requests := seen()
	if err != nil || job.JobID != "job_1" || len(requests) != 2 || requests[0].method != http.MethodPost ||
		requests[1].method != http.MethodGet || requests[1].path != "/v1/jobs/job_1?class="+contract.JobClassService {
		t.Fatalf("upgrade restart: job=%+v err=%v requests=%+v", job, err, requests)
	}
}

// TestUnkeyedMutationReadsBackAfterCommittedReadUnavailable: an unkeyed
// mutation is never sent again; its matching GET supplies the success, and
// the command prints its normal output (exit 0).
func TestUnkeyedMutationReadsBackAfterCommittedReadUnavailable(t *testing.T) {
	clients, seen := postChangeStub(t, func(w http.ResponseWriter) {
		writeJob(w, http.StatusOK, l1.Job{JobID: "job_1", State: contract.JobFailed})
	})
	var stdout, stderr bytes.Buffer
	err := execute(t.Context(), clients, true, []string{"cancel", "job_1"}, &stdout, &stderr)
	requests := seen()
	var job l1.Job
	decode := json.Unmarshal(stdout.Bytes(), &job)
	if err != nil || decode != nil || job.State != contract.JobFailed || len(requests) != 2 ||
		requests[0].method != http.MethodPost || requests[0].path != "/v1/jobs/job_1/cancel" ||
		requests[1].method != http.MethodGet || requests[1].path != "/v1/jobs/job_1" {
		t.Fatalf("read-back: err=%v stdout=%s stderr=%s requests=%+v", err, stdout.String(), stderr.String(), requests)
	}

	// A service mutation reads back its service view.
	clients, seen = postChangeStub(t, func(w http.ResponseWriter) {
		writeJob(w, http.StatusOK, l1.Job{JobID: "job_1", State: contract.JobStopped})
	})
	stopped, err := clients.setServiceDesiredState(t.Context(), "job_1", contract.ServiceDesiredStopped)
	requests = seen()
	if err != nil || stopped.State != contract.JobStopped || len(requests) != 2 ||
		requests[0].method != http.MethodPut || requests[1].method != http.MethodGet ||
		requests[1].path != "/v1/jobs/job_1?class="+contract.JobClassService {
		t.Fatalf("service read-back: job=%+v err=%v requests=%+v", stopped, err, requests)
	}
}

// TestCommittedReadUnavailableWithoutRecoveryKeepsExit13: with no key and
// no GET, nothing more is sent; the answer stays exit 13 with its details and
// says plainly that the change was applied, with a hint to read it again.
// A failed recovery step ends the same way after its single request.
func TestCommittedReadUnavailableWithoutRecoveryKeepsExit13(t *testing.T) {
	for _, test := range []struct {
		name     string
		recovery postChangeRecovery
		later    func(w http.ResponseWriter)
		requests int
		hint     string
	}{
		{"no key and no GET", postChangeRecovery{hint: "run `wefty inspect job_1` to read it again"}, nil, 1, "wefty inspect job_1"},
		{"keyed but not resent", sameKeyReadAgain, nil, 1, "same idempotency key"},
		{"receipt plan without a first receipt", resendKeepingReceipt, nil, 1, "same idempotency key"},
		{"resend unavailable again", resendWithKey, func(w http.ResponseWriter) { writeCommittedReadUnavailable(w, true) }, 2, "same idempotency key"},
		{"read-back refused", readBackWith("/v1/jobs/job_1", "wefty inspect job_1"), writeForbiddenResponse, 2, "wefty inspect job_1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			clients, seen := postChangeStub(t, func(w http.ResponseWriter) {
				if test.later == nil {
					t.Error("a mutation without recovery sent a second request")
					return
				}
				test.later(w)
			})
			var job l1.Job
			_, err := clients.l1.mutate(t.Context(), test.recovery, http.MethodPost, "/v1/jobs/job_1/cancel", nil, &job, http.StatusOK)
			var refusal *apiResponseError
			if commandExitCode(err) != exitUnavailable || !errors.As(err, &refusal) || len(seen()) != test.requests ||
				refusal.APIError.Details["mutation_applied"] != true ||
				!strings.Contains(refusal.APIError.Message, "the change was applied, but reading it back was unavailable") ||
				!strings.Contains(refusal.APIError.Message, test.hint) {
				t.Fatalf("exit=%d requests=%d err=%v", commandExitCode(err), len(seen()), err)
			}
		})
	}
}

// TestCommittedReadUnavailableRecoveryBounds: a non-retryable post-change
// answer and a caller cancellation before the resend both end after the one
// mutation request.
func TestCommittedReadUnavailableRecoveryBounds(t *testing.T) {
	t.Run("not retryable", func(t *testing.T) {
		clients, requests := readRetryStub(t, func(_ int32, _ *http.Request, w http.ResponseWriter) {
			writeCommittedReadUnavailable(w, false)
		})
		_, err := clients.createService(t.Context(), contract.JobSpec{DispatchKey: "create-key"})
		if commandExitCode(err) != exitUnavailable || requests.Load() != 1 || strings.Contains(err.Error(), "reading it back") {
			t.Fatalf("non-retryable committed answer: requests=%d err=%v", requests.Load(), err)
		}
	})
	t.Run("cancelled before the resend", func(t *testing.T) {
		clients, requests := readRetryStub(t, func(_ int32, _ *http.Request, w http.ResponseWriter) {
			writeCommittedReadUnavailable(w, true)
		})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		withRetryWait(t, func(ctx context.Context, _ time.Duration) error {
			cancel()
			return ctx.Err()
		})
		_, err := clients.createService(ctx, contract.JobSpec{DispatchKey: "create-key"})
		if !errors.Is(err, context.Canceled) || requests.Load() != 1 {
			t.Fatalf("cancelled resend: requests=%d err=%v", requests.Load(), err)
		}
	})
}
