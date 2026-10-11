package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// `services revoke --wait` reads through the snapshot door, so a retryable
// 503 (`read_snapshot_expired`) can hit its observation loop; the existing
// behavior ended a two-minute wait on one (#763 round 2). Inside the window
// such an answer is transient, and exhausting the window on retryable answers
// alone is the existing revocation_wait_timeout outcome with the last answer
// recorded.

func revokeWaitStub(t *testing.T, revocationAnswer func(reads int32) func(w http.ResponseWriter)) (*apiClients, *atomic.Int32) {
	t.Helper()
	return revokeWaitRequestStub(t, func(reads int32, w http.ResponseWriter, _ *http.Request) {
		revocationAnswer(reads)(w)
	})
}

func revokeWaitRequestStub(t *testing.T, revocationAnswer func(int32, http.ResponseWriter, *http.Request)) (*apiClients, *atomic.Int32) {
	t.Helper()
	var revocationReads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/computer-handle-resolutions/computer-1":
			_ = json.NewEncoder(w).Encode(l1.ComputerHandleResolution{ComputerID: "computer-1"})
		case r.Method == http.MethodPut && r.URL.Path == "/v1/computers/computer-1/grants/person-1":
			_ = json.NewEncoder(w).Encode(l1.ComputerGrantMutationResult{MutationApplied: true,
				Revocation: &l1.ComputerPolicyRevocation{ComputerID: "computer-1",
					SubjectFabricID: "fabric-1", SubjectUserID: "person-1", PolicyRevision: 2,
					State: l1.ComputerPolicyRevocationPending}})
		case strings.HasPrefix(r.URL.Path, "/v1/computers/computer-1/revocations/"):
			revocationAnswer(revocationReads.Add(1), w, r)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(contract.APIError{Code: contract.ErrorNotFound, Message: r.URL.Path})
		}
	}))
	t.Cleanup(server.Close)
	// Resolving the Computer handle is a one-shot read before the wait loop.
	client := waitStubClient(t, server, "/v1/computer-handle-resolutions/")
	return &apiClients{l1: client, wait: waitForContext}, &revocationReads
}

func assertRevokeWaitTimeout(t *testing.T, ctx context.Context, err error, elapsed time.Duration) *apiResponseError {
	t.Helper()
	var refusal *apiResponseError
	if commandExitCode(err) != exitMutationWaitTimeout || !errors.As(err, &refusal) ||
		refusal.APIError.Code != contract.ErrorRevocationWaitTimeout || ctx.Err() != nil ||
		elapsed > 2*time.Second {
		t.Fatalf("revoke wait: elapsed=%s caller=%v exit=%d, want wait deadline exit 14 within 2s: %v",
			elapsed, ctx.Err(), commandExitCode(err), err)
	}
	last, ok := refusal.APIError.Details["last_observed_revocation"].(*l1.ComputerPolicyRevocation)
	if !refusal.APIError.Retryable || refusal.APIError.Details["mutation_applied"] != true ||
		refusal.APIError.Details["observation_state"] != "pending" || !ok ||
		last.State != l1.ComputerPolicyRevocationPending {
		t.Fatalf("wait timeout lost receipt or pending observation: %#v", refusal.APIError)
	}
	return refusal
}

func TestRevokeWaitBoundsStalledReads(t *testing.T) {
	for _, stall := range []string{"headers", "body"} {
		for _, retryFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/retry-first=%t", stall, retryFirst), func(t *testing.T) {
				clients, reads := revokeWaitRequestStub(t, func(reads int32, w http.ResponseWriter, r *http.Request) {
					if retryFirst && reads == 1 {
						writeRetryableUnavailable(w, "read_snapshot_expired")
						return
					}
					if stall == "body" {
						_, _ = w.Write([]byte("{"))
						w.(http.Flusher).Flush()
					}
					<-r.Context().Done()
				})
				// The caller deadline only releases the original unbounded read.
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				var stdout, stderr bytes.Buffer
				started := time.Now()
				err := executeComputerGrant(ctx, clients, true, revokeWaitArgs("500ms", "5ms"), &stdout, &stderr, true)
				refusal := assertRevokeWaitTimeout(t, ctx, err, time.Since(started))
				wantReads := int32(1)
				if retryFirst {
					wantReads++
					if !strings.Contains(fmt.Sprint(refusal.APIError.Details["observation_failure"]), "read_snapshot_expired") {
						t.Fatalf("last retryable answer lost at stalled read deadline: %#v", refusal.APIError)
					}
				}
				if reads.Load() < wantReads {
					t.Fatalf("revocation reads=%d, want at least %d", reads.Load(), wantReads)
				}
			})
		}
	}
}

func TestRevokeWaitCountsReadTime(t *testing.T) {
	for _, answer := range []string{"pending", "retryable", "late-completed"} {
		t.Run(answer, func(t *testing.T) {
			clients, reads := revokeWaitRequestStub(t, func(_ int32, w http.ResponseWriter, r *http.Request) {
				delay := 100 * time.Millisecond
				if answer == "late-completed" {
					delay = 3 * time.Second
				}
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case <-r.Context().Done():
					return
				case <-timer.C:
				}
				if answer == "retryable" {
					writeRetryableUnavailable(w, "read_snapshot_expired")
					return
				}
				state := l1.ComputerPolicyRevocationPending
				if answer == "late-completed" {
					state = l1.ComputerPolicyRevocationCompleted
				}
				_ = json.NewEncoder(w).Encode(l1.ComputerPolicyRevocation{ComputerID: "computer-1",
					SubjectFabricID: "fabric-1", SubjectUserID: "person-1", PolicyRevision: 2, State: state})
			})
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			var stdout, stderr bytes.Buffer
			started := time.Now()
			err := executeComputerGrant(ctx, clients, true, revokeWaitArgs("500ms", "5ms"), &stdout, &stderr, true)
			refusal := assertRevokeWaitTimeout(t, ctx, err, time.Since(started))
			if answer != "late-completed" && reads.Load() < 2 {
				t.Fatalf("reads=%d, want a slow answer followed by another read", reads.Load())
			}
			if answer == "retryable" && !strings.Contains(fmt.Sprint(refusal.APIError.Details["observation_failure"]), "read_snapshot_expired") {
				t.Fatalf("slow retryable answer lost at deadline: %#v", refusal.APIError)
			}
		})
	}
}

// Reuse the storage-wait fixture to cancel inside Read, after Do has returned
// headers. This proves mid-body cancellation rather than racing the headers.
type revokeCanceledBodyTransport struct {
	base   http.RoundTripper
	cancel context.CancelFunc
	reads  *atomic.Int32
}

func (transport revokeCanceledBodyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if strings.HasPrefix(r.URL.Path, "/v1/computers/computer-1/revocations/") && transport.reads.Add(1) > 1 {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
			Body: canceledResponseBody{cancel: transport.cancel}}, nil
	}
	return transport.base.RoundTrip(r)
}

func TestRevokeWaitCallerCancellationIsLocal(t *testing.T) {
	for _, phase := range []string{"sleep", "headers", "body"} {
		for _, jsonOutput := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/json=%t", phase, jsonOutput), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				reading := make(chan struct{})
				clients, _ := revokeWaitRequestStub(t, func(reads int32, w http.ResponseWriter, r *http.Request) {
					if reads == 1 {
						_ = json.NewEncoder(w).Encode(l1.ComputerPolicyRevocation{ComputerID: "computer-1",
							SubjectFabricID: "fabric-1", SubjectUserID: "person-1", PolicyRevision: 2,
							State: l1.ComputerPolicyRevocationPending, CreatedAt: time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)})
						return
					}
					close(reading)
					<-r.Context().Done()
				})
				if phase == "sleep" {
					var waits int
					clients.wait = func(ctx context.Context, delay time.Duration) error {
						waits++
						if waits > 1 {
							cancel()
						}
						return waitForContext(ctx, delay)
					}
				} else if phase == "body" {
					clients.l1.client.Transport = revokeCanceledBodyTransport{
						base: clients.l1.client.Transport, cancel: cancel, reads: &atomic.Int32{},
					}
				} else {
					go func() {
						select {
						case <-reading:
							cancel()
						case <-ctx.Done():
						}
					}()
				}
				var stdout, stderr bytes.Buffer
				err := executeComputerGrant(ctx, clients, jsonOutput, revokeWaitArgs("5s", "5ms"), &stdout, &stderr, true)
				writeCommandError(&stderr, err, jsonOutput)
				if !errors.Is(err, context.Canceled) || commandExitCode(err) != exitFailure ||
					!strings.Contains(stderr.String(), "revocation was applied but observation failed") {
					t.Fatalf("caller cancellation exit=%d, want local exit 1 with applied evidence: %v / %s",
						commandExitCode(err), err, stderr.String())
				}
				if jsonOutput {
					var envelope contract.ErrorResponse
					if err := json.Unmarshal(stderr.Bytes(), &envelope); err != nil ||
						envelope.Error.Code != contract.ErrorInternal || envelope.Error.Retryable {
						t.Fatalf("cancellation must be local and non-retryable: %v / %s", err, stderr.String())
					}
					var result l1.ComputerGrantMutationResult
					if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || !result.MutationApplied ||
						result.ObservationState != "failed" || result.ObservationFailure != context.Canceled.Error() ||
						result.LastObservedRevocation == nil || result.LastObservedRevocation.State != l1.ComputerPolicyRevocationPending ||
						result.LastObservedRevocation.SubjectUserID != "person-1" || result.LastObservedRevocation.PolicyRevision != 2 ||
						result.LastObservedRevocation.CreatedAt.Format(time.RFC3339) != "2026-10-11T12:00:00Z" {
						t.Fatalf("cancellation lost applied receipt or last observation: %v / %s", err, stdout.String())
					}
				} else if !strings.Contains(stdout.String(), "MUTATION APPLIED") ||
					!strings.Contains(stdout.String(), "\ttrue\tcomputer-1:fabric-1:person-1:pending:2") {
					t.Fatalf("human cancellation lost applied receipt or last observation: %s", stdout.String())
				}
			})
		}
	}
}

func revokeWaitArgs(waitTimeout, pollInterval string) []string {
	return []string{"computer-1", "person-1", "--policy-revision", "2", "--idempotency-key", "revoke-wait",
		"--wait", "--wait-timeout", waitTimeout, "--poll-interval", pollInterval}
}

func TestRevokeWaitToleratesARetryableAnswer(t *testing.T) {
	var completed int32
	withCompletedRevocation := func(int32) func(w http.ResponseWriter) {
		return func(w http.ResponseWriter) {
			_ = json.NewEncoder(w).Encode(l1.ComputerPolicyRevocation{ComputerID: "computer-1",
				SubjectFabricID: "fabric-1", SubjectUserID: "person-1", PolicyRevision: 2,
				State: l1.ComputerPolicyRevocationCompleted})
		}
	}
	retryable503Revocation := func(int32) func(w http.ResponseWriter) {
		return func(w http.ResponseWriter) { writeRetryableUnavailable(w, "read_snapshot_expired") }
	}
	t.Run("retryable 503s mid-wait keep observing", func(t *testing.T) {
		clients, reads := revokeWaitStub(t, func(reads int32) func(w http.ResponseWriter) {
			if reads < 3 {
				return func(w http.ResponseWriter) { writeRetryableUnavailable(w, "read_snapshot_expired") }
			}
			return withCompletedRevocation(completed)
		})
		var stdout, stderr bytes.Buffer
		if err := executeComputerGrant(t.Context(), clients, true, revokeWaitArgs("2s", "5ms"), &stdout, &stderr, true); err != nil {
			t.Fatalf("revoke wait with two retryable 503s: %v\n%s", err, stderr.String())
		}
		var result l1.ComputerGrantMutationResult
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || result.ObservationState != "completed" ||
			reads.Load() < 3 {
			t.Fatalf("revoke wait result = %s decode=%v reads=%d", stdout.String(), err, reads.Load())
		}
	})
	t.Run("only retryable answers until the window closes", func(t *testing.T) {
		clients, reads := revokeWaitStub(t, retryable503Revocation)
		var stdout, stderr bytes.Buffer
		err := executeComputerGrant(t.Context(), clients, true, revokeWaitArgs("500ms", "5ms"), &stdout, &stderr, true)
		var refusal *apiResponseError
		if commandExitCode(err) != exitMutationWaitTimeout || !errors.As(err, &refusal) ||
			refusal.APIError.Code != contract.ErrorRevocationWaitTimeout || reads.Load() < 2 ||
			!strings.Contains(fmt.Sprint(refusal.APIError.Details["observation_failure"]), "read_snapshot_expired") {
			t.Fatalf("retryable-only revoke wait: reads=%d exit=%d err=%v (%#v)", reads.Load(), commandExitCode(err), err, refusal)
		}
	})
}

// The response has been fully decoded when the client's deferred Close runs.
// Hold Close until the request deadline, to probe an answer already in hand
// without depending on scheduler timing between the read and deadline check.
type revokeDeadlineCloseBody struct {
	io.Reader
	ctx context.Context
}

func (body revokeDeadlineCloseBody) Close() error {
	<-body.ctx.Done()
	return nil
}

type revokeDeadlineCloseTransport struct {
	base             http.RoundTripper
	status           int
	body             string
	refuseAtDeadline bool
}

func (transport revokeDeadlineCloseTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if strings.HasPrefix(r.URL.Path, "/v1/computers/computer-1/revocations/") {
		if transport.refuseAtDeadline {
			<-r.Context().Done()
		}
		return &http.Response{StatusCode: transport.status, Header: make(http.Header),
			Body: revokeDeadlineCloseBody{Reader: strings.NewReader(transport.body), ctx: r.Context()}}, nil
	}
	return transport.base.RoundTrip(r)
}

func TestRevokeWaitAcceptsAnswerBeforeDeadlineCheck(t *testing.T) {
	for _, state := range []string{"completed", "pending"} {
		t.Run(state, func(t *testing.T) {
			clients, _ := revokeWaitStub(t, func(int32) func(http.ResponseWriter) {
				return func(http.ResponseWriter) { t.Error("probe must supply the revocation response") }
			})
			clients.l1.client.Transport = revokeDeadlineCloseTransport{
				base: clients.l1.client.Transport, status: http.StatusOK,
				body: fmt.Sprintf(`{"computer_id":"computer-1","subject_fabric_id":"fabric-1","subject_user_id":"person-1","policy_revision":2,"state":%q,"created_at":"2026-10-11T12:00:00Z"}`, state),
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			var stdout, stderr bytes.Buffer
			started := time.Now()
			err := executeComputerGrant(ctx, clients, true, revokeWaitArgs("500ms", "5ms"), &stdout, &stderr, true)
			if state == "completed" {
				var result l1.ComputerGrantMutationResult
				if err != nil || time.Since(started) > 2*time.Second ||
					json.Unmarshal(stdout.Bytes(), &result) != nil || result.ObservationState != "completed" ||
					result.LastObservedRevocation == nil || result.LastObservedRevocation.CreatedAt.Format(time.RFC3339) != "2026-10-11T12:00:00Z" {
					t.Fatalf("decoded completion must win over the deadline check: %v / %s", err, stdout.String())
				}
			} else {
				refusal := assertRevokeWaitTimeout(t, ctx, err, time.Since(started))
				last := refusal.APIError.Details["last_observed_revocation"].(*l1.ComputerPolicyRevocation)
				if last.CreatedAt.Format(time.RFC3339) != "2026-10-11T12:00:00Z" {
					t.Fatalf("pending answer must refresh the last observation before timeout: %#v", last)
				}
			}
		})
	}
}

func TestRevokeWaitTypedRefusalAtDeadline(t *testing.T) {
	clients, _ := revokeWaitStub(t, func(int32) func(http.ResponseWriter) {
		return func(http.ResponseWriter) { t.Error("probe must supply the refusal") }
	})
	clients.l1.client.Transport = revokeDeadlineCloseTransport{
		base: clients.l1.client.Transport, status: http.StatusForbidden, refuseAtDeadline: true,
		body: `{"error":{"code":"forbidden","message":"deadline refusal","retryable":false}}`,
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var stdout, stderr bytes.Buffer
	started := time.Now()
	err := executeComputerGrant(ctx, clients, true, revokeWaitArgs("500ms", "5ms"), &stdout, &stderr, true)
	writeCommandError(&stderr, err, true)
	var envelope contract.ErrorResponse
	if json.Unmarshal(stderr.Bytes(), &envelope) != nil || commandExitCode(err) != exitFailure ||
		ctx.Err() != nil || time.Since(started) > 2*time.Second ||
		envelope.Error.Code != contract.ErrorRevocationObservationFailed ||
		!strings.Contains(fmt.Sprint(envelope.Error.Details["observation_failure"]), "deadline refusal") ||
		envelope.Error.Details["mutation_applied"] != true {
		t.Fatalf("typed refusal at deadline must retain observation-failed exit 1: %v / %s", err, stderr.String())
	}
}

func TestRevokeWaitBoundsLongPollSleep(t *testing.T) {
	clients, reads := revokeWaitRequestStub(t, func(_ int32, _ http.ResponseWriter, r *http.Request) {
		// The sleep timer can win the race with context cancellation at
		// the deadline. Any read it permits must share that same deadline.
		<-r.Context().Done()
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var stdout, stderr bytes.Buffer
	started := time.Now()
	err := executeComputerGrant(ctx, clients, true, revokeWaitArgs("500ms", "5s"), &stdout, &stderr, true)
	assertRevokeWaitTimeout(t, ctx, err, time.Since(started))
	if reads.Load() > 1 {
		t.Fatalf("revocation reads=%d, want at most one deadline-bounded read", reads.Load())
	}
}
