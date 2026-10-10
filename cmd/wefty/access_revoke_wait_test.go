package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

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
			revocationAnswer(revocationReads.Add(1))(w)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(contract.APIError{Code: contract.ErrorNotFound, Message: r.URL.Path})
		}
	}))
	t.Cleanup(server.Close)
	client := waitStubClient(t, server)
	return &apiClients{l1: client, wait: waitForContext}, &revocationReads
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
		if err := executeComputerGrant(t.Context(), clients, true, revokeWaitArgs("2s", "20ms"), &stdout, &stderr, true); err != nil {
			t.Fatalf("revoke wait with two retryable 503s: %v\n%s", err, stderr.String())
		}
		var result l1.ComputerGrantMutationResult
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || result.ObservationState != "completed" ||
			reads.Load() != 3 {
			t.Fatalf("revoke wait result = %s decode=%v reads=%d", stdout.String(), err, reads.Load())
		}
	})
	t.Run("only retryable answers until the window closes", func(t *testing.T) {
		clients, reads := revokeWaitStub(t, retryable503Revocation)
		var stdout, stderr bytes.Buffer
		err := executeComputerGrant(t.Context(), clients, true, revokeWaitArgs("1s", "20ms"), &stdout, &stderr, true)
		var refusal *apiResponseError
		if commandExitCode(err) != exitMutationWaitTimeout || !errors.As(err, &refusal) ||
			refusal.APIError.Code != contract.ErrorRevocationWaitTimeout || reads.Load() < 2 ||
			!strings.Contains(fmt.Sprint(refusal.APIError.Details["observation_failure"]), "read_snapshot_expired") {
			t.Fatalf("retryable-only revoke wait: reads=%d exit=%d err=%v (%#v)", reads.Load(), commandExitCode(err), err, refusal)
		}
	})
}
