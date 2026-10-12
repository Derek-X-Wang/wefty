package agent

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Replay returns errors to inline shutdown as well as the recovery scheduler.
// A failed receipt must not be classified as an L1 delivery response there.
func TestCompletionDeliveryPersistenceFailureIsLocal(t *testing.T) {
	t.Run("replay/read", func(t *testing.T) {
		client, stop := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Error("local read failure reached L1")
		}), time.Second)
		defer stop()
		defer client.Close()
		outbox, err := newEvidenceOutbox(t.TempDir(), "local-read-node", 1024, systemClock{}, 8, time.Hour, time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		defer outbox.Close()
		storeRecoveryCompletion(t, outbox, "local-read")
		if err := outbox.spool.db.Close(); err != nil {
			t.Fatal(err)
		}
		err = outbox.recoverCompletion(t.Context(), client, logSpoolAttempt{attemptID: "local-read"})
		if err == nil || classifyAgentProtocolError(err).destination != errorDestinationUnclassified {
			t.Fatalf("local completion read failure became an L1 verdict: %v", err)
		}
	})
	for _, live := range []bool{false, true} {
		for _, code := range []contract.ErrorCode{"", contract.ErrorLeaseExpired, contract.ErrorAttemptMismatch, contract.ErrorConflict} {
			if live && code == contract.ErrorConflict {
				continue
			} // Live body refusal hands off without a mutation.
			name := "replay/" + string(code)
			if live {
				name = "live/" + string(code)
			}
			t.Run(name, func(t *testing.T) {
				client, stop := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					if code != "" {
						w.WriteHeader(http.StatusConflict)
						_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: code}})
						return
					}
					_ = json.NewEncoder(w).Encode(l1.Job{})
				}), time.Second)
				defer stop()
				defer client.Close()
				outbox, err := newEvidenceOutbox(t.TempDir(), "local-failure-node", 1024, systemClock{}, 8, time.Hour, time.Millisecond)
				if err != nil {
					t.Fatal(err)
				}
				defer outbox.Close()
				claim := spoolTestClaim("local-failure")
				storeRecoveryCompletion(t, outbox, claim.Lease.AttemptID)
				// Fail the production-called durable effects after the HTTP response.
				for _, statement := range []string{
					`CREATE TRIGGER fail_delivery BEFORE INSERT ON spool_completion_receipts BEGIN SELECT RAISE(FAIL, 'receipt disk failure'); END`,
					`CREATE TRIGGER fail_sealing BEFORE UPDATE ON spool_attempts BEGIN SELECT RAISE(FAIL, 'seal disk failure'); END`,
				} {
					if _, err := outbox.spool.db.ExecContext(t.Context(), statement); err != nil {
						t.Fatal(err)
					}
				}
				if live {
					lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{client: client, outbox: outbox, clock: systemClock{}, completionRetry: time.Millisecond})
					failure := lifecycle.completeWithRetry(t.Context(), claim, l1.CompletionRequest{FencingToken: claim.Lease.FencingToken})
					err = failure.err
					if failure.destination != errorDestinationUnclassified {
						t.Fatalf("local failure became live destination %v: %v", failure.destination, err)
					}
				} else {
					err = outbox.recoverCompletion(t.Context(), client, logSpoolAttempt{jobID: claim.Job.JobID, attemptID: claim.Lease.AttemptID, fencingToken: claim.Lease.FencingToken})
				}
				if err == nil {
					t.Fatal("local persistence failure disappeared")
				}
				if got := classifyAgentProtocolError(err).destination; got != errorDestinationUnclassified {
					t.Fatalf("local persistence failure became L1 destination %v: %v", got, err)
				}
				if got := protocolErrorCode(err); got != "" {
					t.Fatalf("local persistence failure carried L1 verdict %s", got)
				}
				if receipt := outbox.spool.inspectCompletion(t.Context(), claim.Lease.AttemptID); receipt.State != "durable_completion" {
					t.Fatalf("failed persistence changed retained completion: %+v", receipt)
				}
			})
		}
	}
}
