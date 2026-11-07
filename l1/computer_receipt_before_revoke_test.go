package l1

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// #1552: restart, storage reset and reimage read their owed revocation after
// the commit. When that read expires, the committed-unread answer must still
// carry the receipt, or the CLI cannot resend with the same key.
func TestAuthorityLossReadExpiryKeepsComputerReceipt(t *testing.T) {
	for _, verb := range []string{"restart", "reset", "reimage"} {
		t.Run(verb, func(t *testing.T) {
			h, _, node, _ := computerCompletionHarness(t)
			computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "receipt-" + verb, Spec: computerCapabilityJobSpec("computer:receipt-" + verb), Actor: "operator"})
			if err != nil {
				t.Fatal(err)
			}
			if verb == "restart" {
				// Restart starts only from stopped.
				if computer, err = h.store.SetComputerDesiredState(t.Context(), computer.ComputerID, computerDesiredRequest(computer, contract.ServiceDesiredStopped, "operator")); err != nil {
					t.Fatal(err)
				}
			} else {
				startComputerAttempt(t, h, node, nil)
			}
			computer = mustGetComputer(t, h, computer.ComputerID)
			h.stopServer()
			called := false
			h.server.computerTokenRevoker = recordingComputerTokenRevoker{revoke: func(context.Context, ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error) {
				called = true
				return contract.ComputerTokenRevocationReceipt{}, nil
			}}
			var payload any
			var handler http.HandlerFunc
			switch verb {
			case "restart":
				payload = ComputerRestartRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "receipt-restart"}
				handler = h.server.restartComputer
			case "reset":
				payload = ComputerStorageResetRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "receipt-reset", TerminateSessions: true}
				handler = h.server.resetComputerStorage
			case "reimage":
				payload = ComputerReimageRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), Image: reimageTarget('b'), IdempotencyKey: "receipt-reimage", TerminateSessions: true}
				handler = h.server.reimageComputer
			}
			encoded, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			// Reimage reads before it commits, so expire reads only from the
			// handler's first header access. The owed-revocation read follows the
			// receipt headers, so it expires while the keyed write has committed.
			identity := fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}}
			r := httptest.NewRequest(http.MethodPost, "/v1/computers/"+computer.ComputerID+"/"+verb, strings.NewReader(string(encoded))).
				WithContext(context.WithValue(t.Context(), identityContextKey{}, identity))
			r.SetPathValue("computer_id", computer.ComputerID)
			w := httptest.NewRecorder()
			handler(receiptExpiryResponseWriter{w, h.store}, r)
			var response contract.ErrorResponse
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatalf("status=%d body=%s: %v", w.Code, w.Body.String(), err)
			}
			api := response.Error
			if w.Code != http.StatusServiceUnavailable || api.Code != contract.ErrorUnavailable || !api.Retryable ||
				api.Details["mutation_applied"] != true || api.Details["reason"] != "read_snapshot_post_change_failed" || called {
				t.Fatalf("status=%d body=%s revoker_called=%t", w.Code, w.Body.String(), called)
			}
			if got := w.Result().Header.Get("Mutation-Applied"); got != "true" {
				t.Fatalf("Mutation-Applied=%q, want true; headers=%v", got, w.Result().Header)
			}
		})
	}
}
