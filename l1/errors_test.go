package l1

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestWriteErrorPublishesTruthfulRetryability(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		wantStatus    int
		wantCode      contract.ErrorCode
		wantRetryable bool
	}{
		{name: "internal", err: internalError(errors.New("database unavailable"), "read node"), wantStatus: http.StatusInternalServerError, wantCode: contract.ErrorInternal, wantRetryable: true},
		{name: "principal forbidden", err: protocolError(contract.ErrorPrincipalForbidden, "wrong principal"), wantStatus: http.StatusForbidden, wantCode: contract.ErrorPrincipalForbidden},
		{name: "identity bound", err: protocolError(contract.ErrorIdentityBound, "identity bound"), wantStatus: http.StatusForbidden, wantCode: contract.ErrorIdentityBound},
		{name: "node not registered", err: protocolError(contract.ErrorNodeNotRegistered, "missing node"), wantStatus: http.StatusConflict, wantCode: contract.ErrorNodeNotRegistered},
		{name: "node dead", err: protocolError(contract.ErrorNodeDead, "dead node"), wantStatus: http.StatusConflict, wantCode: contract.ErrorNodeDead},
		{name: "node draining", err: protocolError(contract.ErrorNodeDraining, "draining node"), wantStatus: http.StatusConflict, wantCode: contract.ErrorNodeDraining},
		{name: "node session replaced", err: protocolError(contract.ErrorNodeSessionReplaced, "replaced session"), wantStatus: http.StatusConflict, wantCode: contract.ErrorNodeSessionReplaced},
		{name: "attempt not found", err: protocolError(contract.ErrorAttemptNotFound, "missing attempt"), wantStatus: http.StatusNotFound, wantCode: contract.ErrorAttemptNotFound},
		{name: "attempt not owned", err: protocolError(contract.ErrorAttemptNotOwned, "unowned attempt"), wantStatus: http.StatusForbidden, wantCode: contract.ErrorAttemptNotOwned},
		{name: "lease expired", err: protocolError(contract.ErrorLeaseExpired, "expired lease"), wantStatus: http.StatusConflict, wantCode: contract.ErrorLeaseExpired},
		{name: "stale fence", err: protocolError(contract.ErrorStaleFence, "stale fence"), wantStatus: http.StatusConflict, wantCode: contract.ErrorStaleFence},
		{name: "attempt mismatch", err: protocolError(contract.ErrorAttemptMismatch, "attempt mismatch"), wantStatus: http.StatusConflict, wantCode: contract.ErrorAttemptMismatch},
		{name: "stale Computer intent", err: protocolError(contract.ErrorStaleIntentRevision, "stale Computer intent"), wantStatus: http.StatusConflict, wantCode: contract.ErrorStaleIntentRevision},
		{name: "Computer storage changed", err: protocolError(contract.ErrorStorageReferenceConflict, "storage changed"), wantStatus: http.StatusConflict, wantCode: contract.ErrorStorageReferenceConflict},
		{name: "Computer resource required", err: protocolError(contract.ErrorComputerResourceRequired, "Computer resource required"), wantStatus: http.StatusConflict, wantCode: contract.ErrorComputerResourceRequired},
		{name: "capacity exhausted", err: protocolErrorWithDetails(contract.ErrorCapacityExhausted, map[string]any{"node_id": "node-1", "occupancy": 2, "capacity": 2}, "full"), wantStatus: http.StatusConflict, wantCode: contract.ErrorCapacityExhausted, wantRetryable: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			writeError(recorder, test.err)
			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, test.wantStatus)
			}
			var response contract.ErrorResponse
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if response.Error.Code != test.wantCode || response.Error.Retryable != test.wantRetryable {
				t.Fatalf("error = %#v, want code %q retryable %t", response.Error, test.wantCode, test.wantRetryable)
			}
		})
	}
}
