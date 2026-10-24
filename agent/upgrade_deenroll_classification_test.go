package agent

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestDeEnrollmentDefinitiveRefusalClassification(t *testing.T) {
	request := l1.DeEnrollNodeGenerationRequest{OperationID: "operation", PrimaryGenerationID: "primary", PrimaryBootSessionID: "boot", RuntimesReaped: true, EvidenceSettled: true}
	for _, tc := range []struct {
		status          int
		retryable, want bool
	}{
		{400, false, false}, {401, false, false}, {403, false, false}, {404, false, false}, {409, false, false},
		{408, false, false}, {429, false, false}, {503, false, false}, {409, true, false},
	} {
		t.Run(fmt.Sprintf("status=%d_retryable=%t", tc.status, tc.retryable), func(t *testing.T) {
			err := fmt.Errorf("wrapped: %w", &ProtocolError{StatusCode: tc.status, APIError: contract.APIError{Code: contract.ErrorConflict, Retryable: tc.retryable}})
			if got := DefinitiveDeEnrollmentRefusal(err, "node", request); got != tc.want {
				t.Fatalf("definitive=%t want %t", got, tc.want)
			}
		})
	}
	if DefinitiveDeEnrollmentRefusal(errors.New("connection reset"), "node", request) {
		t.Fatal("transport error became definitive")
	}
}

func TestDeEnrollmentDefinitiveRefusalRequiresExactBinding(t *testing.T) {
	request := l1.DeEnrollNodeGenerationRequest{OperationID: "operation", PrimaryGenerationID: "primary", PrimaryBootSessionID: "boot", RuntimesReaped: true, EvidenceSettled: true}
	for _, fault := range []string{"none", "node", "operation", "payload", "missing", "retryable", "status", "code"} {
		t.Run(fault, func(t *testing.T) {
			response := &ProtocolError{StatusCode: 409, APIError: contract.APIError{Code: contract.ErrorDeEnrollmentNotCommitted, Details: map[string]any{"node_id": "node", "operation_id": "operation", "request_sha256": l1.DeEnrollmentRequestSHA256(request)}}}
			switch fault {
			case "node":
				response.APIError.Details["node_id"] = "other"
			case "operation":
				response.APIError.Details["operation_id"] = "other"
			case "payload":
				response.APIError.Details["request_sha256"] = "sha256:other"
			case "missing":
				response.APIError.Details = nil
			case "retryable":
				response.APIError.Retryable = true
			case "status":
				response.StatusCode = 401
			case "code":
				response.APIError.Code = contract.ErrorConflict
			}
			if got := DefinitiveDeEnrollmentRefusal(fmt.Errorf("wrapped: %w", response), "node", request); got != (fault == "none") {
				t.Fatal("unverified rejection accepted", fault, got)
			}
		})
	}
}
