package contract

import (
	"encoding/json"
	"testing"
)

func TestSeatComputerMismatchSerializedValue(t *testing.T) {
	body, err := json.Marshal(ErrorResponse{Error: APIError{Code: ErrorForbidden, Message: SeatComputerMismatchMessage}})
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"error":{"code":"forbidden","message":"Seat belongs to another Computer","retryable":false}}`
	if string(body) != want {
		t.Fatalf("serialized refusal=%s want=%s", body, want)
	}
}
