package main

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestSeatScreenPersonDrivingTypedExitFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	address := startStubLedger(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: "person_driving", Message: "a person is driving"}})
	})
	code, output := runWefty(t, binary, 30*time.Second, "--json", "--l1", address, "computers", "list")
	var failure contract.ErrorResponse
	if err := json.Unmarshal([]byte(output), &failure); err != nil || code != exitConflict || failure.Error.Code != "person_driving" || failure.Error.Message != "a person is driving" {
		t.Fatalf("exit=%d response=%s decode=%v", code, output, err)
	}
}
