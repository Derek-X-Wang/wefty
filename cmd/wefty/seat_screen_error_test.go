package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestSeatScreenMissingCredentialTypedFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("TMPDIR", t.TempDir())
	id := "seat_" + strings.Repeat("d", 32)
	address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("screen command without a credential reached L1: %s %s", r.Method, r.URL.Path)
	})
	code, out := runWefty(t, binary, 30*time.Second, "--l1", address, "--json", "seat", "click", id, "1", "2")
	assertSeatCredentialUnavailable(t, code, out, id)
}

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
