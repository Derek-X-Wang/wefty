package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestSeatRunRerunRefusedRealBinary(t *testing.T) {
	binary := buildWefty(t)
	id := "run_" + strings.Repeat("a", 32)
	for _, serverRefusal := range []bool{false, true} {
		address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" {
				params := json.RawMessage(`{"seat_run_version":1,"seat_run":{"seat_id":"seat_00000000000000000000000000000001","argv":["true"]},"seat_binding":{"seat_id":"seat_00000000000000000000000000000001","node_id":"node","computer_id":"computer","computer_job_id":"job","computer_attempt_id":"attempt"}}`)
				if serverRefusal {
					params = json.RawMessage(`{}`)
				}
				json.NewEncoder(w).Encode(contract.RunRecord{RunID: id, Params: params})
				return
			}
			if !serverRefusal {
				t.Error("Seat Run rerun reached submission")
			}
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: "seat_run_not_replayable", Message: "Seat state cannot be replayed; start a new Seat Run"}})
		})
		code, out := runWefty(t, binary, 30*time.Second, "--l3", address, "--json", "rerun", id)
		if code != 5 || !strings.Contains(out, "seat_run_not_replayable") {
			t.Fatalf("exit=%d out=%s", code, out)
		}
	}
}
