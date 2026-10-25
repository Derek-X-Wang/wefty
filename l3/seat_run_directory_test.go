package l3

import (
	"encoding/json"
	"errors"
	"github.com/Derek-X-Wang/wefty/contract"
	"path/filepath"
	"testing"
)

func TestSeatRunWorkingDirectoryParamsAndIdempotency(t *testing.T) {
	binding := contract.SeatRunBinding{SeatID: "seat_00000000000000000000000000000001", NodeID: "node", ComputerID: "computer", ComputerJobID: "job", ComputerAttemptID: "attempt"}
	store, err := OpenStore(filepath.Join(t.TempDir(), "ledger.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	source := func(directory string) *contract.SeatRunInput {
		raw, _ := json.Marshal(map[string]any{"seat_id": binding.SeatID, "argv": []string{"pwd"}, "working_directory": directory})
		var v contract.SeatRunInput
		if err := json.Unmarshal(raw, &v); err != nil {
			t.Fatal(err)
		}
		return &v
	}
	input := CreateRunInput{Actor: "cli", IdempotencyKey: "directory", FabricClient: true, SeatRunBinding: &binding, Request: CreateRunRequest{SeatRun: source("work/pkg")}}
	record, _, err := store.CreateRun(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}
	var params map[string]any
	_ = json.Unmarshal(record.Params, &params)
	if params["seat_run_version"] != float64(2) {
		t.Fatalf("directory Run did not fence older nodes: %s", record.Params)
	}
	if params["seat_run"].(map[string]any)["working_directory"] != "work/pkg" {
		t.Fatalf("folder lost: %s", record.Params)
	}
	input.Request.SeatRun = source("work/other")
	_, _, err = store.CreateRun(t.Context(), input)
	var refusal *Error
	if !errors.As(err, &refusal) || refusal.Code != contract.ErrorIdempotencyConflict {
		t.Fatalf("changed directory refusal: %v", err)
	}
	input.IdempotencyKey = "invalid"
	input.Request.SeatRun = source("work/../escape")
	_, _, err = store.CreateRun(t.Context(), input)
	if !errors.As(err, &refusal) || refusal.Code != contract.ErrorInvalidRequest {
		t.Fatalf("unclean directory refusal: %v", err)
	}
}
