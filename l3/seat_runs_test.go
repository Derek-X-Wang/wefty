package l3

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestSeatRunNormalizesToOrdinaryPinnedRunWithoutCredentials(t *testing.T) {
	binding := contract.SeatRunBinding{SeatID: "seat_00000000000000000000000000000001", NodeID: "node", ComputerID: "computer", ComputerJobID: "job", ComputerAttemptID: "attempt"}
	input := CreateRunInput{Actor: "cli", IdempotencyKey: "seat-run", FabricClient: true, SeatRunBinding: &binding, Request: CreateRunRequest{SeatRun: &contract.SeatRunInput{SeatID: binding.SeatID, Argv: []string{"env"}}}}
	normalized, _, _, err := normalizeCreateRun(input)
	if err != nil {
		t.Fatal(err)
	}
	p, present, err := contract.ParseSeatRunParams(normalized.Request.Params)
	if err != nil || !present {
		t.Fatalf("params=%s %v", normalized.Request.Params, err)
	}
	if p.Binding != binding || normalized.Request.Limits.MaxRuntimeSeconds != 3600 || normalized.Request.Tags[0] != contract.StableNodeTagPrefix+"node" {
		t.Fatalf("normalized=%+v params=%+v", normalized.Request, p)
	}
	store, err := OpenStore(filepath.Join(t.TempDir(), "ledger.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	record, _, err := store.CreateRun(t.Context(), input)
	if err != nil {
		t.Fatal(err)
	}
	if record.SeatID != binding.SeatID {
		t.Fatalf("Run Seat=%q", record.SeatID)
	}
	intents, err := store.pendingDispatches(t.Context())
	if err != nil || len(intents) != 1 {
		t.Fatalf("dispatch=%+v %v", intents, err)
	}
	spec := intents[0].jobSpec("run-secret")
	dispatched, present, err := contract.ParseSeatRunParams([]byte(spec.Labels[contract.LabelRunParams]))
	if err != nil || !present || dispatched.Binding != binding {
		t.Fatalf("dispatched=%+v %v", dispatched, err)
	}
	_, _, err = store.CreateRerun(t.Context(), CreateRerunInput{Actor: "cli", IdempotencyKey: "rerun-seat", SourceRunID: record.RunID})
	var refusal *Error
	if !errors.As(err, &refusal) || refusal.Code != contract.ErrorSeatRunRerun {
		t.Fatalf("rerun=%v", err)
	}
	input.SeatRunBinding = nil
	if _, _, _, err = normalizeCreateRun(input); err == nil {
		t.Fatal("Seat Run admitted without Seat credential proof")
	}
}

type seatRunHTTPJobs struct {
	JobClient
	binding contract.SeatRunBinding
}

func (c *seatRunHTTPJobs) ResolveSeatRun(_ context.Context, id, credential string) (contract.SeatRunBinding, error) {
	if id != c.binding.SeatID || credential != "seat-test-secret" {
		return contract.SeatRunBinding{}, protocolError(contract.ErrorForbidden, "invalid Seat credential")
	}
	return c.binding, nil
}
func TestSeatRunHTTPSourceProvesCredentialAndRetainsOnlyPublicBinding(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "ledger.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	binding := contract.SeatRunBinding{SeatID: "seat_00000000000000000000000000000001", NodeID: "node", ComputerID: "computer", ComputerJobID: "job", ComputerAttemptID: "attempt"}
	server := &Server{store: store, jobs: &seatRunHTTPJobs{binding: binding}, logf: func(string, ...any) {}}
	body := `{"seat_run":{"seat_id":"seat_00000000000000000000000000000001","argv":["env"]},"params":{ },"limits":{"max_runtime_seconds":7200}}`
	request := func(secret, key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/v1/runs", strings.NewReader(body))
		r.Header.Set("Idempotency-Key", key)
		r.Header.Set(contract.SeatCredentialHeader, secret)
		r = r.WithContext(context.WithValue(r.Context(), identityContextKey{}, fabric.Identity{NodeID: "caller"}))
		w := httptest.NewRecorder()
		server.createRun(w, r)
		return w
	}
	response := request("seat-test-secret", "seat-http")
	if response.Code != http.StatusCreated {
		t.Fatalf("submit=%d %s", response.Code, response.Body.String())
	}
	var accepted RunAccepted
	if err = json.Unmarshal(response.Body.Bytes(), &accepted); err != nil {
		t.Fatal(err)
	}
	record, err := store.GetRun(t.Context(), accepted.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.SeatID != binding.SeatID || record.Limits.MaxRuntimeSeconds != 7200 || strings.Contains(string(record.Params), "seat-test-secret") {
		t.Fatalf("Run=%+v", record)
	}
	if bad := request("wrong-secret", "bad-seat-http"); bad.Code != http.StatusForbidden {
		t.Fatalf("bad credential=%d %s", bad.Code, bad.Body.String())
	}
}

func TestOrdinaryRunMayKeepSeatRunNamedParameters(t *testing.T) {
	input := CreateRunInput{Actor: "cli", IdempotencyKey: "ordinary-seat-param", Request: inlineRunRequest("#!/bin/sh\nexit 0\n")}
	input.Request.Params = json.RawMessage(`{"seat_run":{"purpose":"workflow data"}}`)
	if _, _, _, err := normalizeCreateRun(input); err != nil {
		t.Fatalf("ordinary Workflow params were interpreted as Seat authority: %v", err)
	}
}

func TestSeatRunUsesClusterDefaultWithoutChangingExplicitDeadlines(t *testing.T) {
	binding := contract.SeatRunBinding{SeatID: "seat_00000000000000000000000000000001", NodeID: "node", ComputerID: "computer", ComputerJobID: "job", ComputerAttemptID: "attempt", DefaultMaxRuntimeSeconds: 7200}
	for _, seconds := range []int64{0, 900} {
		input := CreateRunInput{Actor: "cli", IdempotencyKey: "cluster-deadline", FabricClient: true, SeatRunBinding: &binding, Request: CreateRunRequest{SeatRun: &contract.SeatRunInput{SeatID: binding.SeatID, Argv: []string{"true"}, MaxRuntimeSeconds: seconds}}}
		normalized, _, _, err := normalizeCreateRun(input)
		if err != nil {
			t.Fatal(err)
		}
		expected := int(seconds)
		if expected == 0 {
			expected = 7200
		}
		if normalized.Request.Limits.MaxRuntimeSeconds != expected {
			t.Fatalf("deadline=%d want %d", normalized.Request.Limits.MaxRuntimeSeconds, expected)
		}
	}
}
