//go:build service_acceptance

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestExecutionDiagnosticsRenderingServiceAcceptance(t *testing.T) {
	exitCode := 0
	apiError := &contract.APIError{
		Code: contract.ErrorCapacityExhausted, Message: "eligible node is full", Retryable: true,
		Details: map[string]any{"node_id": "node-1"}, RequestID: "request-1",
	}
	inspection := runInspection{
		Run:  contract.RunRecord{RunID: "run-1", L1JobID: "job-1", Status: contract.RunQueued},
		Runs: []contract.RunRecord{{RunID: "run-1", L1JobID: "job-1", Status: contract.RunQueued}},
		Execution: &l3.RunExecution{
			RunID: "run-1", L1JobID: "job-1", DispatchAttempts: 2, DispatchError: apiError,
			Job: &l1.Job{JobID: "job-1", State: contract.JobFailed, Attempts: []l1.Attempt{{
				AttemptID: "attempt-1", NodeID: "node-1", State: contract.AttemptLost,
				LateResult: &l1.LateResultEvidence{
					Kind: l1.LateResultObservation, Late: true,
					Result: &l1.ProcessResult{ExitCode: &exitCode},
				},
			}}},
		},
	}
	var human bytes.Buffer
	if err := writeRunInspection(&human, inspection); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"L1 JOB ID", "job-1", "DISPATCH ATTEMPTS", "2", "request_id=request-1",
		`details={"node_id":"node-1"}`, "lost — late evidence: exit 0",
	} {
		if !strings.Contains(human.String(), want) {
			t.Fatalf("human execution diagnostics missing %q:\n%s", want, human.String())
		}
	}

	payload, err := json.Marshal(contract.ErrorResponse{Error: *apiError})
	if err != nil {
		t.Fatal(err)
	}
	client := &apiClient{name: "L3", client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusConflict, Body: io.NopCloser(bytes.NewReader(payload)), Header: make(http.Header),
		}, nil
	})}}
	err = client.do(context.Background(), http.MethodGet, "/v1/runs/missing", nil, nil, nil, http.StatusOK)
	var responseErr *apiResponseError
	if !errors.As(err, &responseErr) {
		t.Fatalf("decoded CLI error = %T %v, want apiResponseError", err, err)
	}
	for _, want := range []string{"retryable=true", "request_id=request-1", `details={"node_id":"node-1"}`} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("human CLI error missing %q: %v", want, err)
		}
	}

	var structured bytes.Buffer
	writeCommandError(&structured, err, true)
	var response contract.ErrorResponse
	if err := json.Unmarshal(structured.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error.Code != apiError.Code || !response.Error.Retryable || response.Error.RequestID != apiError.RequestID ||
		response.Error.Details["node_id"] != "node-1" {
		t.Fatalf("structured CLI error = %#v", response.Error)
	}
}
