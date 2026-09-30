package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

// ledgerStub serves fixed L3 answers by path, so a command's reads are real
// HTTP round trips through its own client.
func ledgerStub(t *testing.T, routes map[string]any) *apiClients {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := routes[r.URL.Path]
		if !ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorNotFound, Message: r.URL.Path}})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(server.Close)
	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &redirectingTransport{target: target, inner: server.Client().Transport}}
	return &apiClients{
		l1: &apiClient{name: "L1", flag: "l1", address: "stub", client: client},
		l3: &apiClient{name: "L3", flag: "l3", address: "stub", client: client},
	}
}

// TestWaitNamesWhyARunFailed is #604 item 3: "failed" alone sent a reader to
// --execution, and a missing required envelope was not shown anywhere.
func TestWaitNamesWhyARunFailed(t *testing.T) {
	t.Parallel()

	recorded := ledgerStub(t, map[string]any{
		"/v1/runs/run-a": contract.RunRecord{RunID: "run-a", Status: contract.RunFailed,
			FailureReason: "the job exited 0 without reporting the envelope --required-envelope requires"},
	})
	var out bytes.Buffer
	err := executeWait(t.Context(), recorded, false, []string{"run-a"}, &out, &bytes.Buffer{})
	if code := commandExitCode(err); code != exitRunFailed {
		t.Fatalf("exit %d (%v), want %d", code, err, exitRunFailed)
	}
	if strings.TrimSpace(out.String()) != string(contract.RunFailed) {
		t.Fatalf("stdout = %q; the bare status is a script's contract", out.String())
	}
	if !strings.Contains(err.Error(), "run run-a failed: the job exited 0 without reporting the envelope") {
		t.Fatalf("error = %q", err.Error())
	}

	// A run failed before the ledger recorded reasons: the L1 job's own
	// evidence is read once instead.
	legacy := ledgerStub(t, map[string]any{
		"/v1/runs/run-b": contract.RunRecord{RunID: "run-b", Status: contract.RunFailed},
		"/v1/runs/run-b/execution": l3.RunExecution{RunID: "run-b", L1JobID: "job-b", Job: &l1.Job{
			JobID: "job-b", State: contract.JobFailed,
			Attempts: []l1.Attempt{{AttemptID: "a", NodeID: "n", State: contract.AttemptFailed,
				Result: &l1.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}}},
		}},
	})
	err = executeWait(t.Context(), legacy, false, []string{"run-b"}, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "run run-b failed: signal terminated (agent)") {
		t.Fatalf("legacy error = %v", err)
	}

	// Nothing to read says so, rather than inventing a cause.
	unknown := ledgerStub(t, map[string]any{
		"/v1/runs/run-c": contract.RunRecord{RunID: "run-c", Status: contract.RunFailed},
	})
	err = executeWait(t.Context(), unknown, false, []string{"run-c"}, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "no reason was recorded") {
		t.Fatalf("unknown error = %v", err)
	}
}

func TestInspectPrintsTheFailureReason(t *testing.T) {
	t.Parallel()

	failed := contract.RunRecord{RunID: "run-a", Status: contract.RunFailed, FailureReason: "exit 3"}
	clients := ledgerStub(t, map[string]any{
		"/v1/runs/run-a":         failed,
		"/v1/runs/run-a/lineage": l3.RunLineage{RunID: "run-a"},
	})
	var out bytes.Buffer
	if err := executeInspect(t.Context(), clients, false, []string{"run-a"}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "failed: run-a: exit 3") {
		t.Fatalf("inspect does not say why the run failed:\n%s", out.String())
	}
	out.Reset()
	if err := executeInspect(t.Context(), clients, true, []string{"run-a"}, &out); err != nil {
		t.Fatal(err)
	}
	var inspection runInspection
	if err := json.Unmarshal(out.Bytes(), &inspection); err != nil {
		t.Fatal(err)
	}
	if inspection.FailureReason != "exit 3" || inspection.Runs[0].FailureReason != "exit 3" {
		t.Fatalf("inspect --json failure = %q / %q", inspection.FailureReason, inspection.Runs[0].FailureReason)
	}
}

// stallingExecutionLedger answers the run read with a failed run that has no
// recorded reason, and never answers the execution read the legacy fallback
// makes.
func stallingExecutionLedger(t *testing.T) *apiClients {
	t.Helper()
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/execution") {
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(contract.RunRecord{RunID: "run-legacy", Status: contract.RunFailed})
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })
	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &redirectingTransport{target: target, inner: server.Client().Transport}}
	return &apiClients{l3: &apiClient{name: "L3", flag: "l3", address: "stub", client: client}}
}

// TestWaitBoundsTheLegacyReasonLookup is the #604 review P2: the fallback read
// for a run failed before reasons were recorded used the caller's context, so
// a stalled /execution held `wait --timeout` open forever. It is bounded by
// the wait's deadline and by its own cap, and a lookup that does not answer
// leaves the reason out without changing the outcome.
//
// Not parallel: the no-deadline case shortens the lookup cap, a package
// variable.
func TestWaitBoundsTheLegacyReasonLookup(t *testing.T) {
	previous := failureReasonLookupBudget
	t.Cleanup(func() { failureReasonLookupBudget = previous })

	for name, test := range map[string]struct {
		args   []string
		budget time.Duration
	}{
		"bounded by --timeout": {args: []string{"run-legacy", "--timeout", "1s"}, budget: time.Hour},
		"bounded by the cap":   {args: []string{"run-legacy"}, budget: 300 * time.Millisecond},
	} {
		t.Run(name, func(t *testing.T) {
			failureReasonLookupBudget = test.budget
			var out bytes.Buffer
			started := time.Now()
			err := executeWait(t.Context(), stallingExecutionLedger(t), false, test.args, &out, &bytes.Buffer{})
			if elapsed := time.Since(started); elapsed > 5*time.Second {
				t.Fatalf("a stalled reason lookup held wait for %s", elapsed)
			}
			if code := commandExitCode(err); code != exitRunFailed {
				t.Fatalf("exit %d (%v), want %d: the outcome must not change", code, err, exitRunFailed)
			}
			if strings.TrimSpace(out.String()) != string(contract.RunFailed) ||
				!strings.Contains(err.Error(), "no reason was recorded") {
				t.Fatalf("stdout %q, error %q", out.String(), err.Error())
			}
		})
	}
}
