package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l3"
)

func TestCancelledRunHumanViews(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name         string
		status       contract.RunState
		cancelStatus string
		want         string
	}{
		{"settled cancellation", contract.RunFailed, "settled", "failed (cancelled)"},
		{"plain failure", contract.RunFailed, "", "failed"},
		{"pending delivery", contract.RunFailed, "pending", "failed"},
		{"refused delivery", contract.RunFailed, "refused", "failed"},
		{"legacy delivery", contract.RunFailed, "completed", "failed"},
		{"stopping after delivery", contract.RunRunning, "settled", "running (cancelling)"},
		{"stopping before delivery", contract.RunRunning, "pending", "running (cancelling)"},
		{"queued and stopping", contract.RunQueued, "pending", "queued (cancelling)"},
		{"refused while running", contract.RunRunning, "refused", "running (cancel refused)"},
		{"running without cancellation", contract.RunRunning, "", "running"},
		{"already succeeded", contract.RunSucceeded, "settled", "succeeded"},
	} {
		t.Run(test.name, func(t *testing.T) {
			run := contract.RunRecord{
				RunID: "run-a", Status: test.status, CancelStatus: test.cancelStatus,
				// A reason mentioning cancellation must not substitute for the field.
				FailureReason: "the command canceled its own operation",
			}
			clients := ledgerStub(t, map[string]any{
				"/v1/runs/run-a":         run,
				"/v1/runs/run-a/lineage": l3.RunLineage{RunID: run.RunID},
				"/v1/runs":               l3.ComputerRunPage{Runs: []contract.RunRecord{run}},
			})
			for _, view := range []struct {
				name string
				args []string
			}{
				{"inspect", []string{"inspect", run.RunID}},
				{"runs list", []string{"runs", "list"}},
				{"runs list origin", []string{"runs", "list", "--origin", "computer:computer-a"}},
			} {
				t.Run(view.name, func(t *testing.T) {
					var out bytes.Buffer
					if err := execute(t.Context(), clients, false, view.args, &out, &bytes.Buffer{}); err != nil {
						t.Fatal(err)
					}
					lines := strings.Split(out.String(), "\n")
					if len(lines) < 2 {
						t.Fatalf("missing run row: %s", out.String())
					}
					// Table spacing and columns differ, but the status cell must agree.
					row := strings.Join(strings.Fields(lines[1]), " ")
					if !strings.Contains(row, " "+test.want+" ") ||
						(!strings.Contains(test.want, "(") && strings.Contains(row, "(")) {
						t.Fatalf("status must be %q in row %q", test.want, row)
					}
				})
			}
		})
	}
}

func TestCancelledRunJSONKeepsFailedStatus(t *testing.T) {
	t.Parallel()

	run := contract.RunRecord{RunID: "run-a", Status: contract.RunFailed, CancelStatus: "settled", FailureReason: "canceled"}
	clients := ledgerStub(t, map[string]any{
		"/v1/runs/run-a":         run,
		"/v1/runs/run-a/lineage": l3.RunLineage{RunID: run.RunID},
		"/v1/runs":               l3.ComputerRunPage{Runs: []contract.RunRecord{run}},
	})
	for _, args := range [][]string{
		{"inspect", run.RunID},
		{"runs", "list"},
		{"runs", "list", "--origin", "computer:computer-a"},
	} {
		var out bytes.Buffer
		if err := execute(t.Context(), clients, true, args, &out, &bytes.Buffer{}); err != nil {
			t.Fatal(err)
		}
		var body struct {
			Run  contract.RunRecord   `json:"run"`
			Runs []contract.RunRecord `json:"runs"`
		}
		if err := json.Unmarshal(out.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if len(body.Runs) != 1 || body.Runs[0].Status != contract.RunFailed || strings.Contains(out.String(), "(cancelled)") {
			t.Fatalf("%v changed JSON status: %s", args, out.String())
		}
		if len(args) == 2 && args[0] == "runs" {
			if strings.Contains(out.String(), "cancel_status") {
				t.Fatalf("general listing JSON gained cancellation fields: %s", out.String())
			}
		} else if body.Runs[0].CancelStatus != "settled" {
			t.Fatalf("%v lost cancellation evidence: %s", args, out.String())
		}
		if args[0] == "inspect" && (body.Run.Status != contract.RunFailed || body.Run.CancelStatus != "settled") {
			t.Fatalf("inspect changed root JSON: %s", out.String())
		}
	}
}

// TestCancelRunTextSaysTheCancelIsUnderway is #1678: an accepted cancel of a
// live Run printed its plain "running" state, which read as if the cancel had
// done nothing. The text names the stopping state; JSON keeps the ledger state
// and its cancel_status unchanged.
func TestCancelRunTextSaysTheCancelIsUnderway(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name         string
		status       contract.RunState
		cancelStatus string
		want         string
	}{
		{"delivered, still stopping", contract.RunRunning, "settled", "running (cancelling)"},
		{"delivery pending", contract.RunRunning, "pending", "running (cancelling)"},
		{"already settled", contract.RunFailed, "settled", "failed (cancelled)"},
		{"finished before cancel", contract.RunSucceeded, "settled", "succeeded"},
		{"refused", contract.RunRunning, "refused", "running (cancel refused)"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			run := contract.RunRecord{RunID: "run_a", Status: test.status, CancelStatus: test.cancelStatus}
			if test.cancelStatus == "refused" {
				run.CancelReason = "permission_denied: not the submitter"
			}
			clients := ledgerStub(t, map[string]any{"/v1/runs/run_a/cancel": run})

			var out bytes.Buffer
			if err := execute(t.Context(), clients, false, []string{"cancel", run.RunID}, &out, &bytes.Buffer{}); err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(out.String(), "\n")
			if len(lines) < 2 || strings.Split(lines[1], "\t")[1] != test.want ||
				strings.Split(lines[1], "\t")[2] != run.CancelReason {
				t.Fatalf("status must be %q, reason %q in %q", test.want, run.CancelReason, out.String())
			}

			out.Reset()
			if err := execute(t.Context(), clients, true, []string{"cancel", run.RunID}, &out, &bytes.Buffer{}); err != nil {
				t.Fatal(err)
			}
			var body contract.RunRecord
			if err := json.Unmarshal(out.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body.Status != test.status || body.CancelStatus != test.cancelStatus {
				t.Fatalf("JSON changed the ledger state: %s", out.String())
			}
		})
	}
}

// TestRunsListCapsLiveCancellationReads: live rows share the queued-run cap,
// so a long live listing cannot spend the read budget before a failed row
// further down shows its settled cancellation.
func TestRunsListCapsLiveCancellationReads(t *testing.T) {
	t.Parallel()

	records := map[string]contract.RunRecord{}
	var page l3.ComputerRunPage
	for i := range maxAnnotatedQueuedRuns + 5 {
		run := contract.RunRecord{RunID: fmt.Sprintf("run-live-%02d", i), Status: contract.RunRunning, CancelStatus: "pending"}
		records["/v1/runs/"+run.RunID] = run
		page.Runs = append(page.Runs, run)
	}
	failed := contract.RunRecord{RunID: "run-failed", Status: contract.RunFailed, CancelStatus: "settled"}
	records["/v1/runs/"+failed.RunID] = failed
	page.Runs = append(page.Runs, failed)

	var mu sync.Mutex
	reads := 0
	routes := map[string]any{"/v1/runs": page}
	for path, record := range records {
		routes[path] = record
	}
	clients := ledgerStub(t, routes)
	transport := clients.l3.client.Transport
	clients.l3.client = &http.Client{Transport: countingTransport(func(r *http.Request) (*http.Response, error) {
		if _, ok := records[r.URL.Path]; ok && strings.HasPrefix(r.URL.Path, "/v1/runs/run-live-") {
			mu.Lock()
			reads++
			mu.Unlock()
		}
		return transport.RoundTrip(r)
	})}

	var out bytes.Buffer
	if err := execute(t.Context(), clients, false, []string{"runs", "list"}, &out, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if reads != maxAnnotatedQueuedRuns {
		t.Fatalf("live record reads=%d, want %d", reads, maxAnnotatedQueuedRuns)
	}
	if strings.Count(out.String(), "running (cancelling)") != maxAnnotatedQueuedRuns || !strings.Contains(out.String(), "failed (cancelled)") {
		t.Fatalf("listing:\n%s", out.String())
	}
}

type countingTransport func(*http.Request) (*http.Response, error)

func (f countingTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
