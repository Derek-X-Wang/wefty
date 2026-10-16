package main

import (
	"bytes"
	"encoding/json"
	"strings"
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
		{"still running", contract.RunRunning, "settled", "running"},
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
						(test.want != "failed (cancelled)" && strings.Contains(row, "(cancelled)")) {
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
