package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l3"
)

var listNow = time.Date(2026, 9, 17, 12, 30, 0, 0, time.UTC)

func TestRunListingTableShowsTheCurrentStepAndAge(t *testing.T) {
	t.Parallel()

	started := listNow.Add(-90 * time.Second)
	finished := listNow.Add(-30 * time.Second)
	page := l3.RunListPage{Runs: []l3.RunSummary{
		{
			RunID: "run-running", Status: contract.RunRunning,
			Trigger: contract.Trigger{Type: "manual"}, CurrentStep: "gates",
			CreatedAt: started, StartedAt: &started,
		},
		{
			RunID: "run-done", Status: contract.RunSucceeded,
			Trigger: contract.Trigger{Type: "chain"},
			// A finished run reported no step, which is a real answer rather
			// than missing data.
			CreatedAt: started, StartedAt: &started, FinishedAt: &finished,
		},
	}}
	var out strings.Builder
	if err := writeRunListing(&out, page, listNow); err != nil {
		t.Fatal(err)
	}
	rendered := out.String()
	for _, want := range []string{"RUN ID", "STATUS", "TRIGGER", "AGE", "STEP", "run-running", "gates", "1m30s"} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("listing is missing %q:\n%s", want, rendered)
		}
	}
	// A finished run's age is how long it took, not how long ago it was.
	if !strings.Contains(rendered, "1m0s") {
		t.Fatalf("a finished run's age is not its duration:\n%s", rendered)
	}
	if !strings.Contains(rendered, "-") {
		t.Fatalf("a run in no step renders no placeholder:\n%s", rendered)
	}
}

func TestRunAgeReadsAsElapsedTime(t *testing.T) {
	t.Parallel()

	created := listNow.Add(-3 * time.Hour)
	tests := map[string]struct {
		run  l3.RunSummary
		want string
	}{
		"seconds":        {l3.RunSummary{CreatedAt: listNow.Add(-5 * time.Second)}, "5s"},
		"minutes":        {l3.RunSummary{CreatedAt: listNow.Add(-125 * time.Second)}, "2m5s"},
		"hours":          {l3.RunSummary{CreatedAt: listNow.Add(-90 * time.Minute)}, "1h30m"},
		"days":           {l3.RunSummary{CreatedAt: listNow.Add(-50 * time.Hour)}, "2d2h"},
		"started counts": {l3.RunSummary{CreatedAt: created, StartedAt: ptrTime(listNow.Add(-time.Minute))}, "1m0s"},
	}
	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := runAge(test.run, listNow); got != test.want {
				t.Fatalf("age = %q, want %q", got, test.want)
			}
		})
	}
}

// TestWaitTurnsATerminalRunIntoAnExitCode is the whole point of the command:
// three outcomes a script can branch on without parsing anything.
func TestWaitTurnsATerminalRunIntoAnExitCode(t *testing.T) {
	t.Parallel()

	succeeded := contract.RunRecord{RunID: "run-ok", Status: contract.RunSucceeded}
	var out strings.Builder
	if err := reportTerminalRun(&out, succeeded, false); err != nil {
		t.Fatalf("a successful run reported an error: %v", err)
	}
	if strings.TrimSpace(out.String()) != string(contract.RunSucceeded) {
		t.Fatalf("wait printed %q", out.String())
	}

	failed := contract.RunRecord{RunID: "run-bad", Status: contract.RunFailed}
	out.Reset()
	err := reportTerminalRun(&out, failed, false)
	if err == nil {
		t.Fatal("a failed run exited zero")
	}
	if code := commandExitCode(err); code != exitRunFailed {
		t.Fatalf("a failed run exited %d, want %d", code, exitRunFailed)
	}
	// The status is still printed: a script that wants the word as well as the
	// code should not have to run a second command for it.
	if strings.TrimSpace(out.String()) != string(contract.RunFailed) {
		t.Fatalf("a failed run printed %q", out.String())
	}

	// A timeout is not a failure. "I do not know yet" and "it failed" lead to
	// different next actions, so they get different codes.
	timeout := &waitTimeoutError{runID: "run-slow", status: contract.RunRunning, waited: time.Minute}
	if code := commandExitCode(timeout); code != exitWaitTimeout {
		t.Fatalf("a timeout exited %d, want %d", code, exitWaitTimeout)
	}
	if exitRunFailed == exitWaitTimeout {
		t.Fatal("a failed run and a timeout share an exit code")
	}
	if !strings.Contains(timeout.Error(), "still running") {
		t.Fatalf("timeout message = %q", timeout.Error())
	}
}

// TestWaitJSONPrintsTheFinalRecord keeps the machine-readable arm parseable on
// both outcomes, including the one that exits non-zero.
func TestWaitJSONPrintsTheFinalRecord(t *testing.T) {
	t.Parallel()

	for _, status := range []contract.RunState{contract.RunSucceeded, contract.RunFailed} {
		var out strings.Builder
		record := contract.RunRecord{RunID: "run-json", Status: status}
		_ = reportTerminalRun(&out, record, true)
		var decoded contract.RunRecord
		if err := json.Unmarshal([]byte(out.String()), &decoded); err != nil {
			t.Fatalf("--json output for a %s run is not JSON: %v (%s)", status, err, out.String())
		}
		if decoded.Status != status || decoded.RunID != "run-json" {
			t.Fatalf("--json record = %#v", decoded)
		}
	}
}

// TestRunIsTerminalFollowsTheLedgersOwnTable keeps this command from holding a
// private opinion about which states are ends.
func TestRunIsTerminalFollowsTheLedgersOwnTable(t *testing.T) {
	t.Parallel()

	for status, transitions := range contract.RunTransitions {
		if got, want := runIsTerminal(status), len(transitions) == 0; got != want {
			t.Fatalf("runIsTerminal(%q) = %t, want %t", status, got, want)
		}
	}
	if runIsTerminal(contract.RunState("invented")) {
		t.Fatal("an unknown state read as terminal")
	}
}

func ptrTime(value time.Time) *time.Time { return &value }
