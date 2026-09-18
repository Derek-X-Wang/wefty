package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
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

	if exitRunFailed == exitWaitTimeout {
		t.Fatal("a failed run and a timeout share an exit code")
	}
}

// waitHarness stands up a fake L3 the wait loop really polls, so the timeout
// paths are exercised rather than described.
type waitHarness struct {
	t        *testing.T
	clients  *apiClients
	mu       sync.Mutex
	statuses []contract.RunState
	failures []int
	stall    chan struct{}
	requests int
}

func newWaitHarness(t *testing.T) *waitHarness {
	t.Helper()
	h := &waitHarness{t: t}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		index := h.requests
		h.requests++
		stall := h.stall
		var failure int
		if index < len(h.failures) {
			failure = h.failures[index]
		}
		status := contract.RunRunning
		if index < len(h.statuses) {
			status = h.statuses[index]
		} else if len(h.statuses) > 0 {
			status = h.statuses[len(h.statuses)-1]
		}
		h.mu.Unlock()
		if stall != nil {
			// Accept the request and never answer it, which is precisely what
			// --timeout has to survive.
			select {
			case <-stall:
			case <-r.Context().Done():
			}
			return
		}
		if failure != 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(failure)
			_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
				Code: contract.ErrorInternal, Message: "not now", Retryable: true}})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(contract.RunRecord{RunID: "run-under-test", Status: status})
	}))
	t.Cleanup(server.Close)
	// The real client always addresses http://wefty.invalid and relies on its
	// transport to decide where that goes, which is exactly the seam a test
	// needs: this one sends it to the fake ledger above.
	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	h.clients = &apiClients{l3: &apiClient{name: "L3", flag: "l3", client: &http.Client{
		Transport: &redirectingTransport{target: target, inner: server.Client().Transport},
	}}}
	return h
}

// redirectingTransport points wefty.invalid at a test server.
type redirectingTransport struct {
	target *url.URL
	inner  http.RoundTripper
}

func (t *redirectingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	routed := request.Clone(request.Context())
	routed.URL.Scheme = t.target.Scheme
	routed.URL.Host = t.target.Host
	routed.Host = t.target.Host
	inner := t.inner
	if inner == nil {
		inner = http.DefaultTransport
	}
	return inner.RoundTrip(routed)
}

func (h *waitHarness) polls() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.requests
}

// TestWaitHonoursItsDeadlineEvenWhenTheLedgerStops is the P1: a ledger that
// accepts a connection and never answers must not defeat --timeout.
func TestWaitHonoursItsDeadlineEvenWhenTheLedgerStops(t *testing.T) {
	t.Parallel()

	h := newWaitHarness(t)
	h.stall = make(chan struct{})
	t.Cleanup(func() { close(h.stall) })

	var out, errOut bytes.Buffer
	started := time.Now()
	err := executeWait(t.Context(), h.clients, false,
		[]string{"run-under-test", "--timeout", "300ms"}, &out, &errOut)
	if code := commandExitCode(err); code != exitWaitTimeout {
		t.Fatalf("a stalled request exited %d (%v), want %d", code, err, exitWaitTimeout)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("the wait ran %s past a 300ms timeout", elapsed)
	}
}

// TestWaitTimesOutOnARunThatIsStillGoing is the ordinary timeout: the ledger
// answers promptly and the run simply has not finished.
func TestWaitTimesOutOnARunThatIsStillGoing(t *testing.T) {
	t.Parallel()

	h := newWaitHarness(t)
	h.statuses = []contract.RunState{contract.RunRunning}

	var out, errOut bytes.Buffer
	err := executeWait(t.Context(), h.clients, false,
		[]string{"run-under-test", "--timeout", "400ms"}, &out, &errOut)
	if code := commandExitCode(err); code != exitWaitTimeout {
		t.Fatalf("a running run exited %d (%v), want %d", code, err, exitWaitTimeout)
	}
	// The message names the state the run was last seen in.
	if !strings.Contains(err.Error(), string(contract.RunRunning)) {
		t.Fatalf("timeout message = %q", err.Error())
	}
	if h.polls() < 2 {
		t.Fatalf("the wait polled %d times, so it was not really polling", h.polls())
	}
}

// TestWaitKeepsWaitingThroughATransientFailure separates the ledger being
// briefly unavailable from the run having an outcome.
func TestWaitKeepsWaitingThroughATransientFailure(t *testing.T) {
	t.Parallel()

	h := newWaitHarness(t)
	h.failures = []int{http.StatusInternalServerError, http.StatusBadGateway}
	h.statuses = []contract.RunState{contract.RunRunning, contract.RunRunning, contract.RunSucceeded}

	var out, errOut bytes.Buffer
	if err := executeWait(t.Context(), h.clients, false,
		[]string{"run-under-test", "--timeout", "30s"}, &out, &errOut); err != nil {
		t.Fatalf("a transient failure ended the wait: %v", err)
	}
	if strings.TrimSpace(out.String()) != string(contract.RunSucceeded) {
		t.Fatalf("wait printed %q", out.String())
	}
	if h.polls() < 3 {
		t.Fatalf("the wait polled %d times, so it did not retry", h.polls())
	}
}

// TestWaitFailsImmediatelyOnAnAnswerThatWillNotChange keeps the command from
// waiting out a timeout on a run that does not exist or cannot be read.
func TestWaitFailsImmediatelyOnAnAnswerThatWillNotChange(t *testing.T) {
	t.Parallel()

	h := newWaitHarness(t)
	h.failures = []int{http.StatusNotFound}

	var out, errOut bytes.Buffer
	started := time.Now()
	err := executeWait(t.Context(), h.clients, false,
		[]string{"run-under-test", "--timeout", "30s"}, &out, &errOut)
	if err == nil {
		t.Fatal("a missing run ended the wait successfully")
	}
	if code := commandExitCode(err); code == exitWaitTimeout {
		t.Fatalf("a missing run was reported as a timeout: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("a permanent failure waited %s", elapsed)
	}
	if h.polls() != 1 {
		t.Fatalf("a permanent failure was polled %d times", h.polls())
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

// TestATerminalRunIsNeverDescribedAsRunning is the projection rule the listing
// and the lineage already follow: a run that has stopped is in no step, even
// when its last bracket never closed.
func TestATerminalRunIsNeverDescribedAsRunning(t *testing.T) {
	t.Parallel()

	envelopes := []contract.Envelope{stepBracketEnvelope("e1", "gates", "started", listNow)}
	running := runStepsFor(contract.RunRecord{Status: contract.RunRunning, Envelopes: envelopes})
	if running.Current != "gates" {
		t.Fatalf("a running run is not in its open step: %#v", running)
	}
	terminal := runStepsFor(contract.RunRecord{Status: contract.RunFailed, Envelopes: envelopes})
	if terminal.Current != "" {
		t.Fatalf("a terminal run is still in step %q", terminal.Current)
	}
	// The interval survives: the run really did start that step, and losing it
	// would hide where the workload got to before it stopped.
	if len(terminal.Steps) != 1 || !terminal.Steps[0].Open || terminal.Steps[0].Seconds != nil {
		t.Fatalf("the unmatched bracket was not preserved as incomplete: %#v", terminal.Steps)
	}
	if terminal.Steps[0].EndedAt != nil {
		t.Fatal("an end time was invented for a step that never ended")
	}

	var out strings.Builder
	if err := writeRunSteps(&out, terminal); err != nil {
		t.Fatal(err)
	}
	rendered := out.String()
	if strings.Contains(rendered, "running") {
		t.Fatalf("a terminal run renders as running:\n%s", rendered)
	}
	if !strings.Contains(rendered, "incomplete") || !strings.Contains(rendered, "no step is open") {
		t.Fatalf("an incomplete step is not reported as one:\n%s", rendered)
	}

	out.Reset()
	if err := writeRunSteps(&out, running); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "running") || !strings.Contains(out.String(), "currently in gates") {
		t.Fatalf("a running run does not read as running:\n%s", out.String())
	}
}

func stepBracketEnvelope(id, name, status string, created time.Time) contract.Envelope {
	extensions, err := json.Marshal(map[string]any{
		"dev.wefty.mailbox": map[string]any{"kind": "step", "step_status": status, "name": name},
	})
	if err != nil {
		panic(err)
	}
	return contract.Envelope{
		EnvelopeID: id, StepID: name, Status: contract.EnvelopePartial,
		Extensions: extensions, CreatedAt: created,
	}
}
