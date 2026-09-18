//go:build darwin || linux

// Package branchgates_test exercises workflows/branch-gates/branch-gates.sh
// against a real single-machine stack: L1, L3, a node agent and the process
// runner, with the workflow submitted exactly the way an operator submits it.
// It is the CI counterpart of the dogfood-workflow contract smoke.
//
// The exercise is opt-in because it starts real processes, clones real Git
// repositories and compiles Go: set WEFTY_BRANCH_GATES_EXERCISE=1, which the
// branch-gates-workflow job in contract-gate.yml does.
package branchgates_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/workflowhelper"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

const (
	nodeID   = "branch-gates-node"
	allGates = "gofmt,fabric-boundary,vet,test"
	// runBudget bounds one run. go test's own default timeout (10m) is the
	// outer bound, and the CI job's timeout-minutes sits outside both.
	runBudget = 5 * time.Minute
	// logSettleBudget bounds the wait for the agent's final log upload and the
	// result upload after the run is already terminal: a failing gate makes L3
	// terminal before the shell exits, so both can still be in flight.
	logSettleBudget = 60 * time.Second
	// doneMarker is the workflow's last log line on every path, so the poll
	// below can wait for a settled log instead of guessing.
	doneMarker = "branch-gates: done"
	// perRequestBudget caps one log read so a stalled request cannot consume
	// the whole settle budget.
	perRequestBudget = 10 * time.Second
	probeMarker      = "wefty environment: ["
	// holdingGateTag guards a fixture test that sleeps, so the exercise has a
	// deterministic window in which a gate is running and the listing can be
	// asked what it is. branch-gates passes GOFLAGS into the subject
	// environment, which is how the tag reaches the code under gate.
	holdingGateTag = "weftyholdinggate"
)

// holdingGateHold is how long the fixture's `test` gate holds. It is comfortably
// longer than the watcher's 100ms poll and short enough not to matter to a
// two-minute exercise.
const holdingGateHold = 3 * time.Second

// gateResult mirrors one entry of the workflow's result.json.
type gateResult struct {
	Name            string `json:"name"`
	Outcome         string `json:"outcome"`
	ExitCode        int    `json:"exit_code"`
	DurationSeconds int    `json:"duration_seconds"`
	Command         string `json:"command"`
}

type branchGatesResult struct {
	SchemaVersion int          `json:"schema_version"`
	Workflow      string       `json:"workflow"`
	RunID         string       `json:"run_id"`
	RepoURL       string       `json:"repo_url"`
	Ref           string       `json:"ref"`
	Commit        string       `json:"commit"`
	Passed        bool         `json:"passed"`
	FailedGates   int          `json:"failed_gates"`
	Gates         []gateResult `json:"gates"`
}

// TestBranchGatesScriptIsValidShell is cheap enough to stay in the default
// suite: the workflow script is shipped source, so a syntax error in it must
// not wait for the opt-in stack exercise.
func TestBranchGatesScriptIsValidShell(t *testing.T) {
	bashPath, err := exec.LookPath("bash")
	if err != nil {
		// Every supported runner has bash; a missing one in CI is a broken
		// lane, not a reason to report a green skip.
		if os.Getenv("GITHUB_ACTIONS") == "true" {
			t.Fatalf("bash is required in CI: %v", err)
		}
		t.Skip("bash is required to check the workflow script")
	}
	if output, err := exec.Command(bashPath, "-n", "branch-gates.sh").CombinedOutput(); err != nil {
		t.Fatalf("bash -n branch-gates.sh: %v\n%s", err, output)
	}
	shellcheckPath, err := exec.LookPath("shellcheck")
	if err != nil {
		// The branch-gates lane installs shellcheck and demands it; other
		// lanes (macOS runners, laptops) report NOT-RUN instead.
		if os.Getenv("WEFTY_REQUIRE_SHELLCHECK") == "1" {
			t.Fatalf("shellcheck is required in this lane: %v", err)
		}
		t.Log("shellcheck is not installed; NOT-RUN")
		return
	}
	// Default severity on purpose: findings are silenced with a scoped
	// directive and a reason, never by raising the threshold. The version is
	// reported because CI's shellcheck is older than a developer's and can
	// report the same thing under a different code.
	if output, err := exec.Command(shellcheckPath, "branch-gates.sh").CombinedOutput(); err != nil {
		version, _ := exec.Command(shellcheckPath, "--version").Output()
		t.Fatalf("shellcheck branch-gates.sh: %v\n%s\n%s", err, output, version)
	}
}

func TestBranchGatesWorkflowHandsBackGateResults(t *testing.T) {
	if os.Getenv("WEFTY_BRANCH_GATES_EXERCISE") != "1" {
		t.Skip("set WEFTY_BRANCH_GATES_EXERCISE=1 to run the branch-gates stack exercise")
	}
	script, err := os.ReadFile("branch-gates.sh")
	if err != nil {
		t.Fatal(err)
	}
	bashPath, err := exec.LookPath("bash")
	if err != nil {
		t.Fatal(err)
	}
	subject := initializeSubjectRepository(t)
	// The holding gate is on for this exercise only, through the one name
	// branch-gates forwards into the subject environment.
	t.Setenv("GOFLAGS", strings.TrimSpace(os.Getenv("GOFLAGS")+" -tags="+holdingGateTag))
	caller, store := startStack(t)

	for _, testCase := range []struct {
		name         string
		ref          string
		wantRunState contract.RunState
		wantPassed   bool
		wantOutcomes map[string]string
		wantFailures []string
		// wantProbes is how many subject-environment probes must appear in the
		// run log: the broken branch runs one in the boundary script and one in
		// the failing test, and both are echoed with failures.txt.
		wantProbes int
	}{
		{
			name:         "clean branch passes every gate",
			ref:          "main",
			wantRunState: contract.RunSucceeded,
			wantPassed:   true,
			wantOutcomes: map[string]string{"gofmt": "pass", "fabric-boundary": "pass", "vet": "pass", "test": "pass"},
		},
		{
			name:         "broken branch reports the failing gates and their output",
			ref:          "broken",
			wantRunState: contract.RunFailed,
			wantPassed:   false,
			wantOutcomes: map[string]string{"gofmt": "fail", "fabric-boundary": "fail", "vet": "pass", "test": "fail"},
			wantFailures: []string{"unformatted.go", "fabric boundary probe", "TestDeliberateFailure", "FAIL"},
			wantProbes:   2,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			params, err := json.Marshal(map[string]string{"ref": testCase.ref, "repo_url": subject, "gates": allGates})
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(script)
			mode := uint32(0o755)
			accepted := submitRun(t, caller, l3.CreateRunRequest{
				InlineScript: &l3.InlineScriptInput{
					Content: string(script), SHA256: hex.EncodeToString(digest[:]),
					Interpreter: []string{bashPath}, Mode: &mode,
				},
				Params: params,
				Tags:   []string{contract.StableNodeTagPrefix + nodeID},
				Limits: &contract.RunLimits{MaxRuntimeSeconds: int(runBudget.Seconds())},
				// The envelope the previous HTTP path guaranteed is still
				// guaranteed, now by the ledger refusing a run that produced
				// none. DispatchAuthority is deliberately absent: branch-gates
				// reports and dispatches nothing, so it runs with no credential
				// at all.
				RequiredEnvelope: true,
			}, "branch-gates-exercise-"+testCase.ref)

			// Observation, proved on a real running job rather than described:
			// while the workflow is going, the general listing names the gate
			// it is on. The `test` gate holds for a known interval (see the
			// fixture subject), so there is a deterministic window to see it
			// in, and the watcher reports its own failures rather than
			// swallowing them into an empty answer.
			watch := newStepWatch(t.Context(), caller, accepted.RunID)

			record := waitForTerminalRun(t, store, accepted.RunID, runBudget)
			// The verdict document is read from the ledger, which is where a
			// person reads it: the node uploads it at completion and the log
			// no longer carries a copy.
			raw, logs := waitForResultJSON(t, caller, accepted.RunID, logSettleBudget)
			if record.Status != testCase.wantRunState {
				t.Fatalf("run status = %q, want %q; logs:\n%s", record.Status, testCase.wantRunState, logs)
			}
			var result branchGatesResult
			if err := json.Unmarshal(raw, &result); err != nil {
				t.Fatalf("decode result.json %q: %v", raw, err)
			}
			if result.SchemaVersion != 1 || result.Workflow != "branch-gates" || result.RunID != accepted.RunID {
				t.Fatalf("result identity = %#v", result)
			}
			// #477 acceptance, all three parts, on the real stack.
			//
			// One: a running job appears in `runs list` with its current gate
			// as the step it is in.
			step, watchErr := watch.result()
			if watchErr != nil {
				t.Fatalf("watching the run listing failed: %v", watchErr)
			}
			if !slices.Contains(strings.Split(allGates, ","), step) {
				t.Fatalf("runs list reported current step %q, which is not one of the gates", step)
			}

			// Two: every gate that ran is one closed interval with a duration,
			// named for that gate, so `inspect` can show per-gate timings.
			steps := l3.DeriveRunSteps(record.Envelopes)
			if steps.Current != "" {
				t.Fatalf("a terminal run is still in step %q", steps.Current)
			}
			intervals := map[string]int{}
			for _, interval := range steps.Steps {
				if interval.Open || interval.Seconds == nil {
					t.Fatalf("gate step %q has no duration: %#v", interval.Name, interval)
				}
				intervals[interval.Name]++
			}
			wantIntervals := map[string]int{}
			for _, gate := range strings.Split(allGates, ",") {
				wantIntervals[gate] = 1
			}
			if !reflect.DeepEqual(intervals, wantIntervals) {
				t.Fatalf("gate intervals = %v, want exactly one per gate %v:\n%s", intervals, wantIntervals, logs)
			}

			if result.Ref != testCase.ref || result.Commit == "" || result.RepoURL != subject {
				t.Fatalf("result subject = ref %q commit %q repo %q", result.Ref, result.Commit, result.RepoURL)
			}
			if result.Passed != testCase.wantPassed {
				t.Fatalf("result.passed = %v, want %v; result:\n%s", result.Passed, testCase.wantPassed, raw)
			}
			outcomes := map[string]string{}
			for _, gate := range result.Gates {
				outcomes[gate.Name] = gate.Outcome
				if gate.Command == "" {
					t.Fatalf("gate %q recorded no command", gate.Name)
				}
			}
			if len(outcomes) != len(testCase.wantOutcomes) {
				t.Fatalf("result gates = %#v, want %#v", outcomes, testCase.wantOutcomes)
			}
			for name, want := range testCase.wantOutcomes {
				if outcomes[name] != want {
					t.Fatalf("gate %q outcome = %q, want %q; result:\n%s", name, outcomes[name], want, raw)
				}
			}

			// Handoff lifecycle: results are retained on every outcome now, so
			// the passing run -- the one an operator most wants to read and the
			// only one that used to leave nothing behind -- keeps its files too.
			handoff := filepath.Join(l3.DefaultHandoffRoot, accepted.RunID)
			retained, err := os.ReadFile(filepath.Join(handoff, "result.json"))
			if err != nil {
				t.Fatalf("read retained result.json: %v; logs:\n%s", err, logs)
			}
			if !bytes.Equal(bytes.TrimSpace(retained), bytes.TrimSpace(raw)) {
				t.Fatalf("retained result.json differs from the logged one:\n%s\n%s", retained, raw)
			}
			if !testCase.wantPassed {
				failures, err := os.ReadFile(filepath.Join(handoff, "failures.txt"))
				if err != nil {
					t.Fatalf("read retained failures.txt: %v", err)
				}
				for _, want := range testCase.wantFailures {
					if !strings.Contains(string(failures), want) {
						t.Fatalf("failures.txt does not contain %q:\n%s", want, failures)
					}
				}
				if strings.Contains(string(failures), "===== vet") {
					t.Fatalf("failures.txt carries output from a passing gate:\n%s", failures)
				}
				// The subject's test printed every WEFTY_* variable it could
				// see. An empty list is the assertion: no run token, no
				// attempt credential, nothing else from the contract.
				// One probe from the boundary script, one from the test.
				assertCredentialProbe(t, "failures.txt", string(failures), 2)
			}
			assertCredentialProbe(t, "run log", logs, testCase.wantProbes)
			assertNoCredentialInTheWorkflowShell(t, logs)
			assertScratchRemoved(t, accepted.RunID)

			// The ledger carries one envelope per gate, the run's result event,
			// and the single final gate result -- all of them published by the
			// node agent from the mailbox, none of them by this job.
			//
			// The step brackets are envelopes too, sharing a step ID with the
			// gate's own envelope, and they report where the run is rather than
			// how the gate went. They are counted as steps below and skipped
			// here, which is the same distinction DeriveRunSteps makes.
			gotSteps := make([]string, 0, len(record.Envelopes))
			for _, envelope := range record.Envelopes {
				if isStepBracket(envelope) {
					continue
				}
				gotSteps = append(gotSteps, envelope.StepID)
				if envelope.AttemptID == "" {
					t.Fatalf("envelope %s carries no server-bound attempt", envelope.EnvelopeID)
				}
				wantStatus := contract.EnvelopeSucceeded
				if testCase.wantOutcomes[envelope.StepID] == "fail" {
					wantStatus = contract.EnvelopeFailed
				}
				if envelope.StepID == "result" && !testCase.wantPassed {
					wantStatus = contract.EnvelopeFailed
				}
				if envelope.Status != wantStatus {
					t.Fatalf("envelope %s status = %q, want %q", envelope.StepID, envelope.Status, wantStatus)
				}
			}
			if want := append(strings.Split(allGates, ","), "result"); !equalStrings(gotSteps, want) {
				t.Fatalf("envelope steps = %v, want %v", gotSteps, want)
			}
			if len(record.Gates) != 1 {
				t.Fatalf("gate results = %d, want exactly one final gate", len(record.Gates))
			}
			finalGate := record.Gates[0]
			wantOutcome := contract.GatePass
			if !testCase.wantPassed {
				wantOutcome = contract.GateFail
			}
			if finalGate.Name != "branch-gates" || finalGate.Outcome != wantOutcome || finalGate.AttemptID == "" {
				t.Fatalf("final gate = %#v, want name branch-gates outcome %q with a bound attempt", finalGate, wantOutcome)
			}
			// The mailbox defaults a gate's step to its name, so the step
			// identity the HTTP path set explicitly is preserved rather than
			// lost. Pinned because it is not obvious from either side alone.
			if finalGate.StepID != "branch-gates" {
				t.Fatalf("final gate step = %q, want branch-gates", finalGate.StepID)
			}
			// A mailbox gate carries exactly one evidence value, so the
			// structure the HTTP path spread across entries now lives inside
			// that single document.
			if len(finalGate.Evidence) != 1 || !strings.Contains(finalGate.Evidence[0].Value, result.Commit) {
				t.Fatalf("final gate evidence = %#v, want one document naming the resolved commit", finalGate.Evidence)
			}

		})
	}
}

// TestBranchGatesWorkflowRejectsBadInput pins the single failure path: every
// rejected input and every checkout failure still leaves the two handoff files,
// a failed envelope and an `error` gate, and never a partial pass.
func TestBranchGatesWorkflowRejectsBadInput(t *testing.T) {
	if os.Getenv("WEFTY_BRANCH_GATES_EXERCISE") != "1" {
		t.Skip("set WEFTY_BRANCH_GATES_EXERCISE=1 to run the branch-gates stack exercise")
	}
	script, err := os.ReadFile("branch-gates.sh")
	if err != nil {
		t.Fatal(err)
	}
	bashPath, err := exec.LookPath("bash")
	if err != nil {
		t.Fatal(err)
	}
	subject := initializeSubjectRepository(t)
	caller, store := startStack(t)

	for _, testCase := range []struct {
		name     string
		params   map[string]string
		wantStep string
		wantText string
	}{
		{
			name:     "empty gate element",
			params:   map[string]string{"ref": "main", "repo_url": subject, "gates": ","},
			wantStep: "input",
			wantText: "empty element",
		},
		{
			name:     "duplicate gate",
			params:   map[string]string{"ref": "main", "repo_url": subject, "gates": "gofmt,gofmt"},
			wantStep: "input",
			wantText: `repeats "gofmt"`,
		},
		{
			name:     "unknown gate",
			params:   map[string]string{"ref": "main", "repo_url": subject, "gates": "bogus"},
			wantStep: "input",
			wantText: `unknown gate "bogus"`,
		},
		{
			name:     "missing ref",
			params:   map[string]string{"repo_url": subject},
			wantStep: "input",
			wantText: "params.ref is required",
		},
		{
			name:     "unknown ref",
			params:   map[string]string{"ref": "no-such-ref", "repo_url": subject},
			wantStep: "checkout",
			wantText: `is not a branch, tag or commit`,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			params, err := json.Marshal(testCase.params)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(script)
			mode := uint32(0o755)
			accepted := submitRun(t, caller, l3.CreateRunRequest{
				InlineScript: &l3.InlineScriptInput{
					Content: string(script), SHA256: hex.EncodeToString(digest[:]),
					Interpreter: []string{bashPath}, Mode: &mode,
				},
				Params:           params,
				Tags:             []string{contract.StableNodeTagPrefix + nodeID},
				Limits:           &contract.RunLimits{MaxRuntimeSeconds: int(runBudget.Seconds())},
				RequiredEnvelope: true,
			}, "branch-gates-negative-"+testCase.name)

			// No watch here: these runs fail before any gate starts, so there
			// is no step to observe and asserting one would be asserting the
			// wrong thing.
			record := waitForTerminalRun(t, store, accepted.RunID, runBudget)
			raw, logs := waitForResultJSON(t, caller, accepted.RunID, logSettleBudget)
			if record.Status != contract.RunFailed {
				t.Fatalf("run status = %q, want failed; logs:\n%s", record.Status, logs)
			}

			var result struct {
				Passed        bool         `json:"passed"`
				Gates         []gateResult `json:"gates"`
				WorkflowError struct {
					Step    string `json:"step"`
					Message string `json:"message"`
				} `json:"workflow_error"`
			}
			if err := json.Unmarshal(raw, &result); err != nil {
				t.Fatalf("decode result.json %q: %v", raw, err)
			}
			if result.Passed || len(result.Gates) != 0 {
				t.Fatalf("workflow-error result claims progress: %s", raw)
			}
			if result.WorkflowError.Step != testCase.wantStep ||
				!strings.Contains(result.WorkflowError.Message, testCase.wantText) {
				t.Fatalf("workflow_error = %#v, want step %q containing %q", result.WorkflowError, testCase.wantStep, testCase.wantText)
			}

			// The failing attempt retains the handoff directory, so both files
			// are on the node exactly as on the gate-failure path.
			handoff := filepath.Join(l3.DefaultHandoffRoot, accepted.RunID)
			retained, err := os.ReadFile(filepath.Join(handoff, "result.json"))
			if err != nil {
				t.Fatalf("read retained result.json: %v; logs:\n%s", err, logs)
			}
			if !bytes.Equal(bytes.TrimSpace(retained), bytes.TrimSpace(raw)) {
				t.Fatalf("retained result.json differs from the logged one:\n%s\n%s", retained, raw)
			}
			diagnostic, err := os.ReadFile(filepath.Join(handoff, "failures.txt"))
			if err != nil {
				t.Fatalf("read retained failures.txt: %v", err)
			}
			if !strings.HasPrefix(string(diagnostic), "===== workflow-error: "+testCase.wantStep) {
				t.Fatalf("failures.txt is not the standard diagnostic:\n%s", diagnostic)
			}

			// The failed step envelope, then the workflow-error result
			// document, both failed and both published from the mailbox.
			if len(record.Envelopes) != 2 ||
				record.Envelopes[0].StepID != testCase.wantStep || record.Envelopes[0].Status != contract.EnvelopeFailed ||
				record.Envelopes[1].StepID != "result" || record.Envelopes[1].Status != contract.EnvelopeFailed {
				t.Fatalf("envelopes = %#v, want a failed %q envelope then a failed result", record.Envelopes, testCase.wantStep)
			}
			if len(record.Gates) != 1 || record.Gates[0].Name != "branch-gates" ||
				record.Gates[0].Outcome != contract.GateError {
				t.Fatalf("gates = %#v, want one branch-gates gate with outcome error", record.Gates)
			}
			assertCredentialProbe(t, "run log", logs, 0)
			assertNoCredentialInTheWorkflowShell(t, logs)
			assertScratchRemoved(t, accepted.RunID)
		})
	}
}

// assertScratchRemoved pins the workflow's own cleanup: the scratch directory
// holds the clone, the Go caches and every document this workflow assembles,
// so it must not outlive the run on either path.
//
// It polls, because a `fail` or `error` gate makes the run terminal in L3 while
// the workload is still exiting: the run reads as failed before the shell has
// finished its EXIT trap. Tearing the stack down the instant a run goes
// terminal truncates that teardown -- which is worth knowing for #477 as well
// as here.
func assertScratchRemoved(t *testing.T, runID string) {
	t.Helper()
	suffix := runID[strings.LastIndex(runID, "_")+1:]
	if len(suffix) > 12 {
		suffix = suffix[:12]
	}
	scratch := filepath.Join("/tmp", "wefty-bg-"+suffix)
	deadline := time.Now().Add(logSettleBudget)
	for {
		_, err := os.Stat(scratch)
		if os.IsNotExist(err) {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("scratch directory %s survived the run by %s: %v", scratch, logSettleBudget, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// assertCredentialProbe requires wantProbes copies of the subject's environment
// probe to be present and every one of them to be empty, then rejects any
// credential that reached an operator-visible surface. Requiring the marker is
// the point: a probe that silently stopped running would otherwise look clean.
func assertCredentialProbe(t *testing.T, surface, content string, wantProbes int) {
	t.Helper()
	if got := strings.Count(content, probeMarker); got < wantProbes {
		t.Fatalf("%s carries %d environment probes, want at least %d:\n%s", surface, got, wantProbes, content)
	}
	if empty := strings.Count(content, probeMarker+"]"); empty != strings.Count(content, probeMarker) {
		t.Fatalf("%s shows WEFTY_* variables inside a subject process:\n%s", surface, content)
	}
	for _, needle := range []string{"WEFTY_RUN_TOKEN", "WEFTY_ATTEMPT_TOKEN", "Bearer "} {
		if strings.Contains(content, needle) {
			t.Fatalf("%s leaks %q:\n%s", surface, needle, content)
		}
	}
}

// assertNoCredentialInTheWorkflowShell reads the workflow's own report of what
// it can see. The subject probe covers the code under test; this covers the
// shell running it, which is the half that used to hold the run token and is
// the whole point of reporting through the mailbox.
func assertNoCredentialInTheWorkflowShell(t *testing.T, logs string) {
	t.Helper()
	const marker = "branch-gates: credentials in the workflow environment: ["
	index := strings.Index(logs, marker)
	if index < 0 {
		t.Fatalf("the run log carries no credential report; the workflow may not have started:\n%s", logs)
	}
	rest := logs[index+len(marker):]
	end := strings.Index(rest, "]")
	if end < 0 {
		t.Fatalf("the credential report is truncated:\n%s", logs)
	}
	if visible := rest[:end]; visible != "" {
		t.Fatalf("the workflow shell was handed credentials it does not need: [%s]", visible)
	}
}

// initializeSubjectRepository builds the repository under test: a `main`
// branch whose gates all pass and a `broken` branch with an unformatted file
// and a deliberately failing test.
func initializeSubjectRepository(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	runGit(t, repo, "init", "-b", "main")
	runGit(t, repo, "config", "user.name", "Wefty Branch Gates")
	runGit(t, repo, "config", "user.email", "branch-gates@example.test")
	writeFile(t, repo, "go.mod", "module branch-gates-subject\n\ngo 1.24\n")
	writeFile(t, repo, "subject.go", "package subject\n\n// Answer is the subject under gate.\nfunc Answer() int { return 42 }\n")
	writeFile(t, repo, "subject_test.go", "package subject\n\nimport \"testing\"\n\nfunc TestAnswer(t *testing.T) {\n\tif Answer() != 42 {\n\t\tt.Fatal(\"wrong answer\")\n\t}\n}\n")
	// A gate that holds for a known interval, so observing a running job's
	// current step is a fact rather than a race. It is behind a build tag the
	// exercise turns on through GOFLAGS -- one of the few names branch-gates
	// passes into the subject environment -- so an ordinary clone of this
	// fixture is unaffected.
	writeFile(t, repo, "slow_test.go", fmt.Sprintf(`//go:build %s

package subject

import (
	"testing"
	"time"
)

func TestHoldsSoTheGateCanBeObserved(t *testing.T) {
	time.Sleep(%d * time.Millisecond)
}
`, holdingGateTag, holdingGateHold.Milliseconds()))
	writeFile(t, repo, "scripts/check-fabric-boundary.sh", "#!/usr/bin/env bash\nset -eu\necho 'fabric boundary holds'\n")
	runGit(t, repo, "add", ".")
	runGit(t, repo, "commit", "-m", "subject: passing baseline")

	runGit(t, repo, "checkout", "-b", "broken")
	writeFile(t, repo, "unformatted.go", "package subject\nfunc  Unformatted()   int {return   1}\n")
	// The boundary script is the second executable boundary: a plain shell
	// process rather than a Go test binary. It prints the same probe and fails,
	// so its raw output also lands in failures.txt.
	writeFile(t, repo, "scripts/check-fabric-boundary.sh", `#!/usr/bin/env bash
set -u
visible=$(env | grep '^WEFTY_' | tr '\n' ' ')
printf 'fabric boundary probe; wefty environment: [%s]\n' "${visible% }"
exit 1
`)
	// The failing test doubles as the credential-leak probe: it prints every
	// WEFTY_* variable it can see, and that raw output is copied into
	// failures.txt before any redaction. The assertion is that the list is
	// empty, which covers the run token, the attempt credential and anything
	// else the contract may add later.
	writeFile(t, repo, "broken_test.go", `package subject

import (
	"os"
	"strings"
	"testing"
)

func TestDeliberateFailure(t *testing.T) {
	var visible []string
	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, "WEFTY_") {
			visible = append(visible, entry)
		}
	}
	t.Fatalf("deliberate branch-gates failure; wefty environment: [%s]", strings.Join(visible, " "))
}
`)
	runGit(t, repo, "add", ".")
	runGit(t, repo, "commit", "-m", "subject: deliberately failing branch")
	runGit(t, repo, "checkout", "main")
	return repo
}

func startStack(t *testing.T) (*http.Client, *l3.Store) {
	t.Helper()
	network := plain.NewNetwork()
	controlFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	ledgerFabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger", Tags: []string{l1.DefaultClientPrincipalTag}})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-agent", Tags: []string{l1.DefaultAgentPrincipalTag}})
	callerFabric := network.NewFabric(fabric.Identity{NodeID: "caller", Tags: []string{l3.DefaultCallerPrincipalTag}})

	l1Store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l1Store.Close() })
	l1Server, err := l1.NewServer(controlFabric, l1Store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{
		nodeID: l1.DefaultNodePolicy(runtime.GOOS, contract.StableNodeTagPrefix+nodeID),
	}})
	if err != nil {
		t.Fatal(err)
	}
	l1Listener, err := controlFabric.Listen("tcp", l3.DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}

	l3Store, err := l3.OpenStore(filepath.Join(t.TempDir(), "l3.sqlite"), l3.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l3Store.Close() })
	ledgerL1Client, err := l3.NewL1Client(ledgerFabric, l3.DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledgerL1Client.CloseIdleConnections)
	reconciler, err := l3.NewReconciler(l3Store, ledgerL1Client, l3.ReconcilerConfig{Interval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	l3Server, err := l3.NewServer(ledgerFabric, l3Store, l3.ServerConfig{Reconciler: reconciler, Logs: ledgerL1Client})
	if err != nil {
		t.Fatal(err)
	}
	l3Listener, err := ledgerFabric.Listen("tcp", l3.DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	l1Done := serve(func() error { return l1Server.Serve(ctx, l1Listener) })
	l3Done := serve(func() error { return l3Server.Serve(ctx, l3Listener) })

	nodeAgent, err := agent.New(agent.Config{
		Fabric: agentFabric, ControlPlaneAddress: l3.DefaultL1Address, RunLedgerAddress: l3.DefaultL3Address,
		NodeID: nodeID, BootSessionID: "boot-branch-gates", Version: "branch-gates-exercise",
		OS: runtime.GOOS, Architecture: runtime.GOARCH, Capabilities: map[string]bool{"kind:process": true},
		HeartbeatInterval: 50 * time.Millisecond, ClaimInterval: 20 * time.Millisecond,
		RenewalInterval: 50 * time.Millisecond, LogFlushInterval: 10 * time.Millisecond, LogRetryInterval: 10 * time.Millisecond,
		LogSpoolDirectory: t.TempDir(),
		WorkloadRuntimes: map[string]agent.WorkloadRuntime{
			contract.JobKindProcess: processrunner.NewAdapter(processrunner.New(processrunner.Config{
				BaseEnvironment: environmentWithWefty(t),
			})),
		},
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(nodeAgent.Close)
	agentDone := serve(func() error { return nodeAgent.Run(ctx) })
	t.Cleanup(func() {
		cancel()
		for name, done := range map[string]<-chan error{"agent": agentDone, "L3": l3Done, "L1": l1Done} {
			if err := <-done; err != nil {
				t.Errorf("%s server: %v", name, err)
			}
		}
	})

	caller := fabricHTTPClient(callerFabric, l3.DefaultL3Address)
	t.Cleanup(caller.CloseIdleConnections)
	return caller, l3Store
}

// environmentWithWefty puts a freshly built `wefty` on the job's PATH. The
// workflow reports through `wefty run`, so the binary is now as much a
// prerequisite of the job rootfs as git and go are -- and building it here is
// what lets the CI job stay exactly as it is.
func environmentWithWefty(t *testing.T) []string {
	t.Helper()
	directory := t.TempDir()
	binary := filepath.Join(directory, "wefty")
	build := exec.Command("go", "build", "-o", binary, "github.com/Derek-X-Wang/wefty/cmd/wefty")
	build.Dir = ".."
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build wefty for the workflow rootfs: %v\n%s", err, output)
	}
	environment := make([]string, 0, len(os.Environ())+1)
	path := directory
	for _, entry := range os.Environ() {
		if value, ok := strings.CutPrefix(entry, "PATH="); ok {
			path = directory + string(os.PathListSeparator) + value
			continue
		}
		environment = append(environment, entry)
	}
	return append(environment, "PATH="+path)
}

func submitRun(t *testing.T, client *http.Client, input l3.CreateRunRequest, idempotencyKey string) l3.RunAccepted {
	t.Helper()
	body, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, "http://run-ledger.invalid/v1/runs", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", idempotencyKey)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	responseBody, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("submit run = %d body=%s", response.StatusCode, responseBody)
	}
	var accepted l3.RunAccepted
	if err := json.Unmarshal(responseBody, &accepted); err != nil {
		t.Fatal(err)
	}
	return accepted
}

func waitForTerminalRun(t *testing.T, store *l3.Store, runID string, timeout time.Duration) contract.RunRecord {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		record, err := store.GetRun(context.Background(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if record.Status == contract.RunSucceeded || record.Status == contract.RunFailed {
			return record
		}
		time.Sleep(50 * time.Millisecond)
	}
	record, err := store.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("run %s did not become terminal within %s: %v", runID, timeout, err)
	}
	t.Fatalf("run %s did not become terminal within %s; status=%s envelopes=%d gates=%d",
		runID, timeout, record.Status, len(record.Envelopes), len(record.Gates))
	return contract.RunRecord{}
}

// fetchRunLogs reads the run log under an explicit deadline.
func fetchRunLogs(client *http.Client, runID string, budget time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://run-ledger.invalid/v1/runs/"+runID+"/logs?limit=1000", nil)
	if err != nil {
		return "", err
	}
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		return "", fmt.Errorf("get run logs = %d body=%s", response.StatusCode, body)
	}
	var page l1.LogPage
	if err := json.NewDecoder(response.Body).Decode(&page); err != nil {
		return "", err
	}
	var output strings.Builder
	for _, event := range page.Events {
		output.Write(event.Bytes)
	}
	return output.String(), nil
}

// fetchRunResult reads the run's uploaded result document under an explicit
// deadline. A run that has not uploaded one yet answers 404, which is an
// ordinary "not yet" here rather than a failure.
func fetchRunResult(client *http.Client, runID string, budget time.Duration) ([]byte, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://run-ledger.invalid/v1/runs/"+runID+"/result", nil)
	if err != nil {
		return nil, false, err
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, false, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		return nil, false, fmt.Errorf("get run result = %d body=%s", response.StatusCode, body)
	}
	var result l3.RunResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, false, err
	}
	if result.SkipReason != "" {
		return nil, false, fmt.Errorf("run %s uploaded no document: %s", runID, result.SkipReason)
	}
	return result.Document, true, nil
}

// waitForResultJSON waits for the workflow's final "done:" line and then for
// the result document to arrive in the ledger.
//
// The document is no longer echoed into the run log, so this reads it where a
// person now reads it -- `wefty results`, over the same L3 route. The two waits
// are separate on purpose: the log settles when the shell exits, and the upload
// happens after that, when the node completes the attempt. Each request is
// bounded so one stalled read cannot consume the whole settle budget.
func waitForResultJSON(t *testing.T, client *http.Client, runID string, timeout time.Duration) ([]byte, string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var logs string
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		if remaining > perRequestBudget {
			remaining = perRequestBudget
		}
		if fetched, err := fetchRunLogs(client, runID, remaining); err == nil {
			logs = fetched
		}
		remaining = time.Until(deadline)
		if remaining <= 0 {
			break
		}
		if remaining > perRequestBudget {
			remaining = perRequestBudget
		}
		document, found, err := fetchRunResult(client, runID, remaining)
		if err != nil {
			if !time.Now().Before(deadline) {
				t.Fatalf("read run result within %s: %v; logs:\n%s", timeout, err, logs)
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if found && strings.Contains(logs, doneMarker) {
			return document, logs
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("run log does not carry %q and the ledger has no result document within %s:\n%s",
		doneMarker, timeout, logs)
	return nil, logs
}

func gateEvidenceContains(gate contract.GateResult, kind, value string) bool {
	for _, evidence := range gate.Evidence {
		if evidence.Kind == kind && evidence.Value == value {
			return true
		}
	}
	return false
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

func fabricHTTPClient(participant fabric.Fabric, address string) *http.Client {
	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return participant.Dial(ctx, network, address)
	}}
	return &http.Client{Transport: transport}
}

func serve(function func() error) <-chan error {
	done := make(chan error, 1)
	go func() { done <- function() }()
	return done
}

func writeFile(t *testing.T, repo, name, content string) {
	t.Helper()
	path := filepath.Join(repo, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	mode := os.FileMode(0o600)
	if strings.HasSuffix(name, ".sh") {
		mode = 0o700
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
}

func runGit(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

// TestEmbeddedInlineWriterMatchesTheShippedTemplate pins the copy. The workflow
// embeds the inline mailbox writer so an image without the wefty binary -- the
// upstream golang image the OCI examples use -- can still report, and a copy
// that drifts from the shipped template is a second, subtly different protocol
// producer. Byte-identical or nothing.
func TestEmbeddedInlineWriterMatchesTheShippedTemplate(t *testing.T) {
	script, err := os.ReadFile("branch-gates.sh")
	if err != nil {
		t.Fatal(err)
	}
	const begin = "# >>> BEGIN embedded inline run-mailbox writer >>>\n"
	const end = "# <<< END embedded inline run-mailbox writer <<<\n"
	start := strings.Index(string(script), begin)
	if start < 0 {
		t.Fatal("branch-gates.sh carries no embedded inline writer block")
	}
	rest := string(script)[start+len(begin):]
	stop := strings.Index(rest, end)
	if stop < 0 {
		t.Fatal("the embedded inline writer block is not terminated")
	}
	if embedded, want := rest[:stop], workflowhelper.InlineBashWriter(); embedded != want {
		t.Fatalf("the embedded inline writer has drifted from the shipped template; "+
			"copy internal/workflowhelper/templates/inline-writer.sh back between the markers "+
			"(embedded %d bytes, template %d bytes)", len(embedded), len(want))
	}
}

// TestBranchGatesReportsWithoutTheWeftyBinary is the fallback under the
// condition that matters: the helper absent and the workflow failing. The
// verdict document must still reach the handoff directory, because that is what
// an operator reads off the node, and it must not depend on a reporter that is
// not there.
func TestBranchGatesReportsWithoutTheWeftyBinary(t *testing.T) {
	bashPath, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is required to run the workflow script")
	}
	handoff := t.TempDir()
	mailbox := filepath.Join(handoff, ".wefty", "run_without_wefty")
	for _, directory := range []string{filepath.Join(mailbox, "tmp"), filepath.Join(mailbox, "events")} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	// No ref: the input failure path, which is the one that reports last and
	// would lose its result document to an absent helper.
	if err := os.WriteFile(filepath.Join(mailbox, "params.json"), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}

	command := exec.Command(bashPath, "branch-gates.sh")
	// A PATH with no wefty on it, and deliberately nothing else from this
	// process: the point is a rootfs that never had the binary.
	command.Env = []string{
		"PATH=/usr/bin:/bin:/usr/sbin:/sbin",
		"WEFTY_RUN_ID=run_without_wefty",
		"WEFTY_RUN_DIR=" + mailbox,
		"WEFTY_HANDOFF_DIR=" + handoff,
		"BRANCH_GATES_WORK_ROOT=" + t.TempDir(),
	}
	output, err := command.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("exit = %v, want 1 on the failure path:\n%s", err, output)
	}
	if !strings.Contains(string(output), "reporting with the inline mailbox writer") {
		t.Fatalf("the workflow did not fall back to the inline writer:\n%s", output)
	}

	retained, err := os.ReadFile(filepath.Join(handoff, "result.json"))
	if err != nil {
		t.Fatalf("no result.json survived an absent helper: %v\n%s", err, output)
	}
	var result struct {
		Workflow      string `json:"workflow"`
		Passed        bool   `json:"passed"`
		WorkflowError struct {
			Step    string `json:"step"`
			Message string `json:"message"`
		} `json:"workflow_error"`
	}
	if err := json.Unmarshal(retained, &result); err != nil {
		t.Fatalf("decode retained result.json %q: %v", retained, err)
	}
	if result.Workflow != "branch-gates" || result.Passed || result.WorkflowError.Step == "" {
		t.Fatalf("retained result.json is not a workflow error: %s", retained)
	}
	if _, err := os.Stat(filepath.Join(handoff, "failures.txt")); err != nil {
		t.Fatalf("no failures.txt survived an absent helper: %v", err)
	}
	// The events are written too, so a node agent would publish them.
	events, err := os.ReadDir(filepath.Join(mailbox, "events"))
	if err != nil || len(events) == 0 {
		t.Fatalf("the inline writer wrote no mailbox events: %v", err)
	}
}

// stepWatch polls the general Run listing while a run is going and holds the
// first current step it sees -- or the failure that stopped it looking. A
// watcher that swallowed its own errors would let this exercise pass with
// observation completely broken, which is the one thing it exists to prove.
type stepWatch struct {
	done chan struct{}
	mu   sync.Mutex
	step string
	err  error
}

func newStepWatch(ctx context.Context, client *http.Client, runID string) *stepWatch {
	watch := &stepWatch{done: make(chan struct{})}
	go watch.poll(ctx, client, runID)
	return watch
}

func (w *stepWatch) poll(ctx context.Context, client *http.Client, runID string) {
	defer close(w.done)
	for ctx.Err() == nil {
		step, err := currentStepOf(ctx, client, runID)
		if err != nil {
			w.finish("", err)
			return
		}
		if step != "" {
			w.finish(step, nil)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (w *stepWatch) finish(step string, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.step, w.err = step, err
}

// result reports what the watch saw. An unfinished watch means the run ended
// without the listing ever naming a step, which is a failure of the thing under
// test rather than a timing accident: one gate holds long enough to be seen.
func (w *stepWatch) result() (string, error) {
	select {
	case <-w.done:
	case <-time.After(logSettleBudget):
		return "", fmt.Errorf("the run listing watch did not settle within %s", logSettleBudget)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return "", w.err
	}
	if w.step == "" {
		return "", fmt.Errorf("the run listing never named a current step while the run was going")
	}
	return w.step, nil
}

// currentStepOf reads the same route, with the same parameters, that
// `wefty runs list` reads.
func currentStepOf(ctx context.Context, client *http.Client, runID string) (string, error) {
	requestCtx, cancel := context.WithTimeout(ctx, perRequestBudget)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet,
		"http://run-ledger.invalid/v1/runs?limit=100", nil)
	if err != nil {
		return "", err
	}
	response, err := client.Do(request)
	if err != nil {
		// The ledger is in this process; a refused or dropped request is a
		// real failure rather than weather.
		return "", fmt.Errorf("read run listing: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		return "", fmt.Errorf("read run listing = %d body=%s", response.StatusCode, body)
	}
	var page l3.RunListPage
	if err := json.NewDecoder(response.Body).Decode(&page); err != nil {
		return "", fmt.Errorf("decode run listing: %w", err)
	}
	for _, run := range page.Runs {
		if run.RunID == runID {
			return run.CurrentStep, nil
		}
	}
	return "", nil
}

// isStepBracket reports the envelopes a workload wrote with `wefty run step`.
// It reads the mailbox's own extension namespace, exactly as the ledger's
// derivation does.
func isStepBracket(envelope contract.Envelope) bool {
	if len(envelope.Extensions) == 0 {
		return false
	}
	var namespaces map[string]struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(envelope.Extensions, &namespaces); err != nil {
		return false
	}
	return namespaces["dev.wefty.mailbox"].Kind == "step"
}
