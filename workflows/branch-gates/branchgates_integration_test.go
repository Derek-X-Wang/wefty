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
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
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
	// logSettleBudget bounds the wait for the agent's final log upload after
	// the run is already terminal: a failing gate makes L3 terminal before the
	// shell exits, so the result.json line can still be in flight.
	logSettleBudget = 60 * time.Second
	resultPrefix    = `{"schema_version":1,"workflow":"branch-gates"`
	// doneMarker is the workflow's last log line on every path, so the poll
	// below can wait for a settled log instead of guessing.
	doneMarker = "branch-gates: done"
	// perRequestBudget caps one log read so a stalled request cannot consume
	// the whole settle budget.
	perRequestBudget = 10 * time.Second
	probeMarker      = "wefty environment: ["
)

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
	if _, err := exec.LookPath("curl"); err != nil {
		t.Fatal(err)
	}

	subject := initializeSubjectRepository(t)
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
				Params:           params,
				Tags:             []string{contract.StableNodeTagPrefix + nodeID},
				Limits:           &contract.RunLimits{MaxRuntimeSeconds: int(runBudget.Seconds())},
				RequiredEnvelope: true,
			}, "branch-gates-exercise-"+testCase.ref)

			record := waitForTerminalRun(t, store, accepted.RunID, runBudget)
			// result.json is echoed into the run log on both paths, because
			// that is the only result surface an OCI run has today.
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

			// Handoff lifecycle: the node agent removes the directory as soon
			// as its attempt succeeds, so the files survive exactly on the
			// path an operator cares about — a failing verdict.
			handoff := filepath.Join(l3.DefaultHandoffRoot, accepted.RunID)
			if testCase.wantPassed {
				if _, err := os.Stat(handoff); !os.IsNotExist(err) {
					t.Fatalf("handoff %s survived a succeeding attempt: %v", handoff, err)
				}
			} else {
				retained, err := os.ReadFile(filepath.Join(handoff, "result.json"))
				if err != nil {
					t.Fatalf("read retained result.json: %v; logs:\n%s", err, logs)
				}
				if !bytes.Equal(bytes.TrimSpace(retained), bytes.TrimSpace(raw)) {
					t.Fatalf("retained result.json differs from the logged one:\n%s\n%s", retained, raw)
				}
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
			assertScratchRemoved(t, accepted.RunID)

			// The ledger carries one envelope per gate plus the single final
			// gate result appended by hand through the L3 API.
			gotSteps := make([]string, 0, len(record.Envelopes))
			for _, envelope := range record.Envelopes {
				gotSteps = append(gotSteps, envelope.StepID)
				if envelope.AttemptID == "" {
					t.Fatalf("envelope %s carries no server-bound attempt", envelope.EnvelopeID)
				}
				wantStatus := contract.EnvelopeSucceeded
				if testCase.wantOutcomes[envelope.StepID] == "fail" {
					wantStatus = contract.EnvelopeFailed
				}
				if envelope.Status != wantStatus {
					t.Fatalf("envelope %s status = %q, want %q", envelope.StepID, envelope.Status, wantStatus)
				}
			}
			if want := strings.Split(allGates, ","); !equalStrings(gotSteps, want) {
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
			if !gateEvidenceContains(finalGate, "commit", result.Commit) {
				t.Fatalf("final gate evidence = %#v, want the resolved commit", finalGate.Evidence)
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

			if len(record.Envelopes) != 1 || record.Envelopes[0].StepID != testCase.wantStep ||
				record.Envelopes[0].Status != contract.EnvelopeFailed {
				t.Fatalf("envelopes = %#v, want one failed %q envelope", record.Envelopes, testCase.wantStep)
			}
			if len(record.Gates) != 1 || record.Gates[0].Name != "branch-gates" ||
				record.Gates[0].Outcome != contract.GateError {
				t.Fatalf("gates = %#v, want one branch-gates gate with outcome error", record.Gates)
			}
			assertCredentialProbe(t, "run log", logs, 0)
			assertScratchRemoved(t, accepted.RunID)
		})
	}
}

// assertScratchRemoved pins the workflow's own cleanup: the scratch directory
// holds the clone, the Go caches and the 0600 curl config carrying the run
// token, so it must not outlive the run on either path.
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
				BaseEnvironment: os.Environ(),
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

// waitForResultJSON polls the run log until the workflow's final "done:" line
// has arrived and result.json is present. A failing gate makes the run terminal
// in L3 before the shell exits, so both the result line and the failure output
// can still be in flight when the run already reads as failed. Each request is
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
		fetched, err := fetchRunLogs(client, runID, remaining)
		if err != nil {
			if !time.Now().Before(deadline) {
				t.Fatalf("read run logs within %s: %v", timeout, err)
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		logs = fetched
		if strings.Contains(logs, doneMarker) {
			for _, line := range strings.Split(logs, "\n") {
				line = strings.TrimSpace(line)
				if strings.HasPrefix(line, resultPrefix) {
					return []byte(line), logs
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("run log does not carry %q and result.json within %s:\n%s", doneMarker, timeout, logs)
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
