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
	nodeID    = "branch-gates-node"
	allGates  = "gofmt,fabric-boundary,vet,test"
	runBudget = 10 * time.Minute
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
		t.Skip("bash is required to check the workflow script")
	}
	if output, err := exec.Command(bashPath, "-n", "branch-gates.sh").CombinedOutput(); err != nil {
		t.Fatalf("bash -n branch-gates.sh: %v\n%s", err, output)
	}
	shellcheckPath, err := exec.LookPath("shellcheck")
	if err != nil {
		t.Log("shellcheck is not installed; NOT-RUN")
		return
	}
	if output, err := exec.Command(shellcheckPath, "branch-gates.sh").CombinedOutput(); err != nil {
		t.Fatalf("shellcheck branch-gates.sh: %v\n%s", err, output)
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
			wantOutcomes: map[string]string{"gofmt": "fail", "fabric-boundary": "pass", "vet": "pass", "test": "fail"},
			wantFailures: []string{"unformatted.go", "TestDeliberateFailure", "FAIL"},
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
			logs := runLogs(t, caller, accepted.RunID)
			if record.Status != testCase.wantRunState {
				t.Fatalf("run status = %q, want %q; logs:\n%s", record.Status, testCase.wantRunState, logs)
			}

			// result.json is echoed into the run log on both paths, because
			// that is the only result surface an OCI run has today.
			raw := resultJSONFromLogs(t, logs)
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
				if strings.Contains(string(failures), "===== fabric-boundary") || strings.Contains(string(failures), "===== vet") {
					t.Fatalf("failures.txt carries output from a passing gate:\n%s", failures)
				}
			}

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

			if strings.Contains(logs, "Bearer ") {
				t.Fatalf("run log leaks an authorization header:\n%s", logs)
			}
		})
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
	writeFile(t, repo, "broken_test.go", "package subject\n\nimport \"testing\"\n\nfunc TestDeliberateFailure(t *testing.T) {\n\tt.Fatalf(\"deliberate branch-gates failure\")\n}\n")
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

func runLogs(t *testing.T, client *http.Client, runID string) string {
	t.Helper()
	response, err := client.Get("http://run-ledger.invalid/v1/runs/" + runID + "/logs?limit=1000")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("get run logs = %d body=%s", response.StatusCode, body)
	}
	var page l1.LogPage
	if err := json.NewDecoder(response.Body).Decode(&page); err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	for _, event := range page.Events {
		output.Write(event.Bytes)
	}
	return output.String()
}

// resultJSONFromLogs extracts the result.json line the workflow echoes into
// the run log.
func resultJSONFromLogs(t *testing.T, logs string) []byte {
	t.Helper()
	for _, line := range strings.Split(logs, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, `{"schema_version":1,"workflow":"branch-gates"`) {
			return []byte(line)
		}
	}
	t.Fatalf("run log does not carry result.json:\n%s", logs)
	return nil
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
