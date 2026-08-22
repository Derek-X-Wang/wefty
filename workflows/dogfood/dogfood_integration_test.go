//go:build darwin || linux

package dogfood_test

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

func TestDogfoodWorkflowContractSmoke(t *testing.T) {
	bundlePath := filepath.Join("dist", "dogfood-workflow.mjs")
	bundle, err := os.ReadFile(bundlePath)
	if os.IsNotExist(err) {
		t.Skip("run npm test in workflows/dogfood to build and exercise the workflow bundle")
	}
	if err != nil {
		t.Fatal(err)
	}
	nodePath, err := exec.LookPath("node")
	if err != nil {
		t.Fatal(err)
	}

	targetRepo := initializeTargetRepository(t)
	fakeBin, fakeHome, invocationLog := writeFakeAgents(t)
	network := plain.NewNetwork()
	controlFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	ledgerFabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger", Tags: []string{l1.DefaultClientPrincipalTag}})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-agent", Tags: []string{l1.DefaultAgentPrincipalTag}})
	callerFabric := network.NewFabric(fabric.Identity{NodeID: "caller", Tags: []string{l3.DefaultCallerPrincipalTag}})

	l1Store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer l1Store.Close()
	l1Server, err := l1.NewServer(controlFabric, l1Store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{
		"dogfood-node": l1.DefaultNodePolicy("linux", contract.StableNodeTagPrefix+"dogfood-node"),
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
	defer l3Store.Close()
	ledgerL1Client, err := l3.NewL1Client(ledgerFabric, l3.DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	defer ledgerL1Client.CloseIdleConnections()
	reconciler, err := l3.NewReconciler(l3Store, ledgerL1Client, l3.ReconcilerConfig{Interval: 10 * time.Millisecond})
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
	l1Done := serve(ctx, func() error { return l1Server.Serve(ctx, l1Listener) })
	l3Done := serve(ctx, func() error { return l3Server.Serve(ctx, l3Listener) })

	baseEnvironment := withoutEnvironment(os.Environ(), "HOME", "PATH", "FAKE_AGENT_LOG")
	baseEnvironment = append(baseEnvironment,
		"HOME="+fakeHome,
		"PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"FAKE_AGENT_LOG="+invocationLog,
	)
	nodeAgent, err := agent.New(agent.Config{
		Fabric: agentFabric, ControlPlaneAddress: l3.DefaultL1Address, RunLedgerAddress: l3.DefaultL3Address,
		NodeID: "dogfood-node", BootSessionID: "boot-dogfood", Version: "contract-smoke",
		OS: "linux", Architecture: "amd64", Capabilities: map[string]bool{"kind:process": true},
		HeartbeatInterval: 50 * time.Millisecond, ClaimInterval: 10 * time.Millisecond,
		RenewalInterval: 50 * time.Millisecond, LogFlushInterval: 5 * time.Millisecond, LogRetryInterval: 5 * time.Millisecond,
		LogSpoolDirectory: t.TempDir(),
		Runner:            processrunner.New(processrunner.Config{BaseEnvironment: baseEnvironment}),
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	agentDone := serve(ctx, func() error { return nodeAgent.Run(ctx) })
	t.Cleanup(func() {
		cancel()
		for name, done := range map[string]<-chan error{"agent": agentDone, "L3": l3Done, "L1": l1Done} {
			if err := <-done; err != nil {
				t.Errorf("%s server: %v", name, err)
			}
		}
	})

	caller := fabricHTTPClient(callerFabric, l3.DefaultL3Address)
	defer caller.CloseIdleConnections()
	workflowPackagePath, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	params, err := json.Marshal(map[string]any{
		"task":                  "Add the dogfood smoke marker and commit it",
		"repo_path":             targetRepo,
		"base_branch":           "main",
		"workflow_package_path": workflowPackagePath,
	})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(bundle)
	request := l3.CreateRunRequest{
		InlineScript: &l3.InlineScriptInput{
			Content: string(bundle), SHA256: hex.EncodeToString(digest[:]), Interpreter: []string{nodePath},
		},
		Params:           params,
		Tags:             []string{"linux", contract.StableNodeTagPrefix + "dogfood-node"},
		Limits:           &contract.RunLimits{MaxRuntimeSeconds: 120},
		RequiredEnvelope: true,
	}
	root := submitRun(t, caller, request)
	rootRecord := waitForTerminalRun(t, l3Store, root.RunID, 30*time.Second)
	if rootRecord.Status != contract.RunSucceeded {
		t.Fatalf("root run status = %q envelopes=%#v gates=%#v logs=\n%s", rootRecord.Status, rootRecord.Envelopes, rootRecord.Gates, getRunLogs(t, caller, root.RunID))
	}

	lineage, err := l3Store.GetLineage(context.Background(), root.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(lineage.Descendants) != 2 || lineage.Descendants[0].Depth != 1 || lineage.Descendants[1].Depth != 2 {
		t.Fatalf("dogfood lineage = %#v", lineage.Descendants)
	}
	records := []contract.RunRecord{rootRecord}
	for _, descendant := range lineage.Descendants {
		record, err := l3Store.GetRun(context.Background(), descendant.RunID)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	wantSteps := []string{"plan", "implement", "review"}
	for index, record := range records {
		if record.Status != contract.RunSucceeded || record.NodeID != "dogfood-node" || len(record.Envelopes) != 1 || len(record.Gates) != 1 {
			t.Fatalf("run %s protocol record = status %q node %q envelopes %d gates %d", record.RunID, record.Status, record.NodeID, len(record.Envelopes), len(record.Gates))
		}
		if record.Envelopes[0].StepID != wantSteps[index] || record.Gates[0].StepID != wantSteps[index] || record.Envelopes[0].AttemptID == "" || record.Gates[0].AttemptID == "" {
			t.Fatalf("run %s protocol identities = envelope %#v gate %#v", record.RunID, record.Envelopes[0], record.Gates[0])
		}
		if record.Gates[0].Outcome != contract.GatePass {
			t.Fatalf("run %s gate outcome = %q", record.RunID, record.Gates[0].Outcome)
		}
	}

	if output := runGit(t, targetRepo, "show", extensionsBranch(records[1].Envelopes[0])+":dogfood-smoke.txt"); strings.TrimSpace(output) != "fake codex implementation" {
		t.Fatalf("implementation branch marker = %q", output)
	}
	invocations, err := os.ReadFile(invocationLog)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(invocations), "claude plan-work\nclaude plan-emit\ncodex implement-work\ncodex implement-emit\nclaude review-work\nclaude review-emit\n"; got != want {
		t.Fatalf("fake agent invocations = %q, want %q", got, want)
	}

	logs := getRunLogs(t, caller, root.RunID)
	if !strings.Contains(logs, "starting dogfood plan step") || !strings.Contains(logs, "dispatched implement child run") {
		t.Fatalf("root logs do not expose workflow progress:\n%s", logs)
	}
}

func initializeTargetRepository(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	runGit(t, repo, "init", "-b", "main")
	runGit(t, repo, "config", "user.name", "Wefty Smoke")
	runGit(t, repo, "config", "user.email", "wefty-smoke@example.test")
	if err := os.WriteFile(filepath.Join(repo, "README.md"), []byte("# dogfood smoke\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, repo, "add", "README.md")
	runGit(t, repo, "commit", "-m", "initial")
	return repo
}

func writeFakeAgents(t *testing.T) (string, string, string) {
	t.Helper()
	directory := t.TempDir()
	home := t.TempDir()
	logPath := filepath.Join(directory, "invocations.log")
	claude := `#!/bin/sh
prompt=$(cat)
case "$prompt" in
  *'Return exactly one <plan>'*)
    phase='plan-emit'
    session_id='fake-claude-plan'
    expected_resume="$session_id"
    output='<plan>Write a marker file, test it, and commit the change.</plan>'
    ;;
  *'Return exactly <review>'*)
    phase='review-emit'
    session_id='fake-claude-review'
    expected_resume="$session_id"
    output='<review>PASS</review>'
    ;;
  *'Cross-review branch'*)
    phase='review-work'
    session_id='fake-claude-review'
    expected_resume=''
    output='WEFTY_WORK_COMPLETE'
    ;;
  *)
    phase='plan-work'
    session_id='fake-claude-plan'
    expected_resume=''
    output='WEFTY_WORK_COMPLETE'
    ;;
esac
resume=''
previous=''
for argument in "$@"; do
  if [ "$previous" = '--resume' ]; then resume="$argument"; fi
  previous="$argument"
done
if [ "$resume" != "$expected_resume" ]; then
  printf 'unexpected Claude resume session: got %s want %s\n' "$resume" "$expected_resume" >&2
  exit 2
fi
mkdir -p "$HOME/.claude/projects/fake"
printf '{"type":"session"}\n' > "$HOME/.claude/projects/fake/$session_id.jsonl"
printf 'claude %s\n' "$phase" >> "$FAKE_AGENT_LOG"
printf '{"type":"system","subtype":"init","session_id":"%s"}\n' "$session_id"
printf '{"type":"assistant","message":{"content":[{"type":"text","text":"%s"}]}}\n' "$output"
printf '{"type":"result","result":"%s"}\n' "$output"
`
	codex := `#!/bin/sh
prompt=$(cat)
session_id='fake-codex-implement'
case "$prompt" in
  *'<implementation>COMPLETE</implementation>'*)
    phase='implement-emit'
    if [ "$1" != 'exec' ] || [ "$2" != 'resume' ] || [ "$3" != "$session_id" ]; then
      printf 'unexpected Codex resume arguments: %s\n' "$*" >&2
      exit 2
    fi
    output='<implementation>COMPLETE</implementation>'
    ;;
  *)
    phase='implement-work'
    if [ "$1" != 'exec' ] || [ "$2" = 'resume' ]; then
      printf 'unexpected initial Codex arguments: %s\n' "$*" >&2
      exit 2
    fi
    printf 'fake codex implementation\n' > dogfood-smoke.txt
    git add dogfood-smoke.txt
    git commit -m 'feat: add dogfood smoke marker' >/dev/null
    output='WEFTY_WORK_COMPLETE'
    ;;
esac
mkdir -p "$HOME/.codex/sessions/fake"
printf '{"type":"session_meta","payload":{"id":"%s"}}\n' "$session_id" > "$HOME/.codex/sessions/fake/rollout-test-$session_id.jsonl"
printf 'codex %s\n' "$phase" >> "$FAKE_AGENT_LOG"
printf '{"type":"thread.started","thread_id":"%s"}\n' "$session_id"
printf '{"type":"item.completed","item":{"type":"agent_message","text":"%s"}}\n' "$output"
`
	for name, content := range map[string]string{"claude": claude, "codex": codex} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(content), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return directory, home, logPath
}

func submitRun(t *testing.T, client *http.Client, input l3.CreateRunRequest) l3.RunAccepted {
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
	request.Header.Set("Idempotency-Key", "dogfood-contract-smoke")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	responseBody, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("submit root run = %d body=%s", response.StatusCode, responseBody)
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
		time.Sleep(20 * time.Millisecond)
	}
	lineage, lineageErr := store.GetLineage(context.Background(), runID)
	if lineageErr != nil {
		t.Fatalf("run %s did not become terminal within %s; lineage: %v", runID, timeout, lineageErr)
	}
	states := make([]string, 0, len(lineage.Descendants)+1)
	for _, id := range append([]string{runID}, lineageRunIDs(lineage.Descendants)...) {
		record, err := store.GetRun(context.Background(), id)
		if err != nil {
			states = append(states, id+": "+err.Error())
			continue
		}
		states = append(states, fmt.Sprintf("%s=%s envelopes=%d gates=%d", id, record.Status, len(record.Envelopes), len(record.Gates)))
	}
	t.Fatalf("run %s did not become terminal within %s; %s", runID, timeout, strings.Join(states, "; "))
	return contract.RunRecord{}
}

func lineageRunIDs(entries []l3.LineageEntry) []string {
	ids := make([]string, len(entries))
	for index, entry := range entries {
		ids[index] = entry.RunID
	}
	return ids
}

func getRunLogs(t *testing.T, client *http.Client, runID string) string {
	t.Helper()
	response, err := client.Get("http://run-ledger.invalid/v1/runs/" + runID + "/logs?limit=1000")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("get root logs = %d body=%s", response.StatusCode, body)
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

func fabricHTTPClient(participant fabric.Fabric, address string) *http.Client {
	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return participant.Dial(ctx, network, address)
	}}
	return &http.Client{Transport: transport}
}

func serve(_ context.Context, function func() error) <-chan error {
	done := make(chan error, 1)
	go func() { done <- function() }()
	return done
}

func withoutEnvironment(environment []string, names ...string) []string {
	prefixes := make([]string, len(names))
	for index, name := range names {
		prefixes[index] = name + "="
	}
	filtered := make([]string, 0, len(environment))
	for _, entry := range environment {
		keep := true
		for _, prefix := range prefixes {
			if strings.HasPrefix(entry, prefix) {
				keep = false
				break
			}
		}
		if keep {
			filtered = append(filtered, entry)
		}
	}
	return filtered
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

func extensionsBranch(envelope contract.Envelope) string {
	var extensions map[string]struct {
		Branch string `json:"branch"`
	}
	if err := json.Unmarshal(envelope.Extensions, &extensions); err != nil {
		return ""
	}
	return extensions["dev.wefty.dogfood"].Branch
}
