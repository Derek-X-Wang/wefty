// Package issuetopr_test exercises the issue-to-pr workflow.
//
// Nothing here reaches GitHub, and nothing here runs a real coding agent. The
// origin is a bare repository in a temporary directory, `gh` is a stub on PATH
// that serves a fixture issue and records the arguments it was asked to open a
// pull request with, and the agent is a script that writes a file and commits
// it. What is under test is the workflow's own mechanics: the phase brackets,
// the marker commits, the gates, the documents it hands back, and the
// resumption that those marker commits exist for.
//
// The real run -- Derek's Mac, his `gh` login, his agent login, a real issue --
// is attended and deliberately not automated here.
package issuetopr_test

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
	"regexp"
	"runtime"
	"slices"
	"strings"
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

func TestIssueToPRScriptIsValidShell(t *testing.T) {
	bashPath, err := exec.LookPath("bash")
	if err != nil {
		// Every supported runner has bash; a missing one in CI is a broken
		// lane, not a reason to report a green skip.
		if os.Getenv("GITHUB_ACTIONS") == "true" {
			t.Fatalf("bash is required in CI: %v", err)
		}
		t.Skip("bash is required to check the workflow script")
	}
	if output, err := exec.Command(bashPath, "-n", "issue-to-pr.sh").CombinedOutput(); err != nil {
		t.Fatalf("bash -n issue-to-pr.sh: %v\n%s", err, output)
	}
	shellcheckPath, err := exec.LookPath("shellcheck")
	if err != nil {
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
	if output, err := exec.Command(shellcheckPath, "issue-to-pr.sh").CombinedOutput(); err != nil {
		version, _ := exec.Command(shellcheckPath, "--version").Output()
		t.Fatalf("shellcheck issue-to-pr.sh: %v\n%s\n%s", err, output, version)
	}
}

// TestEmbeddedInlineWriterMatchesTheShippedTemplate pins the copy, for the same
// reason branch-gates does: a drifted copy is a second, subtly different
// protocol producer.
func TestEmbeddedInlineWriterMatchesTheShippedTemplate(t *testing.T) {
	script, err := os.ReadFile("issue-to-pr.sh")
	if err != nil {
		t.Fatal(err)
	}
	const begin = "# >>> BEGIN embedded inline run-mailbox writer >>>\n"
	const end = "# <<< END embedded inline run-mailbox writer <<<\n"
	start := strings.Index(string(script), begin)
	if start < 0 {
		t.Fatal("issue-to-pr.sh carries no embedded inline writer block")
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

// TestTheWorkflowNeverWritesTheEnvironmentIntoItsDocuments is the secrets rule
// read off the script rather than trusted: the documents this workflow hands
// back are built from the issue, the plan and the gate output, and nothing in
// it copies the environment anywhere a reader can see.
func TestTheWorkflowNeverWritesTheEnvironmentIntoItsDocuments(t *testing.T) {
	script, err := os.ReadFile("issue-to-pr.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{
		"env >", "env |", "printenv", "$GITHUB_TOKEN", "$GH_TOKEN",
	} {
		if strings.Contains(string(script), forbidden) {
			t.Fatalf("the workflow contains %q, which can put the environment where a reader sees it", forbidden)
		}
	}
	// The one place it names credentials is the boundary line it logs, which
	// prints the names it did NOT expect to find and never their values.
	if !strings.Contains(string(script), "credentials in the workflow environment") {
		t.Fatal("the workflow no longer states its credential boundary")
	}
}

// --------------------------------------------------------------------------
// The stack exercise
//
// A real L1, L3 and node agent in this process; a bare repository as origin; a
// `gh` stub and a fake agent on PATH. Nothing reaches GitHub and no real coding
// agent runs, which is the point: what is proved here is the workflow's own
// mechanics, and the run with real logins is attended.
// --------------------------------------------------------------------------

const (
	nodeID           = "issue-to-pr-node"
	runBudget        = 4 * time.Minute
	settleBudget     = 60 * time.Second
	perRequestBudget = 10 * time.Second
	doneMarker       = "issue-to-pr: done"
	issueNumber      = "479"
)

func TestIssueToPRHandsBackADraftPullRequest(t *testing.T) {
	requireExercise(t)
	origin := initializeOriginRepository(t)
	tools := installStubTools(t, origin)
	caller, store := startStack(t)

	accepted := submitIssueToPR(t, caller, map[string]string{
		"issue": issueNumber, "repo": "example/subject", "agent": "claude",
	})
	record := waitForTerminalRun(t, store, accepted.RunID, runBudget)
	logs := runLogs(t, caller, accepted.RunID, settleBudget)
	if record.Status != contract.RunSucceeded {
		t.Fatalf("run status = %q, want succeeded; logs:\n%s", record.Status, logs)
	}

	// Every phase is a closed step interval with a duration, in order, which is
	// what makes `wefty runs list` name the phase a run is in.
	steps := l3.DeriveRunSteps(record.Envelopes)
	if steps.Current != "" {
		t.Fatalf("a terminal run is still in step %q", steps.Current)
	}
	wantPhases := []string{"read-issue", "plan", "implement", "gates", "push", "open-pr"}
	assertPhasesRan(t, steps, wantPhases, logs)

	// The repository gates ran and passed, and the workflow's own gate is the
	// verdict.
	outcomes := map[string]contract.GateOutcome{}
	for _, gate := range record.Gates {
		outcomes[gate.Name] = gate.Outcome
	}
	for _, gate := range []string{"gofmt", "vet", "test"} {
		if outcomes[gate] != contract.GatePass {
			t.Fatalf("gate %s = %q, want pass; logs:\n%s", gate, outcomes[gate], logs)
		}
	}
	if outcomes["issue-to-pr"] != contract.GatePass {
		t.Fatalf("workflow gate = %q, want pass", outcomes["issue-to-pr"])
	}

	// The documents an operator and a script read.
	handoff := filepath.Join(l3.DefaultHandoffRoot, accepted.RunID)
	var pr struct {
		URL     string `json:"url"`
		HeadSHA string `json:"head_sha"`
		Branch  string `json:"branch"`
		Issue   string `json:"issue"`
		Repo    string `json:"repo"`
	}
	readJSONFile(t, filepath.Join(handoff, "pr.json"), &pr)
	if !strings.HasPrefix(pr.URL, "https://") || pr.Issue != issueNumber || pr.Repo != "example/subject" {
		t.Fatalf("pr.json = %#v", pr)
	}
	if len(pr.HeadSHA) != 40 {
		t.Fatalf("pr.json head_sha = %q, want a full commit ID", pr.HeadSHA)
	}
	summary, err := os.ReadFile(filepath.Join(handoff, "summary.md"))
	if err != nil {
		t.Fatalf("read summary.md: %v", err)
	}
	for _, want := range []string{"Implements #" + issueNumber, "## Plan", "draft"} {
		if !strings.Contains(string(summary), want) {
			t.Fatalf("summary.md is missing %q:\n%s", want, summary)
		}
	}

	// The result document reached the ledger, which is where `wefty results`
	// reads it.
	var result struct {
		Passed bool   `json:"passed"`
		Issue  string `json:"issue"`
		Branch string `json:"branch"`
		PRURL  string `json:"pr_url"`
	}
	if err := json.Unmarshal(uploadedResult(t, caller, accepted.RunID, settleBudget), &result); err != nil {
		t.Fatalf("decode the uploaded result: %v", err)
	}
	if !result.Passed || result.Issue != issueNumber || result.PRURL != pr.URL {
		t.Fatalf("uploaded result = %#v", result)
	}

	// `gh pr create` was asked for a draft, on the branch the run pushed, with
	// the plan in its body. The stub records what it was told rather than
	// guessing what GitHub would have done with it.
	recorded := readFile(t, tools.prArgsFile)
	for _, want := range []string{"--draft", "--repo example/subject", "--head " + result.Branch, "--base main"} {
		if !strings.Contains(recorded, want) {
			t.Fatalf("gh pr create was not asked for %q:\n%s", want, recorded)
		}
	}
	body := readFile(t, tools.prBodyFile)
	if !strings.Contains(body, "the plan the fake agent wrote") {
		t.Fatalf("the pull request body does not carry PLAN.md:\n%s", body)
	}

	// The branch really is on origin, with the agent's commit and every phase
	// marker on it.
	subjects := runGit(t, origin, "log", "--format=%s", result.Branch)
	for _, phase := range wantPhases {
		if !strings.Contains(subjects, "issue-to-pr: phase "+phase+" complete") {
			t.Fatalf("origin has no marker for phase %s:\n%s", phase, subjects)
		}
	}
	if !strings.Contains(subjects, "implement issue "+issueNumber) {
		t.Fatalf("origin has no commit from the agent:\n%s", subjects)
	}
	// And nothing from this process's environment reached the handoff files.
	assertNoSecretsInHandoff(t, handoff)
}

// TestIssueToPRResumesAtTheFirstIncompletePhase is what the marker commits are
// for. The first run is stopped after `implement`; the second is a cold run
// with `continue_from`, which clones the pushed branch, skips every phase whose
// marker is already there, and finishes the rest.
func TestIssueToPRResumesAtTheFirstIncompletePhase(t *testing.T) {
	requireExercise(t)
	origin := initializeOriginRepository(t)
	tools := installStubTools(t, origin)
	caller, store := startStack(t)

	// Stopping after `implement`: the stub `gh` refuses to open a pull request
	// and the gates are made to fail, so the first run dies at `gates` with
	// `implement` already marked and pushed. That is the same state a killed
	// run leaves behind, reached deterministically.
	writeFile(t, tools.controlDir, "fail-gates", "1")
	first := submitIssueToPR(t, caller, map[string]string{
		"issue": issueNumber, "repo": "example/subject",
	})
	firstRecord := waitForTerminalRun(t, store, first.RunID, runBudget)
	firstLogs := runLogs(t, caller, first.RunID, settleBudget)
	if firstRecord.Status != contract.RunFailed {
		t.Fatalf("the interrupted run ended %q, want failed; logs:\n%s", firstRecord.Status, firstLogs)
	}
	firstSteps := l3.DeriveRunSteps(firstRecord.Envelopes)
	assertPhasesRan(t, firstSteps, []string{"read-issue", "plan", "implement"}, firstLogs)

	branch := branchFromLogs(t, firstLogs)
	if branch == "" {
		t.Fatalf("the first run never named its branch:\n%s", firstLogs)
	}
	// The markers the first run pushed are what the second one will read.
	subjects := runGit(t, origin, "log", "--format=%s", branch)
	for _, phase := range []string{"read-issue", "plan", "implement"} {
		if !strings.Contains(subjects, "issue-to-pr: phase "+phase+" complete") {
			t.Fatalf("the interrupted run did not push the %s marker:\n%s", phase, subjects)
		}
	}
	if strings.Contains(subjects, "issue-to-pr: phase gates complete") {
		t.Fatalf("the interrupted run marked a phase it did not finish:\n%s", subjects)
	}

	// Now let the gates pass and resume.
	if err := os.Remove(filepath.Join(tools.controlDir, "fail-gates")); err != nil {
		t.Fatal(err)
	}
	second := submitIssueToPR(t, caller, map[string]string{
		"issue": issueNumber, "repo": "example/subject", "continue_from": branch,
	})
	secondRecord := waitForTerminalRun(t, store, second.RunID, runBudget)
	secondLogs := runLogs(t, caller, second.RunID, settleBudget)
	if secondRecord.Status != contract.RunSucceeded {
		t.Fatalf("the resumed run ended %q, want succeeded; logs:\n%s", secondRecord.Status, secondLogs)
	}

	// The completed phases are skipped -- no step bracket at all -- and the
	// rest run.
	secondSteps := l3.DeriveRunSteps(secondRecord.Envelopes)
	ran := map[string]bool{}
	for _, step := range secondSteps.Steps {
		ran[step.Name] = true
	}
	for _, skipped := range []string{"read-issue", "plan", "implement"} {
		if ran[skipped] {
			t.Fatalf("the resumed run re-ran phase %s:\n%s", skipped, secondLogs)
		}
		if !strings.Contains(secondLogs, "phase "+skipped+": already complete") {
			t.Fatalf("the resumed run did not report skipping %s:\n%s", skipped, secondLogs)
		}
	}
	assertPhasesRan(t, secondSteps, []string{"gates", "push", "open-pr"}, secondLogs)

	// The agent was never asked to do the work again, which is the whole
	// benefit: a resumed run does not pay for the phases already bought.
	if invocations := readFile(t, tools.agentCallsFile); strings.Count(invocations, "\n") != 2 {
		t.Fatalf("the agent was invoked %d times across both runs, want 2 (both in the first):\n%s",
			strings.Count(invocations, "\n"), invocations)
	}

	// And the resumed run still hands back the documents.
	handoff := filepath.Join(l3.DefaultHandoffRoot, second.RunID)
	var pr struct {
		URL    string `json:"url"`
		Branch string `json:"branch"`
	}
	readJSONFile(t, filepath.Join(handoff, "pr.json"), &pr)
	if pr.Branch != branch || !strings.HasPrefix(pr.URL, "https://") {
		t.Fatalf("the resumed run's pr.json = %#v", pr)
	}
}

// TestIssueToPRFailsWhenTheAgentProducesNothing is the stop condition that
// matters most: an agent that ran, exited zero and changed nothing must not
// reach a pull request.
func TestIssueToPRFailsWhenTheAgentProducesNothing(t *testing.T) {
	requireExercise(t)
	origin := initializeOriginRepository(t)
	tools := installStubTools(t, origin)
	caller, store := startStack(t)

	writeFile(t, tools.controlDir, "agent-does-nothing", "1")
	accepted := submitIssueToPR(t, caller, map[string]string{
		"issue": issueNumber, "repo": "example/subject",
	})
	record := waitForTerminalRun(t, store, accepted.RunID, runBudget)
	logs := runLogs(t, caller, accepted.RunID, settleBudget)
	if record.Status != contract.RunFailed {
		t.Fatalf("a run whose agent did nothing ended %q, want failed; logs:\n%s", record.Status, logs)
	}
	if !strings.Contains(logs, "produced no commit") {
		t.Fatalf("the failure does not name what went wrong:\n%s", logs)
	}
	// No pull request was attempted for work that does not exist.
	if _, err := os.Stat(tools.prArgsFile); err == nil {
		t.Fatalf("a pull request was opened for a run that changed nothing:\n%s", readFile(t, tools.prArgsFile))
	}
}

// --------------------------------------------------------------------------
// Stubs
// --------------------------------------------------------------------------

// stubTools is the fake world this exercise runs against: a `gh` that serves a
// fixture issue and records what it was asked to open, and an agent that writes
// a plan and a commit. Both are ordinary scripts on PATH, because that is how
// the workflow finds the real ones.
type stubTools struct {
	binDir         string
	controlDir     string
	prArgsFile     string
	prBodyFile     string
	agentCallsFile string
}

func installStubTools(t *testing.T, origin string) stubTools {
	t.Helper()
	root := t.TempDir()
	tools := stubTools{
		binDir:         filepath.Join(root, "bin"),
		controlDir:     filepath.Join(root, "control"),
		prArgsFile:     filepath.Join(root, "pr-args.txt"),
		prBodyFile:     filepath.Join(root, "pr-body.md"),
		agentCallsFile: filepath.Join(root, "agent-calls.txt"),
	}
	for _, directory := range []string{tools.binDir, tools.controlDir} {
		if err := os.MkdirAll(directory, 0o750); err != nil {
			t.Fatal(err)
		}
	}

	// `gh` serves the issue from a fixture and records `pr create`. It never
	// contacts GitHub, and it is deliberately dumb: it models the arguments and
	// the URL, which is all this workflow reads from it.
	writeExecutable(t, tools.binDir, "gh", fmt.Sprintf(`#!/usr/bin/env bash
set -u
control=%q
args_file=%q
body_file=%q
case "$1 ${2:-}" in
"issue view")
	if [ "${3:-}" != %q ]; then
		printf 'unknown issue %%s\n' "${3:-}" >&2
		exit 1
	fi
	for arg in "$@"; do
		if [ "$arg" = "--json" ]; then
			printf '{"number":%s,"title":"Add a greeting","body":"The subject should greet.","labels":[],"url":"https://github.invalid/example/subject/issues/%s"}\n'
			exit 0
		fi
	done
	printf 'title:\tAdd a greeting\nnumber:\t%s\n\n  The subject should greet.\n'
	exit 0
	;;
"pr create")
	if [ -f "$control/fail-pr" ]; then
		printf 'pull request refused by the stub\n' >&2
		exit 1
	fi
	printf '%%s\n' "$*" >"$args_file"
	body=
	previous=
	for arg in "$@"; do
		if [ "$previous" = "--body-file" ]; then body=$arg; fi
		previous=$arg
	done
	[ -z "$body" ] || cp "$body" "$body_file"
	printf 'https://github.invalid/example/subject/pull/1\n'
	exit 0
	;;
esac
printf 'the gh stub does not implement: %%s\n' "$*" >&2
exit 1
`, tools.controlDir, tools.prArgsFile, tools.prBodyFile, issueNumber, issueNumber, issueNumber, issueNumber))

	// The fake agent. It is invoked the way the documented test seam says:
	// `CMD <phase> <prompt-file>`, with the worktree as its working directory.
	writeExecutable(t, tools.binDir, "fake-agent", fmt.Sprintf(`#!/usr/bin/env bash
set -u
control=%q
calls=%q
phase=$1
prompt=$2
printf '%%s %%s\n' "$phase" "$prompt" >>"$calls"
[ -s "$prompt" ] || { printf 'the agent was given an empty prompt\n' >&2; exit 1; }
case $phase in
plan)
	printf '# Plan\n\nthe plan the fake agent wrote for issue %s.\n' >PLAN.md
	;;
implement)
	if [ -f "$control/agent-does-nothing" ]; then
		exit 0
	fi
	printf 'package subject\n\n// Greeting is what the issue asked for.\nfunc Greeting() string { return "hello" }\n' >greeting.go
	git add -A
	git -c user.name=agent -c user.email=agent@wefty.invalid commit --quiet -m "implement issue %s"
	;;
esac
exit 0
`, tools.controlDir, tools.agentCallsFile, issueNumber, issueNumber))

	// A `go` shim that can be made to fail, so the exercise can stop a run at
	// the gates without killing the process and racing the agent's publication.
	goPath, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	writeExecutable(t, tools.binDir, "go", fmt.Sprintf(`#!/usr/bin/env bash
set -u
if [ -f %q/fail-gates ]; then
	printf 'the gate shim was told to fail\n' >&2
	exit 1
fi
exec %q "$@"
`, tools.controlDir, goPath))

	path := tools.binDir
	if existing := os.Getenv("PATH"); existing != "" {
		path = tools.binDir + string(os.PathListSeparator) + existing
	}
	t.Setenv("PATH", path)
	t.Setenv("WEFTY_ISSUE_TO_PR_AGENT_CMD", filepath.Join(tools.binDir, "fake-agent"))
	t.Setenv("ISSUE_TO_PR_REMOTE", origin)
	return tools
}

func writeExecutable(t *testing.T, directory, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(directory, name), []byte(content), 0o700); err != nil {
		t.Fatal(err)
	}
}

// initializeOriginRepository is the bare repository the workflow clones and
// pushes to. Nothing here reaches the network.
func initializeOriginRepository(t *testing.T) string {
	t.Helper()
	seed := t.TempDir()
	runGit(t, seed, "init", "-b", "main")
	runGit(t, seed, "config", "user.name", "Issue To PR Fixture")
	runGit(t, seed, "config", "user.email", "issue-to-pr@example.test")
	writeFile(t, seed, "go.mod", "module issue-to-pr-subject\n\ngo 1.24\n")
	writeFile(t, seed, "subject.go", "package subject\n\n// Answer is the subject under change.\nfunc Answer() int { return 42 }\n")
	runGit(t, seed, "add", ".")
	runGit(t, seed, "commit", "-m", "subject: baseline")

	bare := filepath.Join(t.TempDir(), "origin.git")
	runGit(t, seed, "clone", "--bare", seed, bare)
	// A bare repository refuses a push to its checked-out branch unless it is
	// told not to care; this one has no working tree, so there is nothing to
	// care about.
	runGit(t, bare, "config", "receive.denyCurrentBranch", "ignore")
	return bare
}

// --------------------------------------------------------------------------
// Assertions and small readers
// --------------------------------------------------------------------------

func requireExercise(t *testing.T) {
	t.Helper()
	if os.Getenv("WEFTY_ISSUE_TO_PR_EXERCISE") != "1" {
		t.Skip("set WEFTY_ISSUE_TO_PR_EXERCISE=1 to run the issue-to-pr stack exercise")
	}
}

// assertPhasesRan checks that exactly the named phases are closed intervals, in
// order, with durations.
func assertPhasesRan(t *testing.T, steps l3.RunSteps, want []string, logs string) {
	t.Helper()
	var got []string
	for _, step := range steps.Steps {
		// The repository gates bracket themselves inside the gates phase; this
		// assertion is about the phases.
		if !slices.Contains(want, step.Name) {
			continue
		}
		if step.Open || step.Seconds == nil {
			t.Fatalf("phase %s has no duration: %#v", step.Name, step)
		}
		got = append(got, step.Name)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("phases ran %v, want %v:\n%s", got, want, logs)
	}
}

// assertNoSecretsInHandoff reads every file the run handed back and refuses any
// value this process holds that a reader should never receive.
func assertNoSecretsInHandoff(t *testing.T, handoff string) {
	t.Helper()
	entries, err := os.ReadDir(handoff)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		body := readFile(t, filepath.Join(handoff, entry.Name()))
		for _, name := range []string{"GITHUB_TOKEN", "GH_TOKEN", "WEFTY_RUN_TOKEN", "WEFTY_ATTEMPT_TOKEN"} {
			if strings.Contains(body, name) {
				t.Fatalf("%s names %s", entry.Name(), name)
			}
		}
		if strings.Contains(body, "WEFTY_ISSUE_TO_PR_AGENT_CMD") {
			t.Fatalf("%s carries the test seam's value", entry.Name())
		}
	}
}

func branchFromLogs(t *testing.T, logs string) string {
	t.Helper()
	match := regexp.MustCompile(`branch (issue-to-pr/[A-Za-z0-9._/-]+)`).FindStringSubmatch(logs)
	if len(match) != 2 {
		return ""
	}
	return match[1]
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(body)
}

func readJSONFile(t *testing.T, path string, target any) {
	t.Helper()
	if err := json.Unmarshal([]byte(readFile(t, path)), target); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
}

func writeFile(t *testing.T, directory, name, content string) {
	t.Helper()
	path := filepath.Join(directory, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
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
		NodeID: nodeID, BootSessionID: "boot-issue-to-pr", Version: "issue-to-pr-exercise",
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

// submitIssueToPR submits the workflow as an inline script with the given
// params, pinned to this exercise's node.
func submitIssueToPR(t *testing.T, client *http.Client, params map[string]string) l3.RunAccepted {
	t.Helper()
	script, err := os.ReadFile("issue-to-pr.sh")
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(script)
	encoded, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	return submitRun(t, client, l3.CreateRunRequest{
		InlineScript: &l3.InlineScriptInput{
			Content: string(script), SHA256: hex.EncodeToString(digest[:]),
			Interpreter: []string{"bash"},
		},
		Params: encoded,
		Tags:   []string{contract.StableNodeTagPrefix + nodeID},
		Limits: &contract.RunLimits{MaxRuntimeSeconds: int(runBudget.Seconds())},
		// The workflow reports and dispatches nothing, so it is submitted with
		// no credential at all.
		RequiredEnvelope: true,
	}, "issue-to-pr-exercise-"+fmt.Sprint(time.Now().UnixNano()))
}

// runLogs waits for the workflow's last line and returns the whole log.
func runLogs(t *testing.T, client *http.Client, runID string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var logs string
	for time.Now().Before(deadline) {
		fetched, err := fetchRunLogs(client, runID, perRequestBudget)
		if err == nil {
			logs = fetched
			if strings.Contains(logs, doneMarker) {
				return logs
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return logs
}

// uploadedResult waits for the node to upload the run's result document.
func uploadedResult(t *testing.T, client *http.Client, runID string, timeout time.Duration) []byte {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		document, found, err := fetchRunResult(client, runID, perRequestBudget)
		if err != nil {
			t.Fatalf("read the run result: %v", err)
		}
		if found {
			return document
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("run %s never uploaded a result", runID)
	return nil
}
