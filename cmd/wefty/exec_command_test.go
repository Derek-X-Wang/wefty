package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
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

type execStack struct {
	clients              *apiClients
	requests             []l3.CreateRunRequest
	keys                 []string
	mu                   sync.Mutex
	afterAccept          func()
	retry                bool
	retryStatus          int
	l1Address, l3Address string
}

func newExecStack(t *testing.T, idle time.Duration) *execStack {
	t.Helper()
	// Every test stack reports liveness every 50 ms, so a loaded CI runner's
	// slow process start or clone never looks like a silent command.
	if execHeartbeatSeconds == "60" {
		shortenExecTimers(t)
	}
	t.Setenv("HOME", t.TempDir())
	t.Setenv("TMPDIR", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("WEFTY_EXEC_STORAGE_ROOT", t.TempDir())
	network := plain.NewNetwork()
	control := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	ledger := network.NewFabric(fabric.Identity{NodeID: "run-ledger", Tags: []string{l1.DefaultClientPrincipalTag}})
	operator := network.NewFabric(fabric.Identity{NodeID: "operator", Tags: []string{l3.DefaultCallerPrincipalTag}})
	node := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	one, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = one.Close() })
	server, err := l1.NewServer(control, one, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{"node-cli": l1.DefaultNodePolicy("linux", contract.StableNodeTagPrefix+"node-cli")}})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := control.Listen("tcp", l3.DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	three, err := l3.OpenStore(filepath.Join(t.TempDir(), "l3.sqlite"), l3.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = three.Close() })
	upstream, err := l3.NewL1Client(ledger, l3.DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(upstream.CloseIdleConnections)
	reconciler, err := l3.NewReconciler(three, upstream, l3.ReconcilerConfig{Interval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ledgerServer, err := l3.NewServer(ledger, three, l3.ServerConfig{Reconciler: reconciler, Logs: upstream})
	if err != nil {
		t.Fatal(err)
	}
	ledgerListener, err := ledger.Listen("tcp", l3.DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}
	clients, err := newAPIClients(operator, l3.DefaultL1Address, l3.DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(clients.close)
	clients.nodeConfigPath = filepath.Join(t.TempDir(), "node.json")
	writeExecFile(t, clients.nodeConfigPath, `{"version":1,"node_id":"node-cli","control_socket":"/tmp/unused.sock"}`)
	stack := &execStack{clients: clients, l1Address: listener.Addr().String(), l3Address: ledgerListener.Addr().String()}
	ctx, cancel := context.WithCancel(t.Context())
	l1Done := serveTestServer(ctx, func() error { return server.Serve(ctx, listener) })
	l3Done := serveTestServer(ctx, func() error { return ledgerServer.Serve(ctx, ledgerListener) })
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	n, err := agent.New(agent.Config{Fabric: node, ControlPlaneAddress: l3.DefaultL1Address, NodeID: "node-cli", BootSessionID: "boot-cli", Version: "exec-test", OS: "linux", Architecture: "amd64", Capabilities: map[string]bool{"kind:process": true}, HeartbeatInterval: 20 * time.Millisecond, ClaimInterval: 5 * time.Millisecond, RenewalInterval: 20 * time.Millisecond, LogFlushInterval: 5 * time.Millisecond, LogRetryInterval: 5 * time.Millisecond, LogSpoolDirectory: t.TempDir(), ManagedRootDirectory: root, HandoffRoot: t.TempDir(), WorkloadRuntimes: map[string]agent.WorkloadRuntime{"process": processrunner.NewAdapter(processrunner.New(processrunner.Config{IdleTimeout: idle, TerminationGraceTime: 100 * time.Millisecond}))}})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	nodeDone := serveTestServer(ctx, func() error { return n.Run(ctx) })
	t.Cleanup(func() {
		cancel()
		for name, done := range map[string]<-chan error{"node": nodeDone, "L1": l1Done, "L3": l3Done} {
			if e := <-done; e != nil && name != "node" {
				t.Errorf("%s: %v", name, e)
			}
		}
		n.Close()
	})
	waitForReadyStatus(ctx, t, clients)
	original := clients.l3.client.Transport
	clients.l3.client.Transport = execTransport{base: original, stack: stack}
	return stack
}

type execTransport struct {
	base  http.RoundTripper
	stack *execStack
}

func (tr execTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method != http.MethodPost || r.URL.Path != "/v1/runs" {
		return tr.base.RoundTrip(r)
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	var request l3.CreateRunRequest
	if err = json.Unmarshal(raw, &request); err != nil {
		return nil, err
	}
	tr.stack.mu.Lock()
	tr.stack.requests = append(tr.stack.requests, request)
	tr.stack.keys = append(tr.stack.keys, r.Header.Get("Idempotency-Key"))
	index := len(tr.stack.requests)
	hook := tr.stack.afterAccept
	retry := tr.stack.retry
	retryStatus := tr.stack.retryStatus
	tr.stack.mu.Unlock()
	response, err := tr.base.RoundTrip(r)
	if err == nil && response.StatusCode < 300 {
		if hook != nil {
			hook()
		}
		if retry && index == 1 {
			_ = response.Body.Close()
			if retryStatus != 0 {
				return &http.Response{StatusCode: retryStatus, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":{"code":"internal","message":"post-commit response read failed","retryable":false}}`))}, nil
			}
			return nil, fmt.Errorf("lost submission acknowledgement")
		}
	}
	return response, err
}

func writeExecFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

// execTestIdleLimit is the no-output limit for in-process tests that do not
// test it. Those tests shorten the heartbeat, so a slow clone on a loaded
// hosted runner still reports liveness well inside the limit.
const execTestIdleLimit = 15 * time.Second

// execRealBinaryIdleLimit is the no-output limit for real-binary tests. The
// built binary keeps its production heartbeat, so this limit stays above it,
// as production limits do. Each CLI call's own budget still catches a hang.
const execRealBinaryIdleLimit = 90 * time.Second

// Git 2.47+ commits start auto-maintenance in the background, which can
// briefly lock files under .git while a test compares the checkout.
func execTestGitEnv() []string {
	return append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=test", "GIT_AUTHOR_EMAIL=test@example.com", "GIT_COMMITTER_NAME=test", "GIT_COMMITTER_EMAIL=test@example.com",
		"GIT_CONFIG_COUNT=2", "GIT_CONFIG_KEY_0=maintenance.auto", "GIT_CONFIG_VALUE_0=false", "GIT_CONFIG_KEY_1=gc.auto", "GIT_CONFIG_VALUE_1=0")
}

func execTestGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	c := exec.Command("git", args...)
	c.Dir = dir
	c.Env = execTestGitEnv()
	out, err := c.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return string(out)
}
func execTestRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	execTestGit(t, dir, "init", "-q")
	writeExecFile(t, filepath.Join(dir, "sub", "tracked"), "base")
	writeExecFile(t, filepath.Join(dir, ".gitignore"), "ignored\n")
	execTestGit(t, dir, "add", ".")
	execTestGit(t, dir, "commit", "-qm", "base")
	return dir
}
func shortenExecTimers(t *testing.T) {
	t.Helper()
	oldHeartbeat, oldPoll := execHeartbeatSeconds, execPollInterval
	execHeartbeatSeconds = "0.05"
	execPollInterval = 10 * time.Millisecond
	t.Cleanup(func() { execHeartbeatSeconds = oldHeartbeat; execPollInterval = oldPoll })
}
func execTreeEvidence(t *testing.T, root string) map[string][32]byte {
	t.Helper()
	evidence := map[string][32]byte{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		data, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		rel, _ := filepath.Rel(root, path)
		evidence[rel] = sha256.Sum256(append([]byte(info.Mode().String()), data...))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return evidence
}

func TestExecCapturedCheckoutOverPlainFabric(t *testing.T) {
	t.Setenv("EXEC_NODE_ENV", "node-value")
	shortenExecTimers(t)
	stack := newExecStack(t, execTestIdleLimit)
	repo := execTestRepo(t)
	// Include staged, unstaged and untracked changes, and an existing stash/ref.
	writeExecFile(t, filepath.Join(repo, "sub", "tracked"), "staged")
	execTestGit(t, repo, "add", "sub/tracked")
	writeExecFile(t, filepath.Join(repo, "sub", "tracked"), "working")
	writeExecFile(t, filepath.Join(repo, "sub", "new"), "untracked")
	writeExecFile(t, filepath.Join(repo, "ignored"), "must not copy")
	execTestGit(t, repo, "branch", "unchanged")
	execTestGit(t, repo, "update-ref", "refs/stash", "HEAD")
	before := execTreeEvidence(t, repo)
	t.Chdir(filepath.Join(repo, "sub"))
	// Argument values contain shell syntax; the literal value must survive.
	arg := "spaces 'quotes' \"double\" $(touch should-not-exist)\nnewline"
	script := `test "$EXEC_NODE_ENV" = node-value && test "$(cat tracked)" = working && test "$(cat new)" = untracked && test ! -e ../ignored && test ! -e should-not-exist && test "${PWD##*/}" = sub && ! env | grep '^WEFTY_' && printf '%s\n' "$1"`
	var out, errOut bytes.Buffer
	err := execute(t.Context(), stack.clients, false, []string{"exec", "--tag", "linux", "--", "/bin/sh", "-c", script, "sh", arg}, &out, &errOut)
	if err != nil || !strings.Contains(out.String(), arg+"\n") || !strings.HasSuffix(errOut.String(), "succeeded\n") {
		t.Fatalf("exec: %v stdout=%q stderr=%q", err, out.String(), errOut.String())
	}
	after := execTreeEvidence(t, repo)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("exec changed checkout files, index, stash, refs or object store")
	}
	request := stack.requests[0]
	if request.WorkflowRef != "" || request.DispatchAuthority || request.Limits != nil || !reflect.DeepEqual(request.Tags, []string{"linux", contract.StableNodeTagPrefix + "node-cli"}) {
		t.Fatalf("ordinary Pinned run: %+v", request)
	}
	var params execParams
	if err = json.Unmarshal(request.Params, &params); err != nil {
		t.Fatal(err)
	}
	if !params.Dirty || params.Tree == "" || params.Base != strings.TrimSpace(execTestGit(t, repo, "rev-parse", "HEAD")) || params.Folder != "sub" {
		t.Fatalf("capture record: %+v", params)
	}
	if _, err = os.Stat(params.Storage); !os.IsNotExist(err) {
		t.Fatalf("snapshot remains: %s %v", params.Storage, err)
	}
	page, err := stack.clients.listRuns(t.Context(), "", 20)
	if err != nil || len(page.Runs) != 1 {
		t.Fatalf("runs: %+v %v", page, err)
	}
	result := waitForCLIResult(t.Context(), t, stack.clients, page.Runs[0].RunID)
	if result != "{\"exit_code\":0}\n" {
		t.Fatalf("result: %s", result)
	}
}

func TestExecFrozenInputsAndFreshRetry(t *testing.T) {
	for _, test := range []struct {
		name   string
		remove bool
		status int
	}{{"edit", false, 0}, {"delete", true, 0}, {"post-commit internal", false, 500}} {
		remove := test.remove
		t.Run(test.name, func(t *testing.T) {
			shortenExecTimers(t)
			stack := newExecStack(t, execTestIdleLimit)
			repo := execTestRepo(t)
			t.Chdir(filepath.Join(repo, "sub"))
			setExecTestClaims(t, stack.clients, false)
			stack.retry = true
			stack.retryStatus = test.status
			stack.afterAccept = func() {
				if remove {
					if err := os.RemoveAll(repo); err != nil {
						t.Error(err)
					}
				} else {
					writeExecFile(t, filepath.Join(repo, "sub", "tracked"), "later")
				}
				setExecTestClaims(t, stack.clients, true)
			}
			var out, errOut bytes.Buffer
			if err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "/bin/sh", "-c", "sleep 0.1; cat tracked"}, &out, &errOut); err != nil || !strings.Contains(out.String(), "base") {
				t.Fatalf("frozen run: %v %q %q", err, out.String(), errOut.String())
			}
			if strings.Count(errOut.String(), "submission reply lost") != 1 {
				t.Errorf("missing retry feedback: %q", errOut.String())
			}
			if len(stack.keys) != 2 || stack.keys[0] != stack.keys[1] || !reflect.DeepEqual(stack.requests[0], stack.requests[1]) {
				t.Fatalf("retry changed key or capture: %v", stack.keys)
			}
			runs, err := stack.clients.listRuns(t.Context(), "", 20)
			if err != nil || len(runs.Runs) != 1 {
				t.Fatalf("retry created extra run: %+v %v", runs, err)
			}
			// Return to a live checkout for two identical calls.
			t.Chdir(t.TempDir())
			repo2 := execTestRepo(t)
			t.Chdir(repo2)
			stack.retry = false
			stack.afterAccept = nil
			for i := 0; i < 2; i++ {
				if err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "/bin/sh", "-c", "exit 0"}, io.Discard, io.Discard); err != nil {
					t.Fatal(err)
				}
			}
			runs, err = stack.clients.listRuns(t.Context(), "", 20)
			if err != nil || len(runs.Runs) != 3 || stack.keys[2] == stack.keys[3] {
				t.Fatalf("identical calls replayed: %+v keys=%v %v", runs, stack.keys, err)
			}
		})
	}
}
func setExecTestClaims(t *testing.T, clients *apiClients, enabled bool) {
	t.Helper()
	node, err := clients.getNode(t.Context(), "node-cli")
	if err != nil {
		t.Fatal(err)
	}
	_, err = clients.setNodeClaims(t.Context(), node.NodeID, l1.NodeIntentRequest{ClaimsEnabled: enabled, IntentRevision: node.IntentRevision, Reason: "freeze test execution until the source is changed"})
	if err != nil {
		t.Fatal(err)
	}
}

func TestExecHeartbeatAndFailure(t *testing.T) {
	shortenExecTimers(t)
	// The command stays quiet for longer than the idle limit; only the
	// heartbeat keeps it alive. The margin absorbs a loaded CI runner. The
	// runner reads output only as fast as the node's log spool accepts it,
	// so a spool write stalled past the limit kills the command before it
	// writes its result; hosted runners have stalled one past 2 s.
	stack := newExecStack(t, 5*time.Second)
	repo := execTestRepo(t)
	t.Chdir(repo)
	var out, errOut bytes.Buffer
	started := time.Now()
	err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "/bin/sh", "-c", "sleep 6; exit 7"}, &out, &errOut)
	if commandExitCode(err) != 10 || !strings.Contains(errOut.String(), "still running (1m)") || !strings.HasSuffix(errOut.String(), "failed\n") {
		t.Fatalf("heartbeat/failure: %v %q %q", err, out.String(), errOut.String())
	}
	if time.Since(started) > 18*time.Second {
		t.Fatal("heartbeat kept output pipes open after child exited")
	}
	page, e := stack.clients.listRuns(t.Context(), "", 20)
	if e != nil {
		t.Fatal(e)
	}
	result := waitForCLIResult(t.Context(), t, stack.clients, page.Runs[0].RunID)
	if result != "{\"exit_code\":7}\n" {
		t.Fatalf("command exit result: %s", result)
	}
}

func TestExecCancellation(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprint(queued), func(t *testing.T) {
			shortenExecTimers(t)
			stack := newExecStack(t, execTestIdleLimit)
			repo := execTestRepo(t)
			t.Chdir(repo)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			marker := filepath.Join(t.TempDir(), "started")
			args := []string{"exec"}
			if queued {
				args = append(args, "--tag", "no-node-has-this")
			}
			args = append(args, "--", "/bin/sh", "-c", "touch \"$1\"; sleep 60", "sh", marker)
			stack.afterAccept = func() {
				if queued {
					cancel()
				}
			}
			if !queued {
				go func() {
					for {
						if _, e := os.Stat(marker); e == nil {
							cancel()
							return
						}
						select {
						case <-ctx.Done():
							return
						case <-time.After(5 * time.Millisecond):
						}
					}
				}()
			}
			err := execute(ctx, stack.clients, false, args, io.Discard, io.Discard)
			if err == nil || !strings.Contains(err.Error(), "cancelled run") {
				t.Fatalf("cancel: %v", err)
			}
			runs, e := stack.clients.listRuns(t.Context(), "", 20)
			if e != nil || len(runs.Runs) != 1 || runs.Runs[0].Status != contract.RunFailed {
				t.Fatalf("cancellation record: %+v %v", runs, e)
			}
			var params execParams
			_ = json.Unmarshal(stack.requests[0].Params, &params)
			if _, e = os.Stat(params.Storage); !os.IsNotExist(e) {
				t.Fatalf("cancelled capture remains: %v", e)
			}
		})
	}
}

func TestExecRefusalsSubmitNothing(t *testing.T) {
	stack := newExecStack(t, execTestIdleLimit)
	for _, name := range []string{"not a git repo", "no commits yet", "unresolved merge", "sparse checkout", "partial clone", "ignored start folder", "empty start folder", "Git LFS", "submodules with changes", "no installed node", "requires --l3", "64 KiB", "cancelled before capture"} {
		t.Run(name, func(t *testing.T) {
			repo := execTestRepo(t)
			wantMessage := name
			switch name {
			case "not a git repo":
				repo = t.TempDir()
				wantMessage = "wefty exec runs from inside a Git checkout; cd into the project first"
			case "no commits yet":
				repo = t.TempDir()
				execTestGit(t, repo, "init", "-q")
				wantMessage = "wefty exec needs at least one commit in this checkout; commit first"
			case "unresolved merge":
				execTestGit(t, repo, "checkout", "-qb", "side")
				writeExecFile(t, filepath.Join(repo, "sub", "tracked"), "side")
				execTestGit(t, repo, "commit", "-qam", "side")
				execTestGit(t, repo, "checkout", "-q", "-")
				writeExecFile(t, filepath.Join(repo, "sub", "tracked"), "main")
				execTestGit(t, repo, "commit", "-qam", "main")
				// The merge must stop on its conflict, not on a missing
				// identity: hosted CI runners have no global git identity.
				c := exec.Command("git", "merge", "side")
				c.Dir = repo
				c.Env = execTestGitEnv()
				_ = c.Run()
			case "sparse checkout":
				execTestGit(t, repo, "config", "core.sparseCheckout", "true")
			case "partial clone":
				execTestGit(t, repo, "config", "extensions.partialClone", "origin")
			case "ignored start folder":
				writeExecFile(t, filepath.Join(repo, "ignored", "file"), "excluded")
				repo = filepath.Join(repo, "ignored")
			case "empty start folder":
				repo = filepath.Join(repo, "empty")
				if err := os.Mkdir(repo, 0700); err != nil {
					t.Fatal(err)
				}
			case "Git LFS":
				writeExecFile(t, filepath.Join(repo, ".gitattributes"), "sub/tracked filter=lfs\n")
			case "submodules with changes":
				sub := execTestRepo(t)
				execTestGit(t, repo, "-c", "protocol.file.allow=always", "submodule", "add", sub, "module")
				execTestGit(t, repo, "commit", "-qam", "module")
				writeExecFile(t, filepath.Join(repo, "module", "sub", "tracked"), "dirty")
			}
			t.Chdir(repo)
			originalPath := stack.clients.nodeConfigPath
			originalL3 := stack.clients.l3.address
			if name == "requires --l3" {
				stack.clients.l3.address = ""
			}
			if name == "no installed node" {
				stack.clients.nodeConfigPath = filepath.Join(t.TempDir(), "missing.json")
			}
			defer func() { stack.clients.nodeConfigPath = originalPath; stack.clients.l3.address = originalL3 }()
			args := []string{"exec", "--", "/bin/sh", "-c", "exit 0"}
			if name == "64 KiB" {
				args = append(args, strings.Repeat("x", 65536))
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if name == "cancelled before capture" {
				cancel()
			}
			err := execute(ctx, stack.clients, false, args, io.Discard, io.Discard)
			if name == "cancelled before capture" && !errors.Is(err, context.Canceled) {
				t.Fatalf("pre-capture stop: %v", err)
			}
			if err == nil || (name != "cancelled before capture" && !strings.Contains(err.Error(), wantMessage)) {
				t.Fatalf("%s refusal: %v", name, err)
			}
			if (name == "not a git repo" || name == "no commits yet") && commandExitCode(err) != exitUsage {
				t.Fatalf("%s exit=%d, want usage", name, commandExitCode(err))
			}
			if len(stack.requests) != 0 {
				t.Fatalf("refusal submitted %d requests", len(stack.requests))
			}
		})
	}
}

func TestExecDetachedHEADAndCleanSubmodule(t *testing.T) {
	shortenExecTimers(t)
	stack := newExecStack(t, execTestIdleLimit)
	repo := execTestRepo(t)
	sub := execTestRepo(t)
	execTestGit(t, repo, "-c", "protocol.file.allow=always", "submodule", "add", sub, "module")
	execTestGit(t, repo, "commit", "-qam", "module")
	execTestGit(t, repo, "checkout", "--detach", "-q")
	t.Chdir(repo)
	var out bytes.Buffer
	err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "/bin/sh", "-c", "cat module/sub/tracked; test ! -e module/.git"}, &out, io.Discard)
	if err != nil || !strings.Contains(out.String(), "base") {
		t.Fatalf("detached HEAD and clean submodule: %v %q", err, out.String())
	}
}

func TestExecExitCodesAndSignalsFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	stack := newExecStack(t, execRealBinaryIdleLimit)
	repo := execTestRepo(t)
	t.Chdir(filepath.Join(repo, "sub"))
	base := []string{"--l1=" + stack.l1Address, "--l3=" + stack.l3Address, "--node-config=" + stack.clients.nodeConfigPath, "exec"}
	for _, exit := range []int{0, 7} {
		code, out := runWefty(t, binary, 30*time.Second, append(append([]string{}, base...), "--", "/bin/sh", "-c", fmt.Sprintf("printf 'tail-output\\n'; exit %d", exit))...)
		want := 0
		if exit != 0 {
			want = 10
		}
		if code != want || !strings.Contains(out, "tail-output\n") {
			t.Fatalf("exec exit %d: code=%d output=%s", exit, code, out)
		}
	}
	// Environment metadata selection must work through the real argument path.
	code, out := runWeftyWithEnv(t, binary, 30*time.Second, []string{"WEFTY_NODE_CONFIG=" + stack.clients.nodeConfigPath}, "--l1="+stack.l1Address, "--l3="+stack.l3Address, "exec", "--", "/bin/sh", "-c", "exit 0")
	if code != 0 {
		t.Fatalf("WEFTY_NODE_CONFIG: code=%d output=%s", code, out)
	}
	for _, signal := range []os.Signal{os.Interrupt, syscall.SIGTERM, syscall.SIGHUP} {
		t.Run(signal.String(), func(t *testing.T) {
			marker := filepath.Join(t.TempDir(), "started")
			c := exec.Command(binary, append(append([]string{}, base...), "--", "/bin/sh", "-c", "touch \"$1\"; sleep 60", "sh", marker)...)
			c.Env = append(os.Environ(), "WEFTY_L1_ADDR=", "WEFTY_L3_ADDR=")
			var output bytes.Buffer
			c.Stdout = &output
			c.Stderr = &output
			if err := c.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = c.Process.Kill() })
			deadline := time.Now().Add(15 * time.Second)
			for {
				if _, err := os.Stat(marker); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("exec payload never started")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if err := c.Process.Signal(signal); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- c.Wait() }()
			select {
			case <-done:
			case <-time.After(20 * time.Second):
				t.Fatal("signal did not end exec")
			}
			if !strings.Contains(output.String(), "cancelled run") {
				t.Fatalf("signal output: %s", output.String())
			}
			runs, err := stack.clients.listRuns(t.Context(), "", 20)
			if err != nil {
				t.Fatal(err)
			}
			record, err := stack.clients.getRun(t.Context(), runs.Runs[0].RunID)
			if err != nil {
				t.Fatal(err)
			}
			if record.Status != contract.RunFailed || record.CancelStatus == "" {
				t.Fatalf("signal left run alive: %+v", record)
			}
			var params execParams
			_ = json.Unmarshal(record.Params, &params)
			if _, err = os.Stat(params.Storage); !os.IsNotExist(err) {
				t.Fatalf("signal left snapshot: %v", err)
			}
		})
	}
}

func TestExecHelpAndRefusalsFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	stack := newExecStack(t, execRealBinaryIdleLimit)
	repo := execTestRepo(t)
	t.Chdir(repo)
	code, out := runWefty(t, binary, 30*time.Second, "exec", "--help")
	if code != 0 || !strings.Contains(out, "-- <command...>") || !strings.Contains(out, "Stopping cancels") || !strings.Contains(out, "10") {
		t.Fatalf("exec help: code=%d %s", code, out)
	}
	base := []string{"--l1=" + stack.l1Address, "--l3=" + stack.l3Address, "--node-config=" + filepath.Join(t.TempDir(), "absent.json"), "exec", "--", "/bin/sh", "-c", "exit 0"}
	code, out = runWefty(t, binary, 30*time.Second, base...)
	if code == 0 || !strings.Contains(out, "no installed node") {
		t.Fatalf("missing node: code=%d %s", code, out)
	}
	// --json after the delimiter belongs to the command, as do help flags.
	base = []string{"--l1=" + stack.l1Address, "--l3=" + stack.l3Address, "--node-config=" + stack.clients.nodeConfigPath, "exec", "--", "/bin/sh", "-c", "printf '%s\\n' \"$@\"", "sh", "--json", "--help", "--tag"}
	code, out = runWefty(t, binary, 30*time.Second, base...)
	if code != 0 || !strings.Contains(out, "--json\n--help\n--tag\n") {
		t.Fatalf("literal flags: code=%d %s", code, out)
	}
}

func TestExecMaxRuntimeAndIgnoreSources(t *testing.T) {
	shortenExecTimers(t)
	stack := newExecStack(t, execTestIdleLimit)
	repo := execTestRepo(t)
	// Git's global excludes and local info/exclude participate in the capture.
	global := filepath.Join(t.TempDir(), "excludes")
	writeExecFile(t, global, "global-secret\n")
	writeExecFile(t, filepath.Join(os.Getenv("HOME"), ".gitconfig"), "[core]\n excludesFile = "+global+"\n")
	writeExecFile(t, filepath.Join(repo, ".git", "info", "exclude"), "local-secret\n")
	writeExecFile(t, filepath.Join(repo, "global-secret"), "secret")
	writeExecFile(t, filepath.Join(repo, "local-secret"), "secret")
	writeExecFile(t, filepath.Join(repo, "script"), "#!/bin/sh\nprintf executable\\n\n")
	if err := os.Chmod(filepath.Join(repo, "script"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("sub/tracked", filepath.Join(repo, "link")); err != nil {
		t.Fatal(err)
	}
	writeExecFile(t, filepath.Join(repo, "sub", "tracked"), "deleted")
	execTestGit(t, repo, "add", "sub/tracked")
	if err := os.Remove(filepath.Join(repo, "sub", "tracked")); err != nil {
		t.Fatal(err)
	}
	t.Chdir(repo)
	var out bytes.Buffer
	err := execute(t.Context(), stack.clients, false, []string{"exec", "--max-runtime", "5s", "--", "/bin/sh", "-c", "test ! -e global-secret && test ! -e local-secret && test ! -e sub/tracked && test -L link && ./script && sleep 60"}, &out, io.Discard)
	if commandExitCode(err) != 10 || !strings.Contains(out.String(), "executable") {
		t.Fatalf("runtime/ignore/modes: %v %q", err, out.String())
	}
	page, e := stack.clients.listRuns(t.Context(), "", 20)
	if e != nil || len(page.Runs) != 1 {
		t.Fatalf("runtime run: %+v %v", page, e)
	}
	record, e := stack.clients.getRun(t.Context(), page.Runs[0].RunID)
	if e != nil || record.Status != contract.RunFailed || !strings.HasPrefix(record.FailureReason, "maximum runtime reached") {
		t.Fatalf("runtime cap confused with idle limit: %+v %v", record, e)
	}
	if stack.requests[0].Limits == nil || stack.requests[0].Limits.MaxRuntimeSeconds != 5 {
		t.Fatalf("runtime limits: %+v", stack.requests[0].Limits)
	}
}

func TestExecStoppingBeforeAcceptanceAndLostReply(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(fmt.Sprint(accepted), func(t *testing.T) {
			shortenExecTimers(t)
			stack := newExecStack(t, execTestIdleLimit)
			repo := execTestRepo(t)
			t.Chdir(repo)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			original := stack.clients.l3.client.Transport
			stack.clients.l3.client.Transport = execStopTransport{base: original, accepted: accepted, cancel: cancel}
			var out, errOut bytes.Buffer
			err := execute(ctx, stack.clients, false, []string{"exec", "--tag", "unavailable-node", "--", "/bin/sh", "-c", "exit 0"}, &out, &errOut)
			if err == nil {
				t.Fatal("stopped exec returned success")
			}
			runs, e := stack.clients.listRuns(t.Context(), "", 20)
			if e != nil {
				t.Fatal(e)
			}
			if accepted {
				if !strings.Contains(errOut.String(), "recovering acceptance") {
					t.Errorf("missing recovery feedback: %q", errOut.String())
				}
				if len(runs.Runs) != 1 || !strings.Contains(err.Error(), "cancelled run") {
					t.Fatalf("accepted lost reply: %+v %v", runs, err)
				}
				record, e := stack.clients.getRun(t.Context(), runs.Runs[0].RunID)
				if e != nil || record.Status != contract.RunFailed || record.CancelStatus == "" {
					t.Fatalf("accepted cancellation: %+v %v", record, e)
				}
				if len(stack.keys) != 1 {
					t.Fatalf("signal retried POST: %v", stack.keys)
				}
				var params execParams
				_ = json.Unmarshal(record.Params, &params)
				if _, e = os.Stat(params.Storage); !os.IsNotExist(e) {
					t.Fatalf("cancelled storage remains: %v", e)
				}
			} else if !errors.Is(err, context.Canceled) || len(runs.Runs) != 0 || len(stack.keys) != 0 {
				t.Fatalf("signal before acceptance: %v runs=%+v keys=%v", err, runs, stack.keys)
			}
		})
	}
}

type execStopTransport struct {
	base     http.RoundTripper
	accepted bool
	cancel   context.CancelFunc
}

func (tr execStopTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method != http.MethodPost || r.URL.Path != "/v1/runs" {
		return tr.base.RoundTrip(r)
	}
	if tr.accepted {
		response, err := tr.base.RoundTrip(r)
		if err != nil {
			return nil, err
		}
		_ = response.Body.Close()
		tr.cancel()
		return nil, context.Canceled
	}
	tr.cancel()
	return &http.Response{StatusCode: 503, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":{"code":"unavailable","message":"busy","retryable":true}}`))}, nil
}

func TestExecParamsLimitIncludesCaptureMetadata(t *testing.T) {
	stack := newExecStack(t, execTestIdleLimit)
	repo := execTestRepo(t)
	t.Chdir(repo)
	argv := []string{"/bin/sh", "-c", "exit 0", "sh", ""}
	raw, _ := json.Marshal(execParams{Argv: argv})
	argv[4] = strings.Repeat("x", l3.MaxDispatchedRunParamsBytes-len(raw)-1)
	err := execute(t.Context(), stack.clients, false, append([]string{"exec", "--"}, argv...), io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "64 KiB") || len(stack.requests) != 0 {
		t.Fatalf("near-limit params: %v requests=%d", err, len(stack.requests))
	}
}

func TestExecKeepsWorkingBytesDespiteGitAttributes(t *testing.T) {
	shortenExecTimers(t)
	stack := newExecStack(t, execTestIdleLimit)
	repo := execTestRepo(t)
	writeExecFile(t, filepath.Join(repo, ".gitattributes"), "sub/tracked ident text eol=crlf\n")
	writeExecFile(t, filepath.Join(repo, "sub", "tracked"), "$Id: literal $\nexact\n")
	t.Chdir(repo)
	var out bytes.Buffer
	err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "/bin/sh", "-c", "cat sub/tracked"}, &out, io.Discard)
	if err != nil || !strings.Contains(out.String(), "$Id: literal $\nexact\n") {
		t.Fatalf("Git attributes transformed working bytes: %v %q", err, out.String())
	}
}

func TestExecCaptureInterruptedAndConcurrentEdits(t *testing.T) {
	for _, interrupt := range []bool{false, true} {
		t.Run(fmt.Sprint(interrupt), func(t *testing.T) {
			stack := newExecStack(t, execTestIdleLimit)
			repo := execTestRepo(t)
			t.Chdir(repo)
			// Intercept the real Git capture at its first private tree write. The rest
			// of the operation still uses Git and the public exec entry point.
			git, err := exec.LookPath("git")
			if err != nil {
				t.Fatal(err)
			}
			shim := t.TempDir()
			marker := filepath.Join(t.TempDir(), "capture-started")
			sentinel := filepath.Join(t.TempDir(), "once")
			source := filepath.Join(repo, "sub", "tracked")
			script := fmt.Sprintf("#!/bin/sh\nfor arg do\n if [ \"$arg\" = write-tree ] && [ ! -e '%s' ]; then\n  touch '%s'\n  touch '%s'\n", sentinel, sentinel, marker)
			if interrupt {
				// exec the sleeping process so CommandContext owns its lifetime and pipes.
				script += "  exec sleep 60\n"
			} else {
				script += fmt.Sprintf("  printf second > '%s'\n", source)
			}
			script += fmt.Sprintf(" fi\ndone\nexec '%s' \"$@\"\n", git)
			writeExecFile(t, filepath.Join(shim, "git"), script)
			if err = os.Chmod(filepath.Join(shim, "git"), 0755); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", shim+string(os.PathListSeparator)+os.Getenv("PATH"))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if interrupt {
				go func() {
					for {
						if _, e := os.Stat(marker); e == nil {
							cancel()
							return
						}
						select {
						case <-ctx.Done():
							return
						case <-time.After(5 * time.Millisecond):
						}
					}
				}()
			}
			before, _ := filepath.Glob(filepath.Join(os.Getenv("WEFTY_EXEC_STORAGE_ROOT"), "wefty-exec-*"))
			err = execute(ctx, stack.clients, false, []string{"exec", "--", "/bin/sh", "-c", "exit 0"}, io.Discard, io.Discard)
			if interrupt && (err == nil || len(stack.requests) != 0) {
				t.Fatalf("capture interruption submitted: %v requests=%d", err, len(stack.requests))
			}
			if !interrupt && (err != nil || len(stack.requests) != 1) {
				t.Fatalf("capture did not recover after edit: %v requests=%d", err, len(stack.requests))
			}
			if interrupt && !errors.Is(err, context.Canceled) {
				t.Fatalf("capture stop: %v", err)
			}
			after, _ := filepath.Glob(filepath.Join(os.Getenv("WEFTY_EXEC_STORAGE_ROOT"), "wefty-exec-*"))
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("capture failure leaked storage: before=%v after=%v", before, after)
			}
		})
	}
}

func TestExecCaptureUsesOwnStorageRoot(t *testing.T) {
	stack := newExecStack(t, execTestIdleLimit)
	root := os.Getenv("WEFTY_EXEC_STORAGE_ROOT")
	repo := execTestRepo(t)
	t.Chdir(repo)
	stack.afterAccept = func() {
		var params execParams
		_ = json.Unmarshal(stack.requests[0].Params, &params)
		if filepath.Dir(params.Storage) != root {
			t.Errorf("storage %q is outside test root %q", params.Storage, root)
		}
	}
	if err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "/bin/sh", "-c", "exit 0"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
}

func TestExecCapturesGitQuotedFilenames(t *testing.T) {
	for _, name := range []string{"Screenshot 10.00.00\u202fAM.png", "nonbreaking\u00a0space", "line\nfeed", "control\x01byte", "quote\"and\\slash"} {
		t.Run(name, func(t *testing.T) {
			stack := newExecStack(t, execTestIdleLimit)
			repo := execTestRepo(t)
			writeExecFile(t, filepath.Join(repo, name), "exact bytes")
			t.Chdir(repo)
			script := "cat -- \"$1\""
			var out, errOut bytes.Buffer
			err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "/bin/sh", "-c", script, "sh", name}, &out, &errOut)
			if err != nil || !strings.Contains(out.String(), "exact bytes") {
				t.Fatalf("filename %q: %v stdout=%q stderr=%q", name, err, out.String(), errOut.String())
			}
		})
	}
}

func TestExecSnapshotHasNoTemplateHooks(t *testing.T) {
	stack := newExecStack(t, execTestIdleLimit)
	repo := execTestRepo(t)
	template := t.TempDir()
	writeExecFile(t, filepath.Join(template, "hooks", "post-checkout"), "#!/bin/sh\nexit 99\n")
	writeExecFile(t, filepath.Join(os.Getenv("HOME"), ".gitconfig"), "[init]\n templateDir = "+template+"\n")
	t.Chdir(repo)
	stack.afterAccept = func() {
		var params execParams
		_ = json.Unmarshal(stack.requests[0].Params, &params)
		if _, err := os.Stat(filepath.Join(params.Storage, "snapshot.git", "hooks", "post-checkout")); !os.IsNotExist(err) {
			t.Errorf("template hook copied to private repo: %v", err)
		}
	}
	if err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "/bin/sh", "-c", "test ! -e .git/hooks/post-checkout"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
}
