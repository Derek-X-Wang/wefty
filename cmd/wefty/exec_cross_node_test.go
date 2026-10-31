package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

type execFleet struct {
	*execStack
	ledgerRoot    string
	targetHandoff string
	network       *plain.Network
	startOther    func()
}

func newExecFleet(t *testing.T, capBytes int64, remoteSlots ...int) *execFleet {
	idle := execRealBinaryIdleLimit
	t.Helper()
	// Every test stack reports liveness every 50 ms, so a loaded CI runner's
	// slow process start or clone never looks like a silent command.
	if execHeartbeatSeconds == "60" {
		shortenExecTimers(t)
	}
	t.Setenv("HOME", t.TempDir())
	t.Setenv("TMPDIR", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	execTestStorageRoot(t)
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
	server, err := l1.NewServer(control, one, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{"node-cli": l1.DefaultNodePolicy("linux", contract.StableNodeTagPrefix+"node-cli"), "node-other": l1.DefaultNodePolicy("remote", contract.StableNodeTagPrefix+"node-other")}})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := control.Listen("tcp", l3.DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	ledgerRoot := t.TempDir()
	options := l3.StoreOptions{ExecCaptureMaxBytes: capBytes}
	three, err := l3.OpenStore(filepath.Join(ledgerRoot, "l3.sqlite"), options)
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
	// Exec tests check exec, not admission; agent tests cover that. On a
	// loaded runner, load admission held a second run until a live first run
	// ended, up to its whole length (#1044). Zero turns it off.
	noLoadAdmission := 0.0
	n, err := agent.New(agent.Config{Fabric: node, ControlPlaneAddress: l3.DefaultL1Address, NodeID: "node-cli", BootSessionID: "boot-cli", Version: "exec-test", OS: "linux", Architecture: "amd64", Capabilities: map[string]bool{"kind:process": true, contract.CapabilityExecCaptureBundle: true}, OneshotLoadLimit: &noLoadAdmission, HeartbeatInterval: 20 * time.Millisecond, ClaimInterval: 5 * time.Millisecond, RenewalInterval: 20 * time.Millisecond, LogFlushInterval: 5 * time.Millisecond, LogRetryInterval: 5 * time.Millisecond, LogSpoolDirectory: t.TempDir(), ManagedRootDirectory: root, HandoffRoot: t.TempDir(), WorkloadRuntimes: map[string]agent.WorkloadRuntime{"process": processrunner.NewAdapter(processrunner.New(processrunner.Config{IdleTimeout: idle, TerminationGraceTime: 100 * time.Millisecond}))}})
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
	remoteFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-other", Tags: []string{l1.DefaultAgentPrincipalTag}})
	seedPolicy := l1.DefaultNodePolicy("remote", contract.StableNodeTagPrefix+"node-other")
	if len(remoteSlots) > 0 {
		seedPolicy.MaxOneshotSlots = remoteSlots[0]
	}
	_, err = one.RegisterNode(ctx, fabric.Identity{NodeID: "fabric-other"}, contract.NodeRegistration{NodeID: "node-other", BootSessionID: "seed-other", OS: "linux", Architecture: "amd64", AgentVersion: "test", Capabilities: map[string]bool{"kind:process": true, "exec:capture-bundle": len(remoteSlots) < 2 || remoteSlots[1] != 0}}, seedPolicy, true)
	if err != nil {
		t.Fatal(err)
	}
	remoteRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	remoteHandoff := t.TempDir()
	remote, err := agent.New(agent.Config{Fabric: remoteFabric, ControlPlaneAddress: l3.DefaultL1Address, NodeID: "node-other", BootSessionID: "boot-other", Version: "exec-test", OS: "linux", Architecture: "amd64", Capabilities: map[string]bool{"kind:process": true, contract.CapabilityExecCaptureBundle: true}, OneshotLoadLimit: &noLoadAdmission, HeartbeatInterval: 20 * time.Millisecond, ClaimInterval: 5 * time.Millisecond, RenewalInterval: 20 * time.Millisecond, LogFlushInterval: 5 * time.Millisecond, LogRetryInterval: 5 * time.Millisecond, LogSpoolDirectory: t.TempDir(), ManagedRootDirectory: remoteRoot, HandoffRoot: remoteHandoff, WorkloadRuntimes: map[string]agent.WorkloadRuntime{"process": processrunner.NewAdapter(processrunner.New(processrunner.Config{IdleTimeout: idle, TerminationGraceTime: 100 * time.Millisecond}))}})
	if err != nil {
		t.Fatal(err)
	}
	started := false
	start := func() {
		if started {
			return
		}
		started = true
		done := serveTestServer(ctx, func() error { return remote.Run(ctx) })
		t.Cleanup(func() { cancel(); <-done; remote.Close() })
	}
	t.Cleanup(func() {
		if !started {
			remote.Close()
		}
	})
	return &execFleet{execStack: stack, ledgerRoot: ledgerRoot, targetHandoff: remoteHandoff, network: network, startOther: start}
}

func TestExecNodeSelectionFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	stack := newExecStack(t, execRealBinaryIdleLimit)
	t.Chdir(execTestRepo(t))
	args := []string{"--l1=" + stack.l1Address, "--l3=" + stack.l3Address, "--node-config=" + stack.clients.nodeConfigPath, "exec"}
	code, out, stderr := runPolishBinary(t, binary, append(append([]string{}, args...), "--node", "unknown", "--", "true")...)
	if code != 2 || len(out) != 0 || !strings.Contains(string(stderr), "known nodes") || !strings.Contains(string(stderr), "node-cli") {
		t.Fatalf("unknown exit=%d stdout=%s stderr=%s", code, out, stderr)
	}
	// Both the default and an explicitly selected local Node must retain
	// version 1 storage, with no capture upload or remote wrapper.
	for _, nodeArgs := range [][]string{nil, {"--node", "node-cli"}} {
		localArgs := append(append([]string{}, args...), nodeArgs...)
		code, out, stderr = runPolishBinary(t, binary, append(localArgs, "--detach", "--", "true")...)
		if code != 0 {
			t.Fatalf("local %v exit=%d stdout=%s stderr=%s", nodeArgs, code, out, stderr)
		}
		record, err := stack.clients.getRun(t.Context(), strings.TrimSpace(string(out)))
		if err != nil {
			t.Fatal(err)
		}
		var p map[string]any
		json.Unmarshal(record.Params, &p)
		if p["exec_version"] != float64(1) || p["capture_sha256"] != nil {
			t.Fatalf("local path changed: %s", record.Params)
		}
	}
}
func TestExecCrossNodeFrozenCaptureFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	fleet := newExecFleet(t, 0)
	repo := execTestRepo(t)
	base := strings.TrimSpace(execTestGit(t, repo, "rev-parse", "HEAD"))
	writeExecFile(t, filepath.Join(repo, "sub", "tracked"), "changed")
	writeExecFile(t, filepath.Join(repo, "sub", "new"), "new-file")
	t.Chdir(filepath.Join(repo, "sub"))
	args := []string{"--l1=" + fleet.l1Address, "--l3=" + fleet.l3Address, "--node-config=" + fleet.clients.nodeConfigPath, "exec", "--node", "node-other", "--tag", "remote", "--detach", "--", "/bin/sh", "-c", `printf '%s\n' "$(git rev-parse HEAD)"; cat tracked new; git status --porcelain`}
	code, out, stderr := runPolishBinary(t, binary, args...)
	if code != 0 {
		t.Fatalf("submit exit=%d stdout=%s stderr=%s", code, out, stderr)
	}
	id := strings.TrimSpace(string(out))
	record, err := fleet.clients.getRun(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	var p map[string]any
	json.Unmarshal(record.Params, &p)
	storage, _ := p["snapshot_storage"].(string)
	if _, err := os.Stat(storage); !os.IsNotExist(err) {
		t.Fatalf("submitter capture still exists before target starts: %s err=%v", storage, err)
	}
	hash, _ := p["capture_sha256"].(string)
	if record.Workflow.InlineScript == nil || strings.Contains(record.Workflow.InlineScript.Content, storage) {
		t.Fatalf("remote wrapper contains submitter storage: %s", storage)
	}
	if len(hash) != 64 || p["exec_version"] != float64(2) {
		t.Fatalf("missing bundle identity: %s", record.Params)
	}
	bundle := filepath.Join(fleet.ledgerRoot, "exec-captures", hash+".bundle")
	if info, err := os.Stat(bundle); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("bundle missing/private mode wrong: %v %v", info, err)
	}
	fleet.startOther()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	var stdout, diagnostics bytes.Buffer
	err = execute(ctx, fleet.clients, false, []string{"wait", id, "--timeout", "15s"}, &stdout, &diagnostics)
	if err != nil {
		t.Fatalf("wait: %v stdout=%s stderr=%s", err, &stdout, &diagnostics)
	}
	record, err = fleet.clients.getRun(ctx, id)
	if err != nil || record.NodeID != "node-other" || record.Status != contract.RunSucceeded {
		t.Fatalf("run=%+v err=%v", record, err)
	}
	stdout.Reset()
	diagnostics.Reset()
	if err = execute(ctx, fleet.clients, false, []string{"logs", id, "--all"}, &stdout, &diagnostics); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{base, "changednew-file", " M sub/tracked", "?? sub/new"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("logs lack %q: %s", want, &stdout)
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, err = os.Stat(bundle)
		if os.IsNotExist(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("terminal bundle retained: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	code, out, stderr = runPolishBinary(t, binary, "--l1="+fleet.l1Address, "--l3="+fleet.l3Address, "rerun", id)
	if code != 2 || !strings.Contains(string(stderr), "snapshot was deleted") {
		t.Fatalf("rerun exit=%d stdout=%s stderr=%s", code, out, stderr)
	}
}
func TestExecCrossNodeOverCapFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	fleet := newExecFleet(t, 128)
	t.Chdir(execTestRepo(t))
	code, out, stderr := runPolishBinary(t, binary, "--l1="+fleet.l1Address, "--l3="+fleet.l3Address, "--node-config="+fleet.clients.nodeConfigPath, "exec", "--node", "node-other", "--detach", "--", "true")
	if code != 2 || len(out) != 0 || !strings.Contains(string(stderr), "128") || !strings.Contains(string(stderr), "bytes") {
		t.Fatalf("exit=%d stdout=%s stderr=%s", code, out, stderr)
	}
	page, err := fleet.clients.listRuns(t.Context(), "", 10)
	if err != nil || len(page.Runs) != 0 {
		t.Fatalf("upload refusal submitted: %+v %v", page, err)
	}
}

func TestExecCrossNodeCorruptCaptureNeverRunsCommand(t *testing.T) {
	binary := buildWefty(t)
	for _, kind := range []string{"hash-mismatch", "truncated"} {
		t.Run(kind, func(t *testing.T) {
			fleet := newExecFleet(t, 0)
			t.Chdir(execTestRepo(t))
			marker := filepath.Join(t.TempDir(), "command-ran")
			code, out, stderr := runPolishBinary(t, binary, "--l1="+fleet.l1Address, "--l3="+fleet.l3Address, "--node-config="+fleet.clients.nodeConfigPath, "exec", "--node", "node-other", "--detach", "--", "touch", marker)
			if code != 0 {
				t.Fatalf("submit exit=%d stdout=%s stderr=%s", code, out, stderr)
			}
			id := strings.TrimSpace(string(out))
			record, err := fleet.clients.getRun(t.Context(), id)
			if err != nil {
				t.Fatal(err)
			}
			var p map[string]any
			json.Unmarshal(record.Params, &p)
			path := filepath.Join(fleet.ledgerRoot, "exec-captures", p["capture_sha256"].(string)+".bundle")
			payload, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "truncated" {
				payload = payload[:len(payload)/2]
			} else {
				payload[len(payload)-1] ^= 0xff
			}
			if err = os.WriteFile(path, payload, 0600); err != nil {
				t.Fatal(err)
			}
			fleet.startOther()
			record = execModeAwait(t, fleet.execStack, id)
			if record.Status != contract.RunFailed {
				t.Fatalf("corrupt bundle outcome: %+v", record)
			}
			if _, err = os.Stat(marker); !os.IsNotExist(err) {
				t.Fatalf("command ran with corrupt capture: %v", err)
			}
			var logs, diagnostics bytes.Buffer
			err = execute(t.Context(), fleet.clients, false, []string{"logs", id, "--all"}, &logs, &diagnostics)
			evidence := logs.String() + record.FailureReason
			if err != nil || (!strings.Contains(evidence, "capture") || (!strings.Contains(evidence, "SHA-256") && !strings.Contains(evidence, "size"))) {
				t.Fatalf("missing capture error: %v %s stderr=%s record=%+v", err, &logs, &diagnostics, record)
			}
		})
	}
}
func TestExecCrossNodeStopCancelsUnlessKeepRunning(t *testing.T) {
	binary := buildWefty(t)
	for _, keep := range []bool{false, true} {
		t.Run(fmt.Sprint(keep), func(t *testing.T) {
			fleet := newExecFleet(t, 0)
			t.Chdir(execTestRepo(t))
			fleet.startOther()
			started, release := filepath.Join(t.TempDir(), "started"), filepath.Join(t.TempDir(), "release")
			defer writeExecFile(t, release, "")
			args := []string{"--l1=" + fleet.l1Address, "--l3=" + fleet.l3Address, "--node-config=" + fleet.clients.nodeConfigPath, "exec", "--node", "node-other"}
			if keep {
				args = append(args, "--keep-running")
			}
			args = append(args, "--", "/bin/sh", "-c", `touch "$1"; while [ ! -e "$2" ]; do sleep 0.05; done`, "sh", started, release)
			cmd := exec.Command(binary, args...)
			var output bytes.Buffer
			cmd.Stdout = &output
			cmd.Stderr = &output
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cmd.Process.Kill() })
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			deadline := time.After(20 * time.Second)
			for {
				if _, err := os.Stat(started); err == nil {
					break
				}
				select {
				case err := <-done:
					t.Fatalf("CLI ended before payload start: %v %s", err, &output)
				case <-deadline:
					t.Fatal("cross-node payload did not start")
				case <-time.After(10 * time.Millisecond):
				}
			}
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			var stopped error
			select {
			case stopped = <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("CLI ignored stop")
			}
			exit, ok := stopped.(*exec.ExitError)
			want := 1
			if keep {
				want = 13
			}
			if !ok || exit.ExitCode() != want {
				t.Fatalf("stop exit=%v want=%d output=%s", stopped, want, &output)
			}
			page, err := fleet.clients.listRuns(t.Context(), "", 10)
			if err != nil || len(page.Runs) != 1 {
				t.Fatalf("runs=%+v err=%v", page, err)
			}
			id := page.Runs[0].RunID
			if keep {
				record, err := fleet.clients.getRun(t.Context(), id)
				if err != nil || runIsTerminal(record.Status) || record.CancelStatus != "" {
					t.Fatalf("keep-running cancelled: %+v %v", record, err)
				}
				writeExecFile(t, release, "")
				record = execModeAwait(t, fleet.execStack, id)
				if record.Status != contract.RunSucceeded || record.CancelStatus != "" {
					t.Fatalf("keep-running result: %+v", record)
				}
			} else {
				record := execModeAwait(t, fleet.execStack, id)
				if record.Status != contract.RunFailed || record.CancelStatus == "" {
					t.Fatalf("stop did not cancel: %+v", record)
				}
			}
		})
	}
}

func TestExecCrossNodeZeroCapacityQueuesFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	fleet := newExecFleet(t, 0, 0)
	t.Chdir(execTestRepo(t))
	code, out, stderr := runPolishBinary(t, binary, "--l1="+fleet.l1Address, "--l3="+fleet.l3Address, "--node-config="+fleet.clients.nodeConfigPath, "exec", "--node", "node-other", "--detach", "--", "true")
	if code != 0 {
		t.Fatalf("queue exit=%d stdout=%s stderr=%s", code, out, stderr)
	}
	record, e := fleet.clients.getRun(t.Context(), strings.TrimSpace(string(out)))
	if e != nil || (record.Status == contract.RunRunning || record.Status == contract.RunSucceeded || record.Status == contract.RunFailed) {
		t.Fatalf("queued record=%+v err=%v", record, e)
	}
}
func TestExecCrossNodeOldAgentRefusedBeforeUploadFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	fleet := newExecFleet(t, 0, 1, 0)
	t.Chdir(execTestRepo(t))
	code, out, stderr := runPolishBinary(t, binary, "--l1="+fleet.l1Address, "--l3="+fleet.l3Address, "--node-config="+fleet.clients.nodeConfigPath, "exec", "--node", "node-other", "--detach", "--", "true")
	if code != 5 || len(out) != 0 || !strings.Contains(string(stderr), "node-other") || !strings.Contains(string(stderr), "upgrade") {
		t.Fatalf("old agent exit=%d stdout=%s stderr=%s", code, out, stderr)
	}
	entries, _ := os.ReadDir(filepath.Join(fleet.ledgerRoot, "exec-captures"))
	if len(entries) != 0 {
		t.Fatal("old agent caused an upload")
	}
}
func TestExecCrossNodeRemovesBundleAfterWrapperIsKilled(t *testing.T) {
	binary := buildWefty(t)
	fleet := newExecFleet(t, 0)
	t.Chdir(execTestRepo(t))
	marker := filepath.Join(t.TempDir(), "wrapper-killed")
	code, out, stderr := runPolishBinary(t, binary, "--l1="+fleet.l1Address, "--l3="+fleet.l3Address, "--node-config="+fleet.clients.nodeConfigPath, "exec", "--node", "node-other", "--detach", "--", "/bin/sh", "-c", `kill -KILL "$PPID"; touch "$1"; sleep 1`, "sh", marker)
	if code != 0 {
		t.Fatalf("submit exit=%d stdout=%s stderr=%s", code, out, stderr)
	}
	fleet.startOther()
	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("wrapper-kill command did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// A dead wrapper's heartbeat may still hold its process group open. Cancel
	// through the public API so the node reaps the attempt before checking cleanup.
	id := strings.TrimSpace(string(out))
	cancelCode, cancelOut, cancelErr := runPolishBinary(t, binary, "--l1="+fleet.l1Address, "--l3="+fleet.l3Address, "cancel", id)
	if cancelCode != 0 {
		t.Fatalf("cancel exit=%d stdout=%s stderr=%s", cancelCode, cancelOut, cancelErr)
	}
	record := execModeAwait(t, fleet.execStack, id)
	if record.Status != contract.RunFailed {
		t.Fatalf("wrapper kill did not fail run: %+v", record)
	}
	deadline = time.Now().Add(3 * time.Second)
	for {
		var bundles []string
		err := filepath.Walk(fleet.targetHandoff, func(path string, info os.FileInfo, err error) error {
			if os.IsNotExist(err) {
				return nil
			}
			if err != nil {
				return err
			}
			if info.Name() == "capture.bundle" {
				bundles = append(bundles, path)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(bundles) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("target retained capture bundles after wrapper death: %v", bundles)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestExecCrossNodeStopDuringUploadIsLocalCancellationFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	fleet := newExecFleet(t, 0)
	t.Chdir(execTestRepo(t))
	upstream, _ := url.Parse("http://wefty.invalid")
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	proxy.Transport = fleet.clients.l3.client.Transport
	uploading := make(chan struct{})
	delayed := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/exec-captures/") {
			_, _ = io.Copy(io.Discard, r.Body)
			close(uploading)
			<-r.Context().Done()
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	listener, err := fleet.network.NewFabric(fabric.Identity{NodeID: "upload-proxy"}).Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = delayed.Listener.Close()
	delayed.Listener = listener
	delayed.Start()
	t.Cleanup(delayed.Close)
	address := strings.TrimPrefix(delayed.URL, "http://")
	cmd := exec.Command(binary, "--l1="+fleet.l1Address, "--l3="+address, "--node-config="+fleet.clients.nodeConfigPath, "exec", "--node", "node-other", "--detach", "--", "true")
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-uploading:
	case err := <-done:
		t.Fatalf("CLI ended before upload: %v stdout=%s stderr=%s", err, &out, &stderr)
	case <-time.After(20 * time.Second):
		t.Fatal("upload did not start")
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		exit, ok := err.(*exec.ExitError)
		if !ok || exit.ExitCode() != 1 {
			t.Fatalf("upload cancellation exit=%v want=1 stdout=%s stderr=%s", err, &out, &stderr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("upload cancellation did not stop CLI")
	}
	page, err := fleet.clients.listRuns(t.Context(), "", 10)
	if err != nil || len(page.Runs) != 0 {
		t.Fatalf("cancelled upload submitted: %+v %v", page, err)
	}
	entries, err := os.ReadDir(os.Getenv("WEFTY_EXEC_STORAGE_ROOT"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "wefty-exec-") {
			t.Fatalf("cancelled upload retained local capture: %s", entry.Name())
		}
	}
}

func TestExecCrossNodeCallsKeepIndependentCaptureLifetimes(t *testing.T) {
	binary := buildWefty(t)
	fleet := newExecFleet(t, 0)
	repo := execTestRepo(t)
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	shim := filepath.Join(t.TempDir(), "git")
	quotedGit := "'" + strings.ReplaceAll(realGit, "'", "'\\''") + "'"
	script := "#!/bin/sh\ncase \" $* \" in *\" commit-tree \"*) export GIT_AUTHOR_DATE='2000-01-01T00:00:00Z' GIT_COMMITTER_DATE='2000-01-01T00:00:00Z';; esac\nexec " + quotedGit + " -c pack.threads=1 \"$@\"\n"
	writeExecFile(t, shim, script)
	if err = os.Chmod(shim, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", filepath.Dir(shim)+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Chdir(repo)
	var captures []map[string]any
	for i := 0; i < 2; i++ {
		code, out, stderr := runPolishBinary(t, binary, "--l1="+fleet.l1Address, "--l3="+fleet.l3Address, "--node-config="+fleet.clients.nodeConfigPath, "exec", "--node", "node-other", "--detach", "--", "true")
		if code != 0 {
			t.Fatalf("submit %d exit=%d stdout=%s stderr=%s", i, code, out, stderr)
		}
		record, err := fleet.clients.getRun(t.Context(), strings.TrimSpace(string(out)))
		if err != nil {
			t.Fatal(err)
		}
		var p map[string]any
		if err = json.Unmarshal(record.Params, &p); err != nil {
			t.Fatal(err)
		}
		captures = append(captures, p)
	}
	if captures[0]["base_commit"] != captures[1]["base_commit"] || captures[0]["snapshot_tree"] != captures[1]["snapshot_tree"] {
		t.Fatal("fixture did not freeze identical code")
	}
	if captures[0]["capture_sha256"] == captures[1]["capture_sha256"] {
		t.Fatalf("independent calls share a bundle and its cleanup lifetime: %v", captures[0]["capture_sha256"])
	}
	entries, err := os.ReadDir(filepath.Join(fleet.ledgerRoot, "exec-captures"))
	if err != nil {
		t.Fatal(err)
	}
	bundles := 0
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".bundle") {
			bundles++
		}
	}
	if bundles != 2 {
		t.Fatalf("want one held bundle per call, got %d", bundles)
	}
}

func TestExecCrossNodePreservesTemplateLookingArgumentsFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	fleet := newExecFleet(t, 0)
	t.Chdir(execTestRepo(t))
	fleet.startOther()
	literal := []string{`[ -d "$storage/snapshot.git" ]`, `clone --template= --quiet --no-local --no-checkout -- "$storage/snapshot.git"`, `__WEFTY_EXEC_INPUT__`, `__WEFTY_EXEC_CLEANUP__`, "storage=''\n", "--node", "--json"}
	args := []string{"--l1=" + fleet.l1Address, "--l3=" + fleet.l3Address, "--node-config=" + fleet.clients.nodeConfigPath, "exec", "--node", "node-other", "--", "/bin/sh", "-c", `printf '%s\000' "$@"`, "sh"}
	args = append(args, literal...)
	code, out, stderr := runPolishBinary(t, binary, args...)
	want := strings.Join(literal, "\x00") + "\x00"
	if code != 0 || string(out) != want {
		t.Fatalf("literal arguments changed: exit=%d stdout=%q want=%q stderr=%s", code, out, want, stderr)
	}
}

func TestExecBundleTemplateDriftRefusesToBuild(t *testing.T) {
	for _, needle := range []string{"__WEFTY_EXEC_INPUT__", "__WEFTY_EXEC_CLEANUP__", `[ -d "$storage/snapshot.git" ]`, `[ ! -d "$storage/snapshot.git" ]`, `clone --template= --quiet --no-local --no-checkout -- "$storage/snapshot.git"`} {
		t.Run(needle, func(t *testing.T) {
			for _, template := range []string{strings.Replace(execWrapper, needle, "missing", 1), execWrapper + "\n" + needle} {
				if _, e := buildExecBundleWrapperTemplate(template, execCapture{}, []string{"true"}); e == nil {
					t.Error("template drift silently accepted")
				}
			}
		})
	}
}
