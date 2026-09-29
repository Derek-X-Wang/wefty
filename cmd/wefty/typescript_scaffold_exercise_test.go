//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
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
)

// TestTypeScriptScaffoldBuildsAndSubmitsThroughAStack is the whole
// `--lang ts` promise, end to end, through the commands its README gives:
// `wefty workflow init NAME --lang ts`, then the author's `npm ci` and
// `npm test` (typecheck, esbuild bundle, the scaffold's own Go test), then
// `wefty submit --script=dist/NAME.mjs --interpreter=node` against a real
// single-machine stack -- L1, L3, a node agent and the process runner. The
// node writes the bundle to a file with no extension, which is the reason the
// scaffold bundles at all, and the run must still report every event and
// upload its result.
//
// It is opt-in because `npm ci` fetches the pinned toolchain: set
// WEFTY_TS_SCAFFOLD_EXERCISE=1, which the dogfood-workflow job in
// contract-gate.yml does, on the Node version the README states.
func TestTypeScriptScaffoldBuildsAndSubmitsThroughAStack(t *testing.T) {
	if os.Getenv("WEFTY_TS_SCAFFOLD_EXERCISE") != "1" {
		t.Skip("set WEFTY_TS_SCAFFOLD_EXERCISE=1 to build a TypeScript scaffold with npm and submit it to a stack")
	}
	for _, tool := range []string{"node", "npm", "go"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Fatalf("WEFTY_TS_SCAFFOLD_EXERCISE=1 needs %s on PATH: %v", tool, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	workspace := t.TempDir()
	var stdout, stderr bytes.Buffer
	if err := run(ctx, []string{"workflow", "init", "greeter", "--lang", "ts",
		"--dir", filepath.Join(workspace, "workflows")}, &stdout, &stderr); err != nil {
		t.Fatalf("wefty workflow init --lang ts: %v\n%s", err, stderr.String())
	}
	target := filepath.Join(workspace, "workflows", "greeter")
	// In the wefty repository the scaffold sits inside wefty's own module; out
	// here it needs one of its own for `npm test` to run its Go test. The test
	// imports nothing but the standard library.
	if err := os.WriteFile(filepath.Join(target, "go.mod"), []byte("module example.test/greeter\n\ngo 1.24\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	npm(t, target, "ci", "--no-audit", "--no-fund")
	npm(t, target, "test")
	bundle := filepath.Join(target, "dist", "greeter.mjs")
	if _, err := os.Stat(bundle); err != nil {
		t.Fatalf("npm test left no bundle: %v", err)
	}

	clients := startTypeScriptExerciseStack(t)
	var submitOut bytes.Buffer
	if err := execute(ctx, clients, true, []string{
		"submit", "--script", bundle, "--interpreter", "node",
		"--params", `{"subject":"wefty"}`, "--required-envelope",
		"--tag", "linux", "--tag", contract.StableNodeTagPrefix + "node-ts",
		"--idempotency-key", "typescript-scaffold-exercise",
	}, &submitOut, &stderr); err != nil {
		t.Fatalf("submit the bundle: %v\n%s", err, stderr.String())
	}
	var submitted l3.RunAccepted
	if err := json.Unmarshal(submitOut.Bytes(), &submitted); err != nil {
		t.Fatalf("submit printed %q: %v", submitOut.String(), err)
	}

	var logsOut, logsErr bytes.Buffer
	if err := execute(ctx, clients, false, []string{"logs", submitted.RunID, "--follow", "--poll-interval", "20ms"}, &logsOut, &logsErr); err != nil {
		t.Fatalf("follow the run: %v", err)
	}
	if !strings.Contains(logsOut.String(), "greeter: done: pass") {
		t.Fatalf("the run log does not end the way the starter does:\nstdout:\n%s\nstderr:\n%s", logsOut.String(), logsErr.String())
	}

	// Publication is a sweep, so the ledger is read until the gate lands
	// rather than assumed to hold it the moment the run is terminal.
	var inspection runInspection
	deadline := time.Now().Add(30 * time.Second)
	for {
		var inspectOut bytes.Buffer
		if err := execute(ctx, clients, true, []string{"inspect", submitted.RunID}, &inspectOut, &stderr); err != nil {
			t.Fatalf("inspect: %v", err)
		}
		if err := json.Unmarshal(inspectOut.Bytes(), &inspection); err != nil {
			t.Fatal(err)
		}
		if len(inspection.Run.Gates) > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	record := inspection.Run
	if record.Status != contract.RunSucceeded {
		t.Fatalf("run status = %q\n%s", record.Status, logsErr.String())
	}
	if len(record.Gates) != 1 || record.Gates[0].Name != "greeter" || record.Gates[0].Outcome != contract.GatePass {
		t.Fatalf("gates = %#v, want one greeter pass", record.Gates)
	}
	greeted := false
	for _, envelope := range record.Envelopes {
		if envelope.StepID == "work" && envelope.Status == contract.EnvelopeSucceeded && envelope.Summary == "greeted wefty" {
			greeted = true
		}
	}
	if !greeted {
		t.Fatalf("no succeeded work envelope in %#v", record.Envelopes)
	}

	document := waitForCLIResult(ctx, t, clients, submitted.RunID)
	if document != `{"schema_version":1,"workflow":"greeter","subject":"wefty","passed":true}` {
		t.Fatalf("the uploaded result is %q", document)
	}
}

// npm runs one npm command in the scaffold, the way its author would.
func npm(t *testing.T, directory string, args ...string) {
	t.Helper()
	command := exec.Command("npm", args...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("npm %s: %v\n%s", strings.Join(args, " "), err, output)
	}
}

// startTypeScriptExerciseStack stands up L1, L3 and one process node on a
// plain fabric and returns an operator's clients. The node runs submitted
// scripts with the test's own environment, so `--interpreter=node` resolves
// the Node on this PATH.
func startTypeScriptExerciseStack(t *testing.T) *apiClients {
	t.Helper()
	network := plain.NewNetwork()
	controlFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	ledgerFabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger", Tags: []string{l1.DefaultClientPrincipalTag}})
	operatorFabric := network.NewFabric(fabric.Identity{NodeID: "operator", Tags: []string{l3.DefaultCallerPrincipalTag}})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})

	l1Store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l1Store.Close() })
	l1Server, err := l1.NewServer(controlFabric, l1Store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{
		"node-ts": l1.DefaultNodePolicy("linux", contract.StableNodeTagPrefix+"node-ts"),
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
	l1Done := serveTestServer(ctx, func() error { return l1Server.Serve(ctx, l1Listener) })
	l3Done := serveTestServer(ctx, func() error { return l3Server.Serve(ctx, l3Listener) })
	nodeAgent, err := agent.New(agent.Config{
		Fabric: agentFabric, ControlPlaneAddress: l3.DefaultL1Address, RunLedgerAddress: l3.DefaultL3Address,
		NodeID: "node-ts", BootSessionID: "boot-ts", Version: "typescript-scaffold-exercise",
		OS: "linux", Architecture: "amd64", Capabilities: map[string]bool{"kind:process": true},
		HeartbeatInterval: 50 * time.Millisecond, ClaimInterval: 20 * time.Millisecond,
		RenewalInterval: 50 * time.Millisecond, LogFlushInterval: 10 * time.Millisecond,
		LogRetryInterval: 10 * time.Millisecond, LogSpoolDirectory: t.TempDir(),
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	agentDone := serveTestServer(ctx, func() error { return nodeAgent.Run(ctx) })
	clients, err := newAPIClients(operatorFabric, l3.DefaultL1Address, l3.DefaultL3Address)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		clients.close()
		cancel()
		nodeAgent.Close()
		for name, done := range map[string]<-chan error{"agent": agentDone, "L3": l3Done, "L1": l1Done} {
			if err := <-done; err != nil && name != "agent" {
				t.Errorf("%s server: %v", name, err)
			}
		}
	})
	return clients
}
