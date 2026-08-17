package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
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

func TestOperatorCLIFullFlowOverPlainFabric(t *testing.T) {
	network := plain.NewNetwork()
	controlFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	ledgerFabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger", Tags: []string{l1.DefaultClientPrincipalTag}})
	operatorFabric := network.NewFabric(fabric.Identity{
		NodeID: "operator", Tags: []string{l3.DefaultCallerPrincipalTag},
	})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})

	l1Store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer l1Store.Close()
	l1Server, err := l1.NewServer(controlFabric, l1Store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{
		"node-cli": l1.DefaultNodePolicy("linux", contract.StableNodeTagPrefix+"node-cli"),
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
	l1Done := serveTestServer(ctx, func() error { return l1Server.Serve(ctx, l1Listener) })
	l3Done := serveTestServer(ctx, func() error { return l3Server.Serve(ctx, l3Listener) })

	nodeAgent, err := agent.New(agent.Config{
		Fabric: agentFabric, ControlPlaneAddress: l3.DefaultL1Address,
		NodeID: "node-cli", BootSessionID: "boot-cli", Version: "integration-v1",
		OS: "linux", Architecture: "amd64", Capabilities: map[string]bool{"process": true},
		HeartbeatInterval: 50 * time.Millisecond, ClaimInterval: 10 * time.Millisecond,
		RenewalInterval: 50 * time.Millisecond, LogFlushInterval: 5 * time.Millisecond,
		LogRetryInterval: 5 * time.Millisecond, LogSpoolDirectory: t.TempDir(),
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	agentDone := serveTestServer(ctx, func() error { return nodeAgent.Run(ctx) })
	t.Cleanup(func() {
		cancel()
		for name, done := range map[string]<-chan error{"agent": agentDone, "L3": l3Done, "L1": l1Done} {
			if err := <-done; err != nil && name != "agent" {
				t.Errorf("%s server: %v", name, err)
			}
		}
	})

	clients, err := newAPIClients(operatorFabric, l3.DefaultL1Address, l3.DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}
	defer clients.close()
	scriptPath := filepath.Join(t.TempDir(), "workflow.sh")
	script := "#!/bin/sh\nprintf 'cli-output\\n'\nprintf 'cli-error\\n' >&2\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}

	var submitOut, commandErr bytes.Buffer
	err = execute(ctx, clients, true, []string{
		"submit", "--script", scriptPath, "--params", `{"issue":29}`,
		"--tag", "linux", "--tag", contract.StableNodeTagPrefix + "node-cli",
		"--idempotency-key", "cli-submit",
	}, &submitOut, &commandErr)
	if err != nil {
		t.Fatalf("submit: %v stderr=%s", err, commandErr.String())
	}
	var submitted l3.RunAccepted
	if err := json.Unmarshal(submitOut.Bytes(), &submitted); err != nil {
		t.Fatal(err)
	}
	if submitted.RunID == "" {
		t.Fatal("submit returned an empty run ID")
	}

	var logsOut, logsErr bytes.Buffer
	if err := execute(ctx, clients, false, []string{"logs", submitted.RunID, "--follow", "--poll-interval", "5ms"}, &logsOut, &logsErr); err != nil {
		t.Fatalf("follow submitted logs: %v", err)
	}
	if logsOut.String() != "cli-output\n" || logsErr.String() != "cli-error\n" {
		t.Fatalf("submitted logs stdout/stderr = %q/%q", logsOut.String(), logsErr.String())
	}
	var inspectOut bytes.Buffer
	if err := execute(ctx, clients, true, []string{"inspect", submitted.RunID, "--execution"}, &inspectOut, &commandErr); err != nil {
		t.Fatalf("inspect submitted run: %v", err)
	}
	var inspection runInspection
	if err := json.Unmarshal(inspectOut.Bytes(), &inspection); err != nil {
		t.Fatal(err)
	}
	if inspection.Run.RunID != submitted.RunID || inspection.Run.L1JobID == "" || inspection.Run.Status != contract.RunSucceeded || len(inspection.Runs) != 1 {
		t.Fatalf("run inspection = %#v", inspection)
	}
	if inspection.Execution == nil || inspection.Execution.L1JobID != inspection.Run.L1JobID || inspection.Execution.Job == nil ||
		inspection.Execution.Job.JobID != inspection.Run.L1JobID || len(inspection.Execution.Job.Attempts) != 1 ||
		inspection.Execution.Job.Spec.Execution.SensitiveEnv != nil {
		t.Fatalf("execution inspection = %#v", inspection.Execution)
	}

	if err := os.WriteFile(scriptPath, []byte("#!/bin/sh\nprintf 'changed-on-disk\\n'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	var rerunOut bytes.Buffer
	if err := execute(ctx, clients, true, []string{"rerun", submitted.RunID, "--idempotency-key", "cli-rerun"}, &rerunOut, &commandErr); err != nil {
		t.Fatalf("rerun: %v", err)
	}
	var rerun l3.RunAccepted
	if err := json.Unmarshal(rerunOut.Bytes(), &rerun); err != nil {
		t.Fatal(err)
	}
	if rerun.RunID == "" || rerun.RunID == submitted.RunID {
		t.Fatalf("rerun ID = %q, source = %q", rerun.RunID, submitted.RunID)
	}
	logsOut.Reset()
	logsErr.Reset()
	if err := execute(ctx, clients, false, []string{"logs", rerun.RunID, "--follow", "--poll-interval", "5ms"}, &logsOut, &logsErr); err != nil {
		t.Fatalf("follow rerun logs: %v", err)
	}
	if logsOut.String() != "cli-output\n" || strings.Contains(logsOut.String(), "changed-on-disk") {
		t.Fatalf("rerun did not use stored snapshot: %q", logsOut.String())
	}

	var nodesOut bytes.Buffer
	if err := execute(ctx, clients, false, []string{"nodes", "list"}, &nodesOut, &commandErr); err != nil {
		t.Fatalf("nodes list: %v", err)
	}
	for _, want := range []string{"node-cli", "alive", "linux/amd64", "integration-v1", contract.StableNodeTagPrefix + "node-cli"} {
		if !strings.Contains(nodesOut.String(), want) {
			t.Fatalf("nodes output missing %q:\n%s", want, nodesOut.String())
		}
	}

	var drainOut bytes.Buffer
	if err := execute(ctx, clients, true, []string{"drain", "node-cli"}, &drainOut, &commandErr); err != nil {
		t.Fatalf("drain: %v", err)
	}
	var drained l1.Node
	if err := json.Unmarshal(drainOut.Bytes(), &drained); err != nil {
		t.Fatal(err)
	}
	if drained.State != contract.NodeAlive || drained.ClaimsEnabled {
		t.Fatalf("drained node = %#v", drained)
	}
	nodesOut.Reset()
	if err := execute(ctx, clients, false, []string{"nodes", "list"}, &nodesOut, &commandErr); err != nil {
		t.Fatalf("nodes list after drain: %v", err)
	}
	if !strings.Contains(nodesOut.String(), "false") {
		t.Fatalf("disabled claims intent not visible:\n%s", nodesOut.String())
	}
}

func TestWriteRunInspectionTableFormat(t *testing.T) {
	t.Parallel()

	inspection := runInspection{
		Run: contract.RunRecord{
			RunID:  "run-aaa",
			Status: contract.RunSucceeded,
		},
		Lineage: l3.RunLineage{RunID: "run-aaa"},
		Runs: []contract.RunRecord{
			{
				RunID:  "run-aaa",
				Status: contract.RunSucceeded,
				Envelopes: []contract.Envelope{
					{StepID: "plan", Status: "succeeded", Summary: "Claude produced the implementation plan"},
				},
				Gates: []contract.GateResult{
					{StepID: "plan", Outcome: "pass", Name: "plan-produced"},
				},
			},
		},
	}

	var buf bytes.Buffer
	if err := writeRunInspection(&buf, inspection); err != nil {
		t.Fatal(err)
	}
	out := buf.String()

	if !strings.Contains(out, "RUN ID") || !strings.Contains(out, "STATUS") {
		t.Errorf("run table header missing:\n%s", out)
	}
	if !strings.Contains(out, "KIND") || !strings.Contains(out, "STEP") {
		t.Errorf("envelope/gate header missing:\n%s", out)
	}
	if !strings.Contains(out, "envelope") || !strings.Contains(out, "plan") {
		t.Errorf("envelope row missing:\n%s", out)
	}
	if !strings.Contains(out, "gate") || !strings.Contains(out, "plan-produced") {
		t.Errorf("gate row missing:\n%s", out)
	}
}

func serveTestServer(_ context.Context, serve func() error) <-chan error {
	done := make(chan error, 1)
	go func() { done <- serve() }()
	return done
}
