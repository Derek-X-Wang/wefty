package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
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
	personFabric := network.NewFabric(fabric.Identity{
		NodeID: "operator-person", UserID: "person-alice", DeviceID: "device-a",
	})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})

	l1Store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := l1Store.Close(); err != nil {
			t.Errorf("close L1 store: %v", err)
		}
	})
	l1Server, err := l1.NewServer(controlFabric, l1Store, l1.ServerConfig{
		AllowSelfAssertedPersonIdentities: true,
		NodePolicies: map[string]l1.NodePolicy{
			"node-cli": l1.DefaultNodePolicy("linux", contract.StableNodeTagPrefix+"node-cli"),
		},
	})
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
	t.Cleanup(func() {
		if err := l3Store.Close(); err != nil {
			t.Errorf("close L3 store: %v", err)
		}
	})
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
		OS: "linux", Architecture: "amd64", Capabilities: map[string]bool{"kind:process": true},
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
	challenge, err := l1Store.InitiateAdminBootstrap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	personClients, err := newAPIClients(personFabric, l3.DefaultL1Address, l3.DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}
	defer personClients.close()
	var bootstrapOut bytes.Buffer
	if err := execute(ctx, personClients, true, []string{"admin", "bootstrap", challenge.Nonce},
		&bootstrapOut, &bytes.Buffer{}); err != nil {
		t.Fatalf("admin bootstrap: %v", err)
	}
	var adminPolicy l1.AdminPolicy
	if err := json.Unmarshal(bootstrapOut.Bytes(), &adminPolicy); err != nil {
		t.Fatal(err)
	}
	if adminPolicy.Revision != 1 || len(adminPolicy.Admins) != 1 || adminPolicy.Admins[0].UserID != "person-alice" {
		t.Fatalf("admin bootstrap policy = %#v", adminPolicy)
	}
	// The first command the skill tells an agent to run, against a stack that
	// is actually up: ready, with the node's capabilities and free slots.
	// The node registers on its own schedule, so this waits for `status` to
	// flip rather than assuming it already has -- which is also the proof that
	// it does flip, from a real not-ready to a real ready.
	clusterState := waitForReadyStatus(ctx, t, clients)
	if !clusterState.Ready || clusterState.Verdict != "ready" {
		t.Fatalf("status on a running stack = %#v", clusterState)
	}
	// The fixture's operator is a machine principal (it carries the client tag
	// and no person identity), and status must say that plainly rather than
	// surfacing the person protocol's refusal.
	if clusterState.Identity.Kind != identityMachine {
		t.Fatalf("status identity = %#v", clusterState.Identity)
	}
	if strings.Contains(clusterState.Identity.Detail, "principal_forbidden") {
		t.Fatalf("status leaked a protocol error as an identity: %q", clusterState.Identity.Detail)
	}
	if len(clusterState.Services) != 2 {
		t.Fatalf("status services = %#v", clusterState.Services)
	}
	for _, service := range clusterState.Services {
		if !service.Reachable {
			t.Fatalf("%s unreachable on a running stack: %#v", service.Name, service)
		}
		if service.Endpoint == "" {
			t.Fatalf("%s reported no endpoint: %#v", service.Name, service)
		}
	}
	if len(clusterState.Nodes) != 1 || clusterState.Nodes[0].NodeID != "node-cli" {
		t.Fatalf("status nodes = %#v", clusterState.Nodes)
	}
	statusNode := clusterState.Nodes[0]
	// The capabilities come from what the agent advertised, not from a probe
	// this command ran: this agent declares kind:process only.
	if strings.Join(statusNode.Kinds, ",") != "process" {
		t.Fatalf("status kinds = %v, want the agent's advertised process capability", statusNode.Kinds)
	}
	if statusNode.TotalOneshot <= 0 || statusNode.FreeOneshot <= 0 || !statusNode.AcceptsOneshot {
		t.Fatalf("status slots = %#v", statusNode)
	}
	if statusNode.State != contract.NodeAlive || !statusNode.ClaimsEnabled {
		t.Fatalf("status node = %#v", statusNode)
	}

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

	// A run's result document is uploaded by the node when it completes, so it
	// reads back through L3 with the person's own identity and no node in the
	// picture. This is the whole of `wefty results`, end to end.
	resultScript := filepath.Join(t.TempDir(), "result.sh")
	if err := os.WriteFile(resultScript, []byte(
		"#!/bin/sh\nprintf '{\"passed\":true,\"gates\":[]}' > \"$WEFTY_HANDOFF_DIR/result.json\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	var resultSubmitOut bytes.Buffer
	if err := execute(ctx, clients, true, []string{
		"submit", "--script", resultScript, "--params", `{}`,
		"--tag", "linux", "--tag", contract.StableNodeTagPrefix + "node-cli",
		"--idempotency-key", "cli-submit-result",
	}, &resultSubmitOut, &commandErr); err != nil {
		t.Fatalf("submit result run: %v stderr=%s", err, commandErr.String())
	}
	var resultRun l3.RunAccepted
	if err := json.Unmarshal(resultSubmitOut.Bytes(), &resultRun); err != nil {
		t.Fatal(err)
	}
	logsOut.Reset()
	logsErr.Reset()
	if err := execute(ctx, clients, false, []string{"logs", resultRun.RunID, "--follow", "--poll-interval", "5ms"}, &logsOut, &logsErr); err != nil {
		t.Fatalf("follow result run logs: %v", err)
	}
	// Raw stdout, not trimmed: the document must come back byte for byte, so
	// redirecting it to a file and using --out give the same bytes and the
	// same digest.
	document := waitForCLIResult(ctx, t, clients, resultRun.RunID)
	if document != `{"passed":true,"gates":[]}` {
		t.Fatalf("wefty results printed %q", document)
	}
	// --out writes the same bytes to a file, because a result is usually input
	// to the next thing rather than something to read.
	outPath := filepath.Join(t.TempDir(), "result.json")
	var resultOut bytes.Buffer
	if err := execute(ctx, clients, false, []string{"results", resultRun.RunID, "--out", outPath}, &resultOut, &commandErr); err != nil {
		t.Fatalf("wefty results --out: %v", err)
	}
	written, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(written) != document {
		t.Fatalf("--out wrote %q, stdout wrote %q", written, document)
	}
	var resultJSON bytes.Buffer
	if err := execute(ctx, clients, true, []string{"results", resultRun.RunID}, &resultJSON, &commandErr); err != nil {
		t.Fatalf("wefty --json results: %v", err)
	}
	var view resultDocumentView
	if err := json.Unmarshal(resultJSON.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	if view.RunID != resultRun.RunID || view.Bytes != len(written) || view.UploadedAt.IsZero() {
		t.Fatalf("--json results = %#v", view)
	}
	// The digest the ledger stored is the digest of exactly those bytes, which
	// is only true because nothing was appended on the way out.
	digest := sha256.Sum256(written)
	if view.SHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("stored digest %q does not cover the bytes the CLI emitted", view.SHA256)
	}
	// The document is embedded as JSON rather than a base64 string, so a
	// caller piping this into jq gets the run's own object. The encoder
	// re-indents it, so compare what it means and not how it is spaced.
	var embedded map[string]any
	if err := json.Unmarshal(view.Document, &embedded); err != nil {
		t.Fatalf("--json document is not the run's own object: %v (%s)", err, view.Document)
	}
	if embedded["passed"] != true {
		t.Fatalf("--json document = %s", view.Document)
	}
	var resultInspectOut bytes.Buffer
	if err := execute(ctx, clients, true, []string{"inspect", resultRun.RunID}, &resultInspectOut, &commandErr); err != nil {
		t.Fatalf("inspect the result run: %v", err)
	}
	var resultInspection runInspection
	if err := json.Unmarshal(resultInspectOut.Bytes(), &resultInspection); err != nil {
		t.Fatal(err)
	}
	if resultInspection.Results == nil || !resultInspection.Results.Uploaded ||
		resultInspection.Results.UploadSkipReason != "" {
		t.Fatalf("inspect results block = %#v", resultInspection.Results)
	}
	// The run that wrote no result reports the same block with uploaded false,
	// which is an ordinary answer rather than an error.
	if inspection.Results == nil || inspection.Results.Uploaded {
		t.Fatalf("a run with no result reported %#v", inspection.Results)
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

	// Observation: the general listing, the wait, and the --json arm of every
	// command an agent parses.
	var listOut bytes.Buffer
	if err := execute(ctx, clients, true, []string{"runs", "list"}, &listOut, &commandErr); err != nil {
		t.Fatalf("runs list: %v stderr=%s", err, commandErr.String())
	}
	var listing l3.RunListPage
	if err := json.Unmarshal(listOut.Bytes(), &listing); err != nil {
		t.Fatal(err)
	}
	if len(listing.Runs) < 3 {
		t.Fatalf("runs list returned %d runs, want every run this test submitted", len(listing.Runs))
	}
	// Newest first, so the most recently created run leads -- here the rerun,
	// which was submitted after everything else.
	if listing.Runs[0].RunID != rerun.RunID {
		t.Fatalf("runs list is not newest first: %s leads, want %s", listing.Runs[0].RunID, rerun.RunID)
	}
	var succeededOut bytes.Buffer
	if err := execute(ctx, clients, true, []string{"runs", "list", "--status", "succeeded", "--limit", "2"},
		&succeededOut, &commandErr); err != nil {
		t.Fatalf("runs list --status: %v stderr=%s", err, commandErr.String())
	}
	if err := json.Unmarshal(succeededOut.Bytes(), &listing); err != nil {
		t.Fatal(err)
	}
	if len(listing.Runs) == 0 || len(listing.Runs) > 2 {
		t.Fatalf("filtered listing returned %d runs", len(listing.Runs))
	}
	for _, run := range listing.Runs {
		if run.Status != contract.RunSucceeded {
			t.Fatalf("a %s run answered --status succeeded", run.Status)
		}
	}

	// wait on a run that has already succeeded returns immediately and zero.
	var waitOut bytes.Buffer
	if err := execute(ctx, clients, false, []string{"wait", submitted.RunID, "--timeout", "60s"},
		&waitOut, &commandErr); err != nil {
		t.Fatalf("wait on a succeeded run: %v", err)
	}
	if strings.TrimSpace(waitOut.String()) != string(contract.RunSucceeded) {
		t.Fatalf("wait printed %q", waitOut.String())
	}
	waitOut.Reset()
	if err := execute(ctx, clients, true, []string{"wait", submitted.RunID}, &waitOut, &commandErr); err != nil {
		t.Fatalf("wait --json: %v", err)
	}
	var waited contract.RunRecord
	if err := json.Unmarshal(waitOut.Bytes(), &waited); err != nil {
		t.Fatalf("wait --json is not JSON: %v (%s)", err, waitOut.String())
	}
	if waited.RunID != submitted.RunID {
		t.Fatalf("wait --json returned %s", waited.RunID)
	}

	// A failing run's wait exits 10, invoked as the CLI rather than read off a
	// status field. This is the branch-gates acceptance criterion, proved here
	// because the branch-gates exercise cannot import package main and the
	// in-process plain fabric does not span processes.
	failingScript := filepath.Join(t.TempDir(), "failing.sh")
	if err := os.WriteFile(failingScript, []byte("#!/bin/sh\nexit 3\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	var failingSubmit bytes.Buffer
	if err := execute(ctx, clients, true, []string{
		"submit", "--script", failingScript, "--params", `{}`,
		"--tag", "linux", "--tag", contract.StableNodeTagPrefix + "node-cli",
		"--idempotency-key", "cli-submit-failing",
	}, &failingSubmit, &commandErr); err != nil {
		t.Fatalf("submit failing run: %v stderr=%s", err, commandErr.String())
	}
	var failingRun l3.RunAccepted
	if err := json.Unmarshal(failingSubmit.Bytes(), &failingRun); err != nil {
		t.Fatal(err)
	}
	var failingWait bytes.Buffer
	waitErr := execute(ctx, clients, false, []string{"wait", failingRun.RunID, "--timeout", "120s"},
		&failingWait, &commandErr)
	if waitErr == nil {
		t.Fatalf("wait on a failing run exited zero: %s", failingWait.String())
	}
	if code := commandExitCode(waitErr); code != exitRunFailed {
		t.Fatalf("wait on a failing run exited %d (%v), want %d", code, waitErr, exitRunFailed)
	}
	if strings.TrimSpace(failingWait.String()) != string(contract.RunFailed) {
		t.Fatalf("wait on a failing run printed %q", failingWait.String())
	}

	// Every command an agent parses answers --json with JSON. This is the
	// audit, executed rather than asserted in prose.
	var whoOut bytes.Buffer
	if err := execute(ctx, personClients, true, []string{"whoami"}, &whoOut, &commandErr); err != nil {
		t.Fatalf("whoami --json: %v", err)
	}
	var person l1.AuthenticatedPerson
	if err := json.Unmarshal(whoOut.Bytes(), &person); err != nil {
		t.Fatalf("`wefty --json whoami` did not emit JSON: %v\n%s", err, whoOut.String())
	}
	for _, command := range [][]string{
		{"status"},
		{"runs", "list"},
		{"nodes", "list"},
		{"inspect", submitted.RunID},
		{"logs", submitted.RunID},
		{"results", resultRun.RunID},
		{"wait", submitted.RunID},
	} {
		var machine bytes.Buffer
		if err := execute(ctx, clients, true, command, &machine, &commandErr); err != nil {
			t.Fatalf("%v --json: %v stderr=%s", command, err, commandErr.String())
		}
		var any any
		if err := json.Unmarshal(machine.Bytes(), &any); err != nil {
			t.Fatalf("`wefty --json %s` did not emit JSON: %v\n%s", strings.Join(command, " "), err, machine.String())
		}
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

// waitForCLIResult polls `wefty results` until the node's upload has landed.
// The log follow above returns when the run is terminal, and the upload happens
// as the attempt completes, so the two are close but not ordered.
// waitForReadyStatus polls `wefty status` until the cluster reports ready, and
// returns that answer. A stack that never becomes ready fails here with the
// last verdict, which is the sentence that explains why.
func waitForReadyStatus(ctx context.Context, t *testing.T, clients *apiClients) clusterStatus {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var last clusterStatus
	for time.Now().Before(deadline) {
		var out, errOut bytes.Buffer
		err := execute(ctx, clients, true, []string{"status"}, &out, &errOut)
		if decodeErr := json.Unmarshal(out.Bytes(), &last); decodeErr != nil {
			t.Fatalf("status --json is not JSON: %v\n%s", decodeErr, out.String())
		}
		if err == nil {
			return last
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("the fixture stack never became ready; last verdict: %s", last.Verdict)
	return clusterStatus{}
}

func waitForCLIResult(ctx context.Context, t *testing.T, clients *apiClients, runID string) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		var out, errOut bytes.Buffer
		if err := execute(ctx, clients, false, []string{"results", runID}, &out, &errOut); err == nil {
			return out.String()
		} else {
			last = err
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("the run's result never reached the ledger: %v", last)
	return ""
}

// TestInspectResultsBlockSeparatesUploadedFromRetained pins the two sentences
// the block must keep apart: what a reader can fetch right now, and what is
// merely scheduled to exist on a node they may not be able to reach.
func TestInspectResultsBlockSeparatesUploadedFromRetained(t *testing.T) {
	t.Parallel()

	finished := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	render := func(t *testing.T, mutate func(*runResults)) string {
		t.Helper()
		results := runResultsFor(contract.RunRecord{
			RunID: "run-aaa", Status: contract.RunSucceeded, NodeID: "node-1", FinishedAt: &finished,
		}, finished.Add(time.Hour))
		if results == nil {
			t.Fatal("a finished run reported no results block")
		}
		mutate(results)
		var buf bytes.Buffer
		if err := writeRunInspection(&buf, runInspection{
			Run:     contract.RunRecord{RunID: "run-aaa", Status: contract.RunSucceeded},
			Lineage: l3.RunLineage{RunID: "run-aaa"},
			Runs:    []contract.RunRecord{{RunID: "run-aaa", Status: contract.RunSucceeded}},
			Results: results,
		}); err != nil {
			t.Fatal(err)
		}
		return buf.String()
	}

	uploaded := render(t, func(results *runResults) { results.Uploaded = true })
	if !strings.Contains(uploaded, "result document uploaded") || !strings.Contains(uploaded, "wefty results") {
		t.Errorf("an uploaded result is not reported:\n%s", uploaded)
	}
	skipped := render(t, func(results *runResults) {
		results.UploadSkipReason = contract.ResultUploadSkipOversize
	})
	if !strings.Contains(skipped, "not uploaded (exceeds_upload_bound)") || !strings.Contains(skipped, "on the node") {
		t.Errorf("a result that did not travel is not reported:\n%s", skipped)
	}
	// Nothing in the ledger is not proof the run wrote nothing: an upload that
	// never landed leaves no row either, so this line must send the reader to
	// the node rather than state a conclusion.
	unknown := render(t, func(*runResults) {})
	if !strings.Contains(unknown, "no result document reached the ledger") ||
		!strings.Contains(unknown, "the node that ran it records") {
		t.Errorf("an absent ledger row is reported as a conclusion:\n%s", unknown)
	}
	// A run that positively reported writing none says so.
	silent := render(t, func(results *runResults) {
		results.UploadSkipReason = contract.ResultUploadSkipAbsent
	})
	if !strings.Contains(silent, "the run wrote no result document") {
		t.Errorf("a run that wrote no result is not reported:\n%s", silent)
	}
	// The node-side retention line survives in every case: the files are a
	// separate fact from the document, and the block must not collapse them.
	for name, out := range map[string]string{
		"uploaded": uploaded, "skipped": skipped, "silent": silent, "unknown": unknown,
	} {
		if !strings.Contains(out, "files: retained until") || !strings.Contains(out, "node node-1") {
			t.Errorf("%s output lost the retention line:\n%s", name, out)
		}
	}
}

// TestExplicitFabricPrintEnrollmentURLFalseOverridesInheritedEnv covers
// wefty #498 finding 6: an inherited WEFTY_FABRIC_PRINT_ENROLLMENT_URL=1
// (set by a parent process, or a previous wefty invocation in the same
// shell) must not survive an explicit --fabric-print-enrollment-url=false
// on this invocation. flag.Bool's default read the inherited env var, so
// the explicit false must still win by the time the fabric is opened.
func TestExplicitFabricPrintEnrollmentURLFalseOverridesInheritedEnv(t *testing.T) {
	t.Setenv("WEFTY_FABRIC_PRINT_ENROLLMENT_URL", "1")

	var stderr bytes.Buffer
	options, _, err := parseGlobalOptions([]string{"--fabric-print-enrollment-url=false", "status"}, &stderr)
	if err != nil {
		t.Fatalf("parseGlobalOptions: %v", err)
	}
	if options.printEnrollmentURL {
		t.Fatal("explicit --fabric-print-enrollment-url=false was overridden by the inherited env var in the parsed options")
	}

	// run() re-seeds the environment from the resolved flag value right
	// before opening the fabric; the command itself is expected to fail
	// (no real fabric is set up here), but the env write happens first.
	_ = run(context.Background(), []string{"--fabric-print-enrollment-url=false", "status"}, io.Discard, io.Discard)

	if got := os.Getenv("WEFTY_FABRIC_PRINT_ENROLLMENT_URL"); got != "0" {
		t.Fatalf("WEFTY_FABRIC_PRINT_ENROLLMENT_URL = %q after run(), want %q", got, "0")
	}
}
