package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent/managedroot"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

func TestServiceCLIContractOverJobKeyedL1Routes(t *testing.T) {
	assertServiceCLIContractOverJobKeyedL1Routes(t)
}

func assertServiceCLIContractOverJobKeyedL1Routes(t *testing.T) {
	t.Helper()
	harness := newServiceCLIHarness(t)
	ctx := context.Background()
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	scriptPath := filepath.Join(t.TempDir(), "resident.sh")
	if err := os.WriteFile(scriptPath, []byte("#!/bin/sh\nprintf 'resident\\n'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	createArgs := []string{
		"services", "create", "--script", scriptPath,
		"--interpreter", "/bin/sh", "--mode", "0750",
		"--tag", "service-cli", "--published-port", "18080",
	}
	createdOutput := runServiceCLI(t, ctx, harness.clients, true, createArgs...)
	if bytes.Contains(createdOutput, []byte("run_id")) {
		t.Fatalf("service creation returned an L3 run identity: %s", createdOutput)
	}
	var createdID struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(createdOutput, &createdID); err != nil || createdID.JobID == "" {
		t.Fatalf("decode created service: id=%q err=%v output=%s", createdID.JobID, err, createdOutput)
	}

	created, err := harness.store.GetJob(ctx, createdID.JobID)
	if err != nil {
		t.Fatal(err)
	}
	canonicalScriptPath, err := filepath.EvalSymlinks(scriptPath)
	if err != nil {
		t.Fatal(err)
	}
	if created.Spec.DispatchKey != serviceDispatchKey(canonicalScriptPath) {
		t.Fatalf("default dispatch key = %q, want stable path-derived key", created.Spec.DispatchKey)
	}
	if created.Spec.Class != contract.JobClassService || created.Spec.Restart != contract.RestartAlways || created.Spec.Kind != "process" {
		t.Fatalf("service lifecycle fields = %#v", created.Spec)
	}
	if created.Spec.PublishedPort == nil || *created.Spec.PublishedPort != 18080 ||
		!equalStrings(created.Spec.RoutingTags, []string{"service-cli"}) ||
		!equalStrings(created.Spec.Execution.Executable.Interpreter, []string{"/bin/sh"}) ||
		created.Spec.Execution.Executable.Mode != 0o750 {
		t.Fatalf("service create flags were not translated exactly: %#v", created.Spec)
	}
	if created.Spec.Limits != nil || created.Spec.Execution.HandoffDirectory != "" ||
		len(created.Spec.Execution.Env) != 0 || len(created.Spec.Execution.SensitiveEnv) != 0 ||
		len(created.Spec.Labels) != 0 || created.Spec.Execution.WorkingDirectory != workingDirectory {
		t.Fatalf("service inherited run-only metadata: %#v", created.Spec)
	}

	replayedOutput := runServiceCLI(t, ctx, harness.clients, true, createArgs...)
	var replayedID struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(replayedOutput, &replayedID); err != nil || replayedID.JobID != createdID.JobID {
		t.Fatalf("retry-stable create = id %q err %v, want %q", replayedID.JobID, err, createdID.JobID)
	}
	for _, forbidden := range [][]string{
		{"--class", "service"}, {"--workflow-ref", "workflow://wrong"}, {"--params", `{}`},
		{"--required-envelope"}, {"--max-cost", "1"}, {"--max-runtime", "1"},
		{"--restart-policy", "always"},
	} {
		args := append(append([]string(nil), createArgs...), forbidden...)
		var stdout, stderr bytes.Buffer
		if err := execute(ctx, harness.clients, true, args, &stdout, &stderr); err == nil {
			t.Fatalf("services create accepted forbidden flag %q", forbidden[0])
		}
	}

	portlessScript := filepath.Join(t.TempDir(), "portless.sh")
	if err := os.WriteFile(portlessScript, []byte("#!/bin/sh\nsleep 60\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	portlessOutput := runServiceCLI(t, ctx, harness.clients, true,
		"services", "create", "--script", portlessScript, "--tag", "missing-node", "--idempotency-key", "portless-cli")
	var portless map[string]any
	if err := json.Unmarshal(portlessOutput, &portless); err != nil {
		t.Fatal(err)
	}
	portlessID, _ := portless["job_id"].(string)
	if portlessID == "" || portless["ready"] != nil {
		t.Fatalf("portless readiness must be explicit null: %s", portlessOutput)
	}
	if _, present := portless["managed_data_path"]; !present || portless["managed_data_path"] != nil {
		t.Fatalf("unbound managed path must be explicit null: %s", portlessOutput)
	}
	if portless["working_directory"] != workingDirectory {
		t.Fatalf("working directory = %#v, want %q", portless["working_directory"], workingDirectory)
	}
	if portless["working_directory_policy"] != "external; never deleted" ||
		portless["status"] != "unschedulable" || !strings.Contains(portless["unschedulable_reason"].(string), "routing tags") {
		t.Fatalf("portless status/path policy = %#v", portless)
	}

	firstPageOutput := runServiceCLI(t, ctx, harness.clients, true, "services", "list", "--limit", "1")
	var firstPage serviceListOutput
	if err := json.Unmarshal(firstPageOutput, &firstPage); err != nil {
		t.Fatal(err)
	}
	if len(firstPage.Jobs) != 1 || firstPage.Jobs[0].JobID != createdID.JobID || firstPage.NextCursor == "" {
		t.Fatalf("first stable service page = %#v", firstPage)
	}
	secondPageOutput := runServiceCLI(t, ctx, harness.clients, true,
		"services", "list", "--limit", "1", "--cursor", firstPage.NextCursor)
	var secondPage serviceListOutput
	if err := json.Unmarshal(secondPageOutput, &secondPage); err != nil {
		t.Fatal(err)
	}
	if len(secondPage.Jobs) != 1 || secondPage.Jobs[0].JobID != portlessID {
		t.Fatalf("second stable service page = %#v", secondPage)
	}
	secretSpec := created.Spec
	secretSpec.DispatchKey = "secret-bearing-service"
	secretSpec.PublishedPort = nil
	secretSpec.RoutingTags = []string{"missing-node"}
	secretSpec.Execution.SensitiveEnv = map[string]string{"SERVICE_TOKEN": "must-not-leak"}
	secretJob, _, err := harness.store.CreateJob(ctx, secretSpec)
	if err != nil {
		t.Fatal(err)
	}
	secretStatus := runServiceCLI(t, ctx, harness.clients, true, "services", "status", secretJob.JobID)
	secretList := runServiceCLI(t, ctx, harness.clients, true, "services", "list", "--limit", "10")
	for name, output := range map[string][]byte{"status": secretStatus, "list": secretList} {
		if bytes.Contains(output, []byte("SERVICE_TOKEN")) || bytes.Contains(output, []byte("must-not-leak")) {
			t.Fatalf("service %s leaked SensitiveEnv: %s", name, output)
		}
	}

	nodeIdentity := fabric.Identity{NodeID: "fabric-service-node"}
	node, err := harness.store.RegisterNode(ctx, nodeIdentity, contract.NodeRegistration{
		NodeID: "service-node", BootSessionID: "boot-service-cli", RootInstanceID: "root-service-cli",
		OS: "linux", Architecture: "amd64", AgentVersion: "service-cli-test",
		Capabilities: map[string]bool{"process": true},
	}, l1.NodePolicy{Tags: []string{"service-cli"}, MaxOneshotSlots: 4, MaxServiceSlots: 2}, true)
	if err != nil {
		t.Fatal(err)
	}
	firstClaim, err := harness.store.ClaimJob(ctx, nodeIdentity.NodeID, node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || firstClaim == nil || firstClaim.Job.JobID != createdID.JobID {
		t.Fatalf("claim first CLI service = %#v, %v", firstClaim, err)
	}
	appendServiceCLILog(t, harness.store, nodeIdentity.NodeID, createdID.JobID, firstClaim.Lease, "first attempt\n")

	statusOutput := runServiceCLI(t, ctx, harness.clients, true, "services", "status", createdID.JobID)
	var status map[string]any
	if err := json.Unmarshal(statusOutput, &status); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]any{
		"state": string(contract.JobRunning), "status": string(contract.JobRunning),
		"desired_state": string(contract.ServiceDesiredRunning), "bound_node_id": node.NodeID,
		"current_attempt_id": firstClaim.Lease.AttemptID, "holds_slot": true, "ready": false,
	} {
		if status[key] != want {
			t.Fatalf("status[%q] = %#v, want %#v; output=%s", key, status[key], want, statusOutput)
		}
	}
	if _, present := status["published_port"]; present {
		t.Fatalf("unready service exposed a top-level published port: %s", statusOutput)
	}
	wantManagedSuffix := filepath.ToSlash(filepath.Join(
		"agent", "nodes", managedroot.EncodeID(node.NodeID), "services", managedroot.EncodeID(createdID.JobID), "data",
	))
	if managed, _ := status["managed_data_path"].(string); !strings.HasSuffix(managed, wantManagedSuffix) {
		t.Fatalf("managed data path = %q, want suffix %q", managed, wantManagedSuffix)
	}
	ready := true
	if _, err := harness.store.SetAttemptPublication(ctx, nodeIdentity.NodeID, createdID.JobID, firstClaim.Lease.AttemptID, l1.PublicationRequest{
		FencingToken: firstClaim.Lease.FencingToken, Ready: &ready,
	}); err != nil {
		t.Fatal(err)
	}
	publishedOutput := runServiceCLI(t, ctx, harness.clients, true, "services", "status", createdID.JobID)
	var published map[string]any
	if err := json.Unmarshal(publishedOutput, &published); err != nil || published["ready"] != true || published["published_port"] != float64(18080) {
		t.Fatalf("ready publication projection = %#v, err=%v output=%s", published, err, publishedOutput)
	}
	humanStatus := runServiceCLI(t, ctx, harness.clients, false, "services", "status", createdID.JobID)
	for _, want := range []string{"STATE", "STATUS", "DESIRED", "HOLDS SLOT", "READY", "MANAGED DATA", "WORKING DIRECTORY", "external; never deleted"} {
		if !bytes.Contains(humanStatus, []byte(want)) {
			t.Fatalf("human status missing %q:\n%s", want, humanStatus)
		}
	}

	completionResult := make(chan error, 1)
	go func() {
		time.Sleep(25 * time.Millisecond)
		_, completeErr := harness.store.CompleteAttempt(ctx, nodeIdentity.NodeID, createdID.JobID, firstClaim.Lease.AttemptID, l1.CompletionRequest{
			FencingToken: firstClaim.Lease.FencingToken, IdempotencyKey: "cli-stop-first",
			Result: l1.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent},
		})
		completionResult <- completeErr
	}()
	stoppedOutput := runServiceCLI(t, ctx, harness.clients, true,
		"services", "stop", createdID.JobID, "--wait", "1s", "--poll-interval", "5ms")
	if err := <-completionResult; err != nil {
		t.Fatal(err)
	}
	var stopped map[string]any
	if err := json.Unmarshal(stoppedOutput, &stopped); err != nil || stopped["state"] != string(contract.JobStopped) || stopped["holds_slot"] != false {
		t.Fatalf("bounded stop = %#v, err=%v output=%s", stopped, err, stoppedOutput)
	}

	secondClaimResult := make(chan struct {
		claim *l1.Claim
		err   error
	}, 1)
	go func() {
		time.Sleep(25 * time.Millisecond)
		claim, claimErr := harness.store.ClaimJob(ctx, nodeIdentity.NodeID, node.NodeID, node.BootSessionID, contract.JobClassService)
		if claimErr == nil && claim != nil {
			appendRequest := l1.AppendLogsRequest{FencingToken: claim.Lease.FencingToken, Events: []contract.LogEvent{{
				AttemptID: claim.Lease.AttemptID, Stream: contract.LogStdout, Sequence: 0,
				Timestamp: time.Now().UTC(), Bytes: []byte("second attempt\n"),
			}}}
			_, claimErr = harness.store.AppendLogs(ctx, nodeIdentity.NodeID, claim.Job.JobID, claim.Lease.AttemptID, appendRequest)
		}
		secondClaimResult <- struct {
			claim *l1.Claim
			err   error
		}{claim: claim, err: claimErr}
	}()
	startedOutput := runServiceCLI(t, ctx, harness.clients, true,
		"services", "start", createdID.JobID, "--wait", "1s", "--poll-interval", "5ms")
	secondResult := <-secondClaimResult
	if secondResult.err != nil || secondResult.claim == nil || secondResult.claim.Job.JobID != createdID.JobID {
		t.Fatalf("claim resumed service = %#v, %v", secondResult.claim, secondResult.err)
	}
	var started map[string]any
	if err := json.Unmarshal(startedOutput, &started); err != nil || started["state"] != string(contract.JobRunning) {
		t.Fatalf("bounded start = %#v, err=%v output=%s", started, err, startedOutput)
	}

	if _, err := harness.store.CompleteAttempt(ctx, nodeIdentity.NodeID, createdID.JobID, secondResult.claim.Lease.AttemptID, l1.CompletionRequest{
		FencingToken: secondResult.claim.Lease.FencingToken, IdempotencyKey: "cli-latch-second",
		Result: l1.ProcessResult{SpawnError: &contract.SpawnFailure{
			Code: contract.SpawnFailureUnsupportedKind, Message: "operator must repair the service",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	failedStatusOutput := runServiceCLI(t, ctx, harness.clients, true, "services", "status", createdID.JobID)
	var failedStatus map[string]any
	if err := json.Unmarshal(failedStatusOutput, &failedStatus); err != nil ||
		failedStatus["state"] != string(contract.JobFailed) || failedStatus["holds_slot"] != false ||
		failedStatus["last_failure"] == nil || !strings.Contains(failedStatus["restart_suppressed_reason"].(string), "restart") {
		t.Fatalf("latched failure status = %#v, err=%v output=%s", failedStatus, err, failedStatusOutput)
	}
	var startOut, startErr bytes.Buffer
	err = execute(ctx, harness.clients, true, []string{"services", "start", createdID.JobID}, &startOut, &startErr)
	if err == nil || !strings.Contains(err.Error(), "use restart") {
		t.Fatalf("start of latched failure = %v, want restart instruction", err)
	}

	logsOutput := runServiceCLI(t, ctx, harness.clients, false, "services", "logs", createdID.JobID)
	for _, want := range []string{
		"--- attempt " + firstClaim.Lease.AttemptID + " ---", "first attempt",
		"--- attempt " + secondResult.claim.Lease.AttemptID + " ---", "second attempt",
	} {
		if !bytes.Contains(logsOutput, []byte(want)) {
			t.Fatalf("service logs missing %q:\n%s", want, logsOutput)
		}
	}
	followStarted := time.Now()
	_ = runServiceCLI(t, ctx, harness.clients, false,
		"services", "logs", createdID.JobID, "--follow", "--follow-for", "40ms", "--poll-interval", "5ms")
	if elapsed := time.Since(followStarted); elapsed < 30*time.Millisecond {
		t.Fatalf("service log follow stopped at terminal attempt after %s", elapsed)
	}

	var restartOut, restartErr bytes.Buffer
	err = execute(ctx, harness.clients, true, []string{"services", "restart", createdID.JobID}, &restartOut, &restartErr)
	if err == nil || !strings.Contains(err.Error(), "requires --idempotency-key") {
		t.Fatalf("restart without replay identity = %v", err)
	}
	restartedOutput := runServiceCLI(t, ctx, harness.clients, true,
		"services", "restart", createdID.JobID, "--idempotency-key", "repair-after-latch")
	var restarted map[string]any
	if err := json.Unmarshal(restartedOutput, &restarted); err != nil || restarted["state"] != string(contract.JobQueued) {
		t.Fatalf("explicit restart = %#v, err=%v output=%s", restarted, err, restartedOutput)
	}
	restartPendingClaim, err := harness.store.ClaimJob(ctx, nodeIdentity.NodeID, node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || restartPendingClaim == nil || restartPendingClaim.Job.JobID != createdID.JobID {
		t.Fatalf("claim explicitly restarted service = %#v, %v", restartPendingClaim, err)
	}
	appendServiceCLILog(t, harness.store, nodeIdentity.NodeID, createdID.JobID, restartPendingClaim.Lease, "third attempt\n")
	exitZero := 0
	if _, err := harness.store.CompleteAttempt(ctx, nodeIdentity.NodeID, createdID.JobID, restartPendingClaim.Lease.AttemptID, l1.CompletionRequest{
		FencingToken: restartPendingClaim.Lease.FencingToken, IdempotencyKey: "cli-restart-pending",
		Result: l1.ProcessResult{ExitCode: &exitZero},
	}); err != nil {
		t.Fatal(err)
	}
	restartPendingOutput := runServiceCLI(t, ctx, harness.clients, true, "services", "status", createdID.JobID)
	var restartPending map[string]any
	if err := json.Unmarshal(restartPendingOutput, &restartPending); err != nil ||
		restartPending["state"] != string(contract.JobQueued) || restartPending["status"] != "restart-pending" ||
		restartPending["next_restart_at"] == nil || restartPending["holds_slot"] != true {
		t.Fatalf("restart-pending status = %#v, err=%v output=%s", restartPending, err, restartPendingOutput)
	}

	jobPrefix := strings.TrimSuffix(createdID.JobID, createdID.JobID[len(createdID.JobID)-1:])
	var mutationOut, mutationErr bytes.Buffer
	err = execute(ctx, harness.clients, true, []string{"services", "stop", jobPrefix}, &mutationOut, &mutationErr)
	if err == nil || !strings.Contains(err.Error(), "not_found") {
		t.Fatalf("prefix mutation was not rejected exactly: %v", err)
	}

	waitStarted := time.Now()
	var waitOut, waitErr bytes.Buffer
	err = execute(ctx, harness.clients, true, []string{
		"services", "start", portlessID, "--wait", "30ms", "--poll-interval", "5ms",
	}, &waitOut, &waitErr)
	if err == nil || !strings.Contains(err.Error(), "timed out after 30ms") || time.Since(waitStarted) > time.Second {
		t.Fatalf("bounded unschedulable wait = %v after %s", err, time.Since(waitStarted))
	}

	removedOutput := runServiceCLI(t, ctx, harness.clients, true,
		"services", "remove", portlessID, "--wait", "100ms", "--poll-interval", "5ms")
	var removed map[string]any
	if err := json.Unmarshal(removedOutput, &removed); err != nil ||
		removed["state"] != string(contract.JobRemovedVerified) ||
		removed["status"] != "removed (agent-confirmed)" ||
		removed["desired_state"] != string(contract.ServiceDesiredRemoved) {
		t.Fatalf("bounded remove = %#v, err=%v output=%s", removed, err, removedOutput)
	}
}

type serviceCLIHarness struct {
	store   *l1.Store
	clients *apiClients
}

func newServiceCLIHarness(t *testing.T) *serviceCLIHarness {
	t.Helper()
	network := plain.NewNetwork()
	controlFabric := network.NewFabric(fabric.Identity{NodeID: "service-cli-control"})
	operatorFabric := network.NewFabric(fabric.Identity{
		NodeID: "service-cli-operator", Tags: []string{l1.DefaultClientPrincipalTag},
	})
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	server, err := l1.NewServer(controlFabric, store, l1.ServerConfig{})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	listener, err := controlFabric.Listen("tcp", l3.DefaultL1Address)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := serveTestServer(ctx, func() error { return server.Serve(ctx, listener) })
	clients, err := newAPIClients(operatorFabric, l3.DefaultL1Address, l3.DefaultL3Address)
	if err != nil {
		cancel()
		_ = <-done
		_ = store.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		clients.close()
		cancel()
		if err := <-done; err != nil {
			t.Errorf("L1 server: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Errorf("close L1 store: %v", err)
		}
	})
	return &serviceCLIHarness{store: store, clients: clients}
}

func runServiceCLI(t *testing.T, ctx context.Context, clients *apiClients, jsonOutput bool, args ...string) []byte {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if err := execute(ctx, clients, jsonOutput, args, &stdout, &stderr); err != nil {
		t.Fatalf("execute %q: %v stderr=%s", args, err, stderr.String())
	}
	return stdout.Bytes()
}

func appendServiceCLILog(t *testing.T, store *l1.Store, identityNodeID, jobID string, lease l1.AttemptLease, contents string) {
	t.Helper()
	_, err := store.AppendLogs(context.Background(), identityNodeID, jobID, lease.AttemptID, l1.AppendLogsRequest{
		FencingToken: lease.FencingToken,
		Events: []contract.LogEvent{{
			AttemptID: lease.AttemptID, Stream: contract.LogStdout, Sequence: 0,
			Timestamp: time.Now().UTC(), Bytes: []byte(contents),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func TestServiceLogFollowEndsOnCancellation(t *testing.T) {
	harness := newServiceCLIHarness(t)
	scriptPath := filepath.Join(t.TempDir(), "cancelled-follow.sh")
	if err := os.WriteFile(scriptPath, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	createdOutput := runServiceCLI(t, context.Background(), harness.clients, true,
		"services", "create", "--script", scriptPath, "--tag", "missing")
	var created struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(createdOutput, &created); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var stdout, stderr bytes.Buffer
	err := execute(ctx, harness.clients, false, []string{
		"services", "logs", created.JobID, "--follow", "--poll-interval", "5ms",
	}, &stdout, &stderr)
	if !errors.Is(err, context.Canceled) && (err == nil || !strings.Contains(err.Error(), context.Canceled.Error())) {
		t.Fatalf("cancelled service follow = %v", err)
	}
}

func TestServiceCLIRemoveAndForceForget(t *testing.T) {
	assertServiceCLIRemoveAndForceForget(t)
}

func assertServiceCLIRemoveAndForceForget(t *testing.T) {
	t.Helper()
	harness := newServiceCLIHarness(t)
	ctx := context.Background()
	workingDirectory := t.TempDir()
	sentinelPath := filepath.Join(workingDirectory, "operator-owned")
	if err := os.WriteFile(sentinelPath, []byte("untouched"), 0o600); err != nil {
		t.Fatal(err)
	}
	job, _, err := harness.store.CreateJob(ctx, contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		DispatchKey:   "cli-remove-force-forget",
		Kind:          "process",
		Class:         contract.JobClassService,
		Restart:       contract.RestartAlways,
		RoutingTags:   []string{"remove-cli"},
		Execution: contract.ExecutionSpec{
			Executable:       contract.ExecutableSpec{Path: "/bin/sh"},
			Argv:             []string{"/bin/sh", "-c", "sleep 60"},
			WorkingDirectory: workingDirectory,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	identity := fabric.Identity{NodeID: "remove-cli-fabric-node"}
	node, err := harness.store.RegisterNode(ctx, identity, contract.NodeRegistration{
		NodeID: "remove-cli-node", BootSessionID: "remove-cli-boot", RootInstanceID: "remove-cli-root",
		OS: "linux", Architecture: "amd64", AgentVersion: "remove-cli-test",
		Capabilities: map[string]bool{"process": true},
	}, l1.NodePolicy{Tags: []string{"remove-cli"}, MaxOneshotSlots: 4, MaxServiceSlots: 2}, true)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := harness.store.ClaimJob(ctx, identity.NodeID, node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil || claim.Job.JobID != job.JobID {
		t.Fatalf("claim removal service = %#v, %v", claim, err)
	}

	removedOutput := runServiceCLI(t, ctx, harness.clients, false, "services", "remove", job.JobID)
	managedDataPath := filepath.ToSlash(filepath.Join(
		"<managed-root>", "agent", "nodes", managedroot.EncodeID(node.NodeID),
		"services", managedroot.EncodeID(job.JobID), "data",
	))
	for _, expected := range []string{
		string(contract.JobRemovalPending),
		managedDataPath + " (deleted by remove)",
		workingDirectory + " (external; never deleted)",
	} {
		if !bytes.Contains(removedOutput, []byte(expected)) {
			t.Fatalf("remove output omitted %q: %s", expected, removedOutput)
		}
	}
	if payload, readErr := os.ReadFile(sentinelPath); readErr != nil || string(payload) != "untouched" {
		t.Fatalf("remove touched operator WorkingDirectory: payload=%q err=%v", payload, readErr)
	}

	replayed := runServiceCLI(t, ctx, harness.clients, true, "services", "remove", job.JobID)
	var replayedRemoval map[string]any
	if err := json.Unmarshal(replayed, &replayedRemoval); err != nil ||
		replayedRemoval["state"] != string(contract.JobRemovalPending) {
		t.Fatalf("idempotent remove = %#v, err=%v output=%s", replayedRemoval, err, replayed)
	}
	waitStarted := time.Now()
	var waitOut, waitErr bytes.Buffer
	err = execute(ctx, harness.clients, true, []string{
		"services", "remove", job.JobID, "--wait", "30ms", "--poll-interval", "5ms",
	}, &waitOut, &waitErr)
	if err == nil || !strings.Contains(err.Error(), "timed out after 30ms") || time.Since(waitStarted) > time.Second {
		t.Fatalf("bounded pending removal wait = %v after %s", err, time.Since(waitStarted))
	}

	var forgetOut, forgetErr bytes.Buffer
	err = execute(ctx, harness.clients, true, []string{"services", "forget", job.JobID}, &forgetOut, &forgetErr)
	if err == nil || !strings.Contains(err.Error(), "requires --force") {
		t.Fatalf("forget without proof waiver = %v", err)
	}
	forgottenOutput := runServiceCLI(t, ctx, harness.clients, true, "services", "forget", job.JobID, "--force")
	var forgotten map[string]any
	if err := json.Unmarshal(forgottenOutput, &forgotten); err != nil ||
		forgotten["state"] != string(contract.JobForgottenCleanupUnverified) ||
		forgotten["status"] != "forgotten (cleanup unverified)" {
		t.Fatalf("force forget = %#v, err=%v output=%s", forgotten, err, forgottenOutput)
	}
	statusOutput := runServiceCLI(t, ctx, harness.clients, false, "services", "status", job.JobID)
	if !bytes.Contains(statusOutput, []byte("forgotten (cleanup unverified)")) {
		t.Fatalf("force-forget warning was not permanently visible: %s", statusOutput)
	}
	directives, err := harness.store.ListNodeRemovalDirectives(ctx, identity.NodeID, node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 || directives[0].JobID != job.JobID {
		t.Fatalf("force forget cancelled standing deletion directive: %#v, %v", directives, err)
	}
	if payload, readErr := os.ReadFile(sentinelPath); readErr != nil || string(payload) != "untouched" {
		t.Fatalf("force forget invoked local deletion: payload=%q err=%v", payload, readErr)
	}
}
