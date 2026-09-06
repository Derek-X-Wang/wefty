//go:build service_acceptance

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

const (
	ociOneshotAcceptanceReference = "ghcr.io/derek-x-wang/wefty-echo-service:acceptance"
	ociOneshotAcceptanceDigest    = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func TestServiceAcceptanceOrdinaryL3RunDispatchesOCIOneshot(t *testing.T) {
	network := plain.NewNetwork()
	controlFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	ledgerFabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger", Tags: []string{l1.DefaultClientPrincipalTag}})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "agent-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	callerFabric := network.NewFabric(fabric.Identity{NodeID: "caller", UserID: "acceptance-person", Tags: []string{l3.DefaultCallerPrincipalTag}})

	l1Store, err := l1.OpenStore(t.TempDir()+"/l1.sqlite", l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l1Store.Close() })
	l1Server, err := l1.NewServer(controlFabric, l1Store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{
		"node-1": l1.DefaultNodePolicy("linux", contract.StableNodeTagPrefix+"node-1"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	l1Listener, err := controlFabric.Listen("tcp", l3.DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}

	l3Store, err := l3.OpenStore(t.TempDir()+"/l3.sqlite", l3.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l3Store.Close() })
	l1Client, err := l3.NewL1Client(ledgerFabric, l3.DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(l1Client.CloseIdleConnections)
	reconciler, err := l3.NewReconciler(l3Store, l1Client, l3.ReconcilerConfig{Interval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	l3Server, err := l3.NewServer(ledgerFabric, l3Store, l3.ServerConfig{Reconciler: reconciler, Logs: l1Client})
	if err != nil {
		t.Fatal(err)
	}
	l3Listener, err := ledgerFabric.Listen("tcp", l3.DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	served := make(chan error, 2)
	go func() { served <- l1Server.Serve(ctx, l1Listener) }()
	go func() { served <- l3Server.Serve(ctx, l3Listener) }()

	runtime := &ociOneshotAcceptanceRuntime{
		firstReapUnavailable: make(chan struct{}),
		recoveryReady:        make(chan struct{}),
	}
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: l3.DefaultL1Address, RunLedgerAddress: l3.DefaultL3Address,
		NodeID: "node-1", BootSessionID: "boot-1", Version: "acceptance", OS: "linux", Architecture: "amd64",
		Capabilities: map[string]bool{"kind:process": true},
		CapabilityProbe: capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
			return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true}}, nil
		}),
		OCIIntent:         enabledTestOCIIntent,
		OCIBootBarrier:    readyOCIBootBarrier{},
		WorkloadRuntimes:  map[string]WorkloadRuntime{contract.JobKindOCI: runtime},
		AttemptDeadman:    &recordingDeadmanRenewer{},
		HeartbeatInterval: 20 * time.Millisecond, ClaimInterval: 5 * time.Millisecond, RenewalInterval: 50 * time.Millisecond,
		LogSpoolDirectory: t.TempDir(), HandoffRoot: t.TempDir(), ManagedRootDirectory: managedRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nodeAgent.Close)
	agentDone := make(chan error, 1)
	go func() { agentDone <- nodeAgent.Run(ctx) }()

	caller := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, networkName, _ string) (net.Conn, error) {
		return callerFabric.Dial(ctx, networkName, l3.DefaultL3Address)
	}}}
	t.Cleanup(caller.CloseIdleConnections)
	request := l3.CreateRunRequest{
		Image: &contract.ImageProgram{
			Reference: ociOneshotAcceptanceReference,
			Argv:      []string{"wefty-echo-service", "--once"},
		},
		Params: json.RawMessage(`{"ticket":146}`),
		Tags:   []string{"linux"},
	}
	var accepted l3.RunAccepted
	doOCIOneshotJSON(t, caller, http.MethodPost, "/v1/runs", request, http.Header{"Idempotency-Key": []string{"oci-oneshot-acceptance"}}, http.StatusCreated, &accepted)
	select {
	case <-runtime.firstReapUnavailable:
		close(runtime.recoveryReady)
	case <-time.After(5 * time.Second):
		t.Fatal("pre-start runtime loss did not reach reap while the helper session was unavailable")
	}
	original := waitForOCIOneshotRun(t, caller, accepted.RunID, contract.RunSucceeded, 10*time.Second)
	if original.L1JobID == "" {
		t.Fatalf("successful OCI run omitted L1 job identity: %+v", original)
	}

	var logs l1.LogPage
	doOCIOneshotJSON(t, caller, http.MethodGet, "/v1/runs/"+accepted.RunID+"/logs", nil, nil, http.StatusOK, &logs)
	if !hasOCIOneshotLog(logs.Events, contract.LogStdout, "wefty-echo-once-stdout\n") ||
		!hasOCIOneshotLog(logs.Events, contract.LogStderr, "wefty-echo-once-stderr\n") {
		t.Fatalf("OCI one-shot logs = %+v", logs.Events)
	}

	var rerun l3.RunAccepted
	doOCIOneshotJSON(t, caller, http.MethodPost, "/v1/runs/"+accepted.RunID+"/rerun", nil,
		http.Header{"Idempotency-Key": []string{"oci-oneshot-rerun"}}, http.StatusCreated, &rerun)
	rerunRecord := waitForOCIOneshotRun(t, caller, rerun.RunID, contract.RunFailed, 10*time.Second)
	if rerunRecord.L1JobID == "" {
		t.Fatalf("failed OCI rerun omitted L1 job identity: %+v", rerunRecord)
	}

	runtime.mu.Lock()
	runCalls := runtime.runCalls
	payloadStarts := runtime.payloadStarts
	attemptIDs := slices.Clone(runtime.attemptIDs)
	jobIDs := slices.Clone(runtime.jobIDs)
	runIDs := slices.Clone(runtime.runIDs)
	requestDigests := slices.Clone(runtime.requestDigests)
	bridgeRuns := slices.Clone(runtime.bridgeRuns)
	volumeOwners := slices.Clone(runtime.volumeOwners)
	finalizedOwners := slices.Clone(runtime.finalizedOwners)
	runtime.mu.Unlock()
	if runCalls != 3 || payloadStarts != 2 {
		t.Fatalf("runtime calls=%d payload starts=%d, want one pre-start loss plus two unique starts", runCalls, payloadStarts)
	}
	if len(attemptIDs) != 3 || attemptIDs[0] == attemptIDs[1] || attemptIDs[1] == attemptIDs[2] {
		t.Fatalf("attempt identities = %v, want three distinct fences", attemptIDs)
	}
	if len(jobIDs) != 3 || jobIDs[0] != jobIDs[1] || jobIDs[1] == jobIDs[2] ||
		!slices.Equal(runIDs, []string{accepted.RunID, accepted.RunID, rerun.RunID}) {
		t.Fatalf("job/run identities = jobs %v runs %v, want retry then explicit rerun", jobIDs, runIDs)
	}
	if requestDigests[0] != "" || requestDigests[1] != ociOneshotAcceptanceDigest ||
		requestDigests[2] != ociOneshotAcceptanceDigest {
		t.Fatalf("dispatch digests = %v, want resolve once then frozen retries/rerun", requestDigests)
	}
	if !slices.Equal(bridgeRuns, []string{accepted.RunID}) {
		t.Fatalf("successful bridge calls = %v, want original run exactly once", bridgeRuns)
	}
	if !slices.Equal(volumeOwners, []string{accepted.RunID, accepted.RunID, accepted.RunID}) {
		t.Fatalf("managed handoff owners = %v, want stable source-run identity across retry and rerun", volumeOwners)
	}
	if !slices.Equal(finalizedOwners, []string{accepted.RunID}) {
		t.Fatalf("finalized handoff owners = %v, want deletion only after accepted success", finalizedOwners)
	}

	cancel()
	if err := <-agentDone; err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := <-served; err != nil {
			t.Fatal(err)
		}
	}
}

type ociOneshotAcceptanceRuntime struct {
	mu                   sync.Mutex
	runCalls             int
	payloadStarts        int
	attemptIDs           []string
	jobIDs               []string
	runIDs               []string
	requestDigests       []string
	bridgeRuns           []string
	volumeOwners         []string
	finalizedOwners      []string
	reapCalls            int
	firstReapUnavailable chan struct{}
	recoveryReady        chan struct{}
}

func (runtime *ociOneshotAcceptanceRuntime) Preflight(_ context.Context, request workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	return workloadrunner.Admission{Request: request, Release: func() {}}, workloadrunner.Result{}, nil
}

func (runtime *ociOneshotAcceptanceRuntime) Run(ctx context.Context, request workloadrunner.Request, sink workloadrunner.OutputSink) (workloadrunner.Result, error) {
	runtime.mu.Lock()
	runtime.runCalls++
	call := runtime.runCalls
	runtime.attemptIDs = append(runtime.attemptIDs, request.Authority.AttemptID)
	runtime.jobIDs = append(runtime.jobIDs, request.Authority.JobID)
	runtime.runIDs = append(runtime.runIDs, request.Execution.Env[contract.EnvRunID])
	if request.Execution.OCI == nil {
		runtime.mu.Unlock()
		return workloadrunner.Result{}, errors.New("kind=oci reached its adapter without an OCI execution arm")
	}
	digest := ""
	if request.Execution.OCI.Image.Digest != nil {
		digest = *request.Execution.OCI.Image.Digest
	}
	runtime.requestDigests = append(runtime.requestDigests, digest)
	if len(request.ManagedVolumes) != 1 || request.ManagedVolumes[0].Kind != workloadrunner.ManagedVolumeHandoff {
		runtime.mu.Unlock()
		return workloadrunner.Result{}, errors.New("OCI one-shot managed handoff descriptor is incomplete")
	}
	runtime.volumeOwners = append(runtime.volumeOwners, request.ManagedVolumes[0].OwnerKey)
	runtime.mu.Unlock()

	if request.Execution.Env[contract.EnvHandoffDir] != contract.OCIContainerHandoffDirectory ||
		request.Execution.Env[contract.EnvRunID] == "" || request.Execution.Env[contract.EnvL3Endpoint] == "" ||
		request.Execution.SensitiveEnv[contract.EnvRunToken] == "" ||
		request.Execution.OCI.Image.Reference != ociOneshotAcceptanceReference ||
		!slices.Equal(request.Execution.OCI.Argv, []string{"wefty-echo-service", "--once"}) {
		return workloadrunner.Result{}, errors.New("OCI one-shot reserved execution context is incomplete")
	}
	observation := workloadrunner.OCIImageObservation{
		SubmittedReference: request.Execution.OCI.Image.Reference,
		TopLevelDigest:     ociOneshotAcceptanceDigest, TopLevelMediaType: "application/vnd.oci.image.manifest.v1+json",
		PlatformManifestDigest: ociOneshotAcceptanceDigest,
		PlatformOS:             "linux", PlatformArchitecture: "amd64",
		RuntimeHandler: "io.containerd.runc.v2", Snapshotter: "overlayfs",
	}
	if err := request.OCIImageResolved(ctx, observation); err != nil {
		return workloadrunner.Result{}, err
	}
	if call == 1 {
		err := errors.New("engine lost before Started")
		return workloadrunner.Result{Outcome: contract.ProcessResult{SpawnError: &contract.SpawnFailure{
			Code: contract.SpawnFailureRuntimeUnavailable, Message: err.Error(),
		}}}, err
	}
	if err := request.OCIStarted(ctx, observation); err != nil {
		return workloadrunner.Result{}, err
	}
	if request.OCIHelperAdmitted != nil {
		helperSession, _ := (readyOCIBootBarrier{}).Generation()
		generation := workloadrunner.RuntimeGeneration{
			InstanceID: helperSession.HelperInstanceID, Generation: helperSession.SessionGeneration,
		}
		if err := request.OCIHelperAdmitted(generation); err != nil {
			return workloadrunner.Result{}, err
		}
	}
	runtime.mu.Lock()
	runtime.payloadStarts++
	runtime.mu.Unlock()
	if call == 3 {
		err := errors.New("engine lost after Started")
		return workloadrunner.Result{Outcome: contract.ProcessResult{RuntimeFailure: &contract.RuntimeFailure{
			Code: contract.RuntimeFailureUnavailable, Message: err.Error(),
		}}}, err
	}

	runID := request.Execution.Env[contract.EnvRunID]
	bridgeRequest, err := http.NewRequestWithContext(ctx, http.MethodGet,
		strings.TrimRight(request.Execution.Env[contract.EnvL3Endpoint], "/")+"/v1/runs/"+runID, nil)
	if err != nil {
		return workloadrunner.Result{}, err
	}
	bridgeRequest.Header.Set("Authorization", "Bearer "+request.Execution.SensitiveEnv[contract.EnvRunToken])
	response, err := http.DefaultClient.Do(bridgeRequest)
	if err != nil {
		return workloadrunner.Result{}, err
	}
	_, copyErr := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	if response.StatusCode != http.StatusOK || copyErr != nil || closeErr != nil {
		return workloadrunner.Result{}, errors.Join(copyErr, closeErr, errors.New(response.Status))
	}
	runtime.mu.Lock()
	runtime.bridgeRuns = append(runtime.bridgeRuns, runID)
	runtime.mu.Unlock()
	for _, event := range []contract.LogEvent{
		{AttemptID: request.Authority.AttemptID, Stream: contract.LogStdout, Sequence: 0, Bytes: []byte("wefty-echo-once-stdout\n")},
		{AttemptID: request.Authority.AttemptID, Stream: contract.LogStderr, Sequence: 0, Bytes: []byte("wefty-echo-once-stderr\n")},
	} {
		if err := sink.WriteOutput(ctx, event); err != nil {
			return workloadrunner.Result{}, err
		}
	}
	exitCode := 0
	return workloadrunner.Result{Outcome: contract.ProcessResult{ExitCode: &exitCode}}, nil
}

func (runtime *ociOneshotAcceptanceRuntime) ReapAndVerify(ctx context.Context, _ workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	runtime.mu.Lock()
	runtime.reapCalls++
	call := runtime.reapCalls
	runtime.mu.Unlock()
	if call == 1 {
		close(runtime.firstReapUnavailable)
		select {
		case <-runtime.recoveryReady:
		case <-ctx.Done():
			return workloadrunner.ReapReceipt{}, ctx.Err()
		}
		return workloadrunner.ReapReceipt{
			RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceOCIRuntimeSweep,
			SweepEpoch: "replacement-sweep", HelperGeneration: 2,
		}, nil
	}
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}, nil
}

func (runtime *ociOneshotAcceptanceRuntime) FinalizeManagedVolumes(_ context.Context, request workloadrunner.ManagedVolumeFinalizationRequest) error {
	if len(request.Volumes) != 1 || request.Volumes[0].Kind != workloadrunner.ManagedVolumeHandoff || request.Volumes[0].OwnerKey == "" {
		return errors.New("managed handoff finalization request is incomplete")
	}
	runtime.mu.Lock()
	runtime.finalizedOwners = append(runtime.finalizedOwners, request.Volumes[0].OwnerKey)
	runtime.mu.Unlock()
	return nil
}

func doOCIOneshotJSON(t *testing.T, client *http.Client, method, path string, input any, headers http.Header, wantStatus int, output any) {
	t.Helper()
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, "http://wefty.invalid"+path, body)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	for name, values := range headers {
		request.Header[name] = append([]string(nil), values...)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != wantStatus {
		t.Fatalf("%s %s status=%d want=%d body=%s", method, path, response.StatusCode, wantStatus, payload)
	}
	if output != nil {
		if err := json.Unmarshal(payload, output); err != nil {
			t.Fatalf("decode %s %s: %v body=%s", method, path, err, payload)
		}
	}
}

func waitForOCIOneshotRun(t *testing.T, client *http.Client, runID string, want contract.RunState, timeout time.Duration) contract.RunRecord {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var record contract.RunRecord
		doOCIOneshotJSON(t, client, http.MethodGet, "/v1/runs/"+runID, nil, nil, http.StatusOK, &record)
		if record.Status == want {
			return record
		}
		if record.Status == contract.RunFailed && want != contract.RunFailed {
			t.Fatalf("run %s failed while waiting for %s", runID, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for run %s state %s", runID, want)
	return contract.RunRecord{}
}

func hasOCIOneshotLog(events []contract.LogEvent, stream contract.LogStream, content string) bool {
	for _, event := range events {
		if event.Stream == stream && string(event.Bytes) == content {
			return true
		}
	}
	return false
}
