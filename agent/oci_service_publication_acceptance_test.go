//go:build (service_acceptance_realtiming && linux) || (service_acceptance && darwin)

package agent

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	ocirunner "github.com/Derek-X-Wang/wefty/runner/oci"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

func TestOCIServicePublicationThroughHelperTunnel(t *testing.T) {
	helperSocket := os.Getenv("WEFTY_OCI_HELPER_SOCKET")
	helperChecksum := os.Getenv("WEFTY_OCI_HELPER_CHECKSUM")
	reference := os.Getenv("WEFTY_OCI_PROBE_REFERENCE")
	digest := os.Getenv("WEFTY_OCI_PROBE_DIGEST")
	if helperSocket == "" || helperChecksum == "" || reference == "" || digest == "" {
		if runtime.GOOS == "darwin" {
			t.Skip("NOT-RUN: attended Mac/Lima OCI service publication requires the owner-hardware helper and probe environment")
		}
		t.Fatal("Linux OCI service publication realtiming provisioning is incomplete")
	}
	var healthOK, echoOK, startupTimedOut, withdrawalObserved, republicationObserved bool
	var portCollisionAvoided, portlessOK, gracefulStopOK, restartIdentityOK bool
	defer func() {
		if evidenceDirectory := os.Getenv("WEFTY_REALTIME_EVIDENCE_DIR"); evidenceDirectory != "" {
			helperTunnelOK := healthOK && echoOK
			evidence := fmt.Sprintf("platform=%s/%s\nhealth=%t\necho=%t\nstartup_timeout=%t\nwithdrawal=%t\nrepublication=%t\nport_collision_avoided=%t\nportless_started=%t\nhelper_tunnel=%t\nterm_grace_stop=%t\nfresh_restart_authority=%t\n", runtime.GOOS, runtime.GOARCH, healthOK, echoOK, startupTimedOut, withdrawalObserved, republicationObserved, portCollisionAvoided, portlessOK, helperTunnelOK, gracefulStopOK, restartIdentityOK)
			if err := os.WriteFile(filepath.Join(evidenceDirectory, "oci-service-publication-"+runtime.GOOS+".txt"), []byte(evidence), 0o600); err != nil {
				t.Errorf("write OCI service publication evidence: %v", err)
			}
		}
	}()

	client := ocihelper.NewUnixClient(helperSocket, helperChecksum)
	client.HeartbeatInterval = time.Second
	barrier, err := ocihelper.NewBootBarrier(client, ocihelper.AcquireSessionRequest{
		NodeID: "service-publication-node", BootSessionID: "service-publication-boot",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	if err := barrier.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	adapter := ocirunner.NewAdapter(barrier)
	if err := adapter.Probe(ctx, "service-publication-node", "service-publication-boot", reference, digest, l1.DefaultLeaseDuration); err != nil {
		t.Fatal(err)
	}

	primary := startNativeOCIService(t, ctx, adapter, reference, digest, "primary", "", nil, true)
	defer primary.stop(t, adapter)
	healthOK = string(primary.request(t, http.MethodGet, "/healthz", nil)) == "healthy\n"
	if !healthOK {
		t.Fatal("health response did not traverse the Fabric and helper tunnel")
	}
	echo := []byte("echo-through-fabric-and-helper")
	echoOK = bytes.Equal(primary.request(t, http.MethodPost, "/cgi-bin/echo", echo), echo)
	if !echoOK {
		t.Fatal("echo response did not traverse the Fabric and helper tunnel")
	}

	sibling := startNativeOCIService(t, ctx, adapter, reference, digest, "sibling", "", nil, true)
	portCollisionAvoided = sibling.backendPort.Load() != primary.backendPort.Load()
	if !portCollisionAvoided {
		t.Fatalf("concurrent OCI services shared backend port %d", primary.backendPort.Load())
	}
	sibling.stop(t, adapter)

	primary.triggerPayloadListenerRestart(t)
	withdrawalObserved = primary.waitReachable(t, false, 5*time.Second)
	select {
	case outcome := <-primary.done:
		t.Fatalf("helper-tunnel withdrawal killed payload: (%#v, %v)", outcome.result, outcome.err)
	default:
	}
	republicationObserved = primary.waitReachable(t, true, 5*time.Second)

	timedOut := startNativeOCIService(t, ctx, adapter, reference, digest, "startup-timeout", "", []string{"/bin/sh", "-c", "sleep 600"}, false)
	outcome := waitServiceOutcome(t, timedOut.done)
	startupTimedOut = outcome.err != nil && outcome.result.SpawnError != nil && outcome.result.SpawnError.Code == contract.SpawnFailureStartupReadinessTimeout
	if !startupTimedOut {
		t.Fatalf("OCI startup timeout = (%#v, %v)", outcome.result, outcome.err)
	}
	timedOut.reap(t, adapter)

	portlessStarted := make(chan struct{}, 1)
	portlessEndpoint := false
	portless := nativeOCIServiceRequest(reference, digest, "portless", []string{
		"/bin/sh", "-c", `test "$WEFTY_SERVICE_DIR" = "/wefty/service"`,
	})
	portless.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error {
		portlessStarted <- struct{}{}
		return nil
	}
	portless.AttemptEndpointReady = func(string, workloadrunner.AttemptEndpoint) error {
		portlessEndpoint = true
		return nil
	}
	portlessResult, err := adapter.Run(ctx, portless, nil)
	if err != nil || portlessResult.Outcome.ExitCode == nil || *portlessResult.Outcome.ExitCode != 0 {
		t.Fatalf("portless OCI service = (%#v, %v)", portlessResult.Outcome, err)
	}
	select {
	case <-portlessStarted:
	default:
		t.Fatal("portless OCI service did not report authoritative Started")
	}
	portlessOK = !portlessEndpoint
	if !portlessOK {
		t.Fatal("portless OCI service published an attempt endpoint")
	}
	if receipt, err := adapter.ReapAndVerify(ctx, workloadrunner.ReapRequest{Authority: portless.Authority}); err != nil || !receipt.RuntimeQuiesced {
		t.Fatalf("portless OCI cleanup = (%+v, %v)", receipt, err)
	}

	gracefulStopOK = primary.stop(t, adapter)
	restarted := startNativeOCIService(t, ctx, adapter, reference, digest, "primary-restart", primary.requestAuthority.JobID, nil, true)
	primaryResources, primaryIdentityErr := ocihelper.DeterministicResourceIdentity(ocirunner.HelperAuthority(primary.requestAuthority))
	restartedResources, restartedIdentityErr := ocihelper.DeterministicResourceIdentity(ocirunner.HelperAuthority(restarted.requestAuthority))
	restartIdentityOK = primaryIdentityErr == nil && restartedIdentityErr == nil &&
		restarted.requestAuthority.JobID == primary.requestAuthority.JobID &&
		restarted.requestAuthority.AttemptID != primary.requestAuthority.AttemptID &&
		restarted.requestAuthority.FencingToken != primary.requestAuthority.FencingToken &&
		restartedResources.ContainerID != primaryResources.ContainerID &&
		restarted.backendPort.Load() != primary.backendPort.Load() && restarted.address != primary.address
	if !restartIdentityOK {
		t.Fatalf("fresh restart authority = %+v, prior %+v", restarted.requestAuthority, primary.requestAuthority)
	}
	restarted.stop(t, adapter)
}

func TestOCIServiceRestartStopStartThroughL1Agent(t *testing.T) {
	helperSocket := os.Getenv("WEFTY_OCI_HELPER_SOCKET")
	helperChecksum := os.Getenv("WEFTY_OCI_HELPER_CHECKSUM")
	reference := os.Getenv("WEFTY_OCI_PROBE_REFERENCE")
	digest := os.Getenv("WEFTY_OCI_PROBE_DIGEST")
	if helperSocket == "" || helperChecksum == "" || reference == "" || digest == "" {
		if runtime.GOOS == "darwin" {
			t.Skip("NOT-RUN: attended Mac/Lima OCI L1/agent restart transitions require the owner-hardware helper and probe environment")
		}
		t.Fatal("Linux OCI L1/agent service realtiming provisioning is incomplete")
	}
	var freshRestart, stopStart, saturation, retainedBinding bool
	var removalManifestComplete, removalPending, removalEveryAttempt bool
	var removalServiceDataVolume, removalServiceDataOwnerRecord bool
	var removalCompleted, removalPriorBootSweep bool
	defer func() {
		if evidenceDirectory := os.Getenv("WEFTY_REALTIME_EVIDENCE_DIR"); evidenceDirectory != "" {
			payload := fmt.Sprintf("fresh_restart=%t\nstop_start=%t\nslot_saturation=%t\nretained_binding_digest=%t\nremoval_manifest_complete=%t\nremoval_pending=%t\nremoval_every_attempt=%t\nremoval_service_data_volume=%t\nremoval_service_data_owner_record=%t\nremoval_completed=%t\nremoval_prior_boot_oci_sweep=%t\n", freshRestart, stopStart, saturation, retainedBinding, removalManifestComplete, removalPending, removalEveryAttempt, removalServiceDataVolume, removalServiceDataOwnerRecord, removalCompleted, removalPriorBootSweep)
			if err := os.WriteFile(filepath.Join(evidenceDirectory, "oci-service-l1-agent-linux.txt"), []byte(payload), 0o600); err != nil {
				t.Errorf("write OCI L1/agent evidence: %v", err)
			}
		}
	}()

	network := plain.NewNetwork()
	store, stopServer := startFailureServerWithPolicies(t, network, nil, map[string]l1.NodePolicy{
		"native-service-node": {Tags: []string{"native-service"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	})
	defer stopServer()
	serviceSpec := func(dispatchKey string) contract.JobSpec {
		return contract.JobSpec{
			SchemaVersion: contract.SchemaVersionV1, DispatchKey: dispatchKey, Kind: contract.JobKindOCI,
			Class: contract.JobClassService, Restart: contract.RestartAlways, RoutingTags: []string{"native-service"},
			RuntimeHandler: ocihelper.DefaultRuntimeHandler,
			Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
				Image: contract.OCIImageSpec{Reference: reference, Digest: &digest},
				Argv: []string{"/bin/sh", "-c", `
trap 'exit 0' TERM
test ! -e /rootfs-attempt-marker || exit 92
prior=0
if test -f "$WEFTY_SERVICE_DIR/attempt-count"; then prior="$(cat "$WEFTY_SERVICE_DIR/attempt-count")"; fi
printf 'service-data-prior=%s\n' "$prior"
printf '%s\n' "$((prior + 1))" >"$WEFTY_SERVICE_DIR/attempt-count"
touch /rootfs-attempt-marker
while :; do sleep 1; done
`},
			}},
		}
	}
	primary, _, err := store.CreateJob(t.Context(), serviceSpec("native-service-primary"))
	if err != nil {
		t.Fatal(err)
	}

	client := ocihelper.NewUnixClient(helperSocket, helperChecksum)
	client.HeartbeatInterval = time.Second
	barrier, err := ocihelper.NewBootBarrier(client, ocihelper.AcquireSessionRequest{NodeID: "native-service-node", BootSessionID: "native-service-boot"})
	if err != nil {
		t.Fatal(err)
	}
	adapter := ocirunner.NewAdapter(barrier)
	spoolDirectory := t.TempDir()
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "native-service-agent", Tags: []string{l1.DefaultAgentPrincipalTag}})
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "native-service-node", BootSessionID: "native-service-boot", Version: "realtiming",
		Capabilities: map[string]bool{"kind:process": true},
		CapabilityProbe: capabilityProbeFunc(func(ctx context.Context) (CapabilityProbeResult, error) {
			if err := adapter.Probe(ctx, "native-service-node", "native-service-boot", reference, digest, l1.DefaultLeaseDuration); err != nil {
				return CapabilityProbeResult{MissingCapabilities: []string{"kind:oci"}, ReasonCode: contract.CapabilityReasonProbeFailed}, err
			}
			return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true, "runtime_handler:" + ocihelper.DefaultRuntimeHandler: true}}, nil
		}),
		OCIBootBarrier: barrier, WorkloadRuntimes: map[string]WorkloadRuntime{contract.JobKindOCI: adapter},
		AttemptDeadman:       nativeAcceptanceDeadman{barrier: barrier, nodeID: "native-service-node", bootSessionID: "native-service-boot"},
		ManagedRootDirectory: managedRoot, LogSpoolDirectory: spoolDirectory, MaxServiceSlots: 1,
		HeartbeatInterval: 2 * time.Second, ClaimInterval: 20 * time.Millisecond, RenewalInterval: 200 * time.Millisecond,
		OperationTimeout: 5 * time.Second, FinalizationTimeout: 30 * time.Second, Logf: t.Logf,
	})
	if err != nil {
		_ = barrier.Close()
		t.Fatal(err)
	}
	defer func() { nodeAgent.Close() }()
	runContext, cancelRun := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() { runDone <- nodeAgent.Run(runContext) }()

	firstRunning := waitNativeServiceState(t, store, primary.JobID, contract.JobRunning, 45*time.Second)
	firstAttempt := firstRunning.CurrentAttemptID
	if firstAttempt == "" || firstRunning.BoundNodeID != "native-service-node" || firstRunning.Spec.Execution.OCI == nil || firstRunning.Spec.Execution.OCI.Image.Digest == nil {
		t.Fatalf("initial L1/agent OCI service = %+v", firstRunning)
	}
	sibling, _, err := store.CreateJob(t.Context(), serviceSpec("native-service-sibling"))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)
	siblingQueued, err := store.GetJob(t.Context(), sibling.JobID)
	if err != nil || siblingQueued.State != contract.JobQueued {
		t.Fatalf("saturated sibling = %+v err %v", siblingQueued, err)
	}
	saturation = true

	if _, _, err := store.RestartService(t.Context(), primary.JobID, l1.ServiceRestartRequest{IdempotencyKey: "native-fresh-restart"}); err != nil {
		t.Fatal(err)
	}
	restarted := waitNativeServiceAttempt(t, store, primary.JobID, firstAttempt, 45*time.Second)
	freshRestart = restarted.State == contract.JobRunning && restarted.CurrentAttemptID != firstAttempt
	retainedBinding = restarted.BoundNodeID == firstRunning.BoundNodeID && restarted.Spec.Execution.OCI != nil &&
		restarted.Spec.Execution.OCI.Image.Digest != nil && *restarted.Spec.Execution.OCI.Image.Digest == digest
	if !freshRestart || !retainedBinding {
		t.Fatalf("fresh L1-classified restart = first %+v restarted %+v", firstRunning, restarted)
	}

	if _, err := store.SetServiceDesiredState(t.Context(), primary.JobID, contract.ServiceDesiredStopped); err != nil {
		t.Fatal(err)
	}
	stopped := waitNativeServiceState(t, store, primary.JobID, contract.JobStopped, 45*time.Second)
	siblingRunning := waitNativeServiceState(t, store, sibling.JobID, contract.JobRunning, 45*time.Second)
	if _, err := store.SetServiceDesiredState(t.Context(), primary.JobID, contract.ServiceDesiredRunning); err == nil {
		t.Fatal("stopped service reacquired capacity while sibling held the sole slot")
	}
	if _, err := store.SetServiceDesiredState(t.Context(), sibling.JobID, contract.ServiceDesiredStopped); err != nil {
		t.Fatal(err)
	}
	_ = waitNativeServiceState(t, store, sibling.JobID, contract.JobStopped, 45*time.Second)
	queued, err := store.SetServiceDesiredState(t.Context(), primary.JobID, contract.ServiceDesiredRunning)
	if err != nil || queued.State != contract.JobQueued {
		t.Fatalf("start did not transition through queued = %+v err %v", queued, err)
	}
	startedAgain := waitNativeServiceAttempt(t, store, primary.JobID, stopped.CurrentAttemptID, 45*time.Second)
	stopStart = startedAgain.State == contract.JobRunning && startedAgain.CurrentAttemptID != stopped.CurrentAttemptID && siblingRunning.CurrentAttemptID != ""
	if !stopStart {
		t.Fatalf("stop/start did not reacquire a fresh attempt = stopped %+v started %+v", stopped, startedAgain)
	}
	logs, err := store.GetJobLogs(t.Context(), primary.JobID, "", l1.MaxLogPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	wantMarkers := map[string]bool{"service-data-prior=0\n": false, "service-data-prior=1\n": false, "service-data-prior=2\n": false}
	for _, event := range logs.Events {
		if _, ok := wantMarkers[string(event.Bytes)]; ok {
			wantMarkers[string(event.Bytes)] = true
		}
	}
	for marker, found := range wantMarkers {
		if !found {
			t.Fatalf("OCI service restart/stop-start logs omitted %q: %+v", marker, logs.Events)
		}
	}
	manifestCommitted := make(chan struct{}, 1)
	nodeAgent.logSpool.runtimeRemovalCheckpoint = func(checkpoint runtimeRemovalCheckpoint) error {
		if checkpoint != runtimeRemovalCheckpointAfterManifest {
			return nil
		}
		select {
		case manifestCommitted <- struct{}{}:
		default:
		}
		return errInjectedRuntimeRemovalCrash
	}
	pending, err := store.RemoveService(t.Context(), primary.JobID)
	if err != nil || pending.State != contract.JobRemovalPending {
		t.Fatalf("running OCI removal entry = %+v err=%v", pending, err)
	}
	removalPending = pending.State == contract.JobRemovalPending
	select {
	case <-manifestCommitted:
	case <-time.After(15 * time.Second):
		t.Fatal("removal did not commit its frozen manifest before reap")
	}
	record := waitRuntimeRemovalManifestPhase(t, spoolDirectory, primary.JobID, runtimeRemovalPrepared, 15*time.Second)
	wantRemovalAttempts := map[string]bool{startedAgain.CurrentAttemptID: false}
	for _, attempt := range record.manifest.Attempts {
		if _, expected := wantRemovalAttempts[attempt.AttemptID]; expected {
			wantRemovalAttempts[attempt.AttemptID] = true
		}
	}
	everyAttempt := len(record.manifest.Attempts) == len(wantRemovalAttempts)
	for _, found := range wantRemovalAttempts {
		everyAttempt = everyAttempt && found
	}
	serviceDataVolume := len(record.manifest.Attempts) > 0
	serviceDataOwnerRecord := len(record.manifest.Attempts) > 0
	for _, attempt := range record.manifest.Attempts {
		serviceDataVolume = serviceDataVolume && attempt.ServiceDataVolume != ""
		serviceDataOwnerRecord = serviceDataOwnerRecord && attempt.ServiceDataOwnerRecord != ""
	}
	if !removalPending {
		t.Fatal("OCI removal never exposed removal_pending")
	}
	if !everyAttempt {
		t.Fatalf("frozen OCI removal attempts = %+v, want current attempt %s", record.manifest.Attempts, startedAgain.CurrentAttemptID)
	}
	if !serviceDataVolume {
		t.Fatalf("frozen OCI removal omitted service-data volume: %+v", record.manifest.Attempts)
	}
	if !serviceDataOwnerRecord {
		t.Fatalf("frozen OCI removal omitted service-data owner record: %+v", record.manifest.Attempts)
	}
	removalEveryAttempt = everyAttempt
	removalServiceDataVolume = serviceDataVolume
	removalServiceDataOwnerRecord = serviceDataOwnerRecord
	cancelRun()
	if err := <-runDone; err != nil {
		t.Fatalf("L1/agent realtiming shutdown: %v", err)
	}
	nodeAgent.Close()

	restartBootID := "native-service-boot-removal-restart"
	restartClient := ocihelper.NewUnixClient(helperSocket, helperChecksum)
	restartClient.HeartbeatInterval = time.Second
	restartBarrier, err := ocihelper.NewBootBarrier(restartClient, ocihelper.AcquireSessionRequest{NodeID: "native-service-node", BootSessionID: restartBootID})
	if err != nil {
		t.Fatal(err)
	}
	restartAdapter := ocirunner.NewAdapter(restartBarrier)
	nodeAgent, err = New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "native-service-node", BootSessionID: restartBootID, Version: "realtiming",
		Capabilities: map[string]bool{"kind:process": true},
		CapabilityProbe: capabilityProbeFunc(func(ctx context.Context) (CapabilityProbeResult, error) {
			if err := restartAdapter.Probe(ctx, "native-service-node", restartBootID, reference, digest, l1.DefaultLeaseDuration); err != nil {
				return CapabilityProbeResult{MissingCapabilities: []string{"kind:oci"}, ReasonCode: contract.CapabilityReasonProbeFailed}, err
			}
			return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true, "runtime_handler:" + ocihelper.DefaultRuntimeHandler: true}}, nil
		}),
		OCIBootBarrier: restartBarrier, WorkloadRuntimes: map[string]WorkloadRuntime{contract.JobKindOCI: restartAdapter},
		AttemptDeadman:       nativeAcceptanceDeadman{barrier: restartBarrier, nodeID: "native-service-node", bootSessionID: restartBootID},
		ManagedRootDirectory: managedRoot, LogSpoolDirectory: spoolDirectory, MaxServiceSlots: 1,
		HeartbeatInterval: 2 * time.Second, ClaimInterval: 20 * time.Millisecond, RenewalInterval: 200 * time.Millisecond,
		OperationTimeout: 5 * time.Second, FinalizationTimeout: 30 * time.Second, Logf: t.Logf,
	})
	if err != nil {
		_ = restartBarrier.Close()
		t.Fatal(err)
	}
	type completionEvidence struct {
		record  runtimeRemovalRecord
		receipt workloadrunner.ReapReceipt
	}
	completedEvidence := make(chan completionEvidence, 1)
	recordQuiesced := nodeAgent.session.removals.recordRuntimeQuiesced
	nodeAgent.session.removals.recordRuntimeQuiesced = func(ctx context.Context, removal localRemoval, receipt workloadrunner.ReapReceipt) error {
		if err := recordQuiesced(ctx, removal, receipt); err != nil {
			return err
		}
		stored, found, err := nodeAgent.logSpool.runtimeRemoval(ctx, removal.jobID)
		if err != nil || !found {
			return errors.Join(err, errors.New("completed runtime removal record disappeared before local cleanup"))
		}
		select {
		case completedEvidence <- completionEvidence{record: stored, receipt: receipt}:
		default:
		}
		return nil
	}
	restartContext, cancelRestart := context.WithCancel(t.Context())
	defer cancelRestart()
	restartDone := make(chan error, 1)
	go func() { restartDone <- nodeAgent.Run(restartContext) }()
	removed := waitNativeServiceState(t, store, primary.JobID, contract.JobRemovedVerified, 45*time.Second)
	removalCompleted = removed.State == contract.JobRemovedVerified
	if !removalCompleted {
		t.Fatalf("OCI removal did not complete after restart: %+v", removed)
	}
	select {
	case evidence := <-completedEvidence:
		removalManifestComplete = evidence.record.phase == runtimeRemovalComplete && evidence.record.receipt.RuntimeQuiesced
		if !removalManifestComplete {
			t.Fatalf("runtime removal did not reach complete before legacy cleanup: %+v", evidence.record)
		}
		removalPriorBootSweep = evidence.receipt.Evidence == workloadrunner.ReapEvidencePriorBootOCISweep && evidence.receipt.BootSessionID != "" && evidence.receipt.SweepEpoch != "" && evidence.receipt.HelperGeneration != 0
		if !removalPriorBootSweep {
			t.Fatalf("restart did not use closed prior-boot OCI sweep evidence: %+v", evidence.receipt)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("removal completion omitted captured quiescence evidence")
	}
	cancelRestart()
	if err := <-restartDone; err != nil {
		t.Fatalf("restarted L1/agent realtiming shutdown: %v", err)
	}
}

func waitRuntimeRemovalManifestPhase(t *testing.T, directory, jobID string, want runtimeRemovalPhase, timeout time.Duration) runtimeRemovalRecord {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		entries, err := os.ReadDir(directory)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if filepath.Ext(entry.Name()) != ".sqlite" {
				continue
			}
			database, err := sql.Open("sqlite", filepath.Join(directory, entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			var manifestJSON, receiptJSON []byte
			var phase runtimeRemovalPhase
			err = database.QueryRow(`SELECT manifest_json, runtime_quiescence_json, phase
FROM runtime_removal_manifests WHERE job_id=?`, jobID).Scan(&manifestJSON, &receiptJSON, &phase)
			_ = database.Close()
			if err == nil {
				var record runtimeRemovalRecord
				record.phase = phase
				if err := json.Unmarshal(manifestJSON, &record.manifest); err != nil {
					t.Fatal(err)
				}
				if len(receiptJSON) != 0 {
					if err := json.Unmarshal(receiptJSON, &record.receipt); err != nil {
						t.Fatal(err)
					}
				}
				if record.phase == want {
					return record
				}
			} else if err != sql.ErrNoRows {
				t.Fatal(err)
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("runtime removal manifest for %s did not reach %s", jobID, want)
	return runtimeRemovalRecord{}
}

type nativeAcceptanceDeadman struct {
	barrier               *ocihelper.BootBarrier
	nodeID, bootSessionID string
}

func (renewer nativeAcceptanceDeadman) QueueSuccessfulRenewal(claim l1.Claim, ttl time.Duration) error {
	session, err := renewer.barrier.Session()
	if err != nil {
		return err
	}
	removalGeneration := "attempt"
	if claim.Job.Spec.Class == contract.JobClassService {
		removalGeneration = fmt.Sprint(l1.InitialServiceRemovalGeneration)
	}
	return session.QueueAttemptRenewal(ocihelper.AttemptAuthority{
		NodeID: renewer.nodeID, BootSessionID: renewer.bootSessionID, JobID: claim.Job.JobID,
		AttemptID: claim.Lease.AttemptID, FencingToken: claim.Lease.FencingToken,
		Class: claim.Job.Spec.Class, RemovalGeneration: removalGeneration,
	}, ttl)
}

func waitNativeServiceState(t *testing.T, store *l1.Store, jobID string, state contract.JobState, timeout time.Duration) l1.Job {
	t.Helper()
	job, err := waitForFailureJobState(store, jobID, state, timeout)
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func waitNativeServiceAttempt(t *testing.T, store *l1.Store, jobID, priorAttemptID string, timeout time.Duration) l1.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job, err := store.GetJob(t.Context(), jobID)
		if err != nil {
			t.Fatal(err)
		}
		if job.State == contract.JobRunning && job.CurrentAttemptID != "" && job.CurrentAttemptID != priorAttemptID {
			return job
		}
		time.Sleep(20 * time.Millisecond)
	}
	job, err := store.GetJob(t.Context(), jobID)
	if err != nil {
		t.Fatal(err)
	}
	t.Fatalf("service %s did not reach a fresh running attempt after %s: %+v", jobID, priorAttemptID, job)
	return l1.Job{}
}

type nativeOCIService struct {
	requestAuthority workloadrunner.AttemptAuthority
	cancel           context.CancelFunc
	done             chan serviceRunOutcome
	client           *http.Client
	address          string
	backendPort      atomic.Uint32
	reaped           atomic.Bool
}

func startNativeOCIService(
	t *testing.T,
	parent context.Context,
	adapter *ocirunner.Adapter,
	reference, digest, suffix, stableJobID string,
	argv []string,
	waitForReadiness bool,
) *nativeOCIService {
	t.Helper()
	network := plain.NewNetwork()
	frontDoorFabric := network.NewFabric(fabric.Identity{NodeID: "service-node-" + suffix})
	callerFabric := network.NewFabric(fabric.Identity{NodeID: "service-caller-" + suffix})
	address := "wefty://node/oci-service-" + suffix
	listener, err := frontDoorFabric.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	if argv == nil {
		argv = []string{"/bin/sh", "-c", nativeOCIHTTPServiceScript}
	}
	request := nativeOCIServiceRequest(reference, digest, suffix, argv)
	if stableJobID != "" {
		request.Authority.JobID = stableJobID
	}
	latch := newRuntimeEndpointLatch()
	request.AttemptEndpoints = []string{workloadrunner.AttemptEndpointService}
	endpoint := latch.endpoint(workloadrunner.AttemptEndpointService)
	service := &nativeOCIService{
		requestAuthority: request.Authority, done: make(chan serviceRunOutcome, 1), address: address,
		client: &http.Client{
			Timeout: time.Second,
			Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return callerFabric.Dial(ctx, "tcp", address)
			}},
		},
	}
	request.AttemptEndpointReady = func(name string, value workloadrunner.AttemptEndpoint) error {
		service.backendPort.Store(uint32(value.Port))
		return latch.publish(name, value)
	}
	runContext, cancel := context.WithCancel(parent)
	service.cancel = cancel
	go func() {
		result, runErr := runPortfulService(
			runContext, adapter, request, nil, listener, endpoint,
			serviceSupervisorConfig{
				startupReadinessDeadline:  750 * time.Millisecond,
				readinessProbeInterval:    50 * time.Millisecond,
				readinessConnectTimeout:   200 * time.Millisecond,
				publicationRecoveryWindow: 250 * time.Millisecond,
			},
		)
		service.done <- serviceRunOutcome{result: result, err: runErr}
	}()
	if waitForReadiness {
		service.waitReachable(t, true, 15*time.Second)
	}
	return service
}

func nativeOCIServiceRequest(reference, digest, suffix string, argv []string) workloadrunner.Request {
	return workloadrunner.Request{
		Authority: workloadrunner.AttemptAuthority{
			NodeID: "service-publication-node", BootSessionID: "service-publication-boot",
			JobID: "service-job-" + suffix, AttemptID: "service-attempt-" + suffix,
			FencingToken: "service-fence-" + suffix, WorkloadClass: contract.JobClassService,
			RemovalGeneration: fmt.Sprint(l1.InitialServiceRemovalGeneration),
		},
		RuntimeHandler: ocihelper.DefaultRuntimeHandler,
		ManagedVolumes: []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeServiceData}},
		Execution: contract.ExecutionSpec{
			Env: map[string]string{contract.EnvServiceDir: contract.OCIContainerServiceDirectory},
			OCI: &contract.OCIExecutionSpec{
				Image: contract.OCIImageSpec{Reference: reference, Digest: &digest}, Argv: argv,
			},
		},
		InitialDeadman:   2 * time.Minute,
		LifetimeBoundary: workloadrunner.AgentBootLifetime,
		TerminationGrace: processrunner.DefaultTerminationGraceTime,
		OCIImageResolved: func(context.Context, workloadrunner.OCIImageObservation) error { return nil },
		OCIStarted:       func(context.Context, workloadrunner.OCIImageObservation) error { return nil },
	}
}

const nativeOCIHTTPServiceScript = `
test "$WEFTY_SERVICE_DIR" = "/wefty/service" || exit 91
mkdir -p /tmp/wefty-www/cgi-bin
printf 'healthy\n' >/tmp/wefty-www/healthz
printf '#!/bin/sh\nprintf "Content-Type: application/octet-stream\\r\\n\\r\\n"\ndd bs=1 count="${CONTENT_LENGTH:-0}" 2>/dev/null\n' >/tmp/wefty-www/cgi-bin/echo
printf '#!/bin/sh\ntouch /tmp/wefty-listener-restart\nprintf "Status: 204 No Content\\r\\n\\r\\n"\n' >/tmp/wefty-www/cgi-bin/restart-listener
chmod 0755 /tmp/wefty-www/cgi-bin/echo
chmod 0755 /tmp/wefty-www/cgi-bin/restart-listener
while :; do
  /bin/httpd -f -p "127.0.0.1:$WEFTY_SERVICE_PORT" -h /tmp/wefty-www &
  server=$!
  while kill -0 "$server" 2>/dev/null && test ! -f /tmp/wefty-listener-restart; do sleep 0.05; done
  if test -f /tmp/wefty-listener-restart; then
    kill "$server" 2>/dev/null || true
    wait "$server" 2>/dev/null || true
    sleep 1
    rm -f /tmp/wefty-listener-restart
  else
    wait "$server"
    exit $?
  fi
done
`

func (service *nativeOCIService) request(t *testing.T, method, path string, body []byte) []byte {
	t.Helper()
	request, err := http.NewRequest(method, "http://service.invalid"+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		t.Fatalf("%s %s returned %d: %s", method, path, response.StatusCode, strings.TrimSpace(string(payload)))
	}
	return payload
}

func (service *nativeOCIService) triggerPayloadListenerRestart(t *testing.T) {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, "http://service.invalid/cgi-bin/restart-listener", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, _ := service.client.Do(request)
	if response != nil {
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}
}

func (service *nativeOCIService) waitReachable(t *testing.T, want bool, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		request, err := http.NewRequest(http.MethodGet, "http://service.invalid/healthz", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := service.client.Do(request)
		reachable := err == nil && response.StatusCode == http.StatusOK
		if response != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
			_ = response.Body.Close()
		}
		if reachable == want {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("OCI service reachability did not become %v", want)
	return false
}

func (service *nativeOCIService) stop(t *testing.T, adapter *ocirunner.Adapter) bool {
	t.Helper()
	if service.reaped.Load() {
		return true
	}
	service.cancel()
	select {
	case outcome := <-service.done:
		if outcome.err != nil || outcome.result.Signal != "terminated" || outcome.result.TerminationCause != contract.TerminationCauseAgent {
			t.Fatalf("OCI service graceful stop = (%+v, %v)", outcome.result, outcome.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out stopping OCI service publication acceptance payload")
	}
	service.reap(t, adapter)
	return true
}

func (service *nativeOCIService) reap(t *testing.T, adapter *ocirunner.Adapter) {
	t.Helper()
	if !service.reaped.CompareAndSwap(false, true) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	receipt, err := adapter.ReapAndVerify(ctx, workloadrunner.ReapRequest{Authority: service.requestAuthority})
	if err != nil || !receipt.RuntimeQuiesced {
		t.Fatalf("OCI service cleanup = (%+v, %v)", receipt, err)
	}
}
