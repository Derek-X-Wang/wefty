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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/lima"
	ocirunner "github.com/Derek-X-Wang/wefty/runner/oci"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

func TestOCIServicePublicationThroughHelperTunnel(t *testing.T) {
	helperSocket := os.Getenv("WEFTY_OCI_HELPER_SOCKET")
	helperChecksum := os.Getenv("WEFTY_OCI_HELPER_CHECKSUM")
	reference := os.Getenv("WEFTY_OCI_PROBE_REFERENCE")
	digest := os.Getenv("WEFTY_OCI_PROBE_DIGEST")
	archivePath := os.Getenv("WEFTY_OCI_PROBE_ARCHIVE")
	if helperSocket == "" || helperChecksum == "" || reference == "" || digest == "" || archivePath == "" {
		if runtime.GOOS == "darwin" {
			t.Skip("NOT-RUN: attended Mac/Lima OCI service publication requires the owner-hardware helper and probe environment")
		}
		t.Fatal("Linux OCI service publication realtiming provisioning is incomplete")
	}
	var healthOK, echoOK, startupTimedOut, withdrawalObserved, republicationObserved bool
	var portCollisionAvoided, portlessOK, gracefulStopOK, restartIdentityOK bool
	var killEscalation nativeOCIServiceKILLEscalationEvidence
	defer func() {
		if evidenceDirectory := os.Getenv("WEFTY_REALTIME_EVIDENCE_DIR"); evidenceDirectory != "" {
			helperTunnelOK := healthOK && echoOK
			evidence := fmt.Sprintf("platform=%s/%s\nhealth=%t\necho=%t\nstartup_timeout=%t\nwithdrawal=%t\nrepublication=%t\nport_collision_avoided=%t\nportless_started=%t\nhelper_tunnel=%t\nterm_cooperative_stop=%t\nterm_kill_escalation=%t\nterm_kill_log_evidence_incomplete=%t\nterm_kill_stdout_log=%t\nterm_kill_stderr_log=%t\nterm_grace_stop=%t\nfresh_restart_authority=%t\n", runtime.GOOS, runtime.GOARCH, healthOK, echoOK, startupTimedOut, withdrawalObserved, republicationObserved, portCollisionAvoided, portlessOK, helperTunnelOK, gracefulStopOK, killEscalation.Escalated, killEscalation.LogEvidenceIncomplete, killEscalation.StdoutLog, killEscalation.StderrLog, gracefulStopOK && killEscalation.Escalated, restartIdentityOK)
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
	importRealtimeProbeImage(t, ctx, barrier, reference, digest)
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

	timedOut := startNativeOCIService(t, ctx, adapter, reference, digest, "startup-timeout", "", []string{
		"/bin/sh", "-c", `trap 'exit 143' TERM; while :; do sleep 0.1; done`,
	}, false)
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
	killEscalation = verifyNativeOCIServiceKILLEscalation(t, ctx, adapter, reference, digest)
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
	archivePath := os.Getenv("WEFTY_OCI_PROBE_ARCHIVE")
	if helperSocket == "" || helperChecksum == "" || reference == "" || digest == "" || archivePath == "" {
		if runtime.GOOS == "darwin" {
			t.Skip("NOT-RUN: attended Mac/Lima OCI L1/agent restart transitions require the owner-hardware helper and probe environment")
		}
		t.Fatal("Linux OCI L1/agent service realtiming provisioning is incomplete")
	}
	var freshRestart, stopStart, saturation, retainedBinding bool
	var removalManifestComplete, removalPending, removalEveryAttempt bool
	var removalServiceDataVolume, removalServiceDataOwnerRecord bool
	var removalCompleted, removalPriorBootSweep, removalPostDeleteAttestation, removalDeleteAttestInjection bool
	defer func() {
		if evidenceDirectory := os.Getenv("WEFTY_REALTIME_EVIDENCE_DIR"); evidenceDirectory != "" {
			payload := fmt.Sprintf("fresh_restart=%t\nstop_start=%t\nslot_saturation=%t\nretained_binding_digest=%t\nremoval_manifest_complete=%t\nremoval_pending=%t\nremoval_every_attempt=%t\nremoval_service_data_volume=%t\nremoval_service_data_owner_record=%t\nremoval_post_delete_attestation=%t\nremoval_delete_attest_crash_injected=%t\nremoval_delete_attest_restart=NOT-RUN_hosted_lane\nremoval_completed=%t\nremoval_prior_boot_oci_sweep=%t\n", freshRestart, stopStart, saturation, retainedBinding, removalManifestComplete, removalPending, removalEveryAttempt, removalServiceDataVolume, removalServiceDataOwnerRecord, removalPostDeleteAttestation, removalDeleteAttestInjection, removalCompleted, removalPriorBootSweep)
			if err := os.WriteFile(filepath.Join(evidenceDirectory, "oci-service-l1-agent-linux.txt"), []byte(payload), 0o600); err != nil {
				t.Errorf("write OCI L1/agent evidence: %v", err)
			}
		}
	}()

	network := plain.NewNetwork()
	store, stopServer := startFailureServerWithPoliciesAndLease(t, network, nil, map[string]l1.NodePolicy{
		"native-service-node": {Tags: []string{"native-service"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	}, 2*time.Second)
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
	importRealtimeProbeImage(t, t.Context(), barrier, reference, digest)
	adapter := ocirunner.NewAdapter(barrier)
	spoolDirectory := t.TempDir()
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	intentPath := filepath.Join(t.TempDir(), "oci-intent.json")
	if _, err := lima.InitializeOCIIntent(intentPath, time.Now()); err != nil {
		t.Fatal(err)
	}
	intentSource := lima.FileIntentSource{Path: intentPath}
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "native-service-agent", Tags: []string{l1.DefaultAgentPrincipalTag}})
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "native-service-node", BootSessionID: "native-service-boot", Version: "realtiming",
		Capabilities: map[string]bool{"kind:process": true},
		CapabilityProbe: capabilityProbeFunc(func(ctx context.Context) (CapabilityProbeResult, error) {
			intent, err := intentSource.ReadIntent(ctx)
			if err != nil || !intent.Enabled {
				if err == nil {
					err = errors.New("OCI intent is disabled")
				}
				return CapabilityProbeResult{MissingCapabilities: []string{"kind:oci"}, ReasonCode: contract.CapabilityReasonOCIIntentDisabled}, err
			}
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
	pinsBeforeStop, err := nodeAgent.logSpool.ListOCIImageBindingPins(t.Context())
	if err != nil || !containsBindingPin(pinsBeforeStop, primary.JobID, digest) {
		t.Fatalf("initial OCI binding pin=%+v err=%v", pinsBeforeStop, err)
	}
	stopContext, cancelStop := context.WithTimeout(t.Context(), 15*time.Second)
	if _, err := lima.SetOCIIntent(stopContext, intentPath, 1, false, time.Now()); err != nil {
		cancelStop()
		t.Fatal(err)
	}
	if err := nodeAgent.StopOCIRuntime(stopContext); err != nil {
		cancelStop()
		t.Fatal(err)
	}
	cancelStop()
	// The real two-second lease proves that local OCI intent stop is neither an
	// execution failure nor a hidden service restart. L1's one-second
	// background reconciler must record the attempt as lost before this
	// second, idempotent pass reports no duplicate transition.
	intentStopped := waitNativeServiceState(t, store, primary.JobID, contract.JobQueued, 5*time.Second)
	if result, err := store.Reconcile(t.Context()); err != nil || result.ExpiredAttempts != 0 {
		t.Fatalf("post-intent-stop idempotent reconciliation=%+v err=%v", result, err)
	}
	attempts, err := store.ListJobAttempts(t.Context(), primary.JobID)
	if err != nil || len(attempts) != 1 || attempts[0].AttemptID != firstAttempt || attempts[0].State != contract.AttemptLost || attempts[0].Result != nil {
		t.Fatalf("intent-stop expiry evidence=%+v err=%v", attempts, err)
	}
	if intentStopped.State != contract.JobQueued || intentStopped.BoundNodeID != firstRunning.BoundNodeID ||
		intentStopped.Spec.Execution.OCI == nil || intentStopped.Spec.Execution.OCI.Image.Digest == nil ||
		*intentStopped.Spec.Execution.OCI.Image.Digest != digest ||
		intentStopped.RestartStreak != firstRunning.RestartStreak ||
		intentStopped.LifetimeRestartCount != firstRunning.LifetimeRestartCount ||
		!bytes.Equal(intentStopped.LastFailure, firstRunning.LastFailure) ||
		intentStopped.LeaseLossCount != firstRunning.LeaseLossCount+1 || intentStopped.NextRestartAt == nil ||
		!intentStopped.NextRestartAt.After(intentStopped.UpdatedAt) {
		t.Fatalf("intent stop changed service binding/failure budget: before=%+v after=%+v", firstRunning, intentStopped)
	}
	pinsAfterStop, err := nodeAgent.logSpool.ListOCIImageBindingPins(t.Context())
	if err != nil || !containsBindingPin(pinsAfterStop, primary.JobID, digest) {
		t.Fatalf("intent stop lost OCI binding pin=%+v err=%v", pinsAfterStop, err)
	}
	if _, err := lima.SetOCIIntent(t.Context(), intentPath, 2, true, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := nodeAgent.RecoverOCIRuntimeCapabilities(t.Context()); err != nil {
		t.Fatal(err)
	}
	firstRunning = waitNativeServiceAttempt(t, store, primary.JobID, firstAttempt, 45*time.Second)
	firstAttempt = firstRunning.CurrentAttemptID
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
	quiescenceEvidence := make(chan workloadrunner.ReapReceipt, 1)
	completedEvidence := make(chan runtimeRemovalRecord, 1)
	var crashedBeforeAttestation atomic.Bool
	attestRuntimeRemoval := nodeAgent.session.removals.attestRuntimeRemoval
	nodeAgent.session.removals.attestRuntimeRemoval = func(ctx context.Context, request workloadrunner.RuntimeRemovalProofRequest) (workloadrunner.RuntimeRemovalAttestation, error) {
		if crashedBeforeAttestation.CompareAndSwap(false, true) {
			return workloadrunner.RuntimeRemovalAttestation{}, errInjectedRuntimeRemovalCrash
		}
		return attestRuntimeRemoval(ctx, request)
	}
	recordQuiesced := nodeAgent.session.removals.recordRuntimeQuiesced
	nodeAgent.session.removals.recordRuntimeQuiesced = func(ctx context.Context, removal localRemoval, receipt workloadrunner.ReapReceipt) error {
		if err := recordQuiesced(ctx, removal, receipt); err != nil {
			return err
		}
		select {
		case quiescenceEvidence <- receipt:
		default:
		}
		return nil
	}
	var crashedAfterAttestation atomic.Bool
	nodeAgent.logSpool.runtimeRemovalCheckpoint = func(checkpoint runtimeRemovalCheckpoint) error {
		if checkpoint != runtimeRemovalCheckpointAfterComplete {
			return nil
		}
		stored, found, err := nodeAgent.logSpool.runtimeRemoval(t.Context(), primary.JobID)
		if err != nil || !found {
			return errors.Join(err, errors.New("completed runtime removal record disappeared before acknowledgement"))
		}
		select {
		case completedEvidence <- stored:
		default:
		}
		if crashedAfterAttestation.CompareAndSwap(false, true) {
			return errInjectedRuntimeRemovalCrash
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
		removalManifestComplete = evidence.phase == runtimeRemovalComplete && evidence.receipt.RuntimeQuiesced && evidence.attestation.Version == 1
		if !removalManifestComplete {
			t.Fatalf("runtime removal did not persist post-delete attestation before acknowledgement: %+v", evidence)
		}
		if err := validateRuntimeRemovalAttestation(evidence.manifest, evidence.attestation); err != nil {
			t.Fatalf("persisted removal attestation: %v", err)
		}
		removalPostDeleteAttestation = true
		if !crashedBeforeAttestation.Load() || !crashedAfterAttestation.Load() {
			t.Fatal("production-timing removal did not exercise both delete/attest crash boundaries")
		}
		removalDeleteAttestInjection = true
	case <-time.After(15 * time.Second):
		t.Fatal("removal completion omitted captured absence attestation")
	}
	select {
	case receipt := <-quiescenceEvidence:
		removalPriorBootSweep = receipt.Evidence == workloadrunner.ReapEvidencePriorBootOCISweep && receipt.BootSessionID != "" && receipt.SweepEpoch != "" && receipt.HelperGeneration != 0
		if !removalPriorBootSweep {
			t.Fatalf("restart did not use closed prior-boot OCI sweep evidence: %+v", receipt)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("removal completion omitted captured prior-boot quiescence evidence")
	}
	cancelRestart()
	if err := <-restartDone; err != nil {
		t.Fatalf("restarted L1/agent realtiming shutdown: %v", err)
	}
}

func containsBindingPin(pins []workloadrunner.OCIImageBindingPin, jobID, digest string) bool {
	for _, pin := range pins {
		if pin.JobID == jobID && pin.Digest == digest {
			return true
		}
	}
	return false
}

func importRealtimeProbeImage(t *testing.T, ctx context.Context, barrier *ocihelper.BootBarrier, reference, digest string) {
	t.Helper()
	archivePath := os.Getenv("WEFTY_OCI_PROBE_ARCHIVE")
	if archivePath == "" {
		t.Fatal("OCI service realtiming provisioning requires WEFTY_OCI_PROBE_ARCHIVE")
	}
	if err := barrier.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	archive, err := os.Open(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	var imported ocihelper.EnsureImageResponse
	importErr := session.ImportImage(ctx, ocihelper.EnsureImageRequest{
		Reference: reference, Digest: digest,
		Platform:         ocihelper.OCIPlatform{OS: "linux", Architecture: runtime.GOARCH},
		OperationTimeout: 2 * time.Minute,
	}, archive, func(event ocihelper.EnsureImageEvent) error {
		if event.Result != nil {
			imported = *event.Result
		}
		return nil
	})
	closeErr := archive.Close()
	if importErr != nil || closeErr != nil {
		t.Fatal(errors.Join(importErr, closeErr))
	}
	if imported.TopLevelDigest != digest || imported.PlatformDigest == "" {
		t.Fatalf("realtiming probe import = %+v, want top-level digest %s and a platform digest", imported, digest)
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
server=
terminate() {
  trap - TERM
  if test -n "$server"; then
    kill "$server" 2>/dev/null || true
    wait "$server" 2>/dev/null || true
  fi
  exit 143
}
trap terminate TERM
while :; do
  /bin/httpd -f -p "127.0.0.1:$WEFTY_SERVICE_PORT" -h /tmp/wefty-www &
  server=$!
  # BusyBox ash runs TERM traps between commands. Polling at 100 ms keeps both
  # listener withdrawal and TERM handling inside the measured outcome margin.
  restart_requested=false
  while kill -0 "$server" 2>/dev/null; do
    if test -f /tmp/wefty-listener-restart; then
      restart_requested=true
      kill "$server" 2>/dev/null || true
      break
    fi
    sleep 0.1
  done
  wait "$server"
  status=$?
  if test "$restart_requested" = true; then
    sleep 1
    rm -f /tmp/wefty-listener-restart
  else
    exit "$status"
  fi
done
`

type nativeOCIServiceKILLEscalationEvidence struct {
	Escalated             bool
	LogEvidenceIncomplete bool
	StdoutLog             bool
	StderrLog             bool
}

func verifyNativeOCIServiceKILLEscalation(t *testing.T, parent context.Context, adapter *ocirunner.Adapter, reference, digest string) nativeOCIServiceKILLEscalationEvidence {
	t.Helper()
	request := nativeOCIServiceRequest(reference, digest, "term-ignoring", []string{
		"/bin/sh", "-c", `printf 'kill-escalation-stdout\n'; printf 'kill-escalation-stderr\n' >&2; trap '' TERM; while :; do sleep 60; done`,
	})
	started := make(chan struct{})
	request.Started = func() { close(started) }
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct {
		result workloadrunner.Result
		err    error
	}, 1)
	var logMu sync.Mutex
	var logs []contract.LogEvent
	go func() {
		result, err := adapter.Run(ctx, request, workloadrunner.OutputSinkFunc(func(_ context.Context, event contract.LogEvent) error {
			logMu.Lock()
			defer logMu.Unlock()
			logs = append(logs, event)
			return nil
		}))
		done <- struct {
			result workloadrunner.Result
			err    error
		}{result: result, err: err}
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("TERM-ignoring OCI service did not start")
	}
	stopStarted := time.Now()
	cancel()
	var outcome struct {
		result workloadrunner.Result
		err    error
	}
	select {
	case outcome = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("TERM-ignoring OCI service did not reach bounded KILL escalation")
	}
	elapsed := time.Since(stopStarted)
	// The logger can observe stream EOF before helper cleanup terminates it, or
	// publish an incomplete seal first. Completeness is an additive observation;
	// the terminal authority, bounded escalation, and retained marker bytes are
	// the required stop proof for either valid ordering.
	if outcome.err != nil || outcome.result.Outcome.Signal != "killed" || outcome.result.Outcome.TerminationCause != contract.TerminationCauseAgent {
		t.Fatalf("TERM-ignoring OCI service escalation = (%+v, %v)", outcome.result.Outcome, outcome.err)
	}
	logMu.Lock()
	stdoutLog := acceptanceLogContains(logs, contract.LogStdout, "kill-escalation-stdout")
	stderrLog := acceptanceLogContains(logs, contract.LogStderr, "kill-escalation-stderr")
	logMu.Unlock()
	if !stdoutLog || !stderrLog {
		t.Fatalf("TERM-ignoring OCI service receipt completeness = log_evidence_incomplete:%t stdout_log:%t stderr_log:%t", outcome.result.Outcome.LogEvidenceIncomplete, stdoutLog, stderrLog)
	}
	if elapsed < processrunner.DefaultTerminationGraceTime || elapsed >= 10*time.Second {
		t.Fatalf("TERM-ignoring OCI service escalation elapsed %s, want grace honoured and completion inside stop budget", elapsed)
	}
	ctxReap, cancelReap := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelReap()
	receipt, err := adapter.ReapAndVerify(ctxReap, workloadrunner.ReapRequest{Authority: request.Authority})
	if err != nil || !receipt.RuntimeQuiesced {
		t.Fatalf("TERM-ignoring OCI service cleanup = (%+v, %v)", receipt, err)
	}
	return nativeOCIServiceKILLEscalationEvidence{
		Escalated:             true,
		LogEvidenceIncomplete: outcome.result.Outcome.LogEvidenceIncomplete,
		StdoutLog:             stdoutLog,
		StderrLog:             stderrLog,
	}
}

func acceptanceLogContains(events []contract.LogEvent, stream contract.LogStream, value string) bool {
	for _, event := range events {
		if event.Stream == stream && bytes.Contains(event.Bytes, []byte(value)) {
			return true
		}
	}
	return false
}

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
