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
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/lima"
	ocirunner "github.com/Derek-X-Wang/wefty/runner/oci"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

const (
	nativeOCIReadinessProbeInterval    = 50 * time.Millisecond
	nativeOCIReadinessConnectTimeout   = 200 * time.Millisecond
	nativeOCIPayloadListenerRestartGap = time.Second
	// Round-4 #304 measured cleanup below 600 ms. A one-second cleanup ceiling
	// plus a one-second preface/admission ceiling reserves two seconds of the
	// production lease instead of accepting admission at its expiry instant.
	nativeOCIFreshAttemptLeaseMargin = 2 * time.Second
	// Observation begins after withdrawal. The replacement listener can still
	// owe its scripted restart gap, one in-flight connect timeout, and four probe
	// ticks before the production recovery window starts.
	nativeOCIRepublicationDeadline = DefaultPublicationRecoveryWindow + nativeOCIPayloadListenerRestartGap +
		nativeOCIReadinessConnectTimeout + 4*nativeOCIReadinessProbeInterval
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
	var withdrawalElapsed, republicationElapsed time.Duration
	var portCollisionAvoided, portlessOK, gracefulStopOK, restartIdentityOK bool
	var killEscalation nativeOCIServiceKILLEscalationEvidence
	defer func() {
		if evidenceDirectory := os.Getenv("WEFTY_REALTIME_EVIDENCE_DIR"); evidenceDirectory != "" {
			helperTunnelOK := healthOK && echoOK
			evidence := fmt.Sprintf("platform=%s/%s\nhealth=%t\necho=%t\nstartup_timeout=%t\nwithdrawal=%t\nwithdrawal_elapsed=%s\nrepublication=%t\nrepublication_elapsed=%s\npublication_recovery_bound=%s\nrepublication_observation_deadline=%s\nport_collision_avoided=%t\nportless_started=%t\nhelper_tunnel=%t\nterm_cooperative_stop=%t\nterm_kill_escalation=%t\nterm_kill_log_evidence_incomplete=%t\nterm_kill_log_seal_pairing=%t\nterm_kill_stdout_log=%t\nterm_kill_stderr_log=%t\nterm_grace_stop=%t\nfresh_restart_authority=%t\n", runtime.GOOS, runtime.GOARCH, healthOK, echoOK, startupTimedOut, withdrawalObserved, withdrawalElapsed, republicationObserved, republicationElapsed, DefaultPublicationRecoveryWindow, nativeOCIRepublicationDeadline, portCollisionAvoided, portlessOK, helperTunnelOK, gracefulStopOK, killEscalation.Escalated, killEscalation.LogEvidenceIncomplete, killEscalation.LogSealPairing, killEscalation.StdoutLog, killEscalation.StderrLog, gracefulStopOK && killEscalation.Escalated, restartIdentityOK)
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
	withdrawalElapsed = primary.waitReachableMeasured(t, false, 5*time.Second)
	withdrawalObserved = true
	select {
	case outcome := <-primary.done:
		t.Fatalf("helper-tunnel withdrawal killed payload: (%#v, %v)", outcome.result, outcome.err)
	default:
	}
	republicationElapsed = primary.waitReachableMeasured(t, true, nativeOCIRepublicationDeadline)
	republicationObserved = republicationElapsed >= DefaultPublicationRecoveryWindow
	if !republicationObserved {
		t.Fatalf("OCI republication elapsed %s, want production recovery bound %s", republicationElapsed, DefaultPublicationRecoveryWindow)
	}

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
	var serviceFreshAttemptReadmission bool
	var serviceResidueVerifiedAbsent, serviceRetainedBindingVerified bool
	var serviceHelperLossInjected, serviceLostLogTyped bool
	var serviceLostLogDisposition string
	var serviceRecoveryElapsed, serviceRecoveryBound time.Duration
	var serviceFreshAttemptAdmissionElapsed, serviceKillToFreshAttemptAdmissionElapsed, serviceFreshAttemptAdmissionBound, staleEvidenceElapsed time.Duration
	var serviceFreshAttemptAdmittedAt time.Time
	var serviceBarrierTimeline ocihelper.BootBarrierTimelineReceipt
	var staleEvidenceLate bool
	var staleEvidenceArm string
	var staleOutboxState, staleOutboxReason string
	var removalManifestComplete, removalPending, removalEveryAttempt bool
	var removalServiceDataVolume, removalServiceDataOwnerRecord bool
	var removalCompleted, removalPriorBootSweep, removalPostDeleteAttestation, removalDeleteAttestInjection bool
	defer func() {
		if evidenceDirectory := os.Getenv("WEFTY_REALTIME_EVIDENCE_DIR"); evidenceDirectory != "" {
			payload := fmt.Sprintf("fresh_restart=%t\nstop_start=%t\nslot_saturation=%t\nretained_binding_digest=%t\nservice_helper_loss_injected=%t\nservice_helper_loss_observed=%t\nservice_helper_loss_observed_at=%s\nservice_fresh_attempt_readmission=%t\nservice_recovery_elapsed=%s\nservice_recovery_bound=%s\nservice_fresh_attempt_admission_elapsed=%s\nservice_kill_to_fresh_attempt_admission_elapsed=%s\nservice_fresh_attempt_admission_bound=%s\nservice_fresh_attempt_admission_margin=%s\nservice_fresh_attempt_admission_margin_basis=round4_cleanup_lt_600ms_ceil_1s_plus_preface_admission_ceil_1s\nservice_fresh_attempt_admitted_at=%s\nservice_barrier_advertised_reap_timeout=%s\nservice_barrier_takeover_bound=%s\nservice_barrier_verified_ready_bound=%s\nservice_barrier_started_at=%s\nservice_barrier_preface_completed_at=%s\nservice_barrier_session_admitted_at=%s\nservice_barrier_verified_ready_at=%s\nservice_barrier_prefaced_during_startup=%t\nservice_barrier_handshake_elapsed=%s\nservice_barrier_session_admission_elapsed=%s\nservice_barrier_sweep_elapsed=%s\nservice_barrier_verify_elapsed=%s\nservice_barrier_verified_ready_elapsed=%s\nservice_lost_log_typed=%t\nservice_lost_log_disposition=%s\nservice_stale_evidence_late=%t\nservice_stale_evidence_arm=%s\nservice_stale_evidence_elapsed=%s\nservice_stale_outbox_state=%s\nservice_stale_outbox_reason=%s\nservice_residue_verified_absent=%t\nservice_retained_binding_verified=%t\nremoval_manifest_complete=%t\nremoval_pending=%t\nremoval_every_attempt=%t\nremoval_service_data_volume=%t\nremoval_service_data_owner_record=%t\nremoval_post_delete_attestation=%t\nremoval_delete_attest_crash_injected=%t\nremoval_delete_attest_restart=NOT-RUN_hosted_lane\nremoval_completed=%t\nremoval_prior_boot_oci_sweep=%t\n", freshRestart, stopStart, saturation, retainedBinding, serviceHelperLossInjected, !serviceBarrierTimeline.HelperLossObservedAt.IsZero(), serviceBarrierTimeline.HelperLossObservedAt.UTC().Format(time.RFC3339Nano), serviceFreshAttemptReadmission, serviceRecoveryElapsed, serviceRecoveryBound, serviceFreshAttemptAdmissionElapsed, serviceKillToFreshAttemptAdmissionElapsed, serviceFreshAttemptAdmissionBound, nativeOCIFreshAttemptLeaseMargin, serviceFreshAttemptAdmittedAt.UTC().Format(time.RFC3339Nano), serviceBarrierTimeline.AdvertisedReapTimeout, serviceBarrierTimeline.TakeoverBound, serviceBarrierTimeline.VerifiedReadyBound, serviceBarrierTimeline.BarrierStartedAt.UTC().Format(time.RFC3339Nano), serviceBarrierTimeline.PrefaceCompletedAt.UTC().Format(time.RFC3339Nano), serviceBarrierTimeline.SessionAdmittedAt.UTC().Format(time.RFC3339Nano), serviceBarrierTimeline.VerifiedReadyAt.UTC().Format(time.RFC3339Nano), serviceBarrierTimeline.PrefacedDuringStartup, serviceBarrierTimeline.HandshakeElapsed, serviceBarrierTimeline.SessionAdmissionElapsed, serviceBarrierTimeline.SweepElapsed, serviceBarrierTimeline.VerifyElapsed, serviceBarrierTimeline.VerifiedReadyElapsed, serviceLostLogTyped, serviceLostLogDisposition, staleEvidenceLate, staleEvidenceArm, staleEvidenceElapsed, staleOutboxState, staleOutboxReason, serviceResidueVerifiedAbsent, serviceRetainedBindingVerified, removalManifestComplete, removalPending, removalEveryAttempt, removalServiceDataVolume, removalServiceDataOwnerRecord, removalPostDeleteAttestation, removalDeleteAttestInjection, removalCompleted, removalPriorBootSweep)
			if err := os.WriteFile(filepath.Join(evidenceDirectory, "oci-service-l1-agent-linux.txt"), []byte(payload), 0o600); err != nil {
				t.Errorf("write OCI L1/agent evidence: %v", err)
			}
		}
	}()

	network := plain.NewNetwork()
	l1Clock := newManualClock(time.Now())
	store, stopServer := startFailureServerWithPoliciesAndLease(t, network, l1Clock, map[string]l1.NodePolicy{
		"native-service-node": {Tags: []string{"native-service"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	}, l1.DefaultLeaseDuration)
	defer stopServer()
	publishedPort := reserveNativePublishedPort(t)
	serviceSpec := func(dispatchKey string) contract.JobSpec {
		return contract.JobSpec{
			SchemaVersion: contract.SchemaVersionV1, DispatchKey: dispatchKey, Kind: contract.JobKindOCI,
			Class: contract.JobClassService, Restart: contract.RestartAlways, RoutingTags: []string{"native-service"},
			RuntimeHandler: ocihelper.DefaultRuntimeHandler, PublishedPort: &publishedPort,
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
/usr/local/bin/wefty-echo-service &
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
	intentObservation := func(ctx context.Context) (OCIIntentObservation, error) {
		intent, err := intentSource.ReadIntent(ctx)
		return OCIIntentObservation{Enabled: intent.Enabled, Revision: intent.Revision}, err
	}
	authorities := newNativeClaimAuthorityRecorder()
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
		OCIIntent: intentObservation, OCIBootBarrier: barrier, WorkloadRuntimes: map[string]WorkloadRuntime{contract.JobKindOCI: adapter},
		AttemptDeadman:       nativeAcceptanceDeadman{barrier: barrier, nodeID: "native-service-node", bootSessionID: "native-service-boot", observe: authorities.record},
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
	firstAuthority := authorities.wait(t, firstAttempt, 5*time.Second)
	serviceClientFabric := network.NewFabric(fabric.Identity{NodeID: "native-service-client"})
	waitNativePublishedServiceHealth(t, serviceClientFabric, publishedPort, 15*time.Second)
	pinsBeforeStop, err := nodeAgent.logSpool.ListOCIImageBindingPins(t.Context())
	if err != nil || !containsBindingPin(pinsBeforeStop, primary.JobID, digest) {
		t.Fatalf("initial OCI binding pin=%+v err=%v", pinsBeforeStop, err)
	}
	controller, err := ocicontrol.NewController(ocicontrol.ControllerConfig{IntentPath: intentPath, Runtime: nodeAgent})
	if err != nil {
		t.Fatal(err)
	}
	stopContext, cancelStop := context.WithTimeout(t.Context(), 15*time.Second)
	stopResponse, err := controller.Stop(stopContext, ocicontrol.IntentMutationRequest{ExpectedRevision: 1})
	if err != nil || stopResponse.Intent.Enabled || stopResponse.Intent.Revision != 2 || !stopResponse.RuntimeQuiesced {
		cancelStop()
		t.Fatalf("OCI controller stop=%+v err=%v", stopResponse, err)
	}
	cancelStop()
	l1Clock.Advance(l1.DefaultLeaseDuration)
	// Advancing the configured production lease proves that local OCI intent stop is neither an
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
	intentStopReceipt := nodeAgent.logSpool.inspectCompletion(t.Context(), firstAttempt)
	if intentStopReceipt.State != "suppressed" || intentStopReceipt.Reason != "service_intent_stop" ||
		intentStopReceipt.IntentRevision != stopResponse.Intent.Revision || intentStopReceipt.Result == (l1.ProcessResult{}) {
		t.Fatalf("intent-stop spool disposition=%+v stop=%+v", intentStopReceipt, stopResponse)
	}
	pinsAfterStop, err := nodeAgent.logSpool.ListOCIImageBindingPins(t.Context())
	bindingPinBefore := containsBindingPin(pinsBeforeStop, primary.JobID, digest)
	bindingPinAfter := containsBindingPin(pinsAfterStop, primary.JobID, digest)
	if intentStopped.ServiceJob == nil || firstRunning.ServiceJob == nil {
		t.Fatalf("intent stop omitted service projection: before=%+v after=%+v", firstRunning.ServiceJob, intentStopped.ServiceJob)
	}
	if err != nil || intentStopped.BoundNodeID != firstRunning.BoundNodeID ||
		intentStopped.Spec.Execution.OCI == nil || intentStopped.Spec.Execution.OCI.Image.Digest == nil ||
		*intentStopped.Spec.Execution.OCI.Image.Digest != digest ||
		intentStopped.RestartStreak != firstRunning.RestartStreak ||
		intentStopped.LifetimeRestartCount != firstRunning.LifetimeRestartCount ||
		!bytes.Equal(intentStopped.LastFailure, firstRunning.LastFailure) ||
		intentStopped.LeaseLossCount != firstRunning.LeaseLossCount+1 || intentStopped.NextRestartAt == nil ||
		!intentStopped.NextRestartAt.After(intentStopped.UpdatedAt) ||
		intentStopped.NextRestartAt.After(intentStopped.UpdatedAt.Add(l1.MaximumServiceRestartDelay)) ||
		!bindingPinBefore || !bindingPinAfter {
		t.Fatalf("intent stop changed service binding/failure budget: before={binding_pin:%t BoundNodeID:%q digest:%q RestartStreak:%d LifetimeRestartCount:%d LeaseLossCount:%d LastFailure:%s NextRestartAt:%v} after={binding_pin:%t BoundNodeID:%q digest:%q RestartStreak:%d LifetimeRestartCount:%d LeaseLossCount:%d LastFailure:%s NextRestartAt:%v} pins_err=%v",
			bindingPinBefore, firstRunning.BoundNodeID, nativeOCIJobDigest(firstRunning), firstRunning.RestartStreak, firstRunning.LifetimeRestartCount, firstRunning.LeaseLossCount, firstRunning.LastFailure, firstRunning.NextRestartAt,
			bindingPinAfter, intentStopped.BoundNodeID, nativeOCIJobDigest(intentStopped), intentStopped.RestartStreak, intentStopped.LifetimeRestartCount, intentStopped.LeaseLossCount, intentStopped.LastFailure, intentStopped.NextRestartAt, err)
	}
	// The production lease advance also moves L1 onto its randomized service
	// restart backoff. Move the injected clock to that exact eligibility point
	// before re-enabling OCI; otherwise the harness can remain queued forever
	// even though the wall-clock agent and helper have recovered.
	if restartDelay := intentStopped.NextRestartAt.Sub(l1Clock.Now()); restartDelay > 0 {
		l1Clock.Advance(restartDelay)
	}
	if _, err := lima.SetOCIIntent(t.Context(), intentPath, 2, true, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := nodeAgent.RecoverOCIRuntimeCapabilities(t.Context()); err != nil {
		t.Fatal(err)
	}
	firstRunning = waitNativeServiceAttempt(t, store, nodeAgent, barrier, primary.JobID, firstAttempt, 45*time.Second)
	firstAttempt = firstRunning.CurrentAttemptID
	firstAuthority = authorities.wait(t, firstAttempt, 5*time.Second)
	oldGeneration, ready := barrier.Generation()
	if !ready {
		t.Fatal("service helper generation was not ready before runtime-loss injection")
	}
	serviceFreshAttemptAdmissionBound = l1.DefaultLeaseDuration - nativeOCIFreshAttemptLeaseMargin
	serviceRecoveryBound = serviceFreshAttemptAdmissionBound
	recoveryStarted := time.Now()
	if runtime.GOOS == "linux" {
		recoveryStarted = requestNativeOCIRootFault(t, "kill-helper:service-l1-fresh-attempt")
		serviceHelperLossInjected = true
	} else {
		barrier.Invalidate()
	}
	admissionWait := serviceFreshAttemptAdmissionBound - time.Since(recoveryStarted)
	if admissionWait <= 0 {
		t.Fatalf("helper fault consumed fresh-attempt admission bound %s before observation", serviceFreshAttemptAdmissionBound)
	}
	readmitted := waitNativeServiceAttempt(t, store, nodeAgent, barrier, primary.JobID, firstAttempt, admissionWait)
	serviceFreshAttemptAdmittedAt = time.Now()
	serviceKillToFreshAttemptAdmissionElapsed = serviceFreshAttemptAdmittedAt.Sub(recoveryStarted)
	healthWait := min(15*time.Second, serviceRecoveryBound-time.Since(recoveryStarted))
	if healthWait <= 0 {
		t.Fatalf("fresh attempt consumed recovery bound %s before health observation", serviceRecoveryBound)
	}
	healthElapsed := waitNativePublishedServiceHealth(t, serviceClientFabric, publishedPort, healthWait)
	serviceRecoveryElapsed = time.Since(recoveryStarted)
	newGeneration, ready := barrier.Generation()
	sweepReceipt, sweepReceiptOK := barrier.SweepReceipt()
	retainedIdentity, retainedIdentityErr := ocihelper.DeterministicResourceIdentity(ocirunner.HelperAuthority(firstAuthority))
	serviceResidueVerifiedAbsent = sweepReceiptOK && sweepReceipt.VerifiedAbsent && ocihelper.InventoryEmpty(sweepReceipt.VerifiedResidue)
	serviceBarrierTimeline = sweepReceipt.BarrierTimeline
	serviceFreshAttemptAdmissionElapsed = serviceFreshAttemptAdmittedAt.Sub(serviceBarrierTimeline.BarrierStartedAt)
	serviceRetainedBindingVerified = retainedIdentityErr == nil &&
		slices.Contains(sweepReceipt.VerifiedRetained.ManagedVolumes, retainedIdentity.ServiceVolumeDirectory) &&
		slices.Contains(sweepReceipt.VerifiedRetained.ManagedVolumeRecords, retainedIdentity.ServiceVolumeOwnerRecord)
	if !serviceResidueVerifiedAbsent || !serviceRetainedBindingVerified {
		t.Fatalf("runtime-loss service sweep receipt = %+v present=%t retained_identity=%+v identity_err=%v",
			sweepReceipt, sweepReceiptOK, retainedIdentity, retainedIdentityErr)
	}
	lostLogDispositions := 0
	if retainedIdentityErr == nil {
		for _, evidence := range sweepReceipt.SweepEvidence {
			if evidence.Class == ocihelper.RemovalResourceLogSegments && evidence.ID == retainedIdentity.LogSegmentDirectory && evidence.AttemptID == firstAttempt {
				serviceLostLogTyped = true
				serviceLostLogDisposition = "swept:" + string(evidence.Action)
				lostLogDispositions++
			}
		}
		for _, retention := range sweepReceipt.DurableRetentions {
			if retention.Class == ocihelper.RemovalResourceLogSegments && retention.ID == retainedIdentity.LogSegmentDirectory && retention.AttemptID == firstAttempt {
				serviceLostLogTyped = true
				serviceLostLogDisposition = "retained:" + string(retention.Reason)
				lostLogDispositions++
			}
		}
	}
	if !serviceLostLogTyped || lostLogDispositions != 1 {
		t.Fatalf("lost attempt log segment requires exactly one typed sweep or retention disposition: count=%d identity=%+v identity_err=%v sweep=%+v", lostLogDispositions, retainedIdentity, retainedIdentityErr, sweepReceipt)
	}
	if serviceBarrierTimeline.AdvertisedReapTimeout <= 0 || serviceBarrierTimeline.TakeoverBound != ocihelper.TakeoverTimeoutForReap(serviceBarrierTimeline.AdvertisedReapTimeout) ||
		serviceBarrierTimeline.VerifiedReadyBound != ocihelper.VerifiedReadyTimeoutForReap(serviceBarrierTimeline.AdvertisedReapTimeout) ||
		serviceBarrierTimeline.BarrierStartedAt.IsZero() || serviceBarrierTimeline.PrefaceCompletedAt.Before(serviceBarrierTimeline.BarrierStartedAt) ||
		serviceBarrierTimeline.SessionAdmittedAt.Before(serviceBarrierTimeline.PrefaceCompletedAt) || serviceBarrierTimeline.VerifiedReadyAt.Before(serviceBarrierTimeline.SessionAdmittedAt) ||
		serviceBarrierTimeline.HandshakeElapsed <= 0 || serviceBarrierTimeline.SessionAdmissionElapsed < serviceBarrierTimeline.HandshakeElapsed ||
		serviceBarrierTimeline.VerifiedReadyElapsed < serviceBarrierTimeline.SessionAdmissionElapsed || serviceBarrierTimeline.VerifiedReadyElapsed > serviceBarrierTimeline.VerifiedReadyBound {
		t.Fatalf("runtime-loss barrier timeline is incomplete or outside its derived bound: %+v", serviceBarrierTimeline)
	}
	if runtime.GOOS == "linux" && (serviceBarrierTimeline.HelperLossObservedAt.IsZero() || !serviceBarrierTimeline.PrefacedDuringStartup) {
		t.Fatalf("runtime-loss barrier did not naturally observe helper loss and preface startup: %+v", serviceBarrierTimeline)
	}
	newAuthority := authorities.wait(t, readmitted.CurrentAttemptID, 5*time.Second)
	stale, staleEvidenceElapsed, staleEvidenceLate, staleEvidenceArm, staleEvidenceObserved := waitNativeRuntimeLossEvidence(
		t, store, primary.JobID, firstAttempt, recoveryStarted, l1.DefaultLeaseDuration,
	)
	staleOutbox := nodeAgent.logSpool.inspectCompletion(t.Context(), firstAttempt)
	staleOutboxState = staleOutbox.State
	staleOutboxReason = staleOutbox.Reason
	if !staleEvidenceObserved {
		t.Fatalf("attempt %s did not retain typed runtime-loss evidence within production lease %s: l1=%+v outbox=%+v agent_status=%+v capability=%+v old_generation=%+v new_generation=%+v sweep=%+v",
			firstAttempt, l1.DefaultLeaseDuration, stale, staleOutbox, nodeAgent.Status(), nodeAgent.CapabilitySnapshot(), oldGeneration, newGeneration, sweepReceipt)
	}
	serviceFreshAttemptReadmission = ready && newGeneration != oldGeneration &&
		readmitted.CurrentAttemptID != firstAttempt && newAuthority.FencingToken != firstAuthority.FencingToken &&
		serviceFreshAttemptAdmissionElapsed <= serviceFreshAttemptAdmissionBound &&
		serviceKillToFreshAttemptAdmissionElapsed <= serviceFreshAttemptAdmissionBound &&
		serviceRecoveryElapsed <= serviceRecoveryBound && healthElapsed <= 15*time.Second
	if !serviceFreshAttemptReadmission {
		t.Fatalf("runtime-loss service re-admission = old_generation:%+v new_generation:%+v ready:%t old_authority:%+v new_authority:%+v stale:%+v stale_evidence_late:%t stale_evidence_elapsed:%s current:%+v health_elapsed:%s",
			oldGeneration, newGeneration, ready, firstAuthority, newAuthority, stale, staleEvidenceLate, staleEvidenceElapsed, readmitted, serviceRecoveryElapsed)
	}
	firstRunning = readmitted
	firstAttempt = readmitted.CurrentAttemptID
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
	restarted := waitNativeServiceAttempt(t, store, nodeAgent, barrier, primary.JobID, firstAttempt, 45*time.Second)
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
	startedAgain := waitNativeServiceAttempt(t, store, nodeAgent, barrier, primary.JobID, stopped.CurrentAttemptID, 45*time.Second)
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
		OCIIntent: intentObservation, OCIBootBarrier: restartBarrier, WorkloadRuntimes: map[string]WorkloadRuntime{contract.JobKindOCI: restartAdapter},
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

func nativeOCIJobDigest(job l1.Job) string {
	if job.Spec.Execution.OCI == nil || job.Spec.Execution.OCI.Image.Digest == nil {
		return ""
	}
	return *job.Spec.Execution.OCI.Image.Digest
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

func requestNativeOCIRootFault(t *testing.T, action string) time.Time {
	t.Helper()
	fifo := os.Getenv("WEFTY_OCI_FAULT_FIFO")
	directory := os.Getenv("WEFTY_OCI_FAULT_DIR")
	if fifo == "" || directory == "" {
		t.Fatal("Linux OCI root fault supervisor is not provisioned")
	}
	ack := filepath.Join(directory, action+".done")
	failure := filepath.Join(directory, action+".failed")
	_ = os.Remove(ack)
	_ = os.Remove(failure)
	writeDeadline := time.Now().Add(2 * time.Second)
	var requestedAt time.Time
	for {
		writer, err := os.OpenFile(fifo, os.O_WRONLY|syscall.O_NONBLOCK, 0)
		if err == nil {
			_, writeErr := writer.Write([]byte(action + "\n"))
			closeErr := writer.Close()
			if writeErr == nil && closeErr == nil {
				requestedAt = time.Now()
				break
			}
			err = errors.Join(writeErr, closeErr)
		}
		if time.Now().After(writeDeadline) {
			t.Fatalf("write root fault %s before deadline: %v", action, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(ack); err == nil {
			return requestedAt
		}
		if payload, err := os.ReadFile(failure); err == nil {
			t.Fatalf("root fault %s failed: %s", action, payload)
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("root fault %s was not acknowledged within 20s", action)
	return time.Time{}
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
	observe               func(l1.Claim)
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
	err = session.QueueAttemptRenewal(ocihelper.AttemptAuthority{
		NodeID: renewer.nodeID, BootSessionID: renewer.bootSessionID, JobID: claim.Job.JobID,
		AttemptID: claim.Lease.AttemptID, FencingToken: claim.Lease.FencingToken,
		Class: claim.Job.Spec.Class, RemovalGeneration: removalGeneration,
	}, ttl)
	if err == nil && renewer.observe != nil {
		renewer.observe(claim)
	}
	return err
}

type nativeClaimAuthorityRecorder struct {
	mu          sync.Mutex
	authorities map[string]workloadrunner.AttemptAuthority
}

func newNativeClaimAuthorityRecorder() *nativeClaimAuthorityRecorder {
	return &nativeClaimAuthorityRecorder{authorities: make(map[string]workloadrunner.AttemptAuthority)}
}

func (recorder *nativeClaimAuthorityRecorder) record(claim l1.Claim) {
	recorder.mu.Lock()
	recorder.authorities[claim.Lease.AttemptID] = workloadAuthority("native-service-node", "native-service-boot", claim)
	recorder.mu.Unlock()
}

func (recorder *nativeClaimAuthorityRecorder) wait(t *testing.T, attemptID string, timeout time.Duration) workloadrunner.AttemptAuthority {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		recorder.mu.Lock()
		authority, ok := recorder.authorities[attemptID]
		recorder.mu.Unlock()
		if ok {
			return authority
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("attempt %s did not publish a successful helper deadman renewal", attemptID)
	return workloadrunner.AttemptAuthority{}
}

func reserveNativePublishedPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func waitNativePublishedServiceHealth(t *testing.T, clientFabric fabric.Fabric, port int, timeout time.Duration) time.Duration {
	t.Helper()
	address := net.JoinHostPort(clientFabric.ConnectHost(), fmt.Sprint(port))
	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return clientFabric.Dial(ctx, network, address)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	started := time.Now()
	deadline := started.Add(timeout)
	for time.Now().Before(deadline) {
		requestContext, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
		request, err := http.NewRequestWithContext(requestContext, http.MethodGet, "http://service.invalid/healthz", nil)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		response, responseErr := client.Do(request)
		if responseErr == nil {
			var health struct {
				PID int `json:"pid"`
			}
			decodeErr := json.NewDecoder(response.Body).Decode(&health)
			closeErr := response.Body.Close()
			cancel()
			if response.StatusCode == http.StatusOK && decodeErr == nil && closeErr == nil && health.PID > 0 {
				return time.Since(started)
			}
		} else {
			cancel()
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("published service health was not reachable within %s", timeout)
	return 0
}

func waitNativeServiceState(t *testing.T, store *l1.Store, jobID string, state contract.JobState, timeout time.Duration) l1.Job {
	t.Helper()
	job, err := waitForFailureJobState(store, jobID, state, timeout)
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func waitNativeServiceAttempt(t *testing.T, store *l1.Store, nodeAgent *Agent, barrier *ocihelper.BootBarrier, jobID, priorAttemptID string, timeout time.Duration) l1.Job {
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
	attempts, attemptsErr := store.ListJobAttempts(t.Context(), jobID)
	generation, generationReady := barrier.Generation()
	sweep, sweepReady := barrier.SweepReceipt()
	t.Fatalf("service %s did not reach a fresh running attempt after %s: job=%+v attempts=%+v attempts_err=%v agent_status=%+v capability=%+v helper_generation=%+v helper_ready=%t sweep=%+v sweep_ready=%t",
		jobID, priorAttemptID, job, attempts, attemptsErr, nodeAgent.Status(), nodeAgent.CapabilitySnapshot(), generation, generationReady, sweep, sweepReady)
	return l1.Job{}
}

func waitNativeRuntimeLossEvidence(t *testing.T, store *l1.Store, jobID, attemptID string, anchor time.Time, timeout time.Duration) (l1.Attempt, time.Duration, bool, string, bool) {
	t.Helper()
	deadline := anchor.Add(timeout)
	var last *l1.Attempt
	for time.Now().Before(deadline) {
		attempts, err := store.ListJobAttempts(t.Context(), jobID)
		if err != nil {
			t.Fatal(err)
		}
		for index := range attempts {
			if attempts[index].AttemptID != attemptID {
				continue
			}
			attempt := attempts[index]
			last = &attempt
			if attempt.State == contract.AttemptFailed && attempt.Result != nil && attempt.Result.RuntimeFailure != nil &&
				attempt.Result.RuntimeFailure.Code == contract.RuntimeFailureUnavailable {
				return attempt, time.Since(anchor), false, "result", true
			}
			if attempt.State == contract.AttemptLost && attempt.LateResult != nil && attempt.LateResult.Kind == l1.LateResultObservation &&
				attempt.LateResult.Late && attempt.LateResult.Result != nil && attempt.LateResult.Result.RuntimeFailure != nil &&
				attempt.LateResult.Result.RuntimeFailure.Code == contract.RuntimeFailureUnavailable {
				return attempt, time.Since(anchor), true, "late_result", true
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if last != nil {
		return *last, time.Since(anchor), false, "not_observed", false
	}
	return l1.Attempt{}, time.Since(anchor), false, "not_observed", false
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
				readinessProbeInterval:    nativeOCIReadinessProbeInterval,
				readinessConnectTimeout:   nativeOCIReadinessConnectTimeout,
				publicationRecoveryWindow: DefaultPublicationRecoveryWindow,
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
  status=0
  wait "$server" || status=$?
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
	LogSealPairing        bool
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
	var seals []workloadrunner.OCILogSealObservation
	request.OCILogSealObserved = func(observation workloadrunner.OCILogSealObservation) {
		logMu.Lock()
		defer logMu.Unlock()
		seals = append(seals, observation)
	}
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
	// publish an incomplete seal first. Both orderings are valid, but the helper's
	// aggregate ProcessResult must agree with the two per-stream seal records.
	if outcome.err != nil || outcome.result.Outcome.Signal != "killed" || outcome.result.Outcome.TerminationCause != contract.TerminationCauseAgent {
		t.Fatalf("TERM-ignoring OCI service escalation = (%+v, %v)", outcome.result.Outcome, outcome.err)
	}
	logMu.Lock()
	stdoutLog := acceptanceLogContains(logs, contract.LogStdout, "kill-escalation-stdout")
	stderrLog := acceptanceLogContains(logs, contract.LogStderr, "kill-escalation-stderr")
	sealCounts := map[contract.LogStream]int{}
	sealEvidenceIncomplete := false
	for _, seal := range seals {
		sealCounts[seal.Stream]++
		sealEvidenceIncomplete = sealEvidenceIncomplete || !seal.Complete
	}
	logMu.Unlock()
	logSealPairing := sealCounts[contract.LogStdout] == 1 && sealCounts[contract.LogStderr] == 1 &&
		len(seals) == 2 && outcome.result.Outcome.LogEvidenceIncomplete == sealEvidenceIncomplete
	if !logSealPairing || !stdoutLog || !stderrLog {
		t.Fatalf("TERM-ignoring OCI service receipt completeness = log_evidence_incomplete:%t seal_incomplete:%t seals:%+v pairing:%t stdout_log:%t stderr_log:%t", outcome.result.Outcome.LogEvidenceIncomplete, sealEvidenceIncomplete, seals, logSealPairing, stdoutLog, stderrLog)
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
		LogSealPairing:        logSealPairing,
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
	reachable, _, matched := service.observeReachability(t, want, timeout)
	if !matched {
		t.Fatalf("OCI service reachability did not become %v", want)
	}
	return reachable
}

func (service *nativeOCIService) waitReachableMeasured(t *testing.T, want bool, timeout time.Duration) time.Duration {
	t.Helper()
	_, elapsed, matched := service.observeReachability(t, want, timeout)
	if !matched {
		t.Fatalf("OCI service reachability did not become %v", want)
	}
	return elapsed
}

func (service *nativeOCIService) observeReachability(t *testing.T, want bool, timeout time.Duration) (bool, time.Duration, bool) {
	t.Helper()
	started := time.Now()
	deadline := time.Now().Add(timeout)
	reachable := false
	for time.Now().Before(deadline) {
		request, err := http.NewRequest(http.MethodGet, "http://service.invalid/healthz", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := service.client.Do(request)
		reachable = err == nil && response.StatusCode == http.StatusOK
		if response != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
			_ = response.Body.Close()
		}
		if reachable == want {
			return reachable, time.Since(started), true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return reachable, time.Since(started), false
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
