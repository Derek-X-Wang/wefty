//go:build service_acceptance_realtiming && (darwin || linux)

package serviceacceptance

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
	"github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
	"github.com/coder/websocket"
)

func TestLinuxNativeComputerCLIMatrixAtProductionTimings(t *testing.T) {
	receipt := newLinuxComputerMatrixReceipt()
	evidence := newRealTimingEvidence(t)
	t.Cleanup(func() {
		receipt.finish()
		evidence.recordJSON("linux-computer-matrix.json", receipt)
	})
	if runtime.GOOS != "linux" {
		for _, required := range linuxComputerMatrixRows {
			receipt.begin(required.ID)
			if err := receipt.notRun(required.ID, 128,
				"Linux-native Computer acceptance requires the Ubuntu containerd runner; hosted Darwin does not claim Lima",
				map[string]bool{"darwin_typed_skip_recorded": true}, nil); err != nil {
				t.Fatal(err)
			}
		}
		return
	}
	t.Setenv("WEFTY_DEV_PLAIN_FABRIC_ID", "plain-linux-computer-acceptance")

	reference := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_COMPUTER_REFERENCE")
	digest := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_COMPUTER_DIGEST")
	archive := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_COMPUTER_ARCHIVE")
	variant := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_COMPUTER_VARIANT")
	reimagePrefix := "WEFTY_OCI_WAYLAND_COMPUTER_"
	if variant == "wayland" {
		reimagePrefix = "WEFTY_OCI_XFCE_COMPUTER_"
	} else if variant != "xfce" {
		t.Fatalf("unknown Computer matrix variant %q", variant)
	}
	reimageReference := requiredComputerRealtimeEnvironment(t, reimagePrefix+"REFERENCE")
	reimageDigest := requiredComputerRealtimeEnvironment(t, reimagePrefix+"DIGEST")
	reimageArchive := requiredComputerRealtimeEnvironment(t, reimagePrefix+"ARCHIVE")
	if reimageDigest == digest {
		t.Fatalf("Computer reimage artifact aliases the current %s image digest %s", variant, digest)
	}
	imageRuntime := readPublishedComputerRuntimeReceipt(t, requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_COMPUTER_RUNTIME_RECEIPT"))
	receipt.Image = linuxComputerImageEvidence{Variant: variant, Reference: reference, IndexDigest: digest,
		PlatformDigest: imageRuntime.Digest, Archive: filepath.Base(archive)}
	candidate, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil || strings.TrimSpace(string(candidate)) == "" {
		t.Fatalf("resolve candidate SHA: %v", err)
	}
	receipt.CandidateSHA = strings.TrimSpace(string(candidate))
	if published := requiredComputerRealtimeEnvironment(t, "CANDIDATE_SHA"); published != receipt.CandidateSHA {
		t.Fatalf("candidate SHA %s does not match published artifact SHA %s", receipt.CandidateSHA, published)
	}
	receipt.ResourceCaps = linuxComputerResourceCaps{MemoryBytes: 1 << 30, DiskBytes: 128 << 20, BackupCap: 4, SubmitMaxInflight: l1.DefaultComputerSubmitMaxInflight}
	receipt.Timings = map[string]string{
		"l1_lease":     l1.DefaultLeaseDuration.String(),
		"l1_node_dead": l1.DefaultNodeDeadAfter.String(),
		"l3_reconcile": "production-default",
	}
	receipt.Deviations = []linuxComputerDeviation{{ID: "dev.plain_fabric_identity", Status: "DEVIATION",
		Reason: "secretless Linux CI uses the shipped DEVELOPMENT ONLY self-asserted plain person identity route; attended Fabric identity remains #128"}}

	helperSocket := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_HELPER_SOCKET")
	helperChecksum := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_HELPER_CHECKSUM")
	probeReference := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_PROBE_REFERENCE")
	probeDigest := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_PROBE_DIGEST")
	recordBarrierResidue := func(residue *ocihelper.NamespaceResidueError) {
		receipt.ResidueInventories["between_package_runtime_residue"] = residue.RuntimeResidue
		receipt.ResidueInventories["between_package_durable_retained"] = residue.DurableRetained
		receipt.ResidueInventories["between_package_observed_inventory"] = residue.Observed
		receipt.ResidueAssertions["between_package_absence_blocked_by_runtime_residue"] = !ocihelper.InventoryEmpty(residue.RuntimeResidue)
		receipt.ResidueAssertions["between_package_observed_classified"] = reflect.DeepEqual(
			residue.Observed, mergeAcceptanceInventory(residue.RuntimeResidue, residue.DurableRetained))
	}
	importRealtimeProbeImage(t, requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_PROBE_ARCHIVE"),
		helperSocket, helperChecksum, probeReference, probeDigest, recordBarrierResidue)
	importRealtimeProbeImage(t, archive, helperSocket, helperChecksum, reference, digest, recordBarrierResidue)
	importRealtimeProbeImage(t, reimageArchive, helperSocket, helperChecksum, reimageReference, reimageDigest, recordBarrierResidue)
	intentPath := filepath.Join(t.TempDir(), "oci-intent.json")
	if _, err := lima.InitializeOCIIntent(intentPath, time.Now()); err != nil {
		t.Fatal(err)
	}
	harness := newAcceptanceHarnessWithOptions(t, acceptanceHarnessOptions{
		leaseDuration: l1.DefaultLeaseDuration, productionTimings: true, computerLane: true,
		agentArguments: []string{
			"--oci-helper-socket=" + helperSocket,
			"--oci-helper-checksum=" + helperChecksum,
			"--oci-probe-image=" + probeReference,
			"--oci-probe-digest=" + probeDigest,
			"--oci-intent-file=" + intentPath,
		},
	})
	t.Cleanup(func() {
		evidence.recordProcessOutput("computer-control-plane.log", harness.controlPlane)
		evidence.recordProcessOutput("computer-run-ledger.log", harness.runLedger)
		for index, process := range harness.agents {
			evidence.recordProcessOutput(fmt.Sprintf("computer-agent-%02d.log", index+1), process)
		}
	})

	receipt.begin("linux.create_boot")
	traitRefusal := runComputerCLIExpectError(t, harness, "", "", "services", "create", "--name", "trait-refusal",
		"--image", reference+"@"+digest, "--node", "acceptance-node", "--disk-bytes", fmt.Sprint(64<<20))
	created := runComputerCLI[l1.Computer](t, harness, false, "services", "create", "--computer",
		"--name", "linux-native-acceptance", "--image", reference+"@"+digest, "--node", "acceptance-node",
		"--memory-bytes", fmt.Sprint(1<<30), "--disk-bytes", fmt.Sprint(128<<20), "--backup-cap", "4",
		"--idempotency-key", "linux-native-computer-create")
	ready := waitForComputerCLI(t, harness, created.ComputerID, 3*time.Minute, func(current l1.Computer) bool {
		return computerDisplayPublished(current) && current.AppliedRevision == current.IntentRevision
	})
	recordComputerAuthority(receipt, ready)
	diskEvidence := inspectLiveComputerDisk(t, ready)
	policy := bootstrapComputerAcceptanceAdmin(t, harness)
	adminView := startTakeoverViewCLI(t, evidence, harness, ready.ComputerID, "linux-admin", "linux-admin-device-a", takeoverViewRetryStalePolicy)
	receipt.TakeoverRetryStderr = append(receipt.TakeoverRetryStderr, adminView.toleratedStderr...)
	adminTake := runComputerCLIPerson[contract.ComputerControlReceipt](t, harness, "linux-admin", "linux-admin-device-a",
		"services", "takeover", "take", ready.ComputerID, "--session-token-file", adminView.tokenFile)
	adminRelease := contract.ComputerControlReceipt{}
	if !mutatingLinuxComputerRow("linux.create_boot") {
		adminRelease = runComputerCLIPerson[contract.ComputerControlReceipt](t, harness, "linux-admin", "linux-admin-device-a",
			"services", "takeover", "release", ready.ComputerID, "--session-token-file", adminView.tokenFile)
	}
	adminView.stop(t)
	completeLinuxComputerRow(t, receipt, "linux.create_boot", map[string]bool{
		"trait_only_refusal_observed":     strings.Contains(traitRefusal, "require --computer"),
		"trait_only_publication":          ready.CurrentJob.Spec.PublishedPort == nil,
		"fully_allocated_disk_on_disk":    diskEvidence.FullyAllocated,
		"view_named_endpoint_dialed":      adminView.admitted,
		"control_named_endpoint_dialed":   adminTake.TenureState == contract.ComputerControlTenureHeld,
		"control_named_endpoint_released": adminRelease.TenureState == contract.ComputerControlTenureFree,
		"helper_admitted_real_payload":    ready.CurrentJob.CurrentAttemptID != "",
		"cross_process_plain_authority":   len(policy.Admins) == 1 && policy.Admins[0].FabricID == "plain-linux-computer-acceptance" && adminView.admitted,
	}, map[string]string{"computer_id": ready.ComputerID, "job_id": ready.CurrentJobID,
		"attempt_id": ready.CurrentJob.CurrentAttemptID, "storage_id": ready.StorageID,
		"storage_generation": fmt.Sprint(ready.StorageGeneration), "intent_revision": fmt.Sprint(ready.IntentRevision),
		"disk_path": diskEvidence.Path, "disk_blocks_bytes": fmt.Sprint(diskEvidence.BlocksBytes)})

	receipt.begin("linux.remote_takeover")
	viewerIdentity := runComputerCLIPersonWithEvidence[l1.AuthenticatedPerson](t, evidence, "whoami-cli-viewer.json", harness, "linux-viewer", "linux-viewer-device", "whoami")
	if viewerIdentity.FabricID != policy.Admins[0].FabricID || viewerIdentity.UserID != "linux-viewer" || viewerIdentity.DeviceID != "linux-viewer-device" {
		t.Fatalf("viewer whoami observation = %#v", viewerIdentity)
	}
	viewGrant := runComputerCLIPersonWithEvidence[l1.ComputerGrantMutationResult](t, evidence, "grant-cli-view.json", harness, "linux-admin", "linux-admin-device-a",
		"services", "grant", ready.ComputerID, "linux-viewer", "--permission", "view",
		"--policy-revision", fmt.Sprint(policy.Revision), "--idempotency-key", "linux-native-view-grant")
	if !viewGrant.MutationApplied || viewGrant.Grant.Permission != l1.ComputerGrantView {
		t.Fatalf("Computer view grant = %#v", viewGrant)
	}
	receipt.FabricIdentities = append(receipt.FabricIdentities,
		linuxComputerFabricIdentity{Role: "administrator", FabricID: policy.Admins[0].FabricID, UserID: "linux-admin", DeviceID: "linux-admin-device-a"},
		linuxComputerFabricIdentity{Role: "viewer", FabricID: viewGrant.Grant.FabricID, UserID: "linux-viewer", DeviceID: "linux-viewer-device"})
	// L1 publishes the durable grant before the hosting agent can acknowledge
	// and install that policy revision. Establish live viewer admission through
	// the typed stale-policy retry path before the direct RFB isolation probe.
	viewerView := startTakeoverViewCLI(t, evidence, harness, ready.ComputerID, "linux-viewer", "linux-viewer-device", takeoverViewRetryStalePolicy)
	receipt.TakeoverRetryStderr = append(receipt.TakeoverRetryStderr, viewerView.toleratedStderr...)
	inputIsolation := proveLiveViewInputIsolation(t, harness, ready, "linux-viewer", "linux-viewer-device")
	viewerTakeDenied := runComputerCLIPersonExpectError(t, harness, "linux-viewer", "linux-viewer-device",
		"services", "takeover", "take", ready.ComputerID, "--session-token-file", viewerView.tokenFile)
	controlGrant := runComputerCLIPersonWithEvidence[l1.ComputerGrantMutationResult](t, evidence, "grant-cli-control.json", harness, "linux-admin", "linux-admin-device-a",
		"services", "grant", ready.ComputerID, "linux-viewer", "--permission", "control",
		"--policy-revision", fmt.Sprint(viewGrant.Grant.PolicyRevision), "--idempotency-key", "linux-native-control-grant")
	viewerView.stop(t)
	viewerControl := startTakeoverViewCLI(t, evidence, harness, ready.ComputerID, "linux-viewer", "linux-viewer-device", takeoverViewRetryStalePolicy)
	receipt.TakeoverRetryStderr = append(receipt.TakeoverRetryStderr, viewerControl.toleratedStderr...)
	viewerTake := runComputerCLIPerson[contract.ComputerControlReceipt](t, harness, "linux-viewer", "linux-viewer-device",
		"services", "takeover", "take", ready.ComputerID, "--session-token-file", viewerControl.tokenFile)
	viewerRelease := contract.ComputerControlReceipt{}
	if !mutatingLinuxComputerRow("linux.remote_takeover") {
		viewerRelease = runComputerCLIPerson[contract.ComputerControlReceipt](t, harness, "linux-viewer", "linux-viewer-device",
			"services", "takeover", "release", ready.ComputerID, "--session-token-file", viewerControl.tokenFile)
	}
	if viewerTake.TenureState != contract.ComputerControlTenureHeld {
		t.Fatalf("viewer take receipt = %#v", viewerTake)
	}
	viewerTake = runComputerCLIPerson[contract.ComputerControlReceipt](t, harness, "linux-viewer", "linux-viewer-device",
		"services", "takeover", "take", ready.ComputerID, "--session-token-file", viewerControl.tokenFile)
	adminOverrideView := startTakeoverViewCLI(t, evidence, harness, ready.ComputerID, "linux-admin", "linux-admin-device-b", takeoverViewRetryNone)
	adminOverride := runComputerCLIPerson[contract.ComputerControlReceipt](t, harness, "linux-admin", "linux-admin-device-b",
		"services", "takeover", "take", ready.ComputerID, "--session-token-file", adminOverrideView.tokenFile)
	adminOverrideRelease := runComputerCLIPerson[contract.ComputerControlReceipt](t, harness, "linux-admin", "linux-admin-device-b",
		"services", "takeover", "release", ready.ComputerID, "--session-token-file", adminOverrideView.tokenFile)
	viewerTake = runComputerCLIPerson[contract.ComputerControlReceipt](t, harness, "linux-viewer", "linux-viewer-device",
		"services", "takeover", "take", ready.ComputerID, "--session-token-file", viewerControl.tokenFile)
	viewerDrivingBeforeRevoke := viewerTake.TenureState == contract.ComputerControlTenureHeld && viewerTake.HumanDriving &&
		readLiveComputerHumanDriving(t, ready.CurrentJobID)
	revoked := runComputerCLIPerson[l1.ComputerGrantMutationResult](t, harness, "linux-admin", "linux-admin-device-a",
		"services", "revoke", ready.ComputerID, "linux-viewer", "--policy-revision", fmt.Sprint(controlGrant.Grant.PolicyRevision),
		"--idempotency-key", "linux-native-control-revoke", "--wait", "--wait-timeout", "3m")
	viewerControl.waitClosed(t, 30*time.Second)
	staleSessionDenied := runComputerCLIPersonExpectControlError(t, harness, "linux-viewer", "linux-viewer-device",
		"services", "takeover", "take", ready.ComputerID, "--session-token-file", viewerControl.tokenFile)
	if staleSessionDenied.Receipt == nil {
		t.Fatalf("revoked take-over session omitted its typed terminal receipt: %#v", staleSessionDenied)
	}
	viewerDrivingAfterRevoke := waitLiveComputerHumanDriving(t, ready.CurrentJobID, false, 10*time.Second)
	adminOverrideView.stop(t)
	audit := runComputerCLIPerson[l1.ComputerTakeoverAuditList](t, harness, "linux-admin", "linux-admin-device-a",
		"services", "takeover", "audit", "tail", ready.ComputerID, "--limit", "100")
	auditKinds, auditAuthority := takeoverAuditEvidence(audit)
	revokedControlBeforeSession := takeoverAuditReleasePrecedesClose(audit, viewerTake.HolderSessionID, l1.ComputerTakeoverRevoked)
	receipt.AuthorityGenerations = appendUniqueInt64(receipt.AuthorityGenerations, auditAuthority)
	completeLinuxComputerRow(t, receipt, "linux.remote_takeover", map[string]bool{
		"cli_view_grant_cas":           viewGrant.MutationApplied,
		"view_admission_live":          viewerView.admitted,
		"view_only_take_refused":       strings.Contains(viewerTakeDenied, string(contract.ErrorControlNotAuthorized)),
		"view_pointer_isolation_live":  inputIsolation,
		"cli_control_grant_cas":        controlGrant.MutationApplied,
		"cli_take_live":                viewerTake.TenureState == contract.ComputerControlTenureHeld,
		"cli_release_live":             viewerRelease.TenureState == contract.ComputerControlTenureFree,
		"admin_override_live":          adminOverride.OverrideDisplacedSessionID != "" && adminOverride.SignalStayedTrue && adminOverrideRelease.TenureState == contract.ComputerControlTenureFree,
		"cli_revoke_installed":         revoked.ObservationState == "completed",
		"revoked_while_driving":        viewerDrivingBeforeRevoke,
		"revocation_closed_session":    staleSessionDenied.Error.Code == contract.ErrorTakeoverSessionEnded && staleSessionDenied.Receipt.SessionEndReason == string(l1.ComputerTakeoverRevoked),
		"revocation_cleared_driver":    !viewerDrivingAfterRevoke,
		"revoked_release_before_close": revokedControlBeforeSession,
		"audit_tail_session_open":      auditKinds[l1.ComputerTakeoverSessionOpen],
		"audit_tail_session_close":     auditKinds[l1.ComputerTakeoverSessionClose],
		"audit_tail_control_acquired":  auditKinds[l1.ComputerTakeoverControlAcquired],
		"audit_tail_control_released":  auditKinds[l1.ComputerTakeoverControlReleased],
		"audit_tail_admin_overrode":    auditKinds[l1.ComputerTakeoverAdminOverrode],
	}, map[string]string{
		"policy_revision":      fmt.Sprint(revoked.Grant.PolicyRevision),
		"authority_generation": fmt.Sprint(auditAuthority),
		"viewer_fabric_id":     viewGrant.Grant.FabricID,
		"revoked_session_id":   viewerTake.HolderSessionID,
		"session_end_reason":   staleSessionDenied.Receipt.SessionEndReason,
	})

	receipt.begin("linux.restart_survival")
	oldAttempt, oldStorage, oldGeneration := ready.CurrentJob.CurrentAttemptID, ready.StorageID, ready.StorageGeneration
	profileMarker := plantLiveProfileMarker(t, ready, "restart-survival-marker")
	lossAttempts := map[string]string{}
	var helperLossTerminal *l1.Attempt
	for _, action := range []string{"kill-payload", "kill-shim", "kill-helper", "stop-containerd"} {
		before := ready.CurrentJob.CurrentAttemptID
		faultAction := action
		if action == "kill-payload" || action == "kill-shim" {
			faultAction += ":" + ready.CurrentJobID
		}
		triggerLinuxComputerFault(t, harness, faultAction)
		if action == "stop-containerd" {
			triggerLinuxComputerFault(t, harness, "start-containerd")
		}
		if harness.agent.exited() {
			harness.restartAgent(t)
		}
		ready = waitForComputerCLI(t, harness, ready.ComputerID, 5*time.Minute, func(current l1.Computer) bool {
			return computerDisplayPublished(current) &&
				current.CurrentJob.CurrentAttemptID != "" && current.CurrentJob.CurrentAttemptID != before
		})
		lossAttempts[action] = ready.CurrentJob.CurrentAttemptID
		if action == "kill-helper" {
			terminal := waitForComputerAttemptTerminal(t, harness, ready.CurrentJobID, before, 30*time.Second)
			helperLossTerminal = &terminal
		}
		assertLiveProfileMarker(t, ready, profileMarker)
	}
	beforeAgentLoss := ready.CurrentJob.CurrentAttemptID
	restarted := ready
	if !mutatingLinuxComputerRow("linux.restart_survival") {
		harness.agent.kill(t)
		harness.restartAgent(t)
		restarted = waitForComputerCLI(t, harness, ready.ComputerID, 4*time.Minute, func(current l1.Computer) bool {
			return computerDisplayPublished(current) &&
				current.CurrentJob.CurrentAttemptID != "" && current.CurrentJob.CurrentAttemptID != beforeAgentLoss
		})
	}
	assertLiveProfileMarker(t, restarted, profileMarker)
	// The live-session file retains the old door's ephemeral endpoint. Present
	// the unchanged bearer to the republished door so this row tests Node-lineage
	// terminality rather than a TCP refusal from the dead listener.
	oldSessionEndpoint, currentSessionEndpoint := retargetTakeoverSessionCapability(t, viewerControl.tokenFile, restarted.DisplayEndpoint)
	oldAuthorityRejected := runComputerCLIPersonExpectControlError(t, harness, "linux-viewer", "linux-viewer-device",
		"services", "takeover", "take", restarted.ComputerID, "--session-token-file", viewerControl.tokenFile)
	oldSessionEndReason := ""
	if oldAuthorityRejected.Receipt != nil {
		oldSessionEndReason = oldAuthorityRejected.Receipt.SessionEndReason
	}
	recordComputerAuthority(receipt, restarted)
	helperLossTerminalCode, helperLossTerminalID := "", ""
	if helperLossTerminal != nil {
		helperLossTerminalID = helperLossTerminal.AttemptID
		if helperLossTerminal.Result != nil && helperLossTerminal.Result.RuntimeFailure != nil {
			helperLossTerminalCode = string(helperLossTerminal.Result.RuntimeFailure.Code)
		}
	}
	completeLinuxComputerRow(t, receipt, "linux.restart_survival", map[string]bool{
		"payload_loss_fresh_attempt": lossAttempts["kill-payload"] != "" && lossAttempts["kill-payload"] != oldAttempt,
		"shim_loss_fresh_attempt":    lossAttempts["kill-shim"] != "" && lossAttempts["kill-shim"] != lossAttempts["kill-payload"],
		"helper_loss_fresh_attempt":  lossAttempts["kill-helper"] != "" && lossAttempts["kill-helper"] != lossAttempts["kill-shim"],
		"helper_loss_prior_attempt_typed_terminal": helperLossTerminal != nil && helperLossTerminal.AttemptID == lossAttempts["kill-shim"] &&
			helperLossTerminal.State == contract.AttemptFailed && helperLossTerminal.Result != nil && helperLossTerminal.Result.RuntimeFailure != nil &&
			helperLossTerminal.Result.RuntimeFailure.Code == contract.RuntimeFailureUnavailable,
		"runtime_loss_fresh_attempt":     lossAttempts["stop-containerd"] != "" && lossAttempts["stop-containerd"] != lossAttempts["kill-helper"],
		"agent_loss_fresh_attempt":       restarted.CurrentJob.CurrentAttemptID != beforeAgentLoss,
		"same_storage_generation":        restarted.StorageID == oldStorage && restarted.StorageGeneration == oldGeneration,
		"profile_marker_survived_losses": true,
		"readiness_republished":          oldSessionEndpoint != currentSessionEndpoint,
		"old_session_authority_rejected": oldAuthorityRejected.Error.Code == contract.ErrorTakeoverSessionEnded &&
			oldAuthorityRejected.Receipt != nil && oldAuthorityRejected.Receipt.SessionEndReason == string(l1.ComputerTakeoverAttemptAuthorityLost),
	}, map[string]string{"old_attempt_id": oldAttempt, "new_attempt_id": restarted.CurrentJob.CurrentAttemptID,
		"storage_id": restarted.StorageID, "storage_generation": fmt.Sprint(restarted.StorageGeneration),
		"profile_marker": filepath.Base(profileMarker), "helper_loss_terminal_attempt_id": helperLossTerminalID,
		"helper_loss_terminal_code": helperLossTerminalCode, "old_session_endpoint": oldSessionEndpoint,
		"current_session_endpoint": currentSessionEndpoint, "old_session_end_reason": oldSessionEndReason})

	receipt.begin("linux.reconfiguration")
	resized := runComputerCLI[l1.Computer](t, harness, false, "services", "resize", restarted.ComputerID,
		"--disk-bytes", fmt.Sprint(160<<20), "--expect-current", "--idempotency-key", "linux-native-grow")
	resized = waitForComputerCLI(t, harness, resized.ComputerID, 3*time.Minute, func(current l1.Computer) bool {
		return current.ReconfigurationPhase == l1.ComputerReconfigurationStable && current.AppliedRevision == current.IntentRevision && current.DesiredDiskBytes == 160<<20
	})
	reset := runComputerCLI[l1.Computer](t, harness, false, "services", "reset", resized.ComputerID,
		"--expect-current", "--idempotency-key", "linux-native-reset", "--terminate-sessions")
	resetIntent := reset.IntentRevision
	resetCrashObserved := false
	if !mutatingLinuxComputerRow("linux.reconfiguration") {
		triggerLinuxComputerFault(t, harness, "kill-helper")
		resetCrashObserved = true
		if harness.agent.exited() {
			harness.restartAgent(t)
		}
	}
	reset = waitForComputerCLI(t, harness, reset.ComputerID, 3*time.Minute, func(current l1.Computer) bool {
		return current.ReconfigurationPhase == l1.ComputerReconfigurationStable && current.AppliedRevision == current.IntentRevision && current.StorageGeneration > resized.StorageGeneration
	})
	// Reset can publish its verified empty successor before the first attempt
	// initializes that filesystem root for the image user. Reimage therefore
	// carries the explicit one-shot ownership authority instead of depending on
	// an incidental attach winning this race.
	reimageStarted := time.Now()
	reimaged := runComputerCLI[l1.Computer](t, harness, false, "services", "reimage", reset.ComputerID,
		"--image", reimageReference+"@"+reimageDigest, "--expect-current", "--idempotency-key", "linux-native-reimage", "--terminate-sessions", "--chown")
	reimaged = waitForComputerCLI(t, harness, reimaged.ComputerID, 4*time.Minute, func(current l1.Computer) bool {
		failOnTypedReimagePreflight(t, current, reset.CurrentSpecRevision)
		return current.ReconfigurationPhase == l1.ComputerReconfigurationStable && current.AppliedRevision == current.IntentRevision &&
			current.CurrentSpecRevision > reset.CurrentSpecRevision && computerDisplayPublished(current)
	})
	chownReimaged := reimaged
	chownReimageElapsed := time.Since(reimageStarted)
	ownershipMatchStarted := time.Now()
	reimaged = runComputerCLI[l1.Computer](t, harness, false, "services", "reimage", reimaged.ComputerID,
		"--image", reference+"@"+digest, "--expect-current", "--idempotency-key", "linux-native-reimage-ownership-match", "--terminate-sessions")
	reimaged = waitForComputerCLI(t, harness, reimaged.ComputerID, 4*time.Minute, func(current l1.Computer) bool {
		failOnTypedReimagePreflight(t, current, chownReimaged.CurrentSpecRevision)
		return current.ReconfigurationPhase == l1.ComputerReconfigurationStable && current.AppliedRevision == current.IntentRevision &&
			current.CurrentSpecRevision > chownReimaged.CurrentSpecRevision && computerDisplayPublished(current)
	})
	ownershipMatchElapsed := time.Since(ownershipMatchStarted)
	abortEvidence := exerciseLiveReconfigurationAbort(t, harness, reference, digest)
	detachment := inspectLiveComputerReimageDetachment(t, harness, reimaged)
	recordComputerAuthority(receipt, reimaged)
	completeLinuxComputerRow(t, receipt, "linux.reconfiguration", map[string]bool{
		"grow_applied_live":            resized.DesiredDiskBytes == 160<<20,
		"reset_crash_phase_live":       resetCrashObserved && reset.IntentRevision >= resetIntent,
		"reset_fresh_generation_live":  reset.StorageGeneration > resized.StorageGeneration,
		"reimage_new_projection_live":  reimaged.CurrentSpecRevision > chownReimaged.CurrentSpecRevision && chownReimaged.CurrentSpecRevision > reset.CurrentSpecRevision,
		"reimage_ownership_match_live": ownershipMatchElapsed > 0,
		"detachment_receipt_live":      detachment,
		"abort_after_dead_node_live":   abortEvidence.Aborted,
		"stale_cas_rejected_live":      abortEvidence.StaleCASRejected,
		"no_automatic_rollback_live":   abortEvidence.NoAutoRollback,
	}, map[string]string{"intent_revision": fmt.Sprint(reimaged.IntentRevision), "spec_revision": fmt.Sprint(reimaged.CurrentSpecRevision),
		"storage_generation": fmt.Sprint(reimaged.StorageGeneration), "reimage_reference": reimageReference,
		"reimage_digest": reimageDigest, "reimage_chown_elapsed": chownReimageElapsed.String(),
		"reimage_ownership_match_elapsed": ownershipMatchElapsed.String(), "aborted_computer_id": abortEvidence.ComputerID})

	receipt.begin("linux.storage_provenance")
	backupOutput := runComputerCLI[storageCLIMutationReceipt](t, harness, false, "services", "backup", "create", reimaged.ComputerID,
		"--expect-current", "--allow-power-off", "--idempotency-key", "linux-native-backup", "--wait", "4m")
	if backupOutput.Backups == nil || len(backupOutput.Backups.Backups) != 1 || backupOutput.Backups.Backups[0].Status != "available" {
		t.Fatalf("Computer Backup result = %#v", backupOutput)
	}
	backup := backupOutput.Backups.Backups[0]
	cloneOutput := runComputerCLI[storageCLIMutationReceipt](t, harness, false, "services", "clone", reimaged.ComputerID, backup.BackupID,
		"--name", "linux-native-clone", "--disk-bytes", fmt.Sprint(160<<20), "--expect-current",
		"--idempotency-key", "linux-native-clone", "--wait", "4m")
	if cloneOutput.Computer == nil || cloneOutput.Computer.ComputerID == "" {
		t.Fatalf("Computer clone result = %#v", cloneOutput)
	}
	cloneReceipt := requiredStorageCopyReceipt(t, cloneOutput.StorageProvenance, "clone", cloneOutput.Computer.ComputerID)
	if cloneReceipt.MachineIDBeforeDigest == cloneReceipt.MachineIDAfterDigest || !cloneReceipt.SourceUnchanged ||
		!cloneReceipt.DestinationPrepared || cloneReceipt.PreparationReceipt || cloneReceipt.DestinationChown {
		t.Fatalf("Computer clone receipt = %#v", cloneReceipt)
	}
	startedClone := runComputerCLI[l1.Computer](t, harness, false, "services", "start", cloneOutput.Computer.ComputerID, "--expect-current")
	startedClone = waitForComputerCLI(t, harness, startedClone.ComputerID, 3*time.Minute, computerDisplayPublished)
	cloneOutput.Computer = &startedClone
	recordComputerAuthority(receipt, *cloneOutput.Computer)
	exportDirectory := filepath.Join(t.TempDir(), "custody-export")
	if err := os.MkdirAll(exportDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	exportOutput := runComputerCLI[storageCLIMutationReceipt](t, harness, false, "services", "custody", "export", reimaged.ComputerID, backup.BackupID,
		"--path", exportDirectory, "--expect-current", "--idempotency-key", "linux-native-export", "--wait", "4m")
	if exportOutput.CustodyExport == nil || exportOutput.CustodyExport.Status != "available" || exportOutput.CustodyExport.ManifestDigest == "" {
		t.Fatalf("Computer custody export result = %#v", exportOutput)
	}
	manifestPath := filepath.Join(exportDirectory, "custody.json")
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	manifestHash := sha256.Sum256(manifestBytes)
	manifestDigest := "sha256:" + hex.EncodeToString(manifestHash[:])
	if manifestDigest != exportOutput.CustodyExport.ManifestDigest {
		t.Fatalf("custody manifest digest = %s, want %s", manifestDigest, exportOutput.CustodyExport.ManifestDigest)
	}
	importOutput := runComputerCLI[storageCLIMutationReceipt](t, harness, false, "services", "custody", "import", exportOutput.CustodyExport.ExportID,
		"--name", "linux-native-import", "--disk-bytes", fmt.Sprint(160<<20), "--node", "acceptance-node",
		"--path", exportDirectory, "--manifest", manifestPath, "--manifest-digest", manifestDigest,
		"--idempotency-key", "linux-native-import", "--wait", "4m")
	if importOutput.Computer == nil || importOutput.StorageProvenance == nil || !importOutput.StorageProvenance.CustodyTainted {
		t.Fatalf("Computer custody import result = %#v", importOutput)
	}
	importReceipt := requiredStorageCopyReceipt(t, importOutput.StorageProvenance, "import", importOutput.Computer.ComputerID)
	if importReceipt.MachineIDBeforeDigest == importReceipt.MachineIDAfterDigest || !importReceipt.SourceUnchanged ||
		!importReceipt.DestinationPrepared || importReceipt.PreparationReceipt || importReceipt.DestinationChown {
		t.Fatalf("Computer import receipt = %#v", importReceipt)
	}
	recordComputerAuthority(receipt, *importOutput.Computer)
	if err := os.RemoveAll(exportDirectory); err != nil {
		t.Fatal(err)
	}
	attestOutput := runComputerCLI[storageCLIMutationReceipt](t, harness, false, "services", "custody", "attest",
		exportOutput.CustodyExport.ExportID, "--idempotency-key", "linux-native-export-attested")
	if attestOutput.CustodyExport == nil || !attestOutput.CustodyExport.OperatorAttestedDeleted {
		t.Fatalf("Computer custody attestation result = %#v", attestOutput)
	}
	staleCredential := startTakeoverViewCLI(t, evidence, harness, reimaged.ComputerID, "linux-admin", "linux-admin-device-a", takeoverViewRetryNone)
	stopping := runComputerCLI[l1.Computer](t, harness, false, "services", "stop", reimaged.ComputerID, "--expect-current")
	stopped := waitForComputerCLI(t, harness, stopping.ComputerID, 3*time.Minute, computerStoppedAfterExplicitStop)
	restoreStoppedDetached := computerStoppedAndDetached(stopped)
	restoreBaseline := stopped.StorageGeneration
	restoreOutput := runComputerCLI[storageCLIMutationReceipt](t, harness, false, "services", "restore", stopped.ComputerID, backup.BackupID,
		"--keep-old-as-backup", "--expect-current", "--idempotency-key", "linux-native-restore", "--wait", "4m")
	if restoreOutput.Computer == nil || restoreOutput.Computer.StorageGeneration != restoreBaseline+1 {
		t.Fatalf("Computer restore result = %#v", restoreOutput)
	}
	restoreReceipt := requiredStorageCopyReceipt(t, restoreOutput.StorageProvenance, "restore", restoreOutput.Computer.ComputerID)
	if restoreReceipt.OSIdentityRekeyed || restoreReceipt.MachineIDBeforeDigest != "" || restoreReceipt.MachineIDAfterDigest != "" ||
		!restoreReceipt.SourceUnchanged || !restoreReceipt.DestinationPrepared || restoreReceipt.PreparationReceipt || restoreReceipt.DestinationChown {
		t.Fatalf("Computer restore receipt = %#v", restoreReceipt)
	}
	staleCredential.waitClosed(t, 30*time.Second)
	preRestoreSessionClosed := true
	reimaged = *restoreOutput.Computer
	backupInventory := runComputerCLI[computerBackupInventoryReceipt](t, harness, false, "services", "backup", "list", reimaged.ComputerID)
	capSet := runComputerCLI[storageCLIMutationReceipt](t, harness, false, "services", "backup", "set-cap", reimaged.ComputerID,
		"--cap", fmt.Sprint(len(backupInventory.Backups)), "--expect-current")
	capPressure := ""
	if !mutatingLinuxComputerRow("linux.storage_provenance") {
		capPressure = runComputerCLIExpectError(t, harness, "", "", "services", "backup", "create", reimaged.ComputerID,
			"--expect-current", "--allow-power-off", "--idempotency-key", "linux-native-cap-pressure", "--wait", "4m")
	}
	enospc := exerciseLiveComputerENOSPC(t, harness, reference, digest)
	reimaged = runComputerCLI[l1.Computer](t, harness, false, "services", "start", reimaged.ComputerID, "--expect-current")
	reimaged = waitForComputerCLI(t, harness, reimaged.ComputerID, 3*time.Minute, computerDisplayPublished)
	restoreOldEndpoint, restoreCurrentEndpoint := retargetTakeoverSessionCapability(t, staleCredential.tokenFile, reimaged.DisplayEndpoint)
	preStopSessionRejectedAfterRestore := runComputerCLIPersonExpectControlError(t, harness, "linux-admin", "linux-admin-device-a",
		"services", "takeover", "take", reimaged.ComputerID, "--session-token-file", staleCredential.tokenFile)
	recordComputerAuthority(receipt, reimaged)
	storageAssertions := map[string]bool{
		"cold_backup_available_live":                 backup.Status == "available",
		"clone_fork_created_live":                    cloneOutput.Computer.ComputerID != reimaged.ComputerID,
		"clone_machine_id_rekeyed_live":              cloneReceipt.MachineIDBeforeDigest != cloneReceipt.MachineIDAfterDigest,
		"clone_source_unchanged_live":                cloneReceipt.SourceUnchanged,
		"clone_first_attach_nonfresh_live":           cloneReceipt.DestinationPrepared && !cloneReceipt.PreparationReceipt && !cloneReceipt.DestinationChown && computerDisplayPublished(*cloneOutput.Computer),
		"custody_export_manifest_bound_live":         manifestDigest == exportOutput.CustodyExport.ManifestDigest,
		"custody_import_tainted_live":                importOutput.StorageProvenance.CustodyTainted,
		"import_machine_id_rekeyed_live":             importReceipt.MachineIDBeforeDigest != importReceipt.MachineIDAfterDigest,
		"import_source_unchanged_live":               importReceipt.SourceUnchanged,
		"custody_delete_attested_live":               attestOutput.CustodyExport.OperatorAttestedDeleted,
		"keep_old_restore_live":                      len(backupInventory.Backups) >= 2,
		"restore_fresh_generation_live":              restoreOutput.Computer.StorageGeneration == restoreBaseline+1,
		"restore_stopped_detached_live":              restoreStoppedDetached,
		"restore_preserved_machine_id_live":          !restoreReceipt.OSIdentityRekeyed && restoreReceipt.MachineIDBeforeDigest == "" && restoreReceipt.MachineIDAfterDigest == "",
		"restore_first_attach_nonfresh_live":         restoreReceipt.DestinationPrepared && !restoreReceipt.PreparationReceipt && !restoreReceipt.DestinationChown && computerDisplayPublished(reimaged),
		"prestop_session_closed_before_restore_live": preRestoreSessionClosed,
		"prestop_session_lineage_rejected_after_restore_live": preStopSessionRejectedAfterRestore.Error.Code == contract.ErrorTakeoverSessionEnded &&
			preStopSessionRejectedAfterRestore.Receipt != nil && preStopSessionRejectedAfterRestore.Receipt.SessionEndReason == string(l1.ComputerTakeoverAttemptAuthorityLost),
		"backup_cap_pressure_live": capSet.Computer != nil && strings.Contains(capPressure, string(contract.ErrorConflict)),
		"real_disk_enospc_live":    enospc.Observed,
	}
	storageEvidence := map[string]string{"backup_id": backup.BackupID, "clone_computer_id": cloneOutput.Computer.ComputerID,
		"clone_machine_id_before": cloneReceipt.MachineIDBeforeDigest, "clone_machine_id_after": cloneReceipt.MachineIDAfterDigest,
		"export_id": exportOutput.CustodyExport.ExportID, "import_computer_id": importOutput.Computer.ComputerID,
		"retained_backups": fmt.Sprint(len(backupInventory.Backups)), "enospc_observation": enospc.Detail,
		"restore_old_endpoint": restoreOldEndpoint, "restore_current_endpoint": restoreCurrentEndpoint,
		"restore_session_revocation_evidence": "unavailable: restore publishes no receipt-derived prior takeover-session revocation fact"}
	notRunLinuxComputerRow(t, receipt, "linux.storage_provenance", 286,
		"restore publishes no receipt-derived prior takeover-session revocation fact; the stopped-and-detached prerequisite already closes the pre-stop session, and its post-restore capability rejection reports attempt_authority_lost rather than a restore-specific terminal reason",
		storageAssertions, storageEvidence)

	receipt.begin("linux.guest_authority")
	defaultOff := !reimaged.SubmitEnabled && reimaged.SubmitMaxInflight == l1.DefaultComputerSubmitMaxInflight
	submission := runComputerCLI[l1.ComputerSubmissionMutationResult](t, harness, true, "services", "submission", "enable", reimaged.ComputerID,
		"--expect-current", "--idempotency-key", "linux-native-submission-enable")
	if !submission.MutationApplied || !submission.SubmitEnabled {
		t.Fatalf("Computer submission enable = %#v", submission)
	}
	selfResult := waitForLiveComputerHTTP(t, reimaged, http.MethodGet, "/v1/computer/self", "", nil, 30*time.Second)
	var self l3.ComputerSelf
	if selfResult.Status != http.StatusOK || json.Unmarshal([]byte(selfResult.Body), &self) != nil {
		t.Fatalf("live Computer self status=%d body=%s", selfResult.Status, selfResult.Body)
	}
	limitOne := runComputerCLI[l1.ComputerSubmissionMutationResult](t, harness, true, "services", "submission", "set-inflight", reimaged.ComputerID,
		"--max-inflight", "1", "--expect-current", "--idempotency-key", "linux-native-submission-inflight-one")
	_ = waitForLiveComputerHTTP(t, reimaged, http.MethodGet, "/v1/computer/self", "", nil, 30*time.Second)
	inflight := runComputerCLI[l1.ComputerSubmissionMutationResult](t, harness, true, "services", "submission", "set-inflight", reimaged.ComputerID,
		"--max-inflight", "20", "--expect-current", "--idempotency-key", "linux-native-submission-inflight")
	_ = waitForLiveComputerHTTP(t, reimaged, http.MethodGet, "/v1/computer/self", "", nil, 30*time.Second)
	accepted := make([]l3.RunAccepted, 0, 20)
	for index := 0; index < 20; index++ {
		result := runLiveComputerHTTP(t, reimaged, http.MethodPost, "/v1/runs", fmt.Sprintf("linux-native-root-%02d", index), liveComputerRunRequest(300*time.Second))
		if result.Status != http.StatusCreated {
			t.Fatalf("live Computer root %d status=%d body=%s", index, result.Status, result.Body)
		}
		var run l3.RunAccepted
		if err := json.Unmarshal([]byte(result.Body), &run); err != nil || run.RunID == "" {
			t.Fatalf("decode live Computer root %d: %v body=%s", index, err, result.Body)
		}
		accepted = append(accepted, run)
	}
	rootResult := runLiveComputerHTTP(t, reimaged, http.MethodGet, "/v1/runs/"+accepted[0].RunID, "", nil)
	var root contract.RunRecord
	if rootResult.Status != http.StatusOK || json.Unmarshal([]byte(rootResult.Body), &root) != nil {
		t.Fatalf("live Computer root projection status=%d body=%s", rootResult.Status, rootResult.Body)
	}
	listResult := runLiveComputerHTTP(t, reimaged, http.MethodGet, "/v1/runs?origin=computer:self&limit=100", "", nil)
	var selfRuns l3.ComputerRunPage
	if listResult.Status != http.StatusOK || json.Unmarshal([]byte(listResult.Body), &selfRuns) != nil {
		t.Fatalf("live Computer self-scoped list status=%d body=%s", listResult.Status, listResult.Body)
	}
	forbidden := liveComputerHTTPResult{}
	if !mutatingLinuxComputerRow("linux.guest_authority") {
		forbidden = runLiveComputerHTTP(t, reimaged, http.MethodPost, "/v1/runs/"+accepted[0].RunID+"/cancel", "linux-native-forbidden", map[string]any{})
	}
	limited := runLiveComputerHTTP(t, reimaged, http.MethodPost, "/v1/runs", "linux-native-root-over-limit", liveComputerRunRequest(300*time.Second))
	var limitedError contract.ErrorResponse
	_ = json.Unmarshal([]byte(limited.Body), &limitedError)
	paused := startLiveComputerPausedSubmission(t, reimaged, "linux-native-revocation-race", liveComputerRunRequest(300*time.Second))
	disabled := runComputerCLI[l1.ComputerSubmissionMutationResult](t, harness, true, "services", "submission", "disable", reimaged.ComputerID,
		"--expect-current", "--idempotency-key", "linux-native-submission-disable")
	pausedStatus := paused.finish(t)
	guestAssertions := map[string]bool{
		"live_default_off":                    defaultOff,
		"live_submission_enabled":             submission.SubmitEnabled && submission.Revoked != nil,
		"live_self_scope_exact":               self.ComputerID == reimaged.ComputerID && self.ComputerStorageGeneration == reimaged.StorageGeneration && len(self.Permissions) == 2,
		"live_root_run_provenance":            root.Trigger.Type == "computer" && root.Trigger.ComputerID == reimaged.ComputerID && root.Trigger.ComputerStorageGeneration == reimaged.StorageGeneration,
		"live_self_scoped_list":               len(selfRuns.Runs) == 20,
		"live_forbidden_route_rejected":       forbidden.Status == http.StatusForbidden,
		"live_one_inflight_policy_transition": limitOne.SubmitMaxInflight == 1 && limitOne.Revoked != nil,
		"live_exact_inflight_policy_set":      inflight.SubmitMaxInflight == 20 && inflight.Revoked != nil,
		"live_twenty_inflight_boundary":       limited.Status == http.StatusConflict && limitedError.Error.Code == contract.ErrorSubmitInflightLimit,
		"live_submission_revoked":             !disabled.SubmitEnabled && disabled.Revoked != nil,
		"live_revocation_revision_advanced":   disabled.SubmitIntentRevision > submission.SubmitIntentRevision,
		"live_revocation_race_closed":         pausedStatus == http.StatusUnauthorized || pausedStatus == 0,
	}
	guestEvidence := map[string]string{"policy_revision": fmt.Sprint(submission.PolicyRevision),
		"submit_intent_revision": fmt.Sprint(submission.SubmitIntentRevision),
		"root_run_id":            accepted[0].RunID,
		"blocked_assertion":      "candidate-bound complete M3 OCI matrix root Run execution result"}
	if mutatingLinuxComputerRow("linux.guest_authority") {
		if err := receipt.pass("linux.guest_authority", guestAssertions, guestEvidence); err == nil {
			t.Fatal("guest-authority lane mutation did not fail")
		}
	} else if err := receipt.notRun("linux.guest_authority", 157,
		"the complete M3 OCI matrix does not yet publish the single candidate-bound root Run execution result required to join this live Computer authority proof",
		guestAssertions, guestEvidence); err != nil {
		t.Fatal(err)
	}

	receipt.begin("linux.removal")
	archiveBefore := sha256File(t, archive)
	cacheBefore := liveContainerdImagePresent(t, digest)
	taintedComputerID := importOutput.Computer.ComputerID
	reduced := removeAndWaitComputer(t, harness, *importOutput.Computer, 4*time.Minute)
	for _, target := range []l1.Computer{reimaged, *cloneOutput.Computer} {
		_ = removeAndWaitComputer(t, harness, target, 4*time.Minute)
	}
	verifiedComputer := createReadyComputer(t, harness, reference, digest, "linux-native-verified-removal", "linux-native-verified-removal-create")
	recordComputerAuthority(receipt, verifiedComputer)
	removalCommandIntact := true
	if mutatingLinuxComputerRow("linux.removal") {
		failedRemoval := runComputerCLIExpectError(t, harness, "", "", "services", "remove", verifiedComputer.ComputerID,
			"--intent-revision", fmt.Sprint(verifiedComputer.IntentRevision+99), "--storage-id", verifiedComputer.StorageID,
			"--storage-generation", fmt.Sprint(verifiedComputer.StorageGeneration), "--idempotency-key", "linux-native-removal-mutation")
		removalCommandIntact = !strings.Contains(failedRemoval, string(contract.ErrorStaleIntentRevision))
	}
	verified := removeAndWaitComputer(t, harness, verifiedComputer, 4*time.Minute)
	if verified.RemovalOutcome != "removed_verified" || reduced.RemovalOutcome != "removed_reduced" {
		t.Fatalf("Computer removal outcomes verified=%q reduced=%q", verified.RemovalOutcome, reduced.RemovalOutcome)
	}
	harness.agent.kill(t)
	verification := inspectHelperNamespaceInventory(t, helperSocket, helperChecksum)
	receipt.ResidueInventories["post_removal_observed_inventory"] = verification.Inventory
	receipt.ResidueInventories["post_removal_runtime_residue"] = verification.RuntimeResidue
	receipt.ResidueInventories["post_removal_durable_retained"] = verification.DurableRetained
	receipt.ResidueAssertions["post_removal_observed_inventory_empty"] = ocihelper.InventoryEmpty(verification.Inventory)
	receipt.ResidueAssertions["post_removal_runtime_residue_empty"] = ocihelper.InventoryEmpty(verification.RuntimeResidue)
	receipt.ResidueAssertions["post_removal_durable_retained_matches_expected_empty_custody"] = ocihelper.InventoryEmpty(verification.DurableRetained)
	archiveAfter := sha256File(t, archive)
	cacheAfter := liveContainerdImagePresent(t, digest)
	completeLinuxComputerRow(t, receipt, "linux.removal", map[string]bool{
		"verified_absence_outcome_live":      verified.RemovalOutcome == "removed_verified",
		"reduced_custody_outcome_live":       reduced.RemovalOutcome == "removed_reduced",
		"reduced_bound_to_tainted_computer":  reduced.ComputerID == taintedComputerID,
		"independent_helper_inventory_empty": ocihelper.InventoryEmpty(verification.Inventory),
		"containers_absent":                  len(verification.Inventory.Containers) == 0,
		"tasks_absent":                       len(verification.Inventory.Tasks) == 0,
		"disks_loops_mounts_absent":          len(verification.Inventory.ComputerDiskImages)+len(verification.Inventory.ComputerDiskLoops)+len(verification.Inventory.ComputerDiskMounts) == 0,
		"logs_and_control_absent":            len(verification.Inventory.LogSegments)+len(verification.Inventory.Cgroups) == 0,
		"durable_retained_matches_custody":   ocihelper.InventoryEmpty(verification.DurableRetained),
		"publication_withdrawn":              verified.DisplayEndpoint == nil,
		"operator_bind_source_untouched":     archiveBefore == archiveAfter,
		"shared_image_cache_untouched":       cacheBefore && cacheAfter,
		"removal_command_intact":             removalCommandIntact,
	}, map[string]string{"verified_computer_id": verified.ComputerID, "reduced_computer_id": reduced.ComputerID,
		"custody_tainted_computer_id": taintedComputerID, "verified_outcome": verified.RemovalOutcome,
		"reduced_outcome": reduced.RemovalOutcome, "inventory_source": "helper VerifyNamespace route"})
	validateLinuxComputerLaneMutation(t, receipt)
}

type publishedComputerRuntimeReceipt struct {
	Digest string `json:"digest"`
}

func mergeAcceptanceInventory(left, right ocihelper.ResourceInventory) ocihelper.ResourceInventory {
	return ocihelper.ResourceInventory{
		Leases: append(left.Leases, right.Leases...), Snapshots: append(left.Snapshots, right.Snapshots...),
		Containers: append(left.Containers, right.Containers...), Tasks: append(left.Tasks, right.Tasks...),
		Shims: append(left.Shims, right.Shims...), Cgroups: append(left.Cgroups, right.Cgroups...),
		LogSegments: append(left.LogSegments, right.LogSegments...), ManagedVolumes: append(left.ManagedVolumes, right.ManagedVolumes...),
		ManagedVolumeRecords:    append(left.ManagedVolumeRecords, right.ManagedVolumeRecords...),
		ComputerDiskImages:      append(left.ComputerDiskImages, right.ComputerDiskImages...),
		ComputerDiskAllocations: append(left.ComputerDiskAllocations, right.ComputerDiskAllocations...),
		ComputerDiskQuotas:      append(left.ComputerDiskQuotas, right.ComputerDiskQuotas...),
		ComputerDiskManifests:   append(left.ComputerDiskManifests, right.ComputerDiskManifests...),
		ComputerDiskMounts:      append(left.ComputerDiskMounts, right.ComputerDiskMounts...),
		ComputerDiskLoops:       append(left.ComputerDiskLoops, right.ComputerDiskLoops...),
		ComputerAttachments:     append(left.ComputerAttachments, right.ComputerAttachments...),
		ComputerResetManifests:  append(left.ComputerResetManifests, right.ComputerResetManifests...),
		ComputerQuarantines:     append(left.ComputerQuarantines, right.ComputerQuarantines...),
		ComputerDiskAnomalies:   append(left.ComputerDiskAnomalies, right.ComputerDiskAnomalies...),
	}
}

func readPublishedComputerRuntimeReceipt(t *testing.T, path string) publishedComputerRuntimeReceipt {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var receipt publishedComputerRuntimeReceipt
	if err := json.Unmarshal(payload, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Digest == "" {
		t.Fatal("published Computer runtime receipt omitted its platform digest")
	}
	return receipt
}

type storageCLIMutationReceipt struct {
	MutationApplied   bool                          `json:"mutation_applied"`
	Computer          *l1.Computer                  `json:"computer,omitempty"`
	Backups           *l1.BackupList                `json:"backups,omitempty"`
	CustodyExport     *l1.ComputerCustodyExport     `json:"custody_export,omitempty"`
	StorageProvenance *l1.ComputerStorageProvenance `json:"storage_provenance,omitempty"`
}

func requiredStorageCopyReceipt(t *testing.T, projection *l1.ComputerStorageProvenance, kind, destinationComputerID string) contract.ComputerStorageCopyReceipt {
	t.Helper()
	if projection != nil {
		for _, provenance := range projection.Provenance {
			if provenance.Kind == kind && provenance.DestinationComputerID == destinationComputerID && provenance.CopyReceipt != nil {
				return *provenance.CopyReceipt
			}
		}
	}
	t.Fatalf("%s provenance omitted copy receipt for %s: %#v", kind, destinationComputerID, projection)
	return contract.ComputerStorageCopyReceipt{}
}

type computerBackupInventoryReceipt struct {
	Backups []l1.Backup `json:"backups"`
}

func requiredComputerRealtimeEnvironment(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("Linux Computer acceptance requires %s", name)
	}
	return value
}

func completeLinuxComputerRow(t *testing.T, receipt *linuxComputerMatrixReceipt, id string, assertions map[string]bool, evidence map[string]string) {
	t.Helper()
	mutated := mutatingLinuxComputerRow(id)
	err := receipt.pass(id, assertions, evidence)
	if mutated {
		if err == nil || receipt.Rows[id].Status != "FAIL" {
			t.Fatalf("lane mutation %s did not fail its owning row", id)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
}

func notRunLinuxComputerRow(t *testing.T, receipt *linuxComputerMatrixReceipt, id string, issue int, reason string, assertions map[string]bool, evidence map[string]string) {
	t.Helper()
	mutated := mutatingLinuxComputerRow(id)
	err := receipt.notRun(id, issue, reason, assertions, evidence)
	if mutated {
		if err == nil || receipt.Rows[id].Status != "FAIL" {
			t.Fatalf("lane mutation %s did not fail its owning row", id)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
}

func mutatingLinuxComputerRow(id string) bool {
	return os.Getenv("WEFTY_LINUX_COMPUTER_MUTATE_ROW") == id
}

func validateLinuxComputerLaneMutation(t *testing.T, receipt *linuxComputerMatrixReceipt) {
	t.Helper()
	mutated := os.Getenv("WEFTY_LINUX_COMPUTER_MUTATE_ROW")
	if mutated == "" {
		return
	}
	failures := 0
	for id, row := range receipt.Rows {
		if row.Status == "FAIL" {
			failures++
			if id != mutated {
				t.Fatalf("lane mutation %s also failed row %s", mutated, id)
			}
		}
	}
	if failures != 1 {
		t.Fatalf("lane mutation %s failed %d rows, want exactly one", mutated, failures)
	}
}

func runComputerCLIPerson[T any](t *testing.T, harness *acceptanceHarness, userID, deviceID string, arguments ...string) T {
	t.Helper()
	return runComputerCLIWithIdentity[T](t, harness, userID, deviceID, arguments...)
}

type computerCLIEvidence struct {
	Arguments []string `json:"arguments"`
	Stdout    string   `json:"stdout"`
	Stderr    string   `json:"stderr"`
	ExitCode  int      `json:"exit_code"`
}

func runComputerCLIPersonWithEvidence[T any](t *testing.T, evidence *realTimingEvidence, evidenceName string, harness *acceptanceHarness, userID, deviceID string, arguments ...string) T {
	t.Helper()
	return runComputerCLIWithIdentityObserved[T](t, harness, userID, deviceID, func(observation computerCLIEvidence) {
		evidence.recordJSON(evidenceName, observation)
	}, arguments...)
}

func runComputerCLIWithIdentity[T any](t *testing.T, harness *acceptanceHarness, userID, deviceID string, arguments ...string) T {
	t.Helper()
	return runComputerCLIWithIdentityObserved[T](t, harness, userID, deviceID, nil, arguments...)
}

func runComputerCLIWithIdentityObserved[T any](t *testing.T, harness *acceptanceHarness, userID, deviceID string, observe func(computerCLIEvidence), arguments ...string) T {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, weftyBinaryPath, computerCLIArguments(harness, userID, deviceID, arguments...)...)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	if observe != nil {
		exitCode := 0
		if err != nil {
			exitCode = -1
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				exitCode = exitErr.ExitCode()
			}
		}
		observe(computerCLIEvidence{Arguments: append([]string(nil), arguments...), Stdout: stdout.String(), Stderr: stderr.String(), ExitCode: exitCode})
	}
	if err != nil {
		t.Fatalf("run Computer CLI %v: %v\nstdout:\n%s\nstderr:\n%s", arguments, err, stdout.String(), stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("run Computer CLI %v emitted unexpected stderr:\n%s", arguments, stderr.String())
	}
	var result T
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode Computer CLI %v: %v\n%s", arguments, err, stdout.String())
	}
	return result
}

func computerCLIArguments(harness *acceptanceHarness, userID, deviceID string, arguments ...string) []string {
	args := []string{"--fabric=plain", "--l1=" + harness.controlPlaneAddress, "--plain-identity=linux-computer-cli", "--json"}
	if harness.runLedgerAddress != "" {
		args = append(args, "--l3="+harness.runLedgerAddress)
	}
	if userID != "" || deviceID != "" {
		args = append(args, "--plain-user-id="+userID, "--plain-device-id="+deviceID)
	}
	return append(args, arguments...)
}

func runComputerCLIExpectError(t *testing.T, harness *acceptanceHarness, userID, deviceID string, arguments ...string) string {
	t.Helper()
	return runComputerCLIPersonExpectError(t, harness, userID, deviceID, arguments...)
}

func runComputerCLIPersonExpectError(t *testing.T, harness *acceptanceHarness, userID, deviceID string, arguments ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, weftyBinaryPath, computerCLIArguments(harness, userID, deviceID, arguments...)...)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("Computer CLI %v unexpectedly succeeded:\n%s", arguments, output)
	}
	return string(output)
}

func runComputerCLIPersonExpectControlError(
	t *testing.T,
	harness *acceptanceHarness,
	userID, deviceID string,
	arguments ...string,
) contract.ComputerControlErrorResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, weftyBinaryPath, computerCLIArguments(harness, userID, deviceID, arguments...)...)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("Computer CLI %v unexpectedly succeeded:\n%s", arguments, output)
	}
	var response contract.ComputerControlErrorResponse
	if decodeErr := json.Unmarshal(output, &response); decodeErr != nil || response.Error.Code == "" {
		t.Fatalf("decode Computer control error %v: %v\n%s", arguments, decodeErr, output)
	}
	return response
}

func retargetTakeoverSessionCapability(t *testing.T, path string, endpoint *string) (string, string) {
	t.Helper()
	if endpoint == nil || strings.TrimSpace(*endpoint) == "" {
		t.Fatal("Computer restart omitted its current display endpoint")
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read old Computer session capability: %v", err)
	}
	var capability struct {
		Endpoint string `json:"endpoint"`
		Token    string `json:"token"`
	}
	if err := json.Unmarshal(payload, &capability); err != nil || strings.TrimSpace(capability.Endpoint) == "" || strings.TrimSpace(capability.Token) == "" {
		t.Fatalf("decode old Computer session capability: %v", err)
	}
	oldEndpoint := capability.Endpoint
	capability.Endpoint = *endpoint
	updated, err := json.Marshal(capability)
	if err != nil {
		t.Fatalf("encode retargeted Computer session capability: %v", err)
	}
	updated = append(updated, '\n')
	if err := os.WriteFile(path, updated, 0o600); err != nil {
		t.Fatalf("retarget old Computer session capability: %v", err)
	}
	return oldEndpoint, capability.Endpoint
}

func TestRetargetTakeoverSessionCapabilityPreservesOldBearer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.json")
	if err := os.WriteFile(path, []byte(`{"endpoint":"ws://127.0.0.1:10001/wefty/computer/v1","token":"old-node-lineage-bearer"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	current := "ws://127.0.0.1:10002/wefty/computer/v1"
	old, updated := retargetTakeoverSessionCapability(t, path, &current)
	var capability struct {
		Endpoint string `json:"endpoint"`
		Token    string `json:"token"`
	}
	payload, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(payload, &capability) != nil || old != "ws://127.0.0.1:10001/wefty/computer/v1" ||
		updated != current || capability.Endpoint != current || capability.Token != "old-node-lineage-bearer" {
		t.Fatalf("retargeted capability = old=%q updated=%q capability=%+v err=%v", old, updated, capability, err)
	}
}

type takeoverViewProcess struct {
	tokenFile       string
	admitted        bool
	cancel          context.CancelFunc
	done            chan error
	stdout          bytes.Buffer
	stderr          lockedBuffer
	toleratedStderr []string
	evidence        *realTimingEvidence
	evidencePrefix  string
	computerID      string
	userID          string
	deviceID        string
	retryMode       takeoverViewRetryMode
	activeAttempt   int
	stderrStart     int
	attemptStarted  time.Time
}

type takeoverViewAttemptEvidence struct {
	ComputerID       string    `json:"computer_id"`
	UserID           string    `json:"user_id"`
	DeviceID         string    `json:"device_id"`
	RetryMode        string    `json:"retry_mode"`
	Attempt          int       `json:"attempt"`
	AdmissionOutcome string    `json:"admission_outcome"`
	ExitOutcome      string    `json:"exit_outcome"`
	ExitCode         int       `json:"exit_code"`
	Error            string    `json:"error,omitempty"`
	Stderr           string    `json:"stderr"`
	StartedAt        time.Time `json:"started_at"`
	RecordedAt       time.Time `json:"recorded_at"`
}

type takeoverViewRetryMode string

const (
	takeoverViewRetryNone            takeoverViewRetryMode = "none"
	takeoverViewRetryStalePolicy     takeoverViewRetryMode = "stale_policy_revision"
	takeoverViewHarnessRetryAttempts                       = 3
	takeoverViewHarnessRetryWindow                         = 6 * time.Second
)

var takeoverViewEvidenceSequence atomic.Uint64

func startTakeoverViewCLI(t *testing.T, evidence *realTimingEvidence, harness *acceptanceHarness, computerID, userID, deviceID string, retryMode takeoverViewRetryMode) *takeoverViewProcess {
	t.Helper()
	view := &takeoverViewProcess{
		tokenFile: filepath.Join(t.TempDir(), "takeover-session.json"), done: make(chan error, 1), evidence: evidence,
		evidencePrefix: fmt.Sprintf("takeover-cli-%03d", takeoverViewEvidenceSequence.Add(1)),
		computerID:     computerID, userID: userID, deviceID: deviceID, retryMode: retryMode,
	}
	ctx, cancel := context.WithCancel(t.Context())
	view.cancel = cancel
	deadline := time.Now().Add(30 * time.Second)
	retryDeadline := time.Now().Add(takeoverViewHarnessRetryWindow)
	attempts := 0
	running := false
	for time.Now().Before(deadline) {
		attempts++
		stderrStart := view.stderr.Len()
		view.activeAttempt, view.stderrStart, view.attemptStarted = attempts, stderrStart, time.Now().UTC()
		command := exec.CommandContext(ctx, weftyBinaryPath, computerCLIArguments(harness, userID, deviceID,
			"services", "takeover", "view", computerID, "--session-token-file", view.tokenFile)...)
		command.Stdout, command.Stderr = &view.stdout, &view.stderr
		go func() { view.done <- command.Run() }()
		running = true
		for time.Now().Before(deadline) {
			if info, err := os.Stat(view.tokenFile); err == nil && info.Size() > 0 {
				view.admitted = true
				view.recordAttempt("success", "pending", nil)
				return view
			}
			select {
			case err := <-view.done:
				running = false
				attemptStderr := view.stderr.String()[stderrStart:]
				view.recordAttempt("failure", "failure", err)
				// L1 availability is durable discovery, while the front door is
				// live authority. A grant may be visible before the hosting agent
				// acknowledges and installs that policy revision.
				if retryMode == takeoverViewRetryStalePolicy && attempts < takeoverViewHarnessRetryAttempts && time.Now().Before(retryDeadline) &&
					strings.Contains(attemptStderr, string(contract.ErrorStalePolicyRevision)) {
					view.toleratedStderr = append(view.toleratedStderr, attemptStderr)
					time.Sleep(25 * time.Millisecond)
					break
				}
				cancel()
				t.Fatalf("takeover view exited before admission: %v\n%s", err, view.stderr.String())
			default:
				time.Sleep(25 * time.Millisecond)
				continue
			}
			break
		}
	}
	cancel()
	if running {
		select {
		case err := <-view.done:
			view.recordAttempt("failure", "failure", err)
		case <-time.After(10 * time.Second):
			view.recordAttempt("failure", "timeout", errors.New("takeover view did not stop after its admission deadline"))
			t.Fatal("takeover view did not stop after its admission deadline")
		}
	}
	t.Fatalf("takeover view did not publish its session capability: %s", view.stderr.String())
	return nil
}

func (view *takeoverViewProcess) stop(t *testing.T) {
	t.Helper()
	view.cancel()
	select {
	case err := <-view.done:
		view.recordAttempt("success", "closed", err)
	case <-time.After(10 * time.Second):
		view.recordAttempt("success", "timeout", errors.New("takeover view did not stop"))
		t.Fatal("takeover view did not stop")
	}
}

func (view *takeoverViewProcess) waitClosed(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case err := <-view.done:
		view.recordAttempt("success", "closed", err)
	case <-time.After(timeout):
		view.cancel()
		view.recordAttempt("success", "timeout", errors.New("takeover view remained open after authority revocation"))
		t.Fatal("takeover view remained open after authority revocation")
	}
}

func (view *takeoverViewProcess) recordAttempt(admissionOutcome, exitOutcome string, err error) {
	if view.evidence == nil || view.activeAttempt == 0 {
		return
	}
	exitCode := 0
	errorText := ""
	if exitOutcome == "pending" {
		exitCode = -1
	} else if err != nil {
		exitCode = -1
		errorText = err.Error()
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			exitCode = exitError.ExitCode()
		}
	}
	stderr := view.stderr.String()
	if view.stderrStart < len(stderr) {
		stderr = stderr[view.stderrStart:]
	} else {
		stderr = ""
	}
	view.evidence.recordJSON(fmt.Sprintf("%s-attempt-%02d.json", view.evidencePrefix, view.activeAttempt), takeoverViewAttemptEvidence{
		ComputerID: view.computerID, UserID: view.userID, DeviceID: view.deviceID, RetryMode: string(view.retryMode),
		Attempt: view.activeAttempt, AdmissionOutcome: admissionOutcome, ExitOutcome: exitOutcome, ExitCode: exitCode,
		Error: errorText, Stderr: stderr, StartedAt: view.attemptStarted, RecordedAt: time.Now().UTC(),
	})
}

func TestTakeoverViewAttemptEvidenceIncludesFailureExitAndStderr(t *testing.T) {
	directory := t.TempDir()
	evidence := &realTimingEvidence{t: t, directory: directory}
	view := &takeoverViewProcess{
		evidence: evidence, evidencePrefix: "takeover-cli-test", computerID: "computer-1", userID: "user-1", deviceID: "device-1",
		retryMode: takeoverViewRetryNone, activeAttempt: 1, attemptStarted: time.Now().UTC(),
	}
	_, _ = view.stderr.WriteString(`{"code":"control_not_authorized"}`)
	exitErr := exec.Command("/bin/sh", "-c", "exit 3").Run()
	view.recordAttempt("failure", "failure", exitErr)
	payload, err := os.ReadFile(filepath.Join(directory, "takeover-cli-test-attempt-01.json"))
	if err != nil {
		t.Fatal(err)
	}
	var recorded takeoverViewAttemptEvidence
	if err := json.Unmarshal(payload, &recorded); err != nil {
		t.Fatal(err)
	}
	if recorded.ExitCode != 3 || recorded.AdmissionOutcome != "failure" || !strings.Contains(recorded.Stderr, "control_not_authorized") {
		t.Fatalf("takeover CLI failure evidence = %+v", recorded)
	}
}

type liveDiskEvidence struct {
	Path           string
	BlocksBytes    int64
	FullyAllocated bool
}

func liveComputerDiskName(t *testing.T, computer l1.Computer) string {
	t.Helper()
	name, err := ocihelper.DeterministicComputerDiskName(ocihelper.ComputerStorageReference{
		ComputerID: computer.ComputerID, StorageID: computer.StorageID, StorageGeneration: computer.StorageGeneration,
	})
	if err != nil {
		t.Fatal(err)
	}
	return name
}

func inspectLiveComputerDisk(t *testing.T, computer l1.Computer) liveDiskEvidence {
	t.Helper()
	path := filepath.Join("/var/lib/wefty/oci/computer-disks", liveComputerDiskName(t, computer), "disk.ext4")
	output, err := exec.Command("sudo", "stat", "--format=%s %b", path).CombinedOutput()
	if err != nil {
		t.Fatalf("inspect live Computer disk: %v\n%s", err, output)
	}
	fields := strings.Fields(string(output))
	if len(fields) != 2 {
		t.Fatalf("unexpected disk stat %q", output)
	}
	size, sizeErr := strconv.ParseInt(fields[0], 10, 64)
	blocks, blocksErr := strconv.ParseInt(fields[1], 10, 64)
	if sizeErr != nil || blocksErr != nil {
		t.Fatalf("parse disk stat %q: %v %v", output, sizeErr, blocksErr)
	}
	return liveDiskEvidence{Path: path, BlocksBytes: blocks * 512, FullyAllocated: size == computer.DesiredDiskBytes && blocks*512 >= size}
}

func plantLiveProfileMarker(t *testing.T, computer l1.Computer, marker string) string {
	t.Helper()
	path := filepath.Join("/var/lib/wefty/oci/computer-mounts", liveComputerDiskName(t, computer), marker)
	if output, err := exec.Command("sudo", "touch", path).CombinedOutput(); err != nil {
		t.Fatalf("plant live profile marker: %v\n%s", err, output)
	}
	return path
}

func assertLiveProfileMarker(t *testing.T, computer l1.Computer, priorPath string) {
	t.Helper()
	path := filepath.Join("/var/lib/wefty/oci/computer-mounts", liveComputerDiskName(t, computer), filepath.Base(priorPath))
	if output, err := exec.Command("sudo", "test", "-f", path).CombinedOutput(); err != nil {
		t.Fatalf("profile marker did not survive at %s: %v\n%s", path, err, output)
	}
}

func triggerLinuxComputerFault(t *testing.T, harness *acceptanceHarness, action string) {
	t.Helper()
	fifo := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_FAULT_FIFO")
	directory := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_FAULT_DIR")
	done := filepath.Join(directory, action+".done")
	_ = os.Remove(done)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "sh", "-c", `printf '%s\n' "$1" > "$2"`, "wefty-fault", action, fifo)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("trigger fault %s: %v\n%s", action, err, output)
	}
	for ctx.Err() == nil {
		if _, err := os.Stat(done); err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("fault %s did not complete", action)
}

type liveRFBSession struct {
	endpoint   string
	token      string
	websocket  *websocket.Conn
	connection net.Conn
}

func openLiveRFBSession(t *testing.T, endpoint, userID, deviceID string) *liveRFBSession {
	t.Helper()
	plainNetwork, err := plain.NewNetworkWithID("plain-linux-computer-acceptance")
	if err != nil {
		t.Fatal(err)
	}
	participant := plainNetwork.NewFabric(fabric.Identity{NodeID: deviceID, UserID: userID, DeviceID: deviceID})
	transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		return participant.Dial(ctx, network, address)
	}}
	connection, response, err := websocket.Dial(t.Context(), endpoint, &websocket.DialOptions{
		HTTPClient: &http.Client{Transport: transport}, Subprotocols: []string{contract.ComputerDisplayWebSocketSubprotocol},
	})
	transport.CloseIdleConnections()
	if err != nil {
		t.Fatalf("open live RFB edge for %s: %v", userID, err)
	}
	token := response.Header.Get(contract.ComputerControlTokenHeader)
	network := websocket.NetConn(t.Context(), connection, websocket.MessageBinary)
	if token == "" {
		_ = connection.CloseNow()
		t.Fatal("live RFB session omitted its control capability")
	}
	session := &liveRFBSession{endpoint: endpoint, token: token, websocket: connection, connection: network}
	session.negotiate(t)
	return session
}

func (session *liveRFBSession) negotiate(t *testing.T) {
	t.Helper()
	banner := make([]byte, contract.ComputerRFBVersionBannerBytes)
	if _, err := io.ReadFull(session.connection, banner); err != nil || !contract.ValidComputerRFBVersionBanner(banner) {
		_ = session.websocket.CloseNow()
		t.Fatalf("read live RFB banner: %v %q", err, banner)
	}
	if _, err := session.connection.Write([]byte("RFB 003.008\n")); err != nil {
		t.Fatal(err)
	}
	count := []byte{0}
	if _, err := io.ReadFull(session.connection, count); err != nil || count[0] == 0 {
		t.Fatalf("read live RFB security count: %v", err)
	}
	securityTypes := make([]byte, int(count[0]))
	if _, err := io.ReadFull(session.connection, securityTypes); err != nil || !bytes.Contains(securityTypes, []byte{1}) {
		t.Fatalf("live RFB None security unavailable: %v %x", err, securityTypes)
	}
	if _, err := session.connection.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	securityResult := make([]byte, 4)
	if _, err := io.ReadFull(session.connection, securityResult); err != nil || !bytes.Equal(securityResult, []byte{0, 0, 0, 0}) {
		t.Fatalf("live RFB security result: %v %x", err, securityResult)
	}
	if _, err := session.connection.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	serverInit := make([]byte, 24)
	if _, err := io.ReadFull(session.connection, serverInit); err != nil {
		t.Fatal(err)
	}
	name := make([]byte, int(binary.BigEndian.Uint32(serverInit[20:24])))
	if _, err := io.ReadFull(session.connection, name); err != nil {
		t.Fatal(err)
	}
}

func (session *liveRFBSession) sendPointer(t *testing.T, x, y int) {
	t.Helper()
	for _, event := range [][]byte{
		{5, 1, byte(x >> 8), byte(x), byte(y >> 8), byte(y)},
		{5, 0, byte(x >> 8), byte(x), byte(y >> 8), byte(y)},
	} {
		if _, err := session.connection.Write(event); err != nil {
			t.Fatalf("send live RFB pointer: %v", err)
		}
	}
}

func (session *liveRFBSession) capabilityFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "live-rfb-session.json")
	payload, err := json.Marshal(map[string]string{"endpoint": session.endpoint, "token": session.token})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func (session *liveRFBSession) close() {
	_ = session.websocket.CloseNow()
}

type liveInputObservation struct {
	Version        int      `json:"version"`
	Generation     uint64   `json:"generation"`
	KeyEvents      uint64   `json:"key_events"`
	X              int      `json:"x"`
	Y              int      `json:"y"`
	PointerHistory [][2]int `json:"pointer_history"`
}

type liveComputerDriverObservation struct {
	Version      int  `json:"version"`
	HumanDriving bool `json:"human_driving"`
}

func readLiveComputerHumanDriving(t *testing.T, jobID string) bool {
	t.Helper()
	containerID := liveComputerContainerID(t, jobID)
	containerdAddress := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_CONTAINERD_ADDRESS")
	execID := fmt.Sprintf("driver-oracle-%d", time.Now().UnixNano())
	payload, err := exec.Command("sudo", "/usr/local/bin/ctr", "--address", containerdAddress, "--namespace", ocihelper.ContainerdNamespace,
		"tasks", "exec", "--exec-id", execID, containerID, "/bin/cat", "/wefty/control/driver.json").CombinedOutput()
	if err != nil {
		t.Fatalf("read live Computer driver state: %v\n%s", err, payload)
	}
	var observation liveComputerDriverObservation
	if err := json.Unmarshal(payload, &observation); err != nil || observation.Version != 1 {
		t.Fatalf("decode live Computer driver state: %v\n%s", err, payload)
	}
	return observation.HumanDriving
}

func waitLiveComputerHumanDriving(t *testing.T, jobID string, want bool, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		observed := readLiveComputerHumanDriving(t, jobID)
		if observed == want || !time.Now().Before(deadline) {
			return observed
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func readLiveInputObservation(t *testing.T, jobID string) liveInputObservation {
	t.Helper()
	containerID := liveComputerContainerID(t, jobID)
	containerdAddress := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_CONTAINERD_ADDRESS")
	execID := fmt.Sprintf("input-oracle-%d", time.Now().UnixNano())
	payload, err := exec.Command("sudo", "/usr/local/bin/ctr", "--address", containerdAddress, "--namespace", ocihelper.ContainerdNamespace,
		"tasks", "exec", "--exec-id", execID, containerID, "/bin/cat", "/tmp/wefty-computer/input-oracle.json").CombinedOutput()
	if err != nil {
		t.Fatalf("read live Computer input oracle: %v\n%s", err, payload)
	}
	var observation liveInputObservation
	if err := json.Unmarshal(payload, &observation); err != nil || observation.Version != 1 {
		t.Fatalf("decode live Computer input oracle: %v\n%s", err, payload)
	}
	return observation
}

func liveComputerContainerID(t *testing.T, jobID string) string {
	t.Helper()
	containerdAddress := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_CONTAINERD_ADDRESS")
	list, err := exec.Command("sudo", "/usr/local/bin/ctr", "--address", containerdAddress, "--namespace", ocihelper.ContainerdNamespace,
		"containers", "list", "--quiet").CombinedOutput()
	if err != nil {
		t.Fatalf("list live Computer containers: %v\n%s", err, list)
	}
	containerID := ""
	for _, candidate := range strings.Fields(string(list)) {
		info, infoErr := exec.Command("sudo", "/usr/local/bin/ctr", "--address", containerdAddress, "--namespace", ocihelper.ContainerdNamespace,
			"containers", "info", candidate).CombinedOutput()
		if infoErr == nil && strings.Contains(string(info), jobID) {
			containerID = candidate
			break
		}
	}
	if containerID == "" {
		t.Fatalf("no live container carried job identity %s", jobID)
	}
	return containerID
}

type liveComputerHTTPResult struct {
	Status int    `json:"status"`
	Body   string `json:"body"`
}

const liveComputerHTTPPython = `
import json, os, sys, urllib.error, urllib.request
method, path, key, body = sys.argv[1:5]
payload = body.encode() if body else None
request = urllib.request.Request(os.environ["WEFTY_L3_ENDPOINT"] + path, data=payload, method=method)
request.add_header("Authorization", "Bearer " + open("/wefty/control/computer-token", encoding="utf-8").read().strip())
if payload is not None:
    request.add_header("Content-Type", "application/json")
if key:
    request.add_header("Idempotency-Key", key)
try:
    with urllib.request.urlopen(request, timeout=30) as response:
        status, response_body = response.status, response.read().decode()
except urllib.error.HTTPError as error:
    status, response_body = error.code, error.read().decode()
print(json.dumps({"status": status, "body": response_body}))
`

func liveComputerRunRequest(duration time.Duration) map[string]any {
	content := fmt.Sprintf("sleep %d\n", int(duration.Seconds()))
	digest := sha256.Sum256([]byte(content))
	return map[string]any{"inline_script": map[string]any{
		"content": content, "sha256": hex.EncodeToString(digest[:]), "interpreter": []string{"/bin/sh"},
	}, "params": map[string]any{}}
}

func tryLiveComputerHTTP(t *testing.T, computer l1.Computer, method, path, idempotencyKey string, body any) (liveComputerHTTPResult, error) {
	t.Helper()
	bodyJSON := ""
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return liveComputerHTTPResult{}, err
		}
		bodyJSON = string(payload)
	}
	containerdAddress := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_CONTAINERD_ADDRESS")
	containerID := liveComputerContainerID(t, computer.CurrentJobID)
	execID := fmt.Sprintf("computer-http-%d", time.Now().UnixNano())
	output, err := exec.Command("sudo", "/usr/local/bin/ctr", "--address", containerdAddress, "--namespace", ocihelper.ContainerdNamespace,
		"tasks", "exec", "--exec-id", execID, containerID, "/usr/bin/python3", "-c", liveComputerHTTPPython,
		method, path, idempotencyKey, bodyJSON).CombinedOutput()
	if err != nil {
		return liveComputerHTTPResult{}, fmt.Errorf("execute Computer HTTP probe: %w: %s", err, output)
	}
	var result liveComputerHTTPResult
	lines := bytes.Split(bytes.TrimSpace(output), []byte("\n"))
	if len(lines) == 0 || json.Unmarshal(lines[len(lines)-1], &result) != nil {
		return liveComputerHTTPResult{}, fmt.Errorf("decode Computer HTTP probe: %s", output)
	}
	return result, nil
}

func runLiveComputerHTTP(t *testing.T, computer l1.Computer, method, path, idempotencyKey string, body any) liveComputerHTTPResult {
	t.Helper()
	result, err := tryLiveComputerHTTP(t, computer, method, path, idempotencyKey, body)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func waitForLiveComputerHTTP(t *testing.T, computer l1.Computer, method, path, idempotencyKey string, body any, timeout time.Duration) liveComputerHTTPResult {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		result, err := tryLiveComputerHTTP(t, computer, method, path, idempotencyKey, body)
		if err == nil && result.Status >= 200 && result.Status < 300 {
			return result
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("status=%d body=%s", result.Status, result.Body)
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("live Computer HTTP route did not become ready: %v", lastErr)
	return liveComputerHTTPResult{}
}

const liveComputerPausedHTTPPython = `
import json, os, socket, sys, time, urllib.parse
path, key, body = sys.argv[1:4]
endpoint = urllib.parse.urlsplit(os.environ["WEFTY_L3_ENDPOINT"])
token = open("/wefty/control/computer-token", encoding="utf-8").read().strip()
payload = body.encode()
request = ("POST " + path + " HTTP/1.1\r\nHost: " + endpoint.netloc + "\r\nAuthorization: Bearer " + token +
           "\r\nContent-Type: application/json\r\nIdempotency-Key: " + key + "\r\nConnection: close\r\nContent-Length: " +
           str(len(payload)) + "\r\n\r\n").encode()
status = 0
try:
    connection = socket.create_connection((endpoint.hostname, endpoint.port), timeout=30)
    split = len(payload) // 2
    connection.sendall(request + payload[:split])
    print("PAUSED", flush=True)
    time.sleep(8)
    connection.sendall(payload[split:])
    response = b""
    while True:
        chunk = connection.recv(65536)
        if not chunk:
            break
        response += chunk
    if response:
        status = int(response.split(b" ", 2)[1])
except Exception:
    pass
print(json.dumps({"status": status, "body": "revocation-race"}), flush=True)
`

type liveComputerPausedSubmission struct {
	command *exec.Cmd
	scanner *bufio.Scanner
	stderr  *bytes.Buffer
}

func startLiveComputerPausedSubmission(t *testing.T, computer l1.Computer, idempotencyKey string, body any) *liveComputerPausedSubmission {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	containerdAddress := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_CONTAINERD_ADDRESS")
	containerID := liveComputerContainerID(t, computer.CurrentJobID)
	execID := fmt.Sprintf("computer-race-%d", time.Now().UnixNano())
	command := exec.Command("sudo", "/usr/local/bin/ctr", "--address", containerdAddress, "--namespace", ocihelper.ContainerdNamespace,
		"tasks", "exec", "--exec-id", execID, containerID, "/usr/bin/python3", "-c", liveComputerPausedHTTPPython,
		"/v1/runs", idempotencyKey, string(payload))
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr := &bytes.Buffer{}
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "PAUSED" {
		_ = command.Wait()
		t.Fatalf("Computer revocation race did not pause after authentication: stdout=%q stderr=%q", scanner.Text(), stderr.String())
	}
	return &liveComputerPausedSubmission{command: command, scanner: scanner, stderr: stderr}
}

func (submission *liveComputerPausedSubmission) finish(t *testing.T) int {
	t.Helper()
	if !submission.scanner.Scan() {
		_ = submission.command.Wait()
		t.Fatalf("Computer revocation race omitted its result: %s", submission.stderr.String())
	}
	var result liveComputerHTTPResult
	if err := json.Unmarshal([]byte(submission.scanner.Text()), &result); err != nil {
		_ = submission.command.Wait()
		t.Fatalf("decode Computer revocation race: %v line=%q", err, submission.scanner.Text())
	}
	if err := submission.command.Wait(); err != nil {
		t.Fatalf("Computer revocation race process: %v: %s", err, submission.stderr.String())
	}
	return result.Status
}

func observationHasPointer(observation liveInputObservation, x, y int) bool {
	return pointerHistoryHas(observation.PointerHistory, x, y)
}

func proveLiveViewInputIsolation(t *testing.T, harness *acceptanceHarness, computer l1.Computer, viewerUser, viewerDevice string) bool {
	t.Helper()
	if computer.DisplayEndpoint == nil {
		t.Fatal("Computer omitted its live display endpoint")
	}
	before := readLiveInputObservation(t, computer.CurrentJobID)
	freeViewPointer, heldViewPointer, controlPointer := freshPointerSentinels(before.PointerHistory)
	if freeViewPointer == ([2]int{}) || heldViewPointer == ([2]int{}) || controlPointer == ([2]int{}) {
		t.Fatal("Computer input history exhausted the isolation sentinels")
	}
	view := openLiveRFBSession(t, *computer.DisplayEndpoint, viewerUser, viewerDevice)
	defer view.close()
	view.sendPointer(t, freeViewPointer[0], freeViewPointer[1])
	control := openLiveRFBSession(t, *computer.DisplayEndpoint, "linux-admin", "linux-input-sentinel-device")
	defer control.close()
	controlCapability := control.capabilityFile(t)
	take := runComputerCLIPerson[contract.ComputerControlReceipt](t, harness, "linux-admin", "linux-input-sentinel-device",
		"services", "takeover", "take", computer.ComputerID, "--session-token-file", controlCapability)
	if take.TenureState != contract.ComputerControlTenureHeld {
		t.Fatalf("control sentinel take = %#v", take)
	}
	// The Held-tenure sentinel is the decisive isolation arm: the view-only
	// session must remain unable to drive while another session owns the wheel.
	view.sendPointer(t, heldViewPointer[0], heldViewPointer[1])
	control.sendPointer(t, controlPointer[0], controlPointer[1])
	var after liveInputObservation
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		after = readLiveInputObservation(t, computer.CurrentJobID)
		if after.Generation > before.Generation && observationHasPointer(after, controlPointer[0], controlPointer[1]) {
			break
		}
		time.Sleep(125 * time.Millisecond)
	}
	_ = runComputerCLIPerson[contract.ComputerControlReceipt](t, harness, "linux-admin", "linux-input-sentinel-device",
		"services", "takeover", "release", computer.ComputerID, "--session-token-file", controlCapability)
	return after.Generation > before.Generation && observationHasPointer(after, controlPointer[0], controlPointer[1]) &&
		!observationHasPointer(after, freeViewPointer[0], freeViewPointer[1]) &&
		!observationHasPointer(after, heldViewPointer[0], heldViewPointer[1]) && after.KeyEvents == before.KeyEvents
}

func takeoverAuditEvidence(audit l1.ComputerTakeoverAuditList) (map[l1.ComputerTakeoverAuditEventKind]bool, int64) {
	kinds := map[l1.ComputerTakeoverAuditEventKind]bool{}
	var generation int64
	for _, event := range audit.Events {
		kinds[event.Kind] = true
		if event.AuthorityGeneration > generation {
			generation = event.AuthorityGeneration
		}
	}
	return kinds, generation
}

func takeoverAuditReleasePrecedesClose(
	audit l1.ComputerTakeoverAuditList,
	sessionID string,
	reason l1.ComputerTakeoverReason,
) bool {
	releaseIndex, closeIndex := -1, -1
	for index, event := range audit.Events {
		if event.SessionID != sessionID || event.Reason != reason {
			continue
		}
		switch event.Kind {
		case l1.ComputerTakeoverControlReleased:
			if releaseIndex == -1 {
				releaseIndex = index
			}
		case l1.ComputerTakeoverSessionClose:
			if closeIndex == -1 {
				closeIndex = index
			}
		}
	}
	return sessionID != "" && releaseIndex >= 0 && closeIndex > releaseIndex
}

func waitForComputerAttemptTerminal(
	t *testing.T,
	harness *acceptanceHarness,
	jobID, attemptID string,
	timeout time.Duration,
) l1.Attempt {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var job l1.Job
		status, body := harness.doJSON(t, http.MethodGet, "/v1/jobs/"+jobID+"?class=service", nil, &job)
		if status != http.StatusOK {
			t.Fatalf("get Computer Job attempts = %d body=%s", status, body)
		}
		for _, attempt := range job.Attempts {
			if attempt.AttemptID == attemptID && attempt.Result != nil {
				return attempt
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for authority-bound terminal result for attempt %q", attemptID)
	return l1.Attempt{}
}

func appendUniqueInt64(values []int64, value int64) []int64 {
	if value == 0 {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func inspectLiveComputerReimageDetachment(t *testing.T, harness *acceptanceHarness, computer l1.Computer) bool {
	t.Helper()
	database, err := sql.Open("sqlite", harness.l1Database)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var payload []byte
	var status string
	if err := database.QueryRow(`SELECT preflight_receipt_json, status FROM computer_reimage_operations
		WHERE computer_id=? AND operation_revision=?`, computer.ComputerID, computer.AppliedRevision).
		Scan(&payload, &status); err != nil {
		t.Fatal(err)
	}
	var acknowledgement struct {
		Receipt l1.ComputerReimagePreflightReceipt `json:"receipt"`
	}
	if err := json.Unmarshal(payload, &acknowledgement); err != nil {
		t.Fatal(err)
	}
	receipt := acknowledgement.Receipt
	return status == "completed" && receipt.Kind == "computer_reimage_preflight_verified" &&
		receipt.ComputerID == computer.ComputerID && receipt.StorageID == computer.StorageID &&
		receipt.StorageGeneration == computer.StorageGeneration && receipt.OperationRevision == computer.AppliedRevision &&
		receipt.StagingJobID == computer.CurrentJobID && receipt.StorageEvidenceKind == "computer_reimage_detachment" &&
		receipt.DetachmentReceiptID != "" && receipt.DetachmentAttemptID != "" && receipt.DetachmentFencingToken != "" &&
		receipt.ResetPreparationReceiptID == "" && receipt.ImageUID == receipt.DiskRootUID && receipt.ImageGID == receipt.DiskRootGID
}

type liveAbortEvidence struct {
	ComputerID       string
	Aborted          bool
	StaleCASRejected bool
	NoAutoRollback   bool
}

func exerciseLiveReconfigurationAbort(t *testing.T, harness *acceptanceHarness, reference, digest string) liveAbortEvidence {
	t.Helper()
	target := createReadyComputer(t, harness, reference, digest, "linux-native-abort", "linux-native-abort-create")
	stale := runComputerCLIExpectError(t, harness, "", "", "services", "resize", target.ComputerID,
		"--disk-bytes", fmt.Sprint(256<<20), "--intent-revision", fmt.Sprint(target.IntentRevision+99),
		"--storage-id", target.StorageID, "--storage-generation", fmt.Sprint(target.StorageGeneration),
		"--idempotency-key", "linux-native-stale-cas")
	mutation := runComputerCLI[l1.Computer](t, harness, false, "services", "resize", target.ComputerID,
		"--disk-bytes", fmt.Sprint(512<<20), "--expect-current", "--idempotency-key", "linux-native-abort-grow")
	harness.agent.kill(t)
	time.Sleep(l1.DefaultNodeDeadAfter + 5*time.Second)
	beforeAbort := runComputerCLI[l1.Computer](t, harness, false, "services", "status", target.ComputerID)
	aborted := runComputerCLI[l1.Computer](t, harness, false, "services", "abort", target.ComputerID,
		"--expect-current", "--idempotency-key", "linux-native-abort")
	harness.restartAgent(t)
	_ = removeAndWaitComputer(t, harness, aborted, 5*time.Minute)
	return liveAbortEvidence{ComputerID: target.ComputerID,
		Aborted:          aborted.ReconfigurationPhase == l1.ComputerReconfigurationStable,
		StaleCASRejected: strings.Contains(stale, string(contract.ErrorStaleIntentRevision)),
		NoAutoRollback:   beforeAbort.IntentRevision == mutation.IntentRevision && beforeAbort.AppliedRevision < beforeAbort.IntentRevision}
}

type liveENOSPCEvidence struct {
	Observed bool
	Detail   string
}

func exerciseLiveComputerENOSPC(t *testing.T, harness *acceptanceHarness, reference, digest string) liveENOSPCEvidence {
	t.Helper()
	target := createReadyComputer(t, harness, reference, digest, "linux-native-enospc", "linux-native-enospc-create")
	availableOutput, err := exec.Command("sudo", "df", "--block-size=1", "--output=avail", "/var/lib/wefty/oci").CombinedOutput()
	if err != nil {
		t.Fatalf("observe real disk availability: %v\n%s", err, availableOutput)
	}
	fields := strings.Fields(string(availableOutput))
	available, err := strconv.ParseInt(fields[len(fields)-1], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	requested := available + 1<<30
	failure := runComputerCLIExpectError(t, harness, "", "", "services", "resize", target.ComputerID,
		"--disk-bytes", fmt.Sprint(requested), "--expect-current", "--idempotency-key", "linux-native-enospc-grow")
	_ = removeAndWaitComputer(t, harness, target, 5*time.Minute)
	return liveENOSPCEvidence{Observed: strings.Contains(failure, string(contract.SpawnFailureInsufficientDisk)) || strings.Contains(failure, "insufficient_disk"),
		Detail: fmt.Sprintf("requested=%d observed_available=%d", requested, available)}
}

func inspectHelperNamespaceInventory(t *testing.T, socketPath, checksum string) ocihelper.VerifyResponse {
	t.Helper()
	client := ocihelper.NewUnixClient(socketPath, checksum)
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		session, err := client.OpenSession(ctx, ocihelper.AcquireSessionRequest{NodeID: "acceptance-inventory", BootSessionID: "acceptance-inventory-boot"})
		cancel()
		if err == nil {
			verification, verifyErr := session.Verify(t.Context(), ocihelper.VerifyRequest{Scope: ocihelper.VerifyNamespace})
			_ = session.Close()
			if verifyErr != nil {
				t.Fatalf("verify helper namespace inventory: %v", verifyErr)
			}
			if !verification.Absent {
				t.Fatalf("helper namespace runtime residue=%#v durable retained=%#v observed=%#v", verification.RuntimeResidue, verification.DurableRetained, verification.Inventory)
			}
			return verification
		}
		lastErr = err
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("acquire independent helper inventory session: %v", lastErr)
	return ocihelper.VerifyResponse{}
}

func sha256File(t *testing.T, path string) string {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func liveContainerdImagePresent(t *testing.T, digest string) bool {
	t.Helper()
	address := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_CONTAINERD_ADDRESS")
	output, err := exec.Command("sudo", "/usr/local/bin/ctr", "--address", address, "--namespace", ocihelper.ContainerdNamespace,
		"images", "list").CombinedOutput()
	if err != nil {
		t.Fatalf("list live image cache: %v\n%s", err, output)
	}
	return strings.Contains(string(output), digest)
}

func runComputerCLI[T any](t *testing.T, harness *acceptanceHarness, person bool, arguments ...string) T {
	t.Helper()
	if person {
		return runComputerCLIWithIdentity[T](t, harness, "linux-admin", "linux-admin-device-a", arguments...)
	}
	return runComputerCLIWithIdentity[T](t, harness, "", "", arguments...)
}

func bootstrapComputerAcceptanceAdmin(t *testing.T, harness *acceptanceHarness) l1.AdminPolicy {
	t.Helper()
	if harness.adminBootstrapNonce == "" {
		t.Fatal("Computer harness omitted the shipped L1 admin bootstrap challenge")
	}
	return runComputerCLI[l1.AdminPolicy](t, harness, true, "admin", "bootstrap", harness.adminBootstrapNonce)
}

func waitForComputerCLI(t *testing.T, harness *acceptanceHarness, computerID string, timeout time.Duration, ready func(l1.Computer) bool) l1.Computer {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last l1.Computer
	for time.Now().Before(deadline) {
		last = runComputerCLI[l1.Computer](t, harness, false, "services", "status", computerID)
		if ready(last) {
			return last
		}
		if harness.agent.exited() {
			t.Fatalf("Computer agent exited: %v\n%s", harness.agent.waitError(), harness.agent.outputString())
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("Computer %s did not reach requested state: %#v", computerID, last)
	return l1.Computer{}
}

func computerDisplayPublished(computer l1.Computer) bool {
	// Computers forbid published_port, so ServiceJob.Ready is intentionally nil.
	// The fenced, attempt-bound display_endpoint is their readiness projection.
	return computer.CurrentJob.State == contract.JobRunning && computer.DisplayEndpoint != nil
}

func computerStoppedAndDetached(computer l1.Computer) bool {
	return computer.DesiredState == contract.ServiceDesiredStopped &&
		(computer.CurrentJob.State == contract.JobStopped || computer.CurrentJob.State == contract.JobFailed) &&
		computer.CurrentJob.CurrentAttemptID == "" && computer.ReconfigurationPhase == l1.ComputerReconfigurationStable
}

func computerStoppedAfterExplicitStop(computer l1.Computer) bool {
	return computer.DesiredState == contract.ServiceDesiredStopped &&
		computer.CurrentJob.State == contract.JobStopped && computer.CurrentJob.CurrentAttemptID == "" &&
		computer.ReconfigurationPhase == l1.ComputerReconfigurationStable
}

func failOnTypedReimagePreflight(t *testing.T, computer l1.Computer, priorSpecRevision int64) {
	t.Helper()
	if computer.ReconfigurationPhase != l1.ComputerReconfigurationStable ||
		computer.CurrentSpecRevision > priorSpecRevision || len(computer.CurrentJob.LastFailure) == 0 {
		return
	}
	var failure contract.SpawnFailure
	if json.Unmarshal(computer.CurrentJob.LastFailure, &failure) != nil {
		return
	}
	if failure.Code == contract.SpawnFailureReimagePreflight || failure.Code == contract.SpawnFailureImageUnavailable ||
		failure.Code == contract.SpawnFailureImagePlatformUnsupported {
		t.Fatalf("Computer reimage returned typed preflight failure instead of timing out: code=%s message=%s",
			failure.Code, failure.Message)
	}
}

func createReadyComputer(t *testing.T, harness *acceptanceHarness, reference, digest, name, key string) l1.Computer {
	t.Helper()
	created := runComputerCLI[l1.Computer](t, harness, false, "services", "create", "--computer", "--name", name,
		"--image", reference+"@"+digest, "--node", "acceptance-node", "--memory-bytes", fmt.Sprint(1<<30),
		"--disk-bytes", fmt.Sprint(64<<20), "--idempotency-key", key)
	return waitForComputerCLI(t, harness, created.ComputerID, 3*time.Minute, func(current l1.Computer) bool {
		return computerDisplayPublished(current)
	})
}

func removeAndWaitComputer(t *testing.T, harness *acceptanceHarness, computer l1.Computer, timeout time.Duration) l1.Computer {
	t.Helper()
	removed := runComputerCLI[l1.Computer](t, harness, false, "services", "remove", computer.ComputerID, "--expect-current")
	return waitForComputerCLI(t, harness, removed.ComputerID, timeout, func(current l1.Computer) bool {
		return current.RemovalOutcome != ""
	})
}

func recordComputerAuthority(receipt *linuxComputerMatrixReceipt, computer l1.Computer) {
	receipt.ComputerIDs = appendUnique(receipt.ComputerIDs, computer.ComputerID)
	receipt.JobIDs = appendUnique(receipt.JobIDs, computer.CurrentJobID)
	receipt.AttemptIDs = appendUnique(receipt.AttemptIDs, computer.CurrentJob.CurrentAttemptID)
	receipt.StorageIDs = appendUnique(receipt.StorageIDs, computer.StorageID)
}

func appendUnique(values []string, value string) []string {
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}
