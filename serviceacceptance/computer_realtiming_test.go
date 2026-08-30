//go:build service_acceptance_realtiming && (darwin || linux)

package serviceacceptance

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
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

	reference := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_COMPUTER_REFERENCE")
	digest := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_COMPUTER_DIGEST")
	archive := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_COMPUTER_ARCHIVE")
	imageRuntime := readPublishedComputerRuntimeReceipt(t, requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_COMPUTER_RUNTIME_RECEIPT"))
	receipt.Image = linuxComputerImageEvidence{Variant: requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_COMPUTER_VARIANT"), Reference: reference, IndexDigest: digest,
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
	importRealtimeProbeImage(t, requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_PROBE_ARCHIVE"),
		helperSocket, helperChecksum, probeReference, probeDigest)
	importRealtimeProbeImage(t, archive, helperSocket, helperChecksum, reference, digest)
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
	adminView := startTakeoverViewCLI(t, harness, ready.ComputerID, "linux-admin", "linux-admin-device-a")
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
	}, map[string]string{"computer_id": ready.ComputerID, "job_id": ready.CurrentJobID,
		"attempt_id": ready.CurrentJob.CurrentAttemptID, "storage_id": ready.StorageID,
		"storage_generation": fmt.Sprint(ready.StorageGeneration), "intent_revision": fmt.Sprint(ready.IntentRevision),
		"disk_path": diskEvidence.Path, "disk_blocks_bytes": fmt.Sprint(diskEvidence.BlocksBytes)})

	receipt.begin("linux.remote_takeover")
	viewGrant := runComputerCLIPerson[l1.ComputerGrantMutationResult](t, harness, "linux-admin", "linux-admin-device-a",
		"services", "grant", ready.ComputerID, "linux-viewer", "--permission", "view",
		"--policy-revision", fmt.Sprint(policy.Revision), "--idempotency-key", "linux-native-view-grant")
	if !viewGrant.MutationApplied || viewGrant.Grant.Permission != l1.ComputerGrantView {
		t.Fatalf("Computer view grant = %#v", viewGrant)
	}
	receipt.FabricIdentities = append(receipt.FabricIdentities,
		linuxComputerFabricIdentity{Role: "administrator", FabricID: policy.Admins[0].FabricID, UserID: "linux-admin", DeviceID: "linux-admin-device-a"},
		linuxComputerFabricIdentity{Role: "viewer", FabricID: viewGrant.Grant.FabricID, UserID: "linux-viewer", DeviceID: "linux-viewer-device"})
	inputIsolation := proveLiveViewInputIsolation(t, harness, ready, "linux-viewer", "linux-viewer-device")
	viewerView := startTakeoverViewCLI(t, harness, ready.ComputerID, "linux-viewer", "linux-viewer-device")
	viewerTakeDenied := runComputerCLIPersonExpectError(t, harness, "linux-viewer", "linux-viewer-device",
		"services", "takeover", "take", ready.ComputerID, "--session-token-file", viewerView.tokenFile)
	controlGrant := runComputerCLIPerson[l1.ComputerGrantMutationResult](t, harness, "linux-admin", "linux-admin-device-a",
		"services", "grant", ready.ComputerID, "linux-viewer", "--permission", "control",
		"--policy-revision", fmt.Sprint(viewGrant.Grant.PolicyRevision), "--idempotency-key", "linux-native-control-grant")
	viewerView.stop(t)
	viewerControl := startTakeoverViewCLI(t, harness, ready.ComputerID, "linux-viewer", "linux-viewer-device")
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
	adminOverrideView := startTakeoverViewCLI(t, harness, ready.ComputerID, "linux-admin", "linux-admin-device-b")
	adminOverride := runComputerCLIPerson[contract.ComputerControlReceipt](t, harness, "linux-admin", "linux-admin-device-b",
		"services", "takeover", "take", ready.ComputerID, "--session-token-file", adminOverrideView.tokenFile)
	revoked := runComputerCLIPerson[l1.ComputerGrantMutationResult](t, harness, "linux-admin", "linux-admin-device-a",
		"services", "revoke", ready.ComputerID, "linux-viewer", "--policy-revision", fmt.Sprint(controlGrant.Grant.PolicyRevision),
		"--idempotency-key", "linux-native-control-revoke", "--wait", "--wait-timeout", "3m")
	viewerControl.waitClosed(t, 30*time.Second)
	staleSessionDenied := runComputerCLIPersonExpectError(t, harness, "linux-viewer", "linux-viewer-device",
		"services", "takeover", "take", ready.ComputerID, "--session-token-file", viewerControl.tokenFile)
	adminOverrideView.stop(t)
	audit := runComputerCLIPerson[l1.ComputerTakeoverAuditList](t, harness, "linux-admin", "linux-admin-device-a",
		"services", "takeover", "audit", "tail", ready.ComputerID, "--limit", "100")
	auditKinds, auditAuthority := takeoverAuditEvidence(audit)
	receipt.AuthorityGenerations = appendUniqueInt64(receipt.AuthorityGenerations, auditAuthority)
	completeLinuxComputerRow(t, receipt, "linux.remote_takeover", map[string]bool{
		"cli_view_grant_cas":          viewGrant.MutationApplied,
		"view_admission_live":         viewerView.admitted,
		"view_only_take_refused":      strings.Contains(viewerTakeDenied, string(contract.ErrorControlNotAuthorized)),
		"view_pointer_isolation_live": inputIsolation,
		"cli_control_grant_cas":       controlGrant.MutationApplied,
		"cli_take_live":               viewerTake.TenureState == contract.ComputerControlTenureHeld,
		"cli_release_live":            viewerRelease.TenureState == contract.ComputerControlTenureFree,
		"admin_override_live":         adminOverride.OverrideDisplacedSessionID != "" && adminOverride.SignalStayedTrue,
		"cli_revoke_installed":        revoked.ObservationState == "completed",
		"revocation_closed_session":   strings.Contains(staleSessionDenied, string(contract.ErrorTakeoverSessionEnded)),
		"audit_tail_session_open":     auditKinds[l1.ComputerTakeoverSessionOpen],
		"audit_tail_session_close":    auditKinds[l1.ComputerTakeoverSessionClose],
		"audit_tail_control_acquired": auditKinds[l1.ComputerTakeoverControlAcquired],
		"audit_tail_control_released": auditKinds[l1.ComputerTakeoverControlReleased],
		"audit_tail_admin_overrode":   auditKinds[l1.ComputerTakeoverAdminOverrode],
	}, map[string]string{
		"policy_revision":      fmt.Sprint(revoked.Grant.PolicyRevision),
		"authority_generation": fmt.Sprint(auditAuthority),
		"viewer_fabric_id":     viewGrant.Grant.FabricID,
	})

	receipt.begin("linux.restart_survival")
	oldAttempt, oldStorage, oldGeneration := ready.CurrentJob.CurrentAttemptID, ready.StorageID, ready.StorageGeneration
	profileMarker := plantLiveProfileMarker(t, ready, "restart-survival-marker")
	lossAttempts := map[string]string{}
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
	oldAuthorityRejected := runComputerCLIPersonExpectError(t, harness, "linux-viewer", "linux-viewer-device",
		"services", "takeover", "take", restarted.ComputerID, "--session-token-file", viewerControl.tokenFile)
	recordComputerAuthority(receipt, restarted)
	completeLinuxComputerRow(t, receipt, "linux.restart_survival", map[string]bool{
		"payload_loss_fresh_attempt":     lossAttempts["kill-payload"] != "" && lossAttempts["kill-payload"] != oldAttempt,
		"shim_loss_fresh_attempt":        lossAttempts["kill-shim"] != "" && lossAttempts["kill-shim"] != lossAttempts["kill-payload"],
		"helper_loss_fresh_attempt":      lossAttempts["kill-helper"] != "" && lossAttempts["kill-helper"] != lossAttempts["kill-shim"],
		"runtime_loss_fresh_attempt":     lossAttempts["stop-containerd"] != "" && lossAttempts["stop-containerd"] != lossAttempts["kill-helper"],
		"agent_loss_fresh_attempt":       restarted.CurrentJob.CurrentAttemptID != beforeAgentLoss,
		"same_storage_generation":        restarted.StorageID == oldStorage && restarted.StorageGeneration == oldGeneration,
		"profile_marker_survived_losses": true,
		"readiness_republished":          restarted.DisplayEndpoint != nil,
		"old_session_authority_rejected": strings.Contains(oldAuthorityRejected, string(contract.ErrorTakeoverSessionEnded)),
	}, map[string]string{"old_attempt_id": oldAttempt, "new_attempt_id": restarted.CurrentJob.CurrentAttemptID,
		"storage_id": restarted.StorageID, "storage_generation": fmt.Sprint(restarted.StorageGeneration),
		"profile_marker": filepath.Base(profileMarker)})

	receipt.begin("linux.reconfiguration")
	resized := runComputerCLI[l1.Computer](t, harness, false, "services", "resize", restarted.ComputerID,
		"--disk-bytes", fmt.Sprint(160<<20), "--expect-current", "--idempotency-key", "linux-native-grow")
	resized = waitForComputerCLI(t, harness, resized.ComputerID, 3*time.Minute, func(current l1.Computer) bool {
		return current.ReconfigurationPhase == l1.ComputerReconfigurationStable && current.AppliedRevision == current.IntentRevision && current.DesiredDiskBytes == 160<<20
	})
	reset := runComputerCLI[l1.Computer](t, harness, false, "services", "reset", resized.ComputerID,
		"--expect-current", "--idempotency-key", "linux-native-reset")
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
	reimaged := runComputerCLI[l1.Computer](t, harness, false, "services", "reimage", reset.ComputerID,
		"--image", reference+"@"+digest, "--expect-current", "--idempotency-key", "linux-native-reimage")
	reimaged = waitForComputerCLI(t, harness, reimaged.ComputerID, 4*time.Minute, func(current l1.Computer) bool {
		return current.ReconfigurationPhase == l1.ComputerReconfigurationStable && current.AppliedRevision == current.IntentRevision &&
			current.CurrentSpecRevision > reset.CurrentSpecRevision && current.DisplayEndpoint != nil
	})
	abortEvidence := exerciseLiveReconfigurationAbort(t, harness, reference, digest)
	detachment := inspectLiveComputerDetachment(t, reimaged)
	recordComputerAuthority(receipt, reimaged)
	completeLinuxComputerRow(t, receipt, "linux.reconfiguration", map[string]bool{
		"grow_applied_live":           resized.DesiredDiskBytes == 160<<20,
		"reset_crash_phase_live":      resetCrashObserved && reset.IntentRevision >= resetIntent,
		"reset_fresh_generation_live": reset.StorageGeneration > resized.StorageGeneration,
		"reimage_new_projection_live": reimaged.CurrentSpecRevision > reset.CurrentSpecRevision,
		"detachment_receipt_live":     detachment,
		"abort_after_dead_node_live":  abortEvidence.Aborted,
		"stale_cas_rejected_live":     abortEvidence.StaleCASRejected,
		"no_automatic_rollback_live":  abortEvidence.NoAutoRollback,
	}, map[string]string{"intent_revision": fmt.Sprint(reimaged.IntentRevision), "spec_revision": fmt.Sprint(reimaged.CurrentSpecRevision),
		"storage_generation": fmt.Sprint(reimaged.StorageGeneration), "aborted_computer_id": abortEvidence.ComputerID})

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
	recordComputerAuthority(receipt, *importOutput.Computer)
	if err := os.RemoveAll(exportDirectory); err != nil {
		t.Fatal(err)
	}
	attestOutput := runComputerCLI[storageCLIMutationReceipt](t, harness, false, "services", "custody", "attest",
		exportOutput.CustodyExport.ExportID, "--idempotency-key", "linux-native-export-attested")
	if attestOutput.CustodyExport == nil || !attestOutput.CustodyExport.OperatorAttestedDeleted {
		t.Fatalf("Computer custody attestation result = %#v", attestOutput)
	}
	staleCredential := startTakeoverViewCLI(t, harness, reimaged.ComputerID, "linux-admin", "linux-admin-device-a")
	restoreBaseline := reimaged.StorageGeneration
	restoreOutput := runComputerCLI[storageCLIMutationReceipt](t, harness, false, "services", "restore", reimaged.ComputerID, backup.BackupID,
		"--keep-old-as-backup", "--expect-current", "--idempotency-key", "linux-native-restore", "--wait", "4m")
	if restoreOutput.Computer == nil || restoreOutput.Computer.StorageGeneration <= reimaged.StorageGeneration {
		t.Fatalf("Computer restore result = %#v", restoreOutput)
	}
	staleCredential.waitClosed(t, 30*time.Second)
	staleCredentialRejected := runComputerCLIPersonExpectError(t, harness, "linux-admin", "linux-admin-device-a",
		"services", "takeover", "take", reimaged.ComputerID, "--session-token-file", staleCredential.tokenFile)
	reimaged = *restoreOutput.Computer
	recordComputerAuthority(receipt, reimaged)
	backupInventory := runComputerCLI[computerBackupInventoryReceipt](t, harness, false, "services", "backup", "list", reimaged.ComputerID)
	capSet := runComputerCLI[storageCLIMutationReceipt](t, harness, false, "services", "backup", "set-cap", reimaged.ComputerID,
		"--cap", fmt.Sprint(len(backupInventory.Backups)), "--expect-current")
	capPressure := ""
	if !mutatingLinuxComputerRow("linux.storage_provenance") {
		capPressure = runComputerCLIExpectError(t, harness, "", "", "services", "backup", "create", reimaged.ComputerID,
			"--expect-current", "--allow-power-off", "--idempotency-key", "linux-native-cap-pressure", "--wait", "4m")
	}
	enospc := exerciseLiveComputerENOSPC(t, harness, reference, digest)
	completeLinuxComputerRow(t, receipt, "linux.storage_provenance", map[string]bool{
		"cold_backup_available_live":          backup.Status == "available",
		"clone_fork_created_live":             cloneOutput.Computer.ComputerID != reimaged.ComputerID,
		"custody_export_manifest_bound_live":  manifestDigest == exportOutput.CustodyExport.ManifestDigest,
		"custody_import_tainted_live":         importOutput.StorageProvenance.CustodyTainted,
		"custody_delete_attested_live":        attestOutput.CustodyExport.OperatorAttestedDeleted,
		"keep_old_restore_live":               len(backupInventory.Backups) >= 2,
		"restore_fresh_generation_live":       restoreOutput.Computer.StorageGeneration > restoreBaseline,
		"planted_stale_session_rejected_live": strings.Contains(staleCredentialRejected, string(contract.ErrorTakeoverSessionEnded)),
		"backup_cap_pressure_live":            capSet.Computer != nil && strings.Contains(capPressure, string(contract.ErrorConflict)),
		"real_disk_enospc_live":               enospc.Observed,
	}, map[string]string{"backup_id": backup.BackupID, "clone_computer_id": cloneOutput.Computer.ComputerID,
		"export_id": exportOutput.CustodyExport.ExportID, "import_computer_id": importOutput.Computer.ComputerID,
		"retained_backups": fmt.Sprint(len(backupInventory.Backups)), "enospc_observation": enospc.Detail})

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
	inventory := inspectHelperNamespaceInventory(t, helperSocket, helperChecksum)
	receipt.ResidueInventories["post_removal_helper_namespace"] = inventory
	archiveAfter := sha256File(t, archive)
	cacheAfter := liveContainerdImagePresent(t, digest)
	completeLinuxComputerRow(t, receipt, "linux.removal", map[string]bool{
		"verified_absence_outcome_live":      verified.RemovalOutcome == "removed_verified",
		"reduced_custody_outcome_live":       reduced.RemovalOutcome == "removed_reduced",
		"reduced_bound_to_tainted_computer":  reduced.ComputerID == taintedComputerID,
		"independent_helper_inventory_empty": ocihelper.InventoryEmpty(inventory),
		"containers_absent":                  len(inventory.Containers) == 0,
		"tasks_absent":                       len(inventory.Tasks) == 0,
		"disks_loops_mounts_absent":          len(inventory.ComputerDiskImages)+len(inventory.ComputerDiskLoops)+len(inventory.ComputerDiskMounts) == 0,
		"logs_and_control_absent":            len(inventory.LogSegments)+len(inventory.Cgroups) == 0,
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

func runComputerCLIWithIdentity[T any](t *testing.T, harness *acceptanceHarness, userID, deviceID string, arguments ...string) T {
	t.Helper()
	args := computerCLIArguments(harness, userID, deviceID, arguments...)
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, weftyBinaryPath, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run Computer CLI %v: %v\n%s", arguments, err, output)
	}
	var result T
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode Computer CLI %v: %v\n%s", arguments, err, output)
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

type takeoverViewProcess struct {
	tokenFile string
	admitted  bool
	cancel    context.CancelFunc
	done      chan error
	stdout    bytes.Buffer
	stderr    bytes.Buffer
}

func startTakeoverViewCLI(t *testing.T, harness *acceptanceHarness, computerID, userID, deviceID string) *takeoverViewProcess {
	t.Helper()
	view := &takeoverViewProcess{tokenFile: filepath.Join(t.TempDir(), "takeover-session.json"), done: make(chan error, 1)}
	ctx, cancel := context.WithCancel(t.Context())
	view.cancel = cancel
	command := exec.CommandContext(ctx, weftyBinaryPath, computerCLIArguments(harness, userID, deviceID,
		"services", "takeover", "view", computerID, "--session-token-file", view.tokenFile)...)
	command.Stdout, command.Stderr = &view.stdout, &view.stderr
	go func() { view.done <- command.Run() }()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if info, err := os.Stat(view.tokenFile); err == nil && info.Size() > 0 {
			view.admitted = true
			return view
		}
		select {
		case err := <-view.done:
			t.Fatalf("takeover view exited before admission: %v\n%s", err, view.stderr.String())
		default:
		}
		time.Sleep(25 * time.Millisecond)
	}
	cancel()
	t.Fatalf("takeover view did not publish its session capability: %s", view.stderr.String())
	return nil
}

func (view *takeoverViewProcess) stop(t *testing.T) {
	t.Helper()
	view.cancel()
	select {
	case <-view.done:
	case <-time.After(10 * time.Second):
		t.Fatal("takeover view did not stop")
	}
}

func (view *takeoverViewProcess) waitClosed(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-view.done:
	case <-time.After(timeout):
		view.cancel()
		t.Fatal("takeover view remained open after authority revocation")
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
	participant := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: deviceID, UserID: userID, DeviceID: deviceID})
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
	banner := make([]byte, contract.ComputerRFBVersionBannerBytes)
	if _, err := io.ReadFull(network, banner); err != nil || !contract.ValidComputerRFBVersionBanner(banner) {
		_ = connection.CloseNow()
		t.Fatalf("read live RFB banner: %v %q", err, banner)
	}
	if _, err := network.Write([]byte("RFB 003.008\n")); err != nil {
		t.Fatal(err)
	}
	count := []byte{0}
	if _, err := io.ReadFull(network, count); err != nil || count[0] == 0 {
		t.Fatalf("read live RFB security count: %v", err)
	}
	securityTypes := make([]byte, int(count[0]))
	if _, err := io.ReadFull(network, securityTypes); err != nil || !bytes.Contains(securityTypes, []byte{1}) {
		t.Fatalf("live RFB None security unavailable: %v %x", err, securityTypes)
	}
	if _, err := network.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	securityResult := make([]byte, 4)
	if _, err := io.ReadFull(network, securityResult); err != nil || !bytes.Equal(securityResult, []byte{0, 0, 0, 0}) {
		t.Fatalf("live RFB security result: %v %x", err, securityResult)
	}
	if _, err := network.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	serverInit := make([]byte, 24)
	if _, err := io.ReadFull(network, serverInit); err != nil {
		t.Fatal(err)
	}
	name := make([]byte, int(binary.BigEndian.Uint32(serverInit[20:24])))
	if _, err := io.ReadFull(network, name); err != nil {
		t.Fatal(err)
	}
	if token == "" {
		t.Fatal("live RFB session omitted its control capability")
	}
	return &liveRFBSession{endpoint: endpoint, token: token, websocket: connection, connection: network}
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
	for _, point := range observation.PointerHistory {
		if point[0] == x && point[1] == y {
			return true
		}
	}
	return false
}

func proveLiveViewInputIsolation(t *testing.T, harness *acceptanceHarness, computer l1.Computer, viewerUser, viewerDevice string) bool {
	t.Helper()
	if computer.DisplayEndpoint == nil {
		t.Fatal("Computer omitted its live display endpoint")
	}
	before := readLiveInputObservation(t, computer.CurrentJobID)
	view := openLiveRFBSession(t, *computer.DisplayEndpoint, viewerUser, viewerDevice)
	defer view.close()
	view.sendPointer(t, 211, 173)
	control := openLiveRFBSession(t, *computer.DisplayEndpoint, "linux-admin", "linux-input-sentinel-device")
	defer control.close()
	controlCapability := control.capabilityFile(t)
	take := runComputerCLIPerson[contract.ComputerControlReceipt](t, harness, "linux-admin", "linux-input-sentinel-device",
		"services", "takeover", "take", computer.ComputerID, "--session-token-file", controlCapability)
	if take.TenureState != contract.ComputerControlTenureHeld {
		t.Fatalf("control sentinel take = %#v", take)
	}
	control.sendPointer(t, 947, 611)
	var after liveInputObservation
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		after = readLiveInputObservation(t, computer.CurrentJobID)
		if after.Generation > before.Generation && observationHasPointer(after, 947, 611) {
			break
		}
		time.Sleep(125 * time.Millisecond)
	}
	_ = runComputerCLIPerson[contract.ComputerControlReceipt](t, harness, "linux-admin", "linux-input-sentinel-device",
		"services", "takeover", "release", computer.ComputerID, "--session-token-file", controlCapability)
	return after.Generation > before.Generation && observationHasPointer(after, 947, 611) &&
		!observationHasPointer(after, 211, 173) && after.KeyEvents == before.KeyEvents
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

func inspectLiveComputerDetachment(t *testing.T, computer l1.Computer) bool {
	t.Helper()
	output, err := exec.Command("sudo", "grep", "-R", "-l", `\"previous_detachment\"\|\"preparation_receipt\"`,
		"/var/lib/wefty/oci/computer-disks").CombinedOutput()
	return err == nil && len(bytes.TrimSpace(output)) > 0 && computer.StorageGeneration > 1
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

func inspectHelperNamespaceInventory(t *testing.T, socketPath, checksum string) ocihelper.ResourceInventory {
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
				t.Fatalf("helper namespace retained residue: %#v", verification.Inventory)
			}
			return verification.Inventory
		}
		lastErr = err
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("acquire independent helper inventory session: %v", lastErr)
	return ocihelper.ResourceInventory{}
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
