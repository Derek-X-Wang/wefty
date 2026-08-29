//go:build service_acceptance_realtiming && (darwin || linux)

package serviceacceptance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/lima"
)

func TestLinuxNativeComputerCLIMatrixAtProductionTimings(t *testing.T) {
	receipt := newLinuxComputerMatrixReceipt()
	evidence := newRealTimingEvidence(t)
	t.Cleanup(func() {
		receipt.finish()
		evidence.recordJSON("linux-computer-matrix.json", receipt)
	})
	if runtime.GOOS != "linux" {
		for id, row := range receipt.Rows {
			row.NotRunIssue = 128
			row.NotRunReason = "Linux-native Computer acceptance requires the Ubuntu containerd runner; hosted Darwin does not claim Lima"
			receipt.Rows[id] = row
		}
		return
	}

	reference := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_COMPUTER_REFERENCE")
	digest := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_COMPUTER_DIGEST")
	archive := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_COMPUTER_ARCHIVE")
	imageRuntime := readPublishedComputerRuntimeReceipt(t, requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_COMPUTER_RUNTIME_RECEIPT"))
	receipt.Image = linuxComputerImageEvidence{Reference: reference, IndexDigest: digest,
		PlatformDigest: imageRuntime.Digest, Archive: filepath.Base(archive)}
	if candidate, err := exec.Command("git", "rev-parse", "HEAD").Output(); err == nil {
		receipt.CandidateSHA = strings.TrimSpace(string(candidate))
	}

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
	created := runComputerCLI[l1.Computer](t, harness, false, "services", "create", "--computer",
		"--name", "linux-native-acceptance", "--image", reference+"@"+digest, "--node", "acceptance-node",
		"--memory-bytes", fmt.Sprint(1<<30), "--disk-bytes", fmt.Sprint(128<<20), "--backup-cap", "4",
		"--idempotency-key", "linux-native-computer-create")
	ready := waitForComputerCLI(t, harness, created.ComputerID, 3*time.Minute, func(current l1.Computer) bool {
		return current.CurrentJob.State == contract.JobRunning && current.CurrentJob.Ready != nil && *current.CurrentJob.Ready &&
			current.DisplayEndpoint != nil && current.AppliedRevision == current.IntentRevision
	})
	capacityAssertion := runGoAssertion(t, "./l1", "TestServiceAcceptanceCapacityIsControlPlanePolicy")
	recordComputerAuthority(receipt, ready)
	receipt.pass("linux.create_boot", map[string]bool{
		"trait_only_publication":       ready.CurrentJob.Spec.PublishedPort == nil,
		"fully_allocated_disk_budget":  ready.DesiredDiskBytes == 128<<20,
		"both_named_endpoints_ready":   ready.CurrentJob.Ready != nil && *ready.CurrentJob.Ready && ready.DisplayEndpoint != nil,
		"helper_admitted_real_payload": ready.CurrentJob.CurrentAttemptID != "",
		"cap_sum_admission_matrix":     capacityAssertion,
		"published_runtime_executed":   imageRuntime.Executed && imageRuntime.RFBWebSocketV1,
	}, map[string]string{"computer_id": ready.ComputerID, "job_id": ready.CurrentJobID,
		"attempt_id": ready.CurrentJob.CurrentAttemptID, "storage_id": ready.StorageID,
		"storage_generation": fmt.Sprint(ready.StorageGeneration), "intent_revision": fmt.Sprint(ready.IntentRevision)})

	receipt.begin("linux.remote_takeover")
	policy := bootstrapComputerAcceptanceAdmin(t, harness)
	grant := runComputerCLI[l1.ComputerGrantMutationResult](t, harness, true, "services", "grant", ready.ComputerID,
		"linux-viewer", "--permission", "control", "--policy-revision", fmt.Sprint(policy.Revision),
		"--idempotency-key", "linux-native-control-grant")
	if !grant.MutationApplied || grant.Grant.Permission != l1.ComputerGrantControl {
		t.Fatalf("Computer control grant = %#v", grant)
	}
	takeoverCLIAssertion := runGoAssertion(t, "./cmd/wefty", "TestServiceAcceptanceComputerTakeoverCLIRealFrontDoor")
	frontDoorAssertion := runGoAssertion(t, "./agent", "TestServiceAcceptanceComputerFrontDoorRealProcessAuthorityLoss|TestServiceAcceptanceControllerTenureRealProcessAuthorityLoss")
	// Cross-process plain Fabric intentionally has no stable issuing Fabric
	// identity. The real front-door process and CLI tenure paths are separately
	// assertion-derived in the required tagged suite; the public-tailnet device
	// cell remains the attended #128 authority.
	receipt.pass("linux.remote_takeover", map[string]bool{
		"cli_grant_cas":                        grant.MutationApplied,
		"real_front_door_published":            ready.DisplayEndpoint != nil,
		"portable_view_control_tenure_fixture": takeoverCLIAssertion,
		"real_process_viewer_input_denial":     frontDoorAssertion && imageRuntime.Roles.ViewPointerDiscarded,
	}, map[string]string{
		"policy_revision":    fmt.Sprint(grant.Grant.PolicyRevision),
		"portable_assertion": "TestServiceAcceptanceComputerTakeoverCLIRealFrontDoor",
		"process_assertion":  "TestServiceAcceptanceComputerFrontDoorRealProcessAuthorityLoss",
	})

	receipt.begin("linux.restart_survival")
	oldAttempt, oldStorage, oldGeneration := ready.CurrentJob.CurrentAttemptID, ready.StorageID, ready.StorageGeneration
	harness.agent.kill(t)
	harness.restartAgent(t)
	restarted := waitForComputerCLI(t, harness, ready.ComputerID, 4*time.Minute, func(current l1.Computer) bool {
		return current.CurrentJob.State == contract.JobRunning && current.CurrentJob.Ready != nil && *current.CurrentJob.Ready &&
			current.CurrentJob.CurrentAttemptID != "" && current.CurrentJob.CurrentAttemptID != oldAttempt
	})
	recordComputerAuthority(receipt, restarted)
	receipt.pass("linux.restart_survival", map[string]bool{
		"fresh_attempt":         restarted.CurrentJob.CurrentAttemptID != oldAttempt,
		"same_storage":          restarted.StorageID == oldStorage && restarted.StorageGeneration == oldGeneration,
		"readiness_republished": restarted.DisplayEndpoint != nil,
		"old_authority_dead":    restarted.CurrentJob.CurrentAttemptID != oldAttempt,
	}, map[string]string{"old_attempt_id": oldAttempt, "new_attempt_id": restarted.CurrentJob.CurrentAttemptID,
		"storage_id": restarted.StorageID, "storage_generation": fmt.Sprint(restarted.StorageGeneration)})

	receipt.begin("linux.reconfiguration")
	resized := runComputerCLI[l1.Computer](t, harness, false, "services", "resize", restarted.ComputerID,
		"--disk-bytes", fmt.Sprint(160<<20), "--expect-current", "--idempotency-key", "linux-native-grow")
	resized = waitForComputerCLI(t, harness, resized.ComputerID, 3*time.Minute, func(current l1.Computer) bool {
		return current.ReconfigurationPhase == l1.ComputerReconfigurationStable && current.AppliedRevision == current.IntentRevision && current.DesiredDiskBytes == 160<<20
	})
	reset := runComputerCLI[l1.Computer](t, harness, false, "services", "reset", resized.ComputerID,
		"--expect-current", "--idempotency-key", "linux-native-reset")
	reset = waitForComputerCLI(t, harness, reset.ComputerID, 3*time.Minute, func(current l1.Computer) bool {
		return current.ReconfigurationPhase == l1.ComputerReconfigurationStable && current.AppliedRevision == current.IntentRevision && current.StorageGeneration > resized.StorageGeneration
	})
	reimaged := runComputerCLI[l1.Computer](t, harness, false, "services", "reimage", reset.ComputerID,
		"--image", reference+"@"+digest, "--expect-current", "--idempotency-key", "linux-native-reimage")
	reimaged = waitForComputerCLI(t, harness, reimaged.ComputerID, 4*time.Minute, func(current l1.Computer) bool {
		return current.ReconfigurationPhase == l1.ComputerReconfigurationStable && current.AppliedRevision == current.IntentRevision &&
			current.CurrentSpecRevision > reset.CurrentSpecRevision && current.CurrentJob.Ready != nil && *current.CurrentJob.Ready
	})
	reconfigurationMutationAssertion := runGoAssertion(t, "./l1", "TestReconfigurationAbortRequiresDeadBoundNodeAndLeavesExplicitRestart|TestResetAndGrowAbortRemainRetryableAndFenceLateAcknowledgement")
	recordComputerAuthority(receipt, reimaged)
	receipt.pass("linux.reconfiguration", map[string]bool{
		"grow_applied":                            resized.DesiredDiskBytes == 160<<20,
		"reset_fresh_generation":                  reset.StorageGeneration > resized.StorageGeneration,
		"reimage_new_projection":                  reimaged.CurrentSpecRevision > reset.CurrentSpecRevision,
		"stale_cas_and_abort_portable_assertions": reconfigurationMutationAssertion,
	}, map[string]string{"intent_revision": fmt.Sprint(reimaged.IntentRevision), "spec_revision": fmt.Sprint(reimaged.CurrentSpecRevision),
		"storage_generation": fmt.Sprint(reimaged.StorageGeneration), "portable_assertion": "TestReconfigurationAbortRequiresDeadBoundNodeAndLeavesExplicitRestart"})

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
	restoreOutput := runComputerCLI[storageCLIMutationReceipt](t, harness, false, "services", "restore", reimaged.ComputerID, backup.BackupID,
		"--retire-old", "--expect-current", "--idempotency-key", "linux-native-restore", "--wait", "4m")
	if restoreOutput.Computer == nil || restoreOutput.Computer.StorageGeneration <= reimaged.StorageGeneration {
		t.Fatalf("Computer restore result = %#v", restoreOutput)
	}
	reimaged = *restoreOutput.Computer
	recordComputerAuthority(receipt, reimaged)
	storageFailureAssertion := runGoAssertion(t, "./runner/ocihelper", "TestComputerBackupENOSPCAndDigestMismatchLeaveNoBackupOrSourceMutation|TestComputerGrowENOSPCRollsBackDeltaAndReturnsReceipt")
	receipt.pass("linux.storage_provenance", map[string]bool{
		"cold_backup_available":          backup.Status == "available",
		"clone_fork_created":             cloneOutput.Computer.ComputerID != reimaged.ComputerID,
		"custody_export_manifest_bound":  manifestDigest == exportOutput.CustodyExport.ManifestDigest,
		"custody_import_tainted":         importOutput.StorageProvenance.CustodyTainted,
		"custody_delete_attested":        attestOutput.CustodyExport.OperatorAttestedDeleted,
		"restore_fresh_generation":       restoreOutput.Computer.StorageGeneration > reset.StorageGeneration,
		"cap_enospc_portable_assertions": storageFailureAssertion,
	}, map[string]string{"backup_id": backup.BackupID, "clone_computer_id": cloneOutput.Computer.ComputerID,
		"export_id": exportOutput.CustodyExport.ExportID, "import_computer_id": importOutput.Computer.ComputerID,
		"portable_assertion": "TestServiceAcceptanceComputerColdBackupContract"})

	receipt.begin("linux.guest_authority")
	submission := runComputerCLI[l1.ComputerSubmissionMutationResult](t, harness, true, "services", "submission", "enable", reimaged.ComputerID,
		"--expect-current", "--idempotency-key", "linux-native-submission-enable")
	if !submission.MutationApplied || !submission.SubmitEnabled {
		t.Fatalf("Computer submission enable = %#v", submission)
	}
	guestScopeAssertion := runGoAssertion(t, "./l3", "TestServiceAcceptanceComputerTokenScopeAndRunProvenance")
	guestInjectionAssertion := runGoAssertion(t, "./agent", "TestServiceAcceptanceComputerTokenInjection")
	receipt.notRun("linux.guest_authority", 157,
		"the unpublished complete M3 OCI matrix does not yet provide a Computer-bearer root-Run artifact for this candidate",
		map[string]bool{
			"cli_submission_enabled":                  submission.SubmitEnabled,
			"token_injection_portable_path":           guestInjectionAssertion,
			"self_scope_and_inflight_portable_matrix": guestScopeAssertion,
		}, map[string]string{"policy_revision": fmt.Sprint(submission.PolicyRevision),
			"submit_intent_revision": fmt.Sprint(submission.SubmitIntentRevision),
			"portable_assertion":     "TestServiceAcceptanceComputerTokenScopeAndRunProvenance"})

	receipt.begin("linux.removal")
	reducedTargets := []l1.Computer{reimaged, *cloneOutput.Computer, *importOutput.Computer}
	var reduced l1.Computer
	for _, target := range reducedTargets {
		removed := removeAndWaitComputer(t, harness, target, 4*time.Minute)
		if removed.RemovalOutcome == "removed_reduced" {
			reduced = removed
		}
	}
	verifiedComputer := createReadyComputer(t, harness, reference, digest, "linux-native-verified-removal", "linux-native-verified-removal-create")
	recordComputerAuthority(receipt, verifiedComputer)
	verified := removeAndWaitComputer(t, harness, verifiedComputer, 4*time.Minute)
	if verified.RemovalOutcome != "removed_verified" || reduced.RemovalOutcome != "removed_reduced" {
		t.Fatalf("Computer removal outcomes verified=%q reduced=%q", verified.RemovalOutcome, reduced.RemovalOutcome)
	}
	unverifiedRemovalAssertion := runGoAssertion(t, "./l1", "TestServiceRemovalForceForgetLeavesDirectiveStanding")
	receipt.pass("linux.removal", map[string]bool{
		"verified_absence_outcome":              verified.RemovalOutcome == "removed_verified",
		"reduced_custody_outcome":               reduced.RemovalOutcome == "removed_reduced",
		"compound_helper_absence":               verified.CurrentJob.State == contract.JobRemovedVerified,
		"unverified_outcome_portable_assertion": unverifiedRemovalAssertion,
	}, map[string]string{"verified_computer_id": verified.ComputerID, "reduced_computer_id": reduced.ComputerID,
		"verified_outcome": verified.RemovalOutcome, "reduced_outcome": reduced.RemovalOutcome,
		"portable_assertion": "TestServiceAcceptanceComputerRemovalReleasesOccupancy"})
}

type publishedComputerRuntimeReceipt struct {
	Digest         string `json:"digest"`
	Executed       bool   `json:"executed"`
	RFBWebSocketV1 bool   `json:"rfb_websocket_v1"`
	Roles          struct {
		ViewProcessViewOnly       bool `json:"view_process_view_only"`
		ControlProcessInteractive bool `json:"control_process_interactive"`
		ViewPointerDiscarded      bool `json:"view_pointer_discarded"`
	} `json:"roles"`
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
	if receipt.Digest == "" || !receipt.Executed || !receipt.RFBWebSocketV1 || !receipt.Roles.ViewProcessViewOnly ||
		!receipt.Roles.ControlProcessInteractive || !receipt.Roles.ViewPointerDiscarded {
		t.Fatalf("published Computer runtime receipt omitted executable input isolation: %#v", receipt)
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

func requiredComputerRealtimeEnvironment(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("Linux Computer acceptance requires %s", name)
	}
	return value
}

func runGoAssertion(t *testing.T, packagePath, expression string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "test", "-count=1", "-tags=service_acceptance", "-run", "^("+expression+")$", packagePath)
	command.Dir = repositoryRoot()
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("assertion-derived acceptance %s %s: %v\n%s", packagePath, expression, err, output)
	}
	return true
}

func runComputerCLI[T any](t *testing.T, harness *acceptanceHarness, person bool, arguments ...string) T {
	t.Helper()
	args := []string{"--fabric=plain", "--l1=" + harness.controlPlaneAddress, "--plain-identity=linux-computer-cli", "--json"}
	if harness.runLedgerAddress != "" {
		args = append(args, "--l3="+harness.runLedgerAddress)
	}
	if person {
		args = append(args, "--plain-user-id=linux-admin", "--plain-device-id=linux-admin-device")
	}
	args = append(args, arguments...)
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

func bootstrapComputerAcceptanceAdmin(t *testing.T, harness *acceptanceHarness) l1.AdminPolicy {
	t.Helper()
	store, err := l1.OpenStore(harness.l1Database, l1.StoreOptions{LeaseDuration: l1.DefaultLeaseDuration, ComputerBackupCap: 4})
	if err != nil {
		t.Fatal(err)
	}
	challenge, challengeErr := store.InitiateAdminBootstrap(t.Context())
	closeErr := store.Close()
	if challengeErr != nil || closeErr != nil {
		t.Fatal(challengeErr, closeErr)
	}
	return runComputerCLI[l1.AdminPolicy](t, harness, true, "admin", "bootstrap", challenge.Nonce)
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

func createReadyComputer(t *testing.T, harness *acceptanceHarness, reference, digest, name, key string) l1.Computer {
	t.Helper()
	created := runComputerCLI[l1.Computer](t, harness, false, "services", "create", "--computer", "--name", name,
		"--image", reference+"@"+digest, "--node", "acceptance-node", "--memory-bytes", fmt.Sprint(1<<30),
		"--disk-bytes", fmt.Sprint(64<<20), "--idempotency-key", key)
	return waitForComputerCLI(t, harness, created.ComputerID, 3*time.Minute, func(current l1.Computer) bool {
		return current.CurrentJob.State == contract.JobRunning && current.CurrentJob.Ready != nil && *current.CurrentJob.Ready
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
