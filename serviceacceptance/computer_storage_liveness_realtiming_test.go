//go:build service_acceptance_realtiming && (darwin || linux)

package serviceacceptance

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

// The Computer Storage liveness proofs of the Linux matrix (#1624, #1634).
// Each one makes the real helper fail on a real Node with a root filesystem
// fault from the lane's fault supervisor, and reads how the operation ended
// from L1 through the CLI. No product binary carries a fault hook.

const liveStorageDiskBytes = 64 << 20

const liveStorageRuntimeRoot = "/var/lib/wefty/oci"

// storageCLIOutcome is what an awaited Storage command prints. A command
// whose operation failed still prints it, then exits non-zero.
type storageCLIOutcome struct {
	storageCLIMutationReceipt
	CustodyImport *l1.ComputerCustodyImport `json:"custody_import,omitempty"`
	Stderr        string                    `json:"-"`
	Failed        bool                      `json:"-"`
}

// runStorageCLIOutcome runs an awaited Storage command that may end in a
// failed operation. It requires the printed outcome either way; the caller
// decides which ending it wanted.
func runStorageCLIOutcome(t *testing.T, harness *acceptanceHarness, arguments ...string) storageCLIOutcome {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, weftyBinaryPath, computerCLIArguments(harness, "", "", arguments...)...)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	runErr := command.Run()
	var outcome storageCLIOutcome
	if err := json.Unmarshal(stdout.Bytes(), &outcome); err != nil {
		t.Fatalf("decode Storage CLI %v outcome: %v (run error %v)\nstdout:\n%s\nstderr:\n%s",
			arguments, err, runErr, stdout.String(), stderr.String())
	}
	outcome.Stderr, outcome.Failed = stderr.String(), runErr != nil
	return outcome
}

// liveStorageFault holds one root fault for the duration of a scenario. The
// clearing action is idempotent, so the cleanup also covers a scenario that
// failed before it cleared the fault itself.
func liveStorageFault(t *testing.T, harness *acceptanceHarness, set, clear string) func() {
	t.Helper()
	fifo := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_FAULT_FIFO")
	directory := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_FAULT_DIR")
	cleared := false
	clearFault := func() error {
		if cleared {
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), linuxRootFaultBound)
		defer cancel()
		if err := requestLinuxRootFault(ctx, fifo, directory, clear); err != nil {
			return err
		}
		cleared = true
		return nil
	}
	t.Cleanup(func() {
		if err := clearFault(); err != nil {
			t.Errorf("clear root Storage fault %s: %v", clear, err)
		}
	})
	triggerLinuxComputerFault(t, harness, set)
	return func() {
		t.Helper()
		if err := clearFault(); err != nil {
			t.Fatalf("clear root Storage fault %s: %v", clear, err)
		}
	}
}

// storageBudgetSpent is the rule of #1608 as an operation's read model shows
// it: the class of the last report, at least the full count, and at least the
// window between the first and the last counted failure.
func storageBudgetSpent(failure *l1.ComputerStorageOperationFailure, stage l1.ComputerStorageOperationStage) bool {
	return failure != nil && failure.Stage == stage && failure.Class == l1.ComputerStorageOperationFailureEngine &&
		failure.FailedAttempts >= l1.MaximumStorageOperationFailures && failure.FirstFailedAt != nil &&
		failure.LastFailedAt.Sub(*failure.FirstFailedAt) >= acceptanceStorageFailureWindow
}

func storageBudgetDetail(failure *l1.ComputerStorageOperationFailure) string {
	if failure == nil {
		return "no failure budget recorded"
	}
	elapsed := "unknown"
	if failure.FirstFailedAt != nil {
		elapsed = failure.LastFailedAt.Sub(*failure.FirstFailedAt).String()
	}
	return fmt.Sprintf("stage=%s class=%s code=%s failed_attempts=%d elapsed=%s", failure.Stage, failure.Class, failure.Code,
		failure.FailedAttempts, elapsed)
}

// latchedStorageOperationFailure reads the failure L1 latches on a clone or
// import destination when the budget ends the operation. Its message is L1's
// own sentence: verb, stage, attempt count, last class and code.
func latchedStorageOperationFailure(computer l1.Computer) (verb, stage string, attempts int64, class string, ok bool) {
	var failure contract.SpawnFailure
	if json.Unmarshal(computer.CurrentJob.LastFailure, &failure) != nil ||
		failure.Code != contract.SpawnFailureComputerStorageOperationFailed {
		return "", "", 0, "", false
	}
	if _, err := fmt.Sscanf(failure.Message, "Computer %s %s stage ended after %d failed attempts; last: %s", &verb, &stage, &attempts, &class); err != nil {
		return "", "", 0, "", false
	}
	return verb, stage, attempts, class, true
}

func createLiveStorageComputer(t *testing.T, harness *acceptanceHarness, reference, digest, name string) l1.Computer {
	t.Helper()
	created := runComputerCLI[l1.Computer](t, harness, false, "services", "create", "--computer", "--name", name,
		"--image", reference+"@"+digest, "--node", "acceptance-node", "--memory-bytes", fmt.Sprint(1<<30),
		"--disk-bytes", fmt.Sprint(liveStorageDiskBytes), "--backup-cap", "4", "--idempotency-key", name+"-create")
	return waitForComputerCLI(t, harness, created.ComputerID, 3*time.Minute, computerDisplayPublished)
}

func stopLiveStorageComputer(t *testing.T, harness *acceptanceHarness, computer l1.Computer) l1.Computer {
	t.Helper()
	stopping := runComputerCLI[l1.Computer](t, harness, false, "services", "stop", computer.ComputerID, "--expect-current")
	return waitForComputerCLI(t, harness, stopping.ComputerID, 3*time.Minute, computerStoppedAfterExplicitStop)
}

func startLiveStorageComputer(t *testing.T, harness *acceptanceHarness, computer l1.Computer) l1.Computer {
	t.Helper()
	starting := runComputerCLI[l1.Computer](t, harness, false, "services", "start", computer.ComputerID, "--expect-current")
	return waitForComputerCLI(t, harness, starting.ComputerID, 3*time.Minute, computerDisplayPublished)
}

func nodePathAbsent(t *testing.T, path string) bool {
	t.Helper()
	output, err := exec.Command("sudo", "sh", "-c", `if test -e "$1" || test -L "$1"; then echo present; else echo absent; fi`,
		"wefty-absence", path).CombinedOutput()
	if err != nil {
		t.Fatalf("observe %s on the Node: %v\n%s", path, err, output)
	}
	return strings.TrimSpace(string(output)) == "absent"
}

func liveComputerDiskImageBytes(t *testing.T, diskName string) int64 {
	t.Helper()
	path := filepath.Join(liveStorageRuntimeRoot, "computer-disks", diskName, "disk.ext4")
	output, err := exec.Command("sudo", "stat", "--format=%s", path).CombinedOutput()
	if err != nil {
		t.Fatalf("read live Computer disk size: %v\n%s", err, output)
	}
	size, err := strconv.ParseInt(strings.TrimSpace(string(output)), 10, 64)
	if err != nil {
		t.Fatalf("parse live Computer disk size %q: %v", output, err)
	}
	return size
}

func computerQuarantineEntries(t *testing.T, diskName string) int {
	t.Helper()
	output, err := exec.Command("sudo", "sh", "-c",
		`test -d "$1" || exit 0; find "$1" -mindepth 1 -maxdepth 1 -name "*$2*"`,
		"wefty-quarantine", filepath.Join(liveStorageRuntimeRoot, "computer-disk-quarantine"), diskName).CombinedOutput()
	if err != nil {
		t.Fatalf("list Computer disk quarantine entries: %v\n%s", err, output)
	}
	return len(strings.Fields(string(output)))
}

type liveNodeControlFacts struct {
	HelperSession   string
	PendingRemovals []string
}

func (facts liveNodeControlFacts) removalPending(jobIDs ...string) bool {
	for _, pending := range facts.PendingRemovals {
		for _, jobID := range jobIDs {
			if pending == jobID {
				return true
			}
		}
	}
	return false
}

func readLiveNodeControlFacts(t *testing.T, controlSocket string) liveNodeControlFacts {
	t.Helper()
	client, err := ocicontrol.NewClient(controlSocket)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	doctor, err := client.Doctor(t.Context())
	if err != nil {
		t.Fatalf("read the helper session from the agent doctor: %v", err)
	}
	removals, err := client.Removals(t.Context())
	if err != nil {
		t.Fatalf("read the Node's runtime removals: %v", err)
	}
	facts := liveNodeControlFacts{HelperSession: fmt.Sprintf("%s/%d", doctor.Helper.InstanceID, doctor.Helper.SessionGeneration)}
	if doctor.Helper.InstanceID == "" || doctor.Helper.SessionGeneration == 0 {
		facts.HelperSession = ""
	}
	for _, removal := range removals.Removals {
		facts.PendingRemovals = append(facts.PendingRemovals, removal.JobID)
	}
	return facts
}

type liveRotatedRemovalEvidence struct {
	ComputerID, FirstJobID, RotatedJobID, DiskName, Outcome string
	HelperSessionBefore, HelperSessionAfter                 string
	RotatedJobNeverRan, ResidueAbsent, NoPendingRemoval     bool
}

// exerciseLiveStopReimageRemove is the sequence of #1611: a Computer runs,
// is stopped, and a reimage replaces its Job while it stays stopped. The new
// Job never runs, so removal has only the detach evidence of the old Job's
// attempt to prove the generation is safe to delete.
func exerciseLiveStopReimageRemove(t *testing.T, harness *acceptanceHarness, controlSocket, reference, digest,
	reimageReference, reimageDigest string) liveRotatedRemovalEvidence {
	t.Helper()
	target := createLiveStorageComputer(t, harness, reference, digest, "linux-native-rotated-removal")
	evidence := liveRotatedRemovalEvidence{ComputerID: target.ComputerID, FirstJobID: target.CurrentJobID,
		DiskName: liveComputerDiskName(t, target)}
	_ = plantLiveProfileMarker(t, target, "rotated-removal-marker")
	evidence.HelperSessionBefore = readLiveNodeControlFacts(t, controlSocket).HelperSession
	stopped := stopLiveStorageComputer(t, harness, target)
	reimaged := runComputerCLI[l1.Computer](t, harness, false, "services", "reimage", stopped.ComputerID,
		"--image", reimageReference+"@"+reimageDigest, "--expect-current", "--idempotency-key", "linux-native-rotated-removal-reimage",
		"--terminate-sessions", "--chown")
	reimaged = waitForComputerCLI(t, harness, reimaged.ComputerID, 4*time.Minute, func(current l1.Computer) bool {
		failOnTypedReimagePreflight(t, current, stopped.CurrentSpecRevision)
		return current.ReconfigurationPhase == l1.ComputerReconfigurationStable && current.AppliedRevision == current.IntentRevision &&
			current.CurrentSpecRevision > stopped.CurrentSpecRevision
	})
	evidence.RotatedJobID = reimaged.CurrentJobID
	evidence.RotatedJobNeverRan = reimaged.CurrentJobID != target.CurrentJobID && reimaged.CurrentJob.CurrentAttemptID == "" &&
		reimaged.CurrentJob.State == contract.JobStopped && reimaged.StorageGeneration == target.StorageGeneration
	removed := removeAndWaitComputer(t, harness, reimaged, 5*time.Minute)
	evidence.Outcome = removed.RemovalOutcome
	evidence.ResidueAbsent = nodePathAbsent(t, filepath.Join(liveStorageRuntimeRoot, "computer-disks", evidence.DiskName)) &&
		nodePathAbsent(t, filepath.Join(liveStorageRuntimeRoot, "computer-mounts", evidence.DiskName)) &&
		computerQuarantineEntries(t, evidence.DiskName) == 0
	// The agent releases its removal record when it has processed L1's
	// acknowledgement, which the CLI can see a moment earlier.
	after := readLiveNodeControlFacts(t, controlSocket)
	for deadline := time.Now().Add(30 * time.Second); after.removalPending(evidence.FirstJobID, evidence.RotatedJobID) && time.Now().Before(deadline); {
		time.Sleep(500 * time.Millisecond)
		after = readLiveNodeControlFacts(t, controlSocket)
	}
	evidence.HelperSessionAfter = after.HelperSession
	evidence.NoPendingRemoval = !after.removalPending(evidence.FirstJobID, evidence.RotatedJobID)
	return evidence
}

type liveStorageBudgetEvidence struct {
	Passed bool
	Detail string
}

// exerciseLiveGrowBudget freezes the generation root, so the helper cannot
// write its grow intent. The grow must end `failed` by the budget with the
// disk at its old size.
func exerciseLiveGrowBudget(t *testing.T, harness *acceptanceHarness, reference, digest string) liveStorageBudgetEvidence {
	t.Helper()
	target := createLiveStorageComputer(t, harness, reference, digest, "linux-native-grow-budget")
	diskName := liveComputerDiskName(t, target)
	thaw := liveStorageFault(t, harness, "freeze-computer-disk:"+diskName, "thaw-computer-disk:"+diskName)
	growing := runComputerCLI[l1.Computer](t, harness, false, "services", "resize", target.ComputerID,
		"--disk-bytes", fmt.Sprint(96<<20), "--expect-current", "--idempotency-key", "linux-native-grow-budget")
	ended := waitForComputerCLI(t, harness, target.ComputerID, 5*time.Minute, func(current l1.Computer) bool {
		return current.IntentRevision >= growing.IntentRevision && current.LastGrowOperation != nil &&
			strings.EqualFold(current.LastGrowOperation.Status, "failed") &&
			current.ReconfigurationPhase == l1.ComputerReconfigurationStable
	})
	thaw()
	imageBytes := liveComputerDiskImageBytes(t, diskName)
	evidence := liveStorageBudgetEvidence{
		Passed: storageBudgetSpent(ended.LastGrowOperation.Failure, l1.ComputerStorageOperationPrepare) &&
			ended.DesiredDiskBytes == liveStorageDiskBytes && imageBytes == liveStorageDiskBytes,
		Detail: fmt.Sprintf("%s desired_disk_bytes=%d image_bytes=%d", storageBudgetDetail(ended.LastGrowOperation.Failure),
			ended.DesiredDiskBytes, imageBytes)}
	_ = removeAndWaitComputer(t, harness, ended, 5*time.Minute)
	return evidence
}

// exerciseLiveResetBudget freezes the stopped generation's root, so the
// helper cannot write the retirement fence: the reset fails before the fence.
// Owner decision D1 (#1608): the failures are counted and shown, and the reset
// is not ended. When the fault clears, the same reset completes.
func exerciseLiveResetBudget(t *testing.T, harness *acceptanceHarness, reference, digest string) liveStorageBudgetEvidence {
	t.Helper()
	target := stopLiveStorageComputer(t, harness, createLiveStorageComputer(t, harness, reference, digest, "linux-native-reset-budget"))
	diskName := liveComputerDiskName(t, target)
	thaw := liveStorageFault(t, harness, "freeze-computer-disk:"+diskName, "thaw-computer-disk:"+diskName)
	resetting := runComputerCLI[l1.Computer](t, harness, false, "services", "reset", target.ComputerID,
		"--expect-current", "--idempotency-key", "linux-native-reset-budget", "--terminate-sessions")
	counted := waitForComputerCLI(t, harness, target.ComputerID, 5*time.Minute, func(current l1.Computer) bool {
		return current.IntentRevision >= resetting.IntentRevision && current.LastResetOperation != nil &&
			storageBudgetSpent(current.LastResetOperation.Failure, l1.ComputerStorageOperationPrepare)
	})
	// The budget is spent. One more counted failure must leave the reset
	// running on the old generation.
	held := waitForComputerCLI(t, harness, target.ComputerID, 3*time.Minute, func(current l1.Computer) bool {
		return current.LastResetOperation == nil || current.LastResetOperation.Failure == nil ||
			current.LastResetOperation.Failure.FailedAttempts > counted.LastResetOperation.Failure.FailedAttempts
	})
	notEnded := held.ReconfigurationPhase == l1.ComputerReconfigurationResetting && held.LastResetOperation != nil &&
		held.LastResetOperation.Failure != nil && held.LastResetOperation.CompletedAt == nil &&
		held.StorageGeneration == target.StorageGeneration
	thaw()
	healed := waitForComputerCLI(t, harness, target.ComputerID, 5*time.Minute, func(current l1.Computer) bool {
		return current.ReconfigurationPhase == l1.ComputerReconfigurationStable && current.AppliedRevision == current.IntentRevision &&
			current.StorageGeneration > target.StorageGeneration
	})
	evidence := liveStorageBudgetEvidence{Passed: notEnded,
		Detail: fmt.Sprintf("%s held_phase=%s held_status=%s healed_generation=%d", storageBudgetDetail(counted.LastResetOperation.Failure),
			held.ReconfigurationPhase, held.LastResetOperation.Status, healed.StorageGeneration)}
	_ = removeAndWaitComputer(t, harness, healed, 5*time.Minute)
	return evidence
}

// exerciseLiveImportBudget refuses every new generation root while a Custody
// import copies. The import must end failed by the budget, with the
// destination latched failed and removable. The awaited CLI is not the end
// signal: it reports the first interrupted preparation while L1 still counts,
// so the scenario holds the fault until L1 latches the destination.
func exerciseLiveImportBudget(t *testing.T, harness *acceptanceHarness, exportID, exportDirectory, manifestPath, manifestDigest string) liveStorageBudgetEvidence {
	t.Helper()
	thaw := liveStorageFault(t, harness, "freeze-storage-roots", "thaw-storage-roots")
	outcome := runComputerCLI[storageCLIOutcome](t, harness, false, "services", "custody", "import", exportID,
		"--name", "linux-native-import-budget", "--disk-bytes", fmt.Sprint(160<<20), "--node", "acceptance-node",
		"--path", exportDirectory, "--manifest", manifestPath, "--manifest-digest", manifestDigest,
		"--idempotency-key", "linux-native-import-budget")
	if outcome.CustodyImport == nil || outcome.CustodyImport.DestinationComputerID == "" {
		t.Fatalf("Custody import under a root fault printed no import: %#v", outcome)
	}
	destination := waitForComputerCLI(t, harness, outcome.CustodyImport.DestinationComputerID, 5*time.Minute, func(current l1.Computer) bool {
		_, _, _, _, latched := latchedStorageOperationFailure(current)
		return latched && current.ReconfigurationPhase == l1.ComputerReconfigurationStable
	})
	thaw()
	verb, stage, attempts, class, _ := latchedStorageOperationFailure(destination)
	budget := readLiveStorageOperationBudget(t, harness, destination.ComputerID, l1.ComputerStorageOperationPrepare)
	evidence := liveStorageBudgetEvidence{
		Passed: verb == string(l1.ComputerStorageOperationImport) && stage == string(l1.ComputerStorageOperationPrepare) &&
			class == string(l1.ComputerStorageOperationFailureEngine) && attempts >= l1.MaximumStorageOperationFailures &&
			storageBudgetSpent(budget, l1.ComputerStorageOperationPrepare),
		Detail: fmt.Sprintf("latched verb=%s stage=%s class=%s failed_attempts=%d; %s",
			verb, stage, class, attempts, storageBudgetDetail(budget))}
	removed := removeAndWaitComputer(t, harness, destination, 5*time.Minute)
	evidence.Detail += " removal_outcome=" + removed.RemovalOutcome
	return evidence
}

// readLiveStorageOperationBudget reads one operation stage's failure budget
// from L1's own table. No CLI read carries a Custody import's budget, and the
// awaited import ends too early to be the witness, so the lane reads the
// durable row the way it reads the reimage preflight receipt.
func readLiveStorageOperationBudget(t *testing.T, harness *acceptanceHarness, computerID string, stage l1.ComputerStorageOperationStage) *l1.ComputerStorageOperationFailure {
	t.Helper()
	database, err := sql.Open("sqlite", harness.l1Database)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var class, code string
	var attempts, lastNS int64
	var firstNS sql.NullInt64
	err = database.QueryRow(`SELECT last_failure_class, last_failure_code, failed_attempts, first_failed_ns, last_failed_ns
		FROM computer_storage_operation_failures WHERE computer_id=? AND stage=? ORDER BY operation_revision DESC LIMIT 1`,
		computerID, string(stage)).Scan(&class, &code, &attempts, &firstNS, &lastNS)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		t.Fatalf("read the Storage operation failure budget of %s: %v", computerID, err)
	}
	failure := &l1.ComputerStorageOperationFailure{Stage: stage, Class: l1.ComputerStorageOperationFailureClass(class), Code: code,
		FailedAttempts: attempts, LastFailedAt: time.Unix(0, lastNS)}
	if firstNS.Valid {
		first := time.Unix(0, firstNS.Int64)
		failure.FirstFailedAt = &first
	}
	return failure
}

type liveStorageProvenanceBudgets struct {
	ComputerID                                                      string
	Backup, RestoreQuarantine, Clone, RestoreCleanupOwed            liveStorageBudgetEvidence
	BackupResumed, RestoreKeptOldGeneration, SecondRestoreSucceeded bool
	CloneRemoved, CleanupOwedStarted                                bool
}

// exerciseLiveStorageProvenanceBudgets runs the Backup, clone and restore
// endings of #1608 on one Computer of its own.
func exerciseLiveStorageProvenanceBudgets(t *testing.T, harness *acceptanceHarness, reference, digest string) liveStorageProvenanceBudgets {
	t.Helper()
	source := createLiveStorageComputer(t, harness, reference, digest, "linux-native-storage-budget")
	result := liveStorageProvenanceBudgets{ComputerID: source.ComputerID}

	// Backup: no new Backup copy can be created. The Backup ends failed and
	// the Computer it powered off runs again.
	thaw := liveStorageFault(t, harness, "freeze-backup-roots", "thaw-backup-roots")
	failedBackup := runStorageCLIOutcome(t, harness, "services", "backup", "create", source.ComputerID,
		"--expect-current", "--allow-power-off", "--idempotency-key", "linux-native-backup-budget", "--wait", "6m")
	thaw()
	afterBackup := waitForComputerCLI(t, harness, source.ComputerID, 3*time.Minute, func(current l1.Computer) bool {
		return current.ReconfigurationPhase == l1.ComputerReconfigurationStable && computerDisplayPublished(current)
	})
	result.BackupResumed = computerDisplayPublished(afterBackup)
	backupOperation := afterBackup.LastBackupOperation
	result.Backup = liveStorageBudgetEvidence{Detail: "no Backup operation recorded"}
	if backupOperation != nil {
		result.Backup = liveStorageBudgetEvidence{
			Passed: failedBackup.Failed && backupOperation.Status == "failed" &&
				storageBudgetSpent(backupOperation.Failure, l1.ComputerStorageOperationPrepare),
			Detail: "status=" + backupOperation.Status + " " + storageBudgetDetail(backupOperation.Failure)}
	}

	created := runComputerCLI[storageCLIMutationReceipt](t, harness, false, "services", "backup", "create", source.ComputerID,
		"--expect-current", "--allow-power-off", "--idempotency-key", "linux-native-budget-source-backup", "--wait", "4m")
	backupID := ""
	if created.Backups != nil {
		for _, candidate := range created.Backups.Backups {
			if candidate.Status == "available" {
				backupID = candidate.BackupID
			}
		}
	}
	if backupID == "" {
		t.Fatalf("no available Backup to copy from: %#v", created)
	}
	source = waitForComputerCLI(t, harness, source.ComputerID, 3*time.Minute, computerDisplayPublished)

	// Clone: no new generation root can be created. The clone ends failed,
	// its destination is latched failed, and removing it completes.
	thaw = liveStorageFault(t, harness, "freeze-storage-roots", "thaw-storage-roots")
	failedClone := runStorageCLIOutcome(t, harness, "services", "clone", source.ComputerID, backupID,
		"--name", "linux-native-clone-budget", "--disk-bytes", fmt.Sprint(liveStorageDiskBytes), "--expect-current",
		"--idempotency-key", "linux-native-clone-budget", "--wait", "6m")
	thaw()
	if failedClone.Computer == nil || failedClone.Computer.ComputerID == "" || failedClone.Computer.ComputerID == source.ComputerID {
		t.Fatalf("clone under a root fault printed no destination: %#v\n%s", failedClone, failedClone.Stderr)
	}
	result.Clone = liveStorageBudgetEvidence{Detail: "no clone operation recorded"}
	if operation := failedClone.Computer.CloneOperation; operation != nil {
		_, _, _, _, latched := latchedStorageOperationFailure(*failedClone.Computer)
		result.Clone = liveStorageBudgetEvidence{
			Passed: failedClone.Failed && operation.Status == "failed" && latched &&
				storageBudgetSpent(operation.Failure, l1.ComputerStorageOperationPrepare),
			Detail: fmt.Sprintf("status=%s latched=%t %s", operation.Status, latched, storageBudgetDetail(operation.Failure))}
	}
	removedClone := removeAndWaitComputer(t, harness, *failedClone.Computer, 5*time.Minute)
	result.CloneRemoved = removedClone.RemovalOutcome != ""
	result.Clone.Detail += " removal_outcome=" + removedClone.RemovalOutcome

	// Restore into a quarantined staging generation: quarantine never clears,
	// so the restore ends at once and the Computer keeps its old generation.
	stopped := stopLiveStorageComputer(t, harness, source)
	staging := stopped
	staging.StorageGeneration++
	stagingName := liveComputerDiskName(t, staging)
	clearQuarantine := liveStorageFault(t, harness,
		fmt.Sprintf("quarantine-computer-storage:%s:%s:%s:%d", stagingName, staging.ComputerID, staging.StorageID, staging.StorageGeneration),
		"clear-computer-storage-quarantine:"+stagingName)
	quarantinedRestore := runStorageCLIOutcome(t, harness, "services", "restore", stopped.ComputerID, backupID,
		"--retire-old", "--expect-current", "--idempotency-key", "linux-native-restore-quarantine", "--wait", "4m")
	clearQuarantine()
	result.RestoreQuarantine = liveStorageBudgetEvidence{Detail: "no restore operation recorded"}
	if quarantinedRestore.Computer != nil && quarantinedRestore.Computer.RestoreOperation != nil {
		operation := quarantinedRestore.Computer.RestoreOperation
		result.RestoreQuarantine = liveStorageBudgetEvidence{
			Passed: quarantinedRestore.Failed && operation.Status == "failed" &&
				operation.FailureCode == l1.ComputerRestoreFailureQuarantined && operation.Failure != nil &&
				operation.Failure.Class == l1.ComputerStorageOperationFailureQuarantined &&
				// One report: the quarantine ended it, not the spent budget.
				operation.Failure.Stage == l1.ComputerStorageOperationPrepare && operation.Failure.FailedAttempts == 1,
			Detail: fmt.Sprintf("status=%s failure_code=%s %s", operation.Status, operation.FailureCode, storageBudgetDetail(operation.Failure))}
	}
	onOldGeneration := startLiveStorageComputer(t, harness, stopped)
	result.RestoreKeptOldGeneration = onOldGeneration.StorageGeneration == stopped.StorageGeneration
	stopped = stopLiveStorageComputer(t, harness, onOldGeneration)
	secondRestore := runStorageCLIOutcome(t, harness, "services", "restore", stopped.ComputerID, backupID,
		"--retire-old", "--expect-current", "--idempotency-key", "linux-native-restore-after-quarantine", "--wait", "4m")
	if secondRestore.Failed || secondRestore.Computer == nil {
		t.Fatalf("restore after the cleared quarantine failed: %#v\n%s", secondRestore, secondRestore.Stderr)
	}
	result.SecondRestoreSucceeded = secondRestore.Computer.StorageGeneration > stopped.StorageGeneration
	restored := *secondRestore.Computer

	// Restore whose predecessor cannot be deleted. The deletion fails with a
	// permission error, which the adapter does not retry into a cleanup
	// quarantine, so each pass is one counted retire failure. The restore
	// ends `cleanup_owed` and the Computer starts on the new generation.
	predecessorName := liveComputerDiskName(t, restored)
	thawPredecessor := liveStorageFault(t, harness, "freeze-computer-disk:"+predecessorName, "thaw-computer-disk:"+predecessorName)
	owedRestore := runStorageCLIOutcome(t, harness, "services", "restore", restored.ComputerID, backupID,
		"--retire-old", "--expect-current", "--idempotency-key", "linux-native-restore-cleanup-owed", "--wait", "6m")
	thawPredecessor()
	result.RestoreCleanupOwed = liveStorageBudgetEvidence{Detail: "no restore operation recorded"}
	if owedRestore.Computer == nil {
		t.Fatalf("restore with a refused predecessor deletion printed no Computer: %#v\n%s", owedRestore, owedRestore.Stderr)
	}
	if operation := owedRestore.Computer.RestoreOperation; operation != nil {
		result.RestoreCleanupOwed = liveStorageBudgetEvidence{
			Passed: operation.Status == "cleanup_owed" && storageBudgetSpent(operation.Failure, l1.ComputerStorageOperationRetire) &&
				owedRestore.Computer.StorageGeneration > restored.StorageGeneration,
			Detail: fmt.Sprintf("status=%s %s", operation.Status, storageBudgetDetail(operation.Failure))}
	}
	onNewGeneration := startLiveStorageComputer(t, harness, *owedRestore.Computer)
	result.CleanupOwedStarted = onNewGeneration.StorageGeneration == owedRestore.Computer.StorageGeneration &&
		onNewGeneration.StorageGeneration > restored.StorageGeneration
	// Removal inventories every recorded generation, so it also deletes the
	// owed predecessor now that its root can be deleted again.
	_ = removeAndWaitComputer(t, harness, onNewGeneration, 5*time.Minute)
	result.RestoreCleanupOwed.Passed = result.RestoreCleanupOwed.Passed &&
		nodePathAbsent(t, filepath.Join(liveStorageRuntimeRoot, "computer-disks", predecessorName))
	return result
}

// readLiveStorageCopyOperation reads how L1 recorded a Computer's latest
// restore or clone: its status and failure code.
func readLiveStorageCopyOperation(t *testing.T, harness *acceptanceHarness, destinationComputerID, operation string) (status, failureCode string) {
	t.Helper()
	database, err := sql.Open("sqlite", harness.l1Database)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var code sql.NullString
	err = database.QueryRow(`SELECT status, failure_code FROM computer_storage_copy_operations
		WHERE destination_computer_id=? AND operation=? ORDER BY operation_revision DESC LIMIT 1`,
		destinationComputerID, operation).Scan(&status, &code)
	if err != nil {
		t.Fatalf("read the %s operation of %s: %v", operation, destinationComputerID, err)
	}
	return status, code.String
}

func createLiveStorageBackup(t *testing.T, harness *acceptanceHarness, computer l1.Computer, key string) string {
	t.Helper()
	created := runComputerCLI[storageCLIMutationReceipt](t, harness, false, "services", "backup", "create", computer.ComputerID,
		"--expect-current", "--allow-power-off", "--idempotency-key", key, "--wait", "4m")
	if created.Backups != nil {
		for _, candidate := range created.Backups.Backups {
			if candidate.Status == "available" {
				return candidate.BackupID
			}
		}
	}
	t.Fatalf("Computer %s has no available Backup: %#v", computer.ComputerID, created)
	return ""
}

type liveCopyAbortEvidence struct {
	Restore, Clone liveStorageBudgetEvidence
}

// exerciseLiveCopyAbort proves the dead-Node abort of a restore and of a
// clone (#1608 PR3). Both copies are held unfinished by refusing new
// generation roots, then the agent is killed so the Node goes dead with both
// operations in flight. Abort must return the restore's Computer to its old
// generation, and leave the clone's destination renamed and removable.
func exerciseLiveCopyAbort(t *testing.T, harness *acceptanceHarness, reference, digest string) liveCopyAbortEvidence {
	t.Helper()
	restoreTarget := createLiveStorageComputer(t, harness, reference, digest, "linux-native-restore-abort")
	restoreBackup := createLiveStorageBackup(t, harness, restoreTarget, "linux-native-restore-abort-backup")
	restoreTarget = waitForComputerCLI(t, harness, restoreTarget.ComputerID, 3*time.Minute, computerDisplayPublished)
	restoreTarget = stopLiveStorageComputer(t, harness, restoreTarget)
	cloneSource := createLiveStorageComputer(t, harness, reference, digest, "linux-native-clone-abort-source")
	cloneBackup := createLiveStorageBackup(t, harness, cloneSource, "linux-native-clone-abort-backup")
	cloneSource = waitForComputerCLI(t, harness, cloneSource.ComputerID, 3*time.Minute, computerDisplayPublished)

	thaw := liveStorageFault(t, harness, "freeze-storage-roots", "thaw-storage-roots")
	cloning := runComputerCLI[storageCLIMutationReceipt](t, harness, false, "services", "clone", cloneSource.ComputerID, cloneBackup,
		"--name", "linux-native-clone-abort", "--disk-bytes", fmt.Sprint(liveStorageDiskBytes), "--expect-current",
		"--idempotency-key", "linux-native-clone-abort")
	if cloning.Computer == nil || cloning.Computer.ComputerID == "" || cloning.Computer.ComputerID == cloneSource.ComputerID {
		t.Fatalf("clone printed no destination: %#v", cloning)
	}
	_ = runComputerCLI[storageCLIMutationReceipt](t, harness, false, "services", "restore", restoreTarget.ComputerID, restoreBackup,
		"--retire-old", "--expect-current", "--idempotency-key", "linux-native-restore-abort")
	harness.agent.kill(t)
	thaw()
	time.Sleep(l1.DefaultNodeDeadAfter + 5*time.Second)

	restoreBefore := runComputerCLI[l1.Computer](t, harness, false, "services", "status", restoreTarget.ComputerID)
	cloneBefore := runComputerCLI[l1.Computer](t, harness, false, "services", "status", cloning.Computer.ComputerID)
	restoreAborted := runComputerCLI[l1.Computer](t, harness, false, "services", "abort", restoreTarget.ComputerID,
		"--expect-current", "--idempotency-key", "linux-native-restore-abort-abort")
	cloneAborted := runComputerCLI[l1.Computer](t, harness, false, "services", "abort", cloning.Computer.ComputerID,
		"--expect-current", "--idempotency-key", "linux-native-clone-abort-abort")
	harness.restartAgent(t)

	restoreStatus, restoreCode := readLiveStorageCopyOperation(t, harness, restoreTarget.ComputerID, "restore")
	cloneStatus, cloneCode := readLiveStorageCopyOperation(t, harness, cloning.Computer.ComputerID, "clone")
	var evidence liveCopyAbortEvidence
	evidence.Restore = liveStorageBudgetEvidence{
		Passed: restoreBefore.ReconfigurationPhase == l1.ComputerReconfigurationRestoring &&
			restoreAborted.ReconfigurationPhase == l1.ComputerReconfigurationStable &&
			restoreAborted.StorageGeneration == restoreTarget.StorageGeneration &&
			restoreStatus == "superseded" && restoreCode == string(l1.ComputerRestoreFailureAbortedDeadNode),
		Detail: fmt.Sprintf("before_phase=%s aborted_phase=%s generation=%d->%d operation=%s/%s", restoreBefore.ReconfigurationPhase,
			restoreAborted.ReconfigurationPhase, restoreTarget.StorageGeneration, restoreAborted.StorageGeneration, restoreStatus, restoreCode)}
	evidence.Clone = liveStorageBudgetEvidence{
		Passed: cloneBefore.ReconfigurationPhase == l1.ComputerReconfigurationCloning &&
			cloneAborted.ReconfigurationPhase == l1.ComputerReconfigurationStable &&
			cloneAborted.Name == "aborted-clone-"+cloneAborted.ComputerID &&
			cloneStatus == "superseded" && cloneCode == string(l1.ComputerRestoreFailureAbortedDeadNode),
		Detail: fmt.Sprintf("before_phase=%s aborted_phase=%s name=%s operation=%s/%s", cloneBefore.ReconfigurationPhase,
			cloneAborted.ReconfigurationPhase, cloneAborted.Name, cloneStatus, cloneCode)}
	removedClone := removeAndWaitComputer(t, harness, cloneAborted, 5*time.Minute)
	evidence.Clone.Detail += " removal_outcome=" + removedClone.RemovalOutcome
	removedRestore := removeAndWaitComputer(t, harness, restoreAborted, 5*time.Minute)
	evidence.Restore.Detail += " removal_outcome=" + removedRestore.RemovalOutcome
	_ = removeAndWaitComputer(t, harness, cloneSource, 5*time.Minute)
	return evidence
}
