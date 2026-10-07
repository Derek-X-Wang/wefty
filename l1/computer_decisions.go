package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
)

// These decisions are read-only. Both the atomic writes (after replay checks)
// and the caller projection use them; request-value constraints stay enforced.

func computerDesiredDecision(ctx context.Context, tx *sql.Tx, computer Computer, request ComputerDesiredStateRequest) (bool, error) {
	computerID := computer.ComputerID
	if computer.DesiredState == contract.ServiceDesiredRemoved {
		return false, protocolError(contract.ErrorConflict, "Computer %q is being removed", computerID)
	}
	backupStopWins := computer.ReconfigurationPhase == ComputerReconfigurationBackingUp &&
		request.DesiredState == contract.ServiceDesiredStopped
	if computer.ReconfigurationPhase != ComputerReconfigurationStable && !backupStopWins {
		return false, protocolError(contract.ErrorConflict,
			"Computer %q is in reconfiguration phase %q", computerID, computer.ReconfigurationPhase)
	}
	if request.DesiredState == contract.ServiceDesiredRunning {
		if err := requireCurrentComputerStorage(ctx, tx, computer, "start"); err != nil {
			return false, err
		}
	}
	if request.DesiredState == contract.ServiceDesiredRunning && computer.CurrentJob.State == contract.JobFailed {
		return false, protocolErrorWithDetails(contract.ErrorConflict, map[string]any{
			"computer_id": computerID, "required_operation": "restart",
		}, "Computer %q is latched failed; use POST /v1/computers/%s/restart", computerID, computerID)
	}
	return backupStopWins, nil
}

func computerRestartDecision(ctx context.Context, tx *sql.Tx, computer Computer, request ComputerRestartRequest) (bool, error) {
	computerID := computer.ComputerID
	if computer.DesiredState == contract.ServiceDesiredRemoved {
		return false, protocolError(contract.ErrorConflict, "Computer %q is being removed", computerID)
	}
	if computer.ReconfigurationPhase != ComputerReconfigurationStable {
		return false, protocolError(contract.ErrorConflict,
			"Computer %q is in reconfiguration phase %q", computerID, computer.ReconfigurationPhase)
	}
	if err := requireCurrentComputerStorage(ctx, tx, computer, "restart"); err != nil {
		return false, err
	}
	var latchedFailure contract.SpawnFailure
	activeResourceRestart := (computer.CurrentJob.State == contract.JobClaimed || computer.CurrentJob.State == contract.JobRunning) &&
		json.Unmarshal(computer.CurrentJob.LastFailure, &latchedFailure) == nil &&
		(latchedFailure.Code == contract.SpawnFailureInsufficientDisk || latchedFailure.Code == contract.SpawnFailureInsufficientMemory)
	if computer.CurrentJob.State != contract.JobStopped && computer.CurrentJob.State != contract.JobFailed && !activeResourceRestart {
		return false, protocolError(contract.ErrorConflict,
			"Computer %q can restart only from stopped, failed, or an active insufficient-resource latch, not %q",
			computerID, computer.CurrentJob.State)
	}
	if !activeResourceRestart && !computer.CurrentJob.HoldsSlot(computer.CurrentJob.State) {
		if err := ensureBoundServiceCapacity(ctx, tx, computer.CurrentJob); err != nil {
			return false, err
		}
	}
	return activeResourceRestart, nil
}

func computerBackupCapDecision(ctx context.Context, tx *sql.Tx, computer Computer, request ComputerBackupCapRequest) error {
	computerID := computer.ComputerID
	if computer.DesiredState == contract.ServiceDesiredRemoved || computer.ReconfigurationPhase != ComputerReconfigurationStable {
		return protocolError(contract.ErrorConflict, "Computer %q is not stable", computerID)
	}
	return nil
}

func computerGrowDecision(ctx context.Context, tx *sql.Tx, computer Computer, request ComputerGrowRequest) (string, string, error) {
	computerID := computer.ComputerID
	if computer.DesiredState == contract.ServiceDesiredRemoved {
		return "", "", protocolError(contract.ErrorConflict, "Computer %q is being removed", computerID)
	}
	if computer.ReconfigurationPhase != ComputerReconfigurationStable {
		return "", "", protocolError(contract.ErrorConflict,
			"Computer %q is in reconfiguration phase %q", computerID, computer.ReconfigurationPhase)
	}
	// A grow mutates the current generation's image. With no published
	// generation there is nothing to grow, and the helper would refuse the
	// absent disk on every poll.
	if err := requireCurrentComputerStorage(ctx, tx, computer, "resize"); err != nil {
		return "", "", err
	}
	if request.DiskBytes <= computer.DesiredDiskBytes {
		return "", "", protocolErrorWithDetails(contract.ErrorConflict, map[string]any{
			"computer_id": computerID, "current_disk_bytes": computer.DesiredDiskBytes,
			"requested_disk_bytes": request.DiskBytes,
		}, "Computer resize is grow-only")
	}
	boundNodeID := computer.BoundNodeID
	if boundNodeID == "" {
		boundNodeID = computer.PlacementNodeID
	}
	var rootInstanceID string
	if err := tx.QueryRowContext(ctx, `SELECT root_instance_id FROM nodes WHERE node_id=?`, boundNodeID).Scan(&rootInstanceID); err != nil {
		return "", "", protocolError(contract.ErrorConflict, "bound node %q is unavailable", boundNodeID)
	}
	if rootInstanceID == "" {
		return "", "", protocolError(contract.ErrorConflict, "bound node has no managed-root instance")
	}
	return boundNodeID, rootInstanceID, nil
}

func computerBackupDecision(ctx context.Context, tx *sql.Tx, computer Computer, request ComputerBackupCreateRequest) (string, string, error) {
	computerID := computer.ComputerID
	if computer.DesiredState == contract.ServiceDesiredRunning && !request.AllowPowerOff {
		return "", "", protocolError(contract.ErrorConflict,
			"Computer %q is running; Backup creation requires explicit allow_power_off", computerID)
	}
	if computer.DesiredState == contract.ServiceDesiredRemoved {
		return "", "", protocolError(contract.ErrorConflict, "Computer %q is being removed", computerID)
	}
	if computer.CurrentJob.State == contract.JobFailed {
		return "", "", protocolError(contract.ErrorConflict,
			"Computer %q is latched failed; stop or restart it explicitly before Backup creation", computerID)
	}
	if computer.ReconfigurationPhase != ComputerReconfigurationStable {
		return "", "", protocolError(contract.ErrorConflict, "Computer %q is in reconfiguration phase %q", computerID, computer.ReconfigurationPhase)
	}
	var retained int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM backups WHERE computer_id=? AND status IN ('available', 'pruning')`, computerID).Scan(&retained); err != nil {
		return "", "", internalError(err, "count retained Computer Backups")
	}
	if computer.BackupCap == 0 || retained >= computer.BackupCap {
		return "", "", protocolErrorWithDetails(contract.ErrorConflict, map[string]any{
			"computer_id": computerID, "backup_cap": computer.BackupCap, "retained_backups": retained,
		}, "Computer %q is at its Backup cap", computerID)
	}
	boundNodeID := computer.BoundNodeID
	if boundNodeID == "" {
		return "", "", protocolError(contract.ErrorConflict,
			"Computer %q has no bound source Node to back up", computerID)
	}
	var rootInstanceID string
	if err := tx.QueryRowContext(ctx, `SELECT root_instance_id FROM nodes WHERE node_id=?`, boundNodeID).Scan(&rootInstanceID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", protocolError(contract.ErrorConflict, "bound node %q was not found", boundNodeID)
		}
		return "", "", internalError(err, "read Computer Backup managed-root authority")
	}
	if rootInstanceID == "" {
		return "", "", protocolError(contract.ErrorConflict, "bound node %q has no registered managed-root instance", boundNodeID)
	}
	return boundNodeID, rootInstanceID, nil
}

func computerRestoreDecision(ctx context.Context, tx *sql.Tx, computer Computer, request ComputerRestoreRequest) (Backup, BackupCopy, error) {
	computerID := computer.ComputerID
	if err := validateStoppedDetachedComputer(computer, "restore"); err != nil {
		return Backup{}, BackupCopy{}, err
	}
	if computer.StorageGeneration == math.MaxInt64 {
		return Backup{}, BackupCopy{}, protocolError(contract.ErrorConflict, "Computer %q exhausted Storage generation space", computerID)
	}
	backup, copy, err := readAvailableBackupCopy(ctx, tx, request.BackupID)
	if err != nil {
		return Backup{}, BackupCopy{}, err
	}
	if backup.ComputerID != computerID || backup.SourceStorageID != computer.StorageID {
		return Backup{}, BackupCopy{}, protocolError(contract.ErrorStorageReferenceConflict, "Backup %q does not belong to Computer %q Storage", backup.BackupID, computerID)
	}
	if backup.AllocatedSize > computer.DesiredDiskBytes {
		return Backup{}, BackupCopy{}, protocolErrorWithDetails(contract.ErrorConflict, map[string]any{
			"backup_bytes": backup.AllocatedSize, "destination_budget_bytes": computer.DesiredDiskBytes,
		}, "restore Backup is larger than the Computer disk budget; grow the Computer first")
	}
	if copy.NodeID != computer.BoundNodeID || copy.RootInstanceID == "" {
		return Backup{}, BackupCopy{}, protocolError(contract.ErrorConflict, "restore Backup copy is not on the Computer's bound Node")
	}
	var currentRootInstanceID string
	if err := tx.QueryRowContext(ctx, `SELECT root_instance_id FROM nodes WHERE node_id=?`, copy.NodeID).Scan(&currentRootInstanceID); err != nil {
		return Backup{}, BackupCopy{}, internalError(err, "read restore detachment managed-root authority")
	}
	if currentRootInstanceID == "" || currentRootInstanceID != copy.RootInstanceID {
		return Backup{}, BackupCopy{}, protocolError(contract.ErrorConflict, "restore requires an identity-bound current detachment receipt")
	}
	if request.KeepOldBackup {
		if err := computerRestoreRetentionDecision(ctx, tx, computer); err != nil {
			return Backup{}, BackupCopy{}, err
		}
	}
	return backup, copy, nil
}

func computerExportDecision(ctx context.Context, tx *sql.Tx, computer Computer, request ComputerCustodyExportRequest) (Backup, BackupCopy, error) {
	computerID := computer.ComputerID
	if computer.DesiredState == contract.ServiceDesiredRemoved || computer.ReconfigurationPhase != ComputerReconfigurationStable {
		return Backup{}, BackupCopy{}, protocolError(contract.ErrorConflict, "Computer %q cannot begin a Custody export", computerID)
	}
	backup, copy, err := readAvailableBackupCopy(ctx, tx, request.BackupID)
	if err != nil {
		return Backup{}, BackupCopy{}, err
	}
	if backup.ComputerID != computerID || backup.SourceStorageID != computer.StorageID {
		return Backup{}, BackupCopy{}, protocolError(contract.ErrorStorageReferenceConflict, "Backup %q does not belong to Computer %q Storage", backup.BackupID, computerID)
	}
	if copy.NodeID != computer.BoundNodeID || copy.RootInstanceID == "" {
		return Backup{}, BackupCopy{}, protocolError(contract.ErrorConflict, "Custody export Backup is not on the Computer's bound Node")
	}
	var currentRoot string
	if err := tx.QueryRowContext(ctx, `SELECT root_instance_id FROM nodes WHERE node_id=?`, copy.NodeID).Scan(&currentRoot); err != nil {
		return Backup{}, BackupCopy{}, internalError(err, "read Custody export managed-root identity")
	}
	if currentRoot == "" || currentRoot != copy.RootInstanceID {
		return Backup{}, BackupCopy{}, protocolError(contract.ErrorConflict, "Custody export Backup belongs to a stale managed-root instance")
	}
	return backup, copy, nil
}

func computerAbortDecision(ctx context.Context, tx *sql.Tx, computer Computer, request ComputerReconfigurationAbortRequest) (string, error) {
	computerID := computer.ComputerID
	if computer.ReconfigurationRevision == nil || *computer.ReconfigurationRevision != computer.IntentRevision {
		return "", protocolError(contract.ErrorConflict,
			"Computer %q has no abortable reconfiguration authority", computerID)
	}
	switch computer.ReconfigurationPhase {
	case ComputerReconfigurationBackingUp, ComputerReconfigurationResetting, ComputerReconfigurationReimaging,
		ComputerReconfigurationGrowing, ComputerReconfigurationExporting, ComputerReconfigurationImporting:
	default:
		return "", protocolError(contract.ErrorConflict,
			"Computer %q reconfiguration phase %q is not abortable", computerID, computer.ReconfigurationPhase)
	}
	boundNodeID := computer.BoundNodeID
	if boundNodeID == "" {
		boundNodeID = computer.CurrentJob.BoundNodeID
	}
	if boundNodeID == "" {
		var query string
		switch computer.ReconfigurationPhase {
		case ComputerReconfigurationBackingUp:
			query = `SELECT bound_node_id FROM computer_backup_operations WHERE computer_id=? AND operation_revision=?`
		case ComputerReconfigurationResetting:
			query = `SELECT bound_node_id FROM computer_storage_resets WHERE computer_id=? AND intent_revision=?`
		case ComputerReconfigurationReimaging:
			query = `SELECT bound_node_id FROM computer_reimage_operations WHERE computer_id=? AND operation_revision=?`
		case ComputerReconfigurationGrowing:
			query = `SELECT bound_node_id FROM computer_storage_grows WHERE computer_id=? AND operation_revision=?`
		case ComputerReconfigurationExporting:
			query = `SELECT bound_node_id FROM computer_custody_exports WHERE computer_id=? AND operation_revision=?`
		case ComputerReconfigurationImporting:
			query = `SELECT bound_node_id FROM computer_storage_copy_operations WHERE destination_computer_id=? AND operation_revision=? AND operation='import'`
		}
		if query != "" {
			if err := tx.QueryRowContext(ctx, query, computerID, computer.IntentRevision).Scan(&boundNodeID); err != nil {
				return "", internalError(err, "read aborted Computer operation binding")
			}
		}
	}
	if boundNodeID == "" {
		return "", protocolError(contract.ErrorConflict,
			"Computer %q has no bound Node whose loss can authorize abort", computerID)
	}
	var nodeState contract.NodeState
	if err := tx.QueryRowContext(ctx, `SELECT state FROM nodes WHERE node_id=?`, boundNodeID).Scan(&nodeState); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", protocolError(contract.ErrorConflict, "bound node %q was not found", boundNodeID)
		}
		return "", internalError(err, "read aborted Computer bound Node")
	}
	if nodeState != contract.NodeDead {
		return "", protocolErrorWithDetails(contract.ErrorConflict, map[string]any{
			"computer_id": computerID, "bound_node_id": boundNodeID, "node_state": nodeState,
		}, "Computer reconfiguration abort requires a dead bound Node")
	}
	return boundNodeID, nil
}

func computerResetDecision(ctx context.Context, tx *sql.Tx, computer Computer, request ComputerStorageResetRequest) error {
	computerID := computer.ComputerID
	if computer.DesiredState == contract.ServiceDesiredRemoved {
		return protocolError(contract.ErrorConflict, "Computer %q is being removed", computerID)
	}
	active := computer.CurrentJob.State == contract.JobClaimed || computer.CurrentJob.State == contract.JobRunning ||
		computer.CurrentJob.State == contract.JobStopping
	if active && !request.TerminateSessions {
		return protocolError(contract.ErrorConflict,
			"running Computer reset requires explicit take-over session termination")
	}
	if computer.ReconfigurationPhase != ComputerReconfigurationStable {
		return protocolError(contract.ErrorConflict,
			"Computer %q is in reconfiguration phase %q", computerID, computer.ReconfigurationPhase)
	}
	if computer.StorageGeneration == math.MaxInt64 {
		return protocolError(contract.ErrorConflict, "Computer %q exhausted Storage generation space", computerID)
	}
	// Reset publishes a successor by retiring a `current` predecessor. A
	// Computer whose generation was never published has no predecessor to
	// retire, so admitting one would reserve a successor that can never be
	// published.
	if err := requireCurrentComputerStorage(ctx, tx, computer, "reset"); err != nil {
		return err
	}
	return nil
}

func computerResetCapacityDecision(ctx context.Context, tx *sql.Tx, computer Computer, boundNodeID string) (string, error) {
	var rootInstanceID string
	if err := tx.QueryRowContext(ctx, `SELECT root_instance_id FROM nodes WHERE node_id=?`, boundNodeID).Scan(&rootInstanceID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", protocolError(contract.ErrorConflict, "bound node %q was not found", boundNodeID)
		}
		return "", internalError(err, "read Computer Storage reset managed-root authority")
	}
	if strings.TrimSpace(rootInstanceID) == "" {
		return "", protocolError(contract.ErrorConflict,
			"bound node %q has no registered managed-root instance", boundNodeID)
	}
	// The successor reserves its service Slot before any node-local allocation.
	// Publication cannot fail after preparation because capacity changed later.
	if !computer.CurrentJob.HoldsSlot(computer.CurrentJob.State) {
		if err := ensureBoundServiceCapacity(ctx, tx, computer.CurrentJob); err != nil {
			return "", err
		}
	}
	return rootInstanceID, nil
}

func computerCloneDecision(ctx context.Context, tx *sql.Tx, computer Computer, copy BackupCopy, request ComputerCloneRequest) error {
	source := computer
	if source.DesiredState == contract.ServiceDesiredRemoved || copy.NodeID != source.PlacementNodeID {
		return protocolError(contract.ErrorConflict, "clone source is not available on its Pinned Node")
	}
	var currentRootInstanceID string
	if err := tx.QueryRowContext(ctx, `SELECT root_instance_id FROM nodes WHERE node_id=?`, copy.NodeID).Scan(&currentRootInstanceID); err != nil {
		return internalError(err, "read clone source managed-root authority")
	}
	if currentRootInstanceID == "" || currentRootInstanceID != copy.RootInstanceID {
		return protocolError(contract.ErrorConflict, "clone source Backup copy belongs to a stale managed-root instance")
	}
	if err := computerWritePreconditionDecision(ctx, source, request.ComputerMutationPrecondition); err != nil {
		return err
	}
	return nil
}

func computerRemoveDecision(ctx context.Context, tx *sql.Tx, computer Computer, request ComputerRemoveRequest) error {
	computerID := computer.ComputerID
	if computer.ReconfigurationPhase != ComputerReconfigurationStable &&
		computer.ReconfigurationPhase != ComputerReconfigurationProjecting &&
		computer.ReconfigurationPhase != ComputerReconfigurationResetting &&
		computer.ReconfigurationPhase != ComputerReconfigurationBackingUp &&
		computer.ReconfigurationPhase != ComputerReconfigurationRestoring &&
		computer.ReconfigurationPhase != ComputerReconfigurationCloning &&
		computer.ReconfigurationPhase != ComputerReconfigurationExporting &&
		computer.ReconfigurationPhase != ComputerReconfigurationReimaging &&
		computer.ReconfigurationPhase != ComputerReconfigurationGrowing {
		return protocolError(contract.ErrorConflict,
			"Computer %q is in reconfiguration phase %q", computerID, computer.ReconfigurationPhase)
	}
	_, _, err := computerRemovalBindingDecision(ctx, tx, computer)
	return err
}

func computerProjectionDecision(ctx context.Context, tx *sql.Tx, computer Computer, request ComputerProjectionRequest, operation ComputerIntentOperation) error {
	computerID := computer.ComputerID
	if computer.DesiredState == contract.ServiceDesiredRemoved {
		return protocolError(contract.ErrorConflict, "Computer %q is being removed", computerID)
	}
	if computer.ReconfigurationPhase != ComputerReconfigurationStable {
		return protocolError(contract.ErrorConflict,
			"Computer %q is in reconfiguration phase %q", computerID, computer.ReconfigurationPhase)
	}
	// A projection's spec is the caller's, so it may not name a run (wefty
	// #583). A reimage's is the stored Computer's own with only the image
	// replaced, so it can introduce no claim the Computer did not already
	// hold. Both follow the replay check above.
	if operation == ComputerIntentProject {
		if err := refuseComputerRunIdentity(request.Spec); err != nil {
			return err
		}
	}
	// Refused before any revision is reserved: a projection or reimage of a
	// Computer with no published Storage commits a phase whose directive no
	// helper can ever complete.
	if err := requireCurrentComputerStorage(ctx, tx, computer, string(operation)); err != nil {
		return err
	}
	return computerWritePreconditionDecision(ctx, computer, request.ComputerMutationPrecondition)
}

// Reads must also check the predicates enforced later by projection writes.
// Keep them out of the early write decision: dispatch-key conflicts precede
// root lookup, quiescence and publication refusals in the write transaction.
func computerProjectionReadinessDecision(ctx context.Context, tx *sql.Tx, computer Computer, operation ComputerIntentOperation) error {
	if operation == ComputerIntentReimage {
		if _, err := computerReimageRootDecision(ctx, tx, computer); err != nil {
			return err
		}
	}
	if operation == ComputerIntentProject && computer.CurrentJob.State == contract.JobFailed {
		return computerProjectionPublicationDecision(computer.ComputerID, computer.CurrentJob.State)
	}
	return computerProjectionQuiescenceDecision(computer.CurrentJob)
}

func computerPruneDecision(ctx context.Context, tx *sql.Tx, computer Computer, request ComputerBackupPruneRequest) (Backup, error) {
	computerID := computer.ComputerID
	if computer.DesiredState == contract.ServiceDesiredRemoved || computer.ReconfigurationPhase == ComputerReconfigurationRemoving {
		return Backup{}, protocolError(contract.ErrorConflict, "Computer %q is being removed", computerID)
	}
	backup, err := readBackup(ctx, tx, request.BackupID)
	if errors.Is(err, sql.ErrNoRows) || backup.ComputerID != computerID {
		return Backup{}, protocolError(contract.ErrorNotFound, "Backup %q was not found", request.BackupID)
	}
	if err != nil {
		return Backup{}, internalError(err, "read Computer Backup prune target")
	}
	if backup.Status == "pruned" {
		return backup, nil
	}
	if backup.Status != "available" || len(backup.Copies) != 1 || backup.Copies[0].Phase != "published" {
		return Backup{}, protocolError(contract.ErrorConflict, "Backup %q is not available for pruning", request.BackupID)
	}
	return backup, nil
}

func computerReimageSessionsDecision(computer Computer, terminateSessions bool) error {
	if computerRuntimeActive(computer) && !terminateSessions {
		return protocolError(contract.ErrorConflict, "running Computer reimage requires explicit take-over session termination")
	}
	return nil
}

func computerRuntimeActive(computer Computer) bool {
	return computer.CurrentJob.State == contract.JobClaimed || computer.CurrentJob.State == contract.JobRunning || computer.CurrentJob.State == contract.JobStopping
}

// This is also called immediately before applying the Job transition. Reads
// must include the capacity and observed-state checks behind desired intent.
func computerServiceDesiredDecision(ctx context.Context, tx *sql.Tx, job Job, desired contract.ServiceDesiredState) error {
	if job.ServiceJob == nil {
		return internalError(errors.New("Computer current Job is not a service"), "apply Computer desired state")
	}
	switch desired {
	case contract.ServiceDesiredRunning:
		switch job.State {
		case contract.JobFailed:
			return protocolError(contract.ErrorConflict, "Computer Job %q is latched failed; restart is required", job.JobID)
		case contract.JobStopped:
			if !job.HoldsSlot(job.State) {
				return ensureBoundServiceCapacity(ctx, tx, job)
			}
		case contract.JobStopping:
			return protocolError(contract.ErrorConflict, "Computer Job %q is still stopping", job.JobID)
		case contract.JobQueued, contract.JobClaimed, contract.JobRunning:
			if job.DesiredState != contract.ServiceDesiredRunning {
				return protocolError(contract.ErrorConflict, "Computer Job %q has inconsistent desired state", job.JobID)
			}
		default:
			return protocolError(contract.ErrorConflict, "Computer Job %q cannot start from %q", job.JobID, job.State)
		}
	case contract.ServiceDesiredStopped:
		switch job.State {
		case contract.JobQueued, contract.JobClaimed, contract.JobRunning, contract.JobFailed:
		case contract.JobStopping, contract.JobStopped:
			if job.DesiredState != contract.ServiceDesiredStopped {
				return protocolError(contract.ErrorConflict, "Computer Job %q has inconsistent desired state", job.JobID)
			}
		default:
			return protocolError(contract.ErrorConflict, "Computer Job %q cannot stop from %q", job.JobID, job.State)
		}
	}
	return nil
}

func computerRemovalBindingDecision(ctx context.Context, tx *sql.Tx, computer Computer) (string, string, error) {
	computerID := computer.ComputerID
	boundNodeID := computer.CurrentJob.BoundNodeID
	if computer.BoundNodeID != boundNodeID {
		return "", "", protocolErrorWithDetails(contract.ErrorConflict, map[string]any{
			"computer_id": computerID, "computer_bound_node_id": computer.BoundNodeID,
			"job_bound_node_id": boundNodeID,
		}, "Computer %q and current Job binding diverged", computerID)
	}
	if boundNodeID == "" {
		return "", "", nil
	}
	var rootInstanceID string
	if err := tx.QueryRowContext(ctx, `SELECT root_instance_id FROM nodes WHERE node_id=?`, boundNodeID).Scan(&rootInstanceID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", protocolError(contract.ErrorConflict, "bound node %q was not found", boundNodeID)
		}
		return "", "", internalError(err, "read Computer removal managed-root authority")
	}
	if strings.TrimSpace(rootInstanceID) == "" {
		return "", "", protocolError(contract.ErrorConflict,
			"bound node %q has no registered managed-root instance", boundNodeID)
	}
	return boundNodeID, rootInstanceID, nil
}

func computerReimageRootDecision(ctx context.Context, tx *sql.Tx, computer Computer) (string, error) {
	boundNodeID := computer.BoundNodeID
	if boundNodeID == "" {
		boundNodeID = computer.PlacementNodeID
	}
	var rootInstanceID string
	if err := tx.QueryRowContext(ctx, `SELECT root_instance_id FROM nodes WHERE node_id=?`, boundNodeID).Scan(&rootInstanceID); err != nil {
		return "", protocolError(contract.ErrorConflict, "Computer reimage bound Node is unavailable")
	}
	return rootInstanceID, nil
}

func computerProjectionQuiescenceDecision(job Job) error {
	switch job.State {
	case contract.JobQueued, contract.JobClaimed, contract.JobRunning, contract.JobStopping, contract.JobStopped, contract.JobFailed:
		return nil
	default:
		return protocolError(contract.ErrorConflict, "Computer Job %q cannot enter projection from %q", job.JobID, job.State)
	}
}

func computerRestoreRetentionDecision(ctx context.Context, tx *sql.Tx, computer Computer) error {
	computerID := computer.ComputerID
	var retained int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM backups WHERE computer_id=? AND status IN ('available', 'pruning')`, computerID).Scan(&retained); err != nil {
		return internalError(err, "count retained Backups before restore")
	}
	if computer.BackupCap == 0 || retained >= computer.BackupCap {
		return protocolError(contract.ErrorConflict, "Computer %q is at its Backup cap", computerID)
	}
	return nil
}

func computerProjectionPublicationDecision(computerID string, state contract.JobState) error {
	if state != contract.JobStopped {
		return protocolError(contract.ErrorConflict, "Computer %q has not quiesced its current Job", computerID)
	}
	return nil
}
