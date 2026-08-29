package l1

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const computerStorageCopyReceiptKind = "computer_storage_copy_verified"

type ComputerRestoreRequest struct {
	ComputerMutationPrecondition
	BackupID       string `json:"-"`
	KeepOldBackup  bool   `json:"keep_old_as_backup"`
	IdempotencyKey string `json:"idempotency_key"`
}

type ComputerCloneRequest struct {
	ComputerMutationPrecondition
	BackupID         string `json:"-"`
	Name             string `json:"name"`
	DiskBytes        int64  `json:"disk_bytes"`
	IdempotencyKey   string `json:"idempotency_key"`
	Actor            string `json:"-"`
	SourceComputerID string `json:"-"`
}

type ComputerStorageCopyDirective struct {
	Operation             string `json:"operation"`
	BackupID              string `json:"backup_id"`
	CopyID                string `json:"copy_id"`
	SourceComputerID      string `json:"source_computer_id"`
	SourceStorageID       string `json:"source_storage_id"`
	SourceGeneration      int64  `json:"source_generation"`
	SourceSize            int64  `json:"source_size"`
	SourceDigest          string `json:"source_digest"`
	DestinationComputerID string `json:"destination_computer_id"`
	DestinationStorageID  string `json:"destination_storage_id"`
	OldGeneration         int64  `json:"old_generation"`
	DestinationGeneration int64  `json:"destination_generation"`
	DestinationSize       int64  `json:"destination_size"`
	BoundNodeID           string `json:"bound_node_id"`
	RootInstanceID        string `json:"root_instance_id"`
	JobID                 string `json:"job_id"`
	OperationRevision     int64  `json:"operation_revision"`
	CleanupFence          string `json:"cleanup_fence"`
	KeepOldBackup         bool   `json:"keep_old_as_backup"`
	OldBackupID           string `json:"old_backup_id,omitempty"`
	OldCopyID             string `json:"old_copy_id,omitempty"`
	Phase                 string `json:"phase"`
}

type ComputerStorageCopyReceipt = contract.ComputerStorageCopyReceipt

type ComputerStorageCopyAcknowledgementRequest struct {
	NodeID           string                     `json:"node_id"`
	BootSessionID    string                     `json:"boot_session_id"`
	IdempotencyKey   string                     `json:"idempotency_key"`
	Receipt          ComputerStorageCopyReceipt `json:"receipt"`
	OldBackupReceipt *ComputerBackupCopyReceipt `json:"old_backup_receipt,omitempty"`
}

type computerStorageCopyRow struct {
	DestinationComputerID string
	Operation             string
	OperationRevision     int64
	SourceComputerID      string
	BackupID              string
	CopyID                string
	SourceStorageID       string
	SourceGeneration      int64
	SourceSize            int64
	SourceDigest          string
	DestinationStorageID  string
	OldGeneration         int64
	DestinationGeneration int64
	DestinationSize       int64
	BoundNodeID           string
	RootInstanceID        string
	JobID                 string
	CleanupFence          string
	KeepOldBackup         bool
	OldBackupID           sql.NullString
	OldCopyID             sql.NullString
	IdempotencyKey        string
	RequestHash           string
	Status                string
	AcknowledgementKey    sql.NullString
	AcknowledgementHash   sql.NullString
}

const storageCopyColumns = `destination_computer_id, operation, operation_revision,
	source_computer_id, backup_id, copy_id, source_storage_id, source_generation,
	source_size, source_digest, destination_storage_id, old_generation,
	destination_generation, destination_size, bound_node_id, root_instance_id,
	job_id, cleanup_fence, keep_old_as_backup, old_backup_id, old_copy_id,
	idempotency_key, request_hash, status, acknowledgement_key, acknowledgement_hash`

func scanComputerStorageCopy(scanner interface{ Scan(...any) error }) (computerStorageCopyRow, error) {
	var row computerStorageCopyRow
	err := scanner.Scan(&row.DestinationComputerID, &row.Operation, &row.OperationRevision,
		&row.SourceComputerID, &row.BackupID, &row.CopyID, &row.SourceStorageID,
		&row.SourceGeneration, &row.SourceSize, &row.SourceDigest, &row.DestinationStorageID,
		&row.OldGeneration, &row.DestinationGeneration, &row.DestinationSize,
		&row.BoundNodeID, &row.RootInstanceID, &row.JobID, &row.CleanupFence,
		&row.KeepOldBackup, &row.OldBackupID, &row.OldCopyID, &row.IdempotencyKey,
		&row.RequestHash, &row.Status, &row.AcknowledgementKey, &row.AcknowledgementHash)
	return row, err
}

func storageCopyHash(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func validateStoppedDetachedComputer(computer Computer, action string) error {
	if computer.DesiredState != contract.ServiceDesiredStopped ||
		(computer.CurrentJob.State != contract.JobStopped && computer.CurrentJob.State != contract.JobFailed) ||
		computer.CurrentJob.CurrentAttemptID != "" {
		return protocolError(contract.ErrorConflict, "Computer %q must be stopped and positively detached before %s", computer.ComputerID, action)
	}
	if computer.ReconfigurationPhase != ComputerReconfigurationStable {
		return protocolError(contract.ErrorConflict, "Computer %q is in reconfiguration phase %q", computer.ComputerID, computer.ReconfigurationPhase)
	}
	return nil
}

func readAvailableBackupCopy(ctx context.Context, q queryer, backupID string) (Backup, BackupCopy, error) {
	backup, err := readBackup(ctx, q, backupID)
	if errors.Is(err, sql.ErrNoRows) {
		return Backup{}, BackupCopy{}, protocolError(contract.ErrorNotFound, "Backup %q was not found", backupID)
	}
	if err != nil {
		return Backup{}, BackupCopy{}, internalError(err, "read Storage copy source Backup")
	}
	if backup.Status != "available" || len(backup.Copies) != 1 || backup.Copies[0].Phase != "published" {
		return Backup{}, BackupCopy{}, protocolError(contract.ErrorConflict, "Backup %q has no available source-node copy", backupID)
	}
	copy := backup.Copies[0]
	if copy.AllocatedSize != backup.AllocatedSize || copy.ContentDigest != backup.ContentDigest ||
		!backupDigestPattern.MatchString(copy.ContentDigest) {
		return Backup{}, BackupCopy{}, protocolError(contract.ErrorConflict, "Backup %q copy conflicts with immutable Backup evidence", backupID)
	}
	return backup, copy, nil
}

func (s *Store) BeginComputerRestore(ctx context.Context, computerID string, request ComputerRestoreRequest) (Computer, bool, error) {
	request.Actor = strings.TrimSpace(request.Actor)
	request.BackupID = strings.TrimSpace(request.BackupID)
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	if strings.TrimSpace(computerID) == "" || request.BackupID == "" || request.IdempotencyKey == "" {
		return Computer{}, false, protocolError(contract.ErrorInvalidRequest, "computer_id, backup_id, and idempotency_key are required")
	}
	requestHash, err := storageCopyHash(request)
	if err != nil {
		return Computer{}, false, internalError(err, "encode Computer restore request")
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Computer{}, false, internalError(err, "begin Computer restore")
	}
	defer tx.Rollback()
	computer, err := readComputerAuthority(ctx, tx, computerID, now)
	if errors.Is(err, sql.ErrNoRows) {
		return Computer{}, false, protocolError(contract.ErrorNotFound, "Computer %q was not found", computerID)
	}
	if err != nil {
		return Computer{}, false, internalError(err, "read Computer restore target")
	}
	if replay, replayErr := scanComputerStorageCopy(tx.QueryRowContext(ctx, `SELECT `+storageCopyColumns+`
		FROM computer_storage_copy_operations WHERE destination_computer_id=? AND idempotency_key=?`, computerID, request.IdempotencyKey)); replayErr == nil {
		if replay.Operation != "restore" {
			return Computer{}, false, protocolError(contract.ErrorIdempotencyConflict, "idempotency key was already used for a Computer clone")
		}
		if replay.RequestHash != requestHash {
			return Computer{}, false, protocolError(contract.ErrorIdempotencyConflict, "Computer restore idempotency key was reused with different authority")
		}
		return computer, true, nil
	} else if !errors.Is(replayErr, sql.ErrNoRows) {
		return Computer{}, false, internalError(replayErr, "read Computer restore replay")
	}
	var reusedOperation string
	if err := tx.QueryRowContext(ctx, `SELECT operation FROM computer_storage_copy_operations
		WHERE backup_id=? AND idempotency_key=?`, request.BackupID, request.IdempotencyKey).Scan(&reusedOperation); err == nil {
		return Computer{}, false, protocolError(contract.ErrorIdempotencyConflict,
			"idempotency key was already used for a Computer %s", reusedOperation)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return Computer{}, false, internalError(err, "check cross-verb Storage copy idempotency")
	}
	if err := validateComputerPrecondition(computer, request.ComputerMutationPrecondition); err != nil {
		return Computer{}, false, err
	}
	if err := validateStoppedDetachedComputer(computer, "restore"); err != nil {
		return Computer{}, false, err
	}
	if computer.StorageGeneration == math.MaxInt64 {
		return Computer{}, false, protocolError(contract.ErrorConflict, "Computer %q exhausted Storage generation space", computerID)
	}
	backup, copy, err := readAvailableBackupCopy(ctx, tx, request.BackupID)
	if err != nil {
		return Computer{}, false, err
	}
	if backup.ComputerID != computerID || backup.SourceStorageID != computer.StorageID {
		return Computer{}, false, protocolError(contract.ErrorStorageReferenceConflict, "Backup %q does not belong to Computer %q Storage", backup.BackupID, computerID)
	}
	if backup.AllocatedSize > computer.DesiredDiskBytes {
		return Computer{}, false, protocolErrorWithDetails(contract.ErrorConflict, map[string]any{
			"backup_bytes": backup.AllocatedSize, "destination_budget_bytes": computer.DesiredDiskBytes,
		}, "restore Backup is larger than the Computer disk budget; grow the Computer first")
	}
	if copy.NodeID != computer.BoundNodeID || copy.RootInstanceID == "" {
		return Computer{}, false, protocolError(contract.ErrorConflict, "restore Backup copy is not on the Computer's bound Node")
	}
	var currentRootInstanceID string
	if err := tx.QueryRowContext(ctx, `SELECT root_instance_id FROM nodes WHERE node_id=?`, copy.NodeID).Scan(&currentRootInstanceID); err != nil {
		return Computer{}, false, internalError(err, "read restore detachment managed-root authority")
	}
	if currentRootInstanceID == "" || currentRootInstanceID != copy.RootInstanceID {
		return Computer{}, false, protocolError(contract.ErrorConflict, "restore requires an identity-bound current detachment receipt")
	}
	if request.KeepOldBackup {
		var retained int64
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM backups WHERE computer_id=? AND status<>'pruned'`, computerID).Scan(&retained); err != nil {
			return Computer{}, false, internalError(err, "count retained Backups before restore")
		}
		if computer.BackupCap == 0 || retained >= computer.BackupCap {
			return Computer{}, false, protocolError(contract.ErrorConflict, "Computer %q is at its Backup cap", computerID)
		}
	}
	revision := computer.IntentRevision + 1
	generation := computer.StorageGeneration + 1
	result, err := tx.ExecContext(ctx, `UPDATE computers SET intent_revision=?, reconfiguration_phase=?,
		reconfiguration_revision=?, updated_ns=? WHERE computer_id=? AND intent_revision=?`, revision,
		ComputerReconfigurationRestoring, revision, now.UnixNano(), computerID, computer.IntentRevision)
	if err != nil {
		return Computer{}, false, internalError(err, "reserve Computer restore")
	}
	if err := requireComputerCAS(result, computerID, computer.IntentRevision); err != nil {
		return Computer{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_storage_generations(computer_id, storage_id,
		storage_generation, disk_bytes, phase, reset_revision, created_ns) VALUES(?, ?, ?, ?, 'staging', ?, ?)`,
		computerID, computer.StorageID, generation, computer.DesiredDiskBytes, revision, now.UnixNano()); err != nil {
		return Computer{}, false, internalError(err, "stage restored Storage generation")
	}
	var oldBackupID, oldCopyID any
	if request.KeepOldBackup {
		oldBackupID, oldCopyID = newID("backup"), newID("backup-copy")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_storage_copy_operations(
		destination_computer_id, operation, operation_revision, source_computer_id, backup_id, copy_id,
		source_storage_id, source_generation, source_size, source_digest, destination_storage_id,
		old_generation, destination_generation, destination_size, bound_node_id, root_instance_id,
		job_id, cleanup_fence, keep_old_as_backup, old_backup_id, old_copy_id, idempotency_key,
		request_hash, status, requested_ns) VALUES(?, 'restore', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'reserved', ?)`,
		computerID, revision, computerID, backup.BackupID, copy.CopyID, backup.SourceStorageID,
		backup.SourceGeneration, backup.AllocatedSize, backup.ContentDigest, computer.StorageID,
		computer.StorageGeneration, generation, computer.DesiredDiskBytes, copy.NodeID, copy.RootInstanceID,
		computer.CurrentJobID, newID("restore-fence"), request.KeepOldBackup, oldBackupID, oldCopyID,
		request.IdempotencyKey, requestHash, now.UnixNano()); err != nil {
		return Computer{}, false, internalError(err, "persist Computer restore operation")
	}
	if err := insertComputerIntent(ctx, tx, computerID, revision, ComputerIntentRestore,
		contract.ServiceDesiredStopped, computer.StorageID, generation, computer.CurrentJobID,
		computer.CurrentSpecRevision, request.Actor, now); err != nil {
		return Computer{}, false, err
	}
	updated, err := readComputerAuthority(ctx, tx, computerID, now)
	if err != nil {
		return Computer{}, false, internalError(err, "read reserved Computer restore")
	}
	if err := tx.Commit(); err != nil {
		return Computer{}, false, internalError(err, "commit Computer restore reservation")
	}
	s.notifyComputerPolicyChanged()
	return updated, false, nil
}

func (s *Store) BeginComputerClone(ctx context.Context, request ComputerCloneRequest) (Computer, bool, error) {
	request.Name = strings.TrimSpace(request.Name)
	request.BackupID = strings.TrimSpace(request.BackupID)
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	request.Actor = strings.TrimSpace(request.Actor)
	if err := validateComputerNameAndActor(request.Name, request.Actor); err != nil {
		return Computer{}, false, err
	}
	if request.BackupID == "" || request.IdempotencyKey == "" || request.DiskBytes < 1 {
		return Computer{}, false, protocolError(contract.ErrorInvalidRequest, "backup_id, name, positive disk_bytes, and idempotency_key are required")
	}
	requestHash, err := storageCopyHash(request)
	if err != nil {
		return Computer{}, false, internalError(err, "encode Computer clone request")
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Computer{}, false, internalError(err, "begin Computer clone")
	}
	defer tx.Rollback()
	if replay, replayErr := scanComputerStorageCopy(tx.QueryRowContext(ctx, `SELECT `+storageCopyColumns+`
		FROM computer_storage_copy_operations WHERE backup_id=? AND idempotency_key=?`, request.BackupID, request.IdempotencyKey)); replayErr == nil {
		if replay.Operation != "clone" {
			return Computer{}, false, protocolError(contract.ErrorIdempotencyConflict, "idempotency key was already used for a Computer restore")
		}
		if replay.RequestHash != requestHash {
			return Computer{}, false, protocolError(contract.ErrorIdempotencyConflict, "Computer clone idempotency key was reused with different authority")
		}
		computer, readErr := readComputerAuthority(ctx, tx, replay.DestinationComputerID, now)
		return computer, true, readErr
	} else if !errors.Is(replayErr, sql.ErrNoRows) {
		return Computer{}, false, internalError(replayErr, "read Computer clone replay")
	}
	backup, copy, err := readAvailableBackupCopy(ctx, tx, request.BackupID)
	if err != nil {
		return Computer{}, false, err
	}
	if request.DiskBytes < backup.AllocatedSize {
		return Computer{}, false, protocolError(contract.ErrorConflict, "clone disk_bytes cannot be smaller than its source Backup")
	}
	if request.SourceComputerID != "" && request.SourceComputerID != backup.ComputerID {
		return Computer{}, false, protocolError(contract.ErrorStorageReferenceConflict, "Backup %q does not belong to source Computer %q", backup.BackupID, request.SourceComputerID)
	}
	if existingErr := tx.QueryRowContext(ctx, `SELECT computer_id FROM computers WHERE name=?`, request.Name).Scan(new(string)); existingErr == nil {
		return Computer{}, false, protocolError(contract.ErrorConflict, "Computer name %q is already used", request.Name)
	} else if !errors.Is(existingErr, sql.ErrNoRows) {
		return Computer{}, false, internalError(existingErr, "check cloned Computer name")
	}
	source, err := readComputerAuthority(ctx, tx, backup.ComputerID, now)
	if err != nil {
		return Computer{}, false, internalError(err, "read clone source Computer")
	}
	if source.DesiredState == contract.ServiceDesiredRemoved || copy.NodeID != source.PlacementNodeID {
		return Computer{}, false, protocolError(contract.ErrorConflict, "clone source is not available on its Pinned Node")
	}
	var currentRootInstanceID string
	if err := tx.QueryRowContext(ctx, `SELECT root_instance_id FROM nodes WHERE node_id=?`, copy.NodeID).Scan(&currentRootInstanceID); err != nil {
		return Computer{}, false, internalError(err, "read clone source managed-root authority")
	}
	if currentRootInstanceID == "" || currentRootInstanceID != copy.RootInstanceID {
		return Computer{}, false, protocolError(contract.ErrorConflict, "clone source Backup copy belongs to a stale managed-root instance")
	}
	if err := validateComputerPrecondition(source, request.ComputerMutationPrecondition); err != nil {
		return Computer{}, false, err
	}
	computerID, storageID := newID("computer"), newID("storage")
	spec := source.CurrentJob.Spec
	spec.DispatchKey = "computer:clone:" + computerID
	spec.Execution.OCI.Computer.DiskBytes = request.DiskBytes
	specJSON, specHash, err := encodeJobSpec(spec)
	if err != nil {
		return Computer{}, false, err
	}
	job, err := insertComputerJob(ctx, tx, spec, specJSON, specHash, contract.JobStopped,
		contract.ServiceDesiredStopped, copy.NodeID, now)
	if err != nil {
		return Computer{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computers(computer_id, name, placement_node_id,
		bound_node_id, grants_json, storage_id, storage_generation, backup_cap, desired_state,
		desired_disk_bytes, intent_revision, applied_revision, current_job_id, current_spec_revision,
		reconfiguration_phase, reconfiguration_revision, created_ns, updated_ns)
		VALUES(?, ?, ?, ?, '[]', ?, 1, ?, 'stopped', ?, 1, 0, ?, 1, 'cloning', 1, ?, ?)`,
		computerID, request.Name, copy.NodeID, copy.NodeID, storageID, source.BackupCap,
		request.DiskBytes, job.JobID, now.UnixNano(), now.UnixNano()); err != nil {
		return Computer{}, false, internalError(err, "store cloned Computer authority")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_job_projections(computer_id, job_id,
		spec_revision, current, created_ns) VALUES(?, ?, 1, 1, ?)`, computerID, job.JobID, now.UnixNano()); err != nil {
		return Computer{}, false, internalError(err, "store cloned Computer projection")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_storage_generations(computer_id, storage_id,
		storage_generation, disk_bytes, phase, reset_revision, created_ns) VALUES(?, ?, 1, ?, 'staging', 1, ?)`,
		computerID, storageID, request.DiskBytes, now.UnixNano()); err != nil {
		return Computer{}, false, internalError(err, "stage cloned Computer Storage")
	}
	if err := insertComputerIntent(ctx, tx, computerID, 1, ComputerIntentClone, contract.ServiceDesiredStopped,
		storageID, 1, job.JobID, 1, request.Actor, now); err != nil {
		return Computer{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_storage_copy_operations(
		destination_computer_id, operation, operation_revision, source_computer_id, backup_id, copy_id,
		source_storage_id, source_generation, source_size, source_digest, destination_storage_id,
		old_generation, destination_generation, destination_size, bound_node_id, root_instance_id,
		job_id, cleanup_fence, keep_old_as_backup, idempotency_key, request_hash, status, requested_ns)
		VALUES(?, 'clone', 1, ?, ?, ?, ?, ?, ?, ?, ?, 0, 1, ?, ?, ?, ?, ?, 0, ?, ?, 'reserved', ?)`,
		computerID, source.ComputerID, backup.BackupID, copy.CopyID, backup.SourceStorageID,
		backup.SourceGeneration, backup.AllocatedSize, backup.ContentDigest, storageID, request.DiskBytes,
		copy.NodeID, copy.RootInstanceID, job.JobID, newID("clone-fence"), request.IdempotencyKey,
		requestHash, now.UnixNano()); err != nil {
		return Computer{}, false, internalError(err, "persist Computer clone operation")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO storage_provenance(provenance_id, kind,
		source_storage_id, source_generation, backup_id, destination_computer_id,
		destination_storage_id, destination_generation, created_ns) VALUES(?, 'clone', ?, ?, ?, ?, ?, 1, ?)`,
		newID("storage-provenance"), backup.SourceStorageID, backup.SourceGeneration, backup.BackupID,
		computerID, storageID, now.UnixNano()); err != nil {
		return Computer{}, false, internalError(err, "reserve clone custody fork")
	}
	computer, err := readComputerAuthority(ctx, tx, computerID, now)
	if err != nil {
		return Computer{}, false, internalError(err, "read staged cloned Computer")
	}
	if err := tx.Commit(); err != nil {
		return Computer{}, false, internalError(err, "commit Computer clone reservation")
	}
	s.notifyComputerPolicyChanged()
	return computer, false, nil
}

func storageCopyDirective(row computerStorageCopyRow) ComputerStorageCopyDirective {
	directive := ComputerStorageCopyDirective{Operation: row.Operation, BackupID: row.BackupID,
		CopyID: row.CopyID, SourceComputerID: row.SourceComputerID, SourceStorageID: row.SourceStorageID,
		SourceGeneration: row.SourceGeneration, SourceSize: row.SourceSize, SourceDigest: row.SourceDigest,
		DestinationComputerID: row.DestinationComputerID, DestinationStorageID: row.DestinationStorageID,
		OldGeneration: row.OldGeneration, DestinationGeneration: row.DestinationGeneration,
		DestinationSize: row.DestinationSize, BoundNodeID: row.BoundNodeID,
		RootInstanceID: row.RootInstanceID, JobID: row.JobID, OperationRevision: row.OperationRevision,
		CleanupFence: row.CleanupFence, KeepOldBackup: row.KeepOldBackup, Phase: row.Status}
	if row.OldBackupID.Valid {
		directive.OldBackupID = row.OldBackupID.String
	}
	if row.OldCopyID.Valid {
		directive.OldCopyID = row.OldCopyID.String
	}
	return directive
}

func (s *Store) ListNodeComputerStorageCopyDirectives(ctx context.Context, identityNodeID, nodeID, bootSessionID string) ([]ComputerStorageCopyDirective, error) {
	if err := validateStorageResetNode(ctx, s.db, identityNodeID, nodeID, bootSessionID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+storageCopyColumns+` FROM computer_storage_copy_operations
		WHERE bound_node_id=? AND status IN ('reserved', 'prepared', 'published')
		AND (operation='clone' OR authority_revoked_ns IS NOT NULL)
		ORDER BY requested_ns, destination_computer_id`, nodeID)
	if err != nil {
		return nil, internalError(err, "list Computer Storage copy directives")
	}
	defer rows.Close()
	directives := []ComputerStorageCopyDirective{}
	for rows.Next() {
		row, err := scanComputerStorageCopy(rows)
		if err != nil {
			return nil, internalError(err, "scan Computer Storage copy directive")
		}
		directives = append(directives, storageCopyDirective(row))
	}
	return directives, rows.Err()
}

func (s *Store) ListNodeComputerRestoreRevocations(ctx context.Context, nodeID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT destination_computer_id FROM computer_storage_copy_operations
		WHERE bound_node_id=? AND operation='restore' AND status IN ('reserved', 'prepared')
		ORDER BY requested_ns, destination_computer_id`, nodeID)
	if err != nil {
		return nil, internalError(err, "list pending restore authority revocations")
	}
	defer rows.Close()
	var computerIDs []string
	for rows.Next() {
		var computerID string
		if err := rows.Scan(&computerID); err != nil {
			return nil, internalError(err, "scan pending restore authority revocation")
		}
		computerIDs = append(computerIDs, computerID)
	}
	return computerIDs, rows.Err()
}

func (s *Store) RecordComputerRestoreAuthorityRevoked(ctx context.Context, computerID string) error {
	result, err := s.db.ExecContext(ctx, `UPDATE computer_storage_copy_operations SET authority_revoked_ns=?
		WHERE destination_computer_id=? AND operation='restore' AND status IN ('reserved', 'prepared')`,
		canonicalTime(s.clock.Now()).UnixNano(), computerID)
	if err != nil {
		return internalError(err, "record restore authority revocation")
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return internalError(err, "count restore authority revocation")
	}
	if affected != 1 {
		return protocolError(contract.ErrorStaleIntentRevision, "Computer restore no longer owns authority revocation")
	}
	return nil
}

func validateStorageCopyReceipt(row computerStorageCopyRow, receipt ComputerStorageCopyReceipt) error {
	if receipt.Kind != computerStorageCopyReceiptKind || strings.TrimSpace(receipt.ReceiptID) == "" ||
		receipt.Operation != row.Operation || receipt.BackupID != row.BackupID || receipt.CopyID != row.CopyID ||
		receipt.SourceComputerID != row.SourceComputerID || receipt.SourceStorageID != row.SourceStorageID ||
		receipt.SourceGeneration != row.SourceGeneration || receipt.DestinationComputerID != row.DestinationComputerID ||
		receipt.DestinationStorageID != row.DestinationStorageID || receipt.DestinationGeneration != row.DestinationGeneration ||
		receipt.NodeID != row.BoundNodeID || receipt.RootInstanceID != row.RootInstanceID || receipt.JobID != row.JobID ||
		receipt.OperationRevision != row.OperationRevision || receipt.CleanupFence != row.CleanupFence ||
		receipt.HelperGeneration == 0 || receipt.SourceSize != row.SourceSize ||
		receipt.DestinationSize != row.DestinationSize || receipt.SourceDigest != row.SourceDigest ||
		!backupDigestPattern.MatchString(receipt.SourceDigest) || !backupDigestPattern.MatchString(receipt.DestinationDigest) {
		return protocolError(contract.ErrorConflict, "Computer Storage copy receipt does not match durable operation authority")
	}
	switch row.Operation {
	case "restore":
		if receipt.OSIdentityRekeyed || receipt.FilesystemExpanded != (row.DestinationSize > row.SourceSize) ||
			(row.DestinationSize == row.SourceSize && receipt.SourceDigest != receipt.DestinationDigest) {
			return protocolError(contract.ErrorConflict, "Computer restore receipt lacks required byte preservation or expansion evidence")
		}
	case "clone":
		if !receipt.OSIdentityRekeyed || receipt.FilesystemExpanded != (row.DestinationSize > row.SourceSize) {
			return protocolError(contract.ErrorConflict, "Computer clone receipt lacks required rekey or expansion evidence")
		}
	default:
		return protocolError(contract.ErrorInvalidRequest, "Computer Storage copy operation is invalid")
	}
	return nil
}

func validateOldBackupReceipt(row computerStorageCopyRow, receipt *ComputerBackupCopyReceipt) error {
	if !row.KeepOldBackup {
		if receipt != nil {
			return protocolError(contract.ErrorInvalidRequest, "restore did not precommit an old-generation Backup")
		}
		return nil
	}
	if receipt == nil || !row.OldBackupID.Valid || !row.OldCopyID.Valid {
		return protocolError(contract.ErrorConflict, "restore lacks its precommitted old-generation Backup evidence")
	}
	if receipt.Kind != computerBackupCopyReceiptKind || receipt.FailureCode != "" || receipt.CopyAbsent ||
		!backupDigestPattern.MatchString(receipt.ContentDigest) {
		return protocolErrorWithDetails(contract.ErrorConflict, map[string]any{
			"failure_code": receipt.FailureCode,
		}, "restore predecessor Backup copy did not complete successfully; the old generation remains current")
	}
	return validateBackupReceipt(computerBackupOperationRow{ComputerID: row.DestinationComputerID,
		OperationRevision: row.OperationRevision, BackupID: row.OldBackupID.String, CopyID: row.OldCopyID.String,
		StorageID: row.DestinationStorageID, StorageGeneration: row.OldGeneration,
		AllocatedSize: row.DestinationSize, BoundNodeID: row.BoundNodeID, RootInstanceID: row.RootInstanceID,
		JobID: row.JobID, CleanupFence: row.CleanupFence}, *receipt)
}

func abortRestoreForFailedPredecessorCopy(ctx context.Context, tx *sql.Tx, row computerStorageCopyRow,
	receipt ComputerBackupCopyReceipt, now time.Time) error {
	if err := validateBackupReceipt(computerBackupOperationRow{ComputerID: row.DestinationComputerID,
		OperationRevision: row.OperationRevision, BackupID: row.OldBackupID.String, CopyID: row.OldCopyID.String,
		StorageID: row.DestinationStorageID, StorageGeneration: row.OldGeneration,
		AllocatedSize: row.DestinationSize, BoundNodeID: row.BoundNodeID, RootInstanceID: row.RootInstanceID,
		JobID: row.JobID, CleanupFence: row.CleanupFence}, receipt); err != nil {
		return err
	}
	if receipt.Kind != computerBackupFailureReceiptKind {
		return protocolError(contract.ErrorInvalidRequest, "restore predecessor copy failure evidence is invalid")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE computer_storage_generations SET phase='retired', retired_ns=?
		WHERE computer_id=? AND storage_generation=? AND phase='staging'`, now.UnixNano(),
		row.DestinationComputerID, row.DestinationGeneration); err != nil {
		return internalError(err, "retire aborted restore staging generation")
	}
	receiptJSON, err := json.Marshal(receipt)
	if err != nil {
		return internalError(err, "encode failed restore predecessor copy")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE computer_storage_copy_operations SET status='failed',
		failure_code=?, old_backup_receipt_json=?, completed_ns=?
		WHERE destination_computer_id=? AND operation_revision=? AND status='reserved'`, receipt.FailureCode,
		receiptJSON, now.UnixNano(), row.DestinationComputerID, row.OperationRevision); err != nil {
		return internalError(err, "record failed restore predecessor copy")
	}
	result, err := tx.ExecContext(ctx, `UPDATE computers SET applied_revision=?, reconfiguration_phase='stable',
		reconfiguration_revision=NULL, updated_ns=? WHERE computer_id=? AND intent_revision=? AND reconfiguration_phase='restoring'`,
		row.OperationRevision, now.UnixNano(), row.DestinationComputerID, row.OperationRevision)
	if err != nil {
		return internalError(err, "abort restore after predecessor copy failure")
	}
	return requireComputerCAS(result, row.DestinationComputerID, row.OperationRevision)
}

func (s *Store) AcknowledgeComputerStorageCopy(ctx context.Context, identityNodeID, destinationComputerID string, request ComputerStorageCopyAcknowledgementRequest) (Computer, error) {
	if destinationComputerID == "" || request.NodeID == "" || request.BootSessionID == "" || request.IdempotencyKey == "" {
		return Computer{}, protocolError(contract.ErrorInvalidRequest, "complete Computer Storage copy acknowledgement fields are required")
	}
	bodyHash, err := storageCopyHash(request)
	if err != nil {
		return Computer{}, internalError(err, "encode Computer Storage copy acknowledgement")
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Computer{}, internalError(err, "begin Computer Storage copy acknowledgement")
	}
	defer tx.Rollback()
	if err := validateBackupNodeSession(ctx, tx, identityNodeID, request.NodeID, request.BootSessionID); err != nil {
		return Computer{}, err
	}
	row, err := scanComputerStorageCopy(tx.QueryRowContext(ctx, `SELECT `+storageCopyColumns+`
		FROM computer_storage_copy_operations WHERE destination_computer_id=?
		ORDER BY operation_revision DESC LIMIT 1`, destinationComputerID))
	if errors.Is(err, sql.ErrNoRows) {
		return Computer{}, protocolError(contract.ErrorNotFound, "Computer Storage copy operation was not found")
	}
	if err != nil {
		return Computer{}, internalError(err, "read Computer Storage copy acknowledgement target")
	}
	if err := validateBackupAcknowledgementAuthority(ctx, tx, identityNodeID, request.NodeID, row.BoundNodeID, row.RootInstanceID); err != nil {
		return Computer{}, err
	}
	if row.KeepOldBackup && request.OldBackupReceipt != nil && request.OldBackupReceipt.Kind == computerBackupFailureReceiptKind {
		if err := abortRestoreForFailedPredecessorCopy(ctx, tx, row, *request.OldBackupReceipt, now); err != nil {
			return Computer{}, err
		}
		if err := tx.Commit(); err != nil {
			return Computer{}, internalError(err, "commit failed restore predecessor copy outcome")
		}
		return Computer{}, protocolErrorWithDetails(contract.ErrorConflict, map[string]any{
			"failure_code": request.OldBackupReceipt.FailureCode,
		}, "restore predecessor Backup copy failed; the old generation remains current")
	}
	if err := validateStorageCopyReceipt(row, request.Receipt); err != nil {
		return Computer{}, err
	}
	if err := validateOldBackupReceipt(row, request.OldBackupReceipt); err != nil {
		return Computer{}, err
	}
	if row.AcknowledgementKey.Valid {
		if row.AcknowledgementKey.String != request.IdempotencyKey || !row.AcknowledgementHash.Valid || row.AcknowledgementHash.String != bodyHash {
			return Computer{}, protocolError(contract.ErrorIdempotencyConflict, "Computer Storage copy acknowledgement differs from durable evidence")
		}
		if row.Status == "prepared" {
			if err := tx.Rollback(); err != nil {
				return Computer{}, internalError(err, "close verified Computer Storage copy replay")
			}
			return s.publishVerifiedComputerStorageCopy(ctx, destinationComputerID, row.OperationRevision)
		}
		return readComputerAuthority(ctx, tx, destinationComputerID, now)
	}
	if row.Status != "reserved" {
		return Computer{}, protocolError(contract.ErrorConflict, "Computer Storage copy operation is not awaiting verification")
	}
	receiptJSON, err := json.Marshal(request.Receipt)
	if err != nil {
		return Computer{}, internalError(err, "encode Computer Storage copy receipt")
	}
	var oldReceiptJSON any
	if request.OldBackupReceipt != nil {
		encoded, err := json.Marshal(request.OldBackupReceipt)
		if err != nil {
			return Computer{}, internalError(err, "encode old-generation Backup receipt")
		}
		oldReceiptJSON = encoded
	}
	if _, err := tx.ExecContext(ctx, `UPDATE computer_storage_copy_operations SET status='prepared',
		verification_receipt_json=?, verification_receipt_hash=?, old_backup_receipt_json=?,
		acknowledgement_key=?, acknowledgement_hash=?, verified_ns=?
		WHERE destination_computer_id=? AND status='reserved'`, receiptJSON, bodyHash, oldReceiptJSON,
		request.IdempotencyKey, bodyHash, now.UnixNano(), destinationComputerID); err != nil {
		return Computer{}, internalError(err, "record Computer Storage copy verification")
	}
	if err := tx.Commit(); err != nil {
		return Computer{}, internalError(err, "commit Computer Storage copy verification")
	}
	return s.publishVerifiedComputerStorageCopy(ctx, destinationComputerID, row.OperationRevision)
}

func (s *Store) publishVerifiedComputerStorageCopy(ctx context.Context, destinationComputerID string, operationRevision int64) (Computer, error) {
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Computer{}, internalError(err, "begin verified Computer Storage copy publication")
	}
	defer tx.Rollback()
	row, err := scanComputerStorageCopy(tx.QueryRowContext(ctx, `SELECT `+storageCopyColumns+`
		FROM computer_storage_copy_operations WHERE destination_computer_id=? AND operation_revision=?`,
		destinationComputerID, operationRevision))
	if err != nil {
		return Computer{}, internalError(err, "read verified Computer Storage copy")
	}
	if row.Status == "published" || row.Status == "retired" || row.Status == "complete" {
		return readComputerAuthority(ctx, tx, destinationComputerID, now)
	}
	if row.Status != "prepared" {
		return Computer{}, protocolError(contract.ErrorConflict, "Computer Storage copy has no durable positive verification")
	}
	if row.Operation == "restore" {
		var revoked sql.NullInt64
		if err := tx.QueryRowContext(ctx, `SELECT authority_revoked_ns FROM computer_storage_copy_operations
			WHERE destination_computer_id=? AND operation_revision=?`, destinationComputerID, operationRevision).Scan(&revoked); err != nil {
			return Computer{}, internalError(err, "read durable restore authority revocation")
		}
		if !revoked.Valid || revoked.Int64 <= 0 {
			return Computer{}, protocolError(contract.ErrorConflict, "Computer restore authority was not durably revoked before publication")
		}
	}
	var receiptJSON []byte
	var oldReceiptJSON sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT verification_receipt_json, CAST(old_backup_receipt_json AS TEXT)
		FROM computer_storage_copy_operations WHERE destination_computer_id=? AND operation_revision=?`,
		destinationComputerID, operationRevision).Scan(&receiptJSON, &oldReceiptJSON); err != nil {
		return Computer{}, internalError(err, "read Computer Storage copy verification evidence")
	}
	var request ComputerStorageCopyAcknowledgementRequest
	if err := json.Unmarshal(receiptJSON, &request.Receipt); err != nil {
		return Computer{}, internalError(err, "decode Computer Storage copy verification evidence")
	}
	if oldReceiptJSON.Valid {
		var oldReceipt ComputerBackupCopyReceipt
		if err := json.Unmarshal([]byte(oldReceiptJSON.String), &oldReceipt); err != nil {
			return Computer{}, internalError(err, "decode restore predecessor Backup evidence")
		}
		request.OldBackupReceipt = &oldReceipt
	}
	if err := publishComputerStorageCopy(ctx, tx, row, request, now); err != nil {
		return Computer{}, err
	}
	computer, err := readComputerAuthority(ctx, tx, destinationComputerID, now)
	if err != nil {
		return Computer{}, internalError(err, "read published Computer Storage copy")
	}
	if err := tx.Commit(); err != nil {
		return Computer{}, internalError(err, "commit Computer Storage copy publication")
	}
	s.notifyComputerPolicyChanged()
	return computer, nil
}

func publishComputerStorageCopy(ctx context.Context, tx *sql.Tx, row computerStorageCopyRow, request ComputerStorageCopyAcknowledgementRequest, now time.Time) error {
	computer, err := readComputerAuthority(ctx, tx, row.DestinationComputerID, now)
	if err != nil {
		return internalError(err, "read Computer Storage copy publication authority")
	}
	expectedPhase := ComputerReconfigurationRestoring
	if row.Operation == "clone" {
		expectedPhase = ComputerReconfigurationCloning
	}
	if computer.IntentRevision != row.OperationRevision || computer.ReconfigurationPhase != expectedPhase ||
		computer.ReconfigurationRevision == nil || *computer.ReconfigurationRevision != row.OperationRevision ||
		computer.DesiredState != contract.ServiceDesiredStopped ||
		(computer.CurrentJob.State != contract.JobStopped && computer.CurrentJob.State != contract.JobFailed) {
		return protocolError(contract.ErrorStaleIntentRevision, "Computer Storage copy no longer owns publication")
	}
	if row.Operation == "restore" {
		if computer.StorageGeneration != row.OldGeneration {
			return protocolError(contract.ErrorStorageReferenceConflict, "Computer restore predecessor changed")
		}
		retired, err := tx.ExecContext(ctx, `UPDATE computer_storage_generations SET phase='retired', retired_ns=?
			WHERE computer_id=? AND storage_generation=? AND phase='current'`, now.UnixNano(), row.DestinationComputerID, row.OldGeneration)
		if err != nil {
			return internalError(err, "retire restored Computer predecessor")
		}
		if err := requireSingleStorageGenerationMutation(retired, "retire restored Computer predecessor"); err != nil {
			return err
		}
		if row.KeepOldBackup {
			old := request.OldBackupReceipt
			provenanceID := newID("storage-provenance")
			if _, err := tx.ExecContext(ctx, `INSERT INTO backups(backup_id, computer_id, source_storage_id,
				source_generation, created_ns, allocated_size, content_digest, encryption, provenance_id, status)
				VALUES(?, ?, ?, ?, ?, ?, ?, 'none', ?, 'available')`, row.OldBackupID.String,
				row.DestinationComputerID, row.DestinationStorageID, row.OldGeneration, now.UnixNano(),
				row.DestinationSize, old.ContentDigest, provenanceID); err != nil {
				return internalError(err, "publish precommitted old-generation Backup")
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO backup_copies(copy_id, backup_id, node_id,
				root_instance_id, allocated_size, content_digest, phase, created_ns)
				VALUES(?, ?, ?, ?, ?, ?, 'published', ?)`, row.OldCopyID.String, row.OldBackupID.String,
				row.BoundNodeID, row.RootInstanceID, row.DestinationSize, old.ContentDigest, now.UnixNano()); err != nil {
				return internalError(err, "publish precommitted old-generation Backup copy")
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO storage_provenance(provenance_id, kind,
				source_storage_id, source_generation, backup_id, created_ns) VALUES(?, 'backup', ?, ?, ?, ?)`,
				provenanceID, row.DestinationStorageID, row.OldGeneration, row.OldBackupID.String, now.UnixNano()); err != nil {
				return internalError(err, "publish old-generation Backup provenance")
			}
		}
	}
	published, err := tx.ExecContext(ctx, `UPDATE computer_storage_generations SET phase='current'
		WHERE computer_id=? AND storage_generation=? AND phase='staging' AND reset_revision=?`,
		row.DestinationComputerID, row.DestinationGeneration, row.OperationRevision)
	if err != nil {
		return internalError(err, "publish copied Computer Storage generation")
	}
	if err := requireSingleStorageGenerationMutation(published, "publish copied Computer Storage generation"); err != nil {
		return err
	}
	if row.Operation != "clone" {
		if _, err := tx.ExecContext(ctx, `INSERT INTO storage_provenance(provenance_id, kind,
		source_storage_id, source_generation, backup_id, destination_computer_id,
		destination_storage_id, destination_generation, created_ns) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			newID("storage-provenance"), row.Operation, row.SourceStorageID, row.SourceGeneration,
			row.BackupID, row.DestinationComputerID, row.DestinationStorageID,
			row.DestinationGeneration, now.UnixNano()); err != nil {
			return internalError(err, "publish destination Storage provenance")
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET state='stopped', current_attempt_id=NULL,
		updated_ns=? WHERE job_id=?`, now.UnixNano(), row.JobID); err != nil {
		return internalError(err, "keep copied Computer stopped")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE service_jobs SET desired_state='stopped',
		published_attempt_id=NULL, healthy_since_ns=NULL, next_restart_at=NULL WHERE job_id=?`, row.JobID); err != nil {
		return internalError(err, "keep copied Computer service stopped")
	}
	status := "published"
	phase := expectedPhase
	appliedRevision := computer.AppliedRevision
	if row.Operation == "clone" {
		status, phase, appliedRevision = "complete", ComputerReconfigurationStable, row.OperationRevision
	}
	result, err := tx.ExecContext(ctx, `UPDATE computers SET storage_generation=?, applied_revision=?,
		reconfiguration_phase=?, reconfiguration_revision=CASE WHEN ?='stable' THEN NULL ELSE reconfiguration_revision END,
		updated_ns=? WHERE computer_id=? AND intent_revision=? AND reconfiguration_phase=?`,
		row.DestinationGeneration, appliedRevision, phase, phase, now.UnixNano(), row.DestinationComputerID,
		row.OperationRevision, expectedPhase)
	if err != nil {
		return internalError(err, "publish Computer Storage copy authority")
	}
	if err := requireComputerCAS(result, row.DestinationComputerID, row.OperationRevision); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE computer_storage_copy_operations SET status=?, published_ns=?,
		completed_ns=CASE WHEN ?='complete' THEN ? ELSE completed_ns END WHERE destination_computer_id=? AND status='prepared'`,
		status, now.UnixNano(), status, now.UnixNano(), row.DestinationComputerID); err != nil {
		return internalError(err, "complete Computer Storage copy publication")
	}
	return nil
}

func (s *Store) AcknowledgeComputerRestoreRetirement(ctx context.Context, identityNodeID, computerID string, request RemovalAcknowledgementRequest) (Computer, error) {
	if computerID == "" || request.NodeID == "" || request.BootSessionID == "" || request.RootInstanceID == "" ||
		request.CleanupFence == "" || request.IdempotencyKey == "" || request.RemovalGeneration == 0 {
		return Computer{}, protocolError(contract.ErrorInvalidRequest, "complete Computer restore retirement acknowledgement fields are required")
	}
	bodyHash, err := storageCopyHash(request)
	if err != nil {
		return Computer{}, internalError(err, "encode Computer restore retirement acknowledgement")
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Computer{}, internalError(err, "begin Computer restore retirement acknowledgement")
	}
	defer tx.Rollback()
	if err := validateStorageResetNode(ctx, tx, identityNodeID, request.NodeID, request.BootSessionID); err != nil {
		return Computer{}, err
	}
	row, err := scanComputerStorageCopy(tx.QueryRowContext(ctx, `SELECT `+storageCopyColumns+`
		FROM computer_storage_copy_operations WHERE destination_computer_id=?
		ORDER BY operation_revision DESC LIMIT 1`, computerID))
	if err != nil {
		return Computer{}, protocolError(contract.ErrorStaleIntentRevision, "Computer restore operation is no longer current")
	}
	if row.Operation != "restore" || (row.Status != "published" && row.Status != "retired") || request.NodeID != row.BoundNodeID ||
		request.RootInstanceID != row.RootInstanceID || request.CleanupFence != row.CleanupFence ||
		request.RemovalGeneration != uint64(row.OperationRevision) {
		return Computer{}, protocolError(contract.ErrorStaleFence, "Computer restore retirement authority does not match")
	}
	if row.Status == "retired" {
		var key, hash sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT retirement_acknowledgement_key, retirement_acknowledgement_hash
			FROM computer_storage_copy_operations WHERE destination_computer_id=? AND operation_revision=?`,
			computerID, row.OperationRevision).Scan(&key, &hash); err != nil {
			return Computer{}, internalError(err, "read restore retirement replay")
		}
		if !key.Valid || key.String != request.IdempotencyKey || !hash.Valid || hash.String != bodyHash {
			return Computer{}, protocolError(contract.ErrorIdempotencyConflict, "Computer restore retirement replay differs from durable evidence")
		}
		return readComputerAuthority(ctx, tx, computerID, now)
	}
	computer, err := readComputerAuthority(ctx, tx, computerID, now)
	if err != nil {
		return Computer{}, internalError(err, "read Computer restore retirement target")
	}
	if computer.StorageGeneration != row.DestinationGeneration || computer.ReconfigurationPhase != ComputerReconfigurationRestoring {
		return Computer{}, protocolError(contract.ErrorStaleIntentRevision, "Computer restore no longer owns predecessor retirement")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE computer_storage_copy_operations SET status='retired', completed_ns=?,
		retirement_acknowledgement_key=?, retirement_acknowledgement_hash=?
		WHERE destination_computer_id=? AND status='published'`, now.UnixNano(), request.IdempotencyKey, bodyHash, computerID); err != nil {
		return Computer{}, internalError(err, "complete restored predecessor retirement")
	}
	result, err := tx.ExecContext(ctx, `UPDATE computers SET applied_revision=?, reconfiguration_phase='stable',
		reconfiguration_revision=NULL, updated_ns=? WHERE computer_id=? AND intent_revision=? AND reconfiguration_phase='restoring'`,
		row.OperationRevision, now.UnixNano(), computerID, row.OperationRevision)
	if err != nil {
		return Computer{}, internalError(err, "complete Computer restore lifecycle")
	}
	if err := requireComputerCAS(result, computerID, row.OperationRevision); err != nil {
		return Computer{}, err
	}
	computer, err = readComputerAuthority(ctx, tx, computerID, now)
	if err != nil {
		return Computer{}, internalError(err, "read completed Computer restore")
	}
	if err := tx.Commit(); err != nil {
		return Computer{}, internalError(err, "commit Computer restore retirement")
	}
	return computer, nil
}

func (directive ComputerStorageCopyDirective) String() string {
	return fmt.Sprintf("%s:%s@%d", directive.Operation, directive.DestinationComputerID, directive.OperationRevision)
}
