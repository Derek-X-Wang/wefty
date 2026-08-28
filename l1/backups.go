package l1

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const (
	BackupEncryptionNone             = "none"
	computerBackupCopyReceiptKind    = "computer_backup_copy_verified"
	computerBackupFailureReceiptKind = "computer_backup_copy_failed_absent"
	computerBackupRemovalReceiptKind = "computer_backup_copy_removed"
)

var backupDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

type StorageProvenance struct {
	ProvenanceID     string    `json:"provenance_id"`
	Kind             string    `json:"kind"`
	SourceStorageID  string    `json:"source_storage_id"`
	SourceGeneration int64     `json:"source_generation"`
	BackupID         string    `json:"backup_id"`
	CreatedAt        time.Time `json:"created_at"`
}

type Backup struct {
	BackupID         string            `json:"backup_id"`
	ComputerID       string            `json:"computer_id"`
	SourceStorageID  string            `json:"source_storage_id"`
	SourceGeneration int64             `json:"source_generation"`
	CreatedAt        time.Time         `json:"created_at"`
	AllocatedSize    int64             `json:"allocated_size"`
	ContentDigest    string            `json:"content_digest"`
	Encryption       string            `json:"encryption"`
	Provenance       StorageProvenance `json:"storage_provenance"`
	Copies           []BackupCopy      `json:"copies"`
	Status           string            `json:"status"`
}

type BackupCopy struct {
	CopyID         string     `json:"copy_id"`
	BackupID       string     `json:"backup_id"`
	NodeID         string     `json:"node_id"`
	RootInstanceID string     `json:"root_instance_id"`
	AllocatedSize  int64      `json:"allocated_size"`
	ContentDigest  string     `json:"content_digest"`
	Phase          string     `json:"phase"`
	CreatedAt      time.Time  `json:"created_at"`
	RemovedAt      *time.Time `json:"removed_at,omitempty"`
}

type BackupList struct {
	Backups []Backup `json:"backups"`
}

type ComputerBackupCreateRequest struct {
	ComputerMutationPrecondition
	IdempotencyKey string `json:"idempotency_key"`
}

type ComputerBackupDirective struct {
	BackupID          string `json:"backup_id"`
	CopyID            string `json:"copy_id"`
	ComputerID        string `json:"computer_id"`
	StorageID         string `json:"storage_id"`
	StorageGeneration int64  `json:"storage_generation"`
	AllocatedSize     int64  `json:"allocated_size"`
	BoundNodeID       string `json:"bound_node_id"`
	RootInstanceID    string `json:"root_instance_id"`
	JobID             string `json:"job_id"`
	OperationRevision int64  `json:"operation_revision"`
	CleanupFence      string `json:"cleanup_fence"`
}

type ComputerBackupCopyReceipt = contract.ComputerBackupCopyReceipt

type ComputerBackupAcknowledgementRequest struct {
	NodeID         string                    `json:"node_id"`
	BootSessionID  string                    `json:"boot_session_id"`
	IdempotencyKey string                    `json:"idempotency_key"`
	Receipt        ComputerBackupCopyReceipt `json:"receipt"`
}

type ComputerBackupAcknowledgementResponse struct {
	Backup   *Backup  `json:"backup,omitempty"`
	Computer Computer `json:"computer"`
}

type ComputerBackupPruneRequest struct {
	ComputerMutationPrecondition
	BackupID       string `json:"-"`
	IdempotencyKey string `json:"idempotency_key"`
}

type ComputerBackupPruneDirective struct {
	BackupID          string `json:"backup_id"`
	CopyID            string `json:"copy_id"`
	ComputerID        string `json:"computer_id"`
	StorageID         string `json:"storage_id"`
	StorageGeneration int64  `json:"storage_generation"`
	AllocatedSize     int64  `json:"allocated_size"`
	BoundNodeID       string `json:"bound_node_id"`
	RootInstanceID    string `json:"root_instance_id"`
	OperationRevision int64  `json:"operation_revision"`
	CleanupFence      string `json:"cleanup_fence"`
}

type ComputerBackupCopyRemovalReceipt = contract.ComputerBackupCopyRemovalReceipt

type ComputerBackupPruneAcknowledgementRequest struct {
	NodeID         string                           `json:"node_id"`
	BootSessionID  string                           `json:"boot_session_id"`
	IdempotencyKey string                           `json:"idempotency_key"`
	Receipt        ComputerBackupCopyRemovalReceipt `json:"receipt"`
}

type computerBackupOperationRow struct {
	ComputerID           string
	OperationRevision    int64
	BackupID             string
	CopyID               string
	StorageID            string
	StorageGeneration    int64
	AllocatedSize        int64
	BoundNodeID          string
	RootInstanceID       string
	JobID                string
	CleanupFence         string
	IdempotencyKey       string
	RequestHash          string
	Status               string
	FailureCode          string
	ResumeDesiredRunning bool
	AcknowledgementKey   sql.NullString
	AcknowledgementHash  sql.NullString
}

func backupMutationHash(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(payload)
	return hex.EncodeToString(hash[:]), nil
}

func readComputerBackupOperationByKey(ctx context.Context, q queryer, computerID, key string) (computerBackupOperationRow, error) {
	var row computerBackupOperationRow
	err := q.QueryRowContext(ctx, `SELECT computer_id, operation_revision, backup_id, copy_id,
		source_storage_id, source_generation, allocated_size, bound_node_id, root_instance_id,
		job_id, cleanup_fence, idempotency_key, request_hash, status, failure_code,
		resume_desired_running, acknowledgement_key, acknowledgement_hash
		FROM computer_backup_operations WHERE computer_id=? AND idempotency_key=?`, computerID, key).Scan(
		&row.ComputerID, &row.OperationRevision, &row.BackupID, &row.CopyID,
		&row.StorageID, &row.StorageGeneration, &row.AllocatedSize, &row.BoundNodeID,
		&row.RootInstanceID, &row.JobID, &row.CleanupFence, &row.IdempotencyKey,
		&row.RequestHash, &row.Status, &row.FailureCode, &row.ResumeDesiredRunning,
		&row.AcknowledgementKey, &row.AcknowledgementHash)
	return row, err
}

func (s *Store) BeginComputerBackup(ctx context.Context, computerID string, request ComputerBackupCreateRequest) (Computer, bool, error) {
	request.Actor = strings.TrimSpace(request.Actor)
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	if strings.TrimSpace(computerID) == "" || request.IdempotencyKey == "" {
		return Computer{}, false, protocolError(contract.ErrorInvalidRequest, "computer_id and idempotency_key are required")
	}
	requestHash, err := backupMutationHash(struct {
		Request ComputerBackupCreateRequest `json:"request"`
		Actor   string                      `json:"actor"`
	}{Request: request, Actor: request.Actor})
	if err != nil {
		return Computer{}, false, internalError(err, "encode Computer Backup request")
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Computer{}, false, internalError(err, "begin Computer Backup")
	}
	defer tx.Rollback()
	computer, err := readComputerAuthority(ctx, tx, computerID, now)
	if errors.Is(err, sql.ErrNoRows) {
		return Computer{}, false, protocolError(contract.ErrorNotFound, "Computer %q was not found", computerID)
	}
	if err != nil {
		return Computer{}, false, internalError(err, "read Computer Backup target")
	}
	if replay, replayErr := readComputerBackupOperationByKey(ctx, tx, computerID, request.IdempotencyKey); replayErr == nil {
		if replay.RequestHash != requestHash {
			return Computer{}, false, protocolError(contract.ErrorIdempotencyConflict, "Computer Backup idempotency key was reused with different authority")
		}
		return computer, true, nil
	} else if !errors.Is(replayErr, sql.ErrNoRows) {
		return Computer{}, false, internalError(replayErr, "read Computer Backup replay")
	}
	if err := validateComputerPrecondition(computer, request.ComputerMutationPrecondition); err != nil {
		return Computer{}, false, err
	}
	if computer.DesiredState == contract.ServiceDesiredRemoved {
		return Computer{}, false, protocolError(contract.ErrorConflict, "Computer %q is being removed", computerID)
	}
	if computer.CurrentJob.State == contract.JobFailed {
		return Computer{}, false, protocolError(contract.ErrorConflict,
			"Computer %q is latched failed; stop or restart it explicitly before Backup creation", computerID)
	}
	if computer.ReconfigurationPhase != ComputerReconfigurationStable {
		return Computer{}, false, protocolError(contract.ErrorConflict, "Computer %q is in reconfiguration phase %q", computerID, computer.ReconfigurationPhase)
	}
	var retained int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM backups WHERE computer_id=? AND status<>'pruned'`, computerID).Scan(&retained); err != nil {
		return Computer{}, false, internalError(err, "count retained Computer Backups")
	}
	if computer.BackupCap == 0 || retained >= computer.BackupCap {
		return Computer{}, false, protocolErrorWithDetails(contract.ErrorConflict, map[string]any{
			"computer_id": computerID, "backup_cap": computer.BackupCap, "retained_backups": retained,
		}, "Computer %q is at its Backup cap", computerID)
	}
	boundNodeID := computer.BoundNodeID
	if boundNodeID == "" {
		return Computer{}, false, protocolError(contract.ErrorConflict,
			"Computer %q has no bound source Node to back up", computerID)
	}
	var rootInstanceID string
	if err := tx.QueryRowContext(ctx, `SELECT root_instance_id FROM nodes WHERE node_id=?`, boundNodeID).Scan(&rootInstanceID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Computer{}, false, protocolError(contract.ErrorConflict, "bound node %q was not found", boundNodeID)
		}
		return Computer{}, false, internalError(err, "read Computer Backup managed-root authority")
	}
	if rootInstanceID == "" {
		return Computer{}, false, protocolError(contract.ErrorConflict, "bound node %q has no registered managed-root instance", boundNodeID)
	}
	nextRevision := computer.IntentRevision + 1
	result, err := tx.ExecContext(ctx, `UPDATE computers SET intent_revision=?, reconfiguration_phase=?,
		reconfiguration_revision=?, updated_ns=? WHERE computer_id=? AND intent_revision=?`,
		nextRevision, ComputerReconfigurationBackingUp, nextRevision, now.UnixNano(), computerID, computer.IntentRevision)
	if err != nil {
		return Computer{}, false, internalError(err, "reserve Computer Backup intent")
	}
	if err := requireComputerCAS(result, computerID, computer.IntentRevision); err != nil {
		return Computer{}, false, err
	}
	if err := insertComputerIntent(ctx, tx, computerID, nextRevision, ComputerIntentBackupCreate,
		computer.DesiredState, computer.StorageID, computer.StorageGeneration, computer.CurrentJobID,
		computer.CurrentSpecRevision, request.Actor, now); err != nil {
		return Computer{}, false, err
	}
	if err := setComputerServiceDesiredState(ctx, tx, computer.CurrentJob, contract.ServiceDesiredStopped, now); err != nil {
		return Computer{}, false, err
	}
	backupID := newID("backup")
	copyID := newID("backup-copy")
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_backup_operations(
		computer_id, operation_revision, backup_id, copy_id, source_storage_id, source_generation,
		allocated_size, bound_node_id, root_instance_id, job_id, cleanup_fence, idempotency_key,
		request_hash, status, resume_desired_running, requested_ns
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'planned', ?, ?)`,
		computerID, nextRevision, backupID, copyID, computer.StorageID, computer.StorageGeneration,
		computer.DesiredDiskBytes, boundNodeID, rootInstanceID, computer.CurrentJobID, newID("backup-fence"),
		request.IdempotencyKey, requestHash, computer.DesiredState == contract.ServiceDesiredRunning, now.UnixNano()); err != nil {
		return Computer{}, false, internalError(err, "persist planned Computer Backup copy")
	}
	updated, err := readComputerAuthority(ctx, tx, computerID, now)
	if err != nil {
		return Computer{}, false, internalError(err, "read planned Computer Backup")
	}
	if err := tx.Commit(); err != nil {
		return Computer{}, false, internalError(err, "commit planned Computer Backup")
	}
	return updated, false, nil
}

func validateBackupNodeSession(ctx context.Context, q queryer, identityNodeID, nodeID, bootSessionID string) error {
	var storedIdentity, storedBoot string
	if err := q.QueryRowContext(ctx, `SELECT identity_node_id, boot_session_id FROM nodes WHERE node_id=?`, nodeID).Scan(&storedIdentity, &storedBoot); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return protocolError(contract.ErrorNodeNotRegistered, "node %q is not registered", nodeID)
		}
		return internalError(err, "read Computer Backup node session")
	}
	if storedIdentity != identityNodeID {
		return protocolError(contract.ErrorIdentityBound, "stable node %q is bound to another Fabric identity", nodeID)
	}
	if storedBoot != bootSessionID {
		return protocolError(contract.ErrorNodeSessionReplaced, "node %q boot session has been replaced", nodeID)
	}
	return nil
}

func (s *Store) ListNodeComputerBackupDirectives(ctx context.Context, identityNodeID, nodeID, bootSessionID string) ([]ComputerBackupDirective, error) {
	if err := validateBackupNodeSession(ctx, s.db, identityNodeID, nodeID, bootSessionID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT o.backup_id, o.copy_id, o.computer_id,
		o.source_storage_id, o.source_generation, o.allocated_size, o.bound_node_id,
		o.root_instance_id, o.job_id, o.operation_revision, o.cleanup_fence
		FROM computer_backup_operations o
		JOIN computers c ON c.computer_id=o.computer_id
		JOIN jobs j ON j.job_id=o.job_id
		WHERE o.bound_node_id=? AND o.status='planned'
		AND c.reconfiguration_phase='backing_up' AND c.reconfiguration_revision=o.operation_revision
		AND j.state IN (?, ?)
		ORDER BY o.requested_ns, o.copy_id`, nodeID, contract.JobStopped, contract.JobFailed)
	if err != nil {
		return nil, internalError(err, "list Computer Backup directives")
	}
	defer rows.Close()
	directives := []ComputerBackupDirective{}
	for rows.Next() {
		var directive ComputerBackupDirective
		if err := rows.Scan(&directive.BackupID, &directive.CopyID, &directive.ComputerID,
			&directive.StorageID, &directive.StorageGeneration, &directive.AllocatedSize,
			&directive.BoundNodeID, &directive.RootInstanceID, &directive.JobID,
			&directive.OperationRevision, &directive.CleanupFence); err != nil {
			return nil, internalError(err, "scan Computer Backup directive")
		}
		directives = append(directives, directive)
	}
	return directives, rows.Err()
}

func validateBackupReceipt(row computerBackupOperationRow, receipt ComputerBackupCopyReceipt) error {
	if receipt.ReceiptID == "" || receipt.BackupID != row.BackupID || receipt.CopyID != row.CopyID ||
		receipt.ComputerID != row.ComputerID || receipt.StorageID != row.StorageID ||
		receipt.StorageGeneration != row.StorageGeneration || receipt.NodeID != row.BoundNodeID ||
		receipt.RootInstanceID != row.RootInstanceID || receipt.JobID != row.JobID ||
		receipt.OperationRevision != row.OperationRevision || receipt.CleanupFence != row.CleanupFence ||
		receipt.HelperGeneration == 0 || receipt.AllocatedSize != row.AllocatedSize ||
		receipt.Encryption != BackupEncryptionNone {
		return protocolError(contract.ErrorConflict, "Computer Backup receipt does not match planned copy authority")
	}
	switch receipt.Kind {
	case computerBackupCopyReceiptKind:
		if receipt.FailureCode != "" || receipt.CopyAbsent || !backupDigestPattern.MatchString(receipt.ContentDigest) {
			return protocolError(contract.ErrorInvalidRequest, "successful Computer Backup receipt is incomplete")
		}
	case computerBackupFailureReceiptKind:
		if receipt.ContentDigest != "" || !receipt.CopyAbsent ||
			(receipt.FailureCode != "insufficient_disk" && receipt.FailureCode != "digest_mismatch") {
			return protocolError(contract.ErrorInvalidRequest, "failed Computer Backup receipt lacks positive copy absence")
		}
	default:
		return protocolError(contract.ErrorInvalidRequest, "Computer Backup receipt kind is invalid")
	}
	return nil
}

func (s *Store) AcknowledgeComputerBackup(ctx context.Context, identityNodeID, computerID string, request ComputerBackupAcknowledgementRequest) (Backup, Computer, error) {
	if request.NodeID == "" || request.BootSessionID == "" || request.IdempotencyKey == "" {
		return Backup{}, Computer{}, protocolError(contract.ErrorInvalidRequest, "complete Computer Backup acknowledgement fields are required")
	}
	bodyHash, err := backupMutationHash(request)
	if err != nil {
		return Backup{}, Computer{}, internalError(err, "encode Computer Backup acknowledgement")
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Backup{}, Computer{}, internalError(err, "begin Computer Backup acknowledgement")
	}
	defer tx.Rollback()
	if err := validateBackupNodeSession(ctx, tx, identityNodeID, request.NodeID, request.BootSessionID); err != nil {
		return Backup{}, Computer{}, err
	}
	var row computerBackupOperationRow
	err = tx.QueryRowContext(ctx, `SELECT computer_id, operation_revision, backup_id, copy_id,
		source_storage_id, source_generation, allocated_size, bound_node_id, root_instance_id,
		job_id, cleanup_fence, idempotency_key, request_hash, status, failure_code,
		resume_desired_running, acknowledgement_key, acknowledgement_hash
		FROM computer_backup_operations WHERE computer_id=? AND copy_id=?`, computerID, request.Receipt.CopyID).Scan(
		&row.ComputerID, &row.OperationRevision, &row.BackupID, &row.CopyID, &row.StorageID,
		&row.StorageGeneration, &row.AllocatedSize, &row.BoundNodeID, &row.RootInstanceID,
		&row.JobID, &row.CleanupFence, &row.IdempotencyKey, &row.RequestHash, &row.Status,
		&row.FailureCode, &row.ResumeDesiredRunning, &row.AcknowledgementKey, &row.AcknowledgementHash)
	if errors.Is(err, sql.ErrNoRows) {
		return Backup{}, Computer{}, protocolError(contract.ErrorNotFound, "Computer Backup copy was not planned")
	}
	if err != nil {
		return Backup{}, Computer{}, internalError(err, "read Computer Backup acknowledgement target")
	}
	if err := validateBackupReceipt(row, request.Receipt); err != nil {
		return Backup{}, Computer{}, err
	}
	if row.AcknowledgementKey.Valid {
		if row.AcknowledgementKey.String != request.IdempotencyKey || !row.AcknowledgementHash.Valid || row.AcknowledgementHash.String != bodyHash {
			return Backup{}, Computer{}, protocolError(contract.ErrorConflict, "Computer Backup acknowledgement replay differs from the accepted receipt")
		}
		var backup Backup
		if row.Status == "published" {
			backup, err = readBackup(ctx, tx, row.BackupID)
			if err != nil {
				return Backup{}, Computer{}, internalError(err, "read replayed published Backup")
			}
		}
		computer, readErr := readComputerAuthority(ctx, tx, computerID, now)
		return backup, computer, readErr
	}
	if row.Status != "planned" {
		return Backup{}, Computer{}, protocolError(contract.ErrorConflict, "Computer Backup operation is no longer planned")
	}
	receiptJSON, err := json.Marshal(request.Receipt)
	if err != nil {
		return Backup{}, Computer{}, internalError(err, "encode Computer Backup receipt")
	}
	status := "published"
	failureCode := ""
	var backup Backup
	if request.Receipt.Kind == computerBackupCopyReceiptKind {
		provenanceID := newID("storage-provenance")
		if _, err := tx.ExecContext(ctx, `INSERT INTO backups(backup_id, computer_id, source_storage_id,
			source_generation, created_ns, allocated_size, content_digest, encryption, provenance_id, status)
			VALUES(?, ?, ?, ?, ?, ?, ?, 'none', ?, 'available')`, row.BackupID, row.ComputerID,
			row.StorageID, row.StorageGeneration, now.UnixNano(), row.AllocatedSize,
			request.Receipt.ContentDigest, provenanceID); err != nil {
			return Backup{}, Computer{}, internalError(err, "publish immutable Backup")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO backup_copies(copy_id, backup_id, node_id,
			root_instance_id, allocated_size, content_digest, phase, created_ns)
			VALUES(?, ?, ?, ?, ?, ?, 'published', ?)`, row.CopyID, row.BackupID, row.BoundNodeID,
			row.RootInstanceID, row.AllocatedSize, request.Receipt.ContentDigest, now.UnixNano()); err != nil {
			return Backup{}, Computer{}, internalError(err, "publish Backup copy")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO storage_provenance(provenance_id, kind,
			source_storage_id, source_generation, backup_id, created_ns)
			VALUES(?, 'backup', ?, ?, ?, ?)`, provenanceID, row.StorageID, row.StorageGeneration,
			row.BackupID, now.UnixNano()); err != nil {
			return Backup{}, Computer{}, internalError(err, "publish Backup Storage provenance")
		}
		backup, err = readBackup(ctx, tx, row.BackupID)
		if err != nil {
			return Backup{}, Computer{}, internalError(err, "read published Backup")
		}
	} else {
		status = "failed"
		failureCode = request.Receipt.FailureCode
	}
	if _, err := tx.ExecContext(ctx, `UPDATE computer_backup_operations SET status=?, failure_code=?,
		receipt_json=?, receipt_hash=?, acknowledgement_key=?, acknowledgement_hash=?, completed_ns=?
		WHERE computer_id=? AND operation_revision=? AND status='planned'`, status, failureCode,
		receiptJSON, bodyHash, request.IdempotencyKey, bodyHash, now.UnixNano(), computerID,
		row.OperationRevision); err != nil {
		return Backup{}, Computer{}, internalError(err, "complete Computer Backup operation")
	}
	computer, err := readComputerAuthority(ctx, tx, computerID, now)
	if err != nil {
		return Backup{}, Computer{}, internalError(err, "read Computer Backup completion authority")
	}
	if computer.ReconfigurationPhase != ComputerReconfigurationBackingUp || computer.ReconfigurationRevision == nil ||
		*computer.ReconfigurationRevision != row.OperationRevision {
		return Backup{}, Computer{}, protocolError(contract.ErrorConflict, "Computer Backup operation was superseded")
	}
	if computer.IntentRevision == row.OperationRevision && computer.DesiredState == contract.ServiceDesiredRunning && row.ResumeDesiredRunning {
		if err := setComputerServiceDesiredState(ctx, tx, computer.CurrentJob, contract.ServiceDesiredRunning, now); err != nil {
			return Backup{}, Computer{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE computers SET reconfiguration_phase='stable',
		reconfiguration_revision=NULL, applied_revision=CASE WHEN applied_revision<? THEN ? ELSE applied_revision END,
		updated_ns=? WHERE computer_id=? AND reconfiguration_phase='backing_up' AND reconfiguration_revision=?`,
		row.OperationRevision, row.OperationRevision, now.UnixNano(), computerID, row.OperationRevision); err != nil {
		return Backup{}, Computer{}, internalError(err, "release Computer Backup intent")
	}
	computer, err = readComputerAuthority(ctx, tx, computerID, now)
	if err != nil {
		return Backup{}, Computer{}, internalError(err, "read completed Computer Backup")
	}
	if err := tx.Commit(); err != nil {
		return Backup{}, Computer{}, internalError(err, "commit Computer Backup acknowledgement")
	}
	return backup, computer, nil
}

func readBackup(ctx context.Context, q queryer, backupID string) (Backup, error) {
	var backup Backup
	var createdNS, provenanceCreatedNS int64
	err := q.QueryRowContext(ctx, `SELECT b.backup_id, b.computer_id, b.source_storage_id,
		b.source_generation, b.created_ns, b.allocated_size, b.content_digest, b.encryption,
		b.provenance_id, b.status, p.kind, p.source_storage_id, p.source_generation,
		p.backup_id, p.created_ns
		FROM backups b JOIN storage_provenance p ON p.provenance_id=b.provenance_id
		WHERE b.backup_id=?`, backupID).Scan(&backup.BackupID, &backup.ComputerID,
		&backup.SourceStorageID, &backup.SourceGeneration, &createdNS, &backup.AllocatedSize,
		&backup.ContentDigest, &backup.Encryption, &backup.Provenance.ProvenanceID, &backup.Status,
		&backup.Provenance.Kind, &backup.Provenance.SourceStorageID,
		&backup.Provenance.SourceGeneration, &backup.Provenance.BackupID, &provenanceCreatedNS)
	if err != nil {
		return Backup{}, err
	}
	backup.CreatedAt = time.Unix(0, createdNS).UTC()
	backup.Provenance.CreatedAt = time.Unix(0, provenanceCreatedNS).UTC()
	if backup.Provenance.Kind != "backup" || backup.Provenance.SourceStorageID != backup.SourceStorageID ||
		backup.Provenance.SourceGeneration != backup.SourceGeneration || backup.Provenance.BackupID != backup.BackupID ||
		!backup.Provenance.CreatedAt.Equal(backup.CreatedAt) {
		return Backup{}, errors.New("Backup Storage provenance conflicts with immutable Backup record")
	}
	type queryRows interface {
		QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	}
	rowsQ, ok := q.(queryRows)
	if !ok {
		return Backup{}, errors.New("Backup query source cannot list physical copies")
	}
	rows, err := rowsQ.QueryContext(ctx, `SELECT copy_id, node_id, root_instance_id, allocated_size,
		content_digest, phase, created_ns, removed_ns FROM backup_copies WHERE backup_id=? ORDER BY copy_id`, backupID)
	if err != nil {
		return Backup{}, err
	}
	defer rows.Close()
	backup.Copies = []BackupCopy{}
	for rows.Next() {
		copy := BackupCopy{BackupID: backupID}
		var copyCreatedNS int64
		var removedNS sql.NullInt64
		if err := rows.Scan(&copy.CopyID, &copy.NodeID, &copy.RootInstanceID, &copy.AllocatedSize,
			&copy.ContentDigest, &copy.Phase, &copyCreatedNS, &removedNS); err != nil {
			return Backup{}, err
		}
		copy.CreatedAt = time.Unix(0, copyCreatedNS).UTC()
		if removedNS.Valid {
			value := time.Unix(0, removedNS.Int64).UTC()
			copy.RemovedAt = &value
		}
		backup.Copies = append(backup.Copies, copy)
	}
	return backup, rows.Err()
}

func (s *Store) ListComputerBackups(ctx context.Context, computerID string) (BackupList, error) {
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT 1 FROM computers WHERE computer_id=?`, computerID).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		return BackupList{}, protocolError(contract.ErrorNotFound, "Computer %q was not found", computerID)
	} else if err != nil {
		return BackupList{}, internalError(err, "read Computer Backup owner")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT backup_id FROM backups WHERE computer_id=? ORDER BY created_ns, backup_id`, computerID)
	if err != nil {
		return BackupList{}, internalError(err, "list Computer Backups")
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return BackupList{}, internalError(err, "scan Computer Backup")
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return BackupList{}, internalError(err, "iterate Computer Backups")
	}
	list := BackupList{Backups: []Backup{}}
	for _, id := range ids {
		backup, err := readBackup(ctx, s.db, id)
		if err != nil {
			return BackupList{}, internalError(err, "read Computer Backup")
		}
		list.Backups = append(list.Backups, backup)
	}
	return list, nil
}

func (s *Store) BeginComputerBackupPrune(ctx context.Context, computerID string, request ComputerBackupPruneRequest) (Backup, bool, error) {
	request.BackupID = strings.TrimSpace(request.BackupID)
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	if computerID == "" || request.BackupID == "" || request.IdempotencyKey == "" {
		return Backup{}, false, protocolError(contract.ErrorInvalidRequest, "computer_id, backup_id, and idempotency_key are required")
	}
	requestHash, err := backupMutationHash(struct {
		Request ComputerBackupPruneRequest `json:"request"`
		Backup  string                     `json:"backup_id"`
		Actor   string                     `json:"actor"`
	}{Request: request, Backup: request.BackupID, Actor: request.Actor})
	if err != nil {
		return Backup{}, false, internalError(err, "encode Computer Backup prune")
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Backup{}, false, internalError(err, "begin Computer Backup prune")
	}
	defer tx.Rollback()
	computer, err := readComputerAuthority(ctx, tx, computerID, now)
	if err != nil {
		return Backup{}, false, err
	}
	var storedHash string
	if err := tx.QueryRowContext(ctx, `SELECT request_hash FROM computer_backup_prunes WHERE computer_id=? AND idempotency_key=?`, computerID, request.IdempotencyKey).Scan(&storedHash); err == nil {
		if storedHash != requestHash {
			return Backup{}, false, protocolError(contract.ErrorIdempotencyConflict, "Computer Backup prune key was reused with different authority")
		}
		backup, readErr := readBackup(ctx, tx, request.BackupID)
		return backup, true, readErr
	} else if !errors.Is(err, sql.ErrNoRows) {
		return Backup{}, false, internalError(err, "read Computer Backup prune replay")
	}
	if err := validateComputerPrecondition(computer, request.ComputerMutationPrecondition); err != nil {
		return Backup{}, false, err
	}
	if computer.DesiredState == contract.ServiceDesiredRemoved || computer.ReconfigurationPhase == ComputerReconfigurationRemoving {
		return Backup{}, false, protocolError(contract.ErrorConflict, "Computer %q is being removed", computerID)
	}
	backup, err := readBackup(ctx, tx, request.BackupID)
	if errors.Is(err, sql.ErrNoRows) || backup.ComputerID != computerID {
		return Backup{}, false, protocolError(contract.ErrorNotFound, "Backup %q was not found", request.BackupID)
	}
	if err != nil {
		return Backup{}, false, internalError(err, "read Computer Backup prune target")
	}
	if backup.Status == "pruned" {
		return backup, true, nil
	}
	if backup.Status != "available" || len(backup.Copies) != 1 || backup.Copies[0].Phase != "published" {
		return Backup{}, false, protocolError(contract.ErrorConflict, "Backup %q is not available for pruning", request.BackupID)
	}
	cleanupFence := newID("backup-prune")
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_backup_prunes(computer_id, intent_revision,
		backup_id, copy_id, cleanup_fence, idempotency_key, request_hash, actor, status, requested_ns)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'planned', ?)`, computerID, computer.IntentRevision,
		backup.BackupID, backup.Copies[0].CopyID, cleanupFence, request.IdempotencyKey, requestHash,
		request.Actor, now.UnixNano()); err != nil {
		return Backup{}, false, internalError(err, "plan Computer Backup prune")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE backups SET status='pruning' WHERE backup_id=? AND status='available'`, backup.BackupID); err != nil {
		return Backup{}, false, internalError(err, "mark Backup pruning")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE backup_copies SET phase='removal_pending', cleanup_fence=? WHERE copy_id=? AND phase='published'`, cleanupFence, backup.Copies[0].CopyID); err != nil {
		return Backup{}, false, internalError(err, "mark Backup copy removal pending")
	}
	backup, err = readBackup(ctx, tx, backup.BackupID)
	if err != nil {
		return Backup{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return Backup{}, false, internalError(err, "commit Computer Backup prune")
	}
	return backup, false, nil
}

func (s *Store) ListNodeComputerBackupPruneDirectives(ctx context.Context, identityNodeID, nodeID, bootSessionID string) ([]ComputerBackupPruneDirective, error) {
	if err := validateBackupNodeSession(ctx, s.db, identityNodeID, nodeID, bootSessionID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT p.backup_id, p.copy_id, p.computer_id,
		b.source_storage_id, b.source_generation, b.allocated_size, c.node_id, c.root_instance_id,
		p.intent_revision, p.cleanup_fence FROM computer_backup_prunes p
		JOIN backups b ON b.backup_id=p.backup_id JOIN backup_copies c ON c.copy_id=p.copy_id
		WHERE c.node_id=? AND p.status='planned' AND c.phase='removal_pending'
		ORDER BY p.requested_ns, p.copy_id`, nodeID)
	if err != nil {
		return nil, internalError(err, "list Computer Backup prune directives")
	}
	defer rows.Close()
	directives := []ComputerBackupPruneDirective{}
	for rows.Next() {
		var directive ComputerBackupPruneDirective
		if err := rows.Scan(&directive.BackupID, &directive.CopyID, &directive.ComputerID,
			&directive.StorageID, &directive.StorageGeneration, &directive.AllocatedSize, &directive.BoundNodeID,
			&directive.RootInstanceID, &directive.OperationRevision, &directive.CleanupFence); err != nil {
			return nil, internalError(err, "scan Computer Backup prune directive")
		}
		directives = append(directives, directive)
	}
	return directives, rows.Err()
}

func (s *Store) AcknowledgeComputerBackupPrune(ctx context.Context, identityNodeID, computerID string, request ComputerBackupPruneAcknowledgementRequest) (Backup, error) {
	if request.NodeID == "" || request.BootSessionID == "" || request.IdempotencyKey == "" || !request.Receipt.Absent || request.Receipt.Kind != computerBackupRemovalReceiptKind {
		return Backup{}, protocolError(contract.ErrorInvalidRequest, "positive Computer Backup copy absence receipt is required")
	}
	bodyHash, err := backupMutationHash(request)
	if err != nil {
		return Backup{}, internalError(err, "encode Computer Backup copy removal acknowledgement")
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Backup{}, internalError(err, "begin Computer Backup prune acknowledgement")
	}
	defer tx.Rollback()
	if err := validateBackupNodeSession(ctx, tx, identityNodeID, request.NodeID, request.BootSessionID); err != nil {
		return Backup{}, err
	}
	var stored ComputerBackupPruneDirective
	var storedStatus string
	var acknowledgementKey, acknowledgementHash sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT p.backup_id, p.copy_id, p.computer_id,
		b.source_storage_id, b.source_generation, b.allocated_size, c.node_id, c.root_instance_id,
		p.intent_revision, p.cleanup_fence, p.status, p.acknowledgement_key, p.acknowledgement_hash
		FROM computer_backup_prunes p
		JOIN backups b ON b.backup_id=p.backup_id JOIN backup_copies c ON c.copy_id=p.copy_id
		WHERE p.computer_id=? AND p.copy_id=?`, computerID, request.Receipt.CopyID).Scan(
		&stored.BackupID, &stored.CopyID, &stored.ComputerID, &stored.StorageID,
		&stored.StorageGeneration, &stored.AllocatedSize, &stored.BoundNodeID, &stored.RootInstanceID,
		&stored.OperationRevision, &stored.CleanupFence, &storedStatus, &acknowledgementKey, &acknowledgementHash)
	if errors.Is(err, sql.ErrNoRows) {
		// Removing a Computer can supersede an in-flight create after the helper
		// durably reserved or even published bytes but before L1 publication.
		// The planned operation is the tracking record for that physical copy;
		// accept only its exact positive-absence receipt.
		var operation computerBackupOperationRow
		err = tx.QueryRowContext(ctx, `SELECT computer_id, operation_revision, backup_id, copy_id,
			source_storage_id, source_generation, allocated_size, bound_node_id, root_instance_id,
			job_id, cleanup_fence, idempotency_key, request_hash, status, failure_code,
			resume_desired_running, acknowledgement_key, acknowledgement_hash
			FROM computer_backup_operations WHERE computer_id=? AND copy_id=?`, computerID, request.Receipt.CopyID).Scan(
			&operation.ComputerID, &operation.OperationRevision, &operation.BackupID, &operation.CopyID,
			&operation.StorageID, &operation.StorageGeneration, &operation.AllocatedSize,
			&operation.BoundNodeID, &operation.RootInstanceID, &operation.JobID,
			&operation.CleanupFence, &operation.IdempotencyKey, &operation.RequestHash,
			&operation.Status, &operation.FailureCode, &operation.ResumeDesiredRunning,
			&operation.AcknowledgementKey, &operation.AcknowledgementHash)
		if errors.Is(err, sql.ErrNoRows) {
			return Backup{}, protocolError(contract.ErrorNotFound, "Computer Backup copy removal was not planned")
		}
		if err != nil {
			return Backup{}, internalError(err, "read superseded Computer Backup copy removal")
		}
		r := request.Receipt
		if operation.Status != "superseded" || r.BackupID != operation.BackupID || r.CopyID != operation.CopyID ||
			r.ComputerID != operation.ComputerID || r.StorageID != operation.StorageID ||
			r.StorageGeneration != operation.StorageGeneration || r.NodeID != operation.BoundNodeID ||
			r.RootInstanceID != operation.RootInstanceID || r.OperationRevision != operation.OperationRevision ||
			r.CleanupFence != operation.CleanupFence || r.HelperGeneration == 0 || r.ReceiptID == "" {
			return Backup{}, protocolError(contract.ErrorConflict, "Computer Backup removal receipt does not match superseded planned copy")
		}
		if operation.AcknowledgementKey.Valid {
			if operation.AcknowledgementKey.String != request.IdempotencyKey ||
				!operation.AcknowledgementHash.Valid || operation.AcknowledgementHash.String != bodyHash {
				return Backup{}, protocolError(contract.ErrorConflict, "Computer Backup removal replay differs from the accepted receipt")
			}
			return Backup{}, nil
		}
		receiptJSON, marshalErr := json.Marshal(request.Receipt)
		if marshalErr != nil {
			return Backup{}, internalError(marshalErr, "encode superseded Backup copy removal receipt")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE computer_backup_operations SET receipt_json=?, receipt_hash=?,
			acknowledgement_key=?, acknowledgement_hash=?, completed_ns=?
			WHERE computer_id=? AND operation_revision=? AND status='superseded'`, receiptJSON,
			bodyHash, request.IdempotencyKey, bodyHash, now.UnixNano(), computerID,
			operation.OperationRevision); err != nil {
			return Backup{}, internalError(err, "record superseded Backup copy absence")
		}
		if err := tx.Commit(); err != nil {
			return Backup{}, internalError(err, "commit superseded Backup copy absence")
		}
		return Backup{}, nil
	}
	if err != nil {
		return Backup{}, internalError(err, "read Computer Backup prune acknowledgement")
	}
	r := request.Receipt
	if r.BackupID != stored.BackupID || r.ComputerID != stored.ComputerID || r.StorageID != stored.StorageID ||
		r.StorageGeneration != stored.StorageGeneration || r.NodeID != stored.BoundNodeID ||
		r.RootInstanceID != stored.RootInstanceID || r.OperationRevision != stored.OperationRevision ||
		r.CleanupFence != stored.CleanupFence || r.HelperGeneration == 0 || r.ReceiptID == "" {
		return Backup{}, protocolError(contract.ErrorConflict, "Computer Backup prune receipt does not match planned copy")
	}
	if storedStatus == "removed" {
		if !acknowledgementKey.Valid || acknowledgementKey.String != request.IdempotencyKey ||
			!acknowledgementHash.Valid || acknowledgementHash.String != bodyHash {
			return Backup{}, protocolError(contract.ErrorConflict, "Computer Backup prune replay differs from the accepted receipt")
		}
		backup, err := readBackup(ctx, tx, stored.BackupID)
		if err != nil {
			return Backup{}, internalError(err, "read replayed Computer Backup prune")
		}
		return backup, nil
	}
	if storedStatus != "planned" {
		return Backup{}, protocolError(contract.ErrorConflict, "Computer Backup prune is no longer planned")
	}
	receiptJSON, err := json.Marshal(request.Receipt)
	if err != nil {
		return Backup{}, internalError(err, "encode Computer Backup prune receipt")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE backup_copies SET phase='removed', removed_ns=? WHERE copy_id=? AND phase='removal_pending'`, now.UnixNano(), stored.CopyID); err != nil {
		return Backup{}, internalError(err, "record Backup copy absence")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE backups SET status='pruned', pruned_ns=? WHERE backup_id=?`, now.UnixNano(), stored.BackupID); err != nil {
		return Backup{}, internalError(err, "record Backup prune")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE computer_backup_prunes SET status='removed', receipt_json=?,
		acknowledgement_key=?, acknowledgement_hash=?, completed_ns=? WHERE computer_id=? AND backup_id=?`,
		receiptJSON, request.IdempotencyKey, bodyHash, now.UnixNano(), computerID, stored.BackupID); err != nil {
		return Backup{}, internalError(err, "complete Computer Backup prune")
	}
	backup, err := readBackup(ctx, tx, stored.BackupID)
	if err != nil {
		return Backup{}, err
	}
	if err := tx.Commit(); err != nil {
		return Backup{}, internalError(err, "commit Computer Backup prune acknowledgement")
	}
	return backup, nil
}

func (directive ComputerBackupDirective) String() string {
	return fmt.Sprintf("%s/%s", directive.ComputerID, directive.CopyID)
}
