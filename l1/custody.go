package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

type ComputerCustodyExportRequest struct {
	ComputerMutationPrecondition
	BackupID       string `json:"-"`
	ExternalPath   string `json:"external_path"`
	IdempotencyKey string `json:"idempotency_key"`
}

type ComputerCustodyExport struct {
	ExportID                string     `json:"export_id"`
	ComputerID              string     `json:"computer_id"`
	OperationRevision       int64      `json:"operation_revision"`
	BackupID                string     `json:"backup_id"`
	CopyID                  string     `json:"copy_id"`
	SourceStorageID         string     `json:"source_storage_id"`
	SourceGeneration        int64      `json:"source_generation"`
	AllocatedSize           int64      `json:"allocated_size"`
	ContentDigest           string     `json:"content_digest"`
	BoundNodeID             string     `json:"bound_node_id"`
	RootInstanceID          string     `json:"root_instance_id"`
	ExternalPath            string     `json:"external_path"`
	Status                  string     `json:"status"`
	FailureCode             string     `json:"failure_code,omitempty"`
	ManifestDigest          string     `json:"manifest_digest,omitempty"`
	OperatorAttestedDeleted bool       `json:"operator_attested_deleted"`
	RequestedAt             time.Time  `json:"requested_at"`
	CompletedAt             *time.Time `json:"completed_at,omitempty"`
	OperatorAttestedAt      *time.Time `json:"operator_attested_at,omitempty"`
}

type ComputerCustodyExportDirective struct {
	ExportID          string           `json:"export_id"`
	ComputerID        string           `json:"computer_id"`
	OperationRevision int64            `json:"operation_revision"`
	BackupID          string           `json:"backup_id"`
	CopyID            string           `json:"copy_id"`
	StorageID         string           `json:"storage_id"`
	StorageGeneration int64            `json:"storage_generation"`
	AllocatedSize     int64            `json:"allocated_size"`
	ContentDigest     string           `json:"content_digest"`
	BoundNodeID       string           `json:"bound_node_id"`
	RootInstanceID    string           `json:"root_instance_id"`
	ExternalPath      string           `json:"external_path"`
	CustodyFence      string           `json:"custody_fence"`
	SourceSpec        contract.JobSpec `json:"source_spec"`
	SourceSpecHash    string           `json:"source_spec_hash"`
}

type ComputerCustodyExportReceipt = contract.ComputerCustodyExportReceipt

type ComputerCustodyExportAcknowledgementRequest struct {
	NodeID         string                       `json:"node_id"`
	BootSessionID  string                       `json:"boot_session_id"`
	IdempotencyKey string                       `json:"idempotency_key"`
	Receipt        ComputerCustodyExportReceipt `json:"receipt"`
}

type ComputerCustodyAttestationRequest struct {
	IdempotencyKey string `json:"idempotency_key"`
	Actor          string `json:"-"`
}

type ComputerCustodyImportRequest struct {
	Name           string                           `json:"name"`
	DiskBytes      int64                            `json:"disk_bytes"`
	NodeID         string                           `json:"node_id,omitempty"`
	ExternalPath   string                           `json:"external_path"`
	Manifest       contract.ComputerCustodyManifest `json:"manifest"`
	ManifestDigest string                           `json:"manifest_digest"`
	IdempotencyKey string                           `json:"idempotency_key"`
	Actor          string                           `json:"-"`
}

type ComputerCustodyImport struct {
	ImportID              string     `json:"import_id"`
	ExportID              string     `json:"export_id"`
	DestinationComputerID string     `json:"destination_computer_id"`
	DestinationStorageID  string     `json:"destination_storage_id"`
	DestinationName       string     `json:"destination_name"`
	DestinationSize       int64      `json:"destination_size"`
	Status                string     `json:"status"`
	RequestedAt           time.Time  `json:"requested_at"`
	CompletedAt           *time.Time `json:"completed_at,omitempty"`
}

func scanCustodyExport(scanner interface{ Scan(...any) error }) (ComputerCustodyExport, error) {
	var value ComputerCustodyExport
	var requested int64
	var completed, attested sql.NullInt64
	var attestationKey sql.NullString
	err := scanner.Scan(&value.ExportID, &value.ComputerID, &value.OperationRevision, &value.BackupID,
		&value.CopyID, &value.SourceStorageID, &value.SourceGeneration, &value.AllocatedSize,
		&value.ContentDigest, &value.BoundNodeID, &value.RootInstanceID, &value.ExternalPath,
		&value.Status, &value.FailureCode, &value.ManifestDigest, &attestationKey, &requested, &completed, &attested)
	value.OperatorAttestedDeleted = attestationKey.Valid
	value.RequestedAt = time.Unix(0, requested).UTC()
	if completed.Valid {
		stamp := time.Unix(0, completed.Int64).UTC()
		value.CompletedAt = &stamp
	}
	if attested.Valid {
		stamp := time.Unix(0, attested.Int64).UTC()
		value.OperatorAttestedAt = &stamp
	}
	return value, err
}

const custodyExportColumns = `export_id, computer_id, operation_revision, backup_id, copy_id,
	source_storage_id, source_generation, allocated_size, content_digest, bound_node_id,
	root_instance_id, external_path, status, failure_code, manifest_digest, operator_attestation_key,
	requested_ns, completed_ns, operator_attested_ns`

func (s *Store) BeginComputerCustodyExport(ctx context.Context, computerID string, request ComputerCustodyExportRequest) (ComputerCustodyExport, bool, error) {
	request.BackupID = strings.TrimSpace(request.BackupID)
	request.ExternalPath = filepath.Clean(strings.TrimSpace(request.ExternalPath))
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	request.Actor = strings.TrimSpace(request.Actor)
	if computerID == "" || request.BackupID == "" || request.IdempotencyKey == "" || request.Actor == "" ||
		!filepath.IsAbs(request.ExternalPath) {
		return ComputerCustodyExport{}, false, protocolError(contract.ErrorInvalidRequest,
			"computer_id, backup_id, absolute external_path, actor, and idempotency_key are required")
	}
	requestHash, err := storageCopyHash(struct {
		ComputerCustodyExportRequest
		BackupID string `json:"backup_id"`
	}{request, request.BackupID})
	if err != nil {
		return ComputerCustodyExport{}, false, internalError(err, "encode Custody export request")
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ComputerCustodyExport{}, false, internalError(err, "begin Custody export")
	}
	defer tx.Rollback()
	computer, err := readComputerAuthority(ctx, tx, computerID, now)
	if errors.Is(err, sql.ErrNoRows) {
		return ComputerCustodyExport{}, false, protocolError(contract.ErrorNotFound, "Computer %q was not found", computerID)
	}
	if err != nil {
		return ComputerCustodyExport{}, false, internalError(err, "read Custody export Computer")
	}
	if replay, replayErr := scanCustodyExport(tx.QueryRowContext(ctx, `SELECT `+custodyExportColumns+`
		FROM computer_custody_exports WHERE computer_id=? AND idempotency_key=?`, computerID, request.IdempotencyKey)); replayErr == nil {
		var storedHash string
		if err := tx.QueryRowContext(ctx, `SELECT request_hash FROM computer_custody_exports WHERE export_id=?`, replay.ExportID).Scan(&storedHash); err != nil {
			return ComputerCustodyExport{}, false, internalError(err, "read Custody export replay hash")
		}
		if storedHash != requestHash {
			return ComputerCustodyExport{}, false, protocolError(contract.ErrorIdempotencyConflict, "Custody export idempotency key was reused")
		}
		return replay, true, nil
	} else if !errors.Is(replayErr, sql.ErrNoRows) {
		return ComputerCustodyExport{}, false, internalError(replayErr, "read Custody export replay")
	}
	if err := validateComputerPrecondition(computer, request.ComputerMutationPrecondition); err != nil {
		return ComputerCustodyExport{}, false, err
	}
	if computer.DesiredState == contract.ServiceDesiredRemoved || computer.ReconfigurationPhase != ComputerReconfigurationStable {
		return ComputerCustodyExport{}, false, protocolError(contract.ErrorConflict, "Computer %q cannot begin a Custody export", computerID)
	}
	backup, copy, err := readAvailableBackupCopy(ctx, tx, request.BackupID)
	if err != nil {
		return ComputerCustodyExport{}, false, err
	}
	if backup.ComputerID != computerID || backup.SourceStorageID != computer.StorageID {
		return ComputerCustodyExport{}, false, protocolError(contract.ErrorStorageReferenceConflict, "Backup %q does not belong to Computer %q Storage", backup.BackupID, computerID)
	}
	if copy.NodeID != computer.BoundNodeID || copy.RootInstanceID == "" {
		return ComputerCustodyExport{}, false, protocolError(contract.ErrorConflict, "Custody export Backup is not on the Computer's bound Node")
	}
	var currentRoot string
	if err := tx.QueryRowContext(ctx, `SELECT root_instance_id FROM nodes WHERE node_id=?`, copy.NodeID).Scan(&currentRoot); err != nil {
		return ComputerCustodyExport{}, false, internalError(err, "read Custody export managed-root identity")
	}
	if currentRoot == "" || currentRoot != copy.RootInstanceID {
		return ComputerCustodyExport{}, false, protocolError(contract.ErrorConflict, "Custody export Backup belongs to a stale managed-root instance")
	}
	exportID := newID("custody-export")
	revision := computer.IntentRevision + 1
	spec := computer.CurrentJob.Spec
	spec.Execution.SensitiveEnv = nil
	specJSON, specHash, err := encodeJobSpec(spec)
	if err != nil {
		return ComputerCustodyExport{}, false, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE computers SET intent_revision=?, reconfiguration_phase=?,
		reconfiguration_revision=?, updated_ns=? WHERE computer_id=? AND intent_revision=?`, revision,
		ComputerReconfigurationExporting, revision, now.UnixNano(), computerID, computer.IntentRevision)
	if err != nil {
		return ComputerCustodyExport{}, false, internalError(err, "reserve Custody export")
	}
	if err := requireComputerCAS(result, computerID, computer.IntentRevision); err != nil {
		return ComputerCustodyExport{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_custody_exports(export_id, computer_id,
		operation_revision, backup_id, copy_id, source_storage_id, source_generation, allocated_size,
		content_digest, bound_node_id, root_instance_id, external_path, custody_fence, source_spec_json,
		source_spec_hash, idempotency_key, request_hash, status, requested_ns)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'planned', ?)`, exportID,
		computerID, revision, backup.BackupID, copy.CopyID, backup.SourceStorageID, backup.SourceGeneration,
		backup.AllocatedSize, backup.ContentDigest, copy.NodeID, copy.RootInstanceID, request.ExternalPath,
		newID("custody-fence"), specJSON, specHash, request.IdempotencyKey, requestHash, now.UnixNano()); err != nil {
		return ComputerCustodyExport{}, false, internalError(err, "record Custody export before external bytes")
	}
	if err := insertComputerIntent(ctx, tx, computerID, revision, ComputerIntentCustodyExport,
		computer.DesiredState, computer.StorageID, computer.StorageGeneration, computer.CurrentJobID,
		computer.CurrentSpecRevision, request.Actor, now); err != nil {
		return ComputerCustodyExport{}, false, err
	}
	export, err := scanCustodyExport(tx.QueryRowContext(ctx, `SELECT `+custodyExportColumns+`
		FROM computer_custody_exports WHERE export_id=?`, exportID))
	if err != nil {
		return ComputerCustodyExport{}, false, internalError(err, "read recorded Custody export")
	}
	if err := tx.Commit(); err != nil {
		return ComputerCustodyExport{}, false, internalError(err, "commit Custody export before external bytes")
	}
	return export, false, nil
}

func (s *Store) ListNodeComputerCustodyExportDirectives(ctx context.Context, identityNodeID, nodeID, bootSessionID string) ([]ComputerCustodyExportDirective, error) {
	if err := validateStorageResetNode(ctx, s.db, identityNodeID, nodeID, bootSessionID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT e.export_id, e.computer_id, e.operation_revision, e.backup_id,
		e.copy_id, e.source_storage_id, e.source_generation, e.allocated_size, e.content_digest, e.bound_node_id,
		e.root_instance_id, e.external_path, e.custody_fence, e.source_spec_json, e.source_spec_hash
		FROM computer_custody_exports e JOIN computers c ON c.computer_id=e.computer_id
		WHERE e.bound_node_id=? AND e.status='planned'
		AND c.reconfiguration_phase='exporting' AND c.reconfiguration_revision=e.operation_revision
		ORDER BY e.requested_ns, e.export_id`, nodeID)
	if err != nil {
		return nil, internalError(err, "list Custody export directives")
	}
	defer rows.Close()
	directives := []ComputerCustodyExportDirective{}
	for rows.Next() {
		var directive ComputerCustodyExportDirective
		var specJSON []byte
		if err := rows.Scan(&directive.ExportID, &directive.ComputerID, &directive.OperationRevision,
			&directive.BackupID, &directive.CopyID, &directive.StorageID, &directive.StorageGeneration,
			&directive.AllocatedSize, &directive.ContentDigest, &directive.BoundNodeID,
			&directive.RootInstanceID, &directive.ExternalPath, &directive.CustodyFence, &specJSON,
			&directive.SourceSpecHash); err != nil {
			return nil, internalError(err, "scan Custody export directive")
		}
		if err := json.Unmarshal(specJSON, &directive.SourceSpec); err != nil {
			return nil, internalError(err, "decode Custody export directive specification")
		}
		directives = append(directives, directive)
	}
	return directives, rows.Err()
}

func (s *Store) ListComputerCustodyExports(ctx context.Context, computerID string) ([]ComputerCustodyExport, error) {
	if strings.TrimSpace(computerID) == "" {
		return nil, protocolError(contract.ErrorInvalidRequest, "computer_id is required")
	}
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT 1 FROM computers WHERE computer_id=?`, computerID).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		return nil, protocolError(contract.ErrorNotFound, "Computer %q was not found", computerID)
	} else if err != nil {
		return nil, internalError(err, "read Custody export Computer")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+custodyExportColumns+` FROM computer_custody_exports
		WHERE computer_id=? ORDER BY requested_ns, export_id`, computerID)
	if err != nil {
		return nil, internalError(err, "list Custody exports")
	}
	defer rows.Close()
	exports := []ComputerCustodyExport{}
	for rows.Next() {
		export, err := scanCustodyExport(rows)
		if err != nil {
			return nil, internalError(err, "scan Custody export")
		}
		exports = append(exports, export)
	}
	return exports, rows.Err()
}

func validateCustodyExportReceipt(export ComputerCustodyExportDirective, receipt ComputerCustodyExportReceipt) error {
	if receipt.ReceiptID == "" || receipt.HelperGeneration == 0 || receipt.ExportID != export.ExportID || receipt.BackupID != export.BackupID ||
		receipt.CopyID != export.CopyID || receipt.ComputerID != export.ComputerID || receipt.StorageID != export.StorageID ||
		receipt.StorageGeneration != export.StorageGeneration || receipt.NodeID != export.BoundNodeID ||
		receipt.RootInstanceID != export.RootInstanceID || receipt.OperationRevision != export.OperationRevision ||
		receipt.CustodyFence != export.CustodyFence || receipt.ExternalPath != export.ExternalPath ||
		receipt.AllocatedSize != export.AllocatedSize || receipt.ContentDigest != export.ContentDigest {
		return protocolError(contract.ErrorStorageReferenceConflict, "Custody export receipt does not bind the recorded external transfer")
	}
	switch receipt.Kind {
	case "computer_custody_export_verified":
		if receipt.FailureCode != "" || !backupDigestPattern.MatchString(receipt.ManifestDigest) {
			return protocolError(contract.ErrorInvalidRequest, "successful Custody export receipt is incomplete")
		}
	case "computer_custody_export_failed":
		if receipt.ManifestDigest != "" || (receipt.FailureCode != "insufficient_disk" &&
			receipt.FailureCode != "destination_not_empty" && receipt.FailureCode != "managed_root_path" &&
			receipt.FailureCode != "cancelled") {
			return protocolError(contract.ErrorInvalidRequest, "failed Custody export receipt is incomplete")
		}
	default:
		return protocolError(contract.ErrorInvalidRequest, "Custody export receipt kind is invalid")
	}
	return nil
}

func (s *Store) AcknowledgeComputerCustodyExport(ctx context.Context, identityNodeID, computerID string, request ComputerCustodyExportAcknowledgementRequest) (ComputerCustodyExport, error) {
	if computerID == "" || request.NodeID == "" || request.BootSessionID == "" || request.IdempotencyKey == "" {
		return ComputerCustodyExport{}, protocolError(contract.ErrorInvalidRequest, "complete Custody export acknowledgement is required")
	}
	bodyHash, err := storageCopyHash(request)
	if err != nil {
		return ComputerCustodyExport{}, internalError(err, "encode Custody export acknowledgement")
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ComputerCustodyExport{}, internalError(err, "begin Custody export acknowledgement")
	}
	defer tx.Rollback()
	if err := validateBackupNodeSession(ctx, tx, identityNodeID, request.NodeID, request.BootSessionID); err != nil {
		return ComputerCustodyExport{}, err
	}
	var directive ComputerCustodyExportDirective
	var status string
	var ackKey, ackHash sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT export_id, computer_id, operation_revision, backup_id, copy_id,
		source_storage_id, source_generation, allocated_size, content_digest, bound_node_id, root_instance_id,
		external_path, custody_fence, status, acknowledgement_key, acknowledgement_hash
		FROM computer_custody_exports WHERE export_id=? AND computer_id=?`, request.Receipt.ExportID, computerID).Scan(
		&directive.ExportID, &directive.ComputerID, &directive.OperationRevision, &directive.BackupID,
		&directive.CopyID, &directive.StorageID, &directive.StorageGeneration, &directive.AllocatedSize,
		&directive.ContentDigest, &directive.BoundNodeID, &directive.RootInstanceID, &directive.ExternalPath,
		&directive.CustodyFence, &status, &ackKey, &ackHash)
	if errors.Is(err, sql.ErrNoRows) {
		return ComputerCustodyExport{}, protocolError(contract.ErrorNotFound, "Custody export was not found")
	}
	if err != nil {
		return ComputerCustodyExport{}, internalError(err, "read Custody export acknowledgement target")
	}
	if err := validateBackupAcknowledgementAuthority(ctx, tx, identityNodeID, request.NodeID, directive.BoundNodeID, directive.RootInstanceID); err != nil {
		return ComputerCustodyExport{}, err
	}
	if err := validateCustodyExportReceipt(directive, request.Receipt); err != nil {
		return ComputerCustodyExport{}, err
	}
	if ackKey.Valid {
		if ackKey.String != request.IdempotencyKey || !ackHash.Valid || ackHash.String != bodyHash {
			return ComputerCustodyExport{}, protocolError(contract.ErrorIdempotencyConflict, "Custody export acknowledgement differs from durable evidence")
		}
		return scanCustodyExport(tx.QueryRowContext(ctx, `SELECT `+custodyExportColumns+` FROM computer_custody_exports WHERE export_id=?`, directive.ExportID))
	}
	receiptJSON, err := json.Marshal(request.Receipt)
	if err != nil {
		return ComputerCustodyExport{}, internalError(err, "encode Custody export receipt")
	}
	if status != "planned" {
		return ComputerCustodyExport{}, protocolError(contract.ErrorConflict, "Custody export is no longer awaiting completion")
	}
	completedStatus := "exported"
	if request.Receipt.Kind == "computer_custody_export_failed" {
		completedStatus = "failed"
	}
	if _, err := tx.ExecContext(ctx, `UPDATE computer_custody_exports SET status=?, failure_code=?, manifest_digest=?,
		receipt_json=?, acknowledgement_key=?, acknowledgement_hash=?, completed_ns=? WHERE export_id=?`,
		completedStatus, request.Receipt.FailureCode, request.Receipt.ManifestDigest, receiptJSON, request.IdempotencyKey, bodyHash, now.UnixNano(), directive.ExportID); err != nil {
		return ComputerCustodyExport{}, internalError(err, "record verified Custody export")
	}
	if status == "planned" {
		result, err := tx.ExecContext(ctx, `UPDATE computers SET applied_revision=?, reconfiguration_phase='stable',
			reconfiguration_revision=NULL, updated_ns=? WHERE computer_id=? AND intent_revision=?
			AND reconfiguration_phase='exporting' AND reconfiguration_revision=?`,
			directive.OperationRevision, now.UnixNano(), computerID, directive.OperationRevision, directive.OperationRevision)
		if err != nil {
			return ComputerCustodyExport{}, internalError(err, "complete Custody export intent")
		}
		if err := requireComputerCAS(result, computerID, directive.OperationRevision); err != nil {
			return ComputerCustodyExport{}, protocolError(contract.ErrorStaleIntentRevision, "Custody export was superseded before acknowledgement")
		}
	}
	export, err := scanCustodyExport(tx.QueryRowContext(ctx, `SELECT `+custodyExportColumns+` FROM computer_custody_exports WHERE export_id=?`, directive.ExportID))
	if err != nil {
		return ComputerCustodyExport{}, internalError(err, "read completed Custody export")
	}
	if err := tx.Commit(); err != nil {
		return ComputerCustodyExport{}, internalError(err, "commit Custody export acknowledgement")
	}
	return export, nil
}

func (s *Store) AttestComputerCustodyDeleted(ctx context.Context, exportID string, request ComputerCustodyAttestationRequest) (ComputerCustodyExport, error) {
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	request.Actor = strings.TrimSpace(request.Actor)
	if exportID == "" || request.IdempotencyKey == "" || request.Actor == "" {
		return ComputerCustodyExport{}, protocolError(contract.ErrorInvalidRequest, "export_id, idempotency_key, and actor are required")
	}
	now := canonicalTime(s.clock.Now())
	result, err := s.db.ExecContext(ctx, `UPDATE computer_custody_exports SET operator_attestation_key=?,
		operator_attestation_actor=?, operator_attested_ns=? WHERE export_id=? AND operator_attestation_key IS NULL`,
		request.IdempotencyKey, request.Actor, now.UnixNano(), exportID)
	if err != nil {
		return ComputerCustodyExport{}, internalError(err, "record operator_attested_deleted evidence")
	}
	if changed, _ := result.RowsAffected(); changed == 0 {
		var key, actor string
		if err := s.db.QueryRowContext(ctx, `SELECT operator_attestation_key, operator_attestation_actor
			FROM computer_custody_exports WHERE export_id=?`, exportID).Scan(&key, &actor); errors.Is(err, sql.ErrNoRows) {
			return ComputerCustodyExport{}, protocolError(contract.ErrorNotFound, "Custody export %q was not found", exportID)
		} else if err != nil {
			return ComputerCustodyExport{}, internalError(err, "read Custody deletion attestation")
		} else if key != request.IdempotencyKey || actor != request.Actor {
			return ComputerCustodyExport{}, protocolError(contract.ErrorIdempotencyConflict, "Custody deletion attestation is immutable")
		}
	}
	return scanCustodyExport(s.db.QueryRowContext(ctx, `SELECT `+custodyExportColumns+` FROM computer_custody_exports WHERE export_id=?`, exportID))
}

func (s *Store) BeginComputerCustodyImport(ctx context.Context, exportID string, request ComputerCustodyImportRequest) (ComputerCustodyImport, bool, error) {
	request.Name = strings.TrimSpace(request.Name)
	request.NodeID = strings.TrimSpace(request.NodeID)
	request.ExternalPath = filepath.Clean(strings.TrimSpace(request.ExternalPath))
	request.ManifestDigest = strings.TrimSpace(request.ManifestDigest)
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	request.Actor = strings.TrimSpace(request.Actor)
	if err := validateComputerNameAndActor(request.Name, request.Actor); err != nil {
		return ComputerCustodyImport{}, false, err
	}
	manifest := request.Manifest
	if exportID == "" || request.DiskBytes < 1 || request.IdempotencyKey == "" || !filepath.IsAbs(request.ExternalPath) ||
		request.ManifestDigest == "" || manifest.Version != 1 || manifest.ExportID != exportID || manifest.Phase != "complete" ||
		manifest.DiskFile != "storage.ext4" || manifest.Encryption != "none" || manifest.AllocatedSize < 1 ||
		!backupDigestPattern.MatchString(manifest.ContentDigest) || manifest.ComputerID == "" || manifest.StorageID == "" ||
		manifest.StorageGeneration < 1 || manifest.BackupID == "" || manifest.CopyID == "" {
		return ComputerCustodyImport{}, false, protocolError(contract.ErrorInvalidRequest,
			"complete self-contained Custody manifest, export_id, name, positive disk_bytes, absolute external_path, and idempotency_key are required")
	}
	observedManifestDigest, err := contract.DigestComputerCustodyManifest(manifest)
	if err != nil || observedManifestDigest != request.ManifestDigest {
		return ComputerCustodyImport{}, false, protocolError(contract.ErrorStorageReferenceConflict,
			"Custody import manifest digest does not match its submitted manifest")
	}
	if request.DiskBytes < manifest.AllocatedSize {
		return ComputerCustodyImport{}, false, protocolError(contract.ErrorConflict,
			"Custody import disk_bytes cannot be smaller than its manifest")
	}
	if !isComputerSpec(manifest.JobSpec) || len(manifest.JobSpec.Execution.SensitiveEnv) != 0 {
		return ComputerCustodyImport{}, false, protocolError(contract.ErrorStorageReferenceConflict,
			"Custody import manifest does not contain a safe Computer Job specification")
	}
	_, observedSpecHash, err := encodeJobSpec(manifest.JobSpec)
	if err != nil || observedSpecHash != manifest.JobSpecHash {
		return ComputerCustodyImport{}, false, protocolError(contract.ErrorStorageReferenceConflict,
			"Custody import Job specification digest does not match its manifest")
	}
	if request.NodeID == "" {
		return ComputerCustodyImport{}, false, protocolError(contract.ErrorInvalidRequest,
			"node_id is required when the export path's Node cannot be discovered")
	}
	requestHash, err := storageCopyHash(request)
	if err != nil {
		return ComputerCustodyImport{}, false, internalError(err, "encode Custody import request")
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ComputerCustodyImport{}, false, internalError(err, "begin Custody import")
	}
	defer tx.Rollback()
	if replay, replayErr := scanComputerStorageCopy(tx.QueryRowContext(ctx, `SELECT `+storageCopyColumns+`
		FROM computer_storage_copy_operations WHERE export_id=? AND idempotency_key=?`, exportID,
		request.IdempotencyKey)); replayErr == nil {
		if replay.Operation != "import" || replay.RequestHash != requestHash {
			return ComputerCustodyImport{}, false, protocolError(contract.ErrorIdempotencyConflict, "Custody import idempotency key was reused")
		}
		var name string
		_ = tx.QueryRowContext(ctx, `SELECT name FROM computers WHERE computer_id=?`, replay.DestinationComputerID).Scan(&name)
		return ComputerCustodyImport{ImportID: replay.DestinationComputerID, ExportID: replay.ExportID,
			DestinationComputerID: replay.DestinationComputerID, DestinationStorageID: replay.DestinationStorageID,
			DestinationName: name, DestinationSize: replay.DestinationSize, Status: replay.Status}, true, nil
	} else if !errors.Is(replayErr, sql.ErrNoRows) {
		return ComputerCustodyImport{}, false, internalError(replayErr, "read Custody import replay")
	}
	var rootID string
	if err := tx.QueryRowContext(ctx, `SELECT root_instance_id FROM nodes WHERE node_id=?`, request.NodeID).Scan(&rootID); errors.Is(err, sql.ErrNoRows) {
		return ComputerCustodyImport{}, false, protocolError(contract.ErrorNotFound, "destination Node %q was not found", request.NodeID)
	} else if err != nil {
		return ComputerCustodyImport{}, false, internalError(err, "read Custody import destination managed-root identity")
	}
	if rootID == "" {
		return ComputerCustodyImport{}, false, protocolError(contract.ErrorConflict, "destination Node has no current managed-root identity")
	}
	computerID, storageID, jobID := newID("computer"), newID("storage"), newID("job")
	spec := manifest.JobSpec
	spec.DispatchKey = "computer:import:" + computerID
	spec.Execution.SensitiveEnv = nil
	spec.Execution.OCI.Computer.DiskBytes = request.DiskBytes
	specJSON, specHash, err := encodeJobSpec(spec)
	if err != nil {
		return ComputerCustodyImport{}, false, err
	}
	if _, err := insertComputerJobWithID(ctx, tx, jobID, spec, specJSON, specHash, contract.JobStopped,
		contract.ServiceDesiredStopped, request.NodeID, now); err != nil {
		return ComputerCustodyImport{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computers(computer_id, name, placement_node_id,
		bound_node_id, grants_json, storage_id, storage_generation, desired_disk_bytes, backup_cap, desired_state,
		intent_revision, applied_revision, current_job_id, current_spec_revision, reconfiguration_phase,
		reconfiguration_revision, created_ns, updated_ns) VALUES(?, ?, ?, ?, '[]', ?, 1, ?, 0, 'stopped',
		1, 0, ?, 1, 'importing', 1, ?, ?)`, computerID, request.Name, request.NodeID, request.NodeID,
		storageID, request.DiskBytes, jobID, now.UnixNano(), now.UnixNano()); err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed: computers.name") {
			return ComputerCustodyImport{}, false, protocolError(contract.ErrorConflict, "Computer name %q is already used or reserved", request.Name)
		}
		return ComputerCustodyImport{}, false, internalError(err, "reserve imported Computer identity")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_job_projections(computer_id, job_id,
		spec_revision, current, created_ns) VALUES(?, ?, 1, 1, ?)`, computerID, jobID, now.UnixNano()); err != nil {
		return ComputerCustodyImport{}, false, internalError(err, "reserve imported Computer projection")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_storage_generations(computer_id, storage_id,
		storage_generation, disk_bytes, phase, reset_revision, created_ns) VALUES(?, ?, 1, ?, 'staging', 1, ?)`,
		computerID, storageID, request.DiskBytes, now.UnixNano()); err != nil {
		return ComputerCustodyImport{}, false, internalError(err, "reserve imported Storage generation")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_storage_copy_operations(
		destination_computer_id, operation, operation_revision, source_computer_id, backup_id, copy_id,
		source_storage_id, source_generation, source_size, source_digest, destination_storage_id,
		old_generation, destination_generation, destination_size, bound_node_id, root_instance_id,
		job_id, cleanup_fence, keep_old_as_backup, idempotency_key, request_hash, status, requested_ns,
		export_id, external_path, manifest_digest, source_spec_json, source_spec_hash, actor)
		VALUES(?, 'import', 1, ?, ?, ?, ?, ?, ?, ?, ?, 0, 1, ?, ?, ?, ?, ?, 0, ?, ?, 'reserved', ?, ?, ?, ?, ?, ?, ?)`,
		computerID, manifest.ComputerID, manifest.BackupID, manifest.CopyID, manifest.StorageID,
		manifest.StorageGeneration, manifest.AllocatedSize, manifest.ContentDigest, storageID,
		request.DiskBytes, request.NodeID, rootID, jobID, newID("import-fence"), request.IdempotencyKey,
		requestHash, now.UnixNano(), exportID, request.ExternalPath, request.ManifestDigest, specJSON,
		specHash, request.Actor); err != nil {
		return ComputerCustodyImport{}, false, internalError(err, "reserve Custody import in Storage copy ledger")
	}
	if err := insertComputerIntent(ctx, tx, computerID, 1, ComputerIntentCustodyImport,
		contract.ServiceDesiredStopped, storageID, 1, jobID, 1, request.Actor, now); err != nil {
		return ComputerCustodyImport{}, false, err
	}
	value := ComputerCustodyImport{ImportID: computerID, ExportID: exportID, DestinationComputerID: computerID,
		DestinationStorageID: storageID, DestinationName: request.Name, DestinationSize: request.DiskBytes,
		Status: "reserved", RequestedAt: now}
	if err := tx.Commit(); err != nil {
		return ComputerCustodyImport{}, false, internalError(err, "commit Custody import reservation")
	}
	return value, false, nil
}

func (s *Store) AcknowledgeComputerCustodyImport(ctx context.Context, identityNodeID, destinationComputerID string, request ComputerStorageCopyAcknowledgementRequest) (Computer, error) {
	if destinationComputerID == "" || request.NodeID == "" || request.BootSessionID == "" || request.IdempotencyKey == "" {
		return Computer{}, protocolError(contract.ErrorInvalidRequest, "complete Custody import acknowledgement is required")
	}
	bodyHash, err := storageCopyHash(request)
	if err != nil {
		return Computer{}, internalError(err, "encode Custody import acknowledgement")
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Computer{}, internalError(err, "begin Custody import acknowledgement")
	}
	defer tx.Rollback()
	if err := validateBackupNodeSession(ctx, tx, identityNodeID, request.NodeID, request.BootSessionID); err != nil {
		return Computer{}, err
	}
	row, err := scanComputerStorageCopy(tx.QueryRowContext(ctx, `SELECT `+storageCopyColumns+`
		FROM computer_storage_copy_operations WHERE destination_computer_id=?
		ORDER BY operation_revision DESC LIMIT 1`, destinationComputerID))
	if errors.Is(err, sql.ErrNoRows) {
		return Computer{}, protocolError(contract.ErrorNotFound, "Custody import operation was not found")
	}
	if err != nil {
		return Computer{}, internalError(err, "read Custody import acknowledgement target")
	}
	if row.Operation != "import" {
		return Computer{}, protocolError(contract.ErrorInvalidRequest, "stored Storage copy operation is not a Custody import")
	}
	if err := validateBackupAcknowledgementAuthority(ctx, tx, identityNodeID, request.NodeID, row.BoundNodeID, row.RootInstanceID); err != nil {
		return Computer{}, err
	}
	receipt := request.Receipt
	if err := validateStorageCopyReceipt(row, receipt); err != nil {
		return Computer{}, err
	}
	if row.AcknowledgementKey.Valid {
		if row.AcknowledgementKey.String != request.IdempotencyKey || !row.AcknowledgementHash.Valid || row.AcknowledgementHash.String != bodyHash {
			return Computer{}, protocolError(contract.ErrorIdempotencyConflict, "Custody import acknowledgement differs from durable evidence")
		}
		if row.Status == "failed" {
			return Computer{}, nil
		}
		return readComputerAuthority(ctx, tx, destinationComputerID, now)
	}
	if row.Status != "reserved" {
		return Computer{}, protocolError(contract.ErrorConflict, "Custody import is not awaiting verification")
	}
	receiptJSON, err := json.Marshal(receipt)
	if err != nil {
		return Computer{}, internalError(err, "encode Custody import receipt")
	}
	if receipt.Kind == "computer_storage_copy_failed_absent" {
		if _, err := tx.ExecContext(ctx, `UPDATE computer_storage_copy_operations SET status='failed',
			failure_code=?, verification_receipt_json=?, verification_receipt_hash=?, acknowledgement_key=?,
			acknowledgement_hash=?, completed_ns=? WHERE destination_computer_id=? AND operation_revision=? AND status='reserved'`,
			receipt.FailureCode, receiptJSON, bodyHash, request.IdempotencyKey, bodyHash, now.UnixNano(),
			destinationComputerID, row.OperationRevision); err != nil {
			return Computer{}, internalError(err, "record failed Custody import")
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM computer_job_projections WHERE computer_id=?;
			DELETE FROM computer_intent_history WHERE computer_id=?;
			DELETE FROM computer_storage_generations WHERE computer_id=?;
			DELETE FROM computers WHERE computer_id=?`, destinationComputerID, destinationComputerID,
			destinationComputerID, destinationComputerID); err != nil {
			return Computer{}, internalError(err, "release failed Custody import identities")
		}
		if err := deleteServiceRows(ctx, tx, row.JobID); err != nil {
			return Computer{}, internalError(err, "release failed Custody import Job")
		}
		if err := tx.Commit(); err != nil {
			return Computer{}, internalError(err, "commit failed Custody import")
		}
		return Computer{}, nil
	}
	computer, err := readComputerAuthority(ctx, tx, destinationComputerID, now)
	if err != nil {
		return Computer{}, internalError(err, "read Custody import publication authority")
	}
	if computer.IntentRevision != row.OperationRevision || computer.ReconfigurationPhase != ComputerReconfigurationImporting ||
		computer.ReconfigurationRevision == nil || *computer.ReconfigurationRevision != row.OperationRevision {
		return Computer{}, protocolError(contract.ErrorStaleIntentRevision, "Custody import no longer owns publication")
	}
	var spec contract.JobSpec
	if err := json.Unmarshal(row.SourceSpecJSON, &spec); err != nil {
		return Computer{}, internalError(err, "decode Custody import Computer specification")
	}
	if !isComputerSpec(spec) {
		return Computer{}, internalError(errors.New("Custody import specification lost its Computer trait"), "decode Custody import Computer specification")
	}
	if _, observedHash, err := encodeJobSpec(spec); err != nil || observedHash != row.SourceSpecHash {
		if err == nil {
			err = errors.New("Custody import specification digest mismatch")
		}
		return Computer{}, internalError(err, "verify Custody import Computer specification")
	}
	published, err := tx.ExecContext(ctx, `UPDATE computer_storage_generations SET phase='current'
		WHERE computer_id=? AND storage_generation=1 AND phase='staging' AND reset_revision=1`, destinationComputerID)
	if err != nil {
		return Computer{}, internalError(err, "publish imported Storage generation")
	}
	if err := requireSingleStorageGenerationMutation(published, "publish imported Storage generation"); err != nil {
		return Computer{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO storage_provenance(provenance_id, kind,
		source_storage_id, source_generation, backup_id, destination_computer_id,
		destination_storage_id, destination_generation, created_ns) VALUES(?, 'import', ?, ?, ?, ?, ?, 1, ?)`,
		newID("storage-provenance"), row.SourceStorageID, row.SourceGeneration, row.BackupID,
		destinationComputerID, row.DestinationStorageID, now.UnixNano()); err != nil {
		return Computer{}, internalError(err, "publish imported Storage provenance")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET state='stopped', current_attempt_id=NULL, updated_ns=? WHERE job_id=?;
		UPDATE service_jobs SET desired_state='stopped', published_attempt_id=NULL, healthy_since_ns=NULL,
		next_restart_at=NULL WHERE job_id=?`, now.UnixNano(), row.JobID, row.JobID); err != nil {
		return Computer{}, internalError(err, "keep imported Computer stopped")
	}
	result, err := tx.ExecContext(ctx, `UPDATE computers SET applied_revision=1, reconfiguration_phase='stable',
		reconfiguration_revision=NULL, updated_ns=? WHERE computer_id=? AND intent_revision=1
		AND reconfiguration_phase='importing' AND reconfiguration_revision=1`, now.UnixNano(), destinationComputerID)
	if err != nil {
		return Computer{}, internalError(err, "publish imported Computer authority")
	}
	if err := requireComputerCAS(result, destinationComputerID, 1); err != nil {
		return Computer{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE computer_storage_copy_operations SET status='complete',
		verification_receipt_json=?, verification_receipt_hash=?, acknowledgement_key=?, acknowledgement_hash=?,
		verified_ns=?, published_ns=?, completed_ns=? WHERE destination_computer_id=? AND operation_revision=1 AND status='reserved'`,
		receiptJSON, bodyHash, request.IdempotencyKey, bodyHash, now.UnixNano(), now.UnixNano(), now.UnixNano(),
		destinationComputerID); err != nil {
		return Computer{}, internalError(err, "complete Custody import")
	}
	computer, err = readComputerAuthority(ctx, tx, destinationComputerID, now)
	if err != nil {
		return Computer{}, internalError(err, "read imported Computer")
	}
	if err := tx.Commit(); err != nil {
		return Computer{}, internalError(err, "commit verified Custody import")
	}
	s.notifyComputerPolicyChanged()
	return computer, nil
}
