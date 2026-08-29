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
	ManifestDigest          string     `json:"manifest_digest,omitempty"`
	OperatorAttestedDeleted bool       `json:"operator_attested_deleted"`
	RequestedAt             time.Time  `json:"requested_at"`
	CompletedAt             *time.Time `json:"completed_at,omitempty"`
	OperatorAttestedAt      *time.Time `json:"operator_attested_at,omitempty"`
}

type ComputerCustodyExportDirective struct {
	ExportID          string `json:"export_id"`
	ComputerID        string `json:"computer_id"`
	OperationRevision int64  `json:"operation_revision"`
	BackupID          string `json:"backup_id"`
	CopyID            string `json:"copy_id"`
	StorageID         string `json:"storage_id"`
	StorageGeneration int64  `json:"storage_generation"`
	AllocatedSize     int64  `json:"allocated_size"`
	ContentDigest     string `json:"content_digest"`
	BoundNodeID       string `json:"bound_node_id"`
	RootInstanceID    string `json:"root_instance_id"`
	ExternalPath      string `json:"external_path"`
	CustodyFence      string `json:"custody_fence"`
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
	Name           string `json:"name"`
	DiskBytes      int64  `json:"disk_bytes"`
	ExternalPath   string `json:"external_path"`
	IdempotencyKey string `json:"idempotency_key"`
	Actor          string `json:"-"`
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
		&value.Status, &value.ManifestDigest, &attestationKey, &requested, &completed, &attested)
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
	root_instance_id, external_path, status, manifest_digest, operator_attestation_key,
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
	requestHash, err := storageCopyHash(request)
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
	if err := tx.QueryRowContext(ctx, `SELECT export_id FROM computer_custody_exports WHERE external_path=?`, request.ExternalPath).Scan(new(string)); err == nil {
		return ComputerCustodyExport{}, false, protocolError(contract.ErrorConflict, "external_path is already bound to another Custody export")
	} else if !errors.Is(err, sql.ErrNoRows) {
		return ComputerCustodyExport{}, false, internalError(err, "check Custody export path")
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
	if _, err := tx.ExecContext(ctx, `INSERT INTO storage_provenance(provenance_id, kind, source_storage_id,
		source_generation, backup_id, created_ns) VALUES(?, 'export', ?, ?, ?, ?)`, newID("storage-provenance"),
		backup.SourceStorageID, backup.SourceGeneration, backup.BackupID, now.UnixNano()); err != nil {
		return ComputerCustodyExport{}, false, internalError(err, "record Custody export provenance")
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
	rows, err := s.db.QueryContext(ctx, `SELECT export_id, computer_id, operation_revision, backup_id,
		copy_id, source_storage_id, source_generation, allocated_size, content_digest, bound_node_id,
		root_instance_id, external_path, custody_fence FROM computer_custody_exports
		WHERE bound_node_id=? AND status='planned' ORDER BY requested_ns, export_id`, nodeID)
	if err != nil {
		return nil, internalError(err, "list Custody export directives")
	}
	defer rows.Close()
	directives := []ComputerCustodyExportDirective{}
	for rows.Next() {
		var directive ComputerCustodyExportDirective
		if err := rows.Scan(&directive.ExportID, &directive.ComputerID, &directive.OperationRevision,
			&directive.BackupID, &directive.CopyID, &directive.StorageID, &directive.StorageGeneration,
			&directive.AllocatedSize, &directive.ContentDigest, &directive.BoundNodeID,
			&directive.RootInstanceID, &directive.ExternalPath, &directive.CustodyFence); err != nil {
			return nil, internalError(err, "scan Custody export directive")
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
	if receipt.Kind != "computer_custody_export_verified" || receipt.ReceiptID == "" || receipt.HelperGeneration == 0 ||
		!backupDigestPattern.MatchString(receipt.ManifestDigest) || receipt.ExportID != export.ExportID || receipt.BackupID != export.BackupID ||
		receipt.CopyID != export.CopyID || receipt.ComputerID != export.ComputerID || receipt.StorageID != export.StorageID ||
		receipt.StorageGeneration != export.StorageGeneration || receipt.NodeID != export.BoundNodeID ||
		receipt.RootInstanceID != export.RootInstanceID || receipt.OperationRevision != export.OperationRevision ||
		receipt.CustodyFence != export.CustodyFence || receipt.ExternalPath != export.ExternalPath ||
		receipt.AllocatedSize != export.AllocatedSize || receipt.ContentDigest != export.ContentDigest {
		return protocolError(contract.ErrorStorageReferenceConflict, "Custody export receipt does not bind the recorded external transfer")
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
	if _, err := tx.ExecContext(ctx, `UPDATE computer_custody_exports SET status='exported', manifest_digest=?,
		receipt_json=?, acknowledgement_key=?, acknowledgement_hash=?, completed_ns=? WHERE export_id=?`,
		request.Receipt.ManifestDigest, receiptJSON, request.IdempotencyKey, bodyHash, now.UnixNano(), directive.ExportID); err != nil {
		return ComputerCustodyExport{}, internalError(err, "record verified Custody export")
	}
	if status == "planned" {
		if _, err := tx.ExecContext(ctx, `UPDATE computers SET applied_revision=?, reconfiguration_phase='stable',
			reconfiguration_revision=NULL, updated_ns=? WHERE computer_id=? AND intent_revision=? AND reconfiguration_phase='exporting'`,
			directive.OperationRevision, now.UnixNano(), computerID, directive.OperationRevision); err != nil {
			return ComputerCustodyExport{}, internalError(err, "complete Custody export intent")
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
	request.ExternalPath = filepath.Clean(strings.TrimSpace(request.ExternalPath))
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	request.Actor = strings.TrimSpace(request.Actor)
	if err := validateComputerNameAndActor(request.Name, request.Actor); err != nil {
		return ComputerCustodyImport{}, false, err
	}
	if exportID == "" || request.DiskBytes < 1 || request.IdempotencyKey == "" || !filepath.IsAbs(request.ExternalPath) {
		return ComputerCustodyImport{}, false, protocolError(contract.ErrorInvalidRequest, "export_id, name, positive disk_bytes, absolute external_path, and idempotency_key are required")
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
	var existing ComputerCustodyImport
	var requested int64
	var completed sql.NullInt64
	var storedHash string
	if err := tx.QueryRowContext(ctx, `SELECT import_id, export_id, destination_computer_id,
		destination_storage_id, destination_name, destination_size, status, requested_ns, completed_ns, request_hash
		FROM computer_custody_imports WHERE idempotency_key=?`, request.IdempotencyKey).Scan(&existing.ImportID,
		&existing.ExportID, &existing.DestinationComputerID, &existing.DestinationStorageID, &existing.DestinationName,
		&existing.DestinationSize, &existing.Status, &requested, &completed, &storedHash); err == nil {
		if storedHash != requestHash || existing.ExportID != exportID {
			return ComputerCustodyImport{}, false, protocolError(contract.ErrorIdempotencyConflict, "Custody import idempotency key was reused")
		}
		existing.RequestedAt = time.Unix(0, requested).UTC()
		if completed.Valid {
			stamp := time.Unix(0, completed.Int64).UTC()
			existing.CompletedAt = &stamp
		}
		return existing, true, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return ComputerCustodyImport{}, false, internalError(err, "read Custody import replay")
	}
	var sourceStorageID, nodeID, rootID, manifestDigest, sourceSpecHash string
	var sourceGeneration, sourceSize int64
	var sourceDigest string
	var sourceSpecJSON []byte
	if err := tx.QueryRowContext(ctx, `SELECT source_storage_id,
		source_generation, allocated_size, content_digest, bound_node_id, root_instance_id, manifest_digest,
		source_spec_json, source_spec_hash FROM computer_custody_exports WHERE export_id=? AND status='exported'`, exportID).Scan(
		&sourceStorageID, &sourceGeneration, &sourceSize, &sourceDigest,
		&nodeID, &rootID, &manifestDigest, &sourceSpecJSON, &sourceSpecHash); errors.Is(err, sql.ErrNoRows) {
		return ComputerCustodyImport{}, false, protocolError(contract.ErrorConflict, "Custody export %q is not complete and importable", exportID)
	} else if err != nil {
		return ComputerCustodyImport{}, false, internalError(err, "read Custody import source")
	}
	if request.DiskBytes < sourceSize {
		return ComputerCustodyImport{}, false, protocolError(contract.ErrorConflict, "Custody import disk_bytes cannot be smaller than its manifest")
	}
	if request.ExternalPath == "" || manifestDigest == "" {
		return ComputerCustodyImport{}, false, protocolError(contract.ErrorConflict, "Custody export lacks immutable manifest evidence")
	}
	if err := tx.QueryRowContext(ctx, `SELECT computer_id FROM computers WHERE name=?`, request.Name).Scan(new(string)); err == nil {
		return ComputerCustodyImport{}, false, protocolError(contract.ErrorConflict, "Computer name %q is already used", request.Name)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return ComputerCustodyImport{}, false, internalError(err, "check Custody import name")
	}
	if err := tx.QueryRowContext(ctx, `SELECT import_id FROM computer_custody_imports
		WHERE destination_name=? AND status='reserved'`, request.Name).Scan(new(string)); err == nil {
		return ComputerCustodyImport{}, false, protocolError(contract.ErrorConflict, "Computer name %q is reserved by another Custody import", request.Name)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return ComputerCustodyImport{}, false, internalError(err, "check pending Custody import name")
	}
	var spec contract.JobSpec
	if err := json.Unmarshal(sourceSpecJSON, &spec); err != nil {
		return ComputerCustodyImport{}, false, internalError(err, "decode Custody export Computer specification")
	}
	if !isComputerSpec(spec) {
		return ComputerCustodyImport{}, false, internalError(errors.New("Custody export specification lost its Computer trait"), "decode Custody export Computer specification")
	}
	if _, observedHash, err := encodeJobSpec(spec); err != nil || observedHash != sourceSpecHash {
		if err == nil {
			err = errors.New("Custody export specification digest mismatch")
		}
		return ComputerCustodyImport{}, false, internalError(err, "verify Custody export Computer specification")
	}
	computerID, storageID, jobID := newID("computer"), newID("storage"), newID("job")
	spec.DispatchKey = "computer:import:" + computerID
	spec.Execution.SensitiveEnv = nil
	spec.Execution.OCI.Computer.DiskBytes = request.DiskBytes
	specJSON, specHash, err := encodeJobSpec(spec)
	if err != nil {
		return ComputerCustodyImport{}, false, err
	}
	importID := newID("custody-import")
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_custody_imports(import_id, export_id,
		destination_computer_id, destination_storage_id, destination_job_id, destination_name,
		destination_size, bound_node_id, root_instance_id, external_path, operation_revision,
		cleanup_fence, destination_spec_json, destination_spec_hash, actor, idempotency_key,
		request_hash, status, requested_ns) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, 'reserved', ?)`,
		importID, exportID, computerID, storageID, jobID, request.Name, request.DiskBytes, nodeID, rootID,
		request.ExternalPath, newID("import-fence"), specJSON, specHash, request.Actor, request.IdempotencyKey,
		requestHash, now.UnixNano()); err != nil {
		return ComputerCustodyImport{}, false, internalError(err, "reserve Custody import")
	}
	value := ComputerCustodyImport{ImportID: importID, ExportID: exportID, DestinationComputerID: computerID,
		DestinationStorageID: storageID, DestinationName: request.Name, DestinationSize: request.DiskBytes,
		Status: "reserved", RequestedAt: now}
	if err := tx.Commit(); err != nil {
		return ComputerCustodyImport{}, false, internalError(err, "commit Custody import reservation")
	}
	return value, false, nil
}

func (s *Store) appendCustodyImportDirectives(ctx context.Context, nodeID string, directives []ComputerStorageCopyDirective) ([]ComputerStorageCopyDirective, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT i.export_id, e.backup_id, e.copy_id, e.computer_id,
		e.source_storage_id, e.source_generation, e.allocated_size, e.content_digest,
		i.destination_computer_id, i.destination_storage_id, i.destination_size, i.bound_node_id,
		i.root_instance_id, i.destination_job_id, i.operation_revision, i.cleanup_fence,
		i.external_path, e.manifest_digest FROM computer_custody_imports i
		JOIN computer_custody_exports e ON e.export_id=i.export_id
		WHERE i.bound_node_id=? AND i.status='reserved' ORDER BY i.requested_ns, i.import_id`, nodeID)
	if err != nil {
		return nil, internalError(err, "list Custody import directives")
	}
	defer rows.Close()
	for rows.Next() {
		var directive ComputerStorageCopyDirective
		directive.Operation = "import"
		directive.DestinationGeneration = 1
		if err := rows.Scan(&directive.ExportID, &directive.BackupID, &directive.CopyID,
			&directive.SourceComputerID, &directive.SourceStorageID, &directive.SourceGeneration,
			&directive.SourceSize, &directive.SourceDigest, &directive.DestinationComputerID,
			&directive.DestinationStorageID, &directive.DestinationSize, &directive.BoundNodeID,
			&directive.RootInstanceID, &directive.JobID, &directive.OperationRevision,
			&directive.CleanupFence, &directive.ExternalPath, &directive.ManifestDigest); err != nil {
			return nil, internalError(err, "scan Custody import directive")
		}
		directive.Phase = "reserved"
		directives = append(directives, directive)
	}
	return directives, rows.Err()
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
	var importID, exportID, storageID, jobID, name, nodeID, rootID, cleanupFence, status, actor string
	var destinationSize, revision int64
	var specJSON []byte
	var specHash, backupID, copyID, sourceComputerID, sourceStorageID, sourceDigest, externalPath, manifestDigest string
	var sourceGeneration, sourceSize int64
	var ackKey, ackHash sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT i.import_id, i.export_id, i.destination_storage_id,
		i.destination_job_id, i.destination_name, i.destination_size, i.bound_node_id, i.root_instance_id,
		i.operation_revision, i.cleanup_fence, i.status, i.destination_spec_json, i.destination_spec_hash,
		i.actor, i.acknowledgement_key, i.acknowledgement_hash, e.backup_id, e.copy_id, e.computer_id,
		e.source_storage_id, e.source_generation, e.allocated_size, e.content_digest, i.external_path,
		e.manifest_digest FROM computer_custody_imports i
		JOIN computer_custody_exports e ON e.export_id=i.export_id WHERE i.destination_computer_id=?`, destinationComputerID).Scan(
		&importID, &exportID, &storageID, &jobID, &name, &destinationSize, &nodeID, &rootID,
		&revision, &cleanupFence, &status, &specJSON, &specHash, &actor, &ackKey, &ackHash, &backupID,
		&copyID, &sourceComputerID, &sourceStorageID, &sourceGeneration, &sourceSize, &sourceDigest,
		&externalPath, &manifestDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return Computer{}, protocolError(contract.ErrorNotFound, "Custody import operation was not found")
	}
	if err != nil {
		return Computer{}, internalError(err, "read Custody import acknowledgement target")
	}
	if err := validateBackupAcknowledgementAuthority(ctx, tx, identityNodeID, request.NodeID, nodeID, rootID); err != nil {
		return Computer{}, err
	}
	receipt := request.Receipt
	if receipt.Kind != computerStorageCopyReceiptKind || receipt.ReceiptID == "" || receipt.Operation != "import" ||
		receipt.BackupID != backupID || receipt.CopyID != copyID || receipt.ExportID != exportID ||
		receipt.ExternalPath != externalPath || receipt.ManifestDigest != manifestDigest ||
		receipt.SourceComputerID != sourceComputerID || receipt.SourceStorageID != sourceStorageID ||
		receipt.SourceGeneration != sourceGeneration || receipt.DestinationComputerID != destinationComputerID ||
		receipt.DestinationStorageID != storageID || receipt.DestinationGeneration != 1 || receipt.NodeID != nodeID ||
		receipt.RootInstanceID != rootID || receipt.JobID != jobID || receipt.OperationRevision != revision ||
		receipt.CleanupFence != cleanupFence || receipt.HelperGeneration == 0 || receipt.SourceSize != sourceSize ||
		receipt.DestinationSize != destinationSize || receipt.SourceDigest != sourceDigest ||
		!backupDigestPattern.MatchString(receipt.SourceDigest) || !backupDigestPattern.MatchString(receipt.DestinationDigest) ||
		!receipt.OSIdentityRekeyed || receipt.FilesystemExpanded != (destinationSize > sourceSize) {
		return Computer{}, protocolError(contract.ErrorStorageReferenceConflict, "Custody import receipt does not bind the verified manifest and destination")
	}
	if ackKey.Valid {
		if ackKey.String != request.IdempotencyKey || !ackHash.Valid || ackHash.String != bodyHash {
			return Computer{}, protocolError(contract.ErrorIdempotencyConflict, "Custody import acknowledgement differs from durable evidence")
		}
		return readComputerAuthority(ctx, tx, destinationComputerID, now)
	}
	if status != "reserved" {
		return Computer{}, protocolError(contract.ErrorConflict, "Custody import is not awaiting verification")
	}
	var spec contract.JobSpec
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		return Computer{}, internalError(err, "decode Custody import Computer specification")
	}
	if !isComputerSpec(spec) {
		return Computer{}, internalError(errors.New("Custody import specification lost its Computer trait"), "decode Custody import Computer specification")
	}
	if _, observedHash, err := encodeJobSpec(spec); err != nil || observedHash != specHash {
		if err == nil {
			err = errors.New("Custody import specification digest mismatch")
		}
		return Computer{}, internalError(err, "verify Custody import Computer specification")
	}
	if _, err := insertComputerJobWithID(ctx, tx, jobID, spec, specJSON, specHash, contract.JobStopped,
		contract.ServiceDesiredStopped, nodeID, now); err != nil {
		return Computer{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computers(computer_id, name, placement_node_id,
		bound_node_id, grants_json, storage_id, storage_generation, desired_disk_bytes, backup_cap, desired_state,
		intent_revision, applied_revision, current_job_id, current_spec_revision, reconfiguration_phase,
		created_ns, updated_ns) VALUES(?, ?, ?, ?, '[]', ?, 1, ?, 0, 'stopped', 1, 1, ?, 1, 'stable', ?, ?)`,
		destinationComputerID, name, nodeID, nodeID, storageID, destinationSize, jobID, now.UnixNano(), now.UnixNano()); err != nil {
		return Computer{}, internalError(err, "publish imported Computer identity")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_job_projections(computer_id, job_id,
		spec_revision, current, created_ns) VALUES(?, ?, 1, 1, ?)`, destinationComputerID, jobID, now.UnixNano()); err != nil {
		return Computer{}, internalError(err, "publish imported Computer projection")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_storage_generations(computer_id, storage_id,
		storage_generation, disk_bytes, phase, created_ns) VALUES(?, ?, 1, ?, 'current', ?)`,
		destinationComputerID, storageID, destinationSize, now.UnixNano()); err != nil {
		return Computer{}, internalError(err, "publish imported Storage identity")
	}
	if err := insertComputerIntent(ctx, tx, destinationComputerID, 1, ComputerIntentCustodyImport,
		contract.ServiceDesiredStopped, storageID, 1, jobID, 1, actor, now); err != nil {
		return Computer{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO storage_provenance(provenance_id, kind,
		source_storage_id, source_generation, backup_id, destination_computer_id,
		destination_storage_id, destination_generation, created_ns) VALUES(?, 'import', ?, ?, ?, ?, ?, 1, ?)`,
		newID("storage-provenance"), sourceStorageID, sourceGeneration, backupID,
		destinationComputerID, storageID, now.UnixNano()); err != nil {
		return Computer{}, internalError(err, "publish imported Storage provenance")
	}
	receiptJSON, err := json.Marshal(receipt)
	if err != nil {
		return Computer{}, internalError(err, "encode Custody import receipt")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE computer_custody_imports SET status='complete', receipt_json=?,
		acknowledgement_key=?, acknowledgement_hash=?, completed_ns=? WHERE import_id=? AND status='reserved'`,
		receiptJSON, request.IdempotencyKey, bodyHash, now.UnixNano(), importID); err != nil {
		return Computer{}, internalError(err, "complete Custody import")
	}
	computer, err := readComputerAuthority(ctx, tx, destinationComputerID, now)
	if err != nil {
		return Computer{}, internalError(err, "read imported Computer")
	}
	if err := tx.Commit(); err != nil {
		return Computer{}, internalError(err, "commit verified Custody import")
	}
	s.notifyComputerPolicyChanged()
	return computer, nil
}
