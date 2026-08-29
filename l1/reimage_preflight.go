package l1

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
)

const (
	computerReimagePreflightReceiptKind       = "computer_reimage_preflight_verified"
	computerReimagePreflightFailedReceiptKind = "computer_reimage_preflight_failed_unchanged"
)

type computerReimageOperation struct {
	ComputerID, OldJobID, StagingJobID, StorageID, BoundNodeID, RootInstanceID string
	OperationFence, TargetReference, TargetDigest, IdempotencyKey, RequestHash string
	Status                                                                     string
	OperationRevision, StorageGeneration                                       int64
	Chown                                                                      bool
	AcknowledgementKey, AcknowledgementHash                                    sql.NullString
}

func readComputerReimageOperation(ctx context.Context, q queryer, computerID string, revision int64) (computerReimageOperation, error) {
	var row computerReimageOperation
	err := q.QueryRowContext(ctx, `SELECT computer_id, operation_revision, old_job_id, staging_job_id,
		storage_id, storage_generation, bound_node_id, root_instance_id, operation_fence,
		target_reference, target_digest, chown, idempotency_key, request_hash, status,
		acknowledgement_key, acknowledgement_hash FROM computer_reimage_operations
		WHERE computer_id=? AND operation_revision=?`, computerID, revision).Scan(&row.ComputerID,
		&row.OperationRevision, &row.OldJobID, &row.StagingJobID, &row.StorageID,
		&row.StorageGeneration, &row.BoundNodeID, &row.RootInstanceID, &row.OperationFence,
		&row.TargetReference, &row.TargetDigest, &row.Chown, &row.IdempotencyKey, &row.RequestHash,
		&row.Status, &row.AcknowledgementKey, &row.AcknowledgementHash)
	return row, err
}

func (s *Store) ListNodeComputerReimagePreflightDirectives(ctx context.Context, identityNodeID, nodeID, bootSessionID string) ([]ComputerReimagePreflightDirective, error) {
	if err := validateStorageResetNode(ctx, s.db, identityNodeID, nodeID, bootSessionID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT r.computer_id, r.storage_id, r.storage_generation,
		r.old_job_id, r.staging_job_id, r.bound_node_id, r.root_instance_id, r.operation_revision,
		r.operation_fence, r.target_reference, r.target_digest, r.chown
		FROM computer_reimage_operations r JOIN computers c ON c.computer_id=r.computer_id
		JOIN jobs j ON j.job_id=r.old_job_id
		WHERE r.bound_node_id=? AND r.status='planned' AND c.reconfiguration_phase='reimaging'
		AND c.reconfiguration_revision=r.operation_revision AND j.state='stopped'
		ORDER BY r.requested_ns, r.computer_id`, nodeID)
	if err != nil {
		return nil, internalError(err, "list Computer reimage preflight directives")
	}
	defer rows.Close()
	result := []ComputerReimagePreflightDirective{}
	for rows.Next() {
		var directive ComputerReimagePreflightDirective
		var reference, digest string
		if err := rows.Scan(&directive.ComputerID, &directive.StorageID, &directive.StorageGeneration,
			&directive.OldJobID, &directive.StagingJobID, &directive.BoundNodeID,
			&directive.RootInstanceID, &directive.OperationRevision, &directive.OperationFence,
			&reference, &digest, &directive.Chown); err != nil {
			return nil, internalError(err, "scan Computer reimage preflight directive")
		}
		directive.TargetImage = contract.OCIImageSpec{Reference: reference, Digest: &digest}
		result = append(result, directive)
	}
	return result, rows.Err()
}

func reimagePreflightAcknowledgementHash(request ComputerReimagePreflightAcknowledgementRequest) (string, []byte, error) {
	body, err := json.Marshal(struct {
		IdempotencyKey string                          `json:"idempotency_key"`
		Receipt        ComputerReimagePreflightReceipt `json:"receipt"`
	}{request.IdempotencyKey, request.Receipt})
	if err != nil {
		return "", nil, err
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), body, nil
}

func validateComputerReimagePreflight(row computerReimageOperation, receipt ComputerReimagePreflightReceipt,
	nodeOS, nodeArchitecture string,
) error {
	if (receipt.Kind != computerReimagePreflightReceiptKind && receipt.Kind != computerReimagePreflightFailedReceiptKind) ||
		receipt.ReceiptID == "" ||
		receipt.HelperGeneration == 0 || receipt.ComputerID != row.ComputerID ||
		receipt.StorageID != row.StorageID || receipt.StorageGeneration != row.StorageGeneration ||
		receipt.OldJobID != row.OldJobID || receipt.StagingJobID != row.StagingJobID ||
		receipt.NodeID != row.BoundNodeID || receipt.RootInstanceID != row.RootInstanceID ||
		receipt.OperationRevision != row.OperationRevision || receipt.OperationFence != row.OperationFence ||
		receipt.TargetDigest != row.TargetDigest || receipt.DetachmentReceiptID == "" ||
		receipt.DetachmentAttemptID == "" || receipt.DetachmentFencingToken == "" {
		return protocolError(contract.ErrorConflict, "Computer reimage preflight receipt does not match current authority")
	}
	if receipt.PlatformOS != nodeOS || receipt.PlatformArchitecture != nodeArchitecture {
		return protocolError(contract.ErrorConflict, "Computer reimage image platform does not match its bound Node")
	}
	if receipt.Kind == computerReimagePreflightFailedReceiptKind {
		if receipt.FailureCode != string(contract.SpawnFailureImageUnavailable) &&
			receipt.FailureCode != string(contract.SpawnFailureImagePlatformUnsupported) {
			return protocolError(contract.ErrorConflict, "Computer reimage preflight failure code is not supported")
		}
		return nil
	}
	if receipt.FailureCode != "" {
		return protocolError(contract.ErrorConflict, "successful Computer reimage preflight cannot carry a failure")
	}
	if !row.Chown && (receipt.ImageUID != receipt.DiskRootUID || receipt.ImageGID != receipt.DiskRootGID) {
		return protocolError(contract.ErrorConflict, "Computer reimage image user does not own the current disk root")
	}
	return nil
}

func (s *Store) AcknowledgeComputerReimagePreflight(ctx context.Context, identityNodeID, computerID string,
	request ComputerReimagePreflightAcknowledgementRequest,
) (Computer, error) {
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	if request.NodeID == "" || request.BootSessionID == "" || request.IdempotencyKey == "" {
		return Computer{}, protocolError(contract.ErrorInvalidRequest,
			"complete Computer reimage preflight acknowledgement fields are required")
	}
	bodyHash, body, err := reimagePreflightAcknowledgementHash(request)
	if err != nil {
		return Computer{}, internalError(err, "encode Computer reimage preflight receipt")
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Computer{}, internalError(err, "begin Computer reimage preflight acknowledgement")
	}
	defer tx.Rollback()
	if err := validateStorageResetNode(ctx, tx, identityNodeID, request.NodeID, request.BootSessionID); err != nil {
		return Computer{}, err
	}
	row, err := readComputerReimageOperation(ctx, tx, computerID, request.Receipt.OperationRevision)
	if errors.Is(err, sql.ErrNoRows) {
		return Computer{}, protocolError(contract.ErrorNotFound, "Computer reimage operation was not found")
	}
	if err != nil {
		return Computer{}, internalError(err, "read Computer reimage preflight authority")
	}
	var nodeOS, nodeArchitecture, rootInstanceID string
	if err := tx.QueryRowContext(ctx, `SELECT os, architecture, root_instance_id FROM nodes WHERE node_id=?`,
		row.BoundNodeID).Scan(&nodeOS, &nodeArchitecture, &rootInstanceID); err != nil {
		return Computer{}, internalError(err, "read Computer reimage node facts")
	}
	if rootInstanceID != row.RootInstanceID {
		return Computer{}, protocolError(contract.ErrorStaleFence, "Computer reimage managed-root authority changed")
	}
	if request.NodeID != row.BoundNodeID {
		return Computer{}, protocolError(contract.ErrorAttemptNotOwned, "authenticated Node does not own Computer reimage")
	}
	if err := validateComputerReimagePreflight(row, request.Receipt, nodeOS, nodeArchitecture); err != nil {
		return Computer{}, err
	}
	computer, err := readComputerAuthority(ctx, tx, computerID, now)
	if err != nil {
		return Computer{}, err
	}
	if row.Status != "planned" {
		if (row.Status == "preflight_verified" || row.Status == "failed") && row.AcknowledgementKey.Valid &&
			row.AcknowledgementKey.String == request.IdempotencyKey && row.AcknowledgementHash.Valid &&
			row.AcknowledgementHash.String == bodyHash {
			return computer, tx.Commit()
		}
		return Computer{}, protocolError(contract.ErrorIdempotencyConflict,
			"Computer reimage preflight acknowledgement conflicts with durable outcome")
	}
	if computer.ReconfigurationPhase != ComputerReconfigurationReimaging ||
		computer.ReconfigurationRevision == nil || *computer.ReconfigurationRevision != row.OperationRevision ||
		computer.CurrentJobID != row.OldJobID || computer.CurrentJob.State != contract.JobStopped {
		return Computer{}, protocolError(contract.ErrorStaleIntentRevision,
			"Computer reimage preflight no longer owns current authority")
	}
	if request.Receipt.Kind == computerReimagePreflightFailedReceiptKind {
		failureCode := contract.SpawnFailureCode(request.Receipt.FailureCode)
		lastFailure, marshalErr := json.Marshal(contract.SpawnFailure{Code: failureCode,
			Message: "Computer reimage target could not be verified for the bound Node", NodeID: row.BoundNodeID})
		if marshalErr != nil {
			return Computer{}, internalError(marshalErr, "encode Computer reimage preflight failure")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE computer_reimage_operations SET status='failed',
			preflight_receipt_json=?, preflight_receipt_hash=?, acknowledgement_key=?, acknowledgement_hash=?, completed_ns=?
			WHERE computer_id=? AND operation_revision=? AND status='planned'`, body, bodyHash,
			request.IdempotencyKey, bodyHash, now.UnixNano(), computerID, row.OperationRevision); err != nil {
			return Computer{}, internalError(err, "persist Computer reimage preflight failure")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE computer_job_projections SET retired_ns=?, chown=0
			WHERE computer_id=? AND job_id=? AND current=0 AND retired_ns IS NULL`, now.UnixNano(),
			computerID, row.StagingJobID); err != nil {
			return Computer{}, internalError(err, "retire refused Computer reimage projection")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE service_jobs SET last_failure=?, next_restart_at=NULL
			WHERE job_id=?`, lastFailure, row.OldJobID); err != nil {
			return Computer{}, internalError(err, "record Computer reimage preflight failure")
		}
		result, err := tx.ExecContext(ctx, `UPDATE computers SET applied_revision=?, reconfiguration_phase='stable',
			reconfiguration_revision=NULL, updated_ns=? WHERE computer_id=? AND intent_revision=?
			AND reconfiguration_phase='reimaging' AND reconfiguration_revision=?`, row.OperationRevision,
			now.UnixNano(), computerID, row.OperationRevision, row.OperationRevision)
		if err != nil {
			return Computer{}, internalError(err, "release refused Computer reimage authority")
		}
		if err := requireComputerCAS(result, computerID, row.OperationRevision); err != nil {
			return Computer{}, err
		}
		updated, err := readComputerAuthority(ctx, tx, computerID, now)
		if err != nil {
			return Computer{}, internalError(err, "read refused Computer reimage")
		}
		if err := tx.Commit(); err != nil {
			return Computer{}, internalError(err, "commit Computer reimage preflight failure")
		}
		s.notifyComputerPolicyChanged()
		return updated, nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE computer_reimage_operations SET status='preflight_verified',
		preflight_receipt_json=?, preflight_receipt_hash=?, acknowledgement_key=?, acknowledgement_hash=?, verified_ns=?
		WHERE computer_id=? AND operation_revision=? AND status='planned'`, body, bodyHash,
		request.IdempotencyKey, bodyHash, now.UnixNano(), computerID, row.OperationRevision); err != nil {
		return Computer{}, internalError(err, "persist Computer reimage preflight")
	}
	if err := tx.Commit(); err != nil {
		return Computer{}, internalError(err, "commit Computer reimage preflight")
	}
	return computer, nil
}
