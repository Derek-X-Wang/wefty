package l1

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const (
	computerStorageGrowAppliedKind = "computer_storage_grow_applied"
	computerStorageGrowFailedKind  = "computer_storage_grow_failed_unchanged"
)

type ComputerStorageGrowDirective struct {
	ComputerID        string `json:"computer_id"`
	StorageID         string `json:"storage_id"`
	StorageGeneration int64  `json:"storage_generation"`
	OldDiskBytes      int64  `json:"old_disk_bytes"`
	NewDiskBytes      int64  `json:"new_disk_bytes"`
	BoundNodeID       string `json:"bound_node_id"`
	RootInstanceID    string `json:"root_instance_id"`
	JobID             string `json:"job_id"`
	OperationRevision int64  `json:"operation_revision"`
	OperationFence    string `json:"operation_fence"`
}

type ComputerStorageGrowReceipt = contract.ComputerStorageGrowReceipt

type ComputerStorageGrowAcknowledgementRequest struct {
	NodeID         string                     `json:"node_id"`
	BootSessionID  string                     `json:"boot_session_id"`
	IdempotencyKey string                     `json:"idempotency_key"`
	Receipt        ComputerStorageGrowReceipt `json:"receipt"`
}

// ComputerStorageGrowOutcome projects the latest durable grow operation. The
// optional available-byte fact is populated only from a validated helper
// refusal receipt, so zero remains distinguishable from missing evidence.
type ComputerStorageGrowOutcome struct {
	OperationRevision      int64      `json:"operation_revision"`
	Status                 string     `json:"status"`
	RequestedBytes         int64      `json:"requested_bytes"`
	ObservedAvailableBytes *int64     `json:"observed_available_bytes,omitempty"`
	FailureCode            string     `json:"failure_code,omitempty"`
	CompletedAt            *time.Time `json:"completed_at,omitempty"`
}

type computerStorageGrowRow struct {
	ComputerID, StorageID, BoundNodeID, RootInstanceID, JobID string
	OperationFence, IdempotencyKey, RequestHash, Status       string
	FailureCode                                               string
	OperationRevision, StorageGeneration                      int64
	OldDiskBytes, NewDiskBytes                                int64
	AcknowledgementKey, AcknowledgementHash                   sql.NullString
}

func readLastComputerStorageGrowOutcome(ctx context.Context, q queryer, computerID string) (*ComputerStorageGrowOutcome, error) {
	return scanComputerStorageGrowOutcome(q.QueryRowContext(ctx, `SELECT operation_revision, status, new_disk_bytes, failure_code,
		receipt_json, completed_ns FROM computer_storage_grows WHERE computer_id=?
		ORDER BY operation_revision DESC LIMIT 1`, computerID))
}

func readComputerStorageGrowOutcomeByRevision(ctx context.Context, q queryer, computerID string, operationRevision int64) (*ComputerStorageGrowOutcome, error) {
	return scanComputerStorageGrowOutcome(q.QueryRowContext(ctx, `SELECT operation_revision, status, new_disk_bytes, failure_code,
		receipt_json, completed_ns FROM computer_storage_grows WHERE computer_id=? AND operation_revision=?`,
		computerID, operationRevision))
}

func scanComputerStorageGrowOutcome(row interface{ Scan(...any) error }) (*ComputerStorageGrowOutcome, error) {
	var outcome ComputerStorageGrowOutcome
	var receiptJSON []byte
	var completedNS sql.NullInt64
	err := row.Scan(&outcome.OperationRevision, &outcome.Status,
		&outcome.RequestedBytes, &outcome.FailureCode, &receiptJSON, &completedNS)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if completedNS.Valid {
		completed := time.Unix(0, completedNS.Int64).UTC()
		outcome.CompletedAt = &completed
	}
	if len(receiptJSON) != 0 {
		var acknowledgement struct {
			Receipt ComputerStorageGrowReceipt `json:"receipt"`
		}
		if err := json.Unmarshal(receiptJSON, &acknowledgement); err != nil {
			return nil, err
		}
		if outcome.Status == "failed" && acknowledgement.Receipt.FailureCode != "" {
			available := acknowledgement.Receipt.ObservedAvailableBytes
			outcome.ObservedAvailableBytes = &available
		}
	}
	return &outcome, nil
}

func growRequestHash(request ComputerGrowRequest) (string, error) {
	payload, err := json.Marshal(struct {
		IntentRevision    int64  `json:"intent_revision"`
		StorageID         string `json:"storage_id"`
		StorageGeneration int64  `json:"storage_generation"`
		DiskBytes         int64  `json:"disk_bytes"`
		IdempotencyKey    string `json:"idempotency_key"`
		Actor             string `json:"actor"`
	}{request.IntentRevision, request.StorageID, request.StorageGeneration, request.DiskBytes,
		request.IdempotencyKey, request.Actor})
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(payload)
	return hex.EncodeToString(hash[:]), nil
}

func readComputerGrowByKey(ctx context.Context, q queryer, computerID, key string) (computerStorageGrowRow, error) {
	var row computerStorageGrowRow
	err := q.QueryRowContext(ctx, `SELECT computer_id, operation_revision, storage_id, storage_generation,
		old_disk_bytes, new_disk_bytes, bound_node_id, root_instance_id, job_id, operation_fence,
		idempotency_key, request_hash, status, failure_code, acknowledgement_key, acknowledgement_hash
		FROM computer_storage_grows WHERE computer_id=? AND idempotency_key=?`, computerID, key).Scan(
		&row.ComputerID, &row.OperationRevision, &row.StorageID, &row.StorageGeneration,
		&row.OldDiskBytes, &row.NewDiskBytes, &row.BoundNodeID, &row.RootInstanceID, &row.JobID,
		&row.OperationFence, &row.IdempotencyKey, &row.RequestHash, &row.Status, &row.FailureCode,
		&row.AcknowledgementKey, &row.AcknowledgementHash)
	return row, err
}

func readComputerGrowByRevision(ctx context.Context, q queryer, computerID string, revision int64) (computerStorageGrowRow, error) {
	var row computerStorageGrowRow
	err := q.QueryRowContext(ctx, `SELECT computer_id, operation_revision, storage_id, storage_generation,
		old_disk_bytes, new_disk_bytes, bound_node_id, root_instance_id, job_id, operation_fence,
		idempotency_key, request_hash, status, failure_code, acknowledgement_key, acknowledgement_hash
		FROM computer_storage_grows WHERE computer_id=? AND operation_revision=?`, computerID, revision).Scan(
		&row.ComputerID, &row.OperationRevision, &row.StorageID, &row.StorageGeneration,
		&row.OldDiskBytes, &row.NewDiskBytes, &row.BoundNodeID, &row.RootInstanceID, &row.JobID,
		&row.OperationFence, &row.IdempotencyKey, &row.RequestHash, &row.Status, &row.FailureCode,
		&row.AcknowledgementKey, &row.AcknowledgementHash)
	return row, err
}

func (s *Store) BeginComputerGrow(ctx context.Context, computerID string, request ComputerGrowRequest) (Computer, bool, error) {
	request.Actor = strings.TrimSpace(request.Actor)
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	if strings.TrimSpace(computerID) == "" || request.IdempotencyKey == "" || request.DiskBytes <= 0 {
		return Computer{}, false, protocolError(contract.ErrorInvalidRequest,
			"computer_id, positive disk_bytes, and idempotency_key are required")
	}
	requestHash, err := growRequestHash(request)
	if err != nil {
		return Computer{}, false, internalError(err, "encode Computer grow request")
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Computer{}, false, internalError(err, "begin Computer grow")
	}
	defer tx.Rollback()
	computer, err := readComputerAuthority(ctx, tx, computerID, now)
	if errors.Is(err, sql.ErrNoRows) {
		return Computer{}, false, protocolError(contract.ErrorNotFound, "Computer %q was not found", computerID)
	}
	if err != nil {
		return Computer{}, false, internalError(err, "read Computer grow target")
	}
	if replay, replayErr := readComputerGrowByKey(ctx, tx, computerID, request.IdempotencyKey); replayErr == nil {
		if replay.RequestHash != requestHash {
			return Computer{}, false, protocolError(contract.ErrorIdempotencyConflict,
				"Computer grow idempotency key was reused with different authority")
		}
		outcome, outcomeErr := readComputerStorageGrowOutcomeByRevision(ctx, tx, computerID, replay.OperationRevision)
		if outcomeErr != nil {
			return Computer{}, false, internalError(outcomeErr, "read Computer grow replay outcome")
		}
		computer.LastGrowOperation = outcome
		return computer, true, tx.Commit()
	} else if !errors.Is(replayErr, sql.ErrNoRows) {
		return Computer{}, false, internalError(replayErr, "read Computer grow replay")
	}
	if err := validateComputerPrecondition(computer, request.ComputerMutationPrecondition); err != nil {
		return Computer{}, false, err
	}
	if computer.DesiredState == contract.ServiceDesiredRemoved {
		return Computer{}, false, protocolError(contract.ErrorConflict, "Computer %q is being removed", computerID)
	}
	if computer.ReconfigurationPhase != ComputerReconfigurationStable {
		return Computer{}, false, protocolError(contract.ErrorConflict,
			"Computer %q is in reconfiguration phase %q", computerID, computer.ReconfigurationPhase)
	}
	if request.DiskBytes <= computer.DesiredDiskBytes {
		return Computer{}, false, protocolErrorWithDetails(contract.ErrorConflict, map[string]any{
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
		return Computer{}, false, protocolError(contract.ErrorConflict, "bound node %q is unavailable", boundNodeID)
	}
	if rootInstanceID == "" {
		return Computer{}, false, protocolError(contract.ErrorConflict, "bound node has no managed-root instance")
	}
	nextRevision := computer.IntentRevision + 1
	result, err := tx.ExecContext(ctx, `UPDATE computers SET intent_revision=?, reconfiguration_phase=?,
		reconfiguration_revision=?, updated_ns=? WHERE computer_id=? AND intent_revision=?`, nextRevision,
		ComputerReconfigurationGrowing, nextRevision, now.UnixNano(), computerID, computer.IntentRevision)
	if err != nil {
		return Computer{}, false, internalError(err, "reserve Computer grow intent")
	}
	if err := requireComputerCAS(result, computerID, computer.IntentRevision); err != nil {
		return Computer{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_storage_grows(computer_id, operation_revision,
		storage_id, storage_generation, old_disk_bytes, new_disk_bytes, bound_node_id, root_instance_id,
		job_id, operation_fence, idempotency_key, request_hash, status, requested_ns)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'planned', ?)`, computerID, nextRevision,
		computer.StorageID, computer.StorageGeneration, computer.DesiredDiskBytes, request.DiskBytes,
		boundNodeID, rootInstanceID, computer.CurrentJobID, newID("grow-fence"), request.IdempotencyKey,
		requestHash, now.UnixNano()); err != nil {
		return Computer{}, false, internalError(err, "persist Computer grow intent")
	}
	if err := insertComputerIntent(ctx, tx, computerID, nextRevision, ComputerIntentGrow, computer.DesiredState,
		computer.StorageID, computer.StorageGeneration, computer.CurrentJobID, computer.CurrentSpecRevision,
		request.Actor, now); err != nil {
		return Computer{}, false, err
	}
	updated, err := readComputerAuthority(ctx, tx, computerID, now)
	if err != nil {
		return Computer{}, false, internalError(err, "read planned Computer grow")
	}
	if err := tx.Commit(); err != nil {
		return Computer{}, false, internalError(err, "commit Computer grow intent")
	}
	s.notifyComputerPolicyChanged()
	return updated, false, nil
}

func (s *Store) ListNodeComputerStorageGrowDirectives(ctx context.Context, identityNodeID, nodeID, bootSessionID string) ([]ComputerStorageGrowDirective, error) {
	if err := validateStorageResetNode(ctx, s.db, identityNodeID, nodeID, bootSessionID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT g.computer_id, g.storage_id, g.storage_generation,
		g.old_disk_bytes, g.new_disk_bytes, g.bound_node_id, g.root_instance_id, g.job_id,
		g.operation_revision, g.operation_fence FROM computer_storage_grows g JOIN computers c
		ON c.computer_id=g.computer_id WHERE g.bound_node_id=? AND g.status='planned'
		AND c.reconfiguration_phase='growing' AND c.reconfiguration_revision=g.operation_revision
		ORDER BY g.requested_ns, g.computer_id`, nodeID)
	if err != nil {
		return nil, internalError(err, "list Computer grow directives")
	}
	defer rows.Close()
	directives := []ComputerStorageGrowDirective{}
	for rows.Next() {
		var directive ComputerStorageGrowDirective
		if err := rows.Scan(&directive.ComputerID, &directive.StorageID, &directive.StorageGeneration,
			&directive.OldDiskBytes, &directive.NewDiskBytes, &directive.BoundNodeID,
			&directive.RootInstanceID, &directive.JobID, &directive.OperationRevision,
			&directive.OperationFence); err != nil {
			return nil, internalError(err, "scan Computer grow directive")
		}
		directives = append(directives, directive)
	}
	return directives, rows.Err()
}

func validateComputerGrowReceipt(row computerStorageGrowRow, receipt ComputerStorageGrowReceipt) error {
	if receipt.ReceiptID == "" || receipt.HelperGeneration == 0 || receipt.ComputerID != row.ComputerID ||
		receipt.StorageID != row.StorageID || receipt.StorageGeneration != row.StorageGeneration ||
		receipt.NodeID != row.BoundNodeID || receipt.RootInstanceID != row.RootInstanceID ||
		receipt.JobID != row.JobID || receipt.OperationRevision != row.OperationRevision ||
		receipt.OperationFence != row.OperationFence || receipt.OldDiskBytes != row.OldDiskBytes ||
		receipt.NewDiskBytes != row.NewDiskBytes {
		return protocolError(contract.ErrorConflict, "Computer grow receipt does not match current authority")
	}
	if receipt.Kind == computerStorageGrowAppliedKind && receipt.Applied && receipt.FailureCode == "" {
		return nil
	}
	if receipt.Kind == computerStorageGrowFailedKind && !receipt.Applied && receipt.FailureCode == "insufficient_disk" &&
		receipt.ObservedAvailableBytes >= 0 {
		return nil
	}
	return protocolError(contract.ErrorInvalidRequest, "Computer grow receipt lacks assertion-derived outcome")
}

func computerGrowAcknowledgementHash(request ComputerStorageGrowAcknowledgementRequest) (string, []byte, error) {
	body, err := json.Marshal(struct {
		IdempotencyKey string                     `json:"idempotency_key"`
		Receipt        ComputerStorageGrowReceipt `json:"receipt"`
	}{request.IdempotencyKey, request.Receipt})
	if err != nil {
		return "", nil, err
	}
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:]), body, nil
}

func (s *Store) AcknowledgeComputerStorageGrow(ctx context.Context, identityNodeID, computerID string,
	request ComputerStorageGrowAcknowledgementRequest) (Computer, error) {
	if request.NodeID == "" || request.BootSessionID == "" || request.IdempotencyKey == "" {
		return Computer{}, protocolError(contract.ErrorInvalidRequest, "complete Computer grow acknowledgement fields are required")
	}
	bodyHash, body, err := computerGrowAcknowledgementHash(request)
	if err != nil {
		return Computer{}, internalError(err, "encode Computer grow receipt")
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Computer{}, internalError(err, "begin Computer grow acknowledgement")
	}
	defer tx.Rollback()
	if err := validateStorageResetNode(ctx, tx, identityNodeID, request.NodeID, request.BootSessionID); err != nil {
		return Computer{}, err
	}
	row, err := readComputerGrowByRevision(ctx, tx, computerID, request.Receipt.OperationRevision)
	if errors.Is(err, sql.ErrNoRows) {
		return Computer{}, protocolError(contract.ErrorNotFound, "Computer grow operation was not found")
	}
	if err != nil {
		return Computer{}, internalError(err, "read Computer grow authority")
	}
	if err := validateComputerGrowReceipt(row, request.Receipt); err != nil {
		return Computer{}, err
	}
	if request.NodeID != row.BoundNodeID {
		return Computer{}, protocolError(contract.ErrorAttemptNotOwned, "authenticated node does not own Computer grow")
	}
	var currentRoot string
	if err := tx.QueryRowContext(ctx, `SELECT root_instance_id FROM nodes WHERE node_id=?`, row.BoundNodeID).Scan(&currentRoot); err != nil {
		return Computer{}, internalError(err, "read Computer grow managed-root authority")
	}
	if currentRoot != row.RootInstanceID {
		return Computer{}, protocolError(contract.ErrorStaleFence, "Computer grow managed-root authority changed")
	}
	computer, err := readComputerAuthority(ctx, tx, computerID, now)
	if err != nil {
		return Computer{}, err
	}
	if row.Status != "planned" {
		if !row.AcknowledgementKey.Valid || row.AcknowledgementKey.String != request.IdempotencyKey ||
			!row.AcknowledgementHash.Valid || row.AcknowledgementHash.String != bodyHash {
			return Computer{}, protocolError(contract.ErrorIdempotencyConflict, "Computer grow acknowledgement conflicts with durable outcome")
		}
		return computer, tx.Commit()
	}
	if computer.IntentRevision != row.OperationRevision || computer.ReconfigurationPhase != ComputerReconfigurationGrowing ||
		computer.ReconfigurationRevision == nil || *computer.ReconfigurationRevision != row.OperationRevision ||
		computer.StorageID != row.StorageID || computer.StorageGeneration != row.StorageGeneration ||
		computer.DesiredDiskBytes != row.OldDiskBytes {
		return Computer{}, protocolError(contract.ErrorStaleIntentRevision, "Computer grow no longer owns the current Storage budget")
	}
	status, failure := "applied", ""
	if request.Receipt.Applied {
		if _, err := tx.ExecContext(ctx, `UPDATE computers SET desired_disk_bytes=? WHERE computer_id=?`,
			row.NewDiskBytes, computerID); err != nil {
			return Computer{}, internalError(err, "publish Computer disk budget")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE computer_storage_generations SET disk_bytes=?
			WHERE computer_id=? AND storage_generation=? AND phase='current'`, row.NewDiskBytes,
			computerID, row.StorageGeneration); err != nil {
			return Computer{}, internalError(err, "publish grown Storage allocation")
		}
	} else {
		status, failure = "failed", request.Receipt.FailureCode
		latchedFailure, marshalErr := json.Marshal(contract.SpawnFailure{
			Code: contract.SpawnFailureInsufficientDisk, Message: "Computer disk grow capacity was refused",
			NodeID: row.BoundNodeID, RequestedBytes: row.NewDiskBytes,
			ObservedAvailableBytes: request.Receipt.ObservedAvailableBytes,
		})
		if marshalErr != nil {
			return Computer{}, internalError(marshalErr, "encode Computer grow capacity failure")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE service_jobs SET last_failure=?, next_restart_at=NULL
			WHERE job_id=?`, latchedFailure, row.JobID); err != nil {
			return Computer{}, internalError(err, "record Computer grow capacity failure")
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE computer_storage_grows SET status=?, failure_code=?, receipt_json=?,
		receipt_hash=?, acknowledgement_key=?, acknowledgement_hash=?, completed_ns=?
		WHERE computer_id=? AND operation_revision=? AND status='planned'`, status, failure, body,
		bodyHash, request.IdempotencyKey, bodyHash, now.UnixNano(), computerID, row.OperationRevision); err != nil {
		return Computer{}, internalError(err, "record Computer grow outcome")
	}
	result, err := tx.ExecContext(ctx, `UPDATE computers SET applied_revision=?, reconfiguration_phase='stable',
		reconfiguration_revision=NULL, updated_ns=? WHERE computer_id=? AND intent_revision=?
		AND reconfiguration_phase='growing'`, row.OperationRevision, now.UnixNano(), computerID, row.OperationRevision)
	if err != nil {
		return Computer{}, internalError(err, "complete Computer grow")
	}
	if err := requireComputerCAS(result, computerID, row.OperationRevision); err != nil {
		return Computer{}, err
	}
	updated, err := readComputerAuthority(ctx, tx, computerID, now)
	if err != nil {
		return Computer{}, err
	}
	if err := tx.Commit(); err != nil {
		return Computer{}, internalError(err, "commit Computer grow acknowledgement")
	}
	s.notifyComputerPolicyChanged()
	return updated, nil
}
