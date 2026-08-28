package l1

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

type ComputerStorageGenerationPhase string

const (
	ComputerStorageGenerationCurrent ComputerStorageGenerationPhase = "current"
	ComputerStorageGenerationStaging ComputerStorageGenerationPhase = "staging"
	ComputerStorageGenerationRetired ComputerStorageGenerationPhase = "retired"
)

type ComputerStorageGeneration struct {
	StorageID         string                         `json:"storage_id"`
	StorageGeneration int64                          `json:"storage_generation"`
	DiskBytes         int64                          `json:"disk_bytes"`
	Phase             ComputerStorageGenerationPhase `json:"phase"`
	ResetRevision     *int64                         `json:"reset_revision,omitempty"`
	CreatedAt         time.Time                      `json:"created_at"`
	RetiredAt         *time.Time                     `json:"retired_at,omitempty"`
}

type ComputerStorageGenerationList struct {
	Generations []ComputerStorageGeneration `json:"generations"`
}

type ComputerStorageResetRequest struct {
	ComputerMutationPrecondition
	IdempotencyKey string `json:"idempotency_key"`
}

// ComputerStorageResetDirective is standing node-scoped authority to erase
// exactly one retired-on-success generation. The operation fence is distinct
// from Storage, removal, attempt, and node authority generations.
type ComputerStorageResetDirective struct {
	ComputerID     string `json:"computer_id"`
	BoundNodeID    string `json:"bound_node_id"`
	JobID          string `json:"job_id"`
	IntentRevision int64  `json:"intent_revision"`
	StorageID      string `json:"storage_id"`
	OldGeneration  int64  `json:"old_generation"`
	NewGeneration  int64  `json:"new_generation"`
	DiskBytes      int64  `json:"disk_bytes"`
	CleanupFence   string `json:"cleanup_fence"`
}

type ComputerStorageResetReceipt struct {
	Kind             string `json:"kind"`
	ReceiptID        string `json:"receipt_id"`
	ComputerID       string `json:"computer_id"`
	StorageID        string `json:"storage_id"`
	OldGeneration    int64  `json:"old_generation"`
	NewGeneration    int64  `json:"new_generation"`
	NodeID           string `json:"node_id"`
	JobID            string `json:"job_id"`
	IntentRevision   int64  `json:"intent_revision"`
	CleanupFence     string `json:"cleanup_fence"`
	HelperGeneration uint64 `json:"helper_generation"`
}

const computerStorageResetReceiptKind = "computer_storage_reset_verified"

type ComputerStorageResetAcknowledgementRequest struct {
	NodeID         string                      `json:"node_id"`
	BootSessionID  string                      `json:"boot_session_id"`
	IdempotencyKey string                      `json:"idempotency_key"`
	Receipt        ComputerStorageResetReceipt `json:"receipt"`
}

type computerStorageResetRow struct {
	ComputerID          string
	IntentRevision      int64
	StorageID           string
	OldGeneration       int64
	NewGeneration       int64
	DiskBytes           int64
	BoundNodeID         string
	JobID               string
	CleanupFence        string
	IdempotencyKey      string
	RequestHash         string
	Status              string
	ReceiptJSON         []byte
	ReceiptHash         sql.NullString
	AcknowledgementKey  sql.NullString
	AcknowledgementHash sql.NullString
}

func computerStorageResetRequestHash(request ComputerStorageResetRequest) (string, error) {
	payload, err := json.Marshal(struct {
		IntentRevision    int64  `json:"intent_revision"`
		StorageID         string `json:"storage_id"`
		StorageGeneration int64  `json:"storage_generation"`
		Actor             string `json:"actor"`
		IdempotencyKey    string `json:"idempotency_key"`
	}{request.IntentRevision, request.StorageID, request.StorageGeneration, request.Actor, request.IdempotencyKey})
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(payload)
	return hex.EncodeToString(hash[:]), nil
}

func readComputerStorageResetByKey(ctx context.Context, q queryer, computerID, key string) (computerStorageResetRow, error) {
	var row computerStorageResetRow
	err := q.QueryRowContext(ctx, `SELECT computer_id, intent_revision, storage_id, old_generation,
		new_generation, disk_bytes, bound_node_id, job_id, cleanup_fence, idempotency_key,
		request_hash, status, verification_receipt_json, verification_receipt_hash,
		acknowledgement_key, acknowledgement_hash
		FROM computer_storage_resets WHERE computer_id=? AND idempotency_key=?`, computerID, key).Scan(
		&row.ComputerID, &row.IntentRevision, &row.StorageID, &row.OldGeneration,
		&row.NewGeneration, &row.DiskBytes, &row.BoundNodeID, &row.JobID, &row.CleanupFence,
		&row.IdempotencyKey, &row.RequestHash, &row.Status, &row.ReceiptJSON, &row.ReceiptHash,
		&row.AcknowledgementKey, &row.AcknowledgementHash)
	return row, err
}

func (s *Store) BeginComputerStorageReset(ctx context.Context, computerID string, request ComputerStorageResetRequest) (Computer, bool, error) {
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	request.Actor = strings.TrimSpace(request.Actor)
	if strings.TrimSpace(computerID) == "" || request.IdempotencyKey == "" {
		return Computer{}, false, protocolError(contract.ErrorInvalidRequest, "computer_id and idempotency_key are required")
	}
	requestHash, err := computerStorageResetRequestHash(request)
	if err != nil {
		return Computer{}, false, internalError(err, "encode Computer Storage reset request")
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Computer{}, false, internalError(err, "begin Computer Storage reset")
	}
	defer tx.Rollback()
	computer, err := readComputerAuthority(ctx, tx, computerID, now)
	if errors.Is(err, sql.ErrNoRows) {
		return Computer{}, false, protocolError(contract.ErrorNotFound, "Computer %q was not found", computerID)
	}
	if err != nil {
		return Computer{}, false, internalError(err, "read Computer Storage reset target")
	}
	if replay, replayErr := readComputerStorageResetByKey(ctx, tx, computerID, request.IdempotencyKey); replayErr == nil {
		if replay.RequestHash != requestHash {
			return Computer{}, false, protocolError(contract.ErrorConflict, "Computer Storage reset idempotency key was reused with different authority")
		}
		if err := tx.Commit(); err != nil {
			return Computer{}, false, internalError(err, "commit Computer Storage reset replay")
		}
		return computer, true, nil
	} else if !errors.Is(replayErr, sql.ErrNoRows) {
		return Computer{}, false, internalError(replayErr, "read Computer Storage reset replay")
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
	if computer.StorageGeneration == math.MaxInt64 {
		return Computer{}, false, protocolError(contract.ErrorConflict, "Computer %q exhausted Storage generation space", computerID)
	}
	nextRevision := computer.IntentRevision + 1
	nextGeneration := computer.StorageGeneration + 1
	boundNodeID := computer.BoundNodeID
	if boundNodeID == "" {
		boundNodeID = computer.PlacementNodeID
	}
	cleanupFence := newID("storage-reset")
	result, err := tx.ExecContext(ctx, `UPDATE computers SET intent_revision=?, reconfiguration_phase=?,
		reconfiguration_revision=?, updated_ns=? WHERE computer_id=? AND intent_revision=?`,
		nextRevision, ComputerReconfigurationResetting, nextRevision, now.UnixNano(), computerID, computer.IntentRevision)
	if err != nil {
		return Computer{}, false, internalError(err, "reserve Computer Storage reset")
	}
	if err := requireComputerCAS(result, computerID, computer.IntentRevision); err != nil {
		return Computer{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_storage_generations(
		computer_id, storage_id, storage_generation, disk_bytes, phase, reset_revision, created_ns
	) VALUES(?, ?, ?, ?, 'staging', ?, ?)`, computerID, computer.StorageID, nextGeneration,
		computer.DesiredDiskBytes, nextRevision, now.UnixNano()); err != nil {
		return Computer{}, false, internalError(err, "stage replacement Computer Storage generation")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_storage_resets(
		computer_id, intent_revision, storage_id, old_generation, new_generation, disk_bytes,
		bound_node_id, job_id, cleanup_fence, idempotency_key, request_hash, status, requested_ns
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'reserved', ?)`, computerID, nextRevision,
		computer.StorageID, computer.StorageGeneration, nextGeneration, computer.DesiredDiskBytes,
		boundNodeID, computer.CurrentJobID, cleanupFence, request.IdempotencyKey, requestHash, now.UnixNano()); err != nil {
		return Computer{}, false, internalError(err, "persist Computer Storage reset intent")
	}
	if err := insertComputerIntent(ctx, tx, computerID, nextRevision, ComputerIntentReset,
		computer.DesiredState, computer.StorageID, nextGeneration, computer.CurrentJobID,
		computer.CurrentSpecRevision, request.Actor, now); err != nil {
		return Computer{}, false, err
	}
	if _, err := quiesceComputerProjectionTx(ctx, tx, computer.CurrentJob, now); err != nil {
		return Computer{}, false, err
	}
	updated, err := readComputerAuthority(ctx, tx, computerID, now)
	if err != nil {
		return Computer{}, false, internalError(err, "read reserved Computer Storage reset")
	}
	if err := tx.Commit(); err != nil {
		return Computer{}, false, internalError(err, "commit Computer Storage reset reservation")
	}
	return updated, false, nil
}

func (s *Store) ListComputerStorageGenerations(ctx context.Context, computerID string) (ComputerStorageGenerationList, error) {
	if strings.TrimSpace(computerID) == "" {
		return ComputerStorageGenerationList{}, protocolError(contract.ErrorInvalidRequest, "computer_id is required")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT storage_id, storage_generation, disk_bytes, phase,
		reset_revision, created_ns, retired_ns FROM computer_storage_generations
		WHERE computer_id=? ORDER BY storage_generation`, computerID)
	if err != nil {
		return ComputerStorageGenerationList{}, internalError(err, "list Computer Storage generations")
	}
	defer rows.Close()
	result := ComputerStorageGenerationList{Generations: []ComputerStorageGeneration{}}
	for rows.Next() {
		var generation ComputerStorageGeneration
		var resetRevision, retiredNS sql.NullInt64
		var createdNS int64
		if err := rows.Scan(&generation.StorageID, &generation.StorageGeneration, &generation.DiskBytes,
			&generation.Phase, &resetRevision, &createdNS, &retiredNS); err != nil {
			return ComputerStorageGenerationList{}, internalError(err, "scan Computer Storage generation")
		}
		generation.CreatedAt = time.Unix(0, createdNS).UTC()
		if resetRevision.Valid {
			value := resetRevision.Int64
			generation.ResetRevision = &value
		}
		if retiredNS.Valid {
			value := time.Unix(0, retiredNS.Int64).UTC()
			generation.RetiredAt = &value
		}
		result.Generations = append(result.Generations, generation)
	}
	if err := rows.Err(); err != nil {
		return ComputerStorageGenerationList{}, internalError(err, "iterate Computer Storage generations")
	}
	if len(result.Generations) == 0 {
		var exists bool
		if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM computers WHERE computer_id=?)`, computerID).Scan(&exists); err != nil {
			return ComputerStorageGenerationList{}, internalError(err, "read Computer Storage authority")
		}
		if !exists {
			return ComputerStorageGenerationList{}, protocolError(contract.ErrorNotFound, "Computer %q was not found", computerID)
		}
	}
	return result, nil
}

func validateStorageResetNode(ctx context.Context, q queryer, identityNodeID, nodeID, bootSessionID string) error {
	var storedIdentity, storedBoot string
	err := q.QueryRowContext(ctx, `SELECT identity_node_id, boot_session_id FROM nodes WHERE node_id=?`, nodeID).Scan(&storedIdentity, &storedBoot)
	if errors.Is(err, sql.ErrNoRows) {
		return protocolError(contract.ErrorNodeNotRegistered, "node %q is not registered", nodeID)
	}
	if err != nil {
		return internalError(err, "read Computer Storage reset node")
	}
	if storedIdentity != identityNodeID {
		return protocolError(contract.ErrorIdentityBound, "stable node %q is bound to another Fabric identity", nodeID)
	}
	if storedBoot != bootSessionID {
		return protocolError(contract.ErrorNodeSessionReplaced, "node %q boot session has been replaced", nodeID)
	}
	return nil
}

func (s *Store) ListNodeComputerStorageResetDirectives(ctx context.Context, identityNodeID, nodeID, bootSessionID string) ([]ComputerStorageResetDirective, error) {
	if err := validateStorageResetNode(ctx, s.db, identityNodeID, nodeID, bootSessionID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT computer_id, bound_node_id, job_id, intent_revision,
		storage_id, old_generation, new_generation, disk_bytes, cleanup_fence
		FROM computer_storage_resets WHERE bound_node_id=? AND status IN ('reserved', 'verified')
		ORDER BY requested_ns, computer_id`, nodeID)
	if err != nil {
		return nil, internalError(err, "list Computer Storage reset directives")
	}
	defer rows.Close()
	directives := []ComputerStorageResetDirective{}
	for rows.Next() {
		var directive ComputerStorageResetDirective
		if err := rows.Scan(&directive.ComputerID, &directive.BoundNodeID, &directive.JobID,
			&directive.IntentRevision, &directive.StorageID, &directive.OldGeneration,
			&directive.NewGeneration, &directive.DiskBytes, &directive.CleanupFence); err != nil {
			return nil, internalError(err, "scan Computer Storage reset directive")
		}
		directives = append(directives, directive)
	}
	if err := rows.Err(); err != nil {
		return nil, internalError(err, "iterate Computer Storage reset directives")
	}
	return directives, nil
}

func storageResetAcknowledgementHash(request ComputerStorageResetAcknowledgementRequest) (string, []byte, error) {
	payload, err := json.Marshal(struct {
		IdempotencyKey string                      `json:"idempotency_key"`
		Receipt        ComputerStorageResetReceipt `json:"receipt"`
	}{request.IdempotencyKey, request.Receipt})
	if err != nil {
		return "", nil, err
	}
	hash := sha256.Sum256(payload)
	return hex.EncodeToString(hash[:]), payload, nil
}

func validateStorageResetReceipt(directive computerStorageResetRow, receipt ComputerStorageResetReceipt) error {
	if receipt.Kind != computerStorageResetReceiptKind || strings.TrimSpace(receipt.ReceiptID) == "" ||
		receipt.HelperGeneration == 0 || receipt.ComputerID != directive.ComputerID ||
		receipt.StorageID != directive.StorageID || receipt.OldGeneration != directive.OldGeneration ||
		receipt.NewGeneration != directive.NewGeneration || receipt.NodeID != directive.BoundNodeID ||
		receipt.JobID != directive.JobID || receipt.IntentRevision != directive.IntentRevision ||
		receipt.CleanupFence != directive.CleanupFence {
		return protocolError(contract.ErrorConflict, "Computer Storage reset receipt does not match current reset authority")
	}
	return nil
}

// recordComputerStorageResetVerification commits the positive helper receipt
// without publishing the new generation. Keeping this boundary separate makes
// a crash resume from durable verification instead of deleting old bytes twice.
func (s *Store) recordComputerStorageResetVerification(ctx context.Context, identityNodeID, computerID string, request ComputerStorageResetAcknowledgementRequest) error {
	if strings.TrimSpace(computerID) == "" || strings.TrimSpace(request.NodeID) == "" ||
		strings.TrimSpace(request.BootSessionID) == "" || strings.TrimSpace(request.IdempotencyKey) == "" {
		return protocolError(contract.ErrorInvalidRequest, "complete Computer Storage reset acknowledgement fields are required")
	}
	ackHash, receiptJSON, err := storageResetAcknowledgementHash(request)
	if err != nil {
		return internalError(err, "encode Computer Storage reset acknowledgement")
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return internalError(err, "begin Computer Storage reset verification")
	}
	defer tx.Rollback()
	if err := validateStorageResetNode(ctx, tx, identityNodeID, request.NodeID, request.BootSessionID); err != nil {
		return err
	}
	var reset computerStorageResetRow
	err = tx.QueryRowContext(ctx, `SELECT computer_id, intent_revision, storage_id, old_generation,
		new_generation, disk_bytes, bound_node_id, job_id, cleanup_fence, idempotency_key,
		request_hash, status, verification_receipt_json, verification_receipt_hash,
		acknowledgement_key, acknowledgement_hash FROM computer_storage_resets
		WHERE computer_id=? AND intent_revision=?`, computerID, request.Receipt.IntentRevision).Scan(
		&reset.ComputerID, &reset.IntentRevision, &reset.StorageID, &reset.OldGeneration,
		&reset.NewGeneration, &reset.DiskBytes, &reset.BoundNodeID, &reset.JobID, &reset.CleanupFence,
		&reset.IdempotencyKey, &reset.RequestHash, &reset.Status, &reset.ReceiptJSON, &reset.ReceiptHash,
		&reset.AcknowledgementKey, &reset.AcknowledgementHash)
	if errors.Is(err, sql.ErrNoRows) {
		return protocolError(contract.ErrorStaleIntentRevision, "Computer Storage reset revision is no longer current")
	}
	if err != nil {
		return internalError(err, "read Computer Storage reset verification authority")
	}
	if err := validateStorageResetReceipt(reset, request.Receipt); err != nil {
		return err
	}
	if reset.Status != "reserved" {
		if !reset.AcknowledgementKey.Valid || !reset.AcknowledgementHash.Valid ||
			reset.AcknowledgementKey.String != request.IdempotencyKey || reset.AcknowledgementHash.String != ackHash {
			return protocolError(contract.ErrorConflict, "Computer Storage reset acknowledgement conflicts with durable verification")
		}
		return tx.Commit()
	}
	result, err := tx.ExecContext(ctx, `UPDATE computer_storage_resets SET status='verified',
		verification_receipt_json=?, verification_receipt_hash=?, acknowledgement_key=?,
		acknowledgement_hash=?, verified_ns=? WHERE computer_id=? AND intent_revision=? AND status='reserved'`,
		receiptJSON, ackHash, request.IdempotencyKey, ackHash, now.UnixNano(), computerID, reset.IntentRevision)
	if err != nil {
		return internalError(err, "record Computer Storage reset verification")
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return internalError(err, "record Computer Storage reset verification")
	}
	if affected != 1 {
		return protocolError(contract.ErrorConflict, "Computer Storage reset verification affected %d rows", affected)
	}
	if err := tx.Commit(); err != nil {
		return internalError(err, "commit Computer Storage reset verification")
	}
	return nil
}

func (s *Store) publishVerifiedComputerStorageReset(ctx context.Context, computerID string, intentRevision int64) (Computer, error) {
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Computer{}, internalError(err, "begin Computer Storage reset publication")
	}
	defer tx.Rollback()
	computer, err := readComputerAuthority(ctx, tx, computerID, now)
	if err != nil {
		return Computer{}, internalError(err, "read Computer Storage reset publication authority")
	}
	var reset computerStorageResetRow
	err = tx.QueryRowContext(ctx, `SELECT computer_id, intent_revision, storage_id, old_generation,
		new_generation, disk_bytes, bound_node_id, job_id, cleanup_fence, idempotency_key,
		request_hash, status, verification_receipt_json, verification_receipt_hash,
		acknowledgement_key, acknowledgement_hash FROM computer_storage_resets
		WHERE computer_id=? AND intent_revision=?`, computerID, intentRevision).Scan(
		&reset.ComputerID, &reset.IntentRevision, &reset.StorageID, &reset.OldGeneration,
		&reset.NewGeneration, &reset.DiskBytes, &reset.BoundNodeID, &reset.JobID, &reset.CleanupFence,
		&reset.IdempotencyKey, &reset.RequestHash, &reset.Status, &reset.ReceiptJSON, &reset.ReceiptHash,
		&reset.AcknowledgementKey, &reset.AcknowledgementHash)
	if err != nil {
		return Computer{}, internalError(err, "read verified Computer Storage reset")
	}
	if reset.Status == "published" {
		if err := tx.Commit(); err != nil {
			return Computer{}, internalError(err, "commit Computer Storage reset publication replay")
		}
		return computer, nil
	}
	if reset.Status != "verified" || !reset.ReceiptHash.Valid || len(reset.ReceiptJSON) == 0 {
		return Computer{}, protocolError(contract.ErrorConflict, "Computer Storage reset has no positive deletion verification")
	}
	if computer.IntentRevision != intentRevision || computer.StorageID != reset.StorageID ||
		computer.StorageGeneration != reset.OldGeneration || computer.ReconfigurationPhase != ComputerReconfigurationResetting ||
		computer.ReconfigurationRevision == nil || *computer.ReconfigurationRevision != intentRevision {
		return Computer{}, protocolError(contract.ErrorStaleIntentRevision, "Computer Storage reset no longer owns publication")
	}
	if computer.CurrentJob.State != contract.JobStopped && computer.CurrentJob.State != contract.JobFailed {
		return Computer{}, protocolError(contract.ErrorConflict, "Computer runtime has not positively quiesced for Storage reset")
	}
	if computer.DesiredState == contract.ServiceDesiredRunning && !computer.CurrentJob.HoldsSlot(computer.CurrentJob.State) {
		if err := ensureBoundServiceCapacity(ctx, tx, computer.CurrentJob); err != nil {
			return Computer{}, err
		}
	}
	retired, err := tx.ExecContext(ctx, `UPDATE computer_storage_generations SET phase='retired', retired_ns=?
		WHERE computer_id=? AND storage_generation=? AND phase='current'`, now.UnixNano(), computerID, reset.OldGeneration)
	if err != nil {
		return Computer{}, internalError(err, "retire old Computer Storage generation")
	}
	if err := requireSingleStorageGenerationMutation(retired, "retire current Computer Storage generation"); err != nil {
		return Computer{}, err
	}
	published, err := tx.ExecContext(ctx, `UPDATE computer_storage_generations SET phase='current'
		WHERE computer_id=? AND storage_generation=? AND phase='staging' AND reset_revision=?`,
		computerID, reset.NewGeneration, intentRevision)
	if err != nil {
		return Computer{}, internalError(err, "publish replacement Computer Storage generation")
	}
	if err := requireSingleStorageGenerationMutation(published, "publish replacement Computer Storage generation"); err != nil {
		return Computer{}, err
	}
	nextState := contract.JobStopped
	nextDesired := contract.ServiceDesiredStopped
	if computer.DesiredState == contract.ServiceDesiredRunning {
		nextState = contract.JobQueued
		nextDesired = contract.ServiceDesiredRunning
	}
	if _, err := tx.ExecContext(ctx, `UPDATE service_jobs SET desired_state=?, restart_streak=0,
		next_restart_at=NULL, last_failure=NULL, healthy_since_ns=NULL, published_attempt_id=NULL
		WHERE job_id=?`, nextDesired, computer.CurrentJobID); err != nil {
		return Computer{}, internalError(err, "restore Computer service intent after Storage reset")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET state=?, updated_ns=? WHERE job_id=?`, nextState, now.UnixNano(), computer.CurrentJobID); err != nil {
		return Computer{}, internalError(err, "queue Computer after Storage reset")
	}
	result, err := tx.ExecContext(ctx, `UPDATE computers SET storage_generation=?, applied_revision=?,
		reconfiguration_phase=?, reconfiguration_revision=NULL, updated_ns=?
		WHERE computer_id=? AND intent_revision=? AND storage_generation=? AND reconfiguration_phase=?`,
		reset.NewGeneration, intentRevision, ComputerReconfigurationStable, now.UnixNano(), computerID,
		intentRevision, reset.OldGeneration, ComputerReconfigurationResetting)
	if err != nil {
		return Computer{}, internalError(err, "publish Computer Storage reset authority")
	}
	if err := requireComputerCAS(result, computerID, intentRevision); err != nil {
		return Computer{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE computer_storage_resets SET status='published', published_ns=?
		WHERE computer_id=? AND intent_revision=? AND status='verified'`, now.UnixNano(), computerID, intentRevision); err != nil {
		return Computer{}, internalError(err, "complete Computer Storage reset publication")
	}
	updated, err := readComputerAuthority(ctx, tx, computerID, now)
	if err != nil {
		return Computer{}, internalError(err, "read published Computer Storage reset")
	}
	if err := tx.Commit(); err != nil {
		return Computer{}, internalError(err, "commit Computer Storage reset publication")
	}
	return updated, nil
}

func requireSingleStorageGenerationMutation(result sql.Result, action string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return internalError(err, action)
	}
	if affected != 1 {
		return protocolError(contract.ErrorConflict, "%s affected %d rows", action, affected)
	}
	return nil
}

func (s *Store) AcknowledgeComputerStorageReset(ctx context.Context, identityNodeID, computerID string, request ComputerStorageResetAcknowledgementRequest) (Computer, error) {
	if err := s.recordComputerStorageResetVerification(ctx, identityNodeID, computerID, request); err != nil {
		return Computer{}, err
	}
	return s.publishVerifiedComputerStorageReset(ctx, computerID, request.Receipt.IntentRevision)
}
