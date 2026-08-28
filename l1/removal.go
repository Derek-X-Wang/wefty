package l1

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

type serviceRemovalRow struct {
	boundNodeID         string
	generation          uint64
	cleanupFence        string
	rootInstanceID      string
	status              contract.JobState
	requestedAt         time.Time
	acknowledgedAt      *time.Time
	removedAt           *time.Time
	outcome             ServiceRemovalOutcome
	acknowledgementKey  sql.NullString
	acknowledgementHash sql.NullString
}

// InitialServiceRemovalGeneration is the first and currently only removal
// generation for an immutable service Job.
const InitialServiceRemovalGeneration uint64 = 1

type serviceTombstoneRow struct {
	jobID                 string
	dispatchKeyHash       string
	requestHash           string
	createdAt             time.Time
	removalRequestedAt    time.Time
	removedAt             time.Time
	outcome               ServiceRemovalOutcome
	lastBoundNodeID       string
	removalGeneration     uint64
	rootInstanceID        string
	cleanupAcknowledgedAt *time.Time
}

// RemoveService irreversibly revokes a service and scrubs its secret-bearing
// controller state in one transaction. A service that has never been bound is
// finalized immediately because no node could have created a managed resource.
func (s *Store) RemoveService(ctx context.Context, jobID string) (Job, error) {
	if strings.TrimSpace(jobID) == "" {
		return Job{}, protocolError(contract.ErrorInvalidRequest, "job_id is required")
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, internalError(err, "begin service removal")
	}
	defer tx.Rollback()

	var dispatchKey, requestHash string
	var specJSON []byte
	var createdNS int64
	var boundNodeID, rootInstanceID sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT jobs.dispatch_key, jobs.request_hash, jobs.spec_json, jobs.created_ns,
		service_jobs.bound_node_id, nodes.root_instance_id
		FROM jobs
		JOIN service_jobs ON service_jobs.job_id=jobs.job_id
		LEFT JOIN nodes ON nodes.node_id=service_jobs.bound_node_id
		WHERE jobs.job_id=?`, jobID).Scan(&dispatchKey, &requestHash, &specJSON, &createdNS, &boundNodeID, &rootInstanceID)
	if errors.Is(err, sql.ErrNoRows) {
		tombstone, tombstoneErr := readServiceTombstoneByID(ctx, tx, jobID)
		if errors.Is(tombstoneErr, sql.ErrNoRows) {
			return Job{}, protocolError(contract.ErrorNotFound, "service job %q was not found", jobID)
		}
		if tombstoneErr != nil {
			return Job{}, internalError(tombstoneErr, "read removed service")
		}
		return tombstone.job(), nil
	}
	if err != nil {
		return Job{}, internalError(err, "read service removal target")
	}
	if computerID, mapped, mapErr := computerIDForJob(ctx, tx, jobID); mapErr != nil {
		return Job{}, mapErr
	} else if mapped {
		return Job{}, protocolErrorWithDetails(contract.ErrorComputerResourceRequired,
			map[string]any{"computer_id": computerID},
			"Computer %q is the sole removal authority for Job %q", computerID, jobID)
	}

	if removal, removalErr := readServiceRemoval(ctx, tx, jobID); removalErr == nil {
		job, readErr := getJobByID(ctx, tx, jobID, now)
		if readErr != nil {
			return Job{}, internalError(readErr, "read pending service removal")
		}
		applyServiceRemoval(&job, removal)
		if err := tx.Commit(); err != nil {
			return Job{}, internalError(err, "commit idempotent service removal")
		}
		if err := s.checkpointSecretWAL(ctx); err != nil {
			return Job{}, err
		}
		return job, nil
	} else if !errors.Is(removalErr, sql.ErrNoRows) {
		return Job{}, internalError(removalErr, "read pending service removal")
	}

	scrubbedSpec, err := scrubSensitiveSpec(specJSON)
	if err != nil {
		return Job{}, internalError(err, "scrub service specification")
	}
	if err := scrubServiceControllerState(ctx, tx, jobID, scrubbedSpec, now); err != nil {
		return Job{}, err
	}

	if !boundNodeID.Valid {
		tombstone := serviceTombstoneRow{
			jobID: jobID, dispatchKeyHash: hashDispatchKey(dispatchKey), requestHash: requestHash,
			createdAt: time.Unix(0, createdNS).UTC(), removalRequestedAt: now, removedAt: now,
			outcome: ServiceRemovalVerified, removalGeneration: InitialServiceRemovalGeneration,
		}
		if err := deleteServiceRows(ctx, tx, jobID); err != nil {
			return Job{}, err
		}
		if err := insertServiceTombstone(ctx, tx, tombstone); err != nil {
			return Job{}, err
		}
		if err := tx.Commit(); err != nil {
			return Job{}, internalError(err, "commit unbound service removal")
		}
		if err := s.checkpointSecretWAL(ctx); err != nil {
			return Job{}, err
		}
		return tombstone.job(), nil
	}
	if !rootInstanceID.Valid || strings.TrimSpace(rootInstanceID.String) == "" {
		return Job{}, protocolError(contract.ErrorConflict,
			"bound node %q has no registered managed-root instance", boundNodeID.String)
	}
	removal := serviceRemovalRow{
		boundNodeID: boundNodeID.String, generation: InitialServiceRemovalGeneration,
		cleanupFence: newID("cleanup"), rootInstanceID: rootInstanceID.String,
		status: contract.JobRemovalPending, requestedAt: now,
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO service_removals(
		job_id, bound_node_id, removal_generation, cleanup_fence, root_instance_id, status, requested_ns
	) VALUES(?, ?, ?, ?, ?, ?, ?)`, jobID, removal.boundNodeID, removal.generation,
		removal.cleanupFence, removal.rootInstanceID, removal.status, now.UnixNano()); err != nil {
		return Job{}, internalError(err, "create durable service removal")
	}
	job, err := getJobByID(ctx, tx, jobID, now)
	if err != nil {
		return Job{}, internalError(err, "read requested service removal")
	}
	applyServiceRemoval(&job, removal)
	if err := tx.Commit(); err != nil {
		return Job{}, internalError(err, "commit service removal")
	}
	if err := s.checkpointSecretWAL(ctx); err != nil {
		return Job{}, err
	}
	return job, nil
}

// ForceForgetService waives cleanup proof without cancelling the durable
// directive. The active rows remain until a returning node actually cleans and
// acknowledges; the tombstone's force-forgotten outcome never changes.
func (s *Store) ForceForgetService(ctx context.Context, jobID string) (Job, error) {
	job, err := s.RemoveService(ctx, jobID)
	if err != nil {
		return Job{}, err
	}
	if job.State == contract.JobRemovedVerified || job.State == contract.JobForgottenCleanupUnverified {
		return job, nil
	}

	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, internalError(err, "begin forced service forget")
	}
	defer tx.Rollback()
	removal, err := readServiceRemoval(ctx, tx, jobID)
	if err != nil {
		return Job{}, internalError(err, "read forced service forget")
	}
	var dispatchKey, requestHash string
	var createdNS int64
	if err := tx.QueryRowContext(ctx, `SELECT dispatch_key, request_hash, created_ns FROM jobs WHERE job_id=?`, jobID).
		Scan(&dispatchKey, &requestHash, &createdNS); err != nil {
		return Job{}, internalError(err, "read forced service tombstone fields")
	}
	tombstone := serviceTombstoneRow{
		jobID: jobID, dispatchKeyHash: hashDispatchKey(dispatchKey), requestHash: requestHash,
		createdAt: time.Unix(0, createdNS).UTC(), removalRequestedAt: removal.requestedAt, removedAt: now,
		outcome: ServiceRemovalForgotten, lastBoundNodeID: removal.boundNodeID,
		removalGeneration: removal.generation, rootInstanceID: removal.rootInstanceID,
		cleanupAcknowledgedAt: removal.acknowledgedAt,
	}
	if err := insertServiceTombstone(ctx, tx, tombstone); err != nil {
		return Job{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE service_removals SET status=?, removed_ns=? WHERE job_id=?`,
		contract.JobForgottenCleanupUnverified, now.UnixNano(), jobID); err != nil {
		return Job{}, internalError(err, "mark service cleanup unverified")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET state=?, updated_ns=? WHERE job_id=?`,
		contract.JobForgottenCleanupUnverified, now.UnixNano(), jobID); err != nil {
		return Job{}, internalError(err, "project forced service forget")
	}
	removal.status = contract.JobForgottenCleanupUnverified
	removal.removedAt = &now
	removal.outcome = ServiceRemovalForgotten
	forgotten, err := getJobByID(ctx, tx, jobID, now)
	if err != nil {
		return Job{}, internalError(err, "read forced service forget result")
	}
	applyServiceRemoval(&forgotten, removal)
	if err := tx.Commit(); err != nil {
		return Job{}, internalError(err, "commit forced service forget")
	}
	return forgotten, nil
}

// ListNodeRemovalDirectives returns the standing cleanup work carried on the
// current authenticated boot's heartbeat response.
func (s *Store) ListNodeRemovalDirectives(ctx context.Context, identityNodeID, nodeID, bootSessionID string) ([]RemovalDirective, error) {
	var storedIdentity, storedBoot string
	err := s.db.QueryRowContext(ctx, `SELECT identity_node_id, boot_session_id FROM nodes WHERE node_id=?`, nodeID).
		Scan(&storedIdentity, &storedBoot)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, protocolError(contract.ErrorNodeNotRegistered, "node %q is not registered", nodeID)
	}
	if err != nil {
		return nil, internalError(err, "read removal-directive node")
	}
	if storedIdentity != identityNodeID {
		return nil, protocolError(contract.ErrorIdentityBound, "stable node %q is bound to another Fabric identity", nodeID)
	}
	if storedBoot != bootSessionID {
		return nil, protocolError(contract.ErrorNodeSessionReplaced, "node %q boot session has been replaced", nodeID)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT service_removals.job_id, service_removals.bound_node_id,
		service_removals.removal_generation, service_removals.cleanup_fence, service_removals.root_instance_id,
		jobs.spec_json, computers.computer_id, computers.storage_id, computers.storage_generation
		FROM service_removals JOIN jobs ON jobs.job_id=service_removals.job_id
		LEFT JOIN computer_job_projections ON computer_job_projections.job_id=service_removals.job_id
		LEFT JOIN computers ON computers.computer_id=computer_job_projections.computer_id
		WHERE service_removals.bound_node_id=? AND service_removals.status IN (?, ?)
		ORDER BY service_removals.requested_ns, service_removals.job_id`, nodeID, contract.JobRemovalPending, contract.JobForgottenCleanupUnverified)
	if err != nil {
		return nil, internalError(err, "list service removal directives")
	}
	defer rows.Close()
	directives := []RemovalDirective{}
	for rows.Next() {
		var directive RemovalDirective
		var specJSON []byte
		var computerID, storageID sql.NullString
		var storageGeneration sql.NullInt64
		if err := rows.Scan(&directive.JobID, &directive.BoundNodeID, &directive.RemovalGeneration,
			&directive.CleanupFence, &directive.RootInstanceID, &specJSON, &computerID, &storageID, &storageGeneration); err != nil {
			return nil, internalError(err, "scan service removal directive")
		}
		var spec contract.JobSpec
		if err := json.Unmarshal(specJSON, &spec); err != nil {
			return nil, internalError(err, "decode service removal workload kind")
		}
		if spec.Kind != contract.JobKindProcess && spec.Kind != contract.JobKindOCI {
			return nil, internalError(fmt.Errorf("unsupported workload kind %q", spec.Kind), "decode service removal workload kind")
		}
		directive.Kind = spec.Kind
		if computerID.Valid || storageID.Valid || storageGeneration.Valid {
			if !computerID.Valid || !storageID.Valid || !storageGeneration.Valid || storageGeneration.Int64 < 1 {
				return nil, internalError(errors.New("partial Computer Storage removal identity"), "scan service removal directive")
			}
			directive.ComputerStorage = &ComputerStorageClaim{ComputerID: computerID.String, StorageID: storageID.String,
				StorageGeneration: storageGeneration.Int64}
			directive.ComputerStorageGenerations = &ComputerStorageGenerationClaims{Generations: []ComputerStorageGenerationClaim{}}
			generationRows, generationErr := s.db.QueryContext(ctx, `SELECT storage_id, storage_generation, disk_bytes
				FROM computer_storage_generations WHERE computer_id=? ORDER BY storage_generation`, computerID.String)
			if generationErr != nil {
				return nil, internalError(generationErr, "list Computer Storage removal generations")
			}
			for generationRows.Next() {
				var generation ComputerStorageGenerationClaim
				generation.ComputerID = computerID.String
				if err := generationRows.Scan(&generation.StorageID, &generation.StorageGeneration, &generation.DiskBytes); err != nil {
					generationRows.Close()
					return nil, internalError(err, "scan Computer Storage removal generation")
				}
				directive.ComputerStorageGenerations.Generations = append(directive.ComputerStorageGenerations.Generations, generation)
			}
			if err := generationRows.Close(); err != nil {
				return nil, internalError(err, "close Computer Storage removal generations")
			}
			if len(directive.ComputerStorageGenerations.Generations) == 0 {
				return nil, internalError(errors.New("Computer removal has no Storage generations"), "list Computer Storage removal generations")
			}
		}
		directives = append(directives, directive)
	}
	if err := rows.Err(); err != nil {
		return nil, internalError(err, "iterate service removal directives")
	}
	return directives, nil
}

// AcknowledgeServiceRemoval records a deletion attestation. It never performs
// filesystem inspection or deletion; the authenticated node asserts that
// those operations already completed.
func (s *Store) AcknowledgeServiceRemoval(ctx context.Context, identityNodeID, jobID string, request RemovalAcknowledgementRequest) (Job, error) {
	if strings.TrimSpace(jobID) == "" || strings.TrimSpace(request.NodeID) == "" ||
		strings.TrimSpace(request.BootSessionID) == "" || request.RemovalGeneration == 0 ||
		strings.TrimSpace(request.CleanupFence) == "" || strings.TrimSpace(request.RootInstanceID) == "" ||
		strings.TrimSpace(request.IdempotencyKey) == "" {
		return Job{}, protocolError(contract.ErrorInvalidRequest, "complete removal acknowledgement fields are required")
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return Job{}, internalError(err, "encode removal acknowledgement")
	}
	hash := sha256.Sum256(payload)
	bodyHash := hex.EncodeToString(hash[:])
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, internalError(err, "begin removal acknowledgement")
	}
	defer tx.Rollback()

	removal, err := readServiceRemoval(ctx, tx, jobID)
	if errors.Is(err, sql.ErrNoRows) {
		tombstone, tombstoneErr := readServiceTombstoneByID(ctx, tx, jobID)
		if errors.Is(tombstoneErr, sql.ErrNoRows) {
			return Job{}, protocolError(contract.ErrorNotFound, "service removal %q was not found", jobID)
		}
		if tombstoneErr != nil {
			return Job{}, internalError(tombstoneErr, "read finalized removal acknowledgement")
		}
		if err := validateFinalizedAcknowledgement(ctx, tx, identityNodeID, request, tombstone); err != nil {
			return Job{}, err
		}
		// Job IDs are random 128-bit values. Once this exact tombstone exists,
		// acknowledgement replay cannot refer to a different mutable service.
		return tombstone.job(), nil
	}
	if err != nil {
		return Job{}, internalError(err, "read service removal acknowledgement")
	}
	if removal.status == contract.JobRemovedVerified {
		// Computer removals retain their immutable Job and removal row instead
		// of becoming an ordinary-service tombstone. Preserve acknowledgement
		// replay across a later boot exactly as the tombstone path does, while
		// still requiring the same stable Fabric identity and accepted body.
		var storedIdentity string
		if err := tx.QueryRowContext(ctx, `SELECT identity_node_id FROM nodes WHERE node_id=?`, removal.boundNodeID).
			Scan(&storedIdentity); err != nil {
			return Job{}, internalError(err, "read finalized Computer acknowledgement node")
		}
		if storedIdentity != identityNodeID || request.NodeID != removal.boundNodeID {
			return Job{}, protocolError(contract.ErrorAttemptNotOwned,
				"authenticated node does not own this finalized Computer removal")
		}
		if request.RemovalGeneration != removal.generation || request.RootInstanceID != removal.rootInstanceID ||
			!removal.acknowledgementKey.Valid || removal.acknowledgementKey.String != request.IdempotencyKey ||
			!removal.acknowledgementHash.Valid || removal.acknowledgementHash.String != bodyHash {
			return Job{}, protocolError(contract.ErrorConflict,
				"finalized Computer removal acknowledgement does not match the accepted request")
		}
		job, err := getJobByID(ctx, tx, jobID, now)
		if err != nil {
			return Job{}, internalError(err, "read finalized Computer acknowledgement replay")
		}
		return job, nil
	}
	if err := validateMutableAcknowledgement(ctx, tx, identityNodeID, request, removal); err != nil {
		return Job{}, err
	}
	if removal.acknowledgementKey.Valid {
		if removal.acknowledgementKey.String != request.IdempotencyKey ||
			!removal.acknowledgementHash.Valid || removal.acknowledgementHash.String != bodyHash {
			return Job{}, protocolError(contract.ErrorConflict, "removal acknowledgement replay does not match the accepted request")
		}
		job, err := getJobByID(ctx, tx, jobID, now)
		if err != nil {
			return Job{}, internalError(err, "read replayed removal acknowledgement")
		}
		applyServiceRemoval(&job, removal)
		return job, nil
	}

	nextStatus := contract.JobAgentCleaned
	if removal.status == contract.JobForgottenCleanupUnverified {
		nextStatus = contract.JobForgottenCleanupUnverified
	}
	if _, err := tx.ExecContext(ctx, `UPDATE service_removals SET status=?, cleanup_acknowledgement_key=?,
		cleanup_acknowledgement_hash=?, agent_cleaned_ns=? WHERE job_id=?`, nextStatus,
		request.IdempotencyKey, bodyHash, now.UnixNano(), jobID); err != nil {
		return Job{}, internalError(err, "persist removal acknowledgement")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET state=?, updated_ns=? WHERE job_id=?`, nextStatus, now.UnixNano(), jobID); err != nil {
		return Job{}, internalError(err, "project removal acknowledgement")
	}
	removal.status = nextStatus
	removal.acknowledgementKey = sql.NullString{String: request.IdempotencyKey, Valid: true}
	removal.acknowledgementHash = sql.NullString{String: bodyHash, Valid: true}
	removal.acknowledgedAt = &now
	job, err := getJobByID(ctx, tx, jobID, now)
	if err != nil {
		return Job{}, internalError(err, "read accepted removal acknowledgement")
	}
	applyServiceRemoval(&job, removal)
	if err := tx.Commit(); err != nil {
		return Job{}, internalError(err, "commit removal acknowledgement")
	}
	return job, nil
}

// FinalizeServiceRemoval performs phase four in a transaction separate from
// acknowledgement. Reconcile calls the same helper after a crash between the
// two commits.
func (s *Store) FinalizeServiceRemoval(ctx context.Context, jobID string) (Job, bool, error) {
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, false, internalError(err, "begin service removal finalization")
	}
	defer tx.Rollback()
	job, finalized, err := finalizeServiceRemovalTx(ctx, tx, jobID, now)
	if err != nil {
		return Job{}, false, err
	}
	if !finalized {
		return job, false, nil
	}
	if err := tx.Commit(); err != nil {
		return Job{}, false, internalError(err, "commit service removal finalization")
	}
	return job, true, nil
}

func finalizeServiceRemovalTx(ctx context.Context, tx *sql.Tx, jobID string, now time.Time) (Job, bool, error) {
	removal, err := readServiceRemoval(ctx, tx, jobID)
	if errors.Is(err, sql.ErrNoRows) {
		tombstone, tombstoneErr := readServiceTombstoneByID(ctx, tx, jobID)
		if tombstoneErr == nil {
			return tombstone.job(), false, nil
		}
		if errors.Is(tombstoneErr, sql.ErrNoRows) {
			return Job{}, false, protocolError(contract.ErrorNotFound, "service removal %q was not found", jobID)
		}
		return Job{}, false, internalError(tombstoneErr, "read finalized service removal")
	}
	if err != nil {
		return Job{}, false, internalError(err, "read service removal finalization")
	}
	eligible := removal.status == contract.JobAgentCleaned ||
		(removal.status == contract.JobForgottenCleanupUnverified && removal.acknowledgedAt != nil)
	if !eligible {
		job, readErr := getJobByID(ctx, tx, jobID, now)
		if readErr != nil {
			return Job{}, false, internalError(readErr, "read unfinalized service removal")
		}
		applyServiceRemoval(&job, removal)
		return job, false, nil
	}
	if computerID, mapped, mapErr := computerIDForJob(ctx, tx, jobID); mapErr != nil {
		return Job{}, false, mapErr
	} else if mapped {
		// A Computer keeps its immutable Job evidence and durable authority row.
		// The ordinary removal directive still owns agent cleanup and Slot
		// release; finalization records the terminal observation in place rather
		// than deleting the Job into an ordinary-service tombstone.
		if _, err := tx.ExecContext(ctx, `UPDATE service_removals SET status=?, removed_ns=? WHERE job_id=?`,
			contract.JobRemovedVerified, now.UnixNano(), jobID); err != nil {
			return Job{}, false, internalError(err, "finalize Computer removal directive")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE jobs SET state=?, updated_ns=? WHERE job_id=?`,
			contract.JobRemovedVerified, now.UnixNano(), jobID); err != nil {
			return Job{}, false, internalError(err, "project verified Computer removal")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE service_jobs SET desired_state=?, published_attempt_id=NULL,
			healthy_since_ns=NULL, next_restart_at=NULL WHERE job_id=?`, contract.ServiceDesiredStopped, jobID); err != nil {
			return Job{}, false, internalError(err, "release verified Computer removal Slot")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE computers SET updated_ns=? WHERE computer_id=?`,
			now.UnixNano(), computerID); err != nil {
			return Job{}, false, internalError(err, "timestamp verified Computer removal")
		}
		job, readErr := getJobByID(ctx, tx, jobID, now)
		if readErr != nil {
			return Job{}, false, internalError(readErr, "read finalized Computer removal")
		}
		return job, true, nil
	}

	var dispatchKey, requestHash string
	var createdNS int64
	if err := tx.QueryRowContext(ctx, `SELECT dispatch_key, request_hash, created_ns FROM jobs WHERE job_id=?`, jobID).
		Scan(&dispatchKey, &requestHash, &createdNS); err != nil {
		return Job{}, false, internalError(err, "read removal tombstone source")
	}
	tombstone, tombstoneErr := readServiceTombstoneByID(ctx, tx, jobID)
	insertTombstone := false
	if errors.Is(tombstoneErr, sql.ErrNoRows) {
		tombstone = serviceTombstoneRow{
			jobID: jobID, dispatchKeyHash: hashDispatchKey(dispatchKey), requestHash: requestHash,
			createdAt: time.Unix(0, createdNS).UTC(), removalRequestedAt: removal.requestedAt,
			removedAt: now, outcome: ServiceRemovalVerified, lastBoundNodeID: removal.boundNodeID,
			removalGeneration: removal.generation, rootInstanceID: removal.rootInstanceID,
			cleanupAcknowledgedAt: removal.acknowledgedAt,
		}
		insertTombstone = true
	} else if tombstoneErr != nil {
		return Job{}, false, internalError(tombstoneErr, "read force-forgotten tombstone")
	} else {
		// A late cleanup after force-forget records the fact but never upgrades
		// the permanent unverified outcome.
		tombstone.cleanupAcknowledgedAt = removal.acknowledgedAt
	}
	if err := deleteServiceRows(ctx, tx, jobID); err != nil {
		return Job{}, false, err
	}
	if insertTombstone {
		if err := insertServiceTombstone(ctx, tx, tombstone); err != nil {
			return Job{}, false, err
		}
	} else if _, err := tx.ExecContext(ctx, `UPDATE service_tombstones SET cleanup_acknowledged_ns=? WHERE job_id=?`,
		removal.acknowledgedAt.UnixNano(), jobID); err != nil {
		return Job{}, false, internalError(err, "record forgotten-service cleanup acknowledgement")
	}
	return tombstone.job(), true, nil
}

func scrubServiceControllerState(ctx context.Context, tx *sql.Tx, jobID string, specJSON []byte, now time.Time) error {
	if _, err := tx.ExecContext(ctx, `UPDATE attempts SET state=?, lease_expires_ns=MIN(lease_expires_ns, ?), updated_ns=?
		WHERE job_id=? AND state IN (?, ?, ?)`, contract.AttemptLost, now.UnixNano(), now.UnixNano(), jobID,
		contract.AttemptClaimed, contract.AttemptRunning, contract.AttemptAwaitingInput); err != nil {
		return internalError(err, "invalidate service attempt authority")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE service_jobs SET published_attempt_id=NULL, healthy_since_ns=NULL WHERE job_id=?`, jobID); err != nil {
		return internalError(err, "withdraw service publication authority")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET spec_json=?, state=?, fence_counter=fence_counter+1, updated_ns=? WHERE job_id=?`,
		specJSON, contract.JobRemovalPending, now.UnixNano(), jobID); err != nil {
		return internalError(err, "make service removal intent irreversible")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM log_events WHERE job_id=?`, jobID); err != nil {
		return internalError(err, "scrub service log events")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM service_log_truncations WHERE job_id=?`, jobID); err != nil {
		return internalError(err, "scrub service log truncation")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE job_log_jsonl SET jsonl=X'' WHERE job_id=?`, jobID); err != nil {
		return internalError(err, "scrub authoritative service log")
	}
	return nil
}

func scrubSensitiveSpec(encoded []byte) ([]byte, error) {
	var spec contract.JobSpec
	if err := json.Unmarshal(encoded, &spec); err != nil {
		return nil, err
	}
	spec.Execution.SensitiveEnv = nil
	return json.Marshal(spec)
}

func deleteServiceRows(ctx context.Context, tx *sql.Tx, jobID string) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM attempts WHERE job_id=?`, jobID); err != nil {
		return internalError(err, "delete removed service attempts")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM jobs WHERE job_id=?`, jobID); err != nil {
		return internalError(err, "delete removed service job")
	}
	return nil
}

func (s *Store) checkpointSecretWAL(ctx context.Context) error {
	for {
		var busy, logFrames, checkpointedFrames int
		err := s.db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &logFrames, &checkpointedFrames)
		if err != nil {
			return internalError(err, "truncate secret-bearing SQLite WAL")
		}
		if busy == 0 {
			return nil
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return internalError(ctx.Err(), fmt.Sprintf("retry SQLite WAL truncation (%d/%d frames)", checkpointedFrames, logFrames))
		case <-timer.C:
		}
	}
}

func readServiceRemoval(ctx context.Context, q queryer, jobID string) (serviceRemovalRow, error) {
	var row serviceRemovalRow
	var requestedNS int64
	var acknowledgedNS, removedNS sql.NullInt64
	var outcome sql.NullString
	err := q.QueryRowContext(ctx, `SELECT service_removals.bound_node_id, service_removals.removal_generation,
		service_removals.cleanup_fence, service_removals.root_instance_id, service_removals.status,
		service_removals.requested_ns, service_removals.cleanup_acknowledgement_key,
		service_removals.cleanup_acknowledgement_hash, service_removals.agent_cleaned_ns,
		service_removals.removed_ns, service_tombstones.outcome
		FROM service_removals
		LEFT JOIN service_tombstones ON service_tombstones.job_id=service_removals.job_id
		WHERE service_removals.job_id=?`, jobID).Scan(&row.boundNodeID, &row.generation,
		&row.cleanupFence, &row.rootInstanceID, &row.status, &requestedNS,
		&row.acknowledgementKey, &row.acknowledgementHash, &acknowledgedNS, &removedNS, &outcome)
	if err != nil {
		return serviceRemovalRow{}, err
	}
	row.requestedAt = time.Unix(0, requestedNS).UTC()
	if acknowledgedNS.Valid {
		value := time.Unix(0, acknowledgedNS.Int64).UTC()
		row.acknowledgedAt = &value
	}
	if removedNS.Valid {
		value := time.Unix(0, removedNS.Int64).UTC()
		row.removedAt = &value
	}
	if outcome.Valid {
		row.outcome = ServiceRemovalOutcome(outcome.String)
	}
	return row, nil
}

func applyServiceRemoval(job *Job, removal serviceRemovalRow) {
	job.State = removal.status
	job.NodeID = ""
	job.CurrentAttemptID = ""
	job.ServiceJob = nil
	job.Removal = &ServiceRemoval{
		RemovalDesiredState: contract.ServiceDesiredRemoved, RemovalBoundNodeID: removal.boundNodeID,
		RemovalGeneration: removal.generation, RemovalRequestedAt: removal.requestedAt,
		RemovalOutcome: removal.outcome, RemovedAt: removal.removedAt,
		CleanupAcknowledgedAt: removal.acknowledgedAt,
	}
}

func insertServiceTombstone(ctx context.Context, tx *sql.Tx, tombstone serviceTombstoneRow) error {
	var acknowledged any
	if tombstone.cleanupAcknowledgedAt != nil {
		acknowledged = tombstone.cleanupAcknowledgedAt.UnixNano()
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO service_tombstones(
		job_id, dispatch_key_hash, request_hash, created_ns, removal_requested_ns, removed_ns,
		outcome, last_bound_node_id, removal_generation, root_instance_id, cleanup_acknowledged_ns
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, tombstone.jobID, tombstone.dispatchKeyHash,
		tombstone.requestHash, tombstone.createdAt.UnixNano(), tombstone.removalRequestedAt.UnixNano(),
		tombstone.removedAt.UnixNano(), tombstone.outcome, tombstone.lastBoundNodeID,
		tombstone.removalGeneration, tombstone.rootInstanceID, acknowledged); err != nil {
		return internalError(err, "insert service tombstone")
	}
	return nil
}

func readServiceTombstoneByID(ctx context.Context, q queryer, jobID string) (serviceTombstoneRow, error) {
	return readServiceTombstone(ctx, q, `job_id=?`, jobID)
}

func readServiceTombstoneByDispatchHash(ctx context.Context, q queryer, dispatchKeyHash string) (serviceTombstoneRow, error) {
	return readServiceTombstone(ctx, q, `dispatch_key_hash=?`, dispatchKeyHash)
}

func readServiceTombstone(ctx context.Context, q queryer, predicate string, value any) (serviceTombstoneRow, error) {
	var row serviceTombstoneRow
	var createdNS, requestedNS, removedNS int64
	var acknowledgedNS sql.NullInt64
	err := q.QueryRowContext(ctx, `SELECT job_id, dispatch_key_hash, request_hash, created_ns,
		removal_requested_ns, removed_ns, outcome, last_bound_node_id, removal_generation,
		root_instance_id, cleanup_acknowledged_ns FROM service_tombstones WHERE `+predicate, value).
		Scan(&row.jobID, &row.dispatchKeyHash, &row.requestHash, &createdNS, &requestedNS, &removedNS,
			&row.outcome, &row.lastBoundNodeID, &row.removalGeneration, &row.rootInstanceID, &acknowledgedNS)
	if err != nil {
		return serviceTombstoneRow{}, err
	}
	row.createdAt = time.Unix(0, createdNS).UTC()
	row.removalRequestedAt = time.Unix(0, requestedNS).UTC()
	row.removedAt = time.Unix(0, removedNS).UTC()
	if acknowledgedNS.Valid {
		value := time.Unix(0, acknowledgedNS.Int64).UTC()
		row.cleanupAcknowledgedAt = &value
	}
	return row, nil
}

func (row serviceTombstoneRow) job() Job {
	state := contract.JobRemovedVerified
	if row.outcome == ServiceRemovalForgotten {
		state = contract.JobForgottenCleanupUnverified
	}
	removedAt := row.removedAt
	return Job{
		JobID: row.jobID, State: state, CreatedAt: row.createdAt, UpdatedAt: row.removedAt,
		Removal: &ServiceRemoval{
			RemovalDesiredState: contract.ServiceDesiredRemoved, RemovalBoundNodeID: row.lastBoundNodeID,
			RemovalGeneration: row.removalGeneration, RemovalRequestedAt: row.removalRequestedAt,
			RemovalOutcome: row.outcome, RemovedAt: &removedAt,
			CleanupAcknowledgedAt: row.cleanupAcknowledgedAt,
		},
	}
}

func validateMutableAcknowledgement(ctx context.Context, q queryer, identityNodeID string, request RemovalAcknowledgementRequest, removal serviceRemovalRow) error {
	if removal.status != contract.JobRemovalPending && removal.status != contract.JobAgentCleaned &&
		removal.status != contract.JobForgottenCleanupUnverified {
		return protocolError(contract.ErrorConflict, "service removal state %q does not accept acknowledgement", removal.status)
	}
	var storedIdentity, currentBoot, currentRootInstance string
	if err := q.QueryRowContext(ctx, `SELECT identity_node_id, boot_session_id, root_instance_id FROM nodes WHERE node_id=?`, removal.boundNodeID).
		Scan(&storedIdentity, &currentBoot, &currentRootInstance); err != nil {
		return internalError(err, "read removal acknowledgement node")
	}
	if storedIdentity != identityNodeID || request.NodeID != removal.boundNodeID {
		return protocolError(contract.ErrorAttemptNotOwned, "authenticated node does not own this service removal")
	}
	if request.BootSessionID != currentBoot {
		return protocolError(contract.ErrorNodeSessionReplaced, "node %q boot session has been replaced", removal.boundNodeID)
	}
	if request.RemovalGeneration != removal.generation || request.CleanupFence != removal.cleanupFence ||
		request.RootInstanceID != removal.rootInstanceID || currentRootInstance != removal.rootInstanceID {
		return protocolError(contract.ErrorStaleFence, "service removal authority does not match the standing directive")
	}
	return nil
}

func validateFinalizedAcknowledgement(ctx context.Context, q queryer, identityNodeID string, request RemovalAcknowledgementRequest, tombstone serviceTombstoneRow) error {
	var storedIdentity string
	if err := q.QueryRowContext(ctx, `SELECT identity_node_id FROM nodes WHERE node_id=?`, tombstone.lastBoundNodeID).
		Scan(&storedIdentity); errors.Is(err, sql.ErrNoRows) {
		return protocolError(contract.ErrorNodeNotRegistered, "node %q is not registered", tombstone.lastBoundNodeID)
	} else if err != nil {
		return internalError(err, "read finalized acknowledgement node")
	}
	if storedIdentity != identityNodeID || request.NodeID != tombstone.lastBoundNodeID {
		return protocolError(contract.ErrorAttemptNotOwned, "authenticated node does not own this finalized removal")
	}
	if request.RemovalGeneration != tombstone.removalGeneration || request.RootInstanceID != tombstone.rootInstanceID {
		return protocolError(contract.ErrorStaleFence, "finalized removal acknowledgement does not match the tombstone")
	}
	return nil
}

func hashDispatchKey(dispatchKey string) string {
	digest := sha256.Sum256([]byte(dispatchKey))
	return hex.EncodeToString(digest[:])
}
