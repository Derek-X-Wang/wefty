package agent

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

type runtimeRemovalPhase string

const (
	runtimeRemovalPrepared    runtimeRemovalPhase = "prepared"
	runtimeRemovalQuarantined runtimeRemovalPhase = "quarantined"
	runtimeRemovalComplete    runtimeRemovalPhase = "complete"
)

type runtimeRemovalCheckpoint string

const (
	runtimeRemovalCheckpointAfterManifest   runtimeRemovalCheckpoint = "after-manifest"
	runtimeRemovalCheckpointAfterQuiescence runtimeRemovalCheckpoint = "after-quiescence"
	runtimeRemovalCheckpointAfterComplete   runtimeRemovalCheckpoint = "after-complete"
)

type runtimeRemovalManifest struct {
	Version           int                                      `json:"version"`
	JobID             string                                   `json:"job_id"`
	RemovalGeneration uint64                                   `json:"removal_generation"`
	Attempts          []workloadrunner.RuntimeResourceManifest `json:"attempts"`
}

type runtimeRemovalRecord struct {
	removal     localRemoval
	manifest    runtimeRemovalManifest
	receipt     workloadrunner.ReapReceipt
	attestation workloadrunner.RuntimeRemovalAttestation
	phase       runtimeRemovalPhase
	// invalidReason is set only on the read path, and only for a durable row
	// this agent cannot validate. Such a row is never acted on, but it is
	// still listed: an operator whose Computer sits in removal_pending needs
	// the broken record to be the thing they can see, not the thing that
	// blanks the whole read verb.
	invalidReason string
	preparedAt    time.Time
	quiescedAt    *time.Time
	attestedAt    *time.Time
	completedAt   *time.Time
	// failedAttempts and the refusal it last carried are the only durable
	// record that a removal is not merely slow. Nothing before #450 counted
	// them, so a removal repeating one refusal every heartbeat looked exactly
	// like one that had never been tried.
	failedAttempts    int
	lastRefusalCode   string
	lastRefusalDetail string
	lastAttemptedAt   *time.Time
	// stallDeclaration is the exact bytes of the declaration this agent sent
	// or is about to send, frozen before the first send. A lost response is
	// indistinguishable from a refusal, so the only safe retry is the same
	// declaration again -- rebuilding it would advance the attempt count and
	// turn an accepted declaration into a permanent idempotency conflict.
	stallDeclaration    []byte
	stallDeclarationKey string
	stallDeclaredAt     *time.Time
}

func (spool *logSpool) storeRuntimeResourceManifest(ctx context.Context, manifest workloadrunner.RuntimeResourceManifest, createdAt time.Time) error {
	if err := validateRuntimeResourceManifest(manifest); err != nil {
		return err
	}
	payload, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("agent: encode runtime attempt manifest: %w", err)
	}
	tx, err := spool.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("agent: begin runtime attempt manifest persistence: %w", err)
	}
	defer tx.Rollback()
	var removalStarted bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM spool_removals WHERE job_id=?)`, manifest.JobID).Scan(&removalStarted); err != nil {
		return fmt.Errorf("agent: inspect removal intent before runtime attempt manifest: %w", err)
	}
	if removalStarted {
		return fmt.Errorf("agent: service %q removal already started; refuse a new runtime attempt manifest", manifest.JobID)
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO runtime_attempt_manifests(
attempt_id, job_id, runtime_kind, removal_generation, manifest_json, created_ns
) VALUES(?, ?, ?, ?, ?, ?)
ON CONFLICT(attempt_id) DO NOTHING`, manifest.AttemptID, manifest.JobID, manifest.RuntimeKind,
		manifest.RemovalGeneration, payload, createdAt.UTC().Round(0).UnixNano())
	if err != nil {
		return fmt.Errorf("agent: persist runtime attempt manifest: %w", err)
	}
	if _, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("agent: inspect runtime attempt manifest persistence: %w", err)
	}
	var storedJobID, storedKind, storedGeneration string
	var storedPayload []byte
	if err := tx.QueryRowContext(ctx, `SELECT job_id, runtime_kind, removal_generation, manifest_json
FROM runtime_attempt_manifests WHERE attempt_id=?`, manifest.AttemptID).
		Scan(&storedJobID, &storedKind, &storedGeneration, &storedPayload); err != nil {
		return fmt.Errorf("agent: verify runtime attempt manifest: %w", err)
	}
	if storedJobID != manifest.JobID || storedKind != manifest.RuntimeKind || storedGeneration != manifest.RemovalGeneration || !bytes.Equal(storedPayload, payload) {
		return fmt.Errorf("agent: runtime attempt manifest %q conflicts with its immutable identity", manifest.AttemptID)
	}
	createdNS := createdAt.UTC().Round(0).UnixNano()
	if _, err := tx.ExecContext(ctx, `INSERT INTO runtime_service_manifests(
job_id, attempt_id, removal_generation, manifest_json, created_ns
) VALUES(?, ?, ?, ?, ?)
ON CONFLICT(job_id) DO UPDATE SET
  attempt_id=excluded.attempt_id,
  removal_generation=excluded.removal_generation,
  manifest_json=excluded.manifest_json,
  created_ns=excluded.created_ns
WHERE excluded.created_ns >= runtime_service_manifests.created_ns`, manifest.JobID, manifest.AttemptID,
		manifest.RemovalGeneration, payload, createdNS); err != nil {
		return fmt.Errorf("agent: persist current runtime service manifest: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("agent: commit runtime attempt manifest: %w", err)
	}
	return nil
}

func validateRuntimeResourceManifest(manifest workloadrunner.RuntimeResourceManifest) error {
	if manifest.Version != 1 || manifest.RuntimeKind != contract.JobKindOCI ||
		strings.TrimSpace(manifest.NodeID) == "" || strings.TrimSpace(manifest.BootSessionID) == "" ||
		strings.TrimSpace(manifest.JobID) == "" || strings.TrimSpace(manifest.AttemptID) == "" ||
		strings.TrimSpace(manifest.FencingToken) == "" || manifest.WorkloadClass != contract.JobClassService ||
		strings.TrimSpace(manifest.RemovalGeneration) == "" {
		return errors.New("agent: runtime attempt manifest is incomplete")
	}
	if manifest.StorageOnly {
		if manifest.ComputerStorage == nil {
			return errors.New("agent: Storage-only removal manifest lacks durable preparation evidence")
		}
		if manifest.StorageAbsent {
			if manifest.StoragePreparation != nil || !contract.ValidStorageAbsentRemovalAttemptID(manifest.AttemptID, manifest.ComputerStorage.StorageGeneration) {
				return errors.New("agent: already-absent Storage manifest lacks typed helper evidence")
			}
		} else if manifest.StoragePreparation == nil || !manifest.StoragePreparation.Valid() ||
			!contract.ValidStorageOnlyRemovalAttemptID(manifest.AttemptID, manifest.ComputerStorage.StorageGeneration) ||
			manifest.StoragePreparation.NodeID != manifest.NodeID || manifest.StoragePreparation.JobID != manifest.JobID ||
			manifest.StoragePreparation.ComputerID != manifest.ComputerStorage.ComputerID ||
			manifest.StoragePreparation.StorageID != manifest.ComputerStorage.StorageID ||
			manifest.StoragePreparation.StorageGeneration != manifest.ComputerStorage.StorageGeneration {
			return errors.New("agent: Storage-only removal manifest lacks durable preparation evidence")
		}
	} else if manifest.StorageAbsent || strings.TrimSpace(manifest.LeaseID) == "" || strings.TrimSpace(manifest.TaskID) == "" ||
		strings.TrimSpace(manifest.ContainerID) == "" || strings.TrimSpace(manifest.SnapshotID) == "" ||
		strings.TrimSpace(manifest.ShimID) == "" || strings.TrimSpace(manifest.CgroupID) == "" ||
		strings.TrimSpace(manifest.LogSegmentDirectory) == "" {
		return errors.New("agent: runtime attempt manifest is incomplete")
	}
	hasServiceData := strings.TrimSpace(manifest.ServiceDataVolume) != "" || strings.TrimSpace(manifest.ServiceDataOwnerRecord) != ""
	hasComputerDisk := manifest.ComputerStorage != nil
	if hasServiceData == hasComputerDisk {
		return errors.New("agent: runtime attempt manifest must name exactly one durable service-data class")
	}
	if hasServiceData && (strings.TrimSpace(manifest.ServiceDataVolume) == "" || strings.TrimSpace(manifest.ServiceDataOwnerRecord) == "") {
		return errors.New("agent: runtime attempt manifest has incomplete service-data identity")
	}
	if hasComputerDisk && !validComputerStorage(manifest.ComputerStorage) {
		return errors.New("agent: runtime attempt manifest has incomplete Computer Storage identity")
	}
	return nil
}

func validComputerStorage(storage *workloadrunner.ComputerStorage) bool {
	return storage != nil && strings.TrimSpace(storage.ComputerID) != "" && strings.TrimSpace(storage.StorageID) != "" &&
		storage.StorageGeneration > 0 && storage.DiskBytes > 0
}

func (spool *logSpool) freezeRuntimeRemoval(ctx context.Context, tx *sql.Tx, removal localRemoval, preparedAt time.Time) (bool, error) {
	rows, err := tx.QueryContext(ctx, `SELECT manifest_json FROM runtime_service_manifests
WHERE job_id=?`, removal.jobID)
	if err != nil {
		return false, fmt.Errorf("agent: list runtime attempt manifests for removal: %w", err)
	}
	var attempts []workloadrunner.RuntimeResourceManifest
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			_ = rows.Close()
			return false, fmt.Errorf("agent: scan runtime attempt manifest for removal: %w", err)
		}
		var manifest workloadrunner.RuntimeResourceManifest
		if err := json.Unmarshal(payload, &manifest); err != nil || validateRuntimeResourceManifest(manifest) != nil {
			_ = rows.Close()
			return false, fmt.Errorf("agent: runtime attempt manifest for job %q is corrupt", removal.jobID)
		}
		generation, err := strconv.ParseUint(manifest.RemovalGeneration, 10, 64)
		if err != nil || manifest.JobID != removal.jobID || generation != removal.generation {
			_ = rows.Close()
			return false, fmt.Errorf("agent: runtime attempt manifest for job %q does not match removal generation %d", removal.jobID, removal.generation)
		}
		attempts = append(attempts, manifest)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return false, fmt.Errorf("agent: iterate runtime attempt manifests for removal: %w", err)
	}
	if err := rows.Close(); err != nil {
		return false, fmt.Errorf("agent: close runtime attempt manifests for removal: %w", err)
	}
	if len(attempts) == 0 {
		return false, nil
	}
	manifest := runtimeRemovalManifest{Version: 1, JobID: removal.jobID, RemovalGeneration: removal.generation, Attempts: attempts}
	payload, err := json.Marshal(manifest)
	if err != nil {
		return false, fmt.Errorf("agent: encode frozen runtime removal manifest: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO runtime_removal_manifests(
job_id, removal_generation, cleanup_fence, root_instance_id, manifest_json, phase, prepared_ns
) VALUES(?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(job_id) DO NOTHING`, removal.jobID, removal.generation, removal.cleanupFence,
		removal.rootInstanceID, payload, runtimeRemovalPrepared, preparedAt.UTC().Round(0).UnixNano()); err != nil {
		return false, fmt.Errorf("agent: persist frozen runtime removal manifest: %w", err)
	}
	var storedGeneration uint64
	var storedFence, storedRoot string
	var storedPayload []byte
	if err := tx.QueryRowContext(ctx, `SELECT removal_generation, cleanup_fence, root_instance_id, manifest_json
FROM runtime_removal_manifests WHERE job_id=?`, removal.jobID).
		Scan(&storedGeneration, &storedFence, &storedRoot, &storedPayload); err != nil {
		return false, fmt.Errorf("agent: verify frozen runtime removal manifest: %w", err)
	}
	if storedGeneration != removal.generation || storedFence != removal.cleanupFence || storedRoot != removal.rootInstanceID || !bytes.Equal(storedPayload, payload) {
		return false, fmt.Errorf("agent: frozen runtime removal manifest for job %q conflicts with persisted authority", removal.jobID)
	}
	return true, nil
}

// storeReconstructedRuntimeRemoval freezes helper-observed legacy inventory
// before any reap or local/helper deletion. It uses the same immutable record
// and phase machine as manifests captured before Run.
func (spool *logSpool) storeReconstructedRuntimeRemoval(ctx context.Context, removal localRemoval, attempts []workloadrunner.RuntimeResourceManifest, preparedAt time.Time) error {
	if removal.kind != contract.JobKindOCI || len(attempts) == 0 {
		return errors.New("agent: reconstructed runtime removal requires OCI intent and helper-owned attempts")
	}
	slices.SortFunc(attempts, func(left, right workloadrunner.RuntimeResourceManifest) int {
		return strings.Compare(left.AttemptID, right.AttemptID)
	})
	manifest := runtimeRemovalManifest{Version: 1, JobID: removal.jobID, RemovalGeneration: removal.generation, Attempts: attempts}
	if !validRuntimeRemovalManifest(manifest) {
		return errors.New("agent: reconstructed runtime removal manifest is incomplete")
	}
	payload, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("agent: encode reconstructed runtime removal manifest: %w", err)
	}
	tx, err := spool.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("agent: begin reconstructed runtime removal persistence: %w", err)
	}
	defer tx.Rollback()
	var storedKind string
	var storedGeneration uint64
	var storedFence, storedRoot string
	if err := tx.QueryRowContext(ctx, `SELECT runtime_kind, removal_generation, cleanup_fence, root_instance_id
FROM spool_removals WHERE job_id=?`, removal.jobID).Scan(&storedKind, &storedGeneration, &storedFence, &storedRoot); err != nil {
		return fmt.Errorf("agent: verify reconstructed removal intent: %w", err)
	}
	if storedKind != removal.kind || storedGeneration != removal.generation || storedFence != removal.cleanupFence || storedRoot != removal.rootInstanceID {
		return fmt.Errorf("agent: reconstructed runtime removal %q conflicts with durable authority", removal.jobID)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO runtime_removal_manifests(
job_id, removal_generation, cleanup_fence, root_instance_id, manifest_json, phase, prepared_ns
) VALUES(?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(job_id) DO NOTHING`, removal.jobID, removal.generation, removal.cleanupFence, removal.rootInstanceID,
		payload, runtimeRemovalPrepared, preparedAt.UTC().Round(0).UnixNano()); err != nil {
		return fmt.Errorf("agent: persist reconstructed runtime removal manifest: %w", err)
	}
	var persisted []byte
	var persistedPhase runtimeRemovalPhase
	if err := tx.QueryRowContext(ctx, `SELECT manifest_json, phase FROM runtime_removal_manifests
WHERE job_id=? AND removal_generation=? AND cleanup_fence=? AND root_instance_id=?`, removal.jobID,
		removal.generation, removal.cleanupFence, removal.rootInstanceID).Scan(&persisted, &persistedPhase); err != nil {
		return fmt.Errorf("agent: read reconstructed runtime removal manifest: %w", err)
	}
	if !bytes.Equal(persisted, payload) {
		var prior runtimeRemovalManifest
		if json.Unmarshal(persisted, &prior) != nil || persistedPhase != runtimeRemovalPrepared || !sameStorageOnlyInventory(prior, manifest) {
			return fmt.Errorf("agent: reconstructed runtime removal manifest for job %q conflicts with persisted inventory", removal.jobID)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE runtime_removal_manifests SET manifest_json=?, prepared_ns=?
WHERE job_id=? AND phase=?`, payload, preparedAt.UTC().Round(0).UnixNano(), removal.jobID, runtimeRemovalPrepared); err != nil {
			return fmt.Errorf("agent: refresh reconstructed runtime removal manifest: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("agent: commit reconstructed runtime removal manifest: %w", err)
	}
	if spool.runtimeRemovalCheckpoint != nil {
		if err := spool.runtimeRemovalCheckpoint(runtimeRemovalCheckpointAfterManifest); err != nil {
			return err
		}
	}
	return nil
}

func sameStorageOnlyInventory(left, right runtimeRemovalManifest) bool {
	if left.JobID != right.JobID || left.RemovalGeneration != right.RemovalGeneration || len(left.Attempts) != len(right.Attempts) {
		return false
	}
	for index := range left.Attempts {
		oldAttempt, newAttempt := left.Attempts[index], right.Attempts[index]
		if !oldAttempt.StorageOnly || !newAttempt.StorageOnly || oldAttempt.ComputerStorage == nil || newAttempt.ComputerStorage == nil ||
			oldAttempt.StorageAbsent != newAttempt.StorageAbsent || *oldAttempt.ComputerStorage != *newAttempt.ComputerStorage {
			return false
		}
		if oldAttempt.StorageAbsent {
			if oldAttempt.StoragePreparation != nil || newAttempt.StoragePreparation != nil {
				return false
			}
		} else if oldAttempt.StoragePreparation == nil || newAttempt.StoragePreparation == nil || *oldAttempt.StoragePreparation != *newAttempt.StoragePreparation {
			return false
		}
	}
	return true
}

func (spool *logSpool) runtimeRemoval(ctx context.Context, jobID string) (runtimeRemovalRecord, bool, error) {
	row := spool.db.QueryRowContext(ctx, `SELECT job_id, removal_generation, cleanup_fence, root_instance_id,
manifest_json, runtime_quiescence_json, absence_attestation_json, phase, prepared_ns, quiesced_ns, attested_ns, completed_ns,
failed_attempts, last_refusal_code, last_refusal_detail, last_attempted_ns, stall_declaration_json,
stall_declaration_key, stall_declared_ns
FROM runtime_removal_manifests WHERE job_id=?`, jobID)
	record, err := scanRuntimeRemoval(row)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimeRemovalRecord{}, false, nil
	}
	if err != nil {
		return runtimeRemovalRecord{}, false, fmt.Errorf("agent: read runtime removal manifest: %w", err)
	}
	return record, true, nil
}

type rowScanner interface {
	Scan(...any) error
}

func scanRuntimeRemoval(row rowScanner) (runtimeRemovalRecord, error) {
	var record runtimeRemovalRecord
	var manifestJSON, receiptJSON, attestationJSON []byte
	var preparedNS int64
	var quiescedNS, attestedNS, completedNS, lastAttemptedNS, stallDeclaredNS sql.NullInt64
	var lastRefusalCode, lastRefusalDetail, stallDeclarationKey sql.NullString
	// job_id is read from its own column rather than from the manifest it
	// indexes, so a row whose stored JSON no longer parses still has the one
	// identity an operator can act on.
	if err := row.Scan(&record.removal.jobID, &record.removal.generation, &record.removal.cleanupFence, &record.removal.rootInstanceID,
		&manifestJSON, &receiptJSON, &attestationJSON, &record.phase, &preparedNS, &quiescedNS, &attestedNS, &completedNS,
		&record.failedAttempts, &lastRefusalCode, &lastRefusalDetail, &lastAttemptedNS,
		&record.stallDeclaration, &stallDeclarationKey, &stallDeclaredNS); err != nil {
		return runtimeRemovalRecord{}, err
	}
	record.lastRefusalCode = lastRefusalCode.String
	record.lastRefusalDetail = lastRefusalDetail.String
	record.stallDeclarationKey = stallDeclarationKey.String
	if lastAttemptedNS.Valid {
		value := time.Unix(0, lastAttemptedNS.Int64).UTC()
		record.lastAttemptedAt = &value
	}
	if stallDeclaredNS.Valid {
		value := time.Unix(0, stallDeclaredNS.Int64).UTC()
		record.stallDeclaredAt = &value
	}
	record.removal.kind = contract.JobKindOCI
	record.preparedAt = time.Unix(0, preparedNS).UTC()
	if err := json.Unmarshal(manifestJSON, &record.manifest); err != nil || !validRuntimeRemovalManifest(record.manifest) {
		return record, errors.New("runtime removal manifest is unreadable_json")
	}
	if record.manifest.JobID != record.removal.jobID {
		return record, fmt.Errorf("runtime removal manifest names job %q, not the row it is stored under", record.manifest.JobID)
	}
	if len(receiptJSON) != 0 {
		if err := json.Unmarshal(receiptJSON, &record.receipt); err != nil {
			return record, errors.New("runtime quiescence receipt is unreadable_json")
		}
	}
	if len(attestationJSON) != 0 {
		if err := json.Unmarshal(attestationJSON, &record.attestation); err != nil {
			return record, errors.New("runtime absence attestation is unreadable_json")
		}
	}
	if quiescedNS.Valid {
		value := time.Unix(0, quiescedNS.Int64).UTC()
		record.quiescedAt = &value
	}
	if attestedNS.Valid {
		value := time.Unix(0, attestedNS.Int64).UTC()
		record.attestedAt = &value
	}
	if completedNS.Valid {
		value := time.Unix(0, completedNS.Int64).UTC()
		record.completedAt = &value
	}
	if err := validateRuntimeRemovalRecord(record); err != nil {
		return record, fmt.Errorf("runtime removal record is invalid: %w", err)
	}
	return record, nil
}

func validRuntimeRemovalManifest(manifest runtimeRemovalManifest) bool {
	if manifest.Version != 1 || manifest.JobID == "" || manifest.RemovalGeneration == 0 || len(manifest.Attempts) == 0 {
		return false
	}
	previousAttemptID := ""
	for _, attempt := range manifest.Attempts {
		generation, err := strconv.ParseUint(attempt.RemovalGeneration, 10, 64)
		if err != nil || validateRuntimeResourceManifest(attempt) != nil || attempt.JobID != manifest.JobID || generation != manifest.RemovalGeneration || attempt.AttemptID <= previousAttemptID {
			return false
		}
		previousAttemptID = attempt.AttemptID
	}
	return true
}

// validateRuntimeRemovalRecord names the exact field that makes a durable
// removal record unusable. It returns an error rather than a bool because a
// record that fails here stops a Computer removal for as long as the row
// exists: the operator reading `node oci removals`, and the agent log line that
// repeats every heartbeat, both need to say which field disagreed rather than
// only that something did.
func validateRuntimeRemovalRecord(record runtimeRemovalRecord) error {
	if record.removal.jobID == "" || record.removal.generation == 0 || record.removal.cleanupFence == "" || record.removal.rootInstanceID == "" ||
		record.manifest.JobID != record.removal.jobID || record.manifest.RemovalGeneration != record.removal.generation {
		return errors.New("removal identity (job_id/removal_generation/cleanup_fence/root_instance_id) does not match the frozen manifest")
	}
	for _, attempt := range record.manifest.Attempts {
		if !attempt.StorageOnly {
			continue
		}
		if attempt.FencingToken != record.removal.cleanupFence {
			return fmt.Errorf("Storage-only attempt %q field fencing_token does not carry the removal cleanup fence", attempt.AttemptID)
		}
		if attempt.StorageAbsent {
			if attempt.StoragePreparation != nil {
				return fmt.Errorf("already-absent Storage attempt %q field storage_preparation must be empty", attempt.AttemptID)
			}
		} else if attempt.StoragePreparation == nil || attempt.StoragePreparation.RootInstanceID != record.removal.rootInstanceID {
			return fmt.Errorf("Storage-only attempt %q field storage_preparation.root_instance_id does not match the removal root instance", attempt.AttemptID)
		}
	}
	// `no_runtime_resources` has two contract-legal producers, and the record
	// must accept both. The session records it for a complete Storage-only
	// generation inventory that proves no guardian exists; the OCI adapter
	// records it when image delivery failed before the helper `Run` RPC was
	// entered, and that manifest is an ordinary attempt manifest with runtime
	// identifiers that were reserved but never created. What binds either shape
	// is the boot session: a no-runtime receipt can only speak for attempts
	// frozen under the same boot.
	if record.receipt.Evidence == workloadrunner.ReapEvidenceNoRuntime {
		for _, attempt := range record.manifest.Attempts {
			if attempt.BootSessionID != record.receipt.BootSessionID {
				return fmt.Errorf("attempt %q field boot_session_id does not match the no_runtime_resources receipt boot session", attempt.AttemptID)
			}
		}
	}
	switch record.phase {
	case runtimeRemovalPrepared:
		if record.receipt.RuntimeQuiesced || record.receipt.Evidence != "" || record.attestation.Version != 0 ||
			record.quiescedAt != nil || record.attestedAt != nil || record.completedAt != nil {
			return errors.New("phase \"prepared\" carries quiescence, attestation or completion evidence")
		}
		return nil
	case runtimeRemovalQuarantined:
		if err := validateRuntimeReapReceipt(record.receipt); err != nil {
			return fmt.Errorf("phase \"quarantined\" field runtime_quiescence: %w", err)
		}
		if record.attestation.Version != 0 || record.quiescedAt == nil || record.attestedAt != nil || record.completedAt != nil {
			return errors.New("phase \"quarantined\" requires quiesced_ns alone, without attestation or completion")
		}
		return nil
	case runtimeRemovalComplete:
		if err := validateRuntimeReapReceipt(record.receipt); err != nil {
			return fmt.Errorf("phase \"complete\" field runtime_quiescence: %w", err)
		}
		if err := validateRuntimeRemovalAttestation(record.manifest, record.attestation); err != nil {
			return fmt.Errorf("phase \"complete\" field absence_attestation: %w", err)
		}
		if record.quiescedAt == nil || record.attestedAt == nil || record.completedAt == nil {
			return errors.New("phase \"complete\" requires quiesced_ns, attested_ns and completed_ns")
		}
		return nil
	default:
		return fmt.Errorf("field phase %q is unsupported", record.phase)
	}
}

// storageOnlyNoRuntimeReceipt reports whether a positive quiescence receipt is
// the Storage-only no-runtime proof -- the only one the contract lets skip the
// local managed service-resource deletion step. The adapter's never-entered-Run
// receipt carries the same evidence kind but leaves a prepared managed service
// directory behind, so it takes the ordinary deletion path.
func storageOnlyNoRuntimeReceipt(receipt workloadrunner.ReapReceipt, attempts []workloadrunner.RuntimeResourceManifest) bool {
	if receipt.Evidence != workloadrunner.ReapEvidenceNoRuntime || len(attempts) == 0 {
		return false
	}
	for _, attempt := range attempts {
		if !attempt.StorageOnly {
			return false
		}
	}
	return true
}

func (spool *logSpool) pendingRuntimeRemovals(ctx context.Context) ([]runtimeRemovalRecord, error) {
	rows, err := spool.db.QueryContext(ctx, `SELECT job_id, removal_generation, cleanup_fence, root_instance_id,
manifest_json, runtime_quiescence_json, absence_attestation_json, phase, prepared_ns, quiesced_ns, attested_ns, completed_ns,
failed_attempts, last_refusal_code, last_refusal_detail, last_attempted_ns, stall_declaration_json,
stall_declaration_key, stall_declared_ns
FROM runtime_removal_manifests WHERE phase IN (?, ?, ?) ORDER BY prepared_ns, job_id`,
		runtimeRemovalPrepared, runtimeRemovalQuarantined, runtimeRemovalComplete)
	if err != nil {
		return nil, fmt.Errorf("agent: list pending runtime removals: %w", err)
	}
	defer rows.Close()
	var records []runtimeRemovalRecord
	for rows.Next() {
		record, err := scanRuntimeRemoval(rows)
		if err != nil {
			// One unreadable row used to fail the whole listing, so the single
			// read verb that explains a stuck removal went red exactly when a
			// removal was stuck. Carry the reason on the row instead.
			if record.removal.jobID == "" {
				return nil, fmt.Errorf("agent: scan pending runtime removal: %w", err)
			}
			record.invalidReason = err.Error()
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("agent: iterate pending runtime removals: %w", err)
	}
	return records, nil
}

// recordRuntimeRemovalFailure counts one cleanup attempt that ended in a
// refusal. The streak resets whenever the refusal changes: three identical
// refusals are evidence that retrying cannot help, while three different ones
// are a removal still working through causes.
func (spool *logSpool) recordRuntimeRemovalFailure(ctx context.Context, removal localRemoval,
	refusalCode, refusalDetail string, observedAt time.Time) error {
	if strings.TrimSpace(refusalCode) == "" {
		return errors.New("agent: runtime removal failure requires a refusal code")
	}
	if len(refusalDetail) > l1.MaximumServiceRemovalStallDetail {
		refusalDetail = refusalDetail[:l1.MaximumServiceRemovalStallDetail]
	}
	result, err := spool.db.ExecContext(ctx, `UPDATE runtime_removal_manifests
SET failed_attempts=CASE WHEN last_refusal_code=? THEN failed_attempts+1 ELSE 1 END,
    last_refusal_code=?, last_refusal_detail=?, last_attempted_ns=?
WHERE job_id=? AND removal_generation=? AND cleanup_fence=? AND root_instance_id=?`,
		refusalCode, refusalCode, refusalDetail, observedAt.UTC().Round(0).UnixNano(), removal.jobID,
		removal.generation, removal.cleanupFence, removal.rootInstanceID)
	if err != nil {
		return fmt.Errorf("agent: persist runtime removal failure: %w", err)
	}
	// Stall accounting lives on the runtime removal record, which exists only
	// for a runtime removal. A removal with no such row -- a process service --
	// cannot be declared stalled, and that must be a visible refusal rather
	// than an UPDATE that quietly changes nothing.
	return oneRuntimeRemovalRowAffected(result, removal.jobID, "runtime removal failure")
}

// recordRuntimeRemovalUntypedFailure ends a typed streak without extending it.
// The bound asks for three consecutive attempts that produced the same typed
// refusal; an untyped failure observed nothing about the refusal, so it can
// neither confirm the streak nor be counted into it.
func (spool *logSpool) recordRuntimeRemovalUntypedFailure(ctx context.Context, removal localRemoval, observedAt time.Time) error {
	result, err := spool.db.ExecContext(ctx, `UPDATE runtime_removal_manifests
SET failed_attempts=0, last_refusal_code=NULL, last_refusal_detail=NULL, last_attempted_ns=?
WHERE job_id=? AND removal_generation=? AND cleanup_fence=? AND root_instance_id=?`,
		observedAt.UTC().Round(0).UnixNano(), removal.jobID, removal.generation, removal.cleanupFence,
		removal.rootInstanceID)
	if err != nil {
		return fmt.Errorf("agent: reset runtime removal refusal streak: %w", err)
	}
	return oneRuntimeRemovalRowAffected(result, removal.jobID, "runtime removal refusal streak reset")
}

// freezeRuntimeRemovalStallDeclaration stores the exact declaration bytes and
// key once, and returns whatever is durable afterwards. A second call returns
// the first call's bytes, so every retry -- including one from a later boot --
// sends the same declaration.
func (spool *logSpool) freezeRuntimeRemovalStallDeclaration(ctx context.Context, removal localRemoval,
	declaration []byte, key string) ([]byte, string, error) {
	if len(declaration) == 0 || strings.TrimSpace(key) == "" {
		return nil, "", errors.New("agent: a frozen stall declaration requires bytes and a key")
	}
	if _, err := spool.db.ExecContext(ctx, `UPDATE runtime_removal_manifests
SET stall_declaration_json=?, stall_declaration_key=?
WHERE job_id=? AND removal_generation=? AND cleanup_fence=? AND root_instance_id=? AND stall_declaration_json IS NULL`,
		declaration, key, removal.jobID, removal.generation, removal.cleanupFence, removal.rootInstanceID); err != nil {
		return nil, "", fmt.Errorf("agent: freeze runtime removal stall declaration: %w", err)
	}
	var frozen []byte
	var frozenKey sql.NullString
	if err := spool.db.QueryRowContext(ctx, `SELECT stall_declaration_json, stall_declaration_key
FROM runtime_removal_manifests WHERE job_id=?`, removal.jobID).Scan(&frozen, &frozenKey); err != nil {
		return nil, "", fmt.Errorf("agent: read frozen runtime removal stall declaration: %w", err)
	}
	if len(frozen) == 0 || !frozenKey.Valid || frozenKey.String == "" {
		return nil, "", fmt.Errorf("agent: runtime removal %q has no frozen stall declaration", removal.jobID)
	}
	return frozen, frozenKey.String, nil
}

// removalStartedAt is the immutable moment this node first accepted the
// deletion directive. The frozen manifest's prepared time is refreshed
// whenever a Storage-only inventory is reconstructed, so measuring the bound
// against it would let repeated restarts postpone a declaration forever.
func (spool *logSpool) removalStartedAt(ctx context.Context, jobID string) (time.Time, error) {
	var startedNS int64
	if err := spool.db.QueryRowContext(ctx, `SELECT started_ns FROM spool_removals WHERE job_id=?`, jobID).
		Scan(&startedNS); err != nil {
		return time.Time{}, fmt.Errorf("agent: read service removal start: %w", err)
	}
	return time.Unix(0, startedNS).UTC(), nil
}

func oneRuntimeRemovalRowAffected(result sql.Result, jobID, what string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("agent: inspect %s: %w", what, err)
	}
	if affected != 1 {
		return fmt.Errorf("agent: %s for %q changed %d rows; only a runtime removal keeps stall accounting",
			what, jobID, affected)
	}
	return nil
}

// recordRuntimeRemovalStallDeclared marks the durable record whose stall L1
// has accepted. The record stays: the directive still stands and the agent
// keeps trying. What changes is that the slot it was pinning is now free, so
// the node doctor stops reporting it as a pinned slot.
func (spool *logSpool) recordRuntimeRemovalStallDeclared(ctx context.Context, removal localRemoval, declaredAt time.Time) error {
	if _, err := spool.db.ExecContext(ctx, `UPDATE runtime_removal_manifests
SET stall_declared_ns=? WHERE job_id=? AND removal_generation=? AND cleanup_fence=? AND root_instance_id=? AND stall_declared_ns IS NULL`,
		declaredAt.UTC().Round(0).UnixNano(), removal.jobID, removal.generation, removal.cleanupFence,
		removal.rootInstanceID); err != nil {
		return fmt.Errorf("agent: persist declared runtime removal stall: %w", err)
	}
	return nil
}

func (spool *logSpool) recordRuntimeQuiesced(ctx context.Context, removal localRemoval, receipt workloadrunner.ReapReceipt, observedAt time.Time) error {
	if err := validateRuntimeReapReceipt(receipt); err != nil {
		return err
	}
	receiptJSON, err := json.Marshal(receipt)
	if err != nil {
		return fmt.Errorf("agent: encode runtime quiescence receipt: %w", err)
	}
	record, found, err := spool.runtimeRemoval(ctx, removal.jobID)
	if err != nil {
		return err
	}
	if !found || !sameLocalRemoval(record.removal, removal) {
		return fmt.Errorf("agent: runtime removal %q has no matching frozen manifest", removal.jobID)
	}
	if record.phase == runtimeRemovalPrepared {
		result, err := spool.db.ExecContext(ctx, `UPDATE runtime_removal_manifests
SET runtime_quiescence_json=?, phase=?, quiesced_ns=?
WHERE job_id=? AND removal_generation=? AND cleanup_fence=? AND root_instance_id=? AND phase=?`,
			receiptJSON, runtimeRemovalQuarantined, observedAt.UTC().Round(0).UnixNano(), removal.jobID,
			removal.generation, removal.cleanupFence, removal.rootInstanceID, runtimeRemovalPrepared)
		if err != nil {
			return fmt.Errorf("agent: persist runtime quiescence receipt: %w", err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("agent: inspect runtime quiescence receipt persistence: %w", err)
		}
		if changed != 1 {
			return fmt.Errorf("agent: persist runtime quiescence receipt changed %d rows", changed)
		}
		if spool.runtimeRemovalCheckpoint != nil {
			if err := spool.runtimeRemovalCheckpoint(runtimeRemovalCheckpointAfterQuiescence); err != nil {
				return err
			}
		}
		record, found, err = spool.runtimeRemoval(ctx, removal.jobID)
		if err != nil || !found {
			return errors.Join(err, errors.New("agent: persisted runtime quiescence receipt disappeared"))
		}
	}
	storedReceipt, err := json.Marshal(record.receipt)
	if err != nil || !bytes.Equal(storedReceipt, receiptJSON) {
		return errors.New("agent: runtime quiescence receipt conflicts with persisted evidence")
	}
	if record.phase != runtimeRemovalQuarantined {
		return fmt.Errorf("agent: runtime removal %q has invalid phase %q", removal.jobID, record.phase)
	}
	return nil
}

func (spool *logSpool) recordRuntimeAttested(ctx context.Context, removal localRemoval, attestation workloadrunner.RuntimeRemovalAttestation, observedAt time.Time) error {
	record, found, err := spool.runtimeRemoval(ctx, removal.jobID)
	if err != nil {
		return err
	}
	if !found || !sameLocalRemoval(record.removal, removal) {
		return fmt.Errorf("agent: runtime removal %q has no matching frozen manifest", removal.jobID)
	}
	if err := validateRuntimeRemovalAttestation(record.manifest, attestation); err != nil {
		return err
	}
	payload, err := json.Marshal(attestation)
	if err != nil {
		return fmt.Errorf("agent: encode runtime absence attestation: %w", err)
	}
	if record.phase == runtimeRemovalComplete {
		stored, marshalErr := json.Marshal(record.attestation)
		if marshalErr != nil || !bytes.Equal(stored, payload) {
			return errors.New("agent: runtime absence attestation conflicts with persisted evidence")
		}
		return nil
	}
	if record.phase != runtimeRemovalQuarantined {
		return fmt.Errorf("agent: runtime removal %q cannot attest from phase %q", removal.jobID, record.phase)
	}
	observedNS := observedAt.UTC().Round(0).UnixNano()
	result, err := spool.db.ExecContext(ctx, `UPDATE runtime_removal_manifests
SET absence_attestation_json=?, phase=?, attested_ns=?, completed_ns=?
WHERE job_id=? AND removal_generation=? AND cleanup_fence=? AND root_instance_id=? AND phase=?`,
		payload, runtimeRemovalComplete, observedNS, observedNS, removal.jobID, removal.generation,
		removal.cleanupFence, removal.rootInstanceID, runtimeRemovalQuarantined)
	if err != nil {
		return fmt.Errorf("agent: persist runtime absence attestation: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("agent: inspect runtime absence attestation persistence: %w", err)
	}
	if changed != 1 {
		return fmt.Errorf("agent: persist runtime absence attestation changed %d rows", changed)
	}
	if spool.runtimeRemovalCheckpoint != nil {
		if err := spool.runtimeRemovalCheckpoint(runtimeRemovalCheckpointAfterComplete); err != nil {
			return err
		}
	}
	return nil
}

func validateRuntimeRemovalAttestation(manifest runtimeRemovalManifest, attestation workloadrunner.RuntimeRemovalAttestation) error {
	if attestation.Version != 1 || attestation.JobID != manifest.JobID || attestation.RemovalGeneration != manifest.RemovalGeneration ||
		strings.TrimSpace(attestation.RuntimeInstanceID) == "" || attestation.RuntimeGeneration == 0 || len(attestation.Attempts) == 0 {
		return errors.New("agent: runtime removal requires a complete helper-generation absence attestation")
	}
	if len(attestation.Attempts) < len(manifest.Attempts) {
		return errors.New("agent: runtime absence attestation omitted a frozen attempt")
	}
	wantAttempts, err := json.Marshal(manifest.Attempts)
	if err != nil {
		return err
	}
	gotAttempts, err := json.Marshal(attestation.Attempts[:len(manifest.Attempts)])
	if err != nil || !bytes.Equal(wantAttempts, gotAttempts) {
		return errors.New("agent: runtime absence attestation does not match the frozen attempt manifest")
	}
	computerID, storageID := "", ""
	for _, attempt := range manifest.Attempts {
		if attempt.ComputerStorage != nil {
			computerID, storageID = attempt.ComputerStorage.ComputerID, attempt.ComputerStorage.StorageID
			break
		}
	}
	for _, attempt := range attestation.Attempts[len(manifest.Attempts):] {
		if !attempt.StorageOnly || !validComputerStorage(attempt.ComputerStorage) {
			return errors.New("agent: runtime absence attestation added a non-authoritative Storage manifest")
		}
		if computerID == "" {
			computerID, storageID = attempt.ComputerStorage.ComputerID, attempt.ComputerStorage.StorageID
		}
		if attempt.ComputerStorage.ComputerID != computerID || attempt.ComputerStorage.StorageID != storageID {
			return errors.New("agent: runtime absence attestation mixed Computer Storage authorities")
		}
	}
	want := make(map[workloadrunner.RuntimeRemovalResource]struct{})
	for _, attempt := range attestation.Attempts {
		for _, resource := range attempt.RemovalResources() {
			want[resource] = struct{}{}
		}
	}
	if len(attestation.Assertions) != len(want) {
		return errors.New("agent: runtime absence attestation omitted a manifest resource class")
	}
	for _, assertion := range attestation.Assertions {
		resource := workloadrunner.RuntimeRemovalResource{Class: assertion.Class, ID: assertion.ID}
		if !assertion.Absent {
			return fmt.Errorf("agent: runtime absence assertion %s/%s did not pass", assertion.Class, assertion.ID)
		}
		if _, exists := want[resource]; !exists {
			return fmt.Errorf("agent: runtime absence attestation asserted an unmanifested resource %s/%s", assertion.Class, assertion.ID)
		}
		delete(want, resource)
	}
	if len(want) != 0 {
		return errors.New("agent: runtime absence attestation did not execute every manifest assertion")
	}
	return nil
}

func validateRuntimeReapReceipt(receipt workloadrunner.ReapReceipt) error {
	if !receipt.RuntimeQuiesced || strings.TrimSpace(receipt.BootSessionID) == "" {
		return errors.New("agent: runtime removal requires positive boot-scoped quiescence evidence")
	}
	switch receipt.Evidence {
	case workloadrunner.ReapEvidenceAttempt, workloadrunner.ReapEvidenceNoRuntime:
		if receipt.SweepEpoch != "" || receipt.HelperGeneration != 0 {
			return fmt.Errorf("agent: runtime removal evidence %q carried unexpected sweep authority", receipt.Evidence)
		}
		return nil
	case workloadrunner.ReapEvidenceOCISweep, workloadrunner.ReapEvidencePriorBootOCISweep, workloadrunner.ReapEvidenceOCIRuntimeSweep:
		if strings.TrimSpace(receipt.SweepEpoch) == "" || receipt.HelperGeneration == 0 {
			return fmt.Errorf("agent: runtime removal evidence %q requires sweep epoch and helper generation", receipt.Evidence)
		}
		return nil
	default:
		return fmt.Errorf("agent: runtime removal evidence kind %q is unsupported", receipt.Evidence)
	}
}

func sameLocalRemoval(left, right localRemoval) bool {
	return left.jobID == right.jobID && left.generation == right.generation && left.cleanupFence == right.cleanupFence && left.rootInstanceID == right.rootInstanceID
}

// RuntimeRemovalView is one durable runtime removal rendered for an operator
// read. The removal proof -- the frozen resource manifest, the phase it has
// reached, the quiescence receipt, and the per-resource absence attestation --
// has always been durable, but nothing outside this package could see it, so a
// job sitting at removal_pending was opaque to the operator holding the node.
//
// This is a projection, not a second source of truth: the caller gets the rows
// as persisted, and reading them starts, retries and advances nothing.
type RuntimeRemovalView struct {
	JobID             string
	RemovalGeneration uint64
	CleanupFence      string
	RootInstanceID    string
	Phase             string
	PreparedAt        time.Time
	QuiescedAt        *time.Time
	AttestedAt        *time.Time
	CompletedAt       *time.Time
	Quiescence        workloadrunner.ReapReceipt
	ResourceManifests []workloadrunner.RuntimeResourceManifest
	Attestation       *workloadrunner.RuntimeRemovalAttestation
	// InvalidReason names the field that makes this durable row unusable. It
	// is empty for every record the agent can act on.
	InvalidReason string
	// FailedAttempts and LastRefusalCode explain a removal that keeps being
	// tried and keeps being refused. StallDeclaredAt is set once L1 has
	// accepted that non-completion and released the service slot, which is
	// what turns a stalled record from a pinned slot into a standing chore.
	FailedAttempts    int
	LastRefusalCode   string
	LastRefusalDetail string
	LastAttemptedAt   *time.Time
	StallDeclaredAt   *time.Time
	// StallDeclarationFrozen reports that a declaration has been written but
	// not yet accepted; it is the state a lost response leaves behind.
	StallDeclarationFrozen bool
}

// RuntimeRemovals lists every runtime removal this agent still carries, oldest
// preparation first. A removal leaves the list when L1 acknowledges its
// cleanup, because that is when the agent releases the durable record.
func (a *Agent) RuntimeRemovals(ctx context.Context) ([]RuntimeRemovalView, error) {
	if a == nil || a.outbox == nil {
		return nil, errors.New("agent: runtime removal inspection is unavailable")
	}
	records, err := a.outbox.pendingRuntimeRemovals(ctx)
	if err != nil {
		return nil, err
	}
	views := make([]RuntimeRemovalView, 0, len(records))
	for _, record := range records {
		view := RuntimeRemovalView{
			JobID:                  record.removal.jobID,
			RemovalGeneration:      record.removal.generation,
			CleanupFence:           record.removal.cleanupFence,
			RootInstanceID:         record.removal.rootInstanceID,
			Phase:                  string(record.phase),
			PreparedAt:             record.preparedAt,
			QuiescedAt:             record.quiescedAt,
			AttestedAt:             record.attestedAt,
			CompletedAt:            record.completedAt,
			Quiescence:             record.receipt,
			ResourceManifests:      slices.Clone(record.manifest.Attempts),
			InvalidReason:          record.invalidReason,
			FailedAttempts:         record.failedAttempts,
			LastRefusalCode:        record.lastRefusalCode,
			LastRefusalDetail:      record.lastRefusalDetail,
			LastAttemptedAt:        record.lastAttemptedAt,
			StallDeclaredAt:        record.stallDeclaredAt,
			StallDeclarationFrozen: len(record.stallDeclaration) != 0,
		}
		if record.attestation.Version != 0 {
			attestation := record.attestation
			attestation.Attempts = slices.Clone(record.attestation.Attempts)
			attestation.Assertions = slices.Clone(record.attestation.Assertions)
			view.Attestation = &attestation
		}
		views = append(views, view)
	}
	return views, nil
}
