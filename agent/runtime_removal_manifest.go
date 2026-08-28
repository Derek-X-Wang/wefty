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
	preparedAt  time.Time
	quiescedAt  *time.Time
	attestedAt  *time.Time
	completedAt *time.Time
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
		strings.TrimSpace(manifest.RemovalGeneration) == "" || strings.TrimSpace(manifest.LeaseID) == "" ||
		strings.TrimSpace(manifest.TaskID) == "" || strings.TrimSpace(manifest.ContainerID) == "" ||
		strings.TrimSpace(manifest.SnapshotID) == "" || strings.TrimSpace(manifest.ShimID) == "" ||
		strings.TrimSpace(manifest.CgroupID) == "" || strings.TrimSpace(manifest.LogSegmentDirectory) == "" {
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
	if err := tx.QueryRowContext(ctx, `SELECT manifest_json FROM runtime_removal_manifests
WHERE job_id=? AND removal_generation=? AND cleanup_fence=? AND root_instance_id=?`, removal.jobID,
		removal.generation, removal.cleanupFence, removal.rootInstanceID).Scan(&persisted); err != nil {
		return fmt.Errorf("agent: read reconstructed runtime removal manifest: %w", err)
	}
	if !bytes.Equal(persisted, payload) {
		return fmt.Errorf("agent: reconstructed runtime removal manifest for job %q conflicts with persisted inventory", removal.jobID)
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

func (spool *logSpool) runtimeRemoval(ctx context.Context, jobID string) (runtimeRemovalRecord, bool, error) {
	row := spool.db.QueryRowContext(ctx, `SELECT removal_generation, cleanup_fence, root_instance_id,
manifest_json, runtime_quiescence_json, absence_attestation_json, phase, prepared_ns, quiesced_ns, attested_ns, completed_ns
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
	var quiescedNS, attestedNS, completedNS sql.NullInt64
	if err := row.Scan(&record.removal.generation, &record.removal.cleanupFence, &record.removal.rootInstanceID,
		&manifestJSON, &receiptJSON, &attestationJSON, &record.phase, &preparedNS, &quiescedNS, &attestedNS, &completedNS); err != nil {
		return runtimeRemovalRecord{}, err
	}
	if err := json.Unmarshal(manifestJSON, &record.manifest); err != nil || !validRuntimeRemovalManifest(record.manifest) {
		return runtimeRemovalRecord{}, errors.New("runtime removal manifest is corrupt")
	}
	record.removal.jobID = record.manifest.JobID
	record.removal.kind = contract.JobKindOCI
	record.preparedAt = time.Unix(0, preparedNS).UTC()
	if len(receiptJSON) != 0 {
		if err := json.Unmarshal(receiptJSON, &record.receipt); err != nil {
			return runtimeRemovalRecord{}, errors.New("runtime quiescence receipt is corrupt")
		}
	}
	if len(attestationJSON) != 0 {
		if err := json.Unmarshal(attestationJSON, &record.attestation); err != nil {
			return runtimeRemovalRecord{}, errors.New("runtime absence attestation is corrupt")
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
	if !validRuntimeRemovalRecord(record) {
		return runtimeRemovalRecord{}, errors.New("runtime removal record is invalid")
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

func validRuntimeRemovalRecord(record runtimeRemovalRecord) bool {
	if record.removal.jobID == "" || record.removal.generation == 0 || record.removal.cleanupFence == "" || record.removal.rootInstanceID == "" ||
		record.manifest.JobID != record.removal.jobID || record.manifest.RemovalGeneration != record.removal.generation {
		return false
	}
	switch record.phase {
	case runtimeRemovalPrepared:
		return !record.receipt.RuntimeQuiesced && record.receipt.Evidence == "" && record.attestation.Version == 0 && record.quiescedAt == nil && record.attestedAt == nil && record.completedAt == nil
	case runtimeRemovalQuarantined:
		return validateRuntimeReapReceipt(record.receipt) == nil && record.attestation.Version == 0 && record.quiescedAt != nil && record.attestedAt == nil && record.completedAt == nil
	case runtimeRemovalComplete:
		return validateRuntimeReapReceipt(record.receipt) == nil && validateRuntimeRemovalAttestation(record.manifest, record.attestation) == nil && record.quiescedAt != nil && record.attestedAt != nil && record.completedAt != nil
	default:
		return false
	}
}

func (spool *logSpool) pendingRuntimeRemovals(ctx context.Context) ([]runtimeRemovalRecord, error) {
	rows, err := spool.db.QueryContext(ctx, `SELECT removal_generation, cleanup_fence, root_instance_id,
manifest_json, runtime_quiescence_json, absence_attestation_json, phase, prepared_ns, quiesced_ns, attested_ns, completed_ns
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
			return nil, fmt.Errorf("agent: scan pending runtime removal: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("agent: iterate pending runtime removals: %w", err)
	}
	return records, nil
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
	wantAttempts, err := json.Marshal(manifest.Attempts)
	if err != nil {
		return err
	}
	gotAttempts, err := json.Marshal(attestation.Attempts)
	if err != nil || !bytes.Equal(wantAttempts, gotAttempts) {
		return errors.New("agent: runtime absence attestation does not match the frozen attempt manifest")
	}
	want := make(map[workloadrunner.RuntimeRemovalResource]struct{})
	for _, attempt := range manifest.Attempts {
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
