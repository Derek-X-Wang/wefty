package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// ComputerStorageOperationVerb names the Storage operation a failure report
// is about. Each verb has its own durable operation table; the failure budget
// is shared (#1608).
type ComputerStorageOperationVerb string

const (
	ComputerStorageOperationImport  ComputerStorageOperationVerb = "import"
	ComputerStorageOperationClone   ComputerStorageOperationVerb = "clone"
	ComputerStorageOperationRestore ComputerStorageOperationVerb = "restore"
	ComputerStorageOperationReset   ComputerStorageOperationVerb = "reset"
	ComputerStorageOperationBackup  ComputerStorageOperationVerb = "backup"
	ComputerStorageOperationGrow    ComputerStorageOperationVerb = "grow"
)

// ComputerStorageOperationStage splits an operation at publication. `prepare`
// is every step before the new generation becomes current; `retire` is the
// deletion of the predecessor after publication, which only reset and restore
// have. Each stage has its own budget.
type ComputerStorageOperationStage string

const (
	ComputerStorageOperationPrepare ComputerStorageOperationStage = "prepare"
	ComputerStorageOperationRetire  ComputerStorageOperationStage = "retire"
)

// ComputerStorageOperationFailureClass is the agent's closed classification
// of one failed attempt. The agent maps what it saw to a class; L1 decides
// what the class means for the operation.
type ComputerStorageOperationFailureClass string

const (
	// The live helper session answered with a typed error that is not a
	// receipt.
	ComputerStorageOperationFailureEngine ComputerStorageOperationFailureClass = "engine_failure"
	// The helper session was lost while the call was in flight.
	ComputerStorageOperationFailureRuntimeLoss ComputerStorageOperationFailureClass = "runtime_loss"
	// The helper quarantined the generation. Quarantine never clears by
	// itself, so it ends the operation at once.
	ComputerStorageOperationFailureQuarantined ComputerStorageOperationFailureClass = "quarantined"
	// The helper deferred the call. The helper bounds deferral itself and
	// turns one that never converges into a quarantine, so a deferral is
	// recorded and not counted.
	ComputerStorageOperationFailureResumeDeferred ComputerStorageOperationFailureClass = "resume_deferred"
	// The helper call succeeded and L1 refused its acknowledgement with a
	// non-transient protocol error.
	ComputerStorageOperationFailureAcknowledgementRefused ComputerStorageOperationFailureClass = "acknowledgement_refused"
	// The agent refused the directive locally.
	ComputerStorageOperationFailureAgentRefused ComputerStorageOperationFailureClass = "agent_refused"
)

// ComputerStorageOperationFailureDisposition is L1's answer to one report.
// Every disposition is final for the attempt that was reported.
type ComputerStorageOperationFailureDisposition string

const (
	ComputerStorageOperationFailureCounted    ComputerStorageOperationFailureDisposition = "counted"
	ComputerStorageOperationFailureRecorded   ComputerStorageOperationFailureDisposition = "recorded"
	ComputerStorageOperationFailureReplayed   ComputerStorageOperationFailureDisposition = "replayed"
	ComputerStorageOperationFailureExhausted  ComputerStorageOperationFailureDisposition = "exhausted"
	ComputerStorageOperationFailureSuperseded ComputerStorageOperationFailureDisposition = "superseded"
)

// ComputerStorageOperationFailureRequest reports that one attempt of a
// Storage operation directive failed. report_id is minted once per failed
// attempt, so a retried report is answered `replayed` and not counted twice.
type ComputerStorageOperationFailureRequest struct {
	NodeID            string                               `json:"node_id"`
	BootSessionID     string                               `json:"boot_session_id"`
	ReportID          string                               `json:"report_id"`
	Verb              ComputerStorageOperationVerb         `json:"verb"`
	OperationRevision int64                                `json:"operation_revision"`
	Stage             ComputerStorageOperationStage        `json:"stage"`
	Fence             string                               `json:"fence"`
	Class             ComputerStorageOperationFailureClass `json:"class"`
	Code              string                               `json:"code"`
	Detail            string                               `json:"detail,omitempty"`
	HelperGeneration  uint64                               `json:"helper_generation,omitempty"`
	// PreparationOutcome is the helper's generation-bound evidence for a
	// copy verb's prepare stage. L1 keeps it as the operation's latest
	// preparation observation, as the acknowledgement path does.
	PreparationOutcome *ComputerStoragePreparationOutcome `json:"preparation_outcome,omitempty"`
}

type ComputerStorageOperationFailureResponse struct {
	Disposition    ComputerStorageOperationFailureDisposition `json:"disposition"`
	FailedAttempts int64                                      `json:"failed_attempts"`
	FirstFailedAt  *time.Time                                 `json:"first_failed_at,omitempty"`
	// RetryAfterSeconds is L1's pacing answer for the next attempt of this
	// directive. Zero when the operation is no longer running.
	RetryAfterSeconds int64 `json:"retry_after_seconds"`
	// OperationStatus is the operation's durable status after this report.
	// Absent when the operation no longer exists.
	OperationStatus string `json:"operation_status,omitempty"`
}

// ComputerStorageOperationFailure is the operator projection of one
// operation stage's failure budget: how often it failed, since when, and
// what the last failure was.
type ComputerStorageOperationFailure struct {
	Stage          ComputerStorageOperationStage        `json:"stage"`
	Class          ComputerStorageOperationFailureClass `json:"class"`
	Code           string                               `json:"code"`
	FailedAttempts int64                                `json:"failed_attempts"`
	FirstFailedAt  *time.Time                           `json:"first_failed_at,omitempty"`
	LastFailedAt   time.Time                            `json:"last_failed_at"`
}

var (
	storageOperationReportIDPattern   = regexp.MustCompile(`^[0-9a-f]{32}$`)
	storageOperationFailureCodeFormat = regexp.MustCompile(`^[a-z0-9_]{1,64}$`)
)

// storageResetPrepareEndsByBudget is false until a fenced reset can be healed
// (#1609). The helper fences the old generation before it prepares the
// successor, and nothing clears that fence, so ending a reset early would
// strand a Computer that the same reset might still heal. Reset failures are
// counted and shown meanwhile; turning this on is the whole change.
var storageResetPrepareEndsByBudget = false

func storageOperationStageEndsByBudget(verb ComputerStorageOperationVerb, stage ComputerStorageOperationStage) bool {
	return verb != ComputerStorageOperationReset || stage != ComputerStorageOperationPrepare || storageResetPrepareEndsByBudget
}

// storageOperationFailureCode is the closed failure_code an operation ends
// with, derived from the class of the report that spent its budget.
func storageOperationFailureCode(class ComputerStorageOperationFailureClass) string {
	if class == ComputerStorageOperationFailureQuarantined {
		return ComputerStoragePreparationQuarantined
	}
	return string(class)
}

func storageOperationRetryAfter(failedAttempts int64) int64 {
	delay := storageOperationFailureRetryBase
	for step := int64(1); step < failedAttempts && delay < storageOperationFailureRetryCeiling; step++ {
		delay *= 2
	}
	return int64(min(delay, storageOperationFailureRetryCeiling) / time.Second)
}

func validateStorageOperationFailureRequest(computerID string, request ComputerStorageOperationFailureRequest) error {
	if computerID == "" || request.NodeID == "" || request.BootSessionID == "" || request.Fence == "" || request.OperationRevision <= 0 {
		return protocolError(contract.ErrorInvalidRequest, "complete Computer Storage operation failure report fields are required")
	}
	if !storageOperationReportIDPattern.MatchString(request.ReportID) {
		return protocolError(contract.ErrorInvalidRequest, "report_id must be 32 lowercase hex characters")
	}
	switch request.Verb {
	case ComputerStorageOperationImport, ComputerStorageOperationClone, ComputerStorageOperationBackup, ComputerStorageOperationGrow:
		if request.Stage != ComputerStorageOperationPrepare {
			return protocolError(contract.ErrorInvalidRequest, "Computer %s has only a prepare stage", request.Verb)
		}
	case ComputerStorageOperationRestore, ComputerStorageOperationReset:
		if request.Stage != ComputerStorageOperationPrepare && request.Stage != ComputerStorageOperationRetire {
			return protocolError(contract.ErrorInvalidRequest, "Computer Storage operation stage %q is invalid", request.Stage)
		}
	default:
		return protocolError(contract.ErrorInvalidRequest, "Computer Storage operation verb %q is invalid", request.Verb)
	}
	switch request.Class {
	case ComputerStorageOperationFailureEngine, ComputerStorageOperationFailureRuntimeLoss,
		ComputerStorageOperationFailureQuarantined, ComputerStorageOperationFailureResumeDeferred,
		ComputerStorageOperationFailureAcknowledgementRefused, ComputerStorageOperationFailureAgentRefused:
	default:
		return protocolError(contract.ErrorInvalidRequest, "Computer Storage operation failure class %q is invalid", request.Class)
	}
	if !storageOperationFailureCodeFormat.MatchString(request.Code) {
		return protocolError(contract.ErrorInvalidRequest, "Computer Storage operation failure code must match %s", storageOperationFailureCodeFormat)
	}
	if len(request.Detail) > MaximumStorageOperationFailureDetail {
		return protocolError(contract.ErrorInvalidRequest, "Computer Storage operation failure detail exceeds %d bytes", MaximumStorageOperationFailureDetail)
	}
	if request.PreparationOutcome != nil && (request.Stage != ComputerStorageOperationPrepare ||
		(request.Verb != ComputerStorageOperationImport && request.Verb != ComputerStorageOperationClone && request.Verb != ComputerStorageOperationRestore)) {
		return protocolError(contract.ErrorInvalidRequest, "only a Storage copy prepare stage carries a preparation outcome")
	}
	return nil
}

// storageOperationTarget is the one operation a report names, read from its
// verb's own table.
type storageOperationTarget struct {
	status      string
	boundNodeID string
	fence       string
	copyRow     computerStorageCopyRow
	reset       computerStorageResetRow
	backup      computerBackupOperationRow
	grow        computerStorageGrowRow
}

func readStorageOperationTarget(ctx context.Context, tx *sql.Tx, computerID string, verb ComputerStorageOperationVerb, revision int64) (storageOperationTarget, error) {
	var target storageOperationTarget
	var err error
	switch verb {
	case ComputerStorageOperationReset:
		target.reset, err = readComputerStorageResetByRevision(ctx, tx, computerID, revision)
		target.status, target.boundNodeID, target.fence = target.reset.Status, target.reset.BoundNodeID, target.reset.CleanupFence
	case ComputerStorageOperationBackup:
		target.backup, err = readComputerBackupOperationByRevision(ctx, tx, computerID, revision)
		target.status, target.boundNodeID, target.fence = target.backup.Status, target.backup.BoundNodeID, target.backup.CleanupFence
	case ComputerStorageOperationGrow:
		target.grow, err = readComputerGrowByRevision(ctx, tx, computerID, revision)
		target.status, target.boundNodeID, target.fence = target.grow.Status, target.grow.BoundNodeID, target.grow.OperationFence
	default:
		target.copyRow, err = scanComputerStorageCopy(tx.QueryRowContext(ctx, `SELECT `+storageCopyColumns+`
			FROM computer_storage_copy_operations WHERE destination_computer_id=? AND operation_revision=? AND operation=?`,
			computerID, revision, string(verb)))
		target.status, target.boundNodeID, target.fence = target.copyRow.Status, target.copyRow.BoundNodeID, target.copyRow.CleanupFence
	}
	return target, err
}

// storageOperationStageLive reports whether status is a running status of
// stage. Anything else means the operation ended or moved past the stage.
func storageOperationStageLive(verb ComputerStorageOperationVerb, stage ComputerStorageOperationStage, status string) bool {
	if stage == ComputerStorageOperationRetire {
		return status == "published"
	}
	switch verb {
	case ComputerStorageOperationBackup, ComputerStorageOperationGrow:
		return status == "planned"
	default:
		return status == "reserved" || status == "prepared"
	}
}

type storageOperationFailureRow struct {
	failedAttempts int64
	firstFailedNS  sql.NullInt64
	exhaustedNS    sql.NullInt64
	reportID       string
	reportHash     string
}

func readStorageOperationFailureRow(ctx context.Context, tx *sql.Tx, computerID string, revision int64, stage ComputerStorageOperationStage) (storageOperationFailureRow, bool, error) {
	var row storageOperationFailureRow
	err := tx.QueryRowContext(ctx, `SELECT failed_attempts, first_failed_ns, exhausted_ns, last_report_id, last_report_hash
		FROM computer_storage_operation_failures WHERE computer_id=? AND operation_revision=? AND stage=?`,
		computerID, revision, string(stage)).Scan(&row.failedAttempts, &row.firstFailedNS, &row.exhaustedNS, &row.reportID, &row.reportHash)
	if errors.Is(err, sql.ErrNoRows) {
		return row, false, nil
	}
	return row, err == nil, err
}

func storageOperationFailureResponse(disposition ComputerStorageOperationFailureDisposition, row storageOperationFailureRow, status string) ComputerStorageOperationFailureResponse {
	response := ComputerStorageOperationFailureResponse{Disposition: disposition, FailedAttempts: row.failedAttempts, OperationStatus: status}
	if row.firstFailedNS.Valid {
		first := time.Unix(0, row.firstFailedNS.Int64).UTC()
		response.FirstFailedAt = &first
	}
	if disposition != ComputerStorageOperationFailureExhausted && disposition != ComputerStorageOperationFailureSuperseded &&
		!row.exhaustedNS.Valid {
		response.RetryAfterSeconds = storageOperationRetryAfter(row.failedAttempts)
	}
	return response
}

// ReportComputerStorageOperationFailure is the one rule for every Storage
// verb (#1608). It counts one failed attempt against the operation stage's
// durable budget and ends the operation when the budget is spent: at least
// MaximumStorageOperationFailures counted failures and at least
// StorageOperationFailureWindow since the first, on the L1 clock. A
// quarantine ends the operation at once; a deferral is recorded and not
// counted. A report for an operation that ended, moved past the stage, or was
// overtaken by a newer revision writes nothing and is answered `superseded`.
// Ending deletes nothing on the Node: it releases L1 orchestration and leaves
// the residue for Computer removal.
func (s *Store) ReportComputerStorageOperationFailure(ctx context.Context, identityNodeID, computerID string,
	request ComputerStorageOperationFailureRequest) (ComputerStorageOperationFailureResponse, error) {
	if err := validateStorageOperationFailureRequest(computerID, request); err != nil {
		return ComputerStorageOperationFailureResponse{}, err
	}
	bodyHash, err := storageCopyHash(request)
	if err != nil {
		return ComputerStorageOperationFailureResponse{}, internalError(err, "encode Computer Storage operation failure report")
	}
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return ComputerStorageOperationFailureResponse{}, internalError(err, "begin Computer Storage operation failure report")
	}
	tx, now := write.tx, write.at
	defer write.rollback()
	ctx = write.ctx
	if err := validateStorageResetNode(ctx, tx, identityNodeID, request.NodeID, request.BootSessionID); err != nil {
		return ComputerStorageOperationFailureResponse{}, err
	}
	target, err := readStorageOperationTarget(ctx, tx, computerID, request.Verb, request.OperationRevision)
	targetFound := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return ComputerStorageOperationFailureResponse{}, internalError(err, "read reported Computer Storage operation")
	}
	failures, failuresFound, err := readStorageOperationFailureRow(ctx, tx, computerID, request.OperationRevision, request.Stage)
	if err != nil {
		return ComputerStorageOperationFailureResponse{}, internalError(err, "read Computer Storage operation failure budget")
	}
	// A retried report is answered from the durable row, even when the
	// operation has since ended: the agent that never saw the first answer
	// must read the same one, and the attempt must not count twice.
	if failuresFound && failures.reportID == request.ReportID {
		if targetFound && request.NodeID != target.boundNodeID {
			return ComputerStorageOperationFailureResponse{}, protocolError(contract.ErrorAttemptNotOwned, "authenticated node does not own this Computer Storage operation")
		}
		if failures.reportHash != bodyHash {
			return ComputerStorageOperationFailureResponse{}, protocolError(contract.ErrorIdempotencyConflict, "Computer Storage operation failure report replay differs from durable evidence")
		}
		return storageOperationFailureResponse(ComputerStorageOperationFailureReplayed, failures, target.status), nil
	}
	if !targetFound {
		return ComputerStorageOperationFailureResponse{Disposition: ComputerStorageOperationFailureSuperseded}, nil
	}
	var reconfigurationRevision sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT reconfiguration_revision FROM computers WHERE computer_id=?`, computerID).
		Scan(&reconfigurationRevision); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return ComputerStorageOperationFailureResponse{}, internalError(err, "read reported Computer reconfiguration")
	}
	if !storageOperationStageLive(request.Verb, request.Stage, target.status) || !reconfigurationRevision.Valid ||
		reconfigurationRevision.Int64 != request.OperationRevision {
		return ComputerStorageOperationFailureResponse{Disposition: ComputerStorageOperationFailureSuperseded,
			FailedAttempts: failures.failedAttempts, OperationStatus: target.status}, nil
	}
	if request.NodeID != target.boundNodeID {
		return ComputerStorageOperationFailureResponse{}, protocolError(contract.ErrorAttemptNotOwned, "authenticated node does not own this Computer Storage operation")
	}
	if request.Fence != target.fence {
		return ComputerStorageOperationFailureResponse{}, protocolError(contract.ErrorStaleFence, "Computer Storage operation failure report names another fence")
	}
	if request.PreparationOutcome != nil {
		if err := recordStorageOperationPreparationOutcome(ctx, tx, target.copyRow, request); err != nil {
			return ComputerStorageOperationFailureResponse{}, err
		}
	}
	nowNS := now.UnixNano()
	if request.Class != ComputerStorageOperationFailureResumeDeferred {
		failures.failedAttempts++
		if !failures.firstFailedNS.Valid {
			failures.firstFailedNS = sql.NullInt64{Int64: nowNS, Valid: true}
		}
	}
	exhausted := storageOperationStageEndsByBudget(request.Verb, request.Stage) &&
		(request.Class == ComputerStorageOperationFailureQuarantined ||
			(failures.failedAttempts >= MaximumStorageOperationFailures && failures.firstFailedNS.Valid &&
				now.Sub(time.Unix(0, failures.firstFailedNS.Int64)) >= StorageOperationFailureWindow))
	if exhausted {
		failures.exhaustedNS = sql.NullInt64{Int64: nowNS, Valid: true}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_storage_operation_failures(computer_id, operation_revision, stage, verb,
		failed_attempts, first_failed_ns, last_failed_ns, last_failure_class, last_failure_code, last_failure_detail,
		last_helper_generation, last_report_id, last_report_hash, exhausted_ns)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(computer_id, operation_revision, stage) DO UPDATE SET failed_attempts=excluded.failed_attempts,
		first_failed_ns=excluded.first_failed_ns, last_failed_ns=excluded.last_failed_ns,
		last_failure_class=excluded.last_failure_class, last_failure_code=excluded.last_failure_code,
		last_failure_detail=excluded.last_failure_detail, last_helper_generation=excluded.last_helper_generation,
		last_report_id=excluded.last_report_id, last_report_hash=excluded.last_report_hash, exhausted_ns=excluded.exhausted_ns`,
		computerID, request.OperationRevision, string(request.Stage), string(request.Verb), failures.failedAttempts,
		failures.firstFailedNS, nowNS, string(request.Class), request.Code, request.Detail, int64(request.HelperGeneration),
		request.ReportID, bodyHash, failures.exhaustedNS); err != nil {
		return ComputerStorageOperationFailureResponse{}, internalError(err, "record Computer Storage operation failure")
	}
	disposition, status := ComputerStorageOperationFailureCounted, target.status
	if request.Class == ComputerStorageOperationFailureResumeDeferred {
		disposition = ComputerStorageOperationFailureRecorded
	}
	if exhausted {
		failure := storageOperationLatchedFailure(request, target, failures.failedAttempts)
		status, err = endStorageOperation(ctx, tx, request, target, storageOperationFailureCode(request.Class), failure, now)
		if err != nil {
			return ComputerStorageOperationFailureResponse{}, err
		}
		disposition = ComputerStorageOperationFailureExhausted
	}
	if err := tx.Commit(); err != nil {
		return ComputerStorageOperationFailureResponse{}, internalError(err, "commit Computer Storage operation failure report")
	}
	if exhausted {
		s.notifyComputerPolicyChanged()
	}
	return storageOperationFailureResponse(disposition, failures, status), nil
}

// recordStorageOperationPreparationOutcome keeps the helper's preparation
// evidence beside the operation, bound to the reserved destination and never
// older than what L1 already holds, so the clone and import reads keep
// showing it.
func recordStorageOperationPreparationOutcome(ctx context.Context, tx *sql.Tx, row computerStorageCopyRow, request ComputerStorageOperationFailureRequest) error {
	if err := validateStorageCopyPreparationOutcome(row, "Computer "+string(request.Verb), request.PreparationOutcome); err != nil {
		return err
	}
	var storedOutcomeJSON []byte
	if err := tx.QueryRowContext(ctx, `SELECT preparation_outcome_json FROM computer_storage_copy_operations
		WHERE destination_computer_id=? AND operation_revision=?`, row.DestinationComputerID, row.OperationRevision).
		Scan(&storedOutcomeJSON); err != nil {
		return internalError(err, "read prior Storage copy preparation outcome")
	}
	if len(storedOutcomeJSON) != 0 {
		var stored ComputerStoragePreparationOutcome
		if err := json.Unmarshal(storedOutcomeJSON, &stored); err != nil {
			return internalError(err, "decode prior Storage copy preparation outcome")
		}
		if stored.RecordedAt == nil || request.PreparationOutcome.HelperGeneration < stored.HelperGeneration ||
			request.PreparationOutcome.RecordedAt.Before(*stored.RecordedAt) {
			return protocolError(contract.ErrorConflict, "Computer %s preparation outcome is older than durable evidence", request.Verb)
		}
	}
	outcomeJSON, err := json.Marshal(*request.PreparationOutcome)
	if err != nil {
		return internalError(err, "encode Storage copy preparation outcome")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE computer_storage_copy_operations SET preparation_outcome_json=?
		WHERE destination_computer_id=? AND operation_revision=?`, outcomeJSON, row.DestinationComputerID, row.OperationRevision); err != nil {
		return internalError(err, "record Storage copy preparation outcome")
	}
	return nil
}

func storageOperationLatchedFailure(request ComputerStorageOperationFailureRequest, target storageOperationTarget, attempts int64) []byte {
	failure := contract.SpawnFailure{Code: contract.SpawnFailureComputerStorageOperationFailed,
		Message: fmt.Sprintf("Computer %s %s stage ended after %d failed attempts; last: %s (%s)",
			request.Verb, request.Stage, attempts, request.Class, request.Code),
		NodeID: target.boundNodeID}
	if request.Verb == ComputerStorageOperationClone || request.Verb == ComputerStorageOperationImport {
		failure.RequestedBytes = target.copyRow.DestinationSize
	}
	encoded, _ := json.Marshal(failure)
	return encoded
}

// endStorageOperation is the verb's end function. It returns the status the
// operation ends with.
func endStorageOperation(ctx context.Context, tx *sql.Tx, request ComputerStorageOperationFailureRequest,
	target storageOperationTarget, code string, latchedFailure []byte, now time.Time) (string, error) {
	switch {
	case request.Verb == ComputerStorageOperationRestore && request.Stage == ComputerStorageOperationPrepare:
		return "failed", endComputerRestorePrepare(ctx, tx, target.copyRow, code, now)
	case request.Verb == ComputerStorageOperationRestore:
		return "cleanup_owed", endComputerRestoreRetire(ctx, tx, target.copyRow, code, latchedFailure, now)
	case request.Verb == ComputerStorageOperationReset && request.Stage == ComputerStorageOperationPrepare:
		return "failed", endComputerResetPrepare(ctx, tx, target.reset, code, latchedFailure, now)
	case request.Verb == ComputerStorageOperationReset:
		return "cleanup_owed", endComputerResetRetire(ctx, tx, target.reset, code, latchedFailure, now)
	case request.Verb == ComputerStorageOperationBackup:
		return "failed", endComputerBackup(ctx, tx, target.backup, code, now)
	case request.Verb == ComputerStorageOperationGrow:
		return "failed", endComputerGrow(ctx, tx, target.grow, code, latchedFailure, now)
	case request.Verb == ComputerStorageOperationClone:
		if err := requireCloningComputerCloneAuthority(ctx, tx, target.copyRow, now); err != nil {
			return "", err
		}
		return "failed", endComputerStorageCopyLatched(ctx, tx, target.copyRow, ComputerReconfigurationCloning, code, latchedFailure, now)
	default:
		return "failed", endComputerStorageCopyLatched(ctx, tx, target.copyRow, ComputerReconfigurationImporting, code, latchedFailure, now)
	}
}

func requireSingleOperationEnd(result sql.Result, err error, action string) error {
	if err != nil {
		return internalError(err, action)
	}
	return requireSingleStorageGenerationMutation(result, action)
}

// latchComputerOperationFailure records why a Storage operation ended on the
// Computer's Job. It clears any pending restart, so a stopped Computer runs
// again only after an explicit start; a running Computer keeps running.
func latchComputerOperationFailure(ctx context.Context, tx *sql.Tx, jobID string, latchedFailure []byte) error {
	if _, err := tx.ExecContext(ctx, `UPDATE service_jobs SET next_restart_at=NULL, last_failure=? WHERE job_id=?`,
		latchedFailure, jobID); err != nil {
		return internalError(err, "latch ended Computer Storage operation failure")
	}
	return nil
}

// endComputerRestorePrepare ends a restore before publication: the restore
// records `failed` and the Computer returns to `stable` on its old
// generation, exactly as a failed predecessor copy aborts it.
func endComputerRestorePrepare(ctx context.Context, tx *sql.Tx, row computerStorageCopyRow, code string, now time.Time) error {
	result, err := tx.ExecContext(ctx, `UPDATE computer_storage_copy_operations SET status='failed', failure_code=?, completed_ns=?
		WHERE destination_computer_id=? AND operation_revision=? AND status IN ('reserved', 'prepared')`,
		code, now.UnixNano(), row.DestinationComputerID, row.OperationRevision)
	if err := requireSingleOperationEnd(result, err, "end failed Computer restore"); err != nil {
		return err
	}
	return abortComputerRestore(ctx, tx, row, now)
}

// endComputerRestoreRetire ends a restore whose predecessor cannot be
// deleted. The data switch already happened, so the Computer returns to
// `stable` on the new generation, held stopped, and the restore records
// `cleanup_owed`: the old generation's bytes stay on the Node until removal.
func endComputerRestoreRetire(ctx context.Context, tx *sql.Tx, row computerStorageCopyRow, code string, latchedFailure []byte, now time.Time) error {
	result, err := tx.ExecContext(ctx, `UPDATE computer_storage_copy_operations SET status='cleanup_owed', failure_code=?, completed_ns=?
		WHERE destination_computer_id=? AND operation_revision=? AND status='published'`,
		code, now.UnixNano(), row.DestinationComputerID, row.OperationRevision)
	if err := requireSingleOperationEnd(result, err, "end Computer restore with cleanup owed"); err != nil {
		return err
	}
	if err := latchComputerOperationFailure(ctx, tx, row.JobID, latchedFailure); err != nil {
		return err
	}
	result, err = tx.ExecContext(ctx, `UPDATE computers SET applied_revision=?, reconfiguration_phase='stable',
		reconfiguration_revision=NULL, updated_ns=? WHERE computer_id=? AND intent_revision=? AND storage_generation=?
		AND reconfiguration_phase='restoring'`, row.OperationRevision, now.UnixNano(), row.DestinationComputerID,
		row.OperationRevision, row.DestinationGeneration)
	if err != nil {
		return internalError(err, "release Computer restore with cleanup owed")
	}
	return requireComputerCAS(result, row.DestinationComputerID, row.OperationRevision)
}

// endComputerResetPrepare ends a reset before publication: the staging
// generation retires and the Computer returns to `stable` on the old
// generation, held stopped like a dead-Node abort. It is reachable only once
// storageResetPrepareEndsByBudget is on (#1609).
func endComputerResetPrepare(ctx context.Context, tx *sql.Tx, reset computerStorageResetRow, code string, latchedFailure []byte, now time.Time) error {
	result, err := tx.ExecContext(ctx, `UPDATE computer_storage_resets SET status='failed', failure_code=?, completed_ns=?
		WHERE computer_id=? AND intent_revision=? AND status IN ('reserved', 'prepared')`,
		code, now.UnixNano(), reset.ComputerID, reset.IntentRevision)
	if err := requireSingleOperationEnd(result, err, "end failed Computer Storage reset"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE computer_storage_generations SET phase='retired', retired_ns=?
		WHERE computer_id=? AND reset_revision=? AND phase='staging'`, now.UnixNano(), reset.ComputerID, reset.IntentRevision); err != nil {
		return internalError(err, "retire failed Computer Storage reset staging generation")
	}
	if err := latchComputerOperationFailure(ctx, tx, reset.JobID, latchedFailure); err != nil {
		return err
	}
	result, err = tx.ExecContext(ctx, `UPDATE computers SET applied_revision=?, reconfiguration_phase='stable',
		reconfiguration_revision=NULL, updated_ns=? WHERE computer_id=? AND intent_revision=? AND storage_generation=?
		AND reconfiguration_phase='resetting'`, reset.IntentRevision, now.UnixNano(), reset.ComputerID,
		reset.IntentRevision, reset.OldGeneration)
	if err != nil {
		return internalError(err, "release failed Computer Storage reset")
	}
	return requireComputerCAS(result, reset.ComputerID, reset.IntentRevision)
}

// endComputerResetRetire ends a reset whose predecessor cannot be deleted.
// Like a restore, it returns the Computer to `stable` on the new generation,
// held stopped even when the reset would have resumed it: the reset contract
// resumes only after the predecessor is attested gone.
func endComputerResetRetire(ctx context.Context, tx *sql.Tx, reset computerStorageResetRow, code string, latchedFailure []byte, now time.Time) error {
	result, err := tx.ExecContext(ctx, `UPDATE computer_storage_resets SET status='cleanup_owed', failure_code=?, completed_ns=?
		WHERE computer_id=? AND intent_revision=? AND status='published'`,
		code, now.UnixNano(), reset.ComputerID, reset.IntentRevision)
	if err := requireSingleOperationEnd(result, err, "end Computer Storage reset with cleanup owed"); err != nil {
		return err
	}
	if err := latchComputerOperationFailure(ctx, tx, reset.JobID, latchedFailure); err != nil {
		return err
	}
	result, err = tx.ExecContext(ctx, `UPDATE computers SET applied_revision=?, reconfiguration_phase='stable',
		reconfiguration_revision=NULL, updated_ns=? WHERE computer_id=? AND intent_revision=? AND storage_generation=?
		AND reconfiguration_phase='resetting'`, reset.IntentRevision, now.UnixNano(), reset.ComputerID,
		reset.IntentRevision, reset.NewGeneration)
	if err != nil {
		return internalError(err, "release Computer Storage reset with cleanup owed")
	}
	return requireComputerCAS(result, reset.ComputerID, reset.IntentRevision)
}

// endComputerBackup ends a Backup that kept failing: the operation records
// `failed` and the Computer leaves `backing_up` through the same tail a
// failure receipt takes, resuming a Computer the Backup stopped.
func endComputerBackup(ctx context.Context, tx *sql.Tx, row computerBackupOperationRow, code string, now time.Time) error {
	result, err := tx.ExecContext(ctx, `UPDATE computer_backup_operations SET status='failed', failure_code=?, completed_ns=?
		WHERE computer_id=? AND operation_revision=? AND status='planned'`, code, now.UnixNano(), row.ComputerID, row.OperationRevision)
	if err := requireSingleOperationEnd(result, err, "end failed Computer Backup"); err != nil {
		return err
	}
	return releaseComputerBackupIntent(ctx, tx, row, now)
}

// endComputerGrow ends a grow that kept failing with the Storage budget
// unchanged, latched like a capacity refusal. A running Computer keeps
// running; the latch clears any pending restart.
func endComputerGrow(ctx context.Context, tx *sql.Tx, row computerStorageGrowRow, code string, latchedFailure []byte, now time.Time) error {
	result, err := tx.ExecContext(ctx, `UPDATE computer_storage_grows SET status='failed', failure_code=?, completed_ns=?
		WHERE computer_id=? AND operation_revision=? AND status='planned'`, code, now.UnixNano(), row.ComputerID, row.OperationRevision)
	if err := requireSingleOperationEnd(result, err, "end failed Computer grow"); err != nil {
		return err
	}
	if err := latchComputerOperationFailure(ctx, tx, row.JobID, latchedFailure); err != nil {
		return err
	}
	result, err = tx.ExecContext(ctx, `UPDATE computers SET applied_revision=?, reconfiguration_phase='stable',
		reconfiguration_revision=NULL, updated_ns=? WHERE computer_id=? AND intent_revision=?
		AND reconfiguration_phase='growing'`, row.OperationRevision, now.UnixNano(), row.ComputerID, row.OperationRevision)
	if err != nil {
		return internalError(err, "release failed Computer grow")
	}
	return requireComputerCAS(result, row.ComputerID, row.OperationRevision)
}

// endComputerStorageCopyLatched ends a clone or Custody import before
// publication through the shared terminal tail: the never-published
// generation retires and the destination leaves its phase latched failed.
func endComputerStorageCopyLatched(ctx context.Context, tx *sql.Tx, row computerStorageCopyRow,
	phase ComputerReconfigurationPhase, code string, latchedFailure []byte, now time.Time) error {
	result, err := tx.ExecContext(ctx, `UPDATE computer_storage_copy_operations SET status='failed', failure_code=?, completed_ns=?
		WHERE destination_computer_id=? AND operation_revision=? AND status IN ('reserved', 'prepared')`,
		code, now.UnixNano(), row.DestinationComputerID, row.OperationRevision)
	if err := requireSingleOperationEnd(result, err, "end failed Computer Storage copy"); err != nil {
		return err
	}
	return closeTerminalComputerCopy(ctx, tx, row, phase, latchedFailure, now)
}

// readComputerStorageOperationFailure projects the most recent failure
// budget of one operation, or nil when no attempt was ever reported.
func readComputerStorageOperationFailure(ctx context.Context, q queryer, computerID string, revision int64, verb ComputerStorageOperationVerb) (*ComputerStorageOperationFailure, error) {
	var failure ComputerStorageOperationFailure
	var firstNS sql.NullInt64
	var lastNS int64
	err := q.QueryRowContext(ctx, `SELECT stage, last_failure_class, last_failure_code, failed_attempts, first_failed_ns, last_failed_ns
		FROM computer_storage_operation_failures WHERE computer_id=? AND operation_revision=? AND verb=?
		ORDER BY last_failed_ns DESC LIMIT 1`, computerID, revision, string(verb)).Scan(&failure.Stage, &failure.Class,
		&failure.Code, &failure.FailedAttempts, &firstNS, &lastNS)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read Computer Storage operation failure: %w", err)
	}
	if firstNS.Valid {
		first := time.Unix(0, firstNS.Int64).UTC()
		failure.FirstFailedAt = &first
	}
	failure.LastFailedAt = time.Unix(0, lastNS).UTC()
	return &failure, nil
}
