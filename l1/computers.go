package l1

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Derek-X-Wang/wefty/contract"
)

const initialComputerRevision int64 = 1

type ComputerIntentOperation string

const (
	ComputerIntentCreate  ComputerIntentOperation = "create"
	ComputerIntentStart   ComputerIntentOperation = "start"
	ComputerIntentStop    ComputerIntentOperation = "stop"
	ComputerIntentRestart ComputerIntentOperation = "restart"
	ComputerIntentRemove  ComputerIntentOperation = "remove"
	ComputerIntentProject ComputerIntentOperation = "project"
)

type ComputerGrantPermission string

const (
	ComputerGrantNone    ComputerGrantPermission = "none"
	ComputerGrantView    ComputerGrantPermission = "view"
	ComputerGrantControl ComputerGrantPermission = "control"
)

type ComputerReconfigurationPhase string

const (
	ComputerReconfigurationStable     ComputerReconfigurationPhase = "stable"
	ComputerReconfigurationProjecting ComputerReconfigurationPhase = "projecting"
	ComputerReconfigurationRemoving   ComputerReconfigurationPhase = "removing"
)

type ComputerGrant struct {
	UserID     string                  `json:"user_id"`
	Permission ComputerGrantPermission `json:"permission"`
}

type ComputerIntent struct {
	IntentRevision    int64                        `json:"intent_revision"`
	Operation         ComputerIntentOperation      `json:"operation"`
	DesiredState      contract.ServiceDesiredState `json:"desired_state"`
	StorageID         string                       `json:"storage_id"`
	StorageGeneration int64                        `json:"storage_generation"`
	JobID             string                       `json:"job_id"`
	SpecRevision      int64                        `json:"spec_revision"`
	Actor             string                       `json:"actor"`
	CreatedAt         time.Time                    `json:"created_at"`
}

// Computer is the durable authority behind one current immutable service Job.
// Runtime state remains on CurrentJob; intent and identity survive projection
// replacement.
type Computer struct {
	ComputerID              string                       `json:"computer_id"`
	Name                    string                       `json:"name"`
	PlacementNodeID         string                       `json:"placement_node_id"`
	BoundNodeID             string                       `json:"bound_node_id,omitempty"`
	Grants                  []ComputerGrant              `json:"grants"`
	StorageID               string                       `json:"storage_id"`
	StorageGeneration       int64                        `json:"storage_generation"`
	DesiredState            contract.ServiceDesiredState `json:"desired_state"`
	IntentRevision          int64                        `json:"intent_revision"`
	AppliedRevision         int64                        `json:"applied_revision"`
	CurrentJobID            string                       `json:"current_job_id"`
	CurrentSpecRevision     int64                        `json:"current_spec_revision"`
	ReconfigurationPhase    ComputerReconfigurationPhase `json:"reconfiguration_phase"`
	ReconfigurationRevision *int64                       `json:"reconfiguration_revision,omitempty"`
	CurrentJob              Job                          `json:"current_job"`
	CreatedAt               time.Time                    `json:"created_at"`
	UpdatedAt               time.Time                    `json:"updated_at"`
}

type CreateComputerRequest struct {
	Name  string           `json:"name"`
	Spec  contract.JobSpec `json:"spec"`
	Actor string           `json:"-"`
}

type ComputerMutationPrecondition struct {
	IntentRevision    int64  `json:"intent_revision"`
	StorageID         string `json:"storage_id"`
	StorageGeneration int64  `json:"storage_generation"`
	Actor             string `json:"-"`
}

type ComputerDesiredStateRequest struct {
	ComputerMutationPrecondition
	DesiredState contract.ServiceDesiredState `json:"desired_state"`
}

type ComputerRemoveRequest struct {
	ComputerMutationPrecondition
}

type ComputerProjectionRequest struct {
	ComputerMutationPrecondition
	Spec contract.JobSpec `json:"spec"`
}

type ComputerRestartRequest struct {
	ComputerMutationPrecondition
	IdempotencyKey string `json:"idempotency_key"`
}

type ComputerIntentList struct {
	Intents    []ComputerIntent `json:"intents"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

func isComputerSpec(spec contract.JobSpec) bool {
	return spec.Kind == contract.JobKindOCI && spec.Execution.OCI != nil && spec.Execution.OCI.Computer != nil
}

func computerPlacementNodeID(spec contract.JobSpec) string {
	for _, tag := range spec.RoutingTags {
		tag = strings.ToLower(strings.TrimSpace(tag))
		if strings.HasPrefix(tag, contract.StableNodeTagPrefix) {
			return strings.TrimPrefix(tag, contract.StableNodeTagPrefix)
		}
	}
	return ""
}

func validateComputerNameAndActor(name, actor string) error {
	if strings.TrimSpace(name) == "" || strings.TrimSpace(actor) == "" {
		return protocolError(contract.ErrorInvalidRequest, "Computer name and actor are required")
	}
	if utf8.RuneCountInString(name) > 255 || utf8.RuneCountInString(actor) > 255 {
		return protocolError(contract.ErrorInvalidRequest, "Computer name or actor exceeds 255 characters")
	}
	return nil
}

func validateComputerPrecondition(computer Computer, request ComputerMutationPrecondition) error {
	if request.IntentRevision < 1 || request.StorageGeneration < 1 ||
		strings.TrimSpace(request.StorageID) == "" || strings.TrimSpace(request.Actor) == "" {
		return protocolError(contract.ErrorInvalidRequest,
			"intent_revision, storage_id, storage_generation, and actor are required")
	}
	if computer.IntentRevision != request.IntentRevision {
		return protocolErrorWithDetails(contract.ErrorStaleIntentRevision, map[string]any{
			"computer_id": computer.ComputerID, "expected_revision": computer.IntentRevision,
			"observed_revision": request.IntentRevision,
		}, "Computer %q intent revision changed from %d to %d",
			computer.ComputerID, request.IntentRevision, computer.IntentRevision)
	}
	if computer.StorageID != request.StorageID || computer.StorageGeneration != request.StorageGeneration {
		return protocolErrorWithDetails(contract.ErrorStorageReferenceConflict, map[string]any{
			"computer_id": computer.ComputerID, "expected_storage_id": computer.StorageID,
			"expected_storage_generation": computer.StorageGeneration,
		}, "Computer %q storage reference has changed", computer.ComputerID)
	}
	return nil
}

func requireComputerCAS(result sql.Result, computerID string, observedRevision int64) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return internalError(err, "read Computer CAS result")
	}
	if affected != 1 {
		return protocolErrorWithDetails(contract.ErrorStaleIntentRevision, map[string]any{
			"computer_id": computerID, "observed_revision": observedRevision,
		}, "Computer %q intent revision changed from %d", computerID, observedRevision)
	}
	return nil
}

// CreateComputer creates the durable resource and its first immutable Job in
// one transaction. Bare Computer-trait Job creation is rejected by CreateJob
// so no runtime projection can exist without this authority row.
func (s *Store) CreateComputer(ctx context.Context, request CreateComputerRequest) (Computer, bool, error) {
	request.Name = strings.TrimSpace(request.Name)
	request.Actor = strings.TrimSpace(request.Actor)
	if err := validateComputerNameAndActor(request.Name, request.Actor); err != nil {
		return Computer{}, false, err
	}
	request.Spec.RoutingTags = NormalizeTags(request.Spec.RoutingTags)
	if err := validateJobSpec(&request.Spec); err != nil {
		return Computer{}, false, err
	}
	if !isComputerSpec(request.Spec) {
		return Computer{}, false, protocolError(contract.ErrorInvalidRequest,
			"Computer creation requires execution.oci.computer")
	}
	placementNodeID := computerPlacementNodeID(request.Spec)
	if placementNodeID == "" {
		return Computer{}, false, protocolError(contract.ErrorInvalidRequest,
			"Computer placement node is required")
	}
	specJSON, requestHash, err := encodeJobSpec(request.Spec)
	if err != nil {
		return Computer{}, false, err
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Computer{}, false, internalError(err, "begin Computer creation")
	}
	defer tx.Rollback()

	if existing, storedHash, readErr := getJobByDispatchKey(ctx, tx, request.Spec.DispatchKey, now); readErr == nil {
		if storedHash != requestHash {
			return Computer{}, false, protocolError(contract.ErrorDispatchKeyConflict,
				"dispatch key %q was already used with a different job", request.Spec.DispatchKey)
		}
		computer, computerErr := readComputerByJobID(ctx, tx, existing.JobID, now)
		var creationActor string
		if computerErr == nil {
			computerErr = tx.QueryRowContext(ctx, `SELECT actor FROM computer_intent_history
				WHERE computer_id=? AND intent_revision=?`, computer.ComputerID, initialComputerRevision).Scan(&creationActor)
		}
		if computerErr != nil || computer.Name != request.Name || creationActor != request.Actor {
			return Computer{}, false, protocolError(contract.ErrorDispatchKeyConflict,
				"dispatch key %q does not identify this Computer", request.Spec.DispatchKey)
		}
		return computer, true, nil
	} else if !errors.Is(readErr, sql.ErrNoRows) {
		return Computer{}, false, internalError(readErr, "read Computer dispatch key")
	}
	if tombstone, tombstoneErr := readServiceTombstoneByDispatchHash(ctx, tx, hashDispatchKey(request.Spec.DispatchKey)); tombstoneErr == nil {
		if tombstone.requestHash != requestHash {
			return Computer{}, false, protocolError(contract.ErrorDispatchKeyConflict,
				"dispatch key %q was already used with a different job", request.Spec.DispatchKey)
		}
		// CreateJob can replay an ordinary removed-service tombstone, but a
		// Computer dispatch key cannot identify a Job-only tombstone: Computer
		// removal retains the durable Computer and its historical projections.
		// Keep the divergence explicit until the composite removal ticket owns a
		// Computer-specific terminal representation.
		return Computer{}, false, protocolError(contract.ErrorDispatchKeyConflict,
			"dispatch key %q belongs to a removed service", request.Spec.DispatchKey)
	} else if !errors.Is(tombstoneErr, sql.ErrNoRows) {
		return Computer{}, false, internalError(tombstoneErr, "read removed Computer dispatch key")
	}
	var existingID string
	if err := tx.QueryRowContext(ctx, "SELECT computer_id FROM computers WHERE name=?", request.Name).Scan(&existingID); err == nil {
		return Computer{}, false, protocolError(contract.ErrorConflict,
			"Computer name %q is already used by %q", request.Name, existingID)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return Computer{}, false, internalError(err, "check Computer name")
	}

	job, err := insertComputerJob(ctx, tx, request.Spec, specJSON, requestHash,
		contract.JobQueued, contract.ServiceDesiredRunning, "", now)
	if err != nil {
		return Computer{}, false, err
	}
	computerID := newID("computer")
	storageID := newID("storage")
	grantsJSON := []byte("[]")
	if _, err := tx.ExecContext(ctx, `INSERT INTO computers(
		computer_id, name, placement_node_id, grants_json, storage_id, storage_generation,
		desired_state, intent_revision, applied_revision, current_job_id, current_spec_revision,
		reconfiguration_phase, created_ns, updated_ns
	) VALUES(?, ?, ?, ?, ?, 1, ?, 1, 1, ?, 1, ?, ?, ?)`,
		computerID, request.Name, placementNodeID, grantsJSON, storageID,
		contract.ServiceDesiredRunning, job.JobID, ComputerReconfigurationStable,
		now.UnixNano(), now.UnixNano()); err != nil {
		return Computer{}, false, internalError(err, "store Computer")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_job_projections(
		computer_id, job_id, spec_revision, current, created_ns
	) VALUES(?, ?, 1, 1, ?)`, computerID, job.JobID, now.UnixNano()); err != nil {
		return Computer{}, false, internalError(err, "store initial Computer projection")
	}
	if err := insertComputerIntent(ctx, tx, computerID, initialComputerRevision, ComputerIntentCreate,
		contract.ServiceDesiredRunning, storageID, 1, job.JobID, 1, request.Actor, now); err != nil {
		return Computer{}, false, err
	}
	computer, err := readComputerAuthority(ctx, tx, computerID, now)
	if err != nil {
		return Computer{}, false, internalError(err, "read created Computer")
	}
	if err := tx.Commit(); err != nil {
		return Computer{}, false, internalError(err, "commit Computer creation")
	}
	return computer, false, nil
}

func encodeJobSpec(spec contract.JobSpec) ([]byte, string, error) {
	encoded, err := json.Marshal(spec)
	if err != nil {
		return nil, "", internalError(err, "encode job specification")
	}
	hash := sha256.Sum256(encoded)
	return encoded, hex.EncodeToString(hash[:]), nil
}

func insertComputerJob(
	ctx context.Context,
	tx *sql.Tx,
	spec contract.JobSpec,
	specJSON []byte,
	requestHash string,
	state contract.JobState,
	desired contract.ServiceDesiredState,
	boundNodeID string,
	now time.Time,
) (Job, error) {
	job := Job{JobID: newID("job"), State: state, Spec: spec, CreatedAt: now, UpdatedAt: now}
	if _, err := tx.ExecContext(ctx, `INSERT INTO jobs(
		job_id, dispatch_key, request_hash, spec_json, state, created_ns, updated_ns
	) VALUES(?, ?, ?, ?, ?, ?, ?)`, job.JobID, spec.DispatchKey, requestHash, specJSON,
		state, now.UnixNano(), now.UnixNano()); err != nil {
		return Job{}, internalError(err, "store Computer Job projection")
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO job_log_jsonl(job_id, jsonl) VALUES(?, ?)", job.JobID, []byte{}); err != nil {
		return Job{}, internalError(err, "initialize Computer Job log")
	}
	for _, capability := range RequiredCapabilities(spec) {
		if _, err := tx.ExecContext(ctx, "INSERT INTO job_required_capabilities(job_id, capability) VALUES(?, ?)", job.JobID, capability); err != nil {
			return Job{}, internalError(err, "store Computer Job capability")
		}
	}
	var binding any
	if boundNodeID != "" {
		binding = boundNodeID
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO service_jobs(job_id, desired_state, bound_node_id)
		VALUES(?, ?, ?)`, job.JobID, desired, binding); err != nil {
		return Job{}, internalError(err, "initialize Computer service projection")
	}
	for _, tag := range spec.RoutingTags {
		if _, err := tx.ExecContext(ctx, "INSERT INTO job_tags(job_id, tag) VALUES(?, ?)", job.JobID, tag); err != nil {
			return Job{}, internalError(err, "store Computer Job routing tag")
		}
	}
	job.ServiceJob = &ServiceJob{DesiredState: desired, BoundNodeID: boundNodeID}
	return job, nil
}

func insertComputerIntent(
	ctx context.Context,
	tx *sql.Tx,
	computerID string,
	revision int64,
	operation ComputerIntentOperation,
	desired contract.ServiceDesiredState,
	storageID string,
	storageGeneration int64,
	jobID string,
	specRevision int64,
	actor string,
	now time.Time,
) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_intent_history(
		computer_id, intent_revision, operation, desired_state, storage_id,
		storage_generation, job_id, spec_revision, actor, created_ns
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, computerID, revision, operation, desired,
		storageID, storageGeneration, jobID, specRevision, actor, now.UnixNano()); err != nil {
		return internalError(err, "append immutable Computer intent")
	}
	return nil
}

func (s *Store) GetComputer(ctx context.Context, computerID string) (Computer, error) {
	if strings.TrimSpace(computerID) == "" {
		return Computer{}, protocolError(contract.ErrorInvalidRequest, "computer_id is required")
	}
	computer, err := readComputerAuthority(ctx, s.db, computerID, canonicalTime(s.clock.Now()))
	if errors.Is(err, sql.ErrNoRows) {
		return Computer{}, protocolError(contract.ErrorNotFound, "Computer %q was not found", computerID)
	}
	if err != nil {
		return Computer{}, internalError(err, "read Computer")
	}
	return computer, nil
}

func readComputerByJobID(ctx context.Context, q queryer, jobID string, now time.Time) (Computer, error) {
	var computerID string
	if err := q.QueryRowContext(ctx, `SELECT computer_id FROM computer_job_projections WHERE job_id=?`, jobID).Scan(&computerID); err != nil {
		return Computer{}, err
	}
	return readComputerAuthority(ctx, q, computerID, now)
}

// readComputerAuthority materializes only the bounded authority row and its
// current Job observation. Immutable intent history has its own paginated
// reader so a CAS mutation never grows with the age of the Computer.
func readComputerAuthority(ctx context.Context, q queryer, computerID string, now time.Time) (Computer, error) {
	var computer Computer
	var grantsJSON []byte
	var boundNodeID sql.NullString
	var reconfigurationRevision sql.NullInt64
	var createdNS, updatedNS int64
	err := q.QueryRowContext(ctx, `SELECT computer_id, name, placement_node_id, bound_node_id,
		grants_json, storage_id, storage_generation, desired_state, intent_revision,
		applied_revision, current_job_id, current_spec_revision, reconfiguration_phase,
		reconfiguration_revision, created_ns, updated_ns
		FROM computers WHERE computer_id=?`, computerID).Scan(
		&computer.ComputerID, &computer.Name, &computer.PlacementNodeID, &boundNodeID,
		&grantsJSON, &computer.StorageID, &computer.StorageGeneration, &computer.DesiredState,
		&computer.IntentRevision, &computer.AppliedRevision, &computer.CurrentJobID,
		&computer.CurrentSpecRevision, &computer.ReconfigurationPhase,
		&reconfigurationRevision, &createdNS, &updatedNS)
	if err != nil {
		return Computer{}, err
	}
	if boundNodeID.Valid {
		computer.BoundNodeID = boundNodeID.String
	}
	if err := json.Unmarshal(grantsJSON, &computer.Grants); err != nil {
		return Computer{}, fmt.Errorf("decode Computer grants: %w", err)
	}
	if computer.Grants == nil {
		computer.Grants = []ComputerGrant{}
	}
	if reconfigurationRevision.Valid {
		value := reconfigurationRevision.Int64
		computer.ReconfigurationRevision = &value
	}
	computer.CreatedAt = time.Unix(0, createdNS).UTC()
	computer.UpdatedAt = time.Unix(0, updatedNS).UTC()
	job, err := getJobByID(ctx, q, computer.CurrentJobID, now)
	if err != nil {
		return Computer{}, err
	}
	if job.ServiceJob != nil {
		job.ServiceJob.SlotHeld = job.ServiceJob.HoldsSlot(job.State)
	}
	job.Status = string(job.State)
	computer.CurrentJob = job
	return computer, nil
}

func queryComputerIntents(ctx context.Context, q queryer, computerID string, afterRevision int64, limit int) ([]ComputerIntent, error) {
	type rowsQueryer interface {
		QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	}
	rowsSource, ok := q.(rowsQueryer)
	if !ok {
		return nil, fmt.Errorf("query source cannot list Computer intents")
	}
	rows, err := rowsSource.QueryContext(ctx, `SELECT intent_revision, operation, desired_state,
		storage_id, storage_generation, job_id, spec_revision, actor, created_ns
		FROM computer_intent_history WHERE computer_id=? AND intent_revision>?
		ORDER BY intent_revision LIMIT ?`, computerID, afterRevision, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	intents := []ComputerIntent{}
	for rows.Next() {
		var intent ComputerIntent
		var createdNS int64
		if err := rows.Scan(&intent.IntentRevision, &intent.Operation, &intent.DesiredState,
			&intent.StorageID, &intent.StorageGeneration, &intent.JobID, &intent.SpecRevision,
			&intent.Actor, &createdNS); err != nil {
			return nil, err
		}
		intent.CreatedAt = time.Unix(0, createdNS).UTC()
		intents = append(intents, intent)
	}
	return intents, rows.Err()
}

func encodeComputerIntentCursor(revision int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(revision, 10)))
}

func decodeComputerIntentCursor(value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return 0, protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	revision, err := strconv.ParseInt(string(payload), 10, 64)
	if err != nil || revision < 0 {
		return 0, protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	return revision, nil
}

func (s *Store) ListComputerIntents(ctx context.Context, computerID, cursorValue string, limit int) (ComputerIntentList, error) {
	if strings.TrimSpace(computerID) == "" {
		return ComputerIntentList{}, protocolError(contract.ErrorInvalidRequest, "computer_id is required")
	}
	if limit < 1 || limit > MaxJobPageLimit {
		return ComputerIntentList{}, protocolError(contract.ErrorInvalidRequest,
			"limit must be between 1 and %d", MaxJobPageLimit)
	}
	afterRevision, err := decodeComputerIntentCursor(cursorValue)
	if err != nil {
		return ComputerIntentList{}, err
	}
	var exists bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM computers WHERE computer_id=?)`, computerID).Scan(&exists); err != nil {
		return ComputerIntentList{}, internalError(err, "read Computer intent authority")
	}
	if !exists {
		return ComputerIntentList{}, protocolError(contract.ErrorNotFound, "Computer %q was not found", computerID)
	}
	intents, err := queryComputerIntents(ctx, s.db, computerID, afterRevision, limit+1)
	if err != nil {
		return ComputerIntentList{}, internalError(err, "list Computer intents")
	}
	page := ComputerIntentList{Intents: intents}
	if len(page.Intents) > limit {
		page.Intents = page.Intents[:limit]
		page.NextCursor = encodeComputerIntentCursor(page.Intents[len(page.Intents)-1].IntentRevision)
	}
	return page, nil
}

func computerIDForJob(ctx context.Context, q queryer, jobID string) (string, bool, error) {
	var computerID string
	err := q.QueryRowContext(ctx, "SELECT computer_id FROM computer_job_projections WHERE job_id=?", jobID).Scan(&computerID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, internalError(err, "read Computer Job ownership")
	}
	return computerID, true, nil
}

func markComputerIntentApplied(ctx context.Context, tx *sql.Tx, computerID string, revision int64, now time.Time) error {
	result, err := tx.ExecContext(ctx, `UPDATE computers SET applied_revision=?, updated_ns=?
		WHERE computer_id=? AND intent_revision=? AND applied_revision<?`,
		revision, now.UnixNano(), computerID, revision, revision)
	if err != nil {
		return internalError(err, "advance applied Computer intent")
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return internalError(err, "read applied Computer intent result")
	}
	if affected != 1 {
		return internalError(fmt.Errorf("Computer %s revision %d was not pending apply", computerID, revision),
			"advance applied Computer intent")
	}
	return nil
}

func (s *Store) SetComputerDesiredState(ctx context.Context, computerID string, request ComputerDesiredStateRequest) (Computer, error) {
	if request.DesiredState != contract.ServiceDesiredRunning && request.DesiredState != contract.ServiceDesiredStopped {
		return Computer{}, protocolError(contract.ErrorInvalidRequest,
			"desired_state must be %q or %q", contract.ServiceDesiredRunning, contract.ServiceDesiredStopped)
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Computer{}, internalError(err, "begin Computer desired-state mutation")
	}
	defer tx.Rollback()
	computer, err := readComputerAuthority(ctx, tx, computerID, now)
	if errors.Is(err, sql.ErrNoRows) {
		return Computer{}, protocolError(contract.ErrorNotFound, "Computer %q was not found", computerID)
	}
	if err != nil {
		return Computer{}, internalError(err, "read Computer desired-state target")
	}
	if err := validateComputerPrecondition(computer, request.ComputerMutationPrecondition); err != nil {
		return Computer{}, err
	}
	if computer.DesiredState == contract.ServiceDesiredRemoved {
		return Computer{}, protocolError(contract.ErrorConflict, "Computer %q is being removed", computerID)
	}
	if computer.ReconfigurationPhase != ComputerReconfigurationStable {
		return Computer{}, protocolError(contract.ErrorConflict,
			"Computer %q is in reconfiguration phase %q", computerID, computer.ReconfigurationPhase)
	}
	if request.DesiredState == contract.ServiceDesiredRunning && computer.CurrentJob.State == contract.JobFailed {
		return Computer{}, protocolErrorWithDetails(contract.ErrorConflict, map[string]any{
			"computer_id": computerID, "required_operation": "restart",
		}, "Computer %q is latched failed; use POST /v1/computers/%s/restart", computerID, computerID)
	}
	if computer.DesiredState == request.DesiredState {
		return computer, nil
	}
	nextRevision := computer.IntentRevision + 1
	result, err := tx.ExecContext(ctx, `UPDATE computers SET desired_state=?, intent_revision=?, updated_ns=?
		WHERE computer_id=? AND intent_revision=?`, request.DesiredState, nextRevision, now.UnixNano(), computerID,
		computer.IntentRevision)
	if err != nil {
		return Computer{}, internalError(err, "store Computer desired state")
	}
	if err := requireComputerCAS(result, computerID, computer.IntentRevision); err != nil {
		return Computer{}, err
	}
	operation := ComputerIntentStart
	if request.DesiredState == contract.ServiceDesiredStopped {
		operation = ComputerIntentStop
	}
	if err := insertComputerIntent(ctx, tx, computerID, nextRevision, operation,
		request.DesiredState, computer.StorageID, computer.StorageGeneration,
		computer.CurrentJobID, computer.CurrentSpecRevision, request.Actor, now); err != nil {
		return Computer{}, err
	}
	if err := setComputerServiceDesiredState(ctx, tx, computer.CurrentJob, request.DesiredState, now); err != nil {
		return Computer{}, err
	}
	if err := markComputerIntentApplied(ctx, tx, computerID, nextRevision, now); err != nil {
		return Computer{}, err
	}
	updated, err := readComputerAuthority(ctx, tx, computerID, now)
	if err != nil {
		return Computer{}, internalError(err, "read Computer desired-state result")
	}
	if err := tx.Commit(); err != nil {
		return Computer{}, internalError(err, "commit Computer desired state")
	}
	return updated, nil
}

func setComputerServiceDesiredState(
	ctx context.Context,
	tx *sql.Tx,
	job Job,
	desired contract.ServiceDesiredState,
	now time.Time,
) error {
	if job.ServiceJob == nil {
		return internalError(errors.New("Computer current Job is not a service"), "apply Computer desired state")
	}
	switch desired {
	case contract.ServiceDesiredRunning:
		switch job.State {
		case contract.JobFailed:
			return protocolError(contract.ErrorConflict, "Computer Job %q is latched failed; restart is required", job.JobID)
		case contract.JobStopped:
			if !job.HoldsSlot(job.State) {
				if err := ensureBoundServiceCapacity(ctx, tx, job); err != nil {
					return err
				}
			}
			if err := transitionServiceJob(ctx, tx, job.JobID, desired, contract.JobQueued, now); err != nil {
				return err
			}
		case contract.JobStopping:
			return protocolError(contract.ErrorConflict, "Computer Job %q is still stopping", job.JobID)
		case contract.JobQueued, contract.JobClaimed, contract.JobRunning:
			if job.DesiredState != contract.ServiceDesiredRunning {
				return protocolError(contract.ErrorConflict, "Computer Job %q has inconsistent desired state", job.JobID)
			}
		default:
			return protocolError(contract.ErrorConflict, "Computer Job %q cannot start from %q", job.JobID, job.State)
		}
	case contract.ServiceDesiredStopped:
		switch job.State {
		case contract.JobQueued:
			if err := transitionServiceJob(ctx, tx, job.JobID, desired, contract.JobStopped, now); err != nil {
				return err
			}
		case contract.JobClaimed, contract.JobRunning:
			if err := transitionServiceJob(ctx, tx, job.JobID, desired, contract.JobStopping, now); err != nil {
				return err
			}
		case contract.JobFailed:
			if _, err := tx.ExecContext(ctx, `UPDATE service_jobs SET desired_state=?, next_restart_at=NULL,
				published_attempt_id=NULL, healthy_since_ns=NULL WHERE job_id=?`, desired, job.JobID); err != nil {
				return internalError(err, "stop latched Computer Job")
			}
		case contract.JobStopping, contract.JobStopped:
			if job.DesiredState != contract.ServiceDesiredStopped {
				return protocolError(contract.ErrorConflict, "Computer Job %q has inconsistent desired state", job.JobID)
			}
		default:
			return protocolError(contract.ErrorConflict, "Computer Job %q cannot stop from %q", job.JobID, job.State)
		}
	}
	if _, err := tx.ExecContext(ctx, "UPDATE service_jobs SET next_restart_at=NULL WHERE job_id=?", job.JobID); err != nil {
		return internalError(err, "clear Computer service backoff")
	}
	return nil
}

func computerRestartRequestHash(request ComputerRestartRequest) (string, error) {
	payload, err := json.Marshal(struct {
		IntentRevision    int64  `json:"intent_revision"`
		StorageID         string `json:"storage_id"`
		StorageGeneration int64  `json:"storage_generation"`
		Actor             string `json:"actor"`
		IdempotencyKey    string `json:"idempotency_key"`
	}{
		IntentRevision: request.IntentRevision, StorageID: request.StorageID,
		StorageGeneration: request.StorageGeneration, Actor: request.Actor,
		IdempotencyKey: request.IdempotencyKey,
	})
	if err != nil {
		return "", internalError(err, "encode Computer restart request")
	}
	hash := sha256.Sum256(payload)
	return hex.EncodeToString(hash[:]), nil
}

// RestartComputer clears a stopped or latched-failed current projection under
// Computer CAS. Direct Job restart remains forbidden so the durable resource,
// not an immutable observation, owns the fresh-attempt decision.
func (s *Store) RestartComputer(ctx context.Context, computerID string, request ComputerRestartRequest) (Computer, bool, error) {
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	if request.IdempotencyKey == "" {
		return Computer{}, false, protocolError(contract.ErrorInvalidRequest, "idempotency_key is required")
	}
	requestHash, err := computerRestartRequestHash(request)
	if err != nil {
		return Computer{}, false, err
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Computer{}, false, internalError(err, "begin Computer restart")
	}
	defer tx.Rollback()
	computer, err := readComputerAuthority(ctx, tx, computerID, now)
	if errors.Is(err, sql.ErrNoRows) {
		return Computer{}, false, protocolError(contract.ErrorNotFound, "Computer %q was not found", computerID)
	}
	if err != nil {
		return Computer{}, false, internalError(err, "read Computer restart target")
	}
	var storedHash string
	err = tx.QueryRowContext(ctx, `SELECT request_hash FROM service_restart_requests
		WHERE job_id=? AND idempotency_key=?`, computer.CurrentJobID, request.IdempotencyKey).Scan(&storedHash)
	if err == nil {
		if storedHash != requestHash {
			return Computer{}, false, protocolError(contract.ErrorIdempotencyConflict,
				"Computer restart idempotency key conflicts with the accepted request")
		}
		return computer, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Computer{}, false, internalError(err, "read Computer restart replay")
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
	if computer.CurrentJob.State != contract.JobStopped && computer.CurrentJob.State != contract.JobFailed {
		return Computer{}, false, protocolError(contract.ErrorConflict,
			"Computer %q can restart only from stopped or failed, not %q", computerID, computer.CurrentJob.State)
	}
	if !computer.CurrentJob.HoldsSlot(computer.CurrentJob.State) {
		if err := ensureBoundServiceCapacity(ctx, tx, computer.CurrentJob); err != nil {
			return Computer{}, false, err
		}
	}
	nextRevision := computer.IntentRevision + 1
	result, err := tx.ExecContext(ctx, `UPDATE computers SET desired_state=?, intent_revision=?, updated_ns=?
		WHERE computer_id=? AND intent_revision=?`, contract.ServiceDesiredRunning, nextRevision,
		now.UnixNano(), computerID, computer.IntentRevision)
	if err != nil {
		return Computer{}, false, internalError(err, "store Computer restart intent")
	}
	if err := requireComputerCAS(result, computerID, computer.IntentRevision); err != nil {
		return Computer{}, false, err
	}
	if err := insertComputerIntent(ctx, tx, computerID, nextRevision, ComputerIntentRestart,
		contract.ServiceDesiredRunning, computer.StorageID, computer.StorageGeneration,
		computer.CurrentJobID, computer.CurrentSpecRevision, request.Actor, now); err != nil {
		return Computer{}, false, err
	}
	if err := transitionServiceJob(ctx, tx, computer.CurrentJobID, contract.ServiceDesiredRunning, contract.JobQueued, now); err != nil {
		return Computer{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE service_jobs SET desired_state=?, restart_streak=0,
		next_restart_at=NULL, last_failure=NULL, healthy_since_ns=NULL, published_attempt_id=NULL
		WHERE job_id=?`, contract.ServiceDesiredRunning, computer.CurrentJobID); err != nil {
		return Computer{}, false, internalError(err, "reset Computer restart policy")
	}
	restartCreatedNS := now.UnixNano()
	if computer.CurrentJob.CurrentAttemptID != "" {
		var attemptCreatedNS int64
		if err := tx.QueryRowContext(ctx, "SELECT created_ns FROM attempts WHERE attempt_id=?",
			computer.CurrentJob.CurrentAttemptID).Scan(&attemptCreatedNS); err != nil {
			return Computer{}, false, internalError(err, "read restarted Computer attempt creation time")
		}
		if attemptCreatedNS > restartCreatedNS {
			restartCreatedNS = attemptCreatedNS
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO service_restart_requests(job_id, idempotency_key, request_hash, created_ns)
		VALUES(?, ?, ?, ?)`, computer.CurrentJobID, request.IdempotencyKey, requestHash, restartCreatedNS); err != nil {
		return Computer{}, false, internalError(err, "record durable Computer restart")
	}
	if err := markComputerIntentApplied(ctx, tx, computerID, nextRevision, now); err != nil {
		return Computer{}, false, err
	}
	updated, err := readComputerAuthority(ctx, tx, computerID, now)
	if err != nil {
		return Computer{}, false, internalError(err, "read Computer restart result")
	}
	if err := tx.Commit(); err != nil {
		return Computer{}, false, internalError(err, "commit Computer restart")
	}
	return updated, false, nil
}

// RemoveComputer records irreversible intent and revokes every current attempt
// and future claim. Composite managed-resource deletion belongs to the later
// Computer removal ticket; this method deliberately retains the durable row.
func (s *Store) RemoveComputer(ctx context.Context, computerID string, request ComputerRemoveRequest) (Computer, error) {
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Computer{}, internalError(err, "begin Computer removal")
	}
	defer tx.Rollback()
	computer, err := readComputerAuthority(ctx, tx, computerID, now)
	if errors.Is(err, sql.ErrNoRows) {
		return Computer{}, protocolError(contract.ErrorNotFound, "Computer %q was not found", computerID)
	}
	if err != nil {
		return Computer{}, internalError(err, "read Computer removal target")
	}
	if err := validateComputerPrecondition(computer, request.ComputerMutationPrecondition); err != nil {
		return Computer{}, err
	}
	if computer.DesiredState == contract.ServiceDesiredRemoved {
		return computer, nil
	}
	if computer.ReconfigurationPhase != ComputerReconfigurationStable &&
		computer.ReconfigurationPhase != ComputerReconfigurationProjecting {
		return Computer{}, protocolError(contract.ErrorConflict,
			"Computer %q is in reconfiguration phase %q", computerID, computer.ReconfigurationPhase)
	}
	nextRevision := computer.IntentRevision + 1
	result, err := tx.ExecContext(ctx, `UPDATE computers SET desired_state=?, intent_revision=?,
		reconfiguration_phase=?, reconfiguration_revision=?, updated_ns=?
		WHERE computer_id=? AND intent_revision=?`, contract.ServiceDesiredRemoved, nextRevision,
		ComputerReconfigurationRemoving, nextRevision, now.UnixNano(), computerID, computer.IntentRevision)
	if err != nil {
		return Computer{}, internalError(err, "store Computer removal intent")
	}
	if err := requireComputerCAS(result, computerID, computer.IntentRevision); err != nil {
		return Computer{}, err
	}
	if err := insertComputerIntent(ctx, tx, computerID, nextRevision, ComputerIntentRemove,
		contract.ServiceDesiredRemoved, computer.StorageID, computer.StorageGeneration,
		computer.CurrentJobID, computer.CurrentSpecRevision, request.Actor, now); err != nil {
		return Computer{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE attempts SET state=?, lease_expires_ns=MIN(lease_expires_ns, ?), updated_ns=?
		WHERE job_id IN (SELECT job_id FROM computer_job_projections WHERE computer_id=?)
		AND state IN (?, ?, ?)`, contract.AttemptLost, now.UnixNano(), now.UnixNano(), computerID,
		contract.AttemptClaimed, contract.AttemptRunning, contract.AttemptAwaitingInput); err != nil {
		return Computer{}, internalError(err, "revoke Computer attempt authority")
	}
	if err := scrubComputerControllerState(ctx, tx, computerID); err != nil {
		return Computer{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE service_jobs SET desired_state=?, published_attempt_id=NULL,
		healthy_since_ns=NULL, next_restart_at=NULL WHERE job_id=?`,
		contract.ServiceDesiredStopped, computer.CurrentJobID); err != nil {
		return Computer{}, internalError(err, "withdraw Computer service projection")
	}
	boundNodeID := computer.CurrentJob.BoundNodeID
	if computer.BoundNodeID != boundNodeID {
		return Computer{}, protocolErrorWithDetails(contract.ErrorConflict, map[string]any{
			"computer_id": computerID, "computer_bound_node_id": computer.BoundNodeID,
			"job_bound_node_id": boundNodeID,
		}, "Computer %q and current Job binding diverged", computerID)
	}
	if boundNodeID == "" {
		if _, err := tx.ExecContext(ctx, `UPDATE jobs SET state=?, fence_counter=fence_counter+1,
			updated_ns=? WHERE job_id=?`, contract.JobRemovedVerified, now.UnixNano(), computer.CurrentJobID); err != nil {
			return Computer{}, internalError(err, "finalize never-bound Computer Job removal")
		}
	} else {
		var rootInstanceID string
		if err := tx.QueryRowContext(ctx, `SELECT root_instance_id FROM nodes WHERE node_id=?`, boundNodeID).Scan(&rootInstanceID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return Computer{}, protocolError(contract.ErrorConflict, "bound node %q was not found", boundNodeID)
			}
			return Computer{}, internalError(err, "read Computer removal managed-root authority")
		}
		if strings.TrimSpace(rootInstanceID) == "" {
			return Computer{}, protocolError(contract.ErrorConflict,
				"bound node %q has no registered managed-root instance", boundNodeID)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE jobs SET state=?, fence_counter=fence_counter+1,
			updated_ns=? WHERE job_id=?`, contract.JobRemovalPending, now.UnixNano(), computer.CurrentJobID); err != nil {
			return Computer{}, internalError(err, "foreclose Computer Job")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO service_removals(
			job_id, bound_node_id, removal_generation, cleanup_fence, root_instance_id, status, requested_ns
		) VALUES(?, ?, 1, ?, ?, ?, ?)`, computer.CurrentJobID, boundNodeID, newID("cleanup"),
			rootInstanceID, contract.JobRemovalPending, now.UnixNano()); err != nil {
			return Computer{}, internalError(err, "create durable Computer removal directive")
		}
	}
	if err := markComputerIntentApplied(ctx, tx, computerID, nextRevision, now); err != nil {
		return Computer{}, err
	}
	removed, err := readComputerAuthority(ctx, tx, computerID, now)
	if err != nil {
		return Computer{}, internalError(err, "read removed Computer")
	}
	if err := tx.Commit(); err != nil {
		return Computer{}, internalError(err, "commit Computer removal intent")
	}
	if err := s.checkpointSecretWAL(ctx); err != nil {
		return Computer{}, err
	}
	return removed, nil
}

func scrubComputerControllerState(ctx context.Context, tx *sql.Tx, computerID string) error {
	rows, err := tx.QueryContext(ctx, `SELECT jobs.job_id, jobs.spec_json
		FROM jobs JOIN computer_job_projections ON computer_job_projections.job_id=jobs.job_id
		WHERE computer_job_projections.computer_id=? ORDER BY computer_job_projections.spec_revision`, computerID)
	if err != nil {
		return internalError(err, "list Computer controller state")
	}
	type projection struct {
		jobID string
		spec  []byte
	}
	projections := []projection{}
	for rows.Next() {
		var item projection
		if err := rows.Scan(&item.jobID, &item.spec); err != nil {
			rows.Close()
			return internalError(err, "scan Computer controller state")
		}
		projections = append(projections, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return internalError(err, "iterate Computer controller state")
	}
	if err := rows.Close(); err != nil {
		return internalError(err, "close Computer controller state")
	}
	for _, item := range projections {
		scrubbed, err := scrubSensitiveSpec(item.spec)
		if err != nil {
			return internalError(err, "scrub Computer specification")
		}
		if _, err := tx.ExecContext(ctx, "UPDATE jobs SET spec_json=? WHERE job_id=?", scrubbed, item.jobID); err != nil {
			return internalError(err, "scrub Computer Job specification")
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM log_events WHERE job_id=?", item.jobID); err != nil {
			return internalError(err, "scrub Computer log events")
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM service_log_truncations WHERE job_id=?", item.jobID); err != nil {
			return internalError(err, "scrub Computer log truncation")
		}
		if _, err := tx.ExecContext(ctx, "UPDATE job_log_jsonl SET jsonl=X'' WHERE job_id=?", item.jobID); err != nil {
			return internalError(err, "scrub Computer authoritative log")
		}
	}
	return nil
}

// InstallComputerProjection records one projection intent, quiesces the
// current Job without changing authoritative Computer desired state, and
// atomically transfers authority once the old observation is stopped. A
// caller may repeat the same request while phase=projecting to finish after the
// agent has positively reaped an active attempt.
func (s *Store) InstallComputerProjection(ctx context.Context, computerID string, request ComputerProjectionRequest) (Computer, error) {
	request.Spec.RoutingTags = NormalizeTags(request.Spec.RoutingTags)
	if err := validateJobSpec(&request.Spec); err != nil {
		return Computer{}, err
	}
	if !isComputerSpec(request.Spec) {
		return Computer{}, protocolError(contract.ErrorInvalidRequest,
			"Computer projection requires execution.oci.computer")
	}
	specJSON, requestHash, err := encodeJobSpec(request.Spec)
	if err != nil {
		return Computer{}, err
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Computer{}, internalError(err, "begin Computer projection install")
	}
	defer tx.Rollback()
	computer, err := readComputerAuthority(ctx, tx, computerID, now)
	if errors.Is(err, sql.ErrNoRows) {
		return Computer{}, protocolError(contract.ErrorNotFound, "Computer %q was not found", computerID)
	}
	if err != nil {
		return Computer{}, internalError(err, "read Computer projection target")
	}
	if computer.DesiredState == contract.ServiceDesiredRemoved {
		return Computer{}, protocolError(contract.ErrorConflict, "Computer %q is being removed", computerID)
	}
	if computerPlacementNodeID(request.Spec) != computer.PlacementNodeID {
		return Computer{}, protocolError(contract.ErrorConflict,
			"Computer placement cannot change during projection replacement")
	}
	if request.Spec.Execution.OCI.Computer.DiskBytes != computer.CurrentJob.Spec.Execution.OCI.Computer.DiskBytes {
		return Computer{}, protocolError(contract.ErrorConflict,
			"Computer disk budget cannot change during projection replacement")
	}
	if computer.ReconfigurationPhase == ComputerReconfigurationProjecting {
		if err := validatePendingComputerProjection(ctx, tx, computer, request, requestHash); err != nil {
			return Computer{}, err
		}
		if computer.CurrentJob.State != contract.JobStopped {
			return computer, nil
		}
		if err := finalizeComputerProjectionTx(ctx, tx, computer, now); err != nil {
			return Computer{}, err
		}
		updated, err := readComputerAuthority(ctx, tx, computerID, now)
		if err != nil {
			return Computer{}, internalError(err, "read installed Computer projection")
		}
		if err := tx.Commit(); err != nil {
			return Computer{}, internalError(err, "commit Computer projection install")
		}
		return updated, nil
	}
	if computer.ReconfigurationPhase != ComputerReconfigurationStable {
		return Computer{}, protocolError(contract.ErrorConflict,
			"Computer %q is in reconfiguration phase %q", computerID, computer.ReconfigurationPhase)
	}
	if err := validateComputerPrecondition(computer, request.ComputerMutationPrecondition); err != nil {
		return Computer{}, err
	}
	if _, _, dispatchErr := getJobByDispatchKey(ctx, tx, request.Spec.DispatchKey, now); dispatchErr == nil {
		return Computer{}, protocolError(contract.ErrorDispatchKeyConflict,
			"dispatch key %q is already in use", request.Spec.DispatchKey)
	} else if !errors.Is(dispatchErr, sql.ErrNoRows) {
		return Computer{}, internalError(dispatchErr, "check Computer projection dispatch key")
	}
	if _, tombstoneErr := readServiceTombstoneByDispatchHash(ctx, tx, hashDispatchKey(request.Spec.DispatchKey)); tombstoneErr == nil {
		return Computer{}, protocolError(contract.ErrorDispatchKeyConflict,
			"dispatch key %q belongs to a removed service", request.Spec.DispatchKey)
	} else if !errors.Is(tombstoneErr, sql.ErrNoRows) {
		return Computer{}, internalError(tombstoneErr, "check removed Computer projection dispatch key")
	}
	job, err := insertComputerJob(ctx, tx, request.Spec, specJSON, requestHash,
		contract.JobStopped, contract.ServiceDesiredStopped, computer.BoundNodeID, now)
	if err != nil {
		return Computer{}, err
	}
	nextSpecRevision := computer.CurrentSpecRevision + 1
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_job_projections(
		computer_id, job_id, spec_revision, current, created_ns
	) VALUES(?, ?, ?, 0, ?)`, computerID, job.JobID, nextSpecRevision, now.UnixNano()); err != nil {
		return Computer{}, internalError(err, "store staging Computer Job projection")
	}
	nextRevision := computer.IntentRevision + 1
	result, err := tx.ExecContext(ctx, `UPDATE computers SET intent_revision=?, reconfiguration_phase=?,
		reconfiguration_revision=?, updated_ns=? WHERE computer_id=? AND intent_revision=?`,
		nextRevision, ComputerReconfigurationProjecting, nextRevision, now.UnixNano(), computerID, computer.IntentRevision)
	if err != nil {
		return Computer{}, internalError(err, "store Computer projection intent")
	}
	if err := requireComputerCAS(result, computerID, computer.IntentRevision); err != nil {
		return Computer{}, err
	}
	if err := insertComputerIntent(ctx, tx, computerID, nextRevision, ComputerIntentProject,
		computer.DesiredState, computer.StorageID, computer.StorageGeneration,
		job.JobID, nextSpecRevision, request.Actor, now); err != nil {
		return Computer{}, err
	}
	quiescent, err := quiesceComputerProjectionTx(ctx, tx, computer.CurrentJob, now)
	if err != nil {
		return Computer{}, err
	}
	computer.IntentRevision = nextRevision
	computer.ReconfigurationPhase = ComputerReconfigurationProjecting
	computer.ReconfigurationRevision = &nextRevision
	if quiescent {
		if err := finalizeComputerProjectionTx(ctx, tx, computer, now); err != nil {
			return Computer{}, err
		}
	}
	updated, err := readComputerAuthority(ctx, tx, computerID, now)
	if err != nil {
		return Computer{}, internalError(err, "read installed Computer projection")
	}
	if err := tx.Commit(); err != nil {
		return Computer{}, internalError(err, "commit Computer projection install")
	}
	return updated, nil
}

func validatePendingComputerProjection(
	ctx context.Context,
	tx *sql.Tx,
	computer Computer,
	request ComputerProjectionRequest,
	requestHash string,
) error {
	if computer.ReconfigurationRevision == nil || *computer.ReconfigurationRevision != computer.IntentRevision {
		return internalError(errors.New("projecting Computer has no matching revision"), "read Computer projection fence")
	}
	if request.StorageID != computer.StorageID || request.StorageGeneration != computer.StorageGeneration {
		return protocolErrorWithDetails(contract.ErrorStorageReferenceConflict, map[string]any{
			"computer_id": computer.ComputerID, "expected_storage_id": computer.StorageID,
			"expected_storage_generation": computer.StorageGeneration,
		}, "Computer %q storage reference has changed", computer.ComputerID)
	}
	if request.IntentRevision != computer.IntentRevision && request.IntentRevision != computer.IntentRevision-1 {
		return protocolErrorWithDetails(contract.ErrorStaleIntentRevision, map[string]any{
			"computer_id": computer.ComputerID, "expected_revision": computer.IntentRevision,
			"observed_revision": request.IntentRevision,
		}, "Computer %q projection revision changed", computer.ComputerID)
	}
	var storedHash, actor string
	err := tx.QueryRowContext(ctx, `SELECT jobs.request_hash, computer_intent_history.actor
		FROM computer_job_projections
		JOIN jobs ON jobs.job_id=computer_job_projections.job_id
		JOIN computer_intent_history ON computer_intent_history.computer_id=computer_job_projections.computer_id
			AND computer_intent_history.intent_revision=?
		WHERE computer_job_projections.computer_id=? AND computer_job_projections.current=0
			AND computer_job_projections.retired_ns IS NULL
			AND computer_job_projections.spec_revision=?`, computer.IntentRevision, computer.ComputerID,
		computer.CurrentSpecRevision+1).Scan(&storedHash, &actor)
	if err != nil {
		return internalError(err, "read staging Computer projection")
	}
	if storedHash != requestHash || actor != request.Actor {
		return protocolError(contract.ErrorConflict, "Computer %q has a different projection in progress", computer.ComputerID)
	}
	return nil
}

func quiesceComputerProjectionTx(ctx context.Context, tx *sql.Tx, job Job, now time.Time) (bool, error) {
	switch job.State {
	case contract.JobQueued:
		if err := transitionServiceJob(ctx, tx, job.JobID, contract.ServiceDesiredStopped, contract.JobStopped, now); err != nil {
			return false, err
		}
		return true, nil
	case contract.JobClaimed, contract.JobRunning:
		if err := transitionServiceJob(ctx, tx, job.JobID, contract.ServiceDesiredStopped, contract.JobStopping, now); err != nil {
			return false, err
		}
		return false, nil
	case contract.JobStopping:
		return false, nil
	case contract.JobStopped:
		return true, nil
	case contract.JobFailed:
		if _, err := tx.ExecContext(ctx, `UPDATE service_jobs SET desired_state=?, next_restart_at=NULL,
			published_attempt_id=NULL, healthy_since_ns=NULL WHERE job_id=?`,
			contract.ServiceDesiredStopped, job.JobID); err != nil {
			return false, internalError(err, "quiesce failed Computer projection")
		}
		return true, nil
	default:
		return false, protocolError(contract.ErrorConflict,
			"Computer Job %q cannot enter projection from %q", job.JobID, job.State)
	}
}

func finalizeComputerProjectionTx(ctx context.Context, tx *sql.Tx, computer Computer, now time.Time) error {
	if computer.ReconfigurationRevision == nil {
		return internalError(errors.New("Computer projection revision is missing"), "finalize Computer projection")
	}
	var stagingJobID string
	var stagingSpecRevision int64
	if err := tx.QueryRowContext(ctx, `SELECT job_id, spec_revision FROM computer_job_projections
		WHERE computer_id=? AND current=0 AND retired_ns IS NULL AND spec_revision=?`,
		computer.ComputerID, computer.CurrentSpecRevision+1).Scan(&stagingJobID, &stagingSpecRevision); err != nil {
		return internalError(err, "read staging Computer projection")
	}
	currentJob, err := getJobByID(ctx, tx, computer.CurrentJobID, now)
	if err != nil {
		return internalError(err, "read quiesced Computer projection")
	}
	if currentJob.State != contract.JobStopped && currentJob.State != contract.JobFailed {
		return protocolError(contract.ErrorConflict, "Computer %q has not quiesced its current Job", computer.ComputerID)
	}
	if computer.DesiredState == contract.ServiceDesiredRunning && !currentJob.HoldsSlot(currentJob.State) {
		if err := ensureBoundServiceCapacity(ctx, tx, currentJob); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE computer_job_projections SET current=0, retired_ns=?
		WHERE computer_id=? AND current=1`, now.UnixNano(), computer.ComputerID); err != nil {
		return internalError(err, "retire Computer Job projection")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE computer_job_projections SET current=1
		WHERE computer_id=? AND job_id=? AND current=0 AND retired_ns IS NULL`,
		computer.ComputerID, stagingJobID); err != nil {
		return internalError(err, "activate Computer Job projection")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE service_jobs SET desired_state=?, bound_node_id=NULL,
		published_attempt_id=NULL, healthy_since_ns=NULL, next_restart_at=NULL WHERE job_id=?`,
		contract.ServiceDesiredStopped, computer.CurrentJobID); err != nil {
		return internalError(err, "retire prior Computer service binding")
	}
	nextState := contract.JobStopped
	nextDesired := contract.ServiceDesiredStopped
	if computer.DesiredState == contract.ServiceDesiredRunning {
		nextState = contract.JobQueued
		nextDesired = contract.ServiceDesiredRunning
	}
	var binding any
	if computer.BoundNodeID != "" {
		binding = computer.BoundNodeID
	}
	if _, err := tx.ExecContext(ctx, `UPDATE service_jobs SET desired_state=?, bound_node_id=?,
		restart_streak=0, next_restart_at=NULL, last_failure=NULL, healthy_since_ns=NULL,
		published_attempt_id=NULL WHERE job_id=?`, nextDesired, binding, stagingJobID); err != nil {
		return internalError(err, "activate Computer service projection")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET state=?, updated_ns=? WHERE job_id=?`,
		nextState, now.UnixNano(), stagingJobID); err != nil {
		return internalError(err, "queue Computer service projection")
	}
	result, err := tx.ExecContext(ctx, `UPDATE computers SET current_job_id=?, current_spec_revision=?,
		applied_revision=?, reconfiguration_phase=?, reconfiguration_revision=NULL, updated_ns=?
		WHERE computer_id=? AND intent_revision=? AND reconfiguration_phase=? AND reconfiguration_revision=?`,
		stagingJobID, stagingSpecRevision, *computer.ReconfigurationRevision,
		ComputerReconfigurationStable, now.UnixNano(), computer.ComputerID, computer.IntentRevision,
		ComputerReconfigurationProjecting, *computer.ReconfigurationRevision)
	if err != nil {
		return internalError(err, "transfer Computer projection authority")
	}
	if err := requireComputerCAS(result, computer.ComputerID, computer.IntentRevision); err != nil {
		return err
	}
	return nil
}
