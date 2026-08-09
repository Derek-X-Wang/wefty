package l3

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	_ "modernc.org/sqlite"
)

const defaultInlineMode uint32 = 0o755

var tagPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]*$`)
var workflowIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)

// Store is the L3-owned SQLite ledger. It intentionally has no access to the
// L1 database; the outbox crosses that boundary only through the L1 protocol.
type Store struct {
	db    *sql.DB
	clock Clock
}

// OpenStore opens a file-backed SQLite ledger, enables WAL, and applies the L3
// schema. L1 and L3 callers must pass different database paths.
func OpenStore(path string, options StoreOptions) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("l3: SQLite path is required")
	}
	clock := options.Clock
	if clock == nil {
		clock = systemClock{}
	}
	query := make(url.Values)
	query.Add("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "foreign_keys(1)")
	query.Set("_txlock", "immediate")
	dsn := (&url.URL{Scheme: "file", Path: path, RawQuery: query.Encode()}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("l3: open SQLite: %w", err)
	}
	db.SetMaxOpenConns(16)
	store := &Store{db: db, clock: clock}
	if err := store.initialize(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) initialize(ctx context.Context) error {
	var mode string
	if err := s.db.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&mode); err != nil {
		return fmt.Errorf("l3: enable SQLite WAL: %w", err)
	}
	if !strings.EqualFold(mode, "wal") {
		return fmt.Errorf("l3: SQLite did not enable WAL (mode %q)", mode)
	}
	const schema = `
CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  parent_run_id TEXT REFERENCES runs(run_id),
  dispatch_key TEXT NOT NULL UNIQUE,
  idempotency_key TEXT NOT NULL UNIQUE,
  request_hash TEXT NOT NULL,
  status TEXT NOT NULL,
  params_json BLOB NOT NULL,
  tags_json BLOB NOT NULL,
  limits_json BLOB,
  envelope_schema_json BLOB,
  required_envelope INTEGER NOT NULL DEFAULT 0,
  l1_job_id TEXT,
  created_ns INTEGER NOT NULL,
  updated_ns INTEGER NOT NULL,
  started_ns INTEGER,
  finished_ns INTEGER
);
CREATE INDEX IF NOT EXISTS runs_projection ON runs(status, l1_job_id, created_ns);
CREATE TABLE IF NOT EXISTS run_scripts (
  run_id TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE RESTRICT,
  content BLOB NOT NULL,
  sha256 TEXT NOT NULL,
  interpreter_json BLOB NOT NULL,
  mode INTEGER NOT NULL
);
CREATE TRIGGER IF NOT EXISTS run_scripts_no_update
BEFORE UPDATE ON run_scripts BEGIN SELECT RAISE(ABORT, 'inline scripts are immutable'); END;
CREATE TRIGGER IF NOT EXISTS run_scripts_no_delete
BEFORE DELETE ON run_scripts BEGIN SELECT RAISE(ABORT, 'inline scripts are immutable'); END;
CREATE TABLE IF NOT EXISTS run_workflow_refs (
  run_id TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE RESTRICT,
  workflow_ref TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS run_workflow_refs_no_update
BEFORE UPDATE ON run_workflow_refs BEGIN SELECT RAISE(ABORT, 'resolved workflow refs are immutable'); END;
CREATE TRIGGER IF NOT EXISTS run_workflow_refs_no_delete
BEFORE DELETE ON run_workflow_refs BEGIN SELECT RAISE(ABORT, 'resolved workflow refs are immutable'); END;
CREATE TABLE IF NOT EXISTS workflow_versions (
  workflow_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  content BLOB NOT NULL,
  sha256 TEXT NOT NULL,
  interpreter_json BLOB NOT NULL,
  mode INTEGER NOT NULL,
  created_ns INTEGER NOT NULL,
  PRIMARY KEY(workflow_id, version)
);
CREATE TRIGGER IF NOT EXISTS workflow_versions_no_update
BEFORE UPDATE ON workflow_versions BEGIN SELECT RAISE(ABORT, 'workflow versions are immutable'); END;
CREATE TRIGGER IF NOT EXISTS workflow_versions_no_delete
BEFORE DELETE ON workflow_versions BEGIN SELECT RAISE(ABORT, 'workflow versions are immutable'); END;
CREATE TABLE IF NOT EXISTS run_triggers (
  run_id TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE RESTRICT,
  actor TEXT NOT NULL,
  source TEXT NOT NULL,
  source_run_id TEXT,
  params_json BLOB NOT NULL,
  created_ns INTEGER NOT NULL
);
CREATE TRIGGER IF NOT EXISTS run_triggers_no_update
BEFORE UPDATE ON run_triggers BEGIN SELECT RAISE(ABORT, 'trigger provenance is immutable'); END;
CREATE TRIGGER IF NOT EXISTS run_triggers_no_delete
BEFORE DELETE ON run_triggers BEGIN SELECT RAISE(ABORT, 'trigger provenance is immutable'); END;
CREATE TABLE IF NOT EXISTS dispatch_outbox (
  run_id TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE RESTRICT,
  dispatch_key TEXT NOT NULL UNIQUE,
  job_id TEXT,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  dispatched_ns INTEGER
);
CREATE INDEX IF NOT EXISTS dispatch_outbox_pending ON dispatch_outbox(dispatched_ns, run_id);
`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("l3: apply SQLite schema: %w", err)
	}
	return nil
}

func (s *Store) Close() error { return s.db.Close() }

type workflowSnapshot struct {
	Content     []byte
	SHA256      string
	Interpreter []string
	Mode        uint32
}

// CreateWorkflowVersion appends an immutable version and assigns the next
// monotonically increasing version number for the workflow.
func (s *Store) CreateWorkflowVersion(ctx context.Context, workflowID string, input WorkflowVersionInput) (WorkflowVersion, error) {
	workflowID, err := normalizeWorkflowID(workflowID)
	if err != nil {
		return WorkflowVersion{}, err
	}
	mode, err := normalizeScript(input.Content, input.SHA256, input.Interpreter, input.Mode, "workflow version")
	if err != nil {
		return WorkflowVersion{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkflowVersion{}, internalError(err, "begin workflow version creation")
	}
	defer tx.Rollback()
	var version int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) + 1 FROM workflow_versions WHERE workflow_id=?`, workflowID).Scan(&version); err != nil {
		return WorkflowVersion{}, internalError(err, "assign workflow version")
	}
	interpreterJSON, _ := json.Marshal(nonNilStrings(input.Interpreter))
	now := canonicalTime(s.clock.Now())
	if _, err := tx.ExecContext(ctx, `INSERT INTO workflow_versions(workflow_id, version, content, sha256, interpreter_json, mode, created_ns) VALUES(?, ?, ?, ?, ?, ?, ?)`,
		workflowID, version, []byte(input.Content), input.SHA256, interpreterJSON, mode, now.UnixNano()); err != nil {
		return WorkflowVersion{}, internalError(err, "store workflow version")
	}
	if err := tx.Commit(); err != nil {
		return WorkflowVersion{}, internalError(err, "commit workflow version")
	}
	return s.GetWorkflowVersion(ctx, workflowID, version)
}

func (s *Store) GetWorkflowVersion(ctx context.Context, workflowID string, version int) (WorkflowVersion, error) {
	workflowID, err := normalizeWorkflowID(workflowID)
	if err != nil {
		return WorkflowVersion{}, err
	}
	if version < 1 {
		return WorkflowVersion{}, protocolError(contract.ErrorInvalidRequest, "workflow version must be at least 1")
	}
	var record WorkflowVersion
	var content, interpreterJSON []byte
	var createdNS int64
	err = s.db.QueryRowContext(ctx, `SELECT content, sha256, interpreter_json, mode, created_ns FROM workflow_versions WHERE workflow_id=? AND version=?`, workflowID, version).
		Scan(&content, &record.SHA256, &interpreterJSON, &record.Mode, &createdNS)
	if errors.Is(err, sql.ErrNoRows) {
		return WorkflowVersion{}, protocolError(contract.ErrorNotFound, "workflow %q version v%d was not found", workflowID, version)
	}
	if err != nil {
		return WorkflowVersion{}, internalError(err, "read workflow version")
	}
	record.WorkflowID = workflowID
	record.Version = version
	record.WorkflowRef = pinnedWorkflowRef(workflowID, version)
	record.Content = string(content)
	if err := json.Unmarshal(interpreterJSON, &record.Interpreter); err != nil {
		return WorkflowVersion{}, internalError(err, "decode workflow interpreter")
	}
	if record.Interpreter == nil {
		record.Interpreter = []string{}
	}
	record.CreatedAt = time.Unix(0, createdNS).UTC()
	return record, nil
}

func normalizeWorkflowID(workflowID string) (string, error) {
	workflowID = strings.TrimSpace(workflowID)
	if !workflowIDPattern.MatchString(workflowID) {
		return "", protocolError(contract.ErrorInvalidRequest, "workflow_id %q is invalid", workflowID)
	}
	return workflowID, nil
}

func parseWorkflowRef(ref string) (workflowID string, version int, err error) {
	const prefix = "workflow://"
	if !strings.HasPrefix(ref, prefix) {
		return "", 0, protocolError(contract.ErrorInvalidRequest, "workflow_ref must use workflow://<workflow_id> or workflow://<workflow_id>/v<version>")
	}
	parts := strings.Split(strings.TrimPrefix(ref, prefix), "/")
	if len(parts) < 1 || len(parts) > 2 {
		return "", 0, protocolError(contract.ErrorInvalidRequest, "workflow_ref must use workflow://<workflow_id> or workflow://<workflow_id>/v<version>")
	}
	workflowID, err = normalizeWorkflowID(parts[0])
	if err != nil {
		return "", 0, err
	}
	if len(parts) == 1 {
		return workflowID, 0, nil
	}
	if len(parts[1]) < 2 || parts[1][0] != 'v' {
		return "", 0, protocolError(contract.ErrorInvalidRequest, "workflow_ref version must use v<version>")
	}
	parsed, parseErr := strconv.Atoi(parts[1][1:])
	if parseErr != nil || parsed < 1 || strconv.Itoa(parsed) != parts[1][1:] {
		return "", 0, protocolError(contract.ErrorInvalidRequest, "workflow_ref version must use a positive integer")
	}
	return workflowID, parsed, nil
}

func pinnedWorkflowRef(workflowID string, version int) string {
	return fmt.Sprintf("workflow://%s/v%d", workflowID, version)
}

func resolveWorkflowTx(ctx context.Context, tx *sql.Tx, ref string) (workflowSnapshot, string, error) {
	workflowID, version, err := parseWorkflowRef(ref)
	if err != nil {
		return workflowSnapshot{}, "", err
	}
	var snapshot workflowSnapshot
	var interpreterJSON []byte
	if version == 0 {
		err = tx.QueryRowContext(ctx, `SELECT version, content, sha256, interpreter_json, mode FROM workflow_versions WHERE workflow_id=? ORDER BY version DESC LIMIT 1`, workflowID).
			Scan(&version, &snapshot.Content, &snapshot.SHA256, &interpreterJSON, &snapshot.Mode)
	} else {
		err = tx.QueryRowContext(ctx, `SELECT content, sha256, interpreter_json, mode FROM workflow_versions WHERE workflow_id=? AND version=?`, workflowID, version).
			Scan(&snapshot.Content, &snapshot.SHA256, &interpreterJSON, &snapshot.Mode)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return workflowSnapshot{}, "", protocolError(contract.ErrorNotFound, "workflow_ref %q was not found", ref)
	}
	if err != nil {
		return workflowSnapshot{}, "", internalError(err, "resolve workflow_ref")
	}
	if err := json.Unmarshal(interpreterJSON, &snapshot.Interpreter); err != nil {
		return workflowSnapshot{}, "", internalError(err, "decode resolved workflow interpreter")
	}
	return snapshot, pinnedWorkflowRef(workflowID, version), nil
}

// CreateRun atomically commits the run, immutable script, trigger provenance,
// and dispatch intent. The returned replay flag follows Idempotency-Key
// semantics at the L3 boundary.
func (s *Store) CreateRun(ctx context.Context, input CreateRunInput) (record contract.RunRecord, replayed bool, err error) {
	normalized, requestHash, mode, err := normalizeCreateRun(input)
	if err != nil {
		return contract.RunRecord{}, false, err
	}
	input = normalized

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return contract.RunRecord{}, false, internalError(err, "begin run creation")
	}
	defer tx.Rollback()

	var existingID, existingHash string
	err = tx.QueryRowContext(ctx, "SELECT run_id, request_hash FROM runs WHERE idempotency_key=?", input.IdempotencyKey).Scan(&existingID, &existingHash)
	if err == nil {
		if existingHash != requestHash {
			return contract.RunRecord{}, false, protocolError(contract.ErrorIdempotencyConflict, "idempotency key %q was already used with a different run", input.IdempotencyKey)
		}
		_ = tx.Rollback()
		record, err := s.GetRun(ctx, existingID)
		return record, true, err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return contract.RunRecord{}, false, internalError(err, "read run idempotency key")
	}
	if input.Request.ParentRunID != "" {
		var exists int
		if err := tx.QueryRowContext(ctx, "SELECT 1 FROM runs WHERE run_id=?", input.Request.ParentRunID).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
			return contract.RunRecord{}, false, protocolError(contract.ErrorNotFound, "parent run %q was not found", input.Request.ParentRunID)
		} else if err != nil {
			return contract.RunRecord{}, false, internalError(err, "read parent run")
		}
	}
	var snapshot workflowSnapshot
	var workflowRef string
	if input.Request.InlineScript != nil {
		snapshot = workflowSnapshot{
			Content: []byte(input.Request.InlineScript.Content), SHA256: input.Request.InlineScript.SHA256,
			Interpreter: input.Request.InlineScript.Interpreter, Mode: mode,
		}
	} else {
		snapshot, workflowRef, err = resolveWorkflowTx(ctx, tx, input.Request.WorkflowRef)
		if err != nil {
			return contract.RunRecord{}, false, err
		}
	}

	runID := newID("run")
	dispatchKey := "run:" + runID
	now := canonicalTime(s.clock.Now())
	tagsJSON, _ := json.Marshal(input.Request.Tags)
	var limitsJSON []byte
	if input.Request.Limits != nil {
		limitsJSON, _ = json.Marshal(input.Request.Limits)
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO runs(run_id, parent_run_id, dispatch_key, idempotency_key, request_hash, status, params_json, tags_json, limits_json, envelope_schema_json, required_envelope, created_ns, updated_ns)
VALUES(?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, runID, input.Request.ParentRunID, dispatchKey, input.IdempotencyKey, requestHash,
		contract.RunPending, []byte(input.Request.Params), tagsJSON, nullableBytes(limitsJSON), nullableBytes(input.Request.EnvelopeSchema), input.Request.RequiredEnvelope,
		now.UnixNano(), now.UnixNano())
	if err != nil {
		return contract.RunRecord{}, false, internalError(err, "store run")
	}
	interpreterJSON, _ := json.Marshal(nonNilStrings(snapshot.Interpreter))
	_, err = tx.ExecContext(ctx, `INSERT INTO run_scripts(run_id, content, sha256, interpreter_json, mode) VALUES(?, ?, ?, ?, ?)`,
		runID, snapshot.Content, snapshot.SHA256, interpreterJSON, snapshot.Mode)
	if err != nil {
		return contract.RunRecord{}, false, internalError(err, "store immutable run script snapshot")
	}
	if workflowRef != "" {
		if _, err := tx.ExecContext(ctx, `INSERT INTO run_workflow_refs(run_id, workflow_ref) VALUES(?, ?)`, runID, workflowRef); err != nil {
			return contract.RunRecord{}, false, internalError(err, "store pinned workflow ref")
		}
	}
	source := "manual"
	sourceRunID := ""
	if input.Request.ParentRunID != "" {
		source = "chain"
		sourceRunID = input.Request.ParentRunID
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO run_triggers(run_id, actor, source, source_run_id, params_json, created_ns) VALUES(?, ?, ?, NULLIF(?, ''), ?, ?)`,
		runID, input.Actor, source, sourceRunID, []byte(input.Request.Params), now.UnixNano())
	if err != nil {
		return contract.RunRecord{}, false, internalError(err, "store trigger provenance")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO dispatch_outbox(run_id, dispatch_key) VALUES(?, ?)`, runID, dispatchKey); err != nil {
		return contract.RunRecord{}, false, internalError(err, "store dispatch intent")
	}
	if err := tx.Commit(); err != nil {
		return contract.RunRecord{}, false, internalError(err, "commit run and dispatch intent")
	}
	record, err = s.GetRun(ctx, runID)
	return record, false, err
}

// CreateRerun creates a new run from an existing run's immutable script
// snapshot and original inputs. It deliberately never resolves workflow_ref.
func (s *Store) CreateRerun(ctx context.Context, input CreateRerunInput) (record contract.RunRecord, replayed bool, err error) {
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	input.Actor = strings.TrimSpace(input.Actor)
	input.SourceRunID = strings.TrimSpace(input.SourceRunID)
	if input.IdempotencyKey == "" || len(input.IdempotencyKey) > 255 {
		return contract.RunRecord{}, false, protocolError(contract.ErrorInvalidRequest, "Idempotency-Key must be between 1 and 255 characters")
	}
	if input.Actor == "" {
		return contract.RunRecord{}, false, protocolError(contract.ErrorUnauthorized, "authenticated actor is required")
	}
	if input.SourceRunID == "" {
		return contract.RunRecord{}, false, protocolError(contract.ErrorInvalidRequest, "source run id is required")
	}
	hashInput, _ := json.Marshal(struct {
		Actor   string `json:"actor"`
		RerunOf string `json:"rerun_of"`
	}{Actor: input.Actor, RerunOf: input.SourceRunID})
	digest := sha256.Sum256(hashInput)
	requestHash := hex.EncodeToString(digest[:])

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return contract.RunRecord{}, false, internalError(err, "begin rerun creation")
	}
	defer tx.Rollback()
	var existingID, existingHash string
	err = tx.QueryRowContext(ctx, "SELECT run_id, request_hash FROM runs WHERE idempotency_key=?", input.IdempotencyKey).Scan(&existingID, &existingHash)
	if err == nil {
		if existingHash != requestHash {
			return contract.RunRecord{}, false, protocolError(contract.ErrorIdempotencyConflict, "idempotency key %q was already used with a different run", input.IdempotencyKey)
		}
		_ = tx.Rollback()
		record, err := s.GetRun(ctx, existingID)
		return record, true, err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return contract.RunRecord{}, false, internalError(err, "read rerun idempotency key")
	}

	var paramsJSON, tagsJSON, limitsJSON, envelopeSchemaJSON []byte
	var requiredEnvelope bool
	var snapshot workflowSnapshot
	var interpreterJSON []byte
	var workflowRef sql.NullString
	err = tx.QueryRowContext(ctx, `
SELECT r.params_json, r.tags_json, r.limits_json, r.envelope_schema_json, r.required_envelope,
       s.content, s.sha256, s.interpreter_json, s.mode, w.workflow_ref
FROM runs r JOIN run_scripts s ON s.run_id=r.run_id
LEFT JOIN run_workflow_refs w ON w.run_id=r.run_id
WHERE r.run_id=?`, input.SourceRunID).Scan(&paramsJSON, &tagsJSON, &limitsJSON, &envelopeSchemaJSON, &requiredEnvelope,
		&snapshot.Content, &snapshot.SHA256, &interpreterJSON, &snapshot.Mode, &workflowRef)
	if errors.Is(err, sql.ErrNoRows) {
		return contract.RunRecord{}, false, protocolError(contract.ErrorNotFound, "run %q was not found", input.SourceRunID)
	}
	if err != nil {
		return contract.RunRecord{}, false, internalError(err, "read rerun source snapshot")
	}

	runID := newID("run")
	dispatchKey := "run:" + runID
	now := canonicalTime(s.clock.Now())
	_, err = tx.ExecContext(ctx, `
INSERT INTO runs(run_id, parent_run_id, dispatch_key, idempotency_key, request_hash, status, params_json, tags_json, limits_json, envelope_schema_json, required_envelope, created_ns, updated_ns)
VALUES(?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, runID, dispatchKey, input.IdempotencyKey, requestHash, contract.RunPending,
		paramsJSON, tagsJSON, nullableBytes(limitsJSON), nullableBytes(envelopeSchemaJSON), requiredEnvelope, now.UnixNano(), now.UnixNano())
	if err != nil {
		return contract.RunRecord{}, false, internalError(err, "store rerun")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO run_scripts(run_id, content, sha256, interpreter_json, mode) VALUES(?, ?, ?, ?, ?)`,
		runID, snapshot.Content, snapshot.SHA256, interpreterJSON, snapshot.Mode); err != nil {
		return contract.RunRecord{}, false, internalError(err, "store rerun script snapshot")
	}
	if workflowRef.Valid {
		if _, err := tx.ExecContext(ctx, `INSERT INTO run_workflow_refs(run_id, workflow_ref) VALUES(?, ?)`, runID, workflowRef.String); err != nil {
			return contract.RunRecord{}, false, internalError(err, "store rerun workflow ref")
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO run_triggers(run_id, actor, source, source_run_id, params_json, created_ns) VALUES(?, ?, 'rerun', ?, ?, ?)`,
		runID, input.Actor, input.SourceRunID, paramsJSON, now.UnixNano()); err != nil {
		return contract.RunRecord{}, false, internalError(err, "store rerun provenance")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO dispatch_outbox(run_id, dispatch_key) VALUES(?, ?)`, runID, dispatchKey); err != nil {
		return contract.RunRecord{}, false, internalError(err, "store rerun dispatch intent")
	}
	if err := tx.Commit(); err != nil {
		return contract.RunRecord{}, false, internalError(err, "commit rerun and dispatch intent")
	}
	record, err = s.GetRun(ctx, runID)
	return record, false, err
}

func normalizeCreateRun(input CreateRunInput) (CreateRunInput, string, uint32, error) {
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	input.Actor = strings.TrimSpace(input.Actor)
	if input.IdempotencyKey == "" || len(input.IdempotencyKey) > 255 {
		return input, "", 0, protocolError(contract.ErrorInvalidRequest, "Idempotency-Key must be between 1 and 255 characters")
	}
	if input.Actor == "" {
		return input, "", 0, protocolError(contract.ErrorUnauthorized, "authenticated actor is required")
	}
	request := &input.Request
	request.WorkflowRef = strings.TrimSpace(request.WorkflowRef)
	if request.WorkflowRef != "" && request.InlineScript != nil {
		return input, "", 0, protocolError(contract.ErrorInvalidRequest, "workflow_ref and inline_script are mutually exclusive")
	}
	if request.WorkflowRef == "" && request.InlineScript == nil {
		return input, "", 0, protocolError(contract.ErrorInvalidRequest, "exactly one of workflow_ref or inline_script is required")
	}
	mode := uint32(0)
	var err error
	if request.InlineScript != nil {
		mode, err = normalizeScript(request.InlineScript.Content, request.InlineScript.SHA256, request.InlineScript.Interpreter, request.InlineScript.Mode, "inline_script")
		if err != nil {
			return input, "", 0, err
		}
	} else if _, _, err := parseWorkflowRef(request.WorkflowRef); err != nil {
		return input, "", 0, err
	}
	params, err := canonicalObject(request.Params, true, "params")
	if err != nil {
		return input, "", 0, err
	}
	request.Params = params
	if len(request.EnvelopeSchema) > 0 {
		request.EnvelopeSchema, err = canonicalObject(request.EnvelopeSchema, false, "envelope_schema")
		if err != nil {
			return input, "", 0, err
		}
	}
	request.Tags, err = normalizeTags(request.Tags)
	if err != nil {
		return input, "", 0, err
	}
	if request.Limits != nil {
		if request.Limits.MaxRuntimeSeconds < 0 || request.Limits.MaxCost < 0 {
			return input, "", 0, protocolError(contract.ErrorInvalidRequest, "run limits cannot be negative")
		}
	}
	normalizedForHash := struct {
		Actor   string           `json:"actor"`
		Request CreateRunRequest `json:"request"`
		Mode    uint32           `json:"resolved_mode"`
	}{input.Actor, input.Request, mode}
	encoded, err := json.Marshal(normalizedForHash)
	if err != nil {
		return input, "", 0, internalError(err, "encode normalized run request")
	}
	hash := sha256.Sum256(encoded)
	return input, hex.EncodeToString(hash[:]), mode, nil
}

func normalizeScript(content, suppliedSHA string, interpreter []string, suppliedMode *uint32, name string) (uint32, error) {
	if content == "" {
		return 0, protocolError(contract.ErrorInvalidRequest, "%s.content must not be empty", name)
	}
	digest := sha256.Sum256([]byte(content))
	computed := hex.EncodeToString(digest[:])
	if suppliedSHA != computed {
		return 0, protocolError(contract.ErrorInvalidRequest, "%s.sha256 does not match content", name)
	}
	for _, part := range interpreter {
		if part == "" {
			return 0, protocolError(contract.ErrorInvalidRequest, "%s.interpreter entries must not be empty", name)
		}
	}
	mode := defaultInlineMode
	if suppliedMode != nil {
		mode = *suppliedMode
	}
	if mode > 0o7777 {
		return 0, protocolError(contract.ErrorInvalidRequest, "%s.mode exceeds 07777", name)
	}
	return mode, nil
}

func canonicalObject(raw json.RawMessage, required bool, name string) (json.RawMessage, error) {
	if len(raw) == 0 {
		if required {
			return nil, protocolError(contract.ErrorInvalidRequest, "%s is required", name)
		}
		return nil, nil
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil || value == nil {
		return nil, protocolError(contract.ErrorInvalidRequest, "%s must be a JSON object", name)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, internalError(err, "encode "+name)
	}
	return encoded, nil
}

func normalizeTags(tags []string) ([]string, error) {
	seen := make(map[string]struct{}, len(tags))
	normalized := make([]string, 0, len(tags))
	for _, tag := range tags {
		tag = strings.ToLower(strings.TrimSpace(tag))
		if !tagPattern.MatchString(tag) {
			return nil, protocolError(contract.ErrorInvalidRequest, "routing tag %q is invalid", tag)
		}
		if _, exists := seen[tag]; exists {
			continue
		}
		seen[tag] = struct{}{}
		normalized = append(normalized, tag)
	}
	sort.Strings(normalized)
	return normalized, nil
}

// GetRun returns the public contract record reconstructed from immutable
// script and trigger rows.
func (s *Store) GetRun(ctx context.Context, runID string) (contract.RunRecord, error) {
	var record contract.RunRecord
	var parent, sourceRun, workflowRef sql.NullString
	var paramsJSON, tagsJSON []byte
	var limitsJSON []byte
	var content []byte
	var actor, source, sha string
	var createdNS, updatedNS int64
	var startedNS, finishedNS sql.NullInt64
	err := s.db.QueryRowContext(ctx, `
SELECT r.run_id, r.parent_run_id, r.dispatch_key, r.status, r.params_json, r.tags_json, r.limits_json,
       r.created_ns, r.updated_ns, r.started_ns, r.finished_ns,
       s.content, s.sha256, w.workflow_ref, t.actor, t.source, t.source_run_id
FROM runs r JOIN run_scripts s ON s.run_id=r.run_id
LEFT JOIN run_workflow_refs w ON w.run_id=r.run_id
JOIN run_triggers t ON t.run_id=r.run_id
WHERE r.run_id=?`, runID).Scan(&record.RunID, &parent, &record.DispatchKey, &record.Status, &paramsJSON, &tagsJSON, &limitsJSON,
		&createdNS, &updatedNS, &startedNS, &finishedNS, &content, &sha, &workflowRef, &actor, &source, &sourceRun)
	if errors.Is(err, sql.ErrNoRows) {
		return contract.RunRecord{}, protocolError(contract.ErrorNotFound, "run %q was not found", runID)
	}
	if err != nil {
		return contract.RunRecord{}, internalError(err, "read run")
	}
	record.SchemaVersion = contract.SchemaVersionV1
	record.ParentRunID = parent.String
	record.Params = append(json.RawMessage(nil), paramsJSON...)
	if err := json.Unmarshal(tagsJSON, &record.Tags); err != nil {
		return contract.RunRecord{}, internalError(err, "decode run tags")
	}
	if record.Tags == nil {
		record.Tags = []string{}
	}
	if len(limitsJSON) > 0 {
		record.Limits = &contract.RunLimits{}
		if err := json.Unmarshal(limitsJSON, record.Limits); err != nil {
			return contract.RunRecord{}, internalError(err, "decode run limits")
		}
	}
	record.Trigger = contract.Trigger{Type: source, Principal: actor, SourceRunID: sourceRun.String}
	if workflowRef.Valid {
		record.Workflow = contract.WorkflowSource{WorkflowRef: workflowRef.String}
	} else {
		record.Workflow = contract.WorkflowSource{InlineScript: &contract.InlineScript{Content: string(content), SHA256: sha}}
	}
	record.CreatedAt = time.Unix(0, createdNS).UTC()
	record.UpdatedAt = time.Unix(0, updatedNS).UTC()
	if startedNS.Valid {
		started := time.Unix(0, startedNS.Int64).UTC()
		record.StartedAt = &started
	}
	if finishedNS.Valid {
		finished := time.Unix(0, finishedNS.Int64).UTC()
		record.FinishedAt = &finished
	}
	return record, nil
}

func (s *Store) GetTrigger(ctx context.Context, runID string) (TriggerProvenance, error) {
	var provenance TriggerProvenance
	var sourceRun sql.NullString
	var params []byte
	var createdNS int64
	err := s.db.QueryRowContext(ctx, `SELECT run_id, actor, source, source_run_id, params_json, created_ns FROM run_triggers WHERE run_id=?`, runID).
		Scan(&provenance.RunID, &provenance.Actor, &provenance.Source, &sourceRun, &params, &createdNS)
	if errors.Is(err, sql.ErrNoRows) {
		return TriggerProvenance{}, protocolError(contract.ErrorNotFound, "trigger for run %q was not found", runID)
	}
	if err != nil {
		return TriggerProvenance{}, internalError(err, "read trigger provenance")
	}
	provenance.SourceRunID = sourceRun.String
	provenance.Params = append(json.RawMessage(nil), params...)
	provenance.CreatedAt = time.Unix(0, createdNS).UTC()
	return provenance, nil
}

type dispatchIntent struct {
	RunID       string
	DispatchKey string
	ParentRunID string
	Content     []byte
	SHA256      string
	Interpreter []string
	Mode        uint32
	Tags        []string
	Limits      *contract.RunLimits
}

func (s *Store) pendingDispatches(ctx context.Context) ([]dispatchIntent, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT r.run_id, r.dispatch_key, COALESCE(r.parent_run_id, ''), s.content, s.sha256, s.interpreter_json, s.mode, r.tags_json, r.limits_json
FROM dispatch_outbox o JOIN runs r ON r.run_id=o.run_id JOIN run_scripts s ON s.run_id=r.run_id
WHERE o.dispatched_ns IS NULL ORDER BY r.created_ns, r.run_id`)
	if err != nil {
		return nil, internalError(err, "list pending dispatches")
	}
	defer rows.Close()
	var intents []dispatchIntent
	for rows.Next() {
		var intent dispatchIntent
		var interpreterJSON, tagsJSON, limitsJSON []byte
		if err := rows.Scan(&intent.RunID, &intent.DispatchKey, &intent.ParentRunID, &intent.Content, &intent.SHA256, &interpreterJSON, &intent.Mode, &tagsJSON, &limitsJSON); err != nil {
			return nil, internalError(err, "scan pending dispatch")
		}
		if err := json.Unmarshal(interpreterJSON, &intent.Interpreter); err != nil {
			return nil, internalError(err, "decode dispatch interpreter")
		}
		if err := json.Unmarshal(tagsJSON, &intent.Tags); err != nil {
			return nil, internalError(err, "decode dispatch tags")
		}
		if len(limitsJSON) > 0 {
			intent.Limits = &contract.RunLimits{}
			if err := json.Unmarshal(limitsJSON, intent.Limits); err != nil {
				return nil, internalError(err, "decode dispatch limits")
			}
		}
		intents = append(intents, intent)
	}
	if err := rows.Err(); err != nil {
		return nil, internalError(err, "iterate pending dispatches")
	}
	return intents, nil
}

func (s *Store) beginDispatch(ctx context.Context, runID string) error {
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return internalError(err, "begin dispatch attempt")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET status=CASE WHEN status=? THEN ? ELSE status END, updated_ns=? WHERE run_id=? AND status IN (?, ?)`,
		contract.RunPending, contract.RunDispatching, now.UnixNano(), runID, contract.RunPending, contract.RunDispatching); err != nil {
		return internalError(err, "mark dispatch attempt")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE dispatch_outbox SET attempt_count=attempt_count+1, last_error=NULL WHERE run_id=? AND dispatched_ns IS NULL`, runID); err != nil {
		return internalError(err, "increment dispatch attempt")
	}
	if err := tx.Commit(); err != nil {
		return internalError(err, "commit dispatch attempt")
	}
	return nil
}

func (s *Store) recordDispatchError(ctx context.Context, runID string, dispatchErr error) error {
	_, err := s.db.ExecContext(ctx, `UPDATE dispatch_outbox SET last_error=? WHERE run_id=? AND dispatched_ns IS NULL`, dispatchErr.Error(), runID)
	if err != nil {
		return internalError(err, "record dispatch error")
	}
	return nil
}

func (s *Store) completeDispatch(ctx context.Context, runID, jobID string) error {
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return internalError(err, "begin dispatch completion")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE dispatch_outbox SET job_id=?, dispatched_ns=?, last_error=NULL WHERE run_id=? AND dispatched_ns IS NULL`, jobID, now.UnixNano(), runID); err != nil {
		return internalError(err, "complete dispatch outbox")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET l1_job_id=?, status=?, updated_ns=? WHERE run_id=? AND status IN (?, ?)`,
		jobID, contract.RunQueued, now.UnixNano(), runID, contract.RunPending, contract.RunDispatching); err != nil {
		return internalError(err, "associate run with job")
	}
	if err := tx.Commit(); err != nil {
		return internalError(err, "commit dispatch completion")
	}
	return nil
}

type projectedRun struct {
	RunID            string
	JobID            string
	State            contract.RunState
	RequiredEnvelope bool
}

func (s *Store) activeProjectedRuns(ctx context.Context) ([]projectedRun, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT run_id, l1_job_id, status, required_envelope FROM runs WHERE l1_job_id IS NOT NULL AND status NOT IN (?, ?) ORDER BY created_ns, run_id`, contract.RunSucceeded, contract.RunFailed)
	if err != nil {
		return nil, internalError(err, "list runs for projection")
	}
	defer rows.Close()
	var runs []projectedRun
	for rows.Next() {
		var run projectedRun
		if err := rows.Scan(&run.RunID, &run.JobID, &run.State, &run.RequiredEnvelope); err != nil {
			return nil, internalError(err, "scan run for projection")
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, internalError(err, "iterate runs for projection")
	}
	return runs, nil
}

func (s *Store) projectJobState(ctx context.Context, run projectedRun, jobState contract.JobState) error {
	target, change, err := ProjectJobState(run.State, jobState)
	if err != nil || !change {
		return err
	}
	if jobState == contract.JobSucceeded && run.RequiredEnvelope {
		// Envelope writes land in a later M2 slice. Until one exists, the M0
		// protocol rule deterministically fails a required-envelope run.
		target = contract.RunFailed
	}
	now := canonicalTime(s.clock.Now())
	started := target == contract.RunRunning || target == contract.RunAwaitingInput || target == contract.RunSucceeded || target == contract.RunFailed
	finished := target == contract.RunSucceeded || target == contract.RunFailed
	_, err = s.db.ExecContext(ctx, `
UPDATE runs SET status=?, updated_ns=?,
  started_ns=CASE WHEN ? THEN COALESCE(started_ns, ?) ELSE started_ns END,
  finished_ns=CASE WHEN ? THEN ? ELSE finished_ns END
WHERE run_id=? AND status=?`, target, now.UnixNano(), started, now.UnixNano(), finished, now.UnixNano(), run.RunID, run.State)
	if err != nil {
		return internalError(err, "project job state onto run")
	}
	return nil
}

// ProjectJobState implements the M0 job-to-run projection table. A claimed
// job has not acknowledged execution yet, so the run remains queued.
func ProjectJobState(current contract.RunState, job contract.JobState) (contract.RunState, bool, error) {
	if current == contract.RunSucceeded || current == contract.RunFailed {
		return current, false, nil
	}
	var target contract.RunState
	switch job {
	case contract.JobQueued, contract.JobClaimed:
		target = contract.RunQueued
	case contract.JobRunning:
		target = contract.RunRunning
	case contract.JobAwaitingInput:
		target = contract.RunAwaitingInput
	case contract.JobSucceeded:
		target = contract.RunSucceeded
	case contract.JobFailed:
		target = contract.RunFailed
	default:
		return current, false, protocolError(contract.ErrorInvalidRequest, "unknown job state %q", job)
	}
	if current == target {
		return current, false, nil
	}
	// Ignore stale observations rather than regressing a run. Terminal states
	// remain reachable even if polling missed an intermediate L1 state.
	rank := map[contract.RunState]int{
		contract.RunPending: 0, contract.RunDispatching: 1, contract.RunQueued: 2,
		contract.RunRunning: 3, contract.RunAwaitingInput: 4,
		contract.RunSucceeded: 5, contract.RunFailed: 5,
	}
	resuming := current == contract.RunAwaitingInput && target == contract.RunRunning
	if !resuming && target != contract.RunFailed && target != contract.RunSucceeded && rank[target] < rank[current] {
		return current, false, nil
	}
	return target, true, nil
}

func (intent dispatchIntent) jobSpec() contract.JobSpec {
	handoff := "/tmp/wefty/handoffs/" + intent.RunID
	labels := map[string]string{"run_id": intent.RunID}
	if intent.ParentRunID != "" {
		labels["parent_run_id"] = intent.ParentRunID
	}
	var limits *contract.JobLimits
	if intent.Limits != nil && intent.Limits.MaxRuntimeSeconds > 0 {
		limits = &contract.JobLimits{MaxRuntimeSeconds: intent.Limits.MaxRuntimeSeconds}
	}
	return contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		DispatchKey:   intent.DispatchKey,
		Kind:          "process",
		RoutingTags:   append([]string(nil), intent.Tags...),
		Execution: contract.ExecutionSpec{
			Executable: contract.ExecutableSpec{
				InlineBase64: encodeBase64(intent.Content), SHA256: intent.SHA256,
				Interpreter: append([]string(nil), intent.Interpreter...), Mode: intent.Mode,
			},
			Argv:             []string{"wefty-inline-" + intent.RunID},
			Env:              map[string]string{"WEFTY_RUN_ID": intent.RunID, "WEFTY_L1_ENDPOINT": DefaultL1Address, "WEFTY_L3_ENDPOINT": DefaultL3Address, "WEFTY_HANDOFF_DIR": handoff},
			WorkingDirectory: "/tmp",
			HandoffDirectory: handoff,
		},
		Limits: limits,
		Labels: labels,
	}
}

func encodeBase64(content []byte) string {
	return base64Encoding.EncodeToString(content)
}

var base64Encoding = base64.StdEncoding

func nullableBytes(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func nonNilStrings(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

func canonicalTime(value time.Time) time.Time { return value.UTC().Round(0) }

func newID(prefix string) string {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		panic(fmt.Sprintf("l3: crypto/rand: %v", err))
	}
	return prefix + "_" + hex.EncodeToString(random[:])
}
