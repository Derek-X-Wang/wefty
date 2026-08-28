package l3

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
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
	db                          *sql.DB
	clock                       Clock
	tokenGrace                  time.Duration
	computerAuthorityInstanceID string
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
	tokenGrace := options.RunTokenGrace
	if tokenGrace <= 0 {
		tokenGrace = DefaultRunTokenGrace
	}
	instanceID := strings.TrimSpace(options.ComputerAuthorityInstanceID)
	if instanceID == "" {
		instanceID = "default"
	}
	store := &Store{db: db, clock: clock, tokenGrace: tokenGrace, computerAuthorityInstanceID: instanceID}
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
  node_id TEXT,
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
CREATE TABLE IF NOT EXISTS run_images (
  run_id TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE RESTRICT,
  program_json BLOB NOT NULL
);
CREATE TRIGGER IF NOT EXISTS run_images_no_update
BEFORE UPDATE ON run_images BEGIN SELECT RAISE(ABORT, 'image programs are immutable'); END;
CREATE TRIGGER IF NOT EXISTS run_images_no_delete
BEFORE DELETE ON run_images BEGIN SELECT RAISE(ABORT, 'image programs are immutable'); END;
CREATE TABLE IF NOT EXISTS run_image_resolutions (
  run_id TEXT PRIMARY KEY REFERENCES run_images(run_id) ON DELETE RESTRICT,
  top_level_digest TEXT NOT NULL,
  platform_digest TEXT,
  observed_ns INTEGER NOT NULL,
  source_attempt TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS run_image_resolutions_no_update
BEFORE UPDATE ON run_image_resolutions BEGIN SELECT RAISE(ABORT, 'image resolutions are immutable'); END;
CREATE TRIGGER IF NOT EXISTS run_image_resolutions_no_delete
BEFORE DELETE ON run_image_resolutions BEGIN SELECT RAISE(ABORT, 'image resolutions are immutable'); END;
CREATE TABLE IF NOT EXISTS run_workflow_refs (
  run_id TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE RESTRICT,
  workflow_ref TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS run_workflow_refs_no_update
BEFORE UPDATE ON run_workflow_refs BEGIN SELECT RAISE(ABORT, 'resolved workflow refs are immutable'); END;
CREATE TRIGGER IF NOT EXISTS run_workflow_refs_no_delete
BEFORE DELETE ON run_workflow_refs BEGIN SELECT RAISE(ABORT, 'resolved workflow refs are immutable'); END;
CREATE TABLE IF NOT EXISTS workflow_version_seq (
  workflow_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  PRIMARY KEY(workflow_id, version)
);
CREATE TRIGGER IF NOT EXISTS workflow_version_seq_no_update
BEFORE UPDATE ON workflow_version_seq BEGIN SELECT RAISE(ABORT, 'workflow version sequence is immutable'); END;
CREATE TRIGGER IF NOT EXISTS workflow_version_seq_no_delete
BEFORE DELETE ON workflow_version_seq BEGIN SELECT RAISE(ABORT, 'workflow version sequence is immutable'); END;
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
CREATE TABLE IF NOT EXISTS workflow_image_versions (
  workflow_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  program_json BLOB NOT NULL,
  created_ns INTEGER NOT NULL,
  PRIMARY KEY(workflow_id, version)
);
CREATE TRIGGER IF NOT EXISTS workflow_image_versions_no_update
BEFORE UPDATE ON workflow_image_versions BEGIN SELECT RAISE(ABORT, 'workflow image versions are immutable'); END;
CREATE TRIGGER IF NOT EXISTS workflow_image_versions_no_delete
BEFORE DELETE ON workflow_image_versions BEGIN SELECT RAISE(ABORT, 'workflow image versions are immutable'); END;
CREATE TABLE IF NOT EXISTS run_triggers (
  run_id TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE RESTRICT,
  actor TEXT NOT NULL,
  source TEXT NOT NULL,
  source_run_id TEXT,
  computer_id TEXT,
  computer_attempt_id TEXT,
  computer_storage_generation INTEGER,
  submit_intent_revision INTEGER,
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
	  token_delivery TEXT,
  last_error TEXT,
  dispatched_ns INTEGER
);
CREATE INDEX IF NOT EXISTS dispatch_outbox_pending ON dispatch_outbox(dispatched_ns, run_id);
CREATE TABLE IF NOT EXISTS run_tokens (
  run_id TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE RESTRICT,
  attempt_id TEXT NOT NULL UNIQUE,
  token_hash BLOB NOT NULL UNIQUE,
  minted_ns INTEGER NOT NULL,
  expires_ns INTEGER
);
CREATE TABLE IF NOT EXISTS computer_authority (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
  authority_generation INTEGER NOT NULL CHECK(authority_generation >= 0),
  grant_revision INTEGER NOT NULL CHECK(grant_revision >= 0),
  instance_id TEXT NOT NULL DEFAULT '',
  updated_ns INTEGER NOT NULL
);
INSERT OR IGNORE INTO computer_authority(singleton, authority_generation, grant_revision, updated_ns)
VALUES(1, 0, 0, 0);
CREATE TABLE IF NOT EXISTS computer_token_grants (
  grant_id TEXT PRIMARY KEY,
  computer_id TEXT NOT NULL,
  computer_attempt_id TEXT NOT NULL,
  computer_storage_generation INTEGER NOT NULL CHECK(computer_storage_generation > 0),
  submit_intent_revision INTEGER NOT NULL CHECK(submit_intent_revision > 0),
  host_node_id TEXT NOT NULL,
  l3_authority_generation INTEGER NOT NULL CHECK(l3_authority_generation > 0),
  grant_revision INTEGER NOT NULL UNIQUE CHECK(grant_revision > 0),
  submit_max_inflight INTEGER NOT NULL CHECK(submit_max_inflight > 0),
  token_hash BLOB NOT NULL UNIQUE,
  issued_ns INTEGER NOT NULL,
  revoked_ns INTEGER,
  revocation_reason TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS computer_token_active_attempt
  ON computer_token_grants(computer_id, computer_attempt_id) WHERE revoked_ns IS NULL;
CREATE INDEX IF NOT EXISTS computer_token_active_host
  ON computer_token_grants(host_node_id, computer_id, computer_attempt_id) WHERE revoked_ns IS NULL;
CREATE TABLE IF NOT EXISTS computer_token_audit (
  audit_id TEXT PRIMARY KEY,
  grant_id TEXT NOT NULL,
  operation TEXT NOT NULL CHECK(operation IN ('issued', 'revoked')),
  computer_id TEXT NOT NULL,
  computer_attempt_id TEXT NOT NULL,
  computer_storage_generation INTEGER NOT NULL CHECK(computer_storage_generation > 0),
  submit_intent_revision INTEGER NOT NULL CHECK(submit_intent_revision > 0),
  host_node_id TEXT NOT NULL,
  l3_authority_generation INTEGER NOT NULL CHECK(l3_authority_generation > 0),
  grant_revision INTEGER NOT NULL CHECK(grant_revision > 0),
  submit_max_inflight INTEGER NOT NULL CHECK(submit_max_inflight > 0),
  token_hash BLOB NOT NULL,
  reason TEXT NOT NULL,
  occurred_ns INTEGER NOT NULL,
  UNIQUE(grant_id, operation)
);
CREATE TRIGGER IF NOT EXISTS computer_token_audit_no_update
BEFORE UPDATE ON computer_token_audit BEGIN SELECT RAISE(ABORT, 'Computer token audit is immutable'); END;
CREATE TRIGGER IF NOT EXISTS computer_token_audit_no_delete
BEFORE DELETE ON computer_token_audit BEGIN SELECT RAISE(ABORT, 'Computer token audit is immutable'); END;
CREATE TABLE IF NOT EXISTS envelopes (
  envelope_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
  idempotency_key TEXT NOT NULL,
  body_hash TEXT NOT NULL,
  body_json BLOB NOT NULL,
  accepted_ns INTEGER NOT NULL,
  UNIQUE(run_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS envelopes_by_run ON envelopes(run_id, accepted_ns, envelope_id);
CREATE TRIGGER IF NOT EXISTS envelopes_no_update
BEFORE UPDATE ON envelopes BEGIN SELECT RAISE(ABORT, 'envelopes are append-only'); END;
CREATE TRIGGER IF NOT EXISTS envelopes_no_delete
BEFORE DELETE ON envelopes BEGIN SELECT RAISE(ABORT, 'envelopes are append-only'); END;
CREATE TABLE IF NOT EXISTS gate_results (
  gate_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
  idempotency_key TEXT NOT NULL,
  body_hash TEXT NOT NULL,
  body_json BLOB NOT NULL,
  accepted_ns INTEGER NOT NULL,
  UNIQUE(run_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS gates_by_run ON gate_results(run_id, accepted_ns, gate_id);
CREATE TRIGGER IF NOT EXISTS gate_results_no_update
BEFORE UPDATE ON gate_results BEGIN SELECT RAISE(ABORT, 'gate results are append-only'); END;
CREATE TRIGGER IF NOT EXISTS gate_results_no_delete
BEFORE DELETE ON gate_results BEGIN SELECT RAISE(ABORT, 'gate results are append-only'); END;
CREATE TABLE IF NOT EXISTS protocol_rejections (
  rejection_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE RESTRICT,
  kind TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  body_hash TEXT NOT NULL,
  body_json BLOB NOT NULL,
  reason TEXT NOT NULL,
  created_ns INTEGER NOT NULL,
  UNIQUE(run_id, kind, idempotency_key)
);
CREATE INDEX IF NOT EXISTS protocol_rejections_by_run ON protocol_rejections(run_id, created_ns, rejection_id);
CREATE TRIGGER IF NOT EXISTS protocol_rejections_no_update
BEFORE UPDATE ON protocol_rejections BEGIN SELECT RAISE(ABORT, 'protocol rejections are append-only'); END;
CREATE TRIGGER IF NOT EXISTS protocol_rejections_no_delete
BEFORE DELETE ON protocol_rejections BEGIN SELECT RAISE(ABORT, 'protocol rejections are append-only'); END;
`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("l3: apply SQLite schema: %w", err)
	}
	if err := ensureSQLiteColumn(ctx, s.db, "runs", "node_id", "TEXT"); err != nil {
		return fmt.Errorf("l3: migrate run node attribution: %w", err)
	}
	for _, column := range []struct{ name, definition string }{
		{"computer_id", "TEXT"},
		{"computer_attempt_id", "TEXT"},
		{"computer_storage_generation", "INTEGER"},
		{"submit_intent_revision", "INTEGER"},
	} {
		if err := ensureSQLiteColumn(ctx, s.db, "run_triggers", column.name, column.definition); err != nil {
			return fmt.Errorf("l3: migrate Computer trigger provenance: %w", err)
		}
	}
	if err := ensureSQLiteColumn(ctx, s.db, "computer_authority", "instance_id", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return fmt.Errorf("l3: migrate Computer authority instance marker: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS run_triggers_computer_origin
		ON run_triggers(source, computer_id, run_id)`); err != nil {
		return fmt.Errorf("l3: index Computer trigger provenance: %w", err)
	}
	if err := s.adoptComputerAuthorityInstance(ctx); err != nil {
		return err
	}
	return nil
}

func ensureSQLiteColumn(ctx context.Context, db *sql.DB, table, column, definition string) error {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return err
	}
	found := false
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, dataType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			rows.Close()
			return err
		}
		if name == column {
			found = true
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if found {
		return nil
	}
	_, err = db.ExecContext(ctx, "ALTER TABLE "+table+" ADD COLUMN "+column+" "+definition)
	return err
}

func (s *Store) ensureRunToken(ctx context.Context, runID string) (string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", internalError(err, "begin run token mint")
	}
	defer tx.Rollback()
	var delivery sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT token_delivery FROM dispatch_outbox WHERE run_id=? AND dispatched_ns IS NULL`, runID).Scan(&delivery)
	if errors.Is(err, sql.ErrNoRows) {
		return "", protocolError(contract.ErrorConflict, "run %q has no pending dispatch", runID)
	}
	if err != nil {
		return "", internalError(err, "read pending run token delivery")
	}
	if delivery.Valid && delivery.String != "" {
		return delivery.String, nil
	}
	token := newToken()
	digest := sha256.Sum256([]byte(token))
	attemptID := newID("runattempt")
	now := canonicalTime(s.clock.Now())
	if _, err := tx.ExecContext(ctx, `INSERT INTO run_tokens(run_id, attempt_id, token_hash, minted_ns) VALUES(?, ?, ?, ?)`, runID, attemptID, digest[:], now.UnixNano()); err != nil {
		return "", internalError(err, "store run token hash")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE dispatch_outbox SET token_delivery=? WHERE run_id=? AND dispatched_ns IS NULL`, token, runID); err != nil {
		return "", internalError(err, "stage run token delivery")
	}
	if err := tx.Commit(); err != nil {
		return "", internalError(err, "commit run token mint")
	}
	return token, nil
}

// AuthenticateRunToken verifies an opaque token against its at-rest digest.
// Active runs remain authorized; terminal runs retain authority only for the
// configured grace interval.
func (s *Store) AuthenticateRunToken(ctx context.Context, token string) (RunTokenScope, error) {
	if token == "" {
		return RunTokenScope{}, protocolError(contract.ErrorUnauthorized, "run token is required")
	}
	digest := sha256.Sum256([]byte(token))
	var scope RunTokenScope
	var storedHash []byte
	var expiresNS sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT run_id, attempt_id, token_hash, expires_ns FROM run_tokens WHERE token_hash=?`, digest[:]).
		Scan(&scope.RunID, &scope.AttemptID, &storedHash, &expiresNS)
	if errors.Is(err, sql.ErrNoRows) {
		return RunTokenScope{}, protocolError(contract.ErrorUnauthorized, "run token is invalid")
	}
	if err != nil {
		return RunTokenScope{}, internalError(err, "authenticate run token")
	}
	if len(storedHash) != len(digest) || subtle.ConstantTimeCompare(storedHash, digest[:]) != 1 {
		return RunTokenScope{}, protocolError(contract.ErrorUnauthorized, "run token is invalid")
	}
	if expiresNS.Valid && !canonicalTime(s.clock.Now()).Before(time.Unix(0, expiresNS.Int64).UTC()) {
		return RunTokenScope{}, protocolError(contract.ErrorUnauthorized, "run token has expired")
	}
	return scope, nil
}

// CanReadRun reports whether target is the token's own run or a descendant.
func (s *Store) CanReadRun(ctx context.Context, ownerRunID, targetRunID string) (bool, error) {
	var allowed int
	err := s.db.QueryRowContext(ctx, `
WITH RECURSIVE descendants(run_id) AS (
  SELECT run_id FROM runs WHERE run_id=?
  UNION ALL
  SELECT r.run_id FROM runs r JOIN descendants d ON r.parent_run_id=d.run_id
)
SELECT EXISTS(SELECT 1 FROM descendants WHERE run_id=?)`, ownerRunID, targetRunID).Scan(&allowed)
	if err != nil {
		return false, internalError(err, "authorize run lineage")
	}
	return allowed == 1, nil
}

func (s *Store) Close() error { return s.db.Close() }

type workflowSnapshot struct {
	Content     []byte
	SHA256      string
	Interpreter []string
	Mode        uint32
	Image       *contract.ImageProgram
}

// CreateWorkflowVersion appends an immutable version and assigns the next
// monotonically increasing version number for the workflow.
func (s *Store) CreateWorkflowVersion(ctx context.Context, workflowID string, input WorkflowVersionInput) (WorkflowVersion, error) {
	workflowID, err := normalizeWorkflowID(workflowID)
	if err != nil {
		return WorkflowVersion{}, err
	}
	hasScript := input.Content != "" || input.SHA256 != "" || len(input.Interpreter) > 0 || input.Mode != nil
	if hasScript == (input.Image != nil) {
		return WorkflowVersion{}, protocolError(contract.ErrorInvalidRequest, "workflow version requires exactly one script or image program")
	}
	mode := uint32(0)
	if hasScript {
		mode, err = normalizeScript(input.Content, input.SHA256, input.Interpreter, input.Mode, "workflow version")
		if err != nil {
			return WorkflowVersion{}, err
		}
	} else {
		if err := contract.ValidateImageProgram(*input.Image, contract.JobClassService); err != nil {
			return WorkflowVersion{}, protocolError(contract.ErrorInvalidRequest, "image program: %v", err)
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkflowVersion{}, internalError(err, "begin workflow version creation")
	}
	defer tx.Rollback()
	var version int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) + 1 FROM workflow_version_seq WHERE workflow_id=?`, workflowID).Scan(&version); err != nil {
		return WorkflowVersion{}, internalError(err, "assign workflow version")
	}
	now := canonicalTime(s.clock.Now())
	if _, err := tx.ExecContext(ctx, `INSERT INTO workflow_version_seq(workflow_id, version) VALUES(?, ?)`, workflowID, version); err != nil {
		return WorkflowVersion{}, internalError(err, "reserve workflow version")
	}
	if hasScript {
		interpreterJSON, _ := json.Marshal(nonNilStrings(input.Interpreter))
		if _, err := tx.ExecContext(ctx, `INSERT INTO workflow_versions(workflow_id, version, content, sha256, interpreter_json, mode, created_ns) VALUES(?, ?, ?, ?, ?, ?, ?)`,
			workflowID, version, []byte(input.Content), input.SHA256, interpreterJSON, mode, now.UnixNano()); err != nil {
			return WorkflowVersion{}, internalError(err, "store workflow version")
		}
	} else {
		programJSON, err := json.Marshal(input.Image)
		if err != nil {
			return WorkflowVersion{}, internalError(err, "encode workflow image program")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO workflow_image_versions(workflow_id, version, program_json, created_ns) VALUES(?, ?, ?, ?)`,
			workflowID, version, programJSON, now.UnixNano()); err != nil {
			return WorkflowVersion{}, internalError(err, "store workflow image version")
		}
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
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return WorkflowVersion{}, internalError(err, "read workflow version")
	}
	record.WorkflowID = workflowID
	record.Version = version
	record.WorkflowRef = pinnedWorkflowRef(workflowID, version)
	if err == nil {
		record.Content = string(content)
		if err := json.Unmarshal(interpreterJSON, &record.Interpreter); err != nil {
			return WorkflowVersion{}, internalError(err, "decode workflow interpreter")
		}
		if record.Interpreter == nil {
			record.Interpreter = []string{}
		}
	} else {
		var programJSON []byte
		err = s.db.QueryRowContext(ctx, `SELECT program_json, created_ns FROM workflow_image_versions WHERE workflow_id=? AND version=?`, workflowID, version).
			Scan(&programJSON, &createdNS)
		if errors.Is(err, sql.ErrNoRows) {
			return WorkflowVersion{}, protocolError(contract.ErrorNotFound, "workflow %q version v%d was not found", workflowID, version)
		}
		if err != nil {
			return WorkflowVersion{}, internalError(err, "read workflow image version")
		}
		record.Image = &contract.ImageProgram{}
		if err := json.Unmarshal(programJSON, record.Image); err != nil {
			return WorkflowVersion{}, internalError(err, "decode workflow image program")
		}
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
	if version == 0 {
		err = tx.QueryRowContext(ctx, `SELECT version FROM workflow_version_seq WHERE workflow_id=? ORDER BY version DESC LIMIT 1`, workflowID).Scan(&version)
		if errors.Is(err, sql.ErrNoRows) {
			return workflowSnapshot{}, "", protocolError(contract.ErrorNotFound, "workflow_ref %q was not found", ref)
		}
		if err != nil {
			return workflowSnapshot{}, "", internalError(err, "resolve workflow_ref version")
		}
	}
	snapshot, err := readWorkflowSnapshotTx(ctx, tx, workflowID, version)
	if errors.Is(err, sql.ErrNoRows) {
		return workflowSnapshot{}, "", protocolError(contract.ErrorNotFound, "workflow_ref %q was not found", ref)
	}
	if err != nil {
		return workflowSnapshot{}, "", err
	}
	return snapshot, pinnedWorkflowRef(workflowID, version), nil
}

func readWorkflowSnapshotTx(ctx context.Context, tx *sql.Tx, workflowID string, version int) (workflowSnapshot, error) {
	var snapshot workflowSnapshot
	var interpreterJSON []byte
	err := tx.QueryRowContext(ctx, `SELECT content, sha256, interpreter_json, mode FROM workflow_versions WHERE workflow_id=? AND version=?`, workflowID, version).
		Scan(&snapshot.Content, &snapshot.SHA256, &interpreterJSON, &snapshot.Mode)
	if err == nil {
		if err := json.Unmarshal(interpreterJSON, &snapshot.Interpreter); err != nil {
			return workflowSnapshot{}, internalError(err, "decode resolved workflow interpreter")
		}
		return snapshot, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return workflowSnapshot{}, internalError(err, "resolve workflow_ref")
	}
	var programJSON []byte
	err = tx.QueryRowContext(ctx, `SELECT program_json FROM workflow_image_versions WHERE workflow_id=? AND version=?`, workflowID, version).Scan(&programJSON)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workflowSnapshot{}, sql.ErrNoRows
		}
		return workflowSnapshot{}, internalError(err, "resolve workflow image_ref")
	}
	snapshot.Image = &contract.ImageProgram{}
	if err := json.Unmarshal(programJSON, snapshot.Image); err != nil {
		return workflowSnapshot{}, internalError(err, "decode resolved workflow image program")
	}
	return snapshot, nil
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
	if input.ComputerScope != nil {
		if input.VerifyComputerScope == nil {
			return contract.RunRecord{}, false, protocolError(contract.ErrorUnauthorized, "Computer Run creation requires live scope verification")
		}
		if err := validateComputerGrantTx(ctx, tx, *input.ComputerScope); err != nil {
			return contract.RunRecord{}, false, err
		}
		if err := input.VerifyComputerScope(ctx, *input.ComputerScope); err != nil {
			return contract.RunRecord{}, false, err
		}
		if err := validateComputerGrantTx(ctx, tx, *input.ComputerScope); err != nil {
			return contract.RunRecord{}, false, err
		}
		var inflight int
		if err := tx.QueryRowContext(ctx, `WITH RECURSIVE computer_lineages(root_id, run_id) AS (
			SELECT r.run_id, r.run_id FROM runs r JOIN run_triggers t ON t.run_id=r.run_id
			WHERE t.source='computer' AND t.computer_id=? AND r.parent_run_id IS NULL
			UNION ALL
			SELECT lineage.root_id, child.run_id FROM computer_lineages lineage
			JOIN runs child ON child.parent_run_id=lineage.run_id
		)
		SELECT COUNT(DISTINCT lineage.root_id) FROM computer_lineages lineage
		JOIN runs member ON member.run_id=lineage.run_id
		WHERE member.status NOT IN (?, ?)`, input.ComputerScope.ComputerID, contract.RunSucceeded, contract.RunFailed).Scan(&inflight); err != nil {
			return contract.RunRecord{}, false, internalError(err, "count Computer-submitted root Lineages")
		}
		if inflight >= input.ComputerScope.SubmitMaxInflight {
			return contract.RunRecord{}, false, &Error{Code: contract.ErrorSubmitInflightLimit,
				Retryable: true,
				Message:   fmt.Sprintf("Computer %q has %d nonterminal root Lineages (limit %d)", input.ComputerScope.ComputerID, inflight, input.ComputerScope.SubmitMaxInflight),
				Details:   map[string]any{"computer_id": input.ComputerScope.ComputerID, "count": inflight, "limit": input.ComputerScope.SubmitMaxInflight}}
		}
	}
	if input.Request.ParentRunID != "" {
		var parentStatus contract.RunState
		if err := tx.QueryRowContext(ctx, "SELECT status FROM runs WHERE run_id=?", input.Request.ParentRunID).Scan(&parentStatus); errors.Is(err, sql.ErrNoRows) {
			return contract.RunRecord{}, false, protocolError(contract.ErrorNotFound, "parent run %q was not found", input.Request.ParentRunID)
		} else if err != nil {
			return contract.RunRecord{}, false, internalError(err, "read parent run")
		}
		if parentStatus == contract.RunSucceeded || parentStatus == contract.RunFailed {
			return contract.RunRecord{}, false, protocolError(contract.ErrorConflict, "parent run %q is terminal and cannot dispatch children", input.Request.ParentRunID)
		}
	}
	var snapshot workflowSnapshot
	var workflowRef string
	if input.Request.InlineScript != nil {
		snapshot = workflowSnapshot{
			Content: []byte(input.Request.InlineScript.Content), SHA256: input.Request.InlineScript.SHA256,
			Interpreter: input.Request.InlineScript.Interpreter, Mode: mode,
		}
	} else if input.Request.Image != nil {
		snapshot.Image = input.Request.Image
	} else {
		snapshot, workflowRef, err = resolveWorkflowTx(ctx, tx, input.Request.WorkflowRef)
		if err != nil {
			return contract.RunRecord{}, false, err
		}
	}
	if snapshot.Image != nil {
		if err := contract.ValidateImageProgram(*snapshot.Image, contract.JobClassOneShot); err != nil {
			return contract.RunRecord{}, false, protocolError(contract.ErrorInvalidRequest, "image program: %v", err)
		}
		if err := contract.ValidatePinnedRouting(*snapshot.Image, input.Request.Tags); err != nil {
			return contract.RunRecord{}, false, protocolError(contract.ErrorInvalidRequest, "image routing: %v", err)
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
	if snapshot.Image == nil {
		interpreterJSON, _ := json.Marshal(nonNilStrings(snapshot.Interpreter))
		_, err = tx.ExecContext(ctx, `INSERT INTO run_scripts(run_id, content, sha256, interpreter_json, mode) VALUES(?, ?, ?, ?, ?)`,
			runID, snapshot.Content, snapshot.SHA256, interpreterJSON, snapshot.Mode)
		if err != nil {
			return contract.RunRecord{}, false, internalError(err, "store immutable run script snapshot")
		}
	} else {
		programJSON, err := json.Marshal(snapshot.Image)
		if err != nil {
			return contract.RunRecord{}, false, internalError(err, "encode immutable run image snapshot")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO run_images(run_id, program_json) VALUES(?, ?)`, runID, programJSON); err != nil {
			return contract.RunRecord{}, false, internalError(err, "store immutable run image snapshot")
		}
	}
	if workflowRef != "" {
		if _, err := tx.ExecContext(ctx, `INSERT INTO run_workflow_refs(run_id, workflow_ref) VALUES(?, ?)`, runID, workflowRef); err != nil {
			return contract.RunRecord{}, false, internalError(err, "store pinned workflow ref")
		}
	}
	source := "manual"
	sourceRunID := ""
	var computerID, computerAttemptID any
	var computerStorageGeneration, submitIntentRevision any
	if input.ComputerScope != nil {
		source = "computer"
		computerID = input.ComputerScope.ComputerID
		computerAttemptID = input.ComputerScope.ComputerAttemptID
		computerStorageGeneration = input.ComputerScope.ComputerStorageGeneration
		submitIntentRevision = input.ComputerScope.SubmitIntentRevision
	} else if input.Request.ParentRunID != "" {
		source = "chain"
		sourceRunID = input.Request.ParentRunID
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO run_triggers(
		run_id, actor, source, source_run_id, computer_id, computer_attempt_id,
		computer_storage_generation, submit_intent_revision, params_json, created_ns
	) VALUES(?, ?, ?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?)`, runID, input.Actor, source, sourceRunID,
		computerID, computerAttemptID, computerStorageGeneration, submitIntentRevision, []byte(input.Request.Params), now.UnixNano())
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
	var content, interpreterJSON, imageJSON []byte
	var sha sql.NullString
	var mode sql.NullInt64
	var workflowRef sql.NullString
	var inheritedImageResolution struct {
		present       bool
		topLevel      string
		platform      sql.NullString
		observedNS    int64
		sourceAttempt string
	}
	err = tx.QueryRowContext(ctx, `
SELECT r.params_json, r.tags_json, r.limits_json, r.envelope_schema_json, r.required_envelope,
       s.content, s.sha256, s.interpreter_json, s.mode, i.program_json, w.workflow_ref
FROM runs r LEFT JOIN run_scripts s ON s.run_id=r.run_id
LEFT JOIN run_images i ON i.run_id=r.run_id
LEFT JOIN run_workflow_refs w ON w.run_id=r.run_id
WHERE r.run_id=?`, input.SourceRunID).Scan(&paramsJSON, &tagsJSON, &limitsJSON, &envelopeSchemaJSON, &requiredEnvelope,
		&content, &sha, &interpreterJSON, &mode, &imageJSON, &workflowRef)
	if errors.Is(err, sql.ErrNoRows) {
		return contract.RunRecord{}, false, protocolError(contract.ErrorNotFound, "run %q was not found", input.SourceRunID)
	}
	if err != nil {
		return contract.RunRecord{}, false, internalError(err, "read rerun source snapshot")
	}
	if len(imageJSON) > 0 {
		snapshot.Image = &contract.ImageProgram{}
		if err := json.Unmarshal(imageJSON, snapshot.Image); err != nil {
			return contract.RunRecord{}, false, internalError(err, "decode rerun image snapshot")
		}
		err := tx.QueryRowContext(ctx, `SELECT top_level_digest, platform_digest, observed_ns, source_attempt
			FROM run_image_resolutions WHERE run_id=?`, input.SourceRunID).Scan(
			&inheritedImageResolution.topLevel, &inheritedImageResolution.platform,
			&inheritedImageResolution.observedNS, &inheritedImageResolution.sourceAttempt,
		)
		if err == nil {
			inheritedImageResolution.present = true
		} else if !errors.Is(err, sql.ErrNoRows) {
			return contract.RunRecord{}, false, internalError(err, "read rerun image resolution")
		}
		if snapshot.Image.Digest == nil {
			if !inheritedImageResolution.present {
				return contract.RunRecord{}, false, protocolError(contract.ErrorNoResolvedImageSnapshot,
					"run %q has no resolved image snapshot", input.SourceRunID)
			}
			snapshot.Image.Digest = &inheritedImageResolution.topLevel
			imageJSON, err = json.Marshal(snapshot.Image)
			if err != nil {
				return contract.RunRecord{}, false, internalError(err, "encode resolved rerun image snapshot")
			}
		}
	} else {
		snapshot.Content = content
		snapshot.SHA256 = sha.String
		snapshot.Mode = uint32(mode.Int64)
	}
	var sourceTags []string
	if err := json.Unmarshal(tagsJSON, &sourceTags); err != nil {
		return contract.RunRecord{}, false, internalError(err, "decode rerun source tags")
	}
	stableNodeTags := 0
	for _, tag := range sourceTags {
		if strings.HasPrefix(tag, contract.StableNodeTagPrefix) && len(tag) > len(contract.StableNodeTagPrefix) {
			stableNodeTags++
		}
	}
	if snapshot.Image == nil && stableNodeTags != 1 {
		return contract.RunRecord{}, false, protocolError(contract.ErrorInvalidRequest,
			"cold rerun consuming node-local handoff files requires exactly one reserved stable-node tag %q", contract.StableNodeTagPrefix+"<stable-node-id>")
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
	if snapshot.Image == nil {
		if _, err := tx.ExecContext(ctx, `INSERT INTO run_scripts(run_id, content, sha256, interpreter_json, mode) VALUES(?, ?, ?, ?, ?)`,
			runID, snapshot.Content, snapshot.SHA256, interpreterJSON, snapshot.Mode); err != nil {
			return contract.RunRecord{}, false, internalError(err, "store rerun script snapshot")
		}
	} else {
		if _, err := tx.ExecContext(ctx, `INSERT INTO run_images(run_id, program_json) VALUES(?, ?)`, runID, imageJSON); err != nil {
			return contract.RunRecord{}, false, internalError(err, "store rerun image snapshot")
		}
		if inheritedImageResolution.present {
			var platform any
			if inheritedImageResolution.platform.Valid {
				platform = inheritedImageResolution.platform.String
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO run_image_resolutions(run_id, top_level_digest, platform_digest, observed_ns, source_attempt)
				VALUES(?, ?, ?, ?, ?)`, runID, inheritedImageResolution.topLevel, platform,
				inheritedImageResolution.observedNS, inheritedImageResolution.sourceAttempt); err != nil {
				return contract.RunRecord{}, false, internalError(err, "copy rerun image resolution")
			}
		}
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

// recordRunImageResolution accepts the first L1-observed image identity for a
// run. Later observations cannot move that identity; reruns consume only this
// write-once record when the submitted snapshot was tag-only.
func (s *Store) recordRunImageResolution(ctx context.Context, runID string, evidence AttemptImageEvidence) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, internalError(err, "begin image resolution ingestion")
	}
	defer tx.Rollback()
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM run_image_resolutions WHERE run_id=?)`, runID).Scan(&exists); err != nil {
		return false, internalError(err, "read image resolution")
	}
	if exists == 1 {
		return true, nil
	}
	var programJSON []byte
	err = tx.QueryRowContext(ctx, `SELECT program_json FROM run_images WHERE run_id=?`, runID).Scan(&programJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, internalError(err, "read image snapshot for resolution")
	}
	var program contract.ImageProgram
	if err := json.Unmarshal(programJSON, &program); err != nil {
		return false, internalError(err, "decode image snapshot for resolution")
	}
	if strings.TrimSpace(evidence.AttemptID) == "" || evidence.ObservedAt.IsZero() {
		return false, protocolError(contract.ErrorInvalidRequest, "image resolution requires source attempt and observed_at")
	}
	if evidence.SubmittedReference != program.Reference {
		return false, protocolError(contract.ErrorConflict, "image resolution reference %q does not match run snapshot %q", evidence.SubmittedReference, program.Reference)
	}
	resolved := program
	resolved.Digest = &evidence.TopLevelDigest
	if err := contract.ValidateImageProgram(resolved, contract.JobClassOneShot); err != nil {
		return false, protocolError(contract.ErrorInvalidRequest, "image resolution digest: %v", err)
	}
	if program.Digest != nil && *program.Digest != evidence.TopLevelDigest {
		return false, protocolError(contract.ErrorConflict, "image resolution digest does not match the pinned run snapshot")
	}
	if evidence.PlatformDigest != nil {
		platform := contract.ImageProgram{Reference: program.Reference, Digest: evidence.PlatformDigest}
		if err := contract.ValidateImageProgram(platform, contract.JobClassOneShot); err != nil {
			return false, protocolError(contract.ErrorInvalidRequest, "platform image resolution digest: %v", err)
		}
	}
	var platformDigest any
	if evidence.PlatformDigest != nil {
		platformDigest = *evidence.PlatformDigest
	}
	observedAt := canonicalTime(evidence.ObservedAt)
	if _, err := tx.ExecContext(ctx, `INSERT INTO run_image_resolutions(run_id, top_level_digest, platform_digest, observed_ns, source_attempt) VALUES(?, ?, ?, ?, ?)`,
		runID, evidence.TopLevelDigest, platformDigest, observedAt.UnixNano(), evidence.AttemptID); err != nil {
		return false, internalError(err, "store image resolution")
	}
	if err := tx.Commit(); err != nil {
		return false, internalError(err, "commit image resolution ingestion")
	}
	return true, nil
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
	if input.ComputerScope != nil {
		scope := input.ComputerScope
		if scope.ComputerID == "" || scope.ComputerAttemptID == "" || scope.HostNodeID == "" ||
			scope.ComputerStorageGeneration < 1 || scope.SubmitIntentRevision < 1 ||
			scope.L3AuthorityGeneration < 1 || scope.GrantRevision < 1 || scope.SubmitMaxInflight < 1 {
			return input, "", 0, protocolError(contract.ErrorUnauthorized, "Computer token scope is incomplete")
		}
		if input.Actor != "computer:"+scope.ComputerID {
			return input, "", 0, protocolError(contract.ErrorForbidden, "Computer run principal is derived from its token scope")
		}
		if input.Request.ParentRunID != "" {
			return input, "", 0, protocolError(contract.ErrorForbidden, "Computer tokens may submit only root Runs")
		}
	}
	request := &input.Request
	request.WorkflowRef = strings.TrimSpace(request.WorkflowRef)
	sources := 0
	if request.WorkflowRef != "" {
		sources++
	}
	if request.InlineScript != nil {
		sources++
	}
	if request.Image != nil {
		sources++
	}
	if sources != 1 {
		return input, "", 0, protocolError(contract.ErrorInvalidRequest, "exactly one of workflow_ref, inline_script, or image is required")
	}
	mode := uint32(0)
	var err error
	if request.InlineScript != nil {
		mode, err = normalizeScript(request.InlineScript.Content, request.InlineScript.SHA256, request.InlineScript.Interpreter, request.InlineScript.Mode, "inline_script")
		if err != nil {
			return input, "", 0, err
		}
	} else if request.WorkflowRef != "" {
		if _, _, err := parseWorkflowRef(request.WorkflowRef); err != nil {
			return input, "", 0, err
		}
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
		if _, err := contract.CompileRestrictedSchema(request.EnvelopeSchema); err != nil {
			return input, "", 0, protocolError(contract.ErrorInvalidRequest, "envelope_schema is not in the restricted dialect: %v", err)
		}
	}
	request.Tags, err = normalizeTags(request.Tags)
	if err != nil {
		return input, "", 0, err
	}
	if request.Image != nil {
		if err := contract.ValidateImageProgram(*request.Image, contract.JobClassOneShot); err != nil {
			return input, "", 0, protocolError(contract.ErrorInvalidRequest, "image program: %v", err)
		}
		if err := contract.ValidatePinnedRouting(*request.Image, request.Tags); err != nil {
			return input, "", 0, protocolError(contract.ErrorInvalidRequest, "image routing: %v", err)
		}
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

// GetRun returns the public contract record reconstructed from its immutable
// typed program and trigger rows.
func (s *Store) GetRun(ctx context.Context, runID string) (contract.RunRecord, error) {
	var record contract.RunRecord
	var parent, l1JobID, nodeID, sourceRun, workflowRef sql.NullString
	var computerID, computerAttemptID sql.NullString
	var computerStorageGeneration, submitIntentRevision sql.NullInt64
	var paramsJSON, tagsJSON []byte
	var limitsJSON []byte
	var content, imageJSON []byte
	var actor, source string
	var sha sql.NullString
	var createdNS, updatedNS int64
	var startedNS, finishedNS sql.NullInt64
	err := s.db.QueryRowContext(ctx, `
SELECT r.run_id, r.parent_run_id, r.l1_job_id, r.node_id, r.dispatch_key, r.status, r.params_json, r.tags_json, r.limits_json,
       r.created_ns, r.updated_ns, r.started_ns, r.finished_ns,
	       s.content, s.sha256, i.program_json, w.workflow_ref, t.actor, t.source, t.source_run_id,
	       t.computer_id, t.computer_attempt_id, t.computer_storage_generation, t.submit_intent_revision
FROM runs r LEFT JOIN run_scripts s ON s.run_id=r.run_id
LEFT JOIN run_images i ON i.run_id=r.run_id
LEFT JOIN run_workflow_refs w ON w.run_id=r.run_id
JOIN run_triggers t ON t.run_id=r.run_id
WHERE r.run_id=?`, runID).Scan(&record.RunID, &parent, &l1JobID, &nodeID, &record.DispatchKey, &record.Status, &paramsJSON, &tagsJSON, &limitsJSON,
		&createdNS, &updatedNS, &startedNS, &finishedNS, &content, &sha, &imageJSON, &workflowRef, &actor, &source, &sourceRun,
		&computerID, &computerAttemptID, &computerStorageGeneration, &submitIntentRevision)
	if errors.Is(err, sql.ErrNoRows) {
		return contract.RunRecord{}, protocolError(contract.ErrorNotFound, "run %q was not found", runID)
	}
	if err != nil {
		return contract.RunRecord{}, internalError(err, "read run")
	}
	record.SchemaVersion = contract.SchemaVersionV1
	record.ParentRunID = parent.String
	record.L1JobID = l1JobID.String
	record.NodeID = nodeID.String
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
	record.Trigger = contract.Trigger{Type: source, Principal: actor, SourceRunID: sourceRun.String,
		ComputerID: computerID.String, ComputerAttemptID: computerAttemptID.String,
		ComputerStorageGeneration: computerStorageGeneration.Int64, SubmitIntentRevision: submitIntentRevision.Int64}
	if workflowRef.Valid {
		record.Workflow = contract.WorkflowSource{WorkflowRef: workflowRef.String}
	} else if len(imageJSON) > 0 {
		record.Workflow.Image = &contract.ImageProgram{}
		if err := json.Unmarshal(imageJSON, record.Workflow.Image); err != nil {
			return contract.RunRecord{}, internalError(err, "decode run image snapshot")
		}
	} else {
		record.Workflow = contract.WorkflowSource{InlineScript: &contract.InlineScript{Content: string(content), SHA256: sha.String}}
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
	record.Envelopes, err = s.ListEnvelopes(ctx, runID)
	if err != nil {
		return contract.RunRecord{}, err
	}
	record.Gates, err = s.ListGateResults(ctx, runID)
	if err != nil {
		return contract.RunRecord{}, err
	}
	return record, nil
}

// GetRunExecution returns the durable L3 half of the run-keyed execution
// projection. The server resolves L1JobID through the L1 client when present.
func (s *Store) GetRunExecution(ctx context.Context, runID string) (RunExecution, error) {
	var projection RunExecution
	var l1JobID sql.NullString
	var dispatchErrorJSON []byte
	err := s.db.QueryRowContext(ctx, `SELECT r.run_id, r.l1_job_id, o.attempt_count, o.last_error
		FROM runs r JOIN dispatch_outbox o ON o.run_id=r.run_id WHERE r.run_id=?`, runID).
		Scan(&projection.RunID, &l1JobID, &projection.DispatchAttempts, &dispatchErrorJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return RunExecution{}, protocolError(contract.ErrorNotFound, "run %q was not found", runID)
	}
	if err != nil {
		return RunExecution{}, internalError(err, "read run execution")
	}
	projection.L1JobID = l1JobID.String
	if len(dispatchErrorJSON) > 0 {
		projection.DispatchError = &contract.APIError{}
		if err := json.Unmarshal(dispatchErrorJSON, projection.DispatchError); err != nil {
			return RunExecution{}, internalError(err, "decode dispatch error")
		}
	}
	return projection, nil
}

func (s *Store) runJobID(ctx context.Context, runID string) (string, bool, error) {
	var jobID sql.NullString
	err := s.db.QueryRowContext(ctx, "SELECT l1_job_id FROM runs WHERE run_id=?", runID).Scan(&jobID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, protocolError(contract.ErrorNotFound, "run %q was not found", runID)
	}
	if err != nil {
		return "", false, internalError(err, "read run job ID")
	}
	return jobID.String, jobID.Valid && jobID.String != "", nil
}

func (s *Store) GetTrigger(ctx context.Context, runID string) (TriggerProvenance, error) {
	var provenance TriggerProvenance
	var sourceRun sql.NullString
	var computerID, computerAttemptID sql.NullString
	var computerStorageGeneration, submitIntentRevision sql.NullInt64
	var params []byte
	var createdNS int64
	err := s.db.QueryRowContext(ctx, `SELECT run_id, actor, source, source_run_id,
computer_id, computer_attempt_id, computer_storage_generation, submit_intent_revision,
params_json, created_ns FROM run_triggers WHERE run_id=?`, runID).
		Scan(&provenance.RunID, &provenance.Actor, &provenance.Source, &sourceRun,
			&computerID, &computerAttemptID, &computerStorageGeneration, &submitIntentRevision,
			&params, &createdNS)
	if errors.Is(err, sql.ErrNoRows) {
		return TriggerProvenance{}, protocolError(contract.ErrorNotFound, "trigger for run %q was not found", runID)
	}
	if err != nil {
		return TriggerProvenance{}, internalError(err, "read trigger provenance")
	}
	provenance.SourceRunID = sourceRun.String
	provenance.ComputerID = computerID.String
	provenance.ComputerAttemptID = computerAttemptID.String
	provenance.ComputerStorageGeneration = computerStorageGeneration.Int64
	provenance.SubmitIntentRevision = submitIntentRevision.Int64
	provenance.Params = append(json.RawMessage(nil), params...)
	provenance.CreatedAt = time.Unix(0, createdNS).UTC()
	return provenance, nil
}

func (s *Store) GetLineage(ctx context.Context, runID string) (RunLineage, error) {
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT 1 FROM runs WHERE run_id=?`, runID).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		return RunLineage{}, protocolError(contract.ErrorNotFound, "run %q was not found", runID)
	} else if err != nil {
		return RunLineage{}, internalError(err, "read lineage target")
	}
	lineage := RunLineage{RunID: runID, Ancestors: []LineageEntry{}, Descendants: []LineageEntry{}}
	ancestors, err := s.db.QueryContext(ctx, `
WITH RECURSIVE ancestors(run_id, parent_run_id, status, depth) AS (
  SELECT run_id, parent_run_id, status, 0 FROM runs WHERE run_id=?
  UNION ALL
  SELECT r.run_id, r.parent_run_id, r.status, a.depth + 1
  FROM runs r JOIN ancestors a ON a.parent_run_id=r.run_id
)
SELECT run_id, COALESCE(parent_run_id, ''), status, depth
FROM ancestors WHERE depth > 0 ORDER BY depth DESC, run_id`, runID)
	if err != nil {
		return RunLineage{}, internalError(err, "list run ancestors")
	}
	for ancestors.Next() {
		var entry LineageEntry
		if err := ancestors.Scan(&entry.RunID, &entry.ParentRunID, &entry.Status, &entry.Depth); err != nil {
			ancestors.Close()
			return RunLineage{}, internalError(err, "scan run ancestor")
		}
		lineage.Ancestors = append(lineage.Ancestors, entry)
	}
	if err := ancestors.Err(); err != nil {
		ancestors.Close()
		return RunLineage{}, internalError(err, "iterate run ancestors")
	}
	ancestors.Close()

	descendants, err := s.db.QueryContext(ctx, `
WITH RECURSIVE descendants(run_id, parent_run_id, status, depth) AS (
  SELECT run_id, parent_run_id, status, 0 FROM runs WHERE run_id=?
  UNION ALL
  SELECT r.run_id, r.parent_run_id, r.status, d.depth + 1
  FROM runs r JOIN descendants d ON r.parent_run_id=d.run_id
)
SELECT run_id, COALESCE(parent_run_id, ''), status, depth
FROM descendants WHERE depth > 0 ORDER BY depth, run_id`, runID)
	if err != nil {
		return RunLineage{}, internalError(err, "list run descendants")
	}
	defer descendants.Close()
	for descendants.Next() {
		var entry LineageEntry
		if err := descendants.Scan(&entry.RunID, &entry.ParentRunID, &entry.Status, &entry.Depth); err != nil {
			return RunLineage{}, internalError(err, "scan run descendant")
		}
		lineage.Descendants = append(lineage.Descendants, entry)
	}
	if err := descendants.Err(); err != nil {
		return RunLineage{}, internalError(err, "iterate run descendants")
	}
	return lineage, nil
}

// AppendEnvelope validates and appends an envelope for the token's own run.
// Invalid protocol payloads are stored as rejections and fail the run before
// the validation error is returned to the caller.
func (s *Store) AppendEnvelope(ctx context.Context, scope RunTokenScope, raw json.RawMessage) (contract.Envelope, bool, error) {
	canonical, hash, err := canonicalProtocolBodyWithAttempt(raw, scope.AttemptID)
	if err != nil {
		return contract.Envelope{}, false, err
	}
	var envelope contract.Envelope
	validationErr := contract.ValidateEnvelopeJSON(canonical)
	if validationErr == nil {
		validationErr = json.Unmarshal(canonical, &envelope)
	}
	if validationErr == nil && envelope.RunID != scope.RunID {
		validationErr = fmt.Errorf("run_id must match the authenticated run")
	}
	if validationErr == nil && envelope.AttemptID != scope.AttemptID {
		validationErr = fmt.Errorf("attempt_id must match the authenticated run attempt")
	}
	if validationErr == nil {
		var callerSchema []byte
		err := s.db.QueryRowContext(ctx, `SELECT envelope_schema_json FROM runs WHERE run_id=?`, scope.RunID).Scan(&callerSchema)
		if errors.Is(err, sql.ErrNoRows) {
			return contract.Envelope{}, false, protocolError(contract.ErrorNotFound, "run %q was not found", scope.RunID)
		}
		if err != nil {
			return contract.Envelope{}, false, internalError(err, "read caller envelope schema")
		}
		if len(callerSchema) > 0 {
			compiled, err := contract.CompileRestrictedSchema(callerSchema)
			if err != nil {
				return contract.Envelope{}, false, internalError(err, "compile stored envelope schema")
			}
			validationErr = contract.ValidateRestrictedSchemaJSON(compiled, canonical)
		}
	}
	if validationErr != nil {
		reason := "envelope validation failed: " + validationErr.Error()
		if err := s.rejectProtocolWrite(ctx, scope.RunID, "envelope", protocolIdempotencyKey(canonical, hash), canonical, hash, reason); err != nil {
			return contract.Envelope{}, false, err
		}
		return contract.Envelope{}, false, protocolError(contract.ErrorInvalidRequest, "%s", reason)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return contract.Envelope{}, false, internalError(err, "begin envelope append")
	}
	defer tx.Rollback()
	var existingHash string
	var existingBody []byte
	err = tx.QueryRowContext(ctx, `SELECT body_hash, body_json FROM envelopes WHERE run_id=? AND idempotency_key=?`, scope.RunID, envelope.IdempotencyKey).Scan(&existingHash, &existingBody)
	if err == nil {
		if existingHash != hash {
			return contract.Envelope{}, false, protocolError(contract.ErrorIdempotencyConflict, "envelope idempotency key %q was already used with a different body", envelope.IdempotencyKey)
		}
		if err := json.Unmarshal(existingBody, &envelope); err != nil {
			return contract.Envelope{}, false, internalError(err, "decode replayed envelope")
		}
		return envelope, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return contract.Envelope{}, false, internalError(err, "read envelope idempotency key")
	}
	var existingRunID string
	if err := tx.QueryRowContext(ctx, `SELECT run_id FROM envelopes WHERE envelope_id=?`, envelope.EnvelopeID).Scan(&existingRunID); err == nil {
		return contract.Envelope{}, false, protocolError(contract.ErrorIdempotencyConflict, "envelope_id %q already exists", envelope.EnvelopeID)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return contract.Envelope{}, false, internalError(err, "read envelope id")
	}
	now := canonicalTime(s.clock.Now())
	if _, err := tx.ExecContext(ctx, `INSERT INTO envelopes(envelope_id, run_id, idempotency_key, body_hash, body_json, accepted_ns) VALUES(?, ?, ?, ?, ?, ?)`,
		envelope.EnvelopeID, scope.RunID, envelope.IdempotencyKey, hash, []byte(canonical), now.UnixNano()); err != nil {
		return contract.Envelope{}, false, internalError(err, "append envelope")
	}
	if err := tx.Commit(); err != nil {
		return contract.Envelope{}, false, internalError(err, "commit envelope append")
	}
	return envelope, false, nil
}

// AppendGateResult validates and appends a workflow-evaluated gate result.
// A fail/error gate is an authoritative protocol failure for the run.
func (s *Store) AppendGateResult(ctx context.Context, scope RunTokenScope, raw json.RawMessage) (contract.GateResult, bool, error) {
	canonical, hash, err := canonicalProtocolBodyWithAttempt(raw, scope.AttemptID)
	if err != nil {
		return contract.GateResult{}, false, err
	}
	var gate contract.GateResult
	validationErr := contract.ValidateGateResultJSON(canonical)
	if validationErr == nil {
		validationErr = json.Unmarshal(canonical, &gate)
	}
	if validationErr == nil && gate.RunID != scope.RunID {
		validationErr = fmt.Errorf("run_id must match the authenticated run")
	}
	if validationErr == nil && gate.AttemptID != scope.AttemptID {
		validationErr = fmt.Errorf("attempt_id must match the authenticated run attempt")
	}
	if validationErr != nil {
		reason := "gate result validation failed: " + validationErr.Error()
		if err := s.rejectProtocolWrite(ctx, scope.RunID, "gate", protocolIdempotencyKey(canonical, hash), canonical, hash, reason); err != nil {
			return contract.GateResult{}, false, err
		}
		return contract.GateResult{}, false, protocolError(contract.ErrorInvalidRequest, "%s", reason)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return contract.GateResult{}, false, internalError(err, "begin gate result append")
	}
	defer tx.Rollback()
	var existingHash string
	var existingBody []byte
	err = tx.QueryRowContext(ctx, `SELECT body_hash, body_json FROM gate_results WHERE run_id=? AND idempotency_key=?`, scope.RunID, gate.IdempotencyKey).Scan(&existingHash, &existingBody)
	if err == nil {
		if existingHash != hash {
			return contract.GateResult{}, false, protocolError(contract.ErrorIdempotencyConflict, "gate idempotency key %q was already used with a different body", gate.IdempotencyKey)
		}
		if err := json.Unmarshal(existingBody, &gate); err != nil {
			return contract.GateResult{}, false, internalError(err, "decode replayed gate result")
		}
		return gate, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return contract.GateResult{}, false, internalError(err, "read gate idempotency key")
	}
	var existingRunID string
	if err := tx.QueryRowContext(ctx, `SELECT run_id FROM gate_results WHERE gate_id=?`, gate.GateID).Scan(&existingRunID); err == nil {
		return contract.GateResult{}, false, protocolError(contract.ErrorIdempotencyConflict, "gate_id %q already exists", gate.GateID)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return contract.GateResult{}, false, internalError(err, "read gate id")
	}
	now := canonicalTime(s.clock.Now())
	if _, err := tx.ExecContext(ctx, `INSERT INTO gate_results(gate_id, run_id, idempotency_key, body_hash, body_json, accepted_ns) VALUES(?, ?, ?, ?, ?, ?)`,
		gate.GateID, scope.RunID, gate.IdempotencyKey, hash, []byte(canonical), now.UnixNano()); err != nil {
		return contract.GateResult{}, false, internalError(err, "append gate result")
	}
	if gate.Outcome == contract.GateFail || gate.Outcome == contract.GateError {
		if err := failRunTx(ctx, tx, scope.RunID, now, s.tokenGrace); err != nil {
			return contract.GateResult{}, false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return contract.GateResult{}, false, internalError(err, "commit gate result append")
	}
	return gate, false, nil
}

func (s *Store) ListEnvelopes(ctx context.Context, runID string) ([]contract.Envelope, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT body_json FROM envelopes WHERE run_id=? ORDER BY accepted_ns, envelope_id`, runID)
	if err != nil {
		return nil, internalError(err, "list envelopes")
	}
	defer rows.Close()
	var values []contract.Envelope
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, internalError(err, "scan envelope")
		}
		var value contract.Envelope
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, internalError(err, "decode envelope")
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, internalError(err, "iterate envelopes")
	}
	return values, nil
}

func (s *Store) ListGateResults(ctx context.Context, runID string) ([]contract.GateResult, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT body_json FROM gate_results WHERE run_id=? ORDER BY accepted_ns, gate_id`, runID)
	if err != nil {
		return nil, internalError(err, "list gate results")
	}
	defer rows.Close()
	var values []contract.GateResult
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, internalError(err, "scan gate result")
		}
		var value contract.GateResult
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, internalError(err, "decode gate result")
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, internalError(err, "iterate gate results")
	}
	return values, nil
}

func (s *Store) ListProtocolRejections(ctx context.Context, runID string) ([]ProtocolRejection, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT rejection_id, run_id, kind, idempotency_key, body_json, reason, created_ns FROM protocol_rejections WHERE run_id=? ORDER BY created_ns, rejection_id`, runID)
	if err != nil {
		return nil, internalError(err, "list protocol rejections")
	}
	defer rows.Close()
	var values []ProtocolRejection
	for rows.Next() {
		var value ProtocolRejection
		var body []byte
		var createdNS int64
		if err := rows.Scan(&value.RejectionID, &value.RunID, &value.Kind, &value.IdempotencyKey, &body, &value.Reason, &createdNS); err != nil {
			return nil, internalError(err, "scan protocol rejection")
		}
		value.Body = append(json.RawMessage(nil), body...)
		value.CreatedAt = time.Unix(0, createdNS).UTC()
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, internalError(err, "iterate protocol rejections")
	}
	return values, nil
}

func canonicalProtocolBody(raw json.RawMessage) (json.RawMessage, string, error) {
	var value any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, "", protocolError(contract.ErrorInvalidRequest, "invalid protocol JSON: %v", err)
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, "", internalError(err, "canonicalize protocol JSON")
	}
	digest := sha256.Sum256(canonical)
	return canonical, hex.EncodeToString(digest[:]), nil
}

// canonicalProtocolBodyWithAttempt binds an omitted attempt_id to the
// authenticated run-token scope before validation and persistence. An
// explicitly supplied attempt remains an assertion by the caller and must
// match the token; it is never silently replaced.
func canonicalProtocolBodyWithAttempt(raw json.RawMessage, attemptID string) (json.RawMessage, string, error) {
	var value any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, "", protocolError(contract.ErrorInvalidRequest, "invalid protocol JSON: %v", err)
	}
	if object, ok := value.(map[string]any); ok {
		if supplied, exists := object["attempt_id"]; !exists {
			object["attempt_id"] = attemptID
		} else if suppliedID, ok := supplied.(string); !ok || suppliedID != attemptID {
			return nil, "", protocolError(contract.ErrorConflict, "attempt_id must match the authenticated run attempt")
		}
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, "", internalError(err, "canonicalize protocol JSON")
	}
	digest := sha256.Sum256(canonical)
	return canonical, hex.EncodeToString(digest[:]), nil
}

func protocolIdempotencyKey(raw json.RawMessage, hash string) string {
	var identity struct {
		IdempotencyKey string `json:"idempotency_key"`
	}
	if json.Unmarshal(raw, &identity) == nil && strings.TrimSpace(identity.IdempotencyKey) != "" {
		return identity.IdempotencyKey
	}
	return "body:" + hash
}

func (s *Store) rejectProtocolWrite(ctx context.Context, runID, kind, idempotencyKey string, body json.RawMessage, hash, reason string) error {
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return internalError(err, "begin protocol rejection")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO protocol_rejections(rejection_id, run_id, kind, idempotency_key, body_hash, body_json, reason, created_ns) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
		newID("reject"), runID, kind, idempotencyKey, hash, []byte(body), reason, now.UnixNano()); err != nil {
		return internalError(err, "store protocol rejection")
	}
	if err := failRunTx(ctx, tx, runID, now, s.tokenGrace); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return internalError(err, "commit protocol rejection")
	}
	return nil
}

func failRunTx(ctx context.Context, tx *sql.Tx, runID string, now time.Time, tokenGrace time.Duration) error {
	result, err := tx.ExecContext(ctx, `UPDATE runs SET status=?, updated_ns=?, started_ns=COALESCE(started_ns, ?), finished_ns=COALESCE(finished_ns, ?) WHERE run_id=? AND status NOT IN (?, ?)`,
		contract.RunFailed, now.UnixNano(), now.UnixNano(), now.UnixNano(), runID, contract.RunSucceeded, contract.RunFailed)
	if err != nil {
		return internalError(err, "fail run protocol")
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return internalError(err, "read protocol failure result")
	}
	if changed == 1 {
		expires := canonicalTime(now.Add(tokenGrace))
		if _, err := tx.ExecContext(ctx, `UPDATE run_tokens SET expires_ns=COALESCE(expires_ns, ?) WHERE run_id=?`, expires.UnixNano(), runID); err != nil {
			return internalError(err, "expire protocol-failed run token")
		}
	}
	return nil
}

type dispatchIntent struct {
	RunID          string
	DispatchKey    string
	ParentRunID    string
	HandoffOwnerID string
	Content        []byte
	SHA256         string
	Interpreter    []string
	Mode           uint32
	Image          *contract.ImageProgram
	Tags           []string
	Limits         *contract.RunLimits
}

func (s *Store) pendingDispatches(ctx context.Context) ([]dispatchIntent, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT r.run_id, r.dispatch_key, COALESCE(r.parent_run_id, ''),
       CASE WHEN t.source='rerun' THEN t.source_run_id ELSE r.run_id END,
       s.content, s.sha256, s.interpreter_json, s.mode, i.program_json, r.tags_json, r.limits_json
FROM dispatch_outbox o JOIN runs r ON r.run_id=o.run_id LEFT JOIN run_scripts s ON s.run_id=r.run_id
LEFT JOIN run_images i ON i.run_id=r.run_id
JOIN run_triggers t ON t.run_id=r.run_id
WHERE o.dispatched_ns IS NULL AND r.status IN (?, ?) ORDER BY r.created_ns, r.run_id`, contract.RunPending, contract.RunDispatching)
	if err != nil {
		return nil, internalError(err, "list pending dispatches")
	}
	defer rows.Close()
	var intents []dispatchIntent
	for rows.Next() {
		var intent dispatchIntent
		var content, interpreterJSON, imageJSON, tagsJSON, limitsJSON []byte
		var sha sql.NullString
		var mode sql.NullInt64
		if err := rows.Scan(&intent.RunID, &intent.DispatchKey, &intent.ParentRunID, &intent.HandoffOwnerID, &content, &sha, &interpreterJSON, &mode, &imageJSON, &tagsJSON, &limitsJSON); err != nil {
			return nil, internalError(err, "scan pending dispatch")
		}
		if len(imageJSON) > 0 {
			intent.Image = &contract.ImageProgram{}
			if err := json.Unmarshal(imageJSON, intent.Image); err != nil {
				return nil, internalError(err, "decode dispatch image program")
			}
		} else {
			intent.Content = content
			intent.SHA256 = sha.String
			intent.Mode = uint32(mode.Int64)
			if err := json.Unmarshal(interpreterJSON, &intent.Interpreter); err != nil {
				return nil, internalError(err, "decode dispatch interpreter")
			}
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
	payload, err := json.Marshal(apiErrorFrom(dispatchErr))
	if err != nil {
		return internalError(err, "encode dispatch error")
	}
	_, err = s.db.ExecContext(ctx, `UPDATE dispatch_outbox SET last_error=? WHERE run_id=? AND dispatched_ns IS NULL`, string(payload), runID)
	if err != nil {
		return internalError(err, "record dispatch error")
	}
	return nil
}

func (s *Store) failDispatch(ctx context.Context, runID string, dispatchErr error) error {
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return internalError(err, "begin permanent dispatch failure")
	}
	defer tx.Rollback()
	payload, err := json.Marshal(apiErrorFrom(dispatchErr))
	if err != nil {
		return internalError(err, "encode permanent dispatch error")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE dispatch_outbox SET last_error=?, token_delivery=NULL WHERE run_id=? AND dispatched_ns IS NULL`, string(payload), runID); err != nil {
		return internalError(err, "record permanent dispatch error")
	}
	if err := failRunTx(ctx, tx, runID, now, s.tokenGrace); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return internalError(err, "commit permanent dispatch failure")
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
	if _, err := tx.ExecContext(ctx, `UPDATE dispatch_outbox SET job_id=?, dispatched_ns=?, last_error=NULL, token_delivery=NULL WHERE run_id=? AND dispatched_ns IS NULL`, jobID, now.UnixNano(), runID); err != nil {
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
	rows, err := s.db.QueryContext(ctx, `
WITH RECURSIVE run_depth(run_id, depth) AS (
  SELECT run_id, 0 FROM runs WHERE parent_run_id IS NULL
  UNION ALL
  SELECT r.run_id, d.depth + 1 FROM runs r JOIN run_depth d ON r.parent_run_id=d.run_id
)
SELECT r.run_id, r.l1_job_id, r.status, r.required_envelope
FROM runs r JOIN run_depth d ON d.run_id=r.run_id
WHERE r.l1_job_id IS NOT NULL AND r.status NOT IN (?, ?)
ORDER BY d.depth DESC, r.created_ns, r.run_id`, contract.RunSucceeded, contract.RunFailed)
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
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return internalError(err, "begin run projection")
	}
	defer tx.Rollback()
	if jobState == contract.JobSucceeded {
		var acceptedEnvelopes, rejectedWrites, failedGates, activeChildren, failedChildren int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM envelopes WHERE run_id=?`, run.RunID).Scan(&acceptedEnvelopes); err != nil {
			return internalError(err, "count accepted envelopes")
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM protocol_rejections WHERE run_id=?`, run.RunID).Scan(&rejectedWrites); err != nil {
			return internalError(err, "count protocol rejections")
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM gate_results WHERE run_id=? AND json_extract(body_json, '$.outcome') IN ('fail', 'error')`, run.RunID).Scan(&failedGates); err != nil {
			return internalError(err, "count failed gates")
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE parent_run_id=? AND status NOT IN (?, ?)`, run.RunID, contract.RunSucceeded, contract.RunFailed).Scan(&activeChildren); err != nil {
			return internalError(err, "count active child runs")
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE parent_run_id=? AND status=?`, run.RunID, contract.RunFailed).Scan(&failedChildren); err != nil {
			return internalError(err, "count failed child runs")
		}
		if rejectedWrites > 0 || failedGates > 0 || failedChildren > 0 || (run.RequiredEnvelope && acceptedEnvelopes == 0) {
			target = contract.RunFailed
		} else if activeChildren > 0 {
			// The parent process has exited successfully, but its run remains
			// non-terminal until every child lineage settles.
			target = contract.RunRunning
			if run.State == target {
				return nil
			}
		}
	}
	started := target == contract.RunRunning || target == contract.RunAwaitingInput || target == contract.RunSucceeded || target == contract.RunFailed
	finished := target == contract.RunSucceeded || target == contract.RunFailed
	result, err := tx.ExecContext(ctx, `
UPDATE runs SET status=?, updated_ns=?,
  started_ns=CASE WHEN ? THEN COALESCE(started_ns, ?) ELSE started_ns END,
  finished_ns=CASE WHEN ? THEN ? ELSE finished_ns END
WHERE run_id=? AND status=?`, target, now.UnixNano(), started, now.UnixNano(), finished, now.UnixNano(), run.RunID, run.State)
	if err != nil {
		return internalError(err, "project job state onto run")
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return internalError(err, "read run projection result")
	}
	if changed == 1 && finished {
		expires := canonicalTime(now.Add(s.tokenGrace))
		if _, err := tx.ExecContext(ctx, `UPDATE run_tokens SET expires_ns=COALESCE(expires_ns, ?) WHERE run_id=?`, expires.UnixNano(), run.RunID); err != nil {
			return internalError(err, "expire terminal run token")
		}
	}
	if err := tx.Commit(); err != nil {
		return internalError(err, "commit run projection")
	}
	return nil
}

func (s *Store) recordRunNode(ctx context.Context, runID, nodeID string) error {
	if nodeID == "" {
		return nil
	}
	now := canonicalTime(s.clock.Now())
	if _, err := s.db.ExecContext(ctx, `UPDATE runs SET node_id=?, updated_ns=? WHERE run_id=? AND COALESCE(node_id, '')=''`, nodeID, now.UnixNano(), runID); err != nil {
		return internalError(err, "record run node attribution")
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
	case contract.JobStopping, contract.JobStopped:
		// Service jobs have no L3 run, so their lifecycle-only states have no
		// valid projection onto the one-shot run state machine.
		return current, false, protocolError(contract.ErrorInvalidRequest, "service job state %q cannot project onto a run", job)
	case contract.JobAwaitingInput:
		target = contract.RunAwaitingInput
	case contract.JobSucceeded:
		if current == contract.RunQueued {
			// Polling can miss L1's claimed/running states. Preserve the locked
			// run transition table by projecting the observed success through
			// running; the next reconciliation pass projects the terminal state.
			target = contract.RunRunning
		} else {
			target = contract.RunSucceeded
		}
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

func (intent dispatchIntent) jobSpec(runToken string) contract.JobSpec {
	handoffOwnerID := intent.HandoffOwnerID
	if handoffOwnerID == "" {
		handoffOwnerID = intent.RunID
	}
	handoff := filepath.Join(DefaultHandoffRoot, handoffOwnerID)
	labels := map[string]string{"run_id": intent.RunID}
	if handoffOwnerID != intent.RunID {
		labels["handoff_owner_run_id"] = handoffOwnerID
	}
	if intent.ParentRunID != "" {
		labels["parent_run_id"] = intent.ParentRunID
	}
	var limits *contract.JobLimits
	if intent.Limits != nil && intent.Limits.MaxRuntimeSeconds > 0 {
		limits = &contract.JobLimits{MaxRuntimeSeconds: intent.Limits.MaxRuntimeSeconds}
	}
	if intent.Image != nil {
		program := intent.Image
		return contract.JobSpec{
			SchemaVersion:  contract.SchemaVersionV1,
			DispatchKey:    intent.DispatchKey,
			Kind:           contract.JobKindOCI,
			Class:          contract.JobClassOneShot,
			RuntimeHandler: program.RuntimeHandler,
			RoutingTags:    append([]string(nil), intent.Tags...),
			Execution: contract.ExecutionSpec{
				Env: map[string]string{
					contract.EnvRunID: intent.RunID, contract.EnvL3Endpoint: DefaultL3Address,
					contract.EnvHandoffDir: contract.OCIContainerHandoffDirectory,
				},
				SensitiveEnv: map[string]string{contract.EnvRunToken: runToken},
				OCI: &contract.OCIExecutionSpec{
					Image: contract.OCIImageSpec{
						Reference: program.Reference,
						Digest:    cloneStringPointer(program.Digest),
					},
					Argv:             append([]string(nil), program.Argv...),
					WorkingDirectory: cloneStringPointer(program.WorkingDirectory),
					Mounts:           append([]contract.OCIMount(nil), program.Mounts...),
					Limits:           cloneOCILimits(program.Limits),
				},
			},
			Limits: limits,
			Labels: labels,
		}
	}
	return contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		DispatchKey:   intent.DispatchKey,
		Kind:          contract.JobKindProcess,
		Class:         contract.JobClassOneShot,
		RoutingTags:   append([]string(nil), intent.Tags...),
		Execution: contract.ExecutionSpec{
			Executable: contract.ExecutableSpec{
				InlineBase64: encodeBase64(intent.Content), SHA256: intent.SHA256,
				Interpreter: append([]string(nil), intent.Interpreter...), Mode: intent.Mode,
			},
			Argv: []string{"wefty-inline-" + intent.RunID},
			Env: map[string]string{
				contract.EnvRunID: intent.RunID, contract.EnvL3Endpoint: DefaultL3Address,
				contract.EnvHandoffDir: handoff,
			},
			SensitiveEnv:     map[string]string{contract.EnvRunToken: runToken},
			WorkingDirectory: "/tmp",
			HandoffDirectory: handoff,
		},
		Limits: limits,
		Labels: labels,
	}
}

func cloneStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneOCILimits(value *contract.OCILimits) *contract.OCILimits {
	if value == nil {
		return nil
	}
	return &contract.OCILimits{
		MemoryBytes:   cloneInt64Pointer(value.MemoryBytes),
		CPUMillicores: cloneInt64Pointer(value.CPUMillicores),
	}
}

func cloneInt64Pointer(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
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

func newToken() string {
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		panic(fmt.Sprintf("l3: crypto/rand: %v", err))
	}
	return "wrun_" + hex.EncodeToString(random[:])
}
