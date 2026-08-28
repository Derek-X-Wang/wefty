package l1

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	_ "modernc.org/sqlite"
)

const (
	DefaultLogPageLimit   = 200
	MaxLogPageLimit       = 1000
	MaxLogBatchEvents     = 256
	MaxLogEventBytes      = 4 << 20
	MaxLogUploadBodyBytes = 20 << 20
)

type StoreOptions struct {
	Clock                        Clock
	Jitter                       func(time.Duration) time.Duration
	LeaseDuration                time.Duration
	LateEvidenceWindow           time.Duration
	NodeStaleAfter               time.Duration
	NodeDeadAfter                time.Duration
	ServiceStabilityWindow       time.Duration
	ServiceLogRetentionBytes     int64
	ServiceLogRetentionAge       time.Duration
	PrestartInfrastructureBudget time.Duration
	AdminBootstrapTTL            time.Duration
}

// Store is the durable SQLite substrate for L1 queue operations.
type Store struct {
	db                           *sql.DB
	clock                        Clock
	restartJitter                func(time.Duration) time.Duration
	leaseDuration                time.Duration
	lateEvidenceWindow           time.Duration
	nodeStaleAfter               time.Duration
	nodeDeadAfter                time.Duration
	serviceStabilityWindow       time.Duration
	serviceLogRetentionBytes     int64
	serviceLogRetentionAge       time.Duration
	prestartInfrastructureBudget time.Duration
	adminBootstrapTTL            time.Duration
}

// OpenStore opens a real SQLite database, enables WAL, and applies the L1
// schema. path should name a file so queue concurrency has production SQLite
// semantics rather than an in-memory approximation.
func OpenStore(path string, options StoreOptions) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("l1: SQLite path is required")
	}
	clock := options.Clock
	if clock == nil {
		clock = systemClock{}
	}
	leaseDuration := options.LeaseDuration
	if leaseDuration <= 0 {
		leaseDuration = DefaultLeaseDuration
	}
	restartJitter := options.Jitter
	if restartJitter == nil {
		restartJitter = defaultRestartJitter
	}
	lateEvidenceWindow := options.LateEvidenceWindow
	if lateEvidenceWindow <= 0 {
		lateEvidenceWindow = DefaultLateEvidenceWindow
	}
	nodeStaleAfter := options.NodeStaleAfter
	if nodeStaleAfter <= 0 {
		nodeStaleAfter = DefaultNodeStaleAfter
	}
	nodeDeadAfter := options.NodeDeadAfter
	if nodeDeadAfter <= 0 {
		nodeDeadAfter = DefaultNodeDeadAfter
	}
	if nodeDeadAfter <= nodeStaleAfter {
		return nil, fmt.Errorf("l1: node dead threshold must exceed stale threshold")
	}
	serviceStabilityWindow := options.ServiceStabilityWindow
	if serviceStabilityWindow <= 0 {
		serviceStabilityWindow = DefaultServiceStabilityWindow
	}
	serviceLogRetentionBytes := options.ServiceLogRetentionBytes
	if serviceLogRetentionBytes < 0 {
		return nil, fmt.Errorf("l1: service log retention bytes must be non-negative")
	}
	if serviceLogRetentionBytes == 0 {
		serviceLogRetentionBytes = DefaultServiceLogRetentionBytes
	}
	serviceLogRetentionAge := options.ServiceLogRetentionAge
	if serviceLogRetentionAge < 0 {
		return nil, fmt.Errorf("l1: service log retention age must be non-negative")
	}
	if serviceLogRetentionAge == 0 {
		serviceLogRetentionAge = DefaultServiceLogRetentionAge
	}
	prestartInfrastructureBudget := options.PrestartInfrastructureBudget
	if prestartInfrastructureBudget <= 0 {
		prestartInfrastructureBudget = DefaultPrestartInfrastructureBudget
	}
	adminBootstrapTTL := options.AdminBootstrapTTL
	if adminBootstrapTTL < 0 {
		return nil, fmt.Errorf("l1: admin bootstrap TTL must be non-negative")
	}
	if adminBootstrapTTL == 0 {
		adminBootstrapTTL = DefaultAdminBootstrapTTL
	}

	query := make(url.Values)
	query.Add("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", "secure_delete(1)")
	query.Set("_txlock", "immediate")
	dsn := (&url.URL{Scheme: "file", Path: path, RawQuery: query.Encode()}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("l1: open SQLite: %w", err)
	}
	db.SetMaxOpenConns(16)
	store := &Store{
		db: db, clock: clock, restartJitter: restartJitter, leaseDuration: leaseDuration, lateEvidenceWindow: lateEvidenceWindow,
		nodeStaleAfter: nodeStaleAfter, nodeDeadAfter: nodeDeadAfter, serviceStabilityWindow: serviceStabilityWindow,
		serviceLogRetentionBytes: serviceLogRetentionBytes, serviceLogRetentionAge: serviceLogRetentionAge,
		prestartInfrastructureBudget: prestartInfrastructureBudget,
		adminBootstrapTTL:            adminBootstrapTTL,
	}
	if err := store.initialize(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) initialize(ctx context.Context) error {
	var mode string
	if err := s.db.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&mode); err != nil {
		return fmt.Errorf("l1: enable SQLite WAL: %w", err)
	}
	if !strings.EqualFold(mode, "wal") {
		return fmt.Errorf("l1: SQLite did not enable WAL (mode %q)", mode)
	}
	const schema = `
CREATE TABLE IF NOT EXISTS jobs (
  job_id TEXT PRIMARY KEY,
  dispatch_key TEXT NOT NULL UNIQUE,
  request_hash TEXT NOT NULL,
  spec_json BLOB NOT NULL,
  state TEXT NOT NULL,
  current_attempt_id TEXT,
  fence_counter INTEGER NOT NULL DEFAULT 0,
  prestart_retry_count INTEGER NOT NULL DEFAULT 0 CHECK(prestart_retry_count >= 0),
  prestart_budget_deadline_ns INTEGER,
  prestart_next_retry_at_ns INTEGER,
  prestart_terminal_reason TEXT,
  image_resolution_json BLOB,
  image_resolution_hash TEXT,
  created_ns INTEGER NOT NULL,
  updated_ns INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS jobs_claim_order ON jobs(state, created_ns, job_id);
CREATE TABLE IF NOT EXISTS job_tags (
  job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
  tag TEXT NOT NULL,
  PRIMARY KEY (job_id, tag)
);
CREATE TABLE IF NOT EXISTS job_required_capabilities (
  job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
  capability TEXT NOT NULL,
  PRIMARY KEY (job_id, capability)
);
CREATE TABLE IF NOT EXISTS nodes (
  node_id TEXT PRIMARY KEY,
  identity_node_id TEXT NOT NULL,
  boot_session_id TEXT NOT NULL,
  connect_host TEXT NOT NULL DEFAULT '',
  root_instance_id TEXT NOT NULL DEFAULT '',
  os TEXT NOT NULL,
  architecture TEXT NOT NULL,
  agent_version TEXT NOT NULL,
  capabilities_json BLOB NOT NULL,
  capability_revision INTEGER NOT NULL DEFAULT 1 CHECK(capability_revision >= 1),
  capability_observed_ns INTEGER NOT NULL DEFAULT 0,
  missing_capabilities_json BLOB NOT NULL DEFAULT '[]',
  capability_reason_code TEXT NOT NULL DEFAULT '',
  state TEXT NOT NULL,
  last_heartbeat_ns INTEGER NOT NULL,
  max_oneshot_slots INTEGER NOT NULL CHECK(max_oneshot_slots >= 0),
  max_service_slots INTEGER NOT NULL CHECK(max_service_slots >= 0),
  authority_generation INTEGER NOT NULL DEFAULT 0 CHECK(authority_generation >= 0),
  claims_enabled INTEGER NOT NULL DEFAULT 0 CHECK(claims_enabled IN (0, 1)),
  intent_revision INTEGER NOT NULL DEFAULT 0 CHECK(intent_revision >= 0),
  intent_reason TEXT NOT NULL DEFAULT '',
  intent_updated_at INTEGER,
  intent_actor TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS node_tags (
  node_id TEXT NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
  tag TEXT NOT NULL,
  PRIMARY KEY (node_id, tag)
);
CREATE TABLE IF NOT EXISTS attempts (
  attempt_id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES jobs(job_id),
  node_id TEXT NOT NULL REFERENCES nodes(node_id),
  boot_session_id TEXT NOT NULL,
  state TEXT NOT NULL,
  fencing_token TEXT NOT NULL,
  lease_expires_ns INTEGER NOT NULL,
  authority_generation INTEGER NOT NULL DEFAULT 0 CHECK(authority_generation >= 0),
  completion_key TEXT,
  completion_hash TEXT,
  result_json BLOB,
  image_observation_json BLOB,
  image_observation_hash TEXT,
  started_ns INTEGER,
  late_result_json BLOB,
  late_result_observed_ns INTEGER,
  late_result_authority_lost_ns INTEGER,
  late_result_is_late INTEGER NOT NULL DEFAULT 0 CHECK(late_result_is_late IN (0, 1)),
  created_ns INTEGER NOT NULL,
  updated_ns INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS attempts_job ON attempts(job_id);
CREATE TABLE IF NOT EXISTS service_jobs (
  job_id TEXT PRIMARY KEY REFERENCES jobs(job_id) ON DELETE CASCADE,
  desired_state TEXT NOT NULL CHECK(desired_state IN ('running', 'stopped')),
  bound_node_id TEXT REFERENCES nodes(node_id),
  restart_streak INTEGER NOT NULL DEFAULT 0 CHECK(restart_streak >= 0),
  lifetime_restart_count INTEGER NOT NULL DEFAULT 0 CHECK(lifetime_restart_count >= 0),
  next_restart_at INTEGER,
  published_port INTEGER CHECK(published_port BETWEEN 1 AND 65535),
  last_failure BLOB,
  healthy_since_ns INTEGER,
  published_attempt_id TEXT REFERENCES attempts(attempt_id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS service_jobs_bound_desired ON service_jobs(bound_node_id, desired_state);
CREATE TABLE IF NOT EXISTS computers (
  computer_id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  placement_node_id TEXT NOT NULL,
  bound_node_id TEXT,
  grants_json BLOB NOT NULL,
  storage_id TEXT NOT NULL UNIQUE,
  storage_generation INTEGER NOT NULL CHECK(storage_generation > 0),
  desired_state TEXT NOT NULL CHECK(desired_state IN ('running', 'stopped', 'removed')),
  intent_revision INTEGER NOT NULL CHECK(intent_revision > 0),
  applied_revision INTEGER NOT NULL CHECK(applied_revision >= 0 AND applied_revision <= intent_revision),
  current_job_id TEXT NOT NULL UNIQUE REFERENCES jobs(job_id),
  current_spec_revision INTEGER NOT NULL CHECK(current_spec_revision > 0),
  reconfiguration_phase TEXT NOT NULL CHECK(reconfiguration_phase IN ('stable', 'projecting', 'resetting', 'removing')),
  reconfiguration_revision INTEGER CHECK(reconfiguration_revision > 0),
  created_ns INTEGER NOT NULL,
  updated_ns INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS computers_binding ON computers(bound_node_id, desired_state);
CREATE TABLE IF NOT EXISTS computer_job_projections (
  computer_id TEXT NOT NULL REFERENCES computers(computer_id) ON DELETE CASCADE,
  job_id TEXT NOT NULL UNIQUE REFERENCES jobs(job_id),
  spec_revision INTEGER NOT NULL CHECK(spec_revision > 0),
  current INTEGER NOT NULL CHECK(current IN (0, 1)),
  created_ns INTEGER NOT NULL,
  retired_ns INTEGER,
  PRIMARY KEY(computer_id, spec_revision)
);
CREATE UNIQUE INDEX IF NOT EXISTS computer_current_projection
  ON computer_job_projections(computer_id) WHERE current=1;
CREATE TABLE IF NOT EXISTS computer_intent_history (
  computer_id TEXT NOT NULL REFERENCES computers(computer_id) ON DELETE CASCADE,
  intent_revision INTEGER NOT NULL CHECK(intent_revision > 0),
  operation TEXT NOT NULL CHECK(operation IN ('create', 'start', 'stop', 'restart', 'remove', 'project', 'reset')),
  desired_state TEXT NOT NULL CHECK(desired_state IN ('running', 'stopped', 'removed')),
  storage_id TEXT NOT NULL,
  storage_generation INTEGER NOT NULL CHECK(storage_generation > 0),
  job_id TEXT NOT NULL REFERENCES jobs(job_id),
  spec_revision INTEGER NOT NULL CHECK(spec_revision > 0),
  actor TEXT NOT NULL,
  created_ns INTEGER NOT NULL,
  PRIMARY KEY(computer_id, intent_revision)
);
CREATE TABLE IF NOT EXISTS computer_storage_generations (
  computer_id TEXT NOT NULL REFERENCES computers(computer_id) ON DELETE CASCADE,
  storage_id TEXT NOT NULL,
  storage_generation INTEGER NOT NULL CHECK(storage_generation > 0),
  disk_bytes INTEGER NOT NULL CHECK(disk_bytes > 0),
  phase TEXT NOT NULL CHECK(phase IN ('current', 'staging', 'retired')),
  reset_revision INTEGER CHECK(reset_revision > 0),
  created_ns INTEGER NOT NULL,
  retired_ns INTEGER,
  PRIMARY KEY(computer_id, storage_generation),
  UNIQUE(storage_id, storage_generation)
);
CREATE UNIQUE INDEX IF NOT EXISTS computer_storage_current
  ON computer_storage_generations(computer_id) WHERE phase='current';
CREATE UNIQUE INDEX IF NOT EXISTS computer_storage_staging
  ON computer_storage_generations(computer_id) WHERE phase='staging';
CREATE TABLE IF NOT EXISTS computer_storage_resets (
  computer_id TEXT NOT NULL REFERENCES computers(computer_id) ON DELETE CASCADE,
  intent_revision INTEGER NOT NULL CHECK(intent_revision > 0),
  storage_id TEXT NOT NULL,
  old_generation INTEGER NOT NULL CHECK(old_generation > 0),
  new_generation INTEGER NOT NULL CHECK(new_generation > old_generation),
	  disk_bytes INTEGER NOT NULL CHECK(disk_bytes > 0),
	  bound_node_id TEXT NOT NULL,
	  root_instance_id TEXT NOT NULL,
	  job_id TEXT NOT NULL REFERENCES jobs(job_id),
  cleanup_fence TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
	  status TEXT NOT NULL CHECK(status IN ('reserved', 'prepared', 'published', 'retired', 'superseded')),
  verification_receipt_json BLOB,
  verification_receipt_hash TEXT,
  acknowledgement_key TEXT,
  acknowledgement_hash TEXT,
  requested_ns INTEGER NOT NULL,
  verified_ns INTEGER,
  published_ns INTEGER,
  PRIMARY KEY(computer_id, intent_revision),
  UNIQUE(computer_id, idempotency_key)
);
CREATE UNIQUE INDEX IF NOT EXISTS computer_storage_reset_active
	  ON computer_storage_resets(computer_id) WHERE status NOT IN ('retired', 'superseded');
CREATE TABLE IF NOT EXISTS admin_policy (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
  revision INTEGER NOT NULL CHECK(revision >= 0),
  updated_ns INTEGER NOT NULL CHECK(updated_ns >= 0)
);
INSERT OR IGNORE INTO admin_policy(singleton, revision, updated_ns) VALUES(1, 0, 0);
CREATE TABLE IF NOT EXISTS admins (
  user_id TEXT PRIMARY KEY,
  added_revision INTEGER NOT NULL CHECK(added_revision > 0),
  added_ns INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS admin_policy_audit (
  revision INTEGER PRIMARY KEY CHECK(revision > 0),
  operation TEXT NOT NULL CHECK(operation IN ('bootstrap', 'add', 'remove')),
  actor_user_id TEXT NOT NULL,
  actor_device_id TEXT NOT NULL,
  subject_user_id TEXT NOT NULL,
  created_ns INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS admin_bootstrap_challenges (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
  nonce_hash TEXT NOT NULL UNIQUE,
  created_ns INTEGER NOT NULL,
  expires_ns INTEGER NOT NULL CHECK(expires_ns > created_ns)
);
CREATE TABLE IF NOT EXISTS service_restart_requests (
  job_id TEXT NOT NULL REFERENCES service_jobs(job_id) ON DELETE CASCADE,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  created_ns INTEGER NOT NULL,
  PRIMARY KEY(job_id, idempotency_key)
);
CREATE TABLE IF NOT EXISTS log_events (
  ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
  attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id) ON DELETE CASCADE,
  stream TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  sequence_end INTEGER NOT NULL,
  timestamp_ns INTEGER NOT NULL,
  bytes BLOB NOT NULL,
  event_json BLOB NOT NULL,
  UNIQUE(attempt_id, stream, sequence)
);
CREATE INDEX IF NOT EXISTS log_events_job_order ON log_events(job_id, ordinal);
CREATE TABLE IF NOT EXISTS service_log_truncations (
  job_id TEXT PRIMARY KEY REFERENCES service_jobs(job_id) ON DELETE CASCADE,
  bound_kind TEXT NOT NULL CHECK(bound_kind IN ('bytes', 'age')),
  evicted_event_count INTEGER NOT NULL CHECK(evicted_event_count >= 0),
  evicted_byte_count INTEGER NOT NULL CHECK(evicted_byte_count >= 0),
  evicted_through_ordinal INTEGER NOT NULL CHECK(evicted_through_ordinal >= 0),
  earliest_retained_ns INTEGER,
  updated_ns INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS job_log_jsonl (
  job_id TEXT PRIMARY KEY REFERENCES jobs(job_id) ON DELETE CASCADE,
  jsonl BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS service_removals (
  job_id TEXT PRIMARY KEY REFERENCES jobs(job_id) ON DELETE CASCADE,
  bound_node_id TEXT NOT NULL,
  removal_generation INTEGER NOT NULL CHECK(removal_generation > 0),
  cleanup_fence TEXT NOT NULL,
  root_instance_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('removal_pending', 'agent_cleaned', 'removed_verified', 'forgotten_cleanup_unverified')),
  requested_ns INTEGER NOT NULL,
  cleanup_acknowledgement_key TEXT,
  cleanup_acknowledgement_hash TEXT,
  agent_cleaned_ns INTEGER,
  removed_ns INTEGER
);
CREATE TABLE IF NOT EXISTS service_tombstones (
  job_id TEXT PRIMARY KEY,
  dispatch_key_hash TEXT NOT NULL UNIQUE,
  request_hash TEXT NOT NULL,
  created_ns INTEGER NOT NULL,
  removal_requested_ns INTEGER NOT NULL,
  removed_ns INTEGER NOT NULL,
  outcome TEXT NOT NULL CHECK(outcome IN ('verified_removed', 'force_forgotten')),
  last_bound_node_id TEXT NOT NULL,
  removal_generation INTEGER NOT NULL CHECK(removal_generation > 0),
  root_instance_id TEXT NOT NULL,
  cleanup_acknowledged_ns INTEGER
);
INSERT OR IGNORE INTO job_log_jsonl(job_id, jsonl) SELECT job_id, X'' FROM jobs;
`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("l1: apply SQLite schema: %w", err)
	}
	if err := s.migrateComputerResetConstraints(ctx); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO computer_storage_generations(
		computer_id, storage_id, storage_generation, disk_bytes, phase, created_ns
	) SELECT c.computer_id, c.storage_id, c.storage_generation,
		CAST(json_extract(j.spec_json, '$.execution.oci.computer.disk_bytes') AS INTEGER),
		'current', c.created_ns
	FROM computers c JOIN jobs j ON j.job_id=c.current_job_id`); err != nil {
		return fmt.Errorf("l1: backfill Computer Storage generations: %w", err)
	}
	return nil
}

// migrateComputerResetConstraints widens the two CHECK constraints introduced
// before Storage reset existed. CREATE TABLE IF NOT EXISTS cannot change those
// constraints in an already durable L1 database.
func (s *Store) migrateComputerResetConstraints(ctx context.Context) error {
	connection, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("l1: acquire SQLite schema migration connection: %w", err)
	}
	defer connection.Close()
	var computersSQL, intentsSQL string
	if err := connection.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='table' AND name='computers'`).Scan(&computersSQL); err != nil {
		return fmt.Errorf("l1: inspect Computer schema: %w", err)
	}
	if err := connection.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='table' AND name='computer_intent_history'`).Scan(&intentsSQL); err != nil {
		return fmt.Errorf("l1: inspect Computer intent schema: %w", err)
	}
	migrateComputers := !strings.Contains(computersSQL, "'resetting'")
	migrateIntents := !strings.Contains(intentsSQL, "'reset'")
	if !migrateComputers && !migrateIntents {
		return nil
	}
	if _, err := connection.ExecContext(ctx, "PRAGMA foreign_keys=OFF"); err != nil {
		return fmt.Errorf("l1: disable foreign keys for Computer reset migration: %w", err)
	}
	defer connection.ExecContext(context.Background(), "PRAGMA foreign_keys=ON")
	transaction, err := connection.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("l1: begin Computer reset schema migration: %w", err)
	}
	defer transaction.Rollback()
	if migrateComputers {
		if _, err := transaction.ExecContext(ctx, `CREATE TABLE computers_reset_migration (
			computer_id TEXT PRIMARY KEY,
			name TEXT NOT NULL UNIQUE,
			placement_node_id TEXT NOT NULL,
			bound_node_id TEXT,
			grants_json BLOB NOT NULL,
			storage_id TEXT NOT NULL UNIQUE,
			storage_generation INTEGER NOT NULL CHECK(storage_generation > 0),
			desired_state TEXT NOT NULL CHECK(desired_state IN ('running', 'stopped', 'removed')),
			intent_revision INTEGER NOT NULL CHECK(intent_revision > 0),
			applied_revision INTEGER NOT NULL CHECK(applied_revision >= 0 AND applied_revision <= intent_revision),
			current_job_id TEXT NOT NULL UNIQUE REFERENCES jobs(job_id),
			current_spec_revision INTEGER NOT NULL CHECK(current_spec_revision > 0),
			reconfiguration_phase TEXT NOT NULL CHECK(reconfiguration_phase IN ('stable', 'projecting', 'resetting', 'removing')),
			reconfiguration_revision INTEGER CHECK(reconfiguration_revision > 0),
			created_ns INTEGER NOT NULL,
			updated_ns INTEGER NOT NULL
		)`); err != nil {
			return fmt.Errorf("l1: create widened Computer schema: %w", err)
		}
		if _, err := transaction.ExecContext(ctx, `INSERT INTO computers_reset_migration SELECT * FROM computers`); err != nil {
			return fmt.Errorf("l1: copy Computer authority during migration: %w", err)
		}
		if _, err := transaction.ExecContext(ctx, `DROP TABLE computers`); err != nil {
			return fmt.Errorf("l1: replace old Computer schema: %w", err)
		}
		if _, err := transaction.ExecContext(ctx, `ALTER TABLE computers_reset_migration RENAME TO computers`); err != nil {
			return fmt.Errorf("l1: publish widened Computer schema: %w", err)
		}
		if _, err := transaction.ExecContext(ctx, `CREATE INDEX computers_binding ON computers(bound_node_id, desired_state)`); err != nil {
			return fmt.Errorf("l1: restore Computer binding index: %w", err)
		}
	}
	if migrateIntents {
		if _, err := transaction.ExecContext(ctx, `CREATE TABLE computer_intent_history_reset_migration (
			computer_id TEXT NOT NULL REFERENCES computers(computer_id) ON DELETE CASCADE,
			intent_revision INTEGER NOT NULL CHECK(intent_revision > 0),
			operation TEXT NOT NULL CHECK(operation IN ('create', 'start', 'stop', 'restart', 'remove', 'project', 'reset')),
			desired_state TEXT NOT NULL CHECK(desired_state IN ('running', 'stopped', 'removed')),
			storage_id TEXT NOT NULL,
			storage_generation INTEGER NOT NULL CHECK(storage_generation > 0),
			job_id TEXT NOT NULL REFERENCES jobs(job_id),
			spec_revision INTEGER NOT NULL CHECK(spec_revision > 0),
			actor TEXT NOT NULL,
			created_ns INTEGER NOT NULL,
			PRIMARY KEY(computer_id, intent_revision)
		)`); err != nil {
			return fmt.Errorf("l1: create widened Computer intent schema: %w", err)
		}
		if _, err := transaction.ExecContext(ctx, `INSERT INTO computer_intent_history_reset_migration SELECT * FROM computer_intent_history`); err != nil {
			return fmt.Errorf("l1: copy Computer intents during migration: %w", err)
		}
		if _, err := transaction.ExecContext(ctx, `DROP TABLE computer_intent_history`); err != nil {
			return fmt.Errorf("l1: replace old Computer intent schema: %w", err)
		}
		if _, err := transaction.ExecContext(ctx, `ALTER TABLE computer_intent_history_reset_migration RENAME TO computer_intent_history`); err != nil {
			return fmt.Errorf("l1: publish widened Computer intent schema: %w", err)
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("l1: commit Computer reset schema migration: %w", err)
	}
	if _, err := connection.ExecContext(ctx, "PRAGMA foreign_keys=ON"); err != nil {
		return fmt.Errorf("l1: restore foreign keys after Computer reset migration: %w", err)
	}
	rows, err := connection.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("l1: verify Computer reset schema migration: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		return errors.New("l1: Computer reset schema migration violated a foreign key")
	}
	return rows.Err()
}

func (s *Store) Close() error { return s.db.Close() }

// CreateJob creates a job or returns the identical dispatch-key replay.
func (s *Store) CreateJob(ctx context.Context, spec contract.JobSpec) (job Job, replayed bool, err error) {
	// Preserve the v1 submission contract: L1 canonicalizes routing tags before
	// structural validation and persistence.
	spec.RoutingTags = NormalizeTags(spec.RoutingTags)
	if err := validateJobSpec(&spec); err != nil {
		return Job{}, false, err
	}
	if isComputerSpec(spec) {
		return Job{}, false, protocolError(contract.ErrorComputerResourceRequired,
			"Computer trait Jobs must be created through the durable Computer resource")
	}
	requiredCapabilities := RequiredCapabilities(spec)
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return Job{}, false, internalError(err, "encode job specification")
	}
	hash := sha256.Sum256(specJSON)
	requestHash := hex.EncodeToString(hash[:])

	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, false, internalError(err, "begin job creation")
	}
	defer tx.Rollback()

	job, storedHash, err := getJobByDispatchKey(ctx, tx, spec.DispatchKey, now)
	if err == nil {
		if storedHash != requestHash {
			return Job{}, false, protocolError(contract.ErrorDispatchKeyConflict, "dispatch key %q was already used with a different job", spec.DispatchKey)
		}
		return job, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Job{}, false, internalError(err, "read dispatch key")
	}
	tombstone, err := readServiceTombstoneByDispatchHash(ctx, tx, hashDispatchKey(spec.DispatchKey))
	if err == nil {
		if tombstone.requestHash != requestHash {
			return Job{}, false, protocolError(contract.ErrorDispatchKeyConflict, "dispatch key %q was already used with a different job", spec.DispatchKey)
		}
		return tombstone.job(), true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Job{}, false, internalError(err, "read removed dispatch key")
	}

	job = Job{
		JobID:     newID("job"),
		State:     contract.JobQueued,
		Spec:      spec,
		CreatedAt: now,
		UpdatedAt: now,
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO jobs(job_id, dispatch_key, request_hash, spec_json, state, created_ns, updated_ns)
VALUES(?, ?, ?, ?, ?, ?, ?)`, job.JobID, spec.DispatchKey, requestHash, specJSON, job.State, now.UnixNano(), now.UnixNano())
	if err != nil {
		// A concurrent identical submit can win the unique dispatch key. Read
		// it after rolling this transaction back and preserve replay semantics.
		_ = tx.Rollback()
		return s.readConcurrentSubmit(ctx, spec.DispatchKey, requestHash, err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO job_log_jsonl(job_id, jsonl) VALUES(?, ?)", job.JobID, []byte{}); err != nil {
		return Job{}, false, internalError(err, "initialize authoritative job log")
	}
	for _, capability := range requiredCapabilities {
		if _, err := tx.ExecContext(ctx, "INSERT INTO job_required_capabilities(job_id, capability) VALUES(?, ?)", job.JobID, capability); err != nil {
			return Job{}, false, internalError(err, "store required job capability")
		}
	}
	if spec.Class == contract.JobClassService {
		if _, err := tx.ExecContext(ctx, `INSERT INTO service_jobs(job_id, desired_state, published_port)
			VALUES(?, ?, ?)`, job.JobID, contract.ServiceDesiredRunning, spec.PublishedPort); err != nil {
			return Job{}, false, internalError(err, "initialize service job")
		}
		job.ServiceJob = &ServiceJob{
			DesiredState:  contract.ServiceDesiredRunning,
			PublishedPort: spec.PublishedPort,
		}
		if spec.PublishedPort != nil {
			ready := false
			job.Ready = &ready
		}
	}
	for _, tag := range spec.RoutingTags {
		if _, err := tx.ExecContext(ctx, "INSERT INTO job_tags(job_id, tag) VALUES(?, ?)", job.JobID, tag); err != nil {
			return Job{}, false, internalError(err, "store job routing tags")
		}
	}
	if err := tx.Commit(); err != nil {
		return Job{}, false, internalError(err, "commit job creation")
	}
	return job, false, nil
}

func (s *Store) readConcurrentSubmit(ctx context.Context, dispatchKey, requestHash string, insertErr error) (Job, bool, error) {
	job, storedHash, err := getJobByDispatchKey(ctx, s.db, dispatchKey, canonicalTime(s.clock.Now()))
	if err != nil {
		tombstone, tombstoneErr := readServiceTombstoneByDispatchHash(ctx, s.db, hashDispatchKey(dispatchKey))
		if tombstoneErr != nil {
			return Job{}, false, internalError(insertErr, "store job")
		}
		if tombstone.requestHash != requestHash {
			return Job{}, false, protocolError(contract.ErrorDispatchKeyConflict, "dispatch key %q was already used with a different job", dispatchKey)
		}
		return tombstone.job(), true, nil
	}
	if storedHash != requestHash {
		return Job{}, false, protocolError(contract.ErrorDispatchKeyConflict, "dispatch key %q was already used with a different job", dispatchKey)
	}
	return job, true, nil
}

func (s *Store) GetJob(ctx context.Context, jobID string) (Job, error) {
	job, err := getJobByID(ctx, s.db, jobID, canonicalTime(s.clock.Now()))
	if errors.Is(err, sql.ErrNoRows) {
		tombstone, tombstoneErr := readServiceTombstoneByID(ctx, s.db, jobID)
		if errors.Is(tombstoneErr, sql.ErrNoRows) {
			return Job{}, protocolError(contract.ErrorNotFound, "job %q was not found", jobID)
		}
		if tombstoneErr != nil {
			return Job{}, internalError(tombstoneErr, "read removed job")
		}
		return tombstone.job(), nil
	}
	if err != nil {
		return Job{}, internalError(err, "read job")
	}
	return job, nil
}

// ListJobAttempts returns the retained execution summaries in chronological
// order. Service retention may prune old empty summaries; a one-shot keeps its
// sole attempt. Authority-bearing columns never cross this operator boundary.
func (s *Store) ListJobAttempts(ctx context.Context, jobID string) ([]Attempt, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT attempt_id, node_id, state, lease_expires_ns,
		result_json, late_result_json, image_observation_json, created_ns, updated_ns
		FROM attempts WHERE job_id=? ORDER BY created_ns, attempt_id`, jobID)
	if err != nil {
		return nil, internalError(err, "list job attempts")
	}
	defer rows.Close()
	attempts := []Attempt{}
	for rows.Next() {
		var attempt Attempt
		var resultJSON, lateResultJSON, imageJSON []byte
		var leaseExpiresNS, createdNS, updatedNS int64
		if err := rows.Scan(&attempt.AttemptID, &attempt.NodeID, &attempt.State, &leaseExpiresNS,
			&resultJSON, &lateResultJSON, &imageJSON, &createdNS, &updatedNS); err != nil {
			return nil, internalError(err, "scan job attempt")
		}
		attempt.LeaseExpiresAt = time.Unix(0, leaseExpiresNS).UTC()
		attempt.CreatedAt = time.Unix(0, createdNS).UTC()
		attempt.UpdatedAt = time.Unix(0, updatedNS).UTC()
		if len(resultJSON) > 0 {
			attempt.Result = &ProcessResult{}
			if err := json.Unmarshal(resultJSON, attempt.Result); err != nil {
				return nil, internalError(err, "decode job attempt result")
			}
		}
		if len(lateResultJSON) > 0 {
			attempt.LateResult = &LateResultEvidence{}
			if err := json.Unmarshal(lateResultJSON, attempt.LateResult); err != nil {
				return nil, internalError(err, "decode job attempt late result")
			}
		}
		if len(imageJSON) > 0 {
			attempt.Image = &OCIImageEvidence{}
			if err := json.Unmarshal(imageJSON, attempt.Image); err != nil {
				return nil, internalError(err, "decode job attempt image evidence")
			}
		}
		attempts = append(attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		return nil, internalError(err, "iterate job attempts")
	}
	return attempts, nil
}

// ListNodes returns the operator-visible fleet in stable node ID order.
func (s *Store) ListNodes(ctx context.Context) ([]Node, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT node_id FROM nodes ORDER BY node_id")
	if err != nil {
		return nil, internalError(err, "list node IDs")
	}
	defer rows.Close()

	var nodeIDs []string
	for rows.Next() {
		var nodeID string
		if err := rows.Scan(&nodeID); err != nil {
			return nil, internalError(err, "scan node ID")
		}
		nodeIDs = append(nodeIDs, nodeID)
	}
	if err := rows.Err(); err != nil {
		return nil, internalError(err, "iterate node IDs")
	}

	nodes := make([]Node, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		node, err := getNode(ctx, s.db, nodeID)
		if err != nil {
			return nil, internalError(err, "read listed node")
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

// RegisterNode records a boot session and replaces its eligibility policy with
// the canonical operator-configured policy supplied by the server.
func (s *Store) RegisterNode(ctx context.Context, identity fabric.Identity, registration contract.NodeRegistration, policy NodePolicy, operatorExpected bool) (Node, error) {
	if registration.NodeID == "" || registration.BootSessionID == "" || registration.OS == "" || registration.Architecture == "" || registration.AgentVersion == "" {
		return Node{}, protocolError(contract.ErrorInvalidRequest, "node registration fields must be non-empty")
	}
	if identity.NodeID == "" {
		return Node{}, protocolError(contract.ErrorPrincipalForbidden, "authenticated Fabric identity has no node ID")
	}
	if policy.MaxOneshotSlots < 0 || policy.MaxServiceSlots < 0 {
		return Node{}, protocolError(contract.ErrorInvalidRequest, "configured node slot limits must be non-negative")
	}
	for capability := range registration.Capabilities {
		switch strings.ToLower(strings.TrimSpace(capability)) {
		case "max_oneshot_slots", "max_service_slots":
			return Node{}, protocolError(contract.ErrorInvalidRequest, "node capacity is control-plane policy, not an agent capability")
		}
	}
	tags := NormalizeTags(policy.Tags)
	for _, tag := range tags {
		if !validTag(tag) {
			return Node{}, protocolError(contract.ErrorInvalidRequest, "configured node tag %q is invalid", tag)
		}
	}
	now := canonicalTime(s.clock.Now())
	legacyObservation := registration.CapabilityRevision == 0
	if legacyObservation {
		observation := legacyCapabilityObservation(registration.Capabilities, now)
		registration.Capabilities = observation.Capabilities
		registration.CapabilityRevision = observation.Revision
		registration.CapabilityObservedAt = observation.ObservedAt
		registration.MissingCapabilities = observation.MissingCapabilities
	}
	incoming, err := canonicalCapabilityObservation(registrationCapabilityObservation(registration))
	if err != nil {
		return Node{}, err
	}
	registration.Capabilities = incoming.observation.Capabilities
	registration.CapabilityRevision = incoming.observation.Revision
	registration.CapabilityObservedAt = incoming.observation.ObservedAt
	registration.MissingCapabilities = incoming.observation.MissingCapabilities
	registration.CapabilityReasonCode = incoming.observation.ReasonCode
	if registration.SupersedeCapabilityRevision &&
		(incoming.observation.Capabilities["kind:oci"] ||
			!slices.Contains(incoming.observation.MissingCapabilities, "kind:oci") ||
			!incoming.observation.ReasonCode.ValidOCIRestriction()) {
		return Node{}, protocolError(contract.ErrorInvalidRequest, "capability supersede requires a restrictive OCI observation")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Node{}, internalError(err, "begin node registration")
	}
	defer tx.Rollback()
	var storedIdentity, storedBoot string
	var storedCapabilitiesJSON, storedMissingJSON []byte
	var storedRevision, storedObservedNS int64
	var storedReason contract.CapabilityReasonCode
	readErr := tx.QueryRowContext(ctx, `SELECT identity_node_id, boot_session_id, capabilities_json,
		capability_revision, capability_observed_ns, missing_capabilities_json, capability_reason_code
		FROM nodes WHERE node_id=?`, registration.NodeID).Scan(
		&storedIdentity, &storedBoot, &storedCapabilitiesJSON, &storedRevision, &storedObservedNS, &storedMissingJSON, &storedReason,
	)
	replaceCapabilities := true
	advanceCapabilityObservedAt := false
	switch {
	case errors.Is(readErr, sql.ErrNoRows):
		_, err = tx.ExecContext(ctx, `INSERT INTO nodes(
			node_id, identity_node_id, boot_session_id, connect_host, root_instance_id, os, architecture, agent_version,
			capabilities_json, capability_revision, capability_observed_ns, missing_capabilities_json, capability_reason_code,
			state, last_heartbeat_ns, max_oneshot_slots, max_service_slots, authority_generation, claims_enabled
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)`,
			registration.NodeID, identity.NodeID, registration.BootSessionID, registration.ConnectHost, registration.RootInstanceID,
			registration.OS, registration.Architecture, registration.AgentVersion,
			incoming.capabilitiesJSON, incoming.observation.Revision, incoming.observation.ObservedAt.UnixNano(), incoming.missingJSON, incoming.observation.ReasonCode,
			contract.NodeAlive, now.UnixNano(), policy.MaxOneshotSlots, policy.MaxServiceSlots, operatorExpected)
	case readErr != nil:
		return Node{}, internalError(readErr, "read existing node registration")
	case storedIdentity != identity.NodeID:
		return Node{}, protocolError(contract.ErrorIdentityBound, "stable node %q is bound to another Fabric identity", registration.NodeID)
	default:
		if storedBoot == registration.BootSessionID {
			if registration.SupersedeCapabilityRevision {
				incoming.observation.Revision = storedRevision + 1
				incoming, err = canonicalCapabilityObservation(incoming.observation)
				if err != nil {
					return Node{}, err
				}
				replaceCapabilities = true
			} else if legacyObservation {
				replaceCapabilities = false
			} else {
				decision, decisionErr := decideCapabilityObservation(incoming, storedCapabilitiesJSON, storedRevision, storedMissingJSON, storedReason)
				if decisionErr != nil {
					return Node{}, decisionErr
				}
				replaceCapabilities = decision == capabilityObservationReplace
				advanceCapabilityObservedAt = decision == capabilityObservationReplay && incoming.observation.ObservedAt.UnixNano() > storedObservedNS
			}
		}
		if replaceCapabilities {
			_, err = tx.ExecContext(ctx, `UPDATE nodes SET boot_session_id=?, connect_host=?, root_instance_id=?, os=?, architecture=?, agent_version=?,
				capabilities_json=?, capability_revision=?, capability_observed_ns=?, missing_capabilities_json=?, capability_reason_code=?,
				state=?, last_heartbeat_ns=?, max_oneshot_slots=?, max_service_slots=?, authority_generation=authority_generation+1
				WHERE node_id=?`, registration.BootSessionID, registration.ConnectHost, registration.RootInstanceID, registration.OS,
				registration.Architecture, registration.AgentVersion, incoming.capabilitiesJSON, incoming.observation.Revision,
				incoming.observation.ObservedAt.UnixNano(), incoming.missingJSON, incoming.observation.ReasonCode, contract.NodeAlive,
				now.UnixNano(), policy.MaxOneshotSlots, policy.MaxServiceSlots, registration.NodeID)
		} else if advanceCapabilityObservedAt {
			_, err = tx.ExecContext(ctx, `UPDATE nodes SET boot_session_id=?, connect_host=?, root_instance_id=?, os=?, architecture=?, agent_version=?,
				capability_observed_ns=?, state=?, last_heartbeat_ns=?, max_oneshot_slots=?, max_service_slots=?, authority_generation=authority_generation+1
				WHERE node_id=?`, registration.BootSessionID, registration.ConnectHost, registration.RootInstanceID, registration.OS,
				registration.Architecture, registration.AgentVersion, incoming.observation.ObservedAt.UnixNano(), contract.NodeAlive,
				now.UnixNano(), policy.MaxOneshotSlots, policy.MaxServiceSlots, registration.NodeID)
		} else {
			_, err = tx.ExecContext(ctx, `UPDATE nodes SET boot_session_id=?, connect_host=?, root_instance_id=?, os=?, architecture=?, agent_version=?,
				state=?, last_heartbeat_ns=?, max_oneshot_slots=?, max_service_slots=?, authority_generation=authority_generation+1
				WHERE node_id=?`, registration.BootSessionID, registration.ConnectHost, registration.RootInstanceID, registration.OS,
				registration.Architecture, registration.AgentVersion, contract.NodeAlive, now.UnixNano(), policy.MaxOneshotSlots,
				policy.MaxServiceSlots, registration.NodeID)
		}
	}
	if err != nil {
		return Node{}, internalError(err, "store node registration")
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM node_tags WHERE node_id = ?", registration.NodeID); err != nil {
		return Node{}, internalError(err, "replace node tags")
	}
	for _, tag := range tags {
		if _, err := tx.ExecContext(ctx, "INSERT INTO node_tags(node_id, tag) VALUES(?, ?)", registration.NodeID, tag); err != nil {
			return Node{}, internalError(err, "store node tag")
		}
	}
	node, err := getNode(ctx, tx, registration.NodeID)
	if err != nil {
		return Node{}, internalError(err, "read registered node")
	}
	if err := tx.Commit(); err != nil {
		return Node{}, internalError(err, "commit node registration")
	}
	return node, nil
}

// HeartbeatNode is retained for store tests that exercise liveness without the
// live agent protocol's required capability observation.
func (s *Store) HeartbeatNode(ctx context.Context, identityNodeID, nodeID, bootSessionID string) (Node, error) {
	return s.heartbeatNode(ctx, identityNodeID, nodeID, bootSessionID, nil, nil)
}

// HeartbeatNodeWithPolicy is retained for tests that refresh effective policy
// without going through the live agent protocol's required observation.
func (s *Store) HeartbeatNodeWithPolicy(ctx context.Context, identityNodeID, nodeID, bootSessionID string, policy NodePolicy) (Node, error) {
	return s.heartbeatNode(ctx, identityNodeID, nodeID, bootSessionID, nil, &policy)
}

// HeartbeatNodeWithCapabilityObservation atomically applies one complete
// capability observation with liveness and configured capacity refresh.
func (s *Store) HeartbeatNodeWithCapabilityObservation(ctx context.Context, identityNodeID, nodeID, bootSessionID string, observation contract.CapabilityObservation, policy NodePolicy) (Node, error) {
	return s.heartbeatNode(ctx, identityNodeID, nodeID, bootSessionID, &observation, &policy)
}

func (s *Store) heartbeatNode(ctx context.Context, identityNodeID, nodeID, bootSessionID string, observation *contract.CapabilityObservation, policy *NodePolicy) (Node, error) {
	now := canonicalTime(s.clock.Now())
	if policy != nil && (policy.MaxOneshotSlots < 0 || policy.MaxServiceSlots < 0) {
		return Node{}, protocolError(contract.ErrorInvalidRequest, "configured node slot limits must be non-negative")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Node{}, internalError(err, "begin node heartbeat")
	}
	defer tx.Rollback()
	var storedIdentity, storedBoot string
	var storedState contract.NodeState
	var storedCapabilitiesJSON, storedMissingJSON []byte
	var storedRevision, storedObservedNS int64
	var storedReason contract.CapabilityReasonCode
	if err := tx.QueryRowContext(ctx, `SELECT identity_node_id, boot_session_id, state, capabilities_json,
		capability_revision, capability_observed_ns, missing_capabilities_json, capability_reason_code
		FROM nodes WHERE node_id=?`, nodeID).Scan(&storedIdentity, &storedBoot, &storedState, &storedCapabilitiesJSON,
		&storedRevision, &storedObservedNS, &storedMissingJSON, &storedReason); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Node{}, protocolError(contract.ErrorNodeNotRegistered, "node %q is not registered", nodeID)
		}
		return Node{}, internalError(err, "read heartbeating node session")
	}
	if storedIdentity != identityNodeID || storedBoot != bootSessionID ||
		(storedState != contract.NodeAlive && storedState != contract.NodeStale && storedState != contract.NodeDraining) {
		return Node{}, s.nodeSessionError(ctx, tx, nodeID, identityNodeID, bootSessionID, "heartbeat")
	}
	var incoming storedCapabilityObservation
	replaceCapabilities := false
	advanceCapabilityObservedAt := false
	if observation != nil {
		incoming, err = canonicalCapabilityObservation(*observation)
		if err != nil {
			return Node{}, err
		}
		decision, decisionErr := decideCapabilityObservation(incoming, storedCapabilitiesJSON, storedRevision, storedMissingJSON, storedReason)
		if decisionErr != nil {
			return Node{}, decisionErr
		}
		replaceCapabilities = decision == capabilityObservationReplace
		advanceCapabilityObservedAt = decision == capabilityObservationReplay && incoming.observation.ObservedAt.UnixNano() > storedObservedNS
	}
	state := storedState
	if storedState == contract.NodeAlive || storedState == contract.NodeStale {
		state = contract.NodeAlive
	}
	if replaceCapabilities && policy != nil {
		_, err = tx.ExecContext(ctx, `UPDATE nodes SET state=?, last_heartbeat_ns=?, max_oneshot_slots=?, max_service_slots=?,
			capabilities_json=?, capability_revision=?, capability_observed_ns=?, missing_capabilities_json=?, capability_reason_code=? WHERE node_id=?`,
			state, now.UnixNano(), policy.MaxOneshotSlots, policy.MaxServiceSlots, incoming.capabilitiesJSON, incoming.observation.Revision,
			incoming.observation.ObservedAt.UnixNano(), incoming.missingJSON, incoming.observation.ReasonCode, nodeID)
	} else if replaceCapabilities {
		_, err = tx.ExecContext(ctx, `UPDATE nodes SET state=?, last_heartbeat_ns=?, capabilities_json=?, capability_revision=?,
			capability_observed_ns=?, missing_capabilities_json=?, capability_reason_code=? WHERE node_id=?`, state, now.UnixNano(),
			incoming.capabilitiesJSON, incoming.observation.Revision, incoming.observation.ObservedAt.UnixNano(), incoming.missingJSON,
			incoming.observation.ReasonCode, nodeID)
	} else if advanceCapabilityObservedAt && policy != nil {
		_, err = tx.ExecContext(ctx, `UPDATE nodes SET state=?, last_heartbeat_ns=?, max_oneshot_slots=?, max_service_slots=?,
			capability_observed_ns=? WHERE node_id=?`, state, now.UnixNano(), policy.MaxOneshotSlots, policy.MaxServiceSlots,
			incoming.observation.ObservedAt.UnixNano(), nodeID)
	} else if advanceCapabilityObservedAt {
		_, err = tx.ExecContext(ctx, `UPDATE nodes SET state=?, last_heartbeat_ns=?, capability_observed_ns=? WHERE node_id=?`,
			state, now.UnixNano(), incoming.observation.ObservedAt.UnixNano(), nodeID)
	} else if policy != nil {
		_, err = tx.ExecContext(ctx, `UPDATE nodes SET state=?, last_heartbeat_ns=?, max_oneshot_slots=?, max_service_slots=? WHERE node_id=?`,
			state, now.UnixNano(), policy.MaxOneshotSlots, policy.MaxServiceSlots, nodeID)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE nodes SET state=?, last_heartbeat_ns=? WHERE node_id=?`, state, now.UnixNano(), nodeID)
	}
	if err != nil {
		return Node{}, internalError(err, "update node heartbeat")
	}
	node, err := getNode(ctx, tx, nodeID)
	if err != nil {
		return Node{}, internalError(err, "read heartbeating node")
	}
	if err := tx.Commit(); err != nil {
		return Node{}, internalError(err, "commit node heartbeat")
	}
	return node, nil
}

// ClaimJob atomically performs class-scoped eligibility selection, capacity
// admission, fencing, attempt creation, and lease establishment. Every
// eligibility predicate is part of the UPDATE that wins the queued row.
func (s *Store) ClaimJob(ctx context.Context, identityNodeID, nodeID, bootSessionID, class string, excludedJobIDs ...string) (*Claim, error) {
	if class != contract.JobClassOneShot && class != contract.JobClassService {
		return nil, protocolError(contract.ErrorInvalidRequest, "claim class must be %q or %q", contract.JobClassOneShot, contract.JobClassService)
	}
	exclusionClause, exclusionArguments, err := claimExclusion(excludedJobIDs)
	if err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, internalError(err, "begin job claim")
	}
	defer tx.Rollback()

	var nodeState contract.NodeState
	var storedIdentity, storedBoot string
	var heartbeatNS int64
	var authorityGeneration int64
	var claimsEnabled bool
	err = tx.QueryRowContext(ctx, "SELECT identity_node_id, boot_session_id, state, last_heartbeat_ns, authority_generation, claims_enabled FROM nodes WHERE node_id=?", nodeID).
		Scan(&storedIdentity, &storedBoot, &nodeState, &heartbeatNS, &authorityGeneration, &claimsEnabled)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, protocolError(contract.ErrorNodeNotRegistered, "node %q is not registered", nodeID)
	}
	if err != nil {
		return nil, internalError(err, "read claiming node")
	}
	if storedIdentity != identityNodeID {
		return nil, protocolError(contract.ErrorIdentityBound, "stable node %q is bound to another Fabric identity", nodeID)
	}
	if storedBoot != bootSessionID {
		return nil, protocolError(contract.ErrorNodeSessionReplaced, "node %q boot session has been replaced", nodeID)
	}
	now := canonicalTime(s.clock.Now())
	nodeState, err = s.reconcileClaimingNode(ctx, tx, nodeID, nodeState, time.Unix(0, heartbeatNS).UTC(), now)
	if err != nil {
		return nil, err
	}
	if nodeState != contract.NodeAlive {
		if err := tx.Commit(); err != nil {
			return nil, internalError(err, "commit node liveness transition")
		}
		switch nodeState {
		case contract.NodeDead:
			return nil, protocolError(contract.ErrorNodeDead, "node %q is dead", nodeID)
		case contract.NodeDraining:
			return nil, protocolError(contract.ErrorNodeDraining, "node %q is draining", nodeID)
		default:
			return nil, protocolError(contract.ErrorConflict, "node %q is not alive", nodeID)
		}
	}
	if !claimsEnabled {
		return nil, protocolError(contract.ErrorNodeDraining, "node %q has claims disabled by operator intent", nodeID)
	}
	if class == contract.JobClassOneShot {
		if _, err := tx.ExecContext(ctx, `UPDATE jobs
			SET state=?, current_attempt_id=NULL, prestart_next_retry_at_ns=NULL,
				prestart_terminal_reason=?, updated_ns=?
			WHERE state=? AND prestart_budget_deadline_ns IS NOT NULL
				AND prestart_budget_deadline_ns<=?
				AND NOT EXISTS (SELECT 1 FROM service_jobs WHERE service_jobs.job_id=jobs.job_id)`,
			contract.JobFailed, "pre-start infrastructure budget expired while awaiting scheduling",
			now.UnixNano(), contract.JobQueued, now.UnixNano()); err != nil {
			return nil, internalError(err, "terminalize expired pre-start scheduling gap")
		}
	}

	leaseExpires := canonicalTime(now.Add(s.leaseDuration))
	attemptID := newID("attempt")
	var jobID string
	var specJSON []byte
	var fence int64
	var createdNS int64
	var imageResolutionJSON []byte
	var prestartDeadlineNS sql.NullInt64
	// SQLite's immediate writer transaction serializes this count-then-insert.
	// A Postgres adapter must instead lock the node row or retry serializable
	// transactions so concurrent claims cannot over-admit either class.
	claimQuery := `
UPDATE jobs
SET state=@job_claimed, current_attempt_id=@attempt_id, fence_counter=fence_counter+1, updated_ns=@now_ns
WHERE job_id=(
  SELECT j.job_id
	  FROM jobs j
	  LEFT JOIN service_jobs candidate_service ON candidate_service.job_id=j.job_id
	  LEFT JOIN computer_job_projections candidate_projection ON candidate_projection.job_id=j.job_id
	  LEFT JOIN computers candidate_computer ON candidate_computer.computer_id=candidate_projection.computer_id
		  WHERE j.state=@job_queued
		    AND (candidate_projection.job_id IS NULL OR (
		      candidate_projection.current=1
		      AND candidate_computer.current_job_id=j.job_id
		      AND candidate_computer.desired_state=@desired_running
		      AND candidate_computer.reconfiguration_phase='stable'
		      AND candidate_computer.placement_node_id=@node_id
		    ))
	    AND (
	      (
	        @class='one-shot'
	        AND candidate_service.job_id IS NULL
	        AND (j.prestart_next_retry_at_ns IS NULL OR j.prestart_next_retry_at_ns<=@now_ns)
	        AND (j.prestart_budget_deadline_ns IS NULL OR j.prestart_budget_deadline_ns>@now_ns)
	        AND (
	          SELECT COUNT(*)
	          FROM attempts active_oneshot
	          WHERE active_oneshot.node_id=@node_id
	            AND active_oneshot.state IN (@attempt_claimed, @attempt_running, @attempt_awaiting_input)
	            AND NOT EXISTS (
	              SELECT 1 FROM service_jobs active_service
	              WHERE active_service.job_id=active_oneshot.job_id
	            )
	        ) < (
	          SELECT max_oneshot_slots FROM nodes WHERE node_id=@node_id
	        )
	      )
	      OR
	      (
	        @class='service'
	        AND candidate_service.job_id IS NOT NULL
	        AND candidate_service.desired_state=@desired_running
	        AND (candidate_service.bound_node_id IS NULL OR candidate_service.bound_node_id=@node_id)
	        AND (candidate_service.next_restart_at IS NULL OR candidate_service.next_restart_at<=@now_ns)
	        AND (
	          candidate_service.bound_node_id=@node_id
	          OR (
	            SELECT COUNT(*)
	            FROM service_jobs occupied_service
	            JOIN jobs occupied_job ON occupied_job.job_id=occupied_service.job_id
	            WHERE occupied_service.bound_node_id=@node_id
	              AND (
	                (occupied_job.state=@job_queued AND occupied_service.desired_state=@desired_running)
	                OR occupied_job.state IN (@job_claimed, @job_running, @job_stopping, @job_removal_pending, @job_agent_cleaned)
	              )
	          ) < (
	            SELECT max_service_slots FROM nodes WHERE node_id=@node_id
	          )
	        )
	      )
	    )
	    AND NOT EXISTS (
	      SELECT 1 FROM attempts prior
	      WHERE prior.node_id=@node_id
	        AND prior.boot_session_id<>@boot_session_id
	        AND prior.state IN (@attempt_claimed, @attempt_running, @attempt_awaiting_input)
	    )
	    %s
	    AND NOT EXISTS (
      SELECT 1 FROM job_tags jt
      WHERE jt.job_id=j.job_id
	        AND NOT EXISTS (
	          SELECT 1 FROM node_tags nt WHERE nt.node_id=@node_id AND nt.tag=jt.tag
	        )
    )
	    AND NOT EXISTS (
	      SELECT 1 FROM job_required_capabilities required
	      WHERE required.job_id=j.job_id
	        AND NOT EXISTS (
	          SELECT 1
	          FROM json_each(CAST((SELECT capabilities_json FROM nodes WHERE node_id=@node_id) AS TEXT)) advertised
	          WHERE advertised.key=required.capability AND advertised.value=1
	        )
	    )
  ORDER BY CASE
	WHEN @class='service' AND candidate_service.bound_node_id=@node_id THEN 0
	ELSE 1
  END, j.created_ns, j.job_id
  LIMIT 1
)
AND state=@job_queued
	RETURNING job_id, spec_json, fence_counter, created_ns,
	          image_resolution_json, prestart_budget_deadline_ns`
	claimArguments := []any{
		sql.Named("job_claimed", contract.JobClaimed),
		sql.Named("attempt_id", attemptID),
		sql.Named("now_ns", now.UnixNano()),
		sql.Named("job_queued", contract.JobQueued),
		sql.Named("class", class),
		sql.Named("node_id", nodeID),
		sql.Named("boot_session_id", bootSessionID),
		sql.Named("desired_running", contract.ServiceDesiredRunning),
		sql.Named("attempt_claimed", contract.AttemptClaimed),
		sql.Named("attempt_running", contract.AttemptRunning),
		sql.Named("attempt_awaiting_input", contract.AttemptAwaitingInput),
		sql.Named("job_running", contract.JobRunning),
		sql.Named("job_stopping", contract.JobStopping),
		sql.Named("job_removal_pending", contract.JobRemovalPending),
		sql.Named("job_agent_cleaned", contract.JobAgentCleaned),
	}
	claimArguments = append(claimArguments, exclusionArguments...)
	err = tx.QueryRowContext(ctx, fmt.Sprintf(claimQuery, exclusionClause), claimArguments...).
		Scan(&jobID, &specJSON, &fence, &createdNS, &imageResolutionJSON, &prestartDeadlineNS)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return nil, internalError(err, "commit empty claim")
		}
		return nil, nil
	}
	if err != nil {
		return nil, internalError(err, "atomically claim job")
	}
	var spec contract.JobSpec
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		return nil, internalError(err, "decode claimed job")
	}
	if spec.Kind == contract.JobKindOCI && spec.Class == contract.JobClassOneShot {
		if !prestartDeadlineNS.Valid {
			prestartDeadlineNS = sql.NullInt64{Int64: now.Add(s.prestartInfrastructureBudget).UnixNano(), Valid: true}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE jobs
			SET prestart_budget_deadline_ns=COALESCE(prestart_budget_deadline_ns, ?), prestart_next_retry_at_ns=NULL
			WHERE job_id=?`, prestartDeadlineNS.Int64, jobID); err != nil {
			return nil, internalError(err, "initialize pre-start infrastructure budget")
		}
	}
	if len(imageResolutionJSON) > 0 {
		var resolution jobImageResolution
		if err := json.Unmarshal(imageResolutionJSON, &resolution); err != nil {
			return nil, internalError(err, "decode claimed job image resolution")
		}
		if spec.Kind != contract.JobKindOCI || spec.Execution.OCI == nil {
			return nil, internalError(errors.New("non-OCI job has image resolution"), "decode claimed job image resolution")
		}
		execution := *spec.Execution.OCI
		execution.Image = spec.Execution.OCI.Image
		execution.Image.Digest = &resolution.TopLevelDigest
		spec.Execution.OCI = &execution
	}
	fencingToken := strconv.FormatInt(fence, 10)
	attemptCreatedNS := now.UnixNano()
	var latestRestartNS sql.NullInt64
	if err := tx.QueryRowContext(ctx, "SELECT MAX(created_ns) FROM service_restart_requests WHERE job_id=?", jobID).Scan(&latestRestartNS); err != nil {
		return nil, internalError(err, "read latest service restart request")
	}
	// A restart request applies only to the attempt that existed when it was
	// accepted. SQLite and injected clocks may return the same nanosecond for
	// consecutive transactions, so make the fresh attempt strictly newer than
	// every already-consumed request instead of relying on wall-clock progress.
	if latestRestartNS.Valid && latestRestartNS.Int64 >= attemptCreatedNS {
		attemptCreatedNS = latestRestartNS.Int64 + 1
	}
	_, err = tx.ExecContext(ctx, `
	INSERT INTO attempts(attempt_id, job_id, node_id, boot_session_id, state, fencing_token, lease_expires_ns, authority_generation,
	                     image_observation_json, image_observation_hash, created_ns, updated_ns)
	VALUES(?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)`, attemptID, jobID, nodeID, bootSessionID, contract.AttemptClaimed,
		fencingToken, leaseExpires.UnixNano(), authorityGeneration, attemptCreatedNS, now.UnixNano())
	if err != nil {
		return nil, internalError(err, "create claimed attempt")
	}
	var computerStorage *ComputerStorageClaim
	if class == contract.JobClassService {
		bindingResult, err := tx.ExecContext(ctx, `UPDATE service_jobs
			SET bound_node_id=CASE WHEN bound_node_id IS NULL THEN ? ELSE bound_node_id END,
				next_restart_at=NULL, healthy_since_ns=NULL, published_attempt_id=NULL
			WHERE job_id=? AND (bound_node_id IS NULL OR bound_node_id=?)`, nodeID, jobID, nodeID)
		if err != nil {
			return nil, internalError(err, "begin service restart attempt")
		}
		if err := requireClaimBindingUpdate(bindingResult, "service Job", jobID, nodeID); err != nil {
			return nil, err
		}
		if computerID, mapped, mapErr := computerIDForJob(ctx, tx, jobID); mapErr != nil {
			return nil, mapErr
		} else if mapped {
			bindingResult, err := tx.ExecContext(ctx, `UPDATE computers
				SET bound_node_id=CASE WHEN bound_node_id IS NULL THEN ? ELSE bound_node_id END, updated_ns=?
				WHERE computer_id=? AND current_job_id=? AND placement_node_id=?
					AND (bound_node_id IS NULL OR bound_node_id=?)`,
				nodeID, now.UnixNano(), computerID, jobID, nodeID, nodeID)
			if err != nil {
				return nil, internalError(err, "bind Computer service projection")
			}
			if err := requireClaimBindingUpdate(bindingResult, "Computer", computerID, nodeID); err != nil {
				return nil, err
			}
			var storage ComputerStorageClaim
			if err := tx.QueryRowContext(ctx, `SELECT computer_id, storage_id, storage_generation, intent_revision
				FROM computers WHERE computer_id=? AND current_job_id=?`, computerID, jobID).
				Scan(&storage.ComputerID, &storage.StorageID, &storage.StorageGeneration, &storage.IntentRevision); err != nil {
				return nil, internalError(err, "read claimed Computer Storage identity")
			}
			computerStorage = &storage
		}
	}
	if _, err := pruneServiceAttemptSummaries(ctx, tx, jobID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, internalError(err, "commit job claim")
	}
	claim := &Claim{
		Job: Job{
			JobID: jobID, NodeID: nodeID, State: contract.JobClaimed, Spec: spec, CurrentAttemptID: attemptID,
			CreatedAt: time.Unix(0, createdNS).UTC(), UpdatedAt: now,
		},
		Lease: AttemptLease{
			AttemptID: attemptID, FencingToken: fencingToken, LeaseExpires: leaseExpires,
			LeaseTTL: leaseExpires.Sub(now),
		},
		ComputerStorage: computerStorage,
	}
	if prestartDeadlineNS.Valid {
		deadline := time.Unix(0, prestartDeadlineNS.Int64).UTC()
		claim.PrestartDeadline = &deadline
	}
	return claim, nil
}

func requireClaimBindingUpdate(result sql.Result, resource, resourceID, nodeID string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return internalError(err, "read claimed binding result")
	}
	if affected != 1 {
		return internalError(fmt.Errorf("%s %s binding or placement diverged from claiming node %s",
			resource, resourceID, nodeID), "bind claimed service")
	}
	return nil
}

func claimExclusion(jobIDs []string) (string, []any, error) {
	if len(jobIDs) == 0 {
		return "", nil, nil
	}
	seen := make(map[string]struct{}, len(jobIDs))
	placeholders := make([]string, 0, len(jobIDs))
	arguments := make([]any, 0, len(jobIDs))
	for _, jobID := range jobIDs {
		jobID = strings.TrimSpace(jobID)
		if jobID == "" {
			return "", nil, protocolError(contract.ErrorInvalidRequest, "excluded job IDs cannot be empty")
		}
		if _, exists := seen[jobID]; exists {
			continue
		}
		seen[jobID] = struct{}{}
		name := fmt.Sprintf("excluded_job_%d", len(placeholders))
		placeholders = append(placeholders, "@"+name)
		arguments = append(arguments, sql.Named(name, jobID))
	}
	return "AND j.job_id NOT IN (" + strings.Join(placeholders, ", ") + ")", arguments, nil
}

func (s *Store) RenewLease(ctx context.Context, identityNodeID, jobID, attemptID, fencingToken string) (AttemptLease, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AttemptLease{}, internalError(err, "begin lease renewal")
	}
	defer tx.Rollback()
	attempt, err := readAttemptAuthority(ctx, tx, attemptID)
	if err != nil {
		return AttemptLease{}, err
	}
	if err := validateAttemptAuthority(identityNodeID, jobID, attemptID, fencingToken, attempt); err != nil {
		return AttemptLease{}, err
	}
	now := canonicalTime(s.clock.Now())
	if attempt.state == contract.AttemptLost && !now.Before(attempt.leaseExpires) {
		return AttemptLease{}, protocolError(contract.ErrorLeaseExpired, "attempt lease has expired")
	}
	if attempt.state != contract.AttemptClaimed && attempt.state != contract.AttemptRunning && attempt.state != contract.AttemptAwaitingInput {
		return AttemptLease{}, protocolError(contract.ErrorConflict, "attempt is terminal")
	}
	if !now.Before(attempt.leaseExpires) {
		if err := expireAttempt(ctx, tx, attempt, now); err != nil {
			return AttemptLease{}, err
		}
		if err := tx.Commit(); err != nil {
			return AttemptLease{}, internalError(err, "commit lease expiry")
		}
		return AttemptLease{}, protocolError(contract.ErrorLeaseExpired, "attempt lease has expired")
	}
	directive, err := readAttemptDirective(ctx, tx, jobID, attemptID)
	if err != nil {
		return AttemptLease{}, err
	}
	expires := canonicalTime(now.Add(s.leaseDuration))
	nextState := attempt.state
	if attempt.state == contract.AttemptClaimed && attempt.spec.Kind != contract.JobKindOCI {
		nextState = contract.AttemptRunning
	}
	_, err = tx.ExecContext(ctx, "UPDATE attempts SET state=?, lease_expires_ns=?, updated_ns=? WHERE attempt_id=?", nextState, expires.UnixNano(), now.UnixNano(), attemptID)
	if err != nil {
		return AttemptLease{}, internalError(err, "renew attempt lease")
	}
	if attempt.state == contract.AttemptClaimed && attempt.spec.Kind != contract.JobKindOCI {
		if _, err := tx.ExecContext(ctx, "UPDATE jobs SET state=?, updated_ns=? WHERE job_id=? AND state=?", contract.JobRunning, now.UnixNano(), jobID, contract.JobClaimed); err != nil {
			return AttemptLease{}, internalError(err, "acknowledge renewed job execution")
		}
	}
	if attempt.spec.Kind != contract.JobKindOCI {
		if err := markPortlessServiceStable(ctx, tx, jobID, now); err != nil {
			return AttemptLease{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return AttemptLease{}, internalError(err, "commit lease renewal")
	}
	return AttemptLease{
		AttemptID: attemptID, FencingToken: fencingToken, LeaseExpires: expires,
		LeaseTTL: expires.Sub(now), Directive: directive,
	}, nil
}

// ObserveAttemptImage records the immutable image/runtime identity selected
// for an OCI attempt. Identical replay is a no-op; a different observation is
// an idempotency conflict.
func (s *Store) ObserveAttemptImage(ctx context.Context, identityNodeID, jobID, attemptID string, request ImageObservationRequest) (Job, error) {
	if err := validateImageObservation(request); err != nil {
		return Job{}, err
	}
	requestJSON, err := json.Marshal(imageObservationIdentityFromRequest(request))
	if err != nil {
		return Job{}, internalError(err, "encode image observation")
	}
	hash := sha256.Sum256(requestJSON)
	observationHash := hex.EncodeToString(hash[:])
	jobIdentityJSON, err := json.Marshal(jobImageIdentityFromRequest(request))
	if err != nil {
		return Job{}, internalError(err, "encode job image identity")
	}
	jobHash := sha256.Sum256(jobIdentityJSON)
	jobObservationHash := hex.EncodeToString(jobHash[:])
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, internalError(err, "begin image observation")
	}
	defer tx.Rollback()
	attempt, err := readAttemptAuthority(ctx, tx, attemptID)
	if err != nil {
		return Job{}, err
	}
	if err := validateAttemptEvidence(identityNodeID, jobID, attemptID, request.FencingToken, attempt); err != nil {
		return Job{}, err
	}
	if attempt.spec.Kind != contract.JobKindOCI || attempt.spec.Execution.OCI == nil {
		return Job{}, protocolError(contract.ErrorConflict, "image observation is only valid for OCI attempts")
	}
	if request.SubmittedReference != attempt.spec.Execution.OCI.Image.Reference {
		return Job{}, protocolError(contract.ErrorInvalidRequest, "submitted_reference does not match the job")
	}
	if attempt.spec.Execution.OCI.Image.Digest != nil && request.TopLevelDigest != *attempt.spec.Execution.OCI.Image.Digest {
		return Job{}, protocolError(contract.ErrorInvalidRequest, "top_level_digest does not match the pinned job digest")
	}
	if attempt.imageObservationHash.Valid {
		if attempt.imageObservationHash.String != observationHash {
			return Job{}, protocolError(contract.ErrorIdempotencyConflict, "image observation conflicts with the accepted identity")
		}
		return getJobByID(ctx, tx, jobID, canonicalTime(s.clock.Now()))
	}
	if err := validateAttemptAuthority(identityNodeID, jobID, attemptID, request.FencingToken, attempt); err != nil {
		return Job{}, err
	}
	now := canonicalTime(s.clock.Now())
	if !now.Before(attempt.leaseExpires) {
		if err := expireAttempt(ctx, tx, attempt, now); err != nil {
			return Job{}, err
		}
		if err := tx.Commit(); err != nil {
			return Job{}, internalError(err, "commit image observation lease expiry")
		}
		return Job{}, protocolError(contract.ErrorLeaseExpired, "attempt lease has expired")
	}
	if attempt.state != contract.AttemptClaimed {
		return Job{}, protocolError(contract.ErrorConflict, "image observation must precede Started")
	}
	if attempt.jobImageResolutionHash.Valid && attempt.jobImageResolutionHash.String != jobObservationHash {
		return Job{}, protocolError(contract.ErrorIdempotencyConflict, "image observation conflicts with the job's resolved image identity")
	}
	resolvedAt := now
	if attempt.jobImageResolutionHash.Valid {
		var resolutionJSON []byte
		if err := tx.QueryRowContext(ctx, `SELECT image_resolution_json FROM jobs WHERE job_id=?`, jobID).Scan(&resolutionJSON); err != nil {
			return Job{}, internalError(err, "read job image resolution")
		}
		var resolution jobImageResolution
		if err := json.Unmarshal(resolutionJSON, &resolution); err != nil {
			return Job{}, internalError(err, "decode job image resolution")
		}
		resolvedAt = resolution.ResolvedAt
	}
	evidence := OCIImageEvidence{
		SubmittedReference: request.SubmittedReference, TopLevelDigest: request.TopLevelDigest,
		TopLevelMediaType: request.TopLevelMediaType, IndexDigest: request.IndexDigest,
		PlatformManifestDigest: request.PlatformManifestDigest, Platform: request.Platform,
		RuntimeHandler: request.RuntimeHandler, Snapshotter: request.Snapshotter, ResolvedAt: resolvedAt,
	}
	evidenceJSON, err := json.Marshal(evidence)
	if err != nil {
		return Job{}, internalError(err, "encode accepted image observation")
	}
	if !attempt.jobImageResolutionHash.Valid {
		resolutionJSON, err := json.Marshal(jobImageResolution{
			TopLevelDigest: request.TopLevelDigest, TopLevelMediaType: request.TopLevelMediaType,
			IndexDigest: request.IndexDigest, ResolvedAt: resolvedAt,
		})
		if err != nil {
			return Job{}, internalError(err, "encode job image resolution")
		}
		result, err := tx.ExecContext(ctx, `UPDATE jobs SET image_resolution_json=?, image_resolution_hash=?
			WHERE job_id=? AND image_resolution_hash IS NULL`, resolutionJSON, jobObservationHash, jobID)
		if err != nil {
			return Job{}, internalError(err, "store job image resolution")
		}
		if rows, err := result.RowsAffected(); err != nil || rows != 1 {
			if err != nil {
				return Job{}, internalError(err, "confirm job image resolution")
			}
			return Job{}, protocolError(contract.ErrorIdempotencyConflict, "job image resolution was already accepted")
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE attempts SET image_observation_json=?, image_observation_hash=?, updated_ns=? WHERE attempt_id=?`,
		evidenceJSON, observationHash, now.UnixNano(), attemptID); err != nil {
		return Job{}, internalError(err, "store image observation")
	}
	job, err := getJobByID(ctx, tx, jobID, now)
	if err != nil {
		return Job{}, internalError(err, "read image-observed job")
	}
	if err := tx.Commit(); err != nil {
		return Job{}, internalError(err, "commit image observation")
	}
	return job, nil
}

type imageObservationIdentity struct {
	SubmittedReference     string      `json:"submitted_reference"`
	TopLevelDigest         string      `json:"top_level_digest"`
	TopLevelMediaType      string      `json:"top_level_media_type"`
	IndexDigest            *string     `json:"index_digest,omitempty"`
	PlatformManifestDigest string      `json:"platform_manifest_digest"`
	Platform               OCIPlatform `json:"platform"`
	RuntimeHandler         string      `json:"runtime_handler"`
	Snapshotter            string      `json:"snapshotter"`
}

type jobImageIdentity struct {
	TopLevelDigest    string  `json:"top_level_digest"`
	TopLevelMediaType string  `json:"top_level_media_type"`
	IndexDigest       *string `json:"index_digest,omitempty"`
}

type jobImageResolution struct {
	TopLevelDigest    string    `json:"top_level_digest"`
	TopLevelMediaType string    `json:"top_level_media_type"`
	IndexDigest       *string   `json:"index_digest,omitempty"`
	ResolvedAt        time.Time `json:"resolved_at"`
}

func jobImageIdentityFromRequest(request ImageObservationRequest) jobImageIdentity {
	return jobImageIdentity{
		TopLevelDigest: request.TopLevelDigest, TopLevelMediaType: request.TopLevelMediaType,
		IndexDigest: request.IndexDigest,
	}
}

func imageObservationIdentityFromRequest(request ImageObservationRequest) imageObservationIdentity {
	return imageObservationIdentity{
		SubmittedReference: request.SubmittedReference, TopLevelDigest: request.TopLevelDigest,
		TopLevelMediaType: request.TopLevelMediaType, IndexDigest: request.IndexDigest,
		PlatformManifestDigest: request.PlatformManifestDigest, Platform: request.Platform,
		RuntimeHandler: request.RuntimeHandler, Snapshotter: request.Snapshotter,
	}
}

// StartAttempt is the sole claimed-to-running acknowledgement for OCI.
func (s *Store) StartAttempt(ctx context.Context, identityNodeID, jobID, attemptID string, request StartedRequest) (Job, error) {
	if request.FencingToken == "" {
		return Job{}, protocolError(contract.ErrorInvalidRequest, "fencing_token is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, internalError(err, "begin Started acknowledgement")
	}
	defer tx.Rollback()
	attempt, err := readAttemptAuthority(ctx, tx, attemptID)
	if err != nil {
		return Job{}, err
	}
	if err := validateAttemptAuthority(identityNodeID, jobID, attemptID, request.FencingToken, attempt); err != nil {
		return Job{}, err
	}
	if attempt.spec.Kind != contract.JobKindOCI {
		return Job{}, protocolError(contract.ErrorConflict, "Started is only valid for OCI attempts")
	}
	now := canonicalTime(s.clock.Now())
	if !now.Before(attempt.leaseExpires) {
		if err := expireAttempt(ctx, tx, attempt, now); err != nil {
			return Job{}, err
		}
		if err := tx.Commit(); err != nil {
			return Job{}, internalError(err, "commit Started lease expiry")
		}
		return Job{}, protocolError(contract.ErrorLeaseExpired, "attempt lease has expired")
	}
	if attempt.startedNS.Valid {
		return getJobByID(ctx, tx, jobID, now)
	}
	if attempt.state != contract.AttemptClaimed {
		return Job{}, protocolError(contract.ErrorConflict, "attempt cannot accept Started")
	}
	if len(attempt.imageObservationJSON) == 0 {
		return Job{}, protocolError(contract.ErrorConflict, "OCI attempt has no fenced image observation")
	}
	var image OCIImageEvidence
	if err := json.Unmarshal(attempt.imageObservationJSON, &image); err != nil {
		return Job{}, internalError(err, "decode image observation for Started")
	}
	startedAt := now
	if !startedAt.After(image.ResolvedAt) {
		startedAt = image.ResolvedAt.Add(time.Nanosecond)
	}
	image.StartedAt = &startedAt
	imageJSON, err := json.Marshal(image)
	if err != nil {
		return Job{}, internalError(err, "encode Started image evidence")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE attempts SET state=?, started_ns=?, image_observation_json=?, updated_ns=? WHERE attempt_id=?`,
		contract.AttemptRunning, startedAt.UnixNano(), imageJSON, now.UnixNano(), attemptID); err != nil {
		return Job{}, internalError(err, "acknowledge Started attempt")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET state=?, updated_ns=? WHERE job_id=? AND state=?`,
		contract.JobRunning, now.UnixNano(), jobID, contract.JobClaimed); err != nil {
		return Job{}, internalError(err, "acknowledge Started job")
	}
	if err := markPortlessServiceStable(ctx, tx, jobID, now); err != nil {
		return Job{}, err
	}
	job, err := getJobByID(ctx, tx, jobID, now)
	if err != nil {
		return Job{}, internalError(err, "read Started job")
	}
	if err := tx.Commit(); err != nil {
		return Job{}, internalError(err, "commit Started acknowledgement")
	}
	return job, nil
}

func validateImageObservation(request ImageObservationRequest) error {
	if request.FencingToken == "" || strings.TrimSpace(request.SubmittedReference) == "" ||
		strings.TrimSpace(request.TopLevelMediaType) == "" || strings.TrimSpace(request.RuntimeHandler) == "" ||
		strings.TrimSpace(request.Snapshotter) == "" || strings.TrimSpace(request.Platform.OS) == "" ||
		strings.TrimSpace(request.Platform.Architecture) == "" {
		return protocolError(contract.ErrorInvalidRequest, "image observation fields must be non-empty")
	}
	for name, digest := range map[string]string{
		"top_level_digest": request.TopLevelDigest, "platform_manifest_digest": request.PlatformManifestDigest,
	} {
		if !validOCIDigest(digest) {
			return protocolError(contract.ErrorInvalidRequest, "%s must be a lowercase OCI SHA-256 digest", name)
		}
	}
	if request.IndexDigest != nil && !validOCIDigest(*request.IndexDigest) {
		return protocolError(contract.ErrorInvalidRequest, "index_digest must be a lowercase OCI SHA-256 digest")
	}
	return nil
}

func validOCIDigest(value string) bool {
	return strings.HasPrefix(value, "sha256:") && validSHA256(strings.TrimPrefix(value, "sha256:"))
}

// SetAttemptPublication applies the absolute publication state of one
// authoritative portful service attempt. Unlike evidence writes, publication
// is never replayable after authority or attempt lifecycle ends.
func (s *Store) SetAttemptPublication(
	ctx context.Context,
	identityNodeID, jobID, attemptID string,
	request PublicationRequest,
) (Job, error) {
	if request.FencingToken == "" || request.Ready == nil {
		return Job{}, protocolError(contract.ErrorInvalidRequest, "fencing_token and ready are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, internalError(err, "begin publication mutation")
	}
	defer tx.Rollback()

	attempt, err := readAttemptAuthority(ctx, tx, attemptID)
	if err != nil {
		return Job{}, err
	}
	if err := validateAttemptAuthority(identityNodeID, jobID, attemptID, request.FencingToken, attempt); err != nil {
		return Job{}, err
	}
	if attempt.state != contract.AttemptClaimed && attempt.state != contract.AttemptRunning && attempt.state != contract.AttemptAwaitingInput {
		return Job{}, protocolError(contract.ErrorConflict, "attempt is terminal")
	}

	now := canonicalTime(s.clock.Now())
	if !now.Before(attempt.leaseExpires) {
		if err := expireAttempt(ctx, tx, attempt, now); err != nil {
			return Job{}, err
		}
		if err := tx.Commit(); err != nil {
			return Job{}, internalError(err, "commit publication lease expiry")
		}
		return Job{}, protocolError(contract.ErrorLeaseExpired, "attempt lease has expired")
	}

	var publishedPort sql.NullInt64
	err = tx.QueryRowContext(ctx, "SELECT published_port FROM service_jobs WHERE job_id=?", jobID).Scan(&publishedPort)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, protocolError(contract.ErrorConflict, "publication is not applicable to this job")
	}
	if err != nil {
		return Job{}, internalError(err, "read service publication applicability")
	}
	if !publishedPort.Valid {
		return Job{}, protocolError(contract.ErrorConflict, "publication is not applicable to this job")
	}

	var targetAttempt any
	if *request.Ready {
		targetAttempt = attemptID
	}
	result, err := tx.ExecContext(ctx, `UPDATE service_jobs
		SET published_attempt_id=?,
			healthy_since_ns=CASE WHEN ? THEN COALESCE(healthy_since_ns, ?) ELSE NULL END
		WHERE job_id=? AND published_attempt_id IS NOT ?`,
		targetAttempt, *request.Ready, now.UnixNano(), jobID, targetAttempt)
	if err != nil {
		return Job{}, internalError(err, "write service publication")
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return Job{}, internalError(err, "read service publication result")
	}
	if changed > 0 {
		if _, err := tx.ExecContext(ctx, "UPDATE jobs SET updated_ns=? WHERE job_id=?", now.UnixNano(), jobID); err != nil {
			return Job{}, internalError(err, "timestamp service publication")
		}
	}

	job, err := getJobByID(ctx, tx, jobID, now)
	if err != nil {
		return Job{}, internalError(err, "read published job")
	}
	if err := tx.Commit(); err != nil {
		return Job{}, internalError(err, "commit publication mutation")
	}
	return job, nil
}

func readAttemptDirective(ctx context.Context, tx *sql.Tx, jobID, attemptID string) (AttemptDirective, error) {
	var desiredState string
	var restartRequested bool
	err := tx.QueryRowContext(ctx, `
SELECT service_jobs.desired_state, EXISTS (
  SELECT 1
  FROM service_restart_requests
  JOIN attempts ON attempts.attempt_id=?
  WHERE service_restart_requests.job_id=service_jobs.job_id
    AND service_restart_requests.created_ns >= attempts.created_ns
)
FROM service_jobs
WHERE service_jobs.job_id=?`, attemptID, jobID).Scan(&desiredState, &restartRequested)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", internalError(err, "read attempt directive")
	}
	if desiredState == "stopped" {
		return AttemptDirectiveStop, nil
	}
	if desiredState == "running" && restartRequested {
		return AttemptDirectiveRestart, nil
	}
	return "", nil
}

// AppendLogs accepts a provenance-authenticated batch and owns idempotency for
// log-event keys.
// Identical event replays are acknowledged without changing the authoritative
// event rows. A conflicting replay is rejected.
func (s *Store) AppendLogs(ctx context.Context, identityNodeID, jobID, attemptID string, request AppendLogsRequest) (AppendLogsResponse, error) {
	if request.FencingToken == "" {
		return AppendLogsResponse{}, protocolError(contract.ErrorInvalidRequest, "fencing_token is required")
	}
	if len(request.Events) == 0 || len(request.Events) > MaxLogBatchEvents {
		return AppendLogsResponse{}, protocolError(contract.ErrorInvalidRequest, "events must contain between 1 and %d entries", MaxLogBatchEvents)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AppendLogsResponse{}, internalError(err, "begin log append")
	}
	defer tx.Rollback()
	attempt, err := readAttemptAuthority(ctx, tx, attemptID)
	if err != nil {
		return AppendLogsResponse{}, err
	}
	var removalExists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM service_removals WHERE job_id=?)`, jobID).Scan(&removalExists); err != nil {
		return AppendLogsResponse{}, internalError(err, "read service removal before log append")
	}
	if removalExists {
		return AppendLogsResponse{}, protocolError(contract.ErrorConflict, "service removal has revoked log authority")
	}
	if err := validateAttemptEvidence(identityNodeID, jobID, attemptID, request.FencingToken, attempt); err != nil {
		return AppendLogsResponse{}, err
	}
	hasAuthority := validateAttemptAuthority(identityNodeID, jobID, attemptID, request.FencingToken, attempt) == nil
	now := canonicalTime(s.clock.Now())
	if !now.Before(attempt.leaseExpires) {
		if err := expireAttempt(ctx, tx, attempt, now); err != nil {
			return AppendLogsResponse{}, err
		}
		if attempt.state == contract.AttemptClaimed || attempt.state == contract.AttemptRunning || attempt.state == contract.AttemptAwaitingInput {
			attempt.state = contract.AttemptLost
			attempt.updatedAt = now
		}
	}
	lateWindowExpired := attempt.state == contract.AttemptLost && now.After(attempt.updatedAt.Add(s.lateEvidenceWindow))

	type eventKey struct {
		stream   contract.LogStream
		sequence uint64
	}
	type preparedEvent struct {
		event       contract.LogEvent
		endSequence uint64
		raw         []byte
	}
	seen := make(map[eventKey][]byte, len(request.Events))
	streams := make(map[contract.LogStream]struct{}, 2)
	next := make(map[contract.LogStream]int64, 2)
	acknowledged := make(map[contract.LogStream]uint64, 2)
	newEvents := make([]preparedEvent, 0, len(request.Events))

	for _, input := range request.Events {
		event := input
		if err := validateLogEvent(attemptID, event); err != nil {
			return AppendLogsResponse{}, err
		}
		event.Timestamp = canonicalTime(event.Timestamp)
		originalRaw, err := json.Marshal(event)
		if err != nil {
			return AppendLogsResponse{}, internalError(err, "encode source log event")
		}
		if lateWindowExpired {
			event = lateEvidenceGap(event, originalRaw)
		}
		raw, err := json.Marshal(event)
		if err != nil {
			return AppendLogsResponse{}, internalError(err, "encode log event")
		}
		key := eventKey{stream: event.Stream, sequence: event.Sequence}
		streams[event.Stream] = struct{}{}
		if prior, ok := seen[key]; ok {
			if !bytes.Equal(prior, raw) {
				return AppendLogsResponse{}, protocolError(contract.ErrorIdempotencyConflict, "log event (%s, %d) conflicts within the batch", event.Stream, event.Sequence)
			}
			continue
		}
		seen[key] = raw

		var stored []byte
		err = tx.QueryRowContext(ctx, "SELECT event_json FROM log_events WHERE attempt_id=? AND stream=? AND sequence=?", attemptID, event.Stream, event.Sequence).Scan(&stored)
		switch {
		case err == nil:
			if !bytes.Equal(stored, originalRaw) && !bytes.Equal(stored, raw) {
				return AppendLogsResponse{}, protocolError(contract.ErrorIdempotencyConflict, "log event (%s, %d) conflicts with the accepted event", event.Stream, event.Sequence)
			}
			acknowledged[event.Stream] = maxSequence(acknowledged[event.Stream], event.Sequence)
			continue
		case !errors.Is(err, sql.ErrNoRows):
			return AppendLogsResponse{}, internalError(err, "read accepted log event")
		}

		expected, ok := next[event.Stream]
		if !ok {
			var maximum int64
			if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(sequence_end), -1) FROM log_events WHERE attempt_id=? AND stream=?", attemptID, event.Stream).Scan(&maximum); err != nil {
				return AppendLogsResponse{}, internalError(err, "read log sequence acknowledgement")
			}
			expected = maximum + 1
		}
		if int64(event.Sequence) != expected {
			return AppendLogsResponse{}, protocolError(contract.ErrorConflict, "log stream %s expected sequence %d, got %d", event.Stream, expected, event.Sequence)
		}
		endSequence := logEventEndSequence(event)
		next[event.Stream] = int64(endSequence) + 1
		acknowledged[event.Stream] = endSequence
		newEvents = append(newEvents, preparedEvent{event: event, endSequence: endSequence, raw: raw})
	}

	// Already-accepted events remain replayable after authority loss.
	if len(newEvents) == 0 {
		if err := readLogAcknowledgements(ctx, tx, attemptID, streams, acknowledged); err != nil {
			return AppendLogsResponse{}, err
		}
		if err := tx.Commit(); err != nil {
			return AppendLogsResponse{}, internalError(err, "commit log replay")
		}
		return AppendLogsResponse{Acknowledged: acknowledged}, nil
	}
	if attempt.state == contract.AttemptLost {
	} else if attempt.state != contract.AttemptClaimed && attempt.state != contract.AttemptRunning && attempt.state != contract.AttemptAwaitingInput {
		return AppendLogsResponse{}, protocolError(contract.ErrorConflict, "attempt is terminal")
	}

	for _, prepared := range newEvents {
		event := prepared.event
		storedBytes := event.Bytes
		if event.Gap != nil {
			storedBytes = []byte{}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO log_events(job_id, attempt_id, stream, sequence, sequence_end, timestamp_ns, bytes, event_json)
VALUES(?, ?, ?, ?, ?, ?, ?, ?)`, jobID, attemptID, event.Stream, event.Sequence, prepared.endSequence, event.Timestamp.UnixNano(), storedBytes, prepared.raw); err != nil {
			return AppendLogsResponse{}, internalError(err, "store log event")
		}
	}
	if attempt.state == contract.AttemptClaimed && hasAuthority && attempt.spec.Kind != contract.JobKindOCI {
		if _, err := tx.ExecContext(ctx, "UPDATE attempts SET state=?, updated_ns=? WHERE attempt_id=?", contract.AttemptRunning, now.UnixNano(), attemptID); err != nil {
			return AppendLogsResponse{}, internalError(err, "mark logging attempt running")
		}
		if _, err := tx.ExecContext(ctx, "UPDATE jobs SET state=?, updated_ns=? WHERE job_id=?", contract.JobRunning, now.UnixNano(), jobID); err != nil {
			return AppendLogsResponse{}, internalError(err, "mark logging job running")
		}
	}
	if attempt.spec.Kind != contract.JobKindOCI {
		if err := markPortlessServiceStable(ctx, tx, jobID, now); err != nil {
			return AppendLogsResponse{}, err
		}
	}
	if _, err := s.enforceServiceLogByteRetention(ctx, tx, jobID, now); err != nil {
		return AppendLogsResponse{}, err
	}
	if _, err := pruneServiceAttemptSummaries(ctx, tx, jobID); err != nil {
		return AppendLogsResponse{}, err
	}
	if err := readLogAcknowledgements(ctx, tx, attemptID, streams, acknowledged); err != nil {
		return AppendLogsResponse{}, err
	}
	if err := tx.Commit(); err != nil {
		return AppendLogsResponse{}, internalError(err, "commit log append")
	}
	return AppendLogsResponse{Acknowledged: acknowledged}, nil
}

func readLogAcknowledgements(ctx context.Context, q queryer, attemptID string, streams map[contract.LogStream]struct{}, acknowledgements map[contract.LogStream]uint64) error {
	for stream := range streams {
		var maximum int64
		if err := q.QueryRowContext(ctx, "SELECT MAX(sequence_end) FROM log_events WHERE attempt_id=? AND stream=?", attemptID, stream).Scan(&maximum); err != nil {
			return internalError(err, "read log acknowledgement")
		}
		acknowledgements[stream] = uint64(maximum)
	}
	return nil
}

// GetJobLogs returns one polling page after an opaque reader cursor. The
// internal insertion ordinal is never exposed directly.
func (s *Store) GetJobLogs(ctx context.Context, jobID, cursor string, limit int) (LogPage, error) {
	if limit < 1 || limit > MaxLogPageLimit {
		return LogPage{}, protocolError(contract.ErrorInvalidRequest, "limit must be between 1 and %d", MaxLogPageLimit)
	}
	if _, err := s.GetJob(ctx, jobID); err != nil {
		return LogPage{}, err
	}
	after, err := decodeLogCursor(cursor)
	if err != nil {
		return LogPage{}, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT ordinal, event_json FROM log_events
WHERE job_id=? AND ordinal>? ORDER BY ordinal LIMIT ?`, jobID, after, limit)
	if err != nil {
		return LogPage{}, internalError(err, "read job logs")
	}
	defer rows.Close()
	page := LogPage{Events: []contract.LogEvent{}}
	last := after
	for rows.Next() {
		var ordinal int64
		var raw []byte
		if err := rows.Scan(&ordinal, &raw); err != nil {
			return LogPage{}, internalError(err, "scan job log")
		}
		var event contract.LogEvent
		if err := json.Unmarshal(raw, &event); err != nil {
			return LogPage{}, internalError(err, "decode authoritative log event")
		}
		page.Events = append(page.Events, event)
		last = ordinal
	}
	if err := rows.Err(); err != nil {
		return LogPage{}, internalError(err, "iterate job logs")
	}
	page.NextCursor = encodeLogCursor(last)
	page.Truncation, err = readServiceLogTruncation(ctx, s.db, jobID)
	if err != nil {
		return LogPage{}, err
	}
	return page, nil
}

// RawJobLogJSONL derives JSONL from the authoritative event rows. There is no
// independently retained or append-rewritten raw-log blob.
func (s *Store) RawJobLogJSONL(ctx context.Context, jobID string) ([]byte, error) {
	if _, err := s.GetJob(ctx, jobID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, "SELECT event_json FROM log_events WHERE job_id=? ORDER BY ordinal", jobID)
	if err != nil {
		return nil, internalError(err, "read authoritative job log events for JSONL export")
	}
	defer rows.Close()
	var raw bytes.Buffer
	for rows.Next() {
		var eventJSON []byte
		if err := rows.Scan(&eventJSON); err != nil {
			return nil, internalError(err, "scan authoritative job log event for JSONL export")
		}
		raw.Write(eventJSON)
		raw.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		return nil, internalError(err, "iterate authoritative job log events for JSONL export")
	}
	return raw.Bytes(), nil
}

func validateLogEvent(attemptID string, event contract.LogEvent) error {
	if event.AttemptID != attemptID {
		return protocolError(contract.ErrorAttemptMismatch, "log event attempt_id does not match the request path")
	}
	if event.Stream != contract.LogStdout && event.Stream != contract.LogStderr {
		return protocolError(contract.ErrorInvalidRequest, "log stream %q is invalid", event.Stream)
	}
	if event.Sequence > math.MaxInt64 || (event.Gap != nil && event.Gap.ThroughSequence > math.MaxInt64) {
		return protocolError(contract.ErrorInvalidRequest, "log sequence exceeds the supported range")
	}
	if event.Timestamp.IsZero() {
		return protocolError(contract.ErrorInvalidRequest, "log timestamp is required")
	}
	if event.Gap == nil {
		if len(event.Bytes) == 0 || len(event.Bytes) > MaxLogEventBytes {
			return protocolError(contract.ErrorInvalidRequest, "log event bytes must contain between 1 and %d bytes", MaxLogEventBytes)
		}
		return nil
	}
	if len(event.Bytes) != 0 {
		return protocolError(contract.ErrorInvalidRequest, "log event must contain exactly one of bytes or gap")
	}
	gap := event.Gap
	if gap.ThroughSequence < event.Sequence || gap.LostEventCount != gap.ThroughSequence-event.Sequence+1 || gap.LostByteCount == 0 {
		return protocolError(contract.ErrorInvalidRequest, "log gap range, event count, and byte count are inconsistent")
	}
	switch gap.Reason {
	case contract.LogGapSpoolEviction, contract.LogGapOversizedEvent, contract.LogGapReplayRejected, contract.LogGapLateEvidenceWindowExpired, contract.LogGapLoggerSourceIncomplete:
	default:
		return protocolError(contract.ErrorInvalidRequest, "log gap reason %q is invalid", gap.Reason)
	}
	if gap.SourceEventSHA256 != "" && !validSHA256(gap.SourceEventSHA256) {
		return protocolError(contract.ErrorInvalidRequest, "log gap source_event_sha256 is invalid")
	}
	if gap.Reason == contract.LogGapLateEvidenceWindowExpired && gap.SourceEventSHA256 == "" {
		return protocolError(contract.ErrorInvalidRequest, "late-evidence log gap requires source_event_sha256")
	}
	return nil
}

func logEventEndSequence(event contract.LogEvent) uint64 {
	if event.Gap != nil {
		return event.Gap.ThroughSequence
	}
	return event.Sequence
}

func lateEvidenceGap(event contract.LogEvent, source []byte) contract.LogEvent {
	lostEventCount := uint64(1)
	lostByteCount := uint64(len(event.Bytes))
	throughSequence := event.Sequence
	if event.Gap != nil {
		lostEventCount = event.Gap.LostEventCount
		lostByteCount = event.Gap.LostByteCount
		throughSequence = event.Gap.ThroughSequence
	}
	hash := sha256.Sum256(source)
	event.Bytes = nil
	event.Gap = &contract.LogGap{
		ThroughSequence:   throughSequence,
		LostEventCount:    lostEventCount,
		LostByteCount:     lostByteCount,
		Reason:            contract.LogGapLateEvidenceWindowExpired,
		SourceEventSHA256: hex.EncodeToString(hash[:]),
	}
	return event
}

func maxSequence(left, right uint64) uint64 {
	if right > left {
		return right
	}
	return left
}

func encodeLogCursor(after int64) string {
	payload := make([]byte, 9)
	payload[0] = 1
	binary.BigEndian.PutUint64(payload[1:], uint64(after))
	return base64.RawURLEncoding.EncodeToString(payload)
}

func decodeLogCursor(cursor string) (int64, error) {
	if cursor == "" {
		return 0, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil || len(payload) != 9 || payload[0] != 1 {
		return 0, protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	after := binary.BigEndian.Uint64(payload[1:])
	if after > math.MaxInt64 {
		return 0, protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	return int64(after), nil
}

func (s *Store) CompleteAttempt(ctx context.Context, identityNodeID, jobID, attemptID string, request CompletionRequest) (Job, error) {
	if request.FencingToken == "" || request.IdempotencyKey == "" {
		return Job{}, protocolError(contract.ErrorInvalidRequest, "fencing_token and idempotency_key are required")
	}
	if err := validateProcessResult(request.Result); err != nil {
		return Job{}, err
	}
	if request.RuntimeQuiescenceEvidence != "" && !validRuntimeQuiescenceEvidence(request.RuntimeQuiescenceEvidence) {
		return Job{}, protocolError(contract.ErrorInvalidRequest, "runtime_quiescence_evidence is not recognized")
	}
	requestJSON, err := json.Marshal(request)
	if err != nil {
		return Job{}, internalError(err, "encode completion")
	}
	hash := sha256.Sum256(requestJSON)
	completionHash := hex.EncodeToString(hash[:])
	resultJSON, err := json.Marshal(request.Result)
	if err != nil {
		return Job{}, internalError(err, "encode process result")
	}
	lastFailureJSON := resultJSON
	if request.Result.SpawnError != nil {
		lastFailureJSON, err = json.Marshal(request.Result.SpawnError)
		if err != nil {
			return Job{}, internalError(err, "encode service spawn failure")
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, internalError(err, "begin completion")
	}
	defer tx.Rollback()
	attempt, err := readAttemptAuthority(ctx, tx, attemptID)
	if err != nil {
		return Job{}, err
	}
	var removalExists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM service_removals WHERE job_id=?)`, jobID).Scan(&removalExists); err != nil {
		return Job{}, internalError(err, "read service removal before completion")
	}
	if removalExists {
		return Job{}, protocolError(contract.ErrorConflict, "service removal has revoked completion authority")
	}
	if err := validateAttemptEvidence(identityNodeID, jobID, attemptID, request.FencingToken, attempt); err != nil {
		return Job{}, err
	}
	if err := validateOCICompletionPhase(attempt, request.Result); err != nil {
		return Job{}, err
	}
	now := canonicalTime(s.clock.Now())
	if !now.Before(attempt.leaseExpires) && attempt.state != contract.AttemptLost {
		if err := expireAttempt(ctx, tx, attempt, now); err != nil {
			return Job{}, err
		}
		if attempt.state == contract.AttemptClaimed || attempt.state == contract.AttemptRunning || attempt.state == contract.AttemptAwaitingInput {
			attempt.state = contract.AttemptLost
			attempt.updatedAt = now
		}
	}
	if attempt.completionKey.Valid {
		if attempt.completionKey.String != request.IdempotencyKey || attempt.completionHash.String != completionHash {
			return Job{}, protocolError(contract.ErrorIdempotencyConflict, "completion idempotency key or body conflicts with the accepted completion")
		}
		if attempt.state == contract.AttemptLost {
			return Job{}, protocolError(contract.ErrorLeaseExpired, "attempt lease has expired")
		}
		if err := validateCompletionReplayAuthority(attemptID, attempt); err != nil {
			return Job{}, err
		}
		job, err := getJobByID(ctx, tx, jobID, now)
		if err != nil {
			return Job{}, internalError(err, "read completed job replay")
		}
		return job, nil
	}
	if attempt.state == contract.AttemptLost {
		lateEvidence := LateResultEvidence{
			Kind: LateResultObservation, Result: &request.Result, Late: true,
			ObservedAt: now, AuthorityLostAt: attempt.updatedAt,
		}
		if now.After(attempt.updatedAt.Add(s.lateEvidenceWindow)) {
			lateEvidence.Kind = LateResultGapKind
			lateEvidence.Result = nil
			lateEvidence.Gap = &LateResultGap{Reason: LateResultGapObservationWindowExpired}
		}
		lateResultJSON, err := json.Marshal(lateEvidence)
		if err != nil {
			return Job{}, internalError(err, "encode late result evidence")
		}
		_, err = tx.ExecContext(ctx, `UPDATE attempts SET completion_key=?, completion_hash=?, late_result_json=?,
			late_result_observed_ns=?, late_result_authority_lost_ns=?, late_result_is_late=1 WHERE attempt_id=?`,
			request.IdempotencyKey, completionHash, lateResultJSON, now.UnixNano(), attempt.updatedAt.UnixNano(), attemptID)
		if err != nil {
			return Job{}, internalError(err, "record late completion evidence")
		}
		if err := tx.Commit(); err != nil {
			return Job{}, internalError(err, "commit late completion evidence")
		}
		return Job{}, protocolError(contract.ErrorLeaseExpired, "attempt lease has expired")
	}
	if err := validateAttemptAuthority(identityNodeID, jobID, attemptID, request.FencingToken, attempt); err != nil {
		return Job{}, err
	}
	if !now.Before(attempt.leaseExpires) {
		if err := expireAttempt(ctx, tx, attempt, now); err != nil {
			return Job{}, err
		}
		if err := tx.Commit(); err != nil {
			return Job{}, internalError(err, "commit completion lease expiry")
		}
		return Job{}, protocolError(contract.ErrorLeaseExpired, "attempt lease has expired")
	}
	if attempt.state != contract.AttemptClaimed && attempt.state != contract.AttemptRunning && attempt.state != contract.AttemptAwaitingInput {
		return Job{}, protocolError(contract.ErrorConflict, "attempt is terminal")
	}

	jobBeforeCompletion, err := getJobByID(ctx, tx, jobID, now)
	if err != nil {
		return Job{}, internalError(err, "read completing job policy")
	}
	finalJobState, finalAttemptState := completionStates(request.Result)
	var servicePolicy *serviceCompletionPolicy
	if jobBeforeCompletion.ServiceJob != nil {
		policy := s.classifyServiceCompletion(jobBeforeCompletion, request.Result, request.RuntimeQuiescenceEvidence, lastFailureJSON, now)
		servicePolicy = &policy
		finalJobState = policy.jobState
		finalAttemptState = policy.attemptState
	}
	prestartRequeue := attempt.spec.Kind == contract.JobKindOCI && jobBeforeCompletion.ServiceJob == nil &&
		attempt.state == contract.AttemptClaimed && request.Result.SpawnError != nil &&
		request.Result.SpawnError.Code == contract.SpawnFailureRuntimeUnavailable
	if prestartRequeue {
		var retryCount int
		var deadlineNS sql.NullInt64
		if err := tx.QueryRowContext(ctx, `SELECT prestart_retry_count, prestart_budget_deadline_ns FROM jobs WHERE job_id=?`, jobID).
			Scan(&retryCount, &deadlineNS); err != nil {
			return Job{}, internalError(err, "read pre-start retry budget")
		}
		retryCount++
		delay := prestartRetryDelay(retryCount, s.restartJitter)
		if deadlineNS.Valid && now.Add(delay).Before(time.Unix(0, deadlineNS.Int64)) {
			finalJobState = contract.JobQueued
			nextRetryNS := now.Add(delay).UnixNano()
			if _, err := tx.ExecContext(ctx, `UPDATE jobs SET prestart_retry_count=?, prestart_next_retry_at_ns=? WHERE job_id=?`, retryCount, nextRetryNS, jobID); err != nil {
				return Job{}, internalError(err, "persist pre-start retry backoff")
			}
		} else if _, err := tx.ExecContext(ctx, `UPDATE jobs SET prestart_retry_count=?, prestart_next_retry_at_ns=NULL,
			prestart_terminal_reason=? WHERE job_id=?`, retryCount,
			"pre-start infrastructure budget exhausted after runtime loss", jobID); err != nil {
			return Job{}, internalError(err, "persist exhausted pre-start retry budget")
		}
	}
	// Successful completion passes through running inside the same transaction
	// so it respects the M0 state table without exposing an extra protocol verb.
	if finalAttemptState == contract.AttemptSucceeded && attempt.state == contract.AttemptClaimed && attempt.spec.Kind != contract.JobKindOCI {
		if _, err := tx.ExecContext(ctx, "UPDATE attempts SET state=? WHERE attempt_id=?", contract.AttemptRunning, attemptID); err != nil {
			return Job{}, internalError(err, "acknowledge attempt execution")
		}
		if _, err := tx.ExecContext(ctx, "UPDATE jobs SET state=? WHERE job_id=?", contract.JobRunning, jobID); err != nil {
			return Job{}, internalError(err, "acknowledge job execution")
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE attempts SET state=?, completion_key=?, completion_hash=?, result_json=?, updated_ns=? WHERE attempt_id=?`,
		finalAttemptState, request.IdempotencyKey, completionHash, resultJSON, now.UnixNano(), attemptID)
	if err != nil {
		return Job{}, internalError(err, "complete attempt")
	}
	jobUpdate := "UPDATE jobs SET state=?, updated_ns=? WHERE job_id=?"
	if prestartRequeue && finalJobState == contract.JobQueued {
		jobUpdate = "UPDATE jobs SET state=?, updated_ns=?, current_attempt_id=NULL WHERE job_id=?"
	}
	_, err = tx.ExecContext(ctx, jobUpdate, finalJobState, now.UnixNano(), jobID)
	if err != nil {
		return Job{}, internalError(err, "complete job")
	}
	if servicePolicy != nil {
		if _, err := tx.ExecContext(ctx, `UPDATE service_jobs
			SET restart_streak=?, lifetime_restart_count=?, next_restart_at=?,
				last_failure=CASE WHEN ? THEN ? ELSE last_failure END,
				healthy_since_ns=NULL, published_attempt_id=NULL
			WHERE job_id=?`, servicePolicy.restartStreak, servicePolicy.lifetimeRestartCount,
			servicePolicy.nextRestartNS, servicePolicy.updateLastFailure, servicePolicy.lastFailure, jobID); err != nil {
			return Job{}, internalError(err, "apply service completion policy")
		}
	}
	if request.Result.SpawnError != nil && request.Result.SpawnError.Code == contract.SpawnFailurePublishedPortOccupied {
		if err := recordPublishedPortOccupied(ctx, tx, jobID, attempt.nodeID, *request.Result.SpawnError); err != nil {
			return Job{}, err
		}
	}
	if request.Result.SpawnError != nil && request.Result.SpawnError.Code == contract.SpawnFailureInsufficientDisk {
		failure := *request.Result.SpawnError
		failure.NodeID = attempt.nodeID
		lastFailure, marshalErr := json.Marshal(failure)
		if marshalErr != nil {
			return Job{}, internalError(marshalErr, "encode insufficient disk failure")
		}
		if _, updateErr := tx.ExecContext(ctx, `UPDATE service_jobs SET last_failure=?, next_restart_at=NULL WHERE job_id=?`, lastFailure, jobID); updateErr != nil {
			return Job{}, internalError(updateErr, "record insufficient disk failure")
		}
	}
	if _, err := pruneServiceAttemptSummaries(ctx, tx, jobID); err != nil {
		return Job{}, err
	}
	job, err := getJobByID(ctx, tx, jobID, now)
	if err != nil {
		return Job{}, internalError(err, "read completed job")
	}
	if err := tx.Commit(); err != nil {
		return Job{}, internalError(err, "commit completion")
	}
	return job, nil
}

func recordPublishedPortOccupied(
	ctx context.Context,
	tx *sql.Tx,
	jobID, nodeID string,
	failure contract.SpawnFailure,
) error {
	var port sql.NullInt64
	err := tx.QueryRowContext(ctx, "SELECT published_port FROM service_jobs WHERE job_id=?", jobID).Scan(&port)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return internalError(err, "read occupied service port")
	}
	// published_port NULL means exclusively "this service has no published
	// surface" (§6.4), so published_port_occupied can never apply to it.
	if !port.Valid {
		return nil
	}
	lastFailure, err := json.Marshal(struct {
		Code          contract.SpawnFailureCode `json:"code"`
		Message       string                    `json:"message"`
		NodeID        string                    `json:"node_id"`
		PublishedPort int                       `json:"published_port"`
	}{
		Code: failure.Code, Message: failure.Message, NodeID: nodeID, PublishedPort: int(port.Int64),
	})
	if err != nil {
		return internalError(err, "encode occupied service port failure")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE service_jobs
		SET last_failure=?, next_restart_at=NULL WHERE job_id=?`, lastFailure, jobID); err != nil {
		return internalError(err, "record occupied service port failure")
	}
	return nil
}

type attemptAuthority struct {
	attemptID                  string
	jobID                      string
	nodeID                     string
	identityNodeID             string
	bootSessionID              string
	currentBootSessionID       string
	authorityGeneration        int64
	currentAuthorityGeneration int64
	state                      contract.AttemptState
	fencingToken               string
	leaseExpires               time.Time
	updatedAt                  time.Time
	currentAttempt             sql.NullString
	completionKey              sql.NullString
	completionHash             sql.NullString
	spec                       contract.JobSpec
	imageObservationJSON       []byte
	imageObservationHash       sql.NullString
	jobImageResolutionHash     sql.NullString
	startedNS                  sql.NullInt64
}

func readAttemptAuthority(ctx context.Context, q queryer, attemptID string) (attemptAuthority, error) {
	var a attemptAuthority
	var leaseNS, updatedNS int64
	var specJSON []byte
	err := q.QueryRowContext(ctx, `
	SELECT a.attempt_id, a.job_id, a.node_id, n.identity_node_id, a.boot_session_id, n.boot_session_id,
	       a.authority_generation, n.authority_generation, a.state, a.fencing_token,
	       a.lease_expires_ns, a.updated_ns, j.current_attempt_id, a.completion_key, a.completion_hash,
	       j.spec_json, a.image_observation_json, a.image_observation_hash,
	       j.image_resolution_hash, a.started_ns
FROM attempts a
JOIN jobs j ON j.job_id=a.job_id
JOIN nodes n ON n.node_id=a.node_id
	WHERE a.attempt_id=?`, attemptID).Scan(&a.attemptID, &a.jobID, &a.nodeID, &a.identityNodeID,
		&a.bootSessionID, &a.currentBootSessionID, &a.authorityGeneration, &a.currentAuthorityGeneration,
		&a.state, &a.fencingToken, &leaseNS, &updatedNS, &a.currentAttempt, &a.completionKey, &a.completionHash,
		&specJSON, &a.imageObservationJSON, &a.imageObservationHash,
		&a.jobImageResolutionHash, &a.startedNS)
	if errors.Is(err, sql.ErrNoRows) {
		return attemptAuthority{}, protocolError(contract.ErrorAttemptNotFound, "attempt %q was not found", attemptID)
	}
	if err != nil {
		return attemptAuthority{}, internalError(err, "read attempt authority")
	}
	a.leaseExpires = time.Unix(0, leaseNS).UTC()
	a.updatedAt = time.Unix(0, updatedNS).UTC()
	if err := json.Unmarshal(specJSON, &a.spec); err != nil {
		return attemptAuthority{}, internalError(err, "decode attempt job specification")
	}
	return a, nil
}

func validateAttemptAuthority(identityNodeID, jobID, attemptID, fencingToken string, a attemptAuthority) error {
	if a.identityNodeID != identityNodeID {
		return protocolError(contract.ErrorAttemptNotOwned, "authenticated node does not own this attempt")
	}
	if a.jobID != jobID || a.attemptID != attemptID {
		return protocolError(contract.ErrorAttemptMismatch, "attempt does not match the request path")
	}
	if !a.currentAttempt.Valid || a.currentAttempt.String != attemptID {
		return protocolError(contract.ErrorAttemptMismatch, "attempt is not the job's current attempt")
	}
	if a.fencingToken != fencingToken {
		return protocolError(contract.ErrorStaleFence, "fencing token is stale")
	}
	if a.bootSessionID != a.currentBootSessionID || a.authorityGeneration != a.currentAuthorityGeneration {
		return protocolError(contract.ErrorNodeSessionReplaced, "attempt authority belongs to a replaced node registration")
	}
	return nil
}

func validateAttemptEvidence(identityNodeID, jobID, attemptID, fencingToken string, a attemptAuthority) error {
	if a.identityNodeID != identityNodeID {
		return protocolError(contract.ErrorAttemptNotOwned, "authenticated node does not own this attempt")
	}
	if a.jobID != jobID || a.attemptID != attemptID {
		return protocolError(contract.ErrorAttemptMismatch, "attempt does not match the request path")
	}
	if a.fencingToken != fencingToken {
		return protocolError(contract.ErrorStaleFence, "fencing token is stale")
	}
	return nil
}

func validateCompletionReplayAuthority(attemptID string, a attemptAuthority) error {
	if a.bootSessionID != a.currentBootSessionID || a.authorityGeneration != a.currentAuthorityGeneration {
		return protocolError(contract.ErrorNodeSessionReplaced, "attempt authority belongs to a replaced node registration")
	}
	if a.currentAttempt.Valid && a.currentAttempt.String != attemptID {
		return protocolError(contract.ErrorAttemptMismatch, "attempt is not the job's current attempt")
	}
	return nil
}

func validateOCICompletionPhase(attempt attemptAuthority, result ProcessResult) error {
	if attempt.spec.Kind != contract.JobKindOCI {
		return nil
	}
	if !attempt.startedNS.Valid {
		if result.SpawnError == nil || result.OOM || result.LogEvidenceIncomplete {
			return protocolError(contract.ErrorConflict, "pre-Started OCI completion requires a sole spawn_error without additive runtime evidence")
		}
		return nil
	}
	if result.SpawnError != nil {
		return protocolError(contract.ErrorConflict, "post-Started OCI completion cannot report spawn_error")
	}
	return nil
}

func expireAttempt(ctx context.Context, tx *sql.Tx, attempt attemptAuthority, now time.Time) error {
	result, err := tx.ExecContext(ctx, `UPDATE attempts SET state=?, updated_ns=?
		WHERE attempt_id=? AND state IN (?, ?, ?)`, contract.AttemptLost, now.UnixNano(), attempt.attemptID,
		contract.AttemptClaimed, contract.AttemptRunning, contract.AttemptAwaitingInput)
	if err != nil {
		return internalError(err, "mark attempt lost")
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return internalError(err, "read attempt expiry result")
	}
	if changed > 0 {
		var jobState contract.JobState
		var desiredState sql.NullString
		err := tx.QueryRowContext(ctx, `SELECT jobs.state, service_jobs.desired_state
			FROM jobs LEFT JOIN service_jobs ON service_jobs.job_id=jobs.job_id
			WHERE jobs.job_id=? AND jobs.current_attempt_id=?`, attempt.jobID, attempt.attemptID).
			Scan(&jobState, &desiredState)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return internalError(err, "read job after lease expiry")
		}

		nextState := contract.JobFailed
		if desiredState.Valid && contract.ServiceDesiredState(desiredState.String) == contract.ServiceDesiredRunning &&
			(jobState == contract.JobClaimed || jobState == contract.JobRunning) {
			nextState = contract.JobQueued
		}
		if _, err := tx.ExecContext(ctx, `UPDATE jobs SET state=?, updated_ns=?
			WHERE job_id=? AND current_attempt_id=?`, nextState, now.UnixNano(), attempt.jobID, attempt.attemptID); err != nil {
			return internalError(err, "transition job after lease expiry")
		}
		if desiredState.Valid {
			lifetimeIncrement := 0
			if nextState == contract.JobQueued {
				lifetimeIncrement = 1
			}
			if _, err := tx.ExecContext(ctx, `UPDATE service_jobs
				SET lifetime_restart_count=lifetime_restart_count+?, next_restart_at=NULL,
					healthy_since_ns=NULL, published_attempt_id=NULL
				WHERE job_id=?`, lifetimeIncrement, attempt.jobID); err != nil {
				return internalError(err, "clear service authority after lease expiry")
			}
		}
	}
	return nil
}

func validateJobSpec(spec *contract.JobSpec) error {
	if err := contract.ValidateJobSpec(spec); err != nil {
		return protocolError(errorCode(err), "%v", err)
	}
	return nil
}

func validateProcessResult(result ProcessResult) error {
	set := 0
	if result.SpawnError != nil {
		set++
		if result.SpawnError.Code == "" || strings.TrimSpace(result.SpawnError.Message) == "" {
			return protocolError(contract.ErrorInvalidRequest, "spawn_error code and message are required")
		}
	}
	if result.RuntimeFailure != nil {
		set++
		if result.RuntimeFailure.Code == "" || strings.TrimSpace(result.RuntimeFailure.Message) == "" {
			return protocolError(contract.ErrorInvalidRequest, "runtime_failure code and message are required")
		}
	}
	if result.OutputError != "" {
		set++
	}
	if result.ExitCode != nil {
		set++
	}
	if result.Signal != "" {
		set++
		if !validTerminationCause(result.TerminationCause) {
			return protocolError(contract.ErrorInvalidRequest, "signal result requires a valid termination_cause")
		}
	} else if result.TerminationCause != "" {
		return protocolError(contract.ErrorInvalidRequest, "termination_cause requires signal")
	}
	if set != 1 {
		return protocolError(contract.ErrorInvalidRequest, "result must contain exactly one of spawn_error, runtime_failure, output_error, exit_code, or signal")
	}
	return nil
}

func validTerminationCause(cause contract.TerminationCause) bool {
	switch cause {
	case contract.TerminationCauseSpontaneous, contract.TerminationCauseAgent, contract.TerminationCauseGuardian:
		return true
	default:
		return false
	}
}

func validSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			if r < 'a' || r > 'f' {
				return false
			}
		}
	}
	return true
}

func completionStates(result ProcessResult) (contract.JobState, contract.AttemptState) {
	if result.ExitCode != nil && *result.ExitCode == 0 {
		return contract.JobSucceeded, contract.AttemptSucceeded
	}
	return contract.JobFailed, contract.AttemptFailed
}

func markPortlessServiceStable(ctx context.Context, tx *sql.Tx, jobID string, now time.Time) error {
	if _, err := tx.ExecContext(ctx, `UPDATE service_jobs
		SET healthy_since_ns=COALESCE(healthy_since_ns, ?)
		WHERE job_id=? AND published_port IS NULL`, now.UnixNano(), jobID); err != nil {
		return internalError(err, "record portless service stability")
	}
	return nil
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func getJobByDispatchKey(ctx context.Context, q queryer, dispatchKey string, now time.Time) (Job, string, error) {
	var job Job
	var specJSON []byte
	var currentAttempt sql.NullString
	var createdNS, updatedNS int64
	var requestHash string
	var failureReason sql.NullString
	var serviceColumns serviceJobColumns
	err := q.QueryRowContext(ctx, `SELECT jobs.job_id,
COALESCE((SELECT node_id FROM attempts WHERE attempt_id=jobs.current_attempt_id), ''),
state, spec_json, current_attempt_id, created_ns, updated_ns, request_hash, prestart_terminal_reason,
service_jobs.desired_state, service_jobs.bound_node_id, service_jobs.restart_streak,
service_jobs.lifetime_restart_count, service_jobs.next_restart_at, service_jobs.published_port,
service_jobs.last_failure, service_jobs.healthy_since_ns, service_jobs.published_attempt_id,
CASE
	WHEN service_jobs.published_port IS NULL THEN NULL
	WHEN service_jobs.published_attempt_id=jobs.current_attempt_id
		AND jobs.state IN ('claimed', 'running')
		AND EXISTS (
			SELECT 1 FROM attempts publication_attempt
			JOIN nodes publication_node ON publication_node.node_id=publication_attempt.node_id
			WHERE publication_attempt.attempt_id=service_jobs.published_attempt_id
				AND publication_attempt.job_id=jobs.job_id
				AND publication_attempt.node_id=service_jobs.bound_node_id
				AND publication_attempt.state IN ('claimed', 'running', 'awaiting-input')
				AND publication_attempt.lease_expires_ns>@now_ns
				AND publication_attempt.boot_session_id=publication_node.boot_session_id
				AND publication_attempt.authority_generation=publication_node.authority_generation
		)
	THEN 1 ELSE 0
END
FROM jobs LEFT JOIN service_jobs ON service_jobs.job_id=jobs.job_id
WHERE jobs.dispatch_key=@dispatch_key`, sql.Named("now_ns", now.UnixNano()), sql.Named("dispatch_key", dispatchKey)).Scan(append([]any{
		&job.JobID, &job.NodeID, &job.State, &specJSON, &currentAttempt, &createdNS, &updatedNS, &requestHash, &failureReason,
	}, serviceColumns.scanDestinations()...)...)
	if err != nil {
		return Job{}, "", err
	}
	if err := populateJob(&job, specJSON, currentAttempt, createdNS, updatedNS, serviceColumns); err != nil {
		return Job{}, "", err
	}
	if failureReason.Valid {
		job.FailureReason = failureReason.String
	}
	if removal, removalErr := readServiceRemoval(ctx, q, job.JobID); removalErr == nil {
		applyServiceRemoval(&job, removal)
	} else if !errors.Is(removalErr, sql.ErrNoRows) {
		return Job{}, "", removalErr
	}
	return job, requestHash, nil
}

func getJobByID(ctx context.Context, q queryer, jobID string, now time.Time) (Job, error) {
	var job Job
	var specJSON []byte
	var currentAttempt sql.NullString
	var createdNS, updatedNS int64
	var failureReason sql.NullString
	var serviceColumns serviceJobColumns
	err := q.QueryRowContext(ctx, `SELECT jobs.job_id,
COALESCE((SELECT node_id FROM attempts WHERE attempt_id=jobs.current_attempt_id), ''),
state, spec_json, current_attempt_id, created_ns, updated_ns, prestart_terminal_reason,
service_jobs.desired_state, service_jobs.bound_node_id, service_jobs.restart_streak,
service_jobs.lifetime_restart_count, service_jobs.next_restart_at, service_jobs.published_port,
service_jobs.last_failure, service_jobs.healthy_since_ns, service_jobs.published_attempt_id,
CASE
	WHEN service_jobs.published_port IS NULL THEN NULL
	WHEN service_jobs.published_attempt_id=jobs.current_attempt_id
		AND jobs.state IN ('claimed', 'running')
		AND EXISTS (
			SELECT 1 FROM attempts publication_attempt
			JOIN nodes publication_node ON publication_node.node_id=publication_attempt.node_id
			WHERE publication_attempt.attempt_id=service_jobs.published_attempt_id
				AND publication_attempt.job_id=jobs.job_id
				AND publication_attempt.node_id=service_jobs.bound_node_id
				AND publication_attempt.state IN ('claimed', 'running', 'awaiting-input')
				AND publication_attempt.lease_expires_ns>@now_ns
				AND publication_attempt.boot_session_id=publication_node.boot_session_id
				AND publication_attempt.authority_generation=publication_node.authority_generation
		)
	THEN 1 ELSE 0
END
FROM jobs LEFT JOIN service_jobs ON service_jobs.job_id=jobs.job_id
WHERE jobs.job_id=@job_id`, sql.Named("now_ns", now.UnixNano()), sql.Named("job_id", jobID)).Scan(append([]any{
		&job.JobID, &job.NodeID, &job.State, &specJSON, &currentAttempt, &createdNS, &updatedNS, &failureReason,
	}, serviceColumns.scanDestinations()...)...)
	if err != nil {
		return Job{}, err
	}
	if err := populateJob(&job, specJSON, currentAttempt, createdNS, updatedNS, serviceColumns); err != nil {
		return Job{}, err
	}
	if failureReason.Valid {
		job.FailureReason = failureReason.String
	}
	if removal, removalErr := readServiceRemoval(ctx, q, job.JobID); removalErr == nil {
		applyServiceRemoval(&job, removal)
	} else if !errors.Is(removalErr, sql.ErrNoRows) {
		return Job{}, removalErr
	}
	return job, nil
}

func populateJob(job *Job, specJSON []byte, currentAttempt sql.NullString, createdNS, updatedNS int64, serviceColumns serviceJobColumns) error {
	if err := json.Unmarshal(specJSON, &job.Spec); err != nil {
		return err
	}
	if currentAttempt.Valid {
		job.CurrentAttemptID = currentAttempt.String
	}
	job.CreatedAt = time.Unix(0, createdNS).UTC()
	job.UpdatedAt = time.Unix(0, updatedNS).UTC()
	job.ServiceJob = serviceColumns.projection()
	return nil
}

type nodeQueryer interface {
	queryer
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func getNode(ctx context.Context, q nodeQueryer, nodeID string) (Node, error) {
	var node Node
	var capabilitiesJSON, missingCapabilitiesJSON []byte
	var heartbeatNS, capabilityObservedNS int64
	var intentUpdatedNS sql.NullInt64
	err := q.QueryRowContext(ctx, `SELECT node_id, boot_session_id, connect_host, root_instance_id, os, architecture, agent_version, capabilities_json,
	capability_revision, capability_observed_ns, missing_capabilities_json, capability_reason_code, state, max_oneshot_slots, max_service_slots,
	authority_generation, claims_enabled, intent_revision, intent_reason, intent_updated_at, intent_actor, last_heartbeat_ns
	FROM nodes WHERE node_id=?`, nodeID).Scan(&node.NodeID, &node.BootSessionID, &node.ConnectHost, &node.RootInstanceID, &node.OS, &node.Architecture,
		&node.AgentVersion, &capabilitiesJSON, &node.CapabilityRevision, &capabilityObservedNS, &missingCapabilitiesJSON,
		&node.CapabilityReasonCode, &node.State, &node.MaxOneshotSlots, &node.MaxServiceSlots,
		&node.AuthorityGeneration, &node.ClaimsEnabled, &node.IntentRevision, &node.IntentReason, &intentUpdatedNS,
		&node.IntentActor, &heartbeatNS)
	if err != nil {
		return Node{}, err
	}
	if err := json.Unmarshal(capabilitiesJSON, &node.Capabilities); err != nil {
		return Node{}, err
	}
	if err := json.Unmarshal(missingCapabilitiesJSON, &node.MissingCapabilities); err != nil {
		return Node{}, err
	}
	node.CapabilityObservedAt = time.Unix(0, capabilityObservedNS).UTC()
	rows, err := q.QueryContext(ctx, "SELECT tag FROM node_tags WHERE node_id=? ORDER BY tag", nodeID)
	if err != nil {
		return Node{}, err
	}
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return Node{}, err
		}
		node.AuthoritativeTags = append(node.AuthoritativeTags, tag)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return Node{}, err
	}
	if err := rows.Close(); err != nil {
		return Node{}, err
	}
	if node.AuthoritativeTags == nil {
		node.AuthoritativeTags = []string{}
	}
	node.LastHeartbeatAt = time.Unix(0, heartbeatNS).UTC()
	if intentUpdatedNS.Valid {
		updatedAt := time.Unix(0, intentUpdatedNS.Int64).UTC()
		node.IntentUpdatedAt = &updatedAt
	}
	if err := q.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM attempts one_shot_attempt
		 WHERE one_shot_attempt.node_id=?
		   AND one_shot_attempt.state IN (?, ?, ?)
		   AND NOT EXISTS (SELECT 1 FROM service_jobs WHERE service_jobs.job_id=one_shot_attempt.job_id)),
		(SELECT COUNT(*) FROM service_jobs occupied_service
		 JOIN jobs occupied_job ON occupied_job.job_id=occupied_service.job_id
		 WHERE occupied_service.bound_node_id=?
		   AND ((occupied_job.state=? AND occupied_service.desired_state=?)
		        OR occupied_job.state IN (?, ?, ?, ?, ?)))`,
		nodeID, contract.AttemptClaimed, contract.AttemptRunning, contract.AttemptAwaitingInput,
		nodeID, contract.JobQueued, contract.ServiceDesiredRunning, contract.JobClaimed, contract.JobRunning,
		contract.JobStopping, contract.JobRemovalPending, contract.JobAgentCleaned,
	).Scan(&node.OneshotOccupancy, &node.ServiceOccupancy); err != nil {
		return Node{}, err
	}
	node.Overcommitted = node.OneshotOccupancy > node.MaxOneshotSlots || node.ServiceOccupancy > node.MaxServiceSlots
	return node, nil
}

func normalizeRegistrationCapabilities(capabilities map[string]bool) map[string]bool {
	normalized := make(map[string]bool, len(capabilities))
	for capability, enabled := range capabilities {
		normalized[capability] = enabled
	}
	if legacyProcess, ok := normalized["process"]; ok {
		if _, canonical := normalized["kind:process"]; !canonical {
			normalized["kind:process"] = legacyProcess
		}
		delete(normalized, "process")
	}
	return normalized
}

func canonicalTime(value time.Time) time.Time { return value.UTC().Round(0) }

func newID(prefix string) string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		panic(fmt.Sprintf("l1: crypto/rand failed: %v", err))
	}
	return prefix + "_" + hex.EncodeToString(bytes[:])
}
