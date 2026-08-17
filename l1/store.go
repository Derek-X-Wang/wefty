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
	Clock              Clock
	LeaseDuration      time.Duration
	LateEvidenceWindow time.Duration
	NodeStaleAfter     time.Duration
	NodeDeadAfter      time.Duration
}

// Store is the durable SQLite substrate for L1 queue operations.
type Store struct {
	db                 *sql.DB
	clock              Clock
	leaseDuration      time.Duration
	lateEvidenceWindow time.Duration
	nodeStaleAfter     time.Duration
	nodeDeadAfter      time.Duration
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
		db: db, clock: clock, leaseDuration: leaseDuration, lateEvidenceWindow: lateEvidenceWindow,
		nodeStaleAfter: nodeStaleAfter, nodeDeadAfter: nodeDeadAfter,
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
  created_ns INTEGER NOT NULL,
  updated_ns INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS jobs_claim_order ON jobs(state, created_ns, job_id);
CREATE TABLE IF NOT EXISTS job_tags (
  job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
  tag TEXT NOT NULL,
  PRIMARY KEY (job_id, tag)
);
CREATE TABLE IF NOT EXISTS nodes (
  node_id TEXT PRIMARY KEY,
  identity_node_id TEXT NOT NULL,
  boot_session_id TEXT NOT NULL,
  os TEXT NOT NULL,
  architecture TEXT NOT NULL,
  agent_version TEXT NOT NULL,
  capabilities_json BLOB NOT NULL,
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
	return nil
}

func (s *Store) Close() error { return s.db.Close() }

// CreateJob creates a job or returns the identical dispatch-key replay.
func (s *Store) CreateJob(ctx context.Context, spec contract.JobSpec) (job Job, replayed bool, err error) {
	if err := validateJobSpec(&spec); err != nil {
		return Job{}, false, err
	}
	spec.RoutingTags = NormalizeTags(spec.RoutingTags)
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return Job{}, false, internalError(err, "encode job specification")
	}
	hash := sha256.Sum256(specJSON)
	requestHash := hex.EncodeToString(hash[:])

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, false, internalError(err, "begin job creation")
	}
	defer tx.Rollback()

	job, storedHash, err := getJobByDispatchKey(ctx, tx, spec.DispatchKey)
	if err == nil {
		if storedHash != requestHash {
			return Job{}, false, protocolError(contract.ErrorDispatchKeyConflict, "dispatch key %q was already used with a different job", spec.DispatchKey)
		}
		return job, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Job{}, false, internalError(err, "read dispatch key")
	}

	now := canonicalTime(s.clock.Now())
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
	if spec.Class == contract.JobClassService {
		if _, err := tx.ExecContext(ctx, `INSERT INTO service_jobs(job_id, desired_state, published_port)
			VALUES(?, ?, ?)`, job.JobID, contract.ServiceDesiredRunning, spec.PublishedPort); err != nil {
			return Job{}, false, internalError(err, "initialize service job")
		}
		job.ServiceJob = &ServiceJob{
			DesiredState:  contract.ServiceDesiredRunning,
			PublishedPort: spec.PublishedPort,
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
	job, storedHash, err := getJobByDispatchKey(ctx, s.db, dispatchKey)
	if err != nil {
		return Job{}, false, internalError(insertErr, "store job")
	}
	if storedHash != requestHash {
		return Job{}, false, protocolError(contract.ErrorDispatchKeyConflict, "dispatch key %q was already used with a different job", dispatchKey)
	}
	return job, true, nil
}

func (s *Store) GetJob(ctx context.Context, jobID string) (Job, error) {
	job, err := getJobByID(ctx, s.db, jobID)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, protocolError(contract.ErrorNotFound, "job %q was not found", jobID)
	}
	if err != nil {
		return Job{}, internalError(err, "read job")
	}
	return job, nil
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
	capabilities, err := json.Marshal(nonNilCapabilities(registration.Capabilities))
	if err != nil {
		return Node{}, internalError(err, "encode node capabilities")
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Node{}, internalError(err, "begin node registration")
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
INSERT INTO nodes(node_id, identity_node_id, boot_session_id, os, architecture, agent_version, capabilities_json, state, last_heartbeat_ns, max_oneshot_slots, max_service_slots, authority_generation, claims_enabled)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
ON CONFLICT(node_id) DO UPDATE SET
  boot_session_id=excluded.boot_session_id,
  os=excluded.os,
  architecture=excluded.architecture,
  agent_version=excluded.agent_version,
  capabilities_json=excluded.capabilities_json,
  state=excluded.state,
	last_heartbeat_ns=excluded.last_heartbeat_ns,
	max_oneshot_slots=excluded.max_oneshot_slots,
	max_service_slots=excluded.max_service_slots,
	authority_generation=nodes.authority_generation+1
WHERE nodes.identity_node_id=excluded.identity_node_id`, registration.NodeID, identity.NodeID, registration.BootSessionID,
		registration.OS, registration.Architecture, registration.AgentVersion, capabilities, contract.NodeAlive, now.UnixNano(),
		policy.MaxOneshotSlots, policy.MaxServiceSlots, operatorExpected)
	if err != nil {
		return Node{}, internalError(err, "store node registration")
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return Node{}, internalError(err, "read node registration result")
	}
	if changed == 0 {
		return Node{}, protocolError(contract.ErrorIdentityBound, "stable node %q is bound to another Fabric identity", registration.NodeID)
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

func (s *Store) HeartbeatNode(ctx context.Context, identityNodeID, nodeID, bootSessionID string) (Node, error) {
	now := canonicalTime(s.clock.Now())
	result, err := s.db.ExecContext(ctx, `UPDATE nodes
	SET state=CASE WHEN state IN (?, ?) THEN ? ELSE state END, last_heartbeat_ns=?
	WHERE node_id=? AND identity_node_id=? AND boot_session_id=? AND state IN (?, ?, ?)`,
		contract.NodeAlive, contract.NodeStale, contract.NodeAlive, now.UnixNano(), nodeID, identityNodeID, bootSessionID,
		contract.NodeAlive, contract.NodeStale, contract.NodeDraining)
	if err != nil {
		return Node{}, internalError(err, "update node heartbeat")
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return Node{}, internalError(err, "read heartbeat result")
	}
	if changed == 0 {
		return Node{}, s.nodeSessionError(ctx, nodeID, identityNodeID, bootSessionID, "heartbeat")
	}
	node, err := getNode(ctx, s.db, nodeID)
	if err != nil {
		return Node{}, internalError(err, "read heartbeating node")
	}
	return node, nil
}

// ClaimJob atomically performs eligibility selection, fencing, attempt
// creation, and lease establishment. Tag matching is part of the UPDATE that
// wins the queued row, inside this transaction.
func (s *Store) ClaimJob(ctx context.Context, identityNodeID, nodeID, bootSessionID string) (*Claim, error) {
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

	leaseExpires := canonicalTime(now.Add(s.leaseDuration))
	attemptID := newID("attempt")
	var jobID string
	var specJSON []byte
	var fence int64
	var createdNS int64
	err = tx.QueryRowContext(ctx, `
UPDATE jobs
SET state=?, current_attempt_id=?, fence_counter=fence_counter+1, updated_ns=?
WHERE job_id=(
  SELECT j.job_id FROM jobs j
	  WHERE j.state=?
	    AND NOT EXISTS (
	      SELECT 1 FROM attempts prior
	      WHERE prior.node_id=?
	        AND prior.boot_session_id<>?
	        AND prior.state IN (?, ?, ?)
	    )
	    AND NOT EXISTS (
      SELECT 1 FROM job_tags jt
      WHERE jt.job_id=j.job_id
        AND NOT EXISTS (
          SELECT 1 FROM node_tags nt WHERE nt.node_id=? AND nt.tag=jt.tag
        )
    )
  ORDER BY j.created_ns, j.job_id
  LIMIT 1
)
AND state=?
	RETURNING job_id, spec_json, fence_counter, created_ns`, contract.JobClaimed, attemptID, now.UnixNano(), contract.JobQueued,
		nodeID, bootSessionID, contract.AttemptClaimed, contract.AttemptRunning, contract.AttemptAwaitingInput,
		nodeID, contract.JobQueued).
		Scan(&jobID, &specJSON, &fence, &createdNS)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return nil, internalError(err, "commit empty claim")
		}
		return nil, nil
	}
	if err != nil {
		return nil, internalError(err, "atomically claim job")
	}
	fencingToken := strconv.FormatInt(fence, 10)
	_, err = tx.ExecContext(ctx, `
	INSERT INTO attempts(attempt_id, job_id, node_id, boot_session_id, state, fencing_token, lease_expires_ns, authority_generation, created_ns, updated_ns)
	VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, attemptID, jobID, nodeID, bootSessionID, contract.AttemptClaimed,
		fencingToken, leaseExpires.UnixNano(), authorityGeneration, now.UnixNano(), now.UnixNano())
	if err != nil {
		return nil, internalError(err, "create claimed attempt")
	}
	if err := tx.Commit(); err != nil {
		return nil, internalError(err, "commit job claim")
	}
	var spec contract.JobSpec
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		return nil, internalError(err, "decode claimed job")
	}
	return &Claim{
		Job: Job{
			JobID: jobID, NodeID: nodeID, State: contract.JobClaimed, Spec: spec, CurrentAttemptID: attemptID,
			CreatedAt: time.Unix(0, createdNS).UTC(), UpdatedAt: now,
		},
		Lease: AttemptLease{
			AttemptID: attemptID, FencingToken: fencingToken, LeaseExpires: leaseExpires,
			LeaseTTL: leaseExpires.Sub(now),
		},
	}, nil
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
	if attempt.state == contract.AttemptClaimed {
		nextState = contract.AttemptRunning
	}
	_, err = tx.ExecContext(ctx, "UPDATE attempts SET state=?, lease_expires_ns=?, updated_ns=? WHERE attempt_id=?", nextState, expires.UnixNano(), now.UnixNano(), attemptID)
	if err != nil {
		return AttemptLease{}, internalError(err, "renew attempt lease")
	}
	if attempt.state == contract.AttemptClaimed {
		if _, err := tx.ExecContext(ctx, "UPDATE jobs SET state=?, updated_ns=? WHERE job_id=? AND state=?", contract.JobRunning, now.UnixNano(), jobID, contract.JobClaimed); err != nil {
			return AttemptLease{}, internalError(err, "acknowledge renewed job execution")
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
// Identical event replays are acknowledged without changing either the row
// store or the authoritative per-job JSONL. A conflicting replay is rejected.
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

	var appendedJSONL bytes.Buffer
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
		appendedJSONL.Write(prepared.raw)
		appendedJSONL.WriteByte('\n')
	}
	if _, err := tx.ExecContext(ctx, "UPDATE job_log_jsonl SET jsonl=jsonl || ? WHERE job_id=?", appendedJSONL.Bytes(), jobID); err != nil {
		return AppendLogsResponse{}, internalError(err, "append authoritative job JSONL")
	}
	if attempt.state == contract.AttemptClaimed && hasAuthority {
		if _, err := tx.ExecContext(ctx, "UPDATE attempts SET state=?, updated_ns=? WHERE attempt_id=?", contract.AttemptRunning, now.UnixNano(), attemptID); err != nil {
			return AppendLogsResponse{}, internalError(err, "mark logging attempt running")
		}
		if _, err := tx.ExecContext(ctx, "UPDATE jobs SET state=?, updated_ns=? WHERE job_id=?", contract.JobRunning, now.UnixNano(), jobID); err != nil {
			return AppendLogsResponse{}, internalError(err, "mark logging job running")
		}
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
	return page, nil
}

// RawJobLogJSONL returns the authoritative JSONL representation for a job.
// Every line is the exact JSON object persisted alongside its indexed row.
func (s *Store) RawJobLogJSONL(ctx context.Context, jobID string) ([]byte, error) {
	var raw []byte
	err := s.db.QueryRowContext(ctx, "SELECT jsonl FROM job_log_jsonl WHERE job_id=?", jobID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, protocolError(contract.ErrorNotFound, "job %q was not found", jobID)
	}
	if err != nil {
		return nil, internalError(err, "read authoritative job JSONL")
	}
	return bytes.Clone(raw), nil
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
	case contract.LogGapSpoolEviction, contract.LogGapOversizedEvent, contract.LogGapReplayRejected, contract.LogGapLateEvidenceWindowExpired:
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

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, internalError(err, "begin completion")
	}
	defer tx.Rollback()
	attempt, err := readAttemptAuthority(ctx, tx, attemptID)
	if err != nil {
		return Job{}, err
	}
	if err := validateAttemptEvidence(identityNodeID, jobID, attemptID, request.FencingToken, attempt); err != nil {
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
		if err := validateAttemptAuthority(identityNodeID, jobID, attemptID, request.FencingToken, attempt); err != nil {
			return Job{}, err
		}
		job, err := getJobByID(ctx, tx, jobID)
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

	finalJobState, finalAttemptState := completionStates(request.Result)
	// Successful completion passes through running inside the same transaction
	// so it respects the M0 state table without exposing an extra protocol verb.
	if finalJobState == contract.JobSucceeded && attempt.state == contract.AttemptClaimed {
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
	_, err = tx.ExecContext(ctx, "UPDATE jobs SET state=?, updated_ns=? WHERE job_id=?", finalJobState, now.UnixNano(), jobID)
	if err != nil {
		return Job{}, internalError(err, "complete job")
	}
	job, err := getJobByID(ctx, tx, jobID)
	if err != nil {
		return Job{}, internalError(err, "read completed job")
	}
	if err := tx.Commit(); err != nil {
		return Job{}, internalError(err, "commit completion")
	}
	return job, nil
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
}

func readAttemptAuthority(ctx context.Context, q queryer, attemptID string) (attemptAuthority, error) {
	var a attemptAuthority
	var leaseNS, updatedNS int64
	err := q.QueryRowContext(ctx, `
	SELECT a.attempt_id, a.job_id, a.node_id, n.identity_node_id, a.boot_session_id, n.boot_session_id,
	       a.authority_generation, n.authority_generation, a.state, a.fencing_token,
	       a.lease_expires_ns, a.updated_ns, j.current_attempt_id, a.completion_key, a.completion_hash
FROM attempts a
JOIN jobs j ON j.job_id=a.job_id
JOIN nodes n ON n.node_id=a.node_id
	WHERE a.attempt_id=?`, attemptID).Scan(&a.attemptID, &a.jobID, &a.nodeID, &a.identityNodeID,
		&a.bootSessionID, &a.currentBootSessionID, &a.authorityGeneration, &a.currentAuthorityGeneration,
		&a.state, &a.fencingToken, &leaseNS, &updatedNS, &a.currentAttempt, &a.completionKey, &a.completionHash)
	if errors.Is(err, sql.ErrNoRows) {
		return attemptAuthority{}, protocolError(contract.ErrorAttemptNotFound, "attempt %q was not found", attemptID)
	}
	if err != nil {
		return attemptAuthority{}, internalError(err, "read attempt authority")
	}
	a.leaseExpires = time.Unix(0, leaseNS).UTC()
	a.updatedAt = time.Unix(0, updatedNS).UTC()
	return a, nil
}

func validateAttemptAuthority(identityNodeID, jobID, attemptID, fencingToken string, a attemptAuthority) error {
	if err := validateAttemptEvidence(identityNodeID, jobID, attemptID, fencingToken, a); err != nil {
		return err
	}
	if a.bootSessionID != a.currentBootSessionID || a.authorityGeneration != a.currentAuthorityGeneration {
		return protocolError(contract.ErrorNodeSessionReplaced, "attempt authority belongs to a replaced node registration")
	}
	if !a.currentAttempt.Valid || a.currentAttempt.String != attemptID {
		return protocolError(contract.ErrorAttemptMismatch, "attempt is not the job's current attempt")
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
		if _, err := tx.ExecContext(ctx, `UPDATE jobs SET state=?, updated_ns=?
			WHERE job_id=? AND current_attempt_id=? AND state IN (?, ?, ?)`, contract.JobFailed, now.UnixNano(), attempt.jobID,
			attempt.attemptID, contract.JobClaimed, contract.JobRunning, contract.JobAwaitingInput); err != nil {
			return internalError(err, "fail job after lease expiry")
		}
	}
	return nil
}

func validateJobSpec(spec *contract.JobSpec) error {
	if spec.SchemaVersion != contract.SchemaVersionV1 {
		return protocolError(contract.ErrorInvalidRequest, "schema_version must be %d", contract.SchemaVersionV1)
	}
	if strings.TrimSpace(spec.DispatchKey) == "" || strings.TrimSpace(spec.Kind) == "" || strings.TrimSpace(spec.Class) == "" {
		return protocolError(contract.ErrorInvalidRequest, "dispatch_key, kind, and class are required")
	}
	if len(spec.DispatchKey) > 255 || len(spec.Kind) > 128 || len(spec.Class) > 128 || len(spec.RuntimeHandler) > 128 {
		return protocolError(contract.ErrorInvalidRequest, "job identifier fields exceed contract limits")
	}
	if spec.Kind != "process" {
		return protocolError(contract.ErrorUnsupportedKind, "job kind %q is not supported", spec.Kind)
	}
	if spec.RuntimeHandler != "" {
		return protocolError(contract.ErrorUnsupportedRuntimeHandler, "runtime_handler is not supported for process jobs")
	}
	if spec.Execution.WorkingDirectory == "" || len(spec.Execution.Argv) == 0 {
		return protocolError(contract.ErrorInvalidRequest, "execution argv and working_directory are required")
	}
	if spec.Class == contract.JobClassOneShot && spec.Execution.HandoffDirectory == "" {
		return protocolError(contract.ErrorInvalidRequest, "one-shot execution handoff_directory is required")
	}
	if spec.Class == contract.JobClassService {
		if spec.Restart != contract.RestartAlways {
			return protocolError(contract.ErrorInvalidRequest, "service restart must be %q", contract.RestartAlways)
		}
		if spec.PublishedPort != nil && (*spec.PublishedPort < 1 || *spec.PublishedPort > 65535) {
			return protocolError(contract.ErrorInvalidRequest, "published_port must be between 1 and 65535")
		}
		if spec.MaxRestartStreak != nil && *spec.MaxRestartStreak < 1 {
			return protocolError(contract.ErrorInvalidRequest, "max_restart_streak must be at least 1")
		}
	}
	if (spec.Execution.Executable.Path == "") == (spec.Execution.Executable.InlineBase64 == "") {
		return protocolError(contract.ErrorInvalidRequest, "executable must contain exactly one of path or inline_base64")
	}
	if spec.Execution.Executable.InlineBase64 != "" && !validSHA256(spec.Execution.Executable.SHA256) {
		return protocolError(contract.ErrorInvalidRequest, "inline executable requires a lowercase SHA-256 digest")
	}
	if spec.Execution.Executable.Mode > 4095 {
		return protocolError(contract.ErrorInvalidRequest, "executable mode exceeds 07777")
	}
	for _, tag := range NormalizeTags(spec.RoutingTags) {
		if !validTag(tag) {
			return protocolError(contract.ErrorInvalidRequest, "routing tag %q is invalid", tag)
		}
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
		return protocolError(contract.ErrorInvalidRequest, "result must contain exactly one of spawn_error, output_error, exit_code, or signal")
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

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func getJobByDispatchKey(ctx context.Context, q queryer, dispatchKey string) (Job, string, error) {
	var job Job
	var specJSON []byte
	var currentAttempt sql.NullString
	var createdNS, updatedNS int64
	var requestHash string
	var serviceColumns serviceJobColumns
	err := q.QueryRowContext(ctx, `SELECT jobs.job_id,
COALESCE((SELECT node_id FROM attempts WHERE attempt_id=jobs.current_attempt_id), ''),
state, spec_json, current_attempt_id, created_ns, updated_ns, request_hash,
service_jobs.desired_state, service_jobs.bound_node_id, service_jobs.restart_streak,
service_jobs.lifetime_restart_count, service_jobs.next_restart_at, service_jobs.published_port,
service_jobs.last_failure, service_jobs.healthy_since_ns, service_jobs.published_attempt_id
FROM jobs LEFT JOIN service_jobs ON service_jobs.job_id=jobs.job_id
WHERE jobs.dispatch_key=?`, dispatchKey).Scan(append([]any{
		&job.JobID, &job.NodeID, &job.State, &specJSON, &currentAttempt, &createdNS, &updatedNS, &requestHash,
	}, serviceColumns.scanDestinations()...)...)
	if err != nil {
		return Job{}, "", err
	}
	if err := populateJob(&job, specJSON, currentAttempt, createdNS, updatedNS, serviceColumns); err != nil {
		return Job{}, "", err
	}
	return job, requestHash, nil
}

func getJobByID(ctx context.Context, q queryer, jobID string) (Job, error) {
	var job Job
	var specJSON []byte
	var currentAttempt sql.NullString
	var createdNS, updatedNS int64
	var serviceColumns serviceJobColumns
	err := q.QueryRowContext(ctx, `SELECT jobs.job_id,
COALESCE((SELECT node_id FROM attempts WHERE attempt_id=jobs.current_attempt_id), ''),
state, spec_json, current_attempt_id, created_ns, updated_ns,
service_jobs.desired_state, service_jobs.bound_node_id, service_jobs.restart_streak,
service_jobs.lifetime_restart_count, service_jobs.next_restart_at, service_jobs.published_port,
service_jobs.last_failure, service_jobs.healthy_since_ns, service_jobs.published_attempt_id
FROM jobs LEFT JOIN service_jobs ON service_jobs.job_id=jobs.job_id
WHERE jobs.job_id=?`, jobID).Scan(append([]any{
		&job.JobID, &job.NodeID, &job.State, &specJSON, &currentAttempt, &createdNS, &updatedNS,
	}, serviceColumns.scanDestinations()...)...)
	if err != nil {
		return Job{}, err
	}
	if err := populateJob(&job, specJSON, currentAttempt, createdNS, updatedNS, serviceColumns); err != nil {
		return Job{}, err
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
	var capabilitiesJSON []byte
	var heartbeatNS int64
	var intentUpdatedNS sql.NullInt64
	err := q.QueryRowContext(ctx, `SELECT node_id, boot_session_id, os, architecture, agent_version, capabilities_json, state, max_oneshot_slots, max_service_slots,
	authority_generation, claims_enabled, intent_revision, intent_reason, intent_updated_at, intent_actor, last_heartbeat_ns
	FROM nodes WHERE node_id=?`, nodeID).Scan(&node.NodeID, &node.BootSessionID, &node.OS, &node.Architecture,
		&node.AgentVersion, &capabilitiesJSON, &node.State, &node.MaxOneshotSlots, &node.MaxServiceSlots,
		&node.AuthorityGeneration, &node.ClaimsEnabled, &node.IntentRevision, &node.IntentReason, &intentUpdatedNS,
		&node.IntentActor, &heartbeatNS)
	if err != nil {
		return Node{}, err
	}
	if err := json.Unmarshal(capabilitiesJSON, &node.Capabilities); err != nil {
		return Node{}, err
	}
	rows, err := q.QueryContext(ctx, "SELECT tag FROM node_tags WHERE node_id=? ORDER BY tag", nodeID)
	if err != nil {
		return Node{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return Node{}, err
		}
		node.AuthoritativeTags = append(node.AuthoritativeTags, tag)
	}
	if err := rows.Err(); err != nil {
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
	return node, nil
}

func nonNilCapabilities(capabilities map[string]bool) map[string]bool {
	if capabilities == nil {
		return map[string]bool{}
	}
	return capabilities
}

func canonicalTime(value time.Time) time.Time { return value.UTC().Round(0) }

func newID(prefix string) string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		panic(fmt.Sprintf("l1: crypto/rand failed: %v", err))
	}
	return prefix + "_" + hex.EncodeToString(bytes[:])
}
