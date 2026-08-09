package l1

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	_ "modernc.org/sqlite"
)

type StoreOptions struct {
	Clock          Clock
	LeaseDuration  time.Duration
	NodeStaleAfter time.Duration
	NodeDeadAfter  time.Duration
}

// Store is the durable SQLite substrate for L1 queue operations.
type Store struct {
	db             *sql.DB
	clock          Clock
	leaseDuration  time.Duration
	nodeStaleAfter time.Duration
	nodeDeadAfter  time.Duration
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
	query.Set("_txlock", "immediate")
	dsn := (&url.URL{Scheme: "file", Path: path, RawQuery: query.Encode()}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("l1: open SQLite: %w", err)
	}
	db.SetMaxOpenConns(16)
	store := &Store{
		db: db, clock: clock, leaseDuration: leaseDuration,
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
  last_heartbeat_ns INTEGER NOT NULL
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
  completion_key TEXT,
  completion_hash TEXT,
  created_ns INTEGER NOT NULL,
  updated_ns INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS attempts_job ON attempts(job_id);
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

// RegisterNode records a boot session and replaces its routing tags with the
// canonical operator-configured set supplied by the server.
func (s *Store) RegisterNode(ctx context.Context, identity fabric.Identity, registration contract.NodeRegistration, authoritativeTags []string) (Node, error) {
	if registration.NodeID == "" || registration.BootSessionID == "" || registration.OS == "" || registration.Architecture == "" || registration.AgentVersion == "" {
		return Node{}, protocolError(contract.ErrorInvalidRequest, "node registration fields must be non-empty")
	}
	if identity.NodeID == "" {
		return Node{}, protocolError(contract.ErrorForbidden, "authenticated Fabric identity has no node ID")
	}
	tags := NormalizeTags(authoritativeTags)
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
INSERT INTO nodes(node_id, identity_node_id, boot_session_id, os, architecture, agent_version, capabilities_json, state, last_heartbeat_ns)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(node_id) DO UPDATE SET
  boot_session_id=excluded.boot_session_id,
  os=excluded.os,
  architecture=excluded.architecture,
  agent_version=excluded.agent_version,
  capabilities_json=excluded.capabilities_json,
  state=excluded.state,
	last_heartbeat_ns=excluded.last_heartbeat_ns
WHERE nodes.identity_node_id=excluded.identity_node_id`, registration.NodeID, identity.NodeID, registration.BootSessionID,
		registration.OS, registration.Architecture, registration.AgentVersion, capabilities, contract.NodeAlive, now.UnixNano())
	if err != nil {
		return Node{}, internalError(err, "store node registration")
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return Node{}, internalError(err, "read node registration result")
	}
	if changed == 0 {
		return Node{}, protocolError(contract.ErrorForbidden, "stable node %q is bound to another Fabric identity", registration.NodeID)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM node_tags WHERE node_id = ?", registration.NodeID); err != nil {
		return Node{}, internalError(err, "replace node tags")
	}
	for _, tag := range tags {
		if _, err := tx.ExecContext(ctx, "INSERT INTO node_tags(node_id, tag) VALUES(?, ?)", registration.NodeID, tag); err != nil {
			return Node{}, internalError(err, "store node tag")
		}
	}
	if err := tx.Commit(); err != nil {
		return Node{}, internalError(err, "commit node registration")
	}
	registration.Capabilities = nonNilCapabilities(registration.Capabilities)
	return Node{NodeRegistration: registration, State: contract.NodeAlive, AuthoritativeTags: tags, LastHeartbeatAt: now}, nil
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
		return Node{}, protocolError(contract.ErrorConflict, "node identity or boot session does not match")
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
	err = tx.QueryRowContext(ctx, "SELECT identity_node_id, boot_session_id, state, last_heartbeat_ns FROM nodes WHERE node_id=?", nodeID).Scan(&storedIdentity, &storedBoot, &nodeState, &heartbeatNS)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, protocolError(contract.ErrorConflict, "node %q is not registered", nodeID)
	}
	if err != nil {
		return nil, internalError(err, "read claiming node")
	}
	if storedIdentity != identityNodeID || storedBoot != bootSessionID {
		return nil, protocolError(contract.ErrorForbidden, "authenticated node does not own this registration")
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
		return nil, protocolError(contract.ErrorConflict, "node %q is not alive", nodeID)
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
RETURNING job_id, spec_json, fence_counter, created_ns`, contract.JobClaimed, attemptID, now.UnixNano(), contract.JobQueued, nodeID, contract.JobQueued).
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
INSERT INTO attempts(attempt_id, job_id, node_id, boot_session_id, state, fencing_token, lease_expires_ns, created_ns, updated_ns)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`, attemptID, jobID, nodeID, bootSessionID, contract.AttemptClaimed,
		fencingToken, leaseExpires.UnixNano(), now.UnixNano(), now.UnixNano())
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
			JobID: jobID, State: contract.JobClaimed, Spec: spec, CurrentAttemptID: attemptID,
			CreatedAt: time.Unix(0, createdNS).UTC(), UpdatedAt: now,
		},
		Lease: AttemptLease{AttemptID: attemptID, FencingToken: fencingToken, LeaseExpires: leaseExpires},
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
	return AttemptLease{AttemptID: attemptID, FencingToken: fencingToken, LeaseExpires: expires}, nil
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

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, internalError(err, "begin completion")
	}
	defer tx.Rollback()
	attempt, err := readAttemptAuthority(ctx, tx, attemptID)
	if err != nil {
		return Job{}, err
	}
	if err := validateAttemptAuthority(identityNodeID, jobID, attemptID, request.FencingToken, attempt); err != nil {
		return Job{}, err
	}
	if attempt.completionKey.Valid {
		if attempt.completionKey.String != request.IdempotencyKey || attempt.completionHash.String != completionHash {
			return Job{}, protocolError(contract.ErrorIdempotencyConflict, "completion idempotency key or body conflicts with the accepted completion")
		}
		job, err := getJobByID(ctx, tx, jobID)
		if err != nil {
			return Job{}, internalError(err, "read completed job replay")
		}
		return job, nil
	}
	now := canonicalTime(s.clock.Now())
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
	_, err = tx.ExecContext(ctx, `UPDATE attempts SET state=?, completion_key=?, completion_hash=?, updated_ns=? WHERE attempt_id=?`,
		finalAttemptState, request.IdempotencyKey, completionHash, now.UnixNano(), attemptID)
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
	attemptID      string
	jobID          string
	nodeID         string
	identityNodeID string
	state          contract.AttemptState
	fencingToken   string
	leaseExpires   time.Time
	currentAttempt sql.NullString
	completionKey  sql.NullString
	completionHash sql.NullString
}

func readAttemptAuthority(ctx context.Context, q queryer, attemptID string) (attemptAuthority, error) {
	var a attemptAuthority
	var leaseNS int64
	err := q.QueryRowContext(ctx, `
SELECT a.attempt_id, a.job_id, a.node_id, n.identity_node_id, a.state, a.fencing_token,
       a.lease_expires_ns, j.current_attempt_id, a.completion_key, a.completion_hash
FROM attempts a
JOIN jobs j ON j.job_id=a.job_id
JOIN nodes n ON n.node_id=a.node_id
WHERE a.attempt_id=?`, attemptID).Scan(&a.attemptID, &a.jobID, &a.nodeID, &a.identityNodeID, &a.state,
		&a.fencingToken, &leaseNS, &a.currentAttempt, &a.completionKey, &a.completionHash)
	if errors.Is(err, sql.ErrNoRows) {
		return attemptAuthority{}, protocolError(contract.ErrorNotFound, "attempt %q was not found", attemptID)
	}
	if err != nil {
		return attemptAuthority{}, internalError(err, "read attempt authority")
	}
	a.leaseExpires = time.Unix(0, leaseNS).UTC()
	return a, nil
}

func validateAttemptAuthority(identityNodeID, jobID, attemptID, fencingToken string, a attemptAuthority) error {
	if a.identityNodeID != identityNodeID {
		return protocolError(contract.ErrorForbidden, "authenticated node does not own this attempt")
	}
	if a.jobID != jobID || !a.currentAttempt.Valid || a.currentAttempt.String != attemptID {
		return protocolError(contract.ErrorAttemptMismatch, "attempt is not the job's current attempt")
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
			WHERE job_id=? AND current_attempt_id=? AND state IN (?, ?)`, contract.JobFailed, now.UnixNano(), attempt.jobID,
			attempt.attemptID, contract.JobClaimed, contract.JobRunning); err != nil {
			return internalError(err, "fail job after lease expiry")
		}
	}
	return nil
}

func validateJobSpec(spec *contract.JobSpec) error {
	if spec.SchemaVersion != contract.SchemaVersionV1 {
		return protocolError(contract.ErrorInvalidRequest, "schema_version must be %d", contract.SchemaVersionV1)
	}
	if strings.TrimSpace(spec.DispatchKey) == "" || strings.TrimSpace(spec.Kind) == "" {
		return protocolError(contract.ErrorInvalidRequest, "dispatch_key and kind are required")
	}
	if len(spec.DispatchKey) > 255 || len(spec.Kind) > 128 || len(spec.RuntimeHandler) > 128 {
		return protocolError(contract.ErrorInvalidRequest, "job identifier fields exceed contract limits")
	}
	if spec.Kind != "process" {
		return protocolError(contract.ErrorUnsupportedKind, "job kind %q is not supported", spec.Kind)
	}
	if spec.RuntimeHandler != "" {
		return protocolError(contract.ErrorUnsupportedRuntimeHandler, "runtime_handler is not supported for process jobs")
	}
	if spec.Execution.WorkingDirectory == "" || spec.Execution.HandoffDirectory == "" || len(spec.Execution.Argv) == 0 {
		return protocolError(contract.ErrorInvalidRequest, "execution argv and directories are required")
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
	if result.SpawnError != "" {
		set++
	}
	if result.ExitCode != nil {
		set++
	}
	if result.Signal != "" {
		set++
	}
	if set != 1 {
		return protocolError(contract.ErrorInvalidRequest, "result must contain exactly one of spawn_error, exit_code, or signal")
	}
	return nil
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
	err := q.QueryRowContext(ctx, `SELECT job_id, state, spec_json, current_attempt_id, created_ns, updated_ns, request_hash
FROM jobs WHERE dispatch_key=?`, dispatchKey).Scan(&job.JobID, &job.State, &specJSON, &currentAttempt, &createdNS, &updatedNS, &requestHash)
	if err != nil {
		return Job{}, "", err
	}
	if err := populateJob(&job, specJSON, currentAttempt, createdNS, updatedNS); err != nil {
		return Job{}, "", err
	}
	return job, requestHash, nil
}

func getJobByID(ctx context.Context, q queryer, jobID string) (Job, error) {
	var job Job
	var specJSON []byte
	var currentAttempt sql.NullString
	var createdNS, updatedNS int64
	err := q.QueryRowContext(ctx, `SELECT job_id, state, spec_json, current_attempt_id, created_ns, updated_ns
FROM jobs WHERE job_id=?`, jobID).Scan(&job.JobID, &job.State, &specJSON, &currentAttempt, &createdNS, &updatedNS)
	if err != nil {
		return Job{}, err
	}
	if err := populateJob(&job, specJSON, currentAttempt, createdNS, updatedNS); err != nil {
		return Job{}, err
	}
	return job, nil
}

func populateJob(job *Job, specJSON []byte, currentAttempt sql.NullString, createdNS, updatedNS int64) error {
	if err := json.Unmarshal(specJSON, &job.Spec); err != nil {
		return err
	}
	if currentAttempt.Valid {
		job.CurrentAttemptID = currentAttempt.String
	}
	job.CreatedAt = time.Unix(0, createdNS).UTC()
	job.UpdatedAt = time.Unix(0, updatedNS).UTC()
	return nil
}

func getNode(ctx context.Context, q *sql.DB, nodeID string) (Node, error) {
	var node Node
	var capabilitiesJSON []byte
	var heartbeatNS int64
	err := q.QueryRowContext(ctx, `SELECT node_id, boot_session_id, os, architecture, agent_version, capabilities_json, state, last_heartbeat_ns
FROM nodes WHERE node_id=?`, nodeID).Scan(&node.NodeID, &node.BootSessionID, &node.OS, &node.Architecture,
		&node.AgentVersion, &capabilitiesJSON, &node.State, &heartbeatNS)
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
