package l1

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
)

// MaxSpawnDepth caps how far a job may be from the root job a client
// principal submitted. It is a fixed hygiene guard, not configuration: v1 is
// fire-and-forget, so nothing else bounds an accidental self-spawning loop.
const MaxSpawnDepth = 8

// attemptCredentialBytes is the bearer's entropy before encoding. It matches
// the Computer pass so no credential in the system is weaker than another.
const attemptCredentialBytes = 32

// AttemptCredentialScope is the authority L1 resolved from one presented
// attempt credential. Every member is read from L1's own records; none of it
// can be influenced by the request body.
type AttemptCredentialScope struct {
	AttemptID string
	JobID     string
	NodeID    string
	// IdentityNodeID is the Fabric identity the credential was proved against.
	// Carrying it lets a later transaction re-run the identical liveness
	// predicates without re-deriving who asked.
	IdentityNodeID       string
	OriginatingSubmitter string
	SpawnDepth           int
}

// newAttemptCredential mints one bearer and returns it with the digest L1
// persists. The bearer itself is never stored.
func newAttemptCredential() (string, string, error) {
	raw := make([]byte, attemptCredentialBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", "", err
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	return token, hashAttemptCredential(token), nil
}

func hashAttemptCredential(token string) string {
	digest := sha256.Sum256([]byte(token))
	return hex.EncodeToString(digest[:])
}

// insertAttemptCredential records one credential inside the claim
// transaction, so a claim either yields a credential or does not happen.
func insertAttemptCredential(ctx context.Context, tx *sql.Tx, hash string, scope AttemptCredentialScope, createdNS int64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO attempt_credentials(
		token_hash, attempt_id, job_id, node_id, originating_submitter, spawn_depth, created_ns)
		VALUES(?, ?, ?, ?, ?, ?, ?)`, hash, scope.AttemptID, scope.JobID, scope.NodeID,
		scope.OriginatingSubmitter, scope.SpawnDepth, createdNS)
	if err != nil {
		return internalError(err, "mint attempt credential")
	}
	return nil
}

// ResolveAttemptCredential turns a presented bearer into authority, or
// refuses. Authority is decided by reading the live attempt rather than by
// anything stored on the credential row: the credential therefore stops
// working at the same instant the lease, boot session, or authority
// generation does, and a deleted row is hygiene rather than enforcement
// (ADR-0003, never restore stale authority).
//
// identityNodeID is the Fabric identity the request actually arrived with. It
// must be the node holding the attempt, so a leaked bearer cannot be replayed
// from anywhere else. This mirrors ProveComputerTokenScope's host binding.
func (s *Store) ResolveAttemptCredential(ctx context.Context, token, identityNodeID string) (AttemptCredentialScope, error) {
	token = strings.TrimSpace(token)
	if token == "" || strings.TrimSpace(identityNodeID) == "" {
		return AttemptCredentialScope{}, protocolError(contract.ErrorUnauthorized, "attempt credential is required")
	}
	var scope AttemptCredentialScope
	err := s.db.QueryRowContext(ctx, `SELECT attempt_id, job_id, node_id, originating_submitter, spawn_depth
		FROM attempt_credentials WHERE token_hash=?`, hashAttemptCredential(token)).
		Scan(&scope.AttemptID, &scope.JobID, &scope.NodeID, &scope.OriginatingSubmitter, &scope.SpawnDepth)
	if errors.Is(err, sql.ErrNoRows) {
		return AttemptCredentialScope{}, protocolError(contract.ErrorUnauthorized, "attempt credential is not recognized")
	}
	if err != nil {
		return AttemptCredentialScope{}, internalError(err, "read attempt credential")
	}
	authority, err := readAttemptAuthority(ctx, s.db, scope.AttemptID)
	if err != nil {
		// An attempt that no longer exists cannot confer authority. Report the
		// credential as refused rather than leaking which attempt it named.
		return AttemptCredentialScope{}, protocolError(contract.ErrorUnauthorized, "attempt credential is no longer live")
	}
	if err := validateAttemptCredentialAuthority(identityNodeID, scope, authority,
		canonicalTime(s.clock.Now()).UnixNano()); err != nil {
		return AttemptCredentialScope{}, err
	}
	scope.IdentityNodeID = identityNodeID
	return scope, nil
}

// revalidateAttemptCredential re-runs the same liveness predicates inside a
// write transaction. Authorization happens before the transaction opens, so an
// attempt that loses authority in that window would otherwise still persist a
// child; re-reading through the transaction closes the gap against the same
// snapshot the write commits on.
func revalidateAttemptCredential(ctx context.Context, q queryer, scope AttemptCredentialScope, nowNS int64) error {
	authority, err := readAttemptAuthority(ctx, q, scope.AttemptID)
	if err != nil {
		return protocolError(contract.ErrorUnauthorized, "attempt credential is no longer live")
	}
	return validateAttemptCredentialAuthority(scope.IdentityNodeID, scope, authority, nowNS)
}

// validateAttemptCredentialAuthority is validateAttemptAuthority's predicate
// set minus the fencing token, which the workload never holds. Keeping the
// same predicates in the same order is deliberate: the credential must die
// exactly when the attempt's write authority does, never one condition later.
func validateAttemptCredentialAuthority(identityNodeID string, scope AttemptCredentialScope, a attemptAuthority, nowNS int64) error {
	if a.identityNodeID != identityNodeID {
		return protocolError(contract.ErrorForbidden, "attempt credential was presented from another node")
	}
	if a.jobID != scope.JobID || a.attemptID != scope.AttemptID {
		return protocolError(contract.ErrorUnauthorized, "attempt credential does not match its attempt")
	}
	if !a.currentAttempt.Valid || a.currentAttempt.String != scope.AttemptID {
		return protocolError(contract.ErrorUnauthorized, "attempt credential belongs to a superseded attempt")
	}
	if a.bootSessionID != a.currentBootSessionID || a.authorityGeneration != a.currentAuthorityGeneration {
		return protocolError(contract.ErrorUnauthorized, "attempt credential belongs to a replaced node registration")
	}
	if a.state != contract.AttemptClaimed && a.state != contract.AttemptRunning && a.state != contract.AttemptAwaitingInput {
		return protocolError(contract.ErrorUnauthorized, "attempt credential belongs to a terminal attempt")
	}
	if a.leaseExpires.UnixNano() <= nowNS {
		return protocolError(contract.ErrorUnauthorized, "attempt credential lease has expired")
	}
	return nil
}

// jobSpawnColumns carries the four immutable parent columns through the two
// job read queries without expanding either scan site by hand.
type jobSpawnColumns struct {
	parentJobID          sql.NullString
	parentAttemptID      sql.NullString
	originatingSubmitter sql.NullString
	spawnDepth           sql.NullInt64
}

func (columns *jobSpawnColumns) scanDestinations() []any {
	return []any{&columns.parentJobID, &columns.parentAttemptID, &columns.originatingSubmitter, &columns.spawnDepth}
}

func (columns jobSpawnColumns) apply(job *Job) {
	job.ParentJobID = columns.parentJobID.String
	job.ParentAttemptID = columns.parentAttemptID.String
	job.OriginatingSubmitter = columns.originatingSubmitter.String
	job.SpawnDepth = int(columns.spawnDepth.Int64)
}

// ListChildJobs pages the jobs whose parent is parentJobID, of either class.
// Children are a job-level resource, so a retried parent attempt sees the
// children spawned by earlier attempts.
func (s *Store) ListChildJobs(ctx context.Context, parentJobID, cursorValue string, limit int) (JobList, error) {
	return s.listChildJobsForCaller(ctx, parentJobID, cursorValue, limit, nil)
}

func (s *Store) listChildJobsForCaller(ctx context.Context, parentJobID, cursorValue string, limit int, actor *serviceActionActor) (JobList, error) {
	if strings.TrimSpace(parentJobID) == "" {
		return JobList{}, protocolError(contract.ErrorInvalidRequest, "job_id is required")
	}
	if limit < 1 || limit > MaxJobPageLimit {
		return JobList{}, protocolError(contract.ErrorInvalidRequest, "limit must be between 1 and %d", MaxJobPageLimit)
	}
	cursor, err := decodeServiceJobCursor(cursorValue)
	if err != nil {
		return JobList{}, err
	}
	// Keep membership, Job rows and operator facts in one read-only snapshot.
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return JobList{}, internalError(err, "begin child job listing")
	}
	defer tx.Rollback()
	if actor != nil {
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM jobs WHERE job_id=?)
			OR EXISTS(SELECT 1 FROM service_tombstones WHERE job_id=?)`, parentJobID, parentJobID).Scan(&exists); err != nil {
			return JobList{}, internalError(err, "read child listing parent")
		}
		if !exists {
			return JobList{}, protocolError(contract.ErrorNotFound, "job %q was not found", parentJobID)
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT job_id, created_ns FROM jobs
		WHERE parent_job_id=? AND (created_ns>? OR (created_ns=? AND job_id>?))
		ORDER BY created_ns, job_id LIMIT ?`,
		parentJobID, cursor.CreatedNS, cursor.CreatedNS, cursor.JobID, limit+1)
	if err != nil {
		return JobList{}, internalError(err, "list child job IDs")
	}
	type listedID struct {
		jobID     string
		createdNS int64
	}
	listed := make([]listedID, 0, limit+1)
	for rows.Next() {
		var item listedID
		if err := rows.Scan(&item.jobID, &item.createdNS); err != nil {
			rows.Close()
			return JobList{}, internalError(err, "scan child job ID")
		}
		listed = append(listed, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return JobList{}, internalError(err, "iterate child job IDs")
	}
	page := JobList{Jobs: []Job{}}
	hasMore := len(listed) > limit
	if hasMore {
		listed = listed[:limit]
	}
	now := canonicalTime(s.clock.Now())
	for _, item := range listed {
		job, err := getJobByID(ctx, tx, item.jobID, now)
		if err != nil {
			return JobList{}, err
		}
		page.Jobs = append(page.Jobs, job)
	}
	if actor != nil {
		if err := projectServiceOperatorPage(ctx, tx, page.Jobs, actor); err != nil {
			return JobList{}, err
		}
	}
	for i, job := range page.Jobs {
		projected, err := s.projectJobWithQueryer(ctx, tx, job)
		if err != nil {
			return JobList{}, err
		}
		page.Jobs[i] = projected
	}
	if err := tx.Commit(); err != nil {
		return JobList{}, internalError(err, "finish child job listing")
	}
	if hasMore && len(listed) > 0 {
		last := listed[len(listed)-1]
		page.NextCursor = encodeServiceJobCursor(serviceJobCursor{CreatedNS: last.createdNS, JobID: last.jobID})
	}
	return page, nil
}

// attemptCredentialPruneBatch bounds how many ended attempts' credential
// rows one reconcile pass deletes, so the backlog an upgrade finds (one row
// per one-shot that ever ran) is worked off across passes.
const attemptCredentialPruneBatch = 4096

// pruneEndedAttemptCredentials deletes the credential rows of attempts that
// have ended, or no longer exist. A credential authorizes only while its
// attempt is claimed, running or awaiting input, and an attempt never leaves
// a terminal state (ADR-0003), so such a row can never authorize again.
// pruneAttemptCredentials, at claim, only reaches attempts a newer claim of
// the same job superseded, so before #52 a terminal job's last attempt kept
// its row forever: one per finished one-shot. Nothing else reads the row.
// Completion replay (#553), late evidence and log upload authenticate with
// the node identity and the attempt's fencing token, never with this
// credential, so none of them needs the row through the late-evidence
// window.
func pruneEndedAttemptCredentials(ctx context.Context, tx *sql.Tx, limit int) (int64, error) {
	result, err := tx.ExecContext(ctx, `DELETE FROM attempt_credentials WHERE token_hash IN (
		SELECT c.token_hash FROM attempt_credentials c
		LEFT JOIN attempts a ON a.attempt_id=c.attempt_id
		WHERE a.attempt_id IS NULL OR a.state NOT IN (?, ?, ?)
		LIMIT ?)`, contract.AttemptClaimed, contract.AttemptRunning, contract.AttemptAwaitingInput, limit)
	if err != nil {
		return 0, internalError(err, "prune ended attempt credentials")
	}
	pruned, err := result.RowsAffected()
	if err != nil {
		return 0, internalError(err, "read ended attempt credential pruning")
	}
	return pruned, nil
}

// pruneAttemptCredentials deletes rows that can no longer authorize anything.
// Enforcement never depends on this running: ResolveAttemptCredential already
// refuses a superseded attempt. This only keeps the table from growing with
// the attempt history.
func pruneAttemptCredentials(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM attempt_credentials WHERE attempt_id NOT IN (
		SELECT current_attempt_id FROM jobs WHERE current_attempt_id IS NOT NULL)`); err != nil {
		return internalError(err, "prune superseded attempt credentials")
	}
	return nil
}
