package l1

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// readModel is the internal vocabulary shared by projections and decisions.
// It grants neither SQL access nor mutation authority. Writes adapt their own
// transaction, after replay checks, so decisions remain inside the write.
type readModel interface {
	computerBackupsPage(context.Context, string, string, string, int) (BackupList, error)
	computerViewComputerBackupOperationForKey(ctx context.Context, computerID, idempotencyKey string) (ComputerBackupOperationOutcome, error)
	computerViewComputerBackupOperation(ctx context.Context, computerID, backupID string) (ComputerBackupOperationOutcome, error)
	computerViewComputerPolicyRevisionInstalled(ctx context.Context, revision int64, computerID string) (bool, error)
	computerViewResolvePersonComputerHandle(ctx context.Context, identity fabric.Identity, handle string, administratorRequired bool) (ComputerHandleResolution, error)
	computerViewGetComputerPolicyRevocation(ctx context.Context, identity fabric.Identity, revision int64, computerID, fabricID, userID string) (ComputerPolicyRevocation, error)
	computerViewGetComputerTakeoverAvailability(ctx context.Context, identity fabric.Identity, computerID string) (ComputerTakeoverAvailability, error)
	computerViewListComputerGrants(ctx context.Context, identity fabric.Identity, computerID string) (ComputerGrantList, error)
	computerViewListComputerPolicyAudit(ctx context.Context, identity fabric.Identity, computerID, cursor string, limit int) (ComputerPolicyAuditList, error)
	computerViewGetComputerSubmissionState(ctx context.Context, identity fabric.Identity, computerID string) (ComputerSubmissionState, error)
	computerViewListComputerTakeoverAudit(ctx context.Context, identity fabric.Identity, computerID, cursor string, limit int, tail bool) (ComputerTakeoverAuditList, error)
	computerViewListComputerTakeoverSessions(ctx context.Context, identity fabric.Identity, computerID string) (ComputerTakeoverSessionList, error)
	computerViewListComputerTakeoverAuditTail(ctx context.Context, computerID string, limit int) (ComputerTakeoverAuditList, error)
	computerViewListComputersForCaller(ctx context.Context, cursorValue string, limit int, actor *computerActionActor) (ComputerList, error)
	computerViewListComputerIntents(ctx context.Context, computerID, cursorValue string, limit int) (ComputerIntentList, error)
	computerViewGetComputer(ctx context.Context, computerID string) (Computer, error)
	computerViewListComputerCustodyExports(ctx context.Context, computerID string) ([]ComputerCustodyExport, error)
	computerViewGetComputerCustodyImport(ctx context.Context, importID string) (ComputerCustodyImportObservation, error)
	computerViewComputerCloneOperation(ctx context.Context, computerID string, operationRevision int64) (ComputerCloneOperation, error)
	computerViewComputerRestoreOperationForKey(ctx context.Context, computerID, idempotencyKey string) (ComputerRestoreOperation, error)
	computerViewComputerRestoreOperation(ctx context.Context, computerID string, operationRevision int64) (ComputerRestoreOperation, error)
	computerViewGetComputerWithCloneOperation(ctx context.Context, computerID string, operationRevision int64) (Computer, error)
	computerViewComputerCloneOperationForKey(ctx context.Context, backupID, idempotencyKey string) (ComputerCloneOperation, error)
	computerViewListComputerStorageGenerations(ctx context.Context, computerID string) (ComputerStorageGenerationList, error)
	computerProvenancePage(context.Context, string, string, int) (ComputerStorageProvenance, error)
	now() time.Time
	caller() *serviceActionActor
	validateCredential(context.Context, AttemptCredentialScope) error
	resolveCredential(context.Context, string, string) (AttemptCredentialScope, error)
	jobsPage(context.Context, jobListFilters, string, int) (JobList, error)
	childrenPage(context.Context, string, string, int) (JobList, error)
	ledgerJob(context.Context, string) (Job, error)
	logs(context.Context, string, string, int) (LogPage, error)
	result(context.Context, string) (JobResult, error)
	rawLogs(context.Context, string) ([]byte, error)
	job(context.Context, string) (Job, error)
	attempts(context.Context, string) ([]Attempt, error)
	node(context.Context, string) (Node, error)
	nodes(context.Context) ([]Node, error)
	nodePage(context.Context, nodeListFilters, nodeListCursor, int, nodeLiveness) (NodeList, error)
	nodeRoot(context.Context, string) (string, error)
	nodeState(context.Context, string) (contract.NodeState, error)
	requireStorage(context.Context, Computer, string) error
	backup(context.Context, string) (Backup, error)
	availableBackup(context.Context, string) (Backup, BackupCopy, error)
	backupChoices(context.Context, string) ([]string, error)
	retainedBackups(context.Context, string) (int64, error)
	operationNode(context.Context, Computer) (string, error)
	intents(context.Context, string, int64, int) ([]ComputerIntent, error)
	serviceComputer(context.Context, string) (string, bool, error)
	serviceResumable(context.Context, Job) (bool, error)
	ensureServiceCapacity(context.Context, Job) error
	serviceRemovalRoot(context.Context, Job) error
	attemptConditionTime(context.Context, Job) (time.Time, error)
	projectStatus(context.Context, Job) (Job, error)
	serviceStatus(context.Context, Job) (Job, error)
	queuedStatus(context.Context, Job) (Job, error)
	person(context.Context, string, string) (AuthenticatedPerson, bool, error)
	adminPolicy(context.Context) (AdminPolicy, error)
	adminPolicyRevision(context.Context) (int64, error)
	currentAdmin(context.Context, fabric.Identity) error
	adminAuditPage(context.Context, int64, int) (AdminPolicyAuditList, error)
}

// databaseReads is private SQL plumbing; callers see only readModel.
// A write gets a fresh memo at each decision, never across writes.
type databaseReads struct {
	pageDeadline    time.Time
	q               queryer
	at              time.Time
	actor           *serviceActionActor
	reads           *serviceOperatorReads
	nodeIDs         []string
	placements      map[string]placementRead
	requirements    map[string]placementRequirements
	occupancy       map[string]int
	occupancyLoaded bool
	occupancyErr    error
	nodesLoaded     bool
	nodesErr        error
}
type nodeRead struct {
	value Node
	err   error
}
type rootRead struct {
	value string
	err   error
}
type stateRead struct {
	value contract.NodeState
	err   error
}

func newDatabaseReads(q queryer, at time.Time, actor *serviceActionActor) *databaseReads {
	return &databaseReads{q: q, at: at, actor: actor, reads: newServiceOperatorReads(q), placements: make(map[string]placementRead), requirements: make(map[string]placementRequirements), occupancy: make(map[string]int)}
}
func transactionReads(q queryer, now time.Time) readModel {
	if now.IsZero() {
		panic("l1: transactionReads requires a pinned clock")
	}
	return newDatabaseReads(q, now, nil)
}
func (r *databaseReads) now() time.Time              { return r.at }
func (r *databaseReads) caller() *serviceActionActor { return r.actor }
func (r *databaseReads) job(ctx context.Context, id string) (Job, error) {
	job, err := getJobByID(ctx, r.q, id, r.at)
	if errors.Is(err, sql.ErrNoRows) {
		tombstone, tombstoneErr := readServiceTombstoneByID(ctx, r.q, id)
		if tombstoneErr != nil {
			return Job{}, tombstoneErr
		}
		return tombstone.job(), nil
	}
	return job, err
}
func (r *databaseReads) attempts(ctx context.Context, id string) ([]Attempt, error) {
	return listJobAttempts(ctx, r.q, id)
}
func (r *databaseReads) node(ctx context.Context, id string) (Node, error) {
	value, ok := r.reads.nodes[id]
	if !ok {
		value.value, value.err = getNode(ctx, r.q, id)
		r.reads.nodes[id] = value
	}
	return value.value, value.err
}

// Placement facts deliberately exclude occupancy and active attempts. A legacy
// per-job projection costs one node query; a snapshot shares the same facts
// across bound jobs and candidate scans.
type placementFacts struct {
	id              string
	state           contract.NodeState
	claimsEnabled   bool
	capabilities    map[string]bool
	tags            []string
	maxServiceSlots int
}
type placementRead struct {
	value placementFacts
	err   error
}
type placementRequirements struct {
	capabilities, tags []string
	err                error
}

func (r *databaseReads) placementRequirements(ctx context.Context, jobID string) (placementRequirements, error) {
	if value, ok := r.requirements[jobID]; ok {
		return value, value.err
	}
	value := placementRequirements{}
	rows, err := r.q.QueryContext(ctx, `SELECT 'capability', capability FROM job_required_capabilities WHERE job_id=?
 UNION ALL SELECT 'tag', tag FROM job_tags WHERE job_id=? ORDER BY 1, 2`, jobID, jobID)
	if err == nil {
		for rows.Next() {
			var kind, item string
			if err = rows.Scan(&kind, &item); err != nil {
				break
			}
			if kind == "tag" {
				value.tags = append(value.tags, item)
			} else {
				value.capabilities = append(value.capabilities, item)
			}
		}
		if err == nil {
			err = rows.Err()
		}
		err = errors.Join(err, rows.Close())
	}
	value.err = err
	r.requirements[jobID] = value
	return value, err
}

func (r *databaseReads) loadPlacements(ctx context.Context, id string) error {
	query := `SELECT nodes.node_id, nodes.state, nodes.claims_enabled, nodes.capabilities_json,
 nodes.max_service_slots, node_tags.tag FROM nodes LEFT JOIN node_tags ON node_tags.node_id=nodes.node_id`
	var args []any
	if id != "" {
		query += " WHERE nodes.node_id=?"
		args = append(args, id)
	}
	query += " ORDER BY nodes.node_id, node_tags.tag"
	rows, err := r.q.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	loaded := make(map[string]placementRead)
	var ids []string
	for rows.Next() {
		var value placementFacts
		var caps []byte
		var tag sql.NullString
		if err = rows.Scan(&value.id, &value.state, &value.claimsEnabled, &caps, &value.maxServiceSlots, &tag); err != nil {
			break
		}
		prior, ok := loaded[value.id]
		if !ok {
			prior.value = value
			prior.err = json.Unmarshal(caps, &prior.value.capabilities)
			ids = append(ids, value.id)
		}
		if tag.Valid {
			prior.value.tags = append(prior.value.tags, tag.String)
		}
		loaded[value.id] = prior
	}
	if err == nil {
		err = rows.Err()
	}
	err = errors.Join(err, rows.Close())
	if err != nil {
		return err
	}
	for key, value := range loaded {
		if _, ok := r.placements[key]; !ok {
			r.placements[key] = value
		}
		if _, ok := r.reads.nodeStates[key]; !ok {
			r.reads.nodeStates[key] = stateRead{value: value.value.state}
		}
	}
	if id == "" {
		r.nodeIDs = ids
	}
	return nil
}
func (r *databaseReads) placement(ctx context.Context, id string) (placementFacts, error) {
	value, ok := r.placements[id]
	if !ok {
		if !r.nodesLoaded {
			value.err = r.loadPlacements(ctx, id)
		} else {
			value.err = r.nodesErr
		}
		if value.err == nil {
			value, ok = r.placements[id]
			if !ok {
				value.err = sql.ErrNoRows
			}
		}
		r.placements[id] = value
	}
	return value.value, value.err
}
func (r *databaseReads) eligibleNodeIDs(ctx context.Context, tags []string) ([]string, error) {
	if !r.nodesLoaded {
		r.nodesLoaded = true
		r.nodesErr = r.loadPlacements(ctx, "")
	}
	if r.nodesErr != nil {
		return nil, r.nodesErr
	}
	var eligible []string
	for _, id := range r.nodeIDs {
		matches := true
		for _, tag := range tags {
			found := false
			for _, present := range r.placements[id].value.tags {
				if present == tag {
					found = true
					break
				}
			}
			if !found {
				matches = false
				break
			}
		}
		if matches {
			eligible = append(eligible, id)
		}
	}
	return eligible, nil
}

// Capacity decisions load service occupancy once for the page, independent of
// how many candidates are full. Placement alone never executes this scan.
func (r *databaseReads) serviceOccupancy(ctx context.Context, id string) (int, error) {
	if !r.occupancyLoaded {
		r.occupancyLoaded = true
		rows, err := r.q.QueryContext(ctx, `SELECT nodes.node_id, (SELECT COUNT(*) FROM service_jobs occupied_service
 JOIN jobs occupied_job ON occupied_job.job_id=occupied_service.job_id
 WHERE occupied_service.bound_node_id=nodes.node_id
 AND ((occupied_job.state=? AND occupied_service.desired_state=?) OR occupied_job.state IN (?, ?, ?, ?, ?)))
 FROM nodes`, contract.JobQueued, contract.ServiceDesiredRunning,
			contract.JobClaimed, contract.JobRunning, contract.JobStopping, contract.JobRemovalPending, contract.JobAgentCleaned)
		if err == nil {
			for rows.Next() {
				var nodeID string
				var count int
				if err = rows.Scan(&nodeID, &count); err != nil {
					break
				}
				r.occupancy[nodeID] = count
			}
			if err == nil {
				err = rows.Err()
			}
			err = errors.Join(err, rows.Close())
		}
		r.occupancyErr = err
	}
	return r.occupancy[id], r.occupancyErr
}
func (r *databaseReads) nodeRoot(ctx context.Context, id string) (string, error) {
	value, ok := r.reads.nodeRoots[id]
	if !ok {
		value.err = r.q.QueryRowContext(ctx, `SELECT root_instance_id FROM nodes WHERE node_id=?`, id).Scan(&value.value)
		r.reads.nodeRoots[id] = value
	}
	return value.value, value.err
}
func (r *databaseReads) nodeState(ctx context.Context, id string) (contract.NodeState, error) {
	value, ok := r.reads.nodeStates[id]
	if !ok {
		value.err = r.q.QueryRowContext(ctx, `SELECT state FROM nodes WHERE node_id=?`, id).Scan(&value.value)
		r.reads.nodeStates[id] = value
	}
	return value.value, value.err
}
func (r *databaseReads) requireStorage(ctx context.Context, c Computer, action string) error {
	return requireCurrentComputerStorage(ctx, r.q, c, action)
}
func (r *databaseReads) backup(ctx context.Context, id string) (Backup, error) {
	return readBackup(ctx, r.q, id)
}
func (r *databaseReads) availableBackup(ctx context.Context, id string) (Backup, BackupCopy, error) {
	return readAvailableBackupCopy(ctx, r.q, id)
}
func (r *databaseReads) intents(ctx context.Context, id string, after int64, limit int) ([]ComputerIntent, error) {
	return queryComputerIntents(ctx, r.q, id, after, limit)
}
func (r *databaseReads) person(ctx context.Context, fabricID, userID string) (AuthenticatedPerson, bool, error) {
	var person AuthenticatedPerson
	var lastSeen int64
	err := r.q.QueryRowContext(ctx, `SELECT fabric_id, user_id, last_device_id, last_seen_ns
		FROM authenticated_people WHERE fabric_id=? AND user_id=?`, fabricID, userID).
		Scan(&person.FabricID, &person.UserID, &person.DeviceID, &lastSeen)
	if errors.Is(err, sql.ErrNoRows) {
		return AuthenticatedPerson{}, false, nil
	}
	if err != nil {
		return AuthenticatedPerson{}, false, internalError(err, "read authenticated person")
	}
	person.SeenAt = time.Unix(0, lastSeen).UTC()
	return person, true, nil
}
func (r *databaseReads) adminPolicy(ctx context.Context) (AdminPolicy, error) {
	return readAdminPolicy(ctx, r.q)
}
func (r *databaseReads) adminPolicyRevision(ctx context.Context) (int64, error) {
	var revision int64
	if err := r.q.QueryRowContext(ctx, `SELECT revision FROM admin_policy WHERE singleton=1`).Scan(&revision); err != nil {
		return 0, internalError(err, "read admin policy revision")
	}
	return revision, nil
}
func (r *databaseReads) currentAdmin(ctx context.Context, identity fabric.Identity) error {
	return requireCurrentAdmin(ctx, r.q, identity)
}
func (r *databaseReads) adminAuditPage(ctx context.Context, afterRevision int64, limit int) (AdminPolicyAuditList, error) {
	page := AdminPolicyAuditList{Entries: []AdminPolicyAudit{}}
	rows, err := r.q.QueryContext(ctx, `SELECT revision, operation, actor_kind, actor_fabric_id,
		actor_user_id, actor_device_id, subject_fabric_id, subject_user_id, created_ns FROM admin_policy_audit
		WHERE revision>? ORDER BY revision LIMIT ?`, afterRevision, limit+1)
	if err != nil {
		return AdminPolicyAuditList{}, internalError(err, "list admin policy audit")
	}
	defer rows.Close()
	for rows.Next() {
		var entry AdminPolicyAudit
		var createdNS int64
		if err := rows.Scan(&entry.Revision, &entry.Operation, &entry.ActorKind,
			&entry.ActorFabricID, &entry.ActorUserID, &entry.ActorDeviceID,
			&entry.SubjectFabricID, &entry.SubjectUserID, &createdNS); err != nil {
			return AdminPolicyAuditList{}, internalError(err, "scan admin policy audit")
		}
		entry.CreatedAt = time.Unix(0, createdNS).UTC()
		page.Entries = append(page.Entries, entry)
	}
	if err := rows.Err(); err != nil {
		return AdminPolicyAuditList{}, internalError(err, "read admin policy audit")
	}
	if len(page.Entries) > limit {
		page.Entries = page.Entries[:limit]
		page.NextCursor = encodeAdminAuditCursor(page.Entries[len(page.Entries)-1].Revision)
	}
	return page, nil
}
func (r *databaseReads) projectStatus(ctx context.Context, job Job) (Job, error) {
	return projectJobStatus(ctx, r, job)
}

// The read-only pool is the admission cap. The measured target is independent
// of the hard transaction limit, which stays below the 250ms scrub checkpoint.
const readSnapshotLimit = 12
const readSnapshotBudget = 100 * time.Millisecond
const readSnapshotHardLimit = 200 * time.Millisecond

// Leave 40% of the hard hold limit for finishing a row and rolling back.
// This is elapsed monotonic time, independent of the pinned domain clock.
const readSnapshotPageSoftLimit = readSnapshotHardLimit * 3 / 5

// Tests may lower the cutoff before running the suite; production keeps the
// named default. A context override permits deterministic per-request probes.
var readSnapshotPageCutoff = readSnapshotPageSoftLimit

type readPageCutoffContextKey struct{}

func (r *databaseReads) pageCutoffReached() bool {
	return !r.pageDeadline.IsZero() && !time.Now().Before(r.pageDeadline)
}

type readSnapshotContextKey struct{}

var errNestedReadSnapshot = &Error{Code: contract.ErrorInternal, Message: "nested read snapshot acquisition", notRetryable: true}

func snapshotUnavailable(reason string, cause error) error {
	return &Error{Code: contract.ErrorUnavailable, Message: "read snapshot unavailable", Cause: cause,
		Details: map[string]any{"reason": reason}}
}

func (s *Store) readSnapshotOverrunCount() uint64 { return s.readSnapshotOverruns.Load() }

// Count every person-observation fallback write, but emit at most one
// diagnostic per second per Store so a busy snapshot pool cannot flood logs.
func (s *Store) recordPersonFallbackWrite() {
	s.personObservationFallbacks.Add(1)
	now := time.Now().UnixNano()
	prior := s.personObservationFallbackLastLog.Load()
	if now-prior < int64(time.Second) || !s.personObservationFallbackLastLog.CompareAndSwap(prior, now) {
		return
	}
	if s.logf != nil {
		s.logf("event=l1_person_observation_fallback count=%d", s.personObservationFallbacks.Load())
	}
}

func (s *Store) personObservationFallbackCount() uint64 {
	return s.personObservationFallbacks.Load()
}

// withReadSnapshot anchors SQLite before sampling the clock. The derived
// context marks nested acquisition as a programming error. Independent reads
// may share a parent context; pool admission remains bounded at 200 ms.
func (s *Store) withReadSnapshot(ctx context.Context, caller *serviceActionActor, use func(context.Context, readModel) error) (err error) {
	if ctx.Value(readSnapshotContextKey{}) != nil {
		return errNestedReadSnapshot
	}
	admission, cancelAdmission := context.WithTimeout(ctx, readSnapshotHardLimit)
	conn, err := s.readDB.Conn(admission)
	cancelAdmission()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return snapshotUnavailable("read_snapshot_admission_expired", err)
		}
		return err
	}
	defer conn.Close()
	// Checkout is not part of the transaction hold limit. ReadOnly selects a
	// deferred BEGIN in modernc; the pool's connection pragma enforces no writes.
	ctx, cancel := context.WithTimeout(context.WithValue(ctx, readSnapshotContextKey{}, true), readSnapshotHardLimit)
	defer cancel()
	defer func() {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			err = snapshotUnavailable("read_snapshot_expired", ctx.Err())
		}
	}()
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	anchored := time.Now()
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			err = errors.Join(err, fmt.Errorf("rollback snapshot: %w", rollbackErr))
		}
		elapsed := time.Since(anchored)
		if elapsed > readSnapshotBudget {
			s.recordReadSnapshotOverrun(elapsed)
		}
	}()
	var anchor int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_schema").Scan(&anchor); err != nil {
		return err
	}
	reads := newDatabaseReads(tx, canonicalTime(s.clock.Now()), caller)
	cutoff := readSnapshotPageCutoff
	if override, ok := ctx.Value(readPageCutoffContextKey{}).(time.Duration); ok {
		cutoff = override
	}
	reads.pageDeadline = anchored.Add(cutoff)
	if err = use(ctx, reads); err != nil {
		return err
	}
	return ctx.Err()
}

// Count every overrun, but emit at most one diagnostic per second per Store.
func (s *Store) recordReadSnapshotOverrun(elapsed time.Duration) {
	s.readSnapshotOverruns.Add(1)
	now := time.Now().UnixNano()
	prior := s.readSnapshotLastLog.Load()
	if now-prior < int64(time.Second) || !s.readSnapshotLastLog.CompareAndSwap(prior, now) {
		return
	}
	if s.logf != nil {
		s.logf("event=l1_read_snapshot_target_overrun elapsed_ms=%d target_ms=%d count=%d", elapsed.Milliseconds(), readSnapshotBudget.Milliseconds(), s.readSnapshotOverruns.Load())
	}
}

var _ readModel = (*databaseReads)(nil)

// Component selection serves internal status decisions and complete public views.
// Every component uses the same pinned clock and read vocabulary.
type jobProjectionParts uint8

const (
	projectJobStatusPart jobProjectionParts = 1 << iota
	projectJobOperatorPart
	projectJobAttemptsPart
	projectJobAll = projectJobStatusPart | projectJobOperatorPart | projectJobAttemptsPart
)

func projectJobWithReads(ctx context.Context, reads readModel, job Job, parts jobProjectionParts) (Job, error) {
	var err error
	if parts&projectJobStatusPart != 0 {
		job, err = reads.projectStatus(ctx, job)
		if err != nil {
			return Job{}, err
		}
	}
	if parts&projectJobOperatorPart != 0 {
		job, err = projectServiceOperatorFacts(ctx, reads, job, reads.caller())
		if err != nil {
			return Job{}, err
		}
	}
	if parts&projectJobAttemptsPart != 0 {
		job.Attempts, err = reads.attempts(ctx, job.JobID)
		if err != nil {
			return Job{}, err
		}
	}
	return job, nil
}

// writeTransaction keeps mutation authority and one pinned clock. Each
// decision gets a new read set, so earlier decisions cannot hide later writes.
type writeTransaction struct {
	tx    *sql.Tx
	at    time.Time
	actor *serviceActionActor
}

func (s *Store) beginWriteTransaction(ctx context.Context, caller *serviceActionActor) (*writeTransaction, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &writeTransaction{tx: tx, at: canonicalTime(s.clock.Now()), actor: caller}, nil
}
func (w *writeTransaction) decisionReads() readModel {
	reads := transactionReads(w.tx, w.at).(*databaseReads)
	reads.actor = w.actor
	return reads
}
func (w *writeTransaction) commit() error   { return w.tx.Commit() }
func (w *writeTransaction) rollback() error { return w.tx.Rollback() }

// snapshotReadError keeps typed read-snapshot refusals (unavailable with a
// read_snapshot_* reason) intact for the caller and labels only raw errors as
// internal, so a busy read pool answers 503 rather than a scrubbed 500.
func snapshotReadError(err error, message string) error {
	var typed *Error
	if errors.As(err, &typed) {
		return err
	}
	return internalError(err, message)
}
