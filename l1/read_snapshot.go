package l1

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// readModel is the internal vocabulary shared by projections and decisions.
// It grants neither SQL access nor mutation authority. Writes adapt their own
// transaction, after replay checks, so decisions remain inside the write.
type readModel interface {
	now() time.Time
	caller() *serviceActionActor
	job(context.Context, string) (Job, error)
	attempts(context.Context, string) ([]Attempt, error)
	node(context.Context, string) (Node, error)
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
}

// databaseReads is private SQL plumbing; callers see only readModel. Legacy
// view adapters deliberately keep their existing acquisition and timing until
// #748-#751. A write gets a fresh memo at each decision, never across writes.
type databaseReads struct {
	q     queryer
	at    time.Time
	actor *serviceActionActor
	reads *serviceOperatorReads
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
	return &databaseReads{q: q, at: at, actor: actor, reads: newServiceOperatorReads(q)}
}
func transactionReads(q queryer) readModel           { return newDatabaseReads(q, time.Time{}, nil) }
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
func (r *databaseReads) projectStatus(ctx context.Context, job Job) (Job, error) {
	return projectJobStatus(ctx, r, job)
}

// Twelve readers leave four of the existing sixteen connections for writes and
// cleanup. The transaction deadline is below the 250ms scrubbing checkpoint
// wait. Admission itself is cancellable and does not start that budget.
const readSnapshotLimit = 12
const readSnapshotBudget = 100 * time.Millisecond

type readSnapshotContextKey struct{}

var errNestedReadSnapshot = errors.New("l1: nested read snapshot refused")

// withReadSnapshot anchors SQLite before sampling the clock, after admission
// and pool checkout. The derived context must be used for all snapshot work.
// query_only enforces read-only (modernc's ReadOnly option only selects BEGIN).
// The callback must finish before encoding, external calls or long polling.
func (s *Store) withReadSnapshot(ctx context.Context, caller *serviceActionActor, use func(context.Context, readModel) error) (err error) {
	if ctx.Value(readSnapshotContextKey{}) != nil {
		return errNestedReadSnapshot
	}
	select {
	case s.readSnapshots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-s.readSnapshots }()
	ctx, cancel := context.WithTimeout(context.WithValue(ctx, readSnapshotContextKey{}, true), readSnapshotBudget)
	defer cancel()
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	// Always reset the connection-local pragma before returning to the shared
	// pool. A failed cleanup discards the connection instead of poisoning writes.
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), readSnapshotBudget)
		defer cancel()
		if _, resetErr := conn.ExecContext(cleanup, "PRAGMA query_only=OFF"); resetErr != nil {
			if errors.Is(resetErr, sql.ErrConnDone) {
				// Already discarded by rollback or cancellation.
				return
			}
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			err = errors.Join(err, fmt.Errorf("reset snapshot connection: %w", resetErr))
		}
	}()
	if _, err = conn.ExecContext(ctx, "PRAGMA query_only=ON"); err != nil {
		return err
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			// A failed rollback must never return a potentially active transaction
			// to the writer pool, even if resetting query_only would succeed.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			err = errors.Join(err, fmt.Errorf("rollback snapshot: %w", rollbackErr))
		}
	}()
	var anchor int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_schema").Scan(&anchor); err != nil {
		return err
	}
	reads := newDatabaseReads(tx, canonicalTime(s.clock.Now()), caller)
	if err = use(ctx, reads); err != nil {
		return err
	}
	return ctx.Err()
}

var _ readModel = (*databaseReads)(nil)

// Component selection preserves today's view stitching and refusal order. The
// subsequent tickets request all components inside one snapshot; no route is
// migrated here. Every component uses this same projector and read vocabulary.
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
		job, err = projectServiceOperatorFactsWithReads(ctx, reads, job, reads.caller())
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

// writeTransaction is the other door. Mutation code keeps SQL authority here;
// its projections receive only the embedded readModel. Existing writes retain
// their acquisition/timing through transactionReads until the ratchet shrinks.
type writeTransaction struct {
	tx *sql.Tx
	*databaseReads
}

func (s *Store) beginWriteTransaction(ctx context.Context, caller *serviceActionActor) (*writeTransaction, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &writeTransaction{tx: tx, databaseReads: newDatabaseReads(tx, canonicalTime(s.clock.Now()), caller)}, nil
}
func (w *writeTransaction) commit() error   { return w.tx.Commit() }
func (w *writeTransaction) rollback() error { return w.tx.Rollback() }

var _ readModel = (*writeTransaction)(nil)
