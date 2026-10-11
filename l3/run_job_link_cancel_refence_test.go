package l3

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"modernc.org/sqlite"
)

// Intercept the close of GetRun's last query, after its coherent read has
// completed and before canceledRunJob captures its final dispatch observation.
// A separate connection commits the competing writer; no production hook or
// scheduler timing is needed.
type cancelRefenceConnector struct {
	dsn       string
	afterRead func()
	once      sync.Once
}

func (c *cancelRefenceConnector) Driver() driver.Driver { return &sqlite.Driver{} }
func (c *cancelRefenceConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.Driver().Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &cancelRefenceConn{Conn: conn, connector: c}, nil
}

type cancelRefenceConn struct {
	driver.Conn
	connector *cancelRefenceConnector
}

func (c *cancelRefenceConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}
func (c *cancelRefenceConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, q, args)
	if err == nil && strings.Contains(q, "FROM gate_results WHERE") {
		return &cancelRefenceRows{Rows: rows, connector: c.connector}, nil
	}
	return rows, err
}

type cancelRefenceRows struct {
	driver.Rows
	connector *cancelRefenceConnector
}

func (r *cancelRefenceRows) Close() error {
	err := r.Rows.Close()
	if err == nil {
		r.connector.once.Do(r.connector.afterRead)
	}
	return err
}

type refenceCancelClient struct {
	JobClient
	calls int
}

func (c *refenceCancelClient) CancelJob(_ context.Context, id string) (l1.Job, error) {
	c.calls++
	return l1.Job{JobID: id, State: contract.JobFailed, Outcome: contract.JobOutcomeCanceled}, nil
}

func TestCanceledRunJobRefencesIdentityBeforeDelivery(t *testing.T) {
	s, path, _ := recoveryStore(t)
	run, _ := dispatchedRecoveryRun(t, s, "refence")
	ctx := context.Background()
	if err := s.requestRunCancellation(ctx, run.RunID, "test"); err != nil {
		t.Fatal(err)
	}
	pending, err := s.pendingRunCancellations(ctx, run.RunID)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%+v %v", pending, err)
	}
	changed := false
	connector := &cancelRefenceConnector{dsn: "file:" + path + "?_pragma=busy_timeout(5000)&_txlock=immediate", afterRead: func() {
		legacyMigrationShape(t, s, run.RunID, "new-identity")
		changed = true
	}}
	wrappedDB := sql.OpenDB(connector)
	defer wrappedDB.Close()
	wrappedStore := *s
	wrappedStore.db = wrappedDB
	client := &refenceCancelClient{JobClient: &recordingJobClient{}}
	r, err := NewReconciler(&wrappedStore, client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	err = r.cancelRunJob(ctx, pending[0])
	var internal *Error
	if !changed || !errors.As(err, &internal) || internal.Code != contract.ErrorInternal || internal.Cause == nil || !strings.Contains(internal.Cause.Error(), "dispatch identity changed before cancellation") {
		t.Fatalf("did not exercise final identity fence: changed=%t calls=%d err=%v internal=%+v", changed, client.calls, err, internal)
	}
	record, readErr := s.GetRun(ctx, run.RunID)
	delivery := linkTestCancellation(t, s, run.RunID)
	if readErr != nil || record.L1JobID != "new-job" || record.CancelStatus != "pending" || client.calls != 0 || delivery.completed.Valid || delivery.failures != 1 || delivery.retry <= s.clock.Now().UnixNano() {
		t.Fatalf("stale identity was cancelled or not deferred: run=%+v delivery=%+v calls=%d err=%v", record, delivery, client.calls, readErr)
	}
}

func TestCanceledRunJobWithoutCancellationClient(t *testing.T) {
	s, _, _ := recoveryStore(t)
	run, _ := dispatchedRecoveryRun(t, s, "no-cancel-client")
	ctx := context.Background()
	if err := s.requestRunCancellation(ctx, run.RunID, "test"); err != nil {
		t.Fatal(err)
	}
	pending, err := s.pendingRunCancellations(ctx, run.RunID)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%+v %v", pending, err)
	}
	r, err := NewReconciler(s, &recordingJobClient{}, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	// Call the production resolution seam as well as its delivery caller. The
	// caller has its own type assertion, so testing it alone misses this branch.
	_, _, ready, err := r.canceledRunJob(ctx, ctx, pending[0])
	var internal *Error
	if ready || !errors.As(err, &internal) || internal.Code != contract.ErrorInternal || internal.Cause == nil || !strings.Contains(internal.Cause.Error(), "L1 cancellation client is not configured") {
		t.Fatalf("unsupported client did not return internal error: ready=%t err=%v internal=%+v", ready, err, internal)
	}
	if err := r.cancelRunJob(ctx, pending[0]); err == nil {
		t.Fatal("delivery accepted a client without CancelJob")
	}
	record, err := s.GetRun(ctx, run.RunID)
	delivery := linkTestCancellation(t, s, run.RunID)
	if err != nil || record.CancelStatus != "pending" || delivery.completed.Valid || delivery.failures != 1 {
		t.Fatalf("unsupported client lost cancellation: run=%+v delivery=%+v err=%v", record, delivery, err)
	}
}
