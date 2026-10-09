package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

type snapshotClock struct {
	calls atomic.Int32
	at    atomic.Int64
}

func (c *snapshotClock) Now() time.Time { c.calls.Add(1); return time.Unix(0, c.at.Load()).UTC() }
func snapshotStore(t *testing.T) (*Store, *snapshotClock) {
	t.Helper()
	clock := &snapshotClock{}
	clock.at.Store(time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC).UnixNano())
	store, err := OpenStore(filepath.Join(t.TempDir(), "snapshot.sqlite"), StoreOptions{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	clock.calls.Store(0)
	return store, clock
}
func TestReadSnapshotClockAdmissionAndAnchor(t *testing.T) {
	s, clock := snapshotStore(t)
	// Admission is held without using a connection; the read budget starts only
	// after this wait, and the clock must not be sampled while waiting.
	var held []*sql.Conn
	for range readSnapshotLimit {
		conn, err := s.readDB.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, conn)
	}
	defer func() {
		for _, conn := range held {
			_ = conn.Close()
		}
	}()
	actor := &serviceActionActor{Identity: fabric.Identity{NodeID: "caller"}}
	entered := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(entered)
		done <- s.withReadSnapshot(t.Context(), actor, func(ctx context.Context, r readModel) error {
			at := r.now()
			if at.UnixNano() != clock.at.Load() || r.caller() != actor {
				return errors.New("wrong admitted clock or caller")
			}
			clock.at.Add(int64(time.Hour))
			if r.now() != at || clock.calls.Load() != 1 {
				return errors.New("clock was not pinned")
			}
			return nil
		})
	}()
	<-entered
	// Check after the waiter has started. Since every token is held, no clock
	// read is possible regardless of goroutine scheduling.
	if clock.calls.Load() != 0 {
		t.Fatal("clock sampled before admission")
	}
	clock.at.Add(int64(time.Minute))
	_ = held[0].Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

}
func TestReadSnapshotAnchoredBeforeFirstDomainRead(t *testing.T) {
	s, _ := snapshotStore(t)
	err := s.withReadSnapshot(t.Context(), nil, func(ctx context.Context, r readModel) error {
		// No domain read has run. A writer changes the schema after acquisition;
		// the anchored snapshot must still see the old sqlite_schema.
		if _, err := s.db.ExecContext(ctx, "CREATE TABLE after_anchor(value TEXT)"); err != nil {
			return err
		}
		var count int
		if err := r.(*databaseReads).q.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_schema WHERE name='after_anchor'").Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return errors.New("snapshot began at first domain read")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
func TestReadSnapshotMemoAndReadOnlyPool(t *testing.T) {
	s, _ := snapshotStore(t)
	// Missing-node errors are memoized just like node capabilities, tags and
	// occupancy. A production-called getNode read populates exactly one entry.
	err := s.withReadSnapshot(t.Context(), nil, func(ctx context.Context, r readModel) error {
		for range 2 {
			if _, err := r.node(ctx, "absent"); !errors.Is(err, sql.ErrNoRows) {
				return errors.New("wrong missing node result")
			}
		}
		impl := r.(*databaseReads)
		if len(impl.reads.nodes) != 1 {
			return errors.New("memo not shared")
		}
		// The SQL seam is private to implementation/tests: SQLite itself refuses a
		// write even if accidental SQL is introduced behind the read interface.
		tx := impl.q.(*sql.Tx)
		if _, err := tx.ExecContext(ctx, "CREATE TABLE forbidden(value TEXT)"); err == nil {
			return errors.New("snapshot accepted write")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Writers remain on the independent main pool.
	s.db.SetMaxOpenConns(1)
	if _, err := s.db.ExecContext(t.Context(), "CREATE TABLE reset_ok(value TEXT)"); err != nil {
		t.Fatalf("query_only leaked to writer: %v", err)
	}
	if err = s.withReadSnapshot(t.Context(), nil, func(ctx context.Context, r readModel) error {
		if len(r.(*databaseReads).reads.nodes) != 0 {
			return errors.New("memo crossed snapshots")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

type snapshotQueryCounter struct {
	queryer
	count int
}

func (q *snapshotQueryCounter) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	q.count++
	return q.queryer.QueryRowContext(ctx, query, args...)
}
func (q *snapshotQueryCounter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	q.count++
	return q.queryer.QueryContext(ctx, query, args...)
}
func TestReadSnapshotNodeFactsMemo(t *testing.T) {
	s, clock := snapshotStore(t)
	node, err := s.RegisterNode(t.Context(), fabric.Identity{NodeID: "fabric-node"}, contract.NodeRegistration{
		NodeID: "node", BootSessionID: "boot", RootInstanceID: "root", OS: "linux", Architecture: "amd64", AgentVersion: "test", Capabilities: map[string]bool{"kind:process": true}, CapabilityRevision: 1, CapabilityObservedAt: clock.Now(), MissingCapabilities: []string{},
	}, NodePolicy{Tags: []string{"tag"}, MaxOneshotSlots: 1, MaxServiceSlots: 1}, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.withReadSnapshot(t.Context(), nil, func(ctx context.Context, r readModel) error {
		impl := r.(*databaseReads)
		counter := &snapshotQueryCounter{queryer: impl.q}
		impl.q = counter
		first, err := r.node(ctx, node.NodeID)
		if err != nil {
			return err
		}
		if !first.Capabilities["kind:process"] || !reflect.DeepEqual(first.AuthoritativeTags, []string{"tag"}) || first.ServiceOccupancy != 0 || first.OneshotOccupancy != 0 {
			return errors.New("missing node facts")
		}
		reads := counter.count
		second, err := r.node(ctx, node.NodeID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(first, second) || counter.count != reads {
			return errors.New("node facts queried again")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
func TestReadSnapshotNestedRefused(t *testing.T) {
	s, _ := snapshotStore(t)
	if err := s.withReadSnapshot(t.Context(), nil, func(ctx context.Context, r readModel) error {
		if err := s.withReadSnapshot(ctx, nil, func(context.Context, readModel) error { t.Error("nested callback ran"); return nil }); !errors.Is(err, errNestedReadSnapshot) {
			return errors.New("nested snapshot not refused")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
func TestReadSnapshotCapUnderContention(t *testing.T) {
	s, _ := snapshotStore(t)
	release := make(chan struct{})
	entered := make(chan struct{}, readSnapshotLimit)
	var active, peak atomic.Int32
	var holders sync.WaitGroup
	results := make(chan error, readSnapshotLimit)
	for range readSnapshotLimit {
		holders.Add(1)
		go func() {
			defer holders.Done()
			results <- s.withReadSnapshot(context.WithValue(t.Context(), readSnapshotContextKey{}, nil), nil, func(ctx context.Context, r readModel) error {
				count := active.Add(1)
				defer active.Add(-1)
				for old := peak.Load(); count > old; old = peak.Load() {
					if peak.CompareAndSwap(old, count) {
						break
					}
				}
				entered <- struct{}{}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
		}()
	}
	for range readSnapshotLimit {
		select {
		case <-entered:
		case <-time.After(time.Second):
			close(release)
			holders.Wait()
			t.Fatal("cap holders did not enter")
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.withReadSnapshot(ctx, nil, func(context.Context, readModel) error { t.Error("over-cap read admitted"); return nil }); !errors.Is(err, context.Canceled) {
		t.Errorf("admission cancellation: %v", err)
	}

	// Four additional readers contend with all twelve occupied admissions. Each
	// must time out before its callback; cancellation alone without contention
	// would not demonstrate that the cap is enforced.
	var contenders sync.WaitGroup
	contended := make(chan error, 4)
	for range 4 {
		contenders.Add(1)
		go func() {
			defer contenders.Done()
			ctx, cancel := context.WithTimeout(t.Context(), readSnapshotBudget/4)
			defer cancel()
			contended <- s.withReadSnapshot(ctx, nil, func(context.Context, readModel) error {
				return errors.New("over-cap callback admitted")
			})
		}()
	}
	contenders.Wait()
	close(contended)
	for err := range contended {
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("contended admission: %v", err)
		}
	}

	writer, cancel := context.WithTimeout(t.Context(), readSnapshotBudget/2)
	_, writeErr := s.db.ExecContext(writer, "CREATE TABLE writer_progress(value TEXT)")
	cancel()
	close(release)
	holders.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Error(err)
		}
	}
	if writeErr != nil {
		t.Fatalf("writer had no reserved capacity: %v", writeErr)
	}
	if peak.Load() != readSnapshotLimit || s.readDB.Stats().InUse != 0 {
		t.Fatalf("peak=%d admission remaining=%d", peak.Load(), s.readDB.Stats().InUse)
	}
}
func TestReadSnapshotBudgetReleasesSQLiteReadLock(t *testing.T) {
	s, _ := snapshotStore(t)
	err := s.withReadSnapshot(t.Context(), nil, func(ctx context.Context, r readModel) error {
		<-ctx.Done()
		if _, err := r.node(ctx, "absent"); err == nil {
			return errors.New("expired snapshot still usable")
		}
		return ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline=%v", err)
	}
	if s.readDB.Stats().InUse != 0 {
		t.Fatal("admission leaked")
	}
	if _, err = s.db.ExecContext(t.Context(), "CREATE TABLE after_deadline(value TEXT)"); err != nil {
		t.Fatal(err)
	}
}
func TestReadSnapshotWriteDoorSharesUncommittedReads(t *testing.T) {
	s, _ := snapshotStore(t)
	write, err := s.beginWriteTransaction(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer write.rollback()
	same := write.decisionReads()
	// This node is inserted without committing. Domain reads must use this exact
	// write transaction; a pool read would report not found.
	if _, err = write.tx.ExecContext(t.Context(), "INSERT INTO nodes(node_id, identity_node_id, boot_session_id, os, architecture, agent_version, capabilities_json, missing_capabilities_json, state, max_oneshot_slots, max_service_slots, last_heartbeat_ns) VALUES('inside','fabric-inside','boot','linux','amd64','test','{}','[]','alive',1,1,0)"); err != nil {
		t.Fatal(err)
	}
	state, err := same.nodeState(t.Context(), "inside")
	if err != nil || state != "alive" {
		t.Fatalf("write reads state=%s err=%v", state, err)
	}
}

func TestReadSnapshotSharedJobProjectorMatchesOriginalWire(t *testing.T) {
	for _, state := range jobProjectionCases {
		t.Run(state, func(t *testing.T) {
			h, _, job, claim := jobProjectionFixture(t, state)
			actor := &serviceActionActor{Identity: fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}}, ClientPrincipalTag: DefaultClientPrincipalTag}
			var projected Job
			err := h.store.withReadSnapshot(t.Context(), actor, func(ctx context.Context, r readModel) error {
				current, err := r.job(ctx, job.JobID)
				if err != nil {
					return err
				}
				projected, err = projectJobWithReads(ctx, r, current, projectJobAll)
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(redactJob(projected))
			if err != nil {
				t.Fatal(err)
			}
			got := normalizedJobProjection(t, raw, job, claim)
			want, err := os.ReadFile(filepath.Join("testdata", "job-projection-"+state+".json"))
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != string(want) {
				t.Fatalf("shared projector changed original wire\ngot:\n%s\nwant:\n%s", got, want)
			}
		})
	}
}

func TestReadSnapshotClockAfterPoolCheckout(t *testing.T) {
	s, clock := snapshotStore(t)
	var held []*sql.Conn
	for range s.readDB.Stats().MaxOpenConnections {
		conn, err := s.readDB.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, conn)
	}
	defer func() {
		for _, conn := range held {
			_ = conn.Close()
		}
	}()
	waiting := s.readDB.Stats().WaitCount
	done := make(chan error, 1)
	go func() {
		done <- s.withReadSnapshot(t.Context(), nil, func(ctx context.Context, r readModel) error {
			if r.now().UnixNano() != clock.at.Load() || clock.calls.Load() != 1 {
				return errors.New("clock sampled before pool checkout")
			}
			time.Sleep(125 * time.Millisecond)
			return nil
		})
	}()
	deadline := time.Now().Add(time.Second)
	for s.readDB.Stats().WaitCount == waiting && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.readDB.Stats().WaitCount == waiting {
		t.Fatal("snapshot did not wait for pool")
	}
	if clock.calls.Load() != 0 {
		t.Fatal("clock sampled while waiting for connection")
	}
	time.Sleep(125 * time.Millisecond)
	clock.at.Add(int64(time.Minute))
	_ = held[0].Close()
	held = held[1:]
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestReadSnapshotRollbackFailureDiscardsConnection(t *testing.T) {
	s, _ := snapshotStore(t)
	err := s.withReadSnapshot(t.Context(), nil, func(ctx context.Context, r readModel) error {
		// End SQLite's transaction beneath database/sql's transaction object. Its
		// deferred Rollback will fail, exercising the real driver cleanup path.
		_, err := r.(*databaseReads).q.(*sql.Tx).ExecContext(ctx, "ROLLBACK")
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "rollback snapshot") {
		t.Fatalf("rollback failure was ignored: %v", err)
	}
	if s.readDB.Stats().OpenConnections != 0 {
		t.Fatalf("failed rollback returned connection to pool: %+v", s.readDB.Stats())
	}
	if _, err = s.db.ExecContext(t.Context(), "CREATE TABLE after_failed_rollback(value TEXT)"); err != nil {
		t.Fatal(err)
	}
}
