package l1

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestServiceBindingProofReadsSnapshotWhileWriterHoldsLock(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "binding.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	node, err := store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, contract.NodeRegistration{
		NodeID: "node", BootSessionID: "boot", OS: "linux", Architecture: "arm64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true},
	}, NodePolicy{MaxServiceSlots: 1}, true)
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := store.CreateJob(t.Context(), operatorServiceSpec("binding-snapshot", nil))
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimJob(t.Context(), "agent", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil || claim.Job.JobID != job.JobID {
		t.Fatalf("claim service: claim=%+v err=%v", claim, err)
	}

	// Like the listing regressions (#695/#687), hold a real IMMEDIATE writer
	// throughout the proof. Both authority and binding reads must see committed
	// state without waiting for this writer to finish.
	writer, err := store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback()
	if _, err := writer.ExecContext(t.Context(), `UPDATE nodes SET boot_session_id='replacement' WHERE node_id=?`, node.NodeID); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(t.Context(), `UPDATE service_jobs SET bound_node_id=NULL WHERE job_id=?`, job.JobID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 700*time.Millisecond)
	defer cancel()
	bound, err := store.ProveServiceBinding(ctx, "agent", job.JobID, ServiceBindingProofRequest{
		NodeID: node.NodeID, BootSessionID: node.BootSessionID,
	})
	if err != nil || ctx.Err() != nil || !bound {
		t.Fatalf("proof blocked by writer or lost committed snapshot: bound=%t err=%v context=%v", bound, err, ctx.Err())
	}
}

// Wrap the real SQLite driver only in this test so the concurrent commit lands
// exactly between ProveServiceBinding's authority and binding reads. No mocked
// rows or production test hook substitutes for SQLite's WAL snapshot behavior.
type bindingSnapshotConnector struct {
	driver      driver.Driver
	dsn         string
	beforeQuery func(string) error
}

func (c bindingSnapshotConnector) Driver() driver.Driver { return c.driver }
func (c bindingSnapshotConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return bindingSnapshotConn{Conn: conn, beforeQuery: c.beforeQuery}, nil
}

type bindingSnapshotConn struct {
	driver.Conn
	beforeQuery func(string) error
}

func (c bindingSnapshotConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}
func (c bindingSnapshotConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := c.beforeQuery(query); err != nil {
		return nil, err
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func TestServiceBindingProofKeepsOneSnapshotAcrossConcurrentCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "binding.sqlite")
	store, err := OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	node, err := store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, contract.NodeRegistration{
		NodeID: "node", BootSessionID: "boot", OS: "linux", Architecture: "arm64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true},
	}, NodePolicy{MaxServiceSlots: 1}, true)
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := store.CreateJob(t.Context(), operatorServiceSpec("binding-snapshot", nil))
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimJob(t.Context(), "agent", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil || claim.Job.JobID != job.JobID {
		t.Fatalf("claim service: claim=%+v err=%v", claim, err)
	}
	writerDB := store.db
	sessionReads, commits := 0, 0
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	readerDB := sql.OpenDB(bindingSnapshotConnector{driver: writerDB.Driver(), dsn: sqliteDSN(path, sqliteBusyTimeout),
		beforeQuery: func(query string) error {
			if strings.Contains(query, "SELECT identity_node_id, boot_session_id FROM nodes") {
				sessionReads++
			}
			if !strings.Contains(query, "SELECT service_jobs.bound_node_id") {
				return nil
			}
			if sessionReads != 1 {
				return fmt.Errorf("binding read followed %d session reads, want 1", sessionReads)
			}
			writer, err := writerDB.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer writer.Rollback()
			if _, err := writer.ExecContext(ctx, `UPDATE nodes SET boot_session_id='replacement' WHERE node_id=?`, node.NodeID); err != nil {
				return err
			}
			if _, err := writer.ExecContext(ctx, `UPDATE service_jobs SET bound_node_id=NULL WHERE job_id=?`, job.JobID); err != nil {
				return err
			}
			if err := writer.Commit(); err != nil {
				return err
			}
			commits++
			return nil
		},
	})
	store.db = readerDB
	defer func() { store.db = writerDB; readerDB.Close() }()
	bound, err := store.ProveServiceBinding(ctx, "agent", job.JobID, ServiceBindingProofRequest{
		NodeID: node.NodeID, BootSessionID: node.BootSessionID,
	})
	if err != nil || !bound || sessionReads != 1 || commits != 1 {
		t.Fatalf("proof mixed snapshots across committed replacement: bound=%t err=%v sessionReads=%d commits=%d", bound, err, sessionReads, commits)
	}
	var boot string
	var binding sql.NullString
	if err := writerDB.QueryRowContext(ctx, `SELECT nodes.boot_session_id, service_jobs.bound_node_id
  FROM nodes CROSS JOIN service_jobs WHERE nodes.node_id=? AND service_jobs.job_id=?`, node.NodeID, job.JobID).Scan(&boot, &binding); err != nil || boot != "replacement" || binding.Valid {
		t.Fatalf("concurrent change was not committed: boot=%q binding=%+v err=%v", boot, binding, err)
	}
}
