package l1

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
)

// #598: a removal truncated the WAL on the main pool and retried until it
// succeeded, so a reader holding a snapshot held the removal response, and
// every writer queued behind the checkpoint waited out the pool's 5 s lock
// wait and failed.

const (
	removalWALSecret = "removal-secret-6e0a27"
	// removalWALPromptBound is what a removal, or a writer behind a removal's
	// checkpoint attempt, may take while a reader holds the WAL: about one
	// checkpoint handle wait, plus the write-lock backoff of removals released
	// together, with room for a slow runner. Removals that each make their own
	// attempt take about 1.5 s for concurrentRemovals.
	removalWALPromptBound = 1250 * time.Millisecond
	// removalWALReaderWatchdog releases the held reader only if the test
	// hangs, so a removal that waits for the reader fails on time rather
	// than blocking forever. The reader is otherwise released by the test.
	removalWALReaderWatchdog = 10 * time.Second
	// concurrentRemovals is enough removals that, taking one checkpoint wait
	// each in turn behind the write lock, they would exceed the bound. It is
	// kept small because removals released together from one checkpoint's
	// write-lock wait also retry the lock in step, about one per 100 ms of
	// SQLite's busy-handler backoff, which is not what this test measures.
	concurrentRemovals = 5
)

func TestServiceRemovalDefersWALTruncationPastAHeldReader(t *testing.T) {
	store, path := openRemovalWALStore(t)
	removals := createRemovalServices(t, store, 1)
	assertRemovalsDeferWALTruncation(t, store, path, removals)
}

// Removals under one held reader coalesce: a removal does not queue for the
// checkpoint handle behind another removal, and does not retry a checkpoint
// that has just been deferred, so none of them takes more than about one
// checkpoint wait.
func TestConcurrentServiceRemovalsUnderAHeldReaderEachReturnWithinOneWait(t *testing.T) {
	store, path := openRemovalWALStore(t)
	removals := createRemovalServices(t, store, concurrentRemovals)
	assertRemovalsDeferWALTruncation(t, store, path, removals)
}

func TestComputerRemovalDefersWALTruncationPastAHeldReader(t *testing.T) {
	store, path := openRemovalWALStore(t)
	spec := computerCapabilityJobSpec("computer:removal-held-reader")
	spec.Execution.SensitiveEnv = map[string]string{"COMPUTER_SECRET": removalWALSecret}
	computer, _, err := store.CreateComputer(context.Background(), CreateComputerRequest{
		Name: "held-reader", Spec: spec, Actor: "operator",
	})
	if err != nil {
		t.Fatal(err)
	}
	assertRemovalsDeferWALTruncation(t, store, path, []func() error{func() error {
		_, err := store.RemoveComputer(context.Background(), computer.ComputerID, ComputerRemoveRequest{
			ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
		})
		return err
	}})
}

func openRemovalWALStore(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "removal.sqlite")
	store, err := OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, path
}

func createRemovalServices(t *testing.T, store *Store, count int) []func() error {
	t.Helper()
	removals := make([]func() error, 0, count)
	for i := range count {
		spec := removalServiceSpec(fmt.Sprintf("removal-held-reader-%d", i), nil)
		spec.Execution.SensitiveEnv = map[string]string{"SERVICE_SECRET": removalWALSecret}
		job, _, err := store.CreateJob(context.Background(), spec)
		if err != nil {
			t.Fatal(err)
		}
		removals = append(removals, func() error {
			_, err := store.RemoveService(context.Background(), job.JobID)
			return err
		})
	}
	return removals
}

// assertRemovalsDeferWALTruncation runs removals concurrently while a reader
// holds a WAL snapshot. Every removal and an unrelated writer finish within
// about one checkpoint wait, and once the reader leaves the sweep truncates
// the WAL because the removals marked the truncation due.
func assertRemovalsDeferWALTruncation(t *testing.T, store *Store, path string, removals []func() error) {
	t.Helper()
	ctx := context.Background()
	// The first sweep in a process always truncates. After it the sweep owes
	// nothing, so the later sweep truncates only if a removal said so.
	if sweep, err := store.SweepScrubbedSecrets(ctx); err != nil || !sweep.TruncatedWAL {
		t.Fatalf("settling sweep = %+v err %v, want the WAL truncated", sweep, err)
	}
	if !bytes.Contains(databaseFiles(t, store), []byte(removalWALSecret)) {
		t.Fatal("fixture secret is not in the database files before removal")
	}

	reader, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	held, err := reader.Begin()
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := held.QueryRow(`SELECT COUNT(*) FROM jobs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { _ = held.Rollback() }) }
	defer release()
	watchdogFired := make(chan struct{})
	watchdog := time.AfterFunc(removalWALReaderWatchdog, func() {
		close(watchdogFired)
		release()
	})
	defer watchdog.Stop()

	writerDone := make(chan time.Duration, 1)
	go func() {
		time.Sleep(50 * time.Millisecond)
		began := time.Now()
		if _, _, err := store.CreateJobAs(ctx, validJobSpec("removal-held-reader-writer", nil), JobOrigin{}); err != nil {
			t.Errorf("writer behind the removals: %v", err)
		}
		writerDone <- time.Since(began)
	}()
	elapsed := make([]time.Duration, len(removals))
	errs := make([]error, len(removals))
	var wg sync.WaitGroup
	for i, remove := range removals {
		wg.Add(1)
		go func() {
			defer wg.Done()
			began := time.Now()
			errs[i] = remove()
			elapsed[i] = time.Since(began)
		}()
	}
	wg.Wait()
	waited := <-writerDone
	t.Logf("removals took %v; writer waited %v", elapsed, waited)
	select {
	case <-watchdogFired:
		t.Fatalf("the removals waited for the reader until the %v watchdog released it: %v",
			removalWALReaderWatchdog, elapsed)
	default:
	}
	for i := range removals {
		if errs[i] != nil {
			t.Fatalf("removal %d under a held reader: %v", i, errs[i])
		}
		if elapsed[i] > removalWALPromptBound {
			t.Fatalf("removal %d of %d under a held reader took %v (all: %v), want at most about one %v checkpoint wait",
				i, len(removals), elapsed[i], elapsed, secretWALCheckpointWait)
		}
	}
	if waited > removalWALPromptBound {
		t.Fatalf("a writer behind the removals waited %v, want at most about one %v checkpoint wait",
			waited, secretWALCheckpointWait)
	}
	if info, err := os.Stat(path + "-wal"); err != nil || info.Size() == 0 {
		t.Fatalf("WAL under a held reader = %v err %v, want its frames still there", info, err)
	}

	release()
	sweep, err := store.SweepScrubbedSecrets(ctx)
	if err != nil || !sweep.TruncatedWAL || sweep.TruncationDeferred {
		t.Fatalf("sweep after the reader left = %+v err %v, want the removals' truncation done", sweep, err)
	}
	if info, err := os.Stat(path + "-wal"); err != nil || info.Size() != 0 {
		t.Fatalf("WAL after the sweep = %v err %v, want it truncated", info, err)
	}
	if bytes.Contains(databaseFiles(t, store), []byte(removalWALSecret)) {
		t.Fatalf("database files still hold %q after the sweep", removalWALSecret)
	}
}

type sqliteCodeTestError int

func (e sqliteCodeTestError) Error() string { return fmt.Sprintf("sqlite code %d", int(e)) }
func (e sqliteCodeTestError) Code() int     { return int(e) }

// driverBusyError returns the error the SQLite driver itself gives a write
// transaction that finds the write lock held and has no wait.
func driverBusyError(t *testing.T, path string) error {
	t.Helper()
	holder, err := sql.Open("sqlite", sqliteDSN(path, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	contender, err := sql.Open("sqlite", sqliteDSN(path, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer contender.Close()
	held, err := holder.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer held.Rollback()
	tx, err := contender.Begin()
	if err == nil {
		_ = tx.Rollback()
		t.Fatal("a second write transaction began while the write lock was held")
	}
	return internalError(err, "begin L1 reconciliation")
}

func TestReconcileLockContentionRetriesNextTickAndL1KeepsServing(t *testing.T) {
	for _, other := range []error{
		internalError(sqliteInterruptTestError{}, "commit L1 reconciliation"),
		internalError(sqliteCodeTestError(11), "commit L1 reconciliation"),
		internalError(context.Canceled, "commit L1 reconciliation"),
	} {
		if sqliteLockContention(other) {
			t.Fatalf("sqliteLockContention(%v) = true, want only BUSY and LOCKED", other)
		}
	}
	for _, test := range []struct {
		name  string
		cause func(t *testing.T, path string) error
	}{
		{name: "driver SQLITE_BUSY", cause: driverBusyError},
		{name: "extended SQLITE_LOCKED", cause: func(*testing.T, string) error {
			return internalError(sqliteCodeTestError(6|1<<8), "begin L1 reconciliation")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "l1.sqlite")
			store, err := OpenStore(path, StoreOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			contention := test.cause(t, path)
			if !sqliteLockContention(contention) {
				t.Fatalf("sqliteLockContention(%v) = false", contention)
			}
			network := plain.NewNetwork()
			serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
			server, err := NewServer(serverFabric, store, ServerConfig{ReconcileInterval: time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			recovered := make(chan error, 1)
			calls := 0
			server.reconcile = func(ctx context.Context) (ReconcileResult, error) {
				calls++
				switch calls {
				case 1:
					return store.Reconcile(ctx)
				case 2:
					return ReconcileResult{}, contention
				default:
					result, err := store.Reconcile(ctx)
					select {
					case recovered <- err:
					default:
					}
					return result, err
				}
			}
			var logMu sync.Mutex
			var logs strings.Builder
			server.logf = func(format string, args ...any) {
				logMu.Lock()
				defer logMu.Unlock()
				fmt.Fprintf(&logs, format+"\n", args...)
			}
			listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- server.Serve(ctx, listener) }()
			select {
			case err := <-recovered:
				if err != nil {
					t.Fatalf("reconcile tick after the contention = %v", err)
				}
			case err := <-done:
				t.Fatalf("Serve stopped on lock contention: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("reconciliation did not resume after lock contention")
			}

			participant := network.NewFabric(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
			client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return participant.Dial(ctx, network, "wefty://control-plane")
			}}}
			defer client.CloseIdleConnections()
			status, _, body, err := doRequest(client, http.MethodGet, "/v1/jobs?class=service", nil)
			if err != nil || status != http.StatusOK {
				t.Fatalf("L1 after lock contention: status=%d err=%v body=%s", status, err, body)
			}
			logMu.Lock()
			logged := logs.String()
			logMu.Unlock()
			if !strings.Contains(logged, "event=l1_reconcile_sqlite_busy action=retry_next_tick") {
				t.Fatalf("lock contention log = %q", logged)
			}
			client.CloseIdleConnections()
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("Serve shutdown after lock contention = %v", err)
			}
		})
	}
}
