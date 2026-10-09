package l1

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

type loadSamples struct {
	holds    []time.Duration
	rows     int
	retries  int
	walks    int
	walkTime time.Duration
}
type loadAnswer struct {
	keys []string
	next string
}

// A callback uses the real production projector and admission/hold door.
// pageDeadline identifies the door's anchor without a production timing hook.
// Measurements end after rollback and exclude only admission and encoding.
func loadSnapshot(s *Store, ctx context.Context, actor *serviceActionActor, use func(context.Context, readModel) (loadAnswer, error)) (loadAnswer, time.Duration, error) {
	var answer loadAnswer
	var anchor time.Time
	err := s.withReadSnapshot(ctx, actor, func(ctx context.Context, r readModel) error {
		cutoff := readSnapshotPageCutoff
		if override, ok := ctx.Value(readPageCutoffContextKey{}).(time.Duration); ok {
			cutoff = override
		}
		anchor = r.(*databaseReads).pageDeadline.Add(-cutoff)
		var err error
		answer, err = use(ctx, r)
		return err
	})
	if anchor.IsZero() {
		return answer, 0, err
	}
	return answer, time.Since(anchor), err
}

func writeLoadSecret(ctx context.Context, s *Store, n int) error {
	spec, err := json.Marshal(secretBearingOneShot(fmt.Sprintf("load-secret-%d", n)))
	if err != nil {
		return err
	}
	id := fmt.Sprintf("load-secret-%d", n)
	if _, err = s.db.ExecContext(ctx, `INSERT INTO jobs(job_id,dispatch_key,request_hash,spec_json,state,created_ns,updated_ns) VALUES(?,?, 'load',?,'queued',?,?)`, id, id, spec, n+1, n+1); err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `UPDATE jobs SET state='succeeded',updated_ns=updated_ns+1 WHERE job_id=?`, id)
	return err
}

// A reader deliberately keeps its callback parked until hygiene progresses.
// database/sql must release its transaction at the hard deadline even though
// callback code has not returned. Removing cancellation makes this fail after
// the generous watchdog, rather than accepting one lucky post-load checkpoint.
func TestReadSnapshotLoadHeldReaderSecretProgress(t *testing.T) {
	s, err := OpenStore(filepath.Join(t.TempDir(), "progress.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	go func() {
		done <- s.withReadSnapshot(t.Context(), nil, func(context.Context, readModel) error { close(entered); <-release; return nil })
	}()
	select {
	case <-entered:
	case <-time.After(12 * time.Second):
		t.Fatal("reader did not anchor")
	}
	if err = writeLoadSecret(t.Context(), s, 0); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()
	start := time.Now()
	attempts := 0
	for {
		attempts++
		sweep, err := s.SweepScrubbedSecrets(ctx)
		if err != nil {
			t.Fatalf("checkpoint progress: %v", err)
		}
		if sweep.TruncatedWAL {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("checkpoint progress stalled behind reader: %d attempts", attempts)
		}
	}
	t.Logf("parked-reader checkpoint attempts=%d elapsed=%s hold_limit=%s checkpoint_wait=%s", attempts, time.Since(start), readSnapshotHardLimit, secretWALCheckpointWait)
	unblock()
	select {
	case err = <-done:
	case <-time.After(12 * time.Second):
		t.Fatal("reader did not finish")
	}
	if errorCode(err) != contract.ErrorUnavailable {
		t.Fatalf("parked read=%v", err)
	}
}

// Four paging workers, one disk writer, and sequential secret writes/sweeps.
// The default verdict is membership, no deadlock, and repeated WAL progress;
// speed is diagnostic unless explicitly opted in.
func TestReadSnapshotSustainedLoad(t *testing.T) {
	h, node, computer, backup, _ := publishedBackupForStorageCopy(t, 2)
	h.stopServer()
	export, _ := beginCustodyExport(t, h, node, computer, backup, "load")
	seedLifetimeRows(t, h, computer, export, 64, 1)
	seedComputerSnapshotBackups(t, h, backup, 500, 1)
	// 1000 ordinary services, each with a retained attempt and routing tags.
	// They share the real node facts but no Computer projection ownership.
	tx, err := h.store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	spec, _ := json.Marshal(operatorServiceSpec("load", nil))
	_, err = tx.Exec(`WITH RECURSIVE fixture(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM fixture WHERE n<1000)
 INSERT INTO jobs(job_id,dispatch_key,request_hash,spec_json,state,parent_job_id,created_ns,updated_ns)
 SELECT printf('load-job-%04d',n),printf('load-job-%04d',n),'load',?,'queued',?,n,n FROM fixture`, spec, computer.CurrentJobID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`INSERT INTO service_jobs(job_id,desired_state,bound_node_id) SELECT job_id,'running',? FROM jobs WHERE job_id LIKE 'load-job-%'`, node.NodeID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`INSERT INTO attempts(attempt_id,job_id,node_id,boot_session_id,state,fencing_token,lease_expires_ns,authority_generation,created_ns,updated_ns)
 SELECT 'load-attempt-'||job_id,job_id,?,?,'completed','load-fence-'||job_id,?,0,created_ns,updated_ns FROM jobs WHERE job_id LIKE 'load-job-%'`, node.NodeID, node.BootSessionID, h.clock.Now().Add(time.Hour).UnixNano()); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`UPDATE jobs SET current_attempt_id='load-attempt-'||job_id WHERE job_id LIKE 'load-job-%'`); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`INSERT INTO nodes(node_id,identity_node_id,boot_session_id,os,architecture,agent_version,capabilities_json,state,last_heartbeat_ns,max_oneshot_slots,max_service_slots,claims_enabled)
 SELECT 'load-node-'||n,'load-identity-'||n,'boot-'||n,'linux','amd64','load','{"kind:process":true,"kind:oci":true}','alive',?,4,4,1
 FROM (WITH RECURSIVE fixture(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM fixture WHERE n<32) SELECT n FROM fixture)`, h.clock.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	actor := &serviceActionActor{Identity: fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}}, ClientPrincipalTag: DefaultClientPrincipalTag}
	var mu sync.Mutex
	samples := map[string]*loadSamples{}
	record := func(name string, hold time.Duration, rows int, retry bool) {
		mu.Lock()
		defer mu.Unlock()
		v := samples[name]
		if v == nil {
			v = &loadSamples{}
			samples[name] = v
		}
		if hold > 0 {
			v.holds = append(v.holds, hold)
		}
		v.rows += rows
		if retry {
			v.retries++
		}
	}
	startedReaders := make(chan struct{}, 4)
	var readerStarts sync.Map
	page := func(name, cursor string, ctx context.Context) (loadAnswer, time.Duration, error) {
		return loadSnapshot(h.store, ctx, actor, func(ctx context.Context, r readModel) (loadAnswer, error) {
			a := loadAnswer{}
			if name == "jobs" || name == "children" || name == "backups" || name == "provenance" {
				if _, seen := readerStarts.LoadOrStore(name, true); !seen {
					startedReaders <- struct{}{}
				}
			}
			var err error
			switch name {
			case "jobs", "jobs-one-row":
				p, e := r.jobsPage(ctx, jobListFilters{Class: contract.JobClassService}, cursor, 250)
				err = e
				a.next = p.NextCursor
				for _, j := range p.Jobs {
					a.keys = append(a.keys, j.JobID)
					if j.CurrentAttemptID != "" {
						found := false
						for _, attempt := range j.Attempts {
							if attempt.AttemptID == j.CurrentAttemptID {
								found = true
								break
							}
						}
						if !found {
							return a, fmt.Errorf("current attempt %s missing from %s", j.CurrentAttemptID, j.JobID)
						}
					}
				}
			case "children":
				p, e := r.childrenPage(ctx, computer.CurrentJobID, cursor, 250)
				err = e
				a.next = p.NextCursor
				for _, j := range p.Jobs {
					a.keys = append(a.keys, j.JobID)
					if j.CurrentAttemptID != "" {
						found := false
						for _, attempt := range j.Attempts {
							if attempt.AttemptID == j.CurrentAttemptID {
								found = true
								break
							}
						}
						if !found {
							return a, fmt.Errorf("current attempt %s missing from %s", j.CurrentAttemptID, j.JobID)
						}
					}
				}
			case "computer":
				c, e := r.computerViewGetComputer(ctx, computer.ComputerID)
				err = e
				if err == nil {
					c, err = projectComputerResponse(ctx, r, c, computerActionActor{Identity: actor.Identity, ClientPrincipalTag: DefaultClientPrincipalTag})
				}
				a.keys = []string{c.ComputerID}
			case "backups":
				p, e := r.computerBackupsPage(ctx, computer.ComputerID, cursor, "", 250)
				err = e
				a.next = p.NextCursor
				for _, b := range p.Backups {
					a.keys = append(a.keys, b.BackupID)
				}
			case "provenance":
				p, e := r.computerProvenancePage(ctx, computer.ComputerID, cursor, 250)
				err = e
				a.next = p.NextCursor
				for _, p := range p.Provenance {
					a.keys = append(a.keys, p.ProvenanceID)
				}
			case "generations":
				p, e := r.computerViewListComputerStorageGenerations(ctx, computer.ComputerID, cursor, 250)
				err = e
				a.next = p.NextCursor
				for _, g := range p.Generations {
					a.keys = append(a.keys, fmt.Sprint(g.StorageGeneration))
				}
			case "exports":
				p, e := r.computerViewListComputerCustodyExports(ctx, computer.ComputerID, cursor, 250)
				err = e
				a.next = p.NextCursor
				for _, e := range p.Exports {
					a.keys = append(a.keys, e.ExportID)
				}
			case "nodes":
				c := nodeListCursor{Version: 1, Filters: nodeListFilters{}}
				if cursor != "" {
					c, err = decodeNodeListCursor(cursor, nodeListFilters{})
					if err != nil {
						return a, err
					}
				}
				p, e := r.nodePage(ctx, nodeListFilters{}, c, 1000, h.store.nodeLiveness())
				err = e
				a.next = p.NextCursor
				for _, n := range p.Nodes {
					a.keys = append(a.keys, n.NodeID)
				}
			case "node-detail":
				n, e := r.node(ctx, node.NodeID)
				err = e
				n = h.store.nodeLiveness().project(n, r.now())
				a.keys = []string{n.NodeID}
			}
			return a, err
		})
	}
	walk := func(name string, ctx context.Context) error {
		started := time.Now()
		seen := map[string]bool{}
		cursor := ""
		loadJobs := 0
		for {
			a, hold, err := page(name, cursor, ctx)
			retry := false
			if api := apiErrorFromDecision(err); api != nil && api.Code == contract.ErrorUnavailable && api.Retryable {
				retry = true
			}
			record(name, hold, len(a.keys), retry)
			if err != nil {
				if retry && ctx.Err() == nil {
					continue
				}
				return err
			}
			for _, key := range a.keys {
				if key == "" || seen[key] {
					return fmt.Errorf("%s duplicate/empty key %q", name, key)
				}
				seen[key] = true
				if strings.HasPrefix(key, "load-job-") {
					loadJobs++
				}
			}
			cursor = a.next
			if cursor == "" {
				break
			}
		}
		expected := map[string]int{"children": 1000, "computer": 1, "backups": 501, "provenance": 501, "generations": 65, "exports": 65, "nodes": 33, "node-detail": 1}
		if name == "jobs" || name == "jobs-one-row" {
			if loadJobs != 1000 {
				return fmt.Errorf("%s load membership=%d", name, loadJobs)
			}
		} else if len(seen) != expected[name] {
			return fmt.Errorf("%s membership=%d want=%d", name, len(seen), expected[name])
		}
		mu.Lock()
		v := samples[name]
		v.walks++
		v.walkTime += time.Since(started)
		mu.Unlock()
		return nil
	}
	// Stream a bounded 64 MiB file, syncing each 8 MiB. It is independent of
	// SQLite locks and continues through the one-row measurement.
	ioCtx, stopIO := context.WithCancel(ctx)
	defer stopIO()
	ioDone := make(chan error, 1)
	ioStarted := make(chan struct{})
	var ioBytes atomic.Int64
	go func() {
		f, err := os.Create(filepath.Join(t.TempDir(), "load-io"))
		if err != nil {
			ioDone <- err
			return
		}
		defer f.Close()
		buf := make([]byte, 1<<20)
		for i := range buf {
			buf[i] = byte(i*31 + i/257)
		}
		for block := 0; ; block++ {
			if ioCtx.Err() != nil {
				ioDone <- nil
				return
			}
			if _, err = f.WriteAt(buf, int64(block%64)*int64(len(buf))); err != nil {
				ioDone <- err
				return
			}
			ioBytes.Add(int64(len(buf)))
			if block == 0 {
				close(ioStarted)
			}
			if block%8 == 7 {
				if err = f.Sync(); err != nil {
					ioDone <- err
					return
				}
			}
			select {
			case <-ioCtx.Done():
				ioDone <- nil
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}()
	select {
	case <-ioStarted:
	case err = <-ioDone:
		t.Fatalf("disk writer failed before starting: %v", err)
	case <-ctx.Done():
		t.Fatal("disk writer did not start")
	}
	writesDone := make(chan struct{})
	results := make(chan error, 4)
	var workers sync.WaitGroup
	for _, names := range [][]string{{"jobs", "computer"}, {"children", "nodes", "node-detail"}, {"backups", "generations"}, {"provenance", "exports"}} {
		workers.Add(1)
		go func(names []string) {
			defer workers.Done()
			for round := 0; ; round++ {
				for _, name := range names {
					if err := walk(name, ctx); err != nil {
						results <- err
						return
					}
				}
				if round >= 2 {
					select {
					case <-writesDone:
						results <- nil
						return
					default:
					}
				}
			}
		}(names)
	}
	for range 4 {
		select {
		case <-startedReaders:
		case <-ctx.Done():
			t.Fatal("paging workers did not start")
		}
	}
	checkpoints, attempts := 0, 0
	longest := time.Duration(0)
	for i := 0; i < 8; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				t.Fatal("load watchdog")
			case <-time.After(50 * time.Millisecond):
			}
		}
		if err = writeLoadSecret(ctx, h.store, i); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		progressCtx, finish := context.WithTimeout(ctx, 12*time.Second)
		for {
			attempts++
			sweep, e := h.store.SweepScrubbedSecrets(progressCtx)
			if e != nil {
				finish()
				t.Fatalf("checkpoint progress cycle=%d: %v", i, e)
			}
			if sweep.TruncatedWAL {
				checkpoints++
				break
			}
			if progressCtx.Err() != nil {
				finish()
				t.Fatalf("checkpoint progress stalled cycle=%d", i)
			}
		}
		finish()
		if elapsed := time.Since(start); elapsed > longest {
			longest = elapsed
		}
		var scrubbed int
		if err = h.store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE job_id=? AND secrets_scrubbed_ns IS NOT NULL AND json_extract(spec_json,'$.execution.sensitive_env') IS NULL`, fmt.Sprintf("load-secret-%d", i)).Scan(&scrubbed); err != nil || scrubbed != 1 {
			t.Fatalf("unscrubbed cycle=%d count=%d err=%v", i, scrubbed, err)
		}
		// No concurrent SQLite writer exists between this checkpoint and the next
		// cycle: file pressure and all paging workers are still active.
		var path string
		if err = h.store.db.QueryRowContext(ctx, "SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
			t.Fatal(err)
		}
		if stat, e := os.Stat(path + "-wal"); e != nil || stat.Size() != 0 {
			t.Fatalf("WAL not truncated after progress: %v %v", stat, e)
		}
	}
	close(writesDone)
	workers.Wait()
	for range 4 {
		if err = <-results; err != nil {
			t.Fatal(err)
		}
	}
	// Fixed costs include admission, membership and shared node-fact loading.
	smallCtx := context.WithValue(ctx, readPageCutoffContextKey{}, time.Nanosecond)
	smallStart := time.Now()
	if err = walk("jobs-one-row", smallCtx); err != nil {
		t.Fatal(err)
	}
	smallElapsed := time.Since(smallStart)
	stopIO()
	if err = <-ioDone; err != nil {
		t.Fatal(err)
	}
	t.Logf("machine=%s/%s CPUs=%d Go=%s readers=4 jobs=1000 attempts=1000 backups=501 provenance=501 nodes=33 lifetime=65 disk_MiB=%d checkpoints=%d attempts=%d longest_progress=%s one_row_walk=%s cost_per_row=%s", runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version(), ioBytes.Load()>>20, checkpoints, attempts, longest, smallElapsed, smallElapsed/1000)
	names := make([]string, 0, len(samples))
	for name := range samples {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		v := samples[name]
		sort.Slice(v.holds, func(i, j int) bool { return v.holds[i] < v.holds[j] })
		if len(v.holds) == 0 {
			t.Fatalf("no hold samples for %s", name)
		}
		p50 := v.holds[(len(v.holds)*50+99)/100-1]
		p95 := v.holds[(len(v.holds)*95+99)/100-1]
		max := v.holds[len(v.holds)-1]
		t.Logf("view=%s samples=%d p50=%s p95=%s max=%s rows=%d retries=%d walks=%d mean_walk=%s target=%s hard=%s", name, len(v.holds), p50, p95, max, v.rows, v.retries, v.walks, v.walkTime/time.Duration(v.walks), readSnapshotBudget, readSnapshotHardLimit)
		if os.Getenv("WEFTY_ENFORCE_READ_BUDGET") == "1" {
			if p95 > readSnapshotBudget || max > readSnapshotHardLimit || v.retries != 0 {
				t.Errorf("opt-in read budget exceeded: %s p95=%s max=%s retries=%d", name, p95, max, v.retries)
			}
		}
	}
}
