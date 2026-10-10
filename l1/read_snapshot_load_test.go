package l1

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// Leave time for cancellation and joins before the test binary's watchdog.
// A generous cap catches deadlocks without making slow CPUs a speed verdict.
func loadWatchdogContext(t *testing.T, parent context.Context, cap time.Duration) (context.Context, context.CancelFunc) {
	t.Helper()
	deadline := time.Now().Add(cap)
	if testDeadline, ok := t.Deadline(); ok {
		remaining := time.Until(testDeadline)
		reserve := min(5*time.Second, remaining/10)
		if limit := testDeadline.Add(-reserve); limit.Before(deadline) {
			deadline = limit
		}
	}
	return context.WithDeadline(parent, deadline)
}

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
	if readLoadRaceEnabled {
		t.Skip("race instrumentation changes SQLite hold/checkpoint timing; run the non-race load gate for timing evidence")
	}
	s, err := OpenStore(filepath.Join(t.TempDir(), "progress.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	ctx, cancel := loadWatchdogContext(t, t.Context(), time.Minute)
	var reader sync.WaitGroup
	reader.Add(1)
	t.Cleanup(func() { cancel(); unblock(); reader.Wait() })
	go func() {
		defer reader.Done()
		done <- s.withReadSnapshot(ctx, nil, func(context.Context, readModel) error { close(entered); <-release; return nil })
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("reader did not anchor")
	}
	if err = writeLoadSecret(t.Context(), s, 0); err != nil {
		t.Fatal(err)
	}

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
	case <-ctx.Done():
		t.Fatal("reader did not finish")
	}
	if errorCode(err) != contract.ErrorUnavailable {
		t.Fatalf("parked read=%v", err)
	}
}

// Four paging workers, one disk writer, and sequential secret writes/sweeps.
// The default verdict is membership, no deadlock, and repeated WAL progress;
// speed is diagnostic unless explicitly opted in.
// cutoffBoundedLoadViews are read at the 1000-row maximum with an adaptive
// cutoff that this fixture actually reaches, so their hold is bounded by the
// cutoff rather than by a page cap. Log polls stay far below the cutoff and
// keep the capped-view target.
var cutoffBoundedLoadViews = map[string]bool{"nodes": true}

// cutoffBoundedSlack covers the row in flight at the cutoff plus rollback;
// overshoot beyond it is the regression an adaptive page can have.
const cutoffBoundedSlack = 20 * time.Millisecond

func TestReadSnapshotSustainedLoad(t *testing.T) {
	if readLoadRaceEnabled {
		t.Skip("race instrumentation changes SQLite hold/checkpoint timing; run the non-race load gate for timing evidence")
	}
	enforce := os.Getenv("WEFTY_ENFORCE_READ_BUDGET") == "1"
	trialCap := MaxJobListingPageLimit
	if value := os.Getenv("WEFTY_READ_JOB_TRIAL_CAP"); value != "" {
		var err error
		trialCap, err = strconv.Atoi(value)
		if err != nil || (trialCap != 150 && trialCap != 200 && trialCap != 250) {
			t.Fatal("WEFTY_READ_JOB_TRIAL_CAP must be 150, 200 or 250")
		}
	}
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
 FROM (WITH RECURSIVE fixture(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM fixture WHERE n<999) SELECT n FROM fixture)`, h.clock.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
	admin := fabric.Identity{FabricID: "load-fabric", UserID: "load-admin", DeviceID: "load-device"}
	if _, err = tx.Exec(`INSERT INTO admins(fabric_id,user_id,added_revision,added_ns) VALUES(?,?,1,1)`, admin.FabricID, admin.UserID); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`WITH RECURSIVE fixture(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM fixture WHERE n<1000)
        INSERT INTO admin_policy_audit(revision,operation,actor_kind,actor_fabric_id,actor_user_id,actor_device_id,subject_fabric_id,subject_user_id,created_ns)
        SELECT n,'add','local_operator','','','','','',n FROM fixture`,
		`WITH RECURSIVE fixture(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM fixture WHERE n<1000)
        INSERT INTO computer_policy_audit(policy_revision,computer_id,operation,actor_kind,actor_fabric_id,actor_user_id,actor_device_id,subject_fabric_id,subject_user_id,previous_permission,permission,idempotency_key,request_hash,created_ns)
        SELECT n,?,'grant','local_operator','','','','','','none','view','','load',n FROM fixture`,
		`WITH RECURSIVE fixture(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM fixture WHERE n<1000)
        INSERT INTO computer_takeover_audit(attempt_id,event_id,event_kind,computer_id,job_id,session_id,fabric_id,user_id,device_id,authorized_role,admitted_mode,policy_revision,authority_generation,occurred_ns,stored_ns,reason,event_count,request_hash)
        SELECT 'load',printf('event-%04d',n),'session_open',?,'','','','','','view','view',0,0,n,n,'',1,'load' FROM fixture`,
	} {
		args := []any{}
		if strings.Contains(query, "?") {
			args = append(args, computer.ComputerID)
		}
		if _, err = tx.Exec(query, args...); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = tx.Exec(`WITH RECURSIVE fixture(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM fixture WHERE n<1000)
     INSERT INTO log_events(job_id,attempt_id,stream,sequence,sequence_end,timestamp_ns,bytes,event_json)
     SELECT 'load-job-0001','load-attempt-load-job-0001','stdout',n,n,?,zeroblob(1024),json_object('attempt_id','load-attempt-load-job-0001','stream','stdout','sequence',n,'timestamp','2026-08-09T10:00:00Z') FROM fixture`, h.clock.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
	// Replace only this fixture's history; retained Computer authority stays put.
	if _, err = tx.Exec(`DELETE FROM computer_intent_history WHERE computer_id=?`, computer.ComputerID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`WITH RECURSIVE fixture(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM fixture WHERE n<1000)
     INSERT INTO computer_intent_history(computer_id,intent_revision,operation,desired_state,storage_id,storage_generation,job_id,spec_revision,actor,created_ns)
     SELECT ?,n,'start','running',?,1,?,1,'load',n FROM fixture`, computer.ComputerID, computer.StorageID, computer.CurrentJobID); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := loadWatchdogContext(t, t.Context(), 3*time.Minute)
	defer cancel()
	if trialCap != MaxJobListingPageLimit {
		ctx = context.WithValue(ctx, readJobTrialPageLimitContextKey{}, trialCap)
	}
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
			case "logs":
				p, e := r.logs(ctx, "load-job-0001", cursor, 1000)
				err, a.next = e, p.NextCursor
				if len(p.Events) == 0 {
					a.next = ""
				} // Log polls terminate with an empty page, not an absent cursor.
				for _, event := range p.Events {
					a.keys = append(a.keys, fmt.Sprint(event.Sequence))
				}

			case "admin-audit":
				after, e := decodeAdminAuditCursor(cursor)
				if e != nil {
					return a, e
				}
				p, e := r.adminAuditPage(ctx, after, 1000)
				err, a.next = e, p.NextCursor
				for _, entry := range p.Entries {
					a.keys = append(a.keys, fmt.Sprint(entry.Revision))
				}
			case "policy-audit":
				p, e := r.computerViewListComputerPolicyAudit(ctx, admin, computer.ComputerID, cursor, 1000)
				err, a.next = e, p.NextCursor
				for _, entry := range p.Entries {
					a.keys = append(a.keys, fmt.Sprint(entry.PolicyRevision))
				}
			case "takeover-audit", "takeover-tail":
				p, e := r.computerViewListComputerTakeoverAudit(ctx, admin, computer.ComputerID, cursor, 1000, name == "takeover-tail")
				err, a.next = e, p.NextCursor
				if name == "takeover-tail" {
					a.next = ""
				} // tail's cursor starts a subsequent forward observation.
				for _, entry := range p.Events {
					a.keys = append(a.keys, entry.EventID)
				}
			case "intents":
				p, e := r.computerViewListComputerIntents(ctx, computer.ComputerID, cursor, 1000)
				err, a.next = e, p.NextCursor
				for _, intent := range p.Intents {
					a.keys = append(a.keys, fmt.Sprint(intent.IntentRevision))
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
		expected := map[string]int{"children": 1000, "computer": 1, "backups": 501, "provenance": 501, "generations": 65, "exports": 65, "nodes": 1000, "node-detail": 1, "admin-audit": 1000, "policy-audit": 1000, "takeover-audit": 1000, "takeover-tail": 1000, "intents": 1000, "logs": 1000}
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
	// SQLite locks. Total volume is capped at 256 MiB, including diagnostics.
	ioCtx, stopIO := context.WithCancel(ctx)
	var workers, diskWriter sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		stopIO()
		workers.Wait()
		diskWriter.Wait()
	})
	ioDone := make(chan error, 1)
	ioStarted := make(chan struct{})
	var ioBytes atomic.Int64
	ioPath := filepath.Join(t.TempDir(), "load-io")
	diskWriter.Add(1)
	go func() {
		defer diskWriter.Done()
		f, err := os.Create(ioPath)
		if err != nil {
			ioDone <- err
			return
		}
		defer f.Close()
		buf := make([]byte, 1<<20)
		for i := range buf {
			buf[i] = byte(i*31 + i/257)
		}
		for block := 0; block < 256; block++ {
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
			case <-time.After(50 * time.Millisecond):
			}
		}
		ioDone <- nil
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
	for _, names := range [][]string{{"jobs", "computer"}, {"children", "nodes", "node-detail", "admin-audit", "policy-audit", "takeover-audit", "takeover-tail", "intents", "logs"}, {"backups", "generations"}, {"provenance", "exports"}} {
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
		progressCtx, finish := loadWatchdogContext(t, ctx, time.Minute)
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
	// The 1001-snapshot diagnostic is opt-in and has its own watchdog. Default
	// membership/hygiene verdicts do not depend on finishing this timing probe.
	smallElapsed := time.Duration(0)
	if enforce {
		diagnosticCtx, finish := loadWatchdogContext(t, t.Context(), time.Minute)
		if trialCap != MaxJobListingPageLimit {
			diagnosticCtx = context.WithValue(diagnosticCtx, readJobTrialPageLimitContextKey{}, trialCap)
		}
		smallCtx := context.WithValue(diagnosticCtx, readPageCutoffContextKey{}, time.Nanosecond)
		smallStart := time.Now()
		err = walk("jobs-one-row", smallCtx)
		finish()
		if err != nil {
			t.Fatal(err)
		}
		smallElapsed = time.Since(smallStart)
	}
	stopIO()
	if err = <-ioDone; err != nil {
		t.Fatal(err)
	}
	t.Logf("machine=%s/%s CPUs=%d Go=%s readers=4 jobs=1000 attempts=1000 backups=501 provenance=501 nodes=1000 audit/intents/logs=1000 lifetime=65 job_cap=%d disk_MiB=%d checkpoints=%d attempts=%d longest_progress=%s one_row_walk=%s cost_per_row=%s", runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version(), trialCap, ioBytes.Load()>>20, checkpoints, attempts, longest, smallElapsed, smallElapsed/1000)
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
		if enforce {
			// A maximum-limit node page adapts: by design it reads until the
			// 120 ms cutoff, above the advisory target, and returns
			// next_cursor. It is held to the cutoff plus one row's slack;
			// capped views must keep p95 under the target.
			target := readSnapshotBudget
			if cutoffBoundedLoadViews[name] {
				target = readSnapshotPageCutoff + cutoffBoundedSlack
			}
			if p95 > target || max > readSnapshotHardLimit || v.retries != 0 {
				t.Errorf("opt-in read budget exceeded: %s p95=%s (target %s) max=%s retries=%d", name, p95, target, max, v.retries)
			}
		}
	}
}
