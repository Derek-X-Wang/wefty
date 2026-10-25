package l1

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// #948: two statements #942 left whose work grows with the job or attempt
// count, measured as #939's and #942's tests do, in SQLite pages read on a
// one-connection pool. Each reconcile pass's expired-attempt query read every
// active job, and each claim's prior-boot check read the whole attempts table.

// seedServicesRunning adds total services on the sweep node, each with one
// attempt. The first running of them run that attempt as their current one,
// healthy, with a lease an hour ahead, as services that renew on time; the
// rest are stopped after a failed attempt. Fixtures with different running
// counts hold tables of the same size.
func seedServicesRunning(t *testing.T, store *Store, clock *fakeClock, total, running int) {
	t.Helper()
	seedIdleServices(t, store, "svc", total)
	now := clock.Now().UnixNano()
	retentionSweepExec(t, store, `INSERT INTO attempts(attempt_id, job_id, node_id, boot_session_id, state, fencing_token,
			lease_expires_ns, authority_generation, created_ns, updated_ns)
		SELECT job_id||'-run', job_id, ?, 'boot', CASE WHEN job_id<=? THEN ? ELSE ? END, job_id||'-fence', ?, 0, ?, ?
		FROM jobs WHERE job_id LIKE 'svc-%'`,
		retentionSweepNodeID, seededServiceID("svc", running), contract.AttemptRunning, contract.AttemptFailed,
		clock.Now().Add(time.Hour).UnixNano(), now, now)
	retentionSweepExec(t, store, `UPDATE jobs SET state=?, current_attempt_id=job_id||'-run' WHERE job_id LIKE 'svc-%' AND job_id<=?`,
		contract.JobRunning, seededServiceID("svc", running))
	retentionSweepExec(t, store, `UPDATE jobs SET state=? WHERE job_id LIKE 'svc-%' AND job_id>?`, contract.JobStopped, seededServiceID("svc", running))
	retentionSweepExec(t, store, `UPDATE service_jobs SET bound_node_id=?,
			healthy_since_ns=CASE WHEN job_id<=? THEN ? END,
			desired_state=CASE WHEN job_id<=? THEN ? ELSE ? END
		WHERE job_id LIKE 'svc-%'`, retentionSweepNodeID, seededServiceID("svc", running), now,
		seededServiceID("svc", running), contract.ServiceDesiredRunning, contract.ServiceDesiredStopped)
}

// runningOneshot submits a one-shot and makes attemptID its running current
// attempt on the sweep node, with a lease an hour ahead.
func runningOneshot(t *testing.T, store *Store, clock *fakeClock, dispatchKey, attemptID string) string {
	t.Helper()
	job, _, err := store.CreateJob(context.Background(), validJobSpec(dispatchKey, nil))
	if err != nil {
		t.Fatal(err)
	}
	now := clock.Now().UnixNano()
	retentionSweepExec(t, store, `INSERT INTO attempts(attempt_id, job_id, node_id, boot_session_id, state, fencing_token,
			lease_expires_ns, authority_generation, created_ns, updated_ns) VALUES(?, ?, ?, 'boot', ?, ?, ?, 0, ?, ?)`,
		attemptID, job.JobID, retentionSweepNodeID, contract.AttemptRunning, attemptID+"-fence", clock.Now().Add(time.Hour).UnixNano(), now, now)
	retentionSweepExec(t, store, "UPDATE jobs SET state=?, current_attempt_id=? WHERE job_id=?", contract.JobRunning, attemptID, job.JobID)
	return job.JobID
}

// cancelOneshotsDueNow starts count one-shots and cancels each with its
// settlement deadline at the current instant.
func cancelOneshotsDueNow(t *testing.T, store *Store, clock *fakeClock, label string, count int) []string {
	t.Helper()
	var attemptIDs []string
	for index := 1; index <= count; index++ {
		attemptID := seededServiceID(label+"-run", index)
		jobID := runningOneshot(t, store, clock, seededServiceID(label, index), attemptID)
		retentionSweepExec(t, store, "UPDATE jobs SET outcome='canceled', cancel_settle_by_ns=? WHERE job_id=?", clock.Now().UnixNano(), jobID)
		attemptIDs = append(attemptIDs, attemptID)
	}
	return attemptIDs
}

func attemptState(t *testing.T, store *Store, attemptID string) contract.AttemptState {
	t.Helper()
	var state contract.AttemptState
	if err := store.db.QueryRow("SELECT state FROM attempts WHERE attempt_id=?", attemptID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

// The services that renew on time must cost a reconcile pass nothing, however
// many there are. Both fixtures hold 40000 services with one attempt each, so
// only the number running differs. The pass still expires, on time, the
// leases that ran out and the canceled one-shots at their settlement deadline.
func TestReconcileExpiredAttemptQueryReadsFlatAsRunningServicesGrow(t *testing.T) {
	const total, expiring, canceled = 40000, 8, 2
	pagesWith := func(running int) int {
		store, clock := openRetentionSweepStore(t, StoreOptions{})
		seedServicesRunning(t, store, clock, total, running)
		first := cancelOneshotsDueNow(t, store, clock, "canceled-first", canceled)
		// The first pass after L1 starts reads every service once (#942).
		if result, err := store.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		} else if result.ExpiredAttempts != canceled {
			t.Fatalf("the first pass expired %d attempts, want the %d canceled one-shots at their deadline", result.ExpiredAttempts, canceled)
		}
		due := cancelOneshotsDueNow(t, store, clock, "canceled-next", canceled)
		for index := 1; index <= expiring; index++ {
			attemptID := seededServiceID("svc", index*running/expiring) + "-run"
			retentionSweepExec(t, store, "UPDATE attempts SET lease_expires_ns=? WHERE attempt_id=?", clock.Now().UnixNano(), attemptID)
			due = append(due, attemptID)
		}
		oneConnection(store)
		result, pages := countedPass(t, store)
		if result.ExpiredAttempts != expiring+canceled {
			t.Fatalf("the pass expired %d attempts with %d services running, want %d", result.ExpiredAttempts, running, expiring+canceled)
		}
		for _, attemptID := range append(first, due...) {
			if state := attemptState(t, store, attemptID); state != contract.AttemptLost {
				t.Fatalf("attempt %s is %s after its deadline, want lost", attemptID, state)
			}
		}
		if state := attemptState(t, store, seededServiceID("svc", running-1)+"-run"); state != contract.AttemptRunning {
			t.Fatalf("an attempt with its lease ahead is %s, want running", state)
		}
		return pages
	}
	few, many := pagesWith(4000), pagesWith(total)
	t.Logf("a reconcile pass read %d pages with 4000 of %d services running and %d with all of them", few, total, many)
	if many > few+few/2 {
		t.Fatalf("a reconcile pass read %d pages with %d services running against %d with 4000", many, total, few)
	}
}

// formerExpiredAttemptsSQL is the expired-attempt selection of every reconcile
// pass before #948, copied from main 99ec2005 and independent of the
// production statement.
const formerExpiredAttemptsSQL = `SELECT attempts.attempt_id
		FROM attempts JOIN jobs ON jobs.job_id=attempts.job_id
		WHERE attempts.state IN (?, ?, ?) AND (attempts.lease_expires_ns<=? OR (jobs.outcome='canceled' AND jobs.cancel_settle_by_ns<=?))
			AND jobs.current_attempt_id=attempts.attempt_id
			AND jobs.state IN (?, ?, ?, ?)`

func selectedAttempts(t *testing.T, tx *sql.Tx, query string, args ...any) []string {
	t.Helper()
	rows, err := tx.Query(query, args...)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := scanJobIDs(rows, "expired attempt")
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(ids)
	return ids
}

// formerAndCurrentExpiredAttempts runs both selections in one transaction, so
// from the same state, and returns each sorted.
func formerAndCurrentExpiredAttempts(t *testing.T, store *Store, now time.Time) (former, current []string) {
	t.Helper()
	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	former = selectedAttempts(t, tx, formerExpiredAttemptsSQL,
		contract.AttemptClaimed, contract.AttemptRunning, contract.AttemptAwaitingInput, now.UnixNano(), now.UnixNano(),
		contract.JobClaimed, contract.JobRunning, contract.JobAwaitingInput, contract.JobStopping)
	current = selectedAttempts(t, tx, expiredAttemptsSQL, expiredAttemptsArguments(now)...)
	return former, current
}

// Every combination of attempt state, job state, current-attempt binding,
// lease deadline and cancellation around the instant read: the selection
// must return exactly what the former query returns, at every instant, and
// each pass must expire exactly that, until nothing is left to expire.
func TestExpiredAttemptSelectionMatchesTheFormerQuery(t *testing.T) {
	store, clock := openRetentionSweepStore(t, StoreOptions{})
	template, _, err := store.CreateJob(context.Background(), validJobSpec("expiry-template", nil))
	if err != nil {
		t.Fatal(err)
	}
	base := clock.Now().UnixNano()
	attemptStates := []contract.AttemptState{contract.AttemptClaimed, contract.AttemptRunning, contract.AttemptAwaitingInput,
		contract.AttemptLost, contract.AttemptFailed, contract.AttemptSucceeded}
	jobStates := []contract.JobState{contract.JobQueued, contract.JobClaimed, contract.JobRunning, contract.JobAwaitingInput,
		contract.JobStopping, contract.JobStopped, contract.JobFailed, contract.JobSucceeded, contract.JobRemovalPending}
	// own: the job's current attempt is this one; none: the job has none;
	// foreign: the job's current attempt belongs to another job.
	bindings := []string{"own", "none", "foreign"}
	type cancellation struct {
		outcome string
		settle  sql.NullInt64
	}
	cancellations := []cancellation{{"", sql.NullInt64{}}, {"", sql.NullInt64{Int64: base - 1, Valid: true}},
		{"canceled", sql.NullInt64{}}, {"canceled", sql.NullInt64{Int64: base - 1, Valid: true}},
		{"canceled", sql.NullInt64{Int64: base, Valid: true}}, {"canceled", sql.NullInt64{Int64: base + 1, Valid: true}}}
	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	insertJob, err := tx.Prepare(`INSERT INTO jobs(job_id, dispatch_key, request_hash, spec_json, state, outcome, cancel_settle_by_ns, created_ns, updated_ns)
		SELECT ?, ?, request_hash, spec_json, ?, ?, ?, created_ns, updated_ns FROM jobs WHERE job_id=?`)
	if err != nil {
		t.Fatal(err)
	}
	insertAttempt, err := tx.Prepare(`INSERT INTO attempts(attempt_id, job_id, node_id, boot_session_id, state, fencing_token,
		lease_expires_ns, authority_generation, created_ns, updated_ns) VALUES(?, ?, ?, 'boot', ?, 'fence', ?, 0, ?, ?)`)
	if err != nil {
		t.Fatal(err)
	}
	bind, err := tx.Prepare("UPDATE jobs SET current_attempt_id=? WHERE job_id=?")
	if err != nil {
		t.Fatal(err)
	}
	exec := func(statement *sql.Stmt, args ...any) {
		t.Helper()
		if _, err := statement.Exec(args...); err != nil {
			t.Fatal(err)
		}
	}
	combinations := 0
	for _, attemptState := range attemptStates {
		for _, jobState := range jobStates {
			for _, binding := range bindings {
				for leaseOffset := int64(-1); leaseOffset <= 1; leaseOffset++ {
					for cancelIndex, cancel := range cancellations {
						combinations++
						name := fmt.Sprintf("%s-%s-%s-%d-%d", attemptState, jobState, binding, leaseOffset+1, cancelIndex)
						jobID, attemptID := "job-"+name, "attempt-"+name
						exec(insertJob, jobID, "key-"+name, jobState, cancel.outcome, cancel.settle, template.JobID)
						owner := jobID
						if binding == "foreign" {
							owner = "owner-" + name
							exec(insertJob, owner, "key-owner-"+name, contract.JobFailed, "", nil, template.JobID)
						}
						exec(insertAttempt, attemptID, owner, retentionSweepNodeID, attemptState, base+leaseOffset, base, base)
						if binding != "none" {
							exec(bind, attemptID, jobID)
						}
					}
				}
			}
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	compare := func(at int64) []string {
		t.Helper()
		former, current := formerAndCurrentExpiredAttempts(t, store, time.Unix(0, at).UTC())
		if !slices.Equal(former, current) {
			t.Fatalf("at %+d ns the selection returned %d attempts, the former query %d:\n%v\n%v", at-base, len(current), len(former), current, former)
		}
		return former
	}
	for at := base - 2; at <= base+2; at++ {
		compare(at)
	}
	if selected := compare(base); len(selected) == 0 {
		t.Fatal("the fixture gives the selection nothing to find")
	}
	// Passes at each instant around the deadlines expire exactly what the
	// former query selects from the state each one starts in.
	clock.Advance(-2 * time.Nanosecond)
	expired := 0
	for pass := 0; pass < 5; pass++ {
		now := canonicalTime(clock.Now())
		due := compare(now.UnixNano())
		result, err := store.Reconcile(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if result.ExpiredAttempts != int64(len(due)) {
			t.Fatalf("the pass at %+d ns expired %d attempts, the former query selects %d", now.UnixNano()-base, result.ExpiredAttempts, len(due))
		}
		for _, attemptID := range due {
			if state := attemptState(t, store, attemptID); state != contract.AttemptLost {
				t.Fatalf("attempt %s is %s after the pass that selected it", attemptID, state)
			}
		}
		expired += len(due)
		clock.Advance(time.Nanosecond)
	}
	if left := compare(canonicalTime(clock.Now()).Add(time.Hour).UnixNano()); len(left) != 0 {
		t.Fatalf("after the passes the former query still selects %v", left)
	}
	t.Logf("%d combinations; the passes expired %d attempts", combinations, expired)
}

// claimHistoryStore registers a node under boot-1, gives it count ended
// attempts of a failed one-shot under that boot session, re-registers it under
// boot-2, and submits one service and one one-shot for it to claim.
func claimHistoryStore(t *testing.T, count int) (*Store, Node) {
	t.Helper()
	store, clock := openRetentionSweepStore(t, StoreOptions{})
	ctx := context.Background()
	first := registerStoreNode(t, store, clock, "claim-node", "boot-1")
	history, _, err := store.CreateJob(ctx, validJobSpec("claim-history", nil))
	if err != nil {
		t.Fatal(err)
	}
	created := clock.Now().Add(-24 * time.Hour).UnixNano()
	for _, state := range []contract.AttemptState{contract.AttemptFailed, contract.AttemptSucceeded, contract.AttemptLost} {
		seedNodeAttempts(t, store, history.JobID, string(state), count/3, first, state, created)
	}
	retentionSweepExec(t, store, "UPDATE jobs SET state=? WHERE job_id=?", contract.JobFailed, history.JobID)
	node := registerStoreNode(t, store, clock, "claim-node", "boot-2")
	submitClaimables(t, store, "")
	return store, node
}

// submitClaimables submits one service and one one-shot, each claimable by
// any node, under dispatch keys ending in suffix.
func submitClaimables(t *testing.T, store *Store, suffix string) {
	t.Helper()
	service := validJobSpec("claim-service"+suffix, nil)
	service.Class = contract.JobClassService
	service.Execution.HandoffDirectory = ""
	service.Restart = contract.RestartAlways
	for _, spec := range []contract.JobSpec{service, validJobSpec("claim-oneshot"+suffix, nil)} {
		if _, _, err := store.CreateJob(context.Background(), spec); err != nil {
			t.Fatal(err)
		}
	}
}

// A node's long attempt history must not make its claims read more: the
// prior-boot check, and a one-shot claim's slot count, read only the node's
// active attempts.
func TestClaimReadsFlatAsAttemptHistoryGrows(t *testing.T) {
	type work struct{ service, oneshot int }
	measure := func(attempts int) work {
		store, node := claimHistoryStore(t, attempts)
		oneConnection(store)
		var measured work
		for _, class := range []string{contract.JobClassService, contract.JobClassOneShot} {
			pagesRead(t, store)
			claim, err := store.ClaimJob(context.Background(), "agent-claim-node", node.NodeID, node.BootSessionID, class)
			if err != nil {
				t.Fatal(err)
			}
			if claim == nil {
				t.Fatalf("the %s claim found nothing", class)
			}
			if class == contract.JobClassService {
				measured.service = pagesRead(t, store)
			} else {
				measured.oneshot = pagesRead(t, store)
			}
		}
		return measured
	}
	short, long := measure(2001), measure(20001)
	t.Logf("pages read by a claim over 2001 and 20001 ended attempts: service %d/%d, one-shot %d/%d",
		short.service, long.service, short.oneshot, long.oneshot)
	for _, call := range []struct {
		class       string
		short, long int
	}{{"service", short.service, long.service}, {"one-shot", short.oneshot, long.oneshot}} {
		if call.long > call.short+call.short/2 {
			t.Errorf("a %s claim read %d pages over 20001 ended attempts against %d over 2001", call.class, call.long, call.short)
		}
	}
}

// #948's indexes on attempts; without them a store has main's.
var attemptIndexes948 = []string{"attempts_lease_expiry", "attempts_node_state"}

// claimOutcome is what a claim of one class answered: the claimed job's
// dispatch key, or the refusal code, or neither for an empty claim.
type claimOutcome struct {
	claimed string
	refusal string
}

func claimOnce(t *testing.T, store *Store, node Node, class string) claimOutcome {
	t.Helper()
	claim, err := store.ClaimJob(context.Background(), "agent-"+node.NodeID, node.NodeID, node.BootSessionID, class)
	if err != nil {
		return claimOutcome{refusal: string(errorCode(err))}
	}
	if claim == nil {
		return claimOutcome{}
	}
	return claimOutcome{claimed: claim.Job.Spec.DispatchKey}
}

// Two stores built alike, one with #948's attempt indexes and one with them
// dropped, so that it reads attempts with main's plan, take the same steps.
// For each case a fresh node registers under boot-1 and then boot-2, and one
// attempt in some state runs under one of those boot sessions on that node or
// on another; then the node claims a service and a one-shot. Both stores must
// answer the same, and the claims must be embargoed exactly while an attempt
// of the node's prior boot session is not terminal.
func TestClaimPriorBootEmbargoMatchesMainsScan(t *testing.T) {
	attemptStates := []contract.AttemptState{contract.AttemptClaimed, contract.AttemptRunning, contract.AttemptAwaitingInput,
		contract.AttemptLost, contract.AttemptFailed, contract.AttemptSucceeded}
	type twin struct {
		store *Store
		clock *fakeClock
	}
	var twins [2]twin
	for index := range twins {
		store, clock := openRetentionSweepStore(t, StoreOptions{})
		if index == 1 {
			for _, name := range attemptIndexes948 {
				retentionSweepExec(t, store, "DROP INDEX "+name)
			}
		}
		twins[index] = twin{store, clock}
	}
	ctx := context.Background()
	cases, refused := 0, 0
	for _, onClaimingNode := range []bool{true, false} {
		for _, boot := range []string{"boot-1", "boot-2"} {
			for _, state := range attemptStates {
				cases++
				suffix := fmt.Sprintf("-%02d", cases)
				var outcomes [2][2]claimOutcome
				for index, world := range twins {
					store, clock := world.store, world.clock
					registerStoreNode(t, store, clock, "claim"+suffix, "boot-1")
					node := registerStoreNode(t, store, clock, "claim"+suffix, "boot-2")
					other := registerStoreNode(t, store, clock, "other"+suffix, "boot-2")
					probeNode := other
					if onClaimingNode {
						probeNode = node
					}
					probe, _, err := store.CreateJob(ctx, validJobSpec("probe"+suffix, nil))
					if err != nil {
						t.Fatal(err)
					}
					now := clock.Now().UnixNano()
					retentionSweepExec(t, store, `INSERT INTO attempts(attempt_id, job_id, node_id, boot_session_id, state, fencing_token,
						lease_expires_ns, authority_generation, created_ns, updated_ns) VALUES(?, ?, ?, ?, ?, 'fence', ?, ?, ?, ?)`,
						"probe-attempt"+suffix, probe.JobID, probeNode.NodeID, boot, state, clock.Now().Add(time.Hour).UnixNano(),
						probeNode.AuthorityGeneration, now, now)
					retentionSweepExec(t, store, "UPDATE jobs SET state=?, current_attempt_id=? WHERE job_id=?", contract.JobRunning, "probe-attempt"+suffix, probe.JobID)
					submitClaimables(t, store, suffix)
					for classIndex, class := range []string{contract.JobClassService, contract.JobClassOneShot} {
						outcomes[index][classIndex] = claimOnce(t, store, node, class)
					}
					// What an embargoed node left queued is no later case's.
					retentionSweepExec(t, store, "UPDATE jobs SET state=? WHERE state=?", contract.JobFailed, contract.JobQueued)
				}
				if outcomes[0] != outcomes[1] {
					t.Fatalf("case %d, attempt %s under %s on the claiming node %v: claims answered %+v with #948's indexes, %+v with main's",
						cases, state, boot, onClaimingNode, outcomes[0], outcomes[1])
				}
				want := [2]claimOutcome{{claimed: "claim-service" + suffix}, {claimed: "claim-oneshot" + suffix}}
				if onClaimingNode && boot == "boot-1" && slices.Contains(attemptStates[:3], state) {
					want = [2]claimOutcome{}
					refused++
				}
				if outcomes[0] != want {
					t.Fatalf("case %d, attempt %s under %s on the claiming node %v: claims answered %+v, want %+v",
						cases, state, boot, onClaimingNode, outcomes[0], want)
				}
			}
		}
	}
	if refused != 3 {
		t.Fatalf("%d cases embargoed, want the 3 active states under the prior boot", refused)
	}
}

func queryPlan(t *testing.T, store *Store, query string, args ...any) []string {
	t.Helper()
	rows, err := store.db.Query("EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return plan
}

// Each branch of the expired-attempt selection seeks its own deadline index,
// and reads no job or attempt any other way than by its key. The twin store
// of the embargo test really reads attempts with main's scan.
func TestExpiredAttemptSelectionUsesItsIndexes(t *testing.T) {
	store, clock := openRetentionSweepStore(t, StoreOptions{})
	plan := queryPlan(t, store, expiredAttemptsSQL, expiredAttemptsArguments(clock.Now())...)
	for _, want := range []string{
		"SEARCH attempts USING INDEX attempts_lease_expiry (state=? AND lease_expires_ns<?)",
		"SEARCH attempts USING INDEX attempts_boot_lease_expiry (state=? AND lease_boot_id=? AND lease_deadline_ns>? AND lease_deadline_ns<?)",
		"SEARCH jobs USING INDEX sqlite_autoindex_jobs_1 (job_id=?)",
		"SEARCH jobs USING INDEX jobs_cancel_settlement (state=? AND cancel_settle_by_ns<?)",
		"SEARCH attempts USING INDEX sqlite_autoindex_attempts_1 (attempt_id=?)",
	} {
		if !slices.Contains(plan, want) {
			t.Fatalf("plan = %q, want %q in it", plan, want)
		}
	}
	for _, step := range plan {
		if len(step) >= 4 && step[:4] == "SCAN" {
			t.Fatalf("plan = %q scans", plan)
		}
	}
	for _, index := range append(attemptIndexes948, "attempts_boot_lease_expiry") {
		retentionSweepExec(t, store, "DROP INDEX "+index)
	}
	priorBoot := `SELECT EXISTS (SELECT 1 FROM attempts prior WHERE prior.node_id=? AND prior.boot_session_id<>? AND prior.state IN (?, ?, ?))`
	if plan := queryPlan(t, store, priorBoot, "node", "boot", contract.AttemptClaimed, contract.AttemptRunning, contract.AttemptAwaitingInput); !slices.Contains(plan, "SCAN prior") {
		t.Fatalf("without #948's indexes the prior-boot check plan = %q, want main's scan", plan)
	}
}
