package l1

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// #942: per-service work #939 left unbounded, measured as #939's tests do, in
// SQLite pages read on a one-connection pool. Claims, completions and log
// appends pruned a service's whole attempt history in one statement, and each
// reconcile pass's service-liveness statements read every service row.

// hotPathService submits and claims a service, then gives it kept older
// attempts below the current one: failed attempts that each keep a fresh log
// row, as a crash-looping service has after a week of restarts. elsewhere
// adds as many attempts and rows to a one-shot on the same node, so that
// fixtures with different histories can hold tables of the same size. The
// server is stopped and the store left on one connection, so only the store
// calls the test makes read pages.
func hotPathService(t *testing.T, options StoreOptions, kept, elsewhere int) (*integrationHarness, Node, Job, Claim) {
	t.Helper()
	h := newIntegrationHarnessWithReconcileInterval(t, options, map[string]NodePolicy{"service-node": DefaultNodePolicy("service")}, true, time.Hour)
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "service-node")
	job := submitRestartService(t, h, client, "hot-path-history", []string{"service"}, nil)
	claim := claimRestartService(t, h, agent, node)
	h.stopServer()
	created := h.clock.Now().Add(-24 * time.Hour).UnixNano()
	seedKept := func(jobID string, count int) {
		if count == 0 {
			return
		}
		seedNodeAttempts(t, h.store, jobID, "kept", count, node, contract.AttemptFailed, created)
		retentionSweepExec(t, h.store, `INSERT INTO log_events(job_id, attempt_id, stream, sequence, sequence_end, timestamp_ns, bytes, event_json)
			SELECT job_id, attempt_id, 'stdout', 0, 0, ?, X'6B', X'7B7D' FROM attempts WHERE job_id=? AND attempt_id LIKE ?`,
			h.clock.Now().UnixNano(), jobID, jobID+"-kept-%")
	}
	seedKept(job.JobID, kept)
	if elsewhere > 0 {
		oneshot, _, err := h.store.CreateJob(context.Background(), validJobSpec("hot-path-elsewhere", nil))
		if err != nil {
			t.Fatal(err)
		}
		seedKept(oneshot.JobID, elsewhere)
	}
	oneConnection(h.store)
	return h, node, job, claim
}

// seedNodeAttempts adds count attempts of jobID on node, named
// <jobID>-<label>-000001 onward and created one nanosecond apart from
// createdNS; the last is the newest.
func seedNodeAttempts(t *testing.T, store *Store, jobID, label string, count int, node Node, state contract.AttemptState, createdNS int64) {
	t.Helper()
	retentionSweepExec(t, store, `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<?)
		INSERT INTO attempts(attempt_id, job_id, node_id, boot_session_id, state, fencing_token,
			lease_expires_ns, authority_generation, created_ns, updated_ns)
		SELECT printf('%s-%s-%06d', ?, ?, i), ?, ?, ?, ?, printf('%s-fence-%d', ?, i), ?+i, ?, ?+i, ?+i FROM n`,
		count, jobID, label, jobID, node.NodeID, node.BootSessionID, state, label, createdNS, node.AuthorityGeneration, createdNS, createdNS)
}

func nodeAttemptID(jobID, label string, index int) string {
	return fmt.Sprintf("%s-%s-%06d", jobID, label, index)
}

func appendOneHotPathLog(t *testing.T, h *integrationHarness, job Job, claim Claim, sequence uint64) {
	t.Helper()
	event := logEvent(claim.Lease.AttemptID, contract.LogStdout, sequence, []byte("0123456789"))
	event.Timestamp = h.clock.Now()
	if _, err := h.store.AppendLogs(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, AppendLogsRequest{
		FencingToken: claim.Lease.FencingToken, Events: []contract.LogEvent{event},
	}); err != nil {
		t.Fatal(err)
	}
}

func completeHotPathAttempt(t *testing.T, h *integrationHarness, job Job, claim Claim) {
	t.Helper()
	exit := 1
	if _, err := h.store.CompleteAttemptOutcome(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "hot-path-" + claim.Lease.AttemptID, Result: ProcessResult{ExitCode: &exit},
	}); err != nil {
		t.Fatal(err)
	}
}

// awaitRestart waits out the service's restart backoff, keeping its node
// alive.
func awaitRestart(t *testing.T, h *integrationHarness, node Node, job Job) {
	t.Helper()
	var next sql.NullInt64
	if err := h.store.db.QueryRow("SELECT next_restart_at FROM service_jobs WHERE job_id=?", job.JobID).Scan(&next); err != nil {
		t.Fatal(err)
	}
	if wait := next.Int64 - h.clock.Now().UnixNano(); next.Valid && wait > 0 {
		h.clock.Advance(time.Duration(wait))
	}
	if _, err := h.store.HeartbeatNode(context.Background(), "agent", node.NodeID, node.BootSessionID); err != nil {
		t.Fatal(err)
	}
}

func claimHotPathService(t *testing.T, h *integrationHarness, node Node, job Job) *Claim {
	t.Helper()
	claim, err := h.store.ClaimJob(context.Background(), "agent", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil {
		t.Fatal(err)
	}
	if claim == nil || claim.Job.JobID != job.JobID {
		t.Fatalf("claim = %#v, want the service again", claim)
	}
	return claim
}

// A long history of kept attempts must not make a claim, a log append or a
// completion read more: each prunes a bounded part of it. Both fixtures hold
// the same number of attempts and log rows, so only the history differs.
func TestClaimAppendAndCompletionReadBoundedAttemptHistory(t *testing.T) {
	type work struct{ append, complete, claim int }
	measure := func(kept, elsewhere int) work {
		h, node, job, claim := hotPathService(t, StoreOptions{}, kept, elsewhere)
		var measured work
		pagesRead(t, h.store)
		appendOneHotPathLog(t, h, job, claim, 0)
		measured.append = pagesRead(t, h.store)
		completeHotPathAttempt(t, h, job, claim)
		measured.complete = pagesRead(t, h.store)
		awaitRestart(t, h, node, job)
		pagesRead(t, h.store)
		claimHotPathService(t, h, node, job)
		measured.claim = pagesRead(t, h.store)
		return measured
	}
	short, long := measure(2000, 18000), measure(20000, 0)
	t.Logf("pages read over histories of 2000 and 20000 kept attempts: append %d/%d, completion %d/%d, claim %d/%d",
		short.append, long.append, short.complete, long.complete, short.claim, long.claim)
	for _, call := range []struct {
		name        string
		short, long int
	}{{"log append", short.append, long.append}, {"completion", short.complete, long.complete}, {"claim", short.claim, long.claim}} {
		if call.long > call.short+call.short/2 {
			t.Errorf("a %s read %d pages over a history of 20000 kept attempts against %d over 2000", call.name, call.long, call.short)
		}
	}
}

// What a claim, completion or log append makes prunable at either end of the
// history is pruned by that call; what lies deeper is the sweep's. After both,
// nothing the whole-history prune would remove is left, and every attempt it
// would keep is kept.
func TestBoundedHotPathPruningLeavesNothingTheWholeHistoryPruneWouldRemove(t *testing.T) {
	// Oldest first below the current attempt: attempts whose rows the next
	// append trims under the byte cap, prunable ones with no rows, attempts
	// kept by their rows, more prunable ones, a lost attempt still inside
	// its late-evidence window, and prunable ones reaching up into the floor.
	const trimmed, deep, held, shallow, upper = 200, 50, 100, 50, DefaultServiceAttemptSummaries + 8
	const rowBytes = 10
	h, node, job, claim := hotPathService(t, StoreOptions{ServiceLogRetentionBytes: (held + 1) * rowBytes}, 0, 0)
	ctx := context.Background()
	created := h.clock.Now().Add(-24 * time.Hour).UnixNano()
	for _, group := range []struct {
		label string
		count int
		state contract.AttemptState
	}{{"trimmed", trimmed, contract.AttemptFailed}, {"deep", deep, contract.AttemptFailed}, {"held", held, contract.AttemptFailed},
		{"shallow", shallow, contract.AttemptFailed}, {"lost", 1, contract.AttemptLost}, {"upper", upper, contract.AttemptFailed}} {
		seedNodeAttempts(t, h.store, job.JobID, group.label, group.count, node, group.state, created)
		created += int64(group.count) + 1
	}
	retentionSweepExec(t, h.store, "UPDATE attempts SET updated_ns=? WHERE attempt_id=?", h.clock.Now().UnixNano(), nodeAttemptID(job.JobID, "lost", 1))
	for _, label := range []string{"trimmed", "held"} {
		retentionSweepExec(t, h.store, `INSERT INTO log_events(job_id, attempt_id, stream, sequence, sequence_end, timestamp_ns, bytes, event_json)
			SELECT job_id, attempt_id, 'stdout', 0, 0, ?, X'30313233343536373839', X'7B7D' FROM attempts
			WHERE job_id=? AND attempt_id LIKE ? ORDER BY created_ns`, h.clock.Now().UnixNano(), job.JobID, job.JobID+"-"+label+"-%")
	}
	exists := func(attemptID string) bool {
		t.Helper()
		var found int
		if err := h.store.db.QueryRow("SELECT COUNT(*) FROM attempts WHERE attempt_id=?", attemptID).Scan(&found); err != nil {
			t.Fatal(err)
		}
		return found == 1
	}
	attempts := func() int {
		t.Helper()
		var count int
		if err := h.store.db.QueryRow("SELECT COUNT(*) FROM attempts WHERE job_id=?", job.JobID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	// added: attempts the call itself creates.
	prunedBy := func(call string, added int, run func()) int {
		t.Helper()
		before := attempts()
		run()
		pruned := before + added - attempts()
		if pruned > 2*hotPathSummaryRows {
			t.Fatalf("the %s pruned %d attempt summaries, over its bound of %d", call, pruned, 2*hotPathSummaryRows)
		}
		return pruned
	}

	// The append trims every trimmed attempt's row and prunes the oldest of
	// them, and the attempts just below the floor.
	appended := prunedBy("log append", 0, func() { appendOneHotPathLog(t, h, job, claim, 0) })
	if exists(nodeAttemptID(job.JobID, "trimmed", 1)) || exists(nodeAttemptID(job.JobID, "trimmed", hotPathSummaryRows)) ||
		!exists(nodeAttemptID(job.JobID, "trimmed", hotPathSummaryRows+1)) {
		t.Fatal("the log append did not prune exactly the oldest trimmed attempts")
	}
	if exists(nodeAttemptID(job.JobID, "upper", upper-DefaultServiceAttemptSummaries)) ||
		!exists(nodeAttemptID(job.JobID, "upper", upper-DefaultServiceAttemptSummaries+1)) {
		t.Fatal("the log append did not prune the attempt just below the floor, or pruned the floor")
	}
	completed := prunedBy("completion", 0, func() { completeHotPathAttempt(t, h, job, claim) })
	// The completed attempt stays current until the next claim, which moves
	// it into the floor and the floor's oldest out of it, and prunes that at
	// once.
	awaitRestart(t, h, node, job)
	claimed := prunedBy("claim", 1, func() { claimHotPathService(t, h, node, job) })
	if exists(nodeAttemptID(job.JobID, "upper", upper-DefaultServiceAttemptSummaries+1)) {
		t.Fatal("the claim did not prune the attempt it moved off the floor")
	}
	if !exists(nodeAttemptID(job.JobID, "lost", 1)) {
		t.Fatal("a lost attempt inside its late-evidence window was pruned")
	}
	t.Logf("pruned %d on the append, %d on the completion and %d on the claim", appended, completed, claimed)

	// The sweep finds the rest.
	var swept int64
	for pass := 0; pass < 8; pass++ {
		result, err := h.store.Reconcile(ctx)
		if err != nil {
			t.Fatal(err)
		}
		swept += result.PrunedAttempts
	}
	assertNothingLeftForTheOldSweep(t, h.store)
	// Exactly what the whole-history prune keeps, listed independently of
	// any pruning code: the current attempt, the floor (the completed
	// attempt and the newest upper ones), the attempts held by their rows and
	// the lost one.
	want := map[string]bool{claim.Lease.AttemptID: true, nodeAttemptID(job.JobID, "lost", 1): true}
	for index := upper - DefaultServiceAttemptSummaries + 2; index <= upper; index++ {
		want[nodeAttemptID(job.JobID, "upper", index)] = true
	}
	for index := 1; index <= held; index++ {
		want[nodeAttemptID(job.JobID, "held", index)] = true
	}
	rows, err := h.store.db.Query("SELECT attempt_id FROM attempts WHERE job_id=? AND attempt_id<>(SELECT current_attempt_id FROM jobs WHERE job_id=?)", job.JobID, job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	left, err := scanJobIDs(rows, "retained attempt")
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != len(want) {
		t.Fatalf("%d attempts besides the current one are left, want %d", len(left), len(want))
	}
	for _, attemptID := range left {
		if !want[attemptID] {
			t.Fatalf("attempt %s is left, which the whole-history prune removes", attemptID)
		}
	}
	if total := int64(appended+completed+claimed) + swept; total != trimmed+deep+shallow+upper-DefaultServiceAttemptSummaries+1 {
		t.Fatalf("pruned %d in all, want %d", total, trimmed+deep+shallow+upper-DefaultServiceAttemptSummaries+1)
	}
}

// Services bound to a node whose liveness does not change, services on a
// dead node that already lost their health, and services whose stability
// window has not run out must cost a reconcile pass nothing, however many
// there are.
func TestReconcileServiceLivenessReadsFlatAsServicesGrow(t *testing.T) {
	pagesWith := func(services int) int {
		store, clock := openRetentionSweepStore(t, StoreOptions{})
		retentionSweepExec(t, store, `INSERT INTO nodes(node_id, identity_node_id, boot_session_id, os, architecture,
			agent_version, capabilities_json, state, last_heartbeat_ns, max_oneshot_slots, max_service_slots)
			VALUES('gone-node', 'gone-node', 'boot', 'linux', 'arm64', 'test', '{}', ?, 0, 4, 4)`, contract.NodeDead)
		seedIdleServices(t, store, "svc", services)
		// Half the services are healthy on the sweep node, one in a hundred
		// of them with a failure streak its stability window has not yet
		// reset; the other half are bound to the dead node.
		retentionSweepExec(t, store, `UPDATE service_jobs SET
			bound_node_id=CASE WHEN CAST(substr(job_id, 5) AS INTEGER)%2=0 THEN ? ELSE 'gone-node' END,
			healthy_since_ns=CASE WHEN CAST(substr(job_id, 5) AS INTEGER)%2=0 THEN ? END,
			restart_streak=CASE WHEN CAST(substr(job_id, 5) AS INTEGER)%100=0 THEN 2 ELSE 0 END
			WHERE job_id LIKE 'svc-%'`, retentionSweepNodeID, clock.Now().UnixNano())
		if _, err := store.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		oneConnection(store)
		countedPass(t, store)
		_, pages := countedPass(t, store)
		return pages
	}
	few, many := pagesWith(4000), pagesWith(40000)
	t.Logf("a reconcile pass read %d pages over 4000 services and %d over 40000", few, many)
	if many > few+few/2 {
		t.Fatalf("a reconcile pass read %d pages over 40000 services against %d over 4000", many, few)
	}
}

type livenessNode struct {
	node     Node
	services []string
}

// livenessStore opens a store whose only nodes are the test's, with
// deterministic restart delays so that two stores given the same steps stay
// the same.
func livenessStore(t *testing.T) (*Store, *fakeClock) {
	t.Helper()
	store, clock := openRetentionSweepStore(t, StoreOptions{
		LeaseDuration: time.Hour, NodeStaleAfter: 45 * time.Second, NodeDeadAfter: 10 * time.Minute,
		ServiceStabilityWindow: 30 * time.Second, Jitter: func(delay time.Duration) time.Duration { return delay },
	})
	retentionSweepExec(t, store, "DELETE FROM nodes WHERE node_id=?", retentionSweepNodeID)
	return store, clock
}

// registerLivenessNode registers nodeID under boot and gives it five
// services, each running a current attempt on it and healthy: one with no
// streak, one with a streak not yet due a reset, and three with a streak
// healthy for the whole window, of which one is desired stopped and one has
// an expired lease, so only the first is due.
func registerLivenessNode(t *testing.T, store *Store, clock *fakeClock, nodeID, boot string) livenessNode {
	t.Helper()
	node := registerStoreNode(t, store, clock, nodeID, boot)
	seedIdleServices(t, store, "svc-"+nodeID, 5)
	entry := livenessNode{node: node}
	window := store.serviceStabilityWindow
	for index, streak := range []int{0, 2, 2, 2, 2} {
		jobID := seededServiceID("svc-"+nodeID, index+1)
		healthy := clock.Now()
		if index >= 2 {
			healthy = healthy.Add(-window)
		}
		attemptID := fmt.Sprintf("%s-run-%s", jobID, boot)
		runOnNode(t, store, clock, jobID, node, attemptID)
		retentionSweepExec(t, store, "UPDATE service_jobs SET restart_streak=?, healthy_since_ns=? WHERE job_id=?",
			streak, healthy.UnixNano(), jobID)
		switch index {
		case 3:
			retentionSweepExec(t, store, "UPDATE service_jobs SET desired_state=? WHERE job_id=?", contract.ServiceDesiredStopped, jobID)
		case 4:
			retentionSweepExec(t, store, "UPDATE attempts SET lease_expires_ns=? WHERE attempt_id=?", clock.Now().UnixNano()-1, attemptID)
		}
		entry.services = append(entry.services, jobID)
	}
	return entry
}

func registerStoreNode(t *testing.T, store *Store, clock *fakeClock, nodeID, boot string) Node {
	t.Helper()
	node, err := store.RegisterNode(context.Background(), fabric.Identity{NodeID: "agent-" + nodeID}, contract.NodeRegistration{
		NodeID: nodeID, BootSessionID: boot, RootInstanceID: "root-" + nodeID,
		OS: "linux", Architecture: "arm64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true}, CapabilityRevision: 1, CapabilityObservedAt: clock.Now(), MissingCapabilities: []string{},
	}, DefaultNodePolicy("service"), true)
	if err != nil {
		t.Fatal(err)
	}
	return node
}

// runOnNode makes attemptID the running current attempt of jobID on node,
// with a lease far ahead, bound to node.
func runOnNode(t *testing.T, store *Store, clock *fakeClock, jobID string, node Node, attemptID string) {
	t.Helper()
	now := clock.Now().UnixNano()
	retentionSweepExec(t, store, `INSERT INTO attempts(attempt_id, job_id, node_id, boot_session_id, state, fencing_token,
		lease_expires_ns, authority_generation, created_ns, updated_ns) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		attemptID, jobID, node.NodeID, node.BootSessionID, contract.AttemptRunning, attemptID+"-fence",
		clock.Now().Add(24*time.Hour).UnixNano(), node.AuthorityGeneration, now, now)
	retentionSweepExec(t, store, "UPDATE jobs SET state=?, current_attempt_id=? WHERE job_id=?", contract.JobRunning, attemptID, jobID)
	retentionSweepExec(t, store, "UPDATE service_jobs SET bound_node_id=?, healthy_since_ns=NULL WHERE job_id=?", node.NodeID, jobID)
}

// renewOnNode renews the lease of the service's attempt under boot, which
// establishes its health while its node is alive.
func renewOnNode(t *testing.T, store *Store, nodeID, jobID, boot string) {
	t.Helper()
	attemptID := fmt.Sprintf("%s-run-%s", jobID, boot)
	if _, err := store.RenewLease(context.Background(), "agent-"+nodeID, jobID, attemptID, attemptID+"-fence"); err != nil {
		t.Fatal(err)
	}
}

type serviceLiveness struct {
	healthy bool
	streak  int
}

func readServiceLiveness(t *testing.T, store *Store, jobID string) serviceLiveness {
	t.Helper()
	var state serviceLiveness
	if err := store.db.QueryRow("SELECT healthy_since_ns IS NOT NULL, restart_streak FROM service_jobs WHERE job_id=?", jobID).
		Scan(&state.healthy, &state.streak); err != nil {
		t.Fatal(err)
	}
	return state
}

// A clock step back can return a stale node's heartbeat to the very value the
// last pass saw. Health a recovery establishes in between must still be
// interrupted when the node turns stale again, and must not reset the
// streak once it recovers (#942 review).
func TestServiceHealthIsInterruptedAfterAClockStepRepeatsAStaleHeartbeat(t *testing.T) {
	store, clock := livenessStore(t)
	ctx := context.Background()
	entry := registerLivenessNode(t, store, clock, "stepped", "boot-1")
	reconcile := func() {
		t.Helper()
		if _, err := store.Reconcile(ctx); err != nil {
			t.Fatal(err)
		}
	}
	reconcile()
	heartbeat := clock.Now()
	clock.Advance(time.Minute)
	reconcile()
	service := entry.services[1]
	if got := readServiceLiveness(t, store, service); got != (serviceLiveness{false, 2}) {
		t.Fatalf("service on the stale node = %+v, want its health interrupted", got)
	}
	// The clock steps back to the stale heartbeat: the node recovers at that
	// same instant and a renewal establishes health again.
	clock.Advance(heartbeat.Sub(clock.Now()))
	if _, err := store.HeartbeatNode(ctx, "agent-stepped", "stepped", entry.node.BootSessionID); err != nil {
		t.Fatal(err)
	}
	renewOnNode(t, store, "stepped", service, "boot-1")
	if !readServiceLiveness(t, store, service).healthy {
		t.Fatal("the renewal did not establish health")
	}
	clock.Advance(time.Minute)
	reconcile()
	if got := readServiceLiveness(t, store, service); got != (serviceLiveness{false, 2}) {
		t.Fatalf("service on the node stale again at the same heartbeat = %+v, want its health interrupted", got)
	}
	if _, err := store.HeartbeatNode(ctx, "agent-stepped", "stepped", entry.node.BootSessionID); err != nil {
		t.Fatal(err)
	}
	reconcile()
	if got := readServiceLiveness(t, store, service); got != (serviceLiveness{false, 2}) {
		t.Fatalf("service after the node recovered = %+v, want no health and its streak kept", got)
	}
}

// formerServiceLiveness is the service-liveness step of every reconcile pass
// before #942, copied from main 6b844f59 and independent of the production
// statements: interruption, then the streak reset, each over every service.
func formerServiceLiveness(window time.Duration) func(context.Context, *sql.Tx, time.Time) (int64, error) {
	return func(ctx context.Context, tx *sql.Tx, now time.Time) (int64, error) {
		if _, err := tx.ExecContext(ctx, `UPDATE service_jobs SET healthy_since_ns=NULL
			WHERE healthy_since_ns IS NOT NULL
				AND EXISTS (
					SELECT 1 FROM jobs
					JOIN attempts ON attempts.attempt_id=jobs.current_attempt_id
					JOIN nodes ON nodes.node_id=attempts.node_id
					WHERE jobs.job_id=service_jobs.job_id
						AND service_jobs.bound_node_id=attempts.node_id
						AND (nodes.state<>?
							OR attempts.boot_session_id<>nodes.boot_session_id
							OR attempts.authority_generation<>nodes.authority_generation)
				)`, contract.NodeAlive); err != nil {
			return 0, err
		}
		resets, err := tx.ExecContext(ctx, `UPDATE service_jobs SET restart_streak=0
			WHERE restart_streak>0
				AND desired_state=?
				AND healthy_since_ns IS NOT NULL
				AND healthy_since_ns<=?
				AND EXISTS (
					SELECT 1 FROM jobs
					JOIN attempts ON attempts.attempt_id=jobs.current_attempt_id
					JOIN nodes ON nodes.node_id=attempts.node_id
					WHERE jobs.job_id=service_jobs.job_id
						AND jobs.state=?
						AND attempts.state=?
						AND attempts.lease_expires_ns>?
						AND nodes.state=?
						AND attempts.boot_session_id=nodes.boot_session_id
						AND attempts.authority_generation=nodes.authority_generation
						AND service_jobs.bound_node_id=attempts.node_id
				)`, contract.ServiceDesiredRunning, now.Add(-window).UnixNano(),
			contract.JobRunning, contract.AttemptRunning, now.UnixNano(), contract.NodeAlive)
		if err != nil {
			return 0, err
		}
		return resets.RowsAffected()
	}
}

// livenessWorld is one store carried through a sequence of node changes.
type livenessWorld struct {
	store *Store
	clock *fakeClock
	nodes map[string]livenessNode
}

type storedLiveness struct {
	healthySince sql.NullInt64
	streak       int
}

// serviceLivenessRows reads every service's health and streak by dispatch
// key, which both twins share.
func serviceLivenessRows(t *testing.T, store *Store) map[string]storedLiveness {
	t.Helper()
	rows, err := store.db.Query(`SELECT jobs.dispatch_key, service_jobs.healthy_since_ns, service_jobs.restart_streak
		FROM service_jobs JOIN jobs ON jobs.job_id=service_jobs.job_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	services := map[string]storedLiveness{}
	for rows.Next() {
		var key string
		var stored storedLiveness
		if err := rows.Scan(&key, &stored.healthySince, &stored.streak); err != nil {
			t.Fatal(err)
		}
		services[key] = stored
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return services
}

// Two stores built alike take the same steps through every way a node can
// change: one runs the bounded liveness step, the other the statements every
// pass ran over every service before #942. After each pass, every service's
// health and streak must be the same in both, and the bounded one must apply
// each change on the pass that sees it.
func TestServiceLivenessPassesMatchTheFullStatements(t *testing.T) {
	nodeIDs := []string{"steady", "drained", "replaced", "silent", "flapping", "stepped"}
	var worlds [2]*livenessWorld
	for index := range worlds {
		store, clock := livenessStore(t)
		if index == 1 {
			store.reconcileLivenessForTest = formerServiceLiveness(store.serviceStabilityWindow)
		}
		world := &livenessWorld{store: store, clock: clock, nodes: map[string]livenessNode{}}
		for _, nodeID := range nodeIDs {
			world.nodes[nodeID] = registerLivenessNode(t, store, clock, nodeID, "boot-1")
		}
		worlds[index] = world
	}
	bounded := worlds[0]
	ctx := context.Background()
	each := func(step func(world *livenessWorld)) {
		t.Helper()
		for _, world := range worlds {
			step(world)
		}
	}
	pass := 0
	reconcile := func() ReconcileResult {
		t.Helper()
		pass++
		var results [2]ReconcileResult
		for index, world := range worlds {
			result, err := world.store.Reconcile(ctx)
			if err != nil {
				t.Fatal(err)
			}
			results[index] = result
		}
		if results[0].RestartStreakResets != results[1].RestartStreakResets || results[0].StaleNodes != results[1].StaleNodes ||
			results[0].DeadNodes != results[1].DeadNodes || results[0].ExpiredAttempts != results[1].ExpiredAttempts {
			t.Fatalf("pass %d: bounded result %+v, former %+v", pass, results[0], results[1])
		}
		got, want := serviceLivenessRows(t, worlds[0].store), serviceLivenessRows(t, worlds[1].store)
		if len(got) != len(want) {
			t.Fatalf("pass %d: %d services against %d", pass, len(got), len(want))
		}
		for key, former := range want {
			if got[key] != former {
				t.Errorf("pass %d: service %s health and streak %+v, the former statements leave %+v", pass, key, got[key], former)
			}
		}
		if t.Failed() {
			t.FailNow()
		}
		return results[0]
	}
	heartbeat := func(nodeIDs ...string) {
		t.Helper()
		each(func(world *livenessWorld) {
			for _, nodeID := range nodeIDs {
				if _, err := world.store.HeartbeatNode(ctx, "agent-"+nodeID, nodeID, world.nodes[nodeID].node.BootSessionID); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
	advance := func(duration time.Duration) { each(func(world *livenessWorld) { world.clock.Advance(duration) }) }
	expect := func(nodeID string, index int, want serviceLiveness) {
		t.Helper()
		if got := readServiceLiveness(t, bounded.store, bounded.nodes[nodeID].services[index]); got != want {
			t.Fatalf("pass %d: service %d on %s = %+v, want %+v", pass, index, nodeID, got, want)
		}
	}

	// The first pass resets each due streak; the stopped service and the one
	// whose lease expired keep theirs.
	if result := reconcile(); result.RestartStreakResets != int64(len(nodeIDs)) {
		t.Fatalf("first pass reset %d streaks, want the %d due", result.RestartStreakResets, len(nodeIDs))
	}
	expect("steady", 2, serviceLiveness{true, 0})
	expect("steady", 3, serviceLiveness{true, 2})

	// A drain, a new boot session and a node that stops heartbeating: their
	// services lose health on the pass that sees it, and the remaining
	// streaks elsewhere run out on time.
	each(func(world *livenessWorld) {
		if _, err := world.store.DrainNode(ctx, "agent-drained", "drained", world.nodes["drained"].node.BootSessionID); err != nil {
			t.Fatal(err)
		}
		replaced := world.nodes["replaced"]
		replaced.node = registerStoreNode(t, world.store, world.clock, "replaced", "boot-2")
		world.nodes["replaced"] = replaced
	})
	advance(time.Minute)
	heartbeat("steady", "drained", "replaced", "flapping", "stepped")
	if result := reconcile(); result.StaleNodes != 1 {
		t.Fatalf("stale nodes = %d, want the silent one", result.StaleNodes)
	}
	expect("steady", 1, serviceLiveness{true, 0})
	for _, nodeID := range []string{"drained", "replaced", "silent"} {
		expect(nodeID, 1, serviceLiveness{false, 2})
	}

	// The flapping node turns stale, recovers, regains health through a
	// renewal, and turns stale again through its own claim, all between two
	// passes.
	advance(time.Minute)
	heartbeat("steady", "drained", "replaced", "stepped")
	reconcile()
	expect("flapping", 0, serviceLiveness{false, 0})
	heartbeat("flapping")
	each(func(world *livenessWorld) {
		renewOnNode(t, world.store, "flapping", world.nodes["flapping"].services[0], "boot-1")
	})
	expect("flapping", 0, serviceLiveness{true, 0})
	advance(time.Minute)
	heartbeat("steady", "drained", "replaced", "stepped")
	each(func(world *livenessWorld) {
		if _, err := world.store.ClaimJob(ctx, "agent-flapping", "flapping", world.nodes["flapping"].node.BootSessionID, contract.JobClassService); err == nil {
			t.Fatal("a claim on a node past its heartbeat succeeded")
		}
	})
	reconcile()
	expect("flapping", 0, serviceLiveness{false, 0})

	// The stepped node turns stale; the clock steps back to its last
	// heartbeat, where it recovers and a renewal regains health on a service
	// with a streak; then it turns stale again with the same heartbeat.
	steppedHeartbeat := bounded.clock.Now()
	advance(time.Minute)
	heartbeat("steady", "drained", "replaced")
	reconcile()
	expect("stepped", 1, serviceLiveness{false, 0})
	each(func(world *livenessWorld) {
		retentionSweepExec(t, world.store, "UPDATE service_jobs SET restart_streak=3 WHERE job_id=?", world.nodes["stepped"].services[1])
	})
	stepBack := steppedHeartbeat.Sub(bounded.clock.Now())
	advance(stepBack)
	heartbeat("stepped")
	each(func(world *livenessWorld) {
		renewOnNode(t, world.store, "stepped", world.nodes["stepped"].services[1], "boot-1")
	})
	advance(-stepBack + time.Minute)
	heartbeat("steady", "drained", "replaced")
	reconcile()
	expect("stepped", 1, serviceLiveness{false, 3})
	heartbeat("stepped")
	advance(time.Minute)
	heartbeat("steady", "drained", "replaced", "stepped")
	reconcile()
	expect("stepped", 1, serviceLiveness{false, 3})

	// The silent node dies and returns under a new boot session; a service
	// runs on it again, regains health and resets its streak on time.
	advance(10 * time.Minute)
	heartbeat("steady", "drained", "replaced", "flapping", "stepped")
	if result := reconcile(); result.DeadNodes != 1 {
		t.Fatalf("dead nodes = %d, want the silent one", result.DeadNodes)
	}
	each(func(world *livenessWorld) {
		silent := world.nodes["silent"]
		silent.node = registerStoreNode(t, world.store, world.clock, "silent", "boot-2")
		world.nodes["silent"] = silent
		runOnNode(t, world.store, world.clock, silent.services[1], silent.node, silent.services[1]+"-run-boot-2")
		renewOnNode(t, world.store, "silent", silent.services[1], "boot-2")
	})
	reconcile()
	expect("silent", 1, serviceLiveness{true, 2})
	advance(time.Minute)
	heartbeat("steady", "drained", "replaced", "flapping", "stepped", "silent")
	if result := reconcile(); result.RestartStreakResets != 1 {
		t.Fatalf("resets = %d, want the service stable on the returned node", result.RestartStreakResets)
	}
	expect("silent", 1, serviceLiveness{true, 0})
}

// The streak reset reads only the services due for it, and an interruption
// only one node's healthy services, each through its index.
func TestServiceLivenessStatementsUseTheirIndexes(t *testing.T) {
	store, _ := openRetentionSweepStore(t, StoreOptions{})
	for _, check := range []struct {
		query string
		args  []any
		want  string
	}{
		{serviceStabilityResetSQL, []any{"running", 0, "running", "running", 0, "alive"},
			"SEARCH service_jobs USING INDEX service_jobs_stability_due (healthy_since_ns>? AND healthy_since_ns<?)"},
		{interruptNodeServiceStabilitySQL, []any{"node", "alive"},
			"SEARCH service_jobs USING INDEX service_jobs_healthy_node (bound_node_id=?)"},
	} {
		rows, err := store.db.Query("EXPLAIN QUERY PLAN "+check.query, check.args...)
		if err != nil {
			t.Fatal(err)
		}
		var plan []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			plan = append(plan, detail)
		}
		rows.Close()
		if len(plan) == 0 || plan[0] != check.want {
			t.Fatalf("plan = %q, want %q first", plan, check.want)
		}
	}
}
