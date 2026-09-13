package l1

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// removalStallHarness drives one bound service to removal_pending and hands
// back everything a stall declaration needs. Every test below starts from the
// exact position the first Mac Computer run wedged in: a standing directive
// the node holds and cannot complete.
type removalStallHarness struct {
	h         *integrationHarness
	client    fabric.Identity
	agent     fabric.Identity
	node      Node
	job       Job
	directive RemovalDirective
}

func newRemovalStallHarness(t *testing.T) removalStallHarness {
	t.Helper()
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{
		"node-1": {Tags: []string{"service"}, MaxOneshotSlots: DefaultMaxOneshotSlots, MaxServiceSlots: 1},
	})
	client := fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}}
	agent := fabric.Identity{NodeID: "fabric-agent", Tags: []string{DefaultAgentPrincipalTag}}
	clientClient := h.client(client)
	agentClient := h.client(agent)
	node := h.register(agentClient, "node-1")
	job := submitRemovalService(t, h, clientClient, removalServiceSpec("stalls-on-removal", []string{"service"}))
	claimRestartService(t, h, agentClient, node)

	status, _, body := h.do(clientClient, http.MethodPost, "/v1/jobs/"+job.JobID+"/remove?class=service", nil)
	if status != http.StatusAccepted {
		t.Fatalf("remove status = %d body=%s", status, body)
	}
	directives, err := h.store.ListNodeRemovalDirectives(context.Background(), "fabric-agent", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("removal directives = %#v, %v", directives, err)
	}
	return removalStallHarness{h: h, client: client, agent: agent, node: node, job: job, directive: directives[0]}
}

// advancePastStallBound walks the clock to the bound while the node keeps
// heartbeating. A node that simply went quiet for ten minutes is a different
// failure, and it must not be the one these tests accidentally exercise.
func (harness removalStallHarness) advancePastStallBound(t *testing.T) {
	t.Helper()
	agentClient := harness.h.client(harness.agent)
	for elapsed := time.Duration(0); elapsed < DefaultRemovalStallBound; elapsed += DefaultNodeStaleAfter {
		harness.h.clock.Advance(DefaultNodeStaleAfter)
		status, _, body := harness.h.do(agentClient, http.MethodPost,
			"/v1/agent/nodes/"+harness.node.NodeID+"/heartbeat", heartbeatRequestForNode(harness.node))
		if status != http.StatusOK {
			t.Fatalf("heartbeat while waiting out the stall bound: status=%d body=%s", status, body)
		}
	}
}

func (harness removalStallHarness) declaration(key string) RemovalAcknowledgementRequest {
	prepared := harness.h.clock.Now().UTC()
	return RemovalAcknowledgementRequest{
		NodeID: harness.node.NodeID, BootSessionID: harness.node.BootSessionID,
		RemovalGeneration: harness.directive.RemovalGeneration, CleanupFence: harness.directive.CleanupFence,
		RootInstanceID: harness.directive.RootInstanceID,
		IdempotencyKey: ServiceRemovalStallKeyPrefix + key,
		CleanupStall: &ServiceRemovalStallEvidence{
			Kind: ServiceRemovalStallEvidenceKind, JobID: harness.job.JobID, NodeID: harness.node.NodeID,
			RemovalGeneration: harness.directive.RemovalGeneration,
			CleanupFence:      harness.directive.CleanupFence, Phase: "prepared",
			LastRefusalCode:   "unauthorized_attempt",
			LastRefusalDetail: "attempt authority does not match a live attempt",
			Attempts:          4, PreparedAt: prepared, LastAttemptedAt: prepared.Add(time.Minute),
		},
	}
}

func (harness removalStallHarness) declare(t *testing.T, request RemovalAcknowledgementRequest) (Job, error) {
	t.Helper()
	return harness.h.store.AcknowledgeServiceRemoval(context.Background(), "fabric-agent", harness.job.JobID, request)
}

// TestStalledRemovalReleasesTheSlotSoAFreshServiceCanClaimIt is the whole
// point of the outcome. On main the node's only service slot stays pinned by
// removal_pending forever and the waiting service never places.
func TestStalledRemovalReleasesTheSlotSoAFreshServiceCanClaimIt(t *testing.T) {
	harness := newRemovalStallHarness(t)
	h, agentClient := harness.h, harness.h.client(harness.agent)
	waiting := submitRemovalService(t, h, h.client(harness.client), removalServiceSpec("waits-for-stalled-slot", []string{"service"}))

	status, _, body := h.do(agentClient, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{
		NodeID: harness.node.NodeID, BootSessionID: harness.node.BootSessionID, Class: contract.JobClassService,
	})
	if status != http.StatusNoContent {
		t.Fatalf("removal_pending released the service Slot early: status=%d body=%s", status, body)
	}

	harness.advancePastStallBound(t)
	stalled, err := harness.declare(t, harness.declaration("slot-release"))
	if err != nil {
		t.Fatalf("declare stalled removal: %v", err)
	}
	if stalled.State != contract.JobStalledCleanupUnverified || stalled.Removal == nil ||
		stalled.Removal.RemovalOutcome != ServiceRemovalOutcomeCleanupStalled ||
		stalled.Removal.CleanupStatus != ServiceRemovalCleanupPending || stalled.Removal.StalledAt == nil {
		t.Fatalf("stalled projection = %#v", stalled)
	}
	if stalled.Removal.Stall == nil || stalled.Removal.Stall.LastRefusalCode != "unauthorized_attempt" ||
		stalled.Removal.Stall.Attempts != 4 {
		t.Fatalf("stalled evidence = %#v", stalled.Removal.Stall)
	}
	if stalled.Removal.CleanupAcknowledgedAt != nil {
		t.Fatal("a stalled removal must never report cleanup as acknowledged")
	}

	status, _, body = h.do(agentClient, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{
		NodeID: harness.node.NodeID, BootSessionID: harness.node.BootSessionID, Class: contract.JobClassService,
	})
	if status != http.StatusOK {
		t.Fatalf("stalled removal did not release the service Slot: status=%d body=%s", status, body)
	}
	var next Claim
	if err := json.Unmarshal(body, &next); err != nil {
		t.Fatal(err)
	}
	if next.Job.JobID != waiting.JobID {
		t.Fatalf("post-stall claim = %q, want waiting service %q", next.Job.JobID, waiting.JobID)
	}
}

// TestStallDeclarationIsRefusedBeforeTheTenMinuteBound proves the bound is
// L1's, measured on L1's own clock against its durable request time.
func TestStallDeclarationIsRefusedBeforeTheTenMinuteBound(t *testing.T) {
	harness := newRemovalStallHarness(t)
	harness.h.clock.Advance(DefaultRemovalStallBound - time.Second)
	if _, err := harness.declare(t, harness.declaration("too-early")); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("early stall declaration error = %v, want conflict", err)
	}
	var state contract.JobState
	if err := harness.h.store.db.QueryRow(`SELECT state FROM jobs WHERE job_id=?`, harness.job.JobID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != contract.JobRemovalPending {
		t.Fatalf("state after a refused declaration = %q, want removal_pending", state)
	}
	harness.h.clock.Advance(time.Second)
	if _, err := harness.declare(t, harness.declaration("on-time")); err != nil {
		t.Fatalf("declaration exactly at the bound: %v", err)
	}
}

// TestStallDeclarationRequiresTypedNonCompletionEvidence keeps the outcome
// from becoming a way to drop a Slot without saying why.
func TestStallDeclarationRequiresTypedNonCompletionEvidence(t *testing.T) {
	for name, mutate := range map[string]func(*RemovalAcknowledgementRequest){
		"unknown kind":         func(r *RemovalAcknowledgementRequest) { r.CleanupStall.Kind = "something_else" },
		"no refusal code":      func(r *RemovalAcknowledgementRequest) { r.CleanupStall.LastRefusalCode = "" },
		"too few attempts":     func(r *RemovalAcknowledgementRequest) { r.CleanupStall.Attempts = 2 },
		"no attempt time":      func(r *RemovalAcknowledgementRequest) { r.CleanupStall.LastAttemptedAt = time.Time{} },
		"another job":          func(r *RemovalAcknowledgementRequest) { r.CleanupStall.JobID = "job_other" },
		"completion key space": func(r *RemovalAcknowledgementRequest) { r.IdempotencyKey = "removal:not-a-stall" },
	} {
		t.Run(name, func(t *testing.T) {
			harness := newRemovalStallHarness(t)
			harness.advancePastStallBound(t)
			request := harness.declaration("typed-evidence")
			mutate(&request)
			if _, err := harness.declare(t, request); errorCode(err) != contract.ErrorInvalidRequest {
				t.Fatalf("declaration error = %v, want invalid_request", err)
			}
		})
	}
	t.Run("another directive generation", func(t *testing.T) {
		harness := newRemovalStallHarness(t)
		harness.advancePastStallBound(t)
		request := harness.declaration("stale-fence")
		request.CleanupStall.CleanupFence = "cleanup_other"
		if _, err := harness.declare(t, request); errorCode(err) != contract.ErrorStaleFence {
			t.Fatalf("declaration error = %v, want stale_fence", err)
		}
	})
}

// TestStallDeclarationReplayMatchesKeyAndBodyOrConflicts holds the declaration
// to the same idempotency rules as the completion acknowledgement.
func TestStallDeclarationReplayMatchesKeyAndBodyOrConflicts(t *testing.T) {
	harness := newRemovalStallHarness(t)
	harness.advancePastStallBound(t)
	request := harness.declaration("replay")
	first, err := harness.declare(t, request)
	if err != nil {
		t.Fatalf("declare stalled removal: %v", err)
	}
	replayed, err := harness.declare(t, request)
	if err != nil || replayed.State != first.State || replayed.Removal.StalledAt == nil ||
		!replayed.Removal.StalledAt.Equal(*first.Removal.StalledAt) {
		t.Fatalf("identical replay = %#v, %v", replayed, err)
	}
	changed := harness.declaration("replay")
	changed.CleanupStall.Attempts = 9
	if _, err := harness.declare(t, changed); errorCode(err) != contract.ErrorIdempotencyConflict {
		t.Fatalf("changed-evidence replay error = %v, want idempotency_conflict", err)
	}
	differentKey := harness.declaration("replay-other-key")
	if _, err := harness.declare(t, differentKey); errorCode(err) != contract.ErrorIdempotencyConflict {
		t.Fatalf("different-key replay error = %v, want idempotency_conflict", err)
	}
}

// TestStalledRemovalKeepsItsStandingDirectiveForAReturningNode is what makes
// the outcome honest: the Slot is released, the deletion is not cancelled.
func TestStalledRemovalKeepsItsStandingDirectiveForAReturningNode(t *testing.T) {
	harness := newRemovalStallHarness(t)
	harness.advancePastStallBound(t)
	if _, err := harness.declare(t, harness.declaration("directive-stands")); err != nil {
		t.Fatalf("declare stalled removal: %v", err)
	}
	directives, err := harness.h.store.ListNodeRemovalDirectives(context.Background(), "fabric-agent",
		harness.node.NodeID, harness.node.BootSessionID)
	if err != nil || len(directives) != 1 || directives[0] != harness.directive {
		t.Fatalf("directives after a stall = %#v, %v; want the standing %#v", directives, err, harness.directive)
	}
}

// TestLateCleanupAfterAStallNeverUpgradesTheUnverifiedOutcome mirrors the rule
// force-forget already holds: proof that arrives after the Slot was released
// is recorded, but it cannot retell how the removal ended.
func TestLateCleanupAfterAStallNeverUpgradesTheUnverifiedOutcome(t *testing.T) {
	harness := newRemovalStallHarness(t)
	harness.advancePastStallBound(t)
	if _, err := harness.declare(t, harness.declaration("late-cleanup")); err != nil {
		t.Fatalf("declare stalled removal: %v", err)
	}
	completion := RemovalAcknowledgementRequest{
		NodeID: harness.node.NodeID, BootSessionID: harness.node.BootSessionID,
		RemovalGeneration: harness.directive.RemovalGeneration, CleanupFence: harness.directive.CleanupFence,
		RootInstanceID: harness.directive.RootInstanceID, IdempotencyKey: "removal:late-cleanup",
	}
	cleaned, err := harness.declare(t, completion)
	if err != nil {
		t.Fatalf("late completion acknowledgement: %v", err)
	}
	if cleaned.State != contract.JobStalledCleanupUnverified || cleaned.Removal.CleanupAcknowledgedAt == nil {
		t.Fatalf("late completion projection = %#v", cleaned)
	}
	finalized, done, err := harness.h.store.FinalizeServiceRemoval(context.Background(), harness.job.JobID)
	if err != nil || !done {
		t.Fatalf("finalize after a late cleanup = %#v, %t, %v", finalized, done, err)
	}
	if finalized.State != contract.JobStalledCleanupUnverified ||
		finalized.Removal == nil || finalized.Removal.RemovalOutcome != ServiceRemovalOutcomeCleanupStalled {
		t.Fatalf("finalized projection = %#v", finalized)
	}
	var outcome string
	if err := harness.h.store.db.QueryRow(`SELECT outcome FROM service_tombstones WHERE job_id=?`, harness.job.JobID).
		Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	if outcome != string(ServiceRemovalOutcomeCleanupStalled) {
		t.Fatalf("tombstone outcome = %q, want cleanup_stalled", outcome)
	}
}

// TestAQuarantinedCleanupCannotAlsoBeDeclaredStalled protects the one removal
// outcome that keeps its Slot on purpose.
func TestAQuarantinedCleanupCannotAlsoBeDeclaredStalled(t *testing.T) {
	harness := newRemovalStallHarness(t)
	if _, err := harness.h.store.db.Exec(`UPDATE service_removals SET cleanup_status=? WHERE job_id=?`,
		ServiceRemovalCleanupQuarantined, harness.job.JobID); err != nil {
		t.Fatal(err)
	}
	harness.advancePastStallBound(t)
	if _, err := harness.declare(t, harness.declaration("quarantined")); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("stall declaration over a quarantine = %v, want conflict", err)
	}
}

// TestServiceRemovalStatusMigrationAdmitsTheStalledOutcome opens a database
// whose CHECK constraints predate the outcome and proves the rebuild keeps
// every existing row while admitting the new value.
func TestServiceRemovalStatusMigrationAdmitsTheStalledOutcome(t *testing.T) {
	harness := newRemovalStallHarness(t)
	store := harness.h.store
	if _, err := store.db.Exec(`DROP TABLE service_removals_stall_migration`); err == nil {
		t.Fatal("a migration scratch table survived the rebuild")
	}
	// Rewind both tables to their pre-outcome shape, exactly as a database
	// created before this change carries them.
	for _, rewind := range []struct{ table, from, to string }{
		{"service_removals", "'removal_pending', 'agent_cleaned', 'removed_verified', 'forgotten_cleanup_unverified', 'stalled_cleanup_unverified'",
			"'removal_pending', 'agent_cleaned', 'removed_verified', 'forgotten_cleanup_unverified'"},
		{"service_tombstones", "'verified_removed', 'force_forgotten', 'cleanup_stalled'", "'verified_removed', 'force_forgotten'"},
	} {
		var current string
		if err := store.db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name=?`, rewind.table).
			Scan(&current); err != nil {
			t.Fatal(err)
		}
		narrowed, err := migratedSQLiteCreateTable(current, rewind.table+"_rewind", map[string]string{rewind.from: rewind.to})
		if err != nil {
			t.Fatal(err)
		}
		tx, err := store.db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(narrowed); err != nil {
			t.Fatal(err)
		}
		if err := copySQLiteTableColumns(context.Background(), tx, rewind.table, rewind.table+"_rewind"); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(`DROP TABLE ` + rewind.table); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(`ALTER TABLE ` + rewind.table + `_rewind RENAME TO ` + rewind.table); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.db.Exec(`UPDATE service_removals SET status=? WHERE job_id=?`,
		contract.JobStalledCleanupUnverified, harness.job.JobID); err == nil {
		t.Fatal("the pre-migration schema accepted the stalled outcome")
	}
	if err := store.migrateServiceRemovalStallConstraints(context.Background()); err != nil {
		t.Fatalf("migrate widened removal constraints: %v", err)
	}
	var preserved string
	if err := store.db.QueryRow(`SELECT status FROM service_removals WHERE job_id=?`, harness.job.JobID).
		Scan(&preserved); err != nil {
		t.Fatal(err)
	}
	if preserved != string(contract.JobRemovalPending) {
		t.Fatalf("status after migration = %q, want the row's own removal_pending", preserved)
	}
	if _, err := store.db.Exec(`UPDATE service_removals SET status=? WHERE job_id=?`,
		contract.JobStalledCleanupUnverified, harness.job.JobID); err != nil {
		t.Fatalf("migrated schema still refuses the stalled outcome: %v", err)
	}
	// A rebuild that leaves enforcement off, or that orphaned a row, is not a
	// successful migration.
	var enforced int
	if err := store.db.QueryRow(`PRAGMA foreign_keys`).Scan(&enforced); err != nil {
		t.Fatal(err)
	}
	if enforced != 1 {
		t.Fatal("the migration left foreign-key enforcement disabled")
	}
	violations, err := store.db.Query(`PRAGMA foreign_key_check`)
	if err != nil {
		t.Fatal(err)
	}
	defer violations.Close()
	if violations.Next() {
		t.Fatal("the migration left a foreign-key violation")
	}
}

// TestStalledRemovalKeepsItsBindingImagePinUntilCleanupCompletes is the other
// half of "releases the Slot but claims nothing". The pin is what the standing
// directive still needs; a reconciler that reads the binding proof must not be
// told the binding is gone while cleanup is still owed.
func TestStalledRemovalKeepsItsBindingImagePinUntilCleanupCompletes(t *testing.T) {
	harness := newRemovalStallHarness(t)
	proof := ServiceBindingProofRequest{NodeID: harness.node.NodeID, BootSessionID: harness.node.BootSessionID}
	harness.advancePastStallBound(t)
	if _, err := harness.declare(t, harness.declaration("image-pin")); err != nil {
		t.Fatalf("declare stalled removal: %v", err)
	}
	bound, err := harness.h.store.ProveServiceBinding(context.Background(), "fabric-agent", harness.job.JobID, proof)
	if err != nil || !bound {
		t.Fatalf("stalled binding proof = %t, %v; the pin would be deleted", bound, err)
	}
	// Only positive cleanup ends the obligation.
	completion := RemovalAcknowledgementRequest{
		NodeID: harness.node.NodeID, BootSessionID: harness.node.BootSessionID,
		RemovalGeneration: harness.directive.RemovalGeneration, CleanupFence: harness.directive.CleanupFence,
		RootInstanceID: harness.directive.RootInstanceID, IdempotencyKey: "removal:pin-release",
	}
	if _, err := harness.declare(t, completion); err != nil {
		t.Fatalf("late completion acknowledgement: %v", err)
	}
	bound, err = harness.h.store.ProveServiceBinding(context.Background(), "fabric-agent", harness.job.JobID, proof)
	if err != nil || bound {
		t.Fatalf("cleaned binding proof = %t, %v; the pin must be released", bound, err)
	}
}

// TestForceForgetNeverRewritesAStalledRemoval covers both shapes: the live
// stalled row, and the tombstone a late cleanup leaves behind.
func TestForceForgetNeverRewritesAStalledRemoval(t *testing.T) {
	harness := newRemovalStallHarness(t)
	harness.advancePastStallBound(t)
	if _, err := harness.declare(t, harness.declaration("force-forget")); err != nil {
		t.Fatalf("declare stalled removal: %v", err)
	}
	waived, err := harness.h.store.ForceForgetService(context.Background(), harness.job.JobID)
	if err != nil {
		t.Fatalf("force-forget over a stalled removal: %v", err)
	}
	if waived.State != contract.JobStalledCleanupUnverified ||
		waived.Removal.RemovalOutcome != ServiceRemovalOutcomeCleanupStalled {
		t.Fatalf("force-forget rewrote the terminal agent outcome: %#v", waived)
	}

	completion := RemovalAcknowledgementRequest{
		NodeID: harness.node.NodeID, BootSessionID: harness.node.BootSessionID,
		RemovalGeneration: harness.directive.RemovalGeneration, CleanupFence: harness.directive.CleanupFence,
		RootInstanceID: harness.directive.RootInstanceID, IdempotencyKey: "removal:forget-late",
	}
	if _, err := harness.declare(t, completion); err != nil {
		t.Fatalf("late completion acknowledgement: %v", err)
	}
	if _, _, err := harness.h.store.FinalizeServiceRemoval(context.Background(), harness.job.JobID); err != nil {
		t.Fatalf("finalize after a late cleanup: %v", err)
	}
	finalized, err := harness.h.store.ForceForgetService(context.Background(), harness.job.JobID)
	if err != nil {
		t.Fatalf("force-forget over a finalized stalled removal: %v", err)
	}
	if finalized.State != contract.JobStalledCleanupUnverified ||
		finalized.Removal.RemovalOutcome != ServiceRemovalOutcomeCleanupStalled {
		t.Fatalf("force-forget rewrote a finalized stalled removal: %#v", finalized)
	}
}

// TestFinalizedStalledRemovalKeepsItsEvidenceAndReplayIdentity proves the
// tombstone carries what the deleted row carried: a reader still sees why the
// Slot was released, and a replay is still matched, not merely identified.
func TestFinalizedStalledRemovalKeepsItsEvidenceAndReplayIdentity(t *testing.T) {
	harness := newRemovalStallHarness(t)
	harness.advancePastStallBound(t)
	declaration := harness.declaration("tombstone")
	if _, err := harness.declare(t, declaration); err != nil {
		t.Fatalf("declare stalled removal: %v", err)
	}
	completion := RemovalAcknowledgementRequest{
		NodeID: harness.node.NodeID, BootSessionID: harness.node.BootSessionID,
		RemovalGeneration: harness.directive.RemovalGeneration, CleanupFence: harness.directive.CleanupFence,
		RootInstanceID: harness.directive.RootInstanceID, IdempotencyKey: "removal:tombstone-late",
	}
	if _, err := harness.declare(t, completion); err != nil {
		t.Fatalf("late completion acknowledgement: %v", err)
	}
	finalized, done, err := harness.h.store.FinalizeServiceRemoval(context.Background(), harness.job.JobID)
	if err != nil || !done {
		t.Fatalf("finalize = %#v, %t, %v", finalized, done, err)
	}
	if finalized.Removal == nil || finalized.Removal.Stall == nil ||
		finalized.Removal.Stall.LastRefusalCode != "unauthorized_attempt" ||
		finalized.Removal.Stall.Attempts != 4 || finalized.Removal.StalledAt == nil {
		t.Fatalf("finalized stalled projection lost its evidence: %#v", finalized.Removal)
	}
	replayed, err := harness.declare(t, declaration)
	if err != nil || replayed.State != contract.JobStalledCleanupUnverified {
		t.Fatalf("finalized declaration replay = %#v, %v", replayed, err)
	}
	changed := harness.declaration("tombstone")
	changed.CleanupStall.Attempts = 11
	if _, err := harness.declare(t, changed); errorCode(err) != contract.ErrorIdempotencyConflict {
		t.Fatalf("finalized replay with changed evidence = %v, want idempotency_conflict", err)
	}
}

// TestCrashBetweenLateCleanupAndFinalizeStillReconcilesAStalledRemoval walks
// the transaction boundary: once acknowledged the directive stops being
// redispatched, so only recovery can finish the job.
func TestCrashBetweenLateCleanupAndFinalizeStillReconcilesAStalledRemoval(t *testing.T) {
	harness := newRemovalStallHarness(t)
	harness.advancePastStallBound(t)
	if _, err := harness.declare(t, harness.declaration("recovery")); err != nil {
		t.Fatalf("declare stalled removal: %v", err)
	}
	completion := RemovalAcknowledgementRequest{
		NodeID: harness.node.NodeID, BootSessionID: harness.node.BootSessionID,
		RemovalGeneration: harness.directive.RemovalGeneration, CleanupFence: harness.directive.CleanupFence,
		RootInstanceID: harness.directive.RootInstanceID, IdempotencyKey: "removal:recovery-late",
	}
	// The acknowledgement commits; finalization is the separate transaction a
	// crash lands between, so it is simply never called here.
	if _, err := harness.declare(t, completion); err != nil {
		t.Fatalf("late completion acknowledgement: %v", err)
	}
	directives, err := harness.h.store.ListNodeRemovalDirectives(context.Background(), "fabric-agent",
		harness.node.NodeID, harness.node.BootSessionID)
	if err != nil || len(directives) != 0 {
		t.Fatalf("acknowledged removal still redispatches: %#v, %v", directives, err)
	}
	result, err := harness.h.store.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.FinalizedRemovals != 1 {
		t.Fatalf("recovery finalized %d removals, want 1", result.FinalizedRemovals)
	}
	var outcome string
	if err := harness.h.store.db.QueryRow(`SELECT outcome FROM service_tombstones WHERE job_id=?`, harness.job.JobID).
		Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	if outcome != string(ServiceRemovalOutcomeCleanupStalled) {
		t.Fatalf("recovered tombstone outcome = %q, want cleanup_stalled", outcome)
	}
}

// TestStallDeclarationIsAcceptedFromALaterBoot is what makes a lost response
// survivable: the agent replays the frozen bytes under the same key, and the
// boot that finally lands is not the boot that first sent it.
func TestStallDeclarationIsAcceptedFromALaterBoot(t *testing.T) {
	harness := newRemovalStallHarness(t)
	harness.advancePastStallBound(t)
	declaration := harness.declaration("lost-response")
	if _, err := harness.declare(t, declaration); err != nil {
		t.Fatalf("declare stalled removal: %v", err)
	}
	replacement := harness.node.NodeRegistration
	replacement.BootSessionID = "boot-after-restart"
	status, _, body := harness.h.do(harness.h.client(harness.agent), http.MethodPost,
		"/v1/agent/nodes/register", replacement)
	if status != http.StatusOK {
		t.Fatalf("replacement register status = %d body=%s", status, body)
	}
	// The frozen declaration is unchanged; only the enclosing request carries
	// the new boot, exactly as the agent's replay does.
	retried := declaration
	retried.BootSessionID = replacement.BootSessionID
	replayed, err := harness.declare(t, retried)
	if err != nil || replayed.State != contract.JobStalledCleanupUnverified {
		t.Fatalf("replay from a later boot = %#v, %v", replayed, err)
	}
}
