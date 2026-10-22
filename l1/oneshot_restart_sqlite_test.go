//go:build sqlite_integration

package l1

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestOneshotRestartSQLiteOldSchemaReopenAndMigrationReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.sqlite")
	store, err := OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := store.CreateJob(t.Context(), validJobSpec("old-job", nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	// Reconstruct the exact pre-PR1 schema without restarting on a new schema.
	for table, columns := range map[string][]string{
		"jobs":     {"restart_count", "next_restart_at_ns", "restart_terminal_reason", "restart_last_reason", "restart_previous_attempt_id"},
		"attempts": {"interruption_reason", "interruption_request_key", "interruption_request_json", "interruption_request_reason", "interruption_requested_ns"},
	} {
		for _, column := range columns {
			if _, err := db.Exec("ALTER TABLE " + table + " DROP COLUMN " + column); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		store, err = OpenStore(path, StoreOptions{})
		if err != nil {
			t.Fatal(err)
		}
		got, err := store.GetJob(t.Context(), job.JobID)
		if err != nil || got.Spec.Restartable || got.RestartRecovery != nil || got.State != contract.JobQueued {
			t.Fatalf("old job=%+v err=%v", got, err)
		}
		var count int
		var due sql.NullInt64
		if err := store.db.QueryRow("SELECT restart_count,next_restart_at_ns FROM jobs WHERE job_id=?", job.JobID).Scan(&count, &due); err != nil || count != 0 || due.Valid {
			t.Fatalf("migration defaults=%d/%v err=%v", count, due, err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOneshotRestartSQLiteNonterminalSecretsAndTerminalScrub(t *testing.T) {
	spec := secretBearingOneShot("restart-secrets")
	spec.RoutingTags = nil
	f := newOneshotFixtureWithSpec(t, true, spec)
	c := f.claim(t)
	f.disable(t)
	request := f.interrupt(t, c)
	if _, err := f.h.store.CompleteAttempt(t.Context(), "agent", f.job.JobID, c.Lease.AttemptID, request); err != nil {
		t.Fatal(err)
	}
	retained := f.get(t)
	if retained.State != contract.JobQueued || retained.SecretsScrubbedAt != nil || retained.Spec.Execution.SensitiveEnv[contract.EnvRunToken] != scrubRunTokenSecret || retained.Spec.Execution.Executable.InlineBase64 == "" || retained.Spec.Labels[contract.LabelRunParams] == "" {
		t.Fatalf("nonterminal inputs scrubbed: %+v", retained)
	}
	f.enable(t)
	next := f.claim(t)
	if next.Job.Spec.Execution.SensitiveEnv[contract.EnvRunToken] != scrubRunTokenSecret || next.Job.Spec.Execution.Executable.InlineBase64 != spec.Execution.Executable.InlineBase64 {
		t.Fatal("successor lost frozen inputs")
	}
	exit := 0
	done, err := f.h.store.CompleteAttempt(t.Context(), "agent", f.job.JobID, next.Lease.AttemptID, CompletionRequest{FencingToken: next.Lease.FencingToken, IdempotencyKey: "finish", Result: ProcessResult{ExitCode: &exit}})
	if err != nil || done.State != contract.JobSucceeded || done.SecretsScrubbedAt == nil || len(done.Spec.Execution.SensitiveEnv) > 0 || done.Spec.Execution.Executable.InlineBase64 != "" || done.Spec.Labels[contract.LabelRunParams] != "" {
		t.Fatalf("terminal secrets not scrubbed: %+v err=%v", done, err)
	}
}

func TestOneshotRestartSQLiteRecoverySurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recover.sqlite")
	clock := &fakeClock{now: time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)}
	store, err := OpenStore(path, StoreOptions{Clock: clock, Jitter: func(d time.Duration) time.Duration { return d }})
	if err != nil {
		t.Fatal(err)
	}
	spec := validJobSpec("durable-backoff", nil)
	spec.Restartable = true
	job, _, err := store.CreateJob(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	// This fixture injects only Node admission; claim, renewal, expiry and restart
	// accounting all use the production transaction owners.
	_, err = store.db.Exec(`INSERT INTO nodes(node_id,identity_node_id,boot_session_id,os,architecture,agent_version,capabilities_json,state,last_heartbeat_ns,max_oneshot_slots,max_service_slots,claims_enabled) VALUES('node','agent','boot','linux','arm64','test','{"kind:process":true,"oneshot:restartable":true}','alive',?,4,2,1)`, clock.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.ClaimJob(t.Context(), "agent", "node", "boot", contract.JobClassOneShot)
	if err != nil || first == nil {
		t.Fatalf("first=%v err=%v", first, err)
	}
	clock.Advance(DefaultLeaseDuration)
	if _, err := store.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(path, StoreOptions{Clock: clock, Jitter: func(time.Duration) time.Duration { t.Fatal("reopen redrew jitter"); return 0 }})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	got, err := store.GetJob(t.Context(), job.JobID)
	if err != nil || !got.RestartRecovery.NextRestartAt.Equal(clock.Now().Add(MinimumOneshotRestartDelay)) || got.RestartRecovery.Count != 0 {
		t.Fatalf("persisted=%+v err=%v", got, err)
	}
	early, err := store.ClaimJob(t.Context(), "agent", "node", "boot", contract.JobClassOneShot)
	if err != nil || early != nil {
		t.Fatalf("early=%v err=%v", early, err)
	}
	clock.Advance(MinimumOneshotRestartDelay)
	next, err := store.ClaimJob(t.Context(), "agent", "node", "boot", contract.JobClassOneShot)
	if err != nil || next == nil || next.Job.RestartRecovery.Count != 1 || next.Lease.FencingToken == first.Lease.FencingToken || next.AttemptToken == first.AttemptToken {
		t.Fatalf("reopened successor=%v err=%v", next, err)
	}
}

func TestOneshotRestartSQLiteInterruptionRequestSurvivesReopen(t *testing.T) {
	f := newOneshotFixture(t, true)
	c := f.claim(t)
	f.disable(t)
	req := f.request(c)
	if _, _, err := f.h.store.InterruptRestartableAttempt(t.Context(), "node", c.Lease.AttemptID, req); err != nil {
		t.Fatal(err)
	}
	f.h.stopServer()
	var seq int
	var name, path string
	if err := f.h.store.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	if err := f.h.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(path, StoreOptions{Clock: f.h.clock})
	if err != nil {
		t.Fatal(err)
	}
	// Harness cleanup closes its current Store after the server has joined.
	f.h.store = reopened
	lease, err := reopened.RenewLease(t.Context(), "agent", f.job.JobID, c.Lease.AttemptID, c.Lease.FencingToken)
	if err != nil || lease.Directive != AttemptDirectiveRestart || lease.InterruptionRequestKey != req.IdempotencyKey || lease.InterruptionReason != InterruptionUpgrade {
		t.Fatalf("reopened directive=%+v err=%v", lease, err)
	}
	_, replayed, err := reopened.InterruptRestartableAttempt(t.Context(), "node", c.Lease.AttemptID, req)
	if err != nil || !replayed {
		t.Fatalf("reopened request replay=%v err=%v", replayed, err)
	}
	completion := CompletionRequest{FencingToken: c.Lease.FencingToken, IdempotencyKey: "settle-after-reopen", Result: ProcessResult{Signal: "SIGTERM", TerminationCause: contract.TerminationCauseAgent}, InterruptionReason: InterruptionUpgrade, InterruptionRequestKey: req.IdempotencyKey, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt}
	job, err := reopened.CompleteAttempt(t.Context(), "agent", f.job.JobID, c.Lease.AttemptID, completion)
	if err != nil || job.State != contract.JobQueued || !job.RestartRecovery.Pending || job.RestartRecovery.Count != 0 {
		t.Fatalf("reopened recovery=%+v err=%v", job, err)
	}
}
