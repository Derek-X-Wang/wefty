package l1

import (
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestPortedServiceHealthRecoversAfterStaleNode(t *testing.T) {
	for _, trigger := range []string{"renewal", "started", "logs", "publication"} {
		t.Run(trigger, func(t *testing.T) {
			assertPortedServiceHealthRecovery(t, true, trigger)
		})
	}
}

func TestStaleNodePublicationMustNotStartServiceStability(t *testing.T) {
	assertPortedServiceHealthRecovery(t, false, "renewal")
}

// Exercise health through the real Store write transactions and fake-clock
// reconciliation, including failure accounting before and after recovery.
func assertPortedServiceHealthRecovery(t *testing.T, publishBeforeStale bool, trigger string) {
	t.Helper()
	port := 8080
	f := newPublicationFixture(t, &port)
	f.h.stopServer()
	store, clock := f.h.store, f.h.clock
	fail := func(claim Claim) Job {
		t.Helper()
		exitCode := 1
		queued, err := store.CompleteAttempt(t.Context(), "node-1", f.job.JobID, claim.Lease.AttemptID, CompletionRequest{
			FencingToken: claim.Lease.FencingToken, IdempotencyKey: "failure:" + claim.Lease.AttemptID,
			Result: ProcessResult{ExitCode: &exitCode},
		})
		if err != nil || queued.State != contract.JobQueued || queued.RestartStreak != 1 || queued.NextRestartAt == nil {
			t.Fatalf("restartable failure=%+v err=%v, want queued with streak 1", queued, err)
		}
		return queued
	}
	queued := fail(f.claim)
	clock.Advance(queued.NextRestartAt.Sub(clock.Now()))
	current, err := store.ClaimJob(t.Context(), "node-1", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || current == nil {
		t.Fatalf("claim=%+v err=%v", current, err)
	}
	f.claim = *current
	renew := func() {
		t.Helper()
		if _, err := store.RenewLease(t.Context(), "node-1", f.job.JobID, current.Lease.AttemptID, current.Lease.FencingToken); err != nil {
			t.Fatal(err)
		}
	}
	heartbeat := func() {
		t.Helper()
		if _, err := store.HeartbeatNode(t.Context(), "node-1", f.node.NodeID, f.node.BootSessionID); err != nil {
			t.Fatal(err)
		}
	}
	publish := func() Job {
		t.Helper()
		job, err := store.SetAttemptPublication(t.Context(), "node-1", f.job.JobID, current.Lease.AttemptID, f.request(true))
		if err != nil {
			t.Fatal(err)
		}
		return job
	}
	heartbeat()
	renew()
	if job := getRestartService(t, f.h, f.job.JobID); job.HealthySinceAt != nil {
		t.Fatalf("unpublished ported service gained health on renewal: %+v", job)
	}
	if publishBeforeStale {
		if job := publish(); job.HealthySinceAt == nil || job.PublishedAttemptID != current.Lease.AttemptID {
			t.Fatalf("alive publication did not establish health: %+v", job)
		}
	}
	// Keep the Attempt leased while its Node crosses the stale boundary.
	for i := 0; i < 4; i++ {
		clock.Advance(15 * time.Second)
		renew()
	}
	result, err := store.Reconcile(t.Context())
	if err != nil || result.StaleNodes != 1 || result.RestartStreakResets != 0 {
		t.Fatalf("stale reconciliation=%+v err=%v", result, err)
	}
	if job := getRestartService(t, f.h, f.job.JobID); job.HealthySinceAt != nil {
		t.Fatalf("stale reconciliation retained health: %+v", job)
	}
	if !publishBeforeStale {
		job := publish()
		if job.PublishedAttemptID != current.Lease.AttemptID || job.HealthySinceAt != nil {
			t.Fatalf("stale publication must retain marker without health: %+v", job)
		}
	}
	renew()
	if job := getRestartService(t, f.h, f.job.JobID); job.HealthySinceAt != nil || job.PublishedAttemptID != current.Lease.AttemptID {
		t.Fatalf("stale renewal changed health or publication: %+v", job)
	}
	clock.Advance(15 * time.Second)
	renew() // Stale time must never count toward the stability window.
	heartbeat()
	aliveAt := clock.Now()
	switch trigger {
	case "renewal":
		renew()
	case "started":
		if _, err := store.StartAttempt(t.Context(), "node-1", f.job.JobID, current.Lease.AttemptID,
			StartedRequest{FencingToken: current.Lease.FencingToken}); err != nil {
			t.Fatal(err)
		}
	case "logs":
		if _, err := store.AppendLogs(t.Context(), "node-1", f.job.JobID, current.Lease.AttemptID, AppendLogsRequest{
			FencingToken: current.Lease.FencingToken,
			Events: []contract.LogEvent{{AttemptID: current.Lease.AttemptID, Stream: contract.LogStdout,
				Sequence: 0, Timestamp: clock.Now(), Bytes: []byte("recovered output\n")}},
		}); err != nil {
			t.Fatal(err)
		}
	case "publication":
		publish() // The publication marker is unchanged; health must recover.
	}
	assertHealth := func(wantStreak int) {
		t.Helper()
		job := getRestartService(t, f.h, f.job.JobID)
		if job.CurrentAttemptID != current.Lease.AttemptID || job.PublishedAttemptID != current.Lease.AttemptID ||
			job.HealthySinceAt == nil || !job.HealthySinceAt.Equal(aliveAt) || job.RestartStreak != wantStreak {
			t.Fatalf("recovered service=%+v, want same published Attempt healthy since %s with streak %d", job, aliveAt, wantStreak)
		}
	}
	assertHealth(1)
	for i := 0; i < 7; i++ {
		clock.Advance(15 * time.Second)
		heartbeat()
		renew()
	}
	result, err = store.Reconcile(t.Context())
	if err != nil || result.RestartStreakResets != 0 {
		t.Fatalf("stale time counted before two healthy minutes: result=%+v err=%v", result, err)
	}
	assertHealth(1)
	clock.Advance(15 * time.Second)
	heartbeat()
	renew()
	result, err = store.Reconcile(t.Context())
	if err != nil || result.RestartStreakResets != 1 {
		t.Fatalf("two-minute reconciliation=%+v err=%v, want one reset", result, err)
	}
	assertHealth(0)
	fail(*current) // The next failure earns streak 1 again.
}
