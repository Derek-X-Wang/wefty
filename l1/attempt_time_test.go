package l1

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

type attemptClockFixture struct {
	store   *Store
	clock   *seatBootTestClock
	path    string
	options StoreOptions
	node    Node
	job     Job
	claim   *Claim
}

func newAttemptClockFixture(t *testing.T, classes ...string) *attemptClockFixture {
	t.Helper()
	wall := &fakeClock{now: time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)}
	f := &attemptClockFixture{clock: &seatBootTestClock{fakeClock: wall, boot: "l1-boot-a", elapsed: time.Hour}, path: filepath.Join(t.TempDir(), "l1.sqlite")}
	f.options = StoreOptions{Clock: f.clock, LeaseDuration: time.Minute, NodeStaleAfter: 12 * time.Hour, NodeDeadAfter: 24 * time.Hour}
	var err error
	f.store, err = OpenStore(f.path, f.options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.store.Close(); err != nil {
			t.Error(err)
		}
	})
	f.node = registerStoreNode(t, f.store, wall, "lease-node", "node-boot")
	spec := validJobSpec("boot-lease", nil)
	if len(classes) > 0 {
		spec.Class = classes[0]
		spec.Execution.HandoffDirectory = ""
		spec.Restart = contract.RestartAlways
		port := 8080
		spec.PublishedPort = &port
	}
	f.job, _, err = f.store.CreateJob(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	f.claim, err = f.store.ClaimJob(t.Context(), "agent-lease-node", f.node.NodeID, f.node.BootSessionID, f.job.Spec.Class)
	if err != nil || f.claim == nil {
		t.Fatalf("claim=%+v err=%v", f.claim, err)
	}
	return f
}

func (f *attemptClockFixture) reconcile(t *testing.T, expired int64) {
	t.Helper()
	result, err := f.store.Reconcile(t.Context())
	if err != nil || result.ExpiredAttempts != expired {
		t.Fatalf("expired=%+v want=%d err=%v", result, expired, err)
	}
}

func TestAttemptLeaseWallForwardDoesNotExpire(t *testing.T) {
	f := newAttemptClockFixture(t)
	f.clock.Advance(2 * time.Hour)
	f.reconcile(t, 0)
	if _, err := f.store.RenewLease(t.Context(), "agent-lease-node", f.job.JobID, f.claim.Lease.AttemptID, f.claim.Lease.FencingToken); err != nil {
		t.Fatalf("renew after wall jump: %v", err)
	}
}

func TestAttemptLeaseWallBackwardStillExpires(t *testing.T) {
	f := newAttemptClockFixture(t)
	f.clock.Advance(-2 * time.Hour)
	f.clock.elapsed += time.Minute
	f.reconcile(t, 1)
}

func TestAttemptLeaseSameBootRestartKeepsDeadline(t *testing.T) {
	f := newAttemptClockFixture(t)
	original := f.claim.Lease.LeaseExpires
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(2 * time.Hour)
	f.clock.elapsed += 59 * time.Second
	var err error
	f.store, err = OpenStore(f.path, f.options)
	if err != nil {
		t.Fatal(err)
	}
	f.reconcile(t, 0)
	attempts, err := f.store.ListJobAttempts(t.Context(), f.job.JobID)
	if err != nil || len(attempts) != 1 || !attempts[0].LeaseExpiresAt.Equal(original) {
		t.Fatalf("recorded wall deadline changed: %+v err=%v", attempts, err)
	}
	f.clock.elapsed += time.Second
	f.reconcile(t, 1)
}

func TestAttemptLeaseNewBootFallsBackToWall(t *testing.T) {
	for _, wallAdvance := range []time.Duration{30 * time.Second, 2 * time.Hour} {
		t.Run(wallAdvance.String(), func(t *testing.T) {
			f := newAttemptClockFixture(t)
			f.clock.Advance(2 * time.Hour)
			if _, err := f.store.ResolveAttemptCredential(t.Context(), f.claim.AttemptToken, "agent-lease-node"); err != nil {
				t.Fatalf("same-boot credential: %v", err)
			}
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			f.clock.Advance(wallAdvance - 2*time.Hour)
			f.clock.boot = "l1-boot-b"
			f.clock.elapsed = 8 * time.Hour
			var err error
			f.store, err = OpenStore(f.path, f.options)
			if err != nil {
				t.Fatal(err)
			}
			_, err = f.store.ResolveAttemptCredential(t.Context(), f.claim.AttemptToken, "agent-lease-node")
			if wallAdvance < time.Minute {
				if err != nil {
					t.Fatalf("unexpired new-boot wall lease: %v", err)
				}
				f.reconcile(t, 0)
				f.clock.Advance(30 * time.Second)
			} else if errorCode(err) != contract.ErrorUnauthorized {
				t.Fatalf("new-boot expired credential: %v", err)
			}
			f.reconcile(t, 1)
		})
	}
}

func TestAttemptLeaseRenewalWritesBothDeadlines(t *testing.T) {
	f := newAttemptClockFixture(t)
	f.clock.Advance(2 * time.Hour)
	f.clock.elapsed += 10 * time.Second
	lease, err := f.store.RenewLease(t.Context(), "agent-lease-node", f.job.JobID, f.claim.Lease.AttemptID, f.claim.Lease.FencingToken)
	if err != nil || lease.LeaseTTL != time.Minute || !lease.LeaseExpires.Equal(f.clock.Now().Add(time.Minute)) {
		t.Fatalf("renewal=%+v err=%v", lease, err)
	}
	f.clock.Advance(-4 * time.Hour)
	f.clock.elapsed += 59 * time.Second
	f.reconcile(t, 0)
	f.clock.elapsed += time.Second
	f.reconcile(t, 1)
}

func TestAttemptLeaseLegacyRowUsesWall(t *testing.T) {
	for _, stamp := range []string{"lease_boot_id='',lease_deadline_ns=0", "lease_boot_id=''", "lease_deadline_ns=0"} {
		t.Run(stamp, func(t *testing.T) {
			f := newAttemptClockFixture(t)
			retentionSweepExec(t, f.store, "UPDATE attempts SET "+stamp+" WHERE attempt_id=?", f.claim.Lease.AttemptID)
			f.clock.elapsed += time.Hour
			f.reconcile(t, 0)
			f.clock.Advance(time.Minute)
			f.reconcile(t, 1)
		})
	}
}

func TestAttemptLeaseShorteningCapsBootDeadline(t *testing.T) {
	f := newAttemptClockFixture(t, contract.JobClassService)
	f.clock.Advance(-2 * time.Hour)
	f.clock.elapsed += 10 * time.Second
	if _, err := f.store.RemoveService(t.Context(), f.job.JobID); err != nil {
		t.Fatal(err)
	}
	var boot string
	var elapsed int64
	var wall int64
	if err := f.store.db.QueryRow("SELECT lease_boot_id,lease_deadline_ns,lease_expires_ns FROM attempts WHERE attempt_id=?", f.claim.Lease.AttemptID).Scan(&boot, &elapsed, &wall); err != nil {
		t.Fatal(err)
	}
	if boot != "l1-boot-a" || elapsed != int64(time.Hour+10*time.Second) || wall != f.clock.Now().UnixNano() {
		t.Fatalf("shortened boot=%q elapsed=%d wall=%d", boot, elapsed, wall)
	}
}

func TestAttemptLeasePublicationAndHealthIgnoreWallStep(t *testing.T) {
	f := newAttemptClockFixture(t, contract.JobClassService)
	f.clock.Advance(2 * time.Hour)
	published, err := f.store.SetAttemptPublication(t.Context(), "agent-lease-node", f.job.JobID, f.claim.Lease.AttemptID, PublicationRequest{FencingToken: f.claim.Lease.FencingToken, Ready: boolPointer(true)})
	if err != nil || published.Ready == nil || !*published.Ready || published.HealthySinceAt == nil {
		t.Fatalf("publication=%+v err=%v", published, err)
	}
	read, err := f.store.GetJob(t.Context(), f.job.JobID)
	if err != nil || read.Ready == nil || !*read.Ready {
		t.Fatalf("read readiness=%+v err=%v", read, err)
	}
	replay, repeated, err := f.store.CreateJob(t.Context(), f.job.Spec)
	if err != nil || !repeated || replay.Ready == nil || !*replay.Ready {
		t.Fatalf("replay readiness=%+v repeated=%t err=%v", replay, repeated, err)
	}
	f.clock.Advance(-4 * time.Hour)
	f.clock.elapsed += time.Minute
	read, err = f.store.GetJob(t.Context(), f.job.JobID)
	if err != nil || read.Ready == nil || *read.Ready {
		t.Fatalf("expired readiness=%+v err=%v", read, err)
	}
}

func TestAttemptLeaseExecutionWritesIgnoreWallStep(t *testing.T) {
	for _, action := range []string{"started", "logs", "completion"} {
		t.Run(action, func(t *testing.T) {
			f := newAttemptClockFixture(t)
			f.clock.Advance(2 * time.Hour)
			var err error
			switch action {
			case "started":
				_, err = f.store.StartAttempt(t.Context(), "agent-lease-node", f.job.JobID, f.claim.Lease.AttemptID, StartedRequest{FencingToken: f.claim.Lease.FencingToken})
			case "logs":
				_, err = f.store.AppendLogs(t.Context(), "agent-lease-node", f.job.JobID, f.claim.Lease.AttemptID, AppendLogsRequest{FencingToken: f.claim.Lease.FencingToken, Events: []contract.LogEvent{{AttemptID: f.claim.Lease.AttemptID, Stream: contract.LogStdout, Timestamp: f.clock.Now(), Bytes: []byte("still running\n")}}})
			case "completion":
				code := 0
				_, err = f.store.CompleteAttempt(t.Context(), "agent-lease-node", f.job.JobID, f.claim.Lease.AttemptID, CompletionRequest{FencingToken: f.claim.Lease.FencingToken, IdempotencyKey: "done", Result: ProcessResult{ExitCode: &code}})
			}
			if err != nil {
				t.Fatal(err)
			}
			attempts, err := f.store.ListJobAttempts(t.Context(), f.job.JobID)
			if err != nil || len(attempts) != 1 || attempts[0].State == contract.AttemptLost {
				t.Fatalf("attempt=%+v err=%v", attempts, err)
			}
		})
	}
}

func TestAttemptLeaseWallStepKeepsComputerSeatLive(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	clock := &seatBootTestClock{fakeClock: f.clock, boot: "l1-boot-a", elapsed: time.Hour}
	f.store.clock = clock
	id := "seat_00000000000000000000000000000001"
	prepareSeatLogLive(t, f, id)
	f.clock.Advance(2 * time.Hour)
	result, err := f.store.Reconcile(t.Context())
	if err != nil || result.ExpiredAttempts != 0 {
		t.Fatalf("Computer Attempt expired: %+v err=%v", result, err)
	}
	seat, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || seat.State != SeatLive {
		t.Fatalf("SL-7 Seat=%+v err=%v", seat, err)
	}
}

func TestAttemptLeaseClockFailureDoesNotExpireOrGrantAuthority(t *testing.T) {
	f := newAttemptClockFixture(t)
	f.clock.Advance(2 * time.Hour)
	f.store.clock = failedSeatClock{f.clock}
	f.reconcile(t, 0)
	if _, err := f.store.RenewLease(t.Context(), "agent-lease-node", f.job.JobID, f.claim.Lease.AttemptID, f.claim.Lease.FencingToken); err == nil {
		t.Fatal("renewal granted authority without a lease clock")
	}
	if _, err := f.store.ResolveAttemptCredential(t.Context(), f.claim.AttemptToken, "agent-lease-node"); err == nil {
		t.Fatal("credential granted authority without a lease clock")
	}
	// Historical evidence does not grant execution authority and keeps its
	// existing acceptance rule while the lease clock is unavailable.
	if _, err := f.store.SetAttemptResult(t.Context(), "agent-lease-node", f.job.JobID, f.claim.Lease.AttemptID, AttemptResultRequest{FencingToken: f.claim.Lease.FencingToken, Document: []byte(`{"ok":true}`)}); err != nil {
		t.Fatalf("clock failure blocked evidence upload: %v", err)
	}
	if _, _, err := f.store.CreateJob(t.Context(), validJobSpec("clock-failed-queued", nil)); err != nil {
		t.Fatalf("clock failure blocked unrelated submission: %v", err)
	}
	f.store.clock = f.clock
	f.reconcile(t, 0)
	code := 0
	completion := CompletionRequest{FencingToken: f.claim.Lease.FencingToken, IdempotencyKey: "clock-failure-replay", Result: ProcessResult{ExitCode: &code}}
	if _, err := f.store.CompleteAttempt(t.Context(), "agent-lease-node", f.job.JobID, f.claim.Lease.AttemptID, completion); err != nil {
		t.Fatal(err)
	}
	f.store.clock = failedSeatClock{f.clock}
	if _, err := f.store.CompleteAttempt(t.Context(), "agent-lease-node", f.job.JobID, f.claim.Lease.AttemptID, completion); err != nil {
		t.Fatalf("clock failure blocked accepted completion replay: %v", err)
	}
	service := newAttemptClockFixture(t, contract.JobClassService)
	if _, err := service.store.SetAttemptPublication(t.Context(), "agent-lease-node", service.job.JobID, service.claim.Lease.AttemptID, PublicationRequest{FencingToken: service.claim.Lease.FencingToken, Ready: boolPointer(true)}); err != nil {
		t.Fatal(err)
	}
	service.store.clock = failedSeatClock{service.clock}
	job, err := service.store.GetJob(t.Context(), service.job.JobID)
	if err != nil || job.Ready == nil || *job.Ready {
		t.Fatalf("clock failure retained publication authority: %+v err=%v", job, err)
	}
	if _, err := service.store.RemoveService(t.Context(), service.job.JobID); err != nil {
		t.Fatalf("clock failure blocked owner revocation: %v", err)
	}
	attempts, err := service.store.ListJobAttempts(t.Context(), service.job.JobID)
	if err != nil || len(attempts) != 1 || attempts[0].State != contract.AttemptLost {
		t.Fatalf("revoked Attempt=%+v err=%v", attempts, err)
	}
}
