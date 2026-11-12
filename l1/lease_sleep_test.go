package l1

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

type sleepTestClock struct {
	*seatBootTestClock
	awake    time.Duration
	awakeErr error
}

func (c *sleepTestClock) AwakeElapsed() (time.Duration, error) { return c.awake, c.awakeErr }
func (c *sleepTestClock) advance(awake, sleep time.Duration) {
	c.Advance(awake + sleep)
	c.elapsed += awake + sleep
	c.awake += awake
}

func newSleepLeaseFixture(t *testing.T, beforeClaim ...func(*attemptClockFixture, *sleepTestClock)) (*attemptClockFixture, *sleepTestClock) {
	t.Helper()
	wall := &fakeClock{now: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)}
	base := &seatBootTestClock{fakeClock: wall, boot: "sleep-boot", elapsed: time.Hour}
	clock := &sleepTestClock{seatBootTestClock: base, awake: time.Hour}
	f := &attemptClockFixture{clock: base, path: filepath.Join(t.TempDir(), "l1.sqlite")}
	f.options = StoreOptions{Clock: clock, LeaseDuration: 30 * time.Second, NodeStaleAfter: 45 * time.Second, NodeDeadAfter: 90 * time.Second}
	var err error
	f.store, err = openTestStore(t, f.path, f.options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.store.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, configure := range beforeClaim {
		configure(f, clock)
	}
	f.node = registerStoreNode(t, f.store, wall, "lease-node", "node-boot")
	f.job, _, err = f.store.CreateJob(t.Context(), validJobSpec("sleep-lease", nil))
	if err != nil {
		t.Fatal(err)
	}
	f.claim, err = f.store.ClaimJob(t.Context(), "agent-lease-node", f.node.NodeID, f.node.BootSessionID, f.job.Spec.Class)
	if err != nil || f.claim == nil {
		t.Fatalf("claim=%+v err=%v", f.claim, err)
	}
	return f, clock
}

func sleepLeaseDeadlines(t *testing.T, f *attemptClockFixture) (int64, int64) {
	t.Helper()
	var boot, wall int64
	if err := f.store.db.QueryRow(`SELECT lease_deadline_ns,lease_expires_ns FROM attempts WHERE attempt_id=?`, f.claim.Lease.AttemptID).Scan(&boot, &wall); err != nil {
		t.Fatal(err)
	}
	return boot, wall
}

func TestLeaseSleepReconcileAndRenew(t *testing.T) {
	f, c := newSleepLeaseFixture(t)
	boot, wall := sleepLeaseDeadlines(t, f)
	c.advance(0, 5*time.Minute)
	result, err := f.store.Reconcile(t.Context())
	if err != nil || result.ExpiredAttempts != 0 || result.DeadNodes != 0 || result.StaleNodes != 0 {
		t.Fatalf("wake=%+v err=%v", result, err)
	}
	gotBoot, gotWall := sleepLeaseDeadlines(t, f)
	if gotBoot != boot+int64(5*time.Minute) || gotWall != wall+int64(5*time.Minute) {
		t.Fatalf("deadlines=%d/%d", gotBoot-boot, gotWall-wall)
	}
	c.advance(29*time.Second, 0)
	if _, err := f.store.RenewLease(t.Context(), "agent-lease-node", f.job.JobID, f.claim.Lease.AttemptID, f.claim.Lease.FencingToken); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t, 0)
}

func TestLeaseSleepFailureStillExpires(t *testing.T) {
	f, c := newSleepLeaseFixture(t)
	c.advance(0, 5*time.Minute)
	f.reconcile(t, 0)
	c.advance(29*time.Second, 0)
	f.reconcile(t, 0)
	c.advance(time.Second, 0)
	f.reconcile(t, 1)
	attempts, err := f.store.ListJobAttempts(t.Context(), f.job.JobID)
	if err != nil || len(attempts) != 1 || attempts[0].State != contract.AttemptLost {
		t.Fatalf("attempts=%+v err=%v", attempts, err)
	}
}

func TestLeaseSleepOnceAcrossRollbackAndReopen(t *testing.T) {
	for _, reopenBeforeApply := range []bool{false, true} {
		t.Run(fmt.Sprint(reopenBeforeApply), func(t *testing.T) {
			f, c := newSleepLeaseFixture(t)
			boot, wall := sleepLeaseDeadlines(t, f)
			c.advance(0, 5*time.Minute)
			w, err := f.store.beginWriteTransaction(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if err = w.rollback(); err != nil {
				t.Fatal(err)
			}
			if b, w := sleepLeaseDeadlines(t, f); b != boot || w != wall {
				t.Fatal("rollback consumed gap")
			}
			reopen := func() {
				t.Helper()
				if err := f.store.Close(); err != nil {
					t.Fatal(err)
				}
				var err error
				f.store, err = openTestStore(t, f.path, f.options)
				if err != nil {
					t.Fatal(err)
				}
			}
			if reopenBeforeApply {
				reopen()
			}
			f.reconcile(t, 0)
			f.reconcile(t, 0)
			reopen()
			f.reconcile(t, 0)
			if b, w := sleepLeaseDeadlines(t, f); b != boot+int64(5*time.Minute) || w != wall+int64(5*time.Minute) {
				t.Fatal("gap applied more than once")
			}
			c.advance(30*time.Second, 0)
			f.reconcile(t, 1)
		})
	}
}

func TestLeaseSleepReadDoorsBeforeCatchup(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint(legacy), func(t *testing.T) {
			f, c := newSleepLeaseFixture(t)
			if legacy {
				retentionSweepExec(t, f.store, `UPDATE attempts SET lease_boot_id='',lease_deadline_ns=0 WHERE attempt_id=?`, f.claim.Lease.AttemptID)
			}
			boot, wall := sleepLeaseDeadlines(t, f)
			c.advance(0, 5*time.Minute)
			for _, read := range []func(context.Context, *serviceActionActor, func(context.Context, readModel) error) error{
				f.store.withReadSnapshot,
				func(ctx context.Context, _ *serviceActionActor, use func(context.Context, readModel) error) error {
					return f.store.withAgentReadSnapshot(ctx, use)
				},
			} {
				err := read(t.Context(), nil, func(ctx context.Context, r readModel) error {
					a, err := readAttemptAuthority(ctx, r.(*databaseReads).q, f.claim.Lease.AttemptID)
					if err != nil {
						return err
					}
					live, err := attemptLeaseLive(ctx, r.now(), a)
					if err != nil || !live {
						t.Fatalf("read authority live=%v err=%v", live, err)
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			attempts, err := f.store.ListJobAttempts(t.Context(), f.job.JobID)
			if err != nil || len(attempts) != 1 || !attempts[0].LeaseLive {
				t.Fatalf("attempts=%+v err=%v", attempts, err)
			}
			node, err := f.store.GetNode(t.Context(), f.node.NodeID)
			if err != nil || node.State != contract.NodeAlive {
				t.Fatalf("node=%+v err=%v", node, err)
			}
			page, err := f.store.listNodesPage(t.Context(), nodeListFilters{State: "alive"}, "", 100)
			if err != nil || len(page.Nodes) != 1 {
				t.Fatalf("page=%+v err=%v", page, err)
			}
			if b, w := sleepLeaseDeadlines(t, f); b != boot || w != wall {
				t.Fatal("read snapshot wrote deadlines")
			}
			if _, err := f.store.RenewLease(t.Context(), "agent-lease-node", f.job.JobID, f.claim.Lease.AttemptID, f.claim.Lease.FencingToken); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLeaseSleepDoesNotReviveLost(t *testing.T) {
	f, c := newSleepLeaseFixture(t)
	c.advance(30*time.Second, 0)
	f.reconcile(t, 1)
	boot, wall := sleepLeaseDeadlines(t, f)
	c.advance(0, 5*time.Minute)
	f.reconcile(t, 0)
	if b, w := sleepLeaseDeadlines(t, f); b != boot || w != wall {
		t.Fatal("lost deadline changed")
	}
	if _, err := f.store.RenewLease(t.Context(), "agent-lease-node", f.job.JobID, f.claim.Lease.AttemptID, f.claim.Lease.FencingToken); err == nil {
		t.Fatal("lost Attempt renewed")
	}
}

func TestLeaseSleepWallStepAndNewBoot(t *testing.T) {
	t.Run("wall step", func(t *testing.T) {
		f, c := newSleepLeaseFixture(t)
		c.Advance(2 * time.Hour)
		boot, wall := sleepLeaseDeadlines(t, f)
		f.reconcile(t, 0)
		if b, w := sleepLeaseDeadlines(t, f); b != boot || w != wall {
			t.Fatal("calendar step extended lease")
		}
		c.advance(30*time.Second, 0)
		f.reconcile(t, 1)
	})
	t.Run("new boot", func(t *testing.T) {
		f, c := newSleepLeaseFixture(t)
		if err := f.store.Close(); err != nil {
			t.Fatal(err)
		}
		c.Advance(time.Minute)
		c.boot = "next-boot"
		c.elapsed = 10 * time.Minute
		c.awake = time.Minute
		var err error
		f.store, err = openTestStore(t, f.path, f.options)
		if err != nil {
			t.Fatal(err)
		}
		f.reconcile(t, 1)
	})
}

func TestLeaseSleepClockFailureRefusesComparison(t *testing.T) {
	f, c := newSleepLeaseFixture(t)
	c.advance(0, 5*time.Minute)
	c.awakeErr = errors.New("pausing clock unavailable")
	var logs []string
	f.store.setLogf(func(format string, args ...any) { logs = append(logs, fmt.Sprintf(format, args...)) })
	result, err := f.store.Reconcile(t.Context())
	if err != nil || result.ExpiredAttempts != 0 || result.DeadNodes != 1 || result.StaleNodes != 0 {
		t.Fatalf("failed sample blocked maintenance or expired authority: %+v err=%v", result, err)
	}
	if !strings.Contains(strings.Join(logs, "\n"), "event=attempt_lease_clock_retry action=retry_next_tick") {
		t.Fatalf("missing retry event: %v", logs)
	}
	var state contract.NodeState
	if err := f.store.db.QueryRow(`SELECT state FROM nodes WHERE node_id=?`, f.node.NodeID).Scan(&state); err != nil || state != contract.NodeDead {
		t.Fatalf("failed measurement did not preserve wall-based node liveness: %s err=%v", state, err)
	}
	if _, err := f.store.RenewLease(t.Context(), "agent-lease-node", f.job.JobID, f.claim.Lease.AttemptID, f.claim.Lease.FencingToken); err == nil {
		t.Fatal("failed pausing clock granted renewal")
	}
	if _, err := f.store.ResolveAttemptCredential(t.Context(), f.claim.AttemptToken, "agent-lease-node"); err == nil {
		t.Fatal("failed pausing clock granted credential authority")
	}
	if _, _, err := f.store.CreateJob(t.Context(), validJobSpec("awake-failure-queued", nil)); err != nil {
		t.Fatalf("clock failure blocked unrelated submission: %v", err)
	}
	if _, err := f.store.GetJob(t.Context(), f.job.JobID); err != nil {
		t.Fatalf("clock failure blocked a Job read: %v", err)
	}
	c.awakeErr = nil
	f.reconcile(t, 0)
}

func TestLeaseSleepBoundSeatSurvives(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	base := &seatBootTestClock{fakeClock: f.clock, boot: "seat-sleep-boot", elapsed: time.Hour}
	c := &sleepTestClock{seatBootTestClock: base, awake: time.Hour}
	var err error
	f.store, err = openTestStore(t, f.path, StoreOptions{Clock: c, LeaseDuration: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	claim := reviewLiveSeat(t, f, 81)
	c.advance(0, 5*time.Minute)
	// This agent read proves both token authority and the wanted Seat list
	// remain usable before any writer has persisted the gap.
	auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken}
	list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil || len(list.Seats) != 1 {
		t.Fatalf("list=%+v err=%v", list, err)
	}
	result, err := f.store.Reconcile(t.Context())
	if err != nil || result.ExpiredAttempts != 0 || result.DeadNodes != 0 {
		t.Fatalf("wake=%+v err=%v", result, err)
	}
	seat, err := f.store.GetSeatRequest(t.Context(), fmt.Sprintf("seat_%032x", 81))
	if err != nil || seat.State != SeatLive {
		t.Fatalf("seat=%+v err=%v", seat, err)
	}
	if _, err := f.store.RenewLease(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID, claim.Lease.FencingToken); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseSleepAwakeDelayIsNotSleep(t *testing.T) {
	f, c := newSleepLeaseFixture(t)
	c.advance(5*time.Minute, 0)
	result, err := f.store.Reconcile(t.Context())
	if err != nil || result.ExpiredAttempts != 1 || result.DeadNodes != 1 {
		t.Fatalf("awake delay=%+v err=%v", result, err)
	}
}

func TestLeaseSleepAlreadyExpiredBeforeSleep(t *testing.T) {
	f, c := newSleepLeaseFixture(t)
	c.advance(31*time.Second, 5*time.Minute)
	f.reconcile(t, 1)
}

func TestLeaseSleepSeparateGaps(t *testing.T) {
	f, c := newSleepLeaseFixture(t)
	boot, wall := sleepLeaseDeadlines(t, f)
	c.advance(5*time.Second, 2*time.Minute)
	f.reconcile(t, 0)
	c.advance(5*time.Second, 3*time.Minute)
	f.reconcile(t, 0)
	if b, w := sleepLeaseDeadlines(t, f); b != boot+int64(5*time.Minute) || w != wall+int64(5*time.Minute) {
		t.Fatal("separate gaps not added once each")
	}
	c.advance(20*time.Second, 0)
	f.reconcile(t, 1)
}

// A bound can tighten after wake. Credit only the new part of the bound,
// without letting syscall latency on either side of a sample invent sleep.
func TestLeaseSleepSamplingBounds(t *testing.T) {
	f, c := newSleepLeaseFixture(t)
	boot, wall := sleepLeaseDeadlines(t, f)
	apply := func(lower, upper time.Duration) {
		t.Helper()
		tx, err := f.store.db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		ctx := context.WithValue(t.Context(), attemptClockKey{}, attemptClock{boot: c.boot, ns: int64(c.elapsed), sleepOffset: &sleepClockOffset{lower: int64(lower), upper: int64(upper)}})
		if _, err := withLeaseSleepGap(ctx, tx, true); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	apply(-time.Second, time.Second)
	if b, w := sleepLeaseDeadlines(t, f); b != boot || w != wall {
		t.Fatal("sampling latency invented sleep")
	}
	c.advance(0, 5*time.Minute)
	apply(5*time.Minute-time.Second, 5*time.Minute+time.Second)
	apply(5*time.Minute, 5*time.Minute+time.Millisecond)
	apply(5*time.Minute-time.Second, 5*time.Minute+time.Second)
	if b, w := sleepLeaseDeadlines(t, f); b != boot+int64(5*time.Minute) || w != wall+int64(5*time.Minute) {
		t.Fatal("tighter bounds lost or duplicated sleep credit")
	}
}

func TestLeaseSleepSettlementDoorCatchesUp(t *testing.T) {
	f, c := newSleepLeaseFixture(t)
	boot, wall := sleepLeaseDeadlines(t, f)
	c.advance(0, 5*time.Minute)
	write, err := f.store.beginSettlementWriteTransaction(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer write.rollback()
	a, err := readAttemptAuthority(write.ctx, write.tx, f.claim.Lease.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	live, err := attemptLeaseLive(write.ctx, write.at, a)
	if err != nil || !live {
		t.Fatalf("settlement authority live=%v err=%v", live, err)
	}
	if err := write.commit(); err != nil {
		t.Fatal(err)
	}
	if b, w := sleepLeaseDeadlines(t, f); b != boot+int64(5*time.Minute) || w != wall+int64(5*time.Minute) {
		t.Fatal("settlement did not persist gap")
	}
	f.reconcile(t, 0)
}

func TestLeaseSleepHeartbeatCreditAndRealHeartbeat(t *testing.T) {
	f, c := newSleepLeaseFixture(t)
	original := f.node.LastHeartbeatAt
	c.advance(0, 5*time.Minute)
	f.reconcile(t, 0)
	node, err := f.store.GetNode(t.Context(), f.node.NodeID)
	if err != nil || !node.LastHeartbeatAt.Equal(original) || node.State != contract.NodeAlive {
		t.Fatalf("node=%+v err=%v", node, err)
	}
	if _, err := f.store.HeartbeatNode(t.Context(), "agent-lease-node", f.node.NodeID, f.node.BootSessionID); err != nil {
		t.Fatal(err)
	}
	node, err = f.store.GetNode(t.Context(), f.node.NodeID)
	if err != nil || !node.LastHeartbeatAt.Equal(c.Now()) || node.heartbeatSleep != 0 {
		t.Fatalf("heartbeat=%+v err=%v", node, err)
	}
	c.advance(90*time.Second, 0)
	result, err := f.store.Reconcile(t.Context())
	if err != nil || result.DeadNodes != 1 {
		t.Fatalf("heartbeat credit leaked past renewal: %+v err=%v", result, err)
	}
}

// Each entry scripts the real awake -> boot -> awake call sequence. Ending a
// sample also advances the underlying clocks, modeling descheduling on either
// side of the boot read rather than directly injecting offset bounds.
type sleepClockReadSequence struct{ before, boot, after, endBoot time.Duration }
type sequencedSleepClock struct {
	base        *sleepTestClock
	samples     []sleepClockReadSequence
	after       bool
	bootRead    bool
	repeatWidth time.Duration
	beforeBoot  bool
}

func (c *sequencedSleepClock) Now() time.Time { return c.base.Now() }
func (c *sequencedSleepClock) AwakeElapsed() (time.Duration, error) {
	if len(c.samples) == 0 && c.repeatWidth > 0 {
		boot := c.base.elapsed
		if c.beforeBoot {
			boot += c.repeatWidth
		}
		c.samples = []sleepClockReadSequence{{before: c.base.awake, boot: boot, after: c.base.awake + c.repeatWidth, endBoot: c.base.elapsed + c.repeatWidth}}
	}
	if len(c.samples) == 0 {
		return c.base.AwakeElapsed()
	}
	sample := c.samples[0]
	if !c.after {
		c.after = true
		return sample.before, nil
	}
	if !c.bootRead {
		panic("awake sample completed before the boot read")
	}
	c.after = false
	c.bootRead = false
	c.samples = c.samples[1:]
	c.base.Advance(sample.endBoot - c.base.elapsed)
	c.base.elapsed = sample.endBoot
	c.base.awake = sample.after
	return sample.after, nil
}
func (c *sequencedSleepClock) BootTime() (string, time.Duration, error) {
	if len(c.samples) == 0 {
		return c.base.BootTime()
	}
	if !c.after || c.bootRead {
		panic("boot read outside its awake bracket")
	}
	c.bootRead = true
	return c.base.boot, c.samples[0].boot, nil
}

func looseInitialSleepFixture(t *testing.T) (*attemptClockFixture, *sleepTestClock, *sequencedSleepClock) {
	t.Helper()
	var source *sequencedSleepClock
	f, c := newSleepLeaseFixture(t, func(f *attemptClockFixture, c *sleepTestClock) {
		source = &sequencedSleepClock{base: c, samples: []sleepClockReadSequence{{before: c.awake, boot: c.elapsed + 31*time.Second, after: c.awake + 31*time.Second, endBoot: c.elapsed + 31*time.Second}}}
		if _, err := f.store.db.Exec(`DELETE FROM lease_sleep_clock`); err != nil {
			t.Fatal(err)
		}
		if err := initializeLeaseSleepClock(t.Context(), f.store.db, source); err != nil {
			t.Fatal(err)
		}
		lo, hi := readSleepAnchor(t, f.store)
		if lo != 0 || hi != 31*time.Second {
			t.Fatalf("initial anchor=[%v,%v]", lo, hi)
		}
		if len(source.samples) != 0 {
			t.Fatal("initial scripted sample was not consumed")
		}
		f.store.clock = source
		f.options.Clock = source
	})
	return f, c, source
}

func TestLeaseSleepInitialLooseAnchorTightens(t *testing.T) {
	f, c, _ := looseInitialSleepFixture(t)
	boot, wall := sleepLeaseDeadlines(t, f)
	c.advance(0, 5*time.Minute)
	f.reconcile(t, 0)
	if b, w := sleepLeaseDeadlines(t, f); b != boot+int64(5*time.Minute) || w != wall+int64(5*time.Minute) {
		t.Fatal("loose initial anchor lost sleep credit")
	}
	if _, err := f.store.RenewLease(t.Context(), "agent-lease-node", f.job.JobID, f.claim.Lease.AttemptID, f.claim.Lease.FencingToken); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseSleepWideWakeSampleUsesBounds(t *testing.T) {
	const lease = 90 * time.Second
	f, c := newSleepLeaseFixture(t, func(f *attemptClockFixture, _ *sleepTestClock) { f.store.leaseDuration = lease })
	boot, wall := sleepLeaseDeadlines(t, f)
	c.advance(0, 5*time.Minute)
	// Deschedule L1 for 31 awake seconds after its boot read. The bracket
	// proves only 269 seconds of sleep, so the rest must await a tight sample.
	source := &sequencedSleepClock{base: c, samples: []sleepClockReadSequence{{before: c.awake, boot: c.elapsed, after: c.awake + 31*time.Second, endBoot: c.elapsed + 31*time.Second}}}
	f.store.clock = source
	write, err := f.store.beginWriteTransaction(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer write.rollback()
	if err := attemptClockError(write.ctx); err != nil {
		t.Fatalf("wide sample became a clock error: %v", err)
	}
	if currentAttemptClock(write.ctx).sleepUncertainty != 31*time.Second {
		t.Fatal("wide sample lost its possible credit")
	}
	a, err := readAttemptAuthority(write.ctx, write.tx, f.claim.Lease.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if live, err := attemptLeaseLive(write.ctx, write.at, a); err != nil || !live {
		t.Fatalf("proved-live lease refused: live=%v err=%v", live, err)
	}
	if err := write.commit(); err != nil {
		t.Fatal(err)
	}
	if b, w := sleepLeaseDeadlines(t, f); b != boot+int64(269*time.Second) || w != wall+int64(269*time.Second) {
		t.Fatal("wide sample did not persist its proved credit")
	}
	// A second wide sample advances both clocks by another 31 awake seconds.
	// Reconcile compares expiry against the maximum possible credit.
	source.samples = []sleepClockReadSequence{{before: c.awake, boot: c.elapsed, after: c.awake + 31*time.Second, endBoot: c.elapsed + 31*time.Second}}
	var logs []string
	f.store.setLogf(func(format string, args ...any) { logs = append(logs, fmt.Sprintf(format, args...)) })
	result, err := f.store.Reconcile(t.Context())
	if err != nil || result.ExpiredAttempts != 0 || result.DeadNodes != 0 || result.StaleNodes != 0 {
		t.Fatalf("uncertain wake expired authority: %+v err=%v", result, err)
	}
	if strings.Contains(strings.Join(logs, "\n"), "event=attempt_lease_clock_retry") {
		t.Fatal("wide sample caused an unbounded clock retry")
	}
	if len(source.samples) != 0 {
		t.Fatal("scripted clock reads were not consumed")
	}
	f.reconcile(t, 0)
	if b, w := sleepLeaseDeadlines(t, f); b != boot+int64(5*time.Minute) || w != wall+int64(5*time.Minute) {
		t.Fatal("tight wake sample failed to recover remaining credit")
	}
	c.advance(lease-62*time.Second, 0)
	f.reconcile(t, 1)
}

func TestLeaseSleepNoDoubleCreditAfterTightening(t *testing.T) {
	f, c, _ := looseInitialSleepFixture(t)
	boot, wall := sleepLeaseDeadlines(t, f)
	c.advance(0, 5*time.Minute)
	f.reconcile(t, 0)
	f.reconcile(t, 0)
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	f.store, err = openTestStore(t, f.path, f.options)
	if err != nil {
		t.Fatal(err)
	}
	f.reconcile(t, 0)
	if b, w := sleepLeaseDeadlines(t, f); b != boot+int64(5*time.Minute) || w != wall+int64(5*time.Minute) {
		t.Fatal("tightening duplicated sleep credit")
	}
	c.advance(30*time.Second, 0)
	f.reconcile(t, 1)
}

func TestLeaseSleepWideWakeDefersDueExpiry(t *testing.T) {
	f, c := newSleepLeaseFixture(t)
	boot, wall := sleepLeaseDeadlines(t, f)
	c.advance(0, 5*time.Minute)
	source := &sequencedSleepClock{base: c, samples: []sleepClockReadSequence{{before: c.awake, boot: c.elapsed, after: c.awake + 31*time.Second, endBoot: c.elapsed + 31*time.Second}}}
	f.store.clock = source
	result, err := f.store.Reconcile(t.Context())
	if err != nil || result.ExpiredAttempts != 0 || result.DeadNodes != 0 || result.StaleNodes != 0 {
		t.Fatalf("wide sample irreversibly expired authority: %+v err=%v", result, err)
	}
	// Reconcile's later Seat-maintenance transaction may already take a tight
	// sample. Keep at least the proved credit, with no more than the real gap.
	if b, w := sleepLeaseDeadlines(t, f); b < boot+int64(269*time.Second) || b > boot+int64(5*time.Minute) || w-wall != b-boot {
		t.Fatalf("uncertain reconciliation lost or invented credit: boot=%v wall=%v", time.Duration(b-boot), time.Duration(w-wall))
	}
	// The tight sample recovers all credit before expiry. The node really
	// spent 31 awake seconds without renewing its 30-second lease, so only
	// this comparison can now prove that real failure and settle it as lost.
	f.reconcile(t, 1)
	if b, w := sleepLeaseDeadlines(t, f); b != boot+int64(5*time.Minute) || w != wall+int64(5*time.Minute) {
		t.Fatal("tight sample did not recover full sleep credit before expiry")
	}
}

func TestLeaseSleepRepeatedWideSamplesDetectFailure(t *testing.T) {
	const lease = 24 * time.Second
	const width = 2 * time.Second
	f, c := newSleepLeaseFixture(t, func(f *attemptClockFixture, _ *sleepTestClock) {
		f.store.leaseDuration = lease
		f.store.nodeStaleAfter = lease / 2
		f.store.nodeDeadAfter = lease
	})
	c.advance(0, 5*time.Minute)
	wake := c.awake
	source := &sequencedSleepClock{base: c, repeatWidth: width}
	f.store.clock = source
	var decisionAge time.Duration
	f.store.reconcileLivenessForTest = func(context.Context, *sql.Tx, time.Time) (int64, error) { decisionAge = c.awake - wake; return 0, nil }
	var lostAge, deadAge time.Duration
	for tick := 0; tick < 20 && (lostAge == 0 || deadAge == 0); tick++ {
		result, err := f.store.Reconcile(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if result.ExpiredAttempts > 0 {
			lostAge = decisionAge
		}
		if result.DeadNodes > 0 {
			deadAge = decisionAge
		}
	}
	if lostAge == 0 || deadAge == 0 || lostAge > lease+width || deadAge > lease+width {
		t.Fatalf("wide samples retained a failed node or Attempt beyond one lease plus width: lost=%v dead=%v bound=%v", lostAge, deadAge, lease+width)
	}
	t.Logf("lost at %s, node dead at %s; lease %s, sample width %s", lostAge, deadAge, lease, width)
	attempts, err := f.store.ListJobAttempts(t.Context(), f.job.JobID)
	if err != nil || len(attempts) != 1 || attempts[0].State != contract.AttemptLost {
		t.Fatalf("attempts=%+v err=%v", attempts, err)
	}
	if _, err := f.store.RenewLease(t.Context(), "agent-lease-node", f.job.JobID, f.claim.Lease.AttemptID, f.claim.Lease.FencingToken); err == nil {
		t.Fatal("lost Attempt renewed under a wide sample")
	}
}

func TestLeaseSleepAmbiguousRenewalDoesNotLoseAttempt(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint(legacy), func(t *testing.T) {
			f, c := newSleepLeaseFixture(t)
			if legacy {
				retentionSweepExec(t, f.store, `UPDATE attempts SET lease_boot_id='',lease_deadline_ns=0 WHERE attempt_id=?`, f.claim.Lease.AttemptID)
			}
			c.advance(0, 5*time.Minute)
			source := &sequencedSleepClock{base: c, samples: []sleepClockReadSequence{{before: c.awake, boot: c.elapsed, after: c.awake + 31*time.Second, endBoot: c.elapsed + 31*time.Second}}}
			f.store.clock = source
			_, err := f.store.RenewLease(t.Context(), "agent-lease-node", f.job.JobID, f.claim.Lease.AttemptID, f.claim.Lease.FencingToken)
			if !errors.Is(err, errAttemptLeaseUncertain) {
				t.Fatalf("ambiguous renewal granted authority or proved expiry: %v", err)
			}
			var state contract.AttemptState
			if err := f.store.db.QueryRow(`SELECT state FROM attempts WHERE attempt_id=?`, f.claim.Lease.AttemptID).Scan(&state); err != nil || state != contract.AttemptClaimed {
				t.Fatalf("ambiguous renewal changed state: %s err=%v", state, err)
			}
		})
	}
}

func TestLeaseSleepUpperCreditNeverPersists(t *testing.T) {
	f, c := newSleepLeaseFixture(t)
	boot, wall := sleepLeaseDeadlines(t, f)
	c.advance(0, 5*time.Minute)
	source := &sequencedSleepClock{base: c, samples: []sleepClockReadSequence{{before: c.awake, boot: c.elapsed, after: c.awake + 2*time.Second, endBoot: c.elapsed + 2*time.Second}}}
	f.store.clock = source
	write, err := f.store.beginWriteTransaction(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer write.rollback()
	if currentAttemptClock(write.ctx).sleepUncertainty != 2*time.Second {
		t.Fatal("possible credit was discarded")
	}
	if err := write.commit(); err != nil {
		t.Fatal(err)
	}
	if b, w := sleepLeaseDeadlines(t, f); b != boot+int64(298*time.Second) || w != wall+int64(298*time.Second) {
		t.Fatal("possible credit was persisted as proved credit")
	}
	lo, hi := readSleepAnchor(t, f.store)
	if lo != 298*time.Second || hi != 298*time.Second {
		t.Fatalf("consumed anchor=[%v,%v]", lo, hi)
	}
	f.reconcile(t, 0)
	f.reconcile(t, 0)
	if b, w := sleepLeaseDeadlines(t, f); b != boot+int64(5*time.Minute) || w != wall+int64(5*time.Minute) {
		t.Fatal("upper credit was counted twice")
	}
}

func TestLeaseSleepWideRenewalUsesOnlyProvenCredit(t *testing.T) {
	const lease = 90 * time.Second
	f, c := newSleepLeaseFixture(t, func(f *attemptClockFixture, _ *sleepTestClock) { f.store.leaseDuration = lease })
	c.advance(0, 5*time.Minute)
	beforeWall, beforeBoot := c.Now(), c.elapsed
	f.store.clock = &sequencedSleepClock{base: c, samples: []sleepClockReadSequence{{before: c.awake, boot: c.elapsed, after: c.awake + 31*time.Second, endBoot: c.elapsed + 31*time.Second}}}
	renewed, err := f.store.RenewLease(t.Context(), "agent-lease-node", f.job.JobID, f.claim.Lease.AttemptID, f.claim.Lease.FencingToken)
	if err != nil {
		t.Fatalf("proved-live lease refused renewal: %v", err)
	}
	boot, wall := sleepLeaseDeadlines(t, f)
	if renewed.LeaseTTL != lease || boot != int64(beforeBoot+lease) || wall != beforeWall.Add(lease).UnixNano() {
		t.Fatalf("renewal granted possible credit: lease=%+v boot=%v wall=%v", renewed, time.Duration(boot), time.Unix(0, wall))
	}
}

func TestLeaseSleepWideSampleRefusesSupersededAttempt(t *testing.T) {
	f, c := newSleepLeaseFixture(t, func(f *attemptClockFixture, _ *sleepTestClock) { f.store.leaseDuration = 90 * time.Second })
	if _, err := f.store.db.Exec(`UPDATE jobs SET current_attempt_id=NULL WHERE job_id=?`, f.job.JobID); err != nil {
		t.Fatal(err)
	}
	c.advance(0, 5*time.Minute)
	f.store.clock = &sequencedSleepClock{base: c, samples: []sleepClockReadSequence{{before: c.awake, boot: c.elapsed, after: c.awake + 31*time.Second, endBoot: c.elapsed + 31*time.Second}}}
	if _, err := f.store.RenewLease(t.Context(), "agent-lease-node", f.job.JobID, f.claim.Lease.AttemptID, f.claim.Lease.FencingToken); err == nil {
		t.Fatal("wide sample renewed a superseded Attempt")
	}
}

func TestLeaseSleepGenerationClosureUsesPossibleCredit(t *testing.T) {
	f := newGenerationFixture(t)
	claim := f.claim(t, "wide-generation-resident")
	f.promote(t, "g2")
	elapsed := time.Duration(f.h.clock.Now().UnixNano())
	c := &sleepTestClock{seatBootTestClock: &seatBootTestClock{fakeClock: f.h.clock, boot: "injected-clock", elapsed: elapsed}, awake: elapsed}
	source := &sequencedSleepClock{base: c}
	f.h.store.clock = source
	if err := initializeLeaseSleepClock(t.Context(), f.h.store.db, source); err != nil {
		t.Fatal(err)
	}
	width := claim.Lease.LeaseTTL + time.Second
	c.advance(0, 5*time.Minute+width)
	source.samples = []sleepClockReadSequence{{before: c.awake, boot: c.elapsed, after: c.awake + width, endBoot: c.elapsed + width}}
	ready, err := f.h.store.NodeGenerationClosureReady(t.Context(), "agent", "node", "g1")
	if err != nil || ready {
		t.Fatalf("possible-live resident permitted generation closure: ready=%v err=%v", ready, err)
	}
}

func TestLeaseSleepPossibleCreditDoesNotDelayCancellation(t *testing.T) {
	f, c := newSleepLeaseFixture(t, func(f *attemptClockFixture, _ *sleepTestClock) { f.store.leaseDuration = 90 * time.Second })
	if _, err := f.store.db.Exec(`UPDATE jobs SET outcome='canceled',cancel_settle_by_ns=? WHERE job_id=?`, c.Now().Add(CancelSettlementTimeout).UnixNano(), f.job.JobID); err != nil {
		t.Fatal(err)
	}
	c.advance(0, CancelSettlementTimeout)
	f.store.clock = &sequencedSleepClock{base: c, samples: []sleepClockReadSequence{{before: c.awake, boot: c.elapsed, after: c.awake + 2*time.Second, endBoot: c.elapsed + 2*time.Second}}}
	result, err := f.store.Reconcile(t.Context())
	if err != nil || result.ExpiredAttempts != 1 {
		t.Fatalf("possible sleep credit delayed a wall-based cancellation: %+v err=%v", result, err)
	}
}

func readSleepAnchor(t *testing.T, store *Store) (time.Duration, time.Duration) {
	t.Helper()
	var lo, hi int64
	if err := store.db.QueryRow(`SELECT offset_lo_ns,offset_hi_ns FROM lease_sleep_clock WHERE singleton=1`).Scan(&lo, &hi); err != nil {
		t.Fatal(err)
	}
	if lo > hi {
		t.Fatalf("inverted sleep anchor [%v,%v]", time.Duration(lo), time.Duration(hi))
	}
	return time.Duration(lo), time.Duration(hi)
}

// Model upgrading an existing live lease from before sleep-gap accounting.
// OpenStore creates the absent anchor using actual sequenced clock reads.
// Every initialization bracket is [0,31s]; no later tightening transaction
// occurs between initialization, the remaining-wait advance, and the sleep.
func existingAttemptWithLooseSleepInit(t *testing.T, remaining time.Duration) (*attemptClockFixture, *sleepTestClock) {
	t.Helper()
	f, c := newSleepLeaseFixture(t, func(f *attemptClockFixture, _ *sleepTestClock) {
		f.store.leaseDuration = time.Hour
		f.options.LeaseDuration = time.Hour
		f.store.nodeStaleAfter = 2 * time.Hour
		f.store.nodeDeadAfter = 4 * time.Hour
		f.options.NodeStaleAfter = 2 * time.Hour
		f.options.NodeDeadAfter = 4 * time.Hour
	})
	deadline, _ := sleepLeaseDeadlines(t, f)
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", f.path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE lease_sleep_clock`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	source := &sequencedSleepClock{base: c, repeatWidth: 31 * time.Second, beforeBoot: true}
	f.options.Clock = source
	f.store, err = openTestStore(t, f.path, f.options)
	if err != nil {
		t.Fatal(err)
	}
	source.repeatWidth = 0
	lo, hi := readSleepAnchor(t, f.store)
	if lo != 0 || hi != 31*time.Second {
		t.Fatalf("OpenStore init anchor=[%v,%v]", lo, hi)
	}
	advance := time.Duration(deadline) - c.elapsed - remaining
	if advance < 0 {
		t.Fatalf("initialization consumed the test lease: remaining=%v", time.Duration(deadline)-c.elapsed)
	}
	c.advance(advance, 0)
	return f, c
}

func TestLeaseSleepLooseInitPreservesExistingAttempt(t *testing.T) {
	f, c := existingAttemptWithLooseSleepInit(t, 10*time.Second)
	c.advance(0, 5*time.Minute)
	f.reconcile(t, 0)
	attempts, err := f.store.ListJobAttempts(t.Context(), f.job.JobID)
	if err != nil || len(attempts) != 1 || attempts[0].State == contract.AttemptLost {
		t.Fatalf("loose init lost an existing lease: %+v err=%v", attempts, err)
	}
	// Renewal needs proved credit; the 31 s of initial uncertainty is unproved.
	if _, err := f.store.RenewLease(t.Context(), "agent-lease-node", f.job.JobID, f.claim.Lease.AttemptID, f.claim.Lease.FencingToken); !errors.Is(err, errAttemptLeaseUncertain) {
		t.Fatalf("10-second lease should survive without unproved renewal: %v", err)
	}
	attempts, err = f.store.ListJobAttempts(t.Context(), f.job.JobID)
	if err != nil || len(attempts) != 1 || attempts[0].State == contract.AttemptLost {
		t.Fatalf("refused renewal lost an existing lease: %+v err=%v", attempts, err)
	}
}

func TestLeaseSleepLooseInitAllowsProvedRenewal(t *testing.T) {
	f, c := existingAttemptWithLooseSleepInit(t, 40*time.Second)
	c.advance(0, 5*time.Minute)
	f.reconcile(t, 0)
	if _, err := f.store.RenewLease(t.Context(), "agent-lease-node", f.job.JobID, f.claim.Lease.AttemptID, f.claim.Lease.FencingToken); err != nil {
		t.Fatalf("proved-live lease refused after loose initialization: %v", err)
	}
}

func TestLeaseSleepLooseInitRealFailureIsBounded(t *testing.T) {
	const remaining = 10 * time.Second
	const initWidth = 31 * time.Second
	f, c := existingAttemptWithLooseSleepInit(t, remaining)
	c.advance(0, 5*time.Minute)
	f.reconcile(t, 0)
	var lostAt time.Duration
	for awake := time.Second; awake <= remaining+initWidth; awake += time.Second {
		c.advance(time.Second, 0)
		result, err := f.store.Reconcile(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if result.ExpiredAttempts > 0 {
			lostAt = awake
			break
		}
	}
	// Tight samples after wake leave only the initial width as slack, and that
	// slack exactly covers the unproved part of the sleep: loss comes when the
	// true remaining lease runs out.
	if lostAt != remaining {
		t.Fatalf("initial-width failure bound: lost=%v remaining=%v init=%v", lostAt, remaining, initWidth)
	}
	t.Logf("lost at %s after wake; remaining lease %s, init width %s", lostAt, remaining, initWidth)
}

func TestLeaseSleepIntervalAdvanceAndReopenKeepWidth(t *testing.T) {
	f, c := existingAttemptWithLooseSleepInit(t, 10*time.Second)
	boot, wall := sleepLeaseDeadlines(t, f)
	c.advance(0, 5*time.Minute)
	f.reconcile(t, 0)
	lo, hi := readSleepAnchor(t, f.store)
	if lo != 269*time.Second || hi != 300*time.Second {
		t.Fatalf("credit discarded initial width: [%v,%v]", lo, hi)
	}
	c.advance(0, time.Minute)
	f.reconcile(t, 0)
	f.reconcile(t, 0)
	lo, hi = readSleepAnchor(t, f.store)
	if lo != 329*time.Second || hi != 360*time.Second {
		t.Fatalf("second credit discarded initial width: [%v,%v]", lo, hi)
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	f.store, err = openTestStore(t, f.path, f.options)
	if err != nil {
		t.Fatal(err)
	}
	afterLo, afterHi := readSleepAnchor(t, f.store)
	if afterLo != lo || afterHi != hi {
		t.Fatalf("reopen replaced anchor [%v,%v] with [%v,%v]", lo, hi, afterLo, afterHi)
	}
	f.reconcile(t, 0)
	if b, w := sleepLeaseDeadlines(t, f); b != boot+int64(329*time.Second) || w != wall+int64(329*time.Second) {
		t.Fatal("interval credit was applied twice")
	}
}

func TestLeaseSleepIntervalTighteningLeavesLowerBound(t *testing.T) {
	f, c := existingAttemptWithLooseSleepInit(t, 10*time.Second)
	boot, wall := sleepLeaseDeadlines(t, f)
	c.advance(0, 20*time.Second)
	write, err := f.store.beginWriteTransaction(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer write.rollback()
	if err := write.commit(); err != nil {
		t.Fatal(err)
	}
	lo, hi := readSleepAnchor(t, f.store)
	if lo != 0 || hi != 20*time.Second {
		t.Fatalf("tightening raised the lower bound: [%v,%v]", lo, hi)
	}
	if b, w := sleepLeaseDeadlines(t, f); b != boot || w != wall {
		t.Fatal("tightening invented proved credit")
	}
	c.advance(0, 5*time.Minute)
	f.reconcile(t, 0)
	f.reconcile(t, 0)
	lo, hi = readSleepAnchor(t, f.store)
	if lo != 300*time.Second || hi != 320*time.Second {
		t.Fatalf("tightened interval lost uncredited sleep: [%v,%v]", lo, hi)
	}
	if b, w := sleepLeaseDeadlines(t, f); b != boot+int64(300*time.Second) || w != wall+int64(300*time.Second) {
		t.Fatal("tightened interval double-credited sleep")
	}
}
