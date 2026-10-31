package l1

import (
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestAttemptLeaseLiveReadSurvivesSamplingFailure(t *testing.T) {
	f := newAttemptClockFixture(t)
	f.store.clock = failedSeatClock{f.clock}
	attempts, err := f.store.ListJobAttempts(t.Context(), f.job.JobID)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("attempts=%+v err=%v", attempts, err)
	}
	if attempts[0].AttemptID != f.claim.Lease.AttemptID || attempts[0].State != contract.AttemptClaimed || attempts[0].LeaseLive {
		t.Fatalf("sampling failure changed the Attempt or reported a live lease: %+v", attempts[0])
	}
}

func TestAttemptLeaseLiveReadUsesAuthorityClock(t *testing.T) {
	f := newAttemptClockFixture(t)
	check := func(want bool) {
		t.Helper()
		attempts, err := f.store.ListJobAttempts(t.Context(), f.job.JobID)
		if err != nil || len(attempts) != 1 {
			t.Fatalf("attempts=%+v err=%v", attempts, err)
		}
		if attempts[0].LeaseLive != want {
			t.Fatalf("lease_live=%t want=%t", attempts[0].LeaseLive, want)
		}
		job, err := f.store.readJobResource(t.Context(), f.job.JobID, nil, nil)
		if err != nil || len(job.Attempts) != 1 || job.Attempts[0].LeaseLive != want {
			t.Fatalf("Job lease projection=%+v err=%v", job, err)
		}
	}
	f.clock.Advance(2 * time.Hour)
	check(true)
	f.clock.Advance(-4 * time.Hour)
	f.clock.elapsed += time.Minute
	check(false)
}

func TestGenerationAttemptLeaseLiveReadUsesAuthorityClock(t *testing.T) {
	f := newGenerationFixture(t)
	clock := &seatBootTestClock{fakeClock: f.h.clock, boot: "l1-boot", elapsed: time.Hour}
	f.h.store.clock = clock
	f.claim(t, "resident")
	for _, want := range []bool{true, false} {
		if want {
			clock.Advance(2 * time.Hour)
		} else {
			clock.Advance(-4 * time.Hour)
			clock.elapsed += DefaultLeaseDuration
		}
		node, err := f.h.store.GetNode(t.Context(), "node")
		if err != nil || len(node.Generations) != 1 || len(node.Generations[0].Attempts) != 1 {
			t.Fatalf("node=%+v err=%v", node, err)
		}
		a := node.Generations[0].Attempts[0]
		if a.LeaseLive == nil || *a.LeaseLive != want {
			t.Fatalf("generation lease_live=%v want=%t", a.LeaseLive, want)
		}
	}
}
