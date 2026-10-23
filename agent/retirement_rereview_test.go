//go:build darwin || linux

package agent

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
)

func TestRetirementRecoveredProofSurvivesReceiptPruning(t *testing.T) {
	var blocked atomic.Bool
	blocked.Store(true)
	a, store, done, _ := newRetirementFixtureWithHook(t, func(a *Agent, _ *l1.Store) { blockReviewLogs(a, &blocked) })
	job, ready, _ := retirementReviewJob(t, store, "rereview-recovered-proof", false, nil)
	retirementWait(t, "payload ready", func() bool { _, err := os.Stat(ready); return err == nil })
	if _, err := store.CancelJob(t.Context(), job.JobID, l1.JobCancelCaller{Submitter: "review-submitter"}); err != nil {
		t.Fatal(err)
	}
	retirementWait(t, "canceled primary runtime joined", func() bool { return a.Status().OneShot.Occupied == 0 })
	attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("attempts=%+v %v", attempts, err)
	}
	attemptID := attempts[0].AttemptID
	receipt := a.outbox.spool.inspectCompletion(t.Context(), attemptID)
	if receipt.State != "durable_completion" || receipt.EventCount == 0 {
		t.Fatalf("expected pending evidence after worker exit: %+v", receipt)
	}
	if a.session.isRetiring() {
		t.Fatal("recovery must precede retirement")
	}
	blocked.Store(false)
	a.outbox.scheduleRecovery()
	retirementWait(t, "background delivery while primary", func() bool {
		receipt := a.outbox.spool.inspectCompletion(t.Context(), attemptID)
		return receipt.State == "delivered" && receipt.EventCount == 0
	})
	retirementWait(t, "recovered acceptance transferred out of outbox", func() bool {
		a.outbox.ownershipMu.RLock()
		defer a.outbox.ownershipMu.RUnlock()
		return len(a.outbox.retirementCompletions) == 0
	})
	// Exercise the actual bounded receipt pruning with newer delivery commits.
	for i := 0; i <= maxCompletionInspectionReceipts; i++ {
		if err := a.outbox.completionDelivered(t.Context(), fmt.Sprintf("newer-receipt-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if receipt := a.outbox.spool.inspectCompletion(t.Context(), attemptID); receipt.State != "never_persisted" {
		t.Fatalf("old marker was not pruned: %+v", receipt)
	}
	promoteRetirementFixture(t, store, "node", "retirement-agent")
	if _, err := a.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	reviewRetirementExit(t, a, done)
}

func TestRetirementSettledAttemptsReleaseOutboxTracking(t *testing.T) {
	a, store, done, _ := newRetirementFixture(t)
	for i := 0; i < 32; i++ {
		_, ready, release := retirementReviewJob(t, store, fmt.Sprintf("settled-tracking-%d", i), false, nil)
		retirementWait(t, "payload ready", func() bool { _, err := os.Stat(ready); return err == nil })
		if err := os.WriteFile(release, nil, 0600); err != nil {
			t.Fatal(err)
		}
		retirementWait(t, "settled runtime joined", func() bool { return a.Status().OneShot.Occupied == 0 })
		a.outbox.ownershipMu.RLock()
		retained := len(a.outbox.retirementCompletions)
		a.outbox.ownershipMu.RUnlock()
		if retained != 0 {
			t.Fatalf("after %d settled Attempts, outbox retains %d completion entries", i+1, retained)
		}
	}
	promoteRetirementFixture(t, store, "node", "retirement-agent")
	if _, err := a.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	reviewRetirementExit(t, a, done)
}

func TestOutboxRetirementInspectionDoesNotDeadlockSweep(t *testing.T) {
	outbox, err := newEvidenceOutbox(t.TempDir(), "node", 1<<20, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	claim := spoolTestClaim("sweep-candidate")
	if err := outbox.spool.ensureAttempt(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	sweepHasDB, inspectReached, resumeInspect, resumeSweep := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	writerDone, releaseWriter := make(chan struct{}), make(chan struct{})
	var inspectOnce, sweepOnce, writerOnce sync.Once
	unblockInspect := func() { inspectOnce.Do(func() { close(resumeInspect) }) }
	unblockSweep := func() { sweepOnce.Do(func() { close(resumeSweep) }) }
	unblockWriter := func() { writerOnce.Do(func() { close(releaseWriter) }) }
	defer unblockInspect()
	defer unblockSweep()
	defer unblockWriter()
	outbox.spool.inspectCompletionCheckpoint = func() { close(inspectReached); <-resumeInspect }
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	sweepDone := make(chan error, 1)
	go func() {
		_, err := outbox.spool.sweepDeadOneShotAttempts(ctx, 1, func(id string) bool {
			close(sweepHasDB)
			<-resumeSweep
			return outbox.attemptIsLive(id)
		}, outbox.attemptCreatedHere)
		sweepDone <- err
	}()
	reviewWait(t, "sweep did not acquire sole DB connection", sweepHasDB)
	checkDone := make(chan error, 1)
	go func() { _, err := outbox.attemptEvidenceDelivered(ctx, claim.Lease.AttemptID, false); checkDone <- err }()
	reviewWait(t, "inspection did not reach SQL boundary", inspectReached)
	go func() {
		outbox.ownershipMu.Lock()
		<-releaseWriter
		outbox.ownershipMu.Unlock()
		close(writerDone)
	}()
	// TryRLock fails once the writer owns the lock or is queued behind inspection.
	retirementWait(t, "ownership writer queued", func() bool {
		if !outbox.ownershipMu.TryRLock() {
			return true
		}
		outbox.ownershipMu.RUnlock()
		return false
	})
	unblockWriter()
	waits := outbox.spool.db.Stats().WaitCount
	unblockInspect()
	retirementWait(t, "inspection queued for sweep's DB connection", func() bool { return outbox.spool.db.Stats().WaitCount > waits })
	unblockSweep()
	if err := <-checkDone; err != nil {
		t.Errorf("inspection stalled in ownership/SQL cycle: %v", err)
	}
	if err := <-sweepDone; err != nil {
		t.Errorf("sweep stalled in ownership/SQL cycle: %v", err)
	}
	reviewWait(t, "writer did not finish", writerDone)
}
