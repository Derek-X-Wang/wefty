//go:build darwin || linux

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestGenerationCohortLocksExcludeDuplicateAndLegacyProcesses(t *testing.T) {
	root := t.TempDir()
	for _, id := range []string{"one", "two", "three"} {
		lock, err := acquireAgentNodeLock(root, "node", id)
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Close()
		duplicate, err := acquireAgentNodeLock(root, "node", id)
		if err == nil {
			duplicate.Close()
			t.Fatal("duplicate generation admitted")
		}
	}
	legacy, err := acquireAgentNodeLock(root, "node", "")
	if err == nil {
		legacy.Close()
		t.Fatal("legacy process admitted during overlap")
	}
}

func TestGenerationConcurrentSpoolRecoverySweepAndAggregateBudget(t *testing.T) {
	for _, count := range []int{2, 3} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			root := t.TempDir()
			spools := make([]*logSpool, count)
			for i := range spools {
				spool, err := openLogSpool(root, "node", 12)
				if err != nil {
					t.Fatal(err)
				}
				defer spool.Close()
				spool.generationID = fmt.Sprint(i)
				spools[i] = spool
				for _, suffix := range []string{"empty", "pending"} {
					claim := spoolTestClaim(fmt.Sprintf("%d-%s", i, suffix))
					if err := spool.ensureAttempt(t.Context(), claim); err != nil {
						t.Fatal(err)
					}
					if suffix == "pending" {
						if err := spool.append(t.Context(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 0, "1234")); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			// A live generation's empty row is absent from every other process map.
			// Exercise concurrent SQL sweeps and recovery selection repeatedly.
			var wg sync.WaitGroup
			failures := make(chan error, count)
			for i, spool := range spools {
				wg.Add(1)
				go func(i int, s *logSpool) {
					defer wg.Done()
					for range 5 {
						_, err := s.sweepDeadOneShotAttempts(t.Context(), 256, func(string) bool { return true }, nil)
						if err != nil {
							failures <- err
							return
						}
						pending, err := s.pendingAttempts(t.Context())
						if err != nil {
							failures <- err
							return
						}
						if len(pending) != 1 || pending[0].attemptID != fmt.Sprintf("%d-pending", i) {
							failures <- fmt.Errorf("foreign recovery selection: %+v", pending)
							return
						}
					}
				}(i, spool)
			}
			wg.Wait()
			close(failures)
			for err := range failures {
				t.Fatal(err)
			}
			// Actual foreign cleanup uses an empty ownership map, including for newly
			// created empty rows. Own rows stay protected by the production live map.
			for i, s := range spools {
				_, err := s.sweepDeadOneShotAttempts(t.Context(), 256, func(id string) bool { return id == fmt.Sprintf("%d-empty", i) }, nil)
				if err != nil {
					t.Fatal(err)
				}
				for j := range spools {
					if !spoolRowExists(t, s, fmt.Sprintf("%d-empty", j)) {
						t.Fatal("another generation's empty row swept")
					}
				}
			}
			err := spools[0].append(t.Context(), spoolTestEvent("0-pending", contract.LogStdout, 1, "12345"))
			if !errors.Is(err, ErrLogSpoolFull) {
				t.Fatalf("aggregate budget did not count %d generations: %v", count, err)
			}
			// Closure is positive durable authority; omission and age did not grant it.
			if _, err := spools[count-1].db.Exec(`INSERT INTO closed_agent_generations VALUES('0')`); err != nil {
				t.Fatal(err)
			}
			if _, err := spools[count-1].sweepDeadOneShotAttempts(t.Context(), 256, nil, nil); err != nil {
				t.Fatal(err)
			}
			if spoolRowExists(t, spools[count-1], "0-empty") {
				t.Fatal("closed generation residue retained")
			}
		})
	}
}

func TestGenerationHandoffsKeepOpenMailboxesResultsAndAggregateAccounting(t *testing.T) {
	for _, count := range []int{2, 3} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			root, state := filepath.Join(t.TempDir(), "handoffs"), t.TempDir()
			managers := make([]*handoffManager, count)
			mailboxes := make([]*runMailbox, count)
			owners := make([]*handoffOwnership, count)
			now := time.Now().UTC()
			for i := range managers {
				m := newHandoffManager(root, state, "node-1", time.Hour, nil)
				m.now = func() time.Time { return now }
				m.nodeBytes = 1
				managers[i] = m
				id := fmt.Sprintf("run_%d", i)
				spec := handoffClaim(id, filepath.Join(root, id), nil).Job.Spec
				spec.Execution.Env = map[string]string{contract.EnvRunID: id}
				owners[i] = prepareHandoffForTest(t, m, spec)
				mailbox, err := prepareRunMailbox(spec, fmt.Sprintf("attempt_%d", i), newRecordingAppender(""), time.Hour, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer mailbox.close()
				mailboxes[i] = mailbox
				if err := os.WriteFile(filepath.Join(mailbox.directory, "capture.bundle"), []byte("capture"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, id, "result.json"), []byte("result"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(mailbox.directory, "events", "pending"), []byte("event"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			now = now.Add(48 * time.Hour)
			var wg sync.WaitGroup
			failures := make(chan error, count)
			for _, m := range managers {
				wg.Add(1)
				go func(m *handoffManager) {
					defer wg.Done()
					if err := m.adoptResidue(); err != nil {
						failures <- err
						return
					}
					if err := m.collect(); err != nil {
						failures <- err
						return
					}
					if err := m.accountNode(context.Background()); err != nil {
						failures <- err
					}
				}(m)
			}
			wg.Wait()
			close(failures)
			for err := range failures {
				t.Fatal(err)
			}
			for i, m := range managers {
				for name, want := range map[string]string{"events/pending": "event", "capture.bundle": "capture"} {
					content, truncated, err := readBoundedRegularFile(mailboxes[i].root, name, 1024)
					if err != nil || truncated || string(content) != want {
						t.Fatalf("live mailbox/capture changed: %s %q %v", name, content, err)
					}
				}
				var record retentionRecord
				found := false
				for _, r := range m.loadRecords() {
					if r.RunID == fmt.Sprintf("run_%d", i) {
						record = r
						found = true
					}
				}
				if !found || !record.RetainUntil.IsZero() {
					t.Fatalf("live admission adopted: %+v", record)
				}
				if _, err := os.Stat(filepath.Join(root, fmt.Sprintf("run_%d", i), "result.json")); err != nil {
					t.Fatal(err)
				}
			}
			handoff, err := managers[0].openHandoffRoot()
			if err != nil {
				t.Fatal(err)
			}
			defer handoff.Close()
			status := managers[0].measureNode(t.Context(), handoff, now)
			if status.Runs != count || status.InFlight != count || status.LogicalBytes < int64(count*11) {
				t.Fatalf("aggregate accounting: %+v", status)
			}
			// A candidate cannot prepare/clear an open handoff or its capture files.
			if lease := managers[count-1].tryCollectLease(owners[0].lease.path); lease != nil {
				lease.release()
				t.Fatal("foreign open handoff was collectible")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			if lease, err := managers[count-1].lockPath(ctx, owners[0].lease.path); err == nil {
				lease.release()
				t.Fatal("candidate took open handoff")
			}
		})
	}
}

func TestGenerationCustodyRetainsExactEvidenceAndRequiresPublication(t *testing.T) {
	root := t.TempDir()
	source := openTestLogSpool(t, root, "node", 1024)
	defer source.Close()
	source.generationID = "old"
	claim := spoolTestClaim("custody")
	if err := source.ensureAttempt(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	if err := source.append(t.Context(), spoolTestEvent("custody", contract.LogStdout, 0, "final log")); err != nil {
		t.Fatal(err)
	}
	exit := 17
	if err := source.storeCompletion(t.Context(), "custody", l1.ProcessResult{ExitCode: &exit}, time.Now()); err != nil {
		t.Fatal(err)
	}
	identity := l1.ResidentAttempt{JobID: claim.Job.JobID, AttemptID: "custody", FencingToken: claim.Lease.FencingToken, BootSessionID: "boot", AuthorityGeneration: 1}
	if _, err := source.db.Exec(`INSERT INTO generation_evidence_custodians VALUES('new')`); err != nil {
		t.Fatal(err)
	}
	ok, err := source.acknowledgeEvidenceCustody(t.Context(), "old", "new", identity, 1)
	if err != nil || ok {
		t.Fatalf("unpublished evidence transferred: %v %v", ok, err)
	}
	if _, err := source.db.Exec(`UPDATE spool_attempts SET publication_settled=1`); err != nil {
		t.Fatal(err)
	}
	ok, err = source.acknowledgeEvidenceCustody(t.Context(), "old", "new", identity, 1)
	if err != nil || !ok {
		t.Fatalf("custody: %v %v", ok, err)
	}
	source.Close()
	target := openTestLogSpool(t, root, "node", 1024)
	defer target.Close()
	target.generationID = "new"
	var digest string
	if err := target.db.QueryRow(`SELECT digest FROM generation_evidence_custody WHERE attempt_id='custody'`).Scan(&digest); err != nil || len(digest) != 64 {
		t.Fatalf("durable receipt: %q %v", digest, err)
	}
	pending, err := target.pendingAttempts(t.Context())
	if err != nil || len(pending) != 0 {
		t.Fatal("custody granted live authority", pending, err)
	}
	if _, err := target.db.Exec(`INSERT INTO closed_agent_generations VALUES('old')`); err != nil {
		t.Fatal(err)
	}
	pending, err = target.pendingAttempts(t.Context())
	if err != nil || len(pending) != 1 {
		t.Fatalf("closed historical evidence missing: %+v %v", pending, err)
	}
	completion, _, present, err := target.durableCompletion(t.Context(), "custody")
	if err != nil || !present || completion.Result.ExitCode == nil || *completion.Result.ExitCode != 17 {
		t.Fatalf("completion changed: %+v %v", completion, err)
	}
	batch, err := target.pendingBatch(t.Context(), "custody", 10)
	if err != nil || len(batch) != 1 || string(batch[0].event.Bytes) != "final log" {
		t.Fatalf("log changed: %+v %v", batch, err)
	}
}

// Exercise real node agent Run loops, live shell processes and two successive L1
// promotions against one spool, managed root, result root and capability floor.
func TestGenerationThreeRunningAgentsShareNodeLocalState(t *testing.T) {
	network := plain.NewNetwork()
	store, stop := startFailureServerWithPoliciesAndLease(t, network, nil, map[string]l1.NodePolicy{"node": {Tags: []string{"linux"}, MaxOneshotSlots: 3}}, l1.DefaultLeaseDuration)
	t.Cleanup(stop)
	spoolRoot, handoffRoot, managedRoot := t.TempDir(), t.TempDir(), temporaryManagedRoot(t)
	floorPath := filepath.Join(t.TempDir(), "floor.json")
	var primaryMu sync.Mutex
	primary := "g0"
	var agents []*Agent
	var done []chan error
	var jobs []l1.Job
	var releases []string
	for i := 0; i < 3; i++ {
		id, boot := fmt.Sprintf("g%d", i), fmt.Sprintf("boot%d", i)
		if i > 0 {
			node, err := store.GetNode(t.Context(), "node")
			if err != nil {
				t.Fatal(err)
			}
			node, err = store.SetNodeClaimsByOperator(t.Context(), "node", "operator", l1.NodeIntentRequest{ClaimsEnabled: false, IntentRevision: node.IntentRevision, Reason: "upgrade"})
			if err != nil {
				t.Fatal(err)
			}
			candidate := agents[i-1].registration
			candidate.AgentGenerationID = id
			candidate.BootSessionID = boot
			inventory, err := store.StageNodeGeneration(t.Context(), "cohort-agent", candidate)
			if err != nil {
				t.Fatal(err)
			}
			var old l1.NodeGeneration
			retirees := []string{}
			for _, g := range inventory.Generations {
				if g.Role == l1.NodeGenerationPrimary {
					old = g
				}
				if g.Role == l1.NodeGenerationRetiring {
					retirees = append(retirees, g.AgentGenerationID)
				}
			}
			_, err = store.PromoteNodeGeneration(t.Context(), "cohort-agent", "node", l1.PromoteNodeGenerationRequest{OperationID: "promote-" + id, ExpectedPrimaryGenerationID: old.AgentGenerationID, ExpectedPrimaryBootSessionID: old.BootSessionID, ExpectedAuthorityGeneration: old.AuthorityGeneration, CandidateGenerationID: id, CandidateBootSessionID: boot, ExpectedCatalogRevision: inventory.CatalogRevision, CatalogRevision: inventory.CatalogRevision + 1, IntentRevision: node.IntentRevision, SelectedProtocol: 1, RetiringGenerationIDs: retirees, ResidentAttempts: old.ResidentAttempts})
			if err != nil {
				t.Fatal(err)
			}
			primaryMu.Lock()
			primary = id
			primaryMu.Unlock()
			if _, err := agents[i-1].Retire(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
		zero := 0.0
		a, err := New(Config{Fabric: network.NewFabric(fabric.Identity{NodeID: "cohort-agent", Tags: []string{l1.DefaultAgentPrincipalTag}}), ControlPlaneAddress: "wefty://control-plane", NodeID: "node", BootSessionID: boot, AgentGenerationID: id, OS: "darwin", Version: id, Capabilities: map[string]bool{"kind:process": true, "upgrade:retirement:v1": true, contract.CapabilityExecCaptureBundle: true}, GuardianExecutable: agentBinaryPath, LogSpoolDirectory: spoolRoot, HandoffRoot: handoffRoot, ManagedRootDirectory: managedRoot, CapabilityRevisionPath: floorPath, MaxOneshotSlots: 3, OneshotLoadLimit: &zero, HeartbeatInterval: 50 * time.Millisecond, ClaimInterval: 10 * time.Millisecond, RenewalInterval: 100 * time.Millisecond,
			PrimaryGeneration: func(write func() error) error {
				primaryMu.Lock()
				defer primaryMu.Unlock()
				if primary != id {
					return errors.New("not primary")
				}
				return write()
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		agents = append(agents, a)
		ctx, cancel := context.WithCancel(context.Background())
		finished := make(chan error, 1)
		done = append(done, finished)
		go func() { finished <- a.Run(ctx); close(finished) }()
		t.Cleanup(func() {
			cancel()
			select {
			case <-finished:
			case <-time.After(15 * time.Second):
				t.Error("agent did not join")
			}
			a.Close()
		})
		select {
		case <-a.Registered():
		case err := <-finished:
			t.Fatalf("node generation stopped before registration: %v", err)
		case <-time.After(testwait.Bound):
			t.Fatal("registration timed out")
		}
		node, err := store.GetNode(t.Context(), "node")
		if err != nil {
			t.Fatal(err)
		}
		if i > 0 {
			_, err = store.SetNodeClaimsByOperator(t.Context(), "node", "operator", l1.NodeIntentRequest{ClaimsEnabled: true, IntentRevision: node.IntentRevision, Reason: "upgrade complete"})
			if err != nil {
				t.Fatal(err)
			}
		}
		job, ready, release := retirementReviewJob(t, store, "cohort-work-"+id, false, nil)
		jobs = append(jobs, job)
		releases = append(releases, release)
		t.Cleanup(func() { _ = os.WriteFile(release, nil, 0600) })
		retirementWait(t, "cohort payload ready", func() bool { _, err := os.Stat(ready); return err == nil })
		if err := a.outbox.ensureAttempt(t.Context(), spoolTestClaim("empty-"+id)); err != nil {
			t.Fatal(err)
		}
	}
	// Filesystem sync under -race can outlast a tiny test Lease. Use the
	// normal Lease and positively prove renewal after the final promotion,
	// rather than relying on an incidental test duration to exercise renewal.
	beforeRenewal := make([]time.Time, len(jobs))
	for i, job := range jobs {
		attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
		if err != nil || len(attempts) != 1 {
			t.Fatalf("cohort attempts before renewal: %+v %v", attempts, err)
		}
		beforeRenewal[i] = attempts[0].LeaseExpiresAt
	}
	for i, job := range jobs {
		retirementWait(t, "every generation renews after final promotion", func() bool {
			attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
			return err == nil && len(attempts) == 1 && attempts[0].State == contract.AttemptRunning && attempts[0].LeaseExpiresAt.After(beforeRenewal[i])
		})
	}
	for _, a := range agents {
		if _, err := a.outbox.spool.sweepDeadOneShotAttempts(t.Context(), 256, a.outbox.attemptIsLive, a.outbox.attemptCreatedHere); err != nil {
			t.Fatal(err)
		}
		for i := range agents {
			if !spoolRowExists(t, a.logSpool, fmt.Sprintf("empty-g%d", i)) {
				t.Fatal("live generation empty row lost")
			}
		}
	}
	// Retirees cannot advance the shared floor even through an explicit refresh.
	floorBefore, err := os.ReadFile(floorPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range agents[:2] {
		a.capabilities.revisionFloor.record(9999)
	}
	floorAfter, err := os.ReadFile(floorPath)
	if err != nil || !bytes.Equal(floorBefore, floorAfter) {
		t.Fatal("retiree persisted capability revision", err)
	}
	for _, a := range agents[:2] {
		_, err := a.session.client.Heartbeat(t.Context(), "node", heartbeatRequest(a.registration.BootSessionID, a.capabilities.snapshot()))
		if protocolErrorCode(err) != contract.ErrorNodeSessionReplaced {
			t.Fatalf("retiree Node publication: %v", err)
		}
	}
	for _, release := range releases {
		if err := os.WriteFile(release, nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, job := range jobs {
		retirementWait(t, "real cohort job finished", func() bool {
			j, err := store.GetJob(t.Context(), job.JobID)
			return err == nil && j.State == contract.JobFailed
		})
		attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
		if err != nil || len(attempts) != 1 || attempts[0].Result == nil || attempts[0].Result.ExitCode == nil || *attempts[0].Result.ExitCode != 17 {
			t.Fatalf("real exit changed: %+v %v", attempts, err)
		}
	}
	for _, finished := range done[:2] {
		select {
		case err := <-finished:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(testwait.Bound):
			t.Fatal("retiree did not close")
		}
	}
}

func TestGenerationCustodyLetsReapedRetireeExitAndPrimaryReplayHistoricalEvidence(t *testing.T) {
	clock := newManualClock(time.Now())
	var blocked atomic.Bool
	blocked.Store(true)
	source, store, done, _ := newRetirementFixtureWithStoreClock(t, clock, func(a *Agent, _ *l1.Store) { blockReviewLogs(a, &blocked) })
	job, ready, _ := retirementReviewJob(t, store, "custody-after-ended-lease", false, nil)
	retirementWait(t, "payload ready", func() bool { _, err := os.Stat(ready); return err == nil })
	promoteRetirementFixture(t, store, "node", "retirement-agent")
	if _, err := source.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CancelJob(t.Context(), job.JobID, l1.JobCancelCaller{Submitter: "review-submitter"}); err != nil {
		t.Fatal(err)
	}
	retirementWait(t, "canceled runtime joined", func() bool { return source.Status().OneShot.Occupied == 0 })
	attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("attempts: %+v %v", attempts, err)
	}
	attemptID := attempts[0].AttemptID
	retirementWait(t, "durable pending completion", func() bool {
		return source.outbox.spool.inspectCompletion(t.Context(), attemptID).State == "durable_completion"
	})
	clock.Advance(l1.CancelSettlementTimeout + time.Second)
	if _, err := store.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		t.Fatalf("no custodian accepted storage: %v", err)
	default:
	}
	// Hold local closure until the successor has made an empty initial scan.
	source.session.retirementMu.Lock()
	locked := true
	defer func() {
		if locked {
			source.session.retirementMu.Unlock()
		}
	}()
	targetClock := newManualClock(time.Now())
	target, err := New(Config{Clock: targetClock, Fabric: source.fabric, ControlPlaneAddress: "wefty://control-plane", NodeID: "node", BootSessionID: "next-boot", AgentGenerationID: "next-generation", OS: "darwin", Version: "test", Capabilities: map[string]bool{"kind:process": true, "upgrade:retirement:v1": true, contract.CapabilityExecCaptureBundle: true}, LogSpoolDirectory: source.logSpool.directory, HandoffRoot: source.handoffs.root, GuardianExecutable: agentBinaryPath, HeartbeatInterval: 50 * time.Millisecond, ClaimInterval: 10 * time.Millisecond, RenewalInterval: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	scanned := make(chan struct{})
	var scanOnce sync.Once
	target.outbox.recoveryScanFinished = func() { scanOnce.Do(func() { close(scanned) }) }
	target.outbox.startRecovery(t.Context(), target.session.client, func(err error) { t.Log(err) })
	select {
	case <-scanned:
	case <-time.After(testwait.Bound):
		t.Fatal("initial empty custody scan did not finish")
	}
	// Local scanning no longer waits for inventory. Establish that both the
	// local rescan and independent initial inventory refresh have armed their
	// next timer before advancing this fixture's clock after closure.
	retirementWait(t, "idle cohort scan and inventory timers armed", func() bool {
		targetClock.mu.Lock()
		defer targetClock.mu.Unlock()
		armed := 0
		for _, timer := range targetClock.timers {
			if timer.active && timer.deadline.Equal(targetClock.now.Add(5*time.Second)) {
				armed++
			}
		}
		return armed == 2
	})
	source.session.retirementMu.Unlock()
	locked = false
	reviewRetirementExit(t, source, done)
	var digest string
	if err := target.logSpool.db.QueryRow(`SELECT digest FROM generation_evidence_custody WHERE attempt_id=? AND custodian_generation='next-generation'`, attemptID).Scan(&digest); err != nil || len(digest) != 64 {
		t.Fatalf("source exited without durable exact custody: %q %v", digest, err)
	}
	receipts, err := target.EvidenceCustodyReceipts(t.Context(), "generation")
	if err != nil || len(receipts) != 1 || receipts[0].Attempt.AttemptID != attemptID || receipts[0].Digest != digest {
		t.Fatalf("exact custody receipt: %+v %v", receipts, err)
	}
	source.outbox.recoveryMu.Lock()
	source.outbox.recoveryCancel()
	source.outbox.recoveryMu.Unlock()
	source.outbox.recoveryWG.Wait()
	// No workload finishes and no manual recovery wake occurs. The cohort
	// timer alone must observe L1 closure and deliver historical evidence.
	targetClock.Advance(5 * time.Second)
	retirementWait(t, "idle successor delivered custody", func() bool {
		receipt := target.logSpool.inspectCompletion(t.Context(), attemptID)
		return receipt.EventCount == 0 && receipt.State == "delivered"
	})
	current, err := store.GetJob(t.Context(), job.JobID)
	if err != nil || current.State != contract.JobFailed {
		t.Fatalf("historical evidence changed verdict: %+v %v", current, err)
	}
}

func TestGenerationOwnershipPrecedesManagedRootInitialization(t *testing.T) {
	for _, generation := range []string{"", "generation"} {
		t.Run("generation="+generation, func(t *testing.T) {
			network := plain.NewNetwork()
			root, spool := temporaryManagedRoot(t), t.TempDir()
			config := Config{Fabric: network.NewFabric(fabric.Identity{NodeID: "agent"}), ControlPlaneAddress: "wefty://control-plane", NodeID: "node", Version: "test", BootSessionID: "first", AgentGenerationID: generation, LogSpoolDirectory: spool, ManagedRootDirectory: root}
			first, err := New(config)
			if err != nil {
				t.Fatal(err)
			}
			defer first.Close()
			config.BootSessionID = "second"
			if second, err := New(config); err == nil {
				second.Close()
				t.Fatal("duplicate agent started")
			}
			// A rejected start must not have changed managedroot's active boot fence.
			resource := first.managedResource.(*processManagedResource)
			if _, err := resource.root.CreateService("still-owned", l1.InitialServiceRemovalGeneration); err != nil {
				t.Fatalf("duplicate startup changed managed-root authority: %v", err)
			}
		})
	}
}

func TestGenerationCustodyVetoesUnaccountedOlderEvidence(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "node", 1024)
	defer spool.Close()
	spool.generationID = "old"
	claim := spoolTestClaim("older-backlog")
	if err := spool.ensureAttempt(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	if err := spool.append(t.Context(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 0, "pending")); err != nil {
		t.Fatal(err)
	}
	outbox := &evidenceOutbox{spool: spool}
	// No pending resident identity does not prove the entire generation's shared
	// evidence is delivered. This veto precedes any request to L1 or a custodian.
	settled, err := outbox.settleRetirementEvidence(nil, "node")(t.Context(), l1.NodeGeneration{AgentGenerationID: "old"}, nil)
	if err != nil || settled {
		t.Fatalf("unaccounted evidence allowed retirement: %v %v", settled, err)
	}
}

// Exercise the installed Startup -> Config -> New handover with real kernel
// locks, not a boolean claiming that some other process holds the lock.
func TestGenerationInstalledLifetimeIsBorrowedOnceBeforeManagedRoot(t *testing.T) {
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(home, "LaunchAgents"), 0700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(home, "candidate")
	if err := os.WriteFile(source, []byte("immutable executable"), 0700); err != nil {
		t.Fatal(err)
	}
	store := agentinstall.Store{Data: home, LaunchAgents: filepath.Join(home, "LaunchAgents")}
	const boot = "darwin:11111111-1111-1111-1111-111111111111"
	g, err := store.Initialize("node", source, "test", boot, []int{1})
	if err != nil {
		t.Fatal(err)
	}
	lifetime, err := store.StartupWithLifetime(g.ID, "node", g.Executable, boot)
	if err != nil {
		t.Fatal(err)
	}
	defer lifetime.Close()
	network := plain.NewNetwork()
	spool := t.TempDir()
	config := Config{Fabric: network.NewFabric(fabric.Identity{NodeID: "agent"}), ControlPlaneAddress: "wefty://control-plane", NodeID: "node", Version: "test", BootSessionID: "first", AgentGenerationID: g.ID, GenerationLifetimeLock: lifetime.ClaimRuntime, PrimaryGeneration: func(write func() error) error { return store.WithPrimary(g.ID, write) }, LogSpoolDirectory: spool, ManagedRootDirectory: temporaryManagedRoot(t)}
	first, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	// The installed directory is the sole generation lock. The standalone
	// fallback was not also acquired on another descriptor or at another path.
	fallback, err := acquireNodeLock(spool, "node:generation:"+g.ID)
	if err != nil {
		t.Fatalf("runtime took a redundant generation lock: %v", err)
	}
	fallback.Close()
	config.BootSessionID = "second"
	if second, err := New(config); err == nil {
		second.Close()
		t.Fatal("same lifetime ownership admitted two runtimes")
	}
	resource := first.managedResource.(*processManagedResource)
	if _, err := resource.root.CreateService("still-owned", l1.InitialServiceRemovalGeneration); err != nil {
		t.Fatalf("rejected borrowed startup changed managed-root authority: %v", err)
	}
	first.Close()
	if duplicate, err := store.StartupWithLifetime(g.ID, "node", g.Executable, boot); err == nil {
		duplicate.Close()
		t.Fatal("runtime close prematurely released installation lifetime lock")
	}
	lifetime.Close()
	restarted, err := store.StartupWithLifetime(g.ID, "node", g.Executable, boot)
	if err != nil {
		t.Fatal(err)
	}
	restarted.Close()
}

func generationReplayResponse(value any) (*http.Response, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(payload))}, nil
}

func TestGenerationRecoveryDrainsEligibleEvidenceWhileInventoryBlocked(t *testing.T) {
	clock := newManualClock(time.Now())
	outbox, err := newEvidenceOutbox(t.TempDir(), "node", 1<<20, clock, 1, time.Hour, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	// The durable closure cache was earned earlier. Inventory availability
	// must not revoke this knowledge or gate this generation's own evidence.
	if _, err := outbox.spool.db.Exec(`INSERT INTO closed_agent_generations VALUES('G0')`); err != nil {
		t.Fatal(err)
	}
	for _, generation := range []string{"G0", "G1", "unknown"} {
		outbox.spool.generationID = generation
		claim := spoolTestClaim("inventory-" + generation)
		if err := outbox.spool.ensureAttempt(t.Context(), claim); err != nil {
			t.Fatal(err)
		}
		for sequence, payload := range []string{"first", "second"} {
			if err := outbox.spool.append(t.Context(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, uint64(sequence), payload)); err != nil {
				t.Fatal(err)
			}
		}
		exitCode := 17
		if err := outbox.spool.storeCompletion(t.Context(), claim.Lease.AttemptID, l1.ProcessResult{ExitCode: &exitCode}, clock.Now()); err != nil {
			t.Fatal(err)
		}
	}
	outbox.spool.generationID = "G1"
	inventoryEntered, releaseInventory, inventoryReturned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce, enteredOnce, returnedOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseInventory) }) }
	defer release()
	var inventoryCalls, scans atomic.Int32
	var mu sync.Mutex
	logs := map[string][]contract.LogEvent{}
	completions := map[string]l1.CompletionRequest{}
	client, err := newClient(plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "agent"}), "wefty://control-plane", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/generations"):
			if deadline, ok := r.Context().Deadline(); !ok || time.Until(deadline) > DefaultOperationTimeout {
				t.Error("inventory refresh exceeded the normal operation bound")
			}
			inventoryCalls.Add(1)
			enteredOnce.Do(func() { close(inventoryEntered) })
			select {
			case <-releaseInventory:
			case <-r.Context().Done():
			}
			returnedOnce.Do(func() { close(inventoryReturned) })
			return nil, errors.New("inventory temporarily unavailable")
		case strings.HasSuffix(r.URL.Path, "/logs"):
			var request l1.AppendLogsRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				return nil, err
			}
			mu.Lock()
			for _, event := range request.Events {
				logs[event.AttemptID] = append(logs[event.AttemptID], event)
			}
			mu.Unlock()
			return generationReplayResponse(l1.AppendLogsResponse{Acknowledged: map[contract.LogStream]uint64{contract.LogStdout: eventEndSequence(request.Events[len(request.Events)-1])}})
		case strings.HasSuffix(r.URL.Path, "/complete"):
			var request l1.CompletionRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				return nil, err
			}
			mu.Lock()
			completions[r.URL.Path] = request
			mu.Unlock()
			return generationReplayResponse(l1.Job{})
		default:
			return nil, fmt.Errorf("unexpected recovery endpoint %s", r.URL.Path)
		}
	})
	outbox.recoveryScanFinished = func() { scans.Add(1) }
	outbox.startRecovery(t.Context(), client, func(err error) { t.Errorf("eligible recovery: %v", err) })
	select {
	case <-inventoryEntered:
	case <-time.After(testwait.Bound):
		t.Fatal("inventory refresh did not start")
	}
	// Keep inventory blocked until both ordered logs and completions drain.
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		mu.Lock()
		drained := len(completions) == 2 && len(logs["inventory-G0"]) == 2 && len(logs["inventory-G1"]) == 2
		mu.Unlock()
		if drained {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("own and cached-closed evidence did not drain while inventory was blocked")
		case <-time.After(time.Millisecond):
		}
	}
	for _, id := range []string{"inventory-G0", "inventory-G1"} {
		retirementWait(t, "eligible durable drain", func() bool {
			receipt := outbox.spool.inspectCompletion(t.Context(), id)
			return receipt.State == "delivered" && receipt.EventCount == 0
		})
		mu.Lock()
		events := logs[id]
		mu.Unlock()
		if events[0].Sequence != 0 || string(events[0].Bytes) != "first" || events[1].Sequence != 1 || string(events[1].Bytes) != "second" {
			t.Fatalf("ordered evidence changed: %+v", events)
		}
	}
	release()
	<-inventoryReturned
	// Evidence created after failure also drains without a fresh inventory
	// response or a refresh timer tick.
	afterFailure := spoolTestClaim("inventory-after-failure")
	if err := outbox.spool.ensureAttempt(t.Context(), afterFailure); err != nil {
		t.Fatal(err)
	}
	if err := outbox.spool.append(t.Context(), spoolTestEvent(afterFailure.Lease.AttemptID, contract.LogStdout, 0, "after failure")); err != nil {
		t.Fatal(err)
	}
	exitCode := 19
	if err := outbox.spool.storeCompletion(t.Context(), afterFailure.Lease.AttemptID, l1.ProcessResult{ExitCode: &exitCode}, clock.Now()); err != nil {
		t.Fatal(err)
	}
	outbox.scheduleRecovery()
	retirementWait(t, "own evidence after inventory failure", func() bool {
		receipt := outbox.spool.inspectCompletion(t.Context(), afterFailure.Lease.AttemptID)
		return receipt.State == "delivered" && receipt.EventCount == 0
	})
	// A failed refresh is also throttled. Worker completions and explicit
	// recovery wakes cannot start more inventory calls before the clock moves.
	for range 4 {
		before := scans.Load()
		outbox.scheduleRecovery()
		retirementWait(t, "recovery scan after inventory failure", func() bool { return scans.Load() > before })
	}
	if got := inventoryCalls.Load(); got != 1 {
		t.Fatalf("inventory calls without advancing refresh interval=%d, want 1", got)
	}
	pending, err := outbox.spool.db.Query(`SELECT attempt_id FROM spool_attempts WHERE owner_generation='unknown' AND result_json IS NOT NULL`)
	if err != nil {
		t.Fatal(err)
	}
	defer pending.Close()
	if !pending.Next() {
		t.Fatal("failed inventory refresh discarded an unknown generation's evidence")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(logs["inventory-unknown"]) != 0 {
		t.Fatal("unknown generation gained replay authority")
	}
}

func TestGenerationThreeRecoveryLoopsSerializeClosedBacklog(t *testing.T) {
	clock := newManualClock(time.Now())
	root := t.TempDir()
	claim := spoolTestClaim("closed-G0-backlog")
	claim.Job.Spec.Kind = contract.JobKindProcess
	claim.Job.Spec.Restartable = true
	exitCode := 17
	wantCompletion := l1.CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "completion:" + claim.Lease.AttemptID,
		Result:                    l1.ProcessResult{ExitCode: &exitCode, TerminationCause: contract.TerminationCauseAgent},
		RuntimeQuiescenceEvidence: l1.RuntimeQuiescenceAttempt, TerminationInitiator: contract.TerminationCauseAgent,
		InterruptionReason: l1.InterruptionUpgrade, InterruptionRequestKey: "upgrade-G0",
	}
	var wantLogs []contract.LogEvent
	for sequence, payload := range []string{"first", "second", "third", "fourth", "fifth", "sixth"} {
		wantLogs = append(wantLogs, spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, uint64(sequence), payload))
	}
	entered := make(chan string, 1)
	unexpectedDelivery := make(chan string, 8)
	rejected := make(chan string, 8)
	releaseTransport := make(chan struct{})
	var released atomic.Bool
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { released.Store(true); close(releaseTransport) }) }
	var entries atomic.Int32
	var mu sync.Mutex
	var gotLogs []contract.LogEvent
	var gotCompletions []l1.CompletionRequest
	var outboxes []*evidenceOutbox
	var clients []*Client
	defer func() {
		for _, outbox := range outboxes {
			outbox.Close()
		}
	}()
	defer release()
	for _, generation := range []string{"G0", "G1", "G2"} {
		outbox, err := newEvidenceOutbox(root, "node", 1<<20, clock, 1, time.Hour, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		outbox.spool.generationID = generation
		outboxes = append(outboxes, outbox)
		client, err := newClient(plain.NewNetwork().NewFabric(fabric.Identity{NodeID: generation}), "wefty://control-plane", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(client.Close)
		clients = append(clients, client)
		client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) {
			if strings.HasSuffix(r.URL.Path, "/generations") {
				closed := clock.Now()
				return generationReplayResponse(l1.NodeGenerationInventory{Generations: []l1.NodeGeneration{{AgentGenerationID: "G0", Role: l1.NodeGenerationClosed, ClosedAt: &closed}}})
			}
			if !strings.HasPrefix(r.URL.Path, attemptPath(claim.Job.JobID, claim.Lease.AttemptID)+"/") {
				return nil, fmt.Errorf("unexpected recovery endpoint %s", r.URL.Path)
			}
			if call := entries.Add(1); call == 1 {
				entered <- generation
			} else if !released.Load() {
				unexpectedDelivery <- generation
			}
			// The transport retains the first log request, not merely the
			// selection pass. All three recovery loops remain running.
			select {
			case <-releaseTransport:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
			switch {
			case strings.HasSuffix(r.URL.Path, "/logs"):
				var request l1.AppendLogsRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					return nil, err
				}
				if request.FencingToken != claim.Lease.FencingToken {
					return nil, errors.New("replay changed the Attempt fence")
				}
				mu.Lock()
				gotLogs = append(gotLogs, request.Events...)
				mu.Unlock()
				return generationReplayResponse(l1.AppendLogsResponse{AttemptState: contract.AttemptLost, Acknowledged: map[contract.LogStream]uint64{contract.LogStdout: eventEndSequence(request.Events[len(request.Events)-1])}})
			case strings.HasSuffix(r.URL.Path, "/complete"):
				var request l1.CompletionRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					return nil, err
				}
				mu.Lock()
				defer mu.Unlock()
				if len(gotLogs) != len(wantLogs) {
					return nil, errors.New("completion overtook ordered logs")
				}
				gotCompletions = append(gotCompletions, request)
				return generationReplayResponse(l1.Job{State: contract.JobFailed})
			default:
				return nil, fmt.Errorf("unexpected recovery endpoint %s", r.URL.Path)
			}
		})
	}
	if err := outboxes[0].spool.ensureAttempt(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	for _, event := range wantLogs {
		if err := outboxes[0].spool.append(t.Context(), event); err != nil {
			t.Fatal(err)
		}
	}
	if err := outboxes[0].spool.storeDurableCompletion(t.Context(), claim.Lease.AttemptID, durableCompletion{
		Result: wantCompletion.Result, RuntimeQuiescenceEvidence: l1.RuntimeQuiescenceAttempt,
		TerminationInitiator: contract.TerminationCauseAgent, InterruptionReason: l1.InterruptionUpgrade, InterruptionRequestKey: "upgrade-G0",
	}, clock.Now()); err != nil {
		t.Fatal(err)
	}
	// Earn durable positive closure through the normal inventory adapter. All
	// three handles must then be able to select G0, including its own loop.
	if err := outboxes[0].refreshClosedGenerations(t.Context(), clients[0], "node"); err != nil {
		t.Fatal(err)
	}
	start := func(i int) {
		outboxes[i].startRecovery(t.Context(), clients[i], func(err error) {
			if errors.Is(err, errSharedLeaseBusy) {
				rejected <- outboxes[i].spool.generationID
			} else {
				t.Errorf("generation %s recovery: %v", outboxes[i].spool.generationID, err)
			}
		})
	}
	start(0)
	select {
	case generation := <-entered:
		if generation != "G0" {
			t.Fatalf("first owner=%s", generation)
		}
	case <-time.After(testwait.Bound):
		t.Fatal("G0 did not enter delivery")
	}
	start(1)
	start(2)
	contenders := map[string]bool{}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for len(contenders) < 2 {
		select {
		case generation := <-rejected:
			contenders[generation] = true
		case generation := <-unexpectedDelivery:
			t.Fatalf("generation %s entered delivery while G0 held the Attempt lease", generation)
		case <-deadline.C:
			t.Fatal("both competing recovery loops did not attempt the closed backlog")
		}
	}
	if !contenders["G1"] || !contenders["G2"] || entries.Load() != 1 {
		t.Fatalf("competing delivery escaped exclusion: contenders=%v entries=%d", contenders, entries.Load())
	}
	before := outboxes[0].spool.inspectCompletion(t.Context(), claim.Lease.AttemptID)
	if before.EventCount != int64(len(wantLogs)) || before.State != "durable_completion" || !reflect.DeepEqual(before.Result, wantCompletion.Result) || before.QuiescenceEvidence != l1.RuntimeQuiescenceAttempt {
		t.Fatalf("blocked transport lost durable evidence: %+v", before)
	}
	release()
	retirementWait(t, "all closed-generation evidence drained", func() bool {
		receipt := outboxes[0].spool.inspectCompletion(t.Context(), claim.Lease.AttemptID)
		return receipt.State == "delivered" && receipt.EventCount == 0
	})
	mu.Lock()
	if !reflect.DeepEqual(gotLogs, wantLogs) || len(gotCompletions) != 1 || !reflect.DeepEqual(gotCompletions[0], wantCompletion) {
		t.Errorf("replay changed evidence: logs=%+v completions=%+v", gotLogs, gotCompletions)
	}
	mu.Unlock()
	for _, outbox := range outboxes {
		select {
		case <-outbox.recoveryContext.Done():
			t.Fatal("a recovery loop was canceled to avoid the delivery race")
		default:
		}
		pending, err := outbox.spool.pendingAttempts(t.Context())
		if err != nil || len(pending) != 0 {
			t.Fatalf("generation %s retained pending backlog: %+v %v", outbox.spool.generationID, pending, err)
		}
	}
}
