//go:build darwin || linux

package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// A node holds retained results in two roots and owns one budget over both.
// These prove what it gives up when it is over that budget, and -- as much of
// the rule -- what it never gives up however full it is.

// fakeHelperHandoffRoot is the OCI helper's root as the agent sees it: figures
// that cross the runtime seam, and a deletion the agent can only ask for.
//
// It drops an evicted volume from what it reports next, so the remeasuring the
// budget pass does between deletions sees a smaller root, and it refuses an
// owner key it holds no volume for -- which is what makes "the agent evicts by
// the owner key it derived, not by the name the helper reported" a thing this
// fixture can fail on.
type fakeHelperHandoffRoot struct {
	volumes   []workloadrunner.RetainedHandoffVolume
	detached  int
	exhausted bool
	readErr   error
	evictErr  error
	// attempted records every owner key the node asked about, whether or not
	// the ask succeeded. "the node never asked" is a different assertion from
	// "the node asked and the helper refused", and only the first is what
	// "never a candidate" means.
	attempted []string
	evicted   []string
	reads     int
}

func (fake *fakeHelperHandoffRoot) InventoryRetainedHandoffs(context.Context) (workloadrunner.RetainedHandoffReport, error) {
	fake.reads++
	if fake.readErr != nil {
		return workloadrunner.RetainedHandoffReport{}, fake.readErr
	}
	return workloadrunner.RetainedHandoffReport{
		Volumes:   append([]workloadrunner.RetainedHandoffVolume(nil), fake.volumes...),
		Exhausted: fake.exhausted, DetachedTrees: fake.detached,
	}, nil
}

func (fake *fakeHelperHandoffRoot) RetainedHandoffVolumeName(ownerKey string) (string, error) {
	return ocihelper.DeterministicHandoffVolumeDirectory(ownerKey)
}

func (fake *fakeHelperHandoffRoot) EvictRetainedHandoff(_ context.Context, ownerKey string) error {
	fake.attempted = append(fake.attempted, ownerKey)
	if fake.evictErr != nil {
		return fake.evictErr
	}
	name, err := ocihelper.DeterministicHandoffVolumeDirectory(ownerKey)
	if err != nil {
		return err
	}
	kept := make([]workloadrunner.RetainedHandoffVolume, 0, len(fake.volumes))
	removed := false
	for _, volume := range fake.volumes {
		if volume.Name == name {
			removed = true
			continue
		}
		kept = append(kept, volume)
	}
	if !removed {
		return errors.New("the helper holds no handoff volume for that owner key")
	}
	fake.volumes = kept
	fake.evicted = append(fake.evicted, ownerKey)
	return nil
}

// budget runs one accounting pass with the node's budget set to bytes, which
// is the only pass that enforces it.
func (h *retentionHarness) budget(bytes int64) RetainedResultsStatus {
	h.t.Helper()
	h.manager.nodeBytes = bytes
	var last RetainedResultsStatus
	h.manager.observeAccounting = func(pass RetainedResultsStatus) { last = pass }
	if err := h.manager.accountNode(h.t.Context()); err != nil {
		h.t.Fatal(err)
	}
	h.manager.observeAccounting = nil
	return last
}

func (h *retentionHarness) exists(runID string) bool {
	h.t.Helper()
	_, err := os.Stat(filepath.Join(h.root, runID))
	if err != nil && !os.IsNotExist(err) {
		h.t.Fatal(err)
	}
	return err == nil
}

// ociVolume is the helper's report of one retained volume belonging to a run of
// this node, named exactly as the helper names it.
func ociVolume(t *testing.T, ownerKey string, bytes, entries int64, terminal time.Time) workloadrunner.RetainedHandoffVolume {
	t.Helper()
	name, err := ocihelper.DeterministicHandoffVolumeDirectory(ownerKey)
	if err != nil {
		t.Fatal(err)
	}
	return workloadrunner.RetainedHandoffVolume{
		Name: name, TerminalAt: terminal, TerminalKnown: true,
		LogicalBytes: bytes, DedupedBytes: bytes, Entries: entries,
	}
}

// TestANodeOverBudgetGivesUpAPublishedResultBeforeAnUnpublishedOne is the
// eviction order, and the fixture is built so that any other order fails it:
// the unpublished run is the older of the two, so a node ordering by age alone
// would give up the one whose evidence reached no ledger.
func TestANodeOverBudgetGivesUpAPublishedResultBeforeAnUnpublishedOne(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retain("run_unpublished", true, false, map[string]int{"result.json": 16, "payload.bin": 2 << 20})
	harness.now = harness.now.Add(time.Minute)
	harness.retain("run_published", true, true, map[string]int{"result.json": 16, "payload.bin": 2 << 20})

	harness.budget(3 << 20)

	if harness.exists("run_published") {
		t.Fatal("the node kept the published results it should have given up first")
	}
	if !harness.exists("run_unpublished") {
		t.Fatal("the node gave up results whose evidence reached no ledger while a published run was still there to give up")
	}
	if !harness.logged("run_published's retained results were given up early") {
		t.Fatalf("the eviction was silent: %v", harness.logs)
	}
	if harness.logged(handoffUnpublishedEviction) {
		t.Fatalf("a published eviction was reported as an unpublished one: %v", harness.logs)
	}
}

// TestNothingIsGivenUpWhileTheNodeFitsItsBudget is the other half of the same
// rule, and the one that makes the tests above mean anything: the pass runs on
// every node, and on a node that fits it deletes nothing.
func TestNothingIsGivenUpWhileTheNodeFitsItsBudget(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retain("run_one", true, true, map[string]int{"result.json": 16})
	harness.retain("run_two", true, false, map[string]int{"result.json": 16})

	status := harness.budget(1 << 30)

	if !harness.exists("run_one") || !harness.exists("run_two") {
		t.Fatal("a node inside its budget gave results up anyway")
	}
	if nodeChargedBytes(status) > 1<<30 {
		t.Fatalf("the fixture is not inside the budget it claims: %d charged bytes", nodeChargedBytes(status))
	}
	if harness.logged("given up early") {
		t.Fatalf("a node inside its budget reported an eviction: %v", harness.logs)
	}
}

// TestAnOCIVolumeIsGivenUpThroughTheHelperWithTheOwnerKeyTheNodeDerived is the
// second root. The agent cannot delete that filesystem and cannot name it on
// the wire either: it asks by the owner key it derived from its own records,
// and the helper derives the directory back.
func TestAnOCIVolumeIsGivenUpThroughTheHelperWithTheOwnerKeyTheNodeDerived(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	if err := harness.manager.recordUpload("run_oci", "node-1", "attempt-1", attemptResult{document: []byte("{}")}); err != nil {
		t.Fatal(err)
	}
	helper := &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{
		ociVolume(t, "run_oci", 8<<20, 4, harness.now),
	}}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper

	harness.budget(1 << 20)

	if len(helper.evicted) != 1 || helper.evicted[0] != "run_oci" {
		t.Fatalf("the helper was asked to give up %v, want exactly the owner key run_oci", helper.evicted)
	}
	if len(helper.volumes) != 0 {
		t.Fatalf("the volume survived: %+v", helper.volumes)
	}
	if !harness.logged("the OCI handoff volume for run run_oci was given up early") {
		t.Fatalf("the eviction was silent: %v", harness.logs)
	}
	// And the node looked again rather than believing what it had subtracted.
	if helper.reads < 2 {
		t.Fatalf("the helper's root was read %d time(s); the node must remeasure after a deletion", helper.reads)
	}
}

// TestAResultAnAttemptClaimsAfterTheNodeIsMeasuredIsSkippedNotDeleted is the
// round-2 HIGH from #493's history, staged rather than raced: the attempt takes
// the path between the measurement and the eviction, which is exactly the
// window a pass that selected candidates and then deleted them would lose a
// run's directory in.
func TestAResultAnAttemptClaimsAfterTheNodeIsMeasuredIsSkippedNotDeleted(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	path := harness.retain("run_claimed", true, true, map[string]int{"result.json": 16, "payload.bin": 4 << 20})

	harness.manager.nodeBytes = 1 << 20
	claimed := false
	harness.manager.observeAccounting = func(RetainedResultsStatus) {
		if claimed {
			return
		}
		claimed = true
		lease, err := harness.manager.lock(t.Context(), handoffClaim("run_claimed", path, nil).Job.Spec)
		if err != nil {
			t.Error(err)
			return
		}
		t.Cleanup(lease.release)
	}
	if err := harness.manager.accountNode(t.Context()); err != nil {
		t.Fatal(err)
	}

	if !claimed {
		t.Fatal("the fixture never reached the window it exists to stage")
	}
	if !harness.exists("run_claimed") {
		t.Fatal("the node deleted results an attempt had claimed while it was measuring")
	}
	if _, err := os.Stat(filepath.Join(path, "payload.bin")); err != nil {
		t.Fatalf("the claimed run's files were trimmed: %v", err)
	}
	if !harness.logged("leave run run_claimed's retained results alone this pass: an attempt holds them") {
		t.Fatalf("the skip was silent: %v", harness.logs)
	}
}

// TestTheOldestUnpublishedResultIsGivenUpWhenNothingPublishedRemains is Derek's
// third ruling. Those files are the only copy of what a run did, so the node
// gives one up only when it has nothing published left, and says so by name
// under a fixed token a person can grep for.
func TestTheOldestUnpublishedResultIsGivenUpWhenNothingPublishedRemains(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retain("run_older", true, false, map[string]int{"result.json": 16, "payload.bin": 2 << 20})
	harness.now = harness.now.Add(time.Hour)
	harness.retain("run_newer", true, false, map[string]int{"result.json": 16, "payload.bin": 2 << 20})

	harness.budget(3 << 20)

	if harness.exists("run_older") {
		t.Fatal("the oldest unpublished result was kept while the node stayed over its budget")
	}
	if !harness.exists("run_newer") {
		t.Fatal("the node gave up more than it had to")
	}
	if !harness.logged(handoffUnpublishedEviction) {
		t.Fatalf("an unpublished eviction was not reported under its own token: %v", harness.logs)
	}
	if !harness.logged("gave up run run_older and the") || !harness.logged("whose evidence reached no ledger") {
		t.Fatalf("the loud line did not name the run: %v", harness.logs)
	}
}

// TestAPublishedResultAnAttemptHoldsDoesNotMakeAnUnpublishedOneTheCandidate is
// what "nothing published remains" has to mean. A published run that is merely
// busy this pass is still a published run the node holds, and falling through
// to the unpublished one would give up a run's only copy while a published one
// was a minute away from being available.
func TestAPublishedResultAnAttemptHoldsDoesNotMakeAnUnpublishedOneTheCandidate(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	published := harness.retain("run_published", true, true, map[string]int{"result.json": 16, "payload.bin": 2 << 20})
	harness.now = harness.now.Add(time.Minute)
	harness.retain("run_unpublished", true, false, map[string]int{"result.json": 16, "payload.bin": 2 << 20})

	lease, err := harness.manager.lock(t.Context(), handoffClaim("run_published", published, nil).Job.Spec)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.release()

	harness.budget(3 << 20)

	if !harness.exists("run_published") || !harness.exists("run_unpublished") {
		t.Fatalf("the node gave something up: published=%v unpublished=%v",
			harness.exists("run_published"), harness.exists("run_unpublished"))
	}
	if harness.logged(handoffUnpublishedEviction) {
		t.Fatalf("the node reached an unpublished result while it still held a published one: %v", harness.logs)
	}
	if !harness.logged("leave run run_published's retained results alone this pass: an attempt holds them") {
		t.Fatalf("the skip was silent: %v", harness.logs)
	}
}

// TestAHandoffVolumeNoRunOfThisNodeCanNameIsChargedAndNeverGivenUp is the limit
// the protocol imposes and the contract states: eviction names a volume by the
// owner key the agent derived, so a volume whose name it cannot derive is one
// it cannot ask to have removed. It is still counted, because the bytes are on
// the node either way, and the helper's own expiry is what reclaims it.
func TestAHandoffVolumeNoRunOfThisNodeCanNameIsChargedAndNeverGivenUp(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	helper := &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{
		{Name: "wefty-handoff-volume-deadbeefdeadbeefdeadbeefdeadbeef", TerminalKnown: true,
			LogicalBytes: 8 << 20, DedupedBytes: 8 << 20, Entries: 4, TerminalAt: harness.now},
	}}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper

	status := harness.budget(1 << 20)

	if len(helper.attempted) != 0 {
		t.Fatalf("the node asked the helper to give up a volume it cannot name: %v", helper.attempted)
	}
	if status.OCI == nil || status.OCI.Unattributable != 1 {
		t.Fatalf("the volume was not counted as one this node cannot name: %+v", status.OCI)
	}
	if status.OCI.ChargedBytes < 8<<20 {
		t.Fatalf("a volume the node cannot evict was not charged: %d", status.OCI.ChargedBytes)
	}
	if !harness.logged("has nothing it may give up") {
		t.Fatalf("a node that stayed over its budget with nothing to give up said nothing: %v", harness.logs)
	}
}

// TestALiveHandoffVolumeIsNeverACandidate is the OCI side of "a run an attempt
// holds is never given up". The fact is the helper's, because the helper is
// what knows an attempt registered ownership and superseded the prior receipt.
func TestALiveHandoffVolumeIsNeverACandidate(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	if err := harness.manager.recordUpload("run_live", "node-1", "attempt-1", attemptResult{document: []byte("{}")}); err != nil {
		t.Fatal(err)
	}
	live := ociVolume(t, "run_live", 8<<20, 4, harness.now)
	live.Live = true
	live.TerminalKnown = false
	helper := &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{live}}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper

	status := harness.budget(1 << 20)

	if len(helper.attempted) != 0 {
		t.Fatalf("the node asked the helper to give up a volume an attempt is still writing: %v", helper.attempted)
	}
	if status.OCI == nil || status.OCI.Live != 1 {
		t.Fatalf("the live volume was not reported as live: %+v", status.OCI)
	}
	if status.OCI.ChargedBytes < 8<<20 {
		t.Fatalf("a live volume's bytes were left out of the node figure: %d", status.OCI.ChargedBytes)
	}
}

// TestAVolumeWithNoHelperOwnedTerminalTimeIsGivenUpLast is why the order is not
// one comparison on a timestamp. The contract already refuses to expire a
// volume on a time the workload could have written; ordering every eviction by
// it would let one workload push an honest run's results off a full node
// instead.
func TestAVolumeWithNoHelperOwnedTerminalTimeIsGivenUpLast(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	for _, runID := range []string{"run_dated", "run_undated"} {
		if err := harness.manager.recordUpload(runID, "node-1", "attempt-1", attemptResult{document: []byte("{}")}); err != nil {
			t.Fatal(err)
		}
	}
	// The undated one claims to be far older, which is exactly the lie the
	// order must not act on.
	undated := ociVolume(t, "run_undated", 4<<20, 2, harness.now.Add(-30*24*time.Hour))
	undated.TerminalKnown = false
	helper := &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{
		undated, ociVolume(t, "run_dated", 4<<20, 2, harness.now),
	}}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper

	harness.budget(5 << 20)

	if len(helper.evicted) != 1 || helper.evicted[0] != "run_dated" {
		t.Fatalf("the node gave up %v, want the volume it can actually date", helper.evicted)
	}
}

// TestTheNodeRemeasuresAfterEveryDeletionRatherThanSubtracting is the one that
// makes hard links safe to act on. Two runs share one 8 MiB inode, charged to
// whichever the pass reached first; giving that run up recovers nothing while
// the other still holds a link. A pass that subtracted what it thought the run
// was worth would stop here believing the node empty.
func TestTheNodeRemeasuresAfterEveryDeletionRatherThanSubtracting(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	const payload = 8 << 20
	first := harness.retain("run_first", true, true, map[string]int{"result.json": 16, "big.bin": payload})
	harness.now = harness.now.Add(time.Minute)
	second := harness.retain("run_second", true, true, map[string]int{"result.json": 16})
	if err := os.Link(filepath.Join(first, "big.bin"), filepath.Join(second, "big.bin")); err != nil {
		t.Fatalf("link one file into two runs: %v", err)
	}

	status := harness.budget(1 << 20)

	if harness.exists("run_first") || harness.exists("run_second") {
		t.Fatalf("the node stopped while still over its budget: first=%v second=%v",
			harness.exists("run_first"), harness.exists("run_second"))
	}
	if nodeChargedBytes(status) > 1<<20 {
		t.Fatalf("the node reported %d charged bytes against a budget of %d after its pass",
			nodeChargedBytes(status), 1<<20)
	}
}

// TestTheBudgetIsNeverEnforcedOnAnAttemptsFinalization is where the pass is
// allowed to run. An attempt's completion collects (attempt_lifecycle), and
// eviction needs a measurement, so putting the budget there would put one
// workload's tree in front of another run finishing and of the node lock being
// released.
func TestTheBudgetIsNeverEnforcedOnAnAttemptsFinalization(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retain("run_big", true, true, map[string]int{"result.json": 16, "payload.bin": 4 << 20})
	if err := harness.manager.recordUpload("run_oci", "node-1", "attempt-1", attemptResult{document: []byte("{}")}); err != nil {
		t.Fatal(err)
	}
	helper := &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{
		ociVolume(t, "run_oci", 8<<20, 4, harness.now),
	}}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper
	harness.manager.nodeBytes = 1 << 20

	if err := harness.manager.collect(); err != nil {
		t.Fatal(err)
	}

	if !harness.exists("run_big") {
		t.Fatal("collection gave a run's results up; the budget belongs on the accounting pass")
	}
	if len(helper.evicted) != 0 || helper.reads != 0 {
		t.Fatalf("collection reached the helper's root: evicted=%v reads=%d", helper.evicted, helper.reads)
	}

	// And the pass that is allowed to does.
	if err := harness.manager.accountNode(t.Context()); err != nil {
		t.Fatal(err)
	}
	if harness.exists("run_big") && len(helper.evicted) == 0 {
		t.Fatalf("the accounting pass gave nothing up either: %v", harness.logs)
	}
}

// TestARunStillBeingWrittenIsChargedAndNeverGivenUp keeps the two halves apart.
// An admitted run has no terminal window yet, so it is not a candidate at any
// budget -- and its bytes are still the node's, so they are still counted.
func TestARunStillBeingWrittenIsChargedAndNeverGivenUp(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	spec := handoffClaim("run_inflight", filepath.Join(harness.root, "run_inflight"), nil).Job.Spec
	owner := prepareHandoffForTest(t, harness.manager, spec)
	defer owner.lease.release()
	if err := os.WriteFile(filepath.Join(harness.root, "run_inflight", "payload.bin"), make([]byte, 4<<20), 0o600); err != nil {
		t.Fatal(err)
	}

	status := harness.budget(1 << 20)

	if !harness.exists("run_inflight") {
		t.Fatal("the node gave up a directory a workload is still writing into")
	}
	if status.InFlight != 1 || status.ChargedBytes < 4<<20 {
		t.Fatalf("an in-flight run was not charged: %+v", status)
	}
	if !harness.logged("has nothing it may give up") {
		t.Fatalf("a node that could give nothing up did not say so: %v", harness.logs)
	}
}

// TestAQuarantinedRecordIsChargedAndNeverGivenUp holds the line S1 and S2 drew:
// quarantine means "unsafe to delete", never "excluded from accounting", or it
// becomes a way for a workload to hide storage from the budget.
func TestAQuarantinedRecordIsChargedAndNeverGivenUp(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retain("run_paused", true, true, map[string]int{"result.json": 16, "payload.bin": 4 << 20})
	record := harness.record("run_paused")
	record.Quarantine = handoffExpiryNameNotADirectory
	if err := harness.manager.writeRecord(record); err != nil {
		t.Fatal(err)
	}

	status := harness.budget(1 << 20)

	if !harness.exists("run_paused") {
		t.Fatal("the budget deleted a record the sweep had paused as unsafe to delete")
	}
	if status.QuarantinedRecords != 1 || status.ChargedBytes < 4<<20 {
		t.Fatalf("a quarantined run was not charged: %+v", status)
	}
}

// TestANodeWithNoRuntimeEvictionSaysSoRatherThanFailingQuietly is the shape a
// node in a half-configured state takes: it reports volumes it has no way to
// give up, and the budget says that out loud instead of looping.
func TestANodeWithNoRuntimeEvictionSaysSoRatherThanFailingQuietly(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	if err := harness.manager.recordUpload("run_oci", "node-1", "attempt-1", attemptResult{document: []byte("{}")}); err != nil {
		t.Fatal(err)
	}
	helper := &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{
		ociVolume(t, "run_oci", 8<<20, 4, harness.now),
	}}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = nil

	harness.budget(1 << 20)

	if !harness.logged("its runtime provides no eviction") {
		t.Fatalf("a node that cannot act on its own budget said nothing: %v", harness.logs)
	}
	if helper.reads != 1 {
		t.Fatalf("the node read the helper's root %d times while making no progress", helper.reads)
	}
}

// TestAHelperThatRefusesAnEvictionStopsThePassRatherThanSpinning is the same
// termination rule for the failure that can happen on a healthy node: the pass
// reports it and leaves the rest to the next one, an hour later, rather than
// asking again for something that just refused.
func TestAHelperThatRefusesAnEvictionStopsThePassRatherThanSpinning(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	if err := harness.manager.recordUpload("run_oci", "node-1", "attempt-1", attemptResult{document: []byte("{}")}); err != nil {
		t.Fatal(err)
	}
	helper := &fakeHelperHandoffRoot{
		volumes:  []workloadrunner.RetainedHandoffVolume{ociVolume(t, "run_oci", 8<<20, 4, harness.now)},
		evictErr: errors.New("the helper session is gone"),
	}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper

	harness.budget(1 << 20)

	if helper.reads != 1 {
		t.Fatalf("the node read the helper's root %d times after a refusal", helper.reads)
	}
	if !harness.logged("the helper session is gone") {
		t.Fatalf("the refusal was not reported: %v", harness.logs)
	}
}

// TestPendingDetachedFreesAreNotChargedAgainstTheBudget records the decision
// they posed: their removal is already authorized and the next pass over the
// helper's root finishes them, so counting them would make a node give live
// results up to make room for bytes that are already going away.
func TestPendingDetachedFreesAreNotChargedAgainstTheBudget(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	helper := &fakeHelperHandoffRoot{detached: 3}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper

	status := harness.budget(1 << 20)

	if status.OCI == nil || status.OCI.DetachedTrees != 3 {
		t.Fatalf("pending frees were not reported: %+v", status.OCI)
	}
	if status.OCI.ChargedBytes != 0 {
		t.Fatalf("pending frees were charged %d bytes against the budget", status.OCI.ChargedBytes)
	}
	if !harness.logged("which the budget treats as already reclaimed") {
		t.Fatalf("the node did not say what it does with pending frees: %v", harness.logs)
	}
}

// TestAVolumeIsChargedTheFloorUnderEveryEntry is the unit the node budget is
// in, asserted on the literal constant because it is what makes a tree of a
// million empty files a number a node can act on.
func TestAVolumeIsChargedTheFloorUnderEveryEntry(t *testing.T) {
	if charged := ociChargedBytes(64, 1000); charged != 1000*handoffChargedEntryFloor {
		t.Fatalf("a thousand tiny entries were charged %d bytes, want %d",
			charged, 1000*handoffChargedEntryFloor)
	}
	if charged := ociChargedBytes(64<<20, 2); charged != 64<<20 {
		t.Fatalf("two large entries were charged %d bytes, want their own size", charged)
	}
	if handoffChargedEntryFloor != 4<<10 {
		t.Fatalf("the per-entry floor is %d bytes; the contract states 4 KiB", handoffChargedEntryFloor)
	}
}

// TestTheNodeBudgetIsOneContractNumber is the ruling: not node configuration,
// not an operator override, one figure every node reports the same way.
func TestTheNodeBudgetIsOneContractNumber(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	if harness.manager.nodeBytes != 1<<30 {
		t.Fatalf("a fresh manager's node budget is %d bytes, want one GiB", harness.manager.nodeBytes)
	}
}

// TestTheEvictionOrderIsOneOrderOverBothRoots proves the budget is one figure
// rather than two: the process root is inside its own share and the helper's is
// not, and what the node gives up is decided across them.
func TestTheEvictionOrderIsOneOrderOverBothRoots(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retain("run_process", true, true, map[string]int{"result.json": 16, "payload.bin": 4 << 20})
	harness.now = harness.now.Add(time.Hour)
	if err := harness.manager.recordUpload("run_oci", "node-1", "attempt-1", attemptResult{document: []byte("{}")}); err != nil {
		t.Fatal(err)
	}
	helper := &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{
		ociVolume(t, "run_oci", 4<<20, 2, harness.now),
	}}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper

	// Room for one of the two. The process run is the older, so it goes.
	harness.budget(5 << 20)

	if harness.exists("run_process") {
		t.Fatal("the node kept the older of the two roots' results")
	}
	if len(helper.evicted) != 0 {
		t.Fatalf("the node gave up the newer volume as well: %v", helper.evicted)
	}
	if !strings.Contains(strings.Join(harness.logs, "\n"), "gave 1 retained result(s) up") {
		t.Fatalf("the pass did not report what it gave up: %v", harness.logs)
	}
}
