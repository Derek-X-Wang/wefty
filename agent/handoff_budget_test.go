//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
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
	volumes  []workloadrunner.RetainedHandoffVolume
	detached int
	// page, when positive, is how many volumes one read carries. The rest are
	// reachable only through the cursor, which is what makes "a published
	// volume this node has not been shown" a thing a test can build.
	page     int
	readErr  error
	evictErr error
	// live are the owner keys the helper refuses to give up because an attempt
	// owns them, which is the refusal the node has to tell apart from a
	// failure.
	live map[string]struct{}
	// beforeEvict parks a deletion where a real one spends its time: a helper
	// round trip and a tree free on the far side of the runtime seam.
	beforeEvict func(ownerKey string)
	// scan is the whole-root measurement this fixture's pages are cut from,
	// and generation is the root's shape. A page after the first is refused
	// with Restart when the two have diverged, exactly as the helper does.
	scan           []workloadrunner.RetainedHandoffVolume
	generation     uint64
	scanGeneration uint64
	scans          int
	restarts       int
	// onPage runs after each page is served, which is where a test makes the
	// root change underneath a listing.
	onPage func(fake *fakeHelperHandoffRoot, page int)
	// forgetScan drops the cached scan once, without moving the generation:
	// a helper that restarted mid-listing and has nothing to resume from, on a
	// root quiet enough that its counter reads the same either side.
	forgetScan bool
	// driftGeneration makes the reported generation differ from page to page
	// without the helper ever saying the root changed: a helper the node
	// cannot take at its word, which is what the node's own assertion exists
	// for.
	driftGeneration bool
	// attempted records every owner key the node asked about, whether or not
	// the ask succeeded. "the node never asked" is a different assertion from
	// "the node asked and the helper refused", and only the first is what
	// "never a candidate" means.
	attempted []string
	evicted   []string
	reads     int
}

func (fake *fakeHelperHandoffRoot) InventoryRetainedHandoffs(_ context.Context, after string) (workloadrunner.RetainedHandoffReport, error) {
	fake.reads++
	if fake.readErr != nil {
		return workloadrunner.RetainedHandoffReport{}, fake.readErr
	}
	if after == "" {
		// A first-page request is a fresh whole-root scan, and the scan is
		// what every page of this listing is served from.
		fake.scan = append([]workloadrunner.RetainedHandoffVolume(nil), fake.volumes...)
		sort.Slice(fake.scan, func(left, right int) bool { return fake.scan[left].Name < fake.scan[right].Name })
		fake.scanGeneration = fake.generation
		fake.scans++
	} else if fake.forgetScan || fake.scan == nil || fake.scanGeneration != fake.generation {
		fake.restarts++
		fake.scan, fake.forgetScan = nil, false
		// The generation it reports is the one now. On a helper that restarted
		// and simply has no cached scan, that can equal the generation the
		// reader's first page carried -- a quiet root whose counter never
		// moved -- so `Restart` is the only thing saying the listing cannot be
		// resumed.
		return workloadrunner.RetainedHandoffReport{Restart: true, Generation: fake.generation}, nil
	}
	if fake.onPage != nil {
		fake.onPage(fake, fake.reads)
	}
	generation := fake.scanGeneration
	if fake.driftGeneration {
		generation += uint64(fake.reads)
	}
	report := workloadrunner.RetainedHandoffReport{DetachedTrees: fake.detached, Generation: generation}
	for _, volume := range fake.scan {
		if volume.Name <= after {
			continue
		}
		if fake.page > 0 && len(report.Volumes) >= fake.page {
			report.Exhausted, report.Next = true, report.Volumes[len(report.Volumes)-1].Name
			return report, nil
		}
		report.Volumes = append(report.Volumes, volume)
	}
	return report, nil
}

// mutate is a volume appearing behind a listing's cursor -- a rerun preparing
// its handoff volume while the node is paging -- which is what the generation
// exists to make visible.
func (fake *fakeHelperHandoffRoot) mutate(volume workloadrunner.RetainedHandoffVolume) {
	fake.volumes = append(fake.volumes, volume)
	fake.generation++
}

func (fake *fakeHelperHandoffRoot) RetainedHandoffVolumeName(ownerKey string) (string, error) {
	return ocihelper.DeterministicHandoffVolumeDirectory(ownerKey)
}

func (fake *fakeHelperHandoffRoot) EvictRetainedHandoff(_ context.Context, ownerKey string) error {
	fake.attempted = append(fake.attempted, ownerKey)
	if fake.beforeEvict != nil {
		fake.beforeEvict(ownerKey)
	}
	if _, owned := fake.live[ownerKey]; owned {
		return fmt.Errorf("%w: handoff volume for %s", workloadrunner.ErrRetainedHandoffLive, ownerKey)
	}
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
	var first, last RetainedResultsStatus
	measured := false
	h.manager.observeAccounting = func(pass RetainedResultsStatus) {
		if !measured {
			first, measured = pass, true
		}
		last = pass
	}
	if err := h.manager.accountNode(h.t.Context()); err != nil {
		h.t.Fatal(err)
	}
	h.manager.observeAccounting = nil
	// measured is what the pass found before it acted; the returned status is
	// what the node holds afterwards. A test that wants the first needs it
	// explicitly, because a pass that evicted reports twice.
	h.measured = first
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
		// The helper computes this under the same per-entry rule the agent's
		// own root uses; the fixture stands in for a volume whose files are
		// all larger than the floor.
		ChargedBytes: bytes,
	}
}

// retainOCI is one finished OCI run as this node records it: admitted under
// the volume's lease before the runtime request, then completed.
func (h *retentionHarness) retainOCI(ownerKey, attemptID string, published bool) {
	h.t.Helper()
	spec := ociHandoffClaim(ownerKey, attemptID).Job.Spec
	lease := h.admitOCI(ownerKey, attemptID)
	lease.release()
	if err := h.manager.finishOCIHandoff(spec, "node-1", attemptID, true, published); err != nil {
		h.t.Fatal(err)
	}
}

// admitOCI takes the volume's lease and writes the admission, and hands the
// lease back so a test can hold it the way a running attempt does.
func (h *retentionHarness) admitOCI(ownerKey, attemptID string) *handoffLease {
	h.t.Helper()
	spec := ociHandoffClaim(ownerKey, attemptID).Job.Spec
	lease, err := h.manager.lockOCIHandoff(h.t.Context(), ownerKey)
	if err != nil {
		h.t.Fatal(err)
	}
	if err := h.manager.admitOCIHandoff(lease, spec, "node-1", attemptID); err != nil {
		lease.release()
		h.t.Fatal(err)
	}
	return lease
}

func ociHandoffClaim(ownerKey, attemptID string) l1.Claim {
	return l1.Claim{
		Job: l1.Job{Spec: contract.JobSpec{
			Kind: contract.JobKindOCI, Class: contract.JobClassOneShot,
			Labels: map[string]string{"run_id": ownerKey},
		}},
		Lease: l1.AttemptLease{AttemptID: attemptID},
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
			LogicalBytes: 8 << 20, DedupedBytes: 8 << 20, ChargedBytes: 8 << 20, Entries: 4, TerminalAt: harness.now},
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

// TestTheNodeChargesTheHelpersOwnPerEntryFigure is the one charging rule over
// both roots. The node takes the helper's charged total rather than deriving
// one, because no function of a volume total and an entry count reproduces a
// per-entry floor: the mixed tree below is ~600 MiB of data and ~1.2 GiB of
// node, and a node that derived it would have read the second figure as the
// first and evicted nothing.
func TestTheNodeChargesTheHelpersOwnPerEntryFigure(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retainOCI("run_mixed", "attempt-1", true)
	const (
		oneLargeFile = 600 << 20
		emptyFiles   = 153600
	)
	mixed := ociVolume(t, "run_mixed", oneLargeFile, emptyFiles+1, harness.now)
	// What the helper's per-entry walk produces: the large file's own bytes
	// plus a floor under every entry it does not fill.
	mixed.ChargedBytes = oneLargeFile + emptyFiles*handoffChargedEntryFloor
	helper := &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{mixed}}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper

	harness.budget(1 << 30)
	measured := harness.measured

	if measured.OCI == nil || measured.OCI.ChargedBytes != mixed.ChargedBytes {
		t.Fatalf("the node charged %+v, want the helper's own %d", measured.OCI, mixed.ChargedBytes)
	}
	if measured.OCI.ChargedBytes <= oneLargeFile {
		t.Fatalf("the node charged %d bytes for a tree of %d bytes and %d empty files; the entry floor is missing",
			measured.OCI.ChargedBytes, oneLargeFile, emptyFiles)
	}
	// And a node that holds one such tree is over a one-GiB budget, which is
	// the whole point of measuring it this way.
	if len(helper.evicted) != 1 || helper.evicted[0] != "run_mixed" {
		t.Fatalf("the node gave up %v while holding %d charged bytes against %d",
			helper.evicted, mixed.ChargedBytes, int64(1<<30))
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

// --- the review's findings, each with the interleaving it named ---

// TestAVolumeARerunClaimedMidPassIsNotGivenUp is finding 1's exact
// interleaving, staged rather than raced: the inventory reports the volume
// idle, a rerun admits it before the eviction reaches the helper, and the node
// must keep it. The agent's lease is the first of the two guards; the helper's
// refusal under its ownership lock is the second, and the next test is that
// one.
func TestAVolumeARerunClaimedMidPassIsNotGivenUp(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retainOCI("run_reused", "attempt-1", true)
	helper := &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{
		ociVolume(t, "run_reused", 8<<20, 4, harness.now),
	}}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper

	harness.manager.nodeBytes = 1 << 20
	claimed := false
	harness.manager.observeAccounting = func(RetainedResultsStatus) {
		if claimed {
			return
		}
		claimed = true
		// The rerun: it takes the volume's lease and writes its admission
		// exactly as preparation does, before the runtime request.
		lease := harness.admitOCI("run_reused", "attempt-2")
		t.Cleanup(lease.release)
	}
	if err := harness.manager.accountNode(t.Context()); err != nil {
		t.Fatal(err)
	}

	if !claimed {
		t.Fatal("the fixture never reached the window it exists to stage")
	}
	if len(helper.attempted) != 0 {
		t.Fatalf("the node asked the helper to give up a volume a rerun had claimed: %v", helper.attempted)
	}
	if !harness.logged("an attempt of this node holds it") {
		t.Fatalf("the skip was silent: %v", harness.logs)
	}
}

// TestTheHelpersRefusalOfALiveVolumeIsNotAFailure is the second guard. Even
// with the lease released -- an admission this node never saw, or one made
// through a path it does not control -- the helper refuses under the lock that
// publishes ownership, and the node reads that as the guard working rather
// than as a broken runtime.
func TestTheHelpersRefusalOfALiveVolumeIsNotAFailure(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retainOCI("run_helper_live", "attempt-1", true)
	harness.now = harness.now.Add(time.Hour)
	harness.retainOCI("run_next", "attempt-1", true)
	helper := &fakeHelperHandoffRoot{
		volumes: []workloadrunner.RetainedHandoffVolume{
			ociVolume(t, "run_helper_live", 4<<20, 2, harness.now),
			ociVolume(t, "run_next", 4<<20, 2, harness.now),
		},
		live: map[string]struct{}{"run_helper_live": {}},
	}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper

	harness.budget(5 << 20)

	if len(helper.attempted) == 0 {
		t.Fatal("the node never asked, so the helper's refusal was never exercised")
	}
	if slices.Contains(helper.evicted, "run_helper_live") {
		t.Fatalf("a volume the helper refused was given up anyway: %v", helper.evicted)
	}
	if !harness.logged("refused to give the handoff volume for run run_helper_live up because an attempt owns it") {
		t.Fatalf("the refusal was not reported as one: %v", harness.logs)
	}
	// It took the reselect arm, not the failure arm: the pass went on to the
	// next published volume rather than ending. A failure would have stopped
	// the pass with the node still over budget.
	if !slices.Contains(helper.evicted, "run_next") {
		t.Fatalf("the refusal ended the pass instead of choosing again: evicted=%v logs=%v", helper.evicted, harness.logs)
	}
}

// TestAnAdmittedVolumeIsChargedAndNeverACandidate is the other half of the
// admission record: a volume this node admitted and has not finished is the
// node's bytes, and it is nobody's candidate at any budget.
func TestAnAdmittedVolumeIsChargedAndNeverACandidate(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	lease := harness.admitOCI("run_inflight_oci", "attempt-1")
	defer lease.release()
	helper := &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{
		// The helper has not seen `Run` yet, so it reports the volume idle.
		ociVolume(t, "run_inflight_oci", 8<<20, 4, harness.now),
	}}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper

	harness.budget(1 << 20)
	measured := harness.measured

	if len(helper.attempted) != 0 {
		t.Fatalf("the node asked the helper to give up a volume it had just admitted: %v", helper.attempted)
	}
	if measured.OCI == nil || measured.OCI.AdmittedHere != 1 || measured.OCI.ChargedBytes < 8<<20 {
		t.Fatalf("an admitted volume was not charged or not counted: %+v", measured.OCI)
	}
}

// TestARerunDoesNotInheritTheEarlierAttemptsPublication is finding 4. The
// owner was published once; the rerun's contents are its own and no ledger has
// seen them, so they must not be first in line.
func TestARerunDoesNotInheritTheEarlierAttemptsPublication(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retainOCI("run_republished", "attempt-1", true)
	if err := harness.manager.recordUpload("run_republished", "node-1", "attempt-1",
		attemptResult{document: []byte("{}")}); err != nil {
		t.Fatal(err)
	}
	// attempt-1's result reached the ledger, on the record eviction reads.
	// That is the authority the rerun must not inherit.
	if err := harness.manager.noteOCIHandoffUpload("run_republished", "attempt-1", true); err != nil {
		t.Fatal(err)
	}
	if record, _, _ := harness.manager.readOCIRecord("run_republished"); !record.evidenceReachedLedger() {
		t.Fatal("the fixture never made the first attempt published, so it cannot show the reset")
	}
	// The rerun admits the same volume and finishes without publishing.
	harness.now = harness.now.Add(time.Hour)
	harness.retainOCI("run_republished", "attempt-2", false)
	// And an older, published result to give up instead.
	harness.retain("run_published_process", true, true, map[string]int{"result.json": 16, "payload.bin": 4 << 20})

	helper := &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{
		ociVolume(t, "run_republished", 4<<20, 2, harness.now),
	}}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper

	harness.budget(5 << 20)

	if len(helper.evicted) != 0 {
		t.Fatalf("the rerun's unpublished results were given up first: %v", helper.evicted)
	}
	if harness.exists("run_published_process") {
		t.Fatal("the node kept the published result and reached past it")
	}
}

// TestAnUploadRecordedForAnEarlierAttemptIsNotThisOnes is the same rule at the
// join: an upload outcome belongs to the attempt that produced it.
func TestAnUploadRecordedForAnEarlierAttemptIsNotThisOnes(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retainOCI("run_owner", "attempt-2", false)
	if err := harness.manager.noteOCIHandoffUpload("run_owner", "attempt-1", true); err != nil {
		t.Fatal(err)
	}
	record, found, err := harness.manager.readOCIRecord("run_owner")
	if err != nil || !found {
		t.Fatalf("the record went missing: %v", err)
	}
	if record.Uploaded {
		t.Fatal("an upload made by attempt-1 was recorded against attempt-2's contents")
	}
	if err := harness.manager.noteOCIHandoffUpload("run_owner", "attempt-2", true); err != nil {
		t.Fatal(err)
	}
	if record, _, _ := harness.manager.readOCIRecord("run_owner"); !record.Uploaded {
		t.Fatal("this attempt's own upload was not recorded")
	}
}

// TestAnAdmissionThatNeverFinishedIsResolvedAtStartup covers the restart in
// the reap-to-upload interval: the agent died holding an admission, and at
// startup nothing is executing, so the volume must stop being permanently
// unevictable -- with whatever publication its own attempt's upload record can
// still prove.
func TestAnAdmissionThatNeverFinishedIsResolvedAtStartup(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	lease := harness.admitOCI("run_crashed", "attempt-1")
	if err := harness.manager.recordUpload("run_crashed", "node-1", "attempt-1",
		attemptResult{document: []byte("{}")}); err != nil {
		t.Fatal(err)
	}
	lease.release() // the process went away; the lease did not outlive it

	if err := harness.manager.adoptResidue(); err != nil {
		t.Fatal(err)
	}

	record, found, err := harness.manager.readOCIRecord("run_crashed")
	if err != nil || !found {
		t.Fatalf("the admission was lost: %v", err)
	}
	if record.live() {
		t.Fatal("an admission nothing is executing kept the volume unevictable forever")
	}
	if !record.Adopted || !record.Uploaded {
		t.Fatalf("the reconciled record = %+v, want it marked derived and carrying its own attempt's upload", record)
	}
	// An upload another attempt made says nothing about these contents.
	other := newRetentionHarness(t, 7*24*time.Hour)
	otherLease := other.admitOCI("run_crashed", "attempt-2")
	otherLease.release()
	if err := other.manager.recordUpload("run_crashed", "node-1", "attempt-1",
		attemptResult{document: []byte("{}")}); err != nil {
		t.Fatal(err)
	}
	if err := other.manager.adoptResidue(); err != nil {
		t.Fatal(err)
	}
	if record, _, _ := other.manager.readOCIRecord("run_crashed"); record.Uploaded {
		t.Fatal("an upload attempt-1 made was carried onto attempt-2's contents")
	}
}

// TestAPublishedVolumeBeyondThePageIsNotInvisible is finding 2. The published
// volume is on the second page; the node must read to the end of the root
// before it can conclude that nothing published remains, and then give that
// volume up rather than the unpublished process run.
func TestAPublishedVolumeBeyondThePageIsNotInvisible(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retain("run_unpublished", true, false, map[string]int{"result.json": 16, "payload.bin": 2 << 20})
	for _, owner := range []string{"run_a", "run_b", "run_c"} {
		harness.retainOCI(owner, "attempt-1", owner == "run_c")
	}
	volumes := make([]workloadrunner.RetainedHandoffVolume, 0, 3)
	for _, owner := range []string{"run_a", "run_b", "run_c"} {
		volume := ociVolume(t, owner, 1<<20, 2, harness.now)
		volume.Live = true // a and b are not candidates; only the published c is
		if owner == "run_c" {
			volume.Live = false
		}
		volumes = append(volumes, volume)
	}
	helper := &fakeHelperHandoffRoot{volumes: volumes, page: 1}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper

	// Room for everything but one volume, so exactly one eviction is needed
	// and which one it is is the whole assertion.
	harness.budget(4<<20 + 512<<10)

	if helper.reads < 3 {
		t.Fatalf("the node read the helper's root %d time(s) with a page size of one; it stopped before the end", helper.reads)
	}
	if len(helper.evicted) != 1 || helper.evicted[0] != "run_c" {
		t.Fatalf("the node gave up %v, want the published volume it had to page to find", helper.evicted)
	}
	if !harness.exists("run_unpublished") {
		t.Fatal("the node gave up a result no ledger saw while a published one sat beyond the first page")
	}
}

// TestAFailedInventoryWithholdsUnpublishedEviction is the same rule for a root
// that could not be read at all. A root nobody read is not an empty root.
func TestAFailedInventoryWithholdsUnpublishedEviction(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retain("run_unpublished", true, false, map[string]int{"result.json": 16, "payload.bin": 4 << 20})
	helper := &fakeHelperHandoffRoot{readErr: errors.New("helper session is gone")}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper

	harness.budget(1 << 20)

	if !harness.exists("run_unpublished") {
		t.Fatal("the node gave up its only copy of a run while one of its two roots was unreadable")
	}
	if !harness.logged(handoffUnpublishedEvictionWithheld) {
		t.Fatalf("the node withheld the eviction without saying so under its token: %v", harness.logs)
	}
	if !harness.logged("could not be read at all") {
		t.Fatalf("the node did not say what it did not know: %v", harness.logs)
	}
}

// TestATruncatedMeasurementWithholdsUnpublishedEviction is the third way a
// pass can be ignorant: a published run whose tree it could not finish reads
// as holding nothing and is filtered out of the candidates entirely.
func TestATruncatedMeasurementWithholdsUnpublishedEviction(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retain("run_unpublished", true, false, map[string]int{"result.json": 16, "payload.bin": 4 << 20})
	harness.retainOCI("run_big", "attempt-1", true)
	truncated := ociVolume(t, "run_big", 0, 0, harness.now)
	truncated.Truncated = true
	helper := &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{truncated}}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper

	harness.budget(1 << 20)

	if !harness.exists("run_unpublished") {
		t.Fatal("the node gave up its only copy of a run while a published volume was measured incompletely")
	}
	if !harness.logged(handoffUnpublishedEvictionWithheld) {
		t.Fatalf("the node withheld the eviction without saying so under its token: %v", harness.logs)
	}
}

// TestANodeWithOneRootStillGivesUpUnpublishedResults is the other side of the
// gate: a node with no OCI runtime has one root, it read it whole, and
// withholding there would make a full node stop serving for a root it does not
// have.
func TestANodeWithOneRootStillGivesUpUnpublishedResults(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retain("run_older", true, false, map[string]int{"result.json": 16, "payload.bin": 2 << 20})
	harness.now = harness.now.Add(time.Hour)
	harness.retain("run_newer", true, false, map[string]int{"result.json": 16, "payload.bin": 2 << 20})

	harness.budget(3 << 20)

	if harness.exists("run_older") {
		t.Fatalf("a node whose only root it read whole withheld the eviction: %v", harness.logs)
	}
	if harness.logged(handoffUnpublishedEvictionWithheld) {
		t.Fatalf("the node withheld an eviction it knew enough to make: %v", harness.logs)
	}
}

// TestTwoVolumesWithNoTrustedTimeAreOrderedOnIdentity is finding 6. Neither
// timestamp is the node's to believe, so neither decides; the stable identity
// does, and a workload cannot move that.
func TestTwoVolumesWithNoTrustedTimeAreOrderedOnIdentity(t *testing.T) {
	older := handoffEvictionCandidate{ociVolume: true, name: "wefty-handoff-volume-bbbb", at: time.Unix(0, 0)}
	newer := handoffEvictionCandidate{ociVolume: true, name: "wefty-handoff-volume-aaaa", at: time.Unix(1<<40, 0)}
	if !lessEvictable(newer, older) {
		t.Fatal("two volumes the node cannot date were ordered by a timestamp a workload owns")
	}
	if lessEvictable(older, newer) {
		t.Fatal("the identity tiebreak is not a total order")
	}
	// And when both are trusted, the oldest still goes first.
	older.atKnown, newer.atKnown = true, true
	if !lessEvictable(older, newer) {
		t.Fatal("two volumes the node can date were not ordered oldest first")
	}
}

// TestARerunBetweenSelectionAndDeletionIsNotGivenUp is finding 5. The
// candidate finished again between the measurement and the lease: its results
// are fresh, it is no longer the oldest thing the node holds, and publication
// matching is not enough to tell -- a rerun of a published run is published.
func TestARerunBetweenSelectionAndDeletionIsNotGivenUp(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	path := harness.retain("run_rerun", true, true, map[string]int{"result.json": 16, "payload.bin": 4 << 20})
	harness.now = harness.now.Add(time.Hour)
	harness.retain("run_other", true, true, map[string]int{"result.json": 16, "payload.bin": 4 << 20})

	harness.manager.nodeBytes = 5 << 20
	reran := false
	// The seam is after the candidate is chosen and before its lease is taken,
	// which is the window a pass that selected and then deleted would lose a
	// rerun's fresh results in.
	handoffBudgetRace = func(stage string, candidate handoffEvictionCandidate) {
		if reran || stage != handoffBudgetCandidateChosen || candidate.runID != "run_rerun" {
			return
		}
		reran = true
		// The rerun: the same run retained again, with a fresh window and the
		// same verdict, so publication matching cannot tell the two apart.
		harness.now = harness.now.Add(2 * time.Hour)
		spec := handoffClaim("run_rerun", path, []string{contract.StableNodeTagPrefix + "node-1"}).Job.Spec
		ownership := prepareHandoffForTest(t, harness.manager, spec)
		if err := harness.manager.finish(ownership, spec, "node-1", true, true); err != nil {
			t.Error(err)
		}
		ownership.lease.release()
	}
	t.Cleanup(func() { handoffBudgetRace = nil })
	if err := harness.manager.accountNode(t.Context()); err != nil {
		t.Fatal(err)
	}

	if !reran {
		t.Fatal("the fixture never reached the window it exists to stage")
	}
	if !harness.exists("run_rerun") {
		t.Fatal("the node gave up results a rerun had just retained")
	}
	if !harness.logged("run run_rerun was retained again while this node was being measured") {
		t.Fatalf("the reselect was silent: %v", harness.logs)
	}
	// And the pass still did its job: it chose again and gave up the run that
	// really was oldest once the rerun had moved.
	if harness.exists("run_other") {
		t.Fatalf("the node chose again and then gave up nothing: %v", harness.logs)
	}
}

// TestAnEvictionInFlightDoesNotBlockAnUnrelatedFinalization is finding 3. The
// budget's deletion is held open -- a helper round trip and a tree free on the
// other side of the runtime seam is exactly that shape -- while an unrelated
// attempt finishes. A pass holding the collector lock across its deletion made
// every other attempt's finalization wait behind one workload's storage.
func TestAnEvictionInFlightDoesNotBlockAnUnrelatedFinalization(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retainOCI("run_evicting", "attempt-1", true)
	helper := &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{
		ociVolume(t, "run_evicting", 8<<20, 4, harness.now),
	}}
	entered, release := make(chan struct{}), make(chan struct{})
	helper.beforeEvict = func(string) {
		close(entered)
		<-release
	}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper
	harness.manager.nodeBytes = 1 << 20

	passed := make(chan error, 1)
	go func() { passed <- harness.manager.accountNode(t.Context()) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the budget pass never reached its deletion")
	}

	// An unrelated run finishes while that deletion is parked. This is the
	// whole path an attempt's completion takes on the process root: prepare,
	// finish, collect.
	finished := make(chan error, 1)
	go func() {
		spec := handoffClaim("run_finishing", filepath.Join(harness.root, "run_finishing"), nil).Job.Spec
		lease, err := harness.manager.lock(t.Context(), spec)
		if err != nil {
			finished <- err
			return
		}
		defer lease.release()
		owner, err := harness.manager.prepare(lease, spec, "node-1")
		if err != nil {
			finished <- err
			return
		}
		if err := harness.manager.finish(owner, spec, "node-1", true, true); err != nil {
			finished <- err
			return
		}
		finished <- harness.manager.collect()
	}()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatalf("an unrelated attempt's finalization failed while an eviction was in flight: %v", err)
		}
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("an unrelated attempt's finalization waited on an eviction that had not finished")
	}

	close(release)
	if err := <-passed; err != nil {
		t.Fatal(err)
	}
	if len(helper.evicted) != 1 {
		t.Fatalf("the parked eviction did not complete: %v", helper.evicted)
	}
}

// TestAnAdmissionThatLandsAfterTheCandidateIsChosenIsSeenUnderTheLease
// isolates the second of the agent's two guards. The candidate filter reads
// the records the measurement saw; the re-read under the lease is what catches
// an admission that landed after that. They are separate checks because they
// close separate windows, and this stages the one only the second can see.
func TestAnAdmissionThatLandsAfterTheCandidateIsChosenIsSeenUnderTheLease(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retainOCI("run_late_admit", "attempt-1", true)
	harness.retain("run_process", true, true, map[string]int{"result.json": 16, "payload.bin": 4 << 20})
	helper := &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{
		ociVolume(t, "run_late_admit", 4<<20, 2, harness.now),
	}}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper
	harness.manager.nodeBytes = 5 << 20

	landed := false
	handoffBudgetRace = func(stage string, candidate handoffEvictionCandidate) {
		if landed || stage != handoffBudgetCandidateChosen || candidate.ownerKey != "run_late_admit" {
			return
		}
		landed = true
		// The admission lands without taking the lease, which is what a record
		// a previous process left in flight looks like from here: the lease is
		// free, and only the record says an attempt owns the volume.
		spec := ociHandoffClaim("run_late_admit", "attempt-2").Job.Spec
		lease, err := harness.manager.lockOCIHandoff(t.Context(), "run_late_admit")
		if err != nil {
			t.Error(err)
			return
		}
		if err := harness.manager.admitOCIHandoff(lease, spec, "node-1", "attempt-2"); err != nil {
			t.Error(err)
		}
		lease.release()
	}
	t.Cleanup(func() { handoffBudgetRace = nil })
	if err := harness.manager.accountNode(t.Context()); err != nil {
		t.Fatal(err)
	}

	if !landed {
		t.Fatal("the fixture never reached the window it exists to stage")
	}
	if len(helper.attempted) != 0 {
		t.Fatalf("the node asked the helper to give up a volume admitted after the candidate was chosen: %v", helper.attempted)
	}
	if !harness.logged("was claimed by attempt attempt-2 while this node was being measured") {
		t.Fatalf("the re-read under the lease did not catch the admission: %v", harness.logs)
	}
	// And the pass still did its job with what was left.
	if harness.exists("run_process") {
		t.Fatalf("the node chose again and then gave up nothing: %v", harness.logs)
	}
}

// TestAVolumeCreatedBehindTheCursorRestartsTheListing is the pagination
// window. Independently measured pages let a volume appear behind the cursor
// and be shown by no call at all -- and an invisible published volume is an
// unpublished one destroyed, because the budget concludes that nothing
// published remains. The helper says the root changed, the node starts over,
// and the second listing sees it.
func TestAVolumeCreatedBehindTheCursorRestartsTheListing(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retain("run_unpublished", true, false, map[string]int{"result.json": 16, "payload.bin": 2 << 20})
	// Two volumes sort before the published one that appears mid-listing, so
	// it lands behind the cursor and only a restart can reach it.
	for _, owner := range []string{"run_aaa", "run_bbb"} {
		harness.retainOCI(owner, "attempt-1", false)
	}
	harness.retainOCI("run_late", "attempt-1", true)
	volumes := make([]workloadrunner.RetainedHandoffVolume, 0, 2)
	for _, owner := range []string{"run_aaa", "run_bbb"} {
		live := ociVolume(t, owner, 1<<20, 2, harness.now)
		live.Live = true // neither is a candidate; only the published latecomer is
		volumes = append(volumes, live)
	}
	late := ociVolume(t, "run_late", 1<<20, 2, harness.now)
	// Sort it last so it is genuinely behind the cursor when it appears.
	names := []string{volumes[0].Name, volumes[1].Name, late.Name}
	sort.Strings(names)
	if names[2] != late.Name {
		t.Skipf("the derived names do not put %s last; this fixture needs it behind the cursor", late.Name)
	}

	helper := &fakeHelperHandoffRoot{volumes: volumes, page: 1}
	helper.onPage = func(fake *fakeHelperHandoffRoot, page int) {
		if page == 1 {
			// The rerun prepares its volume while the node is on page one.
			fake.mutate(late)
		}
	}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper

	harness.budget(4<<20 + 512<<10)

	if helper.restarts == 0 {
		t.Fatalf("the listing never restarted, so the fixture did not stage the window: scans=%d reads=%d", helper.scans, helper.reads)
	}
	if helper.scans < 2 {
		t.Fatalf("the node took %d whole-root scans; a restart has to produce a fresh one", helper.scans)
	}
	if len(helper.evicted) != 1 || helper.evicted[0] != "run_late" {
		t.Fatalf("the node gave up %v, want the published volume that appeared behind the cursor", helper.evicted)
	}
	if !harness.exists("run_unpublished") {
		t.Fatal("the node gave up a result no ledger saw while a published volume was hidden behind the cursor")
	}
}

// TestARootThatKeepsChangingWithholdsTheUnpublishedEviction is the bound on
// that. A node whose helper root changes faster than it can be listed never
// gets a whole view, and a pass without one gives nothing up rather than the
// only copy of what some run did.
func TestARootThatKeepsChangingWithholdsTheUnpublishedEviction(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retain("run_unpublished", true, false, map[string]int{"result.json": 16, "payload.bin": 4 << 20})
	harness.retainOCI("run_one", "attempt-1", false)
	harness.retainOCI("run_two", "attempt-1", false)
	volumes := []workloadrunner.RetainedHandoffVolume{
		ociVolume(t, "run_one", 1<<20, 2, harness.now),
		ociVolume(t, "run_two", 1<<20, 2, harness.now),
	}
	helper := &fakeHelperHandoffRoot{volumes: volumes, page: 1}
	// Every page is followed by a change, so no listing ever finishes.
	helper.onPage = func(fake *fakeHelperHandoffRoot, _ int) { fake.generation++ }
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper

	harness.budget(1 << 20)

	if helper.restarts <= maxHandoffInventoryRestarts-1 {
		t.Fatalf("the node restarted %d time(s); the bound is %d", helper.restarts, maxHandoffInventoryRestarts)
	}
	if helper.restarts > maxHandoffInventoryRestarts+1 {
		t.Fatalf("the node restarted %d times; the bound did not hold", helper.restarts)
	}
	if !harness.exists("run_unpublished") {
		t.Fatal("the node gave up its only copy of a run without ever seeing its other root whole")
	}
	if !harness.logged(handoffUnpublishedEvictionWithheld) {
		t.Fatalf("the node withheld the eviction without saying so under its token: %v", harness.logs)
	}
}

// TestPagesOfOneListingMustComeFromOneScan is the assertion the node makes for
// itself. A helper that served two pages from two different measurements would
// be handing the node half of one root joined to half of another, which is the
// defect the restart exists to prevent, so the node treats it as one.
func TestPagesOfOneListingMustComeFromOneScan(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	// Something to give up, so the pass reaches the decision the assertion is
	// about rather than stopping for want of a candidate.
	harness.retain("run_unpublished", true, false, map[string]int{"result.json": 16, "payload.bin": 4 << 20})
	harness.retainOCI("run_one", "attempt-1", false)
	harness.retainOCI("run_two", "attempt-1", false)
	helper := &fakeHelperHandoffRoot{page: 1, volumes: []workloadrunner.RetainedHandoffVolume{
		ociVolume(t, "run_one", 1<<20, 2, harness.now),
		ociVolume(t, "run_two", 1<<20, 2, harness.now),
	}}
	// The generation on the wire drifts without the helper ever saying the
	// root changed, which is a helper the node cannot take at its word.
	helper.driftGeneration = true
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper

	harness.budget(1 << 20)

	if !harness.logged("came from different scans") {
		t.Fatalf("the node stitched pages from different scans together: %v", harness.logs)
	}
	if !harness.logged(handoffUnpublishedEvictionWithheld) {
		t.Fatalf("the node acted on a view it could not vouch for: %v", harness.logs)
	}
	if !harness.exists("run_unpublished") {
		t.Fatal("the node gave up its only copy of a run on a view it could not vouch for")
	}
}

// TestARestartWithAnUnchangedGenerationIsStillARestart isolates the flag from
// the generation. A helper that restarted mid-listing has no scan to resume
// from, and on a quiet root its counter reads the same either side -- so
// `restart` is the only thing that can say the listing cannot be continued.
func TestARestartWithAnUnchangedGenerationIsStillARestart(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retain("run_unpublished", true, false, map[string]int{"result.json": 16, "payload.bin": 4 << 20})
	harness.retainOCI("run_one", "attempt-1", true)
	harness.retainOCI("run_two", "attempt-1", true)
	helper := &fakeHelperHandoffRoot{page: 1, volumes: []workloadrunner.RetainedHandoffVolume{
		ociVolume(t, "run_one", 1<<20, 2, harness.now),
		ociVolume(t, "run_two", 1<<20, 2, harness.now),
	}}
	forgotten := false
	helper.onPage = func(fake *fakeHelperHandoffRoot, _ int) {
		if !forgotten {
			forgotten, fake.forgetScan = true, true
		}
	}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper

	// Room for the process run once both published volumes are given up, so
	// the pass ends without ever having to reach an unpublished result.
	harness.budget(4<<20 + 512<<10)

	if helper.restarts == 0 {
		t.Fatal("the fixture never made the helper forget its scan")
	}
	if helper.scanGeneration != helper.generation {
		t.Fatalf("the fixture moved the generation (%d vs %d); then the flag is not what is under test",
			helper.scanGeneration, helper.generation)
	}
	if !harness.logged("root changed while this node was listing it") {
		t.Fatalf("the node continued a listing the helper could not resume: %v", harness.logs)
	}
	// And it finished properly afterwards: the published volumes were given up
	// and the unpublished process run kept.
	if len(helper.evicted) == 0 {
		t.Fatalf("the node never completed a listing after the restart: %v", harness.logs)
	}
	if !harness.exists("run_unpublished") {
		t.Fatal("the node gave up a result no ledger saw while published volumes remained")
	}
}

// --- the independent review's findings ---

// TestAPublishedResultAnAttemptTakesMidPassYieldsToTheNextPublishedOne is the
// per-class advance. A candidate the pass cannot take is this class being
// busy, not a reason to leave a full node full -- but it is never a reason to
// reach into the other class either, which the surviving unpublished run here
// proves.
func TestAPublishedResultAnAttemptTakesMidPassYieldsToTheNextPublishedOne(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	oldest := harness.retain("run_oldest_published", true, true, map[string]int{"result.json": 16, "payload.bin": 2 << 20})
	harness.now = harness.now.Add(time.Hour)
	harness.retain("run_next_published", true, true, map[string]int{"result.json": 16, "payload.bin": 2 << 20})
	harness.now = harness.now.Add(time.Hour)
	harness.retain("run_unpublished", true, false, map[string]int{"result.json": 16, "payload.bin": 2 << 20})

	harness.manager.nodeBytes = 5 << 20
	claimed := false
	handoffBudgetRace = func(stage string, candidate handoffEvictionCandidate) {
		if claimed || stage != handoffBudgetCandidateChosen || candidate.runID != "run_oldest_published" {
			return
		}
		claimed = true
		// A rerun takes the oldest published run's directory between the
		// choice and the lease.
		lease, err := harness.manager.lock(t.Context(), handoffClaim("run_oldest_published", oldest, nil).Job.Spec)
		if err != nil {
			t.Error(err)
			return
		}
		t.Cleanup(lease.release)
	}
	t.Cleanup(func() { handoffBudgetRace = nil })
	if err := harness.manager.accountNode(t.Context()); err != nil {
		t.Fatal(err)
	}

	if !claimed {
		t.Fatal("the fixture never reached the window it exists to stage")
	}
	if !harness.exists("run_oldest_published") {
		t.Fatal("the node gave up results an attempt was holding")
	}
	if harness.exists("run_next_published") {
		t.Fatalf("the node stopped at the first candidate it could not take: %v", harness.logs)
	}
	if !harness.exists("run_unpublished") {
		t.Fatal("the node reached past a published class that still had members")
	}
	if harness.logged(handoffUnpublishedEviction) {
		t.Fatalf("a busy published run let the node reach an unpublished one: %v", harness.logs)
	}
}

// TestAClassWhoseEveryCandidateIsHeldEndsThePassWithoutReachingTheOther is the
// other half: exhausting a class is where the pass stops, and it stops rather
// than falling through.
func TestAClassWhoseEveryCandidateIsHeldEndsThePassWithoutReachingTheOther(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	held := make([]string, 0, 2)
	for _, runID := range []string{"run_pub_one", "run_pub_two"} {
		held = append(held, harness.retain(runID, true, true, map[string]int{"result.json": 16, "payload.bin": 2 << 20}))
		harness.now = harness.now.Add(time.Minute)
	}
	harness.retain("run_unpublished", true, false, map[string]int{"result.json": 16, "payload.bin": 2 << 20})
	for index, path := range held {
		lease, err := harness.manager.lock(t.Context(), handoffClaim([]string{"run_pub_one", "run_pub_two"}[index], path, nil).Job.Spec)
		if err != nil {
			t.Fatal(err)
		}
		defer lease.release()
	}

	harness.budget(1 << 20)

	if !harness.exists("run_unpublished") {
		t.Fatal("the node reached an unpublished result while published ones were merely busy")
	}
	if !harness.exists("run_pub_one") || !harness.exists("run_pub_two") {
		t.Fatal("the node gave up results attempts were holding")
	}
	// Both were tried, not just the first.
	for _, runID := range []string{"run_pub_one", "run_pub_two"} {
		if !harness.logged("leave run " + runID + "'s retained results alone this pass") {
			t.Fatalf("%s was never tried: %v", runID, harness.logs)
		}
	}
}

// TestGivingUpOneRootDoesNotRemeasureTheOther is the cost of a deletion. The
// two roots are separate filesystems that deduplicate independently, so no
// deletion on one can move the other's figures -- and re-reading the helper's
// root after a process deletion is a protocol round trip nothing depends on.
func TestGivingUpOneRootDoesNotRemeasureTheOther(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retain("run_process_one", true, true, map[string]int{"result.json": 16, "payload.bin": 2 << 20})
	harness.now = harness.now.Add(time.Minute)
	harness.retain("run_process_two", true, true, map[string]int{"result.json": 16, "payload.bin": 2 << 20})
	harness.retainOCI("run_oci", "attempt-1", false)
	helper := &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{
		ociVolume(t, "run_oci", 1<<20, 2, harness.now),
	}}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper

	// Room for one process run and the volume, so two process deletions happen
	// and neither may re-read the helper's root.
	harness.budget(2 << 20)

	if harness.exists("run_process_one") || harness.exists("run_process_two") {
		t.Fatalf("the node did not give both process runs up: %v", harness.logs)
	}
	if len(helper.evicted) != 0 {
		t.Fatalf("the node gave up an OCI volume it did not have to: %v", helper.evicted)
	}
	// One read for the measurement, one for the final report, and none for
	// either deletion.
	if helper.reads > 2 {
		t.Fatalf("the node read the helper's root %d times across two process deletions", helper.reads)
	}
}

// TestAReplacedSubtreeWithholdsUnpublishedEviction is the third way a pass can
// be ignorant of its own root. A published run whose tree stopped being the
// one the pass was measuring is left out of that run's figures entirely, so it
// reports zero charged bytes and is filtered out of the candidates -- and the
// node would conclude that nothing published remains.
func TestAReplacedSubtreeWithholdsUnpublishedEviction(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retain("run_unpublished", true, false, map[string]int{"result.json": 16, "payload.bin": 4 << 20})
	status := RetainedResultsStatus{Replaced: 1}
	if reason := harness.manager.incompleteKnowledge(status); reason == "" {
		t.Fatal("a pass that lost a subtree claimed it knew its own root whole")
	} else if !strings.Contains(reason, "stopped being the directory") {
		t.Fatalf("the reason does not name what was lost: %q", reason)
	}
	if reason := harness.manager.incompleteKnowledge(RetainedResultsStatus{}); reason != "" {
		t.Fatalf("a whole pass on a one-root node reported %q", reason)
	}
}

// TestAHandoffDirectoryThatIsNotAnAbsolutePathTakesNoLease is the namespacing.
// The lease is taken from what the dispatcher sent, before anything has proved
// it is a path this node manages -- so a spec naming an OCI volume's lease key
// would have held that volume's lease for the length of an attempt, and an
// attempt that holds a lease is a thing the budget refuses to give up.
func TestAHandoffDirectoryThatIsNotAnAbsolutePathTakesNoLease(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	for _, directory := range []string{"", "relative/handoff", ociHandoffLeaseKey("run_victim")} {
		spec := handoffClaim("run_forging", directory, nil).Job.Spec
		lease, err := harness.manager.lock(t.Context(), spec)
		if err == nil {
			lease.release()
			t.Fatalf("a handoff directory of %q took a lease", directory)
		}
		if !errors.Is(err, errUnmanagedHandoffDirectory) {
			t.Fatalf("the refusal of %q is not the typed one: %v", directory, err)
		}
	}
	// And the two namespaces cannot spell each other even when they could
	// collide: a process directory whose cleaned path is the OCI key's text
	// is refused above, and the keys themselves differ by construction.
	if handoffPathLeaseKey("/tmp/x") == ociHandoffLeaseKey("/tmp/x") {
		t.Fatal("the two lease namespaces produce the same key")
	}
	// The OCI volume's lease is free, which is what the refusals protect.
	lease, err := harness.manager.lockOCIHandoff(t.Context(), "run_victim")
	if err != nil {
		t.Fatalf("the victim volume's lease was held by the refused specs: %v", err)
	}
	lease.release()
}

// TestTheStartupPassMeasuresAndGivesNothingUp is where the budget's first
// irreversible decision may happen. Not inside the call bringing the node up,
// before it has claimed any work; the first enforcing pass is the first timer
// tick.
func TestTheStartupPassMeasuresAndGivesNothingUp(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retain("run_published", true, true, map[string]int{"result.json": 16, "payload.bin": 4 << 20})
	harness.retainOCI("run_oci", "attempt-1", true)
	helper := &fakeHelperHandoffRoot{volumes: []workloadrunner.RetainedHandoffVolume{
		ociVolume(t, "run_oci", 8<<20, 4, harness.now),
	}}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper
	harness.manager.nodeBytes = 1 << 20

	if err := harness.manager.measureNodeOnly(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !harness.exists("run_published") || len(helper.evicted) != 0 {
		t.Fatalf("the startup pass gave results up: process=%v oci=%v",
			harness.exists("run_published"), helper.evicted)
	}
	if !harness.logged("retained results on this node:") {
		t.Fatalf("the startup pass did not report what it found: %v", harness.logs)
	}
	if harness.logged("given up early") {
		t.Fatalf("the startup pass enforced the budget: %v", harness.logs)
	}

	// And the pass that does act, does.
	if err := harness.manager.accountNode(t.Context()); err != nil {
		t.Fatal(err)
	}
	if harness.exists("run_published") && len(helper.evicted) == 0 {
		t.Fatalf("the enforcing pass gave nothing up either: %v", harness.logs)
	}
}

// TestARerunThatFinishesPublishedStopsTheWalkOfTheUnpublishedClass is the
// defect the per-class advance introduced. Two unpublished candidates, A and
// B, are sorted and walked in order; A reruns and finishes *published* while
// the pass is looking at it. A has left the unpublished class and joined the
// one that must be given up first, so the sorted list the walk is reading is
// no longer the right list -- and walking on from there gives up B's only copy
// while a published result exists beside it.
func TestARerunThatFinishesPublishedStopsTheWalkOfTheUnpublishedClass(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	first := harness.retain("run_a_unpublished", true, false, map[string]int{"result.json": 16, "payload.bin": 2 << 20})
	harness.now = harness.now.Add(time.Hour)
	harness.retain("run_b_unpublished", true, false, map[string]int{"result.json": 16, "payload.bin": 2 << 20})

	harness.manager.nodeBytes = 3 << 20
	republished := false
	handoffBudgetRace = func(stage string, candidate handoffEvictionCandidate) {
		if republished || stage != handoffBudgetCandidateChosen || candidate.runID != "run_a_unpublished" {
			return
		}
		republished = true
		// The rerun: A runs again, finishes published, and releases its lease
		// before the budget reaches its own. Nothing about A is held any more;
		// what changed is which class it belongs to.
		harness.now = harness.now.Add(time.Hour)
		spec := handoffClaim("run_a_unpublished", first, []string{contract.StableNodeTagPrefix + "node-1"}).Job.Spec
		ownership := prepareHandoffForTest(t, harness.manager, spec)
		if err := harness.manager.finish(ownership, spec, "node-1", true, true); err != nil {
			t.Error(err)
		}
		ownership.lease.release()
	}
	t.Cleanup(func() { handoffBudgetRace = nil })
	if err := harness.manager.accountNode(t.Context()); err != nil {
		t.Fatal(err)
	}

	if !republished {
		t.Fatal("the fixture never reached the window it exists to stage")
	}
	if !harness.exists("run_b_unpublished") {
		t.Fatalf("the node gave up an unpublished run's only copy while a published result existed: %v", harness.logs)
	}
	// A is what the node should have given up once it looked again: it is now
	// the only published result on an over-budget node.
	if harness.exists("run_a_unpublished") {
		t.Fatalf("the node chose again and then gave nothing up: %v", harness.logs)
	}
	if !harness.logged("stopped part-way through") {
		t.Fatalf("the walk did not stop when its order went stale: %v", harness.logs)
	}
	if harness.logged(handoffUnpublishedEviction) {
		t.Fatalf("a result no ledger saw was given up while a published one existed: %v", harness.logs)
	}
}

// TestAnUnavailableCandidateStillAdvancesWithinItsClass is the other half, and
// the reason the two outcomes are separate. A candidate an attempt is holding
// has left the candidate set without joining the other class, so the rest of
// this class is still in the right order and the walk goes on.
func TestAnUnavailableCandidateStillAdvancesWithinItsClass(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	held := harness.retain("run_a_unpublished", true, false, map[string]int{"result.json": 16, "payload.bin": 2 << 20})
	harness.now = harness.now.Add(time.Hour)
	harness.retain("run_b_unpublished", true, false, map[string]int{"result.json": 16, "payload.bin": 2 << 20})

	lease, err := harness.manager.lock(t.Context(), handoffClaim("run_a_unpublished", held, nil).Job.Spec)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.release()

	harness.budget(3 << 20)

	if !harness.exists("run_a_unpublished") {
		t.Fatal("the node gave up results an attempt was holding")
	}
	if harness.exists("run_b_unpublished") {
		t.Fatalf("a held candidate stopped the walk instead of advancing it: %v", harness.logs)
	}
	if harness.logged("stopped part-way through") {
		t.Fatalf("a busy candidate was treated as a stale order: %v", harness.logs)
	}
}

// TestTheHelpersLiveRefusalAdvancesRatherThanRestartingThePass keeps the
// helper's guard on the advancing side too: a volume an attempt owns is out of
// the candidate set, and nothing about the order moved.
func TestTheHelpersLiveRefusalAdvancesRatherThanRestartingThePass(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)
	harness.retainOCI("run_live_one", "attempt-1", false)
	harness.now = harness.now.Add(time.Hour)
	harness.retainOCI("run_next_two", "attempt-1", false)
	helper := &fakeHelperHandoffRoot{
		volumes: []workloadrunner.RetainedHandoffVolume{
			ociVolume(t, "run_live_one", 4<<20, 2, harness.now),
			ociVolume(t, "run_next_two", 4<<20, 2, harness.now),
		},
		live: map[string]struct{}{"run_live_one": {}},
	}
	harness.manager.ociHandoffs = helper
	harness.manager.ociEvictor = helper

	harness.budget(5 << 20)

	if !slices.Contains(helper.evicted, "run_next_two") {
		t.Fatalf("the helper's refusal ended the walk instead of advancing it: %v", harness.logs)
	}
	if harness.logged("stopped part-way through") {
		t.Fatalf("a live refusal was treated as a stale order: %v", harness.logs)
	}
}

// TestAdoptionAndReconciliationTakeTheSameLeaseAnAttemptDoes closes the
// namespacing: both startup paths write to a run's record, and both must be
// excluded by an attempt holding that run -- which they are only if they ask
// for the same key an attempt takes.
func TestAdoptionAndReconciliationTakeTheSameLeaseAnAttemptDoes(t *testing.T) {
	harness := newRetentionHarness(t, 7*24*time.Hour)

	// An admission that never finished, whose run an attempt now holds.
	inflight := filepath.Join(harness.root, "run_inflight")
	spec := handoffClaim("run_inflight", inflight, nil).Job.Spec
	owner := prepareHandoffForTest(t, harness.manager, spec)
	defer owner.lease.release()

	// A directory with a marker and no record, whose run an attempt also
	// holds: adoption must leave it alone.
	adoptable := filepath.Join(harness.root, "run_adoptable")
	if err := os.MkdirAll(adoptable, 0o700); err != nil {
		t.Fatal(err)
	}
	marker, err := json.Marshal(handoffMarker{RunID: "run_adoptable", NodeID: "node-1", RetainUntil: harness.now.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(adoptable, handoffMarkerName), marker, 0o600); err != nil {
		t.Fatal(err)
	}
	adoptLease, err := harness.manager.lock(t.Context(),
		handoffClaim("run_adoptable", adoptable, nil).Job.Spec)
	if err != nil {
		t.Fatal(err)
	}
	defer adoptLease.release()

	if err := harness.manager.adoptResidue(); err != nil {
		t.Fatal(err)
	}

	// The in-flight admission keeps its admission: an attempt holds it, so it
	// is not a prior boot's after all.
	if record := harness.record("run_inflight"); !record.RetainUntil.IsZero() {
		t.Fatalf("reconciliation wrote a deadline over a run an attempt holds: %+v", record)
	}
	// And the marked directory is not adopted while an attempt holds it.
	for _, record := range harness.manager.loadRecords() {
		if record.RunID == "run_adoptable" {
			t.Fatalf("adoption claimed a run an attempt holds: %+v", record)
		}
	}
}
