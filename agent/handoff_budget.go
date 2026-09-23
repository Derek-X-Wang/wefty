package agent

import (
	"context"
	"errors"
	"os"
	"sort"
	"strings"
	"time"
)

// maxNodeBudgetEvictions bounds what one pass will give up.
//
// The loop remeasures after every deletion rather than subtracting, which is
// what makes it safe on a root where two runs hard-link one file -- but it
// also means the loop's length is decided by what it finds rather than by what
// it planned, so it needs a bound that does not depend on the filesystem. A
// node that is still over budget after this many evictions is reported and
// left to the next pass: the passes are an hour apart and the node keeps
// working, which is the failure worth having.
const maxNodeBudgetEvictions = 256

// handoffUnpublishedEviction is the token in the one log line a node prints
// when it gives up results no ledger ever saw.
//
// It is a fixed token rather than a sentence so that a person grepping a
// node's log, or anything reading it, can find every one of them without
// matching prose. The line it appears in names the run, because that run's
// files were the only copy of what it did.
const handoffUnpublishedEviction = "retained_results_unpublished_eviction"

// handoffEvictionCandidate is one thing the node could give up, from either
// root, reduced to the facts the order is decided on.
type handoffEvictionCandidate struct {
	// ociVolume distinguishes the two roots. A process candidate is deleted
	// through the verified run handle under its record's path lease; an OCI
	// candidate is deleted by asking the helper, which owns that filesystem.
	ociVolume bool
	// runID names a process candidate; ownerKey names an OCI one and is the
	// only identity DeleteManagedVolume accepts. name is the helper's
	// directory name, carried for the log.
	runID    string
	ownerKey string
	name     string
	// record is the process candidate's record as this pass loaded it. Every
	// decision about it is re-made under the lease against what is on disk
	// then; this is the snapshot that write is refused against.
	record    retentionRecord
	published bool
	// at is when the run finished, and atKnown says whether the node can
	// stand behind it. For a process run it is always known. For a handoff
	// volume with no helper-owned receipt it is a timestamp the workload could
	// have written, which is not a basis for preferring one honest run's loss
	// over another's -- so those volumes are given up last within their class
	// rather than sorted on a number a workload chose.
	at      time.Time
	atKnown bool
	charged int64
}

func (candidate handoffEvictionCandidate) describe() string {
	if candidate.ociVolume {
		return "the OCI handoff volume for run " + candidate.ownerKey
	}
	return "run " + candidate.runID
}

// enforceNodeBudget gives results up until the node fits inside
// contract.MaxRetainedResultNodeBytes, published ones first.
//
// It runs on the accounting pass, which is the collector's own goroutine and
// never an attempt's finalization: an eviction has to measure first, and a
// workload's tree must never sit in front of another run finishing or of the
// node lock being released. Expiry has already run by then, at startup and on
// the same timer, so what this sees is what the schedule alone was not going
// to reclaim.
//
// The loop remeasures after every deletion instead of subtracting the run's
// figure. It has to: a file two runs hard-link is charged to whichever the
// pass reached first, so giving up the other recovers none of those bytes, and
// a loop that subtracted what it thought a run was worth would stop while the
// node was still full.
func (m *handoffManager) enforceNodeBudget(ctx context.Context, root *os.Root, status RetainedResultsStatus) {
	if m == nil || m.nodeBytes <= 0 {
		return
	}
	evicted := 0
	// A pass that gave anything up reports again, on every way out of the loop
	// and not only the one where the node ends up fitting. The figures the
	// node doctor and the status projection carry are the last measurement,
	// and leaving them at the pre-eviction one would describe a node holding
	// results it has just given up.
	defer func() {
		if evicted == 0 {
			return
		}
		m.log("agent: this node gave %d retained result(s) up to fit its budget and now holds %d charged bytes against %d",
			evicted, nodeChargedBytes(status), m.nodeBytes)
		m.reportNodeAccounting(status)
	}()
	for {
		charged := nodeChargedBytes(status)
		if charged <= m.nodeBytes {
			break
		}
		if ctx != nil && ctx.Err() != nil {
			m.log("agent: this node holds %d charged bytes of retained results against a budget of %d, and the pass that would have given some up was cancelled",
				charged, m.nodeBytes)
			return
		}
		if evicted >= maxNodeBudgetEvictions {
			m.log("agent: this node holds %d charged bytes of retained results against a budget of %d after giving %d up; the rest waits for the next pass",
				charged, m.nodeBytes, evicted)
			return
		}
		candidate, ok := m.nextEviction(status, charged)
		if !ok {
			return
		}
		if !m.evictCandidate(ctx, root, candidate, charged) {
			// Nothing was given up, so remeasuring would produce the same
			// candidate and the same refusal. The reason is already logged.
			return
		}
		if !candidate.published {
			// After the deletion, not before it. This token is what a person
			// greps a node's log for, and a line printed on the intention
			// would count evictions that were refused.
			m.log("agent: %s: gave up %s and the %d charged bytes it held, whose evidence reached no ledger, because this node held %d charged bytes of retained results against a budget of %d and nothing published remained to give up first",
				handoffUnpublishedEviction, candidate.describe(), candidate.charged, charged, m.nodeBytes)
		}
		evicted++
		status = m.remeasureNode(ctx, root)
	}
}

// nodeChargedBytes is the one number the budget is about: what both handoff
// roots cost this node, in charged bytes.
//
// Quarantined runs are in it. Quarantine means "unsafe to delete", never
// "excluded from accounting" -- their bytes are on the node, and leaving them
// out would make quarantine a way to hide storage from the budget.
//
// Pending detached frees are not in it, and deliberately: their removal is
// already authorized, nothing this budget could decide would change it, and
// the next pass over the helper's root finishes them. Counting them would make
// the node evict live results to make room for bytes that are already going
// away.
func nodeChargedBytes(status RetainedResultsStatus) int64 {
	total := status.ChargedBytes
	if status.OCI != nil {
		total += status.OCI.ChargedBytes
	}
	return total
}

func (m *handoffManager) remeasureNode(ctx context.Context, root *os.Root) RetainedResultsStatus {
	status := m.measureNode(ctx, root, m.now().UTC())
	status.OCI = m.measureOCIHandoffs(ctx)
	return status
}

// nextEviction picks what the node gives up next, and says why it is giving
// nothing up when that is the answer.
//
// The order is published first, then oldest. A run whose evidence reached a
// ledger has a copy somewhere else; a run whose evidence did not is the only
// copy of what it did, so it is given up only when nothing published remains
// -- which is Derek's ruling and is also why "published" is split out here
// rather than folded into one comparison. A node that filled and stopped
// serving would be the worse loss, so an unpublished result is given up, and
// the caller says so loudly and by name once it actually has been.
//
// The published list is not fallen through when every one of its candidates is
// refused. "Nothing published remains" has to mean the node holds none, not
// that the ones it holds were busy this pass: falling through would give up a
// run's only copy while a published one was a minute away from being
// available.
func (m *handoffManager) nextEviction(status RetainedResultsStatus, charged int64) (handoffEvictionCandidate, bool) {
	published, unpublished := m.evictionCandidates(status)
	if len(published) != 0 {
		return published[0], true
	}
	if len(unpublished) == 0 {
		m.log("agent: this node holds %d charged bytes of retained results against a budget of %d and has nothing it may give up: every retained result is still being written, is held by an attempt, is paused as unsafe to delete, or is a handoff volume no run of this node can name",
			charged, m.nodeBytes)
		return handoffEvictionCandidate{}, false
	}
	return unpublished[0], true
}

// evictionCandidates is everything the node may give up, ordered, split by
// whether its evidence reached a ledger.
//
// What is left out is as much of the rule as what is in. A run an attempt is
// still writing is not a candidate on either root -- on the process side
// because its record has no terminal window yet, on the OCI side because the
// helper says the volume is live. A run whose name is paused as unsafe to
// delete is not a candidate, and is still charged. A handoff volume whose name
// no run of this node derives is not a candidate, because the only way to ask
// the helper to remove one is by an owner key this node cannot produce; it is
// counted, and it is reclaimed by the helper's own expiry instead. And a
// candidate whose measured share is zero is left out because giving it up
// would recover nothing and the loop would come straight back to it.
func (m *handoffManager) evictionCandidates(status RetainedResultsStatus) (published, unpublished []handoffEvictionCandidate) {
	charged := make(map[string]int64, len(status.PerRun))
	for _, run := range status.PerRun {
		charged[run.RunID] = run.ChargedBytes
	}
	// Records are re-read rather than taken from the measurement, for the same
	// reason the measurement re-reads them: this pass holds no lock, and what
	// decides a deletion has to be the record as it is now, not as it was when
	// the figures were taken. Every one of them is read again under its lease
	// before anything is deleted.
	candidates := make([]handoffEvictionCandidate, 0, len(status.PerRun))
	for _, record := range m.loadRecords() {
		if record.RetainUntil.IsZero() || record.Quarantine != "" {
			continue
		}
		bytes := charged[record.RunID]
		if bytes <= 0 {
			continue
		}
		candidates = append(candidates, handoffEvictionCandidate{
			runID: record.RunID, record: record, published: record.Published,
			at: record.RetainedAt, atKnown: true, charged: bytes,
		})
	}
	if status.OCI != nil {
		for _, volume := range status.OCI.PerVolume {
			if volume.Live || strings.TrimSpace(volume.OwnerKey) == "" || volume.ChargedBytes <= 0 {
				continue
			}
			candidates = append(candidates, handoffEvictionCandidate{
				ociVolume: true, ownerKey: volume.OwnerKey, name: volume.Name,
				published: volume.Published, at: volume.TerminalAt,
				atKnown: volume.TerminalKnown, charged: volume.ChargedBytes,
			})
		}
	}
	sort.SliceStable(candidates, func(left, right int) bool {
		return lessEvictable(candidates[left], candidates[right])
	})
	for _, candidate := range candidates {
		if candidate.published {
			published = append(published, candidate)
			continue
		}
		unpublished = append(unpublished, candidate)
	}
	return published, unpublished
}

// lessEvictable orders two candidates of the same publication class: the ones
// the node can date go first, oldest first, and the ones it cannot go last.
//
// The second half is the whole reason this is not one comparison on a
// timestamp. A handoff volume with no helper-owned receipt is dated by its
// directory's mtime, which the workload that ran in it owns and can move. The
// contract already refuses to *expire* a volume on that timestamp; ordering
// every eviction by it would let one workload push an honest run's results out
// of a full node instead, which is the same forgery with a slower fuse.
func lessEvictable(left, right handoffEvictionCandidate) bool {
	if left.atKnown != right.atKnown {
		return left.atKnown
	}
	if !left.at.Equal(right.at) {
		return left.at.Before(right.at)
	}
	// A stable tiebreak so two runs retained in the same instant are given up
	// in one order rather than whichever the filesystem listed first.
	return left.identity() < right.identity()
}

func (candidate handoffEvictionCandidate) identity() string {
	if candidate.ociVolume {
		return "oci:" + candidate.name
	}
	return "process:" + candidate.runID
}

// evictCandidate gives one candidate up and reports whether it did.
//
// collectMu is held for the deletion and nothing else. It is the lock the
// expiry sweep holds while it selects and removes, so taking it here is what
// stops two passes each deleting what the other had chosen, and it is what
// makes two budget passes exclusive of one another on both roots.
//
// It is held for one deletion rather than for the loop because the remeasuring
// between deletions is the part that must never sit in front of an attempt's
// finalization -- that measurement walks a workload's tree, and an attempt's
// completion collects under this same lock.
func (m *handoffManager) evictCandidate(ctx context.Context, root *os.Root, candidate handoffEvictionCandidate, charged int64) bool {
	m.collectMu.Lock()
	defer m.collectMu.Unlock()
	if candidate.ociVolume {
		return m.evictHandoffVolume(ctx, candidate, charged)
	}
	return m.evictRetainedRun(root, candidate, charged)
}

// evictRetainedRun gives one run's retained results up early, through exactly
// the removal expiry uses.
//
// The lease is what keeps this off a run an attempt is holding: an attempt
// that claimed the path while this pass was measuring owns it, and the
// candidate is skipped rather than deleted. Everything after the lease is
// decided on the record as it is on disk under that lease, not on the snapshot
// this pass chose from -- the run may have been rerun and retained again in
// between, and deleting the results of a run that finished seconds ago is the
// same defect as deleting one an attempt is still writing.
func (m *handoffManager) evictRetainedRun(root *os.Root, candidate handoffEvictionCandidate, charged int64) bool {
	lease := m.tryCollectLease(candidate.record.Directory)
	if lease == nil {
		m.log("agent: leave run %s's retained results alone this pass: an attempt holds them, and the node is over its budget by %d bytes",
			candidate.runID, charged-m.nodeBytes)
		return false
	}
	defer lease.release()
	record, ok := m.currentRecord(candidate.record)
	if !ok {
		return false
	}
	if record.RetainUntil.IsZero() || record.Quarantine != "" {
		m.log("agent: leave run %s's retained results alone this pass: its record moved on while the node was being measured",
			candidate.runID)
		return false
	}
	if record.Published != candidate.published {
		// The order this pass chose was decided on a fact that has since
		// changed. Giving the run up now would be giving up a run the order
		// would not have picked.
		m.log("agent: leave run %s's retained results alone this pass: whether its evidence reached the ledger changed while the node was being measured",
			candidate.runID)
		return false
	}
	removed, err := m.removeRetainedRun(root, record, lease, handoffRemovalOverBudget)
	if err != nil {
		m.noteRemovalFailure(record, err)
		return false
	}
	if !removed {
		// The removal found nothing at the run's name and dropped its record
		// instead. Nothing was recovered, so this is not progress -- but a
		// node that measured bytes there and then found none is worth one
		// line rather than none.
		m.log("agent: run %s's retained results were already gone when the node went to give them up, and its record went with them",
			record.RunID)
		return false
	}
	m.log("agent: run %s's retained results were given up early to fit this node's budget, recovering up to the %d charged bytes they were measured at; its result document, if it uploaded one, is still readable with `wefty results`",
		record.RunID, candidate.charged)
	return true
}

// evictHandoffVolume asks the helper to give one handoff volume up.
//
// The agent cannot delete it itself -- on a Mac node that filesystem is inside
// a Lima VM -- and it cannot name it either: `DeleteManagedVolume` takes the
// owner key and derives the directory, which is what confines this to volumes
// the node already accounts for.
//
// The live-attempt guard is the helper's, because the fact is the helper's:
// the volume is live from the moment `Run` registers the attempt's ownership
// and supersedes the prior receipt. This pass reads that fact fresh on the
// measurement it chose from, and re-reads it on every remeasure, which leaves
// the same window the helper's own expiry sweep has between reading its live
// set and deleting on it.
func (m *handoffManager) evictHandoffVolume(ctx context.Context, candidate handoffEvictionCandidate, charged int64) bool {
	if m.ociEvictor == nil {
		m.log("agent: this node holds %d charged bytes of retained results against a budget of %d and cannot give the OCI handoff volume for run %s up: its runtime provides no eviction",
			charged, m.nodeBytes, candidate.ownerKey)
		return false
	}
	if err := m.ociEvictor.EvictRetainedHandoff(ctx, candidate.ownerKey); err != nil {
		if ctx != nil && errors.Is(err, ctx.Err()) {
			m.log("agent: the OCI handoff volume for run %s was not given up: the pass was cancelled", candidate.ownerKey)
			return false
		}
		m.log("agent: give the OCI handoff volume for run %s up to fit this node's budget: %v", candidate.ownerKey, err)
		return false
	}
	m.log("agent: the OCI handoff volume for run %s was given up early to fit this node's budget, recovering up to the %d charged bytes it was measured at; its result document, if it uploaded one, is still readable with `wefty results`",
		candidate.ownerKey, candidate.charged)
	return true
}
