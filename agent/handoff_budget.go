package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
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

// handoffUnpublishedEvictionWithheld is the token for the opposite line: the
// node was over budget, had only results no ledger saw left to give up, and
// did not know enough about its own two roots to be sure of that.
//
// It is a fixed token for the same reason the other one is. A node that stays
// over budget because it cannot read one of its roots is a node somebody has
// to look at, and "it gave nothing up" is invisible without a word to find.
const handoffUnpublishedEvictionWithheld = "retained_results_unpublished_eviction_withheld"

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
	// admittedAt and retainedAt are the candidate's order keys as this pass
	// read them, and they are re-read under the lease before anything is
	// deleted. A record that moved on between selection and the lease is a
	// different generation of the same run -- a rerun that finished in that
	// window -- and its fresh results are not what this pass chose to give up,
	// however identical its publication looks.
	admittedAt time.Time
	retainedAt time.Time
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
	evicted, reselects := 0, 0
	// A pass that gave anything up reports again, on every way out of the loop
	// and not only the one where the node ends up fitting. The figures the
	// node doctor and the status projection carry are the last measurement,
	// and leaving them at the pre-eviction one would describe a node holding
	// results it has just given up.
	defer func() {
		if evicted == 0 {
			return
		}
		// Both roots, freshly. What the pass carried forward between deletions
		// was enough to decide on -- neither root's figures move when the
		// other is deleted from -- but what a person and the node doctor read
		// afterwards should be one measurement of the node as it is now, not
		// one root measured after the last deletion and the other as it stood
		// before the first.
		status = m.remeasureNode(ctx, root)
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
		class, ok := m.nextEvictionClass(status, charged)
		if !ok {
			return
		}
		// One class, in order, until something is given up. A candidate the
		// pass cannot take -- an attempt holds it, its record moved on, the
		// helper refused it -- is a reason to try the next result of the same
		// kind, not a reason to stop: a single busy run used to end the whole
		// pass and leave a full node full. What is never done is falling
		// through to the other class; giving up a run's only copy because a
		// published one was momentarily busy is the loss the order exists to
		// prevent.
		taken, candidate, reselected := m.evictFromClass(ctx, root, class, charged)
		reselects += reselected
		if !taken {
			if reselected != 0 && reselects < maxNodeBudgetEvictions {
				// Some candidate of this class moved while the pass was
				// looking at it. The node is stale rather than stuck, so it
				// measures again and chooses again.
				status = m.remeasureNode(ctx, root)
				continue
			}
			if reselected != 0 {
				m.log("agent: this node holds %d charged bytes of retained results against a budget of %d and kept having to choose again; the rest waits for the next pass",
					charged, m.nodeBytes)
			}
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
		// Only the root the deletion came from. The two roots are measured
		// independently and dedup independently, so giving up a process run
		// cannot change what the helper's volumes cost and vice versa --
		// re-reading the other root would be a filesystem walk or a protocol
		// round trip that no figure depends on, and the figures carried
		// forward are as true as they were when they were taken.
		status = m.remeasureRoot(ctx, root, status, candidate.ociVolume)
	}
}

// evictFromClass walks one publication class in order until something is given
// up or until a candidate proves the walk is working from a stale order.
//
// The two are different answers and the difference is the whole point. A
// candidate the pass cannot take is this class being busy: the next result of
// the same kind is still the right thing to try, and stopping would leave a
// full node full because one run was momentarily held. A candidate whose
// publication or order keys *moved* says something else -- that the sorted
// list this walk is reading was built from a picture of the node that is no
// longer true -- and the specific move that matters is a run rerunning and
// finishing published, which takes it out of the unpublished class and into
// the one that must be given up first. Walking on from there deletes a run's
// only copy while a published result it just created sits beside it.
func (m *handoffManager) evictFromClass(ctx context.Context, root *os.Root, class []handoffEvictionCandidate, charged int64) (bool, handoffEvictionCandidate, int) {
	for position, candidate := range class {
		if handoffBudgetRace != nil {
			handoffBudgetRace(handoffBudgetCandidateChosen, candidate)
		}
		switch m.evictCandidate(ctx, root, candidate, charged) {
		case evictionDone:
			return true, candidate, 0
		case evictionReselect:
			// Stop here, not at the end of the class. Everything after this
			// candidate was sorted against a picture of the node that has
			// just been shown to be wrong, and the specific way it can be
			// wrong is the dangerous one: the candidate may have joined the
			// class that has to be given up first. One `reselected` is
			// enough -- it says the pass is stale, and staleness is not a
			// quantity.
			m.log("agent: the node stopped part-way through %d candidate(s) because %s stopped being the result this pass had chosen; it will measure and choose again",
				len(class)-position, candidate.describe())
			return false, handoffEvictionCandidate{}, 1
		}
		if ctx != nil && ctx.Err() != nil {
			return false, handoffEvictionCandidate{}, 0
		}
	}
	return false, handoffEvictionCandidate{}, 0
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
	status.OCI, status.OCIInventoryFailed = m.measureOCIHandoffs(ctx)
	return status
}

// remeasureRoot measures again the one root a deletion changed, and carries the
// other root's figures forward.
//
// The two roots are separate filesystems measured by separate passes, and each
// deduplicates inodes within itself: no deletion on one can change what the
// other holds or what it is charged. Measuring both after every deletion cost a
// walk of the agent's whole handoff root or a protocol round trip per
// eviction, for a figure that could not have moved.
func (m *handoffManager) remeasureRoot(ctx context.Context, root *os.Root, previous RetainedResultsStatus, ociVolume bool) RetainedResultsStatus {
	if ociVolume {
		status := previous
		status.OCI, status.OCIInventoryFailed = m.measureOCIHandoffs(ctx)
		return status
	}
	status := m.measureNode(ctx, root, m.now().UTC())
	status.OCI, status.OCIInventoryFailed = previous.OCI, previous.OCIInventoryFailed
	return status
}

// nextEvictionClass picks which class the node gives up from next, in order,
// and says why it is giving nothing up when that is the answer.
//
// It returns the whole class rather than its first member because a candidate
// the pass cannot take is not a reason to stop: the caller walks the class in
// order. What it must never do is return the other class as a fallback.
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
func (m *handoffManager) nextEvictionClass(status RetainedResultsStatus, charged int64) ([]handoffEvictionCandidate, bool) {
	published, unpublished := m.evictionCandidates(status)
	if len(published) != 0 {
		return published, true
	}
	if len(unpublished) == 0 {
		m.log("agent: this node holds %d charged bytes of retained results against a budget of %d and has nothing it may give up: every retained result is still being written, is held by an attempt, is paused as unsafe to delete, or is a handoff volume no run of this node can name",
			charged, m.nodeBytes)
		return nil, false
	}
	if reason := m.incompleteKnowledge(status); reason != "" {
		// An empty *visible* published list is not the same fact as a node
		// holding none, and only the second one permits this. A page of the
		// helper's root nobody read, a read that failed outright, a run whose
		// tree the pass could not finish -- each can hide the published result
		// this node is supposed to give up first, and giving up an unpublished
		// one instead destroys the only copy of what that run did.
		m.log("agent: %s: this node holds %d charged bytes of retained results against a budget of %d and would have to give up results no ledger saw, but %s, so it gives up nothing this pass",
			handoffUnpublishedEvictionWithheld, charged, m.nodeBytes, reason)
		return nil, false
	}
	return unpublished, true
}

// incompleteKnowledge names what this pass does not know about the node's two
// handoff roots, and is empty when it knows both whole.
//
// It gates exactly one decision. Every figure is still reported, every
// published result is still given up, and expiry is untouched: what a pass
// without complete knowledge may not do is conclude that no published result
// remains.
func (m *handoffManager) incompleteKnowledge(status RetainedResultsStatus) string {
	if status.Truncated != 0 {
		return fmt.Sprintf("%d run(s) under its own handoff root were measured incompletely, so a published run there may be reading as holding nothing", status.Truncated)
	}
	if status.Replaced != 0 {
		// A subtree that stopped being the one the pass was measuring is left
		// out of that run's figures entirely. A published run whose whole tree
		// went that way reports zero charged bytes, and a run charged nothing
		// is filtered out of the candidates -- so the node would conclude that
		// no published result remains while one is sitting there unmeasured.
		return fmt.Sprintf("%d subtree(s) stopped being the directory this pass was measuring, so a published run may be reading as holding nothing", status.Replaced)
	}
	if m.ociHandoffs == nil {
		// One root, and it was read whole.
		return ""
	}
	if status.OCIInventoryFailed || status.OCI == nil {
		return "its OCI helper's handoff root could not be read at all"
	}
	if !status.OCI.Complete {
		return "its OCI helper's handoff root was not read to the end, so a published volume may be beyond what this pass was shown"
	}
	if status.OCI.Truncated != 0 {
		return fmt.Sprintf("%d volume(s) in its OCI helper's handoff root were measured incompletely, so a published volume there may be reading as holding nothing", status.OCI.Truncated)
	}
	return ""
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
			admittedAt: record.AdmittedAt, retainedAt: record.RetainedAt,
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
	if left.atKnown && !left.at.Equal(right.at) {
		// Only when *both* are trusted. Between two volumes the node cannot
		// date, the timestamps are two workloads' own, and ordering on them
		// would hand one workload the choice of which honest run goes first --
		// the same forgery, one step further in.
		return left.at.Before(right.at)
	}
	// A stable tiebreak, not a neutral one. It settles two candidates the node
	// cannot otherwise tell apart into one order rather than whichever the
	// filesystem happened to list first -- but the identity it sorts on is
	// derived from a run's own `handoff_owner_run_id`, which a submitter
	// chooses, so a submitter can influence where its run falls among others
	// it cannot date. What that cannot do is what matters: this whole class is
	// given up last, after everything the node *can* date, and charged bytes
	// never enter the comparison, so no amount of naming makes a run jump the
	// order or makes a large run look small.
	return left.identity() < right.identity()
}

func (candidate handoffEvictionCandidate) identity() string {
	if candidate.ociVolume {
		return "oci:" + candidate.name
	}
	return "process:" + candidate.runID
}

// handoffBudgetRace is a test seam. It runs at the one point in a pass where a
// concurrent attempt used to be able to slip past -- after a candidate is
// chosen and before its lease is taken -- so that interleaving can be staged
// deterministically instead of raced for. Nothing outside a test ever sets it.
var handoffBudgetRace func(stage string, candidate handoffEvictionCandidate)

const handoffBudgetCandidateChosen = "candidate-chosen"

// evictionOutcome is what one attempt at giving a candidate up produced.
type evictionOutcome int

const (
	// evictionDone: the results are gone and the node is smaller.
	evictionDone evictionOutcome = iota
	// evictionReselect: this candidate's publication or its place in the order
	// changed between the measurement and the lease. The class this pass is
	// walking may not be the right class any more -- a run that reruns and
	// finishes *published* has left the unpublished class and joined the one
	// that must be given up first -- so the walk stops here and the pass
	// measures and chooses again. Continuing on a stale class is how a node
	// gives up a run's only copy while a published result it just created
	// sits beside it.
	evictionReselect
	// evictionUnavailable: this candidate cannot be taken now and nothing
	// about the order changed. An attempt holds it, its record went in flight
	// or into quarantine, the helper refused it as live, the removal failed.
	// It left the candidate set; it did not move to the other class. The walk
	// goes on to the next result of the same kind, because one busy run is not
	// a reason to leave a full node full.
	evictionUnavailable
)

// evictCandidate gives one candidate up and says what happened.
//
// The exclusion is the per-candidate path lease and nothing else. The lease is
// what an attempt takes to admit a directory or a volume, so holding it is
// exactly "this run is not executing", and it is scoped to the one run being
// given up.
//
// collectMu is deliberately *not* held across any of this. It is the lock an
// attempt's own finalization takes when it collects, and a deletion holds a
// workload-sized tree removal or a helper round trip: holding it here would
// put one run's storage in front of another run's completion, which is the
// same defect that moved measurement off the finalization path in the first
// place. Nothing here needs it. The sweep and this pass both take the same
// lease before touching a run, and both re-read the record under it, so two
// passes cannot decide the same run's fate at once whether or not they are
// serialized with each other.
func (m *handoffManager) evictCandidate(ctx context.Context, root *os.Root, candidate handoffEvictionCandidate, charged int64) evictionOutcome {
	if candidate.ociVolume {
		return m.evictHandoffVolume(ctx, candidate, charged)
	}
	return m.evictRetainedRun(root, candidate, charged)
}

// candidateMoved re-reads one candidate's own record and says whether it still
// describes the result this pass chose, and what changed when it does not.
//
// It takes no lease and needs none. A read of a record is safe without one
// because every write to a record is a rename over it -- a reader sees one
// whole version or another and never half of two -- and what this asks is not
// "may I delete this" but "is the sorted list I am walking still the right
// list". That question has to be answerable precisely when the lease is *not*
// available, because an attempt that reran a run and finished it published
// holds its lease until the upload is recorded, and a run that changed
// publication has changed class.
//
// It is deliberately conservative about a record it cannot read: unknowable is
// treated as moved, which costs a remeasure and never costs a run its results.
func (m *handoffManager) candidateMoved(candidate handoffEvictionCandidate) (bool, string) {
	if candidate.ociVolume {
		record, found, err := m.readOCIRecord(candidate.ownerKey)
		switch {
		case err != nil:
			return true, "no longer has a record this node can read"
		case !found:
			// No admission record names it, which is what a volume attributed
			// through the legacy upload record looks like. Nothing about the
			// order has been shown to have changed.
			return false, ""
		case record.live():
			// An attempt is writing into it. It has left the candidate set
			// without joining the other class, so the order is intact and the
			// caller's own message says who holds it.
			return false, ""
		case record.evidenceReachedLedger() != candidate.published:
			return true, "no longer carries the same answer to whether its evidence reached the ledger"
		}
		return false, ""
	}
	record, ok := m.currentRecord(candidate.record)
	if !ok {
		return true, "no longer has a record this node can act on"
	}
	if record.Published != candidate.published {
		return true, "no longer carries the same answer to whether its evidence reached the ledger"
	}
	if !record.AdmittedAt.Equal(candidate.admittedAt) || !record.RetainedAt.Equal(candidate.retainedAt) {
		return true, "was retained again"
	}
	return false, ""
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
func (m *handoffManager) evictRetainedRun(root *os.Root, candidate handoffEvictionCandidate, charged int64) evictionOutcome {
	lease := m.tryCollectLease(handoffPathLeaseKey(candidate.record.Directory))
	if lease == nil {
		// A held lease says an attempt has this run. It does not say the order
		// is still right, and those are different questions: an attempt that
		// reran this run and finished it *published* keeps its lease until its
		// result upload is recorded, so the run can have changed class while
		// still being held. Answering "unavailable" without looking is how the
		// walk goes on to delete an unpublished run's only copy while the
		// candidate it skipped has become the published result that should
		// have gone first.
		if moved, why := m.candidateMoved(candidate); moved {
			m.log("agent: run %s is held by an attempt and %s; it is not the result this pass chose to give up, and the node will choose again",
				candidate.runID, why)
			return evictionReselect
		}
		m.log("agent: leave run %s's retained results alone this pass: an attempt holds them and its record is unchanged, and the node is over its budget by %d bytes",
			candidate.runID, charged-m.nodeBytes)
		return evictionUnavailable
	}
	defer lease.release()
	record, ok := m.currentRecord(candidate.record)
	if !ok {
		// The record is gone or is no longer one this node can act on. Either
		// way the order was built on something that is not there, and what
		// replaced it is not knowable from here.
		return evictionReselect
	}
	if record.RetainUntil.IsZero() || record.Quarantine != "" {
		// It left the candidate set -- readmitted, or paused as unsafe to
		// delete. It did not join the other class, so the rest of this one is
		// still ordered correctly.
		m.log("agent: leave run %s's retained results alone this pass: its record moved on while the node was being measured",
			candidate.runID)
		return evictionUnavailable
	}
	// The order keys, not only the facts the filter reads. currentRecord
	// deliberately returns the *newer* record when the run was retained again,
	// and a rerun that finished between selection and this lease is a
	// different generation of the same run: its results are fresh, it is no
	// longer the oldest thing this node holds, and giving it up would be
	// giving up something the order would not have picked. Publication
	// matching is not enough on its own -- a rerun of a published run is
	// published too.
	if moved, why := m.candidateMoved(candidate); moved {
		m.log("agent: run %s %s while this node was being measured; it is not the result this pass chose to give up, and the node will choose again",
			candidate.runID, why)
		return evictionReselect
	}
	removed, err := m.removeRetainedRun(root, record, lease, handoffRemovalOverBudget)
	if err != nil {
		m.noteRemovalFailure(record, err)
		return evictionUnavailable
	}
	if !removed {
		// The removal found nothing at the run's name and dropped its record
		// instead. Nothing was recovered, so this is not progress -- but a
		// node that measured bytes there and then found none is worth one
		// line rather than none.
		m.log("agent: run %s's retained results were already gone when the node went to give them up, and its record went with them",
			record.RunID)
		return evictionUnavailable
	}
	m.log("agent: run %s's retained results were given up early to fit this node's budget, recovering up to the %d charged bytes they were measured at; its result document, if it uploaded one, is still readable with `wefty results`",
		record.RunID, candidate.charged)
	return evictionDone
}

// evictHandoffVolume gives one handoff volume up, under this node's own lease
// and the helper's own guard.
//
// Two things have to be true at the moment the volume is freed, and neither
// side can establish both. This node knows which of its attempts is admitted
// -- it takes the volume's lease before the runtime request and writes an
// admission record under it -- and it cannot know whether the helper has since
// registered ownership from some other path. The helper knows ownership and
// nothing about publication or budgets. So the lease is taken here, the record
// is re-read under it, and the helper still refuses a volume it finds owned
// when the deletion reaches it, under the same lock `Run` publishes ownership
// under. A refusal is not a failure: it is the guard working, and the node
// simply keeps the run's results and chooses again.
//
// The lease is held across the call, and `collectMu` is not: an unrelated
// attempt's finalization must not wait on a helper round trip and a tree free
// on the other side of the runtime seam.
func (m *handoffManager) evictHandoffVolume(ctx context.Context, candidate handoffEvictionCandidate, charged int64) evictionOutcome {
	if m.ociEvictor == nil {
		m.log("agent: this node holds %d charged bytes of retained results against a budget of %d and cannot give the OCI handoff volume for run %s up: its runtime provides no eviction",
			charged, m.nodeBytes, candidate.ownerKey)
		return evictionUnavailable
	}
	lease := m.tryCollectLease(ociHandoffLeaseKey(candidate.ownerKey))
	if lease == nil {
		// Same question, same answer as the process root: the lease says an
		// attempt has this volume, not that the order is still right. An
		// attempt that reran it and finished it published holds the volume's
		// lease until its upload is recorded.
		if moved, why := m.candidateMoved(candidate); moved {
			m.log("agent: the OCI handoff volume for run %s is held by an attempt and %s; it is not the result this pass chose to give up, and the node will choose again",
				candidate.ownerKey, why)
			return evictionReselect
		}
		m.log("agent: leave the OCI handoff volume for run %s alone this pass: an attempt of this node holds it and its record is unchanged, and the node is over its budget by %d bytes",
			candidate.ownerKey, charged-m.nodeBytes)
		return evictionUnavailable
	}
	defer lease.release()
	// Re-read under the lease. An attempt that admitted this volume between
	// the measurement and the lease has already written its record, so this is
	// where an admission that raced the pass is seen -- and the record is
	// re-read rather than trusted from the snapshot for the same reason the
	// process root re-reads its own.
	record, found, err := m.readOCIRecord(candidate.ownerKey)
	if err != nil {
		// The record the order was built on cannot be read, so what it says
		// now is not knowable from here.
		m.log("agent: leave the OCI handoff volume for run %s alone this pass: re-reading its record failed: %v", candidate.ownerKey, err)
		return evictionReselect
	}
	if found && record.live() {
		// An attempt admitted it. It left the candidate set without joining
		// the other class, so the rest of this one is still in order.
		m.log("agent: the OCI handoff volume for run %s was claimed by attempt %s while this node was being measured; it is not the budget's to give up",
			candidate.ownerKey, record.AttemptID)
		return evictionUnavailable
	}
	if found && record.evidenceReachedLedger() != candidate.published {
		m.log("agent: whether run %s's evidence reached the ledger changed while this node was being measured; the node will choose again", candidate.ownerKey)
		return evictionReselect
	}
	if err := m.ociEvictor.EvictRetainedHandoff(ctx, candidate.ownerKey); err != nil {
		if errors.Is(err, workloadrunner.ErrRetainedHandoffLive) {
			// The helper's guard, and the same shape as this node's own: the
			// volume is an attempt's, so it is out of the candidate set and
			// nothing about the order moved.
			m.log("agent: the OCI helper refused to give the handoff volume for run %s up because an attempt owns it; the node keeps those results: %v",
				candidate.ownerKey, err)
			return evictionUnavailable
		}
		if ctx != nil && errors.Is(err, ctx.Err()) {
			m.log("agent: the OCI handoff volume for run %s was not given up: the pass was cancelled", candidate.ownerKey)
			return evictionUnavailable
		}
		m.log("agent: give the OCI handoff volume for run %s up to fit this node's budget: %v", candidate.ownerKey, err)
		return evictionUnavailable
	}
	// The volume is gone, so the record that named it is not authority over
	// anything any more. Leaving it would keep a published fact standing over
	// a volume a later rerun recreates.
	if err := m.removeOCIRecord(candidate.ownerKey); err != nil {
		m.log("agent: remove the OCI handoff record for run %s after giving its volume up: %v", candidate.ownerKey, err)
	}
	m.log("agent: the OCI handoff volume for run %s was given up early to fit this node's budget, recovering up to the %d charged bytes it was measured at; its result document, if it uploaded one, is still readable with `wefty results`",
		candidate.ownerKey, candidate.charged)
	return evictionDone
}
