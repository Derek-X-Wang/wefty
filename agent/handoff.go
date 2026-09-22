package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const (
	handoffMarkerName = ".wefty-handoff.json"
	// handoffResultName is the one file the per-run bound will not drop. It is
	// the contract's name for a run's result document.
	handoffResultName = "result.json"
	// maxHandoffMarkerBytes bounds the advisory ownership marker read at
	// preparation. It is a handful of fields; anything larger is not one.
	maxHandoffMarkerBytes = 8 << 10
)

type handoffManager struct {
	root string
	// ledgerRoot is the root the run ledger derives dispatched handoff paths
	// from. It is this node's knowledge of the wire default, not a second place
	// results may live: a dispatched path under it is adopted onto root above
	// before anything prepares, retains, reads or sweeps it. It is a field
	// rather than a direct constant read so a test can prove the mapping
	// without writing into the real default root.
	ledgerRoot string
	// stateRoot keeps retention records away from casual writes through the
	// handoff directory. It is not a tamper boundary against the same OS UID.
	stateRoot string
	// nodeID is this node's identity. A record naming another node is not this
	// agent's to act on.
	nodeID    string
	retention time.Duration
	// runBytes is a field rather than a direct constant read so a test can
	// prove the bound without writing 64 MiB.
	runBytes int64
	now      func() time.Time
	logf     func(string, ...any)
	// observeAccounting publishes each accounting pass's figures onto the
	// agent's status projection. It is a hook rather than a direct call
	// because retention is filesystem work and the observer is session state;
	// nothing here should have to know which. Nothing reads the figures yet
	// beyond the log line and that projection -- the node budget that will is
	// a later slice of #494.
	observeAccounting func(RetainedResultsStatus)

	mu    sync.Mutex
	paths map[string]*handoffPathLock

	// collectMu serializes collection with itself. Two sweeps racing would
	// each select candidates the other is deleting.
	collectMu sync.Mutex
}

// handoffOwnership is an opaque preparation receipt for one lock acquisition.
// The handle remains open until that acquisition is released.
type handoffOwnership struct {
	lease  *handoffLease
	runID  string
	nodeID string
	run    *os.Root
	// prepared is the identity of the directory this receipt was issued for,
	// taken while it was certainly still linked. finish compares the run name
	// against this rather than re-stating the handle, because a workload that
	// unlinked the directory leaves a handle whose own "." no longer resolves
	// on Linux -- os.Root reaches it through /proc/self/fd/N -- so asking the
	// handle what it is would fail exactly in the case worth detecting.
	prepared os.FileInfo
}

type handoffLease struct {
	manager   *handoffManager
	path      string
	pathLock  *handoffPathLock
	ownership *handoffOwnership
	once      sync.Once
}

func (lease *handoffLease) release() {
	lease.once.Do(func() {
		m := lease.manager
		m.mu.Lock()
		lease.pathLock.owner = nil
		if lease.ownership != nil {
			lease.ownership.run.Close()
		}
		m.mu.Unlock()
		lease.pathLock.token <- struct{}{}
		m.releasePathReference(lease.path, lease.pathLock)
	})
}

type handoffPathLock struct {
	token chan struct{}
	refs  int
	owner *handoffLease
}

// handoffMarker is advisory and lives inside the workload-writable handoff
// directory. Its whole remaining job is to prove to a cold rerun that the files
// it found are its own, and it is read only at preparation. Retention is
// decided by the agent-local record instead (handoff_records.go), because a
// workload shares this agent's OS identity and anything in here is a file it
// can rewrite.
type handoffMarker struct {
	RunID       string    `json:"run_id"`
	NodeID      string    `json:"node_id"`
	RetainUntil time.Time `json:"retain_until"`
}

func newHandoffManager(root, stateRoot, nodeID string, retention time.Duration, logf func(string, ...any)) *handoffManager {
	if strings.TrimSpace(root) == "" {
		root = contract.DefaultHandoffRoot
	}
	return &handoffManager{
		root: filepath.Clean(root), ledgerRoot: filepath.Clean(contract.DefaultHandoffRoot),
		stateRoot: strings.TrimSpace(stateRoot),
		nodeID:    strings.TrimSpace(nodeID), retention: retention,
		runBytes: contract.MaxRetainedResultBytes,
		now:      time.Now, logf: logf,
		paths: make(map[string]*handoffPathLock),
	}
}

func (m *handoffManager) log(format string, args ...any) {
	if m != nil && m.logf != nil {
		m.logf(format, args...)
	}
}

// lock holds exclusive ownership of one handoff path across the complete
// prepare, execution, completion, and finish lifecycle. Per-call locking is
// insufficient because finish may trim a directory another attempt uses.
func (m *handoffManager) lock(ctx context.Context, spec contract.JobSpec) (*handoffLease, error) {
	path := filepath.Clean(spec.Execution.HandoffDirectory)
	m.mu.Lock()
	pathLock := m.paths[path]
	if pathLock == nil {
		pathLock = &handoffPathLock{token: make(chan struct{}, 1)}
		pathLock.token <- struct{}{}
		m.paths[path] = pathLock
	}
	pathLock.refs++
	m.mu.Unlock()

	if err := context.Cause(ctx); err != nil {
		m.releasePathReference(path, pathLock)
		return nil, err
	}
	select {
	case <-ctx.Done():
		m.releasePathReference(path, pathLock)
		return nil, context.Cause(ctx)
	case <-pathLock.token:
	}
	m.mu.Lock()
	lease := &handoffLease{manager: m, path: path, pathLock: pathLock}
	pathLock.owner = lease
	m.mu.Unlock()
	return lease, nil
}

func (m *handoffManager) releasePathReference(path string, pathLock *handoffPathLock) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pathLock.refs--
	if pathLock.refs == 0 {
		delete(m.paths, path)
	}
}

// errUnmanagedHandoffDirectory is the one refusal this node owes a dispatcher
// whose handoff path it cannot adopt.
//
// It is typed because the defect it replaces was untyped. A path this agent did
// not manage used to produce no ownership and no error at all: the run
// executed, wrote its files into a directory nothing here swept, uploaded no
// result, and `wefty inspect` still advertised a retention window over a
// directory the agent had never taken responsibility for. A node that cannot
// manage the directory a run was given says so.
var errUnmanagedHandoffDirectory = errors.New("handoff directory is not one this node manages")

// errHandoffNameNotADirectory marks the one refusal repeating cannot resolve:
// the name a run's directory should be at holds something else -- a symlink, a
// FIFO, a regular file. Every other failure the sweep meets can clear on its
// own, so this is the only class it is allowed to stop retrying (see
// noteExpiryFailure), and it is a sentinel rather than a string match because
// "stop sweeping this run" is too consequential a decision to key off wording.
var errHandoffNameNotADirectory = errors.New("handoff name is not a directory")

// ownsHandoff reports that this job carries the run identity everything the
// agent does with a handoff directory is keyed by: the retention record, the
// per-run bound, the sweep, and the result upload all name a run.
//
// A one-shot process job submitted straight to L1 may carry none — the job spec
// requires a handoff directory, not a run — and such a job has no retained
// results and no result row on this node by construction. Its directory is
// still created for the workload, explicitly and with a log line, rather than
// being an unannounced nil somewhere inside preparation.
func (m *handoffManager) ownsHandoff(spec contract.JobSpec) bool {
	return handoffOwnerRunID(spec) != ""
}

// prepareUnownedDirectory creates the directory of a job this node manages no
// results for. It owns nothing, records nothing and sweeps nothing: the
// workload is simply given the directory it was dispatched with.
func (m *handoffManager) prepareUnownedDirectory(spec contract.JobSpec) error {
	path := filepath.Clean(spec.Execution.HandoffDirectory)
	if path == "." || !filepath.IsAbs(path) {
		return errors.New("handoff directory must be an absolute path")
	}
	directory, err := openPrivateHandoffDirectory(path)
	if err != nil {
		return err
	}
	m.log("agent: handoff directory %q belongs to a job with no run identity; this node retains and uploads nothing for it", path)
	return directory.Close()
}

// resolveHandoffDirectory maps a dispatched handoff path onto the root this
// node actually manages, and is the single definition of which paths this agent
// accepts for a run it is responsible for.
//
// The node's configured root wins. L3 keeps sending the path it derives from
// the ledger's default root — that path is wire format, and an older node has
// to keep reading it — so a node configured elsewhere adopts the same run-keyed
// leaf under its own root instead. Ownership, the retention record, the result
// upload and what `wefty inspect` claims is retained then all name the one
// directory the agent is really sweeping.
//
// A path under neither root is refused rather than adopted: it is a directory
// this agent has no authority over, and running into one is exactly the silence
// this error exists to replace.
func (m *handoffManager) resolveHandoffDirectory(spec contract.JobSpec) (string, error) {
	path := filepath.Clean(spec.Execution.HandoffDirectory)
	if path == "." || !filepath.IsAbs(path) {
		return "", fmt.Errorf("%w: %q is not an absolute path",
			errUnmanagedHandoffDirectory, spec.Execution.HandoffDirectory)
	}
	runID := handoffOwnerRunID(spec)
	if runID == "" {
		return "", fmt.Errorf("%w: %q names no handoff owner run", errUnmanagedHandoffDirectory, path)
	}
	if !validRunMailboxSegment(runID) {
		return "", fmt.Errorf("%w: handoff owner run %q is not one safe path component",
			errUnmanagedHandoffDirectory, runID)
	}
	managed := filepath.Join(m.root, runID)
	if path == managed || path == filepath.Join(m.ledgerRoot, runID) {
		return managed, nil
	}
	return "", fmt.Errorf("%w: %q is under neither this node's handoff root %q nor the ledger's default %q",
		errUnmanagedHandoffDirectory, path, m.root, m.ledgerRoot)
}

func (m *handoffManager) prepare(lease *handoffLease, spec contract.JobSpec, nodeID string) (*handoffOwnership, error) {
	path := filepath.Clean(spec.Execution.HandoffDirectory)
	m.mu.Lock()
	owned := lease != nil && lease.manager == m && lease.path == path && lease.pathLock.owner == lease && lease.ownership == nil
	m.mu.Unlock()
	if !owned {
		return nil, errors.New("handoff preparation requires this attempt's path lock")
	}
	// Preparation either returns a receipt or says why it could not. It never
	// returns neither: an attempt that proceeds with no ownership is one whose
	// results nothing retains, reads or uploads.
	managed, err := m.resolveHandoffDirectory(spec)
	if err != nil {
		return nil, err
	}
	if managed != path {
		// The lock this attempt holds is on the dispatched path, so preparing a
		// different directory here would retain and read one nothing else in
		// the attempt is holding. The claim adopts the path before the lock is
		// taken; reaching this means it did not, and a refusal is louder than
		// a silent divergence.
		return nil, fmt.Errorf("%w: this attempt holds %q while this node manages %q",
			errUnmanagedHandoffDirectory, path, managed)
	}
	runID := handoffOwnerRunID(spec)
	root, err := openPrivateHandoffDirectory(m.root)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	if err := root.Mkdir(runID, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, err
	}
	run, err := openHandoffDirectory(root, runID)
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			run.Close()
		}
	}()
	directory, err := run.Open(".")
	if err != nil {
		return nil, err
	}
	err = directory.Chmod(0o700)
	directory.Close()
	if err != nil {
		return nil, err
	}
	marker, exists, err := readHandoffMarker(run)
	if err != nil {
		return nil, err
	}
	hasFiles, err := handoffHasFiles(run)
	if err != nil {
		return nil, err
	}
	if !exists && hasFiles {
		return nil, fmt.Errorf("handoff directory %q contains unmanaged files", path)
	}
	if exists {
		if marker.RunID != runID {
			return nil, fmt.Errorf("handoff directory %q belongs to run %q, not %q", path, marker.RunID, runID)
		}
		if marker.NodeID != nodeID {
			return nil, fmt.Errorf("handoff directory %q belongs to stable node %q, not %q", path, marker.NodeID, nodeID)
		}
		if hasFiles {
			stableTag := contract.StableNodeTagPrefix + nodeID
			if !slices.Contains(spec.RoutingTags, stableTag) {
				return nil, fmt.Errorf("cold rerun consuming handoff files must include reserved stable-node tag %q", stableTag)
			}
		}
	}
	if err := writeHandoffMarker(run, handoffMarker{
		RunID: runID, NodeID: nodeID, RetainUntil: m.now().UTC().Add(m.retention),
	}); err != nil {
		return nil, err
	}
	prepared, err := run.Stat(".")
	if err != nil {
		return nil, err
	}
	// The record is written here, at admission, not at finish.
	//
	// A record that appeared only when a run finished made two things
	// impossible. An in-flight run's bytes were invisible to accounting, so a
	// node could not know what it was holding until it was no longer being
	// written; and an agent that died mid-run left a directory no record
	// named, which nothing measures and nothing sweeps -- residue by
	// construction. It carries no deadline: a run that has not finished has
	// nothing to retain yet, and the sweep skips a record with no deadline.
	// Startup is where an admission that never finished gets one
	// (adoptResidue).
	//
	// It fails preparation rather than being best effort. A node that cannot
	// write the record is a node that will not be able to account for, bound
	// or expire this run's results, and saying so before the workload starts
	// is better than discovering it at finish with the files already written.
	if err := m.writeRecord(retentionRecord{
		RunID: runID, NodeID: nodeID, Directory: path, AdmittedAt: m.now().UTC(),
	}); err != nil {
		return nil, fmt.Errorf("record the admission of handoff directory %q: %w", path, err)
	}
	owner := &handoffOwnership{lease: lease, runID: runID, nodeID: nodeID, run: run, prepared: prepared}
	m.mu.Lock()
	lease.ownership = owner
	m.mu.Unlock()
	keep = true
	return owner, nil
}

// finish retains the run's results. It used to delete the directory outright
// when the attempt succeeded, which meant the one outcome an operator most
// wants to read -- a run that worked -- was the one that left nothing behind.
// Both outcomes are now retained on the same rule and expire on the same
// deadline.
//
// It is a no-op without the receipt from this attempt's still-held acquisition.
// Terminal state comes from that agent-held ownership, not from a marker the
// workload could have rewritten, and the caller runs this while it still holds
// the path lock so a successor attempt cannot already be writing here.
func (m *handoffManager) finish(owner *handoffOwnership, spec contract.JobSpec, nodeID string, succeeded, published bool) error {
	path := filepath.Clean(spec.Execution.HandoffDirectory)
	if owner == nil {
		return nil
	}
	if !m.holdsReceipt(owner, path) {
		return nil
	}
	runID := handoffOwnerRunID(spec)
	if owner.runID != runID || owner.nodeID != nodeID || !m.manages(path, runID) {
		return fmt.Errorf("handoff directory %q is prepared for run %q on node %q, not %q on %q",
			path, owner.runID, owner.nodeID, runID, nodeID)
	}
	now := m.now().UTC()
	record := retentionRecord{
		RunID: runID, NodeID: nodeID, Directory: path,
		AdmittedAt: m.admissionOf(runID, nodeID, path, now),
		RetainedAt: now, RetainUntil: now.Add(m.retention),
		Published: published, Succeeded: succeeded,
	}
	// The record is written before the bound runs, so a run whose trimming
	// fails is still an accounted directory that expires on schedule rather
	// than residue nothing sweeps.
	if err := m.writeRecord(record); err != nil {
		return err
	}
	// Identity first, and only then the trim. Trimming before the check means
	// reading a directory the workload may have unlinked, which on Linux fails
	// outright -- os.Root reaches the handle through /proc/self/fd/N and that
	// path stops resolving the moment the directory is gone -- so the one case
	// worth detecting would turn into an attempt failure.
	if anomaly := m.pinnedDirectoryDrift(owner, runID, path); anomaly != "" {
		record.BoundAnomaly = anomaly
		// Best effort, and deliberately so: the anomaly is diagnosis, and a
		// node that cannot write its own diagnosis has not changed what the run
		// did. Failing the attempt here would turn "this node could not write
		// its state directory" into "this run failed", which is a worse lie
		// than the one this whole function exists to stop telling.
		if err := m.writeRecord(record); err != nil {
			m.log("agent: run %s: record that the per-run bound did not reach %q: %v", runID, path, err)
		}
		return nil
	}
	return m.enforceRunBound(owner.run, runID)
}

// pinnedDirectoryDrift reports that the run's name no longer leads to the
// directory preparation pinned, which is the one condition under which the
// per-run bound must not trim anything at all.
//
// The bound used to be a silent no-op against the workload most likely to blow
// through it. A job that does `rm -rf $handoff && mkdir $handoff` leaves the
// handle preparation pinned pointing at an unlinked inode: finish trimmed an
// inode with no links, removed nothing, reported no error, and the files that
// should have been bounded stayed on the node while the node believed they fit.
// Every byte of that run is then mischarged for as long as it is retained,
// which is exactly the number #494's node budget is about to be built on.
//
// Neither directory is trimmed on a mismatch. Not the replacement: the only
// thing that could prove a directory that appeared after preparation is this
// run's own is the ownership marker, and the marker is a file inside a
// directory that shares this agent's OS identity, so a workload can write any
// marker it likes, including a copy of another run's. Not the pinned handle
// either: once the name has moved on, that handle is either an unlinked inode
// -- where reading it fails on Linux and achieves nothing anywhere -- or a
// directory sitting somewhere this run's path lease says nothing about. So the
// conservative half is what lands: the drift is named in the log and recorded
// on the retention record, and nothing is deleted. The bound is no longer
// silently skipped; it is openly not met.
//
// The ownership marker is therefore still read exactly once, at preparation, as
// docs/contracts/run-execution-context.md says.
//
// It never fails the attempt. A workload that destroyed its own handoff
// directory has not failed its run.
func (m *handoffManager) pinnedDirectoryDrift(owner *handoffOwnership, runID, path string) handoffRecordAnomaly {
	if owner.prepared == nil {
		m.log("agent: run %s: %q was prepared without a recorded identity; the per-run bound is not applied", runID, path)
		return handoffBoundDirectoryUnverifiable
	}
	root, err := openPrivateHandoffDirectory(m.root)
	if err != nil {
		m.log("agent: run %s: open the handoff root to check %q before trimming: %v", runID, path, err)
		return handoffBoundDirectoryUnverifiable
	}
	defer root.Close()
	current, err := root.Lstat(runID)
	if errors.Is(err, fs.ErrNotExist) {
		m.log("agent: run %s: %q no longer exists; the per-run bound trimmed nothing, and whatever the workload removed is not accounted for",
			runID, path)
		return handoffBoundDirectoryReplaced
	}
	if err != nil {
		m.log("agent: run %s: inspect %q before trimming: %v", runID, path, err)
		return handoffBoundDirectoryUnverifiable
	}
	if os.SameFile(owner.prepared, current) {
		return ""
	}
	m.log("agent: run %s: %q is no longer the directory preparation pinned (it is now %s); the per-run bound trimmed nothing and both directories are left untouched",
		runID, path, current.Mode())
	return handoffBoundDirectoryReplaced
}

// admissionOf carries this run's admission across from the record preparation
// wrote, so finishing updates one record rather than replacing it with a
// different one for the same run.
//
// Identity has to survive that update. When a run was admitted is how a later
// pass tells an in-flight run from residue, and rewriting it at finish would
// make every finished run look freshly admitted. The caller holds this path's
// lease, so nothing else is writing this record while it is read.
//
// A missing or mismatched admission is not a failure: it is an older agent's
// record, or one something removed mid-run, and the run's own finish time is
// the honest answer then. It is logged because the agent removing its own
// record mid-run is not an ordinary thing to have happened.
func (m *handoffManager) admissionOf(runID, nodeID, path string, now time.Time) time.Time {
	if strings.TrimSpace(m.stateRoot) == "" {
		return now
	}
	current, err := m.readRecord(filepath.Join(m.recordRoot(), recordComponent(runID)))
	switch {
	case err != nil:
		m.log("agent: run %s: no admission record to update at finish (%v); recording this run as admitted when it finished", runID, err)
	case current.RunID != runID || current.NodeID != nodeID || current.Directory != path || current.AdmittedAt.IsZero():
		m.log("agent: run %s: the record at finish is not the one its preparation wrote; recording this run as admitted when it finished", runID)
	default:
		return current.AdmittedAt.UTC()
	}
	return now
}

// readResult reads this run's result document through the same receipt that
// authorizes retention. It is a no-op without that receipt, for the same reason
// finish is: the handle belongs to an acquisition this attempt still holds, and
// a successor attempt's directory is not this one's to read.
func (m *handoffManager) readResult(owner *handoffOwnership, spec contract.JobSpec, nodeID string) attemptResult {
	if owner == nil {
		return attemptResult{skip: contract.ResultUploadSkipAbsent}
	}
	path := filepath.Clean(spec.Execution.HandoffDirectory)
	if !m.holdsReceipt(owner, path) || owner.runID != handoffOwnerRunID(spec) || owner.nodeID != nodeID {
		return attemptResult{skip: contract.ResultUploadSkipAbsent}
	}
	return readHandoffResult(owner.run)
}

// holdsReceipt reports that this receipt is still the live one for this path on
// this manager. It is the single definition of "this attempt still owns the
// directory", so retention and the result read cannot drift apart.
func (m *handoffManager) holdsReceipt(owner *handoffOwnership, path string) bool {
	if owner == nil || owner.lease == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return owner.lease.manager == m && owner.lease.path == path &&
		owner.lease.pathLock.owner == owner.lease && owner.lease.ownership == owner
}

// openRun opens one run's directory as a root, so every operation below is
// relative to a descriptor rather than a pathname that could be replaced
// underneath it. A handoff path that is a symlink is never followed.
func (m *handoffManager) openRun(runID string) (*os.Root, error) {
	if !validRunMailboxSegment(runID) {
		return nil, errors.New("handoff run ID must be one safe component")
	}
	root, err := openPrivateHandoffDirectory(m.root)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	return openHandoffDirectory(root, runID)
}

// openPrivateHandoffDirectory anchors the configured directory in its trusted
// parent. The configured parent may contain OS aliases such as macOS /var;
// the handoff directory itself must be an actual directory, never a symlink.
func openPrivateHandoffDirectory(path string) (*os.Root, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("handoff root must be absolute")
	}
	path = filepath.Clean(path)
	if path == string(filepath.Separator) {
		return os.OpenRoot(path)
	}
	parent, err := os.OpenRoot(filepath.Dir(path))
	if errors.Is(err, fs.ErrNotExist) {
		parent, err = openPrivateHandoffDirectory(filepath.Dir(path))
	}
	if err != nil {
		return nil, err
	}
	defer parent.Close()
	name := filepath.Base(path)
	if err := parent.Mkdir(name, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, err
	}
	root, err := openHandoffDirectory(parent, name)
	if err != nil {
		return nil, err
	}
	if err := root.Chmod(".", 0o700); err != nil {
		root.Close()
		return nil, err
	}
	return root, nil
}

// enforceRunBound keeps one run's retained results inside the per-run bound.
// result.json is never touched: it is the document the retention exists for,
// and a truncated one is worse than none because it still parses as a result.
// Everything else goes largest-first until the run fits, and the run is
// remeasured after every deletion rather than adjusted by arithmetic.
func (m *handoffManager) enforceRunBound(run *os.Root, runID string) error {
	entries, size, err := handoffEntries(run)
	if err != nil {
		return err
	}
	// A result.json that is not a regular file is removed before anything is
	// measured. Leaving it would be the worst of the two outcomes the
	// protection exists to avoid: the alias survives as a result while trimming
	// deletes whatever it pointed at, so what an operator finds is a dangling
	// link named like a verdict.
	for _, entry := range entries {
		if entry.name != handoffResultName || entry.regular {
			continue
		}
		if err := run.RemoveAll(entry.name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("remove an unusable %s for run %q: %w", handoffResultName, runID, err)
		}
		m.log("agent: run %s had a %s that is not a regular file (%s); it is not a result and was removed",
			runID, handoffResultName, entry.mode)
		// Remeasured, not adjusted: the removal changed the directory, and a
		// non-regular result.json may have been a whole subtree.
		if entries, size, err = handoffEntries(run); err != nil {
			return err
		}
		break
	}
	if size <= m.runBytes {
		return nil
	}
	slices.SortFunc(entries, func(left, right handoffEntry) int { return int(right.size - left.size) })
	for _, entry := range entries {
		if size <= m.runBytes {
			break
		}
		if entry.name == handoffMarkerName || entry.name == handoffResultName {
			continue
		}
		if err := run.RemoveAll(entry.name); err != nil {
			return fmt.Errorf("bound retained results for run %q: %w", runID, err)
		}
		m.log("agent: run %s exceeded the %d byte retained-result bound; dropped %q (%d bytes)",
			runID, m.runBytes, entry.name, entry.size)
		// Remeasured, not subtracted: what matters is what is on the node now.
		if _, size, err = handoffEntries(run); err != nil {
			return err
		}
	}
	if size > m.runBytes {
		// Only result.json is left and it is over on its own. Keeping it whole
		// is the deliberate choice: a partial result document is not a result.
		m.log("agent: run %s retains %d bytes, past the %d byte bound, because %s alone exceeds it",
			runID, size, m.runBytes, handoffResultName)
	}
	return nil
}

// collect expires, and then measures. A recorded run whose window has closed
// and which no attempt is holding is removed, together with its record; what is
// left is counted, in both units, and reported.
//
// There is still no node-wide byte budget here and no eviction order: nothing
// this function does after expiry deletes anything. Accounting lands before the
// budget on purpose -- a budget is only as good as the number it enforces, and
// this slice is where that number is made visible and checked while it can
// still cost nothing to be wrong.
//
// It acts only on directories the agent has a record for. A directory under
// the root with no record is someone else's and is never measured or removed.
//
// A record whose directory it cannot remove is retried for as long as the
// failure can clear on its own. The one class that cannot -- the run's name
// holds something that is not a directory -- pauses after a few identical
// refusals (noteExpiryFailure) and resumes by itself the moment a directory is
// back there (liftQuarantine). Neither ever widens what the sweep is willing to
// delete.
func (m *handoffManager) collect() error {
	m.collectMu.Lock()
	defer m.collectMu.Unlock()
	root, err := openPrivateHandoffDirectory(m.root)
	if err != nil {
		return err
	}
	defer root.Close()
	now := m.now().UTC()
	for _, record := range m.loadRecords() {
		if record.Quarantine == "" && (record.RetainUntil.IsZero() || now.Before(record.RetainUntil)) {
			// A record with no deadline is a run that was admitted and has not
			// finished. There is nothing to expire yet, and the accounting
			// pass below still counts what it is writing.
			continue
		}
		m.expireRun(root, record, now)
	}
	// Accounting runs after expiry, on what is left, and changes nothing.
	m.reportNodeAccounting(m.measureNode(root, m.now().UTC()))
	return nil
}

// expireRun decides everything this sweep will decide about one record while
// holding that record's path lease, and writes nothing about it once the lease
// is gone.
//
// Splitting those two used to lose a rerun's results. The sweep released the
// lease and *then* persisted its failure, so a retry that acquired the lease in
// between, ran, and wrote a fresh deadline and verdict had them overwritten by
// the sweep's expired snapshot -- and the next sweep would delete files a run
// had just retained. collectMu does not help: an attempt's finalization writes
// its record before it enters collection at all.
//
// Holding the lease closes the window on one side. The record can still have
// moved on between loadRecords and the lease, so every write below also
// re-reads the record and refuses if it is no longer the one this sweep loaded
// (rewriteRecord).
func (m *handoffManager) expireRun(root *os.Root, record retentionRecord, now time.Time) {
	lease := m.tryCollectLease(record.Directory)
	if lease == nil {
		// An attempt holds this path. It is not the sweep's to touch, and it is
		// not a failure either.
		return
	}
	defer lease.release()
	if handoffSweepRace != nil {
		handoffSweepRace(handoffSweepLeaseAcquired, record)
	}
	// The snapshot was loaded before the lease, so an attempt could have
	// finished in that window and written the run's real terminal facts. Every
	// decision below -- expired or not, quarantined or not, delete or not --
	// runs on what is on disk now, under the lease, not on that snapshot. The
	// sweep deleting results a rerun retained seconds ago is the same bug as
	// the sweep overwriting them.
	current, ok := m.currentRecord(record)
	if !ok {
		return
	}
	record = current
	if record.Quarantine != "" {
		lifted, ok := m.liftQuarantine(root, record)
		if !ok {
			return
		}
		record = lifted
	}
	if record.RetainUntil.IsZero() || now.Before(record.RetainUntil) {
		return
	}
	removed, err := m.removeExpiredRun(root, record, lease)
	if err != nil {
		m.noteExpiryFailure(record, err)
		return
	}
	if removed {
		m.log("agent: retained results for run %s expired and were removed", record.RunID)
	}
}

// liftQuarantine re-checks, once per sweep and with one Lstat, whether the
// condition that paused this record still holds.
//
// Quarantine has to be reversible without a person. The only thing it is ever
// set on is a run name that is structurally not a directory, and that is a
// condition a workload can undo as easily as it created it -- so the sweep
// looks, and the moment a directory is back at the name (or the name is gone
// entirely) the pause lifts and this sweep goes on to expire it normally.
//
// It reports whether the caller should continue with this record.
func (m *handoffManager) liftQuarantine(root *os.Root, record retentionRecord) (retentionRecord, bool) {
	info, err := root.Lstat(record.RunID)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		// Nothing is there any more. Expiry below removes the record.
	case err != nil:
		return record, false
	case !info.IsDir() || info.Mode()&os.ModeSymlink != 0:
		// Still the shape the sweep cannot remove. Say nothing: repeating the
		// refusal every hour is what the quarantine exists to stop.
		return record, false
	}
	lifted := record
	lifted.Quarantine = ""
	lifted.QuarantineDetail = ""
	lifted.QuarantinedAt = time.Time{}
	lifted.ExpiryFailures = 0
	lifted.StructuralRefusals = 0
	m.log("agent: run %s is a directory again at %q; its expiry quarantine is lifted and the sweep resumes",
		record.RunID, record.Directory)
	if !m.rewriteRecord(record, lifted) {
		return record, false
	}
	return lifted, true
}

// noteExpiryFailure records that this sweep could not remove one run, and
// bounds the retry only for the class of failure that repeating cannot fix.
//
// A workload that replaces its own expired run name with a symlink used to buy
// itself an unbounded retry: the sweep refuses to follow the link, logs the
// refusal, and comes back an hour later to refuse it again, every hour, for as
// long as the node runs. Nothing ever decided, and the same line filled the log
// forever. That refusal is structural -- it is byte-for-byte identical on every
// sweep -- so after maxStructuralRefusals of them the record is quarantined,
// reversibly (liftQuarantine).
//
// Everything else is transient and is retried for as long as it keeps failing,
// exactly as it was before quarantine existed. A busy filesystem, an unreadable
// directory, a permission the node regains: four unlucky sweeps -- which an
// attempt's own finalization can trigger back to back, in minutes -- must not
// end the seven-day sweep for that run. The count is still persisted, so being
// stuck is visible even while the sweep keeps trying.
func (m *handoffManager) noteExpiryFailure(record retentionRecord, cause error) {
	if handoffSweepRace != nil {
		handoffSweepRace(handoffSweepFailureRecorded, record)
	}
	updated := record
	updated.ExpiryFailures++
	structural := errors.Is(cause, errHandoffNameNotADirectory)
	if structural {
		updated.StructuralRefusals++
	}
	switch {
	case structural && updated.StructuralRefusals >= maxStructuralRefusals:
		updated.Quarantine = handoffExpiryNameNotADirectory
		updated.QuarantineDetail = boundedQuarantineDetail(cause)
		updated.QuarantinedAt = m.now().UTC()
		m.log("agent: remove expired results for run %s: %v; %d sweeps have found the same shape at %q, so the sweep pauses on it and the name is left exactly as it is until a directory is back there",
			record.RunID, cause, updated.StructuralRefusals, record.Directory)
	case structural:
		m.log("agent: remove expired results for run %s: %v (%d of %d before the sweep pauses on it)",
			record.RunID, cause, updated.StructuralRefusals, maxStructuralRefusals)
	default:
		m.log("agent: remove expired results for run %s: %v (%d consecutive failures; the sweep will try again)",
			record.RunID, cause, updated.ExpiryFailures)
	}
	m.rewriteRecord(record, updated)
}

// rewriteRecord persists a sweep's change to one record only if the record on
// disk is still the one this sweep loaded.
//
// The record it holds is a snapshot taken before the lease, and an attempt that
// finished in between has already written the run's real terminal facts. Those
// facts decide when the directory may be deleted, so writing a stale copy over
// them is how a sweep deletes results a run retained seconds ago. Identity and
// terminal facts are compared; the fields the sweep itself owns are not.
func (m *handoffManager) rewriteRecord(snapshot, updated retentionRecord) bool {
	if strings.TrimSpace(m.stateRoot) == "" {
		return true
	}
	current, err := m.readRecord(filepath.Join(m.recordRoot(), recordComponent(snapshot.RunID)))
	if err != nil {
		m.log("agent: leave run %s's retention record alone: re-reading it failed: %v", snapshot.RunID, err)
		return false
	}
	if !sameRetainedRun(current, snapshot) {
		m.log("agent: leave run %s's retention record alone: it was rewritten while this sweep ran, and the sweep's copy is the older one",
			snapshot.RunID)
		return false
	}
	if err := m.writeRecord(updated); err != nil {
		m.log("agent: update run %s's retention record: %v", snapshot.RunID, err)
		return false
	}
	return true
}

// sameRetainedRun compares the facts an attempt writes and a sweep must never
// invent: who the record is for, when it was admitted, and the terminal verdict
// and window it carries.
//
// An admission with no window and a finished record for the same run are
// therefore *different*, which is the point: the preparation record is a
// legitimate earlier state of that run, and a sweep still holding it has the
// older copy. Refusing its write is the same rule that stopped a sweep
// overwriting a rerun's results, applied to the one new state a record has.
func sameRetainedRun(left, right retentionRecord) bool {
	return left.RunID == right.RunID && left.NodeID == right.NodeID &&
		left.Directory == right.Directory &&
		left.AdmittedAt.Equal(right.AdmittedAt) &&
		left.RetainedAt.Equal(right.RetainedAt) &&
		left.RetainUntil.Equal(right.RetainUntil) &&
		left.Published == right.Published && left.Succeeded == right.Succeeded
}

// handoffSweepRace is a test seam. It runs at the two points in one record's
// sweep where a concurrent attempt used to be able to slip past -- just after
// the path lease is taken, and inside the failure handling -- so those
// interleavings can be staged deterministically instead of raced for. Nothing
// outside a test ever sets it.
var handoffSweepRace func(stage string, record retentionRecord)

const (
	handoffSweepLeaseAcquired   = "lease-acquired"
	handoffSweepFailureRecorded = "failure-recorded"
)

// currentRecord re-reads one record under the path lease and refuses to act on
// anything it cannot read and validate. A record the agent cannot trust is not
// authority to delete a directory, which is the same rule loadRecords applies.
func (m *handoffManager) currentRecord(snapshot retentionRecord) (retentionRecord, bool) {
	if strings.TrimSpace(m.stateRoot) == "" {
		return snapshot, true
	}
	name := recordComponent(snapshot.RunID)
	record, err := m.readRecord(filepath.Join(m.recordRoot(), name))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			m.log("agent: skip run %s this sweep: re-reading its retention record failed: %v", snapshot.RunID, err)
		}
		return retentionRecord{}, false
	}
	if err := validRetentionRecord(record, name, m.root, m.nodeID, m.retention, m.now().UTC()); err != nil {
		m.log("agent: skip run %s this sweep: its retention record is no longer trustworthy: %v", snapshot.RunID, err)
		return retentionRecord{}, false
	}
	if !sameRetainedRun(record, snapshot) {
		m.log("agent: run %s was retained again while this sweep ran; the sweep acts on the newer record",
			snapshot.RunID)
	}
	return record, true
}

// boundedQuarantineDetail keeps one OS error's own words without letting them
// push a record past the size at which it stops being readable at all.
func boundedQuarantineDetail(cause error) string {
	if cause == nil {
		return ""
	}
	detail := cause.Error()
	if len(detail) > maxQuarantineDetailBytes {
		return detail[:maxQuarantineDetailBytes]
	}
	return detail
}

// tryCollectLease reserves only idle paths. It acquires a normal path-lock
// token, and its release hands that token to any attempt arriving meanwhile.
func (m *handoffManager) tryCollectLease(path string) *handoffLease {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.paths[path] != nil {
		return nil
	}
	pathLock := &handoffPathLock{token: make(chan struct{}, 1), refs: 1}
	pathLock.token <- struct{}{}
	<-pathLock.token
	lease := &handoffLease{manager: m, path: path, pathLock: pathLock}
	pathLock.owner = lease
	m.paths[path] = pathLock
	return lease
}

// removeExpiredRun deletes one expired run. Its caller holds the record's path
// lease for the whole call and for the failure accounting afterwards, and it
// re-checks that no attempt holds the path while that lease is held. Selecting
// candidates and then deleting them without it would let an attempt claim the
// path in between and lose its directory to a sweep that decided earlier.
func (m *handoffManager) removeExpiredRun(root *os.Root, record retentionRecord, lease *handoffLease) (bool, error) {
	run, err := openHandoffDirectory(root, record.RunID)
	if errors.Is(err, fs.ErrNotExist) {
		return false, m.removeRecord(record.RunID)
	}
	if err != nil {
		return false, err
	}
	defer run.Close()
	expected, err := run.Stat(".")
	if err != nil {
		return false, err
	}
	m.log("agent: removing expired results for run %s", record.RunID)
	// Recheck ownership and directory identity immediately before deleting.
	m.mu.Lock()
	owned := lease.pathLock.owner == lease && m.paths[record.Directory] == lease.pathLock
	m.mu.Unlock()
	if !owned {
		return false, errors.New("expired handoff lost collector ownership")
	}
	current, err := root.Lstat(record.RunID)
	if err != nil {
		return false, err
	}
	if !os.SameFile(expected, current) {
		return false, errors.New("expired handoff changed directory identity")
	}
	directory, err := run.Open(".")
	if err != nil {
		return false, err
	}
	entries, err := directory.ReadDir(-1)
	directory.Close()
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if err := run.RemoveAll(entry.Name()); err != nil {
			return false, err
		}
	}
	current, err = root.Lstat(record.RunID)
	if err != nil {
		return false, err
	}
	if !os.SameFile(expected, current) {
		return false, errors.New("expired handoff changed directory identity")
	}
	if err := root.Remove(record.RunID); err != nil {
		return false, err
	}
	return true, m.removeRecord(record.RunID)
}

func (m *handoffManager) manages(path, runID string) bool {
	return path == filepath.Join(m.root, runID)
}

type handoffEntry struct {
	name    string
	size    int64
	regular bool
	mode    os.FileMode
}

// handoffEntries measures one retained run's direct children through its
// opened directory, which is the granularity the per-run bound trims at.
//
// Sizes are logical bytes of regular files. A hard-linked file is charged once
// per link, because this is the unit the per-run bound trims in: dropping one
// name recovers nothing if another still holds the inode, and a bound that
// assumed otherwise would stop trimming while the run was still over. Cross-run
// inode identity is the node pass's (handoff_accounting.go), which is the only
// place that sees a whole root at once. A symlink is never followed and
// contributes nothing.
func handoffEntries(run *os.Root) ([]handoffEntry, int64, error) {
	directory, err := run.Open(".")
	if err != nil {
		return nil, 0, err
	}
	defer directory.Close()
	children, err := directory.ReadDir(-1)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, 0, fmt.Errorf("read handoff directory: %w", err)
	}
	entries := make([]handoffEntry, 0, len(children))
	var total int64
	for _, child := range children {
		info, err := run.Lstat(child.Name())
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, 0, fmt.Errorf("inspect %q: %w", child.Name(), err)
		}
		size, err := measureEntry(run, child.Name(), info)
		if err != nil {
			return nil, 0, err
		}
		entries = append(entries, handoffEntry{
			name: child.Name(), size: size,
			regular: info.Mode().IsRegular(), mode: info.Mode(),
		})
		total += size
	}
	return entries, total, nil
}

// measureEntry sums the logical length of the regular files under one entry,
// reached only through the run's own directory handle.
//
// It used to recurse into subdirectories, which made how deep it went the
// workload's decision. It now walks with an explicit stack (walkHandoffTree),
// and passes no file identity: the per-run bound trims names and charges a
// hard-linked file once per link, which is what part 1 states its unit to be.
// Cross-run identity belongs to the node pass, which sees a whole root.
func measureEntry(run *os.Root, name string, info os.FileInfo) (int64, error) {
	tally, err := walkHandoffTree(run, name, info, nil)
	return tally.logical, err
}

func handoffOwnerRunID(spec contract.JobSpec) string {
	if owner := strings.TrimSpace(spec.Labels["handoff_owner_run_id"]); owner != "" {
		return owner
	}
	return strings.TrimSpace(spec.Labels["run_id"])
}

func handoffHasFiles(run *os.Root) (bool, error) {
	directory, err := run.Open(".")
	if err != nil {
		return false, err
	}
	defer directory.Close()
	entries, err := directory.ReadDir(-1)
	if err != nil {
		return false, fmt.Errorf("read handoff directory: %w", err)
	}
	for _, entry := range entries {
		if entry.Name() != handoffMarkerName {
			return true, nil
		}
	}
	return false, nil
}

// handoffMarkerOpenRace is a test seam. It runs in the window between the
// marker's Lstat and the open that must not trust it, so the non-blocking open
// and the post-open identity check can be proved by a swap that really happens
// there rather than by one planted before the Lstat -- which the mode check
// alone already refuses, and which therefore proves nothing about either guard.
// Nothing outside a test ever sets it.
var handoffMarkerOpenRace func()

// readHandoffMarker is bounded, refuses anything that is not a regular file,
// and never follows a link. It runs only at preparation.
func readHandoffMarker(run *os.Root) (handoffMarker, bool, error) {
	info, err := run.Lstat(handoffMarkerName)
	if errors.Is(err, fs.ErrNotExist) {
		return handoffMarker{}, false, nil
	}
	if err != nil {
		return handoffMarker{}, false, fmt.Errorf("inspect handoff marker: %w", err)
	}
	if !info.Mode().IsRegular() {
		return handoffMarker{}, false, fmt.Errorf("handoff marker is not a regular file")
	}
	if handoffMarkerOpenRace != nil {
		handoffMarkerOpenRace()
	}
	file, err := openHandoffFile(run, handoffMarkerName, os.O_RDONLY|noFollowOpenFlag|runMailboxNonBlockingOpen, 0)
	if err != nil {
		return handoffMarker{}, false, fmt.Errorf("read handoff marker: %w", err)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return handoffMarker{}, false, err
	}
	if !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return handoffMarker{}, false, errors.New("handoff marker changed identity while opening")
	}

	payload, err := io.ReadAll(io.LimitReader(file, maxHandoffMarkerBytes+1))
	if err != nil {
		return handoffMarker{}, false, fmt.Errorf("read handoff marker: %w", err)
	}
	if len(payload) > maxHandoffMarkerBytes {
		return handoffMarker{}, false, fmt.Errorf("handoff marker exceeds %d bytes", maxHandoffMarkerBytes)
	}
	var marker handoffMarker
	if err := json.Unmarshal(payload, &marker); err != nil || marker.RunID == "" || marker.NodeID == "" {
		return handoffMarker{}, false, fmt.Errorf("handoff marker is invalid")
	}
	return marker, true, nil
}

// writeHandoffMarker replaces the name rather than writing through it, so a
// link planted there is destroyed instead of truncating its target.
func writeHandoffMarker(run *os.Root, marker handoffMarker) error {
	payload, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("encode handoff marker: %w", err)
	}
	if err := run.Remove(handoffMarkerName); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("replace handoff marker: %w", err)
	}
	file, err := openHandoffFile(run, handoffMarkerName, os.O_WRONLY|os.O_CREATE|os.O_EXCL|noFollowOpenFlag, 0o600)
	if err != nil {
		return fmt.Errorf("write handoff marker: %w", err)
	}
	if _, err := file.Write(payload); err != nil {
		file.Close()
		return fmt.Errorf("write handoff marker: %w", err)
	}
	return file.Close()
}
