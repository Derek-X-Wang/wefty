package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
)

type handoffManager struct {
	root string
	// stateRoot is the agent's own directory, outside anything a workload can
	// write. The authority for retention and eviction lives under it.
	stateRoot string
	retention time.Duration
	// runBytes and rootBytes are fields rather than direct constant reads so a
	// test can prove each bound without writing 64 MiB.
	runBytes  int64
	rootBytes int64
	now       func() time.Time
	logf      func(string, ...any)
	mu        sync.Mutex
	paths     map[string]*handoffPathLock
}

type handoffPathLock struct {
	token chan struct{}
	refs  int
}

// handoffMarker is advisory and lives inside the workload-writable handoff
// directory. Its whole remaining job is to prove to a cold rerun that the files
// it found are its own. Retention, publication and eviction are decided by the
// agent-local record instead (handoff_records.go), because a workload shares
// this agent's OS identity and anything in here is a file it can rewrite.
type handoffMarker struct {
	RunID       string    `json:"run_id"`
	NodeID      string    `json:"node_id"`
	RetainUntil time.Time `json:"retain_until"`
}

func newHandoffManager(root, stateRoot string, retention time.Duration, logf func(string, ...any)) *handoffManager {
	if strings.TrimSpace(root) == "" {
		root = contract.DefaultHandoffRoot
	}
	return &handoffManager{
		root: filepath.Clean(root), stateRoot: strings.TrimSpace(stateRoot), retention: retention,
		runBytes: contract.MaxRetainedResultBytes, rootBytes: contract.MaxRetainedResultRootBytes,
		now: time.Now, logf: logf,
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
// insufficient because finish may remove the directory another attempt uses.
func (m *handoffManager) lock(ctx context.Context, spec contract.JobSpec) (func(), error) {
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
	return func() {
		pathLock.token <- struct{}{}
		m.releasePathReference(path, pathLock)
	}, nil
}

func (m *handoffManager) releasePathReference(path string, pathLock *handoffPathLock) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pathLock.refs--
	if pathLock.refs == 0 {
		delete(m.paths, path)
	}
}

func (m *handoffManager) prepare(spec contract.JobSpec, nodeID string) error {
	path := filepath.Clean(spec.Execution.HandoffDirectory)
	if path == "." || !filepath.IsAbs(path) {
		return fmt.Errorf("handoff directory must be an absolute path")
	}
	if err := ensurePrivateDirectory(path); err != nil {
		return err
	}
	runID := handoffOwnerRunID(spec)
	if runID == "" || !m.manages(path, runID) {
		return nil
	}

	marker, exists, err := readHandoffMarker(path)
	if err != nil {
		return err
	}
	hasFiles, err := handoffHasFiles(path)
	if err != nil {
		return err
	}
	if !exists && hasFiles {
		return fmt.Errorf("handoff directory %q contains unmanaged files", path)
	}
	if exists {
		if marker.RunID != runID {
			return fmt.Errorf("handoff directory %q belongs to run %q, not %q", path, marker.RunID, runID)
		}
		if marker.NodeID != nodeID {
			return fmt.Errorf("handoff directory %q belongs to stable node %q, not %q", path, marker.NodeID, nodeID)
		}
		if hasFiles {
			stableTag := contract.StableNodeTagPrefix + nodeID
			if !slices.Contains(spec.RoutingTags, stableTag) {
				return fmt.Errorf("cold rerun consuming handoff files must include reserved stable-node tag %q", stableTag)
			}
		}
	}
	return writeHandoffMarker(path, handoffMarker{
		RunID: runID, NodeID: nodeID, RetainUntil: m.now().UTC().Add(m.retention),
	})
}

// finish retains the run's results. It used to delete the directory outright
// when the attempt succeeded, which meant the one outcome an operator most
// wants to read -- a run that worked -- was the one that left nothing behind.
// Both outcomes are now retained on the same rule and expire on the same
// deadline; what the outcome still decides is eviction order when the node
// runs out of room.
func (m *handoffManager) finish(spec contract.JobSpec, nodeID string, succeeded, published bool) error {
	path := filepath.Clean(spec.Execution.HandoffDirectory)
	runID := handoffOwnerRunID(spec)
	if runID == "" || !m.manages(path, runID) {
		return nil
	}
	marker, exists, err := readHandoffMarker(path)
	if err != nil {
		return err
	}
	if !exists || marker.RunID != runID || marker.NodeID != nodeID {
		return fmt.Errorf("handoff directory %q lost its ownership marker", path)
	}
	now := m.now().UTC()
	// The record is the agent's, in the agent's own directory. Nothing is
	// written back into the handoff directory here: the marker there proves
	// ownership to a cold rerun and is advisory, and rewriting it after the
	// workload has had the directory is a write into a tree the workload
	// controls.
	if err := m.writeRecord(retentionRecord{
		RunID: runID, NodeID: nodeID, Directory: path,
		RetainedAt: now, RetainUntil: now.Add(m.retention),
		Published: published, Succeeded: succeeded,
	}); err != nil {
		return err
	}
	return m.enforceRunBound(path, runID)
}

// enforceRunBound keeps one run's retained results inside the per-run bound.
// result.json is never touched: it is the document the retention exists for,
// and a truncated one is worse than none because it still parses as a result.
// Everything else goes largest-first until the run fits.
func (m *handoffManager) enforceRunBound(path, runID string) error {
	entries, size, err := handoffEntries(path)
	if err != nil {
		return err
	}
	// A result.json that is not a regular file is removed before anything is
	// measured against the bound. Leaving it would be the worst outcome of the
	// two the protection exists to avoid: the alias survives as a result while
	// trimming deletes whatever it pointed at, so what an operator finds is a
	// dangling link named like a verdict.
	for index, entry := range entries {
		if entry.name != handoffResultName || entry.regular {
			continue
		}
		if err := os.RemoveAll(filepath.Join(path, entry.name)); err != nil {
			return fmt.Errorf("remove an unusable %s for run %q: %w", handoffResultName, runID, err)
		}
		m.log("agent: run %s had a %s that is not a regular file (%s); it is not a result and was removed",
			runID, handoffResultName, entry.mode)
		size -= entry.size
		entries = slices.Delete(entries, index, index+1)
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
		if entry.name == handoffMarkerName {
			continue
		}
		if entry.name == handoffResultName {
			// Protected, and by now guaranteed to be a regular file.
			continue
		}
		if err := os.RemoveAll(filepath.Join(path, entry.name)); err != nil {
			return fmt.Errorf("bound retained results for run %q: %w", runID, err)
		}
		m.log("agent: run %s exceeded the %d byte retained-result bound; dropped %q (%d bytes)",
			runID, m.runBytes, entry.name, entry.size)
		size -= entry.size
	}
	if size > m.runBytes {
		// Only result.json is left and it is over on its own. Keeping it whole
		// is the deliberate choice: a partial result document is not a result.
		m.log("agent: run %s retains %d bytes, past the %d byte bound, because %s alone exceeds it",
			runID, size, m.runBytes, handoffResultName)
	}
	return nil
}

// retainedRun is one run's retained results as the sweep sees them.
type retainedRun struct {
	record retentionRecord
	size   int64
}

// collect expires and evicts. It is the node's whole storage discipline for
// results, and it runs at startup, after every attempt finishes, and on a
// timer -- not only at startup, which is how a long-lived agent used to be
// able to accumulate results without bound.
//
// It acts only on directories the agent has a record for. A directory under
// the root with no record is someone else's and is never measured, never
// evicted and never removed, however full the node is.
func (m *handoffManager) collect() error {
	if err := ensurePrivateDirectory(m.root); err != nil {
		return err
	}
	active := m.activePaths()
	now := m.now().UTC()
	retained := make([]retainedRun, 0, 8)
	var total, activeBytes int64
	for _, record := range m.loadRecords() {
		info, err := os.Lstat(record.Directory)
		if errors.Is(err, fs.ErrNotExist) {
			// The directory is gone; the record has nothing left to describe.
			if err := m.removeRecord(record.RunID); err != nil {
				m.log("agent: remove orphaned retention record for run %s: %v", record.RunID, err)
			}
			continue
		}
		if err != nil {
			m.log("agent: inspect retained results for run %s: %v", record.RunID, err)
			continue
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			m.log("agent: retained results for run %s are not a directory; leaving them alone", record.RunID)
			continue
		}
		size, err := measureRetainedResults(record.Directory)
		if err != nil {
			m.log("agent: measure retained results for run %s: %v", record.RunID, err)
			continue
		}
		if _, live := active[record.Directory]; live {
			// An attempt owns this path right now. It is never expired and
			// never evicted, but its bytes are still the node's bytes.
			activeBytes += size
			total += size
			continue
		}
		if !record.RetainUntil.IsZero() && !now.Before(record.RetainUntil) {
			if err := os.RemoveAll(record.Directory); err != nil {
				return fmt.Errorf("remove expired handoff directory %q: %w", record.Directory, err)
			}
			if err := m.removeRecord(record.RunID); err != nil {
				m.log("agent: remove retention record for expired run %s: %v", record.RunID, err)
			}
			m.log("agent: retained results for run %s expired and were removed", record.RunID)
			continue
		}
		retained = append(retained, retainedRun{record: record, size: size})
		total += size
	}
	return m.evictToRootBound(retained, total, activeBytes)
}

// activePaths is the real answer to "which runs are in flight": the lock
// registry every attempt passes through to own its handoff path. A sweep that
// took the caller's word for it would be one refactor away from deleting a
// directory a running attempt is writing into.
func (m *handoffManager) activePaths() map[string]struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	active := make(map[string]struct{}, len(m.paths))
	for path := range m.paths {
		active[path] = struct{}{}
	}
	return active
}

// evictToRootBound removes whole runs until the node fits. Order is the
// decision: a run whose evidence reached the ledger goes before one whose
// evidence did not, because the ledger still has the first run's story and
// nothing has the second's. Within each group the oldest goes first.
//
// This is the agent's own bound over the process handoff root. Cache-pressure
// rules elsewhere -- the OCI image cache, a node running out of disk -- act on
// their own resources; where they and this bound both apply to one node, each
// enforces its own budget and neither defers to the other.
func (m *handoffManager) evictToRootBound(retained []retainedRun, total, activeBytes int64) error {
	if total <= m.rootBytes {
		return nil
	}
	slices.SortFunc(retained, func(left, right retainedRun) int {
		if left.record.Published != right.record.Published {
			if left.record.Published {
				return -1
			}
			return 1
		}
		return left.record.RetainedAt.Compare(right.record.RetainedAt)
	})
	for _, run := range retained {
		if total <= m.rootBytes {
			break
		}
		if err := os.RemoveAll(run.record.Directory); err != nil {
			return fmt.Errorf("evict retained handoff directory %q: %w", run.record.Directory, err)
		}
		if err := m.removeRecord(run.record.RunID); err != nil {
			m.log("agent: remove retention record for evicted run %s: %v", run.record.RunID, err)
		}
		m.log("agent: node retained %d bytes of results, past the %d byte bound; evicted run %s (%d bytes, published=%t)",
			total, m.rootBytes, run.record.RunID, run.size, run.record.Published)
		// Remeasured rather than subtracted: the snapshot was taken before the
		// removal, and what matters is what is on the node now.
		measured, err := m.measureRetained(retained)
		if err != nil {
			return err
		}
		total = measured + activeBytes
	}
	if total > m.rootBytes {
		// Runs still executing can exceed the budget on their own. Nothing in
		// flight is ever evicted to make room -- that would delete a directory
		// a workload is writing into -- so the node reports the overrun once
		// and collects again when those runs finish.
		m.log("agent: node retains %d bytes of results, past the %d byte bound, with %d bytes still in flight; "+
			"nothing further is evictable until those runs finish", total, m.rootBytes, activeBytes)
	}
	return nil
}

// measureRetained re-walks what is left after a removal.
func (m *handoffManager) measureRetained(retained []retainedRun) (int64, error) {
	var total int64
	for _, run := range retained {
		size, err := measureRetainedResults(run.record.Directory)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, err
		}
		total += size
	}
	return total, nil
}

type handoffEntry struct {
	name    string
	size    int64
	regular bool
	mode    os.FileMode
}

// handoffEntries measures one retained run's direct children, which is the
// granularity the per-run bound trims at.
func handoffEntries(path string) ([]handoffEntry, int64, error) {
	children, err := os.ReadDir(path)
	if err != nil {
		return nil, 0, fmt.Errorf("read handoff directory %q: %w", path, err)
	}
	entries := make([]handoffEntry, 0, len(children))
	var total int64
	seen := map[inodeIdentity]struct{}{}
	for _, child := range children {
		childPath := filepath.Join(path, child.Name())
		info, err := os.Lstat(childPath)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, 0, fmt.Errorf("inspect %q: %w", childPath, err)
		}
		size, err := measureEntry(childPath, seen)
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

func measureRetainedResults(path string) (int64, error) {
	_, total, err := handoffEntries(path)
	return total, err
}

// inodeIdentity is how a hard-linked file is charged once. Two names for one
// inode are one file on the disk, and charging both would evict results that
// are not actually using the space.
type inodeIdentity struct {
	device uint64
	inode  uint64
}

// measureEntry sums the logical length of the regular files under one entry.
// It never follows a symlink: a link into the node is not this run's storage,
// and following one would make measurement a way to walk the filesystem.
func measureEntry(path string, seen map[inodeIdentity]struct{}) (int64, error) {
	var total int64
	err := filepath.WalkDir(path, func(current string, entry fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if !info.Mode().IsRegular() {
			// Directories, symlinks, sockets and devices contribute nothing:
			// the budgets are logical bytes of regular files.
			return nil
		}
		if identity, links, ok := inodeOf(info); ok && links > 1 {
			if _, counted := seen[identity]; counted {
				return nil
			}
			seen[identity] = struct{}{}
		}
		total += info.Size()
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("measure retained results at %q: %w", path, err)
	}
	return total, nil
}

func (m *handoffManager) manages(path, runID string) bool {
	return path == filepath.Join(m.root, runID)
}

func handoffOwnerRunID(spec contract.JobSpec) string {
	if owner := strings.TrimSpace(spec.Labels["handoff_owner_run_id"]); owner != "" {
		return owner
	}
	return strings.TrimSpace(spec.Labels["run_id"])
}

func ensurePrivateDirectory(path string) error {
	info, err := os.Lstat(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		if err := os.MkdirAll(path, 0o700); err != nil {
			return fmt.Errorf("create handoff directory %q: %w", path, err)
		}
	case err != nil:
		return fmt.Errorf("inspect handoff directory %q: %w", path, err)
	case info.Mode()&os.ModeSymlink != 0:
		return fmt.Errorf("handoff directory %q must not be a symbolic link", path)
	case !info.IsDir():
		return fmt.Errorf("handoff directory %q is not a directory", path)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return fmt.Errorf("set handoff directory permissions on %q: %w", path, err)
	}
	return nil
}

func handoffHasFiles(path string) (bool, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return false, fmt.Errorf("read handoff directory %q: %w", path, err)
	}
	for _, entry := range entries {
		if entry.Name() != handoffMarkerName {
			return true, nil
		}
	}
	return false, nil
}

func readHandoffMarker(path string) (handoffMarker, bool, error) {
	payload, err := os.ReadFile(filepath.Join(path, handoffMarkerName))
	if errors.Is(err, fs.ErrNotExist) {
		return handoffMarker{}, false, nil
	}
	if err != nil {
		return handoffMarker{}, false, fmt.Errorf("read handoff marker: %w", err)
	}
	var marker handoffMarker
	if err := json.Unmarshal(payload, &marker); err != nil || marker.RunID == "" || marker.NodeID == "" {
		return handoffMarker{}, false, fmt.Errorf("handoff marker is invalid")
	}
	return marker, true, nil
}

func writeHandoffMarker(path string, marker handoffMarker) error {
	payload, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("encode handoff marker: %w", err)
	}
	markerPath := filepath.Join(path, handoffMarkerName)
	if err := os.WriteFile(markerPath, payload, 0o600); err != nil {
		return fmt.Errorf("write handoff marker: %w", err)
	}
	if err := os.Chmod(markerPath, 0o600); err != nil {
		return fmt.Errorf("set handoff marker permissions: %w", err)
	}
	return nil
}
