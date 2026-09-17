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
	root      string
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

type handoffMarker struct {
	RunID       string    `json:"run_id"`
	NodeID      string    `json:"node_id"`
	RetainUntil time.Time `json:"retain_until"`
	// RetainedAt is when this run finished and its retention began. Eviction
	// orders by it, so it is recorded rather than derived from RetainUntil,
	// which a later rerun of the same run would move.
	RetainedAt time.Time `json:"retained_at,omitempty"`
	// Published records that everything the workload reported reached the
	// ledger. A published run's files are a convenience and are evicted first;
	// an unpublished run's files are the only copy of what it did.
	Published bool `json:"published,omitempty"`
	// Succeeded is the run's own verdict, kept because an operator reading a
	// retained directory should not have to infer it from the files.
	Succeeded bool `json:"succeeded,omitempty"`
}

func newHandoffManager(root string, retention time.Duration, logf func(string, ...any)) *handoffManager {
	if strings.TrimSpace(root) == "" {
		root = contract.DefaultHandoffRoot
	}
	return &handoffManager{
		root: filepath.Clean(root), retention: retention,
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
	marker.RetainUntil = now.Add(m.retention)
	marker.RetainedAt = now
	marker.Succeeded = succeeded
	marker.Published = published
	if err := writeHandoffMarker(path, marker); err != nil {
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
	path   string
	marker handoffMarker
	size   int64
}

// cleanupExpired removes direct children of the configured root whose retention
// deadline has elapsed, then evicts until the node's retained results fit the
// root bound. Both halves touch only directories carrying an agent-owned
// marker: anything else under the root is someone else's and is left alone.
func (m *handoffManager) cleanupExpired(except string) error {
	if err := ensurePrivateDirectory(m.root); err != nil {
		return err
	}
	entries, err := os.ReadDir(m.root)
	if err != nil {
		return err
	}
	now := m.now().UTC()
	retained := make([]retainedRun, 0, len(entries))
	var total int64
	for _, entry := range entries {
		path := filepath.Join(m.root, entry.Name())
		if path == except || !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		marker, exists, err := readHandoffMarker(path)
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		if !marker.RetainUntil.IsZero() && !now.Before(marker.RetainUntil) {
			if err := os.RemoveAll(path); err != nil {
				return fmt.Errorf("remove expired handoff directory %q: %w", path, err)
			}
			m.log("agent: retained results for run %s expired and were removed", marker.RunID)
			continue
		}
		_, size, err := handoffEntries(path)
		if err != nil {
			return err
		}
		retained = append(retained, retainedRun{path: path, marker: marker, size: size})
		total += size
	}
	return m.evictToRootBound(retained, total)
}

// evictToRootBound removes whole runs until the node fits. Order is the
// decision: a run whose evidence reached the ledger goes before one whose
// evidence did not, because the ledger still has the first run's story and
// nothing has the second's. Within each group the oldest goes first.
//
// This is the agent's own bound. Cache-pressure rules elsewhere -- the OCI
// image cache, a node running out of disk -- act on their own resources and are
// unaffected by it; where they and this bound both apply to the same node, each
// enforces its own budget and neither defers to the other.
func (m *handoffManager) evictToRootBound(retained []retainedRun, total int64) error {
	if total <= m.rootBytes {
		return nil
	}
	slices.SortFunc(retained, func(left, right retainedRun) int {
		if left.marker.Published != right.marker.Published {
			if left.marker.Published {
				return -1
			}
			return 1
		}
		return left.marker.RetainedAt.Compare(right.marker.RetainedAt)
	})
	for _, run := range retained {
		if total <= m.rootBytes {
			break
		}
		if err := os.RemoveAll(run.path); err != nil {
			return fmt.Errorf("evict retained handoff directory %q: %w", run.path, err)
		}
		m.log("agent: node retained %d bytes of results, past the %d byte bound; evicted run %s (%d bytes, published=%t)",
			total, m.rootBytes, run.marker.RunID, run.size, run.marker.Published)
		total -= run.size
	}
	if total > m.rootBytes {
		m.log("agent: node still retains %d bytes of results after evicting every eligible run", total)
	}
	return nil
}

type handoffEntry struct {
	name string
	size int64
}

// handoffEntries measures one retained run. It walks the directory rather than
// stat-ing it, because a run's results are files it wrote and the sizes are
// what the bounds are about.
func handoffEntries(path string) ([]handoffEntry, int64, error) {
	children, err := os.ReadDir(path)
	if err != nil {
		return nil, 0, fmt.Errorf("read handoff directory %q: %w", path, err)
	}
	entries := make([]handoffEntry, 0, len(children))
	var total int64
	for _, child := range children {
		size, err := handoffEntrySize(filepath.Join(path, child.Name()))
		if err != nil {
			return nil, 0, err
		}
		entries = append(entries, handoffEntry{name: child.Name(), size: size})
		total += size
	}
	return entries, total, nil
}

// handoffEntrySize never follows a symlink and never leaves the entry: a
// workload writes into this directory, and measuring it must not become a way
// to walk the node.
func handoffEntrySize(path string) (int64, error) {
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
		if info.Mode().IsRegular() {
			total += info.Size()
		}
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
