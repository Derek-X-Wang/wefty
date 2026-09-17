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
	// stateRoot is the agent's own directory, outside anything a workload can
	// write. The authority for retention lives under it.
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

	mu    sync.Mutex
	paths map[string]*handoffPathLock
	// prepared names the paths this agent prepared and still owns. Terminal
	// recording reads it instead of the marker inside the handoff directory,
	// so a workload cannot decide what its own run's retention says.
	prepared map[string]handoffOwnership

	// collectMu serializes collection with itself. Two sweeps racing would
	// each select candidates the other is deleting.
	collectMu sync.Mutex
}

type handoffOwnership struct {
	runID  string
	nodeID string
}

type handoffPathLock struct {
	token chan struct{}
	refs  int
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
		root: filepath.Clean(root), stateRoot: strings.TrimSpace(stateRoot),
		nodeID: strings.TrimSpace(nodeID), retention: retention,
		runBytes: contract.MaxRetainedResultBytes,
		now:      time.Now, logf: logf,
		paths:    make(map[string]*handoffPathLock),
		prepared: make(map[string]handoffOwnership),
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
		m.mu.Lock()
		delete(m.prepared, path)
		m.mu.Unlock()
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
	if err := writeHandoffMarker(path, handoffMarker{
		RunID: runID, NodeID: nodeID, RetainUntil: m.now().UTC().Add(m.retention),
	}); err != nil {
		return err
	}
	// Ownership is agent-held from here. The terminal step reads this, never
	// the marker: by then the workload has had the directory.
	m.mu.Lock()
	m.prepared[path] = handoffOwnership{runID: runID, nodeID: nodeID}
	m.mu.Unlock()
	return nil
}

// ownership reports whether this agent prepared the path and still owns it.
func (m *handoffManager) ownership(path string) (handoffOwnership, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	owner, ok := m.prepared[filepath.Clean(path)]
	return owner, ok
}

// finish retains the run's results. It used to delete the directory outright
// when the attempt succeeded, which meant the one outcome an operator most
// wants to read -- a run that worked -- was the one that left nothing behind.
// Both outcomes are now retained on the same rule and expire on the same
// deadline.
//
// It is a no-op unless this agent prepared the directory and still holds it.
// Terminal state comes from that agent-held ownership, not from a marker the
// workload could have rewritten, and the caller runs this while it still holds
// the path lock so a successor attempt cannot already be writing here.
func (m *handoffManager) finish(spec contract.JobSpec, nodeID string, succeeded, published bool) error {
	path := filepath.Clean(spec.Execution.HandoffDirectory)
	owner, owned := m.ownership(path)
	if !owned {
		return nil
	}
	runID := handoffOwnerRunID(spec)
	if owner.runID != runID || owner.nodeID != nodeID || !m.manages(path, runID) {
		return fmt.Errorf("handoff directory %q is prepared for run %q on node %q, not %q on %q",
			path, owner.runID, owner.nodeID, runID, nodeID)
	}
	now := m.now().UTC()
	if err := m.writeRecord(retentionRecord{
		RunID: runID, NodeID: nodeID, Directory: path,
		RetainedAt: now, RetainUntil: now.Add(m.retention),
		Published: published, Succeeded: succeeded,
	}); err != nil {
		return err
	}
	return m.enforceRunBound(runID)
}

// openRun opens one run's directory as a root, so every operation below is
// relative to a descriptor rather than a pathname that could be replaced
// underneath it. A handoff path that is a symlink is never followed.
func (m *handoffManager) openRun(runID string) (*os.Root, error) {
	root, err := os.OpenRoot(m.root)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	info, err := root.Lstat(runID)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return nil, fmt.Errorf("handoff path %q is not a directory", runID)
	}
	return root.OpenRoot(runID)
}

// enforceRunBound keeps one run's retained results inside the per-run bound.
// result.json is never touched: it is the document the retention exists for,
// and a truncated one is worse than none because it still parses as a result.
// Everything else goes largest-first until the run fits, and the run is
// remeasured after every deletion rather than adjusted by arithmetic.
func (m *handoffManager) enforceRunBound(runID string) error {
	run, err := m.openRun(runID)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	defer run.Close()

	entries, size, err := handoffEntries(run)
	if err != nil {
		return err
	}
	// A result.json that is not a regular file is removed before anything is
	// measured. Leaving it would be the worst of the two outcomes the
	// protection exists to avoid: the alias survives as a result while trimming
	// deletes whatever it pointed at, so what an operator finds is a dangling
	// link named like a verdict.
	for index, entry := range entries {
		if entry.name != handoffResultName || entry.regular {
			continue
		}
		if err := run.Remove(entry.name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("remove an unusable %s for run %q: %w", handoffResultName, runID, err)
		}
		m.log("agent: run %s had a %s that is not a regular file (%s); it is not a result and was removed",
			runID, handoffResultName, entry.mode)
		entries = slices.Delete(entries, index, index+1)
		break
	}
	if entries, size, err = handoffEntries(run); err != nil {
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
		if err := removeRunEntry(run, entry.name); err != nil {
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

// collect expires. That is all it does: a recorded run whose window has closed
// and which no attempt is holding is removed, together with its record.
//
// There is no node-wide byte budget here and no eviction order. Part 1 keeps
// the two rules it can enforce correctly -- a window and a per-run bound -- and
// the node budget, which needs accounting and ordering this layer could not get
// right, is #494.
//
// It acts only on directories the agent has a record for. A directory under
// the root with no record is someone else's and is never measured or removed.
func (m *handoffManager) collect() error {
	m.collectMu.Lock()
	defer m.collectMu.Unlock()
	if err := ensurePrivateDirectory(m.root); err != nil {
		return err
	}
	now := m.now().UTC()
	for _, record := range m.loadRecords() {
		if record.RetainUntil.IsZero() || now.Before(record.RetainUntil) {
			continue
		}
		removed, err := m.removeExpiredRun(record)
		if err != nil {
			m.log("agent: remove expired results for run %s: %v", record.RunID, err)
			continue
		}
		if removed {
			m.log("agent: retained results for run %s expired and were removed", record.RunID)
		}
	}
	return nil
}

// removeExpiredRun deletes one expired run under the path-ownership lock, and
// re-checks that no attempt holds it while that lock is held. Selecting
// candidates and then deleting them without it would let an attempt claim the
// path in between and lose its directory to a sweep that decided earlier.
func (m *handoffManager) removeExpiredRun(record retentionRecord) (bool, error) {
	m.mu.Lock()
	if _, live := m.paths[record.Directory]; live {
		m.mu.Unlock()
		return false, nil
	}
	// Hold a reference for the duration of the delete so no attempt can take
	// the path while it is being removed.
	pathLock := &handoffPathLock{token: make(chan struct{}, 1), refs: 1}
	m.paths[record.Directory] = pathLock
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		if current, ok := m.paths[record.Directory]; ok && current == pathLock {
			delete(m.paths, record.Directory)
		}
		m.mu.Unlock()
	}()

	info, err := os.Lstat(record.Directory)
	if errors.Is(err, fs.ErrNotExist) {
		return false, m.removeRecord(record.RunID)
	}
	if err != nil {
		return false, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		m.log("agent: retained results for run %s are not a directory; leaving them alone", record.RunID)
		return false, nil
	}
	if err := os.RemoveAll(record.Directory); err != nil {
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
// per link, because part 1 does not track inode identity across a run; a
// symlink is never followed and contributes nothing.
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
func measureEntry(run *os.Root, name string, info os.FileInfo) (int64, error) {
	switch {
	case info.Mode().IsRegular():
		return info.Size(), nil
	case !info.IsDir() || info.Mode()&os.ModeSymlink != 0:
		// Symlinks, sockets and devices contribute nothing, and a symlink is
		// never followed: a link into the node is not this run's storage.
		return 0, nil
	}
	child, err := run.OpenRoot(name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	defer child.Close()
	_, total, err := handoffEntries(child)
	return total, err
}

// removeRunEntry removes one direct child through the run's own handle.
// A nonempty directory is removed with its contents, which is the only
// recursive step and is bounded to a child of this run.
func removeRunEntry(run *os.Root, name string) error {
	info, err := run.Lstat(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
		child, err := run.OpenRoot(name)
		if err != nil {
			return err
		}
		entries, _, err := handoffEntries(child)
		if err != nil {
			child.Close()
			return err
		}
		for _, entry := range entries {
			if err := removeRunEntry(child, entry.name); err != nil {
				child.Close()
				return err
			}
		}
		child.Close()
	}
	if err := run.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
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

// readHandoffMarker is bounded, refuses anything that is not a regular file,
// and never follows a link. It runs only at preparation.
func readHandoffMarker(path string) (handoffMarker, bool, error) {
	markerPath := filepath.Join(path, handoffMarkerName)
	info, err := os.Lstat(markerPath)
	if errors.Is(err, fs.ErrNotExist) {
		return handoffMarker{}, false, nil
	}
	if err != nil {
		return handoffMarker{}, false, fmt.Errorf("inspect handoff marker: %w", err)
	}
	if !info.Mode().IsRegular() {
		return handoffMarker{}, false, fmt.Errorf("handoff marker is not a regular file")
	}
	file, err := os.OpenFile(markerPath, os.O_RDONLY|noFollowOpenFlag, 0)
	if err != nil {
		return handoffMarker{}, false, fmt.Errorf("read handoff marker: %w", err)
	}
	defer file.Close()
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
func writeHandoffMarker(path string, marker handoffMarker) error {
	payload, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("encode handoff marker: %w", err)
	}
	markerPath := filepath.Join(path, handoffMarkerName)
	if err := os.Remove(markerPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("replace handoff marker: %w", err)
	}
	file, err := os.OpenFile(markerPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("write handoff marker: %w", err)
	}
	if _, err := file.Write(payload); err != nil {
		file.Close()
		return fmt.Errorf("write handoff marker: %w", err)
	}
	return file.Close()
}
