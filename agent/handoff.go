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
		root: filepath.Clean(root), stateRoot: strings.TrimSpace(stateRoot),
		nodeID: strings.TrimSpace(nodeID), retention: retention,
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

func (m *handoffManager) prepare(lease *handoffLease, spec contract.JobSpec, nodeID string) (*handoffOwnership, error) {
	path := filepath.Clean(spec.Execution.HandoffDirectory)
	m.mu.Lock()
	owned := lease != nil && lease.manager == m && lease.path == path && lease.pathLock.owner == lease && lease.ownership == nil
	m.mu.Unlock()
	if !owned {
		return nil, errors.New("handoff preparation requires this attempt's path lock")
	}
	if path == "." || !filepath.IsAbs(path) {
		return nil, errors.New("handoff directory must be an absolute path")
	}
	runID := handoffOwnerRunID(spec)
	if runID == "" || !m.manages(path, runID) {
		directory, err := openPrivateHandoffDirectory(path)
		if err != nil {
			return nil, err
		}
		return nil, directory.Close()
	}
	if !validRunMailboxSegment(runID) {
		return nil, errors.New("handoff run ID must be one safe component")
	}
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
	owner := &handoffOwnership{lease: lease, runID: runID, nodeID: nodeID, run: run}
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
	m.mu.Lock()
	owned := owner.lease.manager == m && owner.lease.path == path && owner.lease.pathLock.owner == owner.lease && owner.lease.ownership == owner
	m.mu.Unlock()
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
	return m.enforceRunBound(owner.run, runID)
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
	for index, entry := range entries {
		if entry.name != handoffResultName || entry.regular {
			continue
		}
		if err := run.RemoveAll(entry.name); err != nil && !errors.Is(err, fs.ErrNotExist) {
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
	root, err := openPrivateHandoffDirectory(m.root)
	if err != nil {
		return err
	}
	defer root.Close()
	now := m.now().UTC()
	for _, record := range m.loadRecords() {
		if record.RetainUntil.IsZero() || now.Before(record.RetainUntil) {
			continue
		}
		removed, err := m.removeExpiredRun(root, record)
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

// removeExpiredRun deletes one expired run under the path-ownership lock, and
// re-checks that no attempt holds it while that lock is held. Selecting
// candidates and then deleting them without it would let an attempt claim the
// path in between and lose its directory to a sweep that decided earlier.
func (m *handoffManager) removeExpiredRun(root *os.Root, record retentionRecord) (bool, error) {
	lease := m.tryCollectLease(record.Directory)
	if lease == nil {
		return false, nil
	}
	defer lease.release()
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
	child, err := openHandoffDirectory(run, name)
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
