package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const handoffMarkerName = ".wefty-handoff.json"

type handoffManager struct {
	root      string
	retention time.Duration
	now       func() time.Time
}

type handoffMarker struct {
	RunID       string    `json:"run_id"`
	NodeID      string    `json:"node_id"`
	RetainUntil time.Time `json:"retain_until"`
}

func newHandoffManager(root string, retention time.Duration) *handoffManager {
	if strings.TrimSpace(root) == "" {
		root = contract.DefaultHandoffRoot
	}
	return &handoffManager{root: filepath.Clean(root), retention: retention, now: time.Now}
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

func (m *handoffManager) finish(spec contract.JobSpec, nodeID string, succeeded bool) error {
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
	if succeeded {
		return os.RemoveAll(path)
	}
	marker.RetainUntil = m.now().UTC().Add(m.retention)
	return writeHandoffMarker(path, marker)
}

// cleanupExpired removes only direct children of the configured root that
// carry an agent-owned marker whose retention deadline has elapsed.
func (m *handoffManager) cleanupExpired(except string) error {
	if err := ensurePrivateDirectory(m.root); err != nil {
		return err
	}
	entries, err := os.ReadDir(m.root)
	if err != nil {
		return err
	}
	now := m.now().UTC()
	for _, entry := range entries {
		path := filepath.Join(m.root, entry.Name())
		if path == except || !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		marker, exists, err := readHandoffMarker(path)
		if err != nil {
			return err
		}
		if exists && !marker.RetainUntil.IsZero() && !now.Before(marker.RetainUntil) {
			if err := os.RemoveAll(path); err != nil {
				return fmt.Errorf("remove expired handoff directory %q: %w", path, err)
			}
		}
	}
	return nil
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
