package agent

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const capabilityRevisionFloorVersion = 1

// capabilityRevisionFloorState is the node-local durable record of the highest
// capability revision this node has ever published.
type capabilityRevisionFloorState struct {
	Version  int   `json:"version"`
	Revision int64 `json:"capability_revision"`
}

// capabilityRevisionFloor keeps a node's published capability revision
// monotonic across agent restarts. L1 scopes revision monotonicity to one boot
// session and replaces the node row outright for a new one, so without this
// floor a restarted agent republishes revision 1 and an operator sees the
// revision move backwards on a machine that never rebooted.
//
// Reads and writes are deliberately asymmetric. A malformed file is a bug the
// operator must see, so it fails agent start; an unreadable file is an
// environment problem that must not brick an otherwise healthy node, so it
// degrades to the per-process counter. Writes are best effort for the same
// reason: an unwritable state directory must never withdraw a capability the
// node genuinely earned, and a persist failure therefore lets the next boot
// session restart the counter.
//
// A nil floor is that degraded, per-process mode.
type capabilityRevisionFloor struct {
	mu        sync.Mutex
	path      string
	persisted int64
	logf      func(string, ...any)
}

// defaultCapabilityRevisionPath keeps the floor node-scoped and outside the
// managed service tree, whose descriptor-relative layout the agent must not add
// entries to. The node ID is hashed because it is operator-supplied text, not a
// path component.
func defaultCapabilityRevisionPath(managedRoot, nodeID string) string {
	if managedRoot == "" || nodeID == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(nodeID))
	return filepath.Join(managedRoot, "capability", "revision-sha256-"+hex.EncodeToString(digest[:])+".json")
}

func loadCapabilityRevisionFloor(path string, logf func(string, ...any)) (*capabilityRevisionFloor, error) {
	if path == "" {
		return nil, nil
	}
	if !filepath.IsAbs(path) {
		return nil, errors.New("agent: capability revision path must be absolute")
	}
	floor := &capabilityRevisionFloor{path: path, logf: logf}
	payload, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return floor, nil
		}
		// A file this process cannot read — a root-owned marker left by an
		// installer, say — is not evidence about capability at all. Refusing to
		// start would brick a node that has nothing to do with OCI, so degrade
		// loudly to the per-process counter instead.
		if logf != nil {
			logf("agent: capability revision floor at %s is unreadable, falling back to a per-process revision: %v", path, err)
		}
		return nil, nil
	}
	var state capabilityRevisionFloorState
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil || decoder.Decode(&struct{}{}) != io.EOF ||
		state.Version != capabilityRevisionFloorVersion || state.Revision < 1 {
		return nil, fmt.Errorf("agent: read capability revision floor: invalid marker at %s", path)
	}
	floor.persisted = state.Revision
	return floor, nil
}

// start returns the first revision a new boot session may publish. It is
// strictly greater than every revision a previous process recorded.
func (floor *capabilityRevisionFloor) start() int64 {
	if floor == nil {
		return 1
	}
	floor.mu.Lock()
	defer floor.mu.Unlock()
	if floor.persisted < 1 {
		return 1
	}
	return floor.persisted + 1
}

// record durably raises the floor. Callers must not hold a capability lock: the
// write fsyncs, and the recorded value is already monotonic, so no reader needs
// to wait on it.
func (floor *capabilityRevisionFloor) record(revision int64) {
	if floor == nil || revision < 1 {
		return
	}
	floor.mu.Lock()
	defer floor.mu.Unlock()
	if revision <= floor.persisted {
		return
	}
	if err := floor.writeLocked(revision); err != nil {
		if floor.logf != nil {
			floor.logf("agent: persist capability revision floor: %v", err)
		}
		return
	}
	floor.persisted = revision
}

func (floor *capabilityRevisionFloor) writeLocked(revision int64) error {
	payload, err := json.Marshal(capabilityRevisionFloorState{
		Version: capabilityRevisionFloorVersion, Revision: revision,
	})
	if err != nil {
		return fmt.Errorf("encode capability revision floor: %w", err)
	}
	directory := filepath.Dir(floor.path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create capability revision directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".wefty-capability-revision-*")
	if err != nil {
		return fmt.Errorf("create capability revision replacement: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = os.Remove(temporaryPath)
	}()
	// World-readable on purpose: doctor and support tooling read this marker,
	// and it carries a counter rather than anything sensitive.
	if err := temporary.Chmod(0o644); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("set capability revision replacement mode: %w", err)
	}
	if _, err := temporary.Write(payload); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write capability revision replacement: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync capability revision replacement: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close capability revision replacement: %w", err)
	}
	// The rename is atomic but the parent directory is deliberately not synced:
	// this marker is best effort, and a power loss that loses the rename costs
	// one restarted counter rather than any correctness property.
	if err := os.Rename(temporaryPath, floor.path); err != nil {
		return fmt.Errorf("replace capability revision marker: %w", err)
	}
	return nil
}
