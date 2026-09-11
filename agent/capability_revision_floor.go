package agent

import (
	"bytes"
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
// capability revision this node has ever published. It lives beside the durable
// OCI intent marker because that directory is already the node-local operator
// state the agent owns and may write.
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
// A nil floor is the no-durable-state mode: the process starts at revision 1
// and records nothing.
type capabilityRevisionFloor struct {
	mu        sync.Mutex
	path      string
	persisted int64
	logf      func(string, ...any)
}

// loadCapabilityRevisionFloor fails closed on an unreadable or malformed file.
// Silently restarting the counter is the very regression this file prevents, so
// the operator repairs or removes the file rather than losing monotonicity.
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
		return nil, fmt.Errorf("agent: read capability revision floor: %w", err)
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

// record durably raises the floor. It is best effort by design: an unwritable
// state directory must not withdraw a capability the node genuinely earned, so
// the failure is reported and the in-process revision still advances.
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
	temporary, err := os.CreateTemp(directory, ".wefty-capability-revision-*")
	if err != nil {
		return fmt.Errorf("create capability revision replacement: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = os.Remove(temporaryPath)
	}()
	if err := temporary.Chmod(0o600); err != nil {
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
	if err := os.Rename(temporaryPath, floor.path); err != nil {
		return fmt.Errorf("replace capability revision marker: %w", err)
	}
	return nil
}
