//go:build darwin || linux

// Package managedroot owns the descriptor-relative filesystem boundary for
// process-mode service jobs. Deletion callers provide authority facts only;
// every filesystem component is derived from Config.Root and encoded IDs.
package managedroot

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

const (
	RootManifestName      = "root-manifest"
	OwnershipManifestName = "ownership-manifest"
)

var (
	ErrUnsafeRoot           = errors.New("managed root is unsafe")
	ErrInvalidID            = errors.New("managed root identifier is invalid")
	ErrRootManifest         = errors.New("root manifest is missing or corrupt")
	ErrRootManifestMismatch = errors.New("root manifest does not match removal authority")
	ErrOwnershipManifest    = errors.New("service ownership manifest is missing or corrupt")
	ErrRemovalGeneration    = errors.New("service removal generation does not match ownership manifest")
	ErrRemovalRecord        = errors.New("removal record is missing, corrupt, or conflicting")
	ErrProcessTreeNotReaped = errors.New("service process tree has not been positively reaped")
	ErrStaleBootSession     = errors.New("boot session is no longer active")
	ErrSymlink              = errors.New("symbolic link in managed ancestry")
	ErrMountBoundary        = errors.New("filesystem or mount boundary in managed ancestry")
	ErrConcurrentMutation   = errors.New("managed tree changed during descriptor-relative traversal")
	ErrUnsafeLayout         = errors.New("managed root layout is unsafe")
)

type Config struct {
	// Root is the locally configured wefty state root. It is never accepted
	// from a removal directive and must be an absolute, non-home directory.
	Root          string
	NodeID        string
	BootSessionID string

	// FaultInjector is a test seam for simulating process death at durable
	// phase boundaries. Production callers leave it nil.
	FaultInjector func(Checkpoint) error
}

type RootManifest struct {
	Version        int    `json:"version"`
	RootInstanceID string `json:"root_instance_id"`
	NodeID         string `json:"node_id"`
}

type OwnershipManifest struct {
	Version           int    `json:"version"`
	JobID             string `json:"job_id"`
	RemovalGeneration uint64 `json:"removal_generation"`
}

type ServicePaths struct {
	Root     string
	Data     string
	Attempts string
	Runtime  string
}

// Removal carries authority facts, never a path. ProcessTreeReaped is an
// explicit precondition supplied by the guardian/process owner; no filesystem
// phase begins until it is true.
type Removal struct {
	JobID             string
	Generation        uint64
	RootInstanceID    string
	CleanupFence      string
	BootSessionID     string
	ProcessTreeReaped bool
}

type Checkpoint string

const (
	CheckpointBeforeValidate        Checkpoint = "before-validate"
	CheckpointAfterValidate         Checkpoint = "after-validate"
	CheckpointBeforeRecord          Checkpoint = "before-record"
	CheckpointAfterRecord           Checkpoint = "after-record"
	CheckpointBeforeQuarantine      Checkpoint = "before-quarantine"
	CheckpointAfterQuarantineRename Checkpoint = "after-quarantine-rename"
	CheckpointAfterQuarantine       Checkpoint = "after-quarantine"
	CheckpointBeforeDelete          Checkpoint = "before-delete"
	CheckpointAfterDelete           Checkpoint = "after-delete"
	CheckpointBeforeVerify          Checkpoint = "before-verify"
	CheckpointAfterVerify           Checkpoint = "after-verify"
	CheckpointBeforeComplete        Checkpoint = "before-complete"
	CheckpointAfterComplete         Checkpoint = "after-complete"
)

var deletionCheckpoints = []Checkpoint{
	CheckpointBeforeValidate,
	CheckpointAfterValidate,
	CheckpointBeforeRecord,
	CheckpointAfterRecord,
	CheckpointBeforeQuarantine,
	CheckpointAfterQuarantineRename,
	CheckpointAfterQuarantine,
	CheckpointBeforeDelete,
	CheckpointAfterDelete,
	CheckpointBeforeVerify,
	CheckpointAfterVerify,
	CheckpointBeforeComplete,
	CheckpointAfterComplete,
}

// EncodeID maps an arbitrary non-empty node or job ID to one bounded path
// component. The original ID remains in the corresponding manifest and is
// never decoded from a filesystem name.
func EncodeID(id string) string {
	digest := sha256.Sum256([]byte(id))
	return "sha256-" + hex.EncodeToString(digest[:])
}

func validateID(kind, id string) error {
	if strings.TrimSpace(id) == "" {
		return fmt.Errorf("%w: %s ID is empty", ErrInvalidID, kind)
	}
	return nil
}

func validateComponent(name string) error {
	if name == "" || name == "." || name == ".." || filepath.Base(name) != name || strings.ContainsAny(name, `/\\`) {
		return fmt.Errorf("%w: invalid derived component %q", ErrUnsafeLayout, name)
	}
	return nil
}
