//go:build darwin || linux

package managedroot

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

const (
	agentDirectory      = "agent"
	nodesDirectory      = "nodes"
	servicesDirectory   = "services"
	quarantineDirectory = "quarantine"
	removalsDirectory   = "removals"
	stagingDirectory    = "staging"
	activeBootName      = "active-boot-session"
)

type removalPhase string

const (
	removalPrepared    removalPhase = "prepared"
	removalQuarantined removalPhase = "quarantined"
	removalComplete    removalPhase = "complete"
)

type removalRecord struct {
	Version           int          `json:"version"`
	RootInstanceID    string       `json:"root_instance_id"`
	JobID             string       `json:"job_id"`
	Generation        uint64       `json:"generation"`
	CleanupFence      string       `json:"cleanup_fence"`
	ProcessTreeReaped bool         `json:"process_tree_reaped"`
	Phase             removalPhase `json:"phase"`
}

type activeBoot struct {
	Version        int    `json:"version"`
	RootInstanceID string `json:"root_instance_id"`
	BootSessionID  string `json:"boot_session_id"`
}

type mountIdentity struct {
	key string
}

type Manager struct {
	root          string
	nodeID        string
	bootSessionID string
	manifest      RootManifest

	checkpoint      func(Checkpoint) error
	mountIdentityFD func(int) (mountIdentity, error)
	mountIdentityAt func(int, string) (mountIdentity, error)
}

var processLocks sync.Map

func Initialize(config Config) (*Manager, error) {
	manager, err := newManager(config)
	if err != nil {
		return nil, err
	}
	mutex := processLock(manager.lockKey())
	mutex.Lock()
	defer mutex.Unlock()

	nodeFD, rootMount, created, err := manager.openNode(true)
	if err != nil {
		return nil, err
	}
	defer unix.Close(nodeFD)
	if err := lockDescriptor(nodeFD); err != nil {
		return nil, err
	}
	defer unlockDescriptor(nodeFD)

	manifest := RootManifest{}
	if created {
		rootInstanceID, err := randomToken(32)
		if err != nil {
			return nil, fmt.Errorf("generate root instance ID: %w", err)
		}
		manifest = RootManifest{Version: 1, RootInstanceID: rootInstanceID, NodeID: config.NodeID}
		if err := writeJSONAtomicAt(nodeFD, RootManifestName, manifest, false); err != nil {
			return nil, fmt.Errorf("write root manifest: %w", err)
		}
	} else if err := readRootManifestAt(nodeFD, &manifest); err != nil {
		return nil, err
	}
	if manifest.NodeID != config.NodeID {
		return nil, fmt.Errorf("%w: stable node ID is %q, want %q", ErrRootManifest, manifest.NodeID, config.NodeID)
	}
	manager.manifest = manifest
	if err := ensureLayoutDirectories(nodeFD, rootMount); err != nil {
		return nil, err
	}
	if err := manager.activateBootAt(nodeFD); err != nil {
		return nil, err
	}
	return manager, nil
}

func Open(config Config) (*Manager, error) {
	manager, err := newManager(config)
	if err != nil {
		return nil, err
	}
	mutex := processLock(manager.lockKey())
	mutex.Lock()
	defer mutex.Unlock()

	nodeFD, rootMount, _, err := manager.openNode(false)
	if err != nil {
		return nil, err
	}
	defer unix.Close(nodeFD)
	if err := lockDescriptor(nodeFD); err != nil {
		return nil, err
	}
	defer unlockDescriptor(nodeFD)

	var manifest RootManifest
	if err := readRootManifestAt(nodeFD, &manifest); err != nil {
		return nil, err
	}
	if manifest.NodeID != config.NodeID {
		return nil, fmt.Errorf("%w: stable node ID is %q, want %q", ErrRootManifest, manifest.NodeID, config.NodeID)
	}
	manager.manifest = manifest
	if err := verifyLayoutDirectories(nodeFD, rootMount); err != nil {
		return nil, err
	}
	if err := manager.activateBootAt(nodeFD); err != nil {
		return nil, err
	}
	return manager, nil
}

func newManager(config Config) (*Manager, error) {
	root, err := validateRoot(config.Root)
	if err != nil {
		return nil, err
	}
	if err := validateID("node", config.NodeID); err != nil {
		return nil, err
	}
	if err := validateID("boot session", config.BootSessionID); err != nil {
		return nil, err
	}
	return &Manager{
		root: configRootClean(root), nodeID: config.NodeID, bootSessionID: config.BootSessionID,
		checkpoint:      config.FaultInjector,
		mountIdentityFD: platformMountIdentityFD,
		mountIdentityAt: platformMountIdentityAt,
	}, nil
}

func validateRoot(root string) (string, error) {
	if strings.TrimSpace(root) == "" || root == "." || !filepath.IsAbs(root) {
		return "", fmt.Errorf("%w: root must be a non-empty absolute path", ErrUnsafeRoot)
	}
	clean := filepath.Clean(root)
	if clean == string(filepath.Separator) || clean == filepath.VolumeName(clean)+string(filepath.Separator) {
		return "", fmt.Errorf("%w: filesystem root is forbidden", ErrUnsafeRoot)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve user home: %w", err)
	}
	if sameCleanPath(clean, home) {
		return "", fmt.Errorf("%w: user home is forbidden", ErrUnsafeRoot)
	}
	return clean, nil
}

func configRootClean(root string) string { return filepath.Clean(root) }

func sameCleanPath(left, right string) bool {
	return filepath.Clean(left) == filepath.Clean(right)
}

func (m *Manager) Manifest() RootManifest { return m.manifest }

func (m *Manager) BootSessionID() string { return m.bootSessionID }

func (m *Manager) NodePath() string {
	return filepath.Join(m.root, agentDirectory, nodesDirectory, EncodeID(m.nodeID))
}

func (m *Manager) ServicesPath() string {
	return filepath.Join(m.NodePath(), servicesDirectory)
}

func (m *Manager) ServicePaths(jobID string) (ServicePaths, error) {
	if err := validateID("job", jobID); err != nil {
		return ServicePaths{}, err
	}
	root := filepath.Join(m.ServicesPath(), EncodeID(jobID))
	if sameCleanPath(root, m.NodePath()) {
		return ServicePaths{}, fmt.Errorf("%w: service target equals agent root", ErrUnsafeRoot)
	}
	return ServicePaths{
		Root: root, Data: filepath.Join(root, "data"), Attempts: filepath.Join(root, "attempts"), Runtime: filepath.Join(root, "runtime"),
	}, nil
}

func (m *Manager) CreateService(jobID string, generation uint64) (ServicePaths, error) {
	paths, err := m.ServicePaths(jobID)
	if err != nil {
		return ServicePaths{}, err
	}
	err = m.withNodeLock(func(nodeFD int, rootMount mountIdentity) error {
		servicesFD, err := openChildDirectory(nodeFD, servicesDirectory, false, rootMount, m.mountIdentityFD)
		if err != nil {
			return err
		}
		defer unix.Close(servicesFD)
		component := EncodeID(jobID)
		if exists, _, err := entryExistsAt(servicesFD, component); err != nil {
			return err
		} else if exists {
			return m.validateServiceAt(context.Background(), servicesFD, component, rootMount, jobID, generation)
		}

		stagingFD, err := openChildDirectory(nodeFD, stagingDirectory, false, rootMount, m.mountIdentityFD)
		if err != nil {
			return err
		}
		defer unix.Close(stagingFD)
		suffix, err := randomToken(12)
		if err != nil {
			return err
		}
		stagedName := "create-" + component + "-" + suffix
		if err := validateComponent(stagedName); err != nil {
			return err
		}
		if err := unix.Mkdirat(stagingFD, stagedName, 0o700); err != nil {
			return fmt.Errorf("create staged service directory: %w", err)
		}
		cleanupStaged := true
		defer func() {
			if cleanupStaged {
				_ = removeTreeAt(context.Background(), stagingFD, stagedName, rootMount, m.mountIdentityFD, m.mountIdentityAt)
			}
		}()
		serviceFD, err := openChildDirectory(stagingFD, stagedName, false, rootMount, m.mountIdentityFD)
		if err != nil {
			return err
		}
		manifest := OwnershipManifest{Version: 1, JobID: jobID, RemovalGeneration: generation}
		if err := writeJSONAtomicAt(serviceFD, OwnershipManifestName, manifest, false); err != nil {
			unix.Close(serviceFD)
			return fmt.Errorf("write ownership manifest: %w", err)
		}
		for _, name := range []string{"data", "attempts", "runtime"} {
			childFD, err := openChildDirectory(serviceFD, name, true, rootMount, m.mountIdentityFD)
			if err != nil {
				unix.Close(serviceFD)
				return err
			}
			unix.Close(childFD)
		}
		if err := syncDirectory(serviceFD); err != nil {
			unix.Close(serviceFD)
			return err
		}
		unix.Close(serviceFD)
		if err := renameNoReplace(stagingFD, stagedName, servicesFD, component); err != nil {
			return fmt.Errorf("publish service container: %w", err)
		}
		cleanupStaged = false
		if err := syncDirectory(stagingFD); err != nil {
			return err
		}
		return syncDirectory(servicesFD)
	})
	return paths, err
}

func (m *Manager) Remove(ctx context.Context, removal Removal) error {
	if err := validateRemoval(removal); err != nil {
		return err
	}
	return m.withNodeLock(func(nodeFD int, rootMount mountIdentity) error {
		return m.removeLocked(ctx, nodeFD, rootMount, removal)
	})
}

func validateRemoval(removal Removal) error {
	if err := validateID("job", removal.JobID); err != nil {
		return err
	}
	if strings.TrimSpace(removal.RootInstanceID) == "" || strings.TrimSpace(removal.CleanupFence) == "" {
		return fmt.Errorf("%w: root instance ID and cleanup fence are required", ErrRemovalRecord)
	}
	if err := validateID("boot session", removal.BootSessionID); err != nil {
		return err
	}
	if !removal.ProcessTreeReaped {
		return ErrProcessTreeNotReaped
	}
	return nil
}

func (m *Manager) Resume(ctx context.Context) ([]Removal, error) {
	completed := []Removal{}
	err := m.withNodeLock(func(nodeFD int, rootMount mountIdentity) error {
		removalsFD, err := openChildDirectory(nodeFD, removalsDirectory, false, rootMount, m.mountIdentityFD)
		if err != nil {
			return err
		}
		names, err := readDirectoryNames(removalsFD)
		unix.Close(removalsFD)
		if err != nil {
			return err
		}
		sort.Strings(names)
		records := make(map[string]removalRecord, len(names))
		for _, name := range names {
			if !strings.HasSuffix(name, ".json") {
				return fmt.Errorf("%w: unexpected entry %q", ErrRemovalRecord, name)
			}
			removalsFD, err := openChildDirectory(nodeFD, removalsDirectory, false, rootMount, m.mountIdentityFD)
			if err != nil {
				return err
			}
			var record removalRecord
			err = readJSONAt(removalsFD, name, &record)
			unix.Close(removalsFD)
			if err != nil || !validRemovalRecord(record) || removalRecordName(record.JobID, record.Generation) != name {
				return fmt.Errorf("%w: %q", ErrRemovalRecord, name)
			}
			records[quarantineEntryName(record.JobID, record.Generation)] = record
		}
		quarantineFD, err := openChildDirectory(nodeFD, quarantineDirectory, false, rootMount, m.mountIdentityFD)
		if err != nil {
			return err
		}
		quarantineNames, err := readDirectoryNames(quarantineFD)
		unix.Close(quarantineFD)
		if err != nil {
			return err
		}
		for _, name := range quarantineNames {
			record, exists := records[name]
			if !exists || record.Phase == removalComplete {
				return fmt.Errorf("%w: quarantine entry %q has no active record", ErrRemovalRecord, name)
			}
		}
		for _, name := range names {
			removalsFD, err := openChildDirectory(nodeFD, removalsDirectory, false, rootMount, m.mountIdentityFD)
			if err != nil {
				return err
			}
			var record removalRecord
			err = readJSONAt(removalsFD, name, &record)
			unix.Close(removalsFD)
			if err != nil {
				return err
			}
			removal := Removal{
				JobID: record.JobID, Generation: record.Generation, RootInstanceID: record.RootInstanceID,
				CleanupFence: record.CleanupFence, BootSessionID: m.bootSessionID, ProcessTreeReaped: record.ProcessTreeReaped,
			}
			if err := m.removeLocked(ctx, nodeFD, rootMount, removal); err != nil {
				return err
			}
			completed = append(completed, removal)
		}
		return nil
	})
	return completed, err
}

func (m *Manager) removeLocked(ctx context.Context, nodeFD int, rootMount mountIdentity, removal Removal) error {
	if removal.BootSessionID != m.bootSessionID {
		return ErrStaleBootSession
	}
	if removal.RootInstanceID != m.manifest.RootInstanceID {
		return ErrRootManifestMismatch
	}
	if err := m.verifyActiveBootAt(nodeFD); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	servicesFD, err := openChildDirectory(nodeFD, servicesDirectory, false, rootMount, m.mountIdentityFD)
	if err != nil {
		return err
	}
	defer unix.Close(servicesFD)
	quarantineFD, err := openChildDirectory(nodeFD, quarantineDirectory, false, rootMount, m.mountIdentityFD)
	if err != nil {
		return err
	}
	defer unix.Close(quarantineFD)
	removalsFD, err := openChildDirectory(nodeFD, removalsDirectory, false, rootMount, m.mountIdentityFD)
	if err != nil {
		return err
	}
	defer unix.Close(removalsFD)

	recordName := removalRecordName(removal.JobID, removal.Generation)
	record, exists, err := readRemovalRecordAt(removalsFD, recordName)
	if err != nil {
		return err
	}
	if exists {
		if !recordMatches(record, removal) {
			return ErrRemovalRecord
		}
	} else {
		conflictingGeneration, err := hasRemovalRecordForOtherGeneration(removalsFD, removal.JobID, removal.Generation)
		if err != nil {
			return err
		}
		if conflictingGeneration {
			return ErrRemovalGeneration
		}
		if err := m.hit(CheckpointBeforeValidate); err != nil {
			return err
		}
		if err := m.validateServiceAt(ctx, servicesFD, EncodeID(removal.JobID), rootMount, removal.JobID, removal.Generation); err != nil {
			return err
		}
		if err := m.hit(CheckpointAfterValidate); err != nil {
			return err
		}
		if err := m.hit(CheckpointBeforeRecord); err != nil {
			return err
		}
		record = removalRecord{
			Version: 1, RootInstanceID: removal.RootInstanceID, JobID: removal.JobID, Generation: removal.Generation,
			CleanupFence: removal.CleanupFence, ProcessTreeReaped: true, Phase: removalPrepared,
		}
		if err := writeJSONAtomicAt(removalsFD, recordName, record, false); err != nil {
			return fmt.Errorf("persist removal record: %w", err)
		}
		if err := m.hit(CheckpointAfterRecord); err != nil {
			return err
		}
	}

	serviceName := EncodeID(removal.JobID)
	quarantineName := quarantineEntryName(removal.JobID, removal.Generation)
	if record.Phase == removalComplete {
		return verifyBothAbsent(servicesFD, serviceName, quarantineFD, quarantineName)
	}
	if record.Phase == removalPrepared {
		serviceExists, _, err := entryExistsAt(servicesFD, serviceName)
		if err != nil {
			return err
		}
		quarantineExists, _, err := entryExistsAt(quarantineFD, quarantineName)
		if err != nil {
			return err
		}
		if serviceExists && quarantineExists {
			return fmt.Errorf("%w: service and quarantine entries both exist", ErrRemovalRecord)
		}
		if !serviceExists && !quarantineExists {
			return fmt.Errorf("%w: target disappeared before quarantine was recorded", ErrOwnershipManifest)
		}
		if serviceExists {
			if err := m.validateServiceAt(ctx, servicesFD, serviceName, rootMount, removal.JobID, removal.Generation); err != nil {
				return err
			}
			if err := m.hit(CheckpointBeforeQuarantine); err != nil {
				return err
			}
			if err := renameNoReplace(servicesFD, serviceName, quarantineFD, quarantineName); err != nil {
				return fmt.Errorf("quarantine service container: %w", err)
			}
			if err := syncDirectory(servicesFD); err != nil {
				return err
			}
			if err := syncDirectory(quarantineFD); err != nil {
				return err
			}
			if err := m.hit(CheckpointAfterQuarantineRename); err != nil {
				return err
			}
		}
		if err := m.validateServiceAt(ctx, quarantineFD, quarantineName, rootMount, removal.JobID, removal.Generation); err != nil {
			return err
		}
		record.Phase = removalQuarantined
		if err := writeJSONAtomicAt(removalsFD, recordName, record, true); err != nil {
			return fmt.Errorf("record quarantine: %w", err)
		}
		if err := m.hit(CheckpointAfterQuarantine); err != nil {
			return err
		}
	}

	if record.Phase != removalQuarantined {
		return ErrRemovalRecord
	}
	if serviceExists, _, err := entryExistsAt(servicesFD, serviceName); err != nil {
		return err
	} else if serviceExists {
		return fmt.Errorf("%w: service returned after quarantine", ErrRemovalRecord)
	}
	quarantineExists, _, err := entryExistsAt(quarantineFD, quarantineName)
	if err != nil {
		return err
	}
	if quarantineExists {
		if err := m.validateServiceAt(ctx, quarantineFD, quarantineName, rootMount, removal.JobID, removal.Generation); err != nil {
			return err
		}
		if err := m.hit(CheckpointBeforeDelete); err != nil {
			return err
		}
		if err := removeTreeAt(ctx, quarantineFD, quarantineName, rootMount, m.mountIdentityFD, m.mountIdentityAt); err != nil {
			return err
		}
		if err := syncDirectory(quarantineFD); err != nil {
			return err
		}
		if err := m.hit(CheckpointAfterDelete); err != nil {
			return err
		}
	}
	if err := m.hit(CheckpointBeforeVerify); err != nil {
		return err
	}
	if err := verifyBothAbsent(servicesFD, serviceName, quarantineFD, quarantineName); err != nil {
		return err
	}
	if err := syncDirectory(nodeFD); err != nil {
		return err
	}
	if err := m.hit(CheckpointAfterVerify); err != nil {
		return err
	}
	if err := m.hit(CheckpointBeforeComplete); err != nil {
		return err
	}
	record.Phase = removalComplete
	if err := writeJSONAtomicAt(removalsFD, recordName, record, true); err != nil {
		return fmt.Errorf("record completed removal: %w", err)
	}
	if err := m.hit(CheckpointAfterComplete); err != nil {
		return err
	}
	return nil
}

func (m *Manager) validateServiceAt(ctx context.Context, parentFD int, name string, rootMount mountIdentity, jobID string, generation uint64) error {
	serviceFD, err := openChildDirectory(parentFD, name, false, rootMount, m.mountIdentityFD)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return ErrOwnershipManifest
		}
		return err
	}
	defer unix.Close(serviceFD)
	var manifest OwnershipManifest
	if err := readJSONAt(serviceFD, OwnershipManifestName, &manifest); err != nil {
		return fmt.Errorf("%w: %w", ErrOwnershipManifest, err)
	}
	if manifest.Version != 1 || manifest.JobID == "" {
		return ErrOwnershipManifest
	}
	if manifest.JobID != jobID {
		return fmt.Errorf("%w: service belongs to job %q", ErrOwnershipManifest, manifest.JobID)
	}
	if manifest.RemovalGeneration != generation {
		return fmt.Errorf("%w: service generation is %d, want %d", ErrRemovalGeneration, manifest.RemovalGeneration, generation)
	}
	return preflightTree(ctx, serviceFD, rootMount, m.mountIdentityFD, m.mountIdentityAt)
}

func (m *Manager) withNodeLock(fn func(int, mountIdentity) error) error {
	mutex := processLock(m.lockKey())
	mutex.Lock()
	defer mutex.Unlock()
	nodeFD, rootMount, _, err := m.openNode(false)
	if err != nil {
		return err
	}
	defer unix.Close(nodeFD)
	if err := lockDescriptor(nodeFD); err != nil {
		return err
	}
	defer unlockDescriptor(nodeFD)
	var manifest RootManifest
	if err := readRootManifestAt(nodeFD, &manifest); err != nil {
		return err
	}
	if manifest != m.manifest {
		return ErrRootManifestMismatch
	}
	rootMount, err = m.mountIdentityFD(nodeFD)
	if err != nil {
		return err
	}
	if err := verifyLayoutDirectories(nodeFD, rootMount); err != nil {
		return err
	}
	if err := m.verifyActiveBootAt(nodeFD); err != nil {
		return err
	}
	return fn(nodeFD, rootMount)
}

func (m *Manager) openNode(create bool) (int, mountIdentity, bool, error) {
	rootFD, err := openAbsoluteDirectory(m.root, create)
	if err != nil {
		return -1, mountIdentity{}, false, err
	}
	rootMount, err := m.mountIdentityFD(rootFD)
	if err != nil {
		unix.Close(rootFD)
		return -1, mountIdentity{}, false, err
	}
	currentFD := rootFD
	for _, name := range []string{agentDirectory, nodesDirectory} {
		nextFD, err := openChildDirectory(currentFD, name, create, rootMount, m.mountIdentityFD)
		unix.Close(currentFD)
		if err != nil {
			return -1, mountIdentity{}, false, err
		}
		currentFD = nextFD
	}
	nodeName := EncodeID(m.nodeID)
	existed, _, err := entryExistsAt(currentFD, nodeName)
	if err != nil {
		unix.Close(currentFD)
		return -1, mountIdentity{}, false, err
	}
	nodeFD, err := openChildDirectory(currentFD, nodeName, create, rootMount, m.mountIdentityFD)
	unix.Close(currentFD)
	if err != nil {
		return -1, mountIdentity{}, false, err
	}
	return nodeFD, rootMount, !existed, nil
}

func ensureLayoutDirectories(nodeFD int, rootMount mountIdentity) error {
	for _, name := range []string{servicesDirectory, quarantineDirectory, removalsDirectory, stagingDirectory} {
		fd, err := openChildDirectory(nodeFD, name, true, rootMount, platformMountIdentityFD)
		if err != nil {
			return err
		}
		unix.Close(fd)
	}
	return syncDirectory(nodeFD)
}

func verifyLayoutDirectories(nodeFD int, rootMount mountIdentity) error {
	for _, name := range []string{servicesDirectory, quarantineDirectory, removalsDirectory, stagingDirectory} {
		fd, err := openChildDirectory(nodeFD, name, false, rootMount, platformMountIdentityFD)
		if err != nil {
			return fmt.Errorf("%w: %s: %v", ErrUnsafeLayout, name, err)
		}
		unix.Close(fd)
	}
	return nil
}

func (m *Manager) activateBootAt(nodeFD int) error {
	return writeJSONAtomicAt(nodeFD, activeBootName, activeBoot{
		Version: 1, RootInstanceID: m.manifest.RootInstanceID, BootSessionID: m.bootSessionID,
	}, true)
}

func (m *Manager) verifyActiveBootAt(nodeFD int) error {
	var active activeBoot
	if err := readJSONAt(nodeFD, activeBootName, &active); err != nil {
		return fmt.Errorf("%w: %v", ErrStaleBootSession, err)
	}
	if active.Version != 1 || active.RootInstanceID != m.manifest.RootInstanceID || active.BootSessionID != m.bootSessionID {
		return ErrStaleBootSession
	}
	return nil
}

func (m *Manager) hit(checkpoint Checkpoint) error {
	if m.checkpoint == nil {
		return nil
	}
	return m.checkpoint(checkpoint)
}

func (m *Manager) lockKey() string { return m.NodePath() }

func processLock(key string) *sync.Mutex {
	lock, _ := processLocks.LoadOrStore(key, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

func readRootManifestAt(nodeFD int, manifest *RootManifest) error {
	if err := readJSONAt(nodeFD, RootManifestName, manifest); err != nil {
		return fmt.Errorf("%w: %w", ErrRootManifest, err)
	}
	if manifest.Version != 1 || manifest.RootInstanceID == "" || manifest.NodeID == "" {
		return ErrRootManifest
	}
	return nil
}

func readRemovalRecordAt(removalsFD int, name string) (removalRecord, bool, error) {
	var record removalRecord
	err := readJSONAt(removalsFD, name, &record)
	if errors.Is(err, fs.ErrNotExist) {
		return record, false, nil
	}
	if err != nil || !validRemovalRecord(record) {
		return record, false, fmt.Errorf("%w: %v", ErrRemovalRecord, err)
	}
	return record, true, nil
}

func hasRemovalRecordForOtherGeneration(removalsFD int, jobID string, generation uint64) (bool, error) {
	names, err := readDirectoryNames(removalsFD)
	if err != nil {
		return false, err
	}
	prefix := EncodeID(jobID) + ".g-"
	for _, name := range names {
		if name != removalRecordName(jobID, generation) && strings.HasPrefix(name, prefix) && strings.HasSuffix(name, ".json") {
			var record removalRecord
			if err := readJSONAt(removalsFD, name, &record); err != nil || !validRemovalRecord(record) {
				return false, ErrRemovalRecord
			}
			if record.JobID == jobID {
				return true, nil
			}
		}
	}
	return false, nil
}

func validRemovalRecord(record removalRecord) bool {
	return record.Version == 1 && record.RootInstanceID != "" && record.JobID != "" && record.CleanupFence != "" && record.ProcessTreeReaped &&
		(record.Phase == removalPrepared || record.Phase == removalQuarantined || record.Phase == removalComplete)
}

func recordMatches(record removalRecord, removal Removal) bool {
	return validRemovalRecord(record) && record.RootInstanceID == removal.RootInstanceID && record.JobID == removal.JobID &&
		record.Generation == removal.Generation && record.CleanupFence == removal.CleanupFence && removal.ProcessTreeReaped
}

func removalRecordName(jobID string, generation uint64) string {
	return EncodeID(jobID) + ".g-" + strconv.FormatUint(generation, 10) + ".json"
}

func quarantineEntryName(jobID string, generation uint64) string {
	return EncodeID(jobID) + ".g-" + strconv.FormatUint(generation, 10)
}

func verifyBothAbsent(firstFD int, firstName string, secondFD int, secondName string) error {
	first, _, err := entryExistsAt(firstFD, firstName)
	if err != nil {
		return err
	}
	second, _, err := entryExistsAt(secondFD, secondName)
	if err != nil {
		return err
	}
	if first || second {
		return fmt.Errorf("%w: deletion target still exists", ErrRemovalRecord)
	}
	return nil
}

func randomToken(bytes int) (string, error) {
	payload := make([]byte, bytes)
	if _, err := rand.Read(payload); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}
