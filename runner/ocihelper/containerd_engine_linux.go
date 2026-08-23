//go:build linux

package ocihelper

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	eventtypes "github.com/containerd/containerd/api/events"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	"github.com/containerd/typeurl/v2"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sys/unix"
)

type containerdAttempt struct {
	authority       AttemptAuthority
	resources       ResourceIdentity
	container       containerd.Container
	task            containerd.Task
	terminalReady   chan struct{}
	terminalCode    uint32
	terminalErr     error
	stdout          string
	stderr          string
	oom             bool
	oomCancel       context.CancelFunc
	cancel          context.CancelFunc
	signal          Signal
	signalCause     string
	deleted         bool
	logAcknowledged map[string]uint64
	mu              sync.Mutex
}

type ContainerdEngine struct {
	client   *containerd.Client
	config   NativeEngineConfig
	mu       sync.Mutex
	attempts map[string]*containerdAttempt
}

func NewContainerdEngine(config NativeEngineConfig) (*ContainerdEngine, error) {
	if config.Address == "" {
		config.Address = DefaultContainerdAddress
	}
	if config.RuntimeRoot == "" {
		config.RuntimeRoot = "/var/lib/wefty/oci"
	}
	if config.ContainerdStateRoot == "" {
		config.ContainerdStateRoot = "/run/containerd"
	}
	if config.CgroupRoot == "" {
		config.CgroupRoot = "/sys/fs/cgroup"
	}
	if config.LogSealTimeout <= 0 {
		config.LogSealTimeout = 5 * time.Second
	}
	copyResolver := config.ResolverPath == ""
	copyHosts := config.HostsPath == ""
	if copyResolver {
		config.ResolverPath = filepath.Join(config.RuntimeRoot, "network", "resolv.conf")
	}
	if copyHosts {
		config.HostsPath = filepath.Join(config.RuntimeRoot, "network", "hosts")
	}
	if config.LoggerExecutable == "" {
		path, err := os.Executable()
		if err != nil {
			return nil, err
		}
		config.LoggerExecutable = path
	}
	if !filepath.IsAbs(config.LoggerExecutable) || !filepath.IsAbs(config.RuntimeRoot) {
		return nil, errors.New("containerd engine logger and runtime root must be absolute")
	}
	if filepath.Clean(config.RuntimeRoot) == string(filepath.Separator) {
		return nil, errors.New("containerd engine runtime root must not be filesystem root")
	}
	if err := os.MkdirAll(filepath.Join(config.RuntimeRoot, "network"), 0o700); err != nil {
		return nil, fmt.Errorf("create helper-managed network directory: %w", err)
	}
	if copyResolver {
		if err := copyManagedNetworkFile("/etc/resolv.conf", config.ResolverPath); err != nil {
			return nil, err
		}
	}
	if copyHosts {
		if err := copyManagedNetworkFile("/etc/hosts", config.HostsPath); err != nil {
			return nil, err
		}
	}
	client, err := containerd.New(config.Address, containerd.WithDefaultNamespace(ContainerdNamespace))
	if err != nil {
		return nil, fmt.Errorf("connect containerd: %w", err)
	}
	return &ContainerdEngine{client: client, config: config, attempts: make(map[string]*containerdAttempt)}, nil
}

func copyManagedNetworkFile(source, target string) error {
	payload, err := os.ReadFile(source)
	if err != nil {
		return fmt.Errorf("read helper network source %s: %w", source, err)
	}
	if err := os.WriteFile(target, payload, 0o600); err != nil {
		return fmt.Errorf("write helper-managed network source %s: %w", target, err)
	}
	return nil
}

func (engine *ContainerdEngine) refreshManagedNetworkFiles() error {
	if err := copyManagedNetworkFile("/etc/resolv.conf", engine.config.ResolverPath); err != nil {
		return err
	}
	return copyManagedNetworkFile("/etc/hosts", engine.config.HostsPath)
}

// OpenNativeEngine opens the production Linux containerd adapter.
func OpenNativeEngine(config NativeEngineConfig) (Engine, io.Closer, error) {
	engine, err := NewContainerdEngine(config)
	if err != nil {
		return nil, nil, err
	}
	return engine, engine, nil
}

func (engine *ContainerdEngine) Close() error {
	if engine == nil || engine.client == nil {
		return nil
	}
	return engine.client.Close()
}

func engineContext(ctx context.Context) context.Context {
	return namespaces.WithNamespace(ctx, ContainerdNamespace)
}

func (engine *ContainerdEngine) EnsureImage(ctx context.Context, request EnsureImageRequest, emit func(EnsureImageEvent) error) error {
	// Ticket #142 owns registry delivery. This method is deliberately only a
	// probe-preload verifier: the immutable image must already be local.
	image, evidence, err := engine.localImage(engineContext(ctx), request.Reference, request.Digest)
	if err != nil {
		return err
	}
	if err := image.Unpack(engineContext(ctx), DefaultSnapshotter); err != nil && !errdefs.IsAlreadyExists(err) {
		return &ImageUnavailableError{err: fmt.Errorf("unpack pinned local probe image: %w", err)}
	}
	return emit(EnsureImageEvent{Kind: ImageComplete, Result: &EnsureImageResponse{TopLevelDigest: evidence.TopLevelDigest, PlatformDigest: evidence.PlatformManifestDigest}})
}

func (engine *ContainerdEngine) Run(ctx context.Context, request RunRequest) (_ RunResponse, runErr error) {
	ctx = engineContext(ctx)
	if err := os.MkdirAll(engine.config.RuntimeRoot, 0o700); err != nil {
		return RunResponse{}, err
	}
	lease, err := engine.client.LeasesService().Create(ctx, leases.WithID(request.Resources.LeaseID), leases.WithLabels(request.Resources.Labels))
	if err != nil {
		return RunResponse{}, fmt.Errorf("create attempt lease: %w", err)
	}
	leaseContext := leases.WithLease(ctx, lease.ID)
	created := true
	defer func() {
		if runErr != nil && created {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			runErr = errors.Join(runErr, engine.deleteResources(cleanupCtx, request.Authority, request.Resources))
		}
	}()

	image, evidence, err := engine.localImage(leaseContext, request.Workload.ImageReference, request.Workload.ImageDigest)
	if err != nil {
		return RunResponse{}, err
	}
	if err := image.Unpack(leaseContext, DefaultSnapshotter); err != nil && !errdefs.IsAlreadyExists(err) {
		return RunResponse{}, &ImageUnavailableError{err: fmt.Errorf("unpack pinned local image: %w", err)}
	}
	diffs, err := image.RootFS(leaseContext)
	if err != nil {
		return RunResponse{}, &ImageUnavailableError{err: fmt.Errorf("read pinned image rootfs: %w", err)}
	}
	parent := identity.ChainID(diffs)
	snapshotter := engine.client.SnapshotService(DefaultSnapshotter)
	mounts, err := snapshotter.Prepare(leaseContext, request.Resources.SnapshotID, parent.String(), snapshots.WithLabels(request.Resources.Labels))
	if err != nil {
		return RunResponse{}, fmt.Errorf("prepare overlayfs snapshot: %w", err)
	}
	var document *RuntimeSpecDocument
	err = mount.WithTempMount(leaseContext, mounts, func(root string) error {
		imageConfig, configErr := readImageRuntimeConfig(leaseContext, engine.client.ContentStore(), image)
		if configErr != nil {
			return &ImageUnavailableError{err: fmt.Errorf("read pinned image config: %w", configErr)}
		}
		if err := engine.refreshManagedNetworkFiles(); err != nil {
			return err
		}
		managedSources, managedErr := engine.managedVolumeSources(request)
		if managedErr != nil {
			return managedErr
		}
		operatorSources, translateErr := TranslateOperatorMountSources(request.Workload, IdentityOperatorMountSource)
		if translateErr != nil {
			return translateErr
		}
		document, configErr = BuildRuntimeSpec(leaseContext, RuntimeSpecInput{
			ContainerID: request.Resources.ContainerID, CgroupPath: "/" + request.Resources.CgroupID, RootfsPath: root,
			Image: imageConfig, Workload: request.Workload,
			Guest:        GuestKernelFacts{Architecture: runtime.GOARCH, KernelRelease: kernelRelease()},
			ResolverPath: engine.config.ResolverPath, HostsPath: engine.config.HostsPath,
			ManagedRoot: engine.config.RuntimeRoot, AllowedMountRoots: engine.config.AllowedMountRoots,
			ManagedVolumeSources: managedSources, OperatorMountSources: operatorSources,
		})
		return configErr
	})
	if err != nil {
		return RunResponse{}, err
	}
	defer document.Close()
	spec, err := document.ContainerdSpec()
	if err != nil {
		return RunResponse{}, &RuntimeSpecRejectionError{err: err}
	}
	container, err := engine.client.NewContainer(leaseContext, request.Resources.ContainerID,
		containerd.WithImage(image), containerd.WithRuntime(DefaultRuntimeHandler, nil),
		containerd.WithSnapshotter(DefaultSnapshotter), containerd.WithSnapshot(request.Resources.SnapshotID),
		containerd.WithContainerLabels(request.Resources.Labels),
		func(_ context.Context, _ *containerd.Client, record *containers.Container) error {
			record.Spec = spec
			return nil
		},
	)
	if err != nil {
		return RunResponse{}, fmt.Errorf("create runc v2 container: %w", err)
	}
	logDirectory := filepath.Join(engine.config.RuntimeRoot, "logs", request.Resources.LogSegmentDirectory)
	if err := os.MkdirAll(logDirectory, 0o700); err != nil {
		return RunResponse{}, err
	}
	stdout := filepath.Join(logDirectory, "stdout.frames")
	stderr := filepath.Join(logDirectory, "stderr.frames")
	task, err := container.NewTask(leaseContext, binaryLogCreator(engine.config.LoggerExecutable, stdout, stderr))
	if err != nil {
		return RunResponse{}, fmt.Errorf("create runc v2 task with binary-v2 logs: %w", err)
	}
	attemptContext, attemptCancel := context.WithCancel(leases.WithLease(engineContext(context.Background()), lease.ID))
	wait, err := task.Wait(attemptContext)
	if err != nil {
		attemptCancel()
		return RunResponse{}, fmt.Errorf("register task Wait before Start: %w", err)
	}
	attempt := &containerdAttempt{authority: request.Authority, resources: request.Resources, container: container, task: task, stdout: stdout, stderr: stderr, cancel: attemptCancel, terminalReady: make(chan struct{}), logAcknowledged: make(map[string]uint64)}
	engine.watchOOM(attempt)
	engine.mu.Lock()
	engine.attempts[request.Authority.key()] = attempt
	engine.mu.Unlock()
	go attempt.cacheTerminal(wait)
	if err := document.RevalidateMounts(); err != nil {
		return RunResponse{}, &RuntimeSpecRejectionError{err: err}
	}
	if err := task.Start(leaseContext); err != nil {
		return RunResponse{}, fmt.Errorf("start runc v2 task: %w", err)
	}
	created = false
	return RunResponse{Started: true, Image: &evidence}, nil
}

func (engine *ContainerdEngine) Signal(ctx context.Context, request SignalRequest) error {
	attempt, err := engine.attempt(request.Authority)
	if err != nil {
		return err
	}
	signal := syscall.SIGTERM
	if request.Signal == SignalKILL {
		signal = syscall.SIGKILL
	}
	if err := attempt.task.Kill(engineContext(ctx), signal, containerd.WithKillAll); err != nil {
		return err
	}
	attempt.mu.Lock()
	attempt.signal = request.Signal
	attempt.signalCause = "agent"
	attempt.mu.Unlock()
	return nil
}

func (engine *ContainerdEngine) Watch(ctx context.Context, request WatchRequest, emit func(WatchEvent) error) error {
	attempt, err := engine.attempt(request.Authority)
	if err != nil {
		return err
	}
	tailCtx, cancelTail := context.WithCancel(ctx)
	defer cancelTail()
	tailEvents := make(chan logTailEvent, 8)
	for _, stream := range []struct{ name, path string }{{"stdout", attempt.stdout}, {"stderr", attempt.stderr}} {
		attempt.mu.Lock()
		acknowledged := attempt.logAcknowledged[stream.name]
		attempt.mu.Unlock()
		go tailLogSegment(tailCtx, stream.name, stream.path, attempt.terminalReady, engine.config.LogSealTimeout, acknowledged, tailEvents)
	}
	sealed := 0
	terminal := false
	logIncomplete := false
	terminalReady := attempt.terminalReady
	for !terminal || sealed < 2 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-terminalReady:
			terminal = true
			terminalReady = nil
		case item := <-tailEvents:
			if item.err != nil {
				return item.err
			}
			if item.event.Seal != nil {
				sealed++
				logIncomplete = logIncomplete || !item.event.Seal.Complete
			}
			if item.event.Log != nil && item.event.Log.Gap != nil {
				logIncomplete = true
			}
			if err := emit(item.event); err != nil {
				return err
			}
			if item.event.Log != nil {
				next := item.event.Log.Sequence + 1
				if item.event.Log.Gap != nil {
					next = item.event.Log.Gap.ThroughSequence + 1
				}
				attempt.mu.Lock()
				if next > attempt.logAcknowledged[item.event.Log.Stream] {
					attempt.logAcknowledged[item.event.Log.Stream] = next
				}
				attempt.mu.Unlock()
			}
		}
	}
	attempt.mu.Lock()
	oom, sentSignal, signalCause := attempt.oom || cgroupReportedOOM(engine.config.CgroupRoot, attempt.resources.CgroupID), attempt.signal, attempt.signalCause
	code, waitErr := attempt.terminalCode, attempt.terminalErr
	attempt.mu.Unlock()
	if waitErr != nil {
		return emit(WatchEvent{Kind: WatchComplete, Result: &WatchResponse{RuntimeFailure: waitErr.Error(), OutOfMemory: oom, LogEvidenceIncomplete: logIncomplete}})
	}
	result := &WatchResponse{OutOfMemory: oom, LogEvidenceIncomplete: logIncomplete}
	// containerd ExitStatus exposes only a numeric code. Therefore 137/143 are
	// plain exits unless this helper independently observed successful delivery
	// of the matching signal.
	if (sentSignal == SignalKILL && code == 128+uint32(syscall.SIGKILL)) || (sentSignal == SignalTERM && code == 128+uint32(syscall.SIGTERM)) {
		result.Signal = sentSignal
		result.TerminationCause = signalCause
	} else {
		exitCode := int(code)
		result.ExitCode = &exitCode
	}
	return emit(WatchEvent{Kind: WatchComplete, Result: result})
}

func (attempt *containerdAttempt) cacheTerminal(wait <-chan containerd.ExitStatus) {
	exit, ok := <-wait
	attempt.mu.Lock()
	if !ok {
		attempt.terminalErr = errors.New("containerd task Wait closed without exit status")
	} else {
		attempt.terminalCode, _, attempt.terminalErr = exit.Result()
	}
	attempt.mu.Unlock()
	close(attempt.terminalReady)
}

func (engine *ContainerdEngine) ReapAttemptAsGuardian(ctx context.Context, authority AttemptAuthority) error {
	attempt, err := engine.attempt(authority)
	if err == nil {
		if killErr := attempt.task.Kill(engineContext(ctx), syscall.SIGKILL, containerd.WithKillAll); killErr == nil {
			attempt.mu.Lock()
			attempt.signal = SignalKILL
			attempt.signalCause = "guardian"
			attempt.mu.Unlock()
		} else if !errdefs.IsNotFound(killErr) {
			return killErr
		}
		select {
		case <-attempt.terminalReady:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return engine.ReapAttempt(ctx, authority)
}

func (engine *ContainerdEngine) Delete(ctx context.Context, request DeleteRequest) (DeleteResponse, error) {
	attempt, err := engine.attempt(request.Authority)
	if err != nil && !errdefs.IsNotFound(err) {
		return DeleteResponse{}, err
	}
	resources, identityErr := DeterministicResourceIdentity(request.Authority)
	if identityErr != nil {
		return DeleteResponse{}, identityErr
	}
	if attempt != nil {
		resources = attempt.resources
	}
	cleanupCtx, cancel := context.WithTimeout(engineContext(ctx), 10*time.Second)
	defer cancel()
	var lastErr error
	for {
		deleteErr := engine.deleteResources(cleanupCtx, request.Authority, resources)
		verification, verifyErr := engine.Verify(cleanupCtx, VerifyRequest{Scope: VerifyAttempt, Authority: &request.Authority})
		if deleteErr == nil && verifyErr == nil && verification.Absent {
			return DeleteResponse{Deleted: true}, nil
		}
		lastErr = errors.Join(deleteErr, verifyErr)
		if lastErr == nil {
			lastErr = fmt.Errorf("attempt resources remain after delete: %+v", verification.Inventory)
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-cleanupCtx.Done():
			timer.Stop()
			return DeleteResponse{}, errors.Join(lastErr, cleanupCtx.Err())
		case <-timer.C:
		}
	}
}

func (engine *ContainerdEngine) ReapAttempt(ctx context.Context, authority AttemptAuthority) error {
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		return err
	}
	return engine.deleteResources(engineContext(ctx), authority, resources)
}

func (engine *ContainerdEngine) ReapSession(ctx context.Context, _ SessionIdentity) error {
	engine.mu.Lock()
	attempts := make([]*containerdAttempt, 0, len(engine.attempts))
	for _, attempt := range engine.attempts {
		attempts = append(attempts, attempt)
	}
	engine.mu.Unlock()
	for _, attempt := range attempts {
		if err := attempt.task.Kill(engineContext(ctx), syscall.SIGKILL, containerd.WithKillAll); err == nil {
			attempt.mu.Lock()
			attempt.signal = SignalKILL
			attempt.signalCause = "guardian"
			attempt.mu.Unlock()
		} else if !errdefs.IsNotFound(err) {
			return err
		}
		select {
		case <-attempt.terminalReady:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	// Whole-namespace reaping is safe only under the protocol's one-agent-per-
	// node exclusive helper-session assumption.
	_, err := engine.Sweep(ctx, SweepRequest{})
	return err
}

func (engine *ContainerdEngine) Verify(ctx context.Context, request VerifyRequest) (VerifyResponse, error) {
	ctx = engineContext(ctx)
	inventory, err := engine.inventory(ctx)
	if err != nil {
		return VerifyResponse{}, err
	}
	if request.Scope == VerifyAttempt && request.Authority != nil {
		resources, identityErr := DeterministicResourceIdentity(*request.Authority)
		if identityErr != nil {
			return VerifyResponse{}, identityErr
		}
		inventory = filterInventory(inventory, resources)
	}
	return VerifyResponse{Absent: inventoryEmpty(inventory), Inventory: inventory}, nil
}

func (engine *ContainerdEngine) Sweep(ctx context.Context, _ SweepRequest) (SweepResponse, error) {
	ctx = engineContext(ctx)
	engine.mu.Lock()
	for _, attempt := range engine.attempts {
		if attempt.cancel != nil {
			attempt.cancel()
		}
		attempt.mu.Lock()
		if attempt.oomCancel != nil {
			attempt.oomCancel()
		}
		attempt.mu.Unlock()
	}
	engine.mu.Unlock()
	inventory, err := engine.inventory(ctx)
	if err != nil {
		return SweepResponse{}, err
	}
	containersList, err := engine.client.Containers(ctx)
	if err != nil {
		return SweepResponse{}, err
	}
	prior := map[string]struct{}{}
	attempts := map[string]SweptAttemptAuthority{}
	for _, container := range containersList {
		info, infoErr := container.Info(ctx)
		if infoErr != nil {
			return SweepResponse{}, infoErr
		}
		if !strings.HasPrefix(info.ID, "wefty-container-") {
			continue
		}
		authority, labelErr := authorityFromLabels(info.Labels)
		if labelErr != nil {
			return SweepResponse{}, fmt.Errorf("validate container %s labels: %w", info.ID, labelErr)
		}
		expected, identityErr := DeterministicResourceIdentity(authority)
		if identityErr != nil || expected.ContainerID != info.ID {
			return SweepResponse{}, fmt.Errorf("container %s labels do not match its deterministic identity", info.ID)
		}
		captureSweepAuthority(authority, prior, attempts)
		if task, taskErr := container.Task(ctx, nil); taskErr == nil {
			if _, deleteErr := task.Delete(ctx, containerd.WithProcessKill); deleteErr != nil && !errdefs.IsNotFound(deleteErr) {
				return SweepResponse{}, deleteErr
			}
		} else if !errdefs.IsNotFound(taskErr) {
			return SweepResponse{}, taskErr
		}
		if err := container.Delete(ctx, containerd.WithSnapshotCleanup); err != nil && !errdefs.IsNotFound(err) {
			return SweepResponse{}, err
		}
	}
	snapshotter := engine.client.SnapshotService(DefaultSnapshotter)
	var snapshotNames []string
	if err := snapshotter.Walk(ctx, func(_ context.Context, info snapshots.Info) error {
		if strings.HasPrefix(info.Name, "wefty-snapshot-") {
			authority, labelErr := authorityFromLabels(info.Labels)
			if labelErr != nil {
				return fmt.Errorf("validate snapshot %s labels: %w", info.Name, labelErr)
			}
			expected, identityErr := DeterministicResourceIdentity(authority)
			if identityErr != nil || expected.SnapshotID != info.Name {
				return fmt.Errorf("snapshot %s labels do not match its deterministic identity", info.Name)
			}
			captureSweepAuthority(authority, prior, attempts)
			snapshotNames = append(snapshotNames, info.Name)
		}
		return nil
	}); err != nil {
		return SweepResponse{}, err
	}
	for _, name := range snapshotNames {
		if err := snapshotter.Remove(ctx, name); err != nil && !errdefs.IsNotFound(err) {
			return SweepResponse{}, err
		}
	}
	leaseList, err := engine.client.LeasesService().List(ctx)
	if err != nil {
		return SweepResponse{}, err
	}
	for _, lease := range leaseList {
		if strings.HasPrefix(lease.ID, "wefty-lease-") {
			authority, labelErr := authorityFromLabels(lease.Labels)
			if labelErr != nil {
				return SweepResponse{}, fmt.Errorf("validate lease %s labels: %w", lease.ID, labelErr)
			}
			expected, identityErr := DeterministicResourceIdentity(authority)
			if identityErr != nil || expected.LeaseID != lease.ID {
				return SweepResponse{}, fmt.Errorf("lease %s labels do not match its deterministic identity", lease.ID)
			}
			captureSweepAuthority(authority, prior, attempts)
			if err := engine.client.LeasesService().Delete(ctx, lease); err != nil && !errdefs.IsNotFound(err) {
				return SweepResponse{}, err
			}
		}
	}
	for _, authority := range attempts {
		identity, identityErr := DeterministicResourceIdentity(AttemptAuthority{
			NodeID: authority.NodeID, JobID: authority.JobID, AttemptID: authority.AttemptID,
			FencingToken: authority.FencingToken, BootSessionID: authority.PriorBootSessionID,
			Class: authority.Class, RemovalGeneration: authority.RemovalGeneration,
		})
		if identityErr != nil {
			return SweepResponse{}, identityErr
		}
		for _, path := range []string{
			filepath.Join(engine.config.RuntimeRoot, "logs", identity.LogSegmentDirectory),
			filepath.Join(engine.config.RuntimeRoot, "volumes", identity.HandoffVolumeDirectory),
			filepath.Join(engine.config.RuntimeRoot, "volumes", identity.ServiceVolumeDirectory),
		} {
			if err := os.RemoveAll(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return SweepResponse{}, err
			}
		}
	}
	engine.mu.Lock()
	engine.attempts = make(map[string]*containerdAttempt)
	engine.mu.Unlock()
	priorList := mapKeys(prior)
	attemptList := make([]SweptAttemptAuthority, 0, len(attempts))
	for _, attempt := range attempts {
		attemptList = append(attemptList, attempt)
	}
	sort.Slice(attemptList, func(i, j int) bool { return attemptList[i].AttemptID < attemptList[j].AttemptID })
	return SweepResponse{Removed: inventoryCount(inventory), PriorBootSessionsSeen: priorList, Inventory: inventory, Attempts: attemptList}, nil
}

func (engine *ContainerdEngine) DialAttemptPort(context.Context, DialAttemptPortRequest, io.ReadWriteCloser) error {
	return errEngineUnavailable
}
func (engine *ContainerdEngine) DialHostBridge(context.Context, DialHostBridgeRequest, io.ReadWriteCloser) error {
	return errEngineUnavailable
}

func (engine *ContainerdEngine) attempt(authority AttemptAuthority) (*containerdAttempt, error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	attempt := engine.attempts[authority.key()]
	if attempt == nil {
		return nil, fmt.Errorf("attempt %s: %w", authority.AttemptID, errdefs.ErrNotFound)
	}
	return attempt, nil
}

func (engine *ContainerdEngine) deleteResources(ctx context.Context, authority AttemptAuthority, resources ResourceIdentity) error {
	engine.mu.Lock()
	attempt := engine.attempts[authority.key()]
	engine.mu.Unlock()
	var failures []error
	var container containerd.Container
	if attempt != nil {
		if attempt.cancel != nil {
			attempt.cancel()
		}
		if attempt.oomCancel != nil {
			attempt.oomCancel()
		}
		container = attempt.container
	}
	if container == nil {
		loaded, err := engine.client.LoadContainer(ctx, resources.ContainerID)
		if err == nil {
			container = loaded
		} else if !errdefs.IsNotFound(err) {
			failures = append(failures, err)
		}
	}
	if container != nil {
		if task, err := container.Task(ctx, nil); err == nil {
			if _, err := task.Delete(ctx, containerd.WithProcessKill); err != nil && !errdefs.IsNotFound(err) {
				failures = append(failures, err)
			}
		} else if !errdefs.IsNotFound(err) {
			failures = append(failures, err)
		}
		if err := container.Delete(ctx, containerd.WithSnapshotCleanup); err != nil && !errdefs.IsNotFound(err) {
			failures = append(failures, err)
		}
	} else {
		if err := engine.client.SnapshotService(DefaultSnapshotter).Remove(ctx, resources.SnapshotID); err != nil && !errdefs.IsNotFound(err) {
			failures = append(failures, err)
		}
	}
	lease := leases.Lease{ID: resources.LeaseID}
	if err := engine.client.LeasesService().Delete(ctx, lease); err != nil && !errdefs.IsNotFound(err) {
		failures = append(failures, err)
	}
	for _, path := range []string{
		filepath.Join(engine.config.RuntimeRoot, "logs", resources.LogSegmentDirectory),
		filepath.Join(engine.config.RuntimeRoot, "volumes", resources.HandoffVolumeDirectory),
		filepath.Join(engine.config.RuntimeRoot, "volumes", resources.ServiceVolumeDirectory),
	} {
		if err := os.RemoveAll(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			failures = append(failures, err)
		}
	}
	engine.mu.Lock()
	delete(engine.attempts, authority.key())
	engine.mu.Unlock()
	return errors.Join(failures...)
}

func (engine *ContainerdEngine) localImage(ctx context.Context, reference, immutableDigest string) (containerd.Image, ImageEvidence, error) {
	imagesList, err := engine.client.ListImages(ctx)
	if err != nil {
		return nil, ImageEvidence{}, err
	}
	for _, image := range imagesList {
		if image.Target().Digest.String() != immutableDigest {
			continue
		}
		manifestDescriptor, platform, selectErr := selectedManifest(ctx, engine.client.ContentStore(), image.Target())
		if selectErr != nil {
			return nil, ImageEvidence{}, &ImageUnavailableError{err: selectErr}
		}
		var indexDigest *string
		if images.IsIndexType(image.Target().MediaType) {
			value := image.Target().Digest.String()
			indexDigest = &value
		}
		return image, ImageEvidence{
			SubmittedReference: reference, TopLevelDigest: image.Target().Digest.String(), TopLevelMediaType: image.Target().MediaType,
			IndexDigest: indexDigest, PlatformManifestDigest: manifestDescriptor.Digest.String(),
			Platform:       OCIPlatform{OS: platform.OS, Architecture: platform.Architecture, Variant: platform.Variant},
			RuntimeHandler: DefaultRuntimeHandler, Snapshotter: DefaultSnapshotter,
		}, nil
	}
	return nil, ImageEvidence{}, &ImageUnavailableError{err: fmt.Errorf("pinned local image %s: %w", immutableDigest, errdefs.ErrNotFound)}
}

func selectedManifest(ctx context.Context, store content.Store, target ocispec.Descriptor) (ocispec.Descriptor, ocispec.Platform, error) {
	if images.IsManifestType(target.MediaType) {
		manifest, err := images.Manifest(ctx, store, target, platforms.Default())
		if err != nil {
			return ocispec.Descriptor{}, ocispec.Platform{}, err
		}
		platform, err := images.ConfigPlatform(ctx, store, manifest.Config)
		return target, platform, err
	}
	children, err := images.Children(ctx, store, target)
	if err != nil {
		return ocispec.Descriptor{}, ocispec.Platform{}, err
	}
	matcher := platforms.Default()
	for _, child := range children {
		if child.Platform != nil && matcher.Match(*child.Platform) {
			return child, *child.Platform, nil
		}
	}
	return ocispec.Descriptor{}, ocispec.Platform{}, errors.New("pinned image has no manifest for the runtime platform")
}

func readImageRuntimeConfig(ctx context.Context, store content.Store, image containerd.Image) (ImageRuntimeConfig, error) {
	manifest, err := images.Manifest(ctx, store, image.Target(), platforms.Default())
	if err != nil {
		return ImageRuntimeConfig{}, err
	}
	payload, err := content.ReadBlob(ctx, store, manifest.Config)
	if err != nil {
		return ImageRuntimeConfig{}, err
	}
	var config ocispec.Image
	if err := json.Unmarshal(payload, &config); err != nil {
		return ImageRuntimeConfig{}, err
	}
	return ImageRuntimeConfig{User: config.Config.User, Environment: config.Config.Env, Entrypoint: config.Config.Entrypoint, Command: config.Config.Cmd, WorkingDirectory: config.Config.WorkingDir}, nil
}

func (engine *ContainerdEngine) managedVolumeSources(request RunRequest) (map[ManagedVolumeKind]string, error) {
	result := make(map[ManagedVolumeKind]string)
	for _, volume := range request.Workload.ManagedVolumes {
		name := ""
		switch volume.Kind {
		case ManagedVolumeHandoff:
			name = request.Resources.HandoffVolumeDirectory
		case ManagedVolumeServiceData:
			name = request.Resources.ServiceVolumeDirectory
		case ManagedVolumeLogSegments:
			name = request.Resources.LogSegmentDirectory
		default:
			return nil, fmt.Errorf("managed volume kind %q is unsupported", volume.Kind)
		}
		path := filepath.Join(engine.config.RuntimeRoot, "volumes", name)
		if err := os.MkdirAll(path, 0o700); err != nil {
			return nil, err
		}
		result[volume.Kind] = path
	}
	return result, nil
}

type staticLogIO struct{ config cio.Config }

func (stream *staticLogIO) Config() cio.Config { return stream.config }
func (*staticLogIO) Cancel()                   {}
func (*staticLogIO) Wait()                     {}
func (*staticLogIO) Close() error              { return nil }

func binaryLogCreator(executable, stdout, stderr string) cio.Creator {
	return func(string) (cio.IO, error) {
		uri := &url.URL{Scheme: "binary-v2", Path: executable}
		query := uri.Query()
		query.Set("mode", LoggerInvocationArg)
		query.Set("stdout", stdout)
		query.Set("stderr", stderr)
		uri.RawQuery = query.Encode()
		return &staticLogIO{config: cio.Config{Stdout: uri.String(), Stderr: uri.String()}}, nil
	}
}

type logTailEvent struct {
	event WatchEvent
	err   error
}

func tailLogSegment(ctx context.Context, stream, path string, terminal <-chan struct{}, sealTimeout time.Duration, acknowledged uint64, output chan<- logTailEvent) {
	file, err := waitForLogSegment(ctx, path)
	if err != nil {
		sendTailEvent(ctx, output, logTailEvent{event: incompleteLogSeal(stream, err.Error())})
		return
	}
	defer file.Close()
	var offset int64
	var expected uint64
	terminalSeen := false
	var sealDeadline time.Time
	for {
		if !terminalSeen {
			select {
			case <-terminal:
				terminalSeen = true
				sealDeadline = time.Now().Add(sealTimeout)
			default:
			}
		}
		info, statErr := file.Stat()
		if statErr != nil {
			sendTailEvent(ctx, output, logTailEvent{event: incompleteLogSeal(stream, statErr.Error())})
			return
		}
		available := info.Size() - offset
		if available >= logFrameHeaderBytes {
			header := make([]byte, logFrameHeaderBytes)
			if _, err := file.ReadAt(header, offset); err != nil {
				sendTailEvent(ctx, output, logTailEvent{event: incompleteLogSeal(stream, err.Error())})
				return
			}
			magic := string(header[:4])
			if magic != logFrameMagic && magic != logSealMagic && magic != logIncompleteMagic {
				sendCorruptLogGap(ctx, output, stream, expected, uint64(available))
				sendTailEvent(ctx, output, logTailEvent{event: incompleteLogSeal(stream, "invalid frame magic")})
				return
			}
			sequence := binary.BigEndian.Uint64(header[4:12])
			length := binary.BigEndian.Uint32(header[12:16])
			if length > MaxFrameBytes {
				sendCorruptLogGap(ctx, output, stream, expected, uint64(available))
				sendTailEvent(ctx, output, logTailEvent{event: incompleteLogSeal(stream, "frame exceeds protocol bound")})
				return
			}
			recordBytes := int64(logFrameHeaderBytes) + int64(length)
			if available >= recordBytes {
				payload := make([]byte, length)
				if length > 0 {
					if _, err := file.ReadAt(payload, offset+logFrameHeaderBytes); err != nil {
						sendTailEvent(ctx, output, logTailEvent{event: incompleteLogSeal(stream, err.Error())})
						return
					}
				}
				want := sha256.Sum256(payload)
				if !equalBytes(header[16:], want[:]) {
					sendCorruptLogGap(ctx, output, stream, expected, uint64(recordBytes))
					sendTailEvent(ctx, output, logTailEvent{event: incompleteLogSeal(stream, "frame checksum mismatch")})
					return
				}
				offset += recordBytes
				if sequence != expected {
					sendCorruptLogGap(ctx, output, stream, expected, uint64(recordBytes))
					sendTailEvent(ctx, output, logTailEvent{event: incompleteLogSeal(stream, "frame sequence discontinuity")})
					return
				}
				switch magic {
				case logFrameMagic:
					checksum := sha256.Sum256(payload)
					event := WatchEvent{Kind: WatchProgress, Log: &LogFrame{Stream: stream, Sequence: sequence, Bytes: payload, Checksum: hex.EncodeToString(checksum[:])}}
					if sequence >= acknowledged && !sendTailEvent(ctx, output, logTailEvent{event: event}) {
						return
					}
					expected++
					continue
				case logSealMagic:
					sendTailEvent(ctx, output, logTailEvent{event: WatchEvent{Kind: WatchProgress, Seal: &LogSeal{Stream: stream, Complete: true}}})
					return
				case logIncompleteMagic:
					sendTailEvent(ctx, output, logTailEvent{event: incompleteLogSeal(stream, string(payload))})
					return
				}
			}
		}
		if terminalSeen && !time.Now().Before(sealDeadline) {
			if available > 0 {
				gap := LogGapFrame{ThroughSequence: expected, LostEventCount: 1, LostByteCount: uint64(available), Reason: "logger_source_incomplete"}
				if available >= logFrameHeaderBytes {
					header := make([]byte, logFrameHeaderBytes)
					if _, err := file.ReadAt(header, offset); err == nil {
						declared := binary.BigEndian.Uint32(header[12:16])
						if declared <= MaxFrameBytes {
							gap.LostByteCount = uint64(declared)
						}
					}
				}
				sendTailEvent(ctx, output, logTailEvent{event: WatchEvent{Kind: WatchProgress, Log: &LogFrame{Stream: stream, Sequence: expected, Gap: &gap}}})
			}
			sendTailEvent(ctx, output, logTailEvent{event: incompleteLogSeal(stream, "logger pipe EOF seal was not observed")})
			return
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func waitForLogSegment(ctx context.Context, path string) (*os.File, error) {
	for {
		file, err := os.Open(path)
		if err == nil {
			return file, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func incompleteLogSeal(stream, reason string) WatchEvent {
	return WatchEvent{Kind: WatchProgress, Seal: &LogSeal{Stream: stream, Complete: false, Reason: reason}}
}

func sendCorruptLogGap(ctx context.Context, output chan<- logTailEvent, stream string, sequence, lostBytes uint64) {
	if lostBytes == 0 {
		return
	}
	gap := &LogGapFrame{ThroughSequence: sequence, LostEventCount: 1, LostByteCount: lostBytes, Reason: "logger_source_incomplete"}
	sendTailEvent(ctx, output, logTailEvent{event: WatchEvent{Kind: WatchProgress, Log: &LogFrame{Stream: stream, Sequence: sequence, Gap: gap}}})
}

func sendTailEvent(ctx context.Context, output chan<- logTailEvent, event logTailEvent) bool {
	select {
	case output <- event:
		return true
	case <-ctx.Done():
		return false
	}
}

func (engine *ContainerdEngine) watchOOM(attempt *containerdAttempt) {
	ctx, cancel := context.WithCancel(engineContext(context.Background()))
	attempt.oomCancel = cancel
	events, failures := engine.client.Subscribe(ctx, `topic=="/tasks/oom"`)
	go func() {
		for {
			select {
			case envelope, ok := <-events:
				if !ok {
					return
				}
				if envelope.Namespace == ContainerdNamespace {
					decoded, err := typeurl.UnmarshalAny(envelope.Event)
					oom, ok := decoded.(*eventtypes.TaskOOM)
					if err != nil || !ok || oom.ContainerID != attempt.resources.ContainerID {
						continue
					}
					attempt.mu.Lock()
					attempt.oom = true
					attempt.mu.Unlock()
				}
			case <-failures:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (engine *ContainerdEngine) inventory(ctx context.Context) (ResourceInventory, error) {
	result := ResourceInventory{Leases: []string{}, Snapshots: []string{}, Containers: []string{}, Tasks: []string{}, Shims: []string{}, Cgroups: []string{}, LogSegments: []string{}, ManagedVolumes: []string{}}
	leaseList, err := engine.client.LeasesService().List(ctx)
	if err != nil {
		return result, err
	}
	for _, lease := range leaseList {
		result.Leases = append(result.Leases, lease.ID)
		if strings.HasPrefix(lease.ID, "wefty-lease-") {
			authority, labelErr := authorityFromLabels(lease.Labels)
			if labelErr != nil {
				return result, labelErr
			}
			expected, _ := DeterministicResourceIdentity(authority)
			if expected.LeaseID != lease.ID {
				return result, fmt.Errorf("lease %s has mismatched authority labels", lease.ID)
			}
		}
	}
	if err := engine.client.SnapshotService(DefaultSnapshotter).Walk(ctx, func(_ context.Context, info snapshots.Info) error {
		if strings.HasPrefix(info.Name, "wefty-snapshot-") {
			authority, labelErr := authorityFromLabels(info.Labels)
			if labelErr != nil {
				return labelErr
			}
			expected, _ := DeterministicResourceIdentity(authority)
			if expected.SnapshotID != info.Name {
				return fmt.Errorf("snapshot %s has mismatched authority labels", info.Name)
			}
			result.Snapshots = append(result.Snapshots, info.Name)
		}
		return nil
	}); err != nil {
		return result, err
	}
	containersList, err := engine.client.Containers(ctx)
	if err != nil {
		return result, err
	}
	for _, container := range containersList {
		result.Containers = append(result.Containers, container.ID())
		if !strings.HasPrefix(container.ID(), "wefty-container-") {
			continue
		}
		info, infoErr := container.Info(ctx)
		if infoErr != nil {
			return result, infoErr
		}
		authority, labelErr := authorityFromLabels(info.Labels)
		if labelErr != nil {
			return result, labelErr
		}
		expected, _ := DeterministicResourceIdentity(authority)
		if expected.ContainerID != container.ID() {
			return result, fmt.Errorf("container %s has mismatched authority labels", container.ID())
		}
		if task, taskErr := container.Task(ctx, nil); taskErr == nil {
			result.Tasks = append(result.Tasks, task.ID())
		} else if !errdefs.IsNotFound(taskErr) {
			return result, taskErr
		}
	}
	entries, err := readDirectoryIfPresent(filepath.Join(engine.config.RuntimeRoot, "logs"))
	if err != nil {
		return result, err
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "wefty-log-segments-") {
			result.LogSegments = append(result.LogSegments, entry.Name())
		}
	}
	shimEntries, err := readDirectoryIfPresent(filepath.Join(engine.config.ContainerdStateRoot, "io.containerd.runtime.v2.task", ContainerdNamespace))
	if err != nil {
		return result, err
	}
	for _, entry := range shimEntries {
		result.Shims = append(result.Shims, entry.Name())
	}
	if err := filepath.WalkDir(engine.config.CgroupRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && strings.Contains(entry.Name(), "wefty-cgroup-") {
			result.Cgroups = append(result.Cgroups, entry.Name())
		}
		return nil
	}); err != nil {
		return result, err
	}
	volumeEntries, err := readDirectoryIfPresent(filepath.Join(engine.config.RuntimeRoot, "volumes"))
	if err != nil {
		return result, err
	}
	for _, entry := range volumeEntries {
		if strings.HasPrefix(entry.Name(), "wefty-handoff-volume-") || strings.HasPrefix(entry.Name(), "wefty-service-volume-") {
			result.ManagedVolumes = append(result.ManagedVolumes, entry.Name())
		}
	}
	sort.Strings(result.Leases)
	sort.Strings(result.Snapshots)
	sort.Strings(result.Containers)
	sort.Strings(result.Tasks)
	sort.Strings(result.Shims)
	sort.Strings(result.Cgroups)
	sort.Strings(result.LogSegments)
	sort.Strings(result.ManagedVolumes)
	return result, nil
}

func filterInventory(inventory ResourceInventory, resources ResourceIdentity) ResourceInventory {
	filtered := ResourceInventory{Leases: []string{}, Snapshots: []string{}, Containers: []string{}, Tasks: []string{}, Shims: []string{}, Cgroups: []string{}, LogSegments: []string{}, ManagedVolumes: []string{}}
	for _, pair := range []struct {
		values []string
		target string
		output *[]string
	}{{inventory.Leases, resources.LeaseID, &filtered.Leases}, {inventory.Snapshots, resources.SnapshotID, &filtered.Snapshots}, {inventory.Containers, resources.ContainerID, &filtered.Containers}, {inventory.Tasks, resources.ContainerID, &filtered.Tasks}, {inventory.Shims, resources.ContainerID, &filtered.Shims}, {inventory.Cgroups, resources.CgroupID, &filtered.Cgroups}, {inventory.LogSegments, resources.LogSegmentDirectory, &filtered.LogSegments}, {inventory.ManagedVolumes, resources.HandoffVolumeDirectory, &filtered.ManagedVolumes}, {inventory.ManagedVolumes, resources.ServiceVolumeDirectory, &filtered.ManagedVolumes}} {
		for _, value := range pair.values {
			if value == pair.target || (pair.target == resources.CgroupID && strings.Contains(value, pair.target)) {
				*pair.output = append(*pair.output, value)
			}
		}
	}
	return filtered
}

func captureSweepAuthority(authority AttemptAuthority, prior map[string]struct{}, attempts map[string]SweptAttemptAuthority) {
	boot := authority.BootSessionID
	if boot != "" {
		prior[boot] = struct{}{}
	}
	attempt := authority.AttemptID
	if attempt != "" {
		attempts[authority.key()] = SweptAttemptAuthority{NodeID: authority.NodeID, JobID: authority.JobID, RemovalGeneration: authority.RemovalGeneration, AttemptID: attempt, FencingToken: authority.FencingToken, PriorBootSessionID: boot, Class: authority.Class}
	}
}

func authorityFromLabels(labels map[string]string) (AttemptAuthority, error) {
	if len(labels) != 7 {
		return AttemptAuthority{}, fmt.Errorf("resource requires exactly seven authority labels, got %d", len(labels))
	}
	authority := AttemptAuthority{NodeID: labels["io.wefty/node_id"], JobID: labels["io.wefty/job_id"], AttemptID: labels["io.wefty/attempt_id"], FencingToken: labels["io.wefty/fencing_token"], BootSessionID: labels["io.wefty/boot_session_id"], Class: labels["io.wefty/class"], RemovalGeneration: labels["io.wefty/removal_generation"]}
	return authority, authority.validate()
}

func readDirectoryIfPresent(path string) ([]os.DirEntry, error) {
	entries, err := os.ReadDir(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func mapKeys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
func inventoryCount(inventory ResourceInventory) int {
	return len(inventory.Leases) + len(inventory.Snapshots) + len(inventory.Containers) + len(inventory.Tasks) + len(inventory.Shims) + len(inventory.Cgroups) + len(inventory.LogSegments) + len(inventory.ManagedVolumes)
}

func kernelRelease() string {
	var value unix.Utsname
	if unix.Uname(&value) != nil {
		return "unknown"
	}
	return unix.ByteSliceToString(value.Release[:])
}

func cgroupReportedOOM(cgroupRoot, cgroupID string) bool {
	payload, err := os.ReadFile(filepath.Join(cgroupRoot, cgroupID, "memory.events"))
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(payload), "\n") {
		name, value, ok := strings.Cut(line, " ")
		if !ok || name != "oom_kill" {
			continue
		}
		if strings.TrimSpace(value) != "0" {
			return true
		}
	}
	return false
}

var _ Engine = (*ContainerdEngine)(nil)
