//go:build linux

package ocihelper

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	eventtypes "github.com/containerd/containerd/api/events"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/containerd/containerd/v2/core/remotes/docker"
	remoteerrors "github.com/containerd/containerd/v2/core/remotes/errors"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	"github.com/containerd/typeurl/v2"
	distributionref "github.com/distribution/reference"
	digest "github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sys/unix"
)

type containerdAttempt struct {
	authority        AttemptAuthority
	resources        ResourceIdentity
	computerDisk     *computerDiskAttachment
	container        containerd.Container
	task             containerd.Task
	signaler         containerdTaskSignaler
	releaseTask      func(context.Context) error
	terminalReady    chan struct{}
	terminalCode     uint32
	terminalErr      error
	stdout           string
	stderr           string
	oom              bool
	oomCancel        context.CancelFunc
	cancel           context.CancelFunc
	signal           Signal
	signalCause      string
	deleted          bool
	logAcknowledged  map[string]uint64
	hostBridge       net.Listener
	endpoints        map[string]uint16
	endpointHolds    map[string]net.Listener
	controlDirectory string
	computerUID      uint32
	computerGID      uint32
	controlMu        sync.Mutex
	mu               sync.Mutex
}

type containerdTaskSignaler interface {
	Kill(context.Context, syscall.Signal, ...containerd.KillOpts) error
}

type ContainerdEngine struct {
	client                      *containerd.Client
	imageLeaseDeletes           imageLeaseDeletionManager
	config                      NativeEngineConfig
	imageOperations             *imageOperationGroup
	imageNameMu                 sync.Mutex
	imageContentMu              sync.Mutex
	imageResourceMu             sync.Mutex
	activeSpools                map[string]struct{}
	activeLeases                map[string]struct{}
	attemptImagePins            map[string]imageOperationKey
	bindingImagePins            map[string]imageOperationKey
	probeDigests                map[string]struct{}
	cache                       *imageCacheLedger
	cacheMaxBytes               int64
	cacheReady                  bool
	cacheStop                   chan struct{}
	cacheDone                   chan struct{}
	closeOnce                   sync.Once
	closeErr                    error
	mu                          sync.Mutex
	attempts                    map[string]*containerdAttempt
	ports                       map[uint16]string
	nextPort                    uint16
	serviceVolumeMu             sync.Mutex
	storageResetMu              sync.Mutex
	computerBackupMu            sync.Mutex
	storageCopyMu               sync.Mutex
	diskSystem                  computerDiskSystem
	storageResetHook            func(computerStorageResetPhase) error
	computerBackupHook          func(computerBackupCheckpoint) error
	computerBackupAllocate      func(string, int64) error
	computerBackupCopyN         func(io.Writer, io.Reader, int64) (int64, error)
	computerBackupRemovalHook   func()
	computerCustodyHook         func(string) error
	computerCustodyCopyN        func(io.Writer, io.Reader, int64) (int64, error)
	storageCopyHook             func(computerStorageCopyPhase) error
	storageCopyFinalize         func(context.Context, string, string, string, int64, bool) (computerStorageCopyFacts, error)
	computerDiskHook            func(computerDiskCheckpoint) error
	computerGrowHook            func(string) error
	computerGrowResize          func(context.Context, string, string, int64, int64) error
	computerGrowFilesystemBytes func(context.Context, string) (int64, error)
	lastProfile                 *ProfileReceipt
	capacityMu                  sync.Mutex
	capacityReservations        map[string]*capacityReservation
	lastAdmission               *ResourceAdmissionReceipt
	memoryFactsPath             string
}

const (
	defaultAttemptPortMin    uint16 = 42000
	defaultAttemptPortMax    uint16 = 42999
	defaultHandoffRetention         = 24 * time.Hour
	doctorRuntimeReadTimeout        = 2 * time.Second
)

func NewContainerdEngine(config NativeEngineConfig) (*ContainerdEngine, error) {
	if config.MemoryCapacityBytes < 0 || config.MemoryReserveBytes < 0 {
		return nil, errors.New("OCI memory capacity and reserve must not be negative")
	}
	if config.Clock == nil {
		config.Clock = systemClock{}
	}
	if config.Address == "" {
		config.Address = DefaultContainerdAddress
	}
	if config.RuntimeRoot == "" {
		config.RuntimeRoot = "/var/lib/wefty/oci"
	}
	if config.RuncExecutable != "" && !filepath.IsAbs(config.RuncExecutable) {
		return nil, errors.New("configured runc executable must be absolute")
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
	if config.TaskReleaseTimeout <= 0 {
		config.TaskReleaseTimeout = DefaultTaskReleaseTimeout
	}
	if config.HandoffRetention <= 0 {
		config.HandoffRetention = defaultHandoffRetention
	}
	if config.AttemptPortMin == 0 {
		config.AttemptPortMin = defaultAttemptPortMin
	}
	if config.AttemptPortMax == 0 {
		config.AttemptPortMax = defaultAttemptPortMax
	}
	if config.AttemptPortBindTimeout <= 0 {
		config.AttemptPortBindTimeout = 5 * time.Second
	}
	if config.AttemptPortMin > config.AttemptPortMax {
		return nil, errors.New("OCI attempt port range is invalid")
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
	if (config.HostMountRoot == "") != (config.GuestMountRoot == "") {
		return nil, errors.New("Lima host and guest mount roots must be configured together")
	}
	if config.HostMountRoot != "" {
		if !filepath.IsAbs(config.HostMountRoot) || !filepath.IsAbs(config.GuestMountRoot) ||
			filepath.Clean(config.HostMountRoot) == string(filepath.Separator) || filepath.Clean(config.GuestMountRoot) == string(filepath.Separator) {
			return nil, errors.New("Lima host and guest mount roots must be absolute non-root paths")
		}
		config.HostMountRoot = filepath.Clean(config.HostMountRoot)
		config.GuestMountRoot = filepath.Clean(config.GuestMountRoot)
		allowed := false
		for _, root := range config.AllowedMountRoots {
			cleanRoot := filepath.Clean(root)
			if config.GuestMountRoot == cleanRoot || strings.HasPrefix(config.GuestMountRoot, cleanRoot+string(filepath.Separator)) {
				allowed = true
				break
			}
		}
		if !allowed {
			return nil, errors.New("Lima guest mount root must be inside an allowed mount root")
		}
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
	cache, err := openImageCacheLedger(config.RuntimeRoot)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("open OCI image cache ledger: %w", err)
	}
	engine := &ContainerdEngine{
		client: client, imageLeaseDeletes: client.LeasesService(), config: config, imageOperations: newImageOperationGroup(),
		activeSpools: make(map[string]struct{}), activeLeases: make(map[string]struct{}),
		attemptImagePins: make(map[string]imageOperationKey), bindingImagePins: make(map[string]imageOperationKey), probeDigests: make(map[string]struct{}),
		cache: cache, cacheMaxBytes: DefaultImageCacheMaxBytes,
		cacheStop: make(chan struct{}), cacheDone: make(chan struct{}),
		attempts: make(map[string]*containerdAttempt),
		ports:    make(map[uint16]string), nextPort: config.AttemptPortMin,
		capacityReservations: make(map[string]*capacityReservation),
		memoryFactsPath:      "/proc/meminfo",
	}
	go engine.imageCacheLoop()
	return engine, nil
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
	if engine == nil {
		return nil
	}
	engine.closeOnce.Do(func() {
		if engine.imageOperations != nil {
			engine.closeErr = errors.Join(engine.closeErr, engine.imageOperations.CancelAll(context.Background()))
		}
		if engine.cacheStop != nil {
			close(engine.cacheStop)
		}
		if engine.cacheDone != nil {
			<-engine.cacheDone
		}
		if engine.client != nil {
			engine.closeErr = errors.Join(engine.closeErr, engine.client.Close())
		}
	})
	return engine.closeErr
}

func (engine *ContainerdEngine) DoctorStatus(ctx context.Context) (DoctorStatus, error) {
	status := DoctorStatus{RuntimePlatform: OCIPlatform{OS: runtime.GOOS, Architecture: runtime.GOARCH}}
	engine.mu.Lock()
	if engine.lastProfile != nil {
		receipt := *engine.lastProfile
		receipt.Warnings = slices.Clone(receipt.Warnings)
		status.LastProfile = &receipt
	}
	engine.mu.Unlock()
	engine.capacityMu.Lock()
	if engine.lastAdmission != nil {
		receipt := *engine.lastAdmission
		receipt.Warnings = slices.Clone(receipt.Warnings)
		status.LastAdmission = &receipt
	}
	engine.capacityMu.Unlock()
	versionContext, cancelVersion := context.WithTimeout(ctx, doctorRuntimeReadTimeout)
	version, err := engine.client.Version(versionContext)
	cancelVersion()
	if err != nil {
		status.ContainerdRead = DiagnosticReadReceipt{Outcome: DiagnosticReadFailed, ErrorCode: DiagnosticErrorContainerdVersion}
	} else {
		status.ContainerdVersion = version.Version
		status.ContainerdRead = DiagnosticReadReceipt{Outcome: DiagnosticReadOK}
	}
	runcContext, cancelRunc := context.WithTimeout(ctx, doctorRuntimeReadTimeout)
	if engine.config.RuncExecutable != "" {
		payload, runcErr := exec.CommandContext(runcContext, engine.config.RuncExecutable, "--version").Output()
		if runcErr == nil {
			line, _, _ := strings.Cut(strings.TrimSpace(string(payload)), "\n")
			status.RuncVersion = strings.TrimSpace(strings.TrimPrefix(line, "runc version"))
			status.RuncVersionSource = RuncVersionSourceConfiguredPath
		}
		if runcErr != nil || status.RuncVersion == "" {
			status.RuncRead = DiagnosticReadReceipt{Outcome: DiagnosticReadFailed, ErrorCode: DiagnosticErrorRuncVersion}
		} else {
			status.RuncRead = DiagnosticReadReceipt{Outcome: DiagnosticReadOK}
		}
	} else {
		runtimeInfo, runcErr := engine.client.RuntimeInfo(runcContext, DefaultRuntimeHandler, nil)
		if runcErr == nil && runtimeInfo != nil {
			status.RuncVersion = strings.TrimSpace(runtimeInfo.Version.Version)
			status.RuncVersionSource = RuncVersionSourceContainerdInfo
		}
		if runcErr != nil || status.RuncVersion == "" {
			status.RuncRead = DiagnosticReadReceipt{Outcome: DiagnosticReadFailed, ErrorCode: DiagnosticErrorRuncVersion}
		} else {
			status.RuncRead = DiagnosticReadReceipt{Outcome: DiagnosticReadOK}
		}
	}
	cancelRunc()
	cacheContext, cancelCache := context.WithTimeout(ctx, doctorRuntimeReadTimeout)
	cache, cacheErr := engine.ImageCacheStatus(cacheContext)
	cancelCache()
	if cacheErr != nil {
		status.CacheRead = DiagnosticReadReceipt{Outcome: DiagnosticReadFailed, ErrorCode: DiagnosticErrorCacheStatus}
	} else {
		if cache.LastError != "" {
			status.CacheLastErrorCode = DiagnosticErrorCacheEviction
			cache.LastError = ""
		}
		status.Cache = cache
		status.CacheRead = DiagnosticReadReceipt{Outcome: DiagnosticReadOK}
	}
	roots := append([]string(nil), engine.config.AllowedMountRoots...)
	if engine.config.HostMountRoot != "" {
		roots = []string{engine.config.HostMountRoot}
	}
	for index := range roots {
		roots[index] = filepath.Clean(roots[index])
	}
	sort.Strings(roots)
	status.AllowedMountRoots = roots
	status.MountRootsRead = DiagnosticReadReceipt{Outcome: DiagnosticReadOK}
	return status, nil
}

func engineContext(ctx context.Context) context.Context {
	return namespaces.WithNamespace(ctx, ContainerdNamespace)
}

func (engine *ContainerdEngine) EnsureImage(ctx context.Context, request EnsureImageRequest, archive io.Reader, emit func(EnsureImageEvent) error) error {
	timeout := request.OperationTimeout
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	operationContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	source := request.Source
	requestedPlatform := ocispec.Platform{OS: request.Platform.OS, Architecture: request.Platform.Architecture, Variant: request.Platform.Variant}
	requestedPlatform = platforms.Normalize(requestedPlatform)
	platformMatcher := platforms.OnlyStrict(requestedPlatform)
	platformKey := platforms.Format(requestedPlatform)
	if source == "" {
		source = ImageSourceRegistry
	}
	if source == ImageSourceArchive {
		var spoolPath string
		inspection, err := inspectOCIArchiveWithSpoolForPlatform(operationContext, engine.config.RuntimeRoot, archive, request.Reference, request.Digest, platformMatcher, func(directory string) (*os.File, error) {
			engine.imageResourceMu.Lock()
			defer engine.imageResourceMu.Unlock()
			file, createErr := os.CreateTemp(directory, "wefty-image-*.tar")
			if createErr == nil {
				spoolPath = file.Name()
				engine.activeSpools[spoolPath] = struct{}{}
			}
			return file, createErr
		})
		defer func() {
			engine.imageResourceMu.Lock()
			delete(engine.activeSpools, spoolPath)
			engine.imageResourceMu.Unlock()
		}()
		if err != nil {
			var pathError *os.PathError
			var networkError net.Error
			switch {
			case errors.Is(err, syscall.ENOSPC), errors.As(err, &pathError):
				return imageMechanicsError(ImageFailureResourceExhausted, "", err)
			case errors.As(err, &networkError), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
				return imageMechanicsError(ImageFailureNetwork, "", err)
			}
			return imageMechanicsError(ImageFailureManifestRejected, "", err)
		}
		defer os.Remove(inspection.Path)
		if err := emit(EnsureImageEvent{Kind: ImageProgress, Progress: &ImageProgressEvent{Status: "resolved", TopLevelDigest: inspection.TopLevel.Digest.String()}}); err != nil {
			return err
		}
		response, err := engine.importImage(operationContext, request, inspection, timeout)
		if err != nil {
			return err
		}
		return emit(EnsureImageEvent{Kind: ImageComplete, Result: &response})
	}

	reference, topLevelDigest, err := engine.resolvePublicImage(operationContext, request.Reference, request.Digest)
	if err != nil {
		return err
	}
	if err := emit(EnsureImageEvent{Kind: ImageProgress, Progress: &ImageProgressEvent{Status: "resolved", TopLevelDigest: topLevelDigest}}); err != nil {
		return err
	}
	key := imageOperationKey{Namespace: ContainerdNamespace, Digest: topLevelDigest, Platform: platformKey, Snapshotter: DefaultSnapshotter}
	response, err := engine.imageOperations.DoPreparedWithAttach(operationContext, key, timeout, func(prepareContext context.Context) (func(context.Context) (EnsureImageResponse, error), func(), error) {
		leaseContext, release, leaseErr := engine.imageOperationLease(engineContext(prepareContext), key)
		if leaseErr != nil {
			return nil, nil, leaseErr
		}
		return func(context.Context) (EnsureImageResponse, error) {
			engine.imageContentMu.Lock()
			defer engine.imageContentMu.Unlock()
			return engine.pullPublicImage(leaseContext, reference, topLevelDigest, platformMatcher)
		}, release, nil
	}, func(attachContext context.Context, result EnsureImageResponse) error {
		return engine.attachImageHolds(engineContext(attachContext), request, key, result.Evidence)
	})
	if err != nil {
		var mechanics *ImageMechanicsError
		if errors.As(err, &mechanics) && mechanics.Fact.TopLevelDigest == "" {
			fact := mechanics.Fact
			fact.TopLevelDigest = topLevelDigest
			return &ImageMechanicsError{Fact: fact, err: mechanics.err}
		}
		return err
	}
	if err := engine.recordCacheUse(key, response.Evidence, false); err != nil {
		engine.recordCacheError(err)
		log.Printf("OCI image cache post-delivery bookkeeping: %v", err)
	}
	if err := engine.enforceImageCache(context.WithoutCancel(ctx), "post_pull"); err != nil {
		engine.recordCacheError(err)
		log.Printf("OCI image cache post-delivery enforcement: %v", err)
	}
	return emit(EnsureImageEvent{Kind: ImageComplete, Result: &response})
}

func (engine *ContainerdEngine) resolvePublicImage(ctx context.Context, rawReference, expectedDigest string) (string, string, error) {
	named, err := distributionref.ParseNormalizedNamed(rawReference)
	if err != nil {
		return "", "", imageMechanicsError(ImageFailureManifestRejected, "", err)
	}
	if _, ok := named.(distributionref.Digested); ok {
		return "", "", imageMechanicsError(ImageFailureManifestRejected, "", errors.New("registry image reference must not contain a digest"))
	}
	named = distributionref.TagNameOnly(named)
	if expectedDigest != "" {
		if err := digest.Digest(expectedDigest).Validate(); err != nil {
			return "", "", imageMechanicsError(ImageFailureManifestRejected, "", err)
		}
		// A prior successful resolution is immutable policy input. Do not
		// resolve the mutable tag again on agent retry; Pull verifies the
		// pinned descriptor while fetching it by digest.
		return named.String(), expectedDigest, nil
	}
	tracker := &retryAfterTracker{}
	_, descriptor, err := publicResolver(tracker).Resolve(engineContext(ctx), named.String())
	if err != nil {
		return "", "", classifyRegistryError(err, tracker.Delay(), expectedDigest)
	}
	if err := validateResolvedImageDescriptor(descriptor); err != nil {
		return "", "", err
	}
	resolved := descriptor.Digest.String()
	return named.String(), resolved, nil
}

func validateResolvedImageDescriptor(descriptor ocispec.Descriptor) error {
	if !images.IsManifestType(descriptor.MediaType) && !images.IsIndexType(descriptor.MediaType) {
		return imageMechanicsError(ImageFailureManifestRejected, descriptor.Digest.String(), errors.New("registry resolved an unsupported image media type"))
	}
	return nil
}

func (engine *ContainerdEngine) pullPublicImage(ctx context.Context, reference, topLevelDigest string, matcher platforms.MatchComparer) (EnsureImageResponse, error) {
	if image, evidence, localErr := engine.localImageForPlatform(ctx, reference, topLevelDigest, matcher); localErr == nil {
		if unpackErr := image.Unpack(ctx, DefaultSnapshotter); unpackErr != nil && !errdefs.IsAlreadyExists(unpackErr) {
			return EnsureImageResponse{}, classifyImageOperationError(unpackErr, topLevelDigest)
		}
		return ensureImageResponse(evidence), nil
	} else if !isLocalImageNotFound(localErr) {
		return EnsureImageResponse{}, classifyImageOperationError(localErr, topLevelDigest)
	}
	if err := pullPublicImageContent(ctx, engine.client, reference, topLevelDigest, matcher); err != nil {
		return EnsureImageResponse{}, err
	}
	_, evidence, err := engine.localImageForPlatform(ctx, reference, topLevelDigest, matcher)
	if err != nil {
		return EnsureImageResponse{}, classifyImageOperationError(err, topLevelDigest)
	}
	return ensureImageResponse(evidence), nil
}

type publicImagePuller interface {
	Pull(context.Context, string, ...containerd.RemoteOpt) (containerd.Image, error)
}

func pullPublicImageContent(ctx context.Context, puller publicImagePuller, reference, topLevelDigest string, matcher platforms.MatchComparer) error {
	tracker := &retryAfterTracker{}
	_, err := puller.Pull(ctx, reference+"@"+topLevelDigest,
		containerd.WithResolver(publicResolver(tracker)),
		containerd.WithPlatformMatcher(matcher),
		containerd.WithPullUnpack,
		containerd.WithPullSnapshotter(DefaultSnapshotter),
	)
	if err != nil {
		return classifyRegistryError(err, tracker.Delay(), topLevelDigest)
	}
	return nil
}

func isLocalImageNotFound(err error) bool {
	var unavailable *ImageUnavailableError
	return errors.As(err, &unavailable) && errdefs.IsNotFound(unavailable.err)
}

func (engine *ContainerdEngine) importImage(ctx context.Context, request EnsureImageRequest, inspection ociArchiveInspection, timeout time.Duration) (EnsureImageResponse, error) {
	key := imageOperationKey{Namespace: ContainerdNamespace, Digest: inspection.TopLevel.Digest.String(), Platform: platforms.Format(inspection.Platform), Snapshotter: DefaultSnapshotter}
	response, err := engine.imageOperations.DoPreparedWithAttach(ctx, key, timeout, func(prepareContext context.Context) (func(context.Context) (EnsureImageResponse, error), func(), error) {
		file, openErr := os.Open(inspection.Path)
		if openErr != nil {
			return nil, nil, imageMechanicsError(ImageFailureUnavailable, inspection.TopLevel.Digest.String(), openErr)
		}
		leaseContext, release, leaseErr := engine.imageOperationLease(engineContext(prepareContext), key)
		if leaseErr != nil {
			_ = file.Close()
			return nil, nil, leaseErr
		}
		return func(context.Context) (EnsureImageResponse, error) {
			engine.imageContentMu.Lock()
			defer engine.imageContentMu.Unlock()
			return engine.importImageContent(leaseContext, inspection, file)
		}, func() { _ = file.Close(); release() }, nil
	}, func(attachContext context.Context, result EnsureImageResponse) error {
		return engine.attachImageHolds(engineContext(attachContext), request, key, result.Evidence)
	})
	if err != nil {
		return EnsureImageResponse{}, err
	}
	if err := engine.bindImportedImage(engineContext(ctx), inspection.Reference, inspection.TopLevel); err != nil {
		return EnsureImageResponse{}, err
	}
	if err := engine.recordCacheUse(key, response.Evidence, true); err != nil {
		engine.recordCacheError(err)
		log.Printf("OCI image cache offline-import bookkeeping: %v", err)
	}
	if err := engine.enforceImageCache(context.WithoutCancel(ctx), "post_import"); err != nil {
		engine.recordCacheError(err)
		log.Printf("OCI image cache offline-import enforcement: %v", err)
	}
	return response, nil
}

func (engine *ContainerdEngine) importImageContent(ctx context.Context, inspection ociArchiveInspection, file io.Reader) (EnsureImageResponse, error) {
	internalReference := "wefty.local/import@" + inspection.TopLevel.Digest.String()
	matcher := platforms.OnlyStrict(platforms.Normalize(inspection.Platform))
	if _, err := engine.client.Import(ctx, file,
		containerd.WithImportPlatform(matcher),
		containerd.WithImageRefTranslator(func(string) string { return internalReference }),
	); err != nil {
		return EnsureImageResponse{}, classifyImageOperationError(err, inspection.TopLevel.Digest.String())
	}
	image, evidence, err := engine.localImageForPlatform(ctx, inspection.Reference, inspection.TopLevel.Digest.String(), matcher)
	if err != nil {
		return EnsureImageResponse{}, classifyImageOperationError(err, inspection.TopLevel.Digest.String())
	}
	if err := image.Unpack(ctx, DefaultSnapshotter); err != nil && !errdefs.IsAlreadyExists(err) {
		return EnsureImageResponse{}, classifyImageOperationError(err, inspection.TopLevel.Digest.String())
	}
	return ensureImageResponse(evidence), nil
}

func ensureImageResponse(evidence ImageEvidence) EnsureImageResponse {
	return EnsureImageResponse{
		TopLevelDigest: evidence.TopLevelDigest,
		PlatformDigest: evidence.PlatformManifestDigest,
		Evidence:       evidence,
	}
}

func (engine *ContainerdEngine) bindImportedImage(ctx context.Context, reference string, target ocispec.Descriptor) error {
	engine.imageNameMu.Lock()
	defer engine.imageNameMu.Unlock()
	existing, err := engine.client.ImageService().Get(ctx, reference)
	if err == nil {
		if existing.Target.Digest != target.Digest {
			return imageMechanicsError(ImageFailureManifestRejected, target.Digest.String(), errors.New("OCI image name already identifies different bytes"))
		}
		return nil
	}
	if !errdefs.IsNotFound(err) {
		return classifyImageOperationError(err, target.Digest.String())
	}
	if _, err := engine.client.ImageService().Create(ctx, images.Image{Name: reference, Target: target}); err != nil {
		if !errdefs.IsAlreadyExists(err) {
			return classifyImageOperationError(err, target.Digest.String())
		}
		existing, err = engine.client.ImageService().Get(ctx, reference)
		if err != nil || existing.Target.Digest != target.Digest {
			return imageMechanicsError(ImageFailureManifestRejected, target.Digest.String(), errors.New("OCI image name already identifies different bytes"))
		}
	}
	return nil
}

func (engine *ContainerdEngine) imageOperationLease(ctx context.Context, key imageOperationKey) (context.Context, func(), error) {
	hash := sha256.Sum256([]byte(key.Namespace + "\x00" + key.Digest + "\x00" + key.Platform + "\x00" + key.Snapshotter))
	engine.imageResourceMu.Lock()
	lease, err := engine.client.LeasesService().Create(ctx, leases.WithID("wefty-image-op-"+hex.EncodeToString(hash[:16])))
	if err != nil {
		engine.imageResourceMu.Unlock()
		return ctx, func() {}, err
	}
	engine.activeLeases[lease.ID] = struct{}{}
	engine.imageResourceMu.Unlock()
	leaseContext := leases.WithLease(ctx, lease.ID)
	return leaseContext, func() {
		cleanupContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_ = engine.client.LeasesService().Delete(cleanupContext, lease)
		engine.imageResourceMu.Lock()
		delete(engine.activeLeases, lease.ID)
		engine.imageResourceMu.Unlock()
	}, nil
}

func publicResolver(tracker *retryAfterTracker) remotes.Resolver {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 30 * time.Second
	transport.IdleConnTimeout = 90 * time.Second
	return docker.NewResolver(docker.ResolverOptions{Client: &http.Client{Transport: retryAfterTransport{base: transport, tracker: tracker}}})
}

type retryAfterTracker struct {
	mu    sync.Mutex
	delay time.Duration
}

func (tracker *retryAfterTracker) Observe(header string, now time.Time) {
	delay := parseRetryAfter(header, now)
	tracker.mu.Lock()
	if delay > tracker.delay {
		tracker.delay = delay
	}
	tracker.mu.Unlock()
}

func (tracker *retryAfterTracker) Delay() time.Duration {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	return tracker.delay
}

type retryAfterTransport struct {
	base    http.RoundTripper
	tracker *retryAfterTracker
}

func (transport retryAfterTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := transport.base.RoundTrip(request)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		// Hide net.Error from containerd's mechanics-level retry loop. The
		// agent receives the typed transient result and owns all retry timing.
		return nil, &registryTransportError{err: err}
	}
	if response != nil && (response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500) {
		transport.tracker.Observe(response.Header.Get("Retry-After"), time.Now())
		if response.Body != nil {
			_ = response.Body.Close()
		}
		return nil, &registryStatusError{statusCode: response.StatusCode}
	}
	return response, err
}

type registryStatusError struct{ statusCode int }

func (failure *registryStatusError) Error() string {
	return "registry returned a transient HTTP status"
}

type registryTransportError struct{ err error }

func (failure *registryTransportError) Error() string { return failure.err.Error() }

func parseRetryAfter(value string, now time.Time) time.Duration {
	if seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil && when.After(now) {
		return when.Sub(now)
	}
	return 0
}

func classifyRegistryError(err error, retryAfter time.Duration, resolvedDigest string) error {
	if err == nil {
		return nil
	}
	var statusFailure *registryStatusError
	if errors.As(err, &statusFailure) {
		return &ImageMechanicsError{Fact: ImageFailureFact{Kind: ImageFailureHTTP, HTTPStatus: statusFailure.statusCode, RetryAfter: retryAfter, TopLevelDigest: resolvedDigest}, err: err}
	}
	var transportFailure *registryTransportError
	if errors.As(err, &transportFailure) {
		return &ImageMechanicsError{Fact: ImageFailureFact{Kind: ImageFailureNetwork, RetryAfter: retryAfter, TopLevelDigest: resolvedDigest}, err: err}
	}
	var unexpected remoteerrors.ErrUnexpectedStatus
	if errors.As(err, &unexpected) {
		if unexpected.StatusCode == http.StatusBadRequest || unexpected.StatusCode == http.StatusNotAcceptable || unexpected.StatusCode == http.StatusUnprocessableEntity {
			return imageMechanicsError(ImageFailureManifestRejected, resolvedDigest, err)
		}
		return &ImageMechanicsError{Fact: ImageFailureFact{Kind: ImageFailureHTTP, HTTPStatus: unexpected.StatusCode, RetryAfter: retryAfter, TopLevelDigest: resolvedDigest}, err: err}
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "unsupported media type") || strings.Contains(message, "unexpected media type") {
		return imageMechanicsError(ImageFailureManifestRejected, resolvedDigest, err)
	}
	var networkError net.Error
	var dnsError *net.DNSError
	if errors.As(err, &dnsError) || errors.As(err, &networkError) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.DeadlineExceeded) {
		return &ImageMechanicsError{Fact: ImageFailureFact{Kind: ImageFailureNetwork, RetryAfter: retryAfter, TopLevelDigest: resolvedDigest}, err: err}
	}
	if errdefs.IsNotFound(err) {
		return &ImageMechanicsError{Fact: ImageFailureFact{Kind: ImageFailureHTTP, HTTPStatus: http.StatusNotFound, TopLevelDigest: resolvedDigest}, err: err}
	}
	return classifyImageOperationError(err, resolvedDigest)
}

func classifyImageOperationError(err error, resolvedDigest string) error {
	if err == nil {
		return nil
	}
	var mechanics *ImageMechanicsError
	if errors.As(err, &mechanics) {
		if mechanics.Fact.TopLevelDigest == "" {
			fact := mechanics.Fact
			fact.TopLevelDigest = resolvedDigest
			return &ImageMechanicsError{Fact: fact, err: mechanics.err}
		}
		return mechanics
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return imageMechanicsError(ImageFailureNetwork, resolvedDigest, err)
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "no match for platform") || strings.Contains(message, "no manifest for") || strings.Contains(message, "platform does not match") {
		return imageMechanicsError(ImageFailurePlatformMismatch, resolvedDigest, err)
	}
	if errdefs.IsUnavailable(err) || strings.Contains(message, "transport is closing") || strings.Contains(message, "connection refused") {
		return imageMechanicsError(ImageFailureEngineLoss, resolvedDigest, err)
	}
	if errors.Is(err, syscall.ENOSPC) || strings.Contains(message, "resource exhausted") || strings.Contains(message, "no space left") || strings.Contains(message, "snapshotter") {
		return imageMechanicsError(ImageFailureResourceExhausted, resolvedDigest, err)
	}
	return imageMechanicsError(ImageFailureUnavailable, resolvedDigest, err)
}

func (engine *ContainerdEngine) Run(ctx context.Context, request RunRequest) (_ RunResponse, runErr error) {
	ctx = engineContext(ctx)
	if err := os.MkdirAll(engine.config.RuntimeRoot, 0o700); err != nil {
		return RunResponse{}, err
	}
	admission, err := engine.admitResources(request)
	if err != nil {
		return RunResponse{}, err
	}
	lease, err := engine.client.LeasesService().Create(ctx, leases.WithID(request.Resources.LeaseID), leases.WithLabels(request.Resources.Labels))
	if err != nil {
		engine.releaseCapacityReservation(request.Authority.key())
		return RunResponse{}, fmt.Errorf("create attempt lease: %w", err)
	}
	leaseContext := leases.WithLease(ctx, lease.ID)
	created := true
	var computerDisk *computerDiskAttachment
	var hostBridge net.Listener
	var hostBridgeEndpoint string
	endpoints := make(map[string]uint16, len(request.AllocateEndpoints))
	endpointHolds := make(map[string]net.Listener, len(request.AllocateEndpoints))
	defer func() {
		var insufficientDisk *insufficientDiskError
		if errors.As(runErr, &insufficientDisk) {
			engine.releaseCapacityReservation(request.Authority.key())
			admission.MemoryCommittedAfterBytes = admission.MemoryCommittedBeforeBytes
			admission.DiskCommittedAfterBytes = admission.DiskCommittedBeforeBytes
			engine.recordAdmissionFailure(admission, CodeInsufficientDisk)
		}
		if runErr != nil && created {
			if hostBridge != nil {
				_ = hostBridge.Close()
			}
			for _, hold := range endpointHolds {
				_ = hold.Close()
			}
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			runErr = errors.Join(runErr, engine.deleteResources(cleanupCtx, request.Authority, request.Resources, computerDisk))
			verification, verifyErr := engine.Verify(cleanupCtx, VerifyRequest{Scope: VerifyAttempt, Authority: &request.Authority})
			runErr = errors.Join(runErr, verifyErr)
			// Service data volumes, retained handoff bindings, and Computer disk
			// resources outlive one attempt. Verify computes runtime absence from
			// that retention-aware projection while returning observed inventory.
			runtimeAbsent := verification.Absent
			if verifyErr == nil && runtimeAbsent {
				runErr = errors.Join(runErr, engine.releaseVerifiedAttempt(cleanupCtx, request.Authority.key()))
				for _, port := range endpoints {
					engine.releaseAttemptPort(port, request.Authority.key())
				}
			}
		}
	}()
	for _, name := range request.AllocateEndpoints {
		port, hold, reserveErr := engine.reserveAttemptPort(request.Authority.key())
		err = reserveErr
		if err != nil {
			return RunResponse{}, err
		}
		endpoints[name] = port
		endpointHolds[name] = hold
	}
	computer := request.Workload.Computer
	request.Workload.ReservedEnvironment = nil
	for _, volume := range request.Workload.ManagedVolumes {
		switch volume.Kind {
		case ManagedVolumeHandoff:
			request.Workload.ReservedEnvironment = setReservedEnvironment(request.Workload.ReservedEnvironment, contract.EnvHandoffDir, contract.OCIContainerHandoffDirectory)
		case ManagedVolumeServiceData, ManagedVolumeComputerDisk:
			request.Workload.ReservedEnvironment = setReservedEnvironment(request.Workload.ReservedEnvironment, contract.EnvServiceDir, contract.OCIContainerServiceDirectory)
		}
	}
	if request.Workload.L3Endpoint != "" && !contract.IsOCISensitiveReservedEnvironmentName(contract.EnvL3Endpoint) {
		request.Workload.ReservedEnvironment = setReservedEnvironment(request.Workload.ReservedEnvironment, contract.EnvL3Endpoint, request.Workload.L3Endpoint)
	}
	if request.Workload.RunToken != "" && contract.IsOCISensitiveReservedEnvironmentName(contract.EnvRunToken) {
		request.Workload.ReservedEnvironment = setReservedEnvironment(request.Workload.ReservedEnvironment, contract.EnvRunToken, request.Workload.RunToken)
	}
	if computer && request.Workload.ComputerToken != "" && contract.IsOCISensitiveReservedEnvironmentName(contract.EnvComputerToken) {
		request.Workload.ReservedEnvironment = setReservedEnvironment(request.Workload.ReservedEnvironment, contract.EnvComputerToken, request.Workload.ComputerToken)
	}
	if servicePort := endpoints["service"]; servicePort != 0 {
		request.Workload.ReservedEnvironment = setReservedEnvironment(
			request.Workload.ReservedEnvironment, contract.EnvServicePort, fmt.Sprint(servicePort),
		)
	}
	if computer {
		request.Workload.ReservedEnvironment, err = computerEndpointEnvironment(request.Workload.ReservedEnvironment, endpoints)
		if err != nil {
			return RunResponse{}, err
		}
	}
	if request.EnableHostBridgeFallback {
		hostBridge, err = net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			return RunResponse{}, fmt.Errorf("reserve constrained guest-to-host bridge: %w", err)
		}
		request.Workload.ReservedEnvironment, hostBridgeEndpoint, err = fallbackBridgeEnvironment(request.Workload.ReservedEnvironment, hostBridge.Addr(), request.ActivateHostBridgeFallback)
		if err != nil {
			return RunResponse{}, err
		}
		if request.ActivateHostBridgeFallback && request.Workload.L3Endpoint != "" {
			request.Workload.L3Endpoint = hostBridgeEndpoint
		}
	}
	request.Workload.helperMintedReserved = true

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
	logDirectory := filepath.Join(engine.config.RuntimeRoot, "logs", request.Resources.LogSegmentDirectory)
	var controlDirectory string
	if computer {
		controlDirectory, err = prepareComputerControlDirectory(logDirectory, mountComputerControlTmpfs, unmountComputerControlTmpfs)
		if err != nil {
			return RunResponse{}, err
		}
	}
	var document *RuntimeSpecDocument
	var computerUID, computerGID uint32
	err = mount.WithTempMount(leaseContext, mounts, func(root string) error {
		imageConfig, configErr := readImageRuntimeConfig(leaseContext, engine.client.ContentStore(), image)
		if configErr != nil {
			return &ImageUnavailableError{err: fmt.Errorf("read pinned image config: %w", configErr)}
		}
		if err := engine.refreshManagedNetworkFiles(); err != nil {
			return err
		}
		managedSources, freshServiceVolume, attachedDisk, managedErr := engine.managedVolumeSources(leaseContext, &request)
		if managedErr != nil {
			return managedErr
		}
		computerDisk = attachedDisk
		if computerDisk != nil {
			engine.markCapacityDiskMaterialized(request.Authority.JobID)
		}
		operatorSources, translateErr := TranslateOperatorMountSources(request.Workload, engine.translateOperatorMountSource)
		if translateErr != nil {
			return translateErr
		}
		document, configErr = BuildRuntimeSpec(leaseContext, RuntimeSpecInput{
			ContainerID: request.Resources.ContainerID, CgroupPath: "/" + request.Resources.CgroupID, RootfsPath: root,
			Image: imageConfig, Workload: request.Workload,
			Guest:        GuestKernelFacts{Architecture: runtime.GOARCH, KernelRelease: kernelRelease()},
			ResolverPath: engine.config.ResolverPath, HostsPath: engine.config.HostsPath,
			ManagedRoot: engine.config.RuntimeRoot, AllowedMountRoots: engine.config.AllowedMountRoots,
			ManagedVolumeSources: managedSources, ComputerControlSource: controlDirectory, OperatorMountSources: operatorSources,
		})
		if configErr == nil {
			uid, gid, ownerErr := document.ProcessOwner()
			if ownerErr != nil {
				closeErr := document.Close()
				document = nil
				return errors.Join(ownerErr, closeErr)
			}
			if servicePath, ok := managedSources[ManagedVolumeServiceData]; ok {
				if ownerErr = engine.initializeServiceVolume(servicePath, request.Resources.ServiceVolumeOwnerRecord, freshServiceVolume, uid, gid); ownerErr != nil {
					closeErr := document.Close()
					document = nil
					return errors.Join(ownerErr, closeErr)
				}
			}
			if computerDisk != nil {
				if ownerErr = initializeComputerDiskRoot(computerDisk, uid, gid, computerDisk.storage.Chown); ownerErr != nil {
					closeErr := document.Close()
					document = nil
					return errors.Join(ownerErr, closeErr)
				}
			}
			if computer {
				computerUID, computerGID = uid, gid
				if ownerErr = atomicWriteComputerL3Endpoint(controlDirectory, request.Workload.L3Endpoint, uid, gid); ownerErr != nil {
					closeErr := document.Close()
					document = nil
					return errors.Join(ownerErr, closeErr)
				}
				if ownerErr = atomicWriteComputerToken(controlDirectory, request.Workload.ComputerToken, uid, gid); ownerErr != nil {
					closeErr := document.Close()
					document = nil
					return errors.Join(ownerErr, closeErr)
				}
			}
		}
		return configErr
	})
	if err != nil {
		return RunResponse{}, err
	}
	defer document.Close()
	profile, err := document.ProfileReceipt()
	if err != nil {
		return RunResponse{}, &RuntimeSpecRejectionError{err: err}
	}
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
	if err := os.MkdirAll(logDirectory, 0o700); err != nil {
		return RunResponse{}, err
	}
	stdout := filepath.Join(logDirectory, "stdout.frames")
	stderr := filepath.Join(logDirectory, "stderr.frames")
	task, err := container.NewTask(leaseContext, binaryLogCreator(engine.config.LoggerExecutable, stdout, stderr))
	if err != nil {
		return RunResponse{}, fmt.Errorf("create runc v2 task with binary-v2 logs: %w", err)
	}
	if computer {
		memoryMax, oomGroup, swapMax, verifyErr := verifyComputerCgroupMemoryPolicy(engine.config.CgroupRoot, request.Resources.CgroupID, request.Workload.Limits.MemoryBytes)
		profile.MemoryMaxBytes = memoryMax
		profile.MemoryOOMGroup = oomGroup
		profile.MemorySwapMaxBytes = swapMax
		if verifyErr != nil {
			return RunResponse{}, &RuntimeSpecRejectionError{err: verifyErr}
		}
	}
	engine.mu.Lock()
	engine.lastProfile = &profile
	engine.mu.Unlock()
	attemptContext, attemptCancel := context.WithCancel(leases.WithLease(engineContext(context.Background()), lease.ID))
	wait, err := task.Wait(attemptContext)
	if err != nil {
		attemptCancel()
		return RunResponse{}, fmt.Errorf("register task Wait before Start: %w", err)
	}
	attempt := &containerdAttempt{authority: request.Authority, resources: request.Resources, computerDisk: computerDisk, container: container, task: task, signaler: task, releaseTask: func(ctx context.Context) error {
		_, deleteErr := task.Delete(engineContext(ctx))
		if errdefs.IsNotFound(deleteErr) {
			return nil
		}
		return deleteErr
	}, stdout: stdout, stderr: stderr, cancel: attemptCancel, terminalReady: make(chan struct{}), logAcknowledged: make(map[string]uint64), hostBridge: hostBridge, endpoints: endpoints, endpointHolds: endpointHolds, controlDirectory: controlDirectory, computerUID: computerUID, computerGID: computerGID}
	engine.watchOOM(attempt)
	engine.mu.Lock()
	engine.attempts[request.Authority.key()] = attempt
	engine.mu.Unlock()
	go attempt.cacheTerminal(wait, engine.config.CgroupRoot, engine.config.TaskReleaseTimeout)
	if err := document.RevalidateMounts(); err != nil {
		return RunResponse{}, &RuntimeSpecRejectionError{err: err}
	}
	// Transfer the kernel reservation directly into Start. The logical
	// authority remains retained until independently verified deletion.
	for name, hold := range endpointHolds {
		if err := hold.Close(); err != nil {
			return RunResponse{}, fmt.Errorf("transfer attempt endpoint %q reservation: %w", name, err)
		}
		delete(endpointHolds, name)
		attempt.mu.Lock()
		delete(attempt.endpointHolds, name)
		attempt.mu.Unlock()
	}
	if err := task.Start(leaseContext); err != nil {
		return RunResponse{}, fmt.Errorf("start runc v2 task: %w", err)
	}
	startedAt := time.Now().UTC().Round(0)
	created = false
	return RunResponse{Started: true, StartedAt: startedAt, Image: &evidence, Endpoints: endpoints, HostBridgeReady: hostBridge != nil, HostBridgeEndpoint: hostBridgeEndpoint, Profile: profile, Admission: admission}, nil
}

func (engine *ContainerdEngine) waitAttemptPortOwnership(ctx context.Context, cgroupID string, port uint16) error {
	for {
		inode, found, err := loopbackListenInode(port)
		if err != nil {
			return err
		}
		if found {
			owned, err := cgroupSubtreeOwnsSocket(ctx, filepath.Join(engine.config.CgroupRoot, cgroupID), inode)
			if err != nil {
				return err
			}
			if !owned {
				return errors.New("allocated port was bound by a process outside the attempt cgroup")
			}
			return nil
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func loopbackListenInode(port uint16) (string, bool, error) {
	file, err := os.Open("/proc/net/tcp")
	if err != nil {
		return "", false, err
	}
	defer file.Close()
	want := fmt.Sprintf("0100007F:%04X", port)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) > 9 && fields[1] == want && fields[3] == "0A" {
			return fields[9], true, nil
		}
	}
	return "", false, scanner.Err()
}

func cgroupSubtreeOwnsSocket(ctx context.Context, cgroupPath, inode string) (bool, error) {
	owned := false
	err := filepath.WalkDir(cgroupPath, func(path string, entry os.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) || errors.Is(walkErr, syscall.ESRCH) {
				return nil
			}
			return walkErr
		}
		if !entry.IsDir() {
			return nil
		}
		processOwnsSocket, err := cgroupOwnsSocket(filepath.Join(path, "cgroup.procs"), inode)
		if err != nil {
			return err
		}
		if processOwnsSocket {
			owned = true
			return filepath.SkipAll
		}
		return nil
	})
	return owned, err
}

func cgroupOwnsSocket(procsPath, inode string) (bool, error) {
	payload, err := os.ReadFile(procsPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
			return false, nil
		}
		return false, err
	}
	want := "socket:[" + inode + "]"
	for _, field := range strings.Fields(string(payload)) {
		if _, err := strconv.Atoi(field); err != nil {
			return false, fmt.Errorf("invalid cgroup pid %q", field)
		}
		entries, err := os.ReadDir(filepath.Join("/proc", field, "fd"))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
				continue
			}
			return false, err
		}
		for _, entry := range entries {
			target, err := os.Readlink(filepath.Join("/proc", field, "fd", entry.Name()))
			if err == nil && target == want {
				return true, nil
			}
		}
	}
	return false, nil
}

func setReservedEnvironment(environment []EnvironmentVariable, name, value string) []EnvironmentVariable {
	result := append([]EnvironmentVariable(nil), environment...)
	for index := range result {
		if result[index].Name == name {
			result[index].Value = value
			return result
		}
	}
	return append(result, EnvironmentVariable{Name: name, Value: value})
}

func removeReservedEnvironment(environment []EnvironmentVariable, name string) []EnvironmentVariable {
	return slices.DeleteFunc(append([]EnvironmentVariable(nil), environment...), func(variable EnvironmentVariable) bool {
		return variable.Name == name
	})
}

func computerEndpointEnvironment(environment []EnvironmentVariable, endpoints map[string]uint16) ([]EnvironmentVariable, error) {
	view, control := endpoints[contract.ComputerDisplayEndpointView], endpoints[contract.ComputerDisplayEndpointControl]
	if view == 0 || control == 0 || view == control || len(endpoints) != 2 {
		return nil, errors.New("Computer endpoint allocation must be exactly distinct view and control ports")
	}
	result := removeReservedEnvironment(environment, contract.EnvServicePort)
	result = setReservedEnvironment(result, contract.EnvComputerViewPort, fmt.Sprint(view))
	result = setReservedEnvironment(result, contract.EnvComputerControlPort, fmt.Sprint(control))
	return result, nil
}

func mountComputerControlTmpfs(path string) error {
	return unix.Mount("tmpfs", path, "tmpfs", uintptr(unix.MS_NODEV|unix.MS_NOSUID|unix.MS_NOEXEC), "mode=0755,size=64k")
}

func unmountComputerControlTmpfs(path string) error {
	err := unix.Unmount(path, 0)
	if err == nil || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOENT) {
		return nil
	}
	if errors.Is(err, unix.EBUSY) {
		return unix.Unmount(path, unix.MNT_DETACH)
	}
	return err
}

func (engine *ContainerdEngine) reserveAttemptPort(authorityKey string) (uint16, net.Listener, error) {
	width := int(engine.config.AttemptPortMax-engine.config.AttemptPortMin) + 1
	for count := 0; count < width; count++ {
		engine.mu.Lock()
		port := engine.nextPort
		engine.nextPort++
		if engine.nextPort > engine.config.AttemptPortMax || engine.nextPort < engine.config.AttemptPortMin {
			engine.nextPort = engine.config.AttemptPortMin
		}
		_, occupied := engine.ports[port]
		if !occupied {
			engine.ports[port] = authorityKey
		}
		engine.mu.Unlock()
		if occupied {
			continue
		}
		probe, err := net.Listen("tcp4", net.JoinHostPort("127.0.0.1", fmt.Sprint(port)))
		if err == nil {
			return port, probe, nil
		}
		engine.releaseAttemptPort(port, authorityKey)
	}
	return 0, nil, errors.New("OCI attempt port range is exhausted")
}

func (engine *ContainerdEngine) releaseAttemptPort(port uint16, authorityKey string) {
	if port == 0 {
		return
	}
	engine.mu.Lock()
	if engine.ports[port] == authorityKey {
		delete(engine.ports, port)
	}
	engine.mu.Unlock()
}

func (engine *ContainerdEngine) translateOperatorMountSource(source string) (string, error) {
	if engine.config.HostMountRoot == "" {
		return IdentityOperatorMountSource(source)
	}
	clean := filepath.Clean(source)
	relative, err := filepath.Rel(engine.config.HostMountRoot, clean)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", errors.New("operator mount source is outside the configured Lima host mount root")
	}
	translated := filepath.Join(engine.config.GuestMountRoot, relative)
	if translated == engine.config.GuestMountRoot || !strings.HasPrefix(translated, engine.config.GuestMountRoot+string(filepath.Separator)) {
		return "", errors.New("operator mount source translation escaped the configured Lima guest mount root")
	}
	return translated, nil
}

func fallbackBridgeEnvironment(environment []EnvironmentVariable, address net.Addr, activate bool) ([]EnvironmentVariable, string, error) {
	index := -1
	endpoint := &url.URL{Scheme: "http", Host: address.String(), Path: "/l3"}
	for position, variable := range environment {
		if variable.Name != contract.EnvL3Endpoint {
			continue
		}
		parsed, err := url.Parse(variable.Value)
		if err != nil || parsed.Scheme != "http" || parsed.Host == "" {
			return nil, "", errors.New("Lima host bridge fallback requires a valid HTTP WEFTY_L3_ENDPOINT")
		}
		index, endpoint = position, parsed
		break
	}
	if index < 0 {
		return environment, endpoint.String(), nil
	}
	endpoint.Host = address.String()
	if !activate {
		return environment, endpoint.String(), nil
	}
	result := append([]EnvironmentVariable(nil), environment...)
	result[index].Value = endpoint.String()
	return result, endpoint.String(), nil
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
	signaler := attempt.signaler
	if signaler == nil {
		signaler = attempt.task
	}
	return deliverSignalAndRecord(request.Signal, "agent", func() error {
		return normalizeContainerdSignalError(signaler.Kill(engineContext(ctx), signal, containerd.WithKillAll))
	}, func(delivered Signal, cause string) {
		attempt.mu.Lock()
		attempt.signal = delivered
		attempt.signalCause = cause
		attempt.mu.Unlock()
	})
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
	oom := attempt.oom || cgroupReportedOOM(engine.config.CgroupRoot, attempt.resources.CgroupID)
	attempt.mu.Unlock()
	result := attempt.terminalResult(oom, logIncomplete)
	return emit(WatchEvent{Kind: WatchComplete, Result: &result})
}

func (attempt *containerdAttempt) terminalResult(oom, logIncomplete bool) WatchResponse {
	attempt.mu.Lock()
	sentSignal, signalCause := attempt.signal, attempt.signalCause
	code, waitErr := attempt.terminalCode, attempt.terminalErr
	attempt.mu.Unlock()
	// A post-hoc free-byte sample cannot prove that this attempt observed
	// ENOSPC. Until a positive guest/runtime event is available, retain the
	// filesystem sample only in the admission receipt and leave this fact absent.
	// containerd ExitStatus exposes only a numeric code. Therefore 137/143 are
	// plain exits unless this helper independently observed successful delivery
	// of the matching signal.
	return terminalResultFromSignalDelivery(code, waitErr, sentSignal, signalCause, oom, logIncomplete)
}

func (attempt *containerdAttempt) cacheTerminal(wait <-chan containerd.ExitStatus, cgroupRoot string, releaseTimeout time.Duration) {
	exit, ok := <-wait
	attempt.mu.Lock()
	if !ok {
		attempt.terminalErr = errors.New("containerd task Wait closed without exit status")
	} else {
		attempt.terminalCode, _, attempt.terminalErr = exit.Result()
	}
	if cgroupReportedOOM(cgroupRoot, attempt.resources.CgroupID) {
		attempt.oom = true
	}
	attempt.mu.Unlock()
	// Task.Wait was registered with the attempt lease context before Start.
	// Release that completed wait and its lease-scoped client work before
	// Task.Delete asks containerd to drain the shim logger. Leaving the context
	// live made every ordinary task deletion consume the full release bound.
	if attempt.cancel != nil {
		attempt.cancel()
	}
	if err := publishTerminalAfterTaskRelease(releaseTimeout, attempt.releaseTask, attempt.terminalReady); err != nil {
		log.Printf("release exited OCI task %s before log sealing: %v", attempt.authority.AttemptID, err)
	}
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
	resources, identityErr := DeterministicResourceIdentity(request.Authority)
	if identityErr != nil {
		return DeleteResponse{}, identityErr
	}
	attempt, err := engine.attempt(request.Authority)
	if err != nil && !errdefs.IsNotFound(err) {
		return DeleteResponse{}, err
	}
	if attempt != nil {
		resources = attempt.resources
	}
	var computerDisk *computerDiskAttachment
	if attempt != nil {
		computerDisk = attempt.computerDisk
	}
	cleanupCtx, cancel := context.WithTimeout(engineContext(ctx), 10*time.Second)
	defer cancel()
	var lastErr error
	for {
		// A missing in-memory attempt is not absence proof. Validate every
		// label-capable resource that still occupies the deterministic manifest
		// identity before any NotFound result can participate in deletion. Keep
		// this inside the retry budget because containerd inspection is transient.
		identityErr := engine.validateAttemptResourceIdentity(cleanupCtx, request.Authority, resources)
		if identityErr == nil {
			deleteErr := engine.deleteResources(cleanupCtx, request.Authority, resources, computerDisk)
			verification, verifyErr := engine.Verify(cleanupCtx, VerifyRequest{Scope: VerifyAttempt, Authority: &request.Authority})
			// Service data volumes and retained owner-key handoff bindings are
			// finalized separately from an attempt. Verify keeps them visible in
			// inventory while projecting only unexpired bindings from absence.
			runtimeAbsent := verification.Absent
			if deleteErr == nil && verifyErr == nil && runtimeAbsent {
				if releaseErr := engine.releaseVerifiedAttempt(cleanupCtx, request.Authority.key()); releaseErr == nil {
					return DeleteResponse{Deleted: true}, nil
				} else {
					lastErr = releaseErr
				}
			} else {
				lastErr = errors.Join(deleteErr, verifyErr)
			}
			if lastErr == nil {
				lastErr = fmt.Errorf("attempt resources remain after delete: %+v", verification.Inventory)
			}
		} else {
			lastErr = identityErr
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

func (engine *ContainerdEngine) validateAttemptResourceIdentity(ctx context.Context, authority AttemptAuthority, resources ResourceIdentity) error {
	expected, err := DeterministicResourceIdentity(authority)
	if err != nil {
		return err
	}
	// Handoff volumes are keyed by the stable owner key, not attempt
	// authority. The authenticated Run request derived this live name before
	// the engine retained it, so compare the remaining authority-derived names.
	expected.HandoffVolumeDirectory = resources.HandoffVolumeDirectory
	if !sameRuntimeResourceNames(resources, expected) {
		return errors.New("attempt runtime resource names do not match fenced authority")
	}
	container, err := engine.client.LoadContainer(ctx, resources.ContainerID)
	if err == nil {
		info, infoErr := container.Info(ctx)
		if infoErr != nil {
			return fmt.Errorf("inspect container identity before deletion: %w", infoErr)
		}
		if err := validateRuntimeResourceLabels("container", info.ID, resources.ContainerID, info.Labels, authority); err != nil {
			return err
		}
	} else if !errdefs.IsNotFound(err) {
		return fmt.Errorf("load container identity before deletion: %w", err)
	}

	info, err := engine.client.SnapshotService(DefaultSnapshotter).Stat(ctx, resources.SnapshotID)
	if err == nil {
		if err := validateRuntimeResourceLabels("snapshot", info.Name, resources.SnapshotID, info.Labels, authority); err != nil {
			return err
		}
	} else if !errdefs.IsNotFound(err) {
		return fmt.Errorf("inspect snapshot identity before deletion: %w", err)
	}

	leaseList, err := engine.client.LeasesService().List(ctx, "id=="+resources.LeaseID)
	if err != nil {
		return fmt.Errorf("list lease identity before deletion: %w", err)
	}
	for _, lease := range leaseList {
		if lease.ID != resources.LeaseID {
			continue
		}
		if err := validateRuntimeResourceLabels("lease", lease.ID, resources.LeaseID, lease.Labels, authority); err != nil {
			return err
		}
		break
	}
	return nil
}

func sameRuntimeResourceNames(left, right ResourceIdentity) bool {
	return left.LeaseID == right.LeaseID && left.SnapshotID == right.SnapshotID &&
		left.ContainerID == right.ContainerID && left.TaskID == right.TaskID &&
		left.ShimID == right.ShimID && left.CgroupID == right.CgroupID &&
		left.LogSegmentDirectory == right.LogSegmentDirectory &&
		left.HandoffVolumeDirectory == right.HandoffVolumeDirectory &&
		left.ServiceVolumeDirectory == right.ServiceVolumeDirectory &&
		left.ServiceVolumeOwnerRecord == right.ServiceVolumeOwnerRecord
}

func validateRuntimeResourceLabels(kind, observedID, expectedID string, labels map[string]string, authority AttemptAuthority) error {
	if observedID != expectedID {
		return fmt.Errorf("%s %q does not match removal manifest identity %q", kind, observedID, expectedID)
	}
	observed, err := authorityFromLabels(labels)
	if err != nil {
		return fmt.Errorf("validate %s %s labels before deletion: %w", kind, observedID, err)
	}
	if observed.key() != authority.key() {
		return fmt.Errorf("%s %s labels do not match removal authority", kind, observedID)
	}
	return nil
}

func (engine *ContainerdEngine) DeleteManagedVolume(ctx context.Context, request DeleteManagedVolumeRequest) (DeleteManagedVolumeResponse, error) {
	switch request.Kind {
	case ManagedVolumeComputerDisk:
		if request.ComputerStorage == nil || request.Removal == nil {
			return DeleteManagedVolumeResponse{}, errors.New("Computer disk deletion requires Storage and removal authority")
		}
		if err := engine.deleteComputerDisk(*request.ComputerStorage, *request.Removal); err != nil {
			if request.QuarantineOnFailure && request.FailureAttempts > 0 {
				receipt, quarantineErr := engine.quarantineComputerDiskCleanup(request)
				if quarantineErr != nil {
					return DeleteManagedVolumeResponse{}, errors.Join(err, quarantineErr)
				}
				return DeleteManagedVolumeResponse{Quarantine: &receipt}, nil
			}
			return DeleteManagedVolumeResponse{}, err
		}
		return DeleteManagedVolumeResponse{Deleted: true}, nil
	case ManagedVolumeHandoff:
		name, err := DeterministicHandoffVolumeDirectory(request.OwnerKey)
		if err != nil {
			return DeleteManagedVolumeResponse{}, err
		}
		path := filepath.Join(engine.config.RuntimeRoot, "handoffs", name)
		if err := os.RemoveAll(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return DeleteManagedVolumeResponse{}, err
		}
		if err := requirePathAbsent(path, "handoff managed volume"); err != nil {
			return DeleteManagedVolumeResponse{}, err
		}
		return DeleteManagedVolumeResponse{Deleted: true}, nil
	case ManagedVolumeServiceData:
		if request.Removal == nil || request.Removal.JobID != request.OwnerKey || request.Removal.NodeID == "" || request.Removal.BootSessionID == "" || request.Removal.RemovalGeneration == 0 || request.Removal.CleanupFence == "" {
			return DeleteManagedVolumeResponse{}, errors.New("service data deletion requires exact removal authority")
		}
		if err := engine.deleteServiceDataVolume(request.OwnerKey); err != nil {
			return DeleteManagedVolumeResponse{}, err
		}
		return DeleteManagedVolumeResponse{Deleted: true}, nil
	default:
		return DeleteManagedVolumeResponse{}, fmt.Errorf("managed volume kind %q cannot be finalized", request.Kind)
	}
}

func (engine *ContainerdEngine) InventoryRemoval(ctx context.Context, request InventoryRemovalRequest) (InventoryRemovalResponse, error) {
	ctx = engineContext(ctx)
	inventory, err := engine.inventory(ctx)
	if err != nil {
		return InventoryRemovalResponse{}, err
	}
	authorities := make(map[string]AttemptAuthority)
	add := func(authority AttemptAuthority, kind, observedID string) error {
		if authority.JobID != request.Removal.JobID {
			return nil
		}
		if authority.NodeID != request.Removal.NodeID || authority.Class != contract.JobClassService || authority.RemovalGeneration != fmt.Sprint(request.Removal.RemovalGeneration) {
			return fmt.Errorf("legacy removal found %s %q with conflicting authority", kind, observedID)
		}
		identity, identityErr := DeterministicResourceIdentity(authority)
		if identityErr != nil {
			return identityErr
		}
		var expectedID string
		switch kind {
		case "lease":
			expectedID = identity.LeaseID
		case "snapshot":
			expectedID = identity.SnapshotID
		case "container":
			expectedID = identity.ContainerID
		case "computer":
			expectedID = observedID
		default:
			return fmt.Errorf("legacy removal inventory class %q is unsupported", kind)
		}
		if expectedID != observedID {
			return fmt.Errorf("legacy removal %s %q does not match deterministic authority %q", kind, observedID, expectedID)
		}
		authorities[authority.key()] = authority
		return nil
	}
	leases, err := engine.client.LeasesService().List(ctx)
	if err != nil {
		return InventoryRemovalResponse{}, err
	}
	for _, lease := range leases {
		if !strings.HasPrefix(lease.ID, "wefty-lease-") {
			continue
		}
		authority, labelErr := authorityFromLabels(lease.Labels)
		if labelErr != nil {
			return InventoryRemovalResponse{}, labelErr
		}
		if err := add(authority, "lease", lease.ID); err != nil {
			return InventoryRemovalResponse{}, err
		}
	}
	if err := engine.client.SnapshotService(DefaultSnapshotter).Walk(ctx, func(_ context.Context, info snapshots.Info) error {
		if !strings.HasPrefix(info.Name, "wefty-snapshot-") {
			return nil
		}
		authority, labelErr := authorityFromLabels(info.Labels)
		if labelErr != nil {
			return labelErr
		}
		return add(authority, "snapshot", info.Name)
	}); err != nil {
		return InventoryRemovalResponse{}, err
	}
	containers, err := engine.client.Containers(ctx)
	if err != nil {
		return InventoryRemovalResponse{}, err
	}
	for _, container := range containers {
		if !strings.HasPrefix(container.ID(), "wefty-container-") {
			continue
		}
		info, infoErr := container.Info(ctx)
		if infoErr != nil {
			return InventoryRemovalResponse{}, infoErr
		}
		authority, labelErr := authorityFromLabels(info.Labels)
		if labelErr != nil {
			return InventoryRemovalResponse{}, labelErr
		}
		if err := add(authority, "container", info.ID); err != nil {
			return InventoryRemovalResponse{}, err
		}
	}
	var computerStorage *ComputerStorageReference
	if request.ComputerStorage != nil {
		name, nameErr := DeterministicComputerDiskName(*request.ComputerStorage)
		if nameErr != nil {
			return InventoryRemovalResponse{}, nameErr
		}
		for _, anomaly := range inventory.ComputerDiskAnomalies {
			if strings.HasPrefix(anomaly, name+":") {
				return InventoryRemovalResponse{}, fmt.Errorf("legacy Computer removal inventory is anomalous: %s", anomaly)
			}
		}
		root := filepath.Join(engine.config.RuntimeRoot, "computer-disks", name)
		manifest, present, manifestErr := readComputerDiskManifest(filepath.Join(root, "attachment.json"))
		if manifestErr != nil || !present {
			return InventoryRemovalResponse{}, errors.Join(manifestErr, errors.New("legacy Computer removal has no readable disk manifest"))
		}
		if !sameComputerStorageIdentity(manifest.Storage, *request.ComputerStorage) {
			return InventoryRemovalResponse{}, errors.New("legacy Computer removal disk manifest does not match L1 Storage identity")
		}
		storage := manifest.Storage
		computerStorage = &storage
		for _, authority := range []*AttemptAuthority{manifest.Attached, manifest.Pending} {
			if authority != nil {
				if err := add(*authority, "computer", name); err != nil {
					return InventoryRemovalResponse{}, err
				}
			}
		}
	}
	if len(authorities) == 0 {
		return InventoryRemovalResponse{}, errors.New("legacy OCI removal current scan found no matching helper-owned attempt authority")
	}
	attempts := make([]RemovalAttemptManifest, 0, len(authorities))
	for _, authority := range authorities {
		identity, identityErr := DeterministicResourceIdentity(authority)
		if identityErr != nil {
			return InventoryRemovalResponse{}, identityErr
		}
		attempts = append(attempts, RemovalAttemptManifest{
			Authority: authority, ComputerStorage: computerStorage,
			Resources: ExpectedRemovalResources(identity, "", computerStorage),
		})
	}
	sort.Slice(attempts, func(i, j int) bool { return attempts[i].Authority.AttemptID < attempts[j].Authority.AttemptID })
	return InventoryRemovalResponse{Attempts: attempts}, nil
}

func (engine *ContainerdEngine) deleteServiceDataVolume(jobID string) error {
	name, err := DeterministicServiceVolumeDirectory(jobID)
	if err != nil {
		return err
	}
	engine.serviceVolumeMu.Lock()
	defer engine.serviceVolumeMu.Unlock()
	volumeRoot := filepath.Join(engine.config.RuntimeRoot, "service-data")
	stateRoot := filepath.Join(engine.config.RuntimeRoot, "service-data-state")
	volumePath := filepath.Join(volumeRoot, name)
	recordPath := filepath.Join(stateRoot, name+".owner")
	info, statErr := os.Lstat(volumePath)
	if statErr == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("service data volume is not a helper-owned directory")
		}
		payload, readErr := os.ReadFile(recordPath)
		if readErr != nil {
			return fmt.Errorf("read service data owner record before deletion: %w", readErr)
		}
		var record serviceVolumeOwnerRecord
		if json.Unmarshal(payload, &record) != nil || record.Version != 1 {
			return errors.New("service data owner record is invalid before deletion")
		}
		file, openErr := os.OpenFile(volumePath, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
		if openErr != nil {
			return fmt.Errorf("open service data volume before deletion: %w", openErr)
		}
		var stat unix.Stat_t
		inspectErr := unix.Fstat(int(file.Fd()), &stat)
		closeErr := file.Close()
		if inspectErr != nil || closeErr != nil {
			return errors.Join(inspectErr, closeErr)
		}
		if record.Device != uint64(stat.Dev) || record.Inode != stat.Ino {
			return errors.New("service data owner record does not match the directory selected for deletion")
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	if err := os.RemoveAll(volumePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := requirePathAbsent(volumePath, "service data volume"); err != nil {
		return err
	}
	if err := os.Remove(recordPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := requirePathAbsent(recordPath, "service data owner record"); err != nil {
		return err
	}
	return errors.Join(syncDirectoryIfPresent(volumeRoot), syncDirectoryIfPresent(stateRoot))
}

func requirePathAbsent(path, description string) error {
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("%s remains after deletion", description)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func syncDirectoryIfPresent(path string) error {
	directory, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func (engine *ContainerdEngine) AttestRemoval(ctx context.Context, request AttestRemovalRequest) (AttestRemovalResponse, error) {
	if len(request.Attempts) == 0 {
		return AttestRemovalResponse{}, errors.New("removal attestation requires reconstructed attempts")
	}
	inventory, err := engine.inventory(engineContext(ctx))
	if err != nil {
		return AttestRemovalResponse{}, err
	}
	return attestRemovalInventory(inventory, request)
}

func attestRemovalInventory(inventory ResourceInventory, request AttestRemovalRequest) (AttestRemovalResponse, error) {
	seen := make(map[RemovalResource]struct{})
	assertions := make([]RemovalAssertion, 0)
	for _, attempt := range request.Attempts {
		for _, resource := range attempt.Resources {
			if _, duplicate := seen[resource]; duplicate {
				continue
			}
			if err := assertRemovalResourceAbsent(inventory, resource); err != nil {
				return AttestRemovalResponse{}, err
			}
			seen[resource] = struct{}{}
			assertions = append(assertions, RemovalAssertion{Class: resource.Class, ID: resource.ID, Absent: true})
		}
	}
	return AttestRemovalResponse{Assertions: assertions}, nil
}

func assertRemovalResourceAbsent(inventory ResourceInventory, resource RemovalResource) error {
	var values []string
	contains := func(value string) bool { return value == resource.ID }
	computerClass := false
	switch resource.Class {
	case RemovalResourceLease:
		values = inventory.Leases
	case RemovalResourceSnapshot:
		values = inventory.Snapshots
	case RemovalResourceContainer:
		values = inventory.Containers
	case RemovalResourceTask:
		values = inventory.Tasks
	case RemovalResourceShim:
		values = inventory.Shims
	case RemovalResourceCgroup:
		values = inventory.Cgroups
		contains = func(value string) bool { return value == resource.ID || strings.Contains(value, resource.ID) }
	case RemovalResourceLogSegments:
		values = inventory.LogSegments
	case RemovalResourceHandoffVolume, RemovalResourceServiceData:
		values = inventory.ManagedVolumes
	case RemovalResourceServiceDataRecord:
		values = inventory.ManagedVolumeRecords
	case RemovalResourceComputerDiskImage:
		computerClass = true
		values = inventory.ComputerDiskImages
	case RemovalResourceComputerDiskAllocation:
		computerClass = true
		values = inventory.ComputerDiskAllocations
	case RemovalResourceComputerDiskQuota:
		computerClass = true
		values = inventory.ComputerDiskQuotas
	case RemovalResourceComputerDiskManifest:
		computerClass = true
		values = inventory.ComputerDiskManifests
	case RemovalResourceComputerDiskMount:
		computerClass = true
		values = inventory.ComputerDiskMounts
	case RemovalResourceComputerDiskLoop:
		computerClass = true
		values = inventory.ComputerDiskLoops
	case RemovalResourceComputerAttachment:
		computerClass = true
		values = inventory.ComputerAttachments
	case RemovalResourceComputerResetManifest:
		values = inventory.ComputerResetManifests
	case RemovalResourceComputerQuarantine:
		values = inventory.ComputerQuarantines
	default:
		return fmt.Errorf("removal resource class %q is unsupported", resource.Class)
	}
	if computerClass {
		for _, anomaly := range inventory.ComputerDiskAnomalies {
			if strings.HasPrefix(anomaly, resource.ID+":") {
				return fmt.Errorf("removal resource %s/%s has inventory anomaly %q", resource.Class, resource.ID, anomaly)
			}
		}
	}
	for _, value := range values {
		if contains(value) {
			return fmt.Errorf("removal resource %s/%s remains", resource.Class, resource.ID)
		}
	}
	return nil
}

func (engine *ContainerdEngine) ReapAttempt(ctx context.Context, authority AttemptAuthority) error {
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		return err
	}
	engine.mu.Lock()
	var computerDisk *computerDiskAttachment
	if attempt := engine.attempts[authority.key()]; attempt != nil {
		resources = attempt.resources
		computerDisk = attempt.computerDisk
	}
	engine.mu.Unlock()
	return engine.deleteResources(engineContext(ctx), authority, resources, computerDisk)
}

func (engine *ContainerdEngine) ReapSession(ctx context.Context, _ SessionIdentity) error {
	if err := engine.imageOperations.CancelAll(ctx); err != nil {
		return err
	}
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
		engine.mu.Lock()
		var computerDisk *computerDiskAttachment
		if attempt := engine.attempts[request.Authority.key()]; attempt != nil {
			resources = attempt.resources
			computerDisk = attempt.computerDisk
		}
		engine.mu.Unlock()
		inventory = filterInventory(inventory, resources, computerDisk)
	}
	observed := inventory
	projected, err := engine.runtimeAbsenceInventory(observed, time.Now())
	if err != nil {
		return VerifyResponse{}, err
	}
	absent := InventoryEmpty(projected)
	if absent && request.Scope == VerifyNamespace {
		engine.releaseVerifiedNamespace()
	}
	return VerifyResponse{Absent: absent, Inventory: observed}, nil
}

func (engine *ContainerdEngine) Sweep(ctx context.Context, request SweepRequest) (SweepResponse, error) {
	ctx = engineContext(ctx)
	if err := engine.cleanupExpiredHandoffs(time.Now()); err != nil {
		return SweepResponse{}, err
	}
	if err := engine.sweepImageSpools(); err != nil {
		return SweepResponse{}, err
	}
	engine.mu.Lock()
	for _, attempt := range engine.attempts {
		if attempt.cancel != nil {
			attempt.cancel()
		}
		attempt.mu.Lock()
		if attempt.oomCancel != nil {
			attempt.oomCancel()
		}
		if attempt.hostBridge != nil {
			_ = attempt.hostBridge.Close()
			attempt.hostBridge = nil
		}
		attempt.mu.Unlock()
	}
	engine.mu.Unlock()
	inventory, err := engine.inventory(ctx)
	if err != nil {
		return SweepResponse{}, err
	}
	inventory = withoutServiceDataInventory(inventory)
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
	if err := engine.sweepComputerDisks(request.SweepEpoch); err != nil {
		return SweepResponse{}, err
	}
	return engine.finishSweep(ctx, inventory, prior, attempts)
}

func (engine *ContainerdEngine) cleanupExpiredHandoffs(now time.Time) error {
	root := filepath.Join(engine.config.RuntimeRoot, "handoffs")
	entries, err := readDirectoryIfPresent(root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "wefty-handoff-volume-") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if now.Sub(info.ModTime()) < engine.config.HandoffRetention {
			continue
		}
		if err := os.RemoveAll(filepath.Join(root, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (engine *ContainerdEngine) sweepImageSpools() error {
	engine.imageResourceMu.Lock()
	defer engine.imageResourceMu.Unlock()
	imports := filepath.Join(engine.config.RuntimeRoot, "imports")
	entries, readErr := os.ReadDir(imports)
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		return readErr
	}
	for _, entry := range entries {
		spool := filepath.Join(imports, entry.Name())
		if _, live := engine.activeSpools[spool]; live {
			continue
		}
		if err := os.RemoveAll(spool); err != nil {
			return err
		}
	}
	return nil
}

func (engine *ContainerdEngine) finishSweep(ctx context.Context, inventory ResourceInventory, prior map[string]struct{}, attempts map[string]SweptAttemptAuthority) (SweepResponse, error) {
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
		if strings.HasPrefix(lease.ID, "wefty-image-") {
			engine.imageResourceMu.Lock()
			_, live := engine.activeLeases[lease.ID]
			engine.imageResourceMu.Unlock()
			if live {
				continue
			}
			if err := engine.client.LeasesService().Delete(ctx, lease); err != nil && !errdefs.IsNotFound(err) {
				return SweepResponse{}, err
			}
			continue
		}
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
		logDirectory := filepath.Join(engine.config.RuntimeRoot, "logs", identity.LogSegmentDirectory)
		if err := unmountComputerControlTmpfs(filepath.Join(logDirectory, "control")); err != nil {
			return SweepResponse{}, err
		}
		for _, path := range []string{
			logDirectory,
			// M3 handoffs now live under the durable handoffs root. Sweep this
			// legacy attempt-scoped location so upgrades do not strand residue.
			filepath.Join(engine.config.RuntimeRoot, "handoffs", identity.HandoffVolumeDirectory),
		} {
			if err := os.RemoveAll(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return SweepResponse{}, err
			}
		}
	}
	priorList := mapKeys(prior)
	attemptList := make([]SweptAttemptAuthority, 0, len(attempts))
	for _, attempt := range attempts {
		attemptList = append(attemptList, attempt)
	}
	sort.Slice(attemptList, func(i, j int) bool { return attemptList[i].AttemptID < attemptList[j].AttemptID })
	return SweepResponse{Removed: inventoryCount(withoutRetainedBindingInventory(inventory)), PriorBootSessionsSeen: priorList, Inventory: inventory, Attempts: attemptList}, nil
}

func (engine *ContainerdEngine) DialAttemptPort(ctx context.Context, request DialAttemptPortRequest, stream io.ReadWriteCloser) error {
	if request.CgroupID != "" {
		bindContext, cancelBind := context.WithTimeout(ctx, engine.config.AttemptPortBindTimeout)
		err := engine.waitAttemptPortOwnership(bindContext, request.CgroupID, request.Port)
		cancelBind()
		if err != nil {
			return fmt.Errorf("verify payload attempt endpoint ownership: %w", err)
		}
	}
	dialer := &net.Dialer{}
	backend, err := dialer.DialContext(ctx, "tcp4", net.JoinHostPort("127.0.0.1", fmt.Sprint(request.Port)))
	if err != nil {
		return fmt.Errorf("dial attempt loopback port: %w", err)
	}
	defer backend.Close()
	if _, err := stream.Write([]byte{attemptPortBackendReady}); err != nil {
		return fmt.Errorf("confirm attempt loopback connection: %w", err)
	}
	return Relay(ctx, stream, backend)
}

func (engine *ContainerdEngine) SetComputerControlState(_ context.Context, request SetComputerControlStateRequest) error {
	attempt, err := engine.attempt(request.Authority)
	if err != nil {
		return err
	}
	attempt.controlMu.Lock()
	defer attempt.controlMu.Unlock()
	if attempt.controlDirectory == "" {
		return errors.New("attempt has no Computer control state")
	}
	return atomicWriteComputerControlState(attempt.controlDirectory, request.HumanDriving)
}

func (engine *ContainerdEngine) SetComputerToken(_ context.Context, request SetComputerTokenRequest) error {
	if strings.IndexByte(request.Token, 0) >= 0 || strings.IndexByte(request.L3Endpoint, 0) >= 0 {
		return errors.New("Computer submission authority contains NUL")
	}
	if (request.Token == "") != (request.L3Endpoint == "") {
		return errors.New("Computer token and L3 endpoint must be published or removed together")
	}
	attempt, err := engine.attempt(request.Authority)
	if err != nil {
		return err
	}
	attempt.controlMu.Lock()
	defer attempt.controlMu.Unlock()
	if attempt.controlDirectory == "" {
		return errors.New("attempt has no Computer control state")
	}
	if request.Token == "" {
		return errors.Join(
			atomicWriteComputerToken(attempt.controlDirectory, "", attempt.computerUID, attempt.computerGID),
			atomicWriteComputerL3Endpoint(attempt.controlDirectory, "", attempt.computerUID, attempt.computerGID),
		)
	}
	if err := atomicWriteComputerL3Endpoint(attempt.controlDirectory, request.L3Endpoint, attempt.computerUID, attempt.computerGID); err != nil {
		return err
	}
	if err := atomicWriteComputerToken(attempt.controlDirectory, request.Token, attempt.computerUID, attempt.computerGID); err != nil {
		return errors.Join(err, atomicWriteComputerL3Endpoint(attempt.controlDirectory, "", attempt.computerUID, attempt.computerGID))
	}
	return nil
}
func (engine *ContainerdEngine) DialHostBridge(ctx context.Context, request DialHostBridgeRequest, stream io.ReadWriteCloser) error {
	attempt, err := engine.attempt(request.Authority)
	if err != nil {
		return err
	}
	attempt.mu.Lock()
	listener, ok := attempt.hostBridge.(*net.TCPListener)
	attempt.mu.Unlock()
	if !ok || listener == nil {
		return errors.New("attempt has no guest-to-host bridge fallback")
	}
	var guest net.Conn
	for guest == nil {
		if err := listener.SetDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
			return err
		}
		guest, err = listener.Accept()
		if err == nil {
			break
		}
		var timeout net.Error
		if !errors.As(err, &timeout) || !timeout.Timeout() {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	_ = listener.SetDeadline(time.Time{})
	defer guest.Close()
	return Relay(ctx, stream, guest)
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

func (engine *ContainerdEngine) deleteResources(ctx context.Context, authority AttemptAuthority, resources ResourceIdentity, computerDisk *computerDiskAttachment) error {
	engine.mu.Lock()
	attempt := engine.attempts[authority.key()]
	engine.mu.Unlock()
	var failures []error
	var container containerd.Container
	if attempt != nil {
		if attempt.cancel != nil {
			attempt.cancel()
		}
		attempt.mu.Lock()
		if attempt.hostBridge != nil {
			_ = attempt.hostBridge.Close()
			attempt.hostBridge = nil
		}
		for name, hold := range attempt.endpointHolds {
			_ = hold.Close()
			delete(attempt.endpointHolds, name)
		}
		attempt.mu.Unlock()
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
	if computerDisk != nil {
		if err := engine.detachComputerDisk(computerDisk, computerDiskReapReceipt, ""); err != nil {
			failures = append(failures, err)
		}
	}
	lease := leases.Lease{ID: resources.LeaseID}
	if err := engine.client.LeasesService().Delete(ctx, lease); err != nil && !errdefs.IsNotFound(err) {
		failures = append(failures, err)
	}
	for _, path := range []string{
		filepath.Join(engine.config.RuntimeRoot, "logs", resources.LogSegmentDirectory),
	} {
		if err := unmountComputerControlTmpfs(filepath.Join(path, "control")); err != nil {
			failures = append(failures, err)
		}
		if err := os.RemoveAll(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (engine *ContainerdEngine) releaseVerifiedAttempt(ctx context.Context, authorityKey string) error {
	pinErr := engine.releaseAttemptImagePin(ctx, authorityKey)
	engine.releaseAttemptRuntimeState(authorityKey)
	engine.releaseCapacityReservation(authorityKey)
	return pinErr
}

func (engine *ContainerdEngine) releaseAttemptImagePin(ctx context.Context, authorityKey string) error {
	engine.imageResourceMu.Lock()
	_, pinned := engine.attemptImagePins[authorityKey]
	leaseManager := engine.imageLeaseDeletes
	engine.imageResourceMu.Unlock()
	if !pinned {
		return nil
	}
	if leaseManager == nil {
		return errors.New("attempt image-pin lease manager is unavailable")
	}
	deleteContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	err := leaseManager.Delete(engineContext(deleteContext), leases.Lease{ID: imageHoldLeaseID("attempt", authorityKey)}, leases.SynchronousDelete)
	if err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("release attempt image pin: %w", err)
	}
	engine.imageResourceMu.Lock()
	delete(engine.attemptImagePins, authorityKey)
	engine.imageResourceMu.Unlock()
	return nil
}

func (engine *ContainerdEngine) releaseAttemptRuntimeState(authorityKey string) {
	engine.mu.Lock()
	attempt := engine.attempts[authorityKey]
	delete(engine.attempts, authorityKey)
	if attempt != nil {
		attempt.mu.Lock()
		for name, hold := range attempt.endpointHolds {
			_ = hold.Close()
			delete(attempt.endpointHolds, name)
		}
		attempt.mu.Unlock()
	}
	if attempt != nil {
		for _, port := range attempt.endpoints {
			if engine.ports[port] == authorityKey {
				delete(engine.ports, port)
			}
		}
	}
	engine.mu.Unlock()
}

func (engine *ContainerdEngine) releaseVerifiedNamespace() {
	engine.imageResourceMu.Lock()
	engine.attemptImagePins = make(map[string]imageOperationKey)
	engine.bindingImagePins = make(map[string]imageOperationKey)
	engine.cacheReady = false
	engine.imageResourceMu.Unlock()
	engine.mu.Lock()
	for _, attempt := range engine.attempts {
		attempt.mu.Lock()
		for _, hold := range attempt.endpointHolds {
			_ = hold.Close()
		}
		attempt.mu.Unlock()
	}
	engine.attempts = make(map[string]*containerdAttempt)
	engine.ports = make(map[uint16]string)
	engine.nextPort = engine.config.AttemptPortMin
	engine.mu.Unlock()
	engine.clearCapacityReservations()
}

func (engine *ContainerdEngine) localImage(ctx context.Context, reference, immutableDigest string) (containerd.Image, ImageEvidence, error) {
	return engine.localImageForPlatform(ctx, reference, immutableDigest, platforms.DefaultStrict())
}

func (engine *ContainerdEngine) localImageForPlatform(ctx context.Context, reference, immutableDigest string, matcher platforms.MatchComparer) (containerd.Image, ImageEvidence, error) {
	imagesList, err := engine.client.ListImages(ctx)
	if err != nil {
		return nil, ImageEvidence{}, err
	}
	for _, image := range imagesList {
		if image.Target().Digest.String() != immutableDigest {
			continue
		}
		manifestDescriptor, platform, selectErr := selectedManifest(ctx, engine.client.ContentStore(), image.Target(), matcher)
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

func selectedManifest(ctx context.Context, store content.Store, target ocispec.Descriptor, runtimeMatcher platforms.MatchComparer) (ocispec.Descriptor, ocispec.Platform, error) {
	if images.IsManifestType(target.MediaType) {
		manifest, err := images.Manifest(ctx, store, target, runtimeMatcher)
		if err != nil {
			return ocispec.Descriptor{}, ocispec.Platform{}, err
		}
		platform, err := images.ConfigPlatform(ctx, store, manifest.Config)
		if err != nil {
			return ocispec.Descriptor{}, ocispec.Platform{}, err
		}
		if !runtimeMatcher.Match(platform) || (target.Platform != nil && !platforms.OnlyStrict(platforms.Normalize(*target.Platform)).Match(platform)) {
			return ocispec.Descriptor{}, ocispec.Platform{}, imageMechanicsError(ImageFailurePlatformMismatch, target.Digest.String(), errors.New("image manifest config platform does not match its descriptor or runtime platform"))
		}
		return target, platform, nil
	}
	children, err := images.Children(ctx, store, target)
	if err != nil {
		return ocispec.Descriptor{}, ocispec.Platform{}, err
	}
	for _, child := range children {
		if !images.IsManifestType(child.MediaType) || (child.Platform != nil && !runtimeMatcher.Match(*child.Platform)) {
			continue
		}
		manifest, manifestErr := images.Manifest(ctx, store, child, runtimeMatcher)
		if manifestErr != nil {
			return ocispec.Descriptor{}, ocispec.Platform{}, manifestErr
		}
		platform, platformErr := images.ConfigPlatform(ctx, store, manifest.Config)
		if platformErr != nil {
			return ocispec.Descriptor{}, ocispec.Platform{}, platformErr
		}
		if !runtimeMatcher.Match(platform) || (child.Platform != nil && !platforms.OnlyStrict(platforms.Normalize(*child.Platform)).Match(platform)) {
			return ocispec.Descriptor{}, ocispec.Platform{}, imageMechanicsError(ImageFailurePlatformMismatch, child.Digest.String(), errors.New("image index descriptor platform does not match its manifest config"))
		}
		return child, platform, nil
	}
	return ocispec.Descriptor{}, ocispec.Platform{}, imageMechanicsError(ImageFailurePlatformMismatch, target.Digest.String(), errors.New("pinned image has no manifest for the runtime platform"))
}

func readImageRuntimeConfig(ctx context.Context, store content.Store, image containerd.Image) (ImageRuntimeConfig, error) {
	manifest, err := images.Manifest(ctx, store, image.Target(), platforms.DefaultStrict())
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

type serviceVolumeCreation struct {
	device uint64
	inode  uint64
}

func (engine *ContainerdEngine) managedVolumeSources(ctx context.Context, request *RunRequest) (map[ManagedVolumeKind]string, *serviceVolumeCreation, *computerDiskAttachment, error) {
	result := make(map[ManagedVolumeKind]string)
	var freshServiceVolume *serviceVolumeCreation
	var computerDisk *computerDiskAttachment
	for _, volume := range request.Workload.ManagedVolumes {
		name := ""
		root := "volumes"
		switch volume.Kind {
		case ManagedVolumeHandoff:
			name = request.Resources.HandoffVolumeDirectory
			root = "handoffs"
		case ManagedVolumeServiceData:
			name = request.Resources.ServiceVolumeDirectory
			root = "service-data"
		case ManagedVolumeComputerDisk:
			if volume.ComputerStorage == nil {
				return nil, nil, nil, errors.New("Computer disk Storage identity is required")
			}
			attachment, err := engine.attachComputerDisk(ctx, *volume.ComputerStorage, request.Authority)
			if err != nil {
				return nil, nil, nil, err
			}
			computerDisk = attachment
			result[volume.Kind] = attachment.mountPath
			continue
		case ManagedVolumeLogSegments:
			name = request.Resources.LogSegmentDirectory
		default:
			return nil, nil, nil, fmt.Errorf("managed volume kind %q is unsupported", volume.Kind)
		}
		path := filepath.Join(engine.config.RuntimeRoot, root, name)
		if volume.Kind == ManagedVolumeServiceData {
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				return nil, nil, nil, err
			}
			if err := os.Mkdir(path, 0o700); err == nil {
				freshServiceVolume, err = serviceVolumeCreationAt(path)
				if err != nil {
					return nil, nil, nil, err
				}
			} else if !errors.Is(err, os.ErrExist) {
				return nil, nil, nil, err
			}
		} else if err := os.MkdirAll(path, 0o700); err != nil {
			return nil, nil, nil, err
		}
		if volume.Kind == ManagedVolumeHandoff {
			now := time.Now()
			if err := os.Chtimes(path, now, now); err != nil {
				return nil, nil, nil, err
			}
		}
		result[volume.Kind] = path
	}
	return result, freshServiceVolume, computerDisk, nil
}

type serviceVolumeOwnerRecord struct {
	Version uint8  `json:"version"`
	Device  uint64 `json:"device"`
	Inode   uint64 `json:"inode"`
	UID     uint32 `json:"uid"`
	GID     uint32 `json:"gid"`
}

type serviceVolumeInitializationDecision struct {
	chown       bool
	writeRecord bool
	rejection   string
}

func decideServiceVolumeInitialization(fresh, recordPresent, recordMatchesIdentity, recordMatchesOwner, actualMatchesOwner, directoryEmpty, actualRootOwned bool) serviceVolumeInitializationDecision {
	if !fresh && recordPresent && (!recordMatchesIdentity || !recordMatchesOwner) {
		return serviceVolumeInitializationDecision{rejection: "service data owner record and directory identity disagree"}
	}
	decision := serviceVolumeInitializationDecision{writeRecord: fresh || !recordPresent}
	if actualMatchesOwner {
		return decision
	}
	if !fresh && !(!recordPresent && directoryEmpty && actualRootOwned) {
		return serviceVolumeInitializationDecision{rejection: "service data directory owner does not match the pinned image user"}
	}
	decision.chown = true
	decision.writeRecord = true
	return decision
}

func serviceVolumeCreationAt(path string) (*serviceVolumeCreation, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return nil, err
	}
	return &serviceVolumeCreation{device: uint64(stat.Dev), inode: stat.Ino}, nil
}

func (engine *ContainerdEngine) initializeServiceVolume(path, recordName string, fresh *serviceVolumeCreation, uid, gid uint32) error {
	engine.serviceVolumeMu.Lock()
	defer engine.serviceVolumeMu.Unlock()
	if recordName == "" || filepath.Base(path)+".owner" != recordName {
		return &ServiceDataRejectionError{Reason: "service data owner record does not match its resource identity", WantedUID: uid, WantedGID: gid}
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return &ServiceDataRejectionError{Reason: "service data volume is not a helper-owned directory", WantedUID: uid, WantedGID: gid, err: err}
	}
	defer file.Close()
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return fmt.Errorf("inspect service data volume descriptor: %w", err)
	}
	if fresh != nil && (fresh.device != uint64(stat.Dev) || fresh.inode != stat.Ino) {
		return &ServiceDataRejectionError{Reason: "fresh service data directory identity changed before ownership initialization", ActualUID: stat.Uid, ActualGID: stat.Gid, WantedUID: uid, WantedGID: gid}
	}
	actualUID, actualGID := stat.Uid, stat.Gid
	markerRoot := filepath.Join(engine.config.RuntimeRoot, "service-data-state")
	if err := os.MkdirAll(markerRoot, 0o700); err != nil {
		return fmt.Errorf("create service data state root: %w", err)
	}
	marker := filepath.Join(markerRoot, recordName)
	var recorded serviceVolumeOwnerRecord
	recordPresent := false
	if payload, err := os.ReadFile(marker); err == nil {
		if err := json.Unmarshal(payload, &recorded); err != nil || recorded.Version != 1 {
			return &ServiceDataRejectionError{Reason: "service data owner record is invalid", ActualUID: actualUID, ActualGID: actualGID, WantedUID: uid, WantedGID: gid, err: err}
		}
		recordPresent = true
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read service data owner marker: %w", err)
	}
	recordMatchesIdentity := recordPresent && recorded.Device == uint64(stat.Dev) && recorded.Inode == stat.Ino
	recordMatchesOwner := recordPresent && recorded.UID == uid && recorded.GID == gid
	actualMatchesOwner := actualUID == uid && actualGID == gid
	directoryEmpty := false
	if !actualMatchesOwner {
		_, readDirectoryErr := file.Readdirnames(1)
		directoryEmpty = errors.Is(readDirectoryErr, io.EOF)
		if readDirectoryErr != nil && !directoryEmpty {
			return fmt.Errorf("inspect service data contents: %w", readDirectoryErr)
		}
	}
	decision := decideServiceVolumeInitialization(fresh != nil, recordPresent, recordMatchesIdentity, recordMatchesOwner, actualMatchesOwner, directoryEmpty, actualUID == 0 && actualGID == 0)
	if decision.rejection != "" {
		return &ServiceDataRejectionError{Reason: decision.rejection, ActualUID: actualUID, ActualGID: actualGID, WantedUID: uid, WantedGID: gid}
	}
	if decision.chown {
		if err := unix.Fchown(int(file.Fd()), int(uid), int(gid)); err != nil {
			return fmt.Errorf("initialize service data owner %d:%d: %w", uid, gid, err)
		}
		actualUID, actualGID = uid, gid
	}
	want := serviceVolumeOwnerRecord{Version: 1, Device: uint64(stat.Dev), Inode: stat.Ino, UID: actualUID, GID: actualGID}
	if !decision.writeRecord && recordPresent && recorded == want {
		return nil
	}
	return writeAtomicDurableOwnerRecord(markerRoot, recordName, want)
}

func writeAtomicDurableOwnerRecord(root, name string, record serviceVolumeOwnerRecord) error {
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	temporary, err := os.CreateTemp(root, "."+name+".tmp-")
	if err != nil {
		return fmt.Errorf("create service data owner record: %w", err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	writeErr := temporary.Chmod(0o600)
	if writeErr == nil {
		_, writeErr = temporary.Write(payload)
	}
	if writeErr == nil {
		writeErr = temporary.Sync()
	}
	writeErr = errors.Join(writeErr, temporary.Close())
	if writeErr != nil {
		return fmt.Errorf("write service data owner record: %w", writeErr)
	}
	if err := os.Rename(temporaryName, filepath.Join(root, name)); err != nil {
		return fmt.Errorf("publish service data owner record: %w", err)
	}
	directory, err := os.Open(root)
	if err != nil {
		return fmt.Errorf("open service data state root: %w", err)
	}
	return errors.Join(directory.Sync(), directory.Close())
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
					var evidence loggerIncompleteEvidence
					reason := string(payload)
					if json.Unmarshal(payload, &evidence) == nil && evidence.Reason != "" {
						reason = evidence.Reason
						if evidence.LostByteCount > 0 {
							gap := &LogGapFrame{ThroughSequence: expected, LostEventCount: 1, LostByteCount: evidence.LostByteCount, Reason: "logger_source_incomplete"}
							if !sendTailEvent(ctx, output, logTailEvent{event: WatchEvent{Kind: WatchProgress, Log: &LogFrame{Stream: stream, Sequence: expected, Gap: gap}}}) {
								return
							}
						}
					}
					sendTailEvent(ctx, output, logTailEvent{event: incompleteLogSeal(stream, reason)})
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
	result := ResourceInventory{Leases: []string{}, Snapshots: []string{}, Containers: []string{}, Tasks: []string{}, Shims: []string{}, Cgroups: []string{}, LogSegments: []string{}, ManagedVolumes: []string{}, ManagedVolumeRecords: []string{}, ComputerDiskImages: []string{}, ComputerDiskAllocations: []string{}, ComputerDiskQuotas: []string{}, ComputerDiskManifests: []string{}, ComputerDiskMounts: []string{}, ComputerDiskLoops: []string{}, ComputerAttachments: []string{}, ComputerResetManifests: []string{}, ComputerQuarantines: []string{}, ComputerDiskAnomalies: []string{}}
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
	if err := inventoryManagedVolumeResources(engine.config.RuntimeRoot, &result); err != nil {
		return result, err
	}
	if err := engine.inventoryComputerDiskResources(&result); err != nil {
		return result, err
	}
	sort.Strings(result.Leases)
	sort.Strings(result.Snapshots)
	sort.Strings(result.Containers)
	sort.Strings(result.Tasks)
	sort.Strings(result.Shims)
	sort.Strings(result.Cgroups)
	sort.Strings(result.LogSegments)
	sort.Strings(result.ManagedVolumes)
	sort.Strings(result.ManagedVolumeRecords)
	sort.Strings(result.ComputerDiskImages)
	sort.Strings(result.ComputerDiskAllocations)
	sort.Strings(result.ComputerDiskQuotas)
	sort.Strings(result.ComputerDiskManifests)
	sort.Strings(result.ComputerDiskMounts)
	sort.Strings(result.ComputerDiskLoops)
	sort.Strings(result.ComputerAttachments)
	sort.Strings(result.ComputerResetManifests)
	sort.Strings(result.ComputerQuarantines)
	sort.Strings(result.ComputerDiskAnomalies)
	return result, nil
}

func inventoryManagedVolumeResources(runtimeRoot string, result *ResourceInventory) error {
	volumeEntries, err := readDirectoryIfPresent(filepath.Join(runtimeRoot, "handoffs"))
	if err != nil {
		return err
	}
	for _, entry := range volumeEntries {
		if strings.HasPrefix(entry.Name(), "wefty-handoff-volume-") {
			result.ManagedVolumes = append(result.ManagedVolumes, entry.Name())
		}
	}
	serviceEntries, err := readDirectoryIfPresent(filepath.Join(runtimeRoot, "service-data"))
	if err != nil {
		return err
	}
	for _, entry := range serviceEntries {
		if strings.HasPrefix(entry.Name(), "wefty-service-volume-") {
			result.ManagedVolumes = append(result.ManagedVolumes, entry.Name())
		}
	}
	ownerRecordEntries, err := readDirectoryIfPresent(filepath.Join(runtimeRoot, "service-data-state"))
	if err != nil {
		return err
	}
	for _, entry := range ownerRecordEntries {
		if strings.HasPrefix(entry.Name(), "wefty-service-volume-") && strings.HasSuffix(entry.Name(), ".owner") {
			result.ManagedVolumeRecords = append(result.ManagedVolumeRecords, entry.Name())
		}
	}
	return nil
}

func filterInventory(inventory ResourceInventory, resources ResourceIdentity, attachment *computerDiskAttachment) ResourceInventory {
	filtered := ResourceInventory{Leases: []string{}, Snapshots: []string{}, Containers: []string{}, Tasks: []string{}, Shims: []string{}, Cgroups: []string{}, LogSegments: []string{}, ManagedVolumes: []string{}, ManagedVolumeRecords: []string{}, ComputerDiskImages: []string{}, ComputerDiskAllocations: []string{}, ComputerDiskQuotas: []string{}, ComputerDiskManifests: []string{}, ComputerDiskMounts: []string{}, ComputerDiskLoops: []string{}, ComputerAttachments: []string{}, ComputerResetManifests: []string{}, ComputerQuarantines: []string{}, ComputerDiskAnomalies: []string{}}
	for _, pair := range []struct {
		values []string
		target string
		output *[]string
	}{{inventory.Leases, resources.LeaseID, &filtered.Leases}, {inventory.Snapshots, resources.SnapshotID, &filtered.Snapshots}, {inventory.Containers, resources.ContainerID, &filtered.Containers}, {inventory.Tasks, resources.ContainerID, &filtered.Tasks}, {inventory.Shims, resources.ContainerID, &filtered.Shims}, {inventory.Cgroups, resources.CgroupID, &filtered.Cgroups}, {inventory.LogSegments, resources.LogSegmentDirectory, &filtered.LogSegments}, {inventory.ManagedVolumes, resources.HandoffVolumeDirectory, &filtered.ManagedVolumes}, {inventory.ManagedVolumes, resources.ServiceVolumeDirectory, &filtered.ManagedVolumes}, {inventory.ManagedVolumeRecords, resources.ServiceVolumeOwnerRecord, &filtered.ManagedVolumeRecords}} {
		for _, value := range pair.values {
			if value == pair.target || (pair.target == resources.CgroupID && strings.Contains(value, pair.target)) {
				*pair.output = append(*pair.output, value)
			}
		}
	}
	if attachment != nil {
		for _, pair := range []struct {
			values []string
			target string
			output *[]string
		}{{inventory.ComputerDiskImages, attachment.name, &filtered.ComputerDiskImages}, {inventory.ComputerDiskAllocations, attachment.name, &filtered.ComputerDiskAllocations}, {inventory.ComputerDiskQuotas, attachment.name, &filtered.ComputerDiskQuotas}, {inventory.ComputerDiskManifests, attachment.name, &filtered.ComputerDiskManifests}, {inventory.ComputerDiskMounts, attachment.name, &filtered.ComputerDiskMounts}, {inventory.ComputerDiskLoops, attachment.name, &filtered.ComputerDiskLoops}, {inventory.ComputerAttachments, attachment.name, &filtered.ComputerAttachments}, {inventory.ComputerResetManifests, attachment.name, &filtered.ComputerResetManifests}, {inventory.ComputerQuarantines, attachment.name, &filtered.ComputerQuarantines}} {
			for _, value := range pair.values {
				if value == pair.target {
					*pair.output = append(*pair.output, value)
				}
			}
		}
	}
	return filtered
}

func withoutServiceDataInventory(inventory ResourceInventory) ResourceInventory {
	inventory.ManagedVolumes = slices.DeleteFunc(inventory.ManagedVolumes, func(name string) bool {
		return strings.HasPrefix(name, "wefty-service-volume-")
	})
	inventory.ManagedVolumeRecords = []string{}
	return inventory
}

func withoutRetainedBindingInventory(inventory ResourceInventory) ResourceInventory {
	inventory = withoutServiceDataInventory(inventory)
	inventory.ManagedVolumes = slices.DeleteFunc(inventory.ManagedVolumes, func(name string) bool {
		return strings.HasPrefix(name, "wefty-handoff-volume-")
	})
	inventory.ComputerDiskImages = []string{}
	inventory.ComputerDiskAllocations = []string{}
	inventory.ComputerDiskQuotas = []string{}
	inventory.ComputerDiskManifests = []string{}
	return inventory
}

func (engine *ContainerdEngine) runtimeAbsenceInventory(inventory ResourceInventory, now time.Time) (ResourceInventory, error) {
	retention := engine.config.HandoffRetention
	if retention <= 0 {
		retention = defaultHandoffRetention
	}
	return projectRuntimeAbsenceInventory(inventory, func(name string) (bool, error) {
		info, err := os.Stat(filepath.Join(engine.config.RuntimeRoot, "handoffs", name))
		if errors.Is(err, os.ErrNotExist) {
			// A concurrently removed inventory entry cannot be retained evidence;
			// keeping it in the projection makes the next verification retry prove
			// absence from a fresh observation.
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return now.Sub(info.ModTime()) < retention, nil
	})
}

func projectRuntimeAbsenceInventory(inventory ResourceInventory, retainedHandoff func(string) (bool, error)) (ResourceInventory, error) {
	projected := withoutServiceDataInventory(cloneResourceInventory(inventory))
	volumes := make([]string, 0, len(projected.ManagedVolumes))
	for _, name := range projected.ManagedVolumes {
		if !strings.HasPrefix(name, "wefty-handoff-volume-") {
			volumes = append(volumes, name)
			continue
		}
		retained, err := retainedHandoff(name)
		if err != nil {
			return ResourceInventory{}, err
		}
		if !retained {
			volumes = append(volumes, name)
		}
	}
	projected.ManagedVolumes = volumes
	projected.ComputerDiskImages = []string{}
	projected.ComputerDiskAllocations = []string{}
	projected.ComputerDiskQuotas = []string{}
	projected.ComputerDiskManifests = []string{}
	return projected, nil
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
	return len(inventory.Leases) + len(inventory.Snapshots) + len(inventory.Containers) + len(inventory.Tasks) + len(inventory.Shims) + len(inventory.Cgroups) + len(inventory.LogSegments) + len(inventory.ManagedVolumes) + len(inventory.ManagedVolumeRecords) + len(inventory.ComputerDiskImages) + len(inventory.ComputerDiskAllocations) + len(inventory.ComputerDiskQuotas) + len(inventory.ComputerDiskManifests) + len(inventory.ComputerDiskMounts) + len(inventory.ComputerDiskLoops) + len(inventory.ComputerAttachments) + len(inventory.ComputerResetManifests) + len(inventory.ComputerQuarantines)
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
var _ ComputerControlEngine = (*ContainerdEngine)(nil)
