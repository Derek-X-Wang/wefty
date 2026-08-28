//go:build linux

package ocihelper

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type imageLeaseDeletionManager interface {
	Delete(context.Context, leases.Lease, ...leases.DeleteOpt) error
}

type imageCacheEntry struct {
	Key          imageOperationKey `json:"key"`
	Evidence     ImageEvidence     `json:"evidence"`
	LastUsed     time.Time         `json:"last_used"`
	OperatorHold bool              `json:"operator_hold,omitempty"`
}

type imageCacheLedger struct {
	path         string
	mu           sync.Mutex
	Entries      map[string]imageCacheEntry `json:"entries"`
	LastEviction *ImageCacheEviction        `json:"last_eviction,omitempty"`
	LastError    string                     `json:"last_error,omitempty"`
}

func oldestUnused(entries map[string]imageCacheEntry, protected func(imageCacheEntry) bool) []imageCacheEntry {
	ordered := make([]imageCacheEntry, 0, len(entries))
	for _, entry := range entries {
		if !entry.OperatorHold && !protected(entry) {
			ordered = append(ordered, entry)
		}
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].LastUsed.Equal(ordered[j].LastUsed) {
			return imageCacheKey(ordered[i].Key) < imageCacheKey(ordered[j].Key)
		}
		return ordered[i].LastUsed.Before(ordered[j].LastUsed)
	})
	return ordered
}

func (engine *ContainerdEngine) imageCacheLoop() {
	defer close(engine.cacheDone)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-engine.cacheStop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := engine.enforceImageCache(ctx, "periodic"); err != nil {
				engine.recordCacheError(err)
				log.Printf("OCI image cache periodic enforcement: %v", err)
			}
			cancel()
		}
	}
}

func openImageCacheLedger(root string) (*imageCacheLedger, error) {
	ledger := &imageCacheLedger{path: filepath.Join(root, "image-cache.json"), Entries: make(map[string]imageCacheEntry)}
	payload, err := os.ReadFile(ledger.path)
	if err == nil {
		if err := json.Unmarshal(payload, ledger); err != nil {
			return nil, err
		}
		ledger.path = filepath.Join(root, "image-cache.json")
		if ledger.Entries == nil {
			ledger.Entries = make(map[string]imageCacheEntry)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return ledger, nil
}

func imageCacheKey(key imageOperationKey) string {
	return strings.Join([]string{key.Namespace, key.Digest, key.Platform, key.Snapshotter}, "\x00")
}

func (ledger *imageCacheLedger) persistLocked() error {
	payload, err := json.Marshal(ledger)
	if err != nil {
		return err
	}
	temporary := ledger.path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporary, ledger.path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(ledger.path))
	if err != nil {
		return err
	}
	err = directory.Sync()
	closeErr := directory.Close()
	return errors.Join(err, closeErr)
}

func (engine *ContainerdEngine) recordCacheUse(key imageOperationKey, evidence ImageEvidence, operatorHold bool) error {
	engine.imageResourceMu.Lock()
	defer engine.imageResourceMu.Unlock()
	engine.cache.mu.Lock()
	defer engine.cache.mu.Unlock()
	existing := engine.cache.Entries[imageCacheKey(key)]
	engine.cache.Entries[imageCacheKey(key)] = imageCacheEntry{Key: key, Evidence: evidence, LastUsed: time.Now().UTC(), OperatorHold: existing.OperatorHold || operatorHold}
	return engine.cache.persistLocked()
}

func (engine *ContainerdEngine) recordCacheError(err error) {
	if err == nil || engine == nil || engine.cache == nil {
		return
	}
	engine.cache.mu.Lock()
	engine.cache.LastError = err.Error()
	_ = engine.cache.persistLocked()
	engine.cache.mu.Unlock()
}

func imageHoldLeaseID(kind, identity string) string {
	hash := sha256.Sum256([]byte(kind + "\x00" + identity))
	return "wefty-image-" + kind + "-" + hex.EncodeToString(hash[:16])
}

func (engine *ContainerdEngine) imageDescriptors(ctx context.Context, target ocispec.Descriptor, platformDigest string) ([]ocispec.Descriptor, error) {
	seen := make(map[digest.Digest]ocispec.Descriptor)
	root := target
	if images.IsIndexType(target.MediaType) {
		seen[target.Digest] = target
		children, err := images.Children(ctx, engine.client.ContentStore(), target)
		if err != nil {
			return nil, err
		}
		root = ocispec.Descriptor{}
		for _, child := range children {
			if child.Digest.String() == platformDigest {
				root = child
				break
			}
		}
		if root.Digest == "" {
			return nil, errors.New("selected platform manifest is absent from the top-level image")
		}
	}
	handler := images.HandlerFunc(func(ctx context.Context, descriptor ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		seen[descriptor.Digest] = descriptor
		return images.Children(ctx, engine.client.ContentStore(), descriptor)
	})
	if err := images.Walk(ctx, handler, root); err != nil {
		return nil, err
	}
	descriptors := make([]ocispec.Descriptor, 0, len(seen))
	for _, descriptor := range seen {
		descriptors = append(descriptors, descriptor)
	}
	return descriptors, nil
}

func (engine *ContainerdEngine) attachLeaseToImage(ctx context.Context, leaseID string, evidence ImageEvidence) error {
	leaseManager := engine.client.LeasesService()
	lease, err := leaseManager.Create(ctx, leases.WithID(leaseID))
	created := err == nil
	if err != nil {
		if !errdefs.IsAlreadyExists(err) {
			return err
		}
		lease = leases.Lease{ID: leaseID}
	}
	cleanupCreatedLease := func() {
		if created {
			_ = leaseManager.Delete(context.WithoutCancel(ctx), lease, leases.SynchronousDelete)
		}
	}
	imagesInNamespace, err := engine.client.ImageService().List(ctx)
	if err != nil {
		cleanupCreatedLease()
		return err
	}
	var target *ocispec.Descriptor
	for _, image := range imagesInNamespace {
		if image.Target.Digest.String() == evidence.TopLevelDigest {
			copy := image.Target
			target = &copy
			break
		}
	}
	if target == nil {
		cleanupCreatedLease()
		return &ImageUnavailableError{err: errdefs.ErrNotFound}
	}
	descriptors, err := engine.imageDescriptors(ctx, *target, evidence.PlatformManifestDigest)
	if err != nil {
		cleanupCreatedLease()
		return err
	}
	for _, descriptor := range descriptors {
		resource := leases.Resource{Type: "content", ID: descriptor.Digest.String()}
		if err := leaseManager.AddResource(ctx, lease, resource); err != nil && !errdefs.IsAlreadyExists(err) {
			cleanupCreatedLease()
			return err
		}
	}
	return nil
}

func (engine *ContainerdEngine) attachImageHolds(ctx context.Context, request EnsureImageRequest, key imageOperationKey, evidence ImageEvidence) error {
	if request.Pin == nil {
		return nil
	}
	authority := request.Pin.Authority
	if err := authority.validate(); err != nil {
		return err
	}
	engine.imageResourceMu.Lock()
	defer engine.imageResourceMu.Unlock()
	attemptLease := imageHoldLeaseID("attempt", authority.key())
	if err := engine.attachLeaseToImage(ctx, attemptLease, evidence); err != nil {
		return err
	}
	engine.attemptImagePins[authority.key()] = key
	if request.Pin.Binding {
		bindingLease := imageHoldLeaseID("binding", authority.JobID)
		if err := engine.attachLeaseToImage(ctx, bindingLease, evidence); err != nil {
			return err
		}
		engine.bindingImagePins[authority.JobID] = key
	}
	return nil
}

func (engine *ContainerdEngine) ReconcileImagePins(ctx context.Context, request ReconcileImagePinsRequest) (ReconcileImagePinsResponse, error) {
	if request.CacheMaxBytes <= 0 {
		return ReconcileImagePinsResponse{}, errors.New("image cache maximum bytes must be positive")
	}
	engine.imageContentMu.Lock()
	defer engine.imageContentMu.Unlock()
	engine.imageResourceMu.Lock()
	defer engine.imageResourceMu.Unlock()
	engine.cacheReady = false
	engine.cacheMaxBytes = request.CacheMaxBytes
	engine.probeDigests = make(map[string]struct{}, len(request.ProbeDigests))
	for _, value := range request.ProbeDigests {
		if err := digest.Digest(value).Validate(); err != nil {
			return ReconcileImagePinsResponse{}, errors.New("probe image digest is invalid")
		}
		engine.probeDigests[value] = struct{}{}
	}
	wanted := make(map[string]struct{}, len(request.Bindings))
	missingSet := make(map[string]struct{})
	for _, pin := range request.Bindings {
		if strings.TrimSpace(pin.JobID) == "" || strings.TrimSpace(pin.Reference) == "" || pin.Snapshotter != DefaultSnapshotter {
			return ReconcileImagePinsResponse{}, errors.New("binding image pin is incomplete")
		}
		if err := digest.Digest(pin.Digest).Validate(); err != nil {
			return ReconcileImagePinsResponse{}, errors.New("binding image pin digest is invalid")
		}
		if pin.Platform.OS == "" || pin.Platform.Architecture == "" || pin.Platform.OS != strings.ToLower(strings.TrimSpace(pin.Platform.OS)) || pin.Platform.Architecture != strings.ToLower(strings.TrimSpace(pin.Platform.Architecture)) || pin.Platform.Variant != strings.ToLower(strings.TrimSpace(pin.Platform.Variant)) {
			return ReconcileImagePinsResponse{}, errors.New("binding image pin platform is not canonical")
		}
		if _, duplicate := wanted[pin.JobID]; duplicate {
			return ReconcileImagePinsResponse{}, errors.New("binding image pin job ID is duplicated")
		}
		key := imageOperationKey{Namespace: ContainerdNamespace, Digest: pin.Digest, Platform: platformString(pin.Platform), Snapshotter: pin.Snapshotter}
		leaseID := imageHoldLeaseID("binding", pin.JobID)
		wanted[pin.JobID] = struct{}{}
		matcher := platforms.OnlyStrict(platforms.Normalize(ocispec.Platform{OS: pin.Platform.OS, Architecture: pin.Platform.Architecture, Variant: pin.Platform.Variant}))
		_, evidence, localErr := engine.localImageForPlatform(engineContext(ctx), pin.Reference, pin.Digest, matcher)
		if localErr != nil {
			var unavailable *ImageUnavailableError
			if errors.As(localErr, &unavailable) || errdefs.IsNotFound(localErr) {
				missingSet[pin.Digest] = struct{}{}
				continue
			}
			return ReconcileImagePinsResponse{}, localErr
		}
		if err := engine.attachLeaseToImage(engineContext(ctx), leaseID, evidence); err != nil {
			var unavailable *ImageUnavailableError
			if errors.As(err, &unavailable) || errdefs.IsNotFound(err) {
				missingSet[pin.Digest] = struct{}{}
				continue
			}
			return ReconcileImagePinsResponse{}, err
		}
		engine.bindingImagePins[pin.JobID] = key
	}
	for jobID := range engine.bindingImagePins {
		if _, ok := wanted[jobID]; !ok {
			if engine.imageLeaseDeletes == nil {
				return ReconcileImagePinsResponse{}, errors.New("binding image-pin lease manager is unavailable")
			}
			if err := engine.imageLeaseDeletes.Delete(engineContext(ctx), leases.Lease{ID: imageHoldLeaseID("binding", jobID)}, leases.SynchronousDelete); err != nil && !errdefs.IsNotFound(err) {
				return ReconcileImagePinsResponse{}, fmt.Errorf("delete stale binding image lease for %q: %w", jobID, err)
			}
			delete(engine.bindingImagePins, jobID)
		}
	}
	if err := engine.reconcileCacheInventoryLocked(engineContext(ctx)); err != nil {
		return ReconcileImagePinsResponse{}, err
	}
	missing := make([]string, 0, len(missingSet))
	for value := range missingSet {
		missing = append(missing, value)
	}
	sort.Strings(missing)
	if len(missing) == 0 {
		engine.cacheReady = true
		go func() {
			if err := engine.enforceImageCache(context.Background(), "reconcile"); err != nil {
				engine.recordCacheError(err)
			}
		}()
	}
	return ReconcileImagePinsResponse{MissingDigests: missing}, nil
}

func (engine *ContainerdEngine) reconcileCacheInventoryLocked(ctx context.Context) error {
	listed, err := engine.client.ImageService().List(ctx)
	if err != nil {
		return fmt.Errorf("list containerd image inventory: %w", err)
	}
	inventory := make(map[string]images.Image)
	for _, image := range listed {
		if _, err := engine.client.ContentStore().Info(ctx, image.Target.Digest); err != nil {
			if errdefs.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("verify containerd image content %s: %w", image.Target.Digest, err)
		}
		inventory[image.Target.Digest.String()] = image
	}
	engine.cache.mu.Lock()
	defer engine.cache.mu.Unlock()
	changed := false
	known := make(map[string]struct{}, len(engine.cache.Entries))
	for cacheKey, entry := range engine.cache.Entries {
		if _, exists := inventory[entry.Key.Digest]; !exists {
			delete(engine.cache.Entries, cacheKey)
			changed = true
			continue
		}
		known[entry.Key.Digest] = struct{}{}
	}
	for value, image := range inventory {
		if _, exists := known[value]; exists {
			continue
		}
		key := imageOperationKey{Namespace: ContainerdNamespace, Digest: value, Platform: "inventory", Snapshotter: DefaultSnapshotter}
		engine.cache.Entries[imageCacheKey(key)] = imageCacheEntry{
			Key: key,
			Evidence: ImageEvidence{SubmittedReference: image.Name, TopLevelDigest: value, TopLevelMediaType: image.Target.MediaType,
				Snapshotter: DefaultSnapshotter, RuntimeHandler: DefaultRuntimeHandler},
			LastUsed: time.Unix(0, 0).UTC(),
		}
		changed = true
	}
	if changed {
		return engine.cache.persistLocked()
	}
	return nil
}

func platformString(platform OCIPlatform) string {
	value := platform.OS + "/" + platform.Architecture
	if platform.Variant != "" {
		value += "/" + platform.Variant
	}
	return value
}

func (engine *ContainerdEngine) ReleaseImagePin(ctx context.Context, request ReleaseImagePinRequest) error {
	if strings.TrimSpace(request.JobID) == "" {
		return errors.New("binding image pin job ID is required")
	}
	engine.imageResourceMu.Lock()
	if _, exists := engine.bindingImagePins[request.JobID]; !exists {
		engine.imageResourceMu.Unlock()
		engine.releaseJobCapacityReservation(request.JobID)
		return nil
	}
	if engine.imageLeaseDeletes == nil {
		engine.imageResourceMu.Unlock()
		return errors.New("binding image-pin lease manager is unavailable")
	}
	deleteContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	err := engine.imageLeaseDeletes.Delete(engineContext(deleteContext), leases.Lease{ID: imageHoldLeaseID("binding", request.JobID)}, leases.SynchronousDelete)
	if err != nil && !errdefs.IsNotFound(err) {
		engine.imageResourceMu.Unlock()
		return err
	}
	delete(engine.bindingImagePins, request.JobID)
	engine.imageResourceMu.Unlock()
	engine.releaseJobCapacityReservation(request.JobID)
	return nil
}

func (engine *ContainerdEngine) ReleaseAttemptImagePin(ctx context.Context, request ReleaseAttemptImagePinRequest) error {
	if err := request.Authority.validate(); err != nil {
		return err
	}
	return engine.releaseAttemptImagePin(ctx, request.Authority.key())
}

func (engine *ContainerdEngine) contentBytes(ctx context.Context) (int64, error) {
	var total int64
	err := engine.client.ContentStore().Walk(ctx, func(info content.Info) error {
		total += info.Size
		return nil
	})
	return total, err
}

func (engine *ContainerdEngine) ImageCacheStatus(ctx context.Context) (ImageCacheStatus, error) {
	if err := lockMutexContext(ctx, &engine.imageContentMu); err != nil {
		return ImageCacheStatus{}, err
	}
	defer engine.imageContentMu.Unlock()
	if err := lockMutexContext(ctx, &engine.imageResourceMu); err != nil {
		return ImageCacheStatus{}, err
	}
	defer engine.imageResourceMu.Unlock()
	bytes, err := engine.contentBytes(engineContext(ctx))
	if err != nil {
		return ImageCacheStatus{}, err
	}
	engine.cache.mu.Lock()
	defer engine.cache.mu.Unlock()
	var last *ImageCacheEviction
	if engine.cache.LastEviction != nil {
		copy := *engine.cache.LastEviction
		last = &copy
	}
	return ImageCacheStatus{Bytes: bytes, CapBytes: engine.cacheMaxBytes, LastEviction: last, LastError: engine.cache.LastError}, nil
}

func lockMutexContext(ctx context.Context, mutex *sync.Mutex) error {
	for {
		if mutex.TryLock() {
			return nil
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (engine *ContainerdEngine) enforceImageCache(ctx context.Context, reason string) error {
	engine.imageContentMu.Lock()
	defer engine.imageContentMu.Unlock()
	active := engine.imageOperations.ActiveKeys()
	engine.imageResourceMu.Lock()
	if !engine.cacheReady {
		engine.imageResourceMu.Unlock()
		return nil
	}
	before, err := engine.contentBytes(engineContext(ctx))
	if err != nil || before <= engine.cacheMaxBytes {
		engine.imageResourceMu.Unlock()
		return err
	}
	engine.cache.mu.Lock()
	entries := oldestUnused(engine.cache.Entries, func(entry imageCacheEntry) bool {
		if _, probe := engine.probeDigests[entry.Key.Digest]; probe || engine.imageDigestPinned(entry.Key.Digest) {
			return true
		}
		for key := range active {
			if key == entry.Key || key.Digest == entry.Key.Digest {
				return true
			}
		}
		return false
	})
	engine.cache.mu.Unlock()
	if len(entries) == 0 {
		engine.imageResourceMu.Unlock()
		return nil
	}
	entry := entries[0]
	listed, err := engine.client.ImageService().List(engineContext(ctx))
	if err != nil {
		engine.imageResourceMu.Unlock()
		return err
	}
	engine.imageResourceMu.Unlock()

	// Revalidate immediately before deletion: containerd inventory can take
	// long enough for a waiter to arrive or a successful waiter to attach a
	// longer-lived hold.
	active = engine.imageOperations.ActiveKeys()
	engine.imageResourceMu.Lock()
	if !engine.cacheReady || engine.imageDigestPinned(entry.Key.Digest) {
		engine.imageResourceMu.Unlock()
		return nil
	}
	for key := range active {
		if key == entry.Key || key.Digest == entry.Key.Digest {
			engine.imageResourceMu.Unlock()
			return nil
		}
	}
	for _, image := range listed {
		if image.Target.Digest.String() != entry.Key.Digest {
			continue
		}
		if err := engine.client.ImageService().Delete(engineContext(ctx), image.Name, images.DeleteTarget(&image.Target), images.SynchronousDelete()); err != nil && !errdefs.IsNotFound(err) {
			engine.imageResourceMu.Unlock()
			return err
		}
	}
	after, err := engine.contentBytes(engineContext(ctx))
	if err != nil {
		engine.imageResourceMu.Unlock()
		return err
	}
	evictedBytes := before - after
	if evictedBytes < 0 {
		evictedBytes = 0
	}
	eviction := &ImageCacheEviction{Digest: entry.Key.Digest, Reason: reason, Bytes: evictedBytes, EvictedAt: time.Now().UTC()}
	engine.cache.mu.Lock()
	for cacheKey, cached := range engine.cache.Entries {
		if cached.Key.Digest == entry.Key.Digest {
			delete(engine.cache.Entries, cacheKey)
		}
	}
	engine.cache.LastEviction = eviction
	err = engine.cache.persistLocked()
	engine.cache.mu.Unlock()
	engine.imageResourceMu.Unlock()
	if err != nil {
		return err
	}
	return nil
}

func (engine *ContainerdEngine) imageDigestPinned(value string) bool {
	for _, pinned := range engine.attemptImagePins {
		if pinned.Digest == value {
			return true
		}
	}
	for _, pinned := range engine.bindingImagePins {
		if pinned.Digest == value {
			return true
		}
	}
	return false
}
