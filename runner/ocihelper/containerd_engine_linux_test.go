//go:build linux

package ocihelper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/leases"
	contentlocal "github.com/containerd/containerd/v2/plugins/content/local"
	"github.com/containerd/platforms"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sys/unix"
)

func TestContainerdEngineRejectsPATHResolvedRunc(t *testing.T) {
	_, err := NewContainerdEngine(NativeEngineConfig{RuntimeRoot: t.TempDir(), RuncExecutable: "runc"})
	if err == nil || !strings.Contains(err.Error(), "runc executable must be absolute") {
		t.Fatalf("PATH-resolved runc = %v", err)
	}
}

func TestDoctorCacheReadIsBoundedBehindPullLocks(t *testing.T) {
	engine := &ContainerdEngine{}
	engine.imageContentMu.Lock()
	defer engine.imageContentMu.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := engine.ImageCacheStatus(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked cache read = %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("blocked cache read was not bounded: %s", elapsed)
	}
}

func TestSegmentTailerMarksTruncatedFinalFrameIncomplete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stdout.frames")
	var complete bytes.Buffer
	if err := writeLogFrame(&complete, 0, []byte("complete")); err != nil {
		t.Fatal(err)
	}
	var truncated bytes.Buffer
	if err := writeLogFrame(&truncated, 1, []byte("truncated")); err != nil {
		t.Fatal(err)
	}
	payload := append(complete.Bytes(), truncated.Bytes()[:logFrameHeaderBytes+3]...)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	terminal := make(chan struct{})
	close(terminal)
	events := make(chan logTailEvent, 4)
	tailLogSegment(context.Background(), "stdout", path, terminal, time.Millisecond, 0, events)
	var data, gap, incomplete bool
	for len(events) > 0 {
		event := (<-events).event
		data = data || (event.Log != nil && string(event.Log.Bytes) == "complete")
		gap = gap || (event.Log != nil && event.Log.Gap != nil && event.Log.Gap.LostByteCount == uint64(len("truncated")))
		incomplete = incomplete || (event.Seal != nil && !event.Seal.Complete)
	}
	if !data || !gap || !incomplete {
		t.Fatalf("tail evidence data=%t gap=%t incomplete=%t", data, gap, incomplete)
	}
}

func TestRuntimeDeletionValidatesIdentityBeforeNotFound(t *testing.T) {
	authority := testAuthority()
	authority.Class = contract.JobClassService
	authority.RemovalGeneration = "1"
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateRuntimeResourceLabels("lease", resources.LeaseID, resources.LeaseID, resources.Labels, authority); err != nil {
		t.Fatalf("matching resource identity was rejected: %v", err)
	}
	wrong := make(map[string]string, len(resources.Labels))
	for name, value := range resources.Labels {
		wrong[name] = value
	}
	wrong["io.wefty/job_id"] = "different-job"
	if err := validateRuntimeResourceLabels("lease", resources.LeaseID, resources.LeaseID, wrong, authority); err == nil {
		t.Fatal("mismatched resource authority reached the NotFound path")
	}
	if err := validateRuntimeResourceLabels("lease", "different-name", resources.LeaseID, resources.Labels, authority); err == nil {
		t.Fatal("mismatched deterministic identity reached the NotFound path")
	}
}

func TestAuthorityLabelsRequireTheFullTuple(t *testing.T) {
	authority := AttemptAuthority{NodeID: "node", JobID: "job", AttemptID: "attempt", FencingToken: "fence", BootSessionID: "boot", Class: "one-shot", RemovalGeneration: "remove"}
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	delete(resources.Labels, "io.wefty/job_id")
	if _, err := authorityFromLabels(resources.Labels); err == nil {
		t.Fatal("partial authority labels accepted")
	}
}

func TestOOMEvidenceUsesConfiguredCgroupRoot(t *testing.T) {
	root := t.TempDir()
	cgroupID := "wefty-cgroup-test"
	if err := os.Mkdir(filepath.Join(root, cgroupID), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, cgroupID, "memory.events"), []byte("oom 1\noom_kill 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !cgroupReportedOOM(root, cgroupID) {
		t.Fatal("configured cgroup root did not report oom_kill")
	}
	if err := os.WriteFile(filepath.Join(root, cgroupID, "memory.events"), []byte("oom 1\noom_kill 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if cgroupReportedOOM(root, cgroupID) {
		t.Fatal("plain oom counter was classified as an oom_kill")
	}
}

func TestParseRetryAfterAcceptsSecondsAndHTTPDate(t *testing.T) {
	now := time.Date(2026, time.August, 23, 12, 0, 0, 0, time.UTC)
	if delay := parseRetryAfter("17", now); delay != 17*time.Second {
		t.Fatalf("delta Retry-After = %s, want 17s", delay)
	}
	if delay := parseRetryAfter(now.Add(23*time.Second).Format(http.TimeFormat), now); delay != 23*time.Second {
		t.Fatalf("date Retry-After = %s, want 23s", delay)
	}
	if delay := parseRetryAfter("invalid", now); delay != 0 {
		t.Fatalf("invalid Retry-After = %s, want zero", delay)
	}
}

func TestRegistryTransportReturnsTransientStatusToAgentWithoutMechanicsRetry(t *testing.T) {
	tracker := &retryAfterTracker{}
	transport := retryAfterTransport{tracker: tracker, base: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Header:     http.Header{"Retry-After": []string{"11"}},
			Body:       io.NopCloser(bytes.NewReader(nil)),
		}, nil
	})}
	response, err := transport.RoundTrip(&http.Request{})
	var statusFailure *registryStatusError
	if response != nil || !errors.As(err, &statusFailure) || statusFailure.statusCode != http.StatusTooManyRequests {
		t.Fatalf("transient registry response=%+v err=%v", response, err)
	}
	if tracker.Delay() != 11*time.Second {
		t.Fatalf("tracked Retry-After = %s, want 11s", tracker.Delay())
	}
}

func TestPublicRegistryMechanicsFactsComeFromRegistryBehavior(t *testing.T) {
	index := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[]}`)
	tests := []struct {
		name       string
		status     int
		mediaType  string
		retryAfter string
		tokenError bool
		delay      time.Duration
		wantKind   ImageFailureKind
		wantStatus int
		wantRetry  time.Duration
	}{
		{name: "503", status: 503, retryAfter: "7", wantKind: ImageFailureHTTP, wantStatus: 503, wantRetry: 7 * time.Second},
		{name: "429", status: 429, retryAfter: "11", wantKind: ImageFailureHTTP, wantStatus: 429, wantRetry: 11 * time.Second},
		{name: "404", status: 404, wantKind: ImageFailureHTTP, wantStatus: 404},
		{name: "401", tokenError: true, wantKind: ImageFailureHTTP, wantStatus: 401},
		{name: "unsupported media type", status: 200, mediaType: "text/plain", wantKind: ImageFailureManifestRejected},
		{name: "response timeout", status: 200, mediaType: ocispec.MediaTypeImageIndex, delay: 100 * time.Millisecond, wantKind: ImageFailureNetwork},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case "/v2/":
					response.WriteHeader(http.StatusOK)
				case "/token":
					if test.tokenError {
						response.WriteHeader(http.StatusUnauthorized)
						return
					}
					response.Header().Set("Content-Type", "application/json")
					_, _ = response.Write([]byte(`{"token":"test-token"}`))
				case "/v2/repo/manifests/tag":
					if request.Header.Get("Authorization") == "" {
						response.Header().Set("WWW-Authenticate", `Bearer realm="`+server.URL+`/token",service="test",scope="repository:repo:pull"`)
						response.WriteHeader(http.StatusUnauthorized)
						return
					}
					if test.delay > 0 {
						time.Sleep(test.delay)
					}
					if test.retryAfter != "" {
						response.Header().Set("Retry-After", test.retryAfter)
					}
					if test.status != 0 && test.status != http.StatusOK {
						response.WriteHeader(test.status)
						return
					}
					mediaType := test.mediaType
					if mediaType == "" {
						mediaType = ocispec.MediaTypeImageIndex
					}
					response.Header().Set("Content-Type", mediaType)
					response.Header().Set("Docker-Content-Digest", digest.FromBytes(index).String())
					response.Header().Set("Content-Length", fmt.Sprint(len(index)))
					if request.Method != http.MethodHead {
						_, _ = response.Write(index)
					}
				default:
					response.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			tracker := &retryAfterTracker{}
			ctx := t.Context()
			if test.delay > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 10*time.Millisecond)
				defer cancel()
			}
			_, descriptor, err := publicResolver(tracker).Resolve(ctx, strings.TrimPrefix(server.URL, "http://")+"/repo:tag")
			if err == nil {
				err = validateResolvedImageDescriptor(descriptor)
			}
			if err == nil {
				t.Fatal("registry behavior unexpectedly resolved")
			}
			var mechanics *ImageMechanicsError
			if !errors.As(classifyRegistryError(err, tracker.Delay(), ""), &mechanics) {
				t.Fatalf("registry error has no mechanics fact: %v", err)
			}
			if mechanics.Fact.Kind != test.wantKind || mechanics.Fact.HTTPStatus != test.wantStatus || mechanics.Fact.RetryAfter != test.wantRetry {
				t.Fatalf("mechanics fact = %+v", mechanics.Fact)
			}
		})
	}
}

func TestPublicRegistryTokenChallengeResolvesIndex(t *testing.T) {
	manifest := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size":2},"layers":[]}`)
	manifestDescriptor := ocispec.Descriptor{MediaType: ocispec.MediaTypeImageManifest, Digest: digest.FromBytes(manifest), Size: int64(len(manifest)), Platform: &ocispec.Platform{OS: "linux", Architecture: "amd64"}}
	index, err := json.Marshal(struct {
		SchemaVersion int                  `json:"schemaVersion"`
		MediaType     string               `json:"mediaType"`
		Manifests     []ocispec.Descriptor `json:"manifests"`
	}{SchemaVersion: 2, MediaType: ocispec.MediaTypeImageIndex, Manifests: []ocispec.Descriptor{manifestDescriptor}})
	if err != nil {
		t.Fatal(err)
	}
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/token" {
			_, _ = response.Write([]byte(`{"token":"test-token"}`))
			return
		}
		if request.URL.Path == "/v2/repo/manifests/tag" && request.Header.Get("Authorization") == "" {
			response.Header().Set("WWW-Authenticate", `Bearer realm="`+server.URL+`/token",service="test",scope="repository:repo:pull"`)
			response.WriteHeader(http.StatusUnauthorized)
			return
		}
		payload := index
		mediaType := ocispec.MediaTypeImageIndex
		if strings.HasSuffix(request.URL.Path, manifestDescriptor.Digest.String()) {
			payload = manifest
			mediaType = ocispec.MediaTypeImageManifest
		}
		response.Header().Set("Content-Type", mediaType)
		response.Header().Set("Docker-Content-Digest", digest.FromBytes(payload).String())
		response.Header().Set("Content-Length", fmt.Sprint(len(payload)))
		if request.Method != http.MethodHead {
			_, _ = response.Write(payload)
		}
	}))
	defer server.Close()
	resolver := publicResolver(&retryAfterTracker{})
	name := strings.TrimPrefix(server.URL, "http://") + "/repo:tag"
	_, descriptor, err := resolver.Resolve(t.Context(), name)
	if err != nil || descriptor.MediaType != ocispec.MediaTypeImageIndex || descriptor.Digest != digest.FromBytes(index) {
		t.Fatalf("token challenge index resolution = (%+v, %v)", descriptor, err)
	}
	fetcher, err := resolver.Fetcher(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []struct {
		descriptor ocispec.Descriptor
		payload    []byte
	}{{descriptor: descriptor, payload: index}, {descriptor: manifestDescriptor, payload: manifest}} {
		reader, fetchErr := fetcher.Fetch(t.Context(), expected.descriptor)
		if fetchErr != nil {
			t.Fatal(fetchErr)
		}
		payload, readErr := io.ReadAll(reader)
		_ = reader.Close()
		if readErr != nil || !bytes.Equal(payload, expected.payload) {
			t.Fatalf("fetched registry descriptor %s = (%q, %v)", expected.descriptor.Digest, payload, readErr)
		}
	}
}

func TestSelectedManifestRejectsWrongArchitectureAndMislabeledIndexes(t *testing.T) {
	if runtime.GOARCH != "amd64" {
		t.Skip("amd64 runtime is the authority for the arm64-only row")
	}
	store, err := contentlocal.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	wrongManifest := testContentManifest(t, ctx, store, ocispec.Platform{OS: "linux", Architecture: "arm64"})
	armLabel := ocispec.Platform{OS: "linux", Architecture: "arm64"}
	hostLabel := ocispec.Platform{OS: "linux", Architecture: "amd64"}
	tests := []struct {
		name   string
		target ocispec.Descriptor
	}{
		{name: "wrong-arch root manifest", target: wrongManifest},
		{name: "mislabeled index", target: testContentIndex(t, ctx, store, descriptorWithPlatform(wrongManifest, hostLabel))},
		{name: "arm64-only index", target: testContentIndex(t, ctx, store, descriptorWithPlatform(wrongManifest, armLabel))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := selectedManifest(ctx, store, test.target, platforms.DefaultStrict())
			var mechanics *ImageMechanicsError
			if !errors.As(err, &mechanics) || mechanics.Fact.Kind != ImageFailurePlatformMismatch {
				t.Fatalf("platform selection error = %v", err)
			}
		})
	}
}

func TestSweepSkipsSpoolOwnedByLiveImageOperation(t *testing.T) {
	root := t.TempDir()
	imports := filepath.Join(root, "imports")
	if err := os.MkdirAll(imports, 0o700); err != nil {
		t.Fatal(err)
	}
	live := filepath.Join(imports, "wefty-image-live.tar")
	stale := filepath.Join(imports, "wefty-image-stale.tar")
	for _, path := range []string{live, stale} {
		if err := os.WriteFile(path, []byte("archive"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, activeSpools: map[string]struct{}{live: {}}, activeLeases: make(map[string]struct{})}
	if err := engine.sweepImageSpools(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(live); err != nil {
		t.Fatalf("live spool was removed: %v", err)
	}
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale spool remained: %v", err)
	}
}

func TestHandoffRetentionRefreshesAndExpiresStableOwnerVolumes(t *testing.T) {
	root := t.TempDir()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root, HandoffRetention: time.Hour}}
	name, err := DeterministicHandoffVolumeDirectory("run-owner")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "handoffs", name)
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := os.Chtimes(path, now.Add(-30*time.Minute), now.Add(-30*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := engine.cleanupExpiredHandoffs(now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("retry-window handoff was removed: %v", err)
	}
	if err := os.Chtimes(path, now.Add(-2*time.Hour), now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := engine.cleanupExpiredHandoffs(now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired handoff remained: %v", err)
	}
}

func TestOwnerKeyedHandoffBytesSurviveAttemptsUntilExplicitFinalization(t *testing.T) {
	root := t.TempDir()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}}
	name, err := DeterministicHandoffVolumeDirectory("run-owner")
	if err != nil {
		t.Fatal(err)
	}
	request := RunRequest{
		Resources: ResourceIdentity{HandoffVolumeDirectory: name},
		Workload:  WorkloadInput{ManagedVolumes: []ManagedVolumeDescriptor{{Kind: ManagedVolumeHandoff, OwnerKey: "run-owner"}}},
	}
	first, _, _, err := engine.managedVolumeSources(t.Context(), &request)
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(first[ManagedVolumeHandoff], "marker")
	if err := os.WriteFile(marker, []byte("handoff bytes\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	second, _, _, err := engine.managedVolumeSources(t.Context(), &request)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(filepath.Join(second[ManagedVolumeHandoff], "marker"))
	if err != nil || string(payload) != "handoff bytes\n" {
		t.Fatalf("reused handoff marker = %q err=%v", payload, err)
	}
	deleted, err := engine.DeleteManagedVolume(t.Context(), DeleteManagedVolumeRequest{Kind: ManagedVolumeHandoff, OwnerKey: "run-owner"})
	if err != nil || !deleted.Deleted {
		t.Fatalf("handoff finalization = %+v err=%v", deleted, err)
	}
	if _, err := os.Stat(first[ManagedVolumeHandoff]); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("finalized handoff remains: %v", err)
	}
}

func TestServiceDataDeletionRemovesBytesAndOwnerRecord(t *testing.T) {
	root := t.TempDir()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}}
	authority := AttemptAuthority{
		NodeID: "node", BootSessionID: "boot", JobID: "service-delete", AttemptID: "attempt",
		FencingToken: "fence", Class: contract.JobClassService, RemovalGeneration: "1",
	}
	resources, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	volume := filepath.Join(root, "service-data", resources.ServiceVolumeDirectory)
	if err := os.MkdirAll(volume, 0o700); err != nil {
		t.Fatal(err)
	}
	fresh, err := serviceVolumeCreationAt(volume)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.initializeServiceVolume(volume, resources.ServiceVolumeOwnerRecord, fresh, uint32(os.Getuid()), uint32(os.Getgid())); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(volume, "tenant-bytes"), []byte("delete me"), 0o600); err != nil {
		t.Fatal(err)
	}
	removal := &ManagedVolumeRemovalAuthority{
		NodeID: authority.NodeID, BootSessionID: authority.BootSessionID, JobID: authority.JobID,
		RemovalGeneration: 1, CleanupFence: "cleanup-fence",
	}
	response, err := engine.DeleteManagedVolume(t.Context(), DeleteManagedVolumeRequest{Kind: ManagedVolumeServiceData, OwnerKey: authority.JobID, Removal: removal})
	if err != nil || !response.Deleted {
		t.Fatalf("service data deletion = %+v err=%v", response, err)
	}
	for _, path := range []string{volume, filepath.Join(root, "service-data-state", resources.ServiceVolumeOwnerRecord)} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("deleted service-data path %q remains: %v", path, err)
		}
	}
	if _, err := engine.DeleteManagedVolume(t.Context(), DeleteManagedVolumeRequest{Kind: ManagedVolumeServiceData, OwnerKey: authority.JobID, Removal: removal}); err != nil {
		t.Fatalf("idempotent service data deletion: %v", err)
	}
}

func TestServiceDataDeletionRequiresRemovalAuthority(t *testing.T) {
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: t.TempDir()}}
	if _, err := engine.DeleteManagedVolume(t.Context(), DeleteManagedVolumeRequest{Kind: ManagedVolumeServiceData, OwnerKey: "service-delete"}); err == nil {
		t.Fatal("service data deletion without removal authority succeeded")
	}
}

func TestRemovalReceiptRowsAreAssertionDerived(t *testing.T) {
	authority := AttemptAuthority{
		NodeID: "node", BootSessionID: "boot", JobID: "service-attest", AttemptID: "attempt",
		FencingToken: "fence", Class: contract.JobClassService, RemovalGeneration: "1",
	}
	identity, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	request := AttestRemovalRequest{
		JobID: authority.JobID, RemovalGeneration: authority.RemovalGeneration,
		Attempts: []RemovalAttemptManifest{{Authority: authority, Resources: expectedRemovalResources(identity)}},
	}
	residue := ResourceInventory{Tasks: []string{identity.TaskID}}
	if response, err := attestRemovalInventory(residue, request); err == nil || len(response.Assertions) != 0 {
		t.Fatalf("receipt recorded PASS with task residue: response=%+v err=%v", response, err)
	}
	response, err := attestRemovalInventory(ResourceInventory{}, request)
	if err != nil || len(response.Assertions) != len(request.Attempts[0].Resources) {
		t.Fatalf("complete removal assertions = %+v err=%v", response, err)
	}
	for _, assertion := range response.Assertions {
		if !assertion.Absent {
			t.Fatalf("executed assertion did not record absence: %+v", assertion)
		}
	}
	invalid := request
	invalid.Attempts = []RemovalAttemptManifest{{Authority: authority, Resources: append(slices.Clone(request.Attempts[0].Resources), RemovalResource{Class: "future-unverified", ID: "disk"})}}
	if response, err := attestRemovalInventory(ResourceInventory{}, invalid); err == nil || len(response.Assertions) != 0 {
		t.Fatalf("unexecuted future class recorded PASS: response=%+v err=%v", response, err)
	}
}

func TestComputerRemovalReceiptAssertsEveryDiskInventoryClass(t *testing.T) {
	authority := AttemptAuthority{
		NodeID: "node", BootSessionID: "boot", JobID: "computer-attest", AttemptID: "attempt",
		FencingToken: "fence", Class: contract.JobClassService, RemovalGeneration: "1",
	}
	identity, err := DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	storage := &ComputerStorageReference{ComputerID: "computer", StorageID: "storage", StorageGeneration: 2, DiskBytes: 8 << 30}
	resources := expectedRemovalResources(identity, storage)
	request := AttestRemovalRequest{JobID: authority.JobID, RemovalGeneration: authority.RemovalGeneration,
		Attempts: []RemovalAttemptManifest{{Authority: authority, ComputerStorage: storage, Resources: resources}}}
	if err := validateAttestRemovalRequest(request, authority.NodeID); err != nil {
		t.Fatalf("Computer removal manifest rejected: %v", err)
	}
	name, err := DeterministicComputerDiskName(*storage)
	if err != nil {
		t.Fatal(err)
	}
	for _, residue := range []ResourceInventory{
		{ComputerDiskImages: []string{name}}, {ComputerDiskAllocations: []string{name}},
		{ComputerDiskQuotas: []string{name}}, {ComputerDiskManifests: []string{name}},
		{ComputerDiskMounts: []string{name}}, {ComputerDiskLoops: []string{name}},
		{ComputerAttachments: []string{name}},
		{ComputerDiskAnomalies: []string{name + ":image_not_regular"}},
	} {
		if response, err := attestRemovalInventory(residue, request); err == nil || len(response.Assertions) != 0 {
			t.Fatalf("Computer residue recorded PASS: inventory=%+v response=%+v err=%v", residue, response, err)
		}
	}
	response, err := attestRemovalInventory(ResourceInventory{}, request)
	if err != nil || len(response.Assertions) != len(resources) {
		t.Fatalf("Computer absence receipt = %+v err=%v, want %d rows", response, err, len(resources))
	}
}

func TestServiceDataVolumeInitializesOwnerOnlyOnce(t *testing.T) {
	root := t.TempDir()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}}
	resources, err := DeterministicResourceIdentity(AttemptAuthority{
		NodeID: "node", BootSessionID: "boot", JobID: "service-job", AttemptID: "attempt-1",
		FencingToken: "fence-1", Class: contract.JobClassService, RemovalGeneration: "attempt",
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "service-data", resources.ServiceVolumeDirectory)
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
	uid, gid := uint32(os.Getuid()), uint32(os.Getgid())
	fresh, err := serviceVolumeCreationAt(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.initializeServiceVolume(path, resources.ServiceVolumeOwnerRecord, fresh, uid, gid); err != nil {
		t.Fatal(err)
	}
	var stat syscall.Stat_t
	if err := syscall.Stat(path, &stat); err != nil || stat.Uid != uid || stat.Gid != gid {
		t.Fatalf("service data owner = %d:%d err=%v, want %d:%d", stat.Uid, stat.Gid, err, uid, gid)
	}
	marker := filepath.Join(path, "retained")
	if err := os.WriteFile(marker, []byte("service bytes\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := engine.initializeServiceVolume(path, resources.ServiceVolumeOwnerRecord, nil, uid, gid); err != nil {
		t.Fatal(err)
	}
	if payload, err := os.ReadFile(marker); err != nil || string(payload) != "service bytes\n" {
		t.Fatalf("service data after repeated initialization = %q err=%v", payload, err)
	}
	if err := engine.initializeServiceVolume(path, resources.ServiceVolumeOwnerRecord, nil, uid+1, gid); err == nil {
		t.Fatal("service data volume was re-owned for a different image identity")
	}
}

func TestServiceDataOwnerRecordRecoveryRows(t *testing.T) {
	for _, row := range []struct {
		name                                                                             string
		fresh, recordPresent, recordIdentity, recordOwner, actualOwner, empty, rootOwned bool
		wantChown, wantWrite                                                             bool
	}{
		{name: "orphan-marker", fresh: true, recordPresent: true, actualOwner: false, empty: true, rootOwned: true, wantChown: true, wantWrite: true},
		{name: "missing-marker-with-data", recordPresent: false, actualOwner: true, empty: false, wantWrite: true},
		{name: "crash-between-mkdir-and-marker", recordPresent: false, actualOwner: false, empty: true, rootOwned: true, wantChown: true, wantWrite: true},
	} {
		t.Run("decision-"+row.name, func(t *testing.T) {
			decision := decideServiceVolumeInitialization(row.fresh, row.recordPresent, row.recordIdentity, row.recordOwner, row.actualOwner, row.empty, row.rootOwned)
			if decision.rejection != "" || decision.chown != row.wantChown || decision.writeRecord != row.wantWrite {
				t.Fatalf("decision = %+v, want chown=%t write=%t", decision, row.wantChown, row.wantWrite)
			}
		})
	}

	uid, gid := uint32(os.Getuid()), uint32(os.Getgid())
	initializeFresh := func(t *testing.T, engine *ContainerdEngine, path, recordName string) error {
		t.Helper()
		fresh, err := serviceVolumeCreationAt(path)
		if err != nil {
			return err
		}
		return engine.initializeServiceVolume(path, recordName, fresh, uid, gid)
	}
	newFixture := func(t *testing.T, jobID string) (*ContainerdEngine, ResourceIdentity, string, string) {
		t.Helper()
		root := t.TempDir()
		engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}}
		resources, err := DeterministicResourceIdentity(AttemptAuthority{
			NodeID: "node", BootSessionID: "boot", JobID: jobID, AttemptID: "attempt-1",
			FencingToken: "fence-1", Class: contract.JobClassService, RemovalGeneration: "attempt",
		})
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, "service-data", resources.ServiceVolumeDirectory)
		record := filepath.Join(root, "service-data-state", resources.ServiceVolumeOwnerRecord)
		return engine, resources, path, record
	}

	t.Run("orphan-marker", func(t *testing.T) {
		engine, resources, path, record := newFixture(t, "orphan-marker")
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := initializeFresh(t, engine, path, resources.ServiceVolumeOwnerRecord); err != nil {
			t.Fatal(err)
		}
		before, err := os.ReadFile(record)
		if err != nil {
			t.Fatal(err)
		}
		var stale serviceVolumeOwnerRecord
		if err := json.Unmarshal(before, &stale); err != nil {
			t.Fatal(err)
		}
		stale.Inode++
		stalePayload, err := json.Marshal(stale)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(record, stalePayload, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(path); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := initializeFresh(t, engine, path, resources.ServiceVolumeOwnerRecord); err != nil {
			t.Fatalf("fresh directory was rejected by orphan record: %v", err)
		}
		after, err := os.ReadFile(record)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Equal(stalePayload, after) {
			t.Fatal("orphan owner record was not rebound to the fresh directory identity")
		}
		var rebound serviceVolumeOwnerRecord
		var stat unix.Stat_t
		if err := json.Unmarshal(after, &rebound); err != nil {
			t.Fatal(err)
		}
		if err := unix.Stat(path, &stat); err != nil {
			t.Fatal(err)
		}
		if rebound.Device != uint64(stat.Dev) || rebound.Inode != stat.Ino || rebound.UID != uid || rebound.GID != gid {
			t.Fatalf("rebound owner record = %+v, directory = %+v", rebound, stat)
		}
	})

	t.Run("missing-marker-with-data", func(t *testing.T) {
		engine, resources, path, record := newFixture(t, "missing-marker-with-data")
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := initializeFresh(t, engine, path, resources.ServiceVolumeOwnerRecord); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "data"), []byte("retained\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(record); err != nil {
			t.Fatal(err)
		}
		var before unix.Stat_t
		if err := unix.Stat(path, &before); err != nil {
			t.Fatal(err)
		}
		if err := engine.initializeServiceVolume(path, resources.ServiceVolumeOwnerRecord, nil, uid, gid); err != nil {
			t.Fatalf("matching directory without record was rejected: %v", err)
		}
		var after unix.Stat_t
		if err := unix.Stat(path, &after); err != nil {
			t.Fatal(err)
		}
		if before.Ctim != after.Ctim {
			t.Fatal("matching data directory was re-chowned while reconstructing its record")
		}
		if _, err := os.Stat(record); err != nil {
			t.Fatalf("owner record was not reconstructed: %v", err)
		}
	})

	t.Run("crash-between-mkdir-and-marker", func(t *testing.T) {
		engine, resources, path, record := newFixture(t, "mkdir-marker-crash")
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := engine.initializeServiceVolume(path, resources.ServiceVolumeOwnerRecord, nil, uid, gid); err != nil {
			t.Fatalf("interrupted empty directory initialization was not recovered: %v", err)
		}
		if _, err := os.Stat(record); err != nil {
			t.Fatalf("owner record was not durably completed: %v", err)
		}
	})

	t.Run("owner-mismatch-is-typed", func(t *testing.T) {
		engine, resources, path, _ := newFixture(t, "owner-mismatch")
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "data"), []byte("retained\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		err := engine.initializeServiceVolume(path, resources.ServiceVolumeOwnerRecord, nil, uid+1, gid)
		var rejection *ServiceDataRejectionError
		if !errors.As(err, &rejection) || rejection.ActualUID != uid || rejection.WantedUID != uid+1 {
			t.Fatalf("owner mismatch = %#v, want typed actual/wanted rejection", err)
		}
	})

	t.Run("fresh-directory-swap", func(t *testing.T) {
		engine, resources, path, _ := newFixture(t, "fresh-directory-swap")
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
		fresh, err := serviceVolumeCreationAt(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(path, path+"-replaced"); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		err = engine.initializeServiceVolume(path, resources.ServiceVolumeOwnerRecord, fresh, uid, gid)
		var rejection *ServiceDataRejectionError
		if !errors.As(err, &rejection) || !strings.Contains(rejection.Reason, "identity changed") {
			t.Fatalf("fresh directory swap = %v, want typed identity rejection", err)
		}
	})
}

func TestServiceDataDirectoryAndOwnerRecordAreInventorySubjects(t *testing.T) {
	root := t.TempDir()
	resources, err := DeterministicResourceIdentity(AttemptAuthority{
		NodeID: "node", BootSessionID: "boot", JobID: "inventory-service", AttemptID: "attempt-1",
		FencingToken: "fence-1", Class: contract.JobClassService, RemovalGeneration: "attempt",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "service-data", resources.ServiceVolumeDirectory), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "service-data-state"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "service-data-state", resources.ServiceVolumeOwnerRecord), []byte("record\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	inventory := ResourceInventory{ManagedVolumes: []string{}, ManagedVolumeRecords: []string{}}
	if err := inventoryManagedVolumeResources(root, &inventory); err != nil {
		t.Fatal(err)
	}
	filtered := filterInventory(inventory, resources, nil)
	if !slices.Equal(filtered.ManagedVolumes, []string{resources.ServiceVolumeDirectory}) || !slices.Equal(filtered.ManagedVolumeRecords, []string{resources.ServiceVolumeOwnerRecord}) {
		t.Fatalf("service data inventory = %+v, want directory and owner record", filtered)
	}
}

func TestHandoffInventoryUsesDurableHandoffsRoot(t *testing.T) {
	root := t.TempDir()
	name, err := DeterministicHandoffVolumeDirectory("inventory-owner")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "handoffs", name), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "volumes", "legacy-wrong-root"), 0o700); err != nil {
		t.Fatal(err)
	}
	inventory := ResourceInventory{ManagedVolumes: []string{}, ManagedVolumeRecords: []string{}}
	if err := inventoryManagedVolumeResources(root, &inventory); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(inventory.ManagedVolumes, []string{name}) {
		t.Fatalf("managed volume inventory = %v, want durable handoff %q only", inventory.ManagedVolumes, name)
	}
}

func TestImageOperationMechanicsNeverClaimsUnknownErrorsAreInvalidManifests(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want ImageFailureKind
	}{
		{name: "unknown", err: errors.New("opaque engine failure"), want: ImageFailureUnavailable},
		{name: "ENOSPC", err: syscall.ENOSPC, want: ImageFailureResourceExhausted},
		{name: "snapshotter", err: errors.New("snapshotter prepare failed"), want: ImageFailureResourceExhausted},
		{name: "platform wrapped not found", err: fmt.Errorf("no match for platform: %w", os.ErrNotExist), want: ImageFailurePlatformMismatch},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var mechanics *ImageMechanicsError
			if !errors.As(classifyImageOperationError(test.err, "sha256:test"), &mechanics) || mechanics.Fact.Kind != test.want {
				t.Fatalf("mechanics classification = %+v", mechanics)
			}
		})
	}
}

func testContentManifest(t *testing.T, ctx context.Context, store content.Store, platform ocispec.Platform) ocispec.Descriptor {
	t.Helper()
	config, err := json.Marshal(ocispec.Image{Platform: platform})
	if err != nil {
		t.Fatal(err)
	}
	configDescriptor := ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig, Digest: digest.FromBytes(config), Size: int64(len(config))}
	if err := content.WriteBlob(ctx, store, "test-config-"+configDescriptor.Digest.Encoded(), bytes.NewReader(config), configDescriptor); err != nil {
		t.Fatal(err)
	}
	manifest := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"` + ocispec.MediaTypeImageConfig + `","digest":"` + configDescriptor.Digest.String() + `","size":` + fmt.Sprint(configDescriptor.Size) + `},"layers":[]}`)
	descriptor := ocispec.Descriptor{MediaType: ocispec.MediaTypeImageManifest, Digest: digest.FromBytes(manifest), Size: int64(len(manifest))}
	if err := content.WriteBlob(ctx, store, "test-manifest-"+descriptor.Digest.Encoded(), bytes.NewReader(manifest), descriptor); err != nil {
		t.Fatal(err)
	}
	return descriptor
}

func testContentIndex(t *testing.T, ctx context.Context, store content.Store, manifest ocispec.Descriptor) ocispec.Descriptor {
	t.Helper()
	index, err := json.Marshal(struct {
		SchemaVersion int                  `json:"schemaVersion"`
		MediaType     string               `json:"mediaType"`
		Manifests     []ocispec.Descriptor `json:"manifests"`
	}{SchemaVersion: 2, MediaType: ocispec.MediaTypeImageIndex, Manifests: []ocispec.Descriptor{manifest}})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := ocispec.Descriptor{MediaType: ocispec.MediaTypeImageIndex, Digest: digest.FromBytes(index), Size: int64(len(index))}
	if err := content.WriteBlob(ctx, store, "test-index-"+descriptor.Digest.Encoded(), bytes.NewReader(index), descriptor); err != nil {
		t.Fatal(err)
	}
	return descriptor
}

func descriptorWithPlatform(descriptor ocispec.Descriptor, platform ocispec.Platform) ocispec.Descriptor {
	descriptor.Platform = &platform
	return descriptor
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestLimaOperatorMountTranslationStaysWithinConfiguredRoots(t *testing.T) {
	engine := &ContainerdEngine{config: NativeEngineConfig{HostMountRoot: "/Users/operator/wefty", GuestMountRoot: "/mnt/wefty-host"}}
	translated, err := engine.translateOperatorMountSource("/Users/operator/wefty/project/data")
	if err != nil || translated != "/mnt/wefty-host/project/data" {
		t.Fatalf("translated source = %q, %v", translated, err)
	}
	for _, source := range []string{"/Users/operator/wefty", "/Users/operator/other", "/Users/operator/wefty/../escape"} {
		if _, err := engine.translateOperatorMountSource(source); err == nil {
			t.Fatalf("accepted unsafe host mount source %q", source)
		}
	}
}

func TestNewContainerdEngineRejectsGuestMountRootOutsideAllowedRoots(t *testing.T) {
	_, err := NewContainerdEngine(NativeEngineConfig{
		RuntimeRoot: t.TempDir(), HostMountRoot: "/Users/operator/wefty",
		GuestMountRoot: "/mnt/wefty-host", AllowedMountRoots: []string{"/srv/wefty"},
	})
	if err == nil || !strings.Contains(err.Error(), "inside an allowed mount root") {
		t.Fatalf("constructor error = %v", err)
	}
}

func TestDialAttemptPortProxiesOnlyTheRequestedLoopbackPort(t *testing.T) {
	backend, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	backendPort := uint16(backend.Addr().(*net.TCPAddr).Port)
	backendDone := make(chan error, 1)
	go func() {
		connection, err := backend.Accept()
		if err != nil {
			backendDone <- err
			return
		}
		defer connection.Close()
		payload := make([]byte, 4)
		if _, err := io.ReadFull(connection, payload); err == nil && string(payload) != "ping" {
			err = io.ErrUnexpectedEOF
		}
		if err == nil {
			_, err = connection.Write([]byte("pong"))
		}
		backendDone <- err
	}()
	client, helper := net.Pipe()
	engineDone := make(chan error, 1)
	go func() {
		engine := &ContainerdEngine{}
		engineDone <- engine.DialAttemptPort(t.Context(), DialAttemptPortRequest{Port: backendPort}, helper)
	}()
	ready := make([]byte, 1)
	if _, err := io.ReadFull(client, ready); err != nil || ready[0] != attemptPortBackendReady {
		t.Fatalf("attempt-port backend readiness = %v, %v", ready, err)
	}
	if _, err := client.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	response := make([]byte, 4)
	if _, err := io.ReadFull(client, response); err != nil || string(response) != "pong" {
		t.Fatalf("attempt-port response = %q, %v", response, err)
	}
	_ = client.Close()
	if err := <-backendDone; err != nil {
		t.Fatal(err)
	}
	if err := <-engineDone; err != nil && err != context.Canceled {
		t.Fatal(err)
	}
}

func TestComputerEndpointEnvironmentOverridesReservedPortsAndOmitsServicePort(t *testing.T) {
	environment, err := computerEndpointEnvironment([]EnvironmentVariable{
		{Name: contract.EnvServiceDir, Value: contract.OCIContainerServiceDirectory},
		{Name: contract.EnvServicePort, Value: "attacker-service"},
		{Name: contract.EnvComputerViewPort, Value: "attacker-view"},
		{Name: contract.EnvComputerControlPort, Value: "attacker-control"},
	}, map[string]uint16{"view": 42111, "control": 42112})
	if err != nil {
		t.Fatal(err)
	}
	values := make(map[string]string, len(environment))
	for _, variable := range environment {
		values[variable.Name] = variable.Value
	}
	if values[contract.EnvComputerViewPort] != "42111" || values[contract.EnvComputerControlPort] != "42112" ||
		values[contract.EnvServiceDir] != contract.OCIContainerServiceDirectory {
		t.Fatalf("Computer reserved environment = %+v", values)
	}
	if _, present := values[contract.EnvServicePort]; present {
		t.Fatalf("Computer retained WEFTY_SERVICE_PORT: %+v", values)
	}
	if _, err := computerEndpointEnvironment(nil, map[string]uint16{"view": 42111, "control": 42111}); err == nil {
		t.Fatal("Computer accepted duplicate endpoint ports")
	}
}

func TestDialAttemptPortAllowsClientHalfCloseBeforeFullResponse(t *testing.T) {
	backend, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	backendPort := uint16(backend.Addr().(*net.TCPAddr).Port)
	requestEOF := make(chan struct{})
	releaseResponse := make(chan struct{})
	go func() {
		connection, acceptErr := backend.Accept()
		if acceptErr != nil {
			return
		}
		defer connection.Close()
		_, _ = io.ReadAll(connection)
		close(requestEOF)
		<-releaseResponse
		_, _ = io.WriteString(connection, "full-response-after-request-eof")
	}()

	client, helper := tcpConnectionPair(t)
	defer client.Close()
	defer helper.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	engineDone := make(chan error, 1)
	go func() {
		engine := &ContainerdEngine{}
		engineDone <- engine.DialAttemptPort(ctx, DialAttemptPortRequest{Port: backendPort}, &operationStream{Conn: helper, cancel: cancel})
	}()
	var marker [1]byte
	if _, err := io.ReadFull(client, marker[:]); err != nil || marker[0] != attemptPortBackendReady {
		t.Fatalf("backend marker = %v, %v", marker, err)
	}
	if _, err := io.WriteString(client, "request"); err != nil {
		t.Fatal(err)
	}
	if err := client.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	<-requestEOF
	select {
	case <-ctx.Done():
		t.Fatal("request EOF cancelled the helper tunnel before the response")
	default:
	}
	close(releaseResponse)
	response, err := io.ReadAll(client)
	if err != nil || string(response) != "full-response-after-request-eof" {
		t.Fatalf("response = %q, %v", response, err)
	}
	if err := <-engineDone; err != nil {
		t.Fatal(err)
	}
}

func tcpConnectionPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan *net.TCPConn, 1)
	go func() {
		connection, _ := listener.AcceptTCP()
		accepted <- connection
	}()
	client, err := net.DialTCP("tcp4", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	return client, <-accepted
}

func TestAttemptPortAllocationRemainsAttemptScopedUntilRelease(t *testing.T) {
	probe, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := uint16(probe.Addr().(*net.TCPAddr).Port)
	probe.Close()
	engine := &ContainerdEngine{
		config: NativeEngineConfig{AttemptPortMin: port, AttemptPortMax: port},
		ports:  make(map[uint16]string), nextPort: port,
	}
	allocated, hold, err := engine.reserveAttemptPort("attempt-a")
	if err != nil || allocated != port {
		t.Fatalf("first allocation = %d, %v", allocated, err)
	}
	if _, _, err := engine.reserveAttemptPort("attempt-b"); err == nil {
		t.Fatal("allocated one reserved port to two attempts")
	}
	engine.releaseAttemptPort(port, "attempt-b")
	if _, _, err := engine.reserveAttemptPort("attempt-b"); err == nil {
		t.Fatal("wrong attempt released another attempt's port")
	}
	_ = hold.Close()
	engine.releaseAttemptPort(port, "attempt-a")
	if allocated, hold, err := engine.reserveAttemptPort("attempt-b"); err != nil || allocated != port {
		t.Fatalf("allocation after exact release = %d, %v", allocated, err)
	} else {
		_ = hold.Close()
	}
}

func TestTwoComputersReceiveFourUniqueLoopbackPorts(t *testing.T) {
	engine := &ContainerdEngine{
		config: NativeEngineConfig{AttemptPortMin: 25000, AttemptPortMax: 40000},
		ports:  make(map[uint16]string), nextPort: 25000,
	}
	type allocation struct {
		authority string
		port      uint16
		hold      net.Listener
	}
	allocations := make([]allocation, 0, 4)
	for _, authority := range []string{"computer-a", "computer-b"} {
		for range []string{"view", "control"} {
			port, hold, err := engine.reserveAttemptPort(authority)
			if err != nil {
				t.Fatal(err)
			}
			allocations = append(allocations, allocation{authority: authority, port: port, hold: hold})
		}
	}
	defer func() {
		for _, allocation := range allocations {
			_ = allocation.hold.Close()
			engine.releaseAttemptPort(allocation.port, allocation.authority)
		}
	}()
	ports := make(map[uint16]struct{}, len(allocations))
	for _, allocation := range allocations {
		host, port, err := net.SplitHostPort(allocation.hold.Addr().String())
		if err != nil || host != "127.0.0.1" || port != fmt.Sprint(allocation.port) {
			t.Fatalf("Computer allocation %q = %q, port=%d err=%v", allocation.authority, allocation.hold.Addr(), allocation.port, err)
		}
		if _, duplicate := ports[allocation.port]; duplicate {
			t.Fatalf("Computer endpoint port %d was allocated twice", allocation.port)
		}
		ports[allocation.port] = struct{}{}
	}
}

func TestAttemptEndpointOwnershipRejectsWildcardListener(t *testing.T) {
	wildcard, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer wildcard.Close()
	port := uint16(wildcard.Addr().(*net.TCPAddr).Port)
	if inode, found, err := loopbackListenInode(port); err != nil || found || inode != "" {
		t.Fatalf("wildcard listener was accepted as loopback ownership: inode=%q found=%t err=%v", inode, found, err)
	}
}

func TestResidualVerificationRetainsPortAgainstConcurrentAllocation(t *testing.T) {
	probe, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := uint16(probe.Addr().(*net.TCPAddr).Port)
	_ = probe.Close()
	engine := &ContainerdEngine{config: NativeEngineConfig{AttemptPortMin: port, AttemptPortMax: port}, attempts: make(map[string]*containerdAttempt), ports: make(map[uint16]string), nextPort: port}
	allocated, hold, err := engine.reserveAttemptPort("attempt-a")
	if err != nil {
		t.Fatal(err)
	}
	engine.attempts["attempt-a"] = &containerdAttempt{
		endpoints:     map[string]uint16{"service": allocated},
		endpointHolds: map[string]net.Listener{"service": hold},
	}
	// Forced residual verification means releaseVerifiedAttempt is deliberately
	// not called. Both logical and kernel ownership must remain unavailable.
	if _, _, err := engine.reserveAttemptPort("attempt-b"); err == nil {
		t.Fatal("residual verification recycled a live attempt port")
	}
	if err := engine.releaseVerifiedAttempt(t.Context(), "attempt-a"); err != nil {
		t.Fatal(err)
	}
	if _, nextHold, err := engine.reserveAttemptPort("attempt-b"); err != nil {
		t.Fatalf("verified absence did not release port: %v", err)
	} else {
		_ = nextHold.Close()
	}
}

func TestVerifiedAttemptPropagatesPinDeletionAndRetainsRetryState(t *testing.T) {
	key := testImageOperationKey("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	manager := &failingImageLeaseDeletionManager{err: errors.New("containerd lease delete failed")}
	engine := &ContainerdEngine{
		imageLeaseDeletes: manager,
		attemptImagePins:  map[string]imageOperationKey{"attempt-a": key},
		attempts:          map[string]*containerdAttempt{"attempt-a": {}},
		ports:             map[uint16]string{},
	}
	err := engine.releaseVerifiedAttempt(t.Context(), "attempt-a")
	if err == nil || !strings.Contains(err.Error(), "containerd lease delete failed") {
		t.Fatalf("release error = %v", err)
	}
	if _, retained := engine.attemptImagePins["attempt-a"]; !retained {
		t.Fatal("failed lease deletion discarded retryable attempt pin state")
	}
	if _, retained := engine.attempts["attempt-a"]; retained {
		t.Fatal("verified runtime state was retained with independent image-pin failure")
	}
	if !manager.sawDeadline {
		t.Fatal("attempt image-pin deletion was not bounded")
	}
}

func TestStructLiteralEngineCloseIsNilChannelSafeAndIdempotent(t *testing.T) {
	engine := &ContainerdEngine{imageOperations: newImageOperationGroup()}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
}

type failingImageLeaseDeletionManager struct {
	err         error
	sawDeadline bool
}

func (manager *failingImageLeaseDeletionManager) Delete(ctx context.Context, _ leases.Lease, _ ...leases.DeleteOpt) error {
	_, manager.sawDeadline = ctx.Deadline()
	return manager.err
}

func TestAttemptPortRejectsAdversarialBindOutsidePayloadCgroup(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := uint16(listener.Addr().(*net.TCPAddr).Port)
	cgroupRoot := t.TempDir()
	cgroupID := "attempt-cgroup"
	if err := os.Mkdir(filepath.Join(cgroupRoot, cgroupID), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cgroupRoot, cgroupID, "cgroup.procs"), []byte("99999999\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{CgroupRoot: cgroupRoot}}
	err = engine.waitAttemptPortOwnership(t.Context(), cgroupID, port)
	if err == nil || !strings.Contains(err.Error(), "outside the attempt cgroup") {
		t.Fatalf("adversarial bind error = %v", err)
	}
}

func TestPreparedMacFallbackRewritesEndpointOnlyWhenActivated(t *testing.T) {
	original := []EnvironmentVariable{{Name: contract.EnvL3Endpoint, Value: "http://host.lima.internal:4242/l3"}}
	address := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 4343}
	dormant, guestEndpoint, err := fallbackBridgeEnvironment(original, address, false)
	if err != nil {
		t.Fatal(err)
	}
	if dormant[0].Value != original[0].Value || guestEndpoint != "http://127.0.0.1:4343/l3" {
		t.Fatalf("dormant fallback environment=%v guest=%q", dormant, guestEndpoint)
	}
	active, guestEndpoint, err := fallbackBridgeEnvironment(original, address, true)
	if err != nil {
		t.Fatal(err)
	}
	if active[0].Value != guestEndpoint || guestEndpoint != "http://127.0.0.1:4343/l3" {
		t.Fatalf("active fallback environment=%v guest=%q", active, guestEndpoint)
	}
	defaultOff, guestEndpoint, err := fallbackBridgeEnvironment(nil, address, false)
	if err != nil || len(defaultOff) != 0 || guestEndpoint == "" {
		t.Fatalf("default-off dormant fallback environment=%v guest=%q err=%v", defaultOff, guestEndpoint, err)
	}
}

func TestDialHostBridgePairsOnlyTheAttemptsGuestListener(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	authority := AttemptAuthority{NodeID: "node", JobID: "job", AttemptID: "attempt", FencingToken: "fence", BootSessionID: "boot", Class: "one-shot", RemovalGeneration: "attempt"}
	engine := &ContainerdEngine{attempts: map[string]*containerdAttempt{authority.key(): {authority: authority, hostBridge: listener}}}
	host, helper := net.Pipe()
	engineDone := make(chan error, 1)
	go func() {
		engineDone <- engine.DialHostBridge(t.Context(), DialHostBridgeRequest{Authority: authority}, helper)
	}()
	guest, err := net.Dial("tcp4", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := guest.Write([]byte("guest")); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 5)
	if _, err := io.ReadFull(host, payload); err != nil || string(payload) != "guest" {
		t.Fatalf("host payload = %q, %v", payload, err)
	}
	if _, err := host.Write([]byte("host")); err != nil {
		t.Fatal(err)
	}
	payload = make([]byte, 4)
	if _, err := io.ReadFull(guest, payload); err != nil || string(payload) != "host" {
		t.Fatalf("guest payload = %q, %v", payload, err)
	}
	_ = guest.Close()
	_ = host.Close()
	if err := <-engineDone; err != nil && err != context.Canceled {
		t.Fatal(err)
	}
}
