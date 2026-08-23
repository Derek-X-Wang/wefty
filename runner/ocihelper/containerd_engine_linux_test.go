//go:build linux

package ocihelper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/core/content"
	contentlocal "github.com/containerd/containerd/v2/plugins/content/local"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

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
			_, _, err := selectedManifest(ctx, store, test.target)
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
