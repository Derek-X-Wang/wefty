//go:build service_acceptance_realtiming && linux

package ocihelper_test

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type refloatRegistry struct {
	server      *httptest.Server
	blobs       map[string][]byte
	mediaTypes  map[string]string
	original    ocispec.Descriptor
	currentTag  string
	tagRequests int
	mu          sync.Mutex
}

func newRefloatRegistry(t *testing.T, archivePath string) *refloatRegistry {
	t.Helper()
	archive, err := os.Open(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	registry := &refloatRegistry{blobs: make(map[string][]byte), mediaTypes: make(map[string]string)}
	reader := tar.NewReader(archive)
	var index ocispec.Index
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		payload, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		name := strings.TrimPrefix(header.Name, "./")
		if name == "index.json" {
			if err := json.Unmarshal(payload, &index); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if strings.HasPrefix(name, "blobs/sha256/") {
			digest := "sha256:" + strings.TrimPrefix(name, "blobs/sha256/")
			registry.blobs[digest] = payload
			var typed struct {
				MediaType string `json:"mediaType"`
			}
			if json.Unmarshal(payload, &typed) == nil && typed.MediaType != "" {
				registry.mediaTypes[digest] = typed.MediaType
			}
		}
	}
	if len(index.Manifests) != 1 {
		t.Fatalf("probe archive top-level descriptors = %d, want 1", len(index.Manifests))
	}
	registry.original = index.Manifests[0]
	registry.mediaTypes[registry.original.Digest.String()] = registry.original.MediaType
	registry.currentTag = registry.original.Digest.String()
	registry.server = httptest.NewServer(http.HandlerFunc(registry.serveHTTP))
	t.Cleanup(registry.server.Close)
	return registry
}

func (registry *refloatRegistry) reference() string {
	return strings.TrimPrefix(registry.server.URL, "http://") + "/wefty/refloat:latest"
}

func (registry *refloatRegistry) originalDigest() string { return registry.original.Digest.String() }

func (registry *refloatRegistry) addVariant(t *testing.T, label string) string {
	t.Helper()
	registry.mu.Lock()
	payload := append([]byte(nil), registry.blobs[registry.original.Digest.String()]...)
	mediaType := registry.mediaTypes[registry.original.Digest.String()]
	registry.mu.Unlock()
	var document map[string]any
	if err := json.Unmarshal(payload, &document); err != nil {
		t.Fatal(err)
	}
	annotations, _ := document["annotations"].(map[string]any)
	if annotations == nil {
		annotations = make(map[string]any)
	}
	annotations["dev.wefty.cache-variant"] = label
	document["annotations"] = annotations
	payload, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(payload)
	value := "sha256:" + hex.EncodeToString(hash[:])
	registry.mu.Lock()
	registry.blobs[value] = payload
	registry.mediaTypes[value] = mediaType
	registry.mu.Unlock()
	return value
}

func (registry *refloatRegistry) moveTag() {
	payload := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","size":2},"layers":[]}`)
	hash := sha256.Sum256(payload)
	digest := "sha256:" + hex.EncodeToString(hash[:])
	registry.mu.Lock()
	registry.blobs[digest] = payload
	registry.mediaTypes[digest] = "application/vnd.oci.image.manifest.v1+json"
	registry.currentTag = digest
	registry.mu.Unlock()
}

func (registry *refloatRegistry) observedTagRequests() int {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return registry.tagRequests
}

func (registry *refloatRegistry) serveHTTP(response http.ResponseWriter, request *http.Request) {
	if request.URL.Path == "/v2/" {
		response.WriteHeader(http.StatusOK)
		return
	}
	const manifestPrefix = "/v2/wefty/refloat/manifests/"
	const blobPrefix = "/v2/wefty/refloat/blobs/"
	var digest string
	registry.mu.Lock()
	switch {
	case strings.HasPrefix(request.URL.Path, manifestPrefix):
		selector := strings.TrimPrefix(request.URL.Path, manifestPrefix)
		if selector == "latest" {
			registry.tagRequests++
			digest = registry.currentTag
		} else {
			digest = selector
		}
	case strings.HasPrefix(request.URL.Path, blobPrefix):
		digest = strings.TrimPrefix(request.URL.Path, blobPrefix)
	}
	payload, ok := registry.blobs[digest]
	mediaType := registry.mediaTypes[digest]
	registry.mu.Unlock()
	if !ok {
		http.NotFound(response, request)
		return
	}
	if mediaType == "" {
		mediaType = "application/octet-stream"
	}
	response.Header().Set("Content-Type", mediaType)
	response.Header().Set("Docker-Content-Digest", digest)
	response.Header().Set("Content-Length", fmt.Sprint(len(payload)))
	if request.Method != http.MethodHead {
		_, _ = response.Write(payload)
	}
}
