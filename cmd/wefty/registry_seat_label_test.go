package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

type seatLabelRegistry struct {
	t        *testing.T
	blobs    map[string][]byte
	tampered map[string]bool
	paths    []string
}

func (r *seatLabelRegistry) add(value any) string {
	r.t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		r.t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	r.blobs[digest] = body
	return digest
}

func (r *seatLabelRegistry) config(labels map[string]string) string {
	return r.add(map[string]any{"config": map[string]any{"Labels": labels}})
}

func (r *seatLabelRegistry) manifest(config string) string {
	return r.add(map[string]any{"mediaType": "application/vnd.oci.image.manifest.v1+json", "config": map[string]string{"digest": config}})
}

func (r *seatLabelRegistry) resolver() *registryResolver {
	return newRegistryResolver(&http.Client{Transport: imageRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		r.paths = append(r.paths, request.URL.Path)
		if request.URL.Host == "auth.example.test" {
			return jsonResponse(http.StatusOK, map[string]string{"token": "pull-token"}), nil
		}
		if request.Method != http.MethodGet || request.URL.Host != "ghcr.io" {
			r.t.Fatalf("unexpected registry request %s %s", request.Method, request.URL)
		}
		if request.Header.Get("Authorization") != "Bearer pull-token" {
			response := jsonResponse(http.StatusUnauthorized, map[string]any{})
			response.Header.Set("WWW-Authenticate", `Bearer realm="https://auth.example.test/token",service="ghcr.io",scope="repository:example/desk:pull"`)
			return response, nil
		}
		digest := request.URL.Path[strings.LastIndexByte(request.URL.Path, '/')+1:]
		body, ok := r.blobs[digest]
		if !ok {
			return jsonResponse(http.StatusNotFound, map[string]any{}), nil
		}
		if r.tampered[digest] {
			body = append(bytes.Clone(body), ' ')
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(body))}, nil
	})})
}

// TestImageSeatPlacesReadsEveryLinuxPlatformLabel pins the Seat support rule of
// docs/contracts/computer-image.md: every Linux platform must carry the same
// valid label; attestation entries are skipped; content must match its digest.
func TestImageSeatPlacesReadsEveryLinuxPlatformLabel(t *testing.T) {
	registry := &seatLabelRegistry{t: t, blobs: map[string][]byte{}, tampered: map[string]bool{}}
	labelled := map[string]string{imageSeatLabel: "4"}
	index := func(configs ...map[string]string) string {
		manifests := []map[string]any{}
		for i, labels := range configs {
			manifests = append(manifests, map[string]any{"digest": registry.manifest(registry.config(labels)),
				"platform": map[string]string{"os": "linux", "architecture": []string{"amd64", "arm64"}[i%2]}})
		}
		// An attestation manifest has no image config labels and must not count.
		manifests = append(manifests, map[string]any{"digest": registry.manifest(registry.config(nil)),
			"platform": map[string]string{"os": "unknown", "architecture": "unknown"}})
		return registry.add(map[string]any{"mediaType": "application/vnd.oci.image.index.v1+json", "manifests": manifests})
	}
	for _, test := range []struct {
		name   string
		digest string
		want   int
	}{
		{"index with the label on every platform", index(labelled, labelled), 4},
		{"index with one platform unlabelled", index(labelled, nil), 0},
		{"index whose platforms disagree", index(labelled, map[string]string{imageSeatLabel: "2"}), 0},
		{"label out of range", index(map[string]string{imageSeatLabel: "5"}), 0},
		{"single-platform manifest", registry.manifest(registry.config(map[string]string{imageSeatLabel: "2"})), 2},
		{"image without the label", registry.manifest(registry.config(map[string]string{"other": "4"})), 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := registry.resolver().ImageSeatPlaces(context.Background(), "ghcr.io/example/desk:latest", test.digest)
			if err != nil || got != test.want {
				t.Fatalf("ImageSeatPlaces = %d, %v; want %d", got, err, test.want)
			}
		})
	}

	tampered := index(labelled)
	registry.tampered[tampered] = true
	if _, err := registry.resolver().ImageSeatPlaces(context.Background(), "ghcr.io/example/desk", tampered); err == nil || !strings.Contains(err.Error(), "does not match digest") {
		t.Fatalf("tampered index err = %v", err)
	}
	missing := "sha256:" + strings.Repeat("e", 64)
	if _, err := registry.resolver().ImageSeatPlaces(context.Background(), "ghcr.io/example/desk", missing); err == nil {
		t.Fatal("unknown image digest read without an error")
	}
}
