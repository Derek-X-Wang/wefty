package ocihelper

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/containerd/containerd/v2/core/images"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestInspectOCIArchiveVerifiesGraphPlatformAndContainerdExtensions(t *testing.T) {
	archive, topDigest, manifestDigest := testOCIArchive(t, false, false)
	inspection, err := inspectOCIArchive(t.Context(), t.TempDir(), bytes.NewReader(archive), "example.invalid/wefty:test", topDigest.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = removeTestArchive(inspection.Path) })
	if inspection.TopLevel.Digest != topDigest || inspection.PlatformDigest != manifestDigest || inspection.Reference != "example.invalid/wefty:test" {
		t.Fatalf("archive inspection = %+v", inspection)
	}
}

func TestInspectOCIArchiveRejectsRecomputedDigestMismatch(t *testing.T) {
	archive, _, _ := testOCIArchive(t, true, false)
	if _, err := inspectOCIArchive(t.Context(), t.TempDir(), bytes.NewReader(archive), "", ""); err == nil {
		t.Fatal("corrupt OCI archive was accepted")
	}
}

func TestInspectOCIArchiveNormalizesDigestExportAnnotationToProvenanceName(t *testing.T) {
	archive, topDigest, _ := testOCIArchive(t, false, true)
	inspection, err := inspectOCIArchive(t.Context(), t.TempDir(), bytes.NewReader(archive), "example.invalid/wefty:latest", topDigest.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = removeTestArchive(inspection.Path) })
	if inspection.Reference != "example.invalid/wefty:latest" {
		t.Fatalf("normalized archive reference = %q", inspection.Reference)
	}
}

func TestSpoolOCIArchiveRejectsTotalByteOverflow(t *testing.T) {
	var destination bytes.Buffer
	err := spoolOCIArchive(t.Context(), &destination, bytes.NewReader([]byte("123456789")), 8)
	if err == nil || !errors.Is(err, errOCIArchiveTooLarge) {
		t.Fatalf("over-limit spool error = %v", err)
	}
}

func TestInspectOCIArchiveRejectsUnsafeTarShapesBeforeContainerd(t *testing.T) {
	tests := []struct {
		name    string
		headers []tar.Header
	}{
		{name: "traversal", headers: []tar.Header{{Name: "../index.json", Typeflag: tar.TypeReg}}},
		{name: "duplicate", headers: []tar.Header{{Name: "oci-layout", Typeflag: tar.TypeReg}, {Name: "oci-layout", Typeflag: tar.TypeReg}}},
		{name: "non-regular", headers: []tar.Header{{Name: "index.json", Typeflag: tar.TypeSymlink, Linkname: "elsewhere"}}},
		{name: "oversized declared entry", headers: []tar.Header{{Name: "index.json", Typeflag: tar.TypeReg, Size: maxOCIArchiveBytes + 1}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var archive bytes.Buffer
			writer := tar.NewWriter(&archive)
			for index := range test.headers {
				header := test.headers[index]
				header.Mode = 0o600
				if err := writer.WriteHeader(&header); err != nil {
					t.Fatal(err)
				}
			}
			_ = writer.Close()
			if _, err := inspectOCIArchive(t.Context(), t.TempDir(), bytes.NewReader(archive.Bytes()), "", ""); err == nil {
				t.Fatal("unsafe OCI archive was accepted")
			}
		})
	}
}

func TestInspectOCIArchiveRejectsTruncatedTar(t *testing.T) {
	archive, _, _ := testOCIArchive(t, false, false)
	if _, err := inspectOCIArchive(t.Context(), t.TempDir(), bytes.NewReader(archive[:len(archive)-700]), "", ""); err == nil {
		t.Fatal("truncated OCI archive was accepted")
	}
}

func TestInspectOCIArchiveFiltersForeignPlatformBlobsBeforeImport(t *testing.T) {
	archive, topDigest, selected, foreign := testMultiPlatformOCIArchive(t)
	inspection, err := inspectOCIArchive(t.Context(), t.TempDir(), bytes.NewReader(archive), "example.invalid/wefty:test", topDigest.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = removeTestArchive(inspection.Path) })
	if inspection.PlatformDigest != selected {
		t.Fatalf("selected platform digest = %s, want %s", inspection.PlatformDigest, selected)
	}
	file, err := os.Open(inspection.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	names := make(map[string]struct{})
	reader := tar.NewReader(file)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		names[header.Name] = struct{}{}
	}
	if _, exists := names["blobs/sha256/"+foreign.Encoded()]; exists {
		t.Fatal("foreign-platform manifest entered the filtered import archive")
	}
	for _, required := range []string{"oci-layout", "index.json", "blobs/sha256/" + topDigest.Encoded(), "blobs/sha256/" + selected.Encoded()} {
		if _, exists := names[required]; !exists {
			t.Fatalf("filtered archive is missing %s", required)
		}
	}
}

func testMultiPlatformOCIArchive(t *testing.T) ([]byte, digest.Digest, digest.Digest, digest.Digest) {
	t.Helper()
	type platformImage struct {
		manifest   ocispec.Descriptor
		manifestB  []byte
		config     ocispec.Descriptor
		configB    []byte
		layer      ocispec.Descriptor
		layerBytes []byte
	}
	makeImage := func(architecture string, layerBytes []byte) platformImage {
		configBytes, err := json.Marshal(ocispec.Image{Platform: ocispec.Platform{OS: runtime.GOOS, Architecture: architecture}})
		if err != nil {
			t.Fatal(err)
		}
		config := descriptor(ocispec.MediaTypeImageConfig, configBytes)
		layers := []ocispec.Descriptor{}
		var layer ocispec.Descriptor
		if layerBytes != nil {
			layer = descriptor(ocispec.MediaTypeImageLayer, layerBytes)
			layers = append(layers, layer)
		}
		manifestBytes, err := json.Marshal(map[string]any{"schemaVersion": 2, "mediaType": ocispec.MediaTypeImageManifest, "config": config, "layers": layers})
		if err != nil {
			t.Fatal(err)
		}
		manifest := descriptor(ocispec.MediaTypeImageManifest, manifestBytes)
		manifest.Platform = &ocispec.Platform{OS: runtime.GOOS, Architecture: architecture}
		return platformImage{manifest: manifest, manifestB: manifestBytes, config: config, configB: configBytes, layer: layer, layerBytes: layerBytes}
	}
	foreignArchitecture := "arm64"
	if runtime.GOARCH == "arm64" {
		foreignArchitecture = "amd64"
	}
	selected := makeImage(runtime.GOARCH, nil)
	foreign := makeImage(foreignArchitecture, []byte(strings.Repeat("foreign-platform-layer", 4096)))
	indexBytes, err := json.Marshal(map[string]any{"schemaVersion": 2, "mediaType": ocispec.MediaTypeImageIndex, "manifests": []ocispec.Descriptor{selected.manifest, foreign.manifest}})
	if err != nil {
		t.Fatal(err)
	}
	top := descriptor(ocispec.MediaTypeImageIndex, indexBytes)
	top.Annotations = map[string]string{images.AnnotationImageName: "example.invalid/wefty:test"}
	rootBytes, err := json.Marshal(map[string]any{"schemaVersion": 2, "manifests": []ocispec.Descriptor{top}})
	if err != nil {
		t.Fatal(err)
	}
	entries := map[string][]byte{
		"oci-layout": []byte(`{"imageLayoutVersion":"1.0.0"}`), "index.json": rootBytes,
		"blobs/sha256/" + top.Digest.Encoded():               indexBytes,
		"blobs/sha256/" + selected.manifest.Digest.Encoded(): selected.manifestB,
		"blobs/sha256/" + selected.config.Digest.Encoded():   selected.configB,
		"blobs/sha256/" + foreign.manifest.Digest.Encoded():  foreign.manifestB,
		"blobs/sha256/" + foreign.config.Digest.Encoded():    foreign.configB,
		"blobs/sha256/" + foreign.layer.Digest.Encoded():     foreign.layerBytes,
	}
	var output bytes.Buffer
	writer := tar.NewWriter(&output)
	for name, payload := range entries {
		if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(payload)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes(), top.Digest, selected.manifest.Digest, foreign.manifest.Digest
}

func testOCIArchive(t *testing.T, corruptConfig, digestedReference bool) ([]byte, digest.Digest, digest.Digest) {
	t.Helper()
	config, err := json.Marshal(ocispec.Image{Platform: ocispec.Platform{OS: runtime.GOOS, Architecture: runtime.GOARCH}})
	if err != nil {
		t.Fatal(err)
	}
	configDescriptor := descriptor(ocispec.MediaTypeImageConfig, config)
	manifest, err := json.Marshal(ocispec.Manifest{
		Versioned: ocispec.Manifest{}.Versioned,
		Config:    configDescriptor,
		Layers:    []ocispec.Descriptor{},
	})
	if err != nil {
		t.Fatal(err)
	}
	var manifestDocument map[string]any
	if err := json.Unmarshal(manifest, &manifestDocument); err != nil {
		t.Fatal(err)
	}
	manifestDocument["schemaVersion"] = float64(2)
	manifest, err = json.Marshal(manifestDocument)
	if err != nil {
		t.Fatal(err)
	}
	manifestDescriptor := descriptor(ocispec.MediaTypeImageManifest, manifest)
	manifestDescriptor.Platform = &ocispec.Platform{OS: runtime.GOOS, Architecture: runtime.GOARCH}
	indexBlob, err := json.Marshal(ocispec.Index{
		Versioned: ocispec.Index{}.Versioned,
		Manifests: []ocispec.Descriptor{manifestDescriptor},
	})
	if err != nil {
		t.Fatal(err)
	}
	var indexDocument map[string]any
	if err := json.Unmarshal(indexBlob, &indexDocument); err != nil {
		t.Fatal(err)
	}
	indexDocument["schemaVersion"] = float64(2)
	indexBlob, err = json.Marshal(indexDocument)
	if err != nil {
		t.Fatal(err)
	}
	indexDescriptor := descriptor(ocispec.MediaTypeImageIndex, indexBlob)
	reference := "example.invalid/wefty:test"
	if digestedReference {
		reference = "example.invalid/wefty@" + indexDescriptor.Digest.String()
	}
	indexDescriptor.Annotations = map[string]string{
		images.AnnotationImageName: reference,
		ocispec.AnnotationRefName:  "test",
	}
	rootIndex, err := json.Marshal(ocispec.Index{Manifests: []ocispec.Descriptor{indexDescriptor}})
	if err != nil {
		t.Fatal(err)
	}
	var rootDocument map[string]any
	if err := json.Unmarshal(rootIndex, &rootDocument); err != nil {
		t.Fatal(err)
	}
	rootDocument["schemaVersion"] = float64(2)
	rootIndex, err = json.Marshal(rootDocument)
	if err != nil {
		t.Fatal(err)
	}
	layout := []byte(`{"imageLayoutVersion":"1.0.0"}`)
	if corruptConfig {
		config = append(config, '\n')
	}
	entries := map[string][]byte{
		"oci-layout":    layout,
		"index.json":    rootIndex,
		"manifest.json": []byte("[]"),
		"blobs/sha256/" + indexDescriptor.Digest.Encoded():    indexBlob,
		"blobs/sha256/" + manifestDescriptor.Digest.Encoded(): manifest,
		"blobs/sha256/" + configDescriptor.Digest.Encoded():   config,
	}
	var output bytes.Buffer
	writer := tar.NewWriter(&output)
	for _, name := range []string{"blobs/", "blobs/sha256/"} {
		if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0o700, Typeflag: tar.TypeDir}); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"oci-layout", "index.json", "manifest.json", "blobs/sha256/" + indexDescriptor.Digest.Encoded(), "blobs/sha256/" + manifestDescriptor.Digest.Encoded(), "blobs/sha256/" + configDescriptor.Digest.Encoded()} {
		payload := entries[name]
		if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(payload)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes(), indexDescriptor.Digest, manifestDescriptor.Digest
}

func descriptor(mediaType string, payload []byte) ocispec.Descriptor {
	return ocispec.Descriptor{MediaType: mediaType, Digest: digest.FromBytes(payload), Size: int64(len(payload))}
}

func removeTestArchive(path string) error {
	return os.Remove(path)
}
