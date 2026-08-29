package ocihelper

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/platforms"
	distributionref "github.com/distribution/reference"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	maxArchiveMetadataBytes      = 1 << 20
	maxArchiveMetadataTotalBytes = 64 << 20
	maxArchiveEntries            = 100_000
	maxOCIArchiveBytes           = 16 << 30
)

var errOCIArchiveTooLarge = errors.New("OCI archive exceeds the helper byte bound")

// ociArchiveInspectionError marks an archive rejection whose quoted,
// input-derived reason is safe to expose as helper mechanics evidence.
type ociArchiveInspectionError struct{ reason string }

func (err *ociArchiveInspectionError) Error() string { return err.reason }

func archiveInspectionErrorf(format string, arguments ...any) error {
	return &ociArchiveInspectionError{reason: fmt.Sprintf(format, arguments...)}
}

type ociArchiveInspection struct {
	Path           string
	Reference      string
	TopLevel       ocispec.Descriptor
	Platform       ocispec.Platform
	PlatformDigest digest.Digest
}

type archiveBlob struct {
	size    int64
	payload []byte
}

func inspectOCIArchive(ctx context.Context, runtimeRoot string, source io.Reader, requestedReference string, requestedDigest string) (_ ociArchiveInspection, returnErr error) {
	return inspectOCIArchiveWithSpoolForPlatform(ctx, runtimeRoot, source, requestedReference, requestedDigest, platforms.DefaultStrict(), func(directory string) (*os.File, error) {
		return os.CreateTemp(directory, "wefty-image-*.tar")
	})
}

func inspectOCIArchiveWithSpool(ctx context.Context, runtimeRoot string, source io.Reader, requestedReference string, requestedDigest string, createSpool func(string) (*os.File, error)) (_ ociArchiveInspection, returnErr error) {
	return inspectOCIArchiveWithSpoolForPlatform(ctx, runtimeRoot, source, requestedReference, requestedDigest, platforms.DefaultStrict(), createSpool)
}

func inspectOCIArchiveWithSpoolForPlatform(ctx context.Context, runtimeRoot string, source io.Reader, requestedReference string, requestedDigest string, matcher platforms.MatchComparer, createSpool func(string) (*os.File, error)) (_ ociArchiveInspection, returnErr error) {
	if source == nil {
		return ociArchiveInspection{}, errors.New("OCI archive stream is required")
	}
	directory := filepath.Join(runtimeRoot, "imports")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return ociArchiveInspection{}, err
	}
	file, err := createSpool(directory)
	if err != nil {
		return ociArchiveInspection{}, err
	}
	archivePath := file.Name()
	defer func() {
		if file != nil {
			if closeErr := file.Close(); returnErr == nil {
				returnErr = closeErr
			}
		}
		if returnErr != nil {
			_ = os.Remove(archivePath)
		}
	}()
	if err := spoolOCIArchive(ctx, file, source, maxOCIArchiveBytes); err != nil {
		return ociArchiveInspection{}, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return ociArchiveInspection{}, err
	}

	seen := make(map[string]struct{})
	blobs := make(map[digest.Digest]archiveBlob)
	var indexBytes, layoutBytes []byte
	var metadataBytes int64
	entries := 0
	reader := tar.NewReader(file)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return ociArchiveInspection{}, fmt.Errorf("read OCI archive: %w", err)
		}
		entries++
		if entries > maxArchiveEntries {
			return ociArchiveInspection{}, errors.New("OCI archive entry count exceeds the helper bound")
		}
		rawName := strings.TrimPrefix(header.Name, "./")
		name := path.Clean(rawName)
		if name == "." && header.FileInfo().IsDir() && (header.Name == "." || header.Name == "./") {
			continue
		}
		if name == "." || strings.HasPrefix(name, "../") || path.IsAbs(name) {
			return ociArchiveInspection{}, archiveInspectionErrorf("OCI archive path %q is unsafe", header.Name)
		}
		if header.FileInfo().IsDir() {
			if rawName != name && rawName != name+"/" {
				return ociArchiveInspection{}, fmt.Errorf("OCI archive path %q is not canonical", header.Name)
			}
			continue
		}
		if rawName != name {
			return ociArchiveInspection{}, fmt.Errorf("OCI archive path %q is not canonical", header.Name)
		}
		if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
			return ociArchiveInspection{}, fmt.Errorf("OCI archive entry %q is not a regular file", name)
		}
		if header.Size < 0 || header.Size > maxOCIArchiveBytes {
			return ociArchiveInspection{}, fmt.Errorf("OCI archive entry %q exceeds the helper byte bound", name)
		}
		if _, exists := seen[name]; exists {
			return ociArchiveInspection{}, fmt.Errorf("OCI archive entry %q is duplicated", name)
		}
		seen[name] = struct{}{}
		switch name {
		case "index.json":
			indexBytes, err = readArchiveMetadata(reader, header.Size)
		case "oci-layout":
			layoutBytes, err = readArchiveMetadata(reader, header.Size)
		default:
			// OCI image layouts are explicitly extensible, and containerd's
			// own exporter includes a Docker-compatible manifest.json. Ignore
			// bounded, regular extension files after applying the archive-wide
			// path, duplicate, size, and byte-count checks above.
			if !strings.HasPrefix(name, "blobs/") {
				continue
			}
			parts := strings.Split(name, "/")
			if len(parts) != 3 || parts[0] != "blobs" || parts[1] != "sha256" || len(parts[2]) != 64 {
				return ociArchiveInspection{}, fmt.Errorf("OCI archive blob path %q is invalid", name)
			}
			expected := digest.Digest("sha256:" + parts[2])
			if err := expected.Validate(); err != nil {
				return ociArchiveInspection{}, fmt.Errorf("OCI archive blob path %q is invalid", name)
			}
			hasher := sha256.New()
			var metadata strings.Builder
			writer := io.Writer(hasher)
			if header.Size <= maxArchiveMetadataBytes {
				metadataBytes += header.Size
				if metadataBytes > maxArchiveMetadataTotalBytes {
					return ociArchiveInspection{}, errors.New("OCI archive metadata total exceeds the helper bound")
				}
				writer = io.MultiWriter(hasher, &metadata)
			}
			written, copyErr := io.Copy(writer, reader)
			if copyErr != nil {
				return ociArchiveInspection{}, copyErr
			}
			if written != header.Size {
				return ociArchiveInspection{}, fmt.Errorf("OCI archive blob %s is truncated", expected)
			}
			actual := digest.Digest("sha256:" + hex.EncodeToString(hasher.Sum(nil)))
			if actual != expected {
				return ociArchiveInspection{}, fmt.Errorf("OCI archive blob %s digest mismatch", expected)
			}
			blob := archiveBlob{size: written}
			if header.Size <= maxArchiveMetadataBytes {
				blob.payload = []byte(metadata.String())
			}
			blobs[expected] = blob
		}
		if err != nil {
			return ociArchiveInspection{}, err
		}
	}

	var layout ocispec.ImageLayout
	if len(layoutBytes) == 0 || json.Unmarshal(layoutBytes, &layout) != nil || layout.Version != ocispec.ImageLayoutVersion {
		return ociArchiveInspection{}, errors.New("OCI archive has an invalid oci-layout")
	}
	var index ocispec.Index
	if len(indexBytes) == 0 || json.Unmarshal(indexBytes, &index) != nil || index.SchemaVersion != 2 {
		return ociArchiveInspection{}, errors.New("OCI archive has an invalid index.json")
	}
	if len(index.Manifests) != 1 {
		return ociArchiveInspection{}, errors.New("OCI archive must contain exactly one top-level image")
	}
	top := index.Manifests[0]
	if requestedDigest != "" && top.Digest.String() != requestedDigest {
		return ociArchiveInspection{}, errors.New("OCI archive top-level digest does not match the requested digest")
	}
	// containerd export records the canonical source in its image-name
	// annotation and may put only a short tag (for example "latest") in the
	// OCI ref-name annotation. Match containerd import precedence so the
	// provenance comparison cannot reinterpret that tag as a Docker Hub name.
	reference := top.Annotations[images.AnnotationImageName]
	if reference == "" {
		reference = top.Annotations[ocispec.AnnotationRefName]
	}
	if reference != "" {
		reference, err = normalizeArchiveReference(reference, true)
		if err != nil {
			return ociArchiveInspection{}, err
		}
	}
	if requestedReference != "" {
		requestedReference, err = normalizeArchiveReference(requestedReference, false)
		if err != nil {
			return ociArchiveInspection{}, err
		}
		if reference != "" && reference != requestedReference {
			return ociArchiveInspection{}, errors.New("OCI archive image name does not match the requested reference")
		}
		reference = requestedReference
	}
	if reference == "" {
		return ociArchiveInspection{}, errors.New("OCI archive image reference is missing")
	}
	manifest, platform, err := selectArchiveManifest(top, blobs, matcher)
	if err != nil {
		return ociArchiveInspection{}, err
	}
	reachable, found, err := archiveReachableBlobs(top, manifest.Digest, blobs, 0)
	if err != nil {
		return ociArchiveInspection{}, err
	}
	if !found {
		return ociArchiveInspection{}, errors.New("OCI archive selected manifest is not reachable from its top-level descriptor")
	}
	if err := filterOCIArchiveForPlatform(file, archivePath, reachable); err != nil {
		return ociArchiveInspection{}, err
	}
	file = nil
	return ociArchiveInspection{Path: archivePath, Reference: reference, TopLevel: top, Platform: platform, PlatformDigest: manifest.Digest}, nil
}

// archiveReachableBlobs returns the descriptor graph required to import one
// selected platform while retaining the top-level identity. Multi-platform OCI
// archives can contain large foreign-platform layers; those bytes must not
// enter a node whose cache admission and pins account only for the selected
// platform.
func archiveReachableBlobs(descriptor ocispec.Descriptor, selected digest.Digest, blobs map[digest.Digest]archiveBlob, depth int) (map[digest.Digest]struct{}, bool, error) {
	if depth > 32 {
		return nil, false, errors.New("OCI archive image index nesting exceeds the helper bound")
	}
	if images.IsManifestType(descriptor.MediaType) {
		if descriptor.Digest != selected {
			return nil, false, nil
		}
		var manifest ocispec.Manifest
		if err := json.Unmarshal(blobs[descriptor.Digest].payload, &manifest); err != nil {
			return nil, false, errors.New("OCI archive image manifest is invalid")
		}
		reachable := map[digest.Digest]struct{}{descriptor.Digest: {}, manifest.Config.Digest: {}}
		for _, layer := range manifest.Layers {
			reachable[layer.Digest] = struct{}{}
		}
		return reachable, true, nil
	}
	if !images.IsIndexType(descriptor.MediaType) {
		return nil, false, nil
	}
	var index ocispec.Index
	if err := json.Unmarshal(blobs[descriptor.Digest].payload, &index); err != nil {
		return nil, false, errors.New("OCI archive image index is invalid")
	}
	for _, child := range index.Manifests {
		reachable, found, err := archiveReachableBlobs(child, selected, blobs, depth+1)
		if err != nil {
			return nil, false, err
		}
		if found {
			reachable[descriptor.Digest] = struct{}{}
			return reachable, true, nil
		}
	}
	return nil, false, nil
}

func filterOCIArchiveForPlatform(source *os.File, archivePath string, reachable map[digest.Digest]struct{}) (returnErr error) {
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return err
	}
	filtered, err := os.CreateTemp(filepath.Dir(archivePath), "wefty-image-filtered-*.tar")
	if err != nil {
		return err
	}
	filteredPath := filtered.Name()
	defer func() {
		if returnErr != nil {
			_ = os.Remove(filteredPath)
		}
	}()
	writer := tar.NewWriter(filtered)
	reader := tar.NewReader(source)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("filter OCI archive: %w", err)
		}
		name := path.Clean(strings.TrimPrefix(header.Name, "./"))
		keep := name == "oci-layout" || name == "index.json"
		if strings.HasPrefix(name, "blobs/sha256/") {
			_, keep = reachable[digest.Digest("sha256:"+path.Base(name))]
		}
		if !keep || header.FileInfo().IsDir() {
			continue
		}
		copyHeader := *header
		copyHeader.Name = name
		if err := writer.WriteHeader(&copyHeader); err != nil {
			return err
		}
		if _, err := io.CopyN(writer, reader, header.Size); err != nil {
			return err
		}
	}
	if err := writer.Close(); err != nil {
		return err
	}
	if err := filtered.Close(); err != nil {
		return err
	}
	if err := source.Close(); err != nil {
		return err
	}
	if err := os.Rename(filteredPath, archivePath); err != nil {
		return err
	}
	return nil
}

func readArchiveMetadata(reader io.Reader, size int64) ([]byte, error) {
	if size < 0 || size > maxArchiveMetadataBytes {
		return nil, errors.New("OCI archive metadata exceeds the helper bound")
	}
	return io.ReadAll(io.LimitReader(reader, size))
}

func selectArchiveManifest(top ocispec.Descriptor, blobs map[digest.Digest]archiveBlob, matcher platforms.MatchComparer) (ocispec.Descriptor, ocispec.Platform, error) {
	return selectArchivePlatform(top, blobs, matcher, 0)
}

func selectArchivePlatform(top ocispec.Descriptor, blobs map[digest.Digest]archiveBlob, matcher platforms.MatchComparer, depth int) (ocispec.Descriptor, ocispec.Platform, error) {
	if depth > 32 {
		return ocispec.Descriptor{}, ocispec.Platform{}, errors.New("OCI archive image index nesting exceeds the helper bound")
	}
	if err := verifyArchiveDescriptor(top, blobs); err != nil {
		return ocispec.Descriptor{}, ocispec.Platform{}, err
	}
	if images.IsManifestType(top.MediaType) {
		platform, err := archiveManifestPlatform(top, blobs, matcher)
		return top, platform, err
	}
	if !images.IsIndexType(top.MediaType) {
		return ocispec.Descriptor{}, ocispec.Platform{}, errors.New("OCI archive top-level descriptor is not an image manifest or index")
	}
	var index ocispec.Index
	if err := json.Unmarshal(blobs[top.Digest].payload, &index); err != nil || index.SchemaVersion != 2 {
		return ocispec.Descriptor{}, ocispec.Platform{}, errors.New("OCI archive image index is invalid")
	}
	for _, manifest := range index.Manifests {
		if manifest.Platform != nil && !matcher.Match(*manifest.Platform) {
			continue
		}
		selected, platform, err := selectArchivePlatform(manifest, blobs, matcher, depth+1)
		if err == nil {
			return selected, platform, nil
		}
		var mechanics *ImageMechanicsError
		if !errors.As(err, &mechanics) || mechanics.Fact.Kind != ImageFailurePlatformMismatch {
			return ocispec.Descriptor{}, ocispec.Platform{}, err
		}
	}
	return ocispec.Descriptor{}, ocispec.Platform{}, imageMechanicsError(ImageFailurePlatformMismatch, top.Digest.String(), errors.New("OCI archive has no manifest for the runtime platform"))
}

func normalizeArchiveReference(raw string, allowDigest bool) (string, error) {
	named, err := distributionref.ParseNormalizedNamed(raw)
	if err != nil {
		return "", errors.New("OCI archive image reference is invalid")
	}
	if _, digested := named.(distributionref.Digested); digested {
		if !allowDigest {
			return "", errors.New("OCI archive image reference must not contain a digest")
		}
		named = distributionref.TrimNamed(named)
	}
	return distributionref.TagNameOnly(named).String(), nil
}

func archiveManifestPlatform(descriptor ocispec.Descriptor, blobs map[digest.Digest]archiveBlob, matcher platforms.MatchComparer) (ocispec.Platform, error) {
	var manifest ocispec.Manifest
	if err := json.Unmarshal(blobs[descriptor.Digest].payload, &manifest); err != nil || manifest.SchemaVersion != 2 {
		return ocispec.Platform{}, errors.New("OCI archive image manifest is invalid")
	}
	if !images.IsConfigType(manifest.Config.MediaType) {
		return ocispec.Platform{}, errors.New("OCI archive image manifest has an invalid config media type")
	}
	if err := verifyArchiveDescriptor(manifest.Config, blobs); err != nil {
		return ocispec.Platform{}, err
	}
	for _, layer := range manifest.Layers {
		if !images.IsLayerType(layer.MediaType) {
			return ocispec.Platform{}, errors.New("OCI archive image manifest has an invalid layer media type")
		}
		if err := verifyArchiveDescriptor(layer, blobs); err != nil {
			return ocispec.Platform{}, err
		}
	}
	var image ocispec.Image
	if err := json.Unmarshal(blobs[manifest.Config.Digest].payload, &image); err != nil {
		return ocispec.Platform{}, errors.New("OCI archive image config is invalid")
	}
	platform := ocispec.Platform{OS: image.OS, Architecture: image.Architecture, Variant: image.Variant}
	if !matcher.Match(platform) {
		return ocispec.Platform{}, imageMechanicsError(ImageFailurePlatformMismatch, descriptor.Digest.String(), errors.New("OCI archive image config does not match the runtime platform"))
	}
	return platform, nil
}

func verifyArchiveDescriptor(descriptor ocispec.Descriptor, blobs map[digest.Digest]archiveBlob) error {
	if err := descriptor.Digest.Validate(); err != nil || descriptor.Size < 0 {
		return errors.New("OCI archive descriptor is invalid")
	}
	blob, ok := blobs[descriptor.Digest]
	if !ok || blob.size != descriptor.Size {
		return fmt.Errorf("OCI archive descriptor %s is missing or has the wrong size", descriptor.Digest)
	}
	if (images.IsManifestType(descriptor.MediaType) || images.IsIndexType(descriptor.MediaType) || descriptor.MediaType == ocispec.MediaTypeImageConfig) && blob.payload == nil {
		return fmt.Errorf("OCI archive metadata blob %s exceeds the helper bound", descriptor.Digest)
	}
	return nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func spoolOCIArchive(ctx context.Context, destination io.Writer, source io.Reader, limit int64) error {
	written, err := io.Copy(destination, io.LimitReader(&contextReader{ctx: ctx, reader: source}, limit+1))
	if err != nil {
		return err
	}
	if written > limit {
		return errOCIArchiveTooLarge
	}
	return nil
}

func (reader *contextReader) Read(buffer []byte) (int, error) {
	select {
	case <-reader.ctx.Done():
		return 0, reader.ctx.Err()
	default:
		return reader.reader.Read(buffer)
	}
}
