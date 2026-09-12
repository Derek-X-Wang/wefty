package ocihelper

import (
	"context"
	"errors"
	"testing"

	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/errdefs"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type fakeImageStore struct {
	named  map[string]images.Image
	getErr error
}

func newFakeImageStore() *fakeImageStore {
	return &fakeImageStore{named: map[string]images.Image{}}
}

func (store *fakeImageStore) Get(_ context.Context, name string) (images.Image, error) {
	if store.getErr != nil {
		return images.Image{}, store.getErr
	}
	image, ok := store.named[name]
	if !ok {
		return images.Image{}, errdefs.ErrNotFound
	}
	return image, nil
}

func (store *fakeImageStore) List(context.Context, ...string) ([]images.Image, error) {
	listed := make([]images.Image, 0, len(store.named))
	for _, image := range store.named {
		listed = append(listed, image)
	}
	return listed, nil
}

func (store *fakeImageStore) Create(_ context.Context, image images.Image) (images.Image, error) {
	if _, exists := store.named[image.Name]; exists {
		return images.Image{}, errdefs.ErrAlreadyExists
	}
	store.named[image.Name] = image
	return image, nil
}

func (store *fakeImageStore) Update(_ context.Context, image images.Image, _ ...string) (images.Image, error) {
	store.named[image.Name] = image
	return image, nil
}

func (store *fakeImageStore) Delete(_ context.Context, name string, _ ...images.DeleteOpt) error {
	delete(store.named, name)
	return nil
}

func testImageTarget(encoded string) ocispec.Descriptor {
	return ocispec.Descriptor{MediaType: ocispec.MediaTypeImageIndex, Digest: digest.Digest("sha256:" + encoded), Size: 1}
}

// The published image-user echo variants are two artifacts of one repository.
// The binding has to hold both names at once; refusing the second is the #418
// symptom an operator saw as manifest_rejected.
func TestBindImageNameHoldsColocatedNamesForDifferentDigests(t *testing.T) {
	store := newFakeImageStore()
	const repository = "ghcr.io/derek-x-wang/wefty-echo-service"
	numeric := testImageTarget("11111111111111111111111111111111111111111111111111111111111111aa")
	named := testImageTarget("22222222222222222222222222222222222222222222222222222222222222bb")
	if err := bindImageName(t.Context(), store, repository+":candidate-user-numeric", numeric); err != nil {
		t.Fatal(err)
	}
	if err := bindImageName(t.Context(), store, repository+":candidate-user-named", named); err != nil {
		t.Fatal(err)
	}
	if store.named[repository+":candidate-user-numeric"].Target.Digest != numeric.Digest ||
		store.named[repository+":candidate-user-named"].Target.Digest != named.Digest {
		t.Fatalf("colocated bindings = %+v", store.named)
	}
	// Replaying one import is idempotent; it is the same name over the same
	// bytes, not a second claim.
	if err := bindImageName(t.Context(), store, repository+":candidate-user-numeric", numeric); err != nil {
		t.Fatalf("replayed binding: %v", err)
	}
	if len(store.named) != 2 {
		t.Fatalf("binding count = %d", len(store.named))
	}
}

func TestBindImageNameRefusesANameThatIdentifiesDifferentBytes(t *testing.T) {
	store := newFakeImageStore()
	const reference = "ghcr.io/derek-x-wang/wefty-echo-service:candidate"
	first := testImageTarget("33333333333333333333333333333333333333333333333333333333333333cc")
	second := testImageTarget("44444444444444444444444444444444444444444444444444444444444444dd")
	if err := bindImageName(t.Context(), store, reference, first); err != nil {
		t.Fatal(err)
	}
	err := bindImageName(t.Context(), store, reference, second)
	var mechanics *ImageMechanicsError
	if !errors.As(err, &mechanics) || mechanics.Fact.Kind != ImageFailureManifestRejected ||
		mechanics.Fact.TopLevelDigest != second.Digest.String() {
		t.Fatalf("name collision error = %v", err)
	}
	if store.named[reference].Target.Digest != first.Digest {
		t.Fatalf("refused binding rebound the name to %s", store.named[reference].Target.Digest)
	}
}

// A store failure that is not "absent" is the store's, not the archive's: it
// must reach the caller for classification rather than be reported as a name
// collision the operator could act on.
func TestBindImageNameReportsStoreFailuresRaw(t *testing.T) {
	store := newFakeImageStore()
	store.getErr = errors.New("image store is unavailable")
	err := bindImageName(t.Context(), store, "ghcr.io/derek-x-wang/wefty-echo-service:candidate", testImageTarget("55555555555555555555555555555555555555555555555555555555555555ee"))
	var mechanics *ImageMechanicsError
	if err == nil || errors.As(err, &mechanics) {
		t.Fatalf("store failure error = %v", err)
	}
}
