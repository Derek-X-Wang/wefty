package ocihelper

import (
	"context"
	"errors"

	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/errdefs"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// bindImageName gives imported content the operator-facing name the archive
// resolved to. Names are per-artifact, not per-repository: two archives
// exported from one repository bind two names side by side, which is what
// makes the published image-user echo variants importable offline (#418).
//
// A name that already identifies different bytes is refused as
// ImageFailureManifestRejected rather than rebound, because rebinding would
// silently move every later digest pin and service binding that resolves
// through that name onto content the operator did not import. Rebinding the
// same digest is idempotent, so replaying an import is not an error.
//
// Store failures come back raw for the caller to classify; only the collision
// carries typed image mechanics of its own.
func bindImageName(ctx context.Context, store images.Store, reference string, target ocispec.Descriptor) error {
	existing, err := store.Get(ctx, reference)
	if err == nil {
		if existing.Target.Digest != target.Digest {
			return imageNameCollision(target)
		}
		return nil
	}
	if !errdefs.IsNotFound(err) {
		return err
	}
	if _, err := store.Create(ctx, images.Image{Name: reference, Target: target}); err != nil {
		if !errdefs.IsAlreadyExists(err) {
			return err
		}
		// A concurrent import claimed the name between the read and the
		// create. Read it back rather than assume which one it named.
		existing, err = store.Get(ctx, reference)
		if err != nil || existing.Target.Digest != target.Digest {
			return imageNameCollision(target)
		}
	}
	return nil
}

func imageNameCollision(target ocispec.Descriptor) error {
	return imageMechanicsError(ImageFailureManifestRejected, target.Digest.String(), errors.New("OCI image name already identifies different bytes"))
}
