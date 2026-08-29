package ocihelper

import "github.com/containerd/errdefs"

func normalizeContainerdSignalError(err error) error {
	if errdefs.IsNotFound(err) {
		return ErrTaskAlreadyTerminated
	}
	return err
}
