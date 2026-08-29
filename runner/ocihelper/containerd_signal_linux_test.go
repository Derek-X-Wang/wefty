package ocihelper

import (
	"errors"
	"fmt"
	"testing"

	"github.com/containerd/errdefs"
)

func TestContainerdSignalMapsExitedTaskToAlreadyTerminated(t *testing.T) {
	err := normalizeContainerdSignalError(fmt.Errorf("kill task: %w", errdefs.ErrNotFound))
	if !errors.Is(err, ErrTaskAlreadyTerminated) {
		t.Fatalf("exited-task Signal = %v, want ErrTaskAlreadyTerminated", err)
	}
}

func TestContainerdSignalPreservesUnknownMechanicsFailure(t *testing.T) {
	want := errors.New("permission boundary failed")
	if got := normalizeContainerdSignalError(want); !errors.Is(got, want) {
		t.Fatalf("unknown Signal failure = %v, want %v", got, want)
	}
}
