package serviceacceptance

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

const (
	acceptanceShimReapWindow = 2 * time.Second
	acceptanceShimReapPoll   = 100 * time.Millisecond
)

// onlyExitingAcceptanceShims recognizes the post-delete shape: the namespace
// has no tasks, containers, or any other runtime resource, but containerd's
// asynchronous shim teardown has not removed its state directories yet. This
// is permission to wait for fresh positive verification, never absence proof.
func onlyExitingAcceptanceShims(err error) bool {
	var residue *ocihelper.NamespaceResidueError
	if !errors.As(err, &residue) || len(residue.RuntimeResidue.Shims) == 0 {
		return false
	}
	for _, id := range residue.RuntimeResidue.Shims {
		if !strings.HasPrefix(id, "wefty-container-") || id == "wefty-container-" {
			return false
		}
	}
	remaining := residue.RuntimeResidue
	remaining.Shims = nil
	return ocihelper.InventoryEmpty(remaining)
}

// ensureAcceptanceNamespace keeps the production boot sweep and verification
// intact. Only the shared test provisioner retries its refused start. The
// window starts at the first shim-only refusal, and bounds waits AND subsequent
// Ensure calls; a wedged shim still fails with its last inventory attached.
func ensureAcceptanceNamespace(ctx context.Context, ensure func(context.Context) error, wait func(context.Context, time.Duration) error) error {
	err := ensure(ctx)
	if err == nil || !onlyExitingAcceptanceShims(err) {
		return err
	}
	retryContext, cancel := context.WithTimeout(ctx, acceptanceShimReapWindow)
	defer cancel()
	for {
		if waitErr := wait(retryContext, acceptanceShimReapPoll); waitErr != nil {
			return fmt.Errorf("acceptance shim reaping did not complete within %s: %w", acceptanceShimReapWindow, errors.Join(err, waitErr))
		}
		if contextErr := retryContext.Err(); contextErr != nil {
			return errors.Join(err, contextErr)
		}
		nextErr := ensure(retryContext)
		if contextErr := retryContext.Err(); contextErr != nil {
			return errors.Join(err, nextErr, contextErr)
		}
		err = nextErr
		if err == nil || !onlyExitingAcceptanceShims(err) {
			return err
		}
	}
}

func waitAcceptanceShimRetry(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
