package agent

import "context"

type managedResourceAttempt struct {
	dataDirectory    string
	runtimeDirectory string
}

// managedResourceManager keeps the agent contract resource-neutral. The
// process-mode implementation uses a managed directory tree; a future OCI
// implementation may instead prepare a container and its volumes.
type managedResourceManager interface {
	rootInstanceID() string
	prepareAttempt(jobID, attemptID string) (managedResourceAttempt, func(), error)
	remove(context.Context, localRemoval) error
	resumeRemovals(context.Context) ([]localRemoval, error)
}

type localRemoval struct {
	jobID             string
	kind              string
	generation        uint64
	rootInstanceID    string
	cleanupFence      string
	processTreeReaped bool
}
