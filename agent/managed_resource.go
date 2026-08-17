package agent

type managedResourceAttempt struct {
	dataDirectory    string
	runtimeDirectory string
}

// managedResourceManager keeps the agent contract resource-neutral. The
// process-mode implementation uses a managed directory tree; a future OCI
// implementation may instead prepare a container and its volumes.
type managedResourceManager interface {
	prepareAttempt(jobID, attemptID string) (managedResourceAttempt, func(), error)
}
