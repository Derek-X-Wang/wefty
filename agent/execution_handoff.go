package agent

import (
	"fmt"
	"path/filepath"

	"github.com/Derek-X-Wang/wefty/contract"
)

// executionHandoff is resolved once from the immutable submission and L1's
// server-owned identity. All one-shot execution, locking, upload and retention
// use this answer; run entitlement continues to read labels alone. spec is the
// submitted spec, never a spec with synthetic owner labels or a rewritten path.
type executionHandoff struct {
	spec      contract.JobSpec
	ownerKey  string
	directory string
	err       error
}

func (m *handoffManager) resolveExecutionHandoff(spec contract.JobSpec, jobID string) executionHandoff {
	h := executionHandoff{spec: spec, ownerKey: contract.ExecutionHandoffOwnerKey(spec, jobID), directory: spec.Execution.HandoffDirectory}
	if spec.Kind == contract.JobKindOCI {
		// OCI volumes have no host path; the helper hashes the opaque owner key.
		return h
	}
	if h.ownerKey == "" && h.directory != "" {
		return h
	}
	if !validRunMailboxSegment(h.ownerKey) {
		h.err = fmt.Errorf("%w: handoff owner %q is not one safe path component", errUnmanagedHandoffDirectory, h.ownerKey)
		return h
	}
	if h.directory == "" {
		h.directory = filepath.Join(m.root, h.ownerKey)
		return h
	}
	h.directory, h.err = m.resolveHandoffDirectory(spec)
	return h
}

// legacyExecutionHandoff keeps the spec-only helpers used by older callers.
// A missing path gets no fallback without the server-owned identity.
func legacyExecutionHandoff(spec contract.JobSpec) executionHandoff {
	return executionHandoff{spec: spec, ownerKey: contract.HandoffOwnerKey(spec), directory: spec.Execution.HandoffDirectory}
}
