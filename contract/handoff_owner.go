package contract

import (
	"errors"
	"strings"
)

const (
	// LabelRunID names the L3 run a dispatched job executes. L3 sets it on
	// every job it dispatches.
	LabelRunID = "run_id"

	// LabelHandoffOwnerRunID names the run whose handoff a rerun reuses. L3
	// sets it only when that run is not the job's own.
	LabelHandoffOwnerRunID = "handoff_owner_run_id"

	// MaxHandoffOwnerKeyBytes is the helper's bound on the key it derives a
	// handoff volume's name from.
	MaxHandoffOwnerKeyBytes = 255
)

// HandoffOwnerKey is the stable identity a job's retained handoff is keyed by:
// the run whose handoff it reuses, or else its own run. It is read from the
// job's labels alone, so L1 at submission and the node agent at execution
// derive the same answer from the same immutable spec.
func HandoffOwnerKey(spec JobSpec) string {
	if owner := strings.TrimSpace(spec.Labels[LabelHandoffOwnerRunID]); owner != "" {
		return owner
	}
	return strings.TrimSpace(spec.Labels[LabelRunID])
}

// RequiresHandoffOwner reports whether a job cannot execute without a handoff
// owner key. Only an OCI one-shot needs one: its handoff is a helper-owned
// volume named from the key, and the helper refuses to create one without it.
// A process one-shot keeps the directory it was dispatched with and simply
// retains nothing when it names no run; a service's data is keyed by its own
// job ID, and a Computer is a service.
func RequiresHandoffOwner(spec JobSpec) bool {
	return spec.Kind == JobKindOCI && spec.Class == JobClassOneShot
}

// ValidateHandoffOwner refuses a job that needs a handoff owner key and can
// never produce one the helper accepts. The key is derived from labels that
// never change after submission, so a job refused here would be refused on
// every attempt; the error names what the submitter has to supply.
func ValidateHandoffOwner(spec JobSpec) error {
	if !RequiresHandoffOwner(spec) {
		return nil
	}
	owner := HandoffOwnerKey(spec)
	switch {
	case owner == "":
		return errors.New("a kind=oci one-shot job needs a run identity to own its handoff volume: " +
			"submit it as an L3 run, or label it with run_id")
	case len(owner) > MaxHandoffOwnerKeyBytes:
		return errors.New("a kind=oci one-shot job's handoff owner key (handoff_owner_run_id, else run_id) exceeds 255 bytes")
	case strings.IndexByte(owner, 0) >= 0:
		return errors.New("a kind=oci one-shot job's handoff owner key (handoff_owner_run_id, else run_id) contains a NUL byte")
	}
	return nil
}
