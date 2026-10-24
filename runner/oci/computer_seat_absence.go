package oci

import (
	"context"
	"errors"
	"time"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

func computerSeatAttemptAbsent(v ocihelper.VerifyResponse, a ocihelper.AttemptAuthority, now time.Time) error {
	identity, err := ocihelper.DeterministicResourceIdentity(a)
	if err != nil {
		return err
	}
	// A retained cgroup may still consume memory. General namespace readiness
	// permits bounded retention, but returning Seat room requires actual absence.
	if len(ocihelper.ProjectAttemptInventory(v.Inventory, identity, "").Cgroups) != 0 {
		return errors.New("earlier Computer cgroup remains; room stays held")
	}
	return attemptAbsentFromNamespace(v, a, nil, now)
}
func (a *Adapter) VerifyComputerSeatAttemptAbsent(ctx context.Context, authority ocihelper.AttemptAuthority) (bool, error) {
	if a == nil || a.sessions == nil {
		return false, errors.New("OCI helper session is not configured")
	}
	session, err := a.sessions.Session()
	if err != nil {
		return false, err
	}
	// A restarted helper no longer authorizes the earlier Attempt's Verify.
	// Read the namespace and narrow it to the exact durable authority instead.
	receipt, err := session.Verify(ctx, ocihelper.VerifyRequest{Scope: ocihelper.VerifyNamespaceReadOnly})
	if err != nil {
		return false, err
	}
	if err := computerSeatAttemptAbsent(receipt, authority, time.Now().UTC()); err != nil {
		return false, err
	}
	return true, nil
}
