package oci

import (
	"context"
	"errors"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

func (a *Adapter) SetComputerMemoryLimit(ctx context.Context, request ocihelper.SetComputerMemoryLimitRequest) (ocihelper.SetComputerMemoryLimitResponse, error) {
	if a == nil || a.sessions == nil {
		return ocihelper.SetComputerMemoryLimitResponse{}, errors.New("OCI helper session is not configured")
	}
	// A failed or lost resize can still have changed the cgroup. Bind the
	// stronger absence proof before calling the helper, under exact authority.
	a.mu.Lock()
	for authority, entry := range a.runEntered {
		if HelperAuthority(authority) == request.Authority {
			entry.seatMemory = true
			a.runEntered[authority] = entry
			break
		}
	}
	a.mu.Unlock()
	session, err := a.sessions.Session()
	if err != nil {
		return ocihelper.SetComputerMemoryLimitResponse{}, err
	}
	return session.SetComputerMemoryLimit(ctx, request)
}
