package oci

import (
	"context"
	"errors"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// ExecComputerSeat uses the existing helper stream; cancellation closes only
// this command's connection and keeps the Computer Attempt intact.
func (a *Adapter) ExecComputerSeat(ctx context.Context, r ocihelper.ExecComputerSeatRequest, emit func(ocihelper.SeatCommandEvent) error) error {
	if a == nil || a.sessions == nil {
		return errors.New("OCI helper session is not configured")
	}
	session, err := a.sessions.Session()
	if err != nil {
		return err
	}
	return session.ExecComputerSeat(ctx, r, emit)
}
