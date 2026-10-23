package oci

import (
	"context"
	"errors"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

func (a *Adapter) ApplyComputerSeats(ctx context.Context, request ocihelper.ApplyComputerSeatsRequest) (ocihelper.ApplyComputerSeatsResponse, error) {
	if a == nil || a.sessions == nil {
		return ocihelper.ApplyComputerSeatsResponse{}, errors.New("OCI helper session is not configured")
	}
	session, err := a.sessions.Session()
	if err != nil {
		return ocihelper.ApplyComputerSeatsResponse{}, err
	}
	return session.ApplyComputerSeats(ctx, request)
}
func (a *Adapter) ObserveComputerSeats(ctx context.Context, request ocihelper.ObserveComputerSeatsRequest) (ocihelper.ObserveComputerSeatsResponse, error) {
	if a == nil || a.sessions == nil {
		return ocihelper.ObserveComputerSeatsResponse{}, errors.New("OCI helper session is not configured")
	}
	session, err := a.sessions.Session()
	if err != nil {
		return ocihelper.ObserveComputerSeatsResponse{}, err
	}
	return session.ObserveComputerSeats(ctx, request)
}
