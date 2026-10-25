package oci

import (
	"context"
	"errors"
	"net"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

func (a *Adapter) DialSeatEndpoint(ctx context.Context, r ocihelper.DialSeatEndpointRequest) (net.Conn, error) {
	if a == nil || a.sessions == nil {
		return nil, errors.New("OCI helper session is not configured")
	}
	s, err := a.sessions.Session()
	if err != nil {
		return nil, err
	}
	return s.DialSeatEndpoint(ctx, r)
}
func (a *Adapter) SetSeatDriver(ctx context.Context, r ocihelper.SetSeatDriverRequest) error {
	if a == nil || a.sessions == nil {
		return errors.New("OCI helper session is not configured")
	}
	s, err := a.sessions.Session()
	if err != nil {
		return err
	}
	return s.SetSeatDriver(ctx, r)
}
