package oci

import (
	"context"
	"errors"
	"io"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

func (a *Adapter) CopyComputerSeat(ctx context.Context, r ocihelper.CopyComputerSeatRequest, input io.Reader, emit func([]byte) error) (int64, error) {
	if a == nil || a.sessions == nil {
		return 0, errors.New("OCI helper session is not configured")
	}
	session, err := a.sessions.Session()
	if err != nil {
		return 0, err
	}
	return session.CopyComputerSeat(ctx, r, input, emit)
}
