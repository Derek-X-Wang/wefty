//go:build linux

package ocihelper

import "context"

func (engine *ContainerdEngine) SetSeatDriver(ctx context.Context, request SetSeatDriverRequest) error {
	attempt, err := engine.attempt(request.Authority)
	if err != nil {
		return err
	}
	attempt.seatsMu.Lock()
	defer attempt.seatsMu.Unlock()
	snapshot, err := snapshotComputerSeatAttempt(ctx, attempt)
	if err != nil {
		return err
	}
	defer snapshot.namespace.close()
	if err := engine.validateComputerSeatAttempt(ctx, request.Authority, attempt, snapshot); err != nil {
		return err
	}
	return snapshot.seats.setDriver(request.Generation, request.Place, request.HumanDriving)
}
