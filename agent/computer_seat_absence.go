package agent

import (
	"context"
	"errors"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

func (c *computerSeatController) reconcileRetired(ctx context.Context, list l1.ComputerSeatList) (bool, error) {
	if list.Recovery == nil || len(list.Recovery.RetiredAttempts) == 0 {
		return false, nil
	}
	for _, old := range list.Recovery.RetiredAttempts {
		if old.NodeID != c.authority.NodeID || old.AttemptID == c.authority.AttemptID || old.RemovalGeneration == "" {
			return false, errors.New("invalid retired Computer Seat Attempt identity")
		}
		authority := ocihelper.AttemptAuthority{NodeID: old.NodeID, BootSessionID: old.BootSessionID, JobID: old.JobID, AttemptID: old.AttemptID, FencingToken: old.FencingToken, Class: contract.JobClassService, RemovalGeneration: old.RemovalGeneration}
		helperCtx, cancel := context.WithTimeout(ctx, computerSeatHelperBudget)
		absent, err := c.runtime.VerifyComputerSeatAttemptAbsent(helperCtx, authority)
		cancel()
		if err != nil {
			return false, err
		}
		if !absent {
			return false, errors.New("earlier Computer Seat Attempt remains; room stays held")
		}
		report := l1.ComputerSeatAttemptAbsenceReport{ComputerSeatAttemptRequest: l1.ComputerSeatAttemptRequest{NodeID: c.authority.NodeID, BootSessionID: c.authority.BootSessionID, FencingToken: c.authority.FencingToken}, Retired: old, Absent: true}
		reportCtx, reportCancel := context.WithTimeout(ctx, computerSeatL1Budget)
		receipt, err := c.control.ReportComputerSeatAttemptAbsence(reportCtx, c.computerID, c.authority.JobID, c.authority.AttemptID, report)
		reportCancel()
		if err != nil {
			c.cycleL1Error = errors.Join(c.cycleL1Error, err)
			return false, err
		}
		if !receipt.Accepted {
			return false, errors.New("L1 did not acknowledge earlier Attempt absence")
		}
	}
	// Refresh before registering cleanup places. Runtime absence alone returns no room.
	return true, nil
}
