package agent

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// One second leaves ample margin within each Seat's own 60-second deadline,
// including a bounded helper cycle (20s) and two bounded L1 operations (2s each).
const computerSeatPollInterval = time.Second
const computerSeatHelperBudget = 20 * time.Second
const computerSeatL1Budget = 2 * time.Second

type computerSeatRuntime interface {
	ApplyComputerSeats(context.Context, ocihelper.ApplyComputerSeatsRequest) (ocihelper.ApplyComputerSeatsResponse, error)
	ObserveComputerSeats(context.Context, ocihelper.ObserveComputerSeatsRequest) (ocihelper.ObserveComputerSeatsResponse, error)
}
type computerSeatControl interface {
	GetComputerSeatList(context.Context, string, string, string, l1.ComputerSeatAttemptRequest) (l1.ComputerSeatList, error)
	ReportComputerSeats(context.Context, string, string, string, l1.ComputerSeatObservationReport) (l1.ComputerSeatObservationReceipt, error)
}
type seatApply struct {
	revision int64
	seats    []ocihelper.ComputerSeat
	bindings map[int]l1.WantedComputerSeat
}

// The controller lives only as long as this exact attempt. In particular it
// never reconstructs identity from a place after a node agent restart.
type computerSeatController struct {
	control       computerSeatControl
	runtime       computerSeatRuntime
	authority     ocihelper.AttemptAuthority
	computerID    string
	clock         Clock
	logf          func(string, ...any)
	bindings      map[int]l1.WantedComputerSeat
	pendingApply  *seatApply
	pendingReport *l1.ComputerSeatObservationReport
	generation    uint64
	revision      int64
	sequence      int64
	afterStep     func()
}

func newComputerSeatController(control computerSeatControl, runtime computerSeatRuntime, authority ocihelper.AttemptAuthority, computerID string, clock Clock, logf func(string, ...any)) *computerSeatController {
	return &computerSeatController{control: control, runtime: runtime, authority: authority, computerID: computerID, clock: clock, logf: logf, bindings: map[int]l1.WantedComputerSeat{}}
}
func (c *computerSeatController) run(ctx context.Context) {
	timer := c.clock.NewTimer(computerSeatPollInterval)
	defer stopTimer(timer)
	for {
		if ctx.Err() != nil {
			return
		}
		err := c.step(ctx)
		stopTimer(timer)
		timer.Reset(computerSeatPollInterval)
		if err != nil && ctx.Err() == nil && c.logf != nil {
			c.logf("node agent: Computer Seat observation attempt=%s: %v", c.authority.AttemptID, err)
		}
		if c.afterStep != nil {
			c.afterStep()
		}
		select {
		case <-ctx.Done():
			return
		case <-timer.C():
		}
	}
}
func (c *computerSeatController) plan(list l1.ComputerSeatList) *seatApply {
	plan := &seatApply{revision: list.Revision, bindings: map[int]l1.WantedComputerSeat{}, seats: []ocihelper.ComputerSeat{}}
	for place, seat := range c.bindings {
		plan.bindings[place] = seat
	}
	for _, seat := range list.Seats {
		if old, known := c.bindings[seat.Place]; known && old.SeatID != seat.SeatID {
			continue
		} // Omit until cleanup is acknowledged.
		plan.bindings[seat.Place] = seat
		plan.seats = append(plan.seats, ocihelper.ComputerSeat{Place: seat.Place, User: fmt.Sprintf("seat%d", seat.Place), ViewPort: uint16(7100 + 2*seat.Place), ControlPort: uint16(7101 + 2*seat.Place)})
	}
	slices.SortFunc(plan.seats, func(a, b ocihelper.ComputerSeat) int { return a.Place - b.Place })
	return plan
}
func (c *computerSeatController) step(ctx context.Context) error {
	auth := l1.ComputerSeatAttemptRequest{NodeID: c.authority.NodeID, BootSessionID: c.authority.BootSessionID, FencingToken: c.authority.FencingToken}
	readCtx, cancel := context.WithTimeout(ctx, computerSeatL1Budget)
	list, readErr := c.control.GetComputerSeatList(readCtx, c.computerID, c.authority.JobID, c.authority.AttemptID, auth)
	cancel()
	if readErr == nil && c.pendingApply == nil {
		c.pendingApply = c.plan(list)
	}
	helperCtx, helperCancel := context.WithTimeout(ctx, computerSeatHelperBudget)
	defer helperCancel()
	var applyErr error
	if pending := c.pendingApply; pending != nil {
		response, err := c.runtime.ApplyComputerSeats(helperCtx, ocihelper.ApplyComputerSeatsRequest{Authority: c.authority, Seats: pending.seats})
		var rpc *ocihelper.RPCError
		if errors.As(err, &rpc) && rpc.Code == ocihelper.CodeSeatOperationRefused {
			// A refusal has not published. Refresh the wanted list on the next pass.
			c.pendingApply = nil
		} else if err != nil {
			applyErr = err // It may have published: retry this exact list, even if L1 changes.
		} else if response.Generation == 0 {
			applyErr = errors.New("helper returned a zero Seat generation")
		} else {
			c.generation, c.revision, c.bindings = response.Generation, pending.revision, pending.bindings
			c.pendingApply = nil
		}
	}
	var observeErr error
	if c.generation != 0 {
		response, err := c.runtime.ObserveComputerSeats(helperCtx, ocihelper.ObserveComputerSeatsRequest{Authority: c.authority, Generation: c.generation})
		var rpc *ocihelper.RPCError
		if errors.As(err, &rpc) && rpc.Code == ocihelper.CodeSeatOperationRefused { // Wait for the next Apply/Observe pass.
		} else if err != nil {
			observeErr = err
		} else if response.Generation != c.generation {
			observeErr = errors.New("helper Seat observation generation mismatch")
		} else if c.pendingReport == nil && readErr == nil && list.Revision == c.revision {
			seats := []l1.ComputerSeatObservation{}
			for _, observation := range response.Seats {
				binding, known := c.bindings[observation.Place]
				if !known {
					if observation.State == "cleaned" {
						continue
					}
					observeErr = errors.New("helper reported an unbound Seat place")
					break
				}
				seats = append(seats, l1.ComputerSeatObservation{SeatID: binding.SeatID, Place: binding.Place, State: l1.ComputerSeatObservedState(observation.State), Reason: observation.Reason, CleanupComplete: observation.CleanupComplete})
			}
			if observeErr == nil && len(seats) > 0 {
				c.sequence++
				c.pendingReport = &l1.ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: c.revision, Generation: c.generation, Sequence: c.sequence, Seats: seats}
			}
		}
	}
	if c.pendingReport != nil && readErr == nil && c.pendingReport.ListRevision != list.Revision {
		c.pendingReport = nil
	}
	reportErr := c.flush(ctx)
	return errors.Join(readErr, applyErr, observeErr, reportErr)
}
func (c *computerSeatController) flush(ctx context.Context) error {
	report := c.pendingReport
	if report == nil {
		return nil
	}
	reportCtx, cancel := context.WithTimeout(ctx, computerSeatL1Budget)
	defer cancel()
	receipt, err := c.control.ReportComputerSeats(reportCtx, c.computerID, c.authority.JobID, c.authority.AttemptID, *report)
	if err != nil {
		var protocol *ProtocolError
		if errors.As(err, &protocol) && protocol.APIError.Code == contract.ErrorConflict && protocol.APIError.Details["reason"] == "seat_list_revision_changed" {
			c.pendingReport = nil
		}
		return err
	}
	if !receipt.Accepted {
		return errors.New("L1 did not accept Seat observations")
	}
	for _, seat := range report.Seats {
		if seat.State == l1.ComputerSeatCleaned && seat.CleanupComplete {
			if binding, ok := c.bindings[seat.Place]; ok && binding.SeatID == seat.SeatID {
				delete(c.bindings, seat.Place)
			}
		}
	}
	c.pendingReport = nil
	return nil
}
