package agent

import (
	"context"
	"errors"
	"fmt"
	"net/http"
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
const computerSeatL1BackoffCap = 30 * time.Second
const computerSeatLogInterval = 30 * time.Second

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
	control        computerSeatControl
	runtime        computerSeatRuntime
	authority      ocihelper.AttemptAuthority
	computerID     string
	clock          Clock
	logf           func(string, ...any)
	bindings       map[int]l1.WantedComputerSeat
	cleaned        map[string]bool
	pendingApply   *seatApply
	pendingReport  *l1.ComputerSeatObservationReport
	acceptedReport *l1.ComputerSeatObservationReport
	lastApplied    []ocihelper.ComputerSeat
	wanted         *l1.ComputerSeatList
	nextL1         time.Time
	l1Backoff      time.Duration
	l1Conflicts    int
	nextLog        time.Time
	generation     uint64
	revision       int64
	sequence       int64
	afterStep      func()
}

func newComputerSeatController(control computerSeatControl, runtime computerSeatRuntime, authority ocihelper.AttemptAuthority, computerID string, clock Clock, logf func(string, ...any)) *computerSeatController {
	return &computerSeatController{control: control, runtime: runtime, authority: authority, computerID: computerID, clock: clock, logf: logf, bindings: map[int]l1.WantedComputerSeat{}, cleaned: map[string]bool{}}
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
		if err != nil && ctx.Err() == nil && c.logf != nil && !c.clock.Now().Before(c.nextLog) {
			c.nextLog = c.clock.Now().Add(computerSeatLogInterval)
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
		if c.cleaned[seat.SeatID] {
			continue
		}
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
	allowed := !c.clock.Now().Before(c.nextL1)
	var list l1.ComputerSeatList
	var readErr error
	fresh := false
	if allowed {
		readCtx, cancel := context.WithTimeout(ctx, computerSeatL1Budget)
		list, readErr = c.control.GetComputerSeatList(readCtx, c.computerID, c.authority.JobID, c.authority.AttemptID, auth)
		cancel()
		if readErr == nil {
			copy := list
			copy.Seats = slices.Clone(list.Seats)
			c.wanted = &copy
			fresh = true
		}
	}
	if c.wanted != nil {
		list = *c.wanted
	}
	if fresh && c.pendingApply == nil {
		plan := c.plan(list)
		if c.generation != 0 && slices.Equal(plan.seats, c.lastApplied) {
			c.revision, c.bindings = plan.revision, plan.bindings
		} else {
			c.pendingApply = plan
		}
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
			// Cleanup acknowledged while this Apply was uncertain is final. Its old
			// binding must not return with the Apply's earlier planning snapshot.
			for place, seat := range pending.bindings {
				if c.cleaned[seat.SeatID] {
					delete(pending.bindings, place)
				}
			}
			c.generation, c.revision, c.bindings = response.Generation, pending.revision, pending.bindings
			c.lastApplied = slices.Clone(pending.seats)
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
		} else if c.pendingReport == nil && c.wanted != nil && list.Revision == c.revision {
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
			if observeErr == nil && len(seats) > 0 && c.observationsChanged(seats) {
				c.sequence++
				c.pendingReport = &l1.ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: c.revision, Generation: c.generation, Sequence: c.sequence, Seats: seats}
			}
		}
	}
	if c.pendingReport != nil && fresh && c.pendingReport.ListRevision != list.Revision {
		c.pendingReport = nil
	}
	var reportErr error
	if allowed && readErr == nil {
		reportErr = c.flush(ctx)
	}
	if allowed {
		l1Err := errors.Join(readErr, reportErr)
		var protocol *ProtocolError
		conflict := errors.As(l1Err, &protocol) && protocol.StatusCode == http.StatusConflict && !protocol.APIError.Retryable
		if conflict {
			c.l1Conflicts++
		} else {
			c.l1Conflicts = 0
		}
		// One conflict may be an ordinary revision race. A lasting 409 must
		// have a floor too; helper cleanup continues during the L1 backoff.
		if l1Err != nil && (computerSeatRetryableL1Error(l1Err) || c.l1Conflicts >= 2) {
			if c.l1Backoff == 0 {
				c.l1Backoff = 2 * time.Second
			} else {
				c.l1Backoff = min(2*c.l1Backoff, computerSeatL1BackoffCap)
			}
			c.nextL1 = c.clock.Now().Add(c.l1Backoff)
		} else {
			c.l1Backoff = 0
			c.nextL1 = time.Time{}
		}
	}
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
		if errors.As(err, &protocol) && ((protocol.APIError.Code == contract.ErrorConflict && protocol.APIError.Details["reason"] == "seat_list_revision_changed") || (protocol.StatusCode == http.StatusConflict && !protocol.APIError.Retryable)) {
			// L1 rejected this sequence. The replacement must be a fresh snapshot,
			// even if it happens to match an older acknowledged state.
			c.acceptedReport = nil
			c.pendingReport = nil
		}
		return err
	}
	if !receipt.Accepted {
		return errors.New("L1 did not accept Seat observations")
	}
	c.acceptedReport = report
	for _, seat := range report.Seats {
		if seat.State == l1.ComputerSeatCleaned && seat.CleanupComplete {
			c.cleaned[seat.SeatID] = true
			if binding, ok := c.bindings[seat.Place]; ok && binding.SeatID == seat.SeatID {
				delete(c.bindings, seat.Place)
			}
		}
	}
	c.pendingReport = nil
	return nil
}

// An acknowledged cleanup can disappear from subsequent helper batches without
// generating a second write. No heartbeat is needed: L1 retains observations,
// and the existing Attempt lease owns node liveness.
func (c *computerSeatController) observationsChanged(seats []l1.ComputerSeatObservation) bool {
	previous := c.acceptedReport
	if previous == nil || previous.Generation != c.generation || previous.ListRevision != c.revision {
		return true
	}
	for _, seat := range seats {
		found := false
		for _, old := range previous.Seats {
			if old.SeatID == seat.SeatID {
				found = old == seat
				break
			}
		}
		if !found {
			return true
		}
	}
	return false
}

func computerSeatRetryableL1Error(err error) bool {
	var protocol *ProtocolError
	if errors.As(err, &protocol) {
		return protocol.APIError.Retryable || protocol.StatusCode >= 500 || protocol.StatusCode == http.StatusRequestTimeout || protocol.StatusCode == http.StatusTooManyRequests
	}
	return true
}
