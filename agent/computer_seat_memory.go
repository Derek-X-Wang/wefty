package agent

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

func cloneSeatRecovery(value *l1.ComputerSeatRecovery) *l1.ComputerSeatRecovery {
	if value == nil {
		return nil
	}
	copy := *value
	copy.Bindings = slices.Clone(value.Bindings)
	copy.RetiredAttempts = slices.Clone(value.RetiredAttempts)
	if value.Room != nil {
		room := *value.Room
		copy.Room = &room
	}
	return &copy
}
func (c *computerSeatController) restoreBindings(list l1.ComputerSeatList) {
	if list.Recovery == nil {
		return
	}
	c.sequence = max(c.sequence, list.Recovery.Sequence)
	for _, binding := range list.Recovery.Bindings {
		if c.cleaned[binding.SeatID] {
			continue
		}
		if old, known := c.bindings[binding.Place]; !known || old.SeatID == binding.SeatID {
			c.bindings[binding.Place] = binding
		}
	}
}
func seatRuntimeConfig(binding l1.WantedComputerSeat) ocihelper.ComputerSeat {
	return ocihelper.ComputerSeat{Place: binding.Place, User: fmt.Sprintf("seat%d", binding.Place), ViewPort: uint16(7100 + 2*binding.Place), ControlPort: uint16(7101 + 2*binding.Place)}
}
func (c *computerSeatController) cleanupPlaces(list l1.ComputerSeatList) []ocihelper.ComputerSeat {
	if list.Recovery == nil {
		return nil
	}
	wanted := map[string]bool{}
	for _, b := range list.Seats {
		wanted[b.SeatID] = true
	}
	cleanup := []ocihelper.ComputerSeat{}
	for _, binding := range c.bindings {
		if !wanted[binding.SeatID] && !c.cleaned[binding.SeatID] {
			cleanup = append(cleanup, seatRuntimeConfig(binding))
		}
	}
	slices.SortFunc(cleanup, func(a, b ocihelper.ComputerSeat) int { return a.Place - b.Place })
	return cleanup
}
func (c *computerSeatController) reconcileMemory(ctx context.Context, list l1.ComputerSeatList) error {
	if list.Recovery == nil || list.Recovery.Room == nil {
		return nil
	}
	room := *list.Recovery.Room
	if c.memoryRevision == room.RoomRevision && c.memoryGeneration == c.generation {
		return nil
	}
	helperCtx, cancel := context.WithTimeout(ctx, computerSeatHelperBudget)
	defer cancel()
	response, err := c.runtime.SetComputerMemoryLimit(helperCtx, ocihelper.SetComputerMemoryLimitRequest{Authority: c.authority, Generation: c.generation, MemoryBytes: room.TargetMemoryBytes})
	if err != nil {
		return err
	}
	if response.RequestedMemoryBytes != room.TargetMemoryBytes || response.MemoryLimitBytes < room.TargetMemoryBytes || response.ReservedMemoryBytes < response.MemoryLimitBytes {
		return errors.New("helper returned inconsistent Computer memory evidence")
	}
	c.memoryReadbackRevision, c.memoryReadbackGeneration, c.memoryReadbackLimit = room.RoomRevision, c.generation, response.MemoryLimitBytes
	// HeldMemoryBytes in the helper response is only excess above the target.
	// L1 requires the entire effective charge, including an uncertain reservation.
	held := max(response.ReservedMemoryBytes, response.MemoryLimitBytes)
	report := l1.SeatRoomMemoryReport{ComputerSeatAttemptRequest: l1.ComputerSeatAttemptRequest{NodeID: c.authority.NodeID, BootSessionID: c.authority.BootSessionID, FencingToken: c.authority.FencingToken}, RoomRevision: room.RoomRevision, RequestedMemoryBytes: room.TargetMemoryBytes, MemoryLimitBytes: response.MemoryLimitBytes, HeldMemoryBytes: held}
	reportCtx, reportCancel := context.WithTimeout(ctx, computerSeatL1Budget)
	defer reportCancel()
	receipt, err := c.control.ReportComputerRoomMemory(reportCtx, c.computerID, c.authority.JobID, c.authority.AttemptID, report)
	if err != nil {
		c.cycleL1Error = errors.Join(c.cycleL1Error, err)
		return err
	}
	if receipt.RoomRevision != room.RoomRevision || receipt.HeldMemoryBytes != held {
		return errors.New("L1 did not acknowledge Computer memory evidence")
	}
	if response.MemoryLimitBytes == room.TargetMemoryBytes && held == room.TargetMemoryBytes {
		c.memoryRevision = room.RoomRevision
		c.memoryGeneration = c.generation
	}
	return nil
}

func (c *computerSeatController) memoryConfirmed(list l1.ComputerSeatList) bool {
	if list.Recovery == nil || list.Recovery.Room == nil {
		return true
	}
	room := list.Recovery.Room
	return c.memoryReadbackRevision == room.RoomRevision && c.memoryReadbackGeneration == c.generation && c.memoryReadbackLimit >= room.TargetMemoryBytes
}

func (c *computerSeatController) restoreApplied(response ocihelper.ObserveComputerSeatsResponse) {
	c.lastApplied = []ocihelper.ComputerSeat{}
	c.appliedBindings = map[int]string{}
	for _, observed := range response.Seats {
		if binding, known := c.bindings[observed.Place]; known && observed.State == "starting" {
			c.lastApplied = append(c.lastApplied, seatRuntimeConfig(binding))
			c.appliedBindings[observed.Place] = binding.SeatID
		}
	}
	slices.SortFunc(c.lastApplied, func(a, b ocihelper.ComputerSeat) int { return a.Place - b.Place })
}
