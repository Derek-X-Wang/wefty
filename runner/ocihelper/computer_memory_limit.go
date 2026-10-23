package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const MethodSetComputerMemoryLimit Method = "SetComputerMemoryLimit"

// Generation is the current helper Seat directive generation (zero before the
// first directive). A grow must complete before publishing the added Seat.
type SetComputerMemoryLimitRequest struct {
	Authority   AttemptAuthority `json:"authority"`
	MemoryBytes int64            `json:"memory_bytes"`
	Generation  uint64           `json:"generation"`
}
type SetComputerMemoryLimitResponse struct {
	ObservedAt           time.Time `json:"observed_at"`
	RequestedMemoryBytes int64     `json:"requested_memory_bytes"`
	MemoryLimitBytes     int64     `json:"memory_limit_bytes"`
	ReservedMemoryBytes  int64     `json:"reserved_memory_bytes"`
	MemoryCurrentBytes   int64     `json:"memory_current_bytes"`
	HeldMemoryBytes      int64     `json:"held_memory_bytes"`
	Reason               string    `json:"reason"`
}
type ComputerMemoryLimitEngine interface {
	SetComputerMemoryLimit(context.Context, SetComputerMemoryLimitRequest) (SetComputerMemoryLimitResponse, error)
}

// The engine holds seatsMu and capacityMu, but neither lifecycle mutex, across
// this transaction. The runtime pins the exact cgroup directory, not its name.
type computerMemoryRuntime interface {
	readLimit() (int64, error)
	readUsage() (int64, error)
	writeLimit(int64) error
	freeze(context.Context) (func() error, error)
}

func changeComputerMemoryLimit(ctx context.Context, request SetComputerMemoryLimitRequest, seats *computerSeats, tmpfsCeiling int64, reserved *int64, available int64, runtime computerMemoryRuntime, clock Clock, validate func() error) (response SetComputerMemoryLimitResponse, err error) {
	if request.MemoryBytes <= 0 || request.MemoryBytes%4096 != 0 {
		return response, errors.New("memory limit must be a positive multiple of 4096 bytes")
	}
	generation := uint64(0)
	if seats != nil {
		generation = seats.generation
	}
	if request.Generation != generation {
		return response, &SeatOperationRefusalError{Reason: SeatRefusalGenerationMismatch}
	}
	if err = validate(); err != nil {
		return response, err
	}
	old, err := runtime.readLimit()
	if err != nil {
		return response, err
	}
	if old <= 0 || old != *reserved {
		if old > *reserved {
			*reserved = old
		}
		return response, errors.New("live memory limit differs from its reservation")
	}
	target := request.MemoryBytes
	if target < old {
		if tmpfsCeiling <= 0 || target/2 < tmpfsCeiling {
			return response, &SeatOperationRefusalError{Reason: SeatRefusalTmpfsFloor}
		}
		cleaned := false
		if seats != nil {
			for _, entry := range seats.entries {
				if !entry.wanted {
					if !entry.cleaned {
						return response, &SeatOperationRefusalError{Reason: SeatRefusalCleanupNotConfirmed}
					}
					cleaned = cleaned || !entry.memoryReturned
				}
			}
		}
		if !cleaned {
			return response, &SeatOperationRefusalError{Reason: SeatRefusalCleanupNotConfirmed}
		}
	}
	if target > old && target-old > available {
		return response, &SeatOperationRefusalError{Reason: SeatRefusalMemoryCapacity}
	}
	if target != old {
		// Freeze before sampling a shrink, and across a grow's rollback window.
		// This avoids killing remaining Seats by writing below concurrent use.
		thaw, freezeErr := runtime.freeze(ctx)
		if freezeErr != nil {
			return response, freezeErr
		}
		defer func() {
			if thawErr := thaw(); thawErr != nil {
				// Do not wrap a prior typed refusal: an unconfirmed thaw is broken,
				// even when the memory write itself was a recoverable refusal.
				err = fmt.Errorf("thaw Computer failed: %v; prior result: %v", thawErr, err)
			}
		}()
	}
	usage, err := runtime.readUsage()
	if err != nil {
		return response, err
	}
	if usage < 0 {
		return response, errors.New("negative memory usage")
	}
	response = SetComputerMemoryLimitResponse{RequestedMemoryBytes: target, MemoryLimitBytes: old, ReservedMemoryBytes: *reserved, MemoryCurrentBytes: usage}
	if target < old && usage > target {
		response.HeldMemoryBytes = old - target
		response.Reason = "memory_in_use"
	} else if target != old {
		if err = ctx.Err(); err != nil {
			return SetComputerMemoryLimitResponse{}, err
		}
		if err = validate(); err != nil {
			return SetComputerMemoryLimitResponse{}, err
		}
		// Reserve the grow first; release a shrink only after kernel readback.
		if target > old {
			*reserved = target
		}
		writeErr := runtime.writeLimit(target)
		actual, readErr := runtime.readLimit()
		if writeErr != nil || readErr != nil || actual != target {
			// A failed grow is unchanged only when readback proves it. Otherwise try
			// to restore it, and retain the larger reservation on any uncertainty.
			if target > old {
				if readErr != nil || actual != old {
					_ = runtime.writeLimit(old)
					actual, readErr = runtime.readLimit()
				}
				if readErr == nil && actual == old {
					*reserved = old
					return SetComputerMemoryLimitResponse{}, &SeatOperationRefusalError{Reason: SeatRefusalMemoryGrowFailed}
				}
			}
			if readErr == nil && actual > *reserved {
				*reserved = actual
			}
			return SetComputerMemoryLimitResponse{}, errors.New("memory limit update could not be confirmed; room remains held")
		}
		if target < old && seats != nil {
			// The caller requests the final per-Seat target, not a transient usage
			// sample. Only successful readback consumes cleanup; a held shrink keeps
			// its proof even if another Seat needs a grow in the meantime.
			for _, entry := range seats.entries {
				if !entry.wanted && entry.cleaned {
					entry.memoryReturned = true
				}
			}
		}
		*reserved = actual
		response.MemoryLimitBytes = actual
		response.ReservedMemoryBytes = actual
	}
	if err = validate(); err != nil {
		return SetComputerMemoryLimitResponse{}, err
	}
	response.ObservedAt = clock.Now().UTC().Round(0)
	return response, nil
}

// A freeze can be delayed by an uninterruptible task. This bound belongs to
// helper state logic and therefore uses the injected clock, including polling.
func waitComputerMemoryFrozen(ctx context.Context, clock Clock, read func() ([]byte, error)) error {
	deadline := clock.Now().Add(2 * time.Second)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := read()
		if err != nil {
			return err
		}
		frozen := ""
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) > 0 && fields[0] == "frozen" {
				if len(fields) != 2 || frozen != "" || (fields[1] != "0" && fields[1] != "1") {
					return errors.New("invalid cgroup freezer evidence")
				}
				frozen = fields[1]
			}
		}
		if frozen == "" {
			return errors.New("missing cgroup freezer evidence")
		}
		if !clock.Now().Before(deadline) {
			return &SeatOperationRefusalError{Reason: SeatRefusalMemoryBusy}
		}
		if frozen == "1" {
			return nil
		}
		timer := clock.NewTimerAt(clock.Now().Add(10 * time.Millisecond))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C():
			timer.Stop()
		}
	}
}
