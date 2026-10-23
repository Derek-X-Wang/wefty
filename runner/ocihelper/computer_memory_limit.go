package ocihelper

import (
	"context"
	"errors"
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
	RecoveredFreeze      bool      `json:"recovered_freeze"`
	FreezeRequests       int       `json:"freeze_requests"`
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
	recoverFreeze(context.Context) (bool, error)
	shrink(context.Context, int64) (computerMemoryShrinkResult, error)
	freezerRequests() int
}

func changeComputerMemoryLimit(ctx context.Context, request SetComputerMemoryLimitRequest, seats *computerSeats, tmpfsCeiling int64, reserved *int64, available int64, runtime computerMemoryRuntime, clock Clock, validate func() error) (response SetComputerMemoryLimitResponse, err error) {
	recovered := false
	initialFreezeRequests := runtime.freezerRequests()
	defer func() {
		response.RecoveredFreeze = recovered
		response.FreezeRequests = runtime.freezerRequests() - initialFreezeRequests
	}()
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
	recovered, err = runtime.recoverFreeze(ctx)
	if err != nil {
		return response, err
	}
	old, err := runtime.readLimit()
	if err != nil {
		return response, err
	}
	if old <= 0 {
		return response, errors.New("live memory limit must be positive")
	}
	consumeCleanup := func() {
		if seats != nil {
			for _, entry := range seats.entries {
				if !entry.wanted && entry.cleaned {
					entry.memoryReturned = true
				}
			}
		}
	}
	if old < *reserved {
		// A later exact readback completes an earlier uncertain shrink. Release
		// only the proved difference and include it in this call's grow budget.
		available += *reserved - old
		*reserved = old
	} else if old > *reserved {
		*reserved = old
		return response, errors.New("live memory limit exceeds its reservation")
	}
	if seats != nil && seats.pendingShrinkFrom > 0 {
		if old < seats.pendingShrinkFrom {
			consumeCleanup()
		}
		seats.pendingShrinkFrom = 0
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
			return response, err
		}
		// All containerd reads precede freeze. shrink performs only cgroup I/O and
		// finishes thaw before returning, including on error or deadline expiry.
		if err = validate(); err != nil {
			return response, err
		}
		actual := int64(0)
		if target < old {
			var result computerMemoryShrinkResult
			if seats != nil {
				seats.pendingShrinkFrom = old
			}
			result, err = runtime.shrink(ctx, target)
			if err != nil {
				return response, err
			}
			if seats != nil {
				seats.pendingShrinkFrom = 0
			}
			actual = result.limit
			response.MemoryCurrentBytes = result.usage
			if result.usage > target {
				response.HeldMemoryBytes = old - target
				response.Reason = "memory_in_use"
			} else {
				consumeCleanup()
			}
		} else {
			*reserved = target
			writeErr := runtime.writeLimit(target)
			var readErr error
			actual, readErr = runtime.readLimit()
			if readErr != nil {
				return response, errors.New("memory grow readback failed; room remains held")
			}
			if actual != target {
				if actual == old {
					*reserved = old
					return response, &SeatOperationRefusalError{Reason: SeatRefusalMemoryGrowFailed}
				}
				if actual > *reserved {
					*reserved = actual
				}
				return response, errors.New("memory grow could not be confirmed; room remains held")
			}
			// The larger limit is already in force. Never undo a grow by writing a
			// smaller limit, even if the write returned an error.
			_ = writeErr
		}
		if actual <= 0 {
			return response, errors.New("unbounded memory limit readback")
		}
		*reserved = actual
		response.MemoryLimitBytes = actual
		response.ReservedMemoryBytes = actual
	}
	// This can contact containerd only after shrink has completed thaw.
	if err = validate(); err != nil {
		return response, err
	}
	response.ObservedAt = clock.Now().UTC().Round(0)
	return response, nil
}
