package l1

import (
	"errors"
	"fmt"
	"math"
)

const (
	DefaultSeatMemoryBytes      int64 = 1 << 30
	MinimumSeatMemoryBytes      int64 = 256 << 20
	DefaultDedicatedComputerCap       = 1
)

var (
	ErrComputerRoomFull     = errors.New("out of room, try again later")
	ErrDedicatedComputerCap = errors.New("dedicated Computer cap reached")
)

// ComputerRoomConfig is node-local configuration. MemoryBytes is the usable
// Computer ceiling, after the host VM's infrastructure reserve. It is never a
// live memory measurement and does not depend on a running host VM.
type ComputerRoomConfig struct {
	MemoryBytes     int64 `json:"memory_bytes"`
	SeatMemoryBytes int64 `json:"seat_memory_bytes"`
	DedicatedCap    int   `json:"dedicated_cap"`
}

func DefaultComputerRoomConfig(memoryBytes int64) ComputerRoomConfig {
	return ComputerRoomConfig{MemoryBytes: memoryBytes, SeatMemoryBytes: DefaultSeatMemoryBytes,
		DedicatedCap: DefaultDedicatedComputerCap}
}

func (config ComputerRoomConfig) Validate() error {
	if config.MemoryBytes < 0 || config.SeatMemoryBytes < MinimumSeatMemoryBytes || config.DedicatedCap < 0 {
		return errors.New("Computer room requires non-negative memory and dedicated cap, and at least 256 MiB per Seat")
	}
	if config.MemoryBytes/config.SeatMemoryBytes > int64(int(^uint(0)>>1)) {
		return errors.New("Computer room exceeds the service-Slot count range")
	}
	return nil
}

// ComputerRoomUsage is one local record, not an inventory read from the helper.
// MemoryBytes is the effective limit still held, including failed or pending
// cleanup. Only confirmed shrink or absence may reduce it (#1140, #1142).
// Seats also charge their share before runtime growth has finished. An
// owner-made Computer charges its fixed MemoryBytes, without a per-Seat charge.
type ComputerRoomUsage struct {
	ComputerID      string
	MemoryBytes     int64
	SeatRequestMade bool
	SeatLimit       int
	Seats           int
	Running         bool
}

type ComputerRoomSnapshot struct {
	HeldMemoryBytes  int64 `json:"held_memory_bytes"`
	AvailableSeats   int   `json:"available_seats"`
	RunningDedicated int   `json:"running_dedicated"`
}

// Check is the room check used before approval and again by reservation. The
// caller supplies its coherent local records; this function neither contacts
// the host VM nor changes reservations. Reservation fencing belongs to #1142.
func (config ComputerRoomConfig) Check(usage []ComputerRoomUsage, dedicated bool) (ComputerRoomSnapshot, error) {
	var snapshot ComputerRoomSnapshot
	if err := config.Validate(); err != nil {
		return snapshot, err
	}
	seen := make(map[string]bool, len(usage))
	for _, computer := range usage {
		if computer.ComputerID == "" || seen[computer.ComputerID] || computer.MemoryBytes < 0 ||
			computer.Seats < 0 || computer.SeatLimit < 0 || computer.Seats > computer.SeatLimit ||
			computer.SeatRequestMade && computer.SeatLimit == 0 {
			return ComputerRoomSnapshot{}, errors.New("invalid or duplicate local Computer room record")
		}
		seen[computer.ComputerID] = true
		charge := computer.MemoryBytes
		if computer.SeatRequestMade {
			if int64(computer.Seats) > math.MaxInt64/config.SeatMemoryBytes {
				return ComputerRoomSnapshot{}, errors.New("Computer Seat memory charge overflows")
			}
			charge = max(charge, int64(computer.Seats)*config.SeatMemoryBytes)
			if computer.SeatLimit == 1 && (computer.Running || computer.Seats > 0) {
				snapshot.RunningDedicated++
			}
		}
		if charge > math.MaxInt64-snapshot.HeldMemoryBytes {
			return ComputerRoomSnapshot{}, errors.New("Computer memory charge overflows")
		}
		snapshot.HeldMemoryBytes += charge
	}
	remaining := max(int64(0), config.MemoryBytes-snapshot.HeldMemoryBytes)
	snapshot.AvailableSeats = int(remaining / config.SeatMemoryBytes)
	if snapshot.AvailableSeats == 0 {
		return snapshot, ErrComputerRoomFull
	}
	if dedicated && snapshot.RunningDedicated >= config.DedicatedCap {
		return snapshot, fmt.Errorf("%w: %d running or reserved, limit %d", ErrDedicatedComputerCap,
			snapshot.RunningDedicated, config.DedicatedCap)
	}
	return snapshot, nil
}

// WithComputerRoom applies P11's service-Slot floor without changing routing,
// one-shot Slots, claims intent, or the room of other nodes. Setup writes this
// effective service count into L1's installed startup flags. Explicit owner
// counts are authoritative; callers also preserve explicit default-valued counts.
func (policy NodePolicy) WithComputerRoom(config ComputerRoomConfig) (NodePolicy, error) {
	if err := config.Validate(); err != nil {
		return NodePolicy{}, err
	}
	if policy.MaxServiceSlots < 0 {
		return NodePolicy{}, errors.New("configured service Slots must be non-negative")
	}
	if policy.MaxServiceSlots == DefaultMaxServiceSlots {
		policy.MaxServiceSlots = max(policy.MaxServiceSlots, int(config.MemoryBytes/config.SeatMemoryBytes))
	}
	return policy, nil
}
