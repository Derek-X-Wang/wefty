package main

import (
	"errors"
	"flag"
	"log"
	"os"
	"strconv"

	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
	limarunner "github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

type computerRoomFlags struct {
	seatMemoryBytes int64
	dedicatedCap    int
}

func bindComputerRoomFlags(flags *flag.FlagSet) *computerRoomFlags {
	values := &computerRoomFlags{seatMemoryBytes: l1.DefaultSeatMemoryBytes}
	flags.Func("computer-seat-memory-bytes", "node-local room charged per Seat (minimum 268435456; default 1073741824)", func(value string) error {
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || n < l1.MinimumSeatMemoryBytes {
			return errors.New("room per Seat must be at least 256 MiB")
		}
		values.seatMemoryBytes = n
		return nil
	})
	flags.IntVar(&values.dedicatedCap, "computer-dedicated-cap", l1.DefaultDedicatedComputerCap, "running Seat-request-made dedicated Computer cap; 0 disables dedicated requests")
	return values
}

func (values *computerRoomFlags) configured(path string, capacity, reserve int64) (*l1.ComputerRoomConfig, error) {
	if err := (l1.ComputerRoomConfig{SeatMemoryBytes: values.seatMemoryBytes, DedicatedCap: values.dedicatedCap}).Validate(); err != nil {
		return nil, err
	}
	return configuredComputerRoom(path, capacity, reserve, values.seatMemoryBytes, values.dedicatedCap)
}

// configuredComputerRoom reads only node-local configuration. In particular it
// must work before a stopped host VM has a helper or can publish capabilities.
// A legacy node without a configured ceiling keeps its existing admission.
func configuredComputerRoom(setupPath string, capacity, reserve, seatMemory int64, dedicatedCap int) (*l1.ComputerRoomConfig, error) {
	var vmRoom *int64
	if setupPath != "" {
		state, err := ocicontrol.ReadSetupState(setupPath)
		if err != nil {
			log.Printf("event=computer_room_unconfigured reason=setup_state_unavailable error=%q", err)
			return nil, nil
		}
		if capacity == 0 {
			capacity, reserve = state.MemoryCapacityBytes, state.MemoryReserveBytes
		}
		if state.VMType == "vz" {
			total, infra, err := limarunner.DefaultMacComputerCapacity(limarunner.Sizing{Memory: state.VMMemory, CPUs: state.VMCPUs, Disk: state.VMDisk})
			if err != nil {
				return nil, err
			}
			available := total - infra
			vmRoom = &available
		}
	}
	if capacity == 0 {
		return nil, nil
	}
	if capacity < 0 || reserve < 0 {
		return nil, errors.New("Computer capacity and infrastructure reserve must be non-negative")
	}
	available := max(int64(0), capacity-reserve)
	if vmRoom != nil {
		available = min(available, *vmRoom)
	}
	room := l1.ComputerRoomConfig{MemoryBytes: available,
		SeatMemoryBytes: seatMemory, DedicatedCap: dedicatedCap}
	if err := room.Validate(); err != nil {
		return nil, err
	}
	return &room, nil
}

// Bootstrap uses the persisted configured size when one exists. Only a first
// install may ask the setup sizing defaults; neither path reads a running VM.
func configuredMacSizing(setupPath string, defaults func() (limarunner.Sizing, error)) (limarunner.Sizing, error) {
	state, err := ocicontrol.ReadSetupState(setupPath)
	var sizing limarunner.Sizing
	switch {
	case err == nil:
		sizing = limarunner.Sizing{Memory: state.VMMemory, CPUs: state.VMCPUs, Disk: state.VMDisk}
	case errors.Is(err, os.ErrNotExist):
		sizing, err = defaults()
	default:
		return limarunner.Sizing{}, err
	}
	if err != nil {
		return limarunner.Sizing{}, err
	}
	return sizing, sizing.Validate()
}

func computerRoomServiceSlots(room l1.ComputerRoomConfig, count int, explicit bool, profile connprofile.Profile, nodeID string) (int, error) {
	if !explicit {
		if policy, ok := profile.L1().NodePolicies()[nodeID]; ok {
			count = policy.MaxServiceSlots
			explicit = count != l1.DefaultMaxServiceSlots
		}
	}
	policy, err := (l1.NodePolicy{MaxServiceSlots: count}).WithComputerRoom(room)
	if err != nil {
		return 0, err
	}
	if !explicit {
		return policy.MaxServiceSlots, nil
	}
	if count < int(room.MemoryBytes/room.SeatMemoryBytes) {
		log.Printf("event=computer_room_service_slots_owner_override service_slots=%d room_seats=%d", count, room.MemoryBytes/room.SeatMemoryBytes)
	}
	return count, nil
}
