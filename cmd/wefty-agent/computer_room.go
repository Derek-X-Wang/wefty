package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/Derek-X-Wang/wefty/l1"
	limarunner "github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

type computerRoomFlags struct {
	seatMemoryBytes int64
	dedicatedCap    int
}

func bindComputerRoomFlags(flags *flag.FlagSet) *computerRoomFlags {
	values := &computerRoomFlags{}
	flags.Int64Var(&values.seatMemoryBytes, "computer-seat-memory-bytes", l1.DefaultSeatMemoryBytes, "node-local room charged per Seat")
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
	if setupPath != "" {
		state, err := ocicontrol.ReadSetupState(setupPath)
		if err != nil {
			return nil, fmt.Errorf("read configured host VM size for Computer room: %w", err)
		}
		capacity, reserve, err = limarunner.DefaultMacComputerCapacity(limarunner.Sizing{
			Memory: state.VMMemory, CPUs: state.VMCPUs, Disk: state.VMDisk,
		})
		if err != nil {
			return nil, err
		}
	}
	if capacity == 0 {
		return nil, nil
	}
	if capacity < 0 || reserve < 0 {
		return nil, errors.New("Computer capacity and infrastructure reserve must be non-negative")
	}
	room := l1.ComputerRoomConfig{MemoryBytes: max(int64(0), capacity-reserve),
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
