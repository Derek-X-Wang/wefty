package main

import (
	"flag"
	"path/filepath"
	"testing"

	"github.com/Derek-X-Wang/wefty/l1"
	limarunner "github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

func TestComputerRoomReadsConfiguredSizeWhileHostVMIsStopped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "setup.json")
	state := ocicontrol.SetupState{VMMemory: "8GiB", VMCPUs: 4, VMDisk: "32GiB", VMType: "vz",
		HostMountRoot: t.TempDir(), ProbeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		MemoryCapacityBytes: 4 << 30, MemoryReserveBytes: 2 << 30}
	if err := ocicontrol.WriteSetupState(path, state); err != nil {
		t.Fatal(err)
	}
	// A stopped host VM has no helper to call. Stale capacity facts in an
	// older setup file must not override its explicit 8 GiB configured size.
	room, err := configuredComputerRoom(path, 0, 0, l1.DefaultSeatMemoryBytes, 1)
	if err != nil || room == nil || room.MemoryBytes != 7<<30 {
		t.Fatalf("configured room = %+v, %v", room, err)
	}
	policy, err := (l1.NodePolicy{MaxServiceSlots: 2}).WithComputerRoom(*room)
	if err != nil || policy.MaxServiceSlots != 7 {
		t.Fatalf("local service Slots = %d, %v", policy.MaxServiceSlots, err)
	}
	sizing, err := configuredMacSizing(path, func() (limarunner.Sizing, error) {
		t.Fatal("bootstrap consulted host defaults instead of configured VM size")
		return limarunner.Sizing{}, nil
	})
	if err != nil || sizing.Memory != "8GiB" {
		t.Fatalf("bootstrap sizing = %+v, %v", sizing, err)
	}
	room, err = configuredComputerRoom("", 0, 0, l1.DefaultSeatMemoryBytes, 1)
	if err != nil || room != nil {
		t.Fatalf("legacy node without configured room changed: %+v, %v", room, err)
	}
}

func TestComputerRoomStartupFlagsSetNodeSettings(t *testing.T) {
	flags := flag.NewFlagSet("node", flag.ContinueOnError)
	values := bindComputerRoomFlags(flags)
	if err := flags.Parse([]string{"--computer-seat-memory-bytes=536870912", "--computer-dedicated-cap=0"}); err != nil {
		t.Fatal(err)
	}
	room, err := values.configured("", 8<<30, 1<<30)
	if err != nil || room == nil || room.SeatMemoryBytes != 512<<20 || room.DedicatedCap != 0 {
		t.Fatalf("node settings = %+v, %v", room, err)
	}
	if _, err := room.Check(nil, true); err == nil {
		t.Fatal("dedicated cap 0 did not disable dedicated requests")
	}
}
