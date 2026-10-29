package main

import (
	"bytes"
	"errors"
	"flag"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/connprofile"
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
	// A stopped host VM has no helper to call. Its installed helper ceiling
	// still bounds admission until bootstrap updates that ceiling.
	room, err := configuredComputerRoom(path, 0, 0, l1.DefaultSeatMemoryBytes, 1)
	if err != nil || room == nil || room.MemoryBytes != 2<<30 {
		t.Fatalf("configured room = %+v, %v", room, err)
	}
	policy, err := (l1.NodePolicy{MaxServiceSlots: 2}).WithComputerRoom(*room)
	if err != nil || policy.MaxServiceSlots != 2 {
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

func TestComputerRoomUnavailableSetupStateKeepsNodeRunning(t *testing.T) {
	for _, name := range []string{"missing", "unreadable", "invalid"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "setup.json")
			if name == "unreadable" {
				if err := ocicontrol.WriteSetupState(path, ocicontrol.SetupState{VMMemory: "8GiB", VMCPUs: 4, VMDisk: "32GiB", VMType: "native", HostMountRoot: t.TempDir(), ProbeDigest: "sha256:" + strings.Repeat("a", 64), MemoryCapacityBytes: 8 << 30, MemoryReserveBytes: 1 << 30}); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(path, 0000); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Chmod(path, 0600) })
				if _, err := os.ReadFile(path); !errors.Is(err, os.ErrPermission) {
					if os.Geteuid() == 0 {
						t.Skip("root bypasses setup-state permissions; this regression needs the operator UID")
					}
					t.Fatalf("setup state must really be unreadable: %v", err)
				}
			}
			if name == "invalid" {
				if err := os.WriteFile(path, []byte("{invalid"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			var output bytes.Buffer
			prior := log.Writer()
			log.SetOutput(&output)
			defer log.SetOutput(prior)
			room, err := configuredComputerRoom(path, 8<<30, 1<<30, l1.DefaultSeatMemoryBytes, 1)
			if err != nil || room != nil {
				t.Fatalf("node startup must keep legacy admission: %+v, %v", room, err)
			}
			report := ocicontrol.BuildDoctor(t.Context(), ocicontrol.DoctorConfig{SetupStatePath: path})
			found := false
			for _, finding := range report.Findings {
				if finding.Code == "oci_convergence_state_unavailable" {
					found = true
				}
			}
			if !found {
				t.Fatal("unavailable setup state lost its doctor finding")
			}
			if !strings.Contains(output.String(), "computer_room_unconfigured") {
				t.Fatalf("missing room diagnostic: %s", output.String())
			}
		})
	}
}
func TestComputerRoomMatchesHelperOnNativeLinuxAndMac(t *testing.T) {
	for _, tc := range []struct {
		name, vmType            string
		capacity, reserve, want int64
	}{
		{"native uses configured capacity", "native", 12 << 30, 2 << 30, 10 << 30},
		{"Mac bounds larger helper", "vz", 12 << 30, 2 << 30, 7 << 30},
		{"Mac bounds smaller helper", "vz", 4 << 30, 2 << 30, 2 << 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "setup.json")
			state := ocicontrol.SetupState{VMMemory: "8GiB", VMCPUs: 4, VMDisk: "32GiB", VMType: tc.vmType, HostMountRoot: t.TempDir(), ProbeDigest: "sha256:" + strings.Repeat("a", 64), MemoryCapacityBytes: tc.capacity, MemoryReserveBytes: tc.reserve}
			if err := ocicontrol.WriteSetupState(path, state); err != nil {
				t.Fatal(err)
			}
			room, err := configuredComputerRoom(path, 0, 0, 1<<30, 1)
			if err != nil || room == nil || room.MemoryBytes != tc.want {
				t.Fatalf("room=%+v, %v; want %d", room, err, tc.want)
			}
		})
	}
}
func TestComputerRoomRejectsTinySeatFlag(t *testing.T) {
	flags := flag.NewFlagSet("node", flag.ContinueOnError)
	bindComputerRoomFlags(flags)
	if err := flags.Parse([]string{"--computer-seat-memory-bytes=268435455"}); err == nil {
		t.Fatal("Seat flag accepted less than 256 MiB")
	}
}

func TestComputerRoomServiceSlotsHonorExplicitFlagAndProfile(t *testing.T) {
	for _, tc := range []struct {
		name    string
		flag    bool
		profile []connprofile.NodeAdmission
		want    int
	}{
		{name: "default", want: 7},
		{name: "explicit default-valued flag", flag: true, want: 2},
		{name: "setup default-valued profile", profile: []connprofile.NodeAdmission{{NodeID: "local", MaxServiceSlots: 2}}, want: 7},
		{name: "explicit lower profile", profile: []connprofile.NodeAdmission{{NodeID: "local", MaxServiceSlots: 1}}, want: 1},
		{name: "flag wins profile", flag: true, profile: []connprofile.NodeAdmission{{NodeID: "local", MaxServiceSlots: 1}}, want: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			prior := log.Writer()
			log.SetOutput(&output)
			defer log.SetOutput(prior)
			count, err := computerRoomServiceSlots(l1.DefaultComputerRoomConfig(7<<30), 2, tc.flag, connprofile.Profile{NodeAdmission: tc.profile}, "local")
			if err != nil || count != tc.want {
				t.Fatalf("service Slots=%d, %v; want %d", count, err, tc.want)
			}
			if tc.want == 7 && strings.Contains(output.String(), "service_slots_owner_override") {
				t.Fatalf("default count warned as owner override: %s", output.String())
			}
			if tc.want < 7 && !strings.Contains(output.String(), "service_slots_owner_override") {
				t.Fatalf("owner override lacks warning: %s", output.String())
			}
		})
	}
}
