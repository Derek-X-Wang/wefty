//go:build linux && computer_seat_lane

package ocihelper

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	containerd "github.com/containerd/containerd/v2/client"
)

// Uses the existing root-only production-profile fixture. The observable
// driver files are read through the running Computer's own read-only mount.
func TestHelperSeatDriverP8(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Fatal("computer_seat_lane requires root")
	}
	image, address := os.Getenv("WEFTY_SEAT_IMAGE"), os.Getenv("WEFTY_SEAT_CONTAINERD")
	if image == "" || address == "" {
		t.Fatal("Seat lane provisioning missing")
	}
	withSeatEndpointComputer(t, image, address, func(ctx context.Context, engine *ContainerdEngine, a AttemptAuthority, task containerd.Task, _ string) {
		seats := []ComputerSeat{{1, "seat1", 7102, 7103}, {2, "seat2", 7104, 7105}}
		applied, err := engine.ApplyComputerSeats(ctx, ApplyComputerSeatsRequest{Authority: a, Seats: seats})
		if err != nil {
			t.Fatal(err)
		}
		read := func(place int, want string) {
			t.Helper()
			path := filepath.Join(fmt.Sprintf("/proc/%d/root", task.Pid()), "wefty/control/seats", fmt.Sprint(place), "driver.json")
			data, err := os.ReadFile(path)
			if err != nil || string(data) != want {
				t.Fatalf("Seat %d mounted driver=%q %v", place, data, err)
			}
			info, err := os.Stat(path)
			if err != nil || info.Mode().Perm() != 0444 {
				t.Fatalf("Seat %d driver permissions=%v %v", place, info, err)
			}
		}
		read(1, `{"version":1,"human_driving":false}`)
		read(2, `{"version":1,"human_driving":false}`)
		request := SetSeatDriverRequest{Authority: a, Generation: applied.Generation, Place: 1, HumanDriving: true}
		if err = engine.SetSeatDriver(ctx, request); err != nil {
			t.Fatal(err)
		}
		read(1, `{"version":1,"human_driving":true}`)
		read(2, `{"version":1,"human_driving":false}`)
		stale := request
		stale.Authority.FencingToken = "old-fence"
		if err = engine.SetSeatDriver(ctx, stale); err == nil {
			t.Fatal("stale Attempt changed driver")
		}
		stale = request
		stale.Generation++
		if err = engine.SetSeatDriver(ctx, stale); err == nil {
			t.Fatal("stale generation changed driver")
		}
		read(1, `{"version":1,"human_driving":true}`)
		request.HumanDriving = false
		if err = engine.SetSeatDriver(ctx, request); err != nil {
			t.Fatal(err)
		}
		read(1, `{"version":1,"human_driving":false}`)
		request.HumanDriving = true
		if err = engine.SetSeatDriver(ctx, request); err != nil {
			t.Fatal(err)
		}
		if _, err = engine.ApplyComputerSeats(ctx, ApplyComputerSeatsRequest{Authority: a, Seats: seats[1:]}); err != nil {
			t.Fatal(err)
		}
		read(1, `{"version":1,"human_driving":false}`)
		read(2, `{"version":1,"human_driving":false}`)
		status, err := task.Status(ctx)
		if err != nil || status.Status != containerd.Running {
			t.Fatalf("driver operation ended Computer: %+v %v", status, err)
		}
	})
}
