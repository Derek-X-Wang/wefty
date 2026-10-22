package l1

import (
	"errors"
	"fmt"
	"testing"
)

func TestComputerRoomMatrixDoesNotNeedTheHostVM(t *testing.T) {
	for _, test := range []struct {
		macGiB int
		room   int
	}{{16, 3}, {32, 7}} {
		for dedicated := 0; dedicated <= test.room; dedicated++ {
			t.Run(fmt.Sprintf("%dGiB/dedicated=%d", test.macGiB, dedicated), func(t *testing.T) {
				room := DefaultComputerRoomConfig(int64(test.room) << 30)
				// Exercise every shared/dedicated mix when the owner raises the
				// cap. The shipped cap of 1 is checked separately by RM-3.
				room.DedicatedCap = test.room
				var usage []ComputerRoomUsage
				for i := 0; i < dedicated; i++ {
					usage = append(usage, ComputerRoomUsage{ComputerID: fmt.Sprintf("dedicated-%d", i), SeatRequestMade: true,
						SeatLimit: 1, Seats: 1, MemoryBytes: 1 << 30, Running: true})
				}
				left := test.room - dedicated
				for i := 0; left > 0; i++ {
					seats := min(left, 2)
					usage = append(usage, ComputerRoomUsage{ComputerID: fmt.Sprintf("shared-%d", i),
						SeatRequestMade: true, SeatLimit: 2, Seats: seats, MemoryBytes: int64(seats) << 30, Running: true})
					left -= seats
				}
				// The only inputs are the configured room and local records. There
				// is no helper client, runtime observation or host VM process.
				got, err := room.Check(usage, false)
				if !errors.Is(err, ErrComputerRoomFull) || got.AvailableSeats != 0 || got.HeldMemoryBytes != int64(test.room)<<30 {
					t.Fatalf("full room = %+v, %v", got, err)
				}
				policy, err := (NodePolicy{MaxServiceSlots: 2}).WithComputerRoom(room)
				if err != nil || policy.MaxServiceSlots != test.room {
					t.Fatalf("service Slots = %d, %v; want %d", policy.MaxServiceSlots, err, test.room)
				}
				last := &usage[len(usage)-1]
				last.Seats--
				last.MemoryBytes -= 1 << 30 // Only after confirmed cleanup and shrink.
				if last.MemoryBytes == 0 {
					last.Running = false
				}
				got, err = room.Check(usage, false)
				if err != nil || got.AvailableSeats != 1 {
					t.Fatalf("one Seat fits after confirmed shrink = %+v, %v", got, err)
				}
			})
		}
	}
}

func TestDedicatedComputerCapCountsOnlyRunningSeatRequestComputers(t *testing.T) {
	room := DefaultComputerRoomConfig(7 << 30)
	for _, test := range []struct {
		name    string
		made    bool
		running bool
		limit   int
		want    error
	}{
		{"owner-made dedicated", false, true, 1, nil},
		{"stopped on demand", true, false, 1, nil},
		{"running shared", true, true, 2, nil},
		{"RM-3 running dedicated", true, true, 1, ErrDedicatedComputerCap},
	} {
		t.Run(test.name, func(t *testing.T) {
			usage := []ComputerRoomUsage{{ComputerID: "existing", SeatRequestMade: test.made,
				Running: test.running, SeatLimit: test.limit, MemoryBytes: 1 << 30}}
			got, err := room.Check(usage, true)
			if !errors.Is(err, test.want) || got.AvailableSeats != 6 {
				t.Fatalf("dedicated check = %+v, %v; want %v", got, err, test.want)
			}
			if _, err := room.Check(usage, false); err != nil {
				t.Fatalf("dedicated cap blocked a shared request: %v", err)
			}
		})
	}
	// A retained memory limit stays charged even after the Seat is gone.
	usage := []ComputerRoomUsage{{ComputerID: "cleanup-pending", SeatRequestMade: true,
		SeatLimit: 2, Seats: 0, MemoryBytes: 6 << 30}}
	got, err := room.Check(usage, false)
	if err != nil || got.AvailableSeats != 1 {
		t.Fatalf("unshrunk memory was released: %+v, %v", got, err)
	}
	// Owner-made Computers charge their own limit, with no Seat charge.
	usage = append(usage, ComputerRoomUsage{ComputerID: "owner", MemoryBytes: 1 << 30})
	if _, err := room.Check(usage, false); !errors.Is(err, ErrComputerRoomFull) {
		t.Fatalf("owner memory did not use the same room: %v", err)
	}
}
