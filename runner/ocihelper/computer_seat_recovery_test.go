package ocihelper

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestRecoveredSeatCleanupNeedsPositiveAbsence(t *testing.T) {
	s := newComputerSeats(newManualClock(time.Unix(100, 0)), t.TempDir())
	if _, err := s.apply(nil); err != nil {
		t.Fatal(err)
	}
	seat := ComputerSeat{Place: 1, User: "seat1", ViewPort: 7102, ControlPort: 7103}
	if err := s.recoverCleanup([]ComputerSeat{seat}); err != nil {
		t.Fatal(err)
	}
	full := true
	observe := func() ObserveComputerSeatsResponse {
		t.Helper()
		r, e := s.observe(context.Background(), s.generation, func(string) ([]byte, error) { return nil, os.ErrNotExist }, func(context.Context, uint16) error { t.Fatal("cleanup-only Seat was probed as wanted"); return nil }, func(ComputerSeat) error {
			if full {
				return errors.New("no space left on device")
			}
			return nil
		})
		if e != nil {
			t.Fatal(e)
		}
		return r
	}
	if r := observe(); len(r.Seats) != 1 || r.Seats[0].CleanupComplete {
		t.Fatalf("failed cleanup became proof: %+v", r)
	}
	full = false
	if r := observe(); len(r.Seats) != 1 || !r.Seats[0].CleanupComplete || r.Seats[0].State != "cleaned" {
		t.Fatalf("cleanup did not resume: %+v", r)
	}
	if len(s.wanted) != 0 {
		t.Fatal("cleanup registration resurrected a Seat")
	}
}

func TestSeatDiscoveryReportsWantedPlacesWithoutReadiness(t *testing.T) {
	s := newComputerSeats(newManualClock(time.Unix(100, 0)), t.TempDir())
	wanted := []ComputerSeat{{Place: 1, User: "seat1", ViewPort: 7102, ControlPort: 7103}}
	if _, err := s.apply(wanted); err != nil {
		t.Fatal(err)
	}
	if err := s.recoverCleanup([]ComputerSeat{{Place: 2, User: "seat2", ViewPort: 7104, ControlPort: 7105}}); err != nil {
		t.Fatal(err)
	}
	r := s.discover()
	if r.Generation != s.generation || len(r.Seats) != 1 || r.Seats[0].Place != 1 || r.Seats[0].State != "starting" || r.Seats[0].CleanupComplete {
		t.Fatalf("discovery claimed readiness or cleanup: %+v", r)
	}
}
