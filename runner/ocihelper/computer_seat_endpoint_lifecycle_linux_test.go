//go:build linux

package ocihelper

import (
	"errors"
	"os"
	"testing"
)

type seatPinCheckingDiskSystem struct {
	*fakeComputerDiskSystem
	beforeDetach func()
}

func (s *seatPinCheckingDiskSystem) detach(mountPath, loopPath, imagePath string) error {
	s.beforeDetach()
	return s.fakeComputerDiskSystem.detach(mountPath, loopPath, imagePath)
}

// Calls the real disk detachment and sweep entry points, not just the pin
// helper. The fake disk system observes descriptor state at the unmount seam.
func TestComputerDiskDetachmentDrainsSeatPinsBeforeUnmount(t *testing.T) {
	for _, operation := range []string{"reap", "sweep"} {
		t.Run(operation, func(t *testing.T) {
			called := false
			var pins []*os.File
			var attempt *containerdAttempt
			system := &seatPinCheckingDiskSystem{fakeComputerDiskSystem: newFakeComputerDiskSystem(), beforeDetach: func() {
				called = true
				attempt.mu.Lock()
				deleted := attempt.deleted
				attempt.mu.Unlock()
				if !deleted {
					t.Error("disk detach began with live Seat authority")
				}
				for _, pin := range pins {
					if _, err := pin.Stat(); !errors.Is(err, os.ErrClosed) {
						t.Errorf("held Seat pin reached disk detach: %v", err)
					}
				}
			}}
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: t.TempDir()}, diskSystem: system}
			authority := testComputerAuthority("seat-detach", "fence", "boot")
			attachment, err := engine.attachComputerDisk(t.Context(), testComputerStorage(), authority)
			if err != nil {
				t.Fatal(err)
			}
			seats := newComputerSeats(nil, t.TempDir())
			for _, place := range []int{1, 2} {
				entry := &computerSeatEntry{endpointPins: map[string]*os.File{}, pinOwner: seats}
				seats.entries[place] = entry
				for _, kind := range []string{"view", "control"} {
					root, _ := seatEndpointFixture(t)
					f, err := seats.openEndpointPin(func() (*os.File, error) { return openSeatEndpoint(root, 1, "view", uint32(os.Getuid())) })
					if err != nil {
						t.Fatal(err)
					}
					defer f.Close()
					pins = append(pins, f)
					entry.endpointPins[kind] = f
				}
			}
			attempt = &containerdAttempt{authority: authority, computerDisk: attachment, seats: seats}
			engine.attempts = map[string]*containerdAttempt{authority.key(): attempt}
			if operation == "reap" {
				err = engine.detachComputerDisk(attachment, computerDiskReapReceipt, "")
			} else {
				err = engine.sweepComputerDisks(t.Context(), "seat-detach-sweep")
			}
			if err != nil {
				t.Fatal(err)
			}
			if !called {
				t.Fatal("disk detachment was not exercised")
			}
		})
	}
}
