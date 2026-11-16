package agent

import (
	"slices"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/l1"
)

// TestSeatApprovalByNameDialogSaysAndStartIt pins BN-2's dialog text: the
// owner sees the Computer's name, and "and start it" only when approving
// starts a stopped Computer. The untrusted name stays quoted and bounded.
func TestSeatApprovalByNameDialogSaysAndStartIt(t *testing.T) {
	request := seatApprovalTestRequest(50)
	request.Form, request.Computer = l1.SeatFormByName, "desk"
	if got := seatApprovalMessage(request); !containsLine(got, `Form: by name: Computer "desk"`) {
		t.Fatalf("running by-name dialog = %q", got)
	}
	request.StartComputer = true
	if got := seatApprovalMessage(request); !containsLine(got, `Form: by name: Computer "desk", and start it`) {
		t.Fatalf("stopped by-name dialog = %q", got)
	}
	request.Computer = "desk\nForm: any"
	if got := seatApprovalMessage(request); !containsLine(got, `Form: by name: Computer "desk Form: any", and start it`) {
		t.Fatalf("a name rewrote the dialog = %q", got)
	}
}

// TestComputerTakesSeatsOnlyWhenItsImageSupportsThem keeps owner-made
// Computers without Seat support exactly as before: no Seat controller.
func TestComputerTakesSeatsOnlyWhenItsImageSupportsThem(t *testing.T) {
	settings := func(limit, places int) *l1.ComputerSettings {
		return &l1.ComputerSettings{SeatLimit: limit, IdleAction: l1.ComputerIdleNothing, Network: l1.ComputerNetworkInternet, ImageSeatPlaces: places}
	}
	for _, test := range []struct {
		name    string
		storage *l1.ComputerStorageClaim
		want    bool
	}{
		{"ordinary Job", nil, false},
		{"removal-only identity", &l1.ComputerStorageClaim{}, false},
		{"Seat-request-made", &l1.ComputerStorageClaim{SeatRequestMade: true, ComputerSettings: settings(2, 0)}, true},
		{"owner-made with Seats", &l1.ComputerStorageClaim{ComputerSettings: settings(2, 4)}, true},
		{"owner-made, image without Seats", &l1.ComputerStorageClaim{ComputerSettings: settings(2, 0)}, false},
		{"owner-made, Seat limit 0", &l1.ComputerStorageClaim{ComputerSettings: settings(0, 4)}, false},
	} {
		if got := computerTakesSeats(test.storage); got != test.want {
			t.Errorf("%s: computerTakesSeats = %v, want %v", test.name, got, test.want)
		}
	}
}

func containsLine(text, line string) bool {
	return slices.Contains(strings.Split(text, "\n"), line)
}
