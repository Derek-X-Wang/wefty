package computerconformance

import (
	"encoding/binary"
	"testing"
)

func TestRFBInputEventsKeyboardBeforePointerFocus(t *testing.T) {
	events := rfbInputEvents(true, 503, 389)
	if len(events) != 4 {
		t.Fatalf("events = %d, want key down/up then pointer down/up", len(events))
	}
	if events[0][0] != 4 || events[1][0] != 4 || events[2][0] != 5 || events[3][0] != 5 {
		t.Fatalf("event types = %d,%d,%d,%d, want 4,4,5,5", events[0][0], events[1][0], events[2][0], events[3][0])
	}
	if x, y := binary.BigEndian.Uint16(events[2][2:4]), binary.BigEndian.Uint16(events[2][4:6]); x != 503 || y != 389 {
		t.Fatalf("pointer = %d,%d, want 503,389", x, y)
	}
	if events[0][1] != 1 || events[1][1] != 0 || events[0][7] != 'w' || events[1][7] != 'w' {
		t.Fatalf("key events = %v %v, want w down/up", events[0], events[1])
	}
}

func TestHistoryContainsEarlierSentinel(t *testing.T) {
	observation := inputObservation{X: 211, Y: 173, PointerHistory: [][2]int{{0, 0}, {947, 611}, {211, 173}}}
	if !historyContains(observation, 947, 611) {
		t.Fatal("bounded compositor history lost an observed sentinel when a later event became current")
	}
}
