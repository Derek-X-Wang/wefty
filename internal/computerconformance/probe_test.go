package computerconformance

import (
	"encoding/binary"
	"testing"
)

func TestRFBInputEventsFocusBeforeTyping(t *testing.T) {
	events := rfbInputEvents(true, 503, 389)
	if len(events) != 4 {
		t.Fatalf("events = %d, want pointer down/up then key down/up", len(events))
	}
	if events[0][0] != 5 || events[1][0] != 5 || events[2][0] != 4 || events[3][0] != 4 {
		t.Fatalf("event types = %d,%d,%d,%d, want 5,5,4,4", events[0][0], events[1][0], events[2][0], events[3][0])
	}
	if x, y := binary.BigEndian.Uint16(events[0][2:4]), binary.BigEndian.Uint16(events[0][4:6]); x != 503 || y != 389 {
		t.Fatalf("pointer = %d,%d, want 503,389", x, y)
	}
	if events[2][1] != 1 || events[3][1] != 0 || events[2][7] != 'w' || events[3][7] != 'w' {
		t.Fatalf("key events = %v %v, want w down/up", events[2], events[3])
	}
}

func TestHistoryContainsEarlierSentinel(t *testing.T) {
	observation := inputObservation{X: 211, Y: 173, PointerHistory: [][2]int{{0, 0}, {947, 611}, {211, 173}}}
	if !historyContains(observation, 947, 611) {
		t.Fatal("bounded compositor history lost an observed sentinel when a later event became current")
	}
}
