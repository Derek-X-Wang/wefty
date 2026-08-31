package serviceacceptance

import "testing"

func TestFreshPointerSentinelsIgnorePriorConformanceInput(t *testing.T) {
	history := [][2]int{{211, 173}, {947, 611}, {313, 257}}
	freeView, heldView, control := freshPointerSentinels(history)
	if freeView != [2]int{677, 389} || heldView != [2]int{853, 521} || control != [2]int{419, 683} {
		t.Fatalf("fresh pointer sentinels = %v, %v, %v", freeView, heldView, control)
	}
}
