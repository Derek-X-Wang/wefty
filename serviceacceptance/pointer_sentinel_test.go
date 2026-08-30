package serviceacceptance

import "testing"

func TestFreshPointerSentinelsIgnorePriorConformanceInput(t *testing.T) {
	history := [][2]int{{211, 173}, {947, 611}, {313, 257}}
	view, control := freshPointerSentinels(history)
	if view != [2]int{677, 389} || control != [2]int{853, 521} {
		t.Fatalf("fresh pointer sentinels = %v, %v", view, control)
	}
}
