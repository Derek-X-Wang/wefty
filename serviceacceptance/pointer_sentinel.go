package serviceacceptance

func pointerHistoryHas(history [][2]int, x, y int) bool {
	for _, point := range history {
		if point[0] == x && point[1] == y {
			return true
		}
	}
	return false
}

func freshPointerSentinels(history [][2]int) ([2]int, [2]int, [2]int) {
	// Guest history is cumulative and already contains conformance-probe input.
	// Choose this proof's coordinates after reading that baseline so an older
	// control event cannot be attributed to the current view-only session.
	candidates := [...][2]int{{313, 257}, {677, 389}, {853, 521}, {419, 683}, {739, 227}, {541, 617}}
	fresh := make([][2]int, 0, 3)
	for _, point := range candidates {
		if !pointerHistoryHas(history, point[0], point[1]) {
			fresh = append(fresh, point)
			if len(fresh) == 3 {
				return fresh[0], fresh[1], fresh[2]
			}
		}
	}
	return [2]int{}, [2]int{}, [2]int{}
}
