//go:build linux

package ocihelper

// Disk detachment also runs directly during a sweep, without Delete. Retire
// the exact in-memory authority before closing its Seat pins. Slow Seat work
// is never drained here.
func (engine *ContainerdEngine) withSeatEndpointPinsClosed(authority AttemptAuthority, detach func() error) error {
	engine.mu.Lock()
	attempt := engine.attempts[authority.key()]
	engine.mu.Unlock()
	if attempt == nil {
		return detach()
	}
	attempt.mu.Lock()
	attempt.deleted = true
	seats := attempt.seats
	attempt.mu.Unlock()
	return seats.beforeDiskDetach(detach)
}

func (attempt *containerdAttempt) closeSeatEndpointPins() {
	attempt.mu.Lock()
	seats := attempt.seats
	attempt.mu.Unlock()
	seats.closeEndpointPins()
}
