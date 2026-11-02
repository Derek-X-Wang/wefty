package bootclock

import "errors"

// ErrAwakeUnavailable means the platform cannot measure a sleep gap.
var ErrAwakeUnavailable = errors.New("pausing clock requires darwin or linux")
