//go:build !darwin && !linux

package bootclock

import (
	"errors"
	"time"
)

func Now() (time.Duration, error) {
	return 0, errors.New("sleep-counting clock requires darwin or linux")
}
