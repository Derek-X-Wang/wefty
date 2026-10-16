//go:build !darwin && !linux

package nodeload

import "errors"

func Read() (float64, error) { return 0, errors.New("machine load is unavailable on this platform") }
