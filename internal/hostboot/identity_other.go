//go:build !darwin && !linux

package hostboot

import "errors"

func Identity() (string, error) { return "", errors.New("host boot identity requires darwin or linux") }
