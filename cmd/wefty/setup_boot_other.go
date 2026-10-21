//go:build !darwin

package main

import (
	"errors"
	"time"
)

func setupSystemBootTime() (time.Time, error) {
	return time.Time{}, errors.New("fleet setup boot-time check requires macOS")
}
