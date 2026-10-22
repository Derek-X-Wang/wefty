//go:build linux

package main

import (
	"os"
	"time"
)

func setupSystemBootTime() (time.Time, error) {
	payload, err := os.ReadFile("/proc/stat")
	if err != nil {
		return time.Time{}, err
	}
	return parseLinuxSetupBootTime(string(payload))
}
