package main

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

func parseLinuxSetupBootTime(payload string) (time.Time, error) {
	var boot time.Time
	for _, line := range strings.Split(payload, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "btime" {
			continue
		}
		if len(fields) != 2 || !boot.IsZero() {
			return time.Time{}, errors.New("invalid Linux boot time")
		}
		seconds, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil || seconds <= 0 {
			return time.Time{}, errors.New("invalid Linux boot time")
		}
		boot = time.Unix(seconds, 0)
	}
	if boot.IsZero() {
		return boot, errors.New("Linux boot time is unavailable")
	}
	return boot, nil
}
