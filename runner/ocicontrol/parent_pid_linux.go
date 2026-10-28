//go:build linux

package ocicontrol

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func readParentPID(pid int) (int, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	// The comm field may contain spaces or ')'; ppid follows its final ')'.
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		return 0, errPeerPIDUnavailable
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) < 2 {
		return 0, errPeerPIDUnavailable
	}
	return strconv.Atoi(fields[1])
}
