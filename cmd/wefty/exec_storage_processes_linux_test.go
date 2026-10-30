package main

import (
	"fmt"
	"os"
	"time"
)

func execStorageChildGone(pid int) bool {
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Readlink(fmt.Sprintf("/proc/%d/cwd", pid)); os.IsNotExist(err) {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}
