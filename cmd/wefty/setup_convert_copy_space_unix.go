//go:build darwin || linux

package main

import (
	"errors"
	"math"
	"os"

	"golang.org/x/sys/unix"
)

func conversionInventoryAvailableBytes(path string) (uint64, error) {
	directory, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer directory.Close()
	var stat unix.Statfs_t
	if err := unix.Fstatfs(int(directory.Fd()), &stat); err != nil {
		return 0, err
	}
	if stat.Bsize <= 0 {
		return 0, errors.New("invalid destination filesystem block size")
	}
	available, block := uint64(stat.Bavail), uint64(stat.Bsize)
	if available > math.MaxUint64/block {
		return math.MaxUint64, nil
	}
	return available * block, nil
}
