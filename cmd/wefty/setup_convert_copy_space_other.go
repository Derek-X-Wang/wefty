//go:build !darwin && !linux

package main

import "errors"

func conversionInventoryAvailableBytes(string) (uint64, error) {
	return 0, errors.New("conversion copy space inspection is unavailable on this platform")
}
