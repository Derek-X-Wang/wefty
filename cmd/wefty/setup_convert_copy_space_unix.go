//go:build darwin || linux

package main

import "github.com/Derek-X-Wang/wefty/internal/hostspace"

func conversionInventoryAvailableBytes(path string) (uint64, error) {
	return hostspace.AvailableBytes(path)
}
