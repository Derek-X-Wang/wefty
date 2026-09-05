//go:build !darwin && !linux

package ocicontrol

func maximumUnixSocketPathBytes() int { return 0 }
