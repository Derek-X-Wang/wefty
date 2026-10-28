//go:build !darwin && !linux

package ocicontrol

func readParentPID(int) (int, error) { return 0, errPeerPIDUnavailable }
