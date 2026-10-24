package main

type setupLockState uint8

const (
	setupLockUnknown setupLockState = iota
	setupLockFree
	setupLockHeldState
)

type setupLockObservation struct {
	state setupLockState
	cause error
}
