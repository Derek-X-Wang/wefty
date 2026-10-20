//go:build !darwin && !linux

package main

import "errors"

func lockSetup(setupPaths, bool) (func(), error) {
	return nil, errors.New("setup and upgrade require macOS")
}

func setupLockHeld(setupPaths) bool { return false }
