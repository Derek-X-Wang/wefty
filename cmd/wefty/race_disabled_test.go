//go:build !race

package main

// raceEnabled and raceTimingScale leave timing bounds at their product values
// without the race detector. See race_enabled_test.go.
const raceEnabled = false

const raceTimingScale = 1
