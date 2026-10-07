//go:build !race

package agent

// raceTimeoutScale leaves hang guards unscaled without the race detector. See
// race_enabled_test.go.
const raceTimeoutScale = 1
