//go:build race

package main

// raceEnabled marks a race-detector build. The detector slows the CLI and the
// fake RFB door several-fold, so a timing test widens its margin under -race
// (#1688). The plain build keeps every bound at its product value; that build
// is the timing evidence.
const raceEnabled = true

// raceTimingScale stretches a test's timing margin under the race detector.
const raceTimingScale = 4
