//go:build race

package agent

// raceTimeoutScale stretches a test's wall-clock hang guards under the race
// detector, which slows each L1 SQLite transaction several-fold (#662). Scale
// only liveness bounds, never a bound that is itself the property under test.
const raceTimeoutScale = 4
