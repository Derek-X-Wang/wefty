//go:build darwin || linux

package agent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// Real HTTP, SQLite and socket delivery get the production finalization bound
// as scheduling headroom. Domain clocks are driven separately; this is only a
// hang guard, scaled for SQLite instrumentation under -race.
const hostedFixtureTimeout = DefaultFinalizationTimeout * raceTimeoutScale

func awaitFixtureCondition(parent context.Context, phase string, ready func() bool) error {
	started := time.Now()
	ctx, cancel := context.WithTimeout(parent, hostedFixtureTimeout)
	defer cancel()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for {
		if ready() {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("phase=%s elapsed=%s: %w", phase, time.Since(started), ctx.Err())
		case <-poll.C:
		}
	}
}

func (clock *manualClock) hasDeadline(deadline time.Time) bool {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	for _, timer := range clock.timers {
		if timer.active && timer.deadline.Equal(deadline) {
			return true
		}
	}
	return false
}

// Adapt the agent's manual clock to the helper's absolute timer interface.
// Agent and helper clocks remain independent so the test can prove that an
// acknowledged renewal extends the helper deadline beyond its original edge.
type hostedHelperClock struct{ *manualClock }

func (clock hostedHelperClock) NewTimerAt(deadline time.Time) ocihelper.Timer {
	clock.mu.Lock()
	timer := &manualTimer{clock: clock.manualClock, channel: make(chan time.Time, 1), deadline: deadline, active: deadline.After(clock.now)}
	clock.timers = append(clock.timers, timer)
	if !timer.active {
		timer.channel <- clock.now
	}
	clock.mu.Unlock()
	return hostedHelperTimer{timer}
}

type hostedHelperTimer struct{ *manualTimer }

func (timer hostedHelperTimer) ResetAt(deadline time.Time) bool {
	timer.clock.mu.Lock()
	defer timer.clock.mu.Unlock()
	active := timer.active
	timer.deadline = deadline
	timer.active = deadline.After(timer.clock.now)
	select {
	case <-timer.channel:
	default:
	}
	if !timer.active {
		timer.channel <- timer.clock.now
	}
	return active
}

// Any accidental child TestMain rebuild fails immediately, so these cleanup
// regressions prove helper reuse as well as release/join ordering.
func hostedFixtureChildEnvironment(t *testing.T) []string {
	t.Helper()
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "go"), []byte("#!/bin/sh\necho unexpected-Go-rebuild-in-child >&2\nexit 97\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return append(os.Environ(), "WEFTY_AGENT_TEST_BINARIES="+filepath.Dir(agentHelperPath), "PATH="+directory+string(os.PathListSeparator)+os.Getenv("PATH"))
}
