package agent

import (
	"time"

	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

// ShutdownTimeouts are the timeouts that bound a forced shutdown. A zero
// field means the default the node agent uses.
type ShutdownTimeouts struct {
	TerminationGrace time.Duration
	ProcessReap      time.Duration
	Finalization     time.Duration
	Operation        time.Duration
}

// ForcedShutdownBound is the longest the node agent takes, once its Run
// context is cancelled, to end its resident process attempts in order. One
// attempt's phases run one after another: the process runner's termination
// grace and its reap after SIGKILL; the final log flush, which runs to the
// finalization timeout and can overrun it by the one request in flight, one
// L1 operation timeout; then the completion (or its evidence recovery) and
// the result upload, one operation timeout each. Attempts end concurrently,
// so the slowest one bounds the agent. A service manager that kills the agent
// a fixed time after SIGTERM must allow at least its shutdown budget plus
// this.
func ForcedShutdownBound(timeouts ShutdownTimeouts) time.Duration {
	return durationOrDefault(timeouts.TerminationGrace, processrunner.DefaultTerminationGraceTime) +
		durationOrDefault(timeouts.ProcessReap, processrunner.DefaultProcessReapTimeout) +
		durationOrDefault(timeouts.Finalization, DefaultFinalizationTimeout) +
		3*durationOrDefault(timeouts.Operation, DefaultOperationTimeout)
}
