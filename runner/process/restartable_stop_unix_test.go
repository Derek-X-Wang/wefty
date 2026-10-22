//go:build darwin || linux

package process

import (
	"context"
	"errors"
	"github.com/Derek-X-Wang/wefty/contract"
	"testing"
	"time"
)

func TestRestartableSupervisionStopNotifiesBeforeReaping(t *testing.T) {
	for _, guarded := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary process", true: "Guardian"}[guarded], func(t *testing.T) {
			sinkErr := errors.New("first sink failure")
			lateShutdown := errors.New("later shutdown")
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			observed := make(chan error, 2)
			runner := New(Config{GuardianExecutable: guardianAgentPath, TerminationGraceTime: 20 * time.Millisecond, ProcessReapTimeout: time.Second})
			_, err := runner.Run(ctx, Request{AttemptID: "attempt", Execution: helperExecution("hang-both-streams"), Guarded: guarded, StopInitiated: func(cause error) {
				observed <- cause
				cancel(lateShutdown)
			}}, OutputSinkFunc(func(context.Context, contract.LogEvent) error { return sinkErr }))
			if !errors.Is(err, sinkErr) {
				t.Fatalf("stop cause replaced: %v", err)
			}
			select {
			case cause := <-observed:
				if !errors.Is(cause, sinkErr) {
					t.Fatalf("first stop notification: %v", cause)
				}
			default:
				t.Fatal("supervision did not fence before reaping")
			}
		})
	}
}
