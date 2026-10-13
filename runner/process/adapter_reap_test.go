//go:build darwin || linux

package process

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

func TestAdapterRefusesQuiescenceAfterRunnerLimitReapTimeout(t *testing.T) {
	executor, processRequest, clock, sink, start := stalledReapRun(t, nil)
	adapter := NewAdapter(executor)
	request := workloadrunner.Request{
		Authority: testAuthority("job-limit-reap", "attempt-limit-reap", "fence-limit-reap"),
		Execution: processRequest.Execution, IdlePolicy: IgnoreIdle, Started: processRequest.Started,
		Limits: &contract.JobLimits{MaxRuntimeSeconds: 10},
	}
	admission, _, err := adapter.Preflight(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer admission.Release()
	finished := make(chan runOutcome, 1)
	go func() {
		result, err := adapter.Run(context.Background(), admission.Request, sink)
		finished <- runOutcome{result: result.Outcome, err: err}
	}()
	start(0)
	clock.WaitForTimerCount(t, 1)
	clock.Advance(10 * time.Second)
	clock.WaitForTimerCount(t, 2)
	clock.Advance(2 * time.Second)
	outcome := awaitRun(t, finished)

	receipt, err := adapter.ReapAndVerify(context.Background(), workloadrunner.ReapRequest{Authority: request.Authority})
	if !errors.Is(err, ErrProcessReapTimeout) || receipt.RuntimeQuiesced {
		t.Errorf("ReapAndVerify receipt = %#v err=%v, want refused quiescence with reap timeout", receipt, err)
	}
	if !errors.Is(outcome.err, ErrProcessReapTimeout) || !errors.Is(outcome.err, ErrMaxRuntime) {
		t.Errorf("Adapter.Run error = %v, want max runtime and reap timeout", outcome.err)
	}
	assertReapTimeoutResult(t, outcome.result)
}
