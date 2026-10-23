package agent

import (
	"context"
	"sync"

	"github.com/Derek-X-Wang/wefty/contract"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

// Seat-request Computers use per-Seat readiness instead of the legacy
// Computer-wide front door. Endpoint transport is a separate capability.
func runComputerSeats(ctx context.Context, runtime WorkloadRuntime, request workloadrunner.Request, sink workloadrunner.OutputSink, controller *computerSeatController) (contract.ProcessResult, error) {
	seatCtx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	var once sync.Once
	started := request.Started
	request.Started = func() {
		if started != nil {
			started()
		}
		once.Do(func() {
			workers.Add(1)
			go func() {
				defer workers.Done()
				controller.run(seatCtx)
			}()
		})
	}
	defer func() { cancel(); workers.Wait() }()
	result, err := runtime.Run(ctx, request, sink)
	return result.Outcome, err
}
