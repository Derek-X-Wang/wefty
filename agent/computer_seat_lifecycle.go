package agent

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

type seatComputerServiceConfig struct {
	router      *seatFrontDoor
	copyRuntime seatCopyRuntime
	copyControl seatCopyControl
	publish     func(context.Context, bool, string) error
}

// Publication precedes the controller's first live observation. The shared
// listener belongs to Agent.Run; this exact Attempt owns only its registration.
func runComputerSeats(ctx context.Context, runtime WorkloadRuntime, request workloadrunner.Request, sink workloadrunner.OutputSink, controller *computerSeatController, config seatComputerServiceConfig) (contract.ProcessResult, error) {
	if config.router == nil || config.publish == nil || config.copyRuntime == nil || config.copyControl == nil {
		return contract.ProcessResult{}, errors.New("Seat front-door dependencies are incomplete")
	}
	seatCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	mux := http.NewServeMux()
	mux.Handle("POST /v1/seats/{seat_id}/copy", newSeatCopyHandler(config.copyRuntime, config.copyControl, request.Authority, controller.computerID))
	registration, err := config.router.register(seatCtx, controller.computerID, controller.authority.AttemptID, mux)
	if err != nil {
		return contract.ProcessResult{}, err
	}
	defer registration.close()
	controller.onWanted = func(list l1.ComputerSeatList) error {
		ids := make([]string, 0, len(list.Seats))
		for _, seat := range list.Seats {
			ids = append(ids, seat.SeatID)
		}
		return registration.bind(ids)
	}
	var workers sync.WaitGroup
	var once sync.Once
	var startupErr error
	var publicationAttempted bool
	started := request.Started
	request.Started = func() {
		if started != nil {
			started()
		}
		once.Do(func() {
			workers.Add(1)
			go func() {
				defer workers.Done()
				if seatCtx.Err() != nil {
					return
				}
				publicationAttempted = true
				publishCtx, stop := context.WithTimeout(seatCtx, 5*time.Second)
				startupErr = config.publish(publishCtx, true, config.router.endpoint)
				stop()
				if startupErr != nil {
					cancel()
					return
				}
				if seatCtx.Err() == nil {
					controller.run(seatCtx)
				}
			}()
		})
	}
	result, runErr := runtime.Run(seatCtx, request, sink)
	cancel()
	workers.Wait()
	registration.close()
	if !publicationAttempted {
		return result.Outcome, errors.Join(runErr, startupErr)
	}
	withdrawCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	withdrawErr := config.publish(withdrawCtx, false, "")
	stop()
	return result.Outcome, errors.Join(runErr, startupErr, withdrawErr)
}
