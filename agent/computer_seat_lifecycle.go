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
	screen      *seatScreenConfig
	copyRuntime seatCopyRuntime
	copyControl seatCopyControl
	publish     func(context.Context, bool, string) error
	clock       Clock
}

// Publication precedes the controller's first live observation. The shared
// listener belongs to Agent.Run; this exact Attempt owns only its registration.
func runComputerSeats(ctx context.Context, runtime WorkloadRuntime, request workloadrunner.Request, sink workloadrunner.OutputSink, controller *computerSeatController, config seatComputerServiceConfig) (contract.ProcessResult, error) {
	if config.router == nil || config.publish == nil || config.copyRuntime == nil || config.copyControl == nil {
		return contract.ProcessResult{}, errors.New("Seat front-door dependencies are incomplete")
	}
	if config.clock == nil {
		config.clock = systemClock{}
	}
	var nextFailureLog time.Time
	var failureLogMu sync.Mutex
	logFailure := func(err error) {
		now := config.clock.Now()
		failureLogMu.Lock()
		due := controller.logf != nil && (nextFailureLog.IsZero() || !now.Before(nextFailureLog))
		if due {
			nextFailureLog = now.Add(time.Minute)
		}
		failureLogMu.Unlock()
		if due {
			controller.logf("Seat front-door publication failed computer=%q error=%v", controller.computerID, err)
		}
	}
	seatCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	mux := http.NewServeMux()
	if config.screen != nil {
		screenConfig := *config.screen
		screenConfig.ctx = seatCtx
		screenConfig.fatal = func(err error) { cancel(); logFailure(err) }
		// Every screen report must remain visible, including local refusals.
		// Publication retries keep their separate rate-limited logger above.
		screenConfig.logFailure = func(err error) {
			if controller.logf != nil {
				controller.logf("Seat screen failed: %v", err)
			}
		}
		screens, err := newSeatScreenHandler(screenConfig)
		if err != nil {
			return contract.ProcessResult{}, err
		}
		mux.Handle("GET /v1/seats/{seat_id}/screen/{mode}", screens)
		mux.Handle("/v1/seats/{seat_id}/person/", screens)
	}
	mux.Handle("POST /v1/seats/{seat_id}/copy", newSeatCopyHandlerWithClock(config.copyRuntime, config.copyControl, request.Authority, controller.computerID, config.clock))
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
				startupErr = retrySeatPublication(seatCtx, config.clock, func(callCtx context.Context) error {
					return registration.publish(callCtx, true, config.router.endpoint, config.publish)
				}, logFailure)
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
	if !publicationAttempted {
		return result.Outcome, errors.Join(runErr, startupErr)
	}
	withdrawCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), DefaultPublicationRecoveryWindow)
	withdrawErr := retrySeatPublication(withdrawCtx, config.clock, func(callCtx context.Context) error { return registration.publish(callCtx, false, "", config.publish) }, logFailure)
	if errors.Is(withdrawErr, errStaleSeatPublication) {
		withdrawErr = nil
	}
	stop()
	return result.Outcome, errors.Join(runErr, startupErr, withdrawErr)
}

func retrySeatPublication(ctx context.Context, clock Clock, send func(context.Context) error, logFailure func(error)) error {
	delay := DefaultPublicationRetryInterval
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		callCtx, stop := context.WithTimeout(ctx, 5*time.Second)
		err := send(callCtx)
		stop()
		if err == nil {
			return nil
		}
		logFailure(err)
		var protocol *ProtocolError
		if errors.Is(err, errStaleSeatPublication) || classifyAgentProtocolError(err).destination == errorDestinationAttemptAuthority || errors.As(err, &protocol) && protocol.StatusCode >= 400 && protocol.StatusCode < 500 && !protocol.APIError.Retryable {
			return err
		}
		timer := clock.NewTimer(delay)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return ctx.Err()
		case <-timer.C():
		}
		stopTimer(timer)
		delay = min(delay*2, time.Second)
	}
}
