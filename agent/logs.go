package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

// batchingLogSink decouples process-pipe reads from timed network uploads.
// Every WriteOutput is committed to the SQLite spool before it returns. One
// uploader goroutine drains those durable records, so a crash after acceptance
// can only cause an idempotent replay, never a missing event.
type batchingLogSink struct {
	ctx           context.Context
	client        *Client
	claim         l1.Claim
	spool         *logSpool
	clock         Clock
	batchSize     int
	flushInterval time.Duration
	retryInterval time.Duration

	wake          chan struct{}
	closeRequests chan logSinkCloseRequest
	done          chan struct{}
	errMu         sync.Mutex
	terminalErr   error
	closeOnce     sync.Once
	closeErr      error
	pendingEvents atomic.Int64
}

type logSinkCloseRequest struct {
	context  context.Context
	response chan error
}

type logFinalizationStage string

const (
	logFinalizationStageRedaction logFinalizationStage = "redaction flush"
	logFinalizationStageUpload    logFinalizationStage = "log upload"
)

// logFinalizationDeadlineError marks only the agent-owned bounded
// finalization deadline. An HTTP client's independent operation deadline is
// deliberately left as a normal output failure.
type logFinalizationDeadlineError struct {
	stage logFinalizationStage
	cause error
}

func (err *logFinalizationDeadlineError) Error() string {
	return fmt.Sprintf("%s exceeded the bounded finalization deadline: %v", err.stage, err.cause)
}

func (err *logFinalizationDeadlineError) Unwrap() error { return err.cause }

func markLogFinalizationDeadline(ctx context.Context, stage logFinalizationStage, err error) error {
	if err == nil {
		return nil
	}
	// Require the finalization context itself to have expired and the stage to
	// have returned that exact cause. This intentionally does not match an
	// arbitrary wrapped/joined DeadlineExceeded from a destination.
	if cause := context.Cause(ctx); cause == context.DeadlineExceeded && err == cause {
		return &logFinalizationDeadlineError{stage: stage, cause: cause}
	}
	return err
}

func newBatchingLogSink(ctx context.Context, client *Client, claim l1.Claim, spool *logSpool, clock Clock, batchSize int, flushInterval, retryInterval time.Duration) (*batchingLogSink, error) {
	if err := spool.ensureAttempt(ctx, claim); err != nil {
		return nil, err
	}
	pending, err := spool.pendingCount(ctx, claim.Lease.AttemptID)
	if err != nil {
		return nil, err
	}
	sink := &batchingLogSink{
		ctx: ctx, client: client, claim: claim, spool: spool, clock: clock,
		batchSize: batchSize, flushInterval: flushInterval, retryInterval: retryInterval,
		wake:          make(chan struct{}, 1),
		closeRequests: make(chan logSinkCloseRequest),
		done:          make(chan struct{}),
	}
	sink.pendingEvents.Store(int64(pending))
	go sink.run()
	return sink, nil
}

func (sink *batchingLogSink) WriteOutput(ctx context.Context, event contract.LogEvent) error {
	select {
	case <-sink.done:
		return sink.err()
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if err := sink.spool.append(ctx, event); err != nil {
		return err
	}
	sink.pendingEvents.Add(1)
	pending, err := sink.spool.pendingCount(ctx, event.AttemptID)
	if err != nil {
		return err
	}
	if pending < sink.batchSize {
		return nil
	}
	select {
	case sink.wake <- struct{}{}:
	default:
	}
	return nil
}

func (sink *batchingLogSink) Close() error {
	return sink.CloseContext(sink.ctx)
}

// CloseContext flushes every pending event under the caller's finalization
// bound. Periodic uploads use the attempt-long sink context instead.
func (sink *batchingLogSink) CloseContext(ctx context.Context) error {
	sink.closeOnce.Do(func() {
		response := make(chan error, 1)
		select {
		case sink.closeRequests <- logSinkCloseRequest{context: ctx, response: response}:
			// Once the run loop receives the close request it always publishes
			// the flush result before exiting, so prefer that exact result over
			// racing the done channel.
			sink.closeErr = sink.classifyCloseError(ctx, <-response)
		case <-ctx.Done():
			sink.closeErr = sink.classifyCloseError(ctx, context.Cause(ctx))
		case <-sink.done:
			sink.closeErr = sink.classifyCloseError(ctx, sink.err())
		}
	})
	return sink.closeErr
}

func (sink *batchingLogSink) classifyCloseError(ctx context.Context, err error) error {
	if context.Cause(ctx) == context.DeadlineExceeded && errors.Is(err, context.DeadlineExceeded) && sink.pendingEvents.Load() == 0 {
		return nil
	}
	return markLogFinalizationDeadline(ctx, logFinalizationStageUpload, err)
}

func (sink *batchingLogSink) run() {
	flushTimer := sink.clock.NewTimer(sink.flushInterval)
	defer stopTimer(flushTimer)
	defer close(sink.done)

	for {
		select {
		case <-sink.wake:
			if err := sink.uploadAvailable(false); err != nil {
				sink.setError(err)
				return
			}
		case <-flushTimer.C():
			if err := sink.uploadAvailable(false); err != nil {
				sink.setError(err)
				return
			}
			flushTimer.Reset(sink.flushInterval)
		case request := <-sink.closeRequests:
			err := sink.uploadAvailableContext(request.context, true)
			if err != nil {
				sink.setError(err)
			}
			request.response <- err
			return
		case <-sink.ctx.Done():
			sink.setError(context.Cause(sink.ctx))
			return
		}
	}
}

func (sink *batchingLogSink) uploadAvailable(all bool) error {
	return sink.uploadAvailableContext(sink.ctx, all)
}

func (sink *batchingLogSink) uploadAvailableContext(ctx context.Context, all bool) error {
	for {
		events, err := sink.spool.pending(ctx, sink.claim.Lease.AttemptID, sink.batchSize)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return nil
		}
		if err := sink.uploadContext(ctx, events); err != nil {
			return err
		}
		if !all || len(events) < sink.batchSize {
			return nil
		}
	}
}

func (sink *batchingLogSink) upload(events []contract.LogEvent) error {
	return sink.uploadContext(sink.ctx, events)
}

func (sink *batchingLogSink) uploadContext(ctx context.Context, events []contract.LogEvent) error {
	if len(events) == 0 {
		return nil
	}
	batch := append([]contract.LogEvent(nil), events...)
	request := l1.AppendLogsRequest{FencingToken: sink.claim.Lease.FencingToken, Events: batch}
	for {
		response, err := sink.client.AppendLogs(ctx, sink.claim.Job.JobID, sink.claim.Lease.AttemptID, request)
		if err == nil {
			if err := validateLogAcknowledgement(batch, response.Acknowledged); err != nil {
				return err
			}
			if err := sink.spool.acknowledge(ctx, sink.claim.Lease.AttemptID, response.Acknowledged); err != nil {
				return err
			}
			sink.pendingEvents.Add(-int64(len(batch)))
			return nil
		}
		if classifyAgentProtocolError(err).destination != errorDestinationTransient {
			return err
		}
		timer := sink.clock.NewTimer(sink.retryInterval)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return context.Cause(ctx)
		case <-timer.C():
		}
	}
}

func validateLogAcknowledgement(events []contract.LogEvent, acknowledged map[contract.LogStream]uint64) error {
	highest := make(map[contract.LogStream]uint64, 2)
	seen := make(map[contract.LogStream]bool, 2)
	for _, event := range events {
		endSequence := eventEndSequence(event)
		if !seen[event.Stream] || endSequence > highest[event.Stream] {
			highest[event.Stream] = endSequence
			seen[event.Stream] = true
		}
	}
	for stream, sequence := range highest {
		accepted, ok := acknowledged[stream]
		if !ok || accepted < sequence {
			return fmt.Errorf("log upload acknowledgement for %s is %d (present=%t), want at least %d", stream, accepted, ok, sequence)
		}
	}
	return nil
}

func eventEndSequence(event contract.LogEvent) uint64 {
	if event.Gap != nil {
		return event.Gap.ThroughSequence
	}
	return event.Sequence
}

func (sink *batchingLogSink) setError(err error) {
	sink.errMu.Lock()
	if sink.terminalErr == nil {
		sink.terminalErr = err
	}
	sink.errMu.Unlock()
}

func (sink *batchingLogSink) err() error {
	sink.errMu.Lock()
	defer sink.errMu.Unlock()
	if sink.terminalErr == nil {
		return errors.New("log uploader stopped")
	}
	return sink.terminalErr
}

type multiOutputSink []processrunner.OutputSink

func (sinks multiOutputSink) WriteOutput(ctx context.Context, event contract.LogEvent) error {
	for _, sink := range sinks {
		if err := sink.WriteOutput(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

type bestEffortOutputSink struct {
	sink   processrunner.OutputSink
	report func(error)
}

func (sink bestEffortOutputSink) WriteOutput(ctx context.Context, event contract.LogEvent) error {
	if err := sink.sink.WriteOutput(ctx, event); err != nil && sink.report != nil {
		sink.report(err)
	}
	return nil
}

var _ processrunner.OutputSink = (*batchingLogSink)(nil)
var _ processrunner.OutputSink = multiOutputSink{}
var _ processrunner.OutputSink = bestEffortOutputSink{}
