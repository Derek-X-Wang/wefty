package agent

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
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
	closeRequests chan chan error
	done          chan struct{}
	errMu         sync.Mutex
	terminalErr   error
	closeOnce     sync.Once
	closeErr      error
}

func newBatchingLogSink(ctx context.Context, client *Client, claim l1.Claim, spool *logSpool, clock Clock, batchSize int, flushInterval, retryInterval time.Duration) (*batchingLogSink, error) {
	if err := spool.ensureAttempt(ctx, claim); err != nil {
		return nil, err
	}
	sink := &batchingLogSink{
		ctx: ctx, client: client, claim: claim, spool: spool, clock: clock,
		batchSize: batchSize, flushInterval: flushInterval, retryInterval: retryInterval,
		wake:          make(chan struct{}, 1),
		closeRequests: make(chan chan error),
		done:          make(chan struct{}),
	}
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
	sink.closeOnce.Do(func() {
		response := make(chan error, 1)
		select {
		case sink.closeRequests <- response:
			// Once the run loop receives the close request it always publishes
			// the flush result before exiting, so prefer that exact result over
			// racing the done channel.
			sink.closeErr = <-response
		case <-sink.done:
			sink.closeErr = sink.err()
		}
	})
	return sink.closeErr
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
		case response := <-sink.closeRequests:
			err := sink.uploadAvailable(true)
			if err != nil {
				sink.setError(err)
			}
			response <- err
			return
		case <-sink.ctx.Done():
			sink.setError(sink.ctx.Err())
			return
		}
	}
}

func (sink *batchingLogSink) uploadAvailable(all bool) error {
	for {
		events, err := sink.spool.pending(sink.ctx, sink.claim.Lease.AttemptID, sink.batchSize)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return nil
		}
		if err := sink.upload(events); err != nil {
			return err
		}
		if !all || len(events) < sink.batchSize {
			return nil
		}
	}
}

func (sink *batchingLogSink) upload(events []contract.LogEvent) error {
	if len(events) == 0 {
		return nil
	}
	batch := append([]contract.LogEvent(nil), events...)
	request := l1.AppendLogsRequest{FencingToken: sink.claim.Lease.FencingToken, Events: batch}
	for {
		response, err := sink.client.AppendLogs(sink.ctx, sink.claim.Job.JobID, sink.claim.Lease.AttemptID, request)
		if err == nil {
			if err := validateLogAcknowledgement(batch, response.Acknowledged); err != nil {
				return err
			}
			return sink.spool.acknowledge(sink.ctx, sink.claim.Lease.AttemptID, response.Acknowledged)
		}
		var protocolErr *ProtocolError
		if errors.As(err, &protocolErr) && protocolErr.StatusCode < http.StatusInternalServerError {
			return err
		}
		timer := sink.clock.NewTimer(sink.retryInterval)
		select {
		case <-sink.ctx.Done():
			stopTimer(timer)
			return sink.ctx.Err()
		case <-timer.C():
		}
	}
}

func validateLogAcknowledgement(events []contract.LogEvent, acknowledged map[contract.LogStream]uint64) error {
	highest := make(map[contract.LogStream]uint64, 2)
	seen := make(map[contract.LogStream]bool, 2)
	for _, event := range events {
		if !seen[event.Stream] || event.Sequence > highest[event.Stream] {
			highest[event.Stream] = event.Sequence
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

func retryableAgentProtocolError(err error) bool {
	var protocolErr *ProtocolError
	return !errors.As(err, &protocolErr) || protocolErr.StatusCode >= http.StatusInternalServerError
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

var _ processrunner.OutputSink = (*batchingLogSink)(nil)
var _ processrunner.OutputSink = multiOutputSink{}
