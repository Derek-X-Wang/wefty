package ocihelper

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

const (
	computerMemoryFreezeTimeout = 2 * time.Second
	computerMemoryThawBudget    = 500 * time.Millisecond
	computerMemoryThawAttempts  = 3
)

type computerMemoryFreezerIO struct {
	readRequested func() (bool, error)
	readEvents    func() ([]byte, error)
	write         func(bool) error
	log           func(string)
}

func (io computerMemoryFreezerIO) record(event string) {
	if io.log != nil {
		io.log(event)
	}
}
func computerMemoryFrozen(data []byte) (bool, error) {
	value := ""
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[0] == "frozen" {
			if len(fields) != 2 || value != "" || (fields[1] != "0" && fields[1] != "1") {
				return false, errors.New("invalid cgroup freezer evidence")
			}
			value = fields[1]
		}
	}
	if value == "" {
		return false, errors.New("missing cgroup freezer evidence")
	}
	return value == "1", nil
}
func waitComputerMemoryFrozen(ctx context.Context, clock Clock, deadline time.Time, want bool, read func() ([]byte, error)) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := read()
		if err != nil {
			return err
		}
		frozen, err := computerMemoryFrozen(data)
		if err != nil {
			return err
		}
		if !clock.Now().Before(deadline) {
			return &SeatOperationRefusalError{Reason: SeatRefusalMemoryBusy}
		}
		if frozen == want {
			return nil
		}
		next := clock.Now().Add(10 * time.Millisecond)
		if next.After(deadline) {
			next = deadline
		}
		timer := clock.NewTimerAt(next)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C():
			timer.Stop()
		}
	}
}

// Thaw does not inherit request cancellation. The caller reserves this budget
// inside the whole freeze deadline. Every attempt writes 0 and proves both
// requested and effective freezer state; an errno alone is not the outcome.
func thawComputerMemory(clock Clock, deadline time.Time, io computerMemoryFreezerIO) error {
	for attempt := 0; attempt < computerMemoryThawAttempts; attempt++ {
		_ = io.write(false)
		requested, err := io.readRequested()
		if err == nil && !requested {
			until := clock.Now().Add(100 * time.Millisecond)
			if until.After(deadline) {
				until = deadline
			}
			if err = waitComputerMemoryFrozen(context.Background(), clock, until, false, io.readEvents); err == nil {
				return nil
			}
		}
	}
	io.record("computer_memory_thaw_failed")
	return &SeatOperationRefusalError{Reason: SeatRefusalMemoryThawFailed}
}
func recoverComputerMemoryFreeze(ctx context.Context, clock Clock, io computerMemoryFreezerIO) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	requested, err := io.readRequested()
	if err != nil {
		return false, err
	}
	frozen := requested
	if !requested {
		data, err := io.readEvents()
		if err != nil {
			return false, err
		}
		frozen, err = computerMemoryFrozen(data)
		if err != nil {
			return false, err
		}
	}
	if !frozen {
		return false, nil
	}
	io.record("computer_memory_freeze_abandoned")
	if err := thawComputerMemory(clock, clock.Now().Add(computerMemoryThawBudget), io); err != nil {
		return false, err
	}
	io.record("computer_memory_freeze_recovered")
	return true, nil
}

type computerMemoryFreezeGuard struct {
	mu                     sync.Mutex // serializes nonblocking writes with thaw; never containerd I/O
	once                   sync.Once
	done, stop             chan struct{}
	ctx                    context.Context
	cancel                 context.CancelFunc
	clock                  Clock
	io                     computerMemoryFreezerIO
	deadline, workDeadline time.Time
	timer                  Timer
	thawErr                error
}

func startComputerMemoryFreeze(ctx context.Context, clock Clock, io computerMemoryFreezerIO) (*computerMemoryFreezeGuard, error) {
	start := clock.Now()
	work, cancel := context.WithCancel(ctx)
	guard := &computerMemoryFreezeGuard{done: make(chan struct{}), stop: make(chan struct{}), ctx: work, cancel: cancel, clock: clock, io: io, deadline: start.Add(computerMemoryFreezeTimeout), workDeadline: start.Add(computerMemoryFreezeTimeout - computerMemoryThawBudget)}
	guard.timer = clock.NewTimerAt(guard.workDeadline)
	// A blocked cgroup read cannot keep the Computer frozen. The watchdog begins
	// thaw at 1.5 s, leaving 0.5 s for up to three attempts before the 2 s bound.
	go func() {
		select {
		case <-guard.timer.C():
			guard.finish()
		case <-ctx.Done():
			guard.finish()
		case <-guard.stop:
		}
	}()
	guard.mu.Lock()
	err := guard.check()
	if err == nil {
		err = io.write(true)
	}
	guard.mu.Unlock()
	if err == nil {
		err = waitComputerMemoryFrozen(work, clock, guard.workDeadline, true, io.readEvents)
	}
	if err != nil {
		// The watchdog cancels its internal wait to thaw on time. That expiry is
		// retryable backpressure, distinct from cancellation of the RPC itself.
		if errors.Is(err, context.Canceled) && ctx.Err() == nil && !clock.Now().Before(guard.workDeadline) {
			err = &SeatOperationRefusalError{Reason: SeatRefusalMemoryBusy}
		}
		if thawErr := guard.finish(); thawErr != nil {
			return nil, thawErr
		}
		return nil, err
	}
	return guard, nil
}
func (g *computerMemoryFreezeGuard) check() error {
	if !g.clock.Now().Before(g.workDeadline) {
		return &SeatOperationRefusalError{Reason: SeatRefusalMemoryBusy}
	}
	return g.ctx.Err()
}
func (g *computerMemoryFreezeGuard) writeLimit(write func() error) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.check(); err != nil {
		return err
	}
	return write()
}
func (g *computerMemoryFreezeGuard) finish() error {
	g.once.Do(func() {
		g.cancel()
		g.mu.Lock()
		g.thawErr = thawComputerMemory(g.clock, g.deadline, g.io)
		g.mu.Unlock()
		g.timer.Stop()
		close(g.stop)
		close(g.done)
	})
	<-g.done
	return g.thawErr
}

type computerMemoryShrinkResult struct{ limit, usage int64 }

func shrinkComputerMemory(ctx context.Context, clock Clock, io computerMemoryFreezerIO, readUsage func() (int64, error), readLimit func() (int64, error), writeLimit func(int64) error, target int64) (result computerMemoryShrinkResult, err error) {
	guard, err := startComputerMemoryFreeze(ctx, clock, io)
	if err != nil {
		return result, err
	}
	defer func() {
		if thawErr := guard.finish(); thawErr != nil {
			err = thawErr
		}
	}()
	result.usage, err = readUsage()
	if err != nil {
		return result, err
	}
	if err = guard.check(); err != nil {
		return result, err
	}
	if result.usage < 0 {
		return result, errors.New("negative memory usage")
	}
	var writeErr error
	if result.usage <= target {
		writeErr = guard.writeLimit(func() error { return writeLimit(target) })
	}
	if err = guard.check(); err != nil {
		return result, err
	}
	result.limit, err = readLimit()
	if err != nil {
		return result, err
	}
	if err = guard.check(); err != nil {
		return result, err
	}
	if result.usage <= target && result.limit != target {
		return result, errors.New("memory shrink could not be confirmed; room remains held")
	}
	// Exact readback is authoritative even if the write reported an error.
	_ = writeErr
	return result, nil
}
