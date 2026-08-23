package agent

import (
	"context"
	"errors"
	"sync"
	"time"
)

var errAuthorityDeadlineExceeded = errors.New("local attempt authority deadline expired")

const defaultSuspendCheckInterval = time.Second

type localAuthority struct {
	deadline time.Time
}

// authorityWatchdog owns an independent timer for one attempt. Renewal code
// may update its deadline, but it cannot prevent the timer from canceling a
// payload when an RPC is silent.
type authorityWatchdog struct {
	clock         Clock
	checkInterval time.Duration
}

func newAuthorityWatchdog(clock Clock) *authorityWatchdog {
	return &authorityWatchdog{clock: clock, checkInterval: defaultSuspendCheckInterval}
}

func (watchdog *authorityWatchdog) Start(ctx context.Context, authority localAuthority, cancel context.CancelCauseFunc) attemptWatch {
	watchContext, stop := context.WithCancel(ctx)
	watch := &authorityWatch{
		clock: watchdog.clock, checkInterval: watchdog.checkInterval,
		cancelPayload: cancel, stop: stop, updates: make(chan struct{}, 1),
		failures: make(chan error, 1), authority: authority,
	}
	now := watchdog.clock.Now()
	watch.checkpoint = now
	watch.wallCheckpoint = wallNow(watchdog.clock)
	go watch.run(watchContext)
	return watch
}

type authorityWatch struct {
	mu             sync.Mutex
	clock          Clock
	checkInterval  time.Duration
	cancelPayload  context.CancelCauseFunc
	stop           context.CancelFunc
	updates        chan struct{}
	failures       chan error
	authority      localAuthority
	checkpoint     time.Time
	wallCheckpoint time.Time
	failOnce       sync.Once
}

func (watch *authorityWatch) Renewed(authority localAuthority) {
	watch.mu.Lock()
	watch.authority = authority
	watch.checkpoint = watch.clock.Now()
	watch.wallCheckpoint = wallNow(watch.clock)
	watch.mu.Unlock()
	select {
	case watch.updates <- struct{}{}:
	default:
	}
}

func (watch *authorityWatch) Failures() <-chan error { return watch.failures }

func (watch *authorityWatch) Stop() { watch.stop() }

// Check is called immediately before renewal. It shares the same suspend-gap
// test as the independent timer, so a wake cannot race directly into an RPC.
func (watch *authorityWatch) Check() error {
	watch.mu.Lock()
	defer watch.mu.Unlock()
	err := watch.checkLocked(watch.clock.Now(), wallNow(watch.clock))
	if err != nil {
		watch.fail(err)
	}
	return err
}

func (watch *authorityWatch) run(ctx context.Context) {
	for {
		watch.mu.Lock()
		now := watch.clock.Now()
		if err := watch.checkLocked(now, wallNow(watch.clock)); err != nil {
			watch.mu.Unlock()
			watch.fail(err)
			return
		}
		remaining := watch.authority.deadline.Sub(now)
		delay := min(remaining, watch.checkInterval)
		watch.mu.Unlock()

		timer := watch.clock.NewTimer(delay)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return
		case <-watch.updates:
			stopTimer(timer)
		case <-timer.C():
		}
	}
}

func (watch *authorityWatch) checkLocked(now, wall time.Time) error {
	remainingAtCheckpoint := watch.authority.deadline.Sub(watch.checkpoint)
	wallGap := wall.Sub(watch.wallCheckpoint)
	if !now.Before(watch.authority.deadline) || remainingAtCheckpoint <= 0 || wallGap >= remainingAtCheckpoint {
		return errAuthorityDeadlineExceeded
	}
	watch.checkpoint = now
	watch.wallCheckpoint = wall
	return nil
}

func (watch *authorityWatch) fail(err error) {
	watch.failOnce.Do(func() {
		watch.cancelPayload(err)
		watch.failures <- err
	})
}
