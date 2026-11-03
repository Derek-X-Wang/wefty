package agent

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/hostboot"
)

var errAuthorityDeadlineExceeded = errors.New("local attempt authority deadline expired")

const defaultSuspendCheckInterval = time.Second

type localAuthority struct {
	deadline          time.Time
	sleepCreditBoot   string
	requestCheckpoint *authorityCheckpoint
}

// A grant is bounded from request start, including gaps before its reply.
// This checkpoint is local metadata; it is never encoded on the wire.
type authorityCheckpoint struct {
	now, wall time.Time
	sleep     *authoritySleepSample
}

type claimCheckpointKey struct{}

func captureAuthorityCheckpoint(clock Clock) authorityCheckpoint {
	return authorityCheckpoint{now: clock.Now(), wall: wallNow(clock), sleep: sampleAuthoritySleep(clock)}
}

func leaseAuthority(leaseTTL time.Duration, sleepCreditBoot string, checkpoint authorityCheckpoint) localAuthority {
	return localAuthority{deadline: checkpoint.now.Add(leaseTTL), sleepCreditBoot: sleepCreditBoot, requestCheckpoint: &checkpoint}
}

type sleepClockSource interface {
	BootElapsed() (time.Duration, error)
	AwakeElapsed() (time.Duration, error)
}

// Bracket the sleep-counting read with pausing-clock reads. The true offset
// lies in [lower, upper], including time spent descheduled between reads.
type authoritySleepSample struct{ boot, lower, upper time.Duration }

func sampleAuthoritySleep(clock Clock) *authoritySleepSample {
	source, ok := clock.(sleepClockSource)
	if !ok {
		return nil
	}
	before, beforeErr := source.AwakeElapsed()
	boot, bootErr := source.BootElapsed()
	after, afterErr := source.AwakeElapsed()
	if beforeErr != nil || bootErr != nil || afterErr != nil || after < before {
		return nil
	}
	return &authoritySleepSample{boot: boot, lower: boot - after, upper: boot - before}
}

// authorityWatchdog owns an independent timer for one attempt. Renewal code
// may update its deadline, but it cannot prevent the timer from canceling a
// payload when an RPC is silent.
type authorityWatchdog struct {
	clock         Clock
	checkInterval time.Duration
	bootDigest    string
}

func newAuthorityWatchdog(clock Clock) *authorityWatchdog {
	watchdog := &authorityWatchdog{clock: clock, checkInterval: defaultSuspendCheckInterval}
	if source, ok := clock.(interface{ BootIdentity() (string, error) }); ok {
		if boot, err := source.BootIdentity(); err == nil {
			watchdog.bootDigest = hostboot.DigestIdentity(boot)
		}
	}
	return watchdog
}

func (watchdog *authorityWatchdog) Start(ctx context.Context, authority localAuthority, cancel context.CancelCauseFunc) attemptWatch {
	watchContext, stop := context.WithCancel(ctx)
	watch := &authorityWatch{
		clock: watchdog.clock, checkInterval: watchdog.checkInterval,
		cancelPayload: cancel, stop: stop, updates: make(chan struct{}, 1),
		failures: make(chan error, 1), authority: authority,
		wakeRenewals: make(chan struct{}, 1), bootDigest: watchdog.bootDigest,
	}
	watch.setCheckpoint(authority)
	// Validate claim admission synchronously: an expired reply cannot race
	// the independent timer into starting a payload.
	if watch.Check() != nil {
		return watch
	}
	go watch.run(watchContext)
	return watch
}

type authorityWatch struct {
	mu              sync.Mutex
	clock           Clock
	checkInterval   time.Duration
	cancelPayload   context.CancelCauseFunc
	stop            context.CancelFunc
	updates         chan struct{}
	failures        chan error
	authority       localAuthority
	checkpoint      time.Time
	wallCheckpoint  time.Time
	failOnce        sync.Once
	bootDigest      string
	sleepCheckpoint *authoritySleepSample
	wakeRenewals    chan struct{}
	failed          error
}

func (watch *authorityWatch) Renewed(authority localAuthority) {
	watch.mu.Lock()
	// A late reply cannot restore expired local authority.
	if err := watch.checkLocked(watch.clock.Now(), wallNow(watch.clock)); err != nil {
		watch.mu.Unlock()
		watch.fail(err)
		return
	}
	watch.authority = authority
	watch.setCheckpoint(authority)
	// Independently validate the whole new grant from its request checkpoint.
	// The old watch may have consumed several smaller gaps while RPC was in
	// flight. A missing/mismatched reply digest cannot inherit those credits.
	err := watch.checkLocked(watch.clock.Now(), wallNow(watch.clock))
	watch.mu.Unlock()
	if err != nil {
		watch.fail(err)
		return
	}
	select {
	case watch.updates <- struct{}{}:
	default:
	}
}

func (watch *authorityWatch) setCheckpoint(authority localAuthority) {
	checkpoint := authority.requestCheckpoint
	if checkpoint == nil {
		// Direct watchdog users have no RPC; their authority starts now.
		current := captureAuthorityCheckpoint(watch.clock)
		checkpoint = &current
	}
	watch.checkpoint = checkpoint.now
	watch.wallCheckpoint = checkpoint.wall
	watch.sleepCheckpoint = checkpoint.sleep
}

func (watch *authorityWatch) Failures() <-chan error        { return watch.failures }
func (watch *authorityWatch) WakeRenewals() <-chan struct{} { return watch.wakeRenewals }

func (watch *authorityWatch) Stop() { watch.stop() }

// Check is called immediately before renewal. It shares the same suspend-gap
// test as the independent timer. Proven same-host sleep requests renewal;
// every other gap must pass the ordinary authority fence before an RPC.
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
	if watch.failed != nil {
		return watch.failed
	}
	remainingAtCheckpoint := watch.authority.deadline.Sub(watch.checkpoint)
	wallGap := wall.Sub(watch.wallCheckpoint)
	sample := sampleAuthoritySleep(watch.clock)
	provedSleep := time.Duration(0)
	gap := wallGap
	if watch.bootDigest != "" && watch.authority.sleepCreditBoot == watch.bootDigest && sample != nil && watch.sleepCheckpoint != nil && sample.boot >= watch.sleepCheckpoint.boot {
		provedSleep = max(0, sample.lower-watch.sleepCheckpoint.upper)
		// Fence if max(wall gap, boot-clock gap) - proved sleep >= the
		// remaining lease at the checkpoint, or the monotonic deadline passed.
		// Only the lower bound is credited. Process stops advance both clocks;
		// forward wall steps remain visible and backward steps cannot hide a
		// boot-clock gap. Missing proof or failed samples use today's wall rule.
		gap = max(wallGap, sample.boot-watch.sleepCheckpoint.boot) - provedSleep
	}
	if !now.Before(watch.authority.deadline) || remainingAtCheckpoint <= 0 || gap >= remainingAtCheckpoint {
		watch.failed = errAuthorityDeadlineExceeded
		return watch.failed
	}
	if provedSleep > 0 {
		select {
		case watch.wakeRenewals <- struct{}{}:
		default:
		}
	}
	watch.checkpoint = now
	watch.wallCheckpoint = wall
	watch.sleepCheckpoint = sample
	return nil
}

func (watch *authorityWatch) fail(err error) {
	watch.failOnce.Do(func() {
		watch.cancelPayload(err)
		watch.failures <- err
	})
}
