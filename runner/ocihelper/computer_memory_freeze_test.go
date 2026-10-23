package ocihelper

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type memoryFreezerFixture struct {
	holdReady                           bool
	mu                                  sync.Mutex
	frozen                              bool
	freezeWrites, thawWrites, failThaws int
	logs                                []string
	thawed                              chan struct{}
	thawOnce                            sync.Once
}

func (f *memoryFreezerFixture) io() computerMemoryFreezerIO {
	return computerMemoryFreezerIO{
		readRequested: func() (bool, error) { f.mu.Lock(); defer f.mu.Unlock(); return f.frozen, nil },
		readEvents: func() ([]byte, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			v := 0
			if f.frozen && !f.holdReady {
				v = 1
			}
			return []byte(fmt.Sprintf("populated 1\nfrozen %d\n", v)), nil
		},
		write: func(on bool) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			if on {
				f.freezeWrites++
			} else {
				f.thawWrites++
				if f.failThaws > 0 {
					f.failThaws--
					return errors.New("thaw refused")
				}
			}
			f.frozen = on
			if !on && f.thawed != nil {
				f.thawOnce.Do(func() { close(f.thawed) })
			}
			return nil
		},
		log: func(event string) { f.mu.Lock(); defer f.mu.Unlock(); f.logs = append(f.logs, event) },
	}
}
func TestComputerMemoryReworkRecoverAbandonedFreezeAndRetryThaw(t *testing.T) {
	f := &memoryFreezerFixture{frozen: true, failThaws: 2}
	recovered, err := recoverComputerMemoryFreeze(t.Context(), newManualClock(time.Unix(1234, 0)), f.io())
	if err != nil || !recovered || f.frozen || f.thawWrites != 3 || f.freezeWrites != 0 || len(f.logs) < 2 {
		t.Fatalf("recovery: recovered=%t err=%v fixture=%+v", recovered, err, f)
	}
}
func TestComputerMemoryReworkFinalThawFailureIsTyped(t *testing.T) {
	f := &memoryFreezerFixture{frozen: true, failThaws: 99}
	_, err := recoverComputerMemoryFreeze(t.Context(), newManualClock(time.Unix(1234, 0)), f.io())
	var refusal *SeatOperationRefusalError
	if !errors.As(err, &refusal) || refusal.Reason != SeatRefusalMemoryThawFailed || f.thawWrites != 3 || !f.frozen {
		t.Fatalf("lost possible-frozen evidence: %v writes=%d", err, f.thawWrites)
	}
}
func TestComputerMemoryLateThawConfirmsObservedState(t *testing.T) {
	clock := newManualClock(time.Unix(1234, 0))
	deadline := clock.Now().Add(2 * time.Second)
	clock.Advance(3 * time.Second)
	f := &memoryFreezerFixture{frozen: true}
	if err := thawComputerMemory(clock, deadline, f.io()); err != nil {
		t.Fatalf("confirmed late thaw reported failure: %v", err)
	}
	if f.frozen || f.thawWrites != 1 || len(f.logs) != 0 {
		t.Fatalf("late thaw: frozen=%t writes=%d logs=%v", f.frozen, f.thawWrites, f.logs)
	}
}
func TestComputerMemoryReworkDeadlineThawsWhileShrinkReadIsBlocked(t *testing.T) {
	clock := newManualClock(time.Unix(1234, 0))
	started := clock.Now()
	f := &memoryFreezerFixture{thawed: make(chan struct{})}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	result := make(chan error, 1)
	writes := 0
	go func() {
		_, err := shrinkComputerMemory(t.Context(), clock, f.io(), func() (int64, error) { close(entered); <-release; return 128 << 20, nil }, func() (int64, error) { return 2 << 30, nil }, func(int64) error { writes++; return nil }, 1<<30)
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("shrink never reached frozen read")
	}
	clock.Advance(1500 * time.Millisecond)
	select {
	case <-f.thawed:
	case <-time.After(5 * time.Second):
		t.Fatal("deadline left Computer frozen behind a blocked read")
	}
	if clock.Now().Sub(started) > 2*time.Second {
		t.Fatal("whole-span deadline exceeded")
	}
	unblock()
	select {
	case err := <-result:
		if err == nil || writes != 0 {
			t.Fatalf("expired shrink wrote after thaw: err=%v writes=%d", err, writes)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shrink did not finish")
	}
}

func TestComputerMemoryReworkSlowFreezeIsRetryable(t *testing.T) {
	for i := 0; i < 20; i++ {
		clock := &observedClock{manualClock: newManualClock(time.Unix(1234, 0)), timerCreated: make(chan struct{}, 4)}
		f := &memoryFreezerFixture{holdReady: true}
		result := make(chan error, 1)
		go func() {
			_, err := shrinkComputerMemory(t.Context(), clock, f.io(), func() (int64, error) { return 128 << 20, nil }, func() (int64, error) { return 2 << 30, nil }, func(int64) error { return nil }, 1<<30)
			result <- err
		}()
		for j := 0; j < 2; j++ {
			select {
			case <-clock.timerCreated:
			case <-time.After(5 * time.Second):
				t.Fatal("freeze wait did not arm its deadline and poll")
			}
		}
		clock.Advance(1500 * time.Millisecond)
		select {
		case err := <-result:
			var refusal *SeatOperationRefusalError
			if !errors.As(err, &refusal) || refusal.Reason != SeatRefusalMemoryBusy || f.frozen {
				t.Fatalf("slow freeze became broken: %v frozen=%t", err, f.frozen)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("freeze deadline did not finish")
		}
	}
}
