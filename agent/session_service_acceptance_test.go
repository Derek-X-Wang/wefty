//go:build service_acceptance

package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
)

func TestServiceAcceptanceClassGatesAreIndependentResizableCounters(t *testing.T) {
	oneShot := newAdmissionGate(l1.DefaultMaxOneshotSlots)
	service := newAdmissionGate(l1.DefaultMaxServiceSlots)
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		admitted, err := oneShot.execute(context.Background(), func(context.Context) (errorDestination, error) {
			close(started)
			<-release
			return errorDestinationUnclassified, nil
		}, propagateDestinationError)
		if !admitted && err == nil {
			err = errors.New("first one-shot attempt was not admitted")
		}
		done <- err
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first one-shot attempt was not admitted")
	}

	admitted, err := oneShot.execute(context.Background(), successfulAttempt, propagateDestinationError)
	if err != nil {
		t.Fatal(err)
	}
	if !admitted {
		t.Fatal("second one-shot attempt was refused below the widened class limit")
	}
	oneShot.setLimit(1)
	admitted, err = oneShot.execute(context.Background(), successfulAttempt, propagateDestinationError)
	if err != nil {
		t.Fatal(err)
	}
	if admitted {
		t.Fatal("N+1th one-shot attempt was admitted after the class limit was reduced to occupancy")
	}
	oneShot.setLimit(0)
	if occupancy := oneShot.occupancy(); !occupancy.Overcommitted || occupancy.Occupied != 1 || occupancy.Limit != 0 {
		t.Fatalf("reduced one-shot occupancy = %#v, want visible overcommit while the resident attempt drains", occupancy)
	}

	admitted, err = service.execute(context.Background(), successfulAttempt, propagateDestinationError)
	if err != nil {
		t.Fatal(err)
	}
	if !admitted {
		t.Fatal("service-class gate shared occupancy with the one-shot gate")
	}

	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("one-shot gate did not release its admitted attempt")
	}

	oneShot.setLimit(1)
	admitted, err = oneShot.execute(context.Background(), successfulAttempt, propagateDestinationError)
	if err != nil {
		t.Fatal(err)
	}
	if !admitted {
		t.Fatal("one-shot gate did not release capacity after finalization")
	}
}

func TestServiceAcceptanceGateRoutesErrorsByDestination(t *testing.T) {
	gate := newAdmissionGate(1)
	want := errors.New("attempt authority lost")
	work := func(context.Context) (errorDestination, error) {
		return errorDestinationAttemptAuthority, want
	}

	admitted, err := gate.execute(context.Background(), work, func(destination errorDestination, err error) error {
		if destination != errorDestinationAttemptAuthority {
			t.Fatalf("destination = %d, want attempt-authority", destination)
		}
		return err
	})
	if !admitted || !errors.Is(err, want) {
		t.Fatalf("propagated execution = (%t, %v), want (true, %v)", admitted, err, want)
	}

	admitted, err = gate.execute(context.Background(), work, func(destination errorDestination, _ error) error {
		if destination != errorDestinationAttemptAuthority {
			t.Fatalf("destination = %d, want attempt-authority", destination)
		}
		return nil
	})
	if !admitted || err != nil {
		t.Fatalf("absorbed execution = (%t, %v), want (true, nil)", admitted, err)
	}
}

func TestServiceAcceptanceAgentFillsBothGrantedClassPools(t *testing.T) {
	assertAgentFillsGrantedClassSlotsConcurrently(t)
}

func TestServiceAcceptanceAgentRefillsFinalizedSlots(t *testing.T) {
	assertAgentRefillsSlotAfterSiblingFinalization(t)
}

func TestServiceAcceptanceLocalQuiescenceIsPerJob(t *testing.T) {
	assertAgentExcludesARequeuedJobUntilLocalFinalizationReturns(t)
}

func TestServiceAcceptanceHeartbeatResizesClassPools(t *testing.T) {
	assertAgentResizesPoolFromHeartbeatGrantedCapacity(t)
}

func TestServiceAcceptanceFailedCompletionIsSiblingScoped(t *testing.T) {
	assertFailedCompletionLeavesSiblingAndSessionRunning(t)
}

func TestServiceAcceptancePluralDrainJoinsAllResidentAttempts(t *testing.T) {
	assertDrainJoinsEveryResidentOneShotAttempt(t)
}

func TestServiceAcceptancePluralDrainJoinsServicesAndOneShots(t *testing.T) {
	assertPluralServiceDrainJoinsAll(t)
}

func successfulAttempt(context.Context) (errorDestination, error) {
	return errorDestinationUnclassified, nil
}

func propagateDestinationError(_ errorDestination, err error) error { return err }
