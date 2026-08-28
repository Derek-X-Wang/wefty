package agent

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestComputerPolicyInstallDoesNotParkWhileSessionsDrain(t *testing.T) {
	now := time.Date(2026, 8, 28, 16, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	acknowledged := make(chan struct{}, 1)
	client := newRoundTripClient(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			t.Errorf("ack method = %s", request.Method)
		}
		acknowledged <- struct{}{}
		response.WriteHeader(http.StatusNoContent)
	}))
	cache := NewComputerPolicyCache(clock, "node-1", "boot-1")
	controller := newComputerPolicyAckController(client, clock, nil)
	session := &agentSession{client: client, clock: clock, computerPolicy: cache, computerAcks: controller}
	t.Cleanup(cache.Close)
	person := fabric.Identity{FabricID: "fabric-one", UserID: "person-a", DeviceID: "device-a"}
	if _, err := cache.Install(policySnapshot(t, now, 1, 1, nil, l1.ComputerGrant{
		FabricID: person.FabricID, UserID: person.UserID, Permission: l1.ComputerGrantView, PolicyRevision: 1})); err != nil {
		t.Fatal(err)
	}
	authorization, err := cache.AcquireGrant("computer-1", person)
	if err != nil || authorization == nil {
		t.Fatalf("authorization = %#v err=%v", authorization, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		controller.run(ctx)
	}()
	revoked := policySnapshot(t, now.Add(time.Second), 1, 2, nil, l1.ComputerGrant{
		FabricID: person.FabricID, UserID: person.UserID, Permission: l1.ComputerGrantNone, PolicyRevision: 2})
	installed := make(chan error, 1)
	go func() { installed <- session.installComputerPolicy(context.Background(), &revoked) }()
	select {
	case err := <-installed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("policy install parked on an active authorization")
	}
	select {
	case <-acknowledged:
		t.Fatal("policy acknowledged before the authorization released")
	default:
	}
	authorization.Release()
	clock.waitForDeadline(t, clock.Now().Add(computerPolicyPendingReportInterval))
	clock.Advance(computerPolicyPendingReportInterval)
	select {
	case <-acknowledged:
	case <-time.After(time.Second):
		t.Fatal("policy acknowledgement did not retry after the session barrier closed")
	}
	cancel()
	<-done
}
