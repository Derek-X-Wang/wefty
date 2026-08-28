package agent

import (
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestComputerPolicyCacheFailsClosedAndDrainsBeforeAcknowledgement(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	cache := NewComputerPolicyCache(clock, "node-1", "boot-1")
	t.Cleanup(cache.Close)
	person := fabric.Identity{FabricID: "fabric-one", UserID: "person-a", DeviceID: "device-a"}
	machine := person
	machine.Kind = fabric.IdentityKindMachine

	if decision := cache.LookupGrant("computer-1", person); decision.Allowed() {
		t.Fatalf("cold cache decision = %#v", decision)
	}
	view := policySnapshot(t, now, 1, 1, nil, l1.ComputerGrant{FabricID: person.FabricID,
		UserID: person.UserID, Permission: l1.ComputerGrantView, PolicyRevision: 1})
	receipt, err := cache.Install(view)
	if err != nil {
		t.Fatal(err)
	}
	assertClosed(t, receipt.SessionsClosed, "initial install")
	if decision := cache.LookupGrant("computer-1", machine); decision.Allowed() {
		t.Fatalf("machine principal inherited person grant: %#v", decision)
	}
	authorization, err := cache.AcquireGrant("computer-1", person)
	if err != nil || authorization == nil || authorization.Decision().Permission != l1.ComputerGrantView {
		t.Fatalf("view authorization = %#v err=%v", authorization, err)
	}

	control := policySnapshot(t, now.Add(time.Second), 1, 2, nil, l1.ComputerGrant{FabricID: person.FabricID,
		UserID: person.UserID, Permission: l1.ComputerGrantControl, PolicyRevision: 2})
	upgrade, err := cache.Install(control)
	if err != nil {
		t.Fatal(err)
	}
	assertClosed(t, upgrade.SessionsClosed, "upgrade")
	select {
	case revocation := <-authorization.Revocations():
		t.Fatalf("upgrade revoked view authorization: %#v", revocation)
	default:
	}
	controller, err := cache.AcquireGrant("computer-1", person)
	if err != nil || controller == nil || controller.Decision().Permission != l1.ComputerGrantControl {
		t.Fatalf("control authorization = %#v err=%v", controller, err)
	}

	downgrade := policySnapshot(t, now.Add(2*time.Second), 1, 3, nil, l1.ComputerGrant{FabricID: person.FabricID,
		UserID: person.UserID, Permission: l1.ComputerGrantView, PolicyRevision: 3})
	downgradeReceipt, err := cache.Install(downgrade)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case revocation := <-controller.Revocations():
		if revocation.Reason != ComputerPolicyRevoked || revocation.Permission != l1.ComputerGrantView {
			t.Fatalf("downgrade revocation = %#v", revocation)
		}
	case <-time.After(time.Second):
		t.Fatal("control authorization did not receive downgrade")
	}
	assertOpen(t, downgradeReceipt.SessionsClosed, "downgrade before relay close")
	controller.Release()
	assertClosed(t, downgradeReceipt.SessionsClosed, "downgrade after relay close")
	authorization.Release()

	viewer, err := cache.AcquireGrant("computer-1", person)
	if err != nil || viewer == nil {
		t.Fatalf("post-downgrade view authorization = %#v err=%v", viewer, err)
	}
	revoked := policySnapshot(t, now.Add(3*time.Second), 1, 4, nil, l1.ComputerGrant{FabricID: person.FabricID,
		UserID: person.UserID, Permission: l1.ComputerGrantNone, PolicyRevision: 4})
	revokedReceipt, err := cache.Install(revoked)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case revocation := <-viewer.Revocations():
		if revocation.Permission != l1.ComputerGrantNone {
			t.Fatalf("full revocation = %#v", revocation)
		}
	case <-time.After(time.Second):
		t.Fatal("view authorization did not receive revocation")
	}
	assertOpen(t, revokedReceipt.SessionsClosed, "revoke before relay close")
	viewer.Release()
	assertClosed(t, revokedReceipt.SessionsClosed, "revoke after relay close")
	if decision := cache.LookupGrant("computer-1", person); decision.Allowed() {
		t.Fatalf("revoked grant remained usable: %#v", decision)
	}
}

func TestComputerPolicyCacheAuthorityLossAndExpiryFailClosed(t *testing.T) {
	now := time.Date(2026, 8, 28, 13, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	cache := NewComputerPolicyCache(clock, "node-1", "boot-1")
	t.Cleanup(cache.Close)
	person := fabric.Identity{FabricID: "fabric-one", UserID: "person-a", DeviceID: "device-a"}
	admin := l1.ComputerPolicyAdmin{FabricID: person.FabricID, UserID: person.UserID}
	snapshot := policySnapshot(t, now, 5, 7, []l1.ComputerPolicyAdmin{admin})
	if _, err := cache.Install(snapshot); err != nil {
		t.Fatal(err)
	}
	authorization, err := cache.AcquireGrant("computer-1", person)
	if err != nil || authorization == nil || authorization.Decision().Permission != l1.ComputerGrantControl {
		t.Fatalf("admin authorization = %#v err=%v", authorization, err)
	}
	regressed := policySnapshot(t, now.Add(time.Second), 5, 6, []l1.ComputerPolicyAdmin{admin})
	if _, err := cache.Install(regressed); err == nil {
		t.Fatal("revision regression installed")
	}
	assertRevocationReason(t, authorization, ComputerPolicyRevisionRegressed)
	authorization.Release()

	newGeneration := policySnapshot(t, now.Add(2*time.Second), 6, 8, []l1.ComputerPolicyAdmin{admin})
	if _, err := cache.Install(newGeneration); err != nil {
		t.Fatal(err)
	}
	authorization, err = cache.AcquireGrant("computer-1", person)
	if err != nil || authorization == nil {
		t.Fatalf("new generation authorization = %#v err=%v", authorization, err)
	}
	nextGeneration := policySnapshot(t, now.Add(3*time.Second), 7, 9, []l1.ComputerPolicyAdmin{admin})
	receipt, err := cache.Install(nextGeneration)
	if err != nil {
		t.Fatal(err)
	}
	assertRevocationReason(t, authorization, ComputerPolicyGenerationChanged)
	assertOpen(t, receipt.SessionsClosed, "generation change before close")
	authorization.Release()
	assertClosed(t, receipt.SessionsClosed, "generation change after close")

	expiring := policySnapshot(t, clock.Now(), 7, 10, []l1.ComputerPolicyAdmin{admin})
	expiring.FreshUntil = clock.Now().Add(time.Minute)
	var digestErr error
	expiring.SnapshotDigest, digestErr = l1.ComputeComputerPolicySnapshotDigest(expiring)
	if digestErr != nil {
		t.Fatal(digestErr)
	}
	if _, err := cache.Install(expiring); err != nil {
		t.Fatal(err)
	}
	authorization, err = cache.AcquireGrant("computer-1", person)
	if err != nil || authorization == nil {
		t.Fatalf("expiring authorization = %#v err=%v", authorization, err)
	}
	clock.waitForDeadline(t, expiring.FreshUntil)
	clock.Advance(time.Minute)
	assertRevocationReason(t, authorization, ComputerPolicyExpired)
	authorization.Release()

	if _, err := cache.Install(policySnapshot(t, clock.Now(), 7, 11, []l1.ComputerPolicyAdmin{admin})); err != nil {
		t.Fatal(err)
	}
	authorization, err = cache.AcquireGrant("computer-1", person)
	if err != nil || authorization == nil {
		t.Fatalf("watch-loss authorization = %#v err=%v", authorization, err)
	}
	cache.Invalidate(ComputerPolicyWatchLost)
	assertRevocationReason(t, authorization, ComputerPolicyWatchLost)
	authorization.Release()
}

func TestComputerPolicyAcquireRacingRevocationHasNoAdmissionGap(t *testing.T) {
	now := time.Date(2026, 8, 28, 14, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	cache := NewComputerPolicyCache(clock, "node-1", "boot-1")
	t.Cleanup(cache.Close)
	person := fabric.Identity{FabricID: "fabric-one", UserID: "person-a", DeviceID: "device-a"}
	revision := int64(1)
	if _, err := cache.Install(policySnapshot(t, now, 1, revision, nil, l1.ComputerGrant{
		FabricID: person.FabricID, UserID: person.UserID, Permission: l1.ComputerGrantView, PolicyRevision: revision})); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		start := make(chan struct{})
		var authorization *ComputerGrantAuthorization
		var acquireErr, installErr error
		var receipt ComputerPolicyInstallReceipt
		var group sync.WaitGroup
		group.Add(2)
		go func() {
			defer group.Done()
			<-start
			authorization, acquireErr = cache.AcquireGrant("computer-1", person)
		}()
		revision++
		revokeSnapshot := policySnapshot(t, now.Add(time.Duration(revision)*time.Second), 1, revision, nil,
			l1.ComputerGrant{FabricID: person.FabricID, UserID: person.UserID,
				Permission: l1.ComputerGrantNone, PolicyRevision: revision})
		go func() {
			defer group.Done()
			<-start
			receipt, installErr = cache.Install(revokeSnapshot)
		}()
		close(start)
		group.Wait()
		if acquireErr != nil || installErr != nil {
			t.Fatalf("race iteration %d errors: acquire=%v install=%v", i, acquireErr, installErr)
		}
		if authorization != nil {
			assertRevocationReason(t, authorization, ComputerPolicyRevoked)
			authorization.Release()
		}
		assertClosed(t, receipt.SessionsClosed, "raced revocation drain")
		revision++
		if _, err := cache.Install(policySnapshot(t, now.Add(time.Duration(revision)*time.Second), 1, revision, nil,
			l1.ComputerGrant{FabricID: person.FabricID, UserID: person.UserID,
				Permission: l1.ComputerGrantView, PolicyRevision: revision})); err != nil {
			t.Fatal(err)
		}
	}
}

func policySnapshot(t *testing.T, issued time.Time, generation, revision int64, admins []l1.ComputerPolicyAdmin, grants ...l1.ComputerGrant) l1.ComputerPolicySnapshot {
	t.Helper()
	for index := range grants {
		if grants[index].PolicyRevision == 0 {
			grants[index].PolicyRevision = revision
		}
		if grants[index].UpdatedAt.IsZero() {
			grants[index].UpdatedAt = issued
		}
	}
	snapshot := l1.ComputerPolicySnapshot{PolicyGeneration: generation, PolicyRevision: revision,
		NodeID: "node-1", BootSessionID: "boot-1", IssuedAt: issued, FreshUntil: issued.Add(10 * time.Minute),
		Admins: admins, Computers: []l1.ComputerPolicyComputer{{ComputerID: "computer-1", Grants: grants}}}
	if snapshot.Admins == nil {
		snapshot.Admins = []l1.ComputerPolicyAdmin{}
	}
	if snapshot.Computers[0].Grants == nil {
		snapshot.Computers[0].Grants = []l1.ComputerGrant{}
	}
	var err error
	snapshot.SnapshotDigest, err = l1.ComputeComputerPolicySnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func assertRevocationReason(t *testing.T, authorization *ComputerGrantAuthorization, want ComputerPolicyRevocationReason) {
	t.Helper()
	select {
	case revocation := <-authorization.Revocations():
		if revocation.Reason != want {
			t.Fatalf("revocation reason = %q, want %q", revocation.Reason, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("authorization did not receive %q", want)
	}
}

func assertClosed(t *testing.T, channel <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-channel:
	case <-time.After(time.Second):
		t.Fatalf("%s did not close", message)
	}
}

func assertOpen(t *testing.T, channel <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-channel:
		t.Fatalf("%s closed early", message)
	default:
	}
}
