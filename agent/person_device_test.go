package agent

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/coder/websocket"
)

// Inject through the wire representation so this regression also runs against
// the pre-binding agent, which silently drops these formerly unknown fields.
func boundAdminPolicy(t *testing.T, snapshot l1.ComputerPolicySnapshot, device string) l1.ComputerPolicySnapshot {
	t.Helper()
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	wire["admin_device_required"] = true
	admins := wire["admins"].([]any)
	for _, entry := range admins {
		entry.(map[string]any)["device_id"] = device
	}
	raw, err = json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	var bound l1.ComputerPolicySnapshot
	if err := json.Unmarshal(raw, &bound); err != nil {
		t.Fatal(err)
	}
	bound.SnapshotDigest, err = l1.ComputeComputerPolicySnapshotDigest(bound)
	if err != nil {
		t.Fatal(err)
	}
	return bound
}

func TestComputerFrontDoorAdminDeviceBindingAndLiveRebind(t *testing.T) {
	fixture, cache, auditor, _, server, identity := computerFrontDoorFixture(t, l1.ComputerGrantNone)
	defer server.Close()
	admins := []l1.ComputerPolicyAdmin{{FabricID: identity.FabricID, UserID: identity.UserID}}
	// A grant cannot restore access for a rostered administrator on another device.
	grant := l1.ComputerGrant{FabricID: identity.FabricID, UserID: identity.UserID, Permission: l1.ComputerGrantControl}
	snapshot := boundAdminPolicy(t, policySnapshot(t, cache.clock.Now(), 1, 2, admins, grant), identity.DeviceID)
	if _, err := cache.Install(snapshot); err != nil {
		t.Fatal(err)
	}
	other := identity
	other.DeviceID = "other-device"
	fixture.identity.set(other, nil)
	auth, err := cache.AcquireGrant("computer-1", other)
	if err != nil || auth != nil {
		if auth != nil {
			auth.Release()
		}
		t.Fatalf("other admin device acquired authority: %#v %v", auth, err)
	}
	connection, response, err := websocket.Dial(t.Context(), "ws"+server.URL[len("http"):]+computerWebSocketPath, &websocket.DialOptions{Subprotocols: []string{computerWebSocketSubprotocol}})
	if connection != nil {
		connection.CloseNow()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("other admin device front door: response=%v err=%v", response, err)
	}
	fixture.identity.set(identity, nil)
	bound, err := cache.AcquireGrant("computer-1", identity)
	if err != nil || bound == nil || !bound.CanTake() || !bound.IsAdministrator() {
		t.Fatalf("bound administrator lost authority: %v", err)
	}
	bound.Release()
	connection = dialComputerFrontDoor(t, server.URL, nil)
	defer connection.CloseNow()
	if _, _, err := connection.Read(t.Context()); err != nil {
		t.Fatal(err)
	}
	rebound := boundAdminPolicy(t, policySnapshot(t, cache.clock.Now(), 1, 3, admins, grant), other.DeviceID)
	receipt, err := cache.Install(rebound)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-receipt.SessionsClosed:
	case <-time.After(testwait.Bound):
		t.Fatal("device rebind did not drain the live front-door session")
	}
	if _, _, err := connection.Read(t.Context()); err == nil {
		t.Fatal("old device websocket stayed open")
	}
	waitComputerAuditKind(t, auditor, l1.ComputerTakeoverSessionClose)
	events := auditor.snapshot()
	if events[len(events)-1].Reason != l1.ComputerTakeoverRevoked {
		t.Fatalf("rebind used a different revocation path: %#v", events)
	}
	fixture.identity.set(other, nil)
	replacement := dialComputerFrontDoor(t, server.URL, nil)
	defer replacement.CloseNow()
	if _, _, err := replacement.Read(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestComputerFrontDoorFirstBoundSnapshotClosesOtherDevice(t *testing.T) {
	fixture, cache, _, _, server, identity := computerFrontDoorFixture(t, l1.ComputerGrantNone)
	defer server.Close()
	admins := []l1.ComputerPolicyAdmin{{FabricID: identity.FabricID, UserID: identity.UserID}}
	plain := policySnapshot(t, cache.clock.Now(), 1, 2, admins)
	if _, err := cache.Install(plain); err != nil {
		t.Fatal(err)
	}
	connection := dialComputerFrontDoor(t, server.URL, nil)
	defer connection.CloseNow()
	if _, _, err := connection.Read(t.Context()); err != nil {
		t.Fatal(err)
	}
	// The first authenticated snapshot may have the same durable revision.
	bound := boundAdminPolicy(t, plain, "different-enrolled-device")
	receipt, err := cache.Install(bound)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-receipt.SessionsClosed:
	case <-time.After(testwait.Bound):
		t.Fatal("first bound snapshot did not drain")
	}
	if auth, _ := cache.AcquireGrant("computer-1", identity); auth != nil {
		auth.Release()
		t.Fatal("first bound snapshot still authorizes the other device")
	}
	if _, _, err := connection.Read(t.Context()); err == nil {
		t.Fatal("first bound snapshot left the other device session open")
	}
	fixture.identity.set(fabric.Identity{FabricID: identity.FabricID, UserID: identity.UserID, DeviceID: "different-enrolled-device"}, nil)
	accepted := dialComputerFrontDoor(t, server.URL, nil)
	defer accepted.CloseNow()
}

func TestComputerPolicyAdminBindingChangeNeedsRevision(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	admins := []l1.ComputerPolicyAdmin{{FabricID: "fabric-one", UserID: "owner"}}
	for _, change := range []string{"device", "requirement"} {
		for _, watchLost := range []bool{false, true} {
			name := change
			if watchLost {
				name += "/watch-lost"
			}
			t.Run(name, func(t *testing.T) {
				cache := NewComputerPolicyCache(newManualClock(now), "node-1", "boot-1")
				defer cache.Close()
				current := boundAdminPolicy(t, policySnapshot(t, now, 1, 2, admins), "enrolled")
				if _, err := cache.Install(current); err != nil {
					t.Fatal(err)
				}
				if watchLost {
					cache.Invalidate(ComputerPolicyWatchLost)
				}
				next := boundAdminPolicy(t, policySnapshot(t, now, 1, 2, admins), "replacement")
				if change == "requirement" {
					next = policySnapshot(t, now, 1, 2, admins)
				}
				if _, err := cache.Install(next); err == nil {
					t.Fatal("administrator device authority changed without a revision")
				}
				if cache.Valid() {
					t.Fatal("unrevisioned binding change left cache open")
				}
			})
		}
	}
}

func TestComputerPolicyUnboundAdminCannotFallBackToGrant(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	identity := fabric.Identity{FabricID: "fabric-one", UserID: "owner", DeviceID: "any-device"}
	cache := NewComputerPolicyCache(newManualClock(now), "node-1", "boot-1")
	defer cache.Close()
	snapshot := boundAdminPolicy(t, policySnapshot(t, now, 1, 2, []l1.ComputerPolicyAdmin{{FabricID: identity.FabricID, UserID: identity.UserID}}, l1.ComputerGrant{FabricID: identity.FabricID, UserID: identity.UserID, Permission: l1.ComputerGrantControl}), "")
	if _, err := cache.Install(snapshot); err != nil {
		t.Fatal(err)
	}
	if auth, _ := cache.AcquireGrant("computer-1", identity); auth != nil {
		auth.Release()
		t.Fatal("unbound administrator inherited an explicit grant")
	}
}
