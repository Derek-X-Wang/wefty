package l1

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestComputerGrantPolicyDistribution(t *testing.T) {
	assertComputerGrantPolicyDistribution(t)
}

func TestOrdinaryHeartbeatOmitsComputerPolicyBytes(t *testing.T) {
	payload, err := json.Marshal(HeartbeatResponse{Node: Node{}, RemovalDirectives: []RemovalDirective{},
		StorageResetDirectives: []ComputerStorageResetDirective{}})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte("computer_policy")) {
		t.Fatalf("ordinary heartbeat acquired Computer policy bytes: %s", payload)
	}
}

func TestAdminRemovalWritesCurrentNoneBeforeDroppingOverride(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{}, map[string]NodePolicy{
		"computer-node": DefaultNodePolicy(contract.StableNodeTagPrefix + "computer-node"),
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	alice := fabric.Identity{FabricID: "fabric-test", UserID: "person-alice", DeviceID: "device-alice"}
	bob := fabric.Identity{FabricID: "fabric-test", UserID: "person-bob", DeviceID: "device-bob"}
	challenge, err := h.store.InitiateAdminBootstrap(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	policy, err := h.store.BootstrapAdmin(context.Background(), alice, challenge.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	computer, _, err := h.store.CreateComputer(context.Background(), CreateComputerRequest{
		Name: "admin-removal", Spec: computerCapabilityJobSpec("computer:admin-removal"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	policy, err = h.store.AddAdmin(context.Background(), alice, bob.UserID, policy.Revision)
	if err != nil {
		t.Fatal(err)
	}
	// An underlying grant for an administrator must not reappear when the
	// override is removed.
	grant, err := h.store.MutateComputerGrant(context.Background(), alice, computer.ComputerID, alice.UserID,
		ComputerGrantMutationRequest{PolicyRevision: policy.Revision, Permission: ComputerGrantControl,
			IdempotencyKey: "under-admin"})
	if err != nil {
		t.Fatal(err)
	}
	removed, err := h.store.RemoveAdmin(context.Background(), bob, alice.UserID, grant.Grant.PolicyRevision)
	if err != nil || len(removed.Revocations) != 1 || removed.Revocations[0].State != ComputerPolicyRevocationPending {
		t.Fatalf("removed administrator policy = %#v err=%v", removed, err)
	}
	snapshot, err := h.store.IssueComputerPolicySnapshot(context.Background(), "fabric-computer-node",
		node.NodeID, node.BootSessionID, time.Minute)
	if err != nil || snapshot == nil {
		t.Fatalf("admin removal snapshot = %#v err=%v", snapshot, err)
	}
	assertSnapshotGrant(t, *snapshot, computer.ComputerID, alice.FabricID, alice.UserID, ComputerGrantNone)
	for _, admin := range snapshot.Admins {
		if admin.FabricID == alice.FabricID && admin.UserID == alice.UserID {
			t.Fatalf("removed administrator remained in snapshot: %#v", snapshot.Admins)
		}
	}
}

func assertComputerGrantPolicyDistribution(t *testing.T) {
	t.Helper()
	h := newIntegrationHarnessWithOptions(t, StoreOptions{}, map[string]NodePolicy{
		"computer-node": DefaultNodePolicy(contract.StableNodeTagPrefix + "computer-node"),
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	admin := fabric.Identity{FabricID: "fabric-test", UserID: "person-admin", DeviceID: "device-admin"}
	challenge, err := h.store.InitiateAdminBootstrap(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	policy, err := h.store.BootstrapAdmin(context.Background(), admin, challenge.Nonce)
	if err != nil || policy.Revision != 1 {
		t.Fatalf("bootstrap policy = %#v err=%v", policy, err)
	}
	computer, replayed, err := h.store.CreateComputer(context.Background(), CreateComputerRequest{
		Name: "policy-computer", Spec: computerCapabilityJobSpec("computer:policy"), Actor: "operator",
	})
	if err != nil || replayed {
		t.Fatalf("create Computer = %#v replayed=%t err=%v", computer, replayed, err)
	}

	viewRequest := ComputerGrantMutationRequest{PolicyRevision: policy.Revision,
		Permission: ComputerGrantView, IdempotencyKey: "grant-view"}
	view, err := h.store.MutateComputerGrant(context.Background(), admin, computer.ComputerID, "person-viewer", viewRequest)
	if err != nil || view.Replayed || view.Grant.Permission != ComputerGrantView || view.Grant.PolicyRevision != 2 || view.Revocation != nil {
		t.Fatalf("view grant = %#v err=%v", view, err)
	}
	replayedView, err := h.store.MutateComputerGrant(context.Background(), admin, computer.ComputerID, "person-viewer", viewRequest)
	if err != nil || !replayedView.Replayed || replayedView.Grant != view.Grant {
		t.Fatalf("view replay = %#v err=%v", replayedView, err)
	}
	if _, err := h.store.MutateComputerGrant(context.Background(), admin, computer.ComputerID, "person-viewer",
		ComputerGrantMutationRequest{PolicyRevision: 1, Permission: ComputerGrantControl, IdempotencyKey: "stale"}); errorCode(err) != contract.ErrorStalePolicyRevision {
		t.Fatalf("stale grant mutation error = %v, want %q", err, contract.ErrorStalePolicyRevision)
	}
	nonadmin := fabric.Identity{FabricID: "fabric-test", UserID: "person-viewer", DeviceID: "device-viewer"}
	if _, err := h.store.MutateComputerGrant(context.Background(), nonadmin, computer.ComputerID, "person-other",
		ComputerGrantMutationRequest{PolicyRevision: 2, Permission: ComputerGrantView, IdempotencyKey: "forbidden"}); errorCode(err) != contract.ErrorAdminRequired {
		t.Fatalf("nonadmin grant mutation error = %v, want %q", err, contract.ErrorAdminRequired)
	}

	control, err := h.store.MutateComputerGrant(context.Background(), admin, computer.ComputerID, "person-viewer",
		ComputerGrantMutationRequest{PolicyRevision: 2, Permission: ComputerGrantControl, IdempotencyKey: "grant-control"})
	if err != nil || control.Grant.PolicyRevision != 3 || control.Revocation != nil {
		t.Fatalf("control upgrade = %#v err=%v", control, err)
	}
	controlSnapshot, err := h.store.IssueComputerPolicySnapshot(context.Background(), "fabric-computer-node",
		node.NodeID, node.BootSessionID, time.Minute)
	if err != nil || controlSnapshot == nil {
		t.Fatalf("control snapshot = %#v err=%v", controlSnapshot, err)
	}
	if err := ValidateComputerPolicySnapshot(*controlSnapshot); err != nil {
		t.Fatal(err)
	}
	assertSnapshotGrant(t, *controlSnapshot, computer.ComputerID, "fabric-test", "person-viewer", ComputerGrantControl)
	if err := h.store.AcknowledgeComputerPolicyInstallation(context.Background(), "fabric-computer-node",
		acknowledgementFor(*controlSnapshot)); err != nil {
		t.Fatal(err)
	}

	downgradeRequest := ComputerGrantMutationRequest{PolicyRevision: 3,
		Permission: ComputerGrantView, IdempotencyKey: "downgrade-view"}
	downgrade, err := h.store.MutateComputerGrant(context.Background(), admin, computer.ComputerID, "person-viewer", downgradeRequest)
	if err != nil || downgrade.Revocation == nil || downgrade.Revocation.State != ComputerPolicyRevocationPending {
		t.Fatalf("control downgrade = %#v err=%v", downgrade, err)
	}
	replayedDowngrade, err := h.store.MutateComputerGrant(context.Background(), admin, computer.ComputerID, "person-viewer", downgradeRequest)
	if err != nil || !replayedDowngrade.Replayed || replayedDowngrade.Revocation == nil {
		t.Fatalf("downgrade replay = %#v err=%v", replayedDowngrade, err)
	}
	// A cached, explicitly acknowledged older snapshot cannot complete the
	// newer revocation or reveal its old control grant.
	if err := h.store.AcknowledgeComputerPolicyInstallation(context.Background(), "fabric-computer-node",
		acknowledgementFor(*controlSnapshot)); err != nil {
		t.Fatal(err)
	}
	pending, err := h.store.GetComputerPolicyRevocation(context.Background(), admin,
		downgrade.Grant.PolicyRevision, computer.ComputerID, "", "")
	if err != nil || pending.State != ComputerPolicyRevocationPending {
		t.Fatalf("revocation before current install = %#v err=%v", pending, err)
	}
	currentSnapshot, err := h.store.IssueComputerPolicySnapshot(context.Background(), "fabric-computer-node",
		node.NodeID, node.BootSessionID, time.Minute)
	if err != nil || currentSnapshot == nil || currentSnapshot.PolicyRevision != 4 {
		t.Fatalf("current snapshot = %#v err=%v", currentSnapshot, err)
	}
	assertSnapshotGrant(t, *currentSnapshot, computer.ComputerID, "fabric-test", "person-viewer", ComputerGrantView)
	if err := h.store.AcknowledgeComputerPolicyInstallation(context.Background(), "fabric-computer-node",
		acknowledgementFor(*currentSnapshot)); err != nil {
		t.Fatal(err)
	}
	if err := h.store.AcknowledgeComputerPolicyInstallation(context.Background(), "fabric-computer-node",
		acknowledgementFor(*controlSnapshot)); errorCode(err) != contract.ErrorStalePolicyRevision {
		t.Fatalf("regressed installation acknowledgement error = %v, want %q", err, contract.ErrorStalePolicyRevision)
	}
	completed, err := h.store.GetComputerPolicyRevocation(context.Background(), admin,
		downgrade.Grant.PolicyRevision, computer.ComputerID, "", "")
	if err != nil || completed.State != ComputerPolicyRevocationCompleted {
		t.Fatalf("revocation after current install = %#v err=%v", completed, err)
	}
	completedReplay, err := h.store.MutateComputerGrant(context.Background(), admin, computer.ComputerID,
		"person-viewer", downgradeRequest)
	if err != nil || !completedReplay.Replayed || completedReplay.Revocation == nil ||
		completedReplay.Revocation.State != ComputerPolicyRevocationCompleted {
		t.Fatalf("completed revocation replay = %#v err=%v", completedReplay, err)
	}
	replacementRegistration := node.NodeRegistration
	replacementRegistration.BootSessionID = "boot-computer-node-replacement"
	replacementRegistration.CapabilityRevision = 1
	replacementRegistration.CapabilityObservedAt = h.clock.Now()
	node, err = h.store.RegisterNode(context.Background(), fabric.Identity{NodeID: "fabric-computer-node"},
		replacementRegistration, DefaultNodePolicy(contract.StableNodeTagPrefix+"computer-node"), true)
	if err != nil {
		t.Fatal(err)
	}
	pendingAfterRestart, err := h.store.GetComputerPolicyRevocation(context.Background(), admin,
		downgrade.Grant.PolicyRevision, computer.ComputerID, "", "")
	if err != nil || pendingAfterRestart.State != ComputerPolicyRevocationPending {
		t.Fatalf("revocation after node restart = %#v err=%v", pendingAfterRestart, err)
	}
	restartSnapshot, err := h.store.IssueComputerPolicySnapshot(context.Background(), "fabric-computer-node",
		node.NodeID, node.BootSessionID, time.Minute)
	if err != nil || restartSnapshot == nil {
		t.Fatalf("restart snapshot = %#v err=%v", restartSnapshot, err)
	}
	if err := h.store.AcknowledgeComputerPolicyInstallation(context.Background(), "fabric-computer-node",
		acknowledgementFor(*restartSnapshot)); err != nil {
		t.Fatal(err)
	}
	completedAfterRestart, err := h.store.GetComputerPolicyRevocation(context.Background(), admin,
		downgrade.Grant.PolicyRevision, computer.ComputerID, "", "")
	if err != nil || completedAfterRestart.State != ComputerPolicyRevocationCompleted {
		t.Fatalf("revocation after replacement boot install = %#v err=%v", completedAfterRestart, err)
	}

	revoked, err := h.store.MutateComputerGrant(context.Background(), admin, computer.ComputerID, "person-viewer",
		ComputerGrantMutationRequest{PolicyRevision: 4, Permission: ComputerGrantNone, IdempotencyKey: "revoke-none"})
	if err != nil || revoked.Revocation == nil || revoked.Grant.Permission != ComputerGrantNone {
		t.Fatalf("full revocation = %#v err=%v", revoked, err)
	}
	revokedSnapshot, err := h.store.IssueComputerPolicySnapshot(context.Background(), "fabric-computer-node",
		node.NodeID, node.BootSessionID, time.Minute)
	if err != nil || revokedSnapshot == nil {
		t.Fatalf("revoked snapshot = %#v err=%v", revokedSnapshot, err)
	}
	assertSnapshotGrant(t, *revokedSnapshot, computer.ComputerID, "fabric-test", "person-viewer", ComputerGrantNone)

	audit, err := h.store.ListComputerPolicyAudit(context.Background(), admin, computer.ComputerID, "", 10)
	if err != nil || len(audit.Entries) != 4 || audit.Entries[0].ActorDeviceID != "device-admin" ||
		audit.Entries[3].PreviousPermission != ComputerGrantView || audit.Entries[3].Permission != ComputerGrantNone {
		t.Fatalf("Computer policy audit = %#v err=%v", audit, err)
	}
	t.Logf("assertion-derived Computer policy receipt: generation=%d revision=%d digest=%s revocation=%s",
		revokedSnapshot.PolicyGeneration, revokedSnapshot.PolicyRevision, revokedSnapshot.SnapshotDigest,
		revoked.Revocation.State)
}

func acknowledgementFor(snapshot ComputerPolicySnapshot) ComputerPolicyInstallAcknowledgement {
	return ComputerPolicyInstallAcknowledgement{NodeID: snapshot.NodeID, BootSessionID: snapshot.BootSessionID,
		PolicyGeneration: snapshot.PolicyGeneration, PolicyRevision: snapshot.PolicyRevision,
		SnapshotDigest: snapshot.SnapshotDigest}
}

func assertSnapshotGrant(t *testing.T, snapshot ComputerPolicySnapshot, computerID, fabricID, userID string, want ComputerGrantPermission) {
	t.Helper()
	for _, computer := range snapshot.Computers {
		if computer.ComputerID != computerID {
			continue
		}
		for _, grant := range computer.Grants {
			if grant.FabricID == fabricID && grant.UserID == userID {
				if grant.Permission != want {
					t.Fatalf("snapshot grant = %q, want %q", grant.Permission, want)
				}
				return
			}
		}
	}
	t.Fatalf("snapshot missing grant for %s/%s", fabricID, userID)
}
