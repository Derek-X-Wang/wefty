package l1

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestComputerGrantPolicyDistribution(t *testing.T) {
	assertComputerGrantPolicyDistribution(t)
}

func TestOrdinaryHeartbeatOmitsComputerPolicyBytes(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{"ordinary": DefaultNodePolicy()})
	agent := h.client(fabric.Identity{NodeID: "fabric-ordinary", Tags: []string{DefaultAgentPrincipalTag}})
	registration := contract.NodeRegistration{NodeID: "ordinary", BootSessionID: "boot-ordinary",
		RootInstanceID: "root-ordinary", OS: "linux", Architecture: "amd64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true}, CapabilityRevision: 1,
		CapabilityObservedAt: h.clock.Now(), MissingCapabilities: []string{}}
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/nodes/register", registration)
	if status != http.StatusOK {
		t.Fatalf("register ordinary node status=%d body=%s", status, body)
	}
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/ordinary/heartbeat",
		policyHeartbeatRequest(registration))
	if status != http.StatusOK {
		t.Fatalf("ordinary heartbeat status=%d body=%s", status, body)
	}
	if bytes.Contains(body, []byte("computer_policy")) {
		t.Fatalf("ordinary heartbeat acquired Computer policy bytes: %s", body)
	}
	var issued int
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM computer_policy_issued`).Scan(&issued); err != nil || issued != 0 {
		t.Fatalf("ordinary heartbeat policy writes=%d err=%v", issued, err)
	}
}

func TestComputerPolicyWatchUsesInjectedClockAndBoundedWait(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{}, map[string]NodePolicy{
		"computer-node": DefaultNodePolicy(contract.StableNodeTagPrefix + "computer-node"),
	})
	if _, err := NewServer(h.server.fabric, h.store, ServerConfig{
		ComputerPolicyFreshness: 2 * ComputerPolicyClientTimeout,
		ComputerPolicyWatchWait: ComputerPolicyClientTimeout,
	}); err == nil {
		t.Fatal("server accepted a watch wait that reaches the shared client timeout")
	}
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	admin := fabric.Identity{FabricID: "fabric-test", UserID: "person-admin", DeviceID: "device-admin"}
	challenge, err := h.store.InitiateAdminBootstrap(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	policy, err := h.store.BootstrapAdmin(context.Background(), admin, challenge.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.store.CreateComputer(context.Background(), CreateComputerRequest{
		Name: "watch-clock", Spec: computerCapabilityJobSpec("computer:watch-clock"), Actor: "operator"}); err != nil {
		t.Fatal(err)
	}
	agent := h.client(fabric.Identity{NodeID: "fabric-computer-node", Tags: []string{DefaultAgentPrincipalTag}})
	result := make(chan []byte, 1)
	go func() {
		status, _, body := h.do(agent, http.MethodGet, fmt.Sprintf(
			"/v1/agent/nodes/%s/computer-policy?boot_session_id=%s&after_revision=%d",
			node.NodeID, node.BootSessionID, policy.Revision), nil)
		if status != http.StatusOK {
			t.Errorf("watch status=%d body=%s", status, body)
		}
		result <- body
	}()
	h.clock.waitForTimers(t, 1)
	select {
	case body := <-result:
		t.Fatalf("watch returned before injected deadline: %s", body)
	default:
	}
	h.clock.Advance(DefaultComputerPolicyWatchWait)
	select {
	case body := <-result:
		if !bytes.Contains(body, []byte("snapshot_digest")) {
			t.Fatalf("watch response = %s", body)
		}
	case <-time.After(time.Second):
		t.Fatal("watch did not wake from injected clock")
	}
}

func TestComputerPolicyFailureDoesNotFailHeartbeat(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{}, map[string]NodePolicy{
		"computer-node": DefaultNodePolicy(contract.StableNodeTagPrefix + "computer-node"),
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	if _, _, err := h.store.CreateComputer(context.Background(), CreateComputerRequest{
		Name: "policy-error", Spec: computerCapabilityJobSpec("computer:policy-error"), Actor: "operator"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec(`DROP TABLE computer_policy_issued`); err != nil {
		t.Fatal(err)
	}
	agent := h.client(fabric.Identity{NodeID: "fabric-computer-node", Tags: []string{DefaultAgentPrincipalTag}})
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/nodes/computer-node/heartbeat",
		policyHeartbeatRequest(node.NodeRegistration))
	if status != http.StatusOK || bytes.Contains(body, []byte("computer_policy")) {
		t.Fatalf("heartbeat with policy failure status=%d body=%s", status, body)
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
	if _, err := h.store.ObserveAuthenticatedPerson(context.Background(), alice); err != nil {
		t.Fatal(err)
	}
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
		ComputerGrantMutationRequest{PolicyRevision: policy.Revision, Permission: ComputerGrantView,
			IdempotencyKey: "under-admin"})
	if err != nil {
		t.Fatal(err)
	}
	removed, err := h.store.RemoveAdmin(context.Background(), bob, alice.UserID, grant.Grant.PolicyRevision)
	if err != nil || len(removed.Revocations) != 1 || removed.Revocations[0].State != ComputerPolicyRevocationPending {
		t.Fatalf("removed administrator policy = %#v err=%v", removed, err)
	}
	snapshot, err := h.store.IssueComputerPolicySnapshot(context.Background(), "fabric-computer-node", "fabric-test",
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
	audit, err := h.store.ListComputerPolicyAudit(context.Background(), bob, computer.ComputerID, "", 10)
	if err != nil || len(audit.Entries) != 2 || audit.Entries[1].PreviousPermission != ComputerGrantView {
		t.Fatalf("administrator removal audit = %#v err=%v", audit, err)
	}
}

func TestComputerGrantRequiresObservedPersonAndAddressesFabric(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{}, map[string]NodePolicy{
		"computer-node": DefaultNodePolicy(contract.StableNodeTagPrefix + "computer-node"),
	})
	admin := fabric.Identity{FabricID: "fabric-current", UserID: "person-admin", DeviceID: "device-admin"}
	challenge, err := h.store.InitiateAdminBootstrap(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	policy, err := h.store.BootstrapAdmin(context.Background(), admin, challenge.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	computer, _, err := h.store.CreateComputer(context.Background(), CreateComputerRequest{
		Name: "person-proof", Spec: computerCapabilityJobSpec("computer:person-proof"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	request := ComputerGrantMutationRequest{PolicyRevision: policy.Revision,
		Permission: ComputerGrantView, IdempotencyKey: "unproven"}
	if _, err := h.store.MutateComputerGrant(context.Background(), admin, computer.ComputerID, "person-viewer", request); errorCode(err) != contract.ErrorPersonIdentityRequired {
		t.Fatalf("unproven grant error = %v, want %q", err, contract.ErrorPersonIdentityRequired)
	}
	viewer := fabric.Identity{FabricID: admin.FabricID, UserID: "person-viewer", DeviceID: "device-viewer"}
	if _, err := h.store.ObserveAuthenticatedPerson(context.Background(), viewer); err != nil {
		t.Fatal(err)
	}
	request.IdempotencyKey = "proven"
	granted, err := h.store.MutateComputerGrant(context.Background(), admin, computer.ComputerID, viewer.UserID, request)
	if err != nil || granted.Grant.FabricID != admin.FabricID {
		t.Fatalf("default-Fabric grant = %#v err=%v", granted, err)
	}
	foreign := fabric.Identity{FabricID: "fabric-old", UserID: viewer.UserID, DeviceID: "device-old"}
	if _, err := h.store.ObserveAuthenticatedPerson(context.Background(), foreign); err != nil {
		t.Fatal(err)
	}
	foreignGrant, err := h.store.MutateComputerGrant(context.Background(), admin, computer.ComputerID, foreign.UserID,
		ComputerGrantMutationRequest{PolicyRevision: granted.Grant.PolicyRevision, FabricID: foreign.FabricID,
			Permission: ComputerGrantControl, IdempotencyKey: "foreign-grant"})
	if err != nil || foreignGrant.Grant.FabricID != foreign.FabricID {
		t.Fatalf("explicit-Fabric grant = %#v err=%v", foreignGrant, err)
	}
	listed, err := h.store.ListComputerGrants(context.Background(), admin, computer.ComputerID)
	if err != nil || len(listed.Grants) != 2 {
		t.Fatalf("cross-Fabric grant list = %#v err=%v", listed, err)
	}
	revoked, err := h.store.MutateComputerGrant(context.Background(), admin, computer.ComputerID, foreign.UserID,
		ComputerGrantMutationRequest{PolicyRevision: foreignGrant.Grant.PolicyRevision, FabricID: foreign.FabricID,
			Permission: ComputerGrantNone, IdempotencyKey: "foreign-revoke"})
	if err != nil || revoked.Grant.FabricID != foreign.FabricID || revoked.Grant.Permission != ComputerGrantNone {
		t.Fatalf("foreign-Fabric revocation = %#v err=%v", revoked, err)
	}
	newPerson := fabric.Identity{FabricID: admin.FabricID, UserID: "person-none", DeviceID: "device-none"}
	if _, err := h.store.ObserveAuthenticatedPerson(context.Background(), newPerson); err != nil {
		t.Fatal(err)
	}
	none, err := h.store.MutateComputerGrant(context.Background(), admin, computer.ComputerID, newPerson.UserID,
		ComputerGrantMutationRequest{PolicyRevision: revoked.Grant.PolicyRevision,
			Permission: ComputerGrantNone, IdempotencyKey: "explicit-none"})
	if err != nil || none.Grant.Permission != ComputerGrantNone {
		t.Fatalf("first explicit none = %#v err=%v", none, err)
	}
	deleted, err := h.store.DeleteForeignComputerGrant(context.Background(), admin, computer.ComputerID, foreign.UserID,
		ComputerGrantDeleteRequest{PolicyRevision: none.Grant.PolicyRevision, FabricID: foreign.FabricID,
			IdempotencyKey: "delete-foreign"})
	if err != nil || deleted.Grant.FabricID != foreign.FabricID {
		t.Fatalf("foreign-Fabric deletion = %#v err=%v", deleted, err)
	}
	if _, err := h.store.DeleteForeignComputerGrant(context.Background(), admin, computer.ComputerID, viewer.UserID,
		ComputerGrantDeleteRequest{PolicyRevision: deleted.Grant.PolicyRevision, FabricID: admin.FabricID,
			IdempotencyKey: "delete-current"}); errorCode(err) != contract.ErrorInvalidRequest {
		t.Fatalf("current-Fabric deletion error = %v, want %q", err, contract.ErrorInvalidRequest)
	}
	var foreignRows int
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM computer_grants WHERE computer_id=? AND fabric_id=? AND user_id=?`,
		computer.ComputerID, foreign.FabricID, foreign.UserID).Scan(&foreignRows); err != nil || foreignRows != 0 {
		t.Fatalf("foreign-Fabric rows=%d err=%v", foreignRows, err)
	}
	operator := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
	status, _, body := h.do(operator, http.MethodGet, "/v1/computers/"+computer.ComputerID, nil)
	if status != http.StatusOK {
		t.Fatalf("read Computer status=%d body=%s", status, body)
	}
	var redacted Computer
	if err := json.Unmarshal(body, &redacted); err != nil || len(redacted.Grants) != 0 {
		t.Fatalf("client Computer grant projection = %#v err=%v", redacted.Grants, err)
	}
	removed, err := h.store.RemoveComputer(context.Background(), computer.ComputerID, ComputerRemoveRequest{
		ComputerMutationPrecondition: computerPrecondition(computer, "remove-policy-computer"),
	})
	if err != nil || len(removed.Grants) != 0 {
		t.Fatalf("removed Computer grants = %#v err=%v", removed.Grants, err)
	}
	var remaining int
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM computer_grants WHERE computer_id=?`, computer.ComputerID).Scan(&remaining); err != nil || remaining != 0 {
		t.Fatalf("removed Computer grant rows=%d err=%v", remaining, err)
	}
}

func TestAdminResetDeniesEveryGranteeOnEveryComputer(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{}, map[string]NodePolicy{
		"computer-node": DefaultNodePolicy(contract.StableNodeTagPrefix + "computer-node"),
	})
	admin := fabric.Identity{FabricID: "fabric-test", UserID: "person-admin", DeviceID: "device-admin"}
	viewer := fabric.Identity{FabricID: "fabric-test", UserID: "person-viewer", DeviceID: "device-viewer"}
	if _, err := h.store.ObserveAuthenticatedPerson(context.Background(), viewer); err != nil {
		t.Fatal(err)
	}
	challenge, err := h.store.InitiateAdminBootstrap(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	policy, err := h.store.BootstrapAdmin(context.Background(), admin, challenge.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	computers := make([]Computer, 0, 2)
	for index := 0; index < 2; index++ {
		computer, _, err := h.store.CreateComputer(context.Background(), CreateComputerRequest{
			Name: fmt.Sprintf("reset-%d", index), Spec: computerCapabilityJobSpec(fmt.Sprintf("computer:reset:%d", index)), Actor: "operator"})
		if err != nil {
			t.Fatal(err)
		}
		computers = append(computers, computer)
	}
	grant, err := h.store.MutateComputerGrant(context.Background(), admin, computers[0].ComputerID, viewer.UserID,
		ComputerGrantMutationRequest{PolicyRevision: policy.Revision, Permission: ComputerGrantControl, IdempotencyKey: "before-reset"})
	if err != nil {
		t.Fatal(err)
	}
	reset, err := h.store.ResetAdminPolicy(context.Background())
	if err != nil || reset.Revision != grant.Grant.PolicyRevision+1 || len(reset.Revocations) != 4 {
		t.Fatalf("reset policy = %#v err=%v", reset, err)
	}
	for _, computer := range computers {
		for _, subject := range []fabric.Identity{admin, viewer} {
			var permission ComputerGrantPermission
			if err := h.store.db.QueryRow(`SELECT permission FROM computer_grants WHERE computer_id=? AND fabric_id=? AND user_id=?`,
				computer.ComputerID, subject.FabricID, subject.UserID).Scan(&permission); err != nil || permission != ComputerGrantNone {
				t.Fatalf("reset grant %s/%s = %q err=%v", computer.ComputerID, subject.UserID, permission, err)
			}
		}
	}
}

func TestReplacementBootMustOutliveOlderPolicyLease(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{}, map[string]NodePolicy{
		"computer-node": DefaultNodePolicy(contract.StableNodeTagPrefix + "computer-node"),
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	admin := fabric.Identity{FabricID: "fabric-test", UserID: "person-admin", DeviceID: "device-admin"}
	viewer := fabric.Identity{FabricID: "fabric-test", UserID: "person-viewer", DeviceID: "device-viewer"}
	if _, err := h.store.ObserveAuthenticatedPerson(context.Background(), viewer); err != nil {
		t.Fatal(err)
	}
	challenge, err := h.store.InitiateAdminBootstrap(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	policy, err := h.store.BootstrapAdmin(context.Background(), admin, challenge.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	computer, _, err := h.store.CreateComputer(context.Background(), CreateComputerRequest{
		Name: "boot-fence", Spec: computerCapabilityJobSpec("computer:boot-fence"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	grant, err := h.store.MutateComputerGrant(context.Background(), admin, computer.ComputerID, viewer.UserID,
		ComputerGrantMutationRequest{PolicyRevision: policy.Revision, Permission: ComputerGrantControl, IdempotencyKey: "control"})
	if err != nil {
		t.Fatal(err)
	}
	oldSnapshot, err := h.store.IssueComputerPolicySnapshot(context.Background(), "fabric-computer-node", admin.FabricID,
		node.NodeID, node.BootSessionID, time.Minute)
	if err != nil || oldSnapshot == nil {
		t.Fatalf("old boot snapshot = %#v err=%v", oldSnapshot, err)
	}
	if err := h.store.AcknowledgeComputerPolicyInstallation(context.Background(), "fabric-computer-node", acknowledgementFor(*oldSnapshot)); err != nil {
		t.Fatal(err)
	}
	revoked, err := h.store.MutateComputerGrant(context.Background(), admin, computer.ComputerID, viewer.UserID,
		ComputerGrantMutationRequest{PolicyRevision: grant.Grant.PolicyRevision, Permission: ComputerGrantNone, IdempotencyKey: "revoke"})
	if err != nil {
		t.Fatal(err)
	}
	replacement := node.NodeRegistration
	replacement.BootSessionID = "boot-replacement"
	replacement.CapabilityRevision = 1
	replacement.CapabilityObservedAt = h.clock.Now()
	node, err = h.store.RegisterNode(context.Background(), fabric.Identity{NodeID: "fabric-computer-node"}, replacement,
		DefaultNodePolicy(contract.StableNodeTagPrefix+"computer-node"), true)
	if err != nil {
		t.Fatal(err)
	}
	newSnapshot, err := h.store.IssueComputerPolicySnapshot(context.Background(), "fabric-computer-node", admin.FabricID,
		node.NodeID, node.BootSessionID, time.Minute)
	if err != nil || newSnapshot == nil {
		t.Fatalf("replacement snapshot = %#v err=%v", newSnapshot, err)
	}
	if err := h.store.AcknowledgeComputerPolicyInstallation(context.Background(), "fabric-computer-node", acknowledgementFor(*newSnapshot)); err != nil {
		t.Fatal(err)
	}
	status, err := h.store.GetComputerPolicyRevocation(context.Background(), admin, revoked.Grant.PolicyRevision,
		computer.ComputerID, viewer.FabricID, viewer.UserID)
	if err != nil || status.State != ComputerPolicyRevocationPending {
		t.Fatalf("replacement boot bypassed old lease: %#v err=%v", status, err)
	}
	h.clock.Advance(time.Minute)
	status, err = h.store.GetComputerPolicyRevocation(context.Background(), admin, revoked.Grant.PolicyRevision,
		computer.ComputerID, viewer.FabricID, viewer.UserID)
	if err != nil || status.State != ComputerPolicyRevocationCompleted {
		t.Fatalf("expired old boot lease status = %#v err=%v", status, err)
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
	viewer := fabric.Identity{FabricID: "fabric-test", UserID: "person-viewer", DeviceID: "device-viewer"}
	if _, err := h.store.ObserveAuthenticatedPerson(context.Background(), viewer); err != nil {
		t.Fatal(err)
	}
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
	controlSnapshot, err := h.store.IssueComputerPolicySnapshot(context.Background(), "fabric-computer-node", "fabric-test",
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
	currentSnapshot, err := h.store.IssueComputerPolicySnapshot(context.Background(), "fabric-computer-node", "fabric-test",
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
	restartSnapshot, err := h.store.IssueComputerPolicySnapshot(context.Background(), "fabric-computer-node", "fabric-test",
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
	revokedSnapshot, err := h.store.IssueComputerPolicySnapshot(context.Background(), "fabric-computer-node", "fabric-test",
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

func policyHeartbeatRequest(registration contract.NodeRegistration) HeartbeatRequest {
	return HeartbeatRequest{BootSessionID: registration.BootSessionID, Capabilities: registration.Capabilities,
		CapabilityRevision: registration.CapabilityRevision, CapabilityObservedAt: registration.CapabilityObservedAt,
		MissingCapabilities: registration.MissingCapabilities}
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
