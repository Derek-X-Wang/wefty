package l1

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestAdminBootstrapAndPolicyContract(t *testing.T) {
	assertAdminBootstrapAndPolicyContract(t)
}

func assertAdminBootstrapAndPolicyContract(t *testing.T) {
	t.Helper()
	h := newIntegrationHarnessWithOptions(t, StoreOptions{AdminBootstrapTTL: 5 * time.Minute}, nil)
	adminDeviceOne := h.client(fabric.Identity{
		NodeID: "plain-node-a", UserID: "person-alice", DeviceID: "device-a", DisplayName: "Alice Old",
	})
	adminDeviceTwo := h.client(fabric.Identity{
		NodeID: "plain-node-b", UserID: "person-alice", DeviceID: "device-b", DisplayName: "Alice Renamed",
	})
	otherPersonSameDevice := h.client(fabric.Identity{
		NodeID: "plain-node-b", UserID: "person-mallory", DeviceID: "device-b", DisplayName: "Mallory",
	})
	incomplete := h.client(fabric.Identity{NodeID: "plain-node-a", Tags: []string{DefaultClientPrincipalTag}})

	status, _, body := h.do(incomplete, http.MethodGet, "/v1/admin-policy", nil)
	assertAPIError(t, status, body, http.StatusUnauthorized, contract.ErrorPersonIdentityRequired)
	status, _, body = h.do(adminDeviceOne, http.MethodPost, "/v1/admin-bootstrap", BootstrapAdminRequest{Nonce: "remote-first-caller"})
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorAdminBootstrapInvalid)

	challenge, err := h.store.InitiateAdminBootstrap(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var storedNonceHash string
	if err := h.store.db.QueryRow(`SELECT nonce_hash FROM admin_bootstrap_challenges WHERE singleton=1`).Scan(&storedNonceHash); err != nil {
		t.Fatal(err)
	}
	if storedNonceHash == challenge.Nonce || storedNonceHash != bootstrapNonceHash(challenge.Nonce) {
		t.Fatal("bootstrap challenge was not stored only as its hash")
	}
	status, _, body = h.do(adminDeviceOne, http.MethodPost, "/v1/admin-bootstrap", map[string]any{
		"nonce": challenge.Nonce, "user_id": "forged-person",
	})
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
	status, _, body = h.do(adminDeviceOne, http.MethodPost, "/v1/admin-bootstrap", BootstrapAdminRequest{Nonce: challenge.Nonce})
	if status != http.StatusCreated {
		t.Fatalf("bootstrap status = %d body=%s", status, body)
	}
	var policy AdminPolicy
	if err := json.Unmarshal(body, &policy); err != nil {
		t.Fatal(err)
	}
	if policy.Revision != 1 || len(policy.Admins) != 1 || policy.Admins[0].UserID != "person-alice" {
		t.Fatalf("bootstrapped policy = %#v", policy)
	}
	status, _, body = h.do(adminDeviceTwo, http.MethodPost, "/v1/admin-bootstrap", BootstrapAdminRequest{Nonce: challenge.Nonce})
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorAdminBootstrapClosed)

	status, _, body = h.do(otherPersonSameDevice, http.MethodPut,
		"/v1/admin-policy/admins/person-bob", AdminPolicyMutationRequest{PolicyRevision: policy.Revision})
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorAdminRequired)
	status, _, body = h.do(adminDeviceTwo, http.MethodPut,
		"/v1/admin-policy/admins/person-bob", map[string]any{
			"policy_revision": policy.Revision, "actor_device_id": "forged-device",
		})
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
	status, _, body = h.do(adminDeviceTwo, http.MethodPut,
		"/v1/admin-policy/admins/person-bob", AdminPolicyMutationRequest{PolicyRevision: policy.Revision})
	if status != http.StatusOK {
		t.Fatalf("add admin status = %d body=%s", status, body)
	}
	var withBob AdminPolicy
	if err := json.Unmarshal(body, &withBob); err != nil {
		t.Fatal(err)
	}
	if withBob.Revision != 2 || len(withBob.Admins) != 2 {
		t.Fatalf("policy after add = %#v", withBob)
	}

	status, _, body = h.do(adminDeviceOne, http.MethodDelete,
		"/v1/admin-policy/admins/person-bob", AdminPolicyMutationRequest{PolicyRevision: policy.Revision})
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorStalePolicyRevision)
	assertAdminPolicyRevisionAndAuditCount(t, h, 2, 2)

	status, _, body = h.do(adminDeviceOne, http.MethodDelete,
		"/v1/admin-policy/admins/person-alice", AdminPolicyMutationRequest{PolicyRevision: withBob.Revision})
	if status != http.StatusOK {
		t.Fatalf("remove first admin status = %d body=%s", status, body)
	}
	var onlyBob AdminPolicy
	if err := json.Unmarshal(body, &onlyBob); err != nil {
		t.Fatal(err)
	}
	if onlyBob.Revision != 3 || len(onlyBob.Admins) != 1 || onlyBob.Admins[0].UserID != "person-bob" {
		t.Fatalf("policy after removal = %#v", onlyBob)
	}
	bob := h.client(fabric.Identity{NodeID: "bob-node", UserID: "person-bob", DeviceID: "bob-device"})
	status, _, body = h.do(bob, http.MethodDelete,
		"/v1/admin-policy/admins/person-bob", AdminPolicyMutationRequest{PolicyRevision: onlyBob.Revision})
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorFinalAdmin)
	assertAdminPolicyRevisionAndAuditCount(t, h, 3, 3)

	status, _, body = h.do(bob, http.MethodGet, "/v1/admin-policy/audit?limit=2", nil)
	if status != http.StatusOK {
		t.Fatalf("first audit page status = %d body=%s", status, body)
	}
	var firstPage AdminPolicyAuditList
	if err := json.Unmarshal(body, &firstPage); err != nil {
		t.Fatal(err)
	}
	if len(firstPage.Entries) != 2 || firstPage.NextCursor == "" ||
		firstPage.Entries[0].Operation != AdminPolicyBootstrap ||
		firstPage.Entries[0].ActorUserID != "person-alice" ||
		firstPage.Entries[0].ActorDeviceID != "device-a" ||
		firstPage.Entries[1].ActorDeviceID != "device-b" {
		t.Fatalf("first audit page = %#v", firstPage)
	}
	status, _, body = h.do(bob, http.MethodGet,
		"/v1/admin-policy/audit?limit=2&cursor="+firstPage.NextCursor, nil)
	if status != http.StatusOK {
		t.Fatalf("second audit page status = %d body=%s", status, body)
	}
	var secondPage AdminPolicyAuditList
	if err := json.Unmarshal(body, &secondPage); err != nil {
		t.Fatal(err)
	}
	if len(secondPage.Entries) != 1 || secondPage.NextCursor != "" ||
		secondPage.Entries[0].Operation != AdminPolicyRemove ||
		secondPage.Entries[0].ActorUserID != "person-alice" ||
		secondPage.Entries[0].ActorDeviceID != "device-a" {
		t.Fatalf("second audit page = %#v", secondPage)
	}

	receipt := struct {
		PolicyRevision int64  `json:"policy_revision"`
		AdminUserID    string `json:"admin_user_id"`
		AuditRows      int    `json:"audit_rows"`
	}{PolicyRevision: onlyBob.Revision, AdminUserID: onlyBob.Admins[0].UserID,
		AuditRows: len(firstPage.Entries) + len(secondPage.Entries)}
	encodedReceipt, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("assertion-derived admin policy receipt: %s", encodedReceipt)
}

func assertAdminPolicyRevisionAndAuditCount(t *testing.T, h *integrationHarness, revision, rows int64) {
	t.Helper()
	var gotRevision, gotRows int64
	if err := h.store.db.QueryRow(`SELECT revision FROM admin_policy WHERE singleton=1`).Scan(&gotRevision); err != nil {
		t.Fatal(err)
	}
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM admin_policy_audit`).Scan(&gotRows); err != nil {
		t.Fatal(err)
	}
	if gotRevision != revision || gotRows != rows {
		t.Fatalf("policy revision/audit rows = %d/%d, want %d/%d", gotRevision, gotRows, revision, rows)
	}
}

func TestAdminBootstrapChallengeExpiryAndReplacement(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{AdminBootstrapTTL: time.Minute}, nil)
	identity := fabric.Identity{UserID: "person-alice", DeviceID: "device-a"}
	first, err := h.store.InitiateAdminBootstrap(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := h.store.InitiateAdminBootstrap(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.BootstrapAdmin(context.Background(), identity, first.Nonce); errorCode(err) != contract.ErrorAdminBootstrapInvalid {
		t.Fatalf("superseded challenge error = %v, want %q", err, contract.ErrorAdminBootstrapInvalid)
	}
	h.clock.Advance(time.Minute + time.Nanosecond)
	if _, err := h.store.BootstrapAdmin(context.Background(), identity, second.Nonce); errorCode(err) != contract.ErrorAdminBootstrapInvalid {
		t.Fatalf("expired challenge error = %v, want %q", err, contract.ErrorAdminBootstrapInvalid)
	}
	policy, err := h.store.GetAdminPolicy(context.Background())
	if err != nil || policy.Revision != 0 || len(policy.Admins) != 0 {
		t.Fatalf("failed challenges changed policy = %#v err=%v", policy, err)
	}
}

func TestAdminPolicyConcurrentCASHasOneAuditWinner(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{}, nil)
	identity := fabric.Identity{UserID: "person-alice", DeviceID: "device-a"}
	challenge, err := h.store.InitiateAdminBootstrap(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	policy, err := h.store.BootstrapAdmin(context.Background(), identity, challenge.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	var start sync.WaitGroup
	start.Add(1)
	for _, target := range []string{"person-bob", "person-carol"} {
		target := target
		go func() {
			start.Wait()
			_, err := h.store.AddAdmin(context.Background(), identity, target, policy.Revision)
			results <- err
		}()
	}
	start.Done()
	wins, stale := 0, 0
	for range 2 {
		err := <-results
		if err == nil {
			wins++
			continue
		}
		switch errorCode(err) {
		case contract.ErrorStalePolicyRevision:
			stale++
		default:
			t.Fatalf("concurrent mutation error = %v", err)
		}
	}
	if wins != 1 || stale != 1 {
		t.Fatalf("concurrent CAS outcomes = wins %d stale %d", wins, stale)
	}
	assertAdminPolicyRevisionAndAuditCount(t, h, 2, 2)
}
