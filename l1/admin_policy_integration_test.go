package l1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
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
	incompletePerson := h.client(fabric.Identity{NodeID: "plain-node-person"})

	status, _, body := h.do(incomplete, http.MethodGet, "/v1/admin-policy", nil)
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorPrincipalForbidden)
	status, _, body = h.do(incompletePerson, http.MethodGet, "/v1/admin-policy", nil)
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
	if storedNonceHash == challenge.Nonce || storedNonceHash != bootstrapNonceHash(challenge.Nonce, h.store.deploymentID, 1) {
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
	status, _, body = h.do(otherPersonSameDevice, http.MethodGet, "/v1/admin-policy", nil)
	if status != http.StatusOK || string(body) != "{\"revision\":1}\n" {
		t.Fatalf("nonadmin policy = status %d body=%s", status, body)
	}
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

func TestMachinePrincipalRejectedFromEveryPersonRoute(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{}, nil)
	identity := fabric.Identity{
		NodeID: "agent-node", UserID: "enroller-person", DeviceID: "agent-device",
		Tags: []string{DefaultAgentPrincipalTag},
	}
	client := h.client(identity)
	rows := []struct {
		method string
		path   string
		body   any
	}{
		{http.MethodGet, "/v1/whoami", nil},
		{http.MethodPost, "/v1/admin-bootstrap", BootstrapAdminRequest{Nonce: "forged"}},
		{http.MethodGet, "/v1/admin-policy", nil},
		{http.MethodGet, "/v1/admin-policy/audit", nil},
		{http.MethodPut, "/v1/admin-policy/admins/attacker", AdminPolicyMutationRequest{PolicyRevision: 1}},
		{http.MethodDelete, "/v1/admin-policy/admins/attacker", AdminPolicyMutationRequest{PolicyRevision: 1}},
		{http.MethodGet, "/v1/computers/computer-forged/grants", nil},
		{http.MethodPut, "/v1/computers/computer-forged/grants/person-forged", ComputerGrantMutationRequest{
			PolicyRevision: 1, Permission: ComputerGrantView, IdempotencyKey: "forged"}},
		{http.MethodDelete, "/v1/computers/computer-forged/grants/person-forged", ComputerGrantDeleteRequest{
			PolicyRevision: 1, FabricID: "fabric-old", IdempotencyKey: "forged-delete"}},
		{http.MethodGet, "/v1/computers/computer-forged/grants/audit", nil},
		{http.MethodGet, "/v1/computers/computer-forged/revocations/1", nil},
		{http.MethodGet, "/v1/computers/computer-forged/submission", nil},
		{http.MethodPut, "/v1/computers/computer-forged/submission", ComputerSubmissionRequest{
			PolicyRevision: 1, SubmitMaxInflight: 20, IdempotencyKey: "forged-submission"}},
	}
	for _, row := range rows {
		status, _, body := h.do(client, row.method, row.path, row.body)
		assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorPrincipalForbidden)
	}
	machine := h.client(fabric.Identity{
		NodeID: "machine-node", UserID: "enroller-person", DeviceID: "machine-device",
		Kind: fabric.IdentityKindMachine,
	})
	status, _, body := h.do(machine, http.MethodGet, "/v1/admin-policy", nil)
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorPrincipalForbidden)
}

func TestWhoAmIRecordsOnlyAuthenticatedPeople(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{}, nil)
	identity := fabric.Identity{UserID: "person-alice", DeviceID: "device-a"}
	client := h.client(identity)
	status, _, body := h.do(client, http.MethodGet, "/v1/whoami", nil)
	if status != http.StatusOK {
		t.Fatalf("whoami status=%d body=%s", status, body)
	}
	var observed AuthenticatedPerson
	if err := json.Unmarshal(body, &observed); err != nil {
		t.Fatal(err)
	}
	if observed.UserID != identity.UserID || observed.DeviceID != identity.DeviceID || observed.FabricID == "" {
		t.Fatalf("whoami observation = %#v", observed)
	}
	var count int
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM authenticated_people WHERE fabric_id=? AND user_id=?`,
		observed.FabricID, observed.UserID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("authenticated person rows=%d err=%v", count, err)
	}
}

func TestPlainFabricPersonRoutesRequireDevelopmentOverride(t *testing.T) {
	h := newIntegrationHarnessWithPersonIdentityMode(t, StoreOptions{}, nil, false)
	client := h.client(fabric.Identity{UserID: "person-alice", DeviceID: "device-a"})
	status, _, body := h.do(client, http.MethodGet, "/v1/admin-policy", nil)
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorPrincipalForbidden)
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
	identity := fabric.Identity{FabricID: "fabric-one", UserID: "person-alice", DeviceID: "device-a"}
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
	identity := fabric.Identity{FabricID: "fabric-one", UserID: "person-alice", DeviceID: "device-a"}
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

func TestAdminAuthorityIsScopedToIssuingFabric(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{}, nil)
	admin := fabric.Identity{FabricID: "fabric-one", UserID: "person-alice", DeviceID: "device-a"}
	challenge, err := h.store.InitiateAdminBootstrap(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	policy, err := h.store.BootstrapAdmin(context.Background(), admin, challenge.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	repointed := admin
	repointed.FabricID = "fabric-two"
	if _, err := h.store.AddAdmin(context.Background(), repointed, "person-bob", policy.Revision); errorCode(err) != contract.ErrorAdminRequired {
		t.Fatalf("repointed Fabric mutation error = %v, want %q", err, contract.ErrorAdminRequired)
	}
}

func TestAdminPolicyBoundAndAuditFailureRollback(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{}, nil)
	identity := fabric.Identity{FabricID: "fabric-one", UserID: "person-alice", DeviceID: "device-a"}
	challenge, err := h.store.InitiateAdminBootstrap(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	policy, err := h.store.BootstrapAdmin(context.Background(), identity, challenge.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec(`CREATE TRIGGER fail_add_audit BEFORE INSERT ON admin_policy_audit
		WHEN NEW.operation='add' BEGIN SELECT RAISE(ABORT, 'forced audit failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.AddAdmin(context.Background(), identity, "person-bob", policy.Revision); errorCode(err) != contract.ErrorInternal {
		t.Fatalf("audit failure error = %v, want internal", err)
	}
	assertAdminPolicyRevisionAndAuditCount(t, h, 1, 1)
	current, err := h.store.GetAdminPolicy(context.Background())
	if err != nil || len(current.Admins) != 1 {
		t.Fatalf("audit failure changed membership = %#v err=%v", current, err)
	}
	if _, err := h.store.db.Exec(`DROP TRIGGER fail_add_audit`); err != nil {
		t.Fatal(err)
	}
	for index := 2; index <= MaxAdministrators; index++ {
		current, err = h.store.AddAdmin(context.Background(), identity,
			fmt.Sprintf("person-%02d", index), current.Revision)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.store.AddAdmin(context.Background(), identity, "person-over-limit", current.Revision); errorCode(err) != contract.ErrorCapacityExhausted {
		t.Fatalf("over-limit error = %v, want %q", err, contract.ErrorCapacityExhausted)
	}
	if _, err := h.store.db.Exec(`INSERT INTO admins(fabric_id, user_id, added_revision, added_ns)
		VALUES('other-fabric', 'schema-bypass', 999, 0)`); err == nil {
		t.Fatal("admins schema admitted a thirty-third member")
	}
}

func TestLocalAdminResetReopensBootstrapAndIsAudited(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{}, nil)
	typo := fabric.Identity{FabricID: "fabric-one", UserID: "typo-person", DeviceID: "device-a"}
	challenge, err := h.store.InitiateAdminBootstrap(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.BootstrapAdmin(context.Background(), typo, challenge.Nonce); err != nil {
		t.Fatal(err)
	}
	reset, err := h.store.ResetAdminPolicy(context.Background())
	if err != nil || reset.Revision != 2 || len(reset.Admins) != 0 {
		t.Fatalf("reset policy = %#v err=%v", reset, err)
	}
	nextChallenge, err := h.store.InitiateAdminBootstrap(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	correct := fabric.Identity{FabricID: "fabric-one", UserID: "person-alice", DeviceID: "device-b"}
	policy, err := h.store.BootstrapAdmin(context.Background(), correct, nextChallenge.Nonce)
	if err != nil || policy.Revision != 3 || len(policy.Admins) != 1 {
		t.Fatalf("post-reset bootstrap = %#v err=%v", policy, err)
	}
	var operation AdminPolicyOperation
	if err := h.store.db.QueryRow(`SELECT operation FROM admin_policy_audit WHERE revision=2`).Scan(&operation); err != nil {
		t.Fatal(err)
	}
	if operation != AdminPolicyReset {
		t.Fatalf("reset audit operation = %q", operation)
	}
}

func TestBootstrapChallengeCannotRedeemFromClonedDatabase(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.sqlite")
	source, err := OpenStore(sourcePath, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	challenge, err := source.InitiateAdminBootstrap(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	clonePath := filepath.Join(directory, "clone.sqlite")
	if err := os.WriteFile(clonePath, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	clone, err := OpenStore(clonePath, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer clone.Close()
	identity := fabric.Identity{FabricID: "fabric-one", UserID: "person-alice", DeviceID: "device-a"}
	if _, err := clone.BootstrapAdmin(context.Background(), identity, challenge.Nonce); errorCode(err) != contract.ErrorAdminBootstrapInvalid {
		t.Fatalf("cloned challenge error = %v, want %q", err, contract.ErrorAdminBootstrapInvalid)
	}
}
