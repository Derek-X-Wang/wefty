package l1

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestPersonAdminCheckContract(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	ctx := context.Background()
	admin := h.client(fabric.Identity{NodeID: "admin-device", UserID: "admin", DeviceID: "device"})
	status, _, body := h.do(admin, http.MethodGet, "/v1/whoami", nil)
	if status != http.StatusOK {
		t.Fatalf("whoami=%d %s", status, body)
	}
	var who AuthenticatedPerson
	if err := json.Unmarshal(body, &who); err != nil {
		t.Fatal(err)
	}
	challenge, err := h.store.InitiateAdminBootstrap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(admin, http.MethodPost, "/v1/admin-bootstrap", BootstrapAdminRequest{Nonce: challenge.Nonce})
	if status != http.StatusCreated {
		t.Fatalf("bootstrap=%d %s", status, body)
	}
	ledger := h.client(fabric.Identity{NodeID: "run-ledger", Tags: []string{DefaultClientPrincipalTag}})
	evidence := PersonAdminCheckRequest{FabricID: who.FabricID, UserID: who.UserID, DeviceID: who.DeviceID}
	read := func(request PersonAdminCheckRequest, current bool, revision int64) {
		t.Helper()
		status, _, body := h.do(ledger, http.MethodPost, "/v1/person-admin-check", request)
		var answer PersonAdminCheck
		if err := json.Unmarshal(body, &answer); err != nil || status != http.StatusOK || answer.CurrentAdmin != current || answer.PolicyRevision != revision {
			t.Fatalf("admin check=%d %s want %v revision %d: %v", status, body, current, revision, err)
		}
	}
	read(evidence, true, 1)
	another := evidence
	another.DeviceID = "another-device"
	read(another, true, 1)
	another = evidence
	another.FabricID = "foreign-issuer"
	read(another, false, 1)
	another = evidence
	another.UserID = "non-admin"
	read(another, false, 1)
	for _, identity := range []fabric.Identity{
		{NodeID: "other-client", Tags: []string{DefaultClientPrincipalTag}},
		{NodeID: "run-ledger"},
		{NodeID: "admin-device", UserID: "admin", DeviceID: "device"},
	} {
		caller := h.client(identity)
		status, _, body = h.do(caller, http.MethodPost, "/v1/person-admin-check", evidence)
		if status != http.StatusForbidden {
			t.Fatalf("non-ledger read=%d %s", status, body)
		}
	}
	incomplete := evidence
	incomplete.DeviceID = ""
	status, _, body = h.do(ledger, http.MethodPost, "/v1/person-admin-check", incomplete)
	assertAPIError(t, status, body, http.StatusUnauthorized, contract.ErrorPersonIdentityRequired)
	status, _, body = h.do(ledger, http.MethodPost, "/v1/person-admin-check", map[string]any{"fabric_id": who.FabricID, "user_id": who.UserID, "device_id": who.DeviceID, "cancel_on_behalf": true})
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
	// Reads have not advanced policy or created audit/person authority rows.
	var revision, auditCount int64
	if err := h.store.db.QueryRow(`SELECT revision,(SELECT COUNT(*) FROM admin_policy_audit) FROM admin_policy WHERE singleton=1`).Scan(&revision, &auditCount); err != nil || revision != 1 || auditCount != 1 {
		t.Fatalf("read mutated policy: revision=%d audit=%d %v", revision, auditCount, err)
	}
	if _, err := h.store.ResetAdminPolicy(ctx); err != nil {
		t.Fatal(err)
	}
	read(evidence, false, 2)
}
