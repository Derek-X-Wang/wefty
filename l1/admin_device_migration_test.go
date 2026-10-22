package l1

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestAdminPersonDeviceMigrationPreservesLegacyRosterAndAudit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.sqlite")
	store, err := OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	identity := fabric.Identity{FabricID: "issuer", UserID: "owner", DeviceID: "old-observation"}
	challenge, err := store.InitiateAdminBootstrap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.BootstrapAdmin(t.Context(), identity, challenge.Nonce); err != nil {
		t.Fatal(err)
	}
	before, err := store.ListAdminPolicyAudit(t.Context(), identity, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := json.Marshal(before)
	if err != nil {
		t.Fatal(err)
	}
	// Reconstruct the exact pre-binding table shapes, preserving their rows.
	_, err = store.db.Exec(`ALTER TABLE admins DROP COLUMN device_id;
 CREATE TABLE admin_policy_audit_legacy (
 revision INTEGER PRIMARY KEY CHECK(revision>0),
 operation TEXT NOT NULL CHECK(operation IN ('bootstrap','add','remove','reset')),
 actor_kind TEXT NOT NULL CHECK(actor_kind IN ('fabric_person','local_operator')),
 actor_fabric_id TEXT NOT NULL,actor_user_id TEXT NOT NULL,actor_device_id TEXT NOT NULL,
 subject_fabric_id TEXT NOT NULL,subject_user_id TEXT NOT NULL,created_ns INTEGER NOT NULL);
 INSERT INTO admin_policy_audit_legacy SELECT revision,operation,actor_kind,actor_fabric_id,actor_user_id,actor_device_id,subject_fabric_id,subject_user_id,created_ns FROM admin_policy_audit;
 DROP TABLE admin_policy_audit;
 ALTER TABLE admin_policy_audit_legacy RENAME TO admin_policy_audit;`)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		store, err = OpenStore(path, StoreOptions{})
		if err != nil {
			t.Fatal(err)
		}
		page, err := store.ListAdminPolicyAudit(t.Context(), identity, "", 10)
		if err != nil {
			t.Fatal(err)
		}
		got, err := json.Marshal(page)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(expected, got) {
			t.Fatalf("migration changed legacy audit: %s -> %s", expected, got)
		}
		policy, err := store.GetAdminPolicy(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(policy.Admins) != 1 || policy.Admins[0].DeviceID != "" {
			t.Fatalf("migration bound legacy admin: %#v", policy)
		}
		f := &identityAnswerFabric{identity: identity}
		server, err := NewServer(f, store, ServerConfig{})
		if err != nil {
			t.Fatal(err)
		}
		w := httptest.NewRecorder()
		server.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/v1/admin-policy", nil))
		if w.Code != 403 {
			t.Fatalf("migrated legacy admin has authority: %d %s", w.Code, w.Body.String())
		}
		if i == 1 {
			rebound, err := store.RebindAdminDevice(t.Context(), "owner", "replacement-device")
			if err != nil || rebound.Revision != 2 {
				t.Fatalf("migrated rebind: %#v %v", rebound, err)
			}
			f.identity.DeviceID = "replacement-device"
			w = httptest.NewRecorder()
			server.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/v1/admin-policy/audit", nil))
			if w.Code != 200 {
				t.Fatalf("migrated audit: %d %s", w.Code, w.Body.String())
			}
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
