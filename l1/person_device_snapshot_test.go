package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestAuthenticatedComputerPolicyCarriesAdminDeviceBinding(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{}, map[string]NodePolicy{"computer-node": DefaultNodePolicy(contract.StableNodeTagPrefix + "computer-node")})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{"kind:oci": true, "cgroup_v2": true, "computer": true}, []string{contract.StableNodeTagPrefix + "computer-node"})
	if _, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "bound-policy", Spec: computerCapabilityJobSpec("bound-policy"), Actor: "test"}); err != nil {
		t.Fatal(err)
	}
	f := &identityAnswerFabric{identity: fabric.Identity{FabricID: "issuer", UserID: "owner", DeviceID: "enrolled-device"}}
	server, err := NewServer(f, h.store, ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	challenge, err := h.store.InitiateAdminBootstrap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/v1/admin-bootstrap", strings.NewReader(`{"nonce":"`+challenge.Nonce+`"}`)))
	if w.Code != 201 {
		t.Fatalf("bootstrap: %d %s", w.Code, w.Body.String())
	}
	f.identity = fabric.Identity{FabricID: "issuer", NodeID: "fabric-computer-node", DeviceID: "node-device", Kind: fabric.IdentityKindMachine, Tags: []string{DefaultAgentPrincipalTag}}
	heartbeat, _ := json.Marshal(policyHeartbeatRequest(node.NodeRegistration))
	for _, route := range []struct {
		method, path, body string
		heartbeat          bool
	}{
		{"GET", "/v1/agent/nodes/computer-node/computer-policy?boot_session_id=" + node.BootSessionID + "&after_revision=0", "", false},
		{"POST", "/v1/agent/nodes/computer-node/heartbeat", string(heartbeat), true},
	} {
		w = httptest.NewRecorder()
		server.Handler().ServeHTTP(w, httptest.NewRequest(route.method, route.path, strings.NewReader(route.body)))
		if w.Code != 200 {
			t.Fatalf("policy: %d %s", w.Code, w.Body.String())
		}
		raw := w.Body.Bytes()
		if route.heartbeat {
			var response struct {
				Policy json.RawMessage `json:"computer_policy"`
			}
			if err := json.Unmarshal(raw, &response); err != nil {
				t.Fatal(err)
			}
			raw = response.Policy
		}
		var wire struct {
			Required bool `json:"admin_device_required"`
			Admins   []struct {
				Device string `json:"device_id"`
			} `json:"admins"`
		}
		if err := json.Unmarshal(raw, &wire); err != nil {
			t.Fatal(err)
		}
		if !wire.Required || len(wire.Admins) != 1 || wire.Admins[0].Device != "enrolled-device" {
			t.Fatalf("authenticated snapshot omitted binding: %s", raw)
		}
		var snapshot ComputerPolicySnapshot
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			t.Fatal(err)
		}
		if err := ValidateComputerPolicySnapshot(snapshot); err != nil {
			t.Fatal(err)
		}
		if path := os.Getenv("WEFTY_T1090_AUTH_SNAPSHOT"); path != "" {
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestAuthenticatedPersonStoreDiscoveryChecksAdminDevice(t *testing.T) {
	f := &identityAnswerFabric{identity: fabric.Identity{FabricID: "issuer", UserID: "owner", DeviceID: "enrolled-device"}}
	s := newIdentityAnswerServer(t, f)
	challenge, err := s.store.InitiateAdminBootstrap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/v1/admin-bootstrap", strings.NewReader(`{"nonce":"`+challenge.Nonce+`"}`)))
	if w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	computer, _, err := s.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "bound-discovery", Spec: computerCapabilityJobSpec("bound-discovery"), Actor: "test"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(t.Context(), personDeviceBindingContextKey{}, true)
	wrong := f.identity
	wrong.DeviceID = "other-device"
	if _, err := s.store.GetComputerTakeoverAvailability(ctx, wrong, computer.ComputerID); errorCode(err) != contract.ErrorPersonDeviceMismatch {
		t.Fatalf("takeover bypassed binding: %v", err)
	}
	for _, adminRequired := range []bool{false, true} {
		if _, err := s.store.ResolvePersonComputerHandle(ctx, wrong, computer.Name, adminRequired); errorCode(err) != contract.ErrorPersonDeviceMismatch {
			t.Fatalf("handle bypassed binding: adminRequired=%t err=%v", adminRequired, err)
		}
	}
}

func TestAdminAddHasExplicitDeviceParameter(t *testing.T) {
	store := newIdentityAnswerServer(t, &identityAnswerFabric{}).store
	if _, ok := any(store).(interface {
		AddAdmin(context.Context, fabric.Identity, string, int64, string) (AdminPolicy, error)
	}); !ok {
		t.Fatal("AddAdmin must accept one explicit deviceID parameter")
	}
}

func TestAuthenticatedPersonSnapshotUnavailablePrecedesObservation(t *testing.T) {
	f := &identityAnswerFabric{identity: fabric.Identity{FabricID: "issuer", UserID: "unobserved-person", DeviceID: "person-device"}}
	s := newIdentityAnswerServer(t, f)
	var held []*sql.Conn
	for i := 0; i < readSnapshotLimit; i++ {
		conn, err := s.store.readDB.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, conn)
		defer conn.Close()
	}
	for _, route := range personDeviceRefusalRoutes(t) {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest(route.method, route.path, strings.NewReader(`{"policy_revision":1}`)))
		if w.Code != 503 || !strings.Contains(w.Body.String(), `"code":"unavailable"`) {
			t.Fatalf("snapshot-unavailable refusal: %s %s: %d %s", route.method, route.path, w.Code, w.Body.String())
		}
	}
	var people int
	if err := s.store.db.QueryRow(`SELECT COUNT(*) FROM authenticated_people`).Scan(&people); err != nil {
		t.Fatal(err)
	}
	if people != 0 {
		t.Fatalf("refusal used person-observation fallback: %d", people)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/v1/whoami", nil))
	if w.Code != 200 {
		t.Fatalf("whoami exception: %d %s", w.Code, w.Body.String())
	}
	if err := s.store.db.QueryRow(`SELECT COUNT(*) FROM authenticated_people`).Scan(&people); err != nil || people != 1 {
		t.Fatalf("whoami did not retain observation fallback: people=%d err=%v", people, err)
	}
}
