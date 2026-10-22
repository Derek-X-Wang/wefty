package l1

import (
	"context"
	"encoding/json"
	"github.com/Derek-X-Wang/wefty/contract"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestAuthenticatedAdminBootstrapBindsRedeemingDevice(t *testing.T) {
	f := &identityAnswerFabric{identity: fabric.Identity{FabricID: "issuer", UserID: "owner", DeviceID: "enrolled-device"}}
	s := newIdentityAnswerServer(t, f)
	challenge, err := s.store.InitiateAdminBootstrap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/admin-bootstrap", strings.NewReader(`{"nonce":"`+challenge.Nonce+`"}`)))
	if w.Code != http.StatusCreated {
		t.Fatalf("bootstrap: %d %s", w.Code, w.Body.String())
	}
	var policy struct {
		Admins []struct {
			DeviceID string `json:"device_id"`
		} `json:"admins"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &policy); err != nil {
		t.Fatal(err)
	}
	if len(policy.Admins) != 1 || policy.Admins[0].DeviceID != "enrolled-device" {
		t.Fatalf("bootstrap did not bind WhoIs device: %s", w.Body.String())
	}
}

func TestAuthenticatedAdminOtherDeviceRefusedBeforeAnyWrite(t *testing.T) {
	f := &identityAnswerFabric{identity: fabric.Identity{FabricID: "issuer", UserID: "owner", DeviceID: "enrolled-device"}}
	s := newIdentityAnswerServer(t, f)
	challenge, err := s.store.InitiateAdminBootstrap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	serve := func(method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
		return w
	}
	if w := serve("POST", "/v1/admin-bootstrap", `{"nonce":"`+challenge.Nonce+`"}`); w.Code != 201 {
		t.Fatalf("bootstrap: %d %s", w.Code, w.Body.String())
	}
	if w := serve("GET", "/v1/admin-policy", ""); w.Code != 200 {
		t.Fatalf("enrolled: %d %s", w.Code, w.Body.String())
	}
	f.identity.DeviceID = "other-device"
	// Hold the immediate writer lock as well as watching the database version.
	// A refusal must complete through a read-only snapshot, without observation.
	conn, err := s.store.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(t.Context(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(t.Context(), "ROLLBACK")
	var before int64
	if err := conn.QueryRowContext(t.Context(), "PRAGMA data_version").Scan(&before); err != nil {
		t.Fatal(err)
	}
	for _, route := range personDeviceRefusalRoutes(t) {
		t.Run(route.method+route.path, func(t *testing.T) {
			w := serve(route.method, route.path, `{"policy_revision":1}`)
			if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), `"code":"person_device_mismatch"`) {
				t.Fatalf("refusal: %d %s", w.Code, w.Body.String())
			}
			if strings.Contains(w.Body.String(), "enrolled-device") || strings.Contains(w.Body.String(), "other-device") {
				t.Fatalf("device ID leaked: %s", w.Body.String())
			}
		})
	}
	var after int64
	if err := conn.QueryRowContext(t.Context(), "PRAGMA data_version").Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("refused requests wrote: data_version %d -> %d", before, after)
	}
	if _, err := conn.ExecContext(t.Context(), "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	if w := serve("GET", "/v1/whoami", ""); w.Code != 200 || !strings.Contains(w.Body.String(), `"device_id":"other-device"`) || strings.Contains(w.Body.String(), `"admin"`) {
		t.Fatalf("whoami: %d %s", w.Code, w.Body.String())
	}
	if w := serve("GET", "/v1/admin-policy", ""); w.Code != http.StatusForbidden {
		t.Fatalf("whoami silently rebound administrator: %d %s", w.Code, w.Body.String())
	}
	f.identity.UserID = "visitor"
	if w := serve("GET", "/v1/admin-policy", ""); w.Code != 200 || w.Body.String() != "{\"revision\":1}\n" {
		t.Fatalf("nonadmin: %d %s", w.Code, w.Body.String())
	}
}

func TestAuthenticatedAdminAddRequiresExplicitSubjectDevice(t *testing.T) {
	f := &identityAnswerFabric{identity: fabric.Identity{FabricID: "issuer", UserID: "owner", DeviceID: "enrolled-device"}}
	s := newIdentityAnswerServer(t, f)
	challenge, err := s.store.InitiateAdminBootstrap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	serve := func(method, path, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
		return w
	}
	if w := serve("POST", "/v1/admin-bootstrap", `{"nonce":"`+challenge.Nonce+`"}`); w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	w := serve("PUT", "/v1/admin-policy/admins/second", `{"policy_revision":1}`)
	if w.Code != 400 || !strings.Contains(w.Body.String(), `"code":"invalid_request"`) {
		t.Fatalf("missing subject device: %d %s", w.Code, w.Body.String())
	}
	w = serve("PUT", "/v1/admin-policy/admins/second", `{"policy_revision":1,"device_id":"second-enrolled"}`)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"device_id":"second-enrolled"`) {
		t.Fatalf("explicit subject device: %d %s", w.Code, w.Body.String())
	}
	w = serve("GET", "/v1/admin-policy/audit", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"subject_device_id":"second-enrolled"`) || !strings.Contains(w.Body.String(), `"actor_device_id":"enrolled-device"`) {
		t.Fatalf("binding audit: %d %s", w.Code, w.Body.String())
	}
	f.identity.UserID = "second"
	f.identity.DeviceID = "second-enrolled"
	if w = serve("GET", "/v1/admin-policy", ""); w.Code != 200 {
		t.Fatalf("added admin: %d %s", w.Code, w.Body.String())
	}
	f.identity.DeviceID = "second-other"
	if w = serve("GET", "/v1/admin-policy", ""); w.Code != 403 {
		t.Fatalf("added admin other device: %d %s", w.Code, w.Body.String())
	}
}

func TestAuthenticatedLegacyAdminDeviceFailsClosed(t *testing.T) {
	f := &identityAnswerFabric{identity: fabric.Identity{FabricID: "issuer", UserID: "owner", DeviceID: "observed-device"}}
	s := newIdentityAnswerServer(t, f)
	// This is a roster from the previous schema, with no enrolled device.
	if _, err := s.store.db.Exec(`INSERT INTO admins(fabric_id,user_id,added_revision,added_ns) VALUES('issuer','owner',1,0)`); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/v1/admin-policy", nil))
	if w.Code != 403 || !strings.Contains(w.Body.String(), `"code":"person_device_mismatch"`) {
		t.Fatalf("legacy authority: %d %s", w.Code, w.Body.String())
	}
	var people int
	if err := s.store.db.QueryRow(`SELECT COUNT(*) FROM authenticated_people`).Scan(&people); err != nil {
		t.Fatal(err)
	}
	if people != 0 {
		t.Fatalf("refusal observed or silently bound legacy administrator: %d", people)
	}
}

func TestLocalAdminDeviceRebindIsExplicitAndAtomic(t *testing.T) {
	f := &identityAnswerFabric{identity: fabric.Identity{FabricID: "issuer", UserID: "owner", DeviceID: "first-device"}}
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
	rebinder, ok := any(s.store).(interface {
		RebindAdminDevice(context.Context, string, string) (AdminPolicy, error)
	})
	if !ok {
		t.Fatal("no explicit local administrator device rebind")
	}
	if _, err := rebinder.RebindAdminDevice(t.Context(), "missing", "next-device"); errorCode(err) != contract.ErrorNotFound {
		t.Fatalf("unknown admin: %v", err)
	}
	if _, err := rebinder.RebindAdminDevice(t.Context(), "owner", ""); errorCode(err) != contract.ErrorInvalidRequest {
		t.Fatalf("empty device: %v", err)
	}
	policy, err := rebinder.RebindAdminDevice(t.Context(), "owner", "next-device")
	if err != nil || policy.Revision != 2 {
		t.Fatalf("rebind: %#v %v", policy, err)
	}
	w = httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/v1/admin-policy", nil))
	if w.Code != 403 {
		t.Fatalf("old device still has authority: %d %s", w.Code, w.Body.String())
	}
	f.identity.DeviceID = "next-device"
	w = httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/v1/admin-policy/audit", nil))
	var page struct {
		Entries []struct {
			Revision        int64  `json:"revision"`
			Operation       string `json:"operation"`
			ActorKind       string `json:"actor_kind"`
			ActorUserID     string `json:"actor_user_id"`
			ActorDeviceID   string `json:"actor_device_id"`
			SubjectDeviceID string `json:"subject_device_id"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || len(page.Entries) != 2 {
		t.Fatalf("audit: %d %s", w.Code, w.Body.String())
	}
	audit := page.Entries[1]
	if audit.Revision != 2 || audit.Operation != "rebind" || audit.ActorKind != "local_operator" || audit.ActorUserID != "" || audit.ActorDeviceID != "" || audit.SubjectDeviceID != "next-device" {
		t.Fatalf("local audit: %s", w.Body.String())
	}
	if _, err := s.store.db.Exec(`UPDATE admin_policy_audit SET subject_user_id='forged' WHERE revision=2`); err == nil {
		t.Fatal("rebind audit can be rewritten")
	}
	if _, err := s.store.db.Exec(`DELETE FROM admin_policy_audit WHERE revision=2`); err == nil {
		t.Fatal("rebind audit can be deleted")
	}
	// An audit failure must roll back the binding and the single revision.
	if _, err := s.store.db.Exec(`CREATE TRIGGER reject_rebind BEFORE INSERT ON admin_policy_audit WHEN NEW.operation='rebind' BEGIN SELECT RAISE(ABORT,'test audit failure');END`); err != nil {
		t.Fatal(err)
	}
	if _, err := rebinder.RebindAdminDevice(t.Context(), "owner", "uncommitted-device"); err == nil {
		t.Fatal("rebind succeeded without audit")
	}
	w = httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/v1/admin-policy", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"revision":2`) || !strings.Contains(w.Body.String(), `"device_id":"next-device"`) {
		t.Fatalf("failed rebind changed authority: %d %s", w.Code, w.Body.String())
	}
}

func TestAuthenticatedNonAdminGrantRemainsPersonScoped(t *testing.T) {
	f := &identityAnswerFabric{identity: fabric.Identity{FabricID: "issuer", UserID: "owner", DeviceID: "enrolled-device"}}
	s := newIdentityAnswerServer(t, f)
	serve := func(method, path, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
		return w
	}
	challenge, err := s.store.InitiateAdminBootstrap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if w := serve("POST", "/v1/admin-bootstrap", `{"nonce":"`+challenge.Nonce+`"}`); w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	computer, _, err := s.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "person-grant", Spec: computerCapabilityJobSpec("person-grant"), Actor: "test"})
	if err != nil {
		t.Fatal(err)
	}
	f.identity.UserID = "visitor"
	f.identity.DeviceID = "visitor-one"
	if w := serve("GET", "/v1/whoami", ""); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	f.identity.UserID = "owner"
	f.identity.DeviceID = "enrolled-device"
	if w := serve("PUT", "/v1/computers/"+computer.ComputerID+"/grants/visitor", `{"policy_revision":1,"permission":"view","idempotency_key":"visitor-grant"}`); w.Code != 200 {
		t.Fatalf("grant: %d %s", w.Code, w.Body.String())
	}
	f.identity.UserID = "visitor"
	for _, device := range []string{"visitor-one", "visitor-two"} {
		f.identity.DeviceID = device
		if w := serve("GET", "/v1/computers/"+computer.ComputerID+"/takeover", ""); w.Code != 200 {
			t.Fatalf("grantee %s: %d %s", device, w.Code, w.Body.String())
		}
	}
}

type selfAssertedPersonAnswerFabric struct{ *identityAnswerFabric }

func (*selfAssertedPersonAnswerFabric) PersonIdentityTrust() fabric.PersonIdentityTrust {
	return fabric.PersonIdentitySelfAsserted
}

func TestSelfAssertedAdminIgnoresDeviceBinding(t *testing.T) {
	f := &identityAnswerFabric{identity: fabric.Identity{FabricID: "plain-fixture", UserID: "owner", DeviceID: "first-device"}}
	store, err := OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	s, err := NewServer(&selfAssertedPersonAnswerFabric{f}, store, ServerConfig{AllowSelfAssertedPersonIdentities: true})
	if err != nil {
		t.Fatal(err)
	}
	serve := func(method, path, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
		return w
	}
	challenge, err := s.store.InitiateAdminBootstrap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if w := serve("POST", "/v1/admin-bootstrap", `{"nonce":"`+challenge.Nonce+`"}`); w.Code != 201 || strings.Contains(w.Body.String(), `"device_id"`) {
		t.Fatalf("plain bootstrap: %d %s", w.Code, w.Body.String())
	}
	f.identity.DeviceID = "another-device"
	if w := serve("PUT", "/v1/admin-policy/admins/second", `{"policy_revision":1,"device_id":" ignored invalid value "}`); w.Code != 200 || strings.Contains(w.Body.String(), `"device_id"`) {
		t.Fatalf("plain add: %d %s", w.Code, w.Body.String())
	}
	f.identity.UserID = "second"
	f.identity.DeviceID = "any-second-device"
	if w := serve("GET", "/v1/admin-policy", ""); w.Code != 200 || strings.Contains(w.Body.String(), `"device_id"`) {
		t.Fatalf("plain second device: %d %s", w.Code, w.Body.String())
	}
	if w := serve("GET", "/v1/admin-policy/audit", ""); w.Code != 200 || strings.Contains(w.Body.String(), `"subject_device_id"`) {
		t.Fatalf("plain audit: %d %s", w.Code, w.Body.String())
	}
}

// Read the actual person mux registrations so a new person path joins this
// refusal/no-write test automatically, without a parallel production route list.
func personDeviceRefusalRoutes(t *testing.T) []struct{ method, path string } {
	t.Helper()
	source, err := parser.ParseFile(token.NewFileSet(), "server.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var routes []struct{ method, path string }
	foundWhoami := false
	ast.Inspect(source, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || (selector.Sel.Name != "HandleFunc" && selector.Sel.Name != "Handle") {
			return true
		}
		receiver, ok := selector.X.(*ast.Ident)
		if !ok || receiver.Name != "person" {
			return true
		}
		literal, ok := call.Args[0].(*ast.BasicLit)
		if !ok {
			t.Fatal("person route pattern must be an inspectable literal")
		}
		pattern, err := strconv.Unquote(literal.Value)
		if err != nil {
			t.Fatal(err)
		}
		method, path, hasMethod := strings.Cut(pattern, " ")
		if !hasMethod {
			method, path = http.MethodGet, pattern
		}
		if method == http.MethodGet && path == "/v1/whoami" {
			foundWhoami = true
			return true
		}
		segments := strings.Split(path, "/")
		for i, segment := range segments {
			if strings.HasPrefix(segment, "{") && strings.HasSuffix(segment, "}") {
				segments[i] = "missing"
			}
		}
		routes = append(routes, struct{ method, path string }{method, strings.Join(segments, "/")})
		return true
	})
	if !foundWhoami || len(routes) == 0 {
		t.Fatal("person mux routes were not discovered")
	}
	// Person cancellation shares a route with client/attempt cancellation and
	// is selected outside the person mux. Keep that distinct selector covered.
	return append(routes, struct{ method, path string }{"POST", "/v1/jobs/missing/cancel"})
}
