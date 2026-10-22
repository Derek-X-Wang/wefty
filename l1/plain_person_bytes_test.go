package l1

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
)

type plainPersonAnswerFabric struct{ fabric.Fabric }

func (*plainPersonAnswerFabric) WhoIs(context.Context, string) (fabric.Identity, error) {
	return fabric.Identity{FabricID: "plain-byte-fixture", UserID: "owner", DeviceID: "owner-device"}, nil
}
func (*plainPersonAnswerFabric) PersonIdentityTrust() fabric.PersonIdentityTrust {
	return fabric.PersonIdentitySelfAsserted
}

// The fixture was captured with these same served requests on base 12657df6.
// Only the random bootstrap nonce is represented by a fixed request label.
func TestPlainAdminAndWhoAmIBytes(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)}
	store, err := OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), StoreOptions{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server, err := NewServer(&plainPersonAnswerFabric{}, store, ServerConfig{AllowSelfAssertedPersonIdentities: true})
	if err != nil {
		t.Fatal(err)
	}
	challenge, err := store.InitiateAdminBootstrap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var capture bytes.Buffer
	for _, route := range []struct {
		method, path string
		body         any
	}{
		{"POST", "/v1/admin-bootstrap", BootstrapAdminRequest{Nonce: challenge.Nonce}},
		{"GET", "/v1/whoami", nil}, {"GET", "/v1/admin-policy", nil},
		{"PUT", "/v1/admin-policy/admins/second", AdminPolicyMutationRequest{PolicyRevision: 1}},
		{"GET", "/v1/admin-policy/audit", nil},
		{"DELETE", "/v1/admin-policy/admins/second", AdminPolicyMutationRequest{PolicyRevision: 2}},
		{"GET", "/v1/admin-policy", nil}, {"GET", "/v1/admin-policy/audit", nil},
	} {
		var body []byte
		if route.body != nil {
			body, err = json.Marshal(route.body)
			if err != nil {
				t.Fatal(err)
			}
		}
		w := httptest.NewRecorder()
		server.Handler().ServeHTTP(w, httptest.NewRequest(route.method, route.path, bytes.NewReader(body)))
		if w.Code >= 400 {
			t.Fatalf("plain route: %d %s", w.Code, w.Body.String())
		}
		label := body
		if route.path == "/v1/admin-bootstrap" {
			label = []byte(`{"nonce":"<nonce>"}`)
		}
		requestLabel := route.method + " " + route.path
		if len(label) > 0 {
			requestLabel += " " + string(label)
		}
		fmt.Fprintf(&capture, "%s\n%d %s", requestLabel, w.Code, w.Body.Bytes())
	}
	if path := os.Getenv("WEFTY_T1090_CAPTURE"); path != "" {
		if err := os.WriteFile(path, capture.Bytes(), 0600); err != nil {
			t.Fatal(err)
		}
		return
	}
	expected, err := os.ReadFile("testdata/plain-person-base-12657df6.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(capture.Bytes(), expected) {
		t.Fatalf("plain admin or whoami bytes changed:\n%s", capture.Bytes())
	}
}
