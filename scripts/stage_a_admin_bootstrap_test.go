package scripts

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestStageAProofAdminBootstrapAcrossOfflineReopen(t *testing.T) {
	// The runbook must include the offline command, not just an API-only test.
	doc := string(mustReadFile(t, "../docs/acceptance/stage-a-attended.md"))
	if !strings.Contains(doc, "--initiate-admin-bootstrap") {
		t.Fatal("runbook has no offline administrator bootstrap")
	}
	root := filepath.Join(t.TempDir(), ".local", "share", "wefty-proof")
	for _, dir := range []string{"config", "data"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	profile := connprofile.Profile{Version: 1, Fabric: "plain", PlainFabricID: "plain-stage-a-proof", AllowPlainPersonIdentities: true,
		Identities: connprofile.Identities{CLI: "proof-cli", PersonUserID: "proof-owner", PersonDeviceID: "proof-device"}}
	payload, err := json.Marshal(profile)
	if err != nil {
		t.Fatal(err)
	}
	profilePath := filepath.Join(root, "config", "profile.json")
	if err := os.WriteFile(profilePath, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	profile, err = connprofile.Read(profilePath)
	if err != nil {
		t.Fatal(err)
	}
	clock := l1.ClockFunc(func() time.Time { return time.Date(2026, 10, 28, 12, 0, 0, 714700000, time.UTC) })
	db := filepath.Join(root, "data", "l1.sqlite")
	offline, err := l1.OpenStore(db, l1.StoreOptions{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	challenge, err := offline.InitiateAdminBootstrap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := offline.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := l1.OpenStore(db, l1.StoreOptions{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	network, err := plain.NewNetworkWithID(profile.PlainFabricID)
	if err != nil {
		t.Fatal(err)
	}
	control := network.NewFabric(fabric.Identity{NodeID: "proof-l1"})
	server, err := l1.NewServer(control, store, l1.ServerConfig{AllowSelfAssertedPersonIdentities: profile.AllowPlainPersonIdentities})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := control.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: server.Handler()}
	go httpServer.Serve(listener)
	defer httpServer.Close()
	person := network.NewFabric(fabric.Identity{NodeID: profile.Identities.CLI, UserID: profile.Identities.PersonUserID, DeviceID: profile.Identities.PersonDeviceID})
	transport := &http.Transport{DialContext: person.Dial}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	call := func(method, path string, body []byte) (int, []byte) {
		t.Helper()
		request, err := http.NewRequestWithContext(t.Context(), method, "http://"+listener.Addr().String()+path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		value, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, value
	}
	viewer := "/v1/seats/seat_00000000000000000000000000000001/viewer-target"
	if status, _ := call(http.MethodGet, viewer, nil); status != http.StatusForbidden {
		t.Fatalf("fresh proof L1 granted viewer authority: %d", status)
	}
	payload, err = json.Marshal(l1.BootstrapAdminRequest{Nonce: challenge.Nonce})
	if err != nil {
		t.Fatal(err)
	}
	status, body := call(http.MethodPost, "/v1/admin-bootstrap", payload)
	var policy l1.AdminPolicy
	if status != http.StatusCreated || json.Unmarshal(body, &policy) != nil || len(policy.Admins) != 1 || policy.Admins[0].UserID != profile.Identities.PersonUserID || policy.Admins[0].FabricID != profile.PlainFabricID {
		t.Fatalf("proof profile did not become administrator: status=%d", status)
	}
	if status, _ := call(http.MethodGet, "/v1/admin-policy", nil); status != http.StatusOK {
		t.Fatalf("bootstrapped proof owner cannot read administrator policy: %d", status)
	}
	// The request now passes the administrator check and reaches Seat lookup.
	// No real Seat, viewer or browser is started in this preparation test.
	if status, _ := call(http.MethodGet, viewer, nil); status != http.StatusNotFound {
		t.Fatalf("bootstrapped proof owner still lacks viewer authority: %d", status)
	}
}
