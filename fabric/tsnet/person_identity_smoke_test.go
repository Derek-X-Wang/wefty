//go:build tsnet_smoke

package tsnet

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestTSNetPersonWhoAmI(t *testing.T) {
	machineAuthKey := os.Getenv("TS_AUTHKEY")
	personAuthKey := os.Getenv("TS_AUTHKEY_CI_TESTER")
	if machineAuthKey == "" || personAuthKey == "" {
		t.Skip("TS_AUTHKEY and TS_AUTHKEY_CI_TESTER are required")
	}
	controlURL := os.Getenv("TS_CONTROL_URL")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	serverName := "wefty://node/person-smoke-server-" + suffix
	personName := "wefty://node/person-smoke-peer-" + suffix

	serverFabric, err := New(Config{
		Name: serverName, StateDir: t.TempDir(), Credential: fabric.Credential{Value: machineAuthKey},
		Ephemeral: true, CoordinatorURL: controlURL, Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer serverFabric.Close()
	personFabric, err := New(Config{
		Name: personName, StateDir: t.TempDir(), Credential: fabric.Credential{Value: personAuthKey},
		Ephemeral: true, CoordinatorURL: controlURL, Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer personFabric.Close()

	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := serverFabric.Listen("tcp", serverName)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	served := make(chan error, 1)
	go func() { served <- server.Serve(ctx, listener) }()
	defer func() {
		cancel()
		if err := <-served; err != nil {
			t.Errorf("serve person identity control plane: %v", err)
		}
	}()

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return personFabric.Dial(ctx, network, serverName)
		}},
	}
	defer client.CloseIdleConnections()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://fabric.invalid/v1/whoami", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var person l1.AuthenticatedPerson
	decodeErr := json.NewDecoder(response.Body).Decode(&person)
	if response.StatusCode != http.StatusOK || decodeErr != nil {
		t.Fatalf("whoami status=%d decode=%v", response.StatusCode, decodeErr)
	}
	if person.FabricID == "" || person.UserID == "" || person.DeviceID == "" || person.SeenAt.IsZero() {
		t.Fatalf("whoami returned incomplete person identity: %#v", person)
	}
	serverConnectHost := serverFabric.ConnectHost()
	personConnectHost := personFabric.ConnectHost()
	if serverConnectHost == "" || personConnectHost == "" {
		t.Fatalf("ConnectHost server=%q person=%q, want non-empty Fabric presentation addresses", serverConnectHost, personConnectHost)
	}
	writeFabricIdentitySmokeReceipt(t, os.Getenv("WEFTY_FABRIC_PERSON_RECEIPT"), map[string]fabricIdentitySmokeRow{
		"fabric.person_whoami": {
			Status: "PASS",
			Assertions: map[string]bool{
				"person_identity_complete": true,
				"whoami_authenticated":     true,
			},
			Evidence: map[string]string{
				"listener_connect_host": serverConnectHost,
				"peer_connect_host":     personConnectHost,
			},
		},
	})
}
