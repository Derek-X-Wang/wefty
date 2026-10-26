package computerconformance_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Shared by the root screen lane and its non-root registration regression.
// A legacy node has no installed node-agent generation to bind to a host boot.
func screenLaneNodeConfig(t *testing.T, f fabric.Fabric, address string) agent.Config {
	t.Helper()
	room := l1.DefaultComputerRoomConfig(2 << 30)
	return agent.Config{Version: "screen-lane", Fabric: f, ControlPlaneAddress: address, NodeID: "screen-node", BootSessionID: "screen-boot", ManagedRootDirectory: filepath.Join(resolvedTempDir(t), "managed"), HandoffRoot: resolvedTempDir(t), LogSpoolDirectory: resolvedTempDir(t), MaxServiceSlots: 2, MaxOneshotSlots: 1, ComputerRoom: &room, Logf: t.Logf}
}

// t.TempDir can sit under a symlinked temp root (macOS /var, a wefty run's
// $TMPDIR); the managed root refuses paths that leave through a link.
func resolvedTempDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return dir
}
func screenLaneServerConfig() l1.ServerConfig {
	return l1.ServerConfig{AllowSelfAssertedPersonIdentities: true, RunLedgerNodeID: "screen-ledger", NodePolicies: map[string]l1.NodePolicy{"screen-node": {Tags: []string{contract.StableNodeTagPrefix + "screen-node"}, MaxServiceSlots: 2, MaxOneshotSlots: 1}}}
}

// Keep the refusal body visible without printing registration payloads or
// credentials. Only registration is buffered; other L1 operations pass through.
func screenLaneRegistrationHandler(t *testing.T, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/agent/nodes/register" {
			next.ServeHTTP(w, r)
			return
		}
		reply := httptest.NewRecorder()
		next.ServeHTTP(reply, r)
		if reply.Code >= 400 {
			var refusal contract.ErrorResponse
			if json.Unmarshal(reply.Body.Bytes(), &refusal) == nil {
				t.Logf("Seat screen registration refused: HTTP %d code=%s message=%q", reply.Code, refusal.Error.Code, refusal.Error.Message)
			}
		}
		for name, values := range reply.Header() {
			w.Header()[name] = values
		}
		w.WriteHeader(reply.Code)
		_, _ = w.Write(reply.Body.Bytes())
	})
}

func TestSeatScreenLaneNodeRegistration(t *testing.T) {
	// One injected timeline avoids the sandbox-only host-boot read. Node and L1
	// registration, authorization and generation validation remain real.
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{Clock: l1.ClockFunc(func() time.Time { return time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC) })})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	network, _ := plain.NewNetworkWithID("plain-seat-screen-lane")
	control := network.NewFabric(fabric.Identity{NodeID: "control"})
	server, err := l1.NewServer(control, store, screenLaneServerConfig())
	if err != nil {
		t.Fatal(err)
	}
	listener, err := control.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: screenLaneRegistrationHandler(t, server.Handler())}
	go httpServer.Serve(listener)
	defer httpServer.Close()
	nodeFabric := network.NewFabric(fabric.Identity{NodeID: "screen-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	config := screenLaneNodeConfig(t, nodeFabric, listener.Addr().String())
	node, err := agent.New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	registered, err := node.Register(t.Context())
	if err != nil {
		t.Fatalf("lane registration failed: %v", err)
	}
	if registered.NodeID != config.NodeID || registered.BootSessionID != config.BootSessionID || registered.MaxServiceSlots != config.MaxServiceSlots || !registered.ClaimsEnabled || !registered.Capabilities[l1.CapabilitySeatRequests] {
		t.Fatalf("lane registration changed identity, room or admission: %+v", registered)
	}
	// The same boot and Fabric identity can replay registration without conflict.
	if _, err = node.Register(t.Context()); err != nil {
		t.Fatal(err)
	}
}
