//go:build service_acceptance

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
	"github.com/coder/websocket"
)

func TestServiceAcceptanceComputerTakeoverCLIRealFrontDoor(t *testing.T) {
	network := plain.NewNetwork()
	controlFabric := network.NewFabric(fabric.Identity{NodeID: "access-acceptance-control"})
	personIdentity := fabric.Identity{NodeID: "operator-device", UserID: "operator-user", DeviceID: "operator-device-id"}
	personPlain := network.NewFabric(personIdentity)
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server, err := l1.NewServer(controlFabric, store, l1.ServerConfig{AllowSelfAssertedPersonIdentities: true})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := controlFabric.Listen("tcp", l3.DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	serverContext, stopServer := context.WithCancel(t.Context())
	serverDone := serveTestServer(serverContext, func() error { return server.Serve(serverContext, listener) })
	defer func() {
		stopServer()
		if err := <-serverDone; err != nil {
			t.Errorf("L1 server: %v", err)
		}
	}()

	bootstrapClients, err := newAPIClients(personPlain, l3.DefaultL1Address, l3.DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}
	challenge, err := store.InitiateAdminBootstrap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	policy, err := bootstrapClients.bootstrapAdmin(t.Context(), challenge.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	bootstrapClients.close()
	personIdentity.FabricID = policy.Admins[0].FabricID

	computer, _, err := store.CreateComputer(t.Context(), l1.CreateComputerRequest{
		Name: "takeover-cli-acceptance", Spec: accessCLIComputerSpec("takeover-cli-acceptance"), Actor: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	nodeIdentity := fabric.Identity{NodeID: "fabric-computer-node"}
	node, err := store.RegisterNode(t.Context(), nodeIdentity, contract.NodeRegistration{
		NodeID: "computer-node", BootSessionID: "boot-computer-node", RootInstanceID: "root-computer-node",
		OS: "linux", Architecture: "amd64", AgentVersion: "acceptance", CapabilityRevision: 1,
		CapabilityObservedAt: time.Now().UTC(), Capabilities: map[string]bool{"kind:oci": true, "cgroup_v2": true, "computer": true},
	}, l1.NodePolicy{Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxServiceSlots: 1}, true)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimJob(t.Context(), nodeIdentity.NodeID, node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim Computer = %#v err=%v", claim, err)
	}

	now := time.Now().UTC()
	snapshot := l1.ComputerPolicySnapshot{
		PolicyGeneration: 1, PolicyRevision: policy.Revision, IssuingFabricID: personIdentity.FabricID,
		NodeID: node.NodeID, BootSessionID: node.BootSessionID, IssuedAt: now, FreshUntil: now.Add(time.Hour),
		Admins:    []l1.ComputerPolicyAdmin{{FabricID: personIdentity.FabricID, UserID: personIdentity.UserID}},
		Computers: []l1.ComputerPolicyComputer{{ComputerID: computer.ComputerID, Grants: []l1.ComputerGrant{}, SubmitMaxInflight: l1.DefaultComputerSubmitMaxInflight}},
	}
	snapshot.SnapshotDigest, err = l1.ComputeComputerPolicySnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var signalMu sync.Mutex
	signals := []bool{}
	frontFabric := &acceptanceCLIFabric{l1: personPlain, identity: personIdentity,
		connectHost: "cli-connect-address.example.test", routes: make(map[string]string)}
	backendServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{Subprotocols: []string{contract.ComputerDisplayWebSocketSubprotocol}})
		if err != nil {
			return
		}
		defer connection.CloseNow()
		if err := connection.Write(request.Context(), websocket.MessageBinary, []byte("RFB 003.008\n")); err != nil {
			return
		}
		if !completeServiceAcceptanceRFBServerHandshake(request.Context(), connection) {
			return
		}
		for {
			messageType, payload, err := connection.Read(request.Context())
			if err != nil {
				return
			}
			if err := connection.Write(request.Context(), messageType, payload); err != nil {
				return
			}
		}
	}))
	defer backendServer.Close()
	backendAddress := strings.TrimPrefix(backendServer.URL, "http://")
	handler, err := agent.NewComputerFrontDoorAcceptanceHandler(agent.ComputerFrontDoorAcceptanceConfig{
		AuthorityContext: t.Context(), Fabric: frontFabric, Snapshot: snapshot,
		ComputerID: computer.ComputerID, JobID: claim.Job.JobID, AttemptID: claim.Lease.AttemptID,
		FencingToken: claim.Lease.FencingToken, Dial: func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", backendAddress)
		},
		SetControlState: func(_ context.Context, value bool) error {
			signalMu.Lock()
			signals = append(signals, value)
			signalMu.Unlock()
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	frontServer := httptest.NewServer(handler)
	defer frontServer.Close()
	frontURL, err := url.Parse(frontServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	rawConnectHost := "computer-connect-address.example.test"
	rawConnectAddress := net.JoinHostPort(rawConnectHost, frontURL.Port())
	frontFabric.routes[rawConnectAddress] = frontURL.Host
	endpoint := (&url.URL{Scheme: "ws", Host: rawConnectAddress, Path: contract.ComputerDisplayWebSocketPath}).String()
	ready := true
	if _, err := store.SetAttemptPublication(t.Context(), nodeIdentity.NodeID, claim.Job.JobID, claim.Lease.AttemptID,
		l1.PublicationRequest{FencingToken: claim.Lease.FencingToken, Ready: &ready, DisplayEndpoint: &endpoint}); err != nil {
		t.Fatal(err)
	}

	clients, err := newAPIClients(frontFabric, l3.DefaultL1Address, l3.DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}
	defer clients.close()
	tokenFile := filepath.Join(t.TempDir(), "live-session.json")
	viewContext, stopView := context.WithCancel(t.Context())
	var viewOutput, viewError bytes.Buffer
	viewDone := make(chan error, 1)
	go func() {
		viewDone <- execute(viewContext, clients, false, []string{"services", "takeover", "view", computer.ComputerID,
			"--session-token-file", tokenFile}, &viewOutput, &viewError)
	}()
	waitForAcceptanceFile(t, tokenFile, viewDone, &viewError)
	takeOutput := runAccessCLI(t, t.Context(), clients, true, "services", "takeover", "take", computer.ComputerID, "--session-token-file", tokenFile)
	var take contract.ComputerControlReceipt
	if err := json.Unmarshal(takeOutput, &take); err != nil || take.Action != "take" || take.TenureState != contract.ComputerControlTenureHeld || !take.HumanDriving || take.HolderSessionID == "" {
		t.Fatalf("take receipt = %#v err=%v output=%s", take, err, takeOutput)
	}
	releaseOutput := runAccessCLI(t, t.Context(), clients, true, "services", "takeover", "release", computer.ComputerID, "--session-token-file", tokenFile)
	var release contract.ComputerControlReceipt
	if err := json.Unmarshal(releaseOutput, &release); err != nil || release.Action != "release" || release.TenureState != contract.ComputerControlTenureFree || release.HumanDriving {
		t.Fatalf("release receipt = %#v err=%v output=%s", release, err, releaseOutput)
	}
	stopView()
	if err := <-viewDone; err != nil {
		t.Fatalf("view command: %v stderr=%s", err, viewError.String())
	}
	if output := viewOutput.String(); !strings.HasPrefix(output, "FRIENDLY NAME\ttakeover-cli-acceptance\nCONNECT HOST\t"+rawConnectHost+"\n") ||
		strings.Contains(output, frontFabric.connectHost) || strings.Contains(output, "127.0.0.1") || strings.Contains(output, frontServer.URL) {
		t.Fatalf("view projection did not keep friendly name primary and accepted raw connect host secondary: %s", output)
	}
	signalMu.Lock()
	defer signalMu.Unlock()
	if len(signals) < 2 || !signals[0] || signals[len(signals)-1] {
		t.Fatalf("driver signal history = %v", signals)
	}
}

func completeServiceAcceptanceRFBServerHandshake(ctx context.Context, connection *websocket.Conn) bool {
	kind, version, err := connection.Read(ctx)
	if err != nil || kind != websocket.MessageBinary || string(version) != "RFB 003.008\n" {
		return false
	}
	if err := connection.Write(ctx, websocket.MessageBinary, []byte{1, 1}); err != nil {
		return false
	}
	kind, security, err := connection.Read(ctx)
	if err != nil || kind != websocket.MessageBinary || !bytes.Equal(security, []byte{1}) {
		return false
	}
	if err := connection.Write(ctx, websocket.MessageBinary, []byte{0, 0, 0, 0}); err != nil {
		return false
	}
	kind, shared, err := connection.Read(ctx)
	if err != nil || kind != websocket.MessageBinary || !bytes.Equal(shared, []byte{1}) {
		return false
	}
	serverInit := make([]byte, 24)
	binary.BigEndian.PutUint16(serverInit[0:2], 640)
	binary.BigEndian.PutUint16(serverInit[2:4], 480)
	return connection.Write(ctx, websocket.MessageBinary, serverInit) == nil
}

type acceptanceCLIFabric struct {
	l1          fabric.Fabric
	identity    fabric.Identity
	connectHost string
	routes      map[string]string
}

func (f *acceptanceCLIFabric) Listen(network, address string) (net.Listener, error) {
	return f.l1.Listen(network, address)
}
func (f *acceptanceCLIFabric) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	if address == l3.DefaultL1Address || address == l3.DefaultL3Address {
		return f.l1.Dial(ctx, network, address)
	}
	if target, ok := f.routes[address]; ok {
		address = target
	}
	return (&net.Dialer{}).DialContext(ctx, network, address)
}
func (f *acceptanceCLIFabric) WhoIs(context.Context, string) (fabric.Identity, error) {
	return f.identity, nil
}
func (f *acceptanceCLIFabric) ConnectHost() string { return f.connectHost }

func waitForAcceptanceFile(t *testing.T, path string, done <-chan error, stderr *bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if info, err := os.Stat(path); err == nil && info.Size() > 0 {
			return
		}
		select {
		case err := <-done:
			t.Fatalf("view ended before token publication: %v stderr=%s", err, stderr.String())
		default:
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}
