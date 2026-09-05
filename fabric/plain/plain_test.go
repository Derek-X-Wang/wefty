package plain

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestEchoWithInjectedIdentity(t *testing.T) {
	network := NewNetwork()
	server := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	clientIdentity := fabric.Identity{
		NodeID: "runner-1", UserID: "person-1", DeviceID: "device-1", DisplayName: "Agent",
		Tags: []string{"runner", "linux"},
	}
	client := network.NewFabric(clientIdentity)

	ln, err := server.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	serverErr := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()
		got, err := server.WhoIs(context.Background(), conn.RemoteAddr().String())
		if err != nil {
			serverErr <- err
			return
		}
		if got.NodeID != clientIdentity.NodeID || got.UserID != clientIdentity.UserID ||
			got.DeviceID != clientIdentity.DeviceID || got.DisplayName != clientIdentity.DisplayName ||
			!slices.Equal(got.Tags, clientIdentity.Tags) {
			serverErr <- fmt.Errorf("WhoIs() = %#v, want %#v", got, clientIdentity)
			return
		}
		_, err = io.Copy(conn, conn)
		serverErr <- err
	}()

	conn, err := client.Dial(context.Background(), "tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	message := "hello fabric\n"
	if _, err := io.WriteString(conn, message); err != nil {
		t.Fatal(err)
	}
	got, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if got != message {
		t.Fatalf("echo = %q, want %q", got, message)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestExplicitFabricIDJoinsSeparateProcessNetworks(t *testing.T) {
	first, err := NewNetworkWithID("plain-linux-computer-acceptance")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewNetworkWithID("plain-linux-computer-acceptance")
	if err != nil {
		t.Fatal(err)
	}
	if first.fabricID != second.fabricID || first.fabricID != "plain-linux-computer-acceptance" {
		t.Fatalf("plain Fabric IDs = %q and %q", first.fabricID, second.fabricID)
	}
	if left, right := NewNetwork(), NewNetwork(); left.fabricID == right.fabricID {
		t.Fatalf("unconfigured plain networks shared Fabric ID %q", left.fabricID)
	}
}

func TestNewNetworkDoesNotReadProcessEnvironmentAuthority(t *testing.T) {
	t.Setenv("WEFTY_DEV_PLAIN_FABRIC_ID", "plain-library-must-ignore-environment")
	left, right := NewNetwork(), NewNetwork()
	if left.fabricID == "plain-library-must-ignore-environment" || right.fabricID == "plain-library-must-ignore-environment" || left.fabricID == right.fabricID {
		t.Fatalf("isolated library networks inherited environment authority: %q %q", left.fabricID, right.fabricID)
	}
}

func TestExplicitFabricIDRejectsNonPlainAuthority(t *testing.T) {
	for _, value := range []string{"", "fabric-production", "plain-", " plain-dev", "plain-dev "} {
		if _, err := NewNetworkWithID(value); err == nil {
			t.Fatalf("NewNetworkWithID(%q) succeeded", value)
		}
	}
}

func TestConnectionForwardsWriteHalfClose(t *testing.T) {
	network := NewNetwork()
	server := network.NewFabric(fabric.Identity{NodeID: "server"})
	client := network.NewFabric(fabric.Identity{NodeID: "client"})
	listener, err := server.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverDone := make(chan error, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}
		defer connection.Close()
		request, readErr := io.ReadAll(connection)
		if readErr == nil && string(request) != "request" {
			readErr = fmt.Errorf("request = %q", request)
		}
		if readErr == nil {
			_, readErr = io.WriteString(connection, "response")
		}
		serverDone <- readErr
	}()
	connection, err := client.Dial(t.Context(), "tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	half, ok := connection.(interface{ CloseWrite() error })
	if !ok {
		t.Fatalf("plain Fabric connection %T does not expose CloseWrite", connection)
	}
	if _, err := io.WriteString(connection, "request"); err != nil {
		t.Fatal(err)
	}
	if err := half.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	response, err := io.ReadAll(connection)
	if err != nil || string(response) != "response" {
		t.Fatalf("response = %q, %v", response, err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestInjectedIdentityDrivesAuthorization(t *testing.T) {
	network := NewNetwork()
	server := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	allowed := network.NewFabric(fabric.Identity{NodeID: "runner-1", Tags: []string{"agent"}})
	denied := network.NewFabric(fabric.Identity{NodeID: "runner-2", Tags: []string{"observer"}})

	ln, err := server.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	httpServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, err := server.WhoIs(r.Context(), r.RemoteAddr)
		if err != nil || !slices.Contains(identity.Tags, "agent") {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})}
	defer httpServer.Close()
	go func() { _ = httpServer.Serve(ln) }()

	for _, tt := range []struct {
		name string
		f    *Fabric
		want int
	}{
		{name: "authorized identity", f: allowed, want: http.StatusNoContent},
		{name: "unauthorized identity", f: denied, want: http.StatusForbidden},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := &http.Client{Transport: &http.Transport{DialContext: tt.f.Dial}}
			resp, err := client.Get("http://" + ln.Addr().String() + "/health")
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tt.want {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
}

func TestInjectedIdentityCrossesNetworkInstances(t *testing.T) {
	serverNetwork := NewNetwork()
	clientNetwork := NewNetwork()
	server := serverNetwork.NewFabric(fabric.Identity{NodeID: "control-plane"})
	clientIdentity := fabric.Identity{NodeID: "runner-cross-process", Tags: []string{"agent"}}
	client := clientNetwork.NewFabric(clientIdentity)

	ln, err := server.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	accepted := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			accepted <- err
			return
		}
		defer conn.Close()
		identity, err := server.WhoIs(context.Background(), conn.RemoteAddr().String())
		if err == nil && (identity.NodeID != clientIdentity.NodeID || !slices.Equal(identity.Tags, clientIdentity.Tags)) {
			err = fmt.Errorf("WhoIs() = %#v, want %#v", identity, clientIdentity)
		}
		accepted <- err
	}()

	conn, err := client.Dial(context.Background(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-accepted; err != nil {
		t.Fatal(err)
	}
}

func TestConnectionsWithSameRemoteAddressKeepTheirOwnIdentity(t *testing.T) {
	network := NewNetwork()
	server := network.NewFabric(fabric.Identity{NodeID: "control-plane"})

	firstServer, firstClient := net.Pipe()
	secondServer, secondClient := net.Pipe()
	if firstServer.RemoteAddr().String() != secondServer.RemoteAddr().String() {
		t.Fatalf("test connections do not share a remote address: %q != %q", firstServer.RemoteAddr(), secondServer.RemoteAddr())
	}
	underlying := &connectionListener{
		address:     firstServer.LocalAddr(),
		connections: []net.Conn{firstServer, secondServer},
	}
	listener := &listener{Listener: underlying, network: network}

	writeErrors := make(chan error, 2)
	go func() { writeErrors <- writeIdentity(firstClient, fabric.Identity{NodeID: "runner-1"}) }()
	go func() { writeErrors <- writeIdentity(secondClient, fabric.Identity{NodeID: "runner-2"}) }()

	first, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	for _, connection := range []struct {
		name   string
		conn   net.Conn
		nodeID string
	}{
		{name: "first", conn: first, nodeID: "runner-1"},
		{name: "second", conn: second, nodeID: "runner-2"},
	} {
		t.Run(connection.name, func(t *testing.T) {
			identity, err := server.WhoIs(t.Context(), connection.conn.RemoteAddr().String())
			if err != nil {
				t.Fatal(err)
			}
			if identity.NodeID != connection.nodeID {
				t.Fatalf("WhoIs() NodeID = %q, want %q", identity.NodeID, connection.nodeID)
			}
		})
	}

	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := server.WhoIs(t.Context(), second.RemoteAddr().String()); err != nil {
		t.Fatalf("closing first connection removed second identity: %v", err)
	}
	for range 2 {
		if err := <-writeErrors; err != nil {
			t.Fatal(err)
		}
	}
	_ = firstClient.Close()
	_ = secondClient.Close()
}

func TestPeerRegistrationCollisionPreservesExistingIdentity(t *testing.T) {
	network := NewNetwork()
	if err := network.registerPeer("peer-token", fabric.Identity{NodeID: "runner-1"}); err != nil {
		t.Fatal(err)
	}
	if err := network.registerPeer("peer-token", fabric.Identity{NodeID: "runner-2"}); err == nil {
		t.Fatal("registerPeer() overwrote an existing peer")
	}
	identity, err := network.NewFabric(fabric.Identity{}).WhoIs(t.Context(), "peer-token")
	if err != nil {
		t.Fatal(err)
	}
	if identity.NodeID != "runner-1" {
		t.Fatalf("WhoIs() NodeID = %q, want runner-1", identity.NodeID)
	}
}

type connectionListener struct {
	address     net.Addr
	connections []net.Conn
}

func (l *connectionListener) Accept() (net.Conn, error) {
	if len(l.connections) == 0 {
		return nil, net.ErrClosed
	}
	connection := l.connections[0]
	l.connections = l.connections[1:]
	return connection, nil
}

func (l *connectionListener) Close() error {
	for _, connection := range l.connections {
		_ = connection.Close()
	}
	l.connections = nil
	return nil
}

func (l *connectionListener) Addr() net.Addr { return l.address }

func TestGarbageConnectionDoesNotStopListenerAccepting(t *testing.T) {
	network := NewNetwork()
	server := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	clientIdentity := fabric.Identity{NodeID: "runner-after-garbage", Tags: []string{"agent"}}
	client := network.NewFabric(clientIdentity)

	ln, err := server.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	type acceptResult struct {
		connection net.Conn
		err        error
	}
	accepted := make(chan acceptResult, 1)
	go func() {
		connection, acceptErr := ln.Accept()
		accepted <- acceptResult{connection: connection, err: acceptErr}
	}()

	garbage, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(garbage, "not-a-wefty-identity-header"); err != nil {
		t.Fatal(err)
	}
	if err := garbage.Close(); err != nil {
		t.Fatal(err)
	}

	valid, err := client.Dial(context.Background(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer valid.Close()

	select {
	case result := <-accepted:
		if result.err != nil {
			t.Fatalf("Accept() returned a per-connection identity error: %v", result.err)
		}
		defer result.connection.Close()
		identity, err := server.WhoIs(context.Background(), result.connection.RemoteAddr().String())
		if err != nil {
			t.Fatal(err)
		}
		if identity.NodeID != clientIdentity.NodeID || !slices.Equal(identity.Tags, clientIdentity.Tags) {
			t.Fatalf("WhoIs() = %#v, want %#v", identity, clientIdentity)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("listener did not accept a valid connection after garbage input")
	}
}

func TestPlainRejectsNonLocalAddress(t *testing.T) {
	f := NewNetwork().NewFabric(fabric.Identity{})
	if _, err := f.Listen("tcp", "0.0.0.0:0"); err == nil {
		t.Fatal("Listen() accepted a non-loopback address")
	}
}

func TestConnectHostProjectsPublishedLoopbackHost(t *testing.T) {
	f := NewNetwork().NewFabric(fabric.Identity{NodeID: "node-1"})
	if got := f.ConnectHost(); got != "127.0.0.1" {
		t.Fatalf("ConnectHost() = %q, want 127.0.0.1", got)
	}
}
