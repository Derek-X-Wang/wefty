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
	clientIdentity := fabric.Identity{NodeID: "runner-1", User: "agent@example.com", Tags: []string{"runner", "linux"}}
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
		if got.NodeID != clientIdentity.NodeID || got.User != clientIdentity.User || !slices.Equal(got.Tags, clientIdentity.Tags) {
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
