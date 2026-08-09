package plain

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"testing"

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

func TestPlainRejectsNonLocalAddress(t *testing.T) {
	f := NewNetwork().NewFabric(fabric.Identity{})
	if _, err := f.Listen("tcp", "0.0.0.0:0"); err == nil {
		t.Fatal("Listen() accepted a non-loopback address")
	}
}
