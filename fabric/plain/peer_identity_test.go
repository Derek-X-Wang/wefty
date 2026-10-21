package plain_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
)

func TestPeerProbeOutboundIdentity(t *testing.T) {
	for _, target := range []string{"wefty://run-ledger", "127.0.0.1:0"} {
		t.Run(target, func(t *testing.T) {
			network := plain.NewNetwork()
			observer := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
			peer := network.NewFabric(fabric.Identity{NodeID: "run-ledger"})
			listener, err := peer.Listen("tcp", target)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			accepted := make(chan net.Conn, 1)
			go func() { c, _ := listener.Accept(); accepted <- c }()
			if target == "127.0.0.1:0" {
				target = listener.Addr().String()
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			conn, err := observer.Dial(ctx, "tcp", target)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			var incoming net.Conn
			select {
			case incoming = <-accepted:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if incoming == nil {
				t.Fatal("accept failed")
			}
			defer incoming.Close()
			caller, err := peer.WhoIs(ctx, incoming.RemoteAddr().String())
			if err != nil || caller.NodeID != "control-plane" {
				t.Fatalf("caller=%+v err=%v", caller, err)
			}
			remote := conn.RemoteAddr().String()
			identity, err := observer.WhoIs(ctx, remote)
			if err != nil || identity.NodeID != "run-ledger" {
				t.Fatalf("answering peer=%+v err=%v", identity, err)
			}
			if observer.PersonIdentityTrust() != fabric.PersonIdentitySelfAsserted {
				t.Fatal("plain identity must stay self-asserted")
			}
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := observer.WhoIs(context.Background(), remote); !errors.Is(err, fabric.ErrIdentityNotFound) {
				t.Fatalf("closed connection retained identity: %v", err)
			}
		})
	}
}
