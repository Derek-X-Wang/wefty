//go:build tsnet_smoke

package tsnet

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestTSNetSmoke(t *testing.T) {
	authKey := os.Getenv("TS_AUTHKEY")
	if authKey == "" {
		t.Skip("TS_AUTHKEY is not available")
	}
	controlURL := os.Getenv("TS_CONTROL_URL")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	serverName := "wefty://node/smoke-server-" + suffix
	clientName := "wefty://node/smoke-client-" + suffix

	server, err := New(Config{
		Name:           serverName,
		StateDir:       t.TempDir(),
		Credential:     fabric.Credential{Value: authKey},
		Ephemeral:      true,
		CoordinatorURL: controlURL,
		Logf:           t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client, err := New(Config{
		Name:           clientName,
		StateDir:       t.TempDir(),
		Credential:     fabric.Credential{Value: authKey},
		Ephemeral:      true,
		CoordinatorURL: controlURL,
		Logf:           t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ln, err := server.Listen("tcp", serverName)
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
		identity, err := server.WhoIs(context.Background(), conn.RemoteAddr().String())
		if err != nil {
			serverErr <- err
			return
		}
		if identity.NodeID == "" {
			serverErr <- fmt.Errorf("WhoIs returned an empty node ID")
			return
		}
		_, err = io.Copy(conn, conn)
		serverErr <- err
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	conn, err := client.Dial(ctx, "tcp", serverName)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(conn, "tsnet smoke\n"); err != nil {
		t.Fatal(err)
	}
	got, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if got != "tsnet smoke\n" {
		t.Fatalf("echo = %q, want %q", got, "tsnet smoke\n")
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}
