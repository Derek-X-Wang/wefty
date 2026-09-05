//go:build tsnet_smoke

package tsnet

import (
	"bufio"
	"context"
	"encoding/json"
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
		if identity.NodeID == "" || identity.DeviceID == "" || identity.FabricID == "" ||
			identity.Kind != fabric.IdentityKindMachine || identity.UserID != "" {
			serverErr <- fmt.Errorf("WhoIs returned invalid machine identity")
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
	serverConnectHost := server.ConnectHost()
	clientConnectHost := client.ConnectHost()
	if serverConnectHost == "" || clientConnectHost == "" || serverConnectHost == clientConnectHost {
		t.Fatalf("ConnectHost server=%q client=%q, want two distinct Fabric presentation addresses", serverConnectHost, clientConnectHost)
	}
	writeFabricIdentitySmokeReceipt(t, os.Getenv("WEFTY_FABRIC_MACHINE_RECEIPT"), map[string]fabricIdentitySmokeRow{
		"fabric.machine_dns_acl": {
			Status: "PASS",
			Assertions: map[string]bool{
				"acl_dial_succeeded":             true,
				"dns_resolved":                   true,
				"machine_identity_authenticated": true,
			},
			Evidence: map[string]string{"listener_connect_host": serverConnectHost, "peer_connect_host": clientConnectHost},
		},
		"fabric.machine_second_peer_reachability": {
			Status: "PASS",
			Assertions: map[string]bool{
				"distinct_peer":   true,
				"echo_round_trip": true,
			},
			Evidence: map[string]string{"listener_connect_host": serverConnectHost, "peer_connect_host": clientConnectHost},
		},
	})
}

type fabricIdentitySmokeRow struct {
	Status     string            `json:"status"`
	Assertions map[string]bool   `json:"assertions"`
	Evidence   map[string]string `json:"evidence"`
	Deviations []any             `json:"deviations"`
}

func writeFabricIdentitySmokeReceipt(t *testing.T, output string, rows map[string]fabricIdentitySmokeRow) {
	t.Helper()
	if output == "" {
		return
	}
	for id, row := range rows {
		if row.Deviations == nil {
			row.Deviations = []any{}
			rows[id] = row
		}
	}
	candidate := os.Getenv("CANDIDATE_SHA")
	if len(candidate) != 40 {
		t.Fatalf("CANDIDATE_SHA = %q, want 40 lowercase hexadecimal characters", candidate)
	}
	for _, character := range candidate {
		if character < '0' || character > '9' {
			if character < 'a' || character > 'f' {
				t.Fatalf("CANDIDATE_SHA = %q, want 40 lowercase hexadecimal characters", candidate)
			}
		}
	}
	payload, err := json.MarshalIndent(map[string]any{"version": 1, "candidate_sha": candidate, "rows": rows}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	payload = append(payload, '\n')
	if err := os.WriteFile(output, payload, 0o600); err != nil {
		t.Fatal(err)
	}
}
