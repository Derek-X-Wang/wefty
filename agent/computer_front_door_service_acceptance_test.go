//go:build service_acceptance

package agent

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/coder/websocket"
)

func TestServiceAcceptanceComputerFrontDoorRealProcessAuthorityLoss(t *testing.T) {
	command := exec.Command(os.Args[0], "-test.run=^TestComputerFrontDoorBackendProcess$", "--")
	command.Env = append(os.Environ(), "WEFTY_COMPUTER_BACKEND_HELPER=1")
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = stdin.Close()
		if err := command.Wait(); err != nil {
			t.Errorf("backend helper process: %v", err)
		}
	}()
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		t.Fatalf("backend helper did not publish an address: %v", scanner.Err())
	}
	backendAddress := scanner.Text()

	now := time.Now().UTC()
	identity := fabric.Identity{FabricID: "fabric-one", UserID: "person-real", DeviceID: "device-real"}
	identityFabric := &mutableWhoIsFabric{}
	identityFabric.set(identity, nil)
	cache := NewComputerPolicyCache(systemClock{}, "node-1", "boot-1")
	defer cache.Close()
	if _, err := cache.Install(policySnapshot(t, now, 1, 1, nil, l1.ComputerGrant{
		FabricID: identity.FabricID, UserID: identity.UserID, Permission: l1.ComputerGrantView, PolicyRevision: 1,
	})); err != nil {
		t.Fatal(err)
	}
	auditor := &recordingComputerAuditor{}
	// Real-process acceptance deliberately keeps this authority interval above
	// one second so shared runners cannot turn scheduling jitter into a false
	// lease-loss result.
	authority, cancelAuthority := context.WithTimeout(t.Context(), 1500*time.Millisecond)
	defer cancelAuthority()
	dialBackend := func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", backendAddress)
	}
	frontDoor, err := newComputerFrontDoor(computerFrontDoorConfig{
		authorityContext: authority, fabric: identityFabric, authorizer: cache, auditor: auditor,
		computerID: "computer-1", jobID: "job-1", attemptID: "attempt-1", fencingToken: "fence-1",
		dial: dialBackend,
	})
	if err != nil {
		t.Fatal(err)
	}
	frontDoor.SetReady(true)
	server := httptest.NewServer(frontDoor)
	defer server.Close()
	connection := dialComputerFrontDoor(t, server.URL, nil)
	defer connection.CloseNow()
	if _, banner, err := connection.Read(t.Context()); err != nil || string(banner) != "RFB 003.008\n" {
		t.Fatalf("real-process RFB banner = %q err=%v", banner, err)
	}
	select {
	case <-authority.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("real-process authority lease did not expire")
	}
	waitComputerAuditKind(t, auditor, l1.ComputerTakeoverSessionClose)
	events := auditor.snapshot()
	if got := events[len(events)-1].Reason; got != "attempt_authority_lost" {
		t.Fatalf("real-process authority close reason = %q", got)
	}
}

func TestComputerFrontDoorBackendProcess(t *testing.T) {
	if os.Getenv("WEFTY_COMPUTER_BACKEND_HELPER") != "1" {
		return
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != computerWebSocketPath {
			http.NotFound(writer, request)
			return
		}
		connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{Subprotocols: []string{computerWebSocketSubprotocol}})
		if err != nil {
			return
		}
		defer connection.CloseNow()
		if err := connection.Write(request.Context(), websocket.MessageBinary, []byte("RFB 003.008\n")); err != nil {
			return
		}
		for {
			kind, payload, err := connection.Read(request.Context())
			if err != nil {
				return
			}
			if err := connection.Write(request.Context(), kind, payload); err != nil {
				return
			}
		}
	})}
	go server.Serve(listener)
	if _, err := fmt.Fprintln(os.Stdout, listener.Addr().String()); err != nil {
		t.Fatal(err)
	}
	_ = os.Stdout.Sync()
	_, _ = io.Copy(io.Discard, os.Stdin)
	_ = server.Close()
}
