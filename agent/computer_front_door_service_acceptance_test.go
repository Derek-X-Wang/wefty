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
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/takeover"
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
		computerID: "computer-1", jobID: "job-1", attemptID: "attempt-1", storageID: "storage-1", storageGeneration: 1,
		fencingToken: "fence-1",
		dial:         dialBackend,
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

func TestServiceAcceptanceControllerTenureRealProcessAuthorityLoss(t *testing.T) {
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

	// The real process interval remains above one second so the acceptance
	// proof cannot be won or lost by shared-runner scheduling jitter.
	authority, cancelAuthority := context.WithTimeout(t.Context(), 1500*time.Millisecond)
	defer cancelAuthority()
	auditor := &recordingComputerAuditor{}
	identity := fabric.Identity{FabricID: "fabric-one", UserID: "person-real", DeviceID: "device-real"}
	identityFabric := &mutableWhoIsFabric{}
	identityFabric.set(identity, nil)
	cache := NewComputerPolicyCache(systemClock{}, "node-1", "boot-1")
	defer cache.Close()
	if _, err := cache.Install(policySnapshot(t, time.Now().UTC(), 1, 1, nil, l1.ComputerGrant{
		FabricID: identity.FabricID, UserID: identity.UserID, Permission: l1.ComputerGrantControl, PolicyRevision: 1,
	})); err != nil {
		t.Fatal(err)
	}
	var signalMu sync.Mutex
	var signals []bool
	dialBackend := func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", backendAddress)
	}
	tenure, err := newControllerTenure(controllerTenureConfig{
		authorityContext: authority,
		dial:             dialBackend,
		setControlState: func(_ context.Context, value bool) error {
			signalMu.Lock()
			signals = append(signals, value)
			signalMu.Unlock()
			return nil
		},
		record: func(ctx context.Context, event l1.ComputerTakeoverAuditEvent) (l1.ComputerTakeoverAuditReceipt, error) {
			return auditor.AppendComputerTakeoverAudit(ctx, "computer-1", "job-1", "attempt-1",
				l1.ComputerTakeoverAuditRequest{FencingToken: "fence-1", Event: event})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	frontDoor, err := newComputerFrontDoor(computerFrontDoorConfig{
		authorityContext: authority, fabric: identityFabric, authorizer: cache, auditor: auditor,
		computerID: "computer-1", jobID: "job-1", attemptID: "attempt-1", storageID: "storage-1", storageGeneration: 1,
		fencingToken: "fence-1",
		dial:         dialBackend, controlTenure: tenure,
	})
	if err != nil {
		t.Fatal(err)
	}
	tenure.config.report = frontDoor.report
	frontDoor.SetReady(true)
	server := httptest.NewServer(frontDoor)
	defer server.Close()
	connection, token := dialComputerFrontDoorWithToken(t, server.URL, nil)
	defer connection.CloseNow()
	if _, banner, err := connection.Read(t.Context()); err != nil || string(banner) != "RFB 003.008\n" {
		t.Fatalf("real-process Controller banner = %q err=%v", banner, err)
	}
	if _, err := takeover.Perform(t.Context(), directTakeoverFabric{},
		"ws"+server.URL[len("http"):]+contract.ComputerDisplayWebSocketPath, token, "take"); err != nil {
		t.Fatalf("real-process CLI take adapter: %v", err)
	}
	if err := connection.Write(t.Context(), websocket.MessageBinary, []byte("real input")); err != nil {
		t.Fatal(err)
	}
	if _, payload, err := connection.Read(t.Context()); err != nil || string(payload) != "real input" {
		t.Fatalf("real-process control relay = %q err=%v", payload, err)
	}
	select {
	case <-authority.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("real-process Controller authority did not expire")
	}
	readContext, cancelRead := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelRead()
	if _, _, err := connection.Read(readContext); err == nil {
		t.Fatal("real-process expired control session remained open")
	}
	waitComputerAuditKind(t, auditor, l1.ComputerTakeoverSessionClose)
	events := auditor.snapshot()
	if len(events) != 4 || events[0].Kind != l1.ComputerTakeoverSessionOpen || events[1].Kind != l1.ComputerTakeoverControlAcquired ||
		events[2].Kind != l1.ComputerTakeoverControlReleased || events[2].Reason != l1.ComputerTakeoverAttemptAuthorityLost ||
		events[3].Kind != l1.ComputerTakeoverSessionClose || events[3].Reason != l1.ComputerTakeoverAttemptAuthorityLost {
		t.Fatalf("real-process Controller audit = %#v", events)
	}
	signalMu.Lock()
	defer signalMu.Unlock()
	if len(signals) != 2 || !signals[0] || signals[1] {
		t.Fatalf("real-process Controller signals = %v", signals)
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
