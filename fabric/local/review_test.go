package local

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestSlowEstablishmentKeepsSuccessAndIdentityRefusal(t *testing.T) {
	for _, operation := range []string{"check", "dial"} {
		for _, refused := range []bool{false, true} {
			name := operation + "/success"
			if refused {
				name = operation + "/identity-refusal"
			}
			t.Run(name, func(t *testing.T) {
				slow := func(ctx context.Context) error {
					select {
					case <-time.After(60 * time.Millisecond):
					case <-ctx.Done():
						return ctx.Err()
					}
					if refused {
						return &fabric.ParticipantIdentityError{Reason: "needs authorization"}
					}
					return nil
				}
				p := Participant{NodeID: "client", Check: func(context.Context) error { return nil }}
				if operation == "check" {
					p.Check = slow
				} else {
					p.Fabric = &blockingDialFabric{dial: func(ctx context.Context) (net.Conn, error) {
						if err := slow(ctx); err != nil {
							return nil, err
						}
						a, b := net.Pipe()
						t.Cleanup(func() { _ = b.Close() })
						return a, nil
					}}
				}
				s, err := NewServer(shortSocket(t), map[string]Participant{Client: p})
				if err != nil {
					t.Fatal(err)
				}
				s.handshakeTimeout = 15 * time.Millisecond
				serveLocal(t, s)
				d, _ := New(s.path, Client, "client")
				if operation == "check" {
					err = d.Check(t.Context())
				} else {
					var c net.Conn
					c, err = d.Dial(t.Context(), "tcp", "wefty://control-plane")
					if c != nil {
						_ = c.Close()
					}
				}
				if refused {
					var identity *fabric.ParticipantIdentityError
					if !errors.As(err, &identity) {
						t.Fatalf("late refusal lost its type: %v", err)
					}
				} else if err != nil {
					t.Fatalf("late success lost: %v", err)
				}
			})
		}
	}
}

func TestFullServiceReturnsBusyAndReleasesIdleRequests(t *testing.T) {
	s, _ := NewServer(shortSocket(t), map[string]Participant{Client: {NodeID: "client", Check: func(context.Context) error { return nil }}})
	s.slots = make(chan struct{}, 1)
	s.handshakeTimeout = 100 * time.Millisecond
	serveLocal(t, s)
	idle, err := net.Dial("unix", s.path)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	deadline := time.Now().Add(time.Second)
	for len(s.slots) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("idle request was not accepted")
		}
		time.Sleep(time.Millisecond)
	}
	d, _ := New(s.path, Client, "client")
	err = d.Check(t.Context())
	var busy *BusyError
	if !errors.As(err, &busy) {
		t.Fatalf("full service looked dead: %v", err)
	}
	_ = idle.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := idle.Read(make([]byte, 1)); err == nil {
		t.Fatal("idle request stayed open")
	}
	deadline = time.Now().Add(time.Second)
	for {
		err = d.Check(t.Context())
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("service did not recover capacity: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
}

type temporaryAcceptListener struct {
	net.Listener
	failed bool
}

func (l *temporaryAcceptListener) Accept() (net.Conn, error) {
	if !l.failed {
		l.failed = true
		return nil, &net.OpError{Op: "accept", Net: "unix", Err: syscall.EMFILE}
	}
	return l.Listener.Accept()
}
func TestServiceSurvivesTemporaryAcceptError(t *testing.T) {
	s, _ := NewServer(shortSocket(t), map[string]Participant{Client: {NodeID: "client", Check: func(context.Context) error { return nil }}})
	s.listen = func(network, address string) (net.Listener, error) {
		listener, err := net.Listen(network, address)
		if err != nil {
			return nil, err
		}
		return &temporaryAcceptListener{Listener: listener}, nil
	}
	serveLocal(t, s)
	d, _ := New(s.path, Client, "client")
	if err := d.Check(t.Context()); err != nil {
		t.Fatalf("temporary accept error stopped service: %v", err)
	}
}

func TestSocketPublicationUsesItsOwnLockAndError(t *testing.T) {
	s, _ := NewServer(shortSocket(t), map[string]Participant{Client: {NodeID: "client", Check: func(context.Context) error { return nil }}})
	serveLocal(t, s)
	second, _ := NewServer(s.path, map[string]Participant{Client: {NodeID: "other", Check: func(context.Context) error { return nil }}})
	err := second.Serve(t.Context())
	var running *AlreadyRunningError
	if !errors.As(err, &running) {
		t.Fatalf("socket publisher reported participant ownership: %v", err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(s.path), ".participant.lock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket publication used participant lock: %v", err)
	}
}

func TestClientAuthenticatesSocketServerBeforeSending(t *testing.T) {
	path := shortSocket(t)
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	received := make(chan []byte, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			received <- nil
			return
		}
		defer conn.Close()
		payload, _ := io.ReadAll(conn)
		received <- payload
	}()
	d, _ := New(path, Client, "client")
	d.peerUID = func(net.Conn) (uint32, error) { return uint32(os.Geteuid() + 1), nil }
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	err = d.Check(ctx)
	var identity *PeerIdentityError
	if !errors.As(err, &identity) {
		t.Fatalf("impostor service accepted: %v", err)
	}
	if payload := <-received; len(payload) != 0 {
		t.Fatalf("sent %d bytes before authenticating the server", len(payload))
	}
}
func TestSocketPermissionRefusalPreservesFence(t *testing.T) {
	d, _ := New(shortSocket(t), Client, "client")
	d.dialSocket = func(context.Context, string, string) (net.Conn, error) { return nil, syscall.EPERM }
	err := d.Check(t.Context())
	var fenced *FencedCallerError
	if !errors.As(err, &fenced) || !strings.Contains(err.Error(), "ask the owner") || !strings.Contains(err.Error(), "do not look for another way in") || strings.Contains(err.Error(), "allow") || strings.Contains(err.Error(), "start or restart") {
		t.Fatalf("wrong sandbox advice: %v", err)
	}
}
func TestClientToleratesReplyExtensionsButServerRequestsStayStrict(t *testing.T) {
	path := shortSocket(t)
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		var req request
		if err := readFrame(conn, &req); err != nil {
			done <- err
			return
		}
		done <- writeFrame(conn, map[string]any{"future_field": map[string]any{"new": "value"}})
	}()
	d, _ := New(path, Client, "client")
	if err := d.Check(t.Context()); err != nil {
		t.Fatalf("new reply field broke old client: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	s, _ := NewServer(shortSocket(t), map[string]Participant{Client: {NodeID: "client", Check: func(context.Context) error { t.Error("unknown request reached backend"); return nil }}})
	serveLocal(t, s)
	conn, err := net.Dial("unix", s.path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	if err := writeFrame(conn, map[string]any{"version": 1, "operation": "check", "participant": Client, "node_id": "client", "timeout_ms": 1000, "unknown": true}); err != nil {
		t.Fatal(err)
	}
	// The refusal is answered, so a client of another version is not told
	// the service is busy, as a close without reply bytes would say.
	var reply response
	if err := readFrame(conn, &reply); err != nil || reply.Code != "invalid_request" {
		t.Fatalf("unknown request field reply=%+v err=%v", reply, err)
	}
}
func TestDestinationPolicyRefusalSurvivesService(t *testing.T) {
	backend := &blockingDialFabric{dial: func(context.Context) (net.Conn, error) { return nil, &fabric.DestinationRefusedError{} }}
	s, _ := NewServer(shortSocket(t), map[string]Participant{Client: {NodeID: "client", Fabric: backend, Check: func(context.Context) error { return nil }}})
	serveLocal(t, s)
	d, _ := New(s.path, Client, "client")
	_, err := d.Dial(t.Context(), "tcp", "example.com:443")
	var refused *fabric.DestinationRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("destination policy became an availability failure: %v", err)
	}
}

func TestClientPeerCheckPreservesCancellation(t *testing.T) {
	s, _ := NewServer(shortSocket(t), map[string]Participant{Client: {NodeID: "client", Check: func(context.Context) error { return nil }}})
	serveLocal(t, s)
	d, _ := New(s.path, Client, "client")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	d.peerUID = func(net.Conn) (uint32, error) { cancel(); return 0, errors.New("credential check interrupted") }
	if err := d.Check(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation became a policy refusal: %v", err)
	}
}
