package local

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
)

func shortSocket(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "wl-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "s")
}
func serveLocal(t *testing.T, s *Server) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	// The socket file appears at bind, before listen; a dial in between is
	// refused. Wait until listen has returned, not only for the file.
	listening := make(chan struct{})
	listen := s.listen
	s.listen = func(network, address string) (net.Listener, error) {
		defer close(listening)
		return listen(network, address)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("service did not stop")
		}
	})
	select {
	case <-listening:
	case <-time.After(3 * time.Second):
		t.Fatal("socket was not published")
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if info, err := os.Stat(s.path); err == nil && info.Mode()&os.ModeSocket != 0 && info.Mode().Perm() == 0o600 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("socket was not published")
}
func TestLocalStreamAndServerIdentity(t *testing.T) {
	network := plain.NewNetwork()
	receiver := network.NewFabric(fabric.Identity{NodeID: "l1"})
	listener, err := receiver.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	want := fabric.Identity{NodeID: "client", Tags: []string{"tag:wefty-client"}}
	backend := network.NewFabric(want)
	s, err := NewServer(shortSocket(t), map[string]Participant{Client: {NodeID: "client", Fabric: backend, Check: func(context.Context) error { return nil }}})
	if err != nil {
		t.Fatal(err)
	}
	serveLocal(t, s)
	for _, path := range []string{filepath.Dir(s.path), s.path} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		expected := os.FileMode(0o600)
		if info.IsDir() {
			expected = 0o700
		}
		if info.Mode().Perm() != expected {
			t.Fatalf("mode=%o want %o", info.Mode().Perm(), expected)
		}
	}
	received := make(chan fabric.Identity, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		id, _ := receiver.WhoIs(context.Background(), conn.RemoteAddr().String())
		received <- id
		for _, part := range []string{"first\n", "second\n"} {
			_, _ = io.WriteString(conn, part)
			time.Sleep(20 * time.Millisecond)
		}
	}()
	d, err := New(s.path, Client, "client")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	conn, err := d.Dial(ctx, "tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cancel() // establishment context must not kill the returned stream.
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	data, err := io.ReadAll(conn)
	if err != nil || string(data) != "first\nsecond\n" {
		t.Fatalf("stream=%q err=%v", data, err)
	}
	got := <-received
	got.FabricID = ""
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("server saw %+v want %+v", got, want)
	}
}
func TestLocalCancellationStopsBackendCheck(t *testing.T) {
	entered, stopped := make(chan struct{}), make(chan struct{})
	s, err := NewServer(shortSocket(t), map[string]Participant{Client: {NodeID: "client", Check: func(ctx context.Context) error { close(entered); <-ctx.Done(); close(stopped); return ctx.Err() }}})
	if err != nil {
		t.Fatal(err)
	}
	serveLocal(t, s)
	d, _ := New(s.path, Client, "client")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- d.Check(ctx) }()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("client close did not cancel service check")
	}
}
func TestLocalRefusesDifferentUID(t *testing.T) {
	called := make(chan struct{}, 1)
	s, _ := NewServer(shortSocket(t), map[string]Participant{Client: {NodeID: "client", Check: func(context.Context) error { called <- struct{}{}; return nil }}})
	s.peerUID = func(net.Conn) (uint32, error) { return s.uid + 1, nil }
	serveLocal(t, s)
	d, _ := New(s.path, Client, "client")
	if err := d.Check(t.Context()); err == nil {
		t.Fatal("different uid was accepted")
	}
	select {
	case <-called:
		t.Fatal("different uid reached backend")
	default:
	}
}
func TestLocalTypedRefusalsAndBounds(t *testing.T) {
	s, _ := NewServer(shortSocket(t), map[string]Participant{Client: {NodeID: "client", Check: func(context.Context) error { return &fabric.ParticipantIdentityError{Reason: "needs authorization"} }}})
	serveLocal(t, s)
	for _, tc := range []struct {
		role, node string
		person     bool
	}{{Client, "client", false}, {Client, "stale-profile", false}, {Person, "person", true}} {
		d, _ := New(s.path, tc.role, tc.node)
		err := d.Check(t.Context())
		var identity *fabric.ParticipantIdentityError
		if !errors.As(err, &identity) || identity.Person != tc.person {
			t.Fatalf("%s refusal=%v", tc.role, err)
		}
	}
	d, _ := New(s.path, Client, "client")
	for _, address := range []string{"", "bad address", string(make([]byte, 513))} {
		if _, err := d.Dial(t.Context(), "tcp", address); err == nil {
			t.Fatal("unbounded address accepted")
		}
	}
	if _, err := d.Dial(t.Context(), "unix", "/tmp/arbitrary"); err == nil {
		t.Fatal("non-Fabric dial accepted")
	}
}

func TestLocalRefusesPublicSocketFolderWithoutChangingIt(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := privateDirectory(directory); err == nil {
		t.Fatal("public socket folder accepted")
	}
	info, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatal("changed an existing folder's permissions")
	}
}

func TestLocalCancellationStopsBlockedDial(t *testing.T) {
	entered, stopped := make(chan struct{}), make(chan struct{})
	backend := &blockingDialFabric{dial: func(ctx context.Context) (net.Conn, error) {
		close(entered)
		<-ctx.Done()
		close(stopped)
		return nil, ctx.Err()
	}}
	s, err := NewServer(shortSocket(t), map[string]Participant{Client: {NodeID: "client", Fabric: backend, Check: func(context.Context) error { return nil }}})
	if err != nil {
		t.Fatal(err)
	}
	serveLocal(t, s)
	d, _ := New(s.path, Client, "client")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := d.Dial(ctx, "tcp", "wefty://control-plane"); done <- err }()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("client close did not cancel service dial")
	}
}

type blockingDialFabric struct {
	fabric.Fabric
	dial func(context.Context) (net.Conn, error)
}

func (f *blockingDialFabric) Dial(ctx context.Context, _, _ string) (net.Conn, error) {
	return f.dial(ctx)
}

func TestLocalDeadlineIsTypedUnconfirmedParticipant(t *testing.T) {
	s, _ := NewServer(shortSocket(t), map[string]Participant{Client: {NodeID: "client", Check: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}})
	serveLocal(t, s)
	d, _ := New(s.path, Client, "client")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	err := d.Check(ctx)
	var unreachable *fabric.ParticipantUnreachableError
	if !errors.As(err, &unreachable) {
		t.Fatalf("unconfirmed deadline lost type: %v", err)
	}
}

func TestLocalRejectsUnboundedOrUnknownWireRequests(t *testing.T) {
	calls := make(chan struct{}, 16)
	s, _ := NewServer(shortSocket(t), map[string]Participant{Client: {NodeID: "client", Check: func(context.Context) error { calls <- struct{}{}; return nil }}})
	serveLocal(t, s)
	for _, change := range []func(*request){
		func(r *request) { r.Operation = "exec" },
		func(r *request) { r.Network = "unix" },
		func(r *request) { r.TimeoutMillis = 30001 },
		func(r *request) { r.Address = string(make([]byte, 513)) },
		func(r *request) { r.Version = 2 },
	} {
		r := request{Version: 1, Operation: "dial", Participant: Client, NodeID: "client", Network: "tcp", Address: "wefty://control-plane", TimeoutMillis: 1000}
		change(&r)
		conn, err := net.Dial("unix", s.path)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		if err := writeFrame(conn, r); err != nil {
			t.Fatal(err)
		}
		var reply response
		if err := readFrame(conn, &reply); err != nil || reply.Code != "invalid_request" {
			t.Fatalf("wire refusal=%+v err=%v", reply, err)
		}
		_ = conn.Close()
	}
	conn, err := net.Dial("unix", s.path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	_, _ = conn.Write([]byte{0, 1, 0, 0})
	var reply response
	if err := readFrame(conn, &reply); err != nil || reply.Code != "invalid_request" {
		t.Fatalf("oversized frame refusal=%+v err=%v", reply, err)
	}
	select {
	case <-calls:
		t.Fatal("invalid wire request reached a participant")
	default:
	}
}
