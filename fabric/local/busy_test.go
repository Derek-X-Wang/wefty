package local

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// Occupy the production-sized admission limit with requests whose backend
// checks are waiting, so the handshake deadline cannot free capacity for us.
func fillService(t *testing.T) (*Server, []net.Conn) {
	t.Helper()
	entered := make(chan struct{}, 128)
	var calls atomic.Int32
	s, err := NewServer(shortSocket(t), map[string]Participant{Client: {NodeID: "client", Check: func(ctx context.Context) error {
		if calls.Add(1) > 128 {
			return nil
		}
		entered <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}}})
	if err != nil {
		t.Fatal(err)
	}
	serveLocal(t, s)
	var active []net.Conn
	t.Cleanup(func() {
		for _, c := range active {
			_ = c.Close()
		}
	})
	for range 128 {
		c := connectService(t, s)
		active = append(active, c)
		if err := writeFrame(c, request{Version: 1, Operation: "check", Participant: Client, NodeID: "client", TimeoutMillis: 30000}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("request did not occupy a service slot")
		}
	}
	return s, active
}

func connectService(t *testing.T, s *Server) net.Conn {
	t.Helper()
	c, err := net.Dial("unix", s.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestSilentOverflowDoesNotStallBusyReply(t *testing.T) {
	s, _ := fillService(t)
	_ = connectService(t, s) // silent overflow holds a busy-response worker
	d, _ := New(s.path, Client, "client")
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	err := d.Check(ctx)
	var busy *BusyError
	if !errors.As(err, &busy) {
		t.Fatalf("silent overflow stalled busy reply: %v", err)
	}
}

func TestSilentOverflowDoesNotStallFreedSlot(t *testing.T) {
	s, active := fillService(t)
	idle := connectService(t, s)
	// A busy exchange after the idle connection proves it was accepted first.
	// On the base this times out, but keep going to test the freed slot too.
	probe := connectService(t, s)
	_ = probe.SetDeadline(time.Now().Add(100 * time.Millisecond))
	_ = writeFrame(probe, request{Version: 1, Operation: "check", Participant: Client, NodeID: "client", TimeoutMillis: 1000})
	var reply response
	_ = readFrame(probe, &reply)
	_ = active[0].Close()
	deadline := time.Now().Add(time.Second)
	for len(s.slots) == 128 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(s.slots) == 128 {
		t.Fatal("closed caller did not free its slot")
	}
	d, _ := New(s.path, Client, "client")
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	err := d.Check(ctx)
	if err != nil {
		t.Fatalf("silent overflow stalled freed slot: %v", err)
	}
	_ = idle.Close()
}

func TestFullBusyPoolClosesOverflowImmediately(t *testing.T) {
	s, _ := fillService(t)
	for range 4 {
		_ = connectService(t, s)
	}
	extra := connectService(t, s)
	_ = extra.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, err := extra.Read(make([]byte, 1))
	if err == nil {
		t.Fatal("overflow beyond the busy pool stayed open")
	}
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		t.Fatalf("full busy pool stalled overflow close: %v", err)
	}
}

func TestSaturatedServiceCloseIsRetryable(t *testing.T) {
	s, _ := fillService(t)
	for range 4 {
		_ = connectService(t, s)
	}
	deadline := time.Now().Add(time.Second)
	for len(s.busySlots) != 4 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(s.busySlots) != 4 {
		t.Fatal("busy workers did not fill")
	}
	d, _ := New(s.path, Client, "client")
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	err := d.Check(ctx)
	var busy *BusyError
	if !errors.As(err, &busy) {
		t.Fatalf("saturated live service looked missing: %T %v (cause: %v)", err, err, errors.Unwrap(err))
	}
}
