package local

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
	"testing"
)

type failedExchangeConn struct {
	net.Conn
	writeErr, readErr error
	prefix            []byte
}

func (c *failedExchangeConn) Write(p []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	return len(p), nil
}
func (c *failedExchangeConn) Read(p []byte) (int, error) {
	if len(c.prefix) > 0 {
		n := copy(p, c.prefix)
		c.prefix = c.prefix[n:]
		return n, nil
	}
	return 0, c.readErr
}

func TestAuthenticatedCloseBeforeReplyIsRetryable(t *testing.T) {
	for _, tc := range []struct {
		name              string
		writeErr, readErr error
		prefix            []byte
		busy              bool
	}{
		{name: "write-pipe", writeErr: syscall.EPIPE, busy: true},
		{name: "write-reset", writeErr: syscall.ECONNRESET, busy: true},
		{name: "read-eof", readErr: io.EOF, busy: true},
		{name: "read-reset", readErr: syscall.ECONNRESET, busy: true},
		{name: "partial-reply-reset", readErr: syscall.ECONNRESET, prefix: []byte{0}, busy: false},
		{name: "partial-reply-eof", readErr: io.EOF, prefix: []byte{0}, busy: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := net.Pipe()
			t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
			d, _ := New(shortSocket(t), Client, "client")
			authenticated := false
			d.peerUID = func(net.Conn) (uint32, error) { authenticated = true; return uint32(syscall.Geteuid()), nil }
			d.dialSocket = func(context.Context, string, string) (net.Conn, error) {
				return &failedExchangeConn{Conn: a, writeErr: tc.writeErr, readErr: tc.readErr, prefix: tc.prefix}, nil
			}
			err := d.Check(t.Context())
			var busy *BusyError
			if !authenticated || errors.As(err, &busy) != tc.busy || err == nil {
				t.Fatalf("wrong close classification: authenticated=%v err=%T %v", authenticated, err, err)
			}
		})
	}
}

func TestMissingServiceStillNeedsOwnerAction(t *testing.T) {
	for _, cause := range []error{syscall.ENOENT, syscall.ECONNREFUSED} {
		t.Run(cause.Error(), func(t *testing.T) {
			d, _ := New(shortSocket(t), Client, "client")
			d.dialSocket = func(context.Context, string, string) (net.Conn, error) { return nil, cause }
			err := d.Check(t.Context())
			var missing *UnavailableError
			var busy *BusyError
			if !errors.As(err, &missing) || errors.As(err, &busy) {
				t.Fatalf("missing service became retryable: %T %v", err, err)
			}
		})
	}
}

// A service of another version answers invalid_request; that needs the
// owner, not a retry.
func TestInvalidRequestReplyNeedsOwnerAction(t *testing.T) {
	a, b := net.Pipe()
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	go func() {
		var req request
		if readFrame(b, &req) == nil {
			_ = writeFrame(b, response{Code: "invalid_request"})
		}
	}()
	d, _ := New(shortSocket(t), Client, "client")
	d.peerUID = func(net.Conn) (uint32, error) { return uint32(syscall.Geteuid()), nil }
	d.dialSocket = func(context.Context, string, string) (net.Conn, error) { return a, nil }
	err := d.Check(t.Context())
	var unavailable *UnavailableError
	var busy *BusyError
	if !errors.As(err, &unavailable) || errors.As(err, &busy) {
		t.Fatalf("refused request format: %T %v", err, err)
	}
}
