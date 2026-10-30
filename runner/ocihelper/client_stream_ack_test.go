package ocihelper

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"
)

// closedAfterReplyConn plays a helper that wrote its stream frames and closed
// without reading the acknowledgement: the frames sit unread, and the one-byte
// acknowledgement write fails (#1282).
type closedAfterReplyConn struct {
	net.Conn
	replies *bytes.Reader
}

func (c *closedAfterReplyConn) Read(p []byte) (int, error) { return c.replies.Read(p) }
func (c *closedAfterReplyConn) Write(p []byte) (int, error) {
	if len(p) == 1 {
		return 0, syscall.EPIPE
	}
	return len(p), nil
}
func (*closedAfterReplyConn) Close() error                     { return nil }
func (*closedAfterReplyConn) SetDeadline(time.Time) error      { return nil }
func (*closedAfterReplyConn) SetReadDeadline(time.Time) error  { return nil }
func (*closedAfterReplyConn) SetWriteDeadline(time.Time) error { return nil }

func TestWatchKeepsHelperReplyAfterFailedAcknowledgement(t *testing.T) {
	event, err := json.Marshal(WatchEvent{Kind: WatchProgress, Status: "running"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		frames []frame
		check  func(t *testing.T, err error, health error)
	}{
		{"typed refusal waiting", []frame{
			{Version: ProtocolVersion, OK: true, Body: event},
			{Version: ProtocolVersion, Error: &RPCError{Code: CodeConnectionLimit, Message: "no slot"}},
		}, func(t *testing.T, err error, health error) {
			var refusal *RPCError
			if !errors.As(err, &refusal) || refusal.Code != CodeConnectionLimit || health != nil {
				t.Fatalf("helper reply lost behind the failed acknowledgement: %v health=%v", err, health)
			}
		}},
		{"final reply waiting", []frame{
			{Version: ProtocolVersion, OK: true, Body: event},
			{Version: ProtocolVersion, OK: true},
		}, func(t *testing.T, err error, health error) {
			if err != nil || health != nil {
				t.Fatalf("final reply lost behind the failed acknowledgement: %v health=%v", err, health)
			}
		}},
		{"nothing waiting", []frame{
			{Version: ProtocolVersion, OK: true, Body: event},
		}, func(t *testing.T, err error, _ error) {
			var loss *RuntimeLossError
			if !errors.As(err, &loss) || !errors.Is(err, syscall.EPIPE) {
				t.Fatalf("a broken stream with no reply is no longer a transport loss: %v", err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var replies bytes.Buffer
			for _, value := range tc.frames {
				payload, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				_ = binary.Write(&replies, binary.BigEndian, uint32(len(payload)))
				replies.Write(payload)
			}
			conn := &closedAfterReplyConn{replies: bytes.NewReader(replies.Bytes())}
			control, controlPeer := net.Pipe()
			defer control.Close()
			defer controlPeer.Close()
			session := &Session{client: &Client{Dial: func(context.Context) (net.Conn, error) { return conn, nil }}, capability: "test", control: control}
			events := 0
			err := session.Watch(t.Context(), WatchRequest{}, func(WatchEvent) error { events++; return nil })
			if events != 1 {
				t.Fatalf("events=%d, want 1", events)
			}
			tc.check(t, err, session.HealthError())
		})
	}
}
