package ocihelper

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/coder/websocket"
)

func TestComputerSeatGreetingRequiresNegotiatedBinaryRFB(t *testing.T) {
	for _, test := range []struct {
		name      string
		protocols []string
		kind      websocket.MessageType
		banner    string
		want      bool
	}{
		{"ready", []string{contract.ComputerDisplayWebSocketSubprotocol}, websocket.MessageBinary, "RFB 003.008\n", true},
		{"no subprotocol", nil, websocket.MessageBinary, "RFB 003.008\n", false},
		{"text greeting", []string{contract.ComputerDisplayWebSocketSubprotocol}, websocket.MessageText, "RFB 003.008\n", false},
		{"invalid greeting", []string{contract.ComputerDisplayWebSocketSubprotocol}, websocket.MessageBinary, "NOT RFB 000\n", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != contract.ComputerDisplayWebSocketPath {
					http.NotFound(w, r)
					return
				}
				c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: test.protocols})
				if err != nil {
					return
				}
				defer c.CloseNow()
				if err := c.Write(r.Context(), test.kind, []byte(test.banner)); err != nil {
					return
				}
				// Retain the socket until the helper closes its probe.
				_, _, _ = c.Read(r.Context())
			}))
			defer server.Close()
			err := probeComputerSeatGreeting(t.Context(), func(ctx context.Context) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "tcp4", strings.TrimPrefix(server.URL, "http://"))
			})
			if (err == nil) != test.want {
				t.Fatalf("greeting result = %v, want success=%t", err, test.want)
			}
		})
	}
}

func TestComputerSeatReadyFileCannotReplaceEitherGreeting(t *testing.T) {
	seats := newComputerSeats(nil, t.TempDir())
	seats.apply([]ComputerSeat{{1, "seat1", 7102, 7103}})
	read := func(string) ([]byte, error) {
		return []byte(`{"version":1,"generation":1,"place":1,"state":"ready","reason":"","cleanup_complete":false}`), nil
	}
	r, err := seats.observe(t.Context(), 1, read, func(_ context.Context, port uint16) error {
		if port == 7103 {
			return io.EOF
		}
		return nil
	}, func(ComputerSeat) error { return nil })
	if err != nil || r.Seats[0].State == "ready" {
		t.Fatalf("ready file bypassed second greeting: %+v %v", r, err)
	}
}
