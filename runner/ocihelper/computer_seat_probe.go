package ocihelper

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/coder/websocket"
)

func probeComputerSeatGreeting(ctx context.Context, dial func(context.Context) (net.Conn, error)) error {
	// A failed endpoint does not consume the other Seats' entire readiness
	// budget. The injected Seat clock still decides the final publication.
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) { return dial(ctx) }}
	defer transport.CloseIdleConnections()
	connection, _, err := websocket.Dial(ctx, "ws://computer-backend.invalid"+contract.ComputerDisplayWebSocketPath, &websocket.DialOptions{HTTPClient: &http.Client{Transport: transport}, Subprotocols: []string{contract.ComputerDisplayWebSocketSubprotocol}})
	if err != nil {
		return err
	}
	defer connection.CloseNow()
	if connection.Subprotocol() != contract.ComputerDisplayWebSocketSubprotocol {
		return errors.New("Seat binary subprotocol not negotiated")
	}
	connection.SetReadLimit(8192)
	stream := websocket.NetConn(ctx, connection, websocket.MessageBinary)
	banner := make([]byte, contract.ComputerRFBVersionBannerBytes)
	if _, err := io.ReadFull(stream, banner); err != nil {
		return err
	}
	if !contract.ValidComputerRFBVersionBanner(banner) {
		return errors.New("invalid Seat RFB greeting")
	}
	return nil
}
