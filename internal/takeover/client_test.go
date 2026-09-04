package takeover

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/coder/websocket"
)

func TestOpenAcceptsAndRetainsRawConnectHost(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set(contract.ComputerControlTokenHeader, "session-token")
		connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{
			Subprotocols: []string{contract.ComputerDisplayWebSocketSubprotocol},
		})
		if err != nil {
			return
		}
		defer connection.CloseNow()
		if err := connection.Write(request.Context(), websocket.MessageBinary, []byte("RFB 003.008\n")); err != nil {
			return
		}
		<-request.Context().Done()
	}))
	defer server.Close()

	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	for _, rawHost := range []string{"fabric-address.example.test", "2001:db8::1"} {
		t.Run(rawHost, func(t *testing.T) {
			rawAddress := net.JoinHostPort(rawHost, target.Port())
			participant := &routedFabric{rawAddress: rawAddress, targetAddress: target.Host}
			endpoint := (&url.URL{Scheme: "ws", Host: rawAddress, Path: contract.ComputerDisplayWebSocketPath}).String()
			session, err := Open(t.Context(), participant, endpoint)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			if participant.dialedAddress != rawAddress || session.ConnectHost != rawAddress {
				t.Fatalf("raw connect host = dialed %q projected %q, want %q / %q",
					participant.dialedAddress, session.ConnectHost, rawAddress, rawAddress)
			}
		})
	}
}

func TestPerformUsesStructuredControlCodesAndUnknownStatusesAreNotRetryable(t *testing.T) {
	for _, test := range []struct {
		name      string
		status    int
		code      contract.ErrorCode
		want      contract.ErrorCode
		retryable bool
	}{
		{name: "already held", status: http.StatusConflict, code: contract.ErrorControllerAlreadyHeld, want: contract.ErrorControllerAlreadyHeld},
		{name: "busy", status: http.StatusConflict, code: contract.ErrorControllerBusy, want: contract.ErrorControllerBusy, retryable: true},
		{name: "unknown", status: http.StatusBadGateway, code: contract.ErrorTenureUnavailable, want: contract.ErrorInternal},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(test.status)
				_ = json.NewEncoder(writer).Encode(contract.ComputerControlErrorResponse{Error: contract.APIError{
					Code: test.code, Message: "injected", Retryable: true,
				}})
			}))
			defer server.Close()
			endpoint := "ws" + strings.TrimPrefix(server.URL, "http") + contract.ComputerDisplayWebSocketPath
			_, err := Perform(t.Context(), directFabric{}, endpoint, "session-token", "take")
			var actionErr *ActionError
			if !errors.As(err, &actionErr) || actionErr.APIError.Code != test.want || actionErr.APIError.Retryable != test.retryable {
				t.Fatalf("Perform error = %#v, want code=%q retryable=%t", err, test.want, test.retryable)
			}
		})
	}
}

type directFabric struct{}

func (directFabric) Listen(string, string) (net.Listener, error) { return nil, errors.New("unused") }
func (directFabric) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, address)
}
func (directFabric) WhoIs(context.Context, string) (fabric.Identity, error) {
	return fabric.Identity{}, errors.New("unused")
}
func (directFabric) ConnectHost() string { return "unused" }

type routedFabric struct {
	rawAddress    string
	targetAddress string
	dialedAddress string
}

func (*routedFabric) Listen(string, string) (net.Listener, error) {
	return nil, errors.New("unused")
}
func (f *routedFabric) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	f.dialedAddress = address
	if address != f.rawAddress {
		return nil, errors.New("unexpected raw connect host")
	}
	return (&net.Dialer{}).DialContext(ctx, network, f.targetAddress)
}
func (*routedFabric) WhoIs(context.Context, string) (fabric.Identity, error) {
	return fabric.Identity{}, errors.New("unused")
}
func (*routedFabric) ConnectHost() string { return "unused" }
