package takeover

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

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
