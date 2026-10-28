package ocicontrol

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestNodePeerRefusalsAreStructuredAndOwnerPasses(t *testing.T) {
	for _, mode := range []string{"owner", "descendant", "lookup-error"} {
		t.Run(mode, func(t *testing.T) {
			server, err := NewNodeServer("/tmp/test-control", ServiceFuncs{})
			if err != nil {
				t.Fatal(err)
			}
			server.peerPID = func(net.Conn) (int, error) { return os.Getpid() + 1000000, nil }
			server.parentPID = func(int) (int, error) {
				switch mode {
				case "descendant":
					return os.Getpid(), nil
				case "lookup-error":
					return 0, errors.New("denied")
				default:
					return 1, nil
				}
			}
			reached := false
			handler := server.authorizePeer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { reached = true; w.WriteHeader(204) }))
			peer := &controlPeer{}
			for range 2 {
				recorder := httptest.NewRecorder()
				request := httptest.NewRequest("GET", "/v1/intent", nil).WithContext(context.WithValue(t.Context(), controlPeerKey{}, peer))
				handler.ServeHTTP(recorder, request)
				if mode == "owner" {
					if recorder.Code != 204 || !reached {
						t.Fatal("owner was refused")
					}
					continue
				}
				var refusal contract.ErrorResponse
				if err := json.Unmarshal(recorder.Body.Bytes(), &refusal); err != nil {
					t.Fatal(err)
				}
				if recorder.Code != 403 || refusal.Error.Code != contract.ErrorForbidden || !strings.Contains(refusal.Error.Message, "owner's own shell") || reached {
					t.Fatalf("unstructured/refused caller reached service: %s", recorder.Body.String())
				}
			}
		})
	}
}
