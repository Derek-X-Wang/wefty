package computerconformance_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

func screenLaneJSON(ctx context.Context, f fabric.Fabric, host, method, path, credential string, body any, result any) error {
	var reader io.Reader
	if body != nil {
		data, e := json.Marshal(body)
		if e != nil {
			return e
		}
		reader = bytes.NewReader(data)
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) { return f.Dial(ctx, network, host) }}
	defer transport.CloseIdleConnections()
	request, e := http.NewRequestWithContext(ctx, method, "http://"+host+path, reader)
	if e != nil {
		return e
	}
	if strings.Contains(path, "/person/") {
		request.Header.Set(contract.ComputerControlTokenHeader, credential)
	} else {
		request.Header.Set(l1.SeatCredentialHeader, credential)
	}
	response, e := (&http.Client{Transport: transport}).Do(request)
	if e != nil {
		return e
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		data, _ := io.ReadAll(response.Body)
		return fmt.Errorf("%s %s: HTTP %d %s", method, path, response.StatusCode, data)
	}
	if result != nil {
		return json.NewDecoder(response.Body).Decode(result)
	}
	return nil
}

// Exercise the same request and Fabric transport used by the root screen lane.
func TestSeatScreenLanePersonHost(t *testing.T) {
	network, err := plain.NewNetworkWithID("plain-screen-host-test")
	if err != nil {
		t.Fatal(err)
	}
	door := network.NewFabric(fabric.Identity{NodeID: "screen-node"})
	listener, err := door.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	host := listener.Addr().String()
	path := "/v1/seats/seat_00000000000000000000000000000001/person/wefty/control/take"
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != host {
			http.Error(w, "Seat viewer host is not allowed", http.StatusForbidden)
			return
		}
		if r.URL.Path != path || r.Method != http.MethodPost || r.Header.Get(contract.ComputerControlTokenHeader) != "owner-token" || r.Header.Get(l1.SeatCredentialHeader) != "" {
			http.Error(w, "wrong person request", http.StatusBadRequest)
		}
	})}
	go server.Serve(listener)
	defer server.Close()
	owner := network.NewFabric(fabric.Identity{NodeID: "screen-owner"})
	if err := screenLaneJSON(t.Context(), owner, host, http.MethodPost, path, "owner-token", nil, nil); err != nil {
		t.Fatal(err)
	}
}
