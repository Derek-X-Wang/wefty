package l1_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

type peerFixture struct {
	network            *plain.Network
	client             fabric.Fabric
	l1f, l3f           fabric.Fabric
	l1server, l3server http.Handler
	l1close, l3close   func()
	dir                string
}

func newPeerFixture(t *testing.T, ledgerID, controlID string) *peerFixture {
	t.Helper()
	n := plain.NewNetwork()
	dir := t.TempDir()
	f := &peerFixture{network: n, dir: dir, client: n.NewFabric(fabric.Identity{NodeID: "client", Tags: []string{l1.DefaultClientPrincipalTag}}), l1f: n.NewFabric(fabric.Identity{NodeID: controlID}), l3f: n.NewFabric(fabric.Identity{NodeID: ledgerID, Tags: []string{l1.DefaultClientPrincipalTag}})}
	s1, err := l1.OpenStore(filepath.Join(dir, "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s1.Close() })
	s3, err := l3.OpenStore(filepath.Join(dir, "l3.sqlite"), l3.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s3.Close() })
	a, err := l1.NewServer(f.l1f, s1, l1.ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := l3.NewServer(f.l3f, s3, l3.ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	f.l1server, f.l3server = a.Handler(), b.Handler()
	f.l1close = servePeer(t, f.l1f, "wefty://control-plane", f.l1server)
	f.l3close = servePeer(t, f.l3f, "wefty://run-ledger", f.l3server)
	return f
}

func servePeer(t *testing.T, f fabric.Fabric, address string, h http.Handler) func() {
	t.Helper()
	ln, err := f.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: h}
	done := make(chan error, 1)
	go func() { done <- server.Serve(ln) }()
	close := func() { _ = server.Close(); <-done }
	var closed bool
	once := func() {
		if !closed {
			closed = true
			close()
		}
	}
	t.Cleanup(once)
	return once
}

func peerRequest(t *testing.T, f fabric.Fabric, address, path string) (int, []byte) {
	t.Helper()
	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) { return f.Dial(ctx, network, address) }}
	defer transport.CloseIdleConnections()
	c := &http.Client{Transport: transport, Timeout: 4 * time.Second}
	res, err := c.Get("http://peer.invalid" + path)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res.StatusCode, body
}

func TestPeerHealthBothDirections(t *testing.T) {
	f := newPeerFixture(t, "run-ledger", "control-plane")
	for _, address := range []string{"wefty://control-plane", "wefty://run-ledger"} {
		code, body := peerRequest(t, f.client, address, "/v1/peer-health")
		var result struct {
			Reachable  bool      `json:"reachable"`
			ErrorClass string    `json:"error_class"`
			CheckedAt  time.Time `json:"checked_at"`
		}
		if code != 200 || json.Unmarshal(body, &result) != nil || !result.Reachable || result.ErrorClass != "" || result.CheckedAt.IsZero() {
			t.Fatalf("%s: status=%d body=%s", address, code, body)
		}
	}
}

func peerDBBytes(t *testing.T, dir string) map[string]string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.sqlite*"))
	if err != nil {
		t.Fatal(err)
	}
	result := map[string]string{}
	for _, path := range files {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		result[filepath.Base(path)] = fmt.Sprintf("%x", sha256.Sum256(b))
	}
	return result
}

func TestPeerHealthFailuresAndReadOnly(t *testing.T) {
	for _, direction := range []string{"L1", "L3"} {
		for _, mode := range []string{"healthy", "down", "wrong peer"} {
			t.Run(direction+"/"+mode, func(t *testing.T) {
				ledgerID, controlID := "run-ledger", "control-plane"
				if mode == "wrong peer" {
					if direction == "L1" {
						ledgerID = "other-ledger"
					} else {
						controlID = "other-control"
					}
				}
				f := newPeerFixture(t, ledgerID, controlID)
				address := "wefty://control-plane"
				if direction == "L3" {
					address = "wefty://run-ledger"
				}
				if mode == "down" {
					if direction == "L1" {
						f.l3close()
					} else {
						f.l1close()
					}
				}
				before := peerDBBytes(t, f.dir)
				started := time.Now()
				code, body := peerRequest(t, f.client, address, "/v1/peer-health")
				var result contract.PeerHealth
				if code != 200 || json.Unmarshal(body, &result) != nil {
					t.Fatalf("status=%d body=%s", code, body)
				}
				if result.Reachable != (mode == "healthy") || result.CheckedAt.IsZero() {
					t.Fatalf("mode=%s result=%+v", mode, result)
				}
				expected := ""
				if mode == "down" {
					expected = "unavailable"
				}
				if mode == "wrong peer" {
					expected = "forbidden"
				}
				if result.ErrorClass != expected {
					t.Fatalf("class=%q, want %q", result.ErrorClass, expected)
				}
				if time.Since(started) > 3*time.Second {
					t.Fatal("probe exceeded its ceiling")
				}
				var fields map[string]any
				if json.Unmarshal(body, &fields) != nil || len(fields) != 3 {
					t.Fatalf("unexpected public fields: %s", body)
				}
				if !reflect.DeepEqual(before, peerDBBytes(t, f.dir)) {
					t.Fatal("probe changed database bytes or sidecars")
				}
			})
		}
	}
}

func TestPeerHealthAndPingPrincipalsDoNotWrite(t *testing.T) {
	f := newPeerFixture(t, "run-ledger", "control-plane")
	before := peerDBBytes(t, f.dir)
	for _, target := range []string{"wefty://control-plane", "wefty://run-ledger"} {
		for _, identity := range []fabric.Identity{{NodeID: "person", UserID: "person", DeviceID: "device"}, {NodeID: "agent", Tags: []string{l1.DefaultAgentPrincipalTag}}} {
			participant := f.network.NewFabric(identity)
			for _, path := range []string{"/v1/peer-health", "/v1/peer/ping"} {
				code, body := peerRequest(t, participant, target, path)
				if code != 403 {
					t.Fatalf("%s %s: %d %s", target, path, code, body)
				}
			}
		}
		code, body := peerRequest(t, f.client, target, "/v1/peer/ping")
		if code != 403 {
			t.Fatalf("client ping: %d %s", code, body)
		}
	}
	for _, route := range []struct {
		f       fabric.Fabric
		address string
	}{{f.l1f, "wefty://run-ledger"}, {f.l3f, "wefty://control-plane"}} {
		code, body := peerRequest(t, route.f, route.address, "/v1/peer/ping")
		if code != 200 || string(body) != "{\"ok\":true}\n" {
			t.Fatalf("peer ping: %d %s", code, body)
		}
	}
	if !reflect.DeepEqual(before, peerDBBytes(t, f.dir)) {
		t.Fatal("principal checks or pings changed database bytes")
	}
}

func TestPeerHealthConcurrentRequestsDoNotWrite(t *testing.T) {
	f := newPeerFixture(t, "run-ledger", "control-plane")
	f.l1close()
	f.l3close()
	const count = 25
	var requests, pings atomic.Int32
	allRequests, pingEntered, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	servePeer(t, f.l1f, "wefty://control-plane", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peer-health" && requests.Add(1) == count {
			close(allRequests)
		}
		f.l1server.ServeHTTP(w, r)
	}))
	servePeer(t, f.l3f, "wefty://run-ledger", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/peer/ping" {
			if pings.Add(1) == 1 {
				close(pingEntered)
			}
			<-release
		}
		f.l3server.ServeHTTP(w, r)
	}))
	before := peerDBBytes(t, f.dir)
	var wg sync.WaitGroup
	call := func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			code, body := peerRequest(t, f.client, "wefty://control-plane", "/v1/peer-health")
			var result contract.PeerHealth
			if code != 200 || json.Unmarshal(body, &result) != nil || !result.Reachable {
				t.Errorf("concurrent observation: %d %s", code, body)
			}
		}()
	}
	call()
	select {
	case <-pingEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("peer ping not reached")
	}
	for range count - 1 {
		call()
	}
	select {
	case <-allRequests:
	case <-time.After(3 * time.Second):
		t.Fatal("concurrent requests did not reach observer")
	}
	if pings.Load() != 1 {
		t.Errorf("concurrent callers started %d outbound pings", pings.Load())
	}
	unblock()
	wg.Wait()
	if !reflect.DeepEqual(before, peerDBBytes(t, f.dir)) {
		t.Fatal("concurrent probes changed database bytes or sidecars")
	}
}
