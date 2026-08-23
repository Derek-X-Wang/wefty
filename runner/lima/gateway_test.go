package lima

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

type gatewayEpoch struct{ generation uint64 }

func (epoch *gatewayEpoch) Generation() (ocihelper.HelperSession, bool) {
	return ocihelper.HelperSession{HelperInstanceID: "helper", SessionGeneration: epoch.generation}, true
}

func TestBridgeBinderUsesDiscoveredGatewayWithoutHardCoding(t *testing.T) {
	var command []string
	var listenAddress string
	binder := NewBridgeBinder("ticket-145")
	binder.run = func(_ context.Context, name string, arguments ...string) ([]byte, error) {
		command = append([]string{name}, arguments...)
		return []byte("198.18.0.2 STREAM host.lima.internal\n"), nil
	}
	binder.route = func(context.Context, string, ...string) ([]byte, error) { return []byte("interface: bridge100\n"), nil }
	binder.listen = func(network, address string) (net.Listener, error) {
		listenAddress = address
		return net.Listen("tcp4", "127.0.0.1:0")
	}
	binding, err := binder.Bind(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer binding.Listener.Close()
	if binding.AdvertiseHost != HostGatewayName || binding.HostBridgeFallback {
		t.Fatalf("primary binding = %#v", binding)
	}
	if listenAddress != "198.18.0.2:0" {
		t.Fatalf("listen address = %q", listenAddress)
	}
	joined := strings.Join(command, " ")
	forbiddenGateway := strings.Join([]string{"192", "168", "5", "2"}, ".")
	if !strings.Contains(joined, "getent ahostsv4 "+HostGatewayName) || strings.Contains(joined, forbiddenGateway) {
		t.Fatalf("discovery command = %q", joined)
	}
}

func TestBridgeBinderUsesHelperFallbackOnlyAfterGatewayBindFailure(t *testing.T) {
	binder := NewBridgeBinder("ticket-145")
	binder.run = func(context.Context, string, ...string) ([]byte, error) {
		return []byte("198.18.0.2 STREAM host.lima.internal\n"), nil
	}
	binder.route = func(context.Context, string, ...string) ([]byte, error) { return []byte("interface: bridge100\n"), nil }
	calls := 0
	binder.listen = func(network, address string) (net.Listener, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("gateway surface cannot be bound")
		}
		if address != "127.0.0.1:0" {
			t.Fatalf("fallback address = %q", address)
		}
		return net.Listen("tcp4", address)
	}
	binding, err := binder.Bind(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer binding.Listener.Close()
	if !binding.HostBridgeFallback || binding.AdvertiseHost != "127.0.0.1" || calls != 2 {
		t.Fatalf("fallback binding = %#v, calls=%d", binding, calls)
	}
}

func TestBridgeBinderRejectsPhysicalGatewayRoute(t *testing.T) {
	binder := NewBridgeBinder("ticket-145")
	binder.run = func(context.Context, string, ...string) ([]byte, error) {
		return []byte("10.0.0.24 STREAM host.lima.internal\n"), nil
	}
	binder.route = func(context.Context, string, ...string) ([]byte, error) { return []byte("interface: en0\n"), nil }
	binder.listen = func(string, string) (net.Listener, error) {
		t.Fatal("physical gateway reached bind")
		return nil, nil
	}
	if _, err := binder.Bind(t.Context()); err == nil || !strings.Contains(err.Error(), "physical interface") {
		t.Fatalf("physical route error = %v", err)
	}
}

func TestBridgeBinderDoesNotTunnelAroundDiscoveryFailure(t *testing.T) {
	binder := NewBridgeBinder("ticket-145")
	binder.run = func(context.Context, string, ...string) ([]byte, error) {
		return nil, errors.New("VM unavailable")
	}
	binder.listen = func(string, string) (net.Listener, error) {
		t.Fatal("listener called after discovery failure")
		return nil, nil
	}
	if _, err := binder.Bind(t.Context()); err == nil {
		t.Fatal("discovery failure incorrectly selected the helper fallback")
	}
}

func TestBridgeBinderCachesDiscoveryOnlyForAuthoritativeHelperEpoch(t *testing.T) {
	epoch := &gatewayEpoch{generation: 1}
	binder := NewBridgeBinder("ticket-145")
	binder.Epoch = epoch
	calls := 0
	binder.run = func(context.Context, string, ...string) ([]byte, error) {
		calls++
		return []byte("198.18.0.2 STREAM host.lima.internal\n"), nil
	}
	binder.route = func(context.Context, string, ...string) ([]byte, error) { return []byte("interface: bridge100\n"), nil }
	binder.listen = func(string, string) (net.Listener, error) { return net.Listen("tcp4", "127.0.0.1:0") }
	for range 2 {
		binding, err := binder.Bind(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		_ = binding.Listener.Close()
	}
	if calls != 1 {
		t.Fatalf("same epoch discovery calls = %d", calls)
	}
	epoch.generation++
	binding, err := binder.Bind(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	_ = binding.Listener.Close()
	if calls != 2 {
		t.Fatalf("new epoch discovery calls = %d", calls)
	}
}
