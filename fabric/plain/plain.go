// Package plain provides a localhost-only Fabric for development and tests.
package plain

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sync"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/internal/naming"
)

// Network isolates logical names and injected identities for a set of plain
// Fabric instances. Tests should create a fresh Network to avoid shared state.
type Network struct {
	dialMu sync.Mutex
	mu     sync.RWMutex
	names  map[string]string
	peers  map[string]fabric.Identity
}

// NewNetwork creates an isolated localhost fabric network.
func NewNetwork() *Network {
	return &Network{
		names: make(map[string]string),
		peers: make(map[string]fabric.Identity),
	}
}

// Fabric is one identity-bearing participant in a plain Network.
type Fabric struct {
	network  *Network
	identity fabric.Identity
}

// NewFabric creates a participant whose identity will be returned by WhoIs on
// peers reached through Dial.
func (n *Network) NewFabric(identity fabric.Identity) *Fabric {
	identity.Tags = append([]string(nil), identity.Tags...)
	return &Fabric{network: n, identity: identity}
}

// Listen listens on localhost. A wefty logical address is registered in this
// Network and backed by an ephemeral loopback port.
func (f *Fabric) Listen(network, address string) (net.Listener, error) {
	name, logical, err := naming.Parse(address)
	if err != nil {
		return nil, err
	}
	listenAddress := address
	if logical {
		listenAddress = "127.0.0.1:0"
	} else if listenAddress, err = localAddress(address, true); err != nil {
		return nil, err
	}

	ln, err := net.Listen(network, listenAddress)
	if err != nil {
		return nil, err
	}
	l := &listener{Listener: ln, network: f.network}
	if logical {
		l.name = name.String()
		if err := f.network.registerName(l.name, ln.Addr().String()); err != nil {
			_ = ln.Close()
			return nil, err
		}
	}
	return l, nil
}

// Dial connects only to localhost or a logical name registered in this
// Network. The caller's injected identity is associated with the connection.
func (f *Fabric) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	name, logical, err := naming.Parse(address)
	if err != nil {
		return nil, err
	}
	if logical {
		address, err = f.network.resolveName(name.String())
	} else {
		address, err = localAddress(address, false)
	}
	if err != nil {
		return nil, err
	}

	// Accept waits on the same mutex. This makes identity registration visible
	// before the server can receive the connection and call WhoIs.
	f.network.dialMu.Lock()
	defer f.network.dialMu.Unlock()
	conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	key := conn.LocalAddr().String()
	f.network.registerPeer(key, f.identity)
	return &connWithIdentity{Conn: conn, network: f.network, key: key}, nil
}

// WhoIs returns the identity injected into the peer's plain Fabric.
func (f *Fabric) WhoIs(_ context.Context, remoteAddress string) (fabric.Identity, error) {
	f.network.mu.RLock()
	identity, ok := f.network.peers[remoteAddress]
	f.network.mu.RUnlock()
	if !ok {
		return fabric.Identity{}, fmt.Errorf("plain fabric: identity for %q: %w", remoteAddress, fabric.ErrIdentityNotFound)
	}
	identity.Tags = append([]string(nil), identity.Tags...)
	return identity, nil
}

func (n *Network) registerName(name, address string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, exists := n.names[name]; exists {
		return fmt.Errorf("plain fabric: logical address %q is already listening", name)
	}
	n.names[name] = address
	return nil
}

func (n *Network) resolveName(name string) (string, error) {
	n.mu.RLock()
	address, ok := n.names[name]
	n.mu.RUnlock()
	if !ok {
		return "", fmt.Errorf("plain fabric: logical address %q is not listening", name)
	}
	return address, nil
}

func (n *Network) registerPeer(address string, identity fabric.Identity) {
	identity.Tags = append([]string(nil), identity.Tags...)
	n.mu.Lock()
	n.peers[address] = identity
	n.mu.Unlock()
}

func (n *Network) unregisterPeer(address string) {
	n.mu.Lock()
	delete(n.peers, address)
	n.mu.Unlock()
}

func (n *Network) unregisterName(name string) {
	n.mu.Lock()
	delete(n.names, name)
	n.mu.Unlock()
}

func localAddress(address string, listen bool) (string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", fmt.Errorf("plain fabric: address %q: %w", address, err)
	}
	if host == "" {
		if !listen {
			return "", fmt.Errorf("plain fabric: dial address %q has no host", address)
		}
		host = "127.0.0.1"
	} else if host == "localhost" {
		host = "127.0.0.1"
	} else if ip, err := netip.ParseAddr(host); err != nil || !ip.IsLoopback() {
		return "", fmt.Errorf("plain fabric: address %q is not localhost", address)
	}
	return net.JoinHostPort(host, port), nil
}

type listener struct {
	net.Listener
	network *Network
	name    string
	once    sync.Once
}

func (l *listener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.network.dialMu.Lock()
	l.network.dialMu.Unlock()
	return conn, nil
}

func (l *listener) Close() error {
	l.once.Do(func() {
		if l.name != "" {
			l.network.unregisterName(l.name)
		}
	})
	return l.Listener.Close()
}

type connWithIdentity struct {
	net.Conn
	network *Network
	key     string
	once    sync.Once
}

func (c *connWithIdentity) Close() error {
	c.once.Do(func() { c.network.unregisterPeer(c.key) })
	return c.Conn.Close()
}

var _ fabric.Fabric = (*Fabric)(nil)
