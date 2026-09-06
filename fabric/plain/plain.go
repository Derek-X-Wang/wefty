// Package plain provides a localhost-only Fabric for development and tests.
package plain

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/internal/naming"
)

// Network isolates logical names and injected identities for a set of plain
// Fabric instances. Tests should create a fresh Network to avoid shared state.
type Network struct {
	mu       sync.RWMutex
	names    map[string]*nameRegistration
	peers    map[string]fabric.Identity
	fabricID string
	// listenFn is a test-only seam for controlling localhost bind completion.
	// Access it through listen and setListenFuncForTest so concurrent tests do
	// not race with listener creation.
	listenFn func(network, address string) (net.Listener, error)
}

const (
	identityMagic   = "WEFTYPLAIN1"
	maxIdentitySize = 64 << 10
	// logicalRegistrationWait spends at most 5% of the service-acceptance
	// harness's 5-second Fabric dial budget waiting for an in-process loopback
	// bind, leaving 4.75 seconds for the connection and request itself.
	logicalRegistrationWait = 250 * time.Millisecond
)

type nameRegistration struct {
	ready   chan struct{}
	address string
	err     error
}

// LogicalRegistrationTimeoutError reports that a claimed logical address did
// not finish its localhost bind within the plain Fabric registration ceiling.
type LogicalRegistrationTimeoutError struct {
	Address string
	Timeout time.Duration
}

func (e *LogicalRegistrationTimeoutError) Error() string {
	return fmt.Sprintf("plain fabric: logical address %q registration did not complete within %s", e.Address, e.Timeout)
}

// NewNetwork creates an isolated localhost fabric network.
func NewNetwork() *Network {
	identity := make([]byte, 32)
	if _, err := rand.Read(identity); err != nil {
		panic(fmt.Sprintf("plain fabric: generate network identity: %v", err))
	}
	return &Network{
		names:    make(map[string]*nameRegistration),
		peers:    make(map[string]fabric.Identity),
		fabricID: "plain-" + hex.EncodeToString(identity),
	}
}

// NewNetworkWithID joins separate DEVELOPMENT ONLY plain-Fabric processes to
// one explicit authority. The reserved prefix prevents plain identity from
// being presented as a production Fabric implementation.
func NewNetworkWithID(fabricID string) (*Network, error) {
	if !strings.HasPrefix(fabricID, "plain-") || len(fabricID) <= len("plain-") || len(fabricID) > 255 || strings.TrimSpace(fabricID) != fabricID {
		return nil, errors.New("plain fabric: explicit Fabric ID must use the bounded plain- prefix")
	}
	return &Network{names: make(map[string]*nameRegistration), peers: make(map[string]fabric.Identity), fabricID: fabricID}, nil
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
	var registration *nameRegistration
	if logical {
		registration, err = f.network.beginNameRegistration(name.String())
		if err != nil {
			return nil, err
		}
	}

	ln, err := f.network.listen(network, listenAddress)
	if err != nil {
		if registration != nil {
			f.network.failNameRegistration(name.String(), registration, err)
		}
		return nil, err
	}
	l := &listener{Listener: ln, network: f.network, registration: registration}
	if logical {
		l.name = name.String()
		if err := f.network.completeNameRegistration(l.name, registration, ln.Addr().String()); err != nil {
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
		address, err = f.network.resolveName(ctx, name.String())
	} else {
		address, err = localAddress(address, false)
	}
	if err != nil {
		return nil, err
	}

	conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	if err := writeIdentity(conn, f.identity); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
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
	identity.FabricID = f.network.fabricID
	return identity, nil
}

// PersonIdentityTrust reports that plain identities are peer-supplied test/dev
// data and must never be accepted for person authority without an explicit
// development override at the server.
func (f *Fabric) PersonIdentityTrust() fabric.PersonIdentityTrust {
	return fabric.PersonIdentitySelfAsserted
}

// ConnectHost returns the loopback host backing published listeners in the
// localhost-only Fabric.
func (f *Fabric) ConnectHost() string { return "127.0.0.1" }

func (n *Network) beginNameRegistration(name string) (*nameRegistration, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.names[name] != nil {
		return nil, fmt.Errorf("plain fabric: logical address %q is already listening", name)
	}
	registration := &nameRegistration{ready: make(chan struct{})}
	n.names[name] = registration
	return registration, nil
}

func (n *Network) listen(network, address string) (net.Listener, error) {
	n.mu.RLock()
	listen := n.listenFn
	n.mu.RUnlock()
	if listen == nil {
		listen = net.Listen
	}
	return listen(network, address)
}

func (n *Network) setListenFuncForTest(listen func(network, address string) (net.Listener, error)) {
	n.mu.Lock()
	n.listenFn = listen
	n.mu.Unlock()
}

func (n *Network) completeNameRegistration(name string, registration *nameRegistration, address string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.names[name] != registration {
		return fmt.Errorf("plain fabric: logical address %q registration lost ownership", name)
	}
	registration.address = address
	close(registration.ready)
	return nil
}

func (n *Network) failNameRegistration(name string, registration *nameRegistration, err error) {
	n.mu.Lock()
	if n.names[name] == registration {
		registration.err = err
		close(registration.ready)
		delete(n.names, name)
	}
	n.mu.Unlock()
}

func (n *Network) resolveName(ctx context.Context, name string) (string, error) {
	n.mu.RLock()
	registration := n.names[name]
	if registration == nil {
		n.mu.RUnlock()
		return "", fmt.Errorf("plain fabric: logical address %q is not listening", name)
	}
	if registration.address != "" {
		address := registration.address
		n.mu.RUnlock()
		return address, nil
	}
	n.mu.RUnlock()

	timer := time.NewTimer(logicalRegistrationWait)
	defer timer.Stop()
	var waitErr error
	select {
	case <-registration.ready:
	case <-ctx.Done():
		waitErr = fmt.Errorf("plain fabric: logical address %q registration wait: %w", name, ctx.Err())
	case <-timer.C:
		waitErr = &LogicalRegistrationTimeoutError{Address: name, Timeout: logicalRegistrationWait}
	}

	n.mu.RLock()
	address, registrationErr := registration.address, registration.err
	n.mu.RUnlock()
	// Publication is authoritative even if cancellation or the ceiling became
	// selectable in the same scheduler turn as ready.
	if address != "" {
		return address, nil
	}
	if registrationErr != nil {
		return "", fmt.Errorf("plain fabric: logical address %q failed to listen: %w", name, registrationErr)
	}
	if waitErr != nil {
		return "", waitErr
	}
	return "", fmt.Errorf("plain fabric: logical address %q is not listening", name)
}

func (n *Network) registerPeer(address string, identity fabric.Identity) error {
	identity.Tags = append([]string(nil), identity.Tags...)
	identity.FabricID = n.fabricID
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, exists := n.peers[address]; exists {
		return fmt.Errorf("plain fabric: identity for %q is already registered", address)
	}
	n.peers[address] = identity
	return nil
}

func (n *Network) unregisterPeer(address string) {
	n.mu.Lock()
	delete(n.peers, address)
	n.mu.Unlock()
}

func (n *Network) unregisterName(name string, registration *nameRegistration) {
	n.mu.Lock()
	if n.names[name] == registration {
		delete(n.names, name)
	}
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
	network      *Network
	name         string
	registration *nameRegistration
	once         sync.Once
}

type peerAddress struct {
	net.Addr
	token string
}

func (a peerAddress) String() string { return a.Addr.String() + "#" + a.token }

func (l *listener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		identity, err := readIdentity(conn)
		if err != nil {
			_ = conn.Close()
			continue
		}
		token, err := newPeerToken()
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		address := peerAddress{Addr: conn.RemoteAddr(), token: token}
		key := address.String()
		if err := l.network.registerPeer(key, identity); err != nil {
			_ = conn.Close()
			continue
		}
		return &connWithIdentity{Conn: conn, network: l.network, key: key, remoteAddress: address}, nil
	}
}

func (l *listener) Close() error {
	l.once.Do(func() {
		if l.name != "" {
			l.network.unregisterName(l.name, l.registration)
		}
	})
	return l.Listener.Close()
}

type connWithIdentity struct {
	net.Conn
	network       *Network
	key           string
	remoteAddress net.Addr
	once          sync.Once
}

func (c *connWithIdentity) RemoteAddr() net.Addr { return c.remoteAddress }

func (c *connWithIdentity) Close() error {
	c.once.Do(func() { c.network.unregisterPeer(c.key) })
	return c.Conn.Close()
}

func (c *connWithIdentity) CloseWrite() error {
	if connection, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return connection.CloseWrite()
	}
	return fmt.Errorf("plain fabric: connection %T does not support CloseWrite", c.Conn)
}

func newPeerToken() (string, error) {
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return "", fmt.Errorf("plain fabric: generate peer token: %w", err)
	}
	return hex.EncodeToString(token), nil
}

func writeIdentity(writer io.Writer, identity fabric.Identity) error {
	payload, err := json.Marshal(identity)
	if err != nil {
		return fmt.Errorf("plain fabric: encode injected identity: %w", err)
	}
	if len(payload) > maxIdentitySize {
		return fmt.Errorf("plain fabric: injected identity is too large")
	}
	header := make([]byte, len(identityMagic)+4)
	copy(header, identityMagic)
	binary.BigEndian.PutUint32(header[len(identityMagic):], uint32(len(payload)))
	if _, err := writer.Write(header); err != nil {
		return fmt.Errorf("plain fabric: write identity header: %w", err)
	}
	if _, err := writer.Write(payload); err != nil {
		return fmt.Errorf("plain fabric: write identity: %w", err)
	}
	return nil
}

func readIdentity(reader io.Reader) (fabric.Identity, error) {
	header := make([]byte, len(identityMagic)+4)
	if _, err := io.ReadFull(reader, header); err != nil {
		return fabric.Identity{}, fmt.Errorf("plain fabric: read identity header: %w", err)
	}
	if string(header[:len(identityMagic)]) != identityMagic {
		return fabric.Identity{}, fmt.Errorf("plain fabric: invalid identity header")
	}
	size := binary.BigEndian.Uint32(header[len(identityMagic):])
	if size > maxIdentitySize {
		return fabric.Identity{}, fmt.Errorf("plain fabric: injected identity is too large")
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return fabric.Identity{}, fmt.Errorf("plain fabric: read identity: %w", err)
	}
	var identity fabric.Identity
	if err := json.Unmarshal(payload, &identity); err != nil {
		return fabric.Identity{}, fmt.Errorf("plain fabric: decode injected identity: %w", err)
	}
	return identity, nil
}

var _ fabric.Fabric = (*Fabric)(nil)
