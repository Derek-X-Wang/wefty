// Package tsnet provides the production Fabric backed by an embedded tailnet
// node. Implementation-specific identities and network names are translated
// before they cross the fabric seam.
package tsnet

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/internal/naming"
	"github.com/Derek-X-Wang/wefty/fabric/participantstate"
	"tailscale.com/client/local"
	"tailscale.com/client/tailscale/apitype"
	"tailscale.com/envknob"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/net/tsaddr"
	"tailscale.com/net/tsdial"
	tailscaletsnet "tailscale.com/tsnet"
)

// Config contains the primitive configuration needed by an embedded fabric
// node. Name is the node's wefty logical name, such as
// wefty://control-plane or wefty://node/runner-1.
type Config struct {
	Name string
	// NetworkName reuses a recorded grant. Empty preserves the legacy mapping.
	NetworkName      string
	StateDir         string
	Credential       fabric.Credential
	Ephemeral        bool
	CoordinatorURL   string
	Logf             func(format string, args ...any)
	ExpectedIdentity *fabric.Identity
}

// Fabric is an embedded production fabric node.
type Fabric struct {
	releaseState   func() error
	closeOnce      sync.Once
	closeErr       error
	server         *tailscaletsnet.Server
	localName      naming.Name
	coordinatorURL string

	clientMu            sync.Mutex
	client              *local.Client
	fabricID            string
	expectedIdentity    *fabric.Identity
	recordedNetworkName string
	// Transport calls stay injectable for secretless seam tests.
	dialNode       func(context.Context, string, string) (net.Conn, error)
	dialPlan       func(context.Context, string, string) (netip.AddrPort, bool, error)
	outboundDialer *tsdial.Dialer
	listenNode     func(string, string) (net.Listener, error)
}

// New locks an explicit state folder and creates a tsnet-backed Fabric
// without starting it. The first Listen, Dial, or WhoIs call starts the
// embedded node. Call Close even when no transport operation was made.
func New(config Config) (*Fabric, error) {
	// The embedding process owns enrolment. Never let the dependency consume
	// an ambient join credential, or pass one on to later child processes.
	if err := ClearAmbientCredentials(); err != nil {
		return nil, err
	}
	name, logical, err := naming.Parse(config.Name)
	if err != nil {
		return nil, err
	}
	if !logical {
		return nil, fmt.Errorf("tsnet fabric: Name %q is not a wefty logical name", config.Name)
	}
	networkName := config.NetworkName
	if networkName == "" {
		networkName = name.Hostname()
	}
	if err := fabric.ValidateNetworkName(networkName); err != nil {
		return nil, err
	}
	if config.ExpectedIdentity != nil {
		if config.Credential.Value != "" || config.Ephemeral || config.ExpectedIdentity.NodeID == "" {
			return nil, fmt.Errorf("saved participant requires a recorded node ID and no enrolment credential or ephemeral lifetime")
		}
		// The dependency can initiate a login flow while starting a revoked node.
		// Never expose that flow, even when legacy enrollment URL output is opted in.
		// VerifyParticipant and every transport operation refuse unusable state.
	}
	// Wefty owns its logs: the embedded node must never upload its log to
	// the pinned tsnet dependency's log service. This must run before the
	// tailscaletsnet.Server below is constructed, and in particular before
	// its first Start (triggered lazily by Listen, Dial, or WhoIs): that is
	// when tsnet.Server.startLogger (tsnet/tsnet.go:1053) builds the log
	// upload transport via logpolicy.NewLogtailTransport ->
	// TransportOptions.New and wires it into s.logtail through
	// logtail.Config.HTTPC (tsnet/tsnet.go:1085,1088). TransportOptions.New
	// checks envknob.NoLogsNoSupport() at call time and substitutes a no-op
	// transport when it is set (tailscale.com@v1.102.3
	// logpolicy/logpolicy.go:891-894). envknob.SetNoLogsNoSupport()
	// (envknob/envknob.go:477-479) sets TS_NO_LOGS_NO_SUPPORT=true through
	// the dependency's own Setenv, and envknob.NoLogsNoSupport() re-reads
	// os.Getenv on every call rather than a once-cached value
	// (envknob/envknob.go:270,280-293, Bool -> boolOr), so there is no
	// earlier-cached read to race against as long as this runs before
	// Start. Calling the dependency's own setter (rather than os.Setenv
	// with the variable's name) keeps that name inside the fabric seam.
	var release func() error
	if config.StateDir != "" {
		release, err = participantstate.Acquire(config.StateDir, config.Name)
		if err != nil {
			return nil, err
		}
	}
	// Recheck saved state after taking ownership. The process-level profile
	// check alone cannot fence deletion between that check and backend start.
	if config.ExpectedIdentity != nil && !participantstate.HasSavedState(config.StateDir) {
		if release != nil {
			_ = release()
		}
		return nil, &fabric.ParticipantIdentityError{Person: config.ExpectedIdentity.UserID != "", Reason: "state is missing; restore this participant's original saved state"}
	}
	envknob.SetNoLogsNoSupport()
	return &Fabric{
		releaseState:        release,
		localName:           name,
		expectedIdentity:    config.ExpectedIdentity,
		recordedNetworkName: config.NetworkName,
		coordinatorURL:      config.CoordinatorURL,
		server: &tailscaletsnet.Server{
			Dir:        config.StateDir,
			Hostname:   networkName,
			AuthKey:    config.Credential.Value,
			Ephemeral:  config.Ephemeral,
			ControlURL: config.CoordinatorURL,
			// Both loggers are always wefty-owned functions, never the
			// caller's config.Logf (or nil) directly: the pinned tsnet
			// dependency defaults an unset UserLogf to raw stderr, which
			// can print an enrollment URL during first login (wefty
			// #498). wrapUserLogf and wrapBackendLogf redact and gate that
			// the same way; wrapBackendLogf additionally discards backend
			// output entirely when config.Logf is nil, since there is no
			// caller-supplied sink to forward it to.
			Logf:     participantLogSink(wrapBackendLogf(config.Logf), config.ExpectedIdentity != nil),
			UserLogf: participantLogSink(wrapUserLogf(config.Logf), config.ExpectedIdentity != nil),
		},
	}, nil
}

// Listen listens on the tailnet. Logical listeners must match the local name;
// their private transport address is chosen inside this package.
func (f *Fabric) Listen(network, address string) (net.Listener, error) {
	ctx, cancel := context.WithTimeout(context.Background(), participantStartTimeout)
	defer cancel()
	if err := f.checkParticipant(ctx); err != nil {
		return nil, err
	}
	name, logical, err := naming.Parse(address)
	if err != nil {
		return nil, err
	}
	if logical {
		if name.String() != f.localName.String() {
			return nil, fmt.Errorf("tsnet fabric: cannot listen as %q from %q", name.String(), f.localName.String())
		}
		_, port, err := net.SplitHostPort(name.TransportAddress())
		if err != nil {
			return nil, err
		}
		address = net.JoinHostPort("", port)
	} else if _, _, err := net.SplitHostPort(address); err != nil {
		if _, err := f.opaqueAddress(ctx, address, true); err != nil {
			return nil, err
		}
		address = ":80"
	}
	if f.listenNode != nil {
		return f.listenNode(network, address)
	}
	return f.server.Listen(network, address)
}

// Dial connects over the tailnet, resolving wefty logical names internally.
func (f *Fabric) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	if err := f.checkParticipant(ctx); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	name, logical, err := naming.Parse(address)
	if err != nil {
		return nil, err
	}
	status, err := f.runningStatus(ctx)
	if errors.Is(ctx.Err(), context.Canceled) {
		return nil, ctx.Err()
	}
	if err != nil {
		var identity *fabric.ParticipantIdentityError
		if errors.As(err, &identity) {
			return nil, err
		}
		return nil, &fabric.ParticipantUnreachableError{Cause: participantStatusError{cause: err}}
	}
	if logical {
		if f.expectedIdentity != nil {
			return nil, &fabric.DestinationRefusedError{}
		}
		address = name.TransportAddress()
	} else if _, _, err := net.SplitHostPort(address); err != nil {
		address, err = opaquePeerAddress(status, address, false)
		if err != nil {
			return nil, err
		}
	}
	address, err = authenticatedAddress(status, network, address)
	if err != nil {
		return nil, err
	}
	target, err := netip.ParseAddrPort(address)
	if err != nil || !authenticatedPeerTarget(status, target) {
		return nil, &fabric.DestinationRefusedError{}
	}
	dialer := f.outboundDialer
	plan := f.dialPlan
	if plan == nil {
		if dialer == nil {
			dialer = f.server.Sys().Dialer.Get()
		}
		plan = dialer.UserDialPlan
	}
	resolved, viaFabric, err := plan(ctx, network, address)
	if err != nil {
		return nil, err
	}
	if !viaFabric || !resolved.IsValid() || !authenticatedPeerTarget(status, resolved) {
		return nil, &fabric.DestinationRefusedError{}
	}
	// Dial the plan's literal address. Passing a hostname again would permit a
	// second DNS result to escape the checked route through the host network.
	address = resolved.String()
	if f.dialNode != nil {
		return f.dialNode(ctx, network, address)
	}
	if dialer == nil {
		dialer = f.server.Sys().Dialer.Get()
	}
	// UserDial reclassifies even a literal IP and can fall back to the host if
	// routes change after the plan. Use only the embedded peer transport.
	switch network {
	case "tcp", "tcp4", "tcp6":
		if dialer.NetstackDialTCP != nil {
			return dialer.NetstackDialTCP(ctx, resolved)
		}
	case "udp", "udp4", "udp6":
		if dialer.NetstackDialUDP != nil {
			return dialer.NetstackDialUDP(ctx, resolved)
		}
	default:
		return nil, errors.New("unsupported Fabric dial network")
	}
	return nil, &fabric.ParticipantUnreachableError{Cause: errors.New("Fabric peer transport is unavailable")}
}

// Opaque node IDs are resolved only from authenticated network state. They
// are never treated as DNS names. A listener must name this very participant.
func (f *Fabric) opaqueAddress(ctx context.Context, nodeID string, localOnly bool) (string, error) {
	status, err := f.runningStatus(ctx)
	if err != nil {
		return "", err
	}
	return opaquePeerAddress(status, nodeID, localOnly)
}

func opaquePeerAddress(status *ipnstate.Status, nodeID string, localOnly bool) (string, error) {
	var peer *ipnstate.PeerStatus
	if status.Self != nil && string(status.Self.ID) == nodeID {
		if !localOnly {
			return "", &fabric.DestinationRefusedError{}
		}
		peer = status.Self
	}
	if !localOnly && peer == nil {
		for _, candidate := range status.Peer {
			if candidate != nil && string(candidate.ID) == nodeID {
				peer = candidate
				break
			}
		}
	}
	if peer == nil {
		return "", fabric.ErrIdentityNotFound
	}
	if localOnly {
		return ":80", nil
	}
	if len(peer.TailscaleIPs) == 0 {
		return "", fabric.ErrIdentityIncomplete
	}
	return net.JoinHostPort(peer.TailscaleIPs[0].String(), "80"), nil
}

// Resolve only names assigned by authenticated network state. In particular,
// never pass a name to UserDialPlan: an unknown name can reach the host resolver.
func authenticatedAddress(status *ipnstate.Status, network, address string) (string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", &fabric.DestinationRefusedError{}
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return address, nil
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	suffix := ""
	if status.CurrentTailnet != nil {
		suffix = strings.ToLower(strings.TrimSuffix(status.CurrentTailnet.MagicDNSSuffix, "."))
	}
	peers := []*ipnstate.PeerStatus{status.Self}
	for _, peer := range status.Peer {
		peers = append(peers, peer)
	}
	var matched *ipnstate.PeerStatus
	for _, peer := range peers {
		if peer == nil || peer.DNSName == "" {
			continue
		}
		fullName := strings.ToLower(strings.TrimSuffix(peer.DNSName, "."))
		shortName := fullName
		if suffix != "" {
			shortName = strings.TrimSuffix(fullName, "."+suffix)
		}
		if host != fullName && host != shortName {
			continue
		}
		if peer == status.Self || matched != nil {
			return "", &fabric.DestinationRefusedError{}
		}
		matched = peer
	}
	if matched != nil {
		for _, ip := range matched.TailscaleIPs {
			if !ip.IsValid() || strings.HasSuffix(network, "4") && !ip.Is4() || strings.HasSuffix(network, "6") && !ip.Is6() {
				continue
			}
			return net.JoinHostPort(ip.String(), port), nil
		}
	}
	return "", &fabric.DestinationRefusedError{}
}

// Routes also include local network services and subnet routes. Neither is
// authority to dial: only a remote peer address outside its backend services is.
func authenticatedPeerTarget(status *ipnstate.Status, target netip.AddrPort) bool {
	ip := target.Addr()
	if !target.IsValid() || ip == tsaddr.TailscaleServiceIP() || ip == tsaddr.TailscaleServiceIPv6() {
		return false
	}
	hasIP := func(peer *ipnstate.PeerStatus) bool {
		if peer == nil {
			return false
		}
		for _, assigned := range peer.TailscaleIPs {
			if assigned == ip {
				return true
			}
		}
		return false
	}
	if hasIP(status.Self) {
		return false
	}
	peers := []*ipnstate.PeerStatus{status.Self}
	for _, peer := range status.Peer {
		peers = append(peers, peer)
	}
	for _, peer := range peers {
		if peer == nil {
			continue
		}
		for _, raw := range peer.PeerAPIURL {
			endpoint, err := url.Parse(raw)
			if err != nil {
				continue
			}
			backend, err := netip.ParseAddrPort(endpoint.Host)
			if err == nil && backend == target {
				return false
			}
		}
	}
	for _, peer := range status.Peer {
		if hasIP(peer) {
			return true
		}
	}
	return false
}

// WhoIs translates the embedded network's authenticated peer record into a
// wefty-owned Identity.
func (f *Fabric) WhoIs(ctx context.Context, remoteAddress string) (fabric.Identity, error) {
	if err := f.checkParticipant(ctx); err != nil {
		return fabric.Identity{}, err
	}
	client, err := f.localClient()
	if err != nil {
		return fabric.Identity{}, err
	}
	who, err := client.WhoIs(ctx, remoteAddress)
	if err != nil {
		if errors.Is(err, local.ErrPeerNotFound) {
			return fabric.Identity{}, fmt.Errorf("tsnet fabric: identity for %q: %w", remoteAddress, fabric.ErrIdentityNotFound)
		}
		return fabric.Identity{}, err
	}
	fabricID, err := f.issuingFabricID(ctx, client)
	if err != nil {
		return fabric.Identity{}, err
	}
	return identityFromWhoIs(who, fabricID)
}

// PersonIdentityTrust reports that WhoIs authenticates person identity.
func (f *Fabric) PersonIdentityTrust() fabric.PersonIdentityTrust {
	return fabric.PersonIdentityAuthenticated
}

// ConnectHost returns the recorded network name requested by this backend,
// or the legacy mapping when no name was recorded. It performs no network IO.
func (f *Fabric) ConnectHost() string { return f.server.Hostname }

func (f *Fabric) localClient() (*local.Client, error) {
	f.clientMu.Lock()
	defer f.clientMu.Unlock()
	if f.client != nil {
		return f.client, nil
	}
	client, err := f.server.LocalClient()
	if err != nil {
		return nil, err
	}
	f.client = client
	return client, nil
}

func (f *Fabric) issuingFabricID(ctx context.Context, client *local.Client) (string, error) {
	f.clientMu.Lock()
	defer f.clientMu.Unlock()
	if f.fabricID != "" {
		return f.fabricID, nil
	}
	status, err := client.Status(ctx)
	if err != nil {
		return "", fmt.Errorf("tsnet fabric: read issuing Fabric identity: %w", err)
	}
	if status.CurrentTailnet == nil || status.CurrentTailnet.MagicDNSSuffix == "" {
		return "", fabric.ErrIssuingFabricUnavailable
	}
	f.fabricID = coordinatorFabricID(f.coordinatorURL, status.CurrentTailnet.MagicDNSSuffix)
	return f.fabricID, nil
}

func identityFromWhoIs(who *apitype.WhoIsResponse, fabricID string) (fabric.Identity, error) {
	if who == nil || who.Node == nil || who.Node.StableID == "" {
		return fabric.Identity{}, fabric.ErrIdentityIncomplete
	}
	identity := fabric.Identity{
		NodeID:   string(who.Node.StableID),
		DeviceID: string(who.Node.StableID),
		FabricID: fabricID,
		Tags:     append([]string(nil), who.Node.Tags...),
	}
	if who.Node.IsTagged() {
		identity.Kind = fabric.IdentityKindMachine
		return identity, nil
	}
	if who.UserProfile == nil || who.UserProfile.ID == 0 {
		return fabric.Identity{}, fabric.ErrIdentityIncomplete
	}
	identity.UserID = strconv.FormatInt(int64(who.UserProfile.ID), 10)
	identity.DisplayName = who.UserProfile.DisplayName
	return identity, nil
}

func coordinatorFabricID(coordinatorURL, identityDomain string) string {
	if coordinatorURL == "" {
		coordinatorURL = "wefty-default-coordinator"
	}
	digest := sha256.Sum256([]byte(coordinatorURL + "\x00" + identityDomain))
	return fmt.Sprintf("fabric-%x", digest[:])
}

// Close shuts down the embedded node and its listeners.
func (f *Fabric) Close() error {
	f.closeOnce.Do(func() {
		f.closeErr = f.server.Close()
		if f.releaseState != nil {
			f.closeErr = errors.Join(f.closeErr, f.releaseState())
		}
	})
	return f.closeErr
}

var _ fabric.Fabric = (*Fabric)(nil)
