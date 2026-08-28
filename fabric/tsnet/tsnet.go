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
	"strconv"
	"sync"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/internal/naming"
	"tailscale.com/client/local"
	"tailscale.com/client/tailscale/apitype"
	tailscaletsnet "tailscale.com/tsnet"
)

// Config contains the primitive configuration needed by an embedded fabric
// node. Name is the node's wefty logical name, such as
// wefty://control-plane or wefty://node/runner-1.
type Config struct {
	Name           string
	StateDir       string
	Credential     fabric.Credential
	Ephemeral      bool
	CoordinatorURL string
	Logf           func(format string, args ...any)
}

// Fabric is an embedded production fabric node.
type Fabric struct {
	server         *tailscaletsnet.Server
	localName      naming.Name
	coordinatorURL string

	clientMu sync.Mutex
	client   *local.Client
	fabricID string
}

// New creates a tsnet-backed Fabric without starting it. The first Listen,
// Dial, or WhoIs call starts the embedded node.
func New(config Config) (*Fabric, error) {
	name, logical, err := naming.Parse(config.Name)
	if err != nil {
		return nil, err
	}
	if !logical {
		return nil, fmt.Errorf("tsnet fabric: Name %q is not a wefty logical name", config.Name)
	}
	return &Fabric{
		localName:      name,
		coordinatorURL: config.CoordinatorURL,
		server: &tailscaletsnet.Server{
			Dir:        config.StateDir,
			Hostname:   name.Hostname(),
			AuthKey:    config.Credential.Value,
			Ephemeral:  config.Ephemeral,
			ControlURL: config.CoordinatorURL,
			Logf:       config.Logf,
		},
	}, nil
}

// Listen listens on the tailnet. Logical listeners must match the local name;
// their private transport address is chosen inside this package.
func (f *Fabric) Listen(network, address string) (net.Listener, error) {
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
	}
	return f.server.Listen(network, address)
}

// Dial connects over the tailnet, resolving wefty logical names internally.
func (f *Fabric) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	name, logical, err := naming.Parse(address)
	if err != nil {
		return nil, err
	}
	if logical {
		address = name.TransportAddress()
	}
	return f.server.Dial(ctx, network, address)
}

// WhoIs translates the embedded network's authenticated peer record into a
// wefty-owned Identity.
func (f *Fabric) WhoIs(ctx context.Context, remoteAddress string) (fabric.Identity, error) {
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

// ConnectHost returns the private tailnet hostname owned by this Fabric
// implementation. The logical wefty name remains behind the seam.
func (f *Fabric) ConnectHost() string { return f.localName.Hostname() }

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
		return "", errors.New("tsnet fabric: issuing Fabric identity is unavailable")
	}
	f.fabricID = coordinatorFabricID(f.coordinatorURL, status.CurrentTailnet.MagicDNSSuffix)
	return f.fabricID, nil
}

func identityFromWhoIs(who *apitype.WhoIsResponse, fabricID string) (fabric.Identity, error) {
	if who == nil || who.Node == nil || who.Node.StableID == "" {
		return fabric.Identity{}, errors.New("tsnet fabric: WhoIs returned an incomplete identity")
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
		return fabric.Identity{}, errors.New("tsnet fabric: WhoIs returned an incomplete identity")
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
	return f.server.Close()
}

var _ fabric.Fabric = (*Fabric)(nil)
