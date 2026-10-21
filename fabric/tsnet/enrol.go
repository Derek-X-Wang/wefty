package tsnet

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/internal/naming"
	"github.com/Derek-X-Wang/wefty/fabric/participantstate"
	"tailscale.com/client/local"
	"tailscale.com/ipn"
	"tailscale.com/ipn/ipnstate"
)

type enrolNode interface {
	LocalClient() (*local.Client, error)
	Close() error
}

// Enroller implements the setup-only enrolment seam. The constructor seam
// allows secretless tests to drive the real local API translation path.
type Enroller struct {
	newNode func(Config) (enrolNode, error)
}

func (e Enroller) node(config Config) (enrolNode, error) {
	if e.newNode != nil {
		release, err := participantstate.Acquire(config.StateDir, config.Name)
		if err != nil {
			return nil, err
		}
		node, err := e.newNode(config)
		if err != nil {
			_ = release()
			return nil, err
		}
		return &lockedEnrolNode{enrolNode: node, release: release}, nil
	}
	f, err := New(config)
	if err != nil {
		return nil, err
	}
	return &ownedEnrolNode{Fabric: f}, nil
}

// Enrol holds the join credential only in memory and waits for Running. A
// person has no credential and receives login URLs only through its explicit
// callback. The granted network label is checked against the readable request
// before crossing the seam, including the coordinator collision suffix.
func Enrol(ctx context.Context, spec fabric.EnrolSpec) (fabric.SelfIdentity, error) {
	return (Enroller{}).Enrol(ctx, spec)
}

func (e Enroller) Enrol(ctx context.Context, spec fabric.EnrolSpec) (self fabric.SelfIdentity, result error) {
	if err := ClearAmbientCredentials(); err != nil {
		return self, err
	}
	defer func() {
		if result != nil && spec.Credential.Value != "" && strings.Contains(result.Error(), spec.Credential.Value) {
			result = errors.New(strings.ReplaceAll(result.Error(), spec.Credential.Value, "[redacted]"))
		}
	}()
	name, logical, err := naming.Parse(spec.Name)
	if err != nil {
		return self, err
	}
	if !logical || spec.StateDirectory == "" {
		return self, errors.New("enrolment requires a logical name and participant state directory")
	}
	if spec.Credential.Value == "" && spec.LoginURL == nil {
		return self, errors.New("enrolment requires a join credential or person login callback")
	}
	node, err := e.node(Config{Name: spec.Name, NetworkName: name.ReadableHostname(), StateDir: spec.StateDirectory, Credential: spec.Credential, CoordinatorURL: spec.CoordinatorURL, Logf: func(string, ...any) {}})
	if err != nil {
		return self, err
	}
	client, err := node.LocalClient() // starts the embedded node
	if err != nil {
		return self, errors.Join(err, node.Close())
	}
	defer func() { result = errors.Join(result, node.Close()) }()
	watcher, err := client.WatchIPNBus(ctx, ipn.NotifyInitialState)
	if err != nil {
		return self, err
	}
	defer watcher.Close()
	lastURL := ""
	for {
		notice, err := watcher.Next()
		if err != nil {
			return self, err
		}
		if notice.ErrMessage != nil {
			return self, errors.New("Fabric enrolment: " + *notice.ErrMessage)
		}
		if notice.BrowseToURL != nil && *notice.BrowseToURL != "" && *notice.BrowseToURL != lastURL && spec.LoginURL != nil {
			lastURL = *notice.BrowseToURL
			spec.LoginURL(lastURL)
		}
		if notice.State == nil || *notice.State != ipn.Running {
			continue
		}
		status, err := client.Status(ctx)
		if err != nil {
			return self, err
		}
		self, err := selfIdentityFromStatus(status, name)
		if err == nil {
			self.FabricID = coordinatorFabricID(spec.CoordinatorURL, status.CurrentTailnet.MagicDNSSuffix)
		}
		return self, err
	}
}

func selfIdentityFromStatus(status *ipnstate.Status, name naming.Name) (fabric.SelfIdentity, error) {
	if status == nil || status.BackendState != "Running" || status.Self == nil || status.Self.ID == "" || len(status.Self.TailscaleIPs) == 0 {
		return fabric.SelfIdentity{}, fabric.ErrIdentityIncomplete
	}
	peer := status.Self
	// HostName is merely the requested label. DNSName is the granted unique
	// label: a stale device can make it differ even when HostName matches.
	granted, _, _ := strings.Cut(strings.TrimSuffix(peer.DNSName, "."), ".")
	if !acceptedNetworkGrant(name.ReadableHostname(), granted) {
		return fabric.SelfIdentity{}, fabric.ErrGrantedNameMismatch
	}
	if status.CurrentTailnet == nil || status.CurrentTailnet.MagicDNSSuffix == "" {
		return fabric.SelfIdentity{}, fabric.ErrIssuingFabricUnavailable
	}
	self := fabric.SelfIdentity{Identity: fabric.Identity{NodeID: string(peer.ID), DeviceID: string(peer.ID)}, Name: name.String(), NetworkName: granted}
	if peer.Tags != nil {
		self.Tags = append([]string(nil), peer.Tags.AsSlice()...)
	}
	if len(self.Tags) != 0 {
		self.Kind = fabric.IdentityKindMachine
	} else {
		if peer.UserID == 0 {
			return fabric.SelfIdentity{}, fabric.ErrIdentityIncomplete
		}
		self.UserID = strconv.FormatInt(int64(peer.UserID), 10)
		if user, ok := status.User[peer.UserID]; ok {
			self.DisplayName = user.DisplayName
		}
	}
	if peer.KeyExpiry != nil {
		self.KeyExpiry = *peer.KeyExpiry
	}
	return self, nil
}

// Logout is used only to roll back a participant created by setup. It opens
// that saved state without any credential; setup removes the folder afterward.
func (e Enroller) Logout(ctx context.Context, spec fabric.EnrolSpec) (result error) {
	if err := ClearAmbientCredentials(); err != nil {
		return err
	}
	requested, err := fabric.RequestedNetworkName(spec.Name)
	if err != nil {
		return err
	}
	node, err := e.node(Config{Name: spec.Name, NetworkName: requested, StateDir: spec.StateDirectory, CoordinatorURL: spec.CoordinatorURL, Logf: func(string, ...any) {}})
	if err != nil {
		return err
	}
	client, err := node.LocalClient()
	if err != nil {
		return errors.Join(err, node.Close())
	}
	defer func() { result = errors.Join(result, node.Close()) }()
	if err := client.Logout(ctx); err != nil {
		return fmt.Errorf("Fabric logout: %w", err)
	}
	return nil
}

var _ fabric.Enroller = Enroller{}

type lockedEnrolNode struct {
	enrolNode
	release func() error
}

func (n *lockedEnrolNode) Close() error { return errors.Join(n.enrolNode.Close(), n.release()) }

// Keep the backend-local API inside this private enrolment adapter while
// retaining Fabric.Close's ownership lock through backend shutdown.
type ownedEnrolNode struct{ *Fabric }

func (n *ownedEnrolNode) LocalClient() (*local.Client, error) { return n.localClient() }

// The coordinator assigns DNSName; the embedded backend does not allocate it.
// Its documented de-duplication form is <requested>-1, -2, ...:
// https://tailscale.com/docs/concepts/machine-names#machine-name-conflicts
// Do not accept arbitrary renames or a match on the requested HostName alone.
func acceptedNetworkGrant(requested, granted string) bool {
	if fabric.ValidateNetworkName(granted) != nil {
		return false
	}
	if granted == requested {
		return true
	}
	suffix, ok := strings.CutPrefix(granted, requested+"-")
	if !ok || len(suffix) == 0 || suffix[0] < '1' || suffix[0] > '9' {
		return false
	}
	for _, c := range suffix {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
