package lima

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"

	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

const HostGatewayName = "host.lima.internal"

// vzVMType is the Lima virtual machine type whose user-mode network the host
// never owns an interface for.
const vzVMType = "vz"

var instanceNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)

type commandRunner func(context.Context, string, ...string) ([]byte, error)
type listenFunc func(string, string) (net.Listener, error)

// BridgeBinder discovers Lima's host gateway from inside the named VM. It
// binds only that exact non-wildcard address; the helper reverse tunnel is
// selected only when that bind fails.
type BridgeBinder struct {
	Instance string
	Limactl  string
	// Transport is retained for the lifetime of the Mac agent. Epoch authority
	// comes from the helper handshake exposed by Epoch, never socket metadata.
	Transport *EpochSocketDialer
	Epoch     interface {
		Generation() (ocihelper.HelperSession, bool)
	}
	run           commandRunner
	route         commandRunner
	listen        listenFunc
	mu            sync.Mutex
	cachedEpoch   ocihelper.HelperSession
	cachedGateway netip.Addr
}

func NewBridgeBinder(instance string) *BridgeBinder {
	return &BridgeBinder{Instance: instance, Limactl: "limactl"}
}

func (binder *BridgeBinder) Bind(ctx context.Context) (workloadrunner.WorkflowBridgeBinding, error) {
	if binder == nil || !instanceNamePattern.MatchString(binder.Instance) {
		return workloadrunner.WorkflowBridgeBinding{}, errors.New("Lima instance name is required for the workflow bridge")
	}
	run := binder.run
	if run == nil {
		run = runCommand
	}
	limactl := binder.Limactl
	if limactl == "" {
		limactl = "limactl"
	}
	discoveryContext, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var epoch ocihelper.HelperSession
	if binder.Epoch != nil {
		var ok bool
		epoch, ok = binder.Epoch.Generation()
		if !ok {
			return workloadrunner.WorkflowBridgeBinding{}, errors.New("Lima helper epoch is not prepared")
		}
	}
	binder.mu.Lock()
	address := binder.cachedGateway
	cacheHit := address.IsValid() && binder.cachedEpoch == epoch
	binder.mu.Unlock()
	var err error
	if !cacheHit {
		address, err = discoverHostGateway(discoveryContext, run, limactl, binder.Instance)
		if err == nil {
			route := binder.route
			if route == nil {
				route = runCommand
			}
			err = validateGatewayProvenance(discoveryContext, run, route, limactl, binder.Instance, address)
		}
		if err == nil {
			binder.mu.Lock()
			binder.cachedEpoch, binder.cachedGateway = epoch, address
			binder.mu.Unlock()
		}
	}
	if err != nil {
		return workloadrunner.WorkflowBridgeBinding{}, err
	}
	listen := binder.listen
	if listen == nil {
		listen = net.Listen
	}
	listener, bindErr := listen("tcp4", net.JoinHostPort(address.String(), "0"))
	if bindErr == nil {
		return workloadrunner.WorkflowBridgeBinding{Listener: listener, AdvertiseHost: HostGatewayName}, nil
	}
	fallback, fallbackErr := listen("tcp4", "127.0.0.1:0")
	if fallbackErr != nil {
		return workloadrunner.WorkflowBridgeBinding{}, errors.Join(
			fmt.Errorf("bind discovered Lima host gateway %s: %w", address, bindErr),
			fmt.Errorf("bind constrained helper fallback: %w", fallbackErr),
		)
	}
	return workloadrunner.WorkflowBridgeBinding{Listener: fallback, AdvertiseHost: "127.0.0.1", HostBridgeFallback: true}, nil
}

// validateGatewayProvenance proves the discovered gateway is VM-private before
// anything is bound to it. The proof depends on how Lima attached the
// instance's network, so it asks Lima which arrangement this instance actually
// has instead of trusting the template we asked for:
//
//   - vz: the user-mode network lives inside Virtualization.framework, so the
//     host owns no interface for the gateway and an interface-name proof can
//     never succeed. The proof is instead that the discovered address is this
//     instance's own user-network gateway, the address Lima hands the guest.
//   - everything else (vmnet, socket_vmnet, qemu): the gateway is a real host
//     interface address, so the interface name is the proof.
//
// Anything Lima will not answer for is refused: an unprovable gateway must
// never reach the bind.
func validateGatewayProvenance(ctx context.Context, run, route commandRunner, limactl, instance string, address netip.Addr) error {
	vmType, err := instanceVMType(ctx, run, limactl, instance)
	if err != nil {
		return err
	}
	if vmType != vzVMType {
		return validateGatewayRoute(ctx, route, address)
	}
	gateway, err := vzUserNetworkGateway(ctx, run, limactl, instance)
	if err != nil {
		return err
	}
	if gateway != address {
		return fmt.Errorf("gateway %s is not the vz user network gateway %s of Lima instance %q", address, gateway, instance)
	}
	return nil
}

// instanceVMType reads the virtual machine type Lima recorded for this
// instance from its own inventory. `limactl list --json` emits one object per
// instance, so the record is matched by name rather than by position.
func instanceVMType(ctx context.Context, run commandRunner, limactl, instance string) (string, error) {
	output, err := run(ctx, limactl, "list", "--json", instance)
	if err != nil {
		return "", fmt.Errorf("inspect the virtual machine type of Lima instance %q: %w", instance, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(output))
	for {
		var record struct {
			Name   string `json:"name"`
			VMType string `json:"vmType"`
		}
		decodeErr := decoder.Decode(&record)
		if errors.Is(decodeErr, io.EOF) {
			break
		}
		if decodeErr != nil {
			return "", fmt.Errorf("inspect the virtual machine type of Lima instance %q: %w", instance, decodeErr)
		}
		if record.Name == instance && record.VMType != "" {
			return record.VMType, nil
		}
	}
	return "", fmt.Errorf("Lima instance %q reported no virtual machine type", instance)
}

// vzUserNetworkGateway reports the user-mode network gateway Lima configured
// for this instance. Lima hands the address to the guest over its own DHCP
// lease and resolves HostGatewayName to it, so the instance's default route is
// Lima's configured value rather than a literal compiled in here. Exactly one
// IPv4 default gateway must be reported: a second one means the instance is on
// a network arrangement this proof does not cover.
func vzUserNetworkGateway(ctx context.Context, run commandRunner, limactl, instance string) (netip.Addr, error) {
	output, err := run(ctx, limactl, "--tty=false", "shell", "--workdir=/", instance, "ip", "-4", "route", "show", "default")
	if err != nil {
		return netip.Addr{}, fmt.Errorf("inspect the vz user network gateway of Lima instance %q: %w", instance, err)
	}
	var gateway netip.Addr
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		for index := 0; index+1 < len(fields); index++ {
			if fields[index] != "via" {
				continue
			}
			candidate, parseErr := netip.ParseAddr(fields[index+1])
			if parseErr != nil || !candidate.Is4() {
				return netip.Addr{}, fmt.Errorf("Lima instance %q reported an unusable default gateway %q", instance, fields[index+1])
			}
			if gateway.IsValid() && gateway != candidate {
				return netip.Addr{}, fmt.Errorf("Lima instance %q reported default gateways %s and %s", instance, gateway, candidate)
			}
			gateway = candidate
		}
	}
	if !gateway.IsValid() {
		return netip.Addr{}, fmt.Errorf("Lima instance %q reported no IPv4 default gateway", instance)
	}
	return gateway, nil
}

// validateGatewayRoute is the proof for instances whose gateway is a real host
// interface address: the route to it must leave through a virtual machine
// interface, never a physical one.
func validateGatewayRoute(ctx context.Context, run commandRunner, address netip.Addr) error {
	output, err := run(ctx, "route", "-n", "get", address.String())
	if err != nil {
		return fmt.Errorf("inspect Lima gateway route provenance: %w", err)
	}
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "interface:" {
			for _, prefix := range []string{"bridge", "vmenet", "vmnet", "lima"} {
				if strings.HasPrefix(fields[1], prefix) {
					return nil
				}
			}
			return fmt.Errorf("gateway %s routes through physical interface %s", address, fields[1])
		}
	}
	return fmt.Errorf("gateway %s route omitted interface provenance", address)
}

func discoverHostGateway(ctx context.Context, run commandRunner, limactl, instance string) (netip.Addr, error) {
	output, err := run(ctx, limactl, "--tty=false", "shell", "--workdir=/", instance, "getent", "ahostsv4", HostGatewayName)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("discover %s in Lima instance %q: %w", HostGatewayName, instance, err)
	}
	for _, line := range bytes.Split(output, []byte{'\n'}) {
		fields := bytes.Fields(line)
		if len(fields) == 0 {
			continue
		}
		address, parseErr := netip.ParseAddr(string(fields[0]))
		if parseErr == nil && address.Is4() && !address.IsUnspecified() && !address.IsLoopback() && !address.IsMulticast() {
			return address, nil
		}
	}
	return netip.Addr{}, fmt.Errorf("discover %s in Lima instance %q: no safe IPv4 gateway in output", HostGatewayName, instance)
}

func runCommand(ctx context.Context, name string, arguments ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, arguments...)
	output, err := command.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%s: %w: %s", name, err, strings.TrimSpace(string(output)))
	}
	return output, nil
}

var _ workloadrunner.WorkflowBridgeBinder = (*BridgeBinder)(nil)
