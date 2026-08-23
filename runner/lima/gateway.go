package lima

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
			err = validateGatewayRoute(discoveryContext, route, address)
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
