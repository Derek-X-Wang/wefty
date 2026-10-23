//go:build linux

package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"net"

	netlink "github.com/tailscale/netlink"
)

func (engine *ContainerdEngine) prepareComputerWorkloadNetwork(ctx context.Context, namespace *pinnedNetworkNamespace, port uint16, workload WorkloadInput) (*computerNetworkAttachment, error) {
	if err := validateComputerNetwork(workload); err != nil {
		return nil, err
	}
	if workload.ComputerNetwork != ComputerNetworkNone {
		return engine.prepareComputerNetwork(ctx, namespace, port)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// The runtime spec creates a fresh network namespace. No veth, default
	// route, NAT, DNS proxy, egress probe or firewall is installed for none.
	if err := inNetworkNamespace(namespace, func() error {
		links, err := netlink.LinkList()
		if err != nil {
			return err
		}
		if len(links) != 1 || links[0].Attrs().Name != "lo" || links[0].Attrs().Flags&net.FlagLoopback == 0 {
			return errors.New("Computer network none requires a fresh loopback-only namespace")
		}
		if err := netlink.LinkSetUp(links[0]); err != nil {
			return err
		}
		routes, err := netlink.RouteList(nil, netlink.FAMILY_ALL)
		if err != nil {
			return err
		}
		for _, route := range routes {
			// Local loopback routes are created by the kernel when lo goes up.
			// They cannot leave this namespace. No default or outward route is
			// accepted, including an IPv6 route.
			if route.LinkIndex != links[0].Attrs().Index || route.Gw != nil || route.Dst == nil || !route.Dst.IP.IsLoopback() {
				return fmt.Errorf("Computer network none has an outward route: %s", route.String())
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &computerNetworkAttachment{}, nil
}

func (engine *ContainerdEngine) prepareComputerWorkloadNetworkFiles(workload WorkloadInput) error {
	if workload.Computer && workload.ComputerNetwork == ComputerNetworkNone {
		return prepareComputerNoneNetworkFiles(engine.config.RuntimeRoot)
	}
	return engine.refreshManagedNetworkFiles()
}
