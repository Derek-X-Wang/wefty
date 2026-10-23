//go:build linux

package ocihelper

import (
	"context"
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
		loopback, err := netlink.LinkByName("lo")
		if err != nil {
			return &RuntimeSpecRejectionError{err: fmt.Errorf("Computer network none requires loopback: %w", err)}
		}
		if err := netlink.LinkSetUp(loopback); err != nil {
			return err
		}
		links, err := netlink.LinkList()
		if err != nil {
			return err
		}
		facts := make([]computerNetworkLink, 0, len(links))
		for _, link := range links {
			addresses, err := netlink.AddrList(link, netlink.FAMILY_ALL)
			if err != nil {
				return err
			}
			attrs := link.Attrs()
			facts = append(facts, computerNetworkLink{name: attrs.Name, kind: link.Type(), index: attrs.Index,
				up: attrs.Flags&net.FlagUp != 0, loopback: attrs.Flags&net.FlagLoopback != 0, addresses: len(addresses)})
		}
		loopbackIndex, err := validateComputerNoneNetworkLinks(facts)
		if err != nil {
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
			if route.LinkIndex != loopbackIndex || route.Gw != nil || route.Dst == nil || !route.Dst.IP.IsLoopback() {
				return &RuntimeSpecRejectionError{err: fmt.Errorf("Computer network none has an outward route: %s", route.String())}
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
