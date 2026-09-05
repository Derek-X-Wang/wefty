//go:build !linux

package ocihelper

import (
	"context"
	"errors"
)

// ComputerDNSUpstreamObservation is unavailable off Linux, where the OCI
// helper does not create Computer network namespaces or DNS proxies.
type ComputerDNSUpstreamObservation struct {
	Address   string
	Source    string
	Reachable bool
}

// ObserveComputerDNSUpstream fails closed off Linux. The service-acceptance
// caller invokes it only after establishing that the runtime is Linux.
func ObserveComputerDNSUpstream(context.Context, string) (ComputerDNSUpstreamObservation, error) {
	return ComputerDNSUpstreamObservation{}, errors.New("Computer DNS upstream observation requires Linux")
}
