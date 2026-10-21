package main

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

// L1 currently exposes agent versions, but neither its own build version nor
// the scheduling Node ID of its machine. Do not mistake a CLI receipt, an
// endpoint's Fabric ID, or the first Node for the control plane's version.
func nodeVersionWarnings(nodes l1.NodeList) []statusReason {
	versions := map[string]bool{}
	for _, node := range nodes.Nodes {
		if node.State != contract.NodeDead && node.AgentVersion != "" {
			versions[node.AgentVersion] = true
		}
	}
	if len(versions) < 2 {
		return nil
	}
	ordered := append([]l1.Node(nil), nodes.Nodes...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].NodeID < ordered[j].NodeID })
	var detail []string
	for _, node := range ordered {
		if node.State == contract.NodeDead {
			continue
		}
		version := node.AgentVersion
		if version == "" {
			version = "unknown"
		}
		detail = append(detail, fmt.Sprintf("%s (%s)", node.NodeID, version))
	}
	return []statusReason{{Code: "agent_version_mismatch", Detail: "Node agent versions differ: " + strings.Join(detail, ", ") + ". Control-plane version is unavailable; upgrade each machine to the same commit."}}
}

func fleetUpgradeVersionWarnings(ctx context.Context, host setupHost, paths setupPaths, p connprofile.Profile, commit string) ([]statusReason, error) {
	bounded, cancel := context.WithTimeout(ctx, statusProbeBudget)
	defer cancel()
	open := host.fleetClients
	if open == nil {
		open = setupFleetClients
	}
	clients, err := open(bounded, paths, p)
	if err != nil {
		return nil, err
	}
	defer clients.close()
	nodes, err := clients.listNodes(bounded)
	if err != nil {
		return nil, err
	}
	warnings := nodeVersionWarnings(nodes)
	// Also name every other machine which is not yet at this upgrade's commit,
	// even if all observed agents still have one older version.
	var pending []string
	for _, node := range nodes.Nodes {
		if node.State != contract.NodeDead && node.NodeID != p.Identities.Node && node.AgentVersion != commit {
			version := node.AgentVersion
			if version == "" {
				version = "unknown"
			}
			pending = append(pending, fmt.Sprintf("%s (%s)", node.NodeID, version))
		}
	}
	sort.Strings(pending)
	if len(pending) > 0 {
		warnings = append(warnings, statusReason{Code: "machines_need_upgrade", Detail: "Machines still needing upgrade to " + commit + ": " + strings.Join(pending, ", ") + ". Run wefty upgrade on each machine; this command upgrades only this joining machine."})
	}
	return warnings, nil
}
