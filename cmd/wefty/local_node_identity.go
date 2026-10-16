package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

// localNodeIdentity is enough to route a Pinned run to this machine. Fabric
// caller identity and hostname are deliberately not sources of node identity.
type localNodeIdentity struct {
	NodeID string `json:"node_id"`
	Tag    string `json:"tag"`
}

type localNodeErrorCode string

const (
	localNodeNotInstalled  localNodeErrorCode = "local_node_not_installed"
	localNodeIDMissing     localNodeErrorCode = "local_node_id_missing"
	localNodeConfigInvalid localNodeErrorCode = "local_node_config_invalid"
	localNodeUnknown       localNodeErrorCode = "local_node_unknown"
	localNodeUnavailable   localNodeErrorCode = "local_node_unavailable"
	localNodeTagMissing    localNodeErrorCode = "local_node_tag_missing"
)

// localNodeError lets a caller distinguish installation, registration and
// transport failures without parsing the diagnostic sentence.
type localNodeError struct {
	Code   localNodeErrorCode
	Detail string
	Cause  error
}

func (e *localNodeError) Error() string { return e.Detail }
func (e *localNodeError) Unwrap() error { return e.Cause }

// resolveLocalNode reads only the selected installation metadata, then checks
// that exact stable ID and its reserved tag through the public L1 node API.
// It needs neither a running control socket nor access to node credentials.
func resolveLocalNode(ctx context.Context, clients *apiClients) (localNodeIdentity, error) {
	fail := func(code localNodeErrorCode, detail string, cause error) (localNodeIdentity, error) {
		return localNodeIdentity{}, &localNodeError{Code: code, Detail: detail, Cause: cause}
	}
	if clients.nodeConfigPath == "" {
		return fail(localNodeNotInstalled, "no installed node on this machine; install a node or select its metadata with --node-config or WEFTY_NODE_CONFIG", nil)
	}
	path, err := filepath.Abs(clients.nodeConfigPath)
	if err != nil {
		return fail(localNodeConfigInvalid, "cannot resolve installed node configuration path", err)
	}
	config, err := ocicontrol.ReadInstalledConfig(path)
	if errors.Is(err, os.ErrNotExist) {
		return fail(localNodeNotInstalled, "no installed node configuration at "+path+"; install a node or select its metadata with --node-config or WEFTY_NODE_CONFIG", err)
	}
	if err != nil {
		return fail(localNodeConfigInvalid, "cannot read installed node configuration at "+path+"; check access or upgrade or reinstall the node: "+err.Error(), err)
	}
	if strings.TrimSpace(config.NodeID) == "" {
		return fail(localNodeIDMissing, "installed node configuration at "+path+" does not record a stable node ID; upgrade or reinstall the node", nil)
	}
	if strings.TrimSpace(config.NodeID) != config.NodeID || strings.ContainsAny(config.NodeID, "\r\n\t") {
		return fail(localNodeConfigInvalid, "installed node configuration has an invalid stable node ID; upgrade or reinstall the node", nil)
	}
	node, err := clients.getNode(ctx, config.NodeID)
	if err != nil {
		var responseErr *apiResponseError
		if errors.As(err, &responseErr) && responseErr.APIError.Code == contract.ErrorNotFound {
			return fail(localNodeUnknown, fmt.Sprintf("installed node %q is not known to L1 at %s; check the L1 endpoint and the node installation", config.NodeID, clients.l1.address), err)
		}
		var sandbox *networkSandboxError
		if errors.As(err, &sandbox) {
			return fail(localNodeUnavailable, sandbox.Error(), err)
		}
		return fail(localNodeUnavailable, fmt.Sprintf("cannot verify installed node %q with L1: %s", config.NodeID, probeDetail(err)), err)
	}
	if node.NodeID != config.NodeID {
		return fail(localNodeUnknown, fmt.Sprintf("L1 did not return installed node %q", config.NodeID), nil)
	}
	// Reuse the CLI's existing Pinned routing helper and its tag normalization.
	tags, err := pinnedImageTags(nil, config.NodeID, true)
	if err != nil {
		return fail(localNodeConfigInvalid, "cannot form installed node routing tag; upgrade or reinstall the node", err)
	}
	if !slices.Contains(node.AuthoritativeTags, tags[0]) {
		return fail(localNodeTagMissing, fmt.Sprintf("installed node %q does not have reserved routing tag %q in L1; add it to the node's authoritative tags", config.NodeID, tags[0]), nil)
	}
	return localNodeIdentity{NodeID: config.NodeID, Tag: tags[0]}, nil
}
