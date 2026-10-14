package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"net/http/httputil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestLocalNodeIdentityAndStatus(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, metadata, nodeID, tag string
		code                        localNodeErrorCode
		wantDetail                  string
		l1Down                      bool
	}{
		{name: "installed node", metadata: `"node_id":"Node-A"`, nodeID: "Node-A", tag: "wefty:node:node-a"},
		{name: "no installed node", code: localNodeNotInstalled, wantDetail: "install a node"},
		{name: "old installation", code: localNodeIDMissing, wantDetail: "upgrade or reinstall the node"},
		{name: "node unknown to L1", metadata: `"node_id":"unknown-node"`, code: localNodeUnknown, wantDetail: `installed node "unknown-node" is not known to L1`},
		{name: "reserved tag missing", metadata: `"node_id":"Node-A"`, nodeID: "Node-A", code: localNodeTagMissing, wantDetail: "authoritative tags"},
		{name: "L1 unavailable", metadata: `"node_id":"Node-A"`, nodeID: "Node-A", l1Down: true, code: localNodeUnavailable, wantDetail: "cannot verify installed node"},
		{name: "invalid metadata", metadata: `"node_id":42`, code: localNodeConfigInvalid, wantDetail: "upgrade or reinstall the node"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			h := newStatusHarness(t)
			// Even a singleton ready fleet must never be selected by inference.
			h.nodes = l1.NodeList{Nodes: []l1.Node{readyNode("other-node")}}
			if test.nodeID != "" {
				node := readyNode(test.nodeID)
				node.AuthoritativeTags = []string{"unrelated", test.tag}
				h.nodes.Nodes = append(h.nodes.Nodes, node)
			}
			h.l1Down = test.l1Down
			clients := h.clients()
			clients.nodeConfigPath = filepath.Join(t.TempDir(), "node.json")
			if test.code != localNodeNotInstalled {
				payload := `{"version":1,"control_socket":"/tmp/unused-wefty-control.sock"`
				if test.metadata != "" {
					payload += "," + test.metadata
				}
				payload += "}"
				if err := os.WriteFile(clients.nodeConfigPath, []byte(payload), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			local, err := resolveLocalNode(t.Context(), clients)
			if test.code == "" {
				if err != nil || local.NodeID != test.nodeID || local.Tag != test.tag {
					t.Fatalf("local identity=%+v error=%v", local, err)
				}
			} else {
				var typed *localNodeError
				if !errors.As(err, &typed) || typed.Code != test.code || !strings.Contains(typed.Detail, test.wantDetail) {
					t.Fatalf("local identity error=%v; want typed %s with %q", err, test.code, test.wantDetail)
				}
				if local != (localNodeIdentity{}) {
					t.Fatalf("failed resolution returned identity=%+v", local)
				}
			}
			for _, jsonOutput := range []bool{false, true} {
				var out, errOut bytes.Buffer
				err := executeStatus(t.Context(), clients, jsonOutput, nil, &out, &errOut)
				if !test.l1Down && err != nil {
					t.Fatalf("local installation problem stopped cluster status: %v", err)
				}
				if test.l1Down && commandExitCode(err) != exitNotReady {
					t.Fatalf("unavailable L1 error=%v", err)
				}
				if errOut.Len() != 0 {
					t.Fatalf("unexpected stderr: %s", errOut.String())
				}
				if jsonOutput {
					// Decode the published document, including an explicit null on
					// a machine that has no local node; omission is ambiguous.
					var doc map[string]json.RawMessage
					if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
						t.Fatal(err)
					}
					raw, exists := doc["local_node"]
					if !exists {
						t.Fatalf("JSON has no local_node: %s", out.String())
					}
					var status clusterStatus
					if err := json.Unmarshal(out.Bytes(), &status); err != nil {
						t.Fatal(err)
					}
					if test.code == "" {
						if status.LocalNode == nil || *status.LocalNode != local || status.LocalNodeError != nil {
							t.Fatalf("local status=%s", out.String())
						}
					} else if string(raw) != "null" || status.LocalNodeError == nil || status.LocalNodeError.Code != string(test.code) {
						t.Fatalf("local error status=%s", out.String())
					}
					for _, node := range status.Nodes {
						want := test.code == "" && node.NodeID == test.nodeID
						if node.ThisMachine != want {
							t.Fatalf("node marker=%+v want=%t", node, want)
						}
					}
				} else {
					want := test.wantDetail
					if test.code == "" {
						want = "this machine: " + test.nodeID + " (Pinned tag: " + test.tag + ")"
						if !strings.Contains(out.String(), test.nodeID+" (this machine)") {
							t.Fatalf("node row is not marked: %s", out.String())
						}
					}
					if !strings.Contains(out.String(), want) {
						t.Fatalf("human status missing %q: %s", want, out.String())
					}
				}
			}
		})
	}
}

// Exercise the root option wiring as well as the status command. Neither
// caller Fabric identity nor another config in the environment may win over
// an explicit --node-config.
func TestStatusUsesSelectedNodeInstallation(t *testing.T) {
	t.Setenv("WEFTY_FABRIC_PRINT_ENROLLMENT_URL", "0")
	h := newStatusHarness(t)
	node := readyNode("selected-node")
	node.AuthoritativeTags = []string{contract.StableNodeTagPrefix + node.NodeID}
	h.nodes = l1.NodeList{Nodes: []l1.Node{node}}
	clients := h.clients()
	config := filepath.Join(t.TempDir(), "node.json")
	if err := os.WriteFile(config, []byte(`{"version":1,"control_socket":"/tmp/unused-control.sock","node_id":"selected-node"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	// Plain Fabric carries an identity handshake before HTTP. Put its
	// listener in front of the same HTTP fixtures used above.
	participant := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "status-fixture"})
	addresses := make([]string, 0, 2)
	for _, client := range []*apiClient{clients.l1, clients.l3} {
		target := client.client.Transport.(*redirectingTransport).target
		listener, err := participant.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		server := httptest.NewUnstartedServer(httputil.NewSingleHostReverseProxy(target))
		server.Listener = listener
		server.Start()
		t.Cleanup(server.Close)
		addresses = append(addresses, listener.Addr().String())
	}
	l1Address, l3Address := addresses[0], addresses[1]
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "environment", true: "flag overrides environment"}[explicit], func(t *testing.T) {
			args := []string{"--fabric=plain", "--plain-identity=other-node", "--l1=" + l1Address, "--l3=" + l3Address, "--json", "status"}
			t.Setenv("WEFTY_NODE_CONFIG", config)
			if explicit {
				t.Setenv("WEFTY_NODE_CONFIG", filepath.Join(t.TempDir(), "missing.json"))
				args = append([]string{"--node-config=" + config}, args...)
			}
			var out, errOut bytes.Buffer
			if err := run(t.Context(), args, &out, &errOut); err != nil {
				t.Fatalf("root status error=%v stderr=%s", err, errOut.String())
			}
			var status clusterStatus
			if err := json.Unmarshal(out.Bytes(), &status); err != nil {
				t.Fatal(err)
			}
			if status.LocalNode == nil || status.LocalNode.NodeID != node.NodeID || status.LocalNode.Tag != node.AuthoritativeTags[0] {
				t.Fatalf("root status ignored selected installation: %s", out.String())
			}
		})
	}
}
