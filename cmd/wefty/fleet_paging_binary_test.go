package main

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestFleetPagingFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	network := plain.NewNetwork()
	control := network.NewFabric(fabric.Identity{NodeID: "control"})
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "fleet.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	server, err := l1.NewServer(control, store, l1.ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := control.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := serveTestServer(ctx, func() error { return server.Serve(ctx, listener) })
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("node-%d", i)
		_, err := store.RegisterNode(ctx, fabric.Identity{NodeID: "agent-" + id}, contract.NodeRegistration{NodeID: id, BootSessionID: "boot", OS: "linux", Architecture: "arm64", AgentVersion: "test", CapabilityRevision: 1, CapabilityObservedAt: time.Now(), MissingCapabilities: []string{}, Capabilities: map[string]bool{"kind:process": true, "kind:oci": i%2 == 0}}, l1.NodePolicy{}, i%2 == 0)
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = store.CreateComputer(ctx, l1.CreateComputerRequest{Name: fmt.Sprintf("computer-%d", i), Spec: cliComputerSpec(fmt.Sprintf("paging-%d", i)), Actor: "wefty-cli"})
		if err != nil {
			t.Fatal(err)
		}
	}
	base := []string{"--fabric=plain", "--l1=" + listener.Addr().String(), "--l3="}
	call := func(args ...string) (int, string) {
		return runWefty(t, binary, 30*time.Second, append(append([]string{}, base...), args...)...)
	}
	for _, kind := range []string{"nodes", "computers"} {
		t.Run(kind, func(t *testing.T) {
			var got []string
			cursor := ""
			firstCursor := ""
			for i := 0; i < 5; i++ {
				args := []string{"--json", kind, "list", "--limit=1"}
				if cursor != "" {
					args = append(args, "--cursor="+cursor)
				}
				code, output := call(args...)
				if code != 0 {
					t.Fatalf("exit=%d output=%s", code, output)
				}
				var page struct {
					Nodes      []l1.Node     `json:"nodes"`
					Computers  []l1.Computer `json:"computers"`
					NextCursor string        `json:"next_cursor"`
				}
				if err := json.Unmarshal([]byte(output), &page); err != nil {
					t.Fatal(err)
				}
				ids := []string{}
				for _, n := range page.Nodes {
					ids = append(ids, n.NodeID)
				}
				for _, c := range page.Computers {
					ids = append(ids, c.Name)
					if c.ComputerID == c.CurrentJobID || c.CurrentJobID == "" {
						t.Fatalf("Computer authority missing: %+v", c)
					}
				}
				if len(ids) != 1 {
					t.Fatalf("page not bounded: %s", output)
				}
				got = append(got, ids...)
				cursor = page.NextCursor
				if i == 0 {
					firstCursor = cursor
				}
				if cursor == "" {
					break
				}
			}
			prefix := "node"
			if kind == "computers" {
				prefix = "computer"
			}
			want := []string{prefix + "-0", prefix + "-1", prefix + "-2", prefix + "-3"}
			if !reflect.DeepEqual(got, want) || cursor != "" {
				t.Fatalf("walk=%v cursor=%s", got, cursor)
			}
			code, output := call(kind, "list", "--limit=1")
			if code != 0 || !strings.Contains(output, "NEXT CURSOR") || !strings.Contains(output, strings.ToUpper(prefix)+" ID") {
				t.Fatalf("table exit=%d output=%s", code, output)
			}
			code, output = call("--json", kind, "list", "--limit=2", "--all")
			if code != 0 {
				t.Fatalf("all exit=%d output=%s", code, output)
			}
			var all map[string]json.RawMessage
			if err := json.Unmarshal([]byte(output), &all); err != nil {
				t.Fatal(err)
			}
			var rows []json.RawMessage
			if err := json.Unmarshal(all[kind], &rows); err != nil {
				t.Fatal(err)
			}
			var nextCursor string
			if raw := all["next_cursor"]; raw != nil {
				if err := json.Unmarshal(raw, &nextCursor); err != nil {
					t.Fatal(err)
				}
			}
			if len(rows) != 4 || nextCursor != "" {
				t.Fatalf("all=%s", output)
			}
			code, output = call("--json", kind, "list", "--cursor="+firstCursor, "--limit=2", "--all")
			if code != 0 {
				t.Fatalf("remaining all exit=%d output=%s", code, output)
			}
			if err := json.Unmarshal([]byte(output), &all); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(all[kind], &rows); err != nil {
				t.Fatal(err)
			}
			if len(rows) != 3 {
				t.Fatalf("remaining all=%s", output)
			}
			badFlags := [][]string{{"--limit=0"}, {"--limit=1001"}, {"--limit=bad"}, {"--unknown"}, {"extra"}, {"--cursor=bad"}}
			if kind == "nodes" {
				badFlags = append(badFlags, []string{"--state=ALIVE"}, []string{"--state="}, []string{"--claims-enabled=1"}, []string{"--claims-enabled="}, []string{"--capability="})
			}
			for _, flags := range badFlags {
				code, output := call(append([]string{"--json", kind, "list"}, flags...)...)
				if code != exitUsage {
					t.Fatalf("%v exit=%d output=%s", flags, code, output)
				}
				var refusal contract.ErrorResponse
				if err := json.Unmarshal([]byte(firstJSONDocument(output)), &refusal); err != nil || refusal.Error.Code != contract.ErrorInvalidRequest {
					t.Fatalf("error=%s err=%v", output, err)
				}
			}
		})
	}
	code, output := call("--json", "nodes", "list", "--state=alive", "--claims-enabled=true", "--capability=kind:oci", "--limit=1", "--all")
	if code != 0 {
		t.Fatalf("filtered exit=%d output=%s", code, output)
	}
	var filtered struct {
		Nodes []l1.Node `json:"nodes"`
	}
	if err := json.Unmarshal([]byte(output), &filtered); err != nil {
		t.Fatal(err)
	}
	if len(filtered.Nodes) != 2 || filtered.Nodes[0].NodeID != "node-0" || filtered.Nodes[1].NodeID != "node-2" {
		t.Fatalf("filters=%s", output)
	}
	// The default page must be bounded, while --all and the complete-fleet
	// helper used by readiness/routing probes still see beyond that page.
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("extra-%03d", i)
		_, err := store.RegisterNode(ctx, fabric.Identity{NodeID: "agent-" + id}, contract.NodeRegistration{NodeID: id, BootSessionID: "boot", OS: "linux", Architecture: "arm64", AgentVersion: "test", Capabilities: map[string]bool{"kind:process": true}}, l1.NodePolicy{}, false)
		if err != nil {
			t.Fatal(err)
		}
	}
	code, output = call("--json", "nodes", "list")
	if code != 0 {
		t.Fatalf("default page exit=%d output=%s", code, output)
	}
	var page l1.NodeList
	if err := json.Unmarshal([]byte(output), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Nodes) != l1.DefaultJobPageLimit || page.NextCursor == "" {
		t.Fatalf("unbounded default page: count=%d cursor=%s", len(page.Nodes), page.NextCursor)
	}
	code, output = call("--json", "nodes", "list", "--limit=37", "--all")
	if code != 0 {
		t.Fatalf("large all exit=%d output=%s", code, output)
	}
	if err := json.Unmarshal([]byte(output), &page); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, node := range page.Nodes {
		if seen[node.NodeID] {
			t.Fatalf("duplicate node %s", node.NodeID)
		}
		seen[node.NodeID] = true
	}
	if len(seen) != 104 || page.NextCursor != "" {
		t.Fatalf("incomplete all: count=%d cursor=%s", len(seen), page.NextCursor)
	}
	participant := network.NewFabric(fabric.Identity{NodeID: "probe", Tags: []string{l1.DefaultClientPrincipalTag}})
	clients, err := newAPIClients(participant, listener.Addr().String(), "")
	if err != nil {
		t.Fatal(err)
	}
	defer clients.close()
	complete, err := clients.listNodes(ctx)
	if err != nil || len(complete.Nodes) != 104 {
		t.Fatalf("complete-fleet probe: count=%d err=%v", len(complete.Nodes), err)
	}

}
