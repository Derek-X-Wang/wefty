package l1

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

type nodePageWire struct {
	Nodes      []Node `json:"nodes"`
	NextCursor string `json:"next_cursor"`
}

func TestNodesListingPagingAndExactFilters(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	client := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
	seed := func(id, state string, claims bool, caps map[string]bool) {
		t.Helper()
		_, err := h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent-" + id}, contract.NodeRegistration{NodeID: id, BootSessionID: "boot", OS: "linux", Architecture: "arm64", AgentVersion: "test", Capabilities: caps, CapabilityRevision: 1, CapabilityObservedAt: h.clock.Now(), MissingCapabilities: []string{}}, NodePolicy{}, claims)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.store.db.Exec("UPDATE nodes SET state=? WHERE node_id=?", state, id); err != nil {
			t.Fatal(err)
		}
	}
	seed("b", "alive", true, map[string]bool{"kind:process": true, "kind:oci": true})
	seed("d", "alive", true, map[string]bool{"kind:process": true, "kind:oci": true})
	seed("f", "alive", false, map[string]bool{"kind:process": true, "kind:oci": false})
	seed("h", "draining", true, map[string]bool{"kind:process": true, "kind:oci-extra": true})
	read := func(query string) nodePageWire {
		t.Helper()
		status, _, body := h.do(client, http.MethodGet, "/v1/nodes?"+query, nil)
		if status != http.StatusOK {
			t.Fatalf("status=%d body=%s", status, body)
		}
		var page nodePageWire
		if err := json.Unmarshal(body, &page); err != nil {
			t.Fatal(err)
		}
		if page.Nodes == nil {
			t.Fatalf("null nodes: %s", body)
		}
		return page
	}
	for _, check := range []struct {
		query string
		want  []string
	}{
		{"state=alive", []string{"b", "d", "f"}},
		{"state=draining", []string{"h"}},
		{"claims_enabled=false", []string{"f"}},
		{"claims_enabled=true", []string{"b", "d", "h"}},
		{"capability=kind:oci", []string{"b", "d"}},
		{"state=alive&claims_enabled=true&capability=kind:oci", []string{"b", "d"}},
		{"capability=missing", []string{}},
	} {
		t.Run(check.query, func(t *testing.T) {
			page := read(check.query)
			got := []string{}
			for _, n := range page.Nodes {
				got = append(got, n.NodeID)
				if len(n.AllowedActions) == 0 || n.ActiveAttempts == nil {
					t.Fatalf("projection missing: %#v", n)
				}
			}
			if !reflect.DeepEqual(got, check.want) {
				t.Fatalf("got=%v want=%v", got, check.want)
			}
		})
	}
	first := read("limit=1")
	if len(first.Nodes) != 1 || first.Nodes[0].NodeID != "b" || first.NextCursor == "" {
		t.Fatalf("first page=%+v", first)
	}
	// Inserts on both sides of the keyset must not join this walk.
	seed("a", "alive", true, map[string]bool{"kind:process": true})
	seed("c", "alive", true, map[string]bool{"kind:process": true})
	seed("z", "alive", true, map[string]bool{"kind:process": true})
	// Maintenance and idempotent migration must not rewrite walk membership.
	if _, err := h.store.db.Exec("VACUUM"); err != nil {
		t.Fatal(err)
	}
	if err := h.store.initializeNodeListing(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Another boot keeps the same node membership and identity key.
	_, err := h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent-d"}, contract.NodeRegistration{NodeID: "d", BootSessionID: "new-boot", OS: "linux", Architecture: "arm64", AgentVersion: "test", Capabilities: map[string]bool{"kind:process": true}}, NodePolicy{}, true)
	if err != nil {
		t.Fatal(err)
	}
	got := []string{"b"}
	cursor := first.NextCursor
	for i := 0; cursor != ""; i++ {
		if i > 10 {
			t.Fatal("cursor did not terminate")
		}
		page := read("limit=2&cursor=" + url.QueryEscape(cursor))
		for _, n := range page.Nodes {
			got = append(got, n.NodeID)
		}
		cursor = page.NextCursor
	}
	if !reflect.DeepEqual(got, []string{"b", "d", "f", "h"}) {
		t.Fatalf("unstable walk: %v", got)
	}
	for _, query := range []string{"limit=0", "limit=1001", "limit=bad", "limit=1&limit=2", "state=ALIVE", "state=", "state=alive&state=dead", "claims_enabled=1", "claims_enabled=", "claims_enabled=true&claims_enabled=false", "capability=", "capability=x&capability=y", "cursor=bad", "cursor=" + url.QueryEscape(first.NextCursor) + "&state=alive"} {
		t.Run(query, func(t *testing.T) {
			status, _, body := h.do(client, http.MethodGet, "/v1/nodes?"+query, nil)
			assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
		})
	}
}

func TestNodesListingPlanAndReadSnapshot(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	_, err := h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, contract.NodeRegistration{NodeID: "node", BootSessionID: "boot", OS: "linux", Architecture: "arm64", AgentVersion: "test", Capabilities: map[string]bool{"kind:process": true}}, NodePolicy{}, true)
	if err != nil {
		t.Fatal(err)
	}
	enabled := true
	for _, state := range []string{"", "alive"} {
		for _, claims := range []*bool{nil, &enabled} {
			for _, capability := range []string{"", "kind:process"} {
				for _, continuation := range []bool{false, true} {
					cursor := nodeListCursor{HighWater: 1}
					if continuation {
						cursor.NodeID = "m"
						cursor.Identity = "agent"
					}
					query, args := nodeListingQuery(nodeListFilters{State: state, ClaimsEnabled: claims, Capability: capability}, cursor, 1, h.clock.Now(), h.store.nodeLiveness())
					rows, err := h.store.db.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+query, args...)
					if err != nil {
						t.Fatal(err)
					}
					var plan []string
					for rows.Next() {
						var id, parent, unused int
						var detail string
						if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
							t.Fatal(err)
						}
						plan = append(plan, detail)
					}
					err = rows.Err()
					rows.Close()
					if err != nil {
						t.Fatal(err)
					}
					details := strings.Join(plan, "\n")
					t.Logf("state=%s claims=%v capability=%s continuation=%v\n%s", state, claims, capability, continuation, details)
					if len(plan) == 0 || (!strings.Contains(plan[0], "SEARCH nodes USING") || !strings.Contains(plan[0], "nodes_listing_")) || !strings.Contains(plan[0], "node_id,identity_node_id") || strings.Contains(details, "TEMP B-TREE") || strings.Contains(details, "SCAN nodes") {
						t.Errorf("listing must seek the row-value key without scanning or sorting nodes:\n%s", details)
					}
					if !strings.Contains(query, "(nodes.node_id, nodes.identity_node_id) > (?, ?)") {
						t.Error("listing must use a row-value keyset")
					}
				}
			}
		}
	}
	// The IMMEDIATE default must not make a page reader wait for a writer.
	tx, err := h.store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	page, err := h.store.listNodesPage(ctx, nodeListFilters{}, "", 1)
	if err != nil || ctx.Err() != nil || len(page.Nodes) != 1 {
		t.Fatalf("read blocked by writer: page=%+v err=%v context=%v", page, err, ctx.Err())
	}
}
