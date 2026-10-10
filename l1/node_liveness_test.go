package l1

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestNodeEffectiveLivenessBoundariesPreserveRecordedFacts(t *testing.T) {
	for _, check := range []struct {
		state contract.NodeState
		age   time.Duration
		want  contract.NodeState
	}{
		{contract.NodeAlive, DefaultNodeStaleAfter - time.Nanosecond, contract.NodeAlive},
		{contract.NodeAlive, DefaultNodeStaleAfter, contract.NodeStale},
		{contract.NodeAlive, DefaultNodeDeadAfter - time.Nanosecond, contract.NodeStale},
		{contract.NodeAlive, DefaultNodeDeadAfter, contract.NodeDead},
		{contract.NodeStale, DefaultNodeDeadAfter, contract.NodeDead},
		{contract.NodeDraining, DefaultNodeStaleAfter, contract.NodeDraining},
		{contract.NodeDraining, DefaultNodeDeadAfter, contract.NodeDead},
		{contract.NodeDead, 0, contract.NodeDead},
	} {
		t.Run(string(check.state)+"/"+check.age.String(), func(t *testing.T) {
			h := newIntegrationHarnessWithReconcileInterval(t, StoreOptions{}, nil, true, time.Hour)
			agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
			operator := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
			registered := h.register(agent, "node")
			if _, err := h.store.db.Exec("UPDATE nodes SET state=? WHERE node_id='node'", check.state); err != nil {
				t.Fatal(err)
			}
			h.clock.Advance(check.age)
			status, _, body := h.do(operator, http.MethodGet, "/v1/nodes/node", nil)
			var node Node
			if err := json.Unmarshal(body, &node); err != nil || status != http.StatusOK {
				t.Fatalf("detail=%d %s err=%v", status, body, err)
			}
			if node.State != check.want || !reflect.DeepEqual(node.LastCondition, registered.LastCondition) {
				t.Fatalf("effective state/recorded condition=%#v, want %s with %#v", node, check.want, registered.LastCondition)
			}
			var recorded contract.NodeState
			if err := h.store.db.QueryRow("SELECT state FROM nodes WHERE node_id='node'").Scan(&recorded); err != nil || recorded != check.state {
				t.Fatalf("view wrote state=%s err=%v", recorded, err)
			}
		})
	}
}

func TestNodesEffectiveLivenessFiltersBeforePaging(t *testing.T) {
	h := newIntegrationHarnessWithReconcileInterval(t, StoreOptions{}, nil, true, time.Hour)
	for _, item := range []struct {
		id    string
		state contract.NodeState
		age   time.Duration
	}{
		{"a", contract.NodeAlive, 0},
		{"b", contract.NodeAlive, DefaultNodeDeadAfter},
		{"c", contract.NodeAlive, 0},
		{"d", contract.NodeDraining, DefaultNodeDeadAfter},
		{"e", contract.NodeAlive, DefaultNodeStaleAfter},
		{"f", contract.NodeStale, DefaultNodeDeadAfter},
		{"g", contract.NodeDead, 0},
		{"h", contract.NodeDraining, DefaultNodeStaleAfter},
	} {
		_, err := h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent-" + item.id}, contract.NodeRegistration{
			NodeID: item.id, BootSessionID: "boot", OS: "linux", Architecture: "arm64", AgentVersion: "test",
			Capabilities: map[string]bool{"kind:process": true}, CapabilityRevision: 1, CapabilityObservedAt: h.clock.Now(), MissingCapabilities: []string{},
		}, NodePolicy{}, true)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.store.db.Exec("UPDATE nodes SET state=?, last_heartbeat_ns=? WHERE node_id=?", item.state, h.clock.Now().Add(-item.age).UnixNano(), item.id); err != nil {
			t.Fatal(err)
		}
	}
	enabled := true
	for _, check := range []struct {
		state string
		want  []string
	}{
		{"alive", []string{"a", "c"}},
		{"stale", []string{"e"}},
		{"dead", []string{"b", "d", "f", "g"}},
		{"draining", []string{"h"}},
	} {
		t.Run(check.state, func(t *testing.T) {
			filters := nodeListFilters{State: check.state, ClaimsEnabled: &enabled, Capability: "kind:process"}
			var cursor string
			var ids []string
			for pageNumber := 0; ; pageNumber++ {
				if pageNumber > 250 {
					t.Fatal("paging did not terminate")
				}
				page, err := h.store.listNodesPage(t.Context(), filters, cursor, 2)
				if err != nil {
					t.Fatal(err)
				}
				remaining := len(check.want) - len(ids)
				// An adaptive page may stop short of its limit at the read
				// cutoff, so requiring a full page here would fail the slow
				// machine. The essential property is what these pages must
				// never contain: rows the filter excludes.
				if len(page.Nodes) > min(2, remaining) {
					t.Fatalf("filter applied after paging: page=%#v want rows<=%d", page, min(2, remaining))
				}
				for _, node := range page.Nodes {
					if string(node.State) != check.state {
						t.Fatalf("filter/projection disagree: %#v", node)
					}
					ids = append(ids, node.NodeID)
				}
				cursor = page.NextCursor
				if cursor == "" {
					break
				}
			}
			if !reflect.DeepEqual(ids, check.want) {
				t.Fatalf("effective filter IDs=%v want=%v", ids, check.want)
			}
		})
	}
}

func TestNodeViewsPinOneClock(t *testing.T) {
	store, clock := snapshotStore(t)
	for _, id := range []string{"a", "b"} {
		if _, err := store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent-" + id}, contract.NodeRegistration{
			NodeID: id, BootSessionID: "boot", OS: "linux", Architecture: "arm64", AgentVersion: "test",
			Capabilities: map[string]bool{"kind:process": true}, CapabilityRevision: 1, CapabilityObservedAt: clock.Now(), MissingCapabilities: []string{},
		}, NodePolicy{}, true); err != nil {
			t.Fatal(err)
		}
	}
	clock.at.Add(int64(DefaultNodeStaleAfter))
	for _, view := range []string{"page", "detail", "fleet"} {
		t.Run(view, func(t *testing.T) {
			clock.calls.Store(0)
			var nodes []Node
			var err error
			switch view {
			case "page":
				var page NodeList
				page, err = store.listNodesPage(t.Context(), nodeListFilters{State: "stale"}, "", 2)
				nodes = page.Nodes
			case "detail":
				var node Node
				node, err = store.GetNode(t.Context(), "a")
				nodes = []Node{node}
			case "fleet":
				nodes, err = store.ListNodes(t.Context())
			}
			if err != nil || len(nodes) == 0 || clock.calls.Load() != 1 {
				t.Fatalf("view=%s nodes=%#v err=%v clock calls=%d", view, nodes, err, clock.calls.Load())
			}
			for _, node := range nodes {
				if node.State != contract.NodeStale {
					t.Fatalf("view did not project the pinned boundary: %#v", node)
				}
			}
		})
	}
}

func TestNodeBackgroundReconcileRecordsEffectiveLiveness(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node": {"linux"}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	operator := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
	node := h.register(agent, "node")
	job := h.submit(operator, "expiry", []string{"linux"})
	claim, err := h.store.ClaimJob(t.Context(), "agent", node.NodeID, node.BootSessionID, contract.JobClassOneShot)
	if err != nil || claim == nil {
		t.Fatalf("claim=%#v err=%v", claim, err)
	}
	tx, err := h.store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	h.clock.Advance(DefaultNodeDeadAfter)
	operator.Timeout = 10 * time.Second
	status, _, body := h.do(operator, http.MethodGet, "/v1/nodes/node", nil)
	var effective Node
	if err := json.Unmarshal(body, &effective); err != nil || status != http.StatusOK {
		t.Fatalf("detail=%d %s err=%v", status, body, err)
	}
	if effective.State != contract.NodeDead || len(effective.ActiveAttempts) != 1 || !reflect.DeepEqual(effective.LastCondition, node.LastCondition) {
		t.Fatalf("effective view changed recorded consequences: %#v", effective)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	// Poll only durable facts after releasing the writer. No GET or explicit
	// Reconcile drives cleanup; the production Serve ticker records it.
	deadline := time.Now().Add(h.server.reconcileInterval + 10*time.Second)
	for {
		var recorded contract.NodeState
		var attemptState contract.AttemptState
		if err := h.store.db.QueryRow("SELECT state FROM nodes WHERE node_id='node'").Scan(&recorded); err != nil {
			t.Fatal(err)
		}
		if err := h.store.db.QueryRow("SELECT state FROM attempts WHERE attempt_id=?", claim.Lease.AttemptID).Scan(&attemptState); err != nil {
			t.Fatal(err)
		}
		if recorded == contract.NodeDead && attemptState == contract.AttemptLost {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("background cadence elapsed: node=%s attempt=%s job=%s", recorded, attemptState, job.JobID)
		}
		time.Sleep(10 * time.Millisecond)
	}
	status, _, body = h.do(operator, http.MethodGet, "/v1/nodes/node", nil)
	var recorded Node
	if err := json.Unmarshal(body, &recorded); err != nil || status != http.StatusOK || len(recorded.ActiveAttempts) != 0 || recorded.LastCondition == nil || recorded.LastCondition.Code != "node_dead" || !recorded.LastCondition.Since.Equal(h.clock.Now()) {
		t.Fatalf("recorded cleanup=%d %s err=%v", status, body, err)
	}
}
