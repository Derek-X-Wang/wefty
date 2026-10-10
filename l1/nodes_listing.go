package l1

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

type nodeListFilters struct {
	State         string `json:"state"`
	ClaimsEnabled *bool  `json:"claims_enabled"`
	Capability    string `json:"capability"`
}

func parseNodeListFilters(r *http.Request) (nodeListFilters, error) {
	query := r.URL.Query()
	for _, name := range []string{"state", "claims_enabled", "capability", "cursor", "limit"} {
		if values, present := query[name]; present && (len(values) != 1 || (values[0] == "" && name != "cursor" && name != "limit")) {
			return nodeListFilters{}, protocolError(contract.ErrorInvalidRequest, "%s must have one non-empty value", name)
		}
	}
	filters := nodeListFilters{State: query.Get("state"), Capability: query.Get("capability")}
	if filters.State != "" {
		if _, valid := contract.NodeTransitions[contract.NodeState(filters.State)]; !valid {
			return nodeListFilters{}, protocolError(contract.ErrorInvalidRequest, "state must be a node liveness state")
		}
	}
	if value := query.Get("claims_enabled"); value != "" {
		if value != "true" && value != "false" {
			return nodeListFilters{}, protocolError(contract.ErrorInvalidRequest, "claims_enabled must be true or false")
		}
		enabled := value == "true"
		filters.ClaimsEnabled = &enabled
	}
	return filters, nil
}

// Node identity binding is immutable. Together with node_id it gives the
// row-value seek a stable key independent of boots, liveness and intent.
// The insertion watermark excludes later nodes even if their ID sorts earlier.
type nodeListCursor struct {
	Version   int             `json:"v"`
	HighWater int64           `json:"high_water"`
	NodeID    string          `json:"node_id"`
	Identity  string          `json:"identity"`
	Filters   nodeListFilters `json:"filters"`
}

func decodeNodeListCursor(value string, filters nodeListFilters) (nodeListCursor, error) {
	payload, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return nodeListCursor{}, protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	var cursor nodeListCursor
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil {
		return nodeListCursor{}, protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nodeListCursor{}, protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	want, _ := json.Marshal(filters)
	got, _ := json.Marshal(cursor.Filters)
	if cursor.Version != 1 || cursor.HighWater < 1 || cursor.NodeID == "" || cursor.Identity == "" || string(want) != string(got) {
		return nodeListCursor{}, protocolError(contract.ErrorInvalidRequest, "cursor is invalid or does not match the listing filters")
	}
	return cursor, nil
}

func (s *Store) initializeNodeListing(ctx context.Context) error {
	// AUTOINCREMENT keeps insertion membership stable through VACUUM and row
	// deletion; registration of another boot does not create new membership.
	// Drop legacy recorded-state indexes on existing stores: effective liveness
	// filtering seeks the stable key instead of either state index.
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS node_listing_order (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT,
 node_id TEXT NOT NULL UNIQUE REFERENCES nodes(node_id) ON DELETE CASCADE
);
CREATE TRIGGER IF NOT EXISTS nodes_listing_insert AFTER INSERT ON nodes BEGIN
 INSERT INTO node_listing_order(node_id) VALUES(NEW.node_id);
END;
INSERT INTO node_listing_order(node_id)
 SELECT node_id FROM nodes WHERE NOT EXISTS
 (SELECT 1 FROM node_listing_order WHERE node_listing_order.node_id=nodes.node_id)
 ORDER BY node_id;
CREATE INDEX IF NOT EXISTS nodes_listing_order ON nodes(node_id, identity_node_id);
DROP INDEX IF EXISTS nodes_listing_state;
CREATE INDEX IF NOT EXISTS nodes_listing_claims ON nodes(claims_enabled, node_id, identity_node_id);
DROP INDEX IF EXISTS nodes_listing_state_claims;
`)
	if err != nil {
		return internalError(err, "initialize node listing")
	}
	return nil
}

func nodeListingQuery(filters nodeListFilters, cursor nodeListCursor, limit int, now time.Time, liveness nodeLiveness) (string, []any) {
	predicates := []string{"(nodes.node_id, nodes.identity_node_id) > (?, ?)", "EXISTS (SELECT 1 FROM node_listing_order WHERE node_listing_order.node_id=nodes.node_id AND sequence<=?)"}
	args := []any{cursor.NodeID, cursor.Identity, cursor.HighWater}
	index := "nodes_listing_order"
	if filters.State != "" {
		// Apply the same liveness thresholds as projection, before LIMIT. State
		// indexes cannot order a mix of recorded states, so seek the stable key.
		predicates = append(predicates, `(CASE
 WHEN nodes.state IN ('alive', 'stale', 'draining') AND nodes.last_heartbeat_ns<=? THEN 'dead'
 WHEN nodes.state='alive' AND nodes.last_heartbeat_ns<=? THEN 'stale'
 ELSE nodes.state END)=?`)
		args = append(args, now.Add(-liveness.deadAfter).UnixNano(), now.Add(-liveness.staleAfter).UnixNano(), filters.State)
	}
	if filters.ClaimsEnabled != nil {
		predicates = append(predicates, "nodes.claims_enabled=?")
		args = append(args, *filters.ClaimsEnabled)
		index = "nodes_listing_claims"
	}
	if filters.Capability != "" {
		// json_each compares literal keys (including dots/quotes), and only an
		// advertised JSON true counts. False and missing capabilities do not match.
		predicates = append(predicates, "EXISTS (SELECT 1 FROM json_each(nodes.capabilities_json) WHERE key=? AND type='true')")
		args = append(args, filters.Capability)
	}
	args = append(args, limit+1)
	return "SELECT nodes.node_id, nodes.identity_node_id FROM nodes INDEXED BY " + index + " WHERE " + strings.Join(predicates, " AND ") + " ORDER BY nodes.node_id, nodes.identity_node_id LIMIT ?", args
}

func (s *Store) listNodesPage(ctx context.Context, filters nodeListFilters, cursorValue string, limit int) (NodeList, error) {
	if limit < 1 || limit > MaxJobPageLimit {
		return NodeList{}, protocolError(contract.ErrorInvalidRequest, "limit must be between 1 and %d", MaxJobPageLimit)
	}
	cursor := nodeListCursor{Version: 1, Filters: filters}
	if cursorValue != "" {
		var err error
		cursor, err = decodeNodeListCursor(cursorValue, filters)
		if err != nil {
			return NodeList{}, err
		}
	}
	var page NodeList
	err := s.withReadSnapshot(ctx, nil, func(ctx context.Context, reads readModel) error {
		var err error
		page, err = reads.nodePage(ctx, filters, cursor, limit, s.nodeLiveness())
		return err
	})
	return page, err
}

func (r *databaseReads) nodePage(ctx context.Context, filters nodeListFilters, cursor nodeListCursor, limit int, liveness nodeLiveness) (NodeList, error) {
	if cursor.HighWater == 0 {
		if err := r.q.QueryRowContext(ctx, "SELECT COALESCE(MAX(sequence),0) FROM node_listing_order").Scan(&cursor.HighWater); err != nil {
			return NodeList{}, internalError(err, "read node insertion watermark")
		}
	}
	query, args := nodeListingQuery(filters, cursor, limit, r.now(), liveness)
	rows, err := r.q.QueryContext(ctx, query, args...)
	if err != nil {
		return NodeList{}, internalError(err, "list node page IDs")
	}
	type key struct{ id, identity string }
	keys := make([]key, 0, limit+1)
	for rows.Next() {
		var item key
		if err := rows.Scan(&item.id, &item.identity); err != nil {
			rows.Close()
			return NodeList{}, internalError(err, "scan node page ID")
		}
		keys = append(keys, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return NodeList{}, internalError(err, "iterate node page IDs")
	}
	more := len(keys) > limit
	if more {
		keys = keys[:limit]
	}
	page := NodeList{Nodes: []Node{}}
	for i, item := range keys {
		node, err := r.node(ctx, item.id)
		if err != nil {
			return NodeList{}, internalError(err, "read paged node")
		}
		page.Nodes = append(page.Nodes, liveness.project(node, r.now()))
		// Finish at least one node before yielding. Continue from the last
		// returned key, including when the cutoff shortens a maximum page.
		if r.pageCutoffReached() {
			more = more || i+1 < len(keys)
			break
		}
	}
	if more {
		last := keys[len(page.Nodes)-1]
		cursor.NodeID, cursor.Identity = last.id, last.identity
		payload, _ := json.Marshal(cursor)
		page.NextCursor = base64.RawURLEncoding.EncodeToString(payload)
	}
	return page, nil
}
