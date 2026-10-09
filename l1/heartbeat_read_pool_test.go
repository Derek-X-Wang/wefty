package l1

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestHeartbeatDeliversDirectivesWithReadSnapshotPoolHeld(t *testing.T) {
	fixture := newRemovalStallHarness(t)
	h := fixture.h
	agent := h.client(fixture.agent)
	client := h.client(fixture.client)
	job := h.submit(client, "cancel-with-read-pool-held", []string{"service"})
	claim := claimClass(t, h, agent, fixture.node, contract.JobClassOneShot)
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs/"+job.JobID+"/cancel", nil)
	if status != http.StatusOK || decodeJob(t, body).Outcome != contract.JobOutcomeCanceled {
		t.Fatalf("cancel = %d %s", status, body)
	}

	// Hold every operator snapshot connection until the heartbeat has answered.
	// Explicit connections avoid snapshot deadlines releasing the pool mid-test.
	var held []*sql.Conn
	defer func() {
		for _, conn := range held {
			if err := conn.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	for range readSnapshotLimit {
		conn, err := h.store.readDB.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, conn)
	}
	if stats := h.store.readDB.Stats(); stats.InUse != readSnapshotLimit || stats.MaxOpenConnections != readSnapshotLimit {
		t.Fatalf("operator read pool is not saturated: %+v", stats)
	}
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/"+fixture.node.NodeID+"/heartbeat", heartbeatRequestForNode(fixture.node))
	if status != http.StatusOK {
		t.Fatalf("heartbeat with operator read pool held = %d %s", status, body)
	}
	var heartbeat HeartbeatResponse
	if err := json.Unmarshal(body, &heartbeat); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(heartbeat.RemovalDirectives, []RemovalDirective{fixture.directive}) {
		t.Fatalf("removal directives = %#v, want %#v", heartbeat.RemovalDirectives, fixture.directive)
	}
	wantCancels := []OneShotCancelDirective{{JobID: job.JobID, AttemptID: claim.Lease.AttemptID, FencingToken: claim.Lease.FencingToken}}
	if !reflect.DeepEqual(heartbeat.OneShotCancelDirectives, wantCancels) {
		t.Fatalf("cancel directives = %#v, want %#v", heartbeat.OneShotCancelDirectives, wantCancels)
	}
}
