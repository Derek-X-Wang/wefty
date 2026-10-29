package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

// #1332: stubbed CLI acceptance hid refusals in both real server handlers.
func TestExecSeatThroughRealL1AndL3QueuesBoundJob(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("WEFTY_SEAT_STATE_ROOT", filepath.Join(t.TempDir(), "seats"))
	h := newStorageCLIHarness(t, &l1.ComputerSettings{SeatLimit: 2, IdleAction: l1.ComputerIdleDelete, Network: l1.ComputerNetworkNone})
	ctx := h.ctx
	const identity = "fabric-storage-node"
	node, err := h.store.HeartbeatNodeWithCapabilityObservation(ctx, identity, h.node.NodeID, h.node.BootSessionID, contract.CapabilityObservation{
		Capabilities: map[string]bool{"kind:process": true, "kind:oci": true, "cgroup_v2": true, "computer": true, "computer:seat-requests": true},
		Revision:     2, ObservedAt: time.Date(2026, 8, 28, 20, 0, 0, 0, time.UTC), MissingCapabilities: []string{},
	}, l1.NodePolicy{Tags: []string{contract.StableNodeTagPrefix + h.node.NodeID}, MaxOneshotSlots: 1, MaxServiceSlots: 2})
	if err != nil {
		t.Fatal(err)
	}
	id := execSeatTestID
	if _, err = h.store.CreateSeatCall(ctx, "operator", l1.SeatCallInput{SeatID: id, NodeID: node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
		t.Fatal(err)
	}
	if _, err = h.store.BeginSeatApproval(ctx, identity, l1.SeatApprovalRequest{SeatID: id, NodeID: node.NodeID, BootSessionID: node.BootSessionID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none", AnswerWait: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if _, err = h.store.AnswerSeatApproval(ctx, identity, id, l1.SeatApprovalAnswer{NodeID: node.NodeID, BootSessionID: node.BootSessionID, Approved: true}); err != nil {
		t.Fatal(err)
	}
	if _, err = h.store.ReserveSeatRoom(ctx, identity, id, l1.SeatRoomReservationRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, ComputerID: h.computer.ComputerID, Place: 1, Room: l1.DefaultComputerRoomConfig(1 << 30)}); err != nil {
		t.Fatal(err)
	}
	if _, err = h.store.StartSeatRequest(ctx, identity, id, l1.SeatStartRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, ReadyWait: time.Minute}); err != nil {
		t.Fatal(err)
	}
	auth := l1.ComputerSeatAttemptRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, FencingToken: h.claim.Lease.FencingToken}
	list, err := h.store.GetComputerSeatListForAttempt(ctx, identity, h.computer.ComputerID, h.claim.Job.JobID, h.claim.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.store.ReportComputerSeatObservations(ctx, identity, h.computer.ComputerID, h.claim.Job.JobID, h.claim.Lease.AttemptID, l1.ComputerSeatObservationReport{
		NodeID: node.NodeID, BootSessionID: node.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 1, Sequence: 1,
		Seats: []l1.ComputerSeatObservation{{SeatID: id, Place: 1, State: l1.ComputerSeatReady}},
	}); err != nil {
		t.Fatal(err)
	}
	seat, err := h.store.GetSeatCall(ctx, "operator", id)
	if err != nil || seat.State != l1.SeatLive || seat.Credential == "" {
		t.Fatalf("live Seat=%+v: %v", seat.SeatRequest, err)
	}
	if _, err = storeSeatCredential(id, seat.Credential); err != nil {
		t.Fatal(err)
	}

	ledger := h.network.NewFabric(fabric.Identity{NodeID: "run-ledger", Tags: []string{l1.DefaultClientPrincipalTag}})
	store, err := l3.OpenStore(filepath.Join(t.TempDir(), "l3.sqlite"), l3.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	upstream, err := l3.NewL1Client(ledger, l3.DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(upstream.CloseIdleConnections)
	reconciler, err := l3.NewReconciler(store, upstream, l3.ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	// Drive reconciliation explicitly so the queued Job can be inspected before claim.
	server, err := l3.NewServer(ledger, store, l3.ServerConfig{Jobs: upstream, Logs: upstream})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := ledger.Listen("tcp", l3.DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, cancel := context.WithCancel(ctx)
	done := serveTestServer(serveCtx, func() error { return server.Serve(serveCtx, listener) })
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})

	argv := []string{"printf", "%s", "seat command"}
	var stdout, stderr bytes.Buffer
	if err = execute(ctx, h.clients, true, append([]string{"exec", "--seat", id, "--detach", "--"}, argv...), &stdout, &stderr); err != nil {
		t.Fatalf("CLI Seat submission: %v; stderr=%s", err, &stderr)
	}
	var accepted l3.RunAccepted
	if err = json.Unmarshal(stdout.Bytes(), &accepted); err != nil || accepted.RunID == "" {
		t.Fatalf("acceptance=%s: %v", &stdout, err)
	}
	if err = reconciler.ReconcileOnce(ctx); err != nil {
		t.Fatalf("dispatch through real L1: %v", err)
	}
	record, err := store.GetRun(ctx, accepted.RunID)
	if err != nil || record.SeatID != id || record.L1JobID == "" {
		t.Fatalf("Run=%+v: %v", record, err)
	}
	job, err := h.store.GetJob(ctx, record.L1JobID)
	if err != nil || job.State != contract.JobQueued {
		t.Fatalf("queued Job=%+v: %v", job, err)
	}
	params, present, err := contract.ParseSeatRunParams([]byte(job.Spec.Labels[contract.LabelRunParams]))
	if err != nil || !present || params.Binding.SeatID != id || params.Binding.ComputerID != h.computer.ComputerID || params.Binding.ComputerJobID != h.claim.Job.JobID || params.Binding.ComputerAttemptID != h.claim.Lease.AttemptID || params.Binding.NodeID != node.NodeID || !reflect.DeepEqual(params.SeatRun.Argv, argv) || !reflect.DeepEqual(job.Spec.RoutingTags, []string{contract.StableNodeTagPrefix + node.NodeID}) {
		t.Fatalf("Seat binding=%+v Job=%+v: %v", params, job, err)
	}
	if bytes.Contains(record.Params, []byte(seat.Credential)) {
		t.Fatal("Seat credential entered immutable params")
	}
	// Use the real node HTTP handler as well: routing and claim filters must admit it.
	agentClient := newAPIClient("L1", "l1", h.agentFabric, l3.DefaultL1Address)
	t.Cleanup(func() { agentClient.client.CloseIdleConnections() })
	var claim l1.Claim
	if err = agentClient.do(ctx, http.MethodPost, "/v1/agent/jobs/claim", l1.ClaimRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, Class: contract.JobClassOneShot}, nil, &claim, http.StatusOK); err != nil {
		t.Fatalf("claim Seat Run: %v", err)
	}
	if claim.Job.JobID != job.JobID {
		t.Fatalf("claimed Job=%s want %s", claim.Job.JobID, job.JobID)
	}
}
