package l1

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestColdStartClaimPollHintsWithoutLeasing(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	node := registerCapabilityNode(t, h, "cold", map[string]bool{"kind:process": true})
	spec := capabilityJobSpec("cold-oci", contract.JobKindOCI, contract.JobClassOneShot, "", nil)
	spec.RoutingTags = []string{"cold-only"}
	job, _, err := h.store.CreateJobAs(t.Context(), spec, runLedgerOrigin)
	if err != nil {
		t.Fatal(err)
	}
	caller := h.client(fabric.Identity{NodeID: "fabric-cold", Tags: []string{DefaultAgentPrincipalTag}})
	poll := func(want bool) {
		t.Helper()
		status, _, body := h.do(caller, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, Class: contract.JobClassOneShot, AcceptQueuedOCIHint: true})
		var reply map[string]any
		if status == 200 {
			if err := json.Unmarshal(body, &reply); err != nil {
				t.Fatal(err)
			}
		}
		if want && (status != 200 || reply["queued_oci_work"] != true || reply["lease"] != nil) {
			t.Fatalf("hint reply: %d %s", status, body)
		}
		if !want && status != 204 {
			t.Fatalf("unmatched work hinted: %d %s", status, body)
		}
	}
	poll(false)
	if _, err := h.store.db.Exec(`INSERT INTO node_tags(node_id,tag) VALUES(?,?)`, node.NodeID, "cold-only"); err != nil {
		t.Fatal(err)
	}
	poll(true)
	value, err := h.store.GetJob(t.Context(), job.JobID)
	if err != nil || value.State != contract.JobQueued || value.CurrentAttemptID != "" {
		t.Fatalf("hint claimed work: %+v %v", value, err)
	}
	process, _, err := h.store.CreateJob(t.Context(), validJobSpec("cold-process", nil))
	if err != nil {
		t.Fatal(err)
	}
	status, _, body := h.do(caller, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, Class: contract.JobClassOneShot, AcceptQueuedOCIHint: true})
	var won ClaimPollResponse
	if err := json.Unmarshal(body, &won); err != nil {
		t.Fatal(err)
	}
	if status != 200 || won.Claim == nil || won.Job.JobID != process.JobID || !won.QueuedOCIWork || won.AttemptToken == "" || won.Lease.AttemptID == "" {
		t.Fatalf("process claim with hint: %d %s", status, body)
	}
}

// RM-4: approval owns the budget, including a delayed first advance and wait replay.
func TestColdStartApprovalDeadlineAndReply(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	input := SeatCallInput{SeatID: "seat_00000000000000000000000000001144", NodeID: f.node.NodeID, Folder: "/workspace", Form: "any", Network: "none"}
	poll := SeatCallPoll{SeatNodeRequest: SeatNodeRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID}, Room: DefaultComputerRoomConfig(3 << 30), HostEnabled: true, AnswerWait: time.Minute}
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", input); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll); err != nil {
		t.Fatal(err)
	}
	// The Mac has withdrawn OCI to stop. Approval must still publish a Computer.
	reg := contract.NodeRegistration{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, OS: "darwin", Architecture: "arm64", AgentVersion: "test", RootInstanceID: "root", Capabilities: map[string]bool{CapabilitySeatRequests: true}, CapabilityRevision: 2, CapabilityObservedAt: f.clock.Now(), MissingCapabilities: []string{"kind:oci"}, CapabilityReasonCode: contract.CapabilityReasonHostVMIdle}
	if _, err := f.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "fabric-computer-node"}, reg, NodePolicy{Tags: []string{contract.StableNodeTagPrefix + f.node.NodeID}, MaxServiceSlots: 4}, true); err != nil {
		t.Fatal(err)
	}
	approvedAt := f.clock.Now()
	if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", input.SeatID, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Approved: true}); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(20 * time.Second)
	value, err := f.store.AdvanceSeatCall(t.Context(), "fabric-computer-node", input.SeatID, poll)
	if err != nil || value.State != SeatStarting || !value.Deadline.Equal(approvedAt.Add(3*time.Minute)) {
		t.Fatalf("approval budget=%+v %v", value, err)
	}
	result, err := f.store.GetSeatCall(t.Context(), "cli", input.SeatID)
	data, _ := json.Marshal(result)
	if err != nil || !strings.Contains(string(data), `"host_vm_starting":true`) {
		t.Fatalf("cold reply=%s %v", data, err)
	}
	// A failed space/start probe supplies the public node reason, not raw Lima
	// diagnostics. Waiting and an L1 restart must retain the approval deadline.
	hb := heartbeatRequestForBoot(f.node, f.node.BootSessionID)
	hb.CapabilityRevision = 3
	hb.Capabilities = reg.Capabilities
	hb.MissingCapabilities = reg.MissingCapabilities
	hb.CapabilityReasonCode = contract.CapabilityReasonPrerequisiteMissing
	if _, err := f.store.HeartbeatNodeWithCapabilityObservation(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, heartbeatCapabilityObservation(hb), NodePolicy{Tags: []string{contract.StableNodeTagPrefix + f.node.NodeID}, MaxServiceSlots: 4}); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	f.clock.Advance(160*time.Second - time.Nanosecond)
	result, err = f.store.GetSeatCall(t.Context(), "cli", input.SeatID)
	if err != nil || result.State != SeatStarting || !result.Deadline.Equal(value.Deadline) {
		t.Fatalf("deadline moved or fired early: %+v %v", result, err)
	}
	f.clock.Advance(time.Nanosecond)
	result, err = f.store.GetSeatCall(t.Context(), "cli", input.SeatID)
	if err != nil || result.State != SeatRefused || !strings.Contains(result.Reason, "prerequisite_missing") || !strings.Contains(result.Reason, "wefty node doctor") {
		t.Fatalf("deadline refusal=%+v %v", result, err)
	}
	page, err := f.store.GetSeatLogs(t.Context(), "cli", input.SeatID, "", "", 100)
	if err != nil || len(page.Events) == 0 || page.Events[len(page.Events)-1].Kind != SeatEventRefused {
		t.Fatalf("expiry log=%+v err=%v", page, err)
	}
	// Never-live refused history ages from the original request, not refusal.
	f.clock.Advance(DefaultComputerTakeoverAuditRetentionAge - 3*time.Minute - time.Nanosecond)
	swept, err := f.store.Reconcile(t.Context())
	if err != nil || swept.PrunedSeatLogs != 0 {
		t.Fatalf("early log sweep=%+v err=%v", swept, err)
	}
	f.clock.Advance(time.Nanosecond)
	swept, err = f.store.Reconcile(t.Context())
	if err != nil || swept.PrunedSeatLogs != 1 {
		t.Fatalf("refused log sweep=%+v err=%v", swept, err)
	}
	if _, err := f.store.GetSeatLogs(t.Context(), "cli", input.SeatID, "", "", 100); errorCode(err) != contract.ErrorNotFound {
		t.Fatalf("expired history survived: %v", err)
	}
}

func TestColdStartIdleComputerIsStartingNotUnschedulable(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	hb := heartbeatRequestForBoot(f.node, f.node.BootSessionID)
	hb.CapabilityRevision = 2
	hb.Capabilities = map[string]bool{CapabilitySeatRequests: true}
	hb.MissingCapabilities = []string{"kind:oci", "computer", "cgroup_v2"}
	hb.CapabilityReasonCode = contract.CapabilityReasonHostVMIdle
	if _, err := f.store.HeartbeatNodeWithCapabilityObservation(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, heartbeatCapabilityObservation(hb), NodePolicy{Tags: []string{contract.StableNodeTagPrefix + f.node.NodeID}, MaxServiceSlots: 4}); err != nil {
		t.Fatal(err)
	}
	c, err := f.store.GetComputer(t.Context(), f.computer.ComputerID)
	if err != nil || c.CurrentJob.Status != "host VM starting" || c.CurrentJob.UnschedulableReason != "" {
		t.Fatalf("idle queued Computer=%+v %v", c.CurrentJob, err)
	}
}

// HV-4 / RM-4 with fakes: stopped runtime, demand hint, ready runtime, ready Seat.
func TestColdStartRecoveredRuntimeDeliversSameSeat(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	var registerErr error
	f.node, registerErr = f.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "fabric-computer-node"}, contract.NodeRegistration{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, RootInstanceID: "root", OS: "darwin", Architecture: "arm64", AgentVersion: "test", Capabilities: f.node.Capabilities, CapabilityRevision: 2, CapabilityObservedAt: f.clock.Now(), MissingCapabilities: []string{}}, NodePolicy{Tags: []string{contract.StableNodeTagPrefix + f.node.NodeID}, MaxServiceSlots: 4}, true)
	if registerErr != nil {
		t.Fatal(registerErr)
	}
	id := "seat_00000000000000000000000000001145"
	poll := SeatCallPoll{SeatNodeRequest: SeatNodeRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID}, Room: DefaultComputerRoomConfig(3 << 30), HostEnabled: true, AnswerWait: time.Minute}
	input := SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Folder: "/workspace", Form: "any", Network: "none"}
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", input); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll); err != nil {
		t.Fatal(err)
	}
	approvedAt := f.clock.Now()
	if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", id, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Approved: true}); err != nil {
		t.Fatal(err)
	}
	hb := heartbeatRequestForBoot(f.node, f.node.BootSessionID)
	hb.CapabilityRevision = 3
	hb.Capabilities = map[string]bool{CapabilitySeatRequests: true}
	hb.MissingCapabilities = []string{"kind:oci", "computer", "cgroup_v2"}
	hb.CapabilityReasonCode = contract.CapabilityReasonHostVMIdle
	policy := NodePolicy{Tags: []string{contract.StableNodeTagPrefix + f.node.NodeID}, MaxServiceSlots: 4}
	if _, err := f.store.HeartbeatNodeWithCapabilityObservation(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, heartbeatCapabilityObservation(hb), policy); err != nil {
		t.Fatal(err)
	}
	value, err := f.store.AdvanceSeatCall(t.Context(), "fabric-computer-node", id, poll)
	if err != nil || value.State != SeatStarting || !value.Deadline.Equal(approvedAt.Add(3*time.Minute)) {
		t.Fatalf("start after idle withdrawal=%+v %v", value, err)
	}
	f.reopen(t)
	hint, err := f.store.ClaimJobPoll(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || !hint.QueuedOCIWork || hint.Claim != nil {
		t.Fatalf("stopped runtime hint=%+v %v", hint, err)
	}
	pending, err := f.store.GetSeatCall(t.Context(), "cli", id)
	if err != nil || !pending.HostVMStarting {
		t.Fatalf("Mac cold reply=%+v %v", pending, err)
	}
	// Finish shutdown and cold start on the injected timeline, then publish the
	// normal ready capability. No helper or host VM is started by this test.
	f.clock.Advance(50 * time.Second)
	hb = heartbeatRequestForBoot(f.node, f.node.BootSessionID)
	hb.CapabilityRevision = 4
	if _, err := f.store.HeartbeatNodeWithCapabilityObservation(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, heartbeatCapabilityObservation(hb), policy); err != nil {
		t.Fatal(err)
	}
	ready, err := f.store.ClaimJobPoll(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || ready.Claim == nil || ready.QueuedOCIWork {
		t.Fatalf("recovered claim=%+v %v", ready, err)
	}
	auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: ready.Lease.FencingToken}
	list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", value.ComputerID, ready.Job.JobID, ready.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	report := ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 1, Sequence: 1, Seats: []ComputerSeatObservation{{SeatID: id, Place: value.Place, State: ComputerSeatReady}}}
	if _, err := f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", value.ComputerID, ready.Job.JobID, ready.Lease.AttemptID, report); err != nil {
		t.Fatal(err)
	}
	result, err := f.store.GetSeatCall(t.Context(), "cli", id)
	if err != nil || result.State != SeatLive || result.SeatID != id || result.Credential == "" || f.clock.Now().Sub(approvedAt) >= 3*time.Minute {
		t.Fatalf("cold Seat=%+v %v", result, err)
	}
}

// HV-5: an off switch is a refusal before the node offers an owner dialog.
func TestColdStartOffRefusesBeforeDialog(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	input := SeatCallInput{SeatID: "seat_00000000000000000000000000001146", NodeID: f.node.NodeID, Folder: "/workspace", Form: "any", Network: "none"}
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", input); err != nil {
		t.Fatal(err)
	}
	poll := SeatCallPoll{SeatNodeRequest: SeatNodeRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID}, Room: DefaultComputerRoomConfig(3 << 30), HostEnabled: false, AnswerWait: time.Minute}
	list, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll)
	if err != nil || len(list.Requests) != 0 {
		t.Fatalf("off request offered a dialog: %+v %v", list, err)
	}
	result, err := f.store.GetSeatCall(t.Context(), "cli", input.SeatID)
	if err != nil || result.State != SeatRefused || result.Reason != "the host VM is off" || result.ComputerID != "" || result.HostVMStarting {
		t.Fatalf("off refusal=%+v %v", result, err)
	}
}

func TestColdStartClaimPollNegotiatesHintReader(t *testing.T) {
	for _, tc := range []struct {
		name    string
		support *bool
		process bool
	}{
		{name: "old-empty"},
		{name: "old-process", process: true},
		{name: "explicitly-disabled", support: new(false)},
		{name: "new-empty", support: new(true)},
		{name: "new-process", support: new(true), process: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newIntegrationHarness(t, nil)
			node := registerCapabilityNode(t, h, "cold", map[string]bool{"kind:process": true})
			if _, _, err := h.store.CreateJobAs(t.Context(), capabilityJobSpec("cold-oci", contract.JobKindOCI, contract.JobClassOneShot, "", nil), runLedgerOrigin); err != nil {
				t.Fatal(err)
			}
			var processID string
			if tc.process {
				job, _, err := h.store.CreateJob(t.Context(), validJobSpec("cold-process", nil))
				if err != nil {
					t.Fatal(err)
				}
				processID = job.JobID
			}
			input := map[string]any{"node_id": node.NodeID, "boot_session_id": node.BootSessionID, "class": contract.JobClassOneShot}
			if tc.support != nil {
				input["accept_queued_oci_hint"] = *tc.support
			}
			caller := h.client(fabric.Identity{NodeID: "fabric-cold", Tags: []string{DefaultAgentPrincipalTag}})
			status, _, body := h.do(caller, http.MethodPost, "/v1/agent/jobs/claim", input)
			accepts := tc.support != nil && *tc.support
			if !accepts && !tc.process {
				if status != 204 || len(body) != 0 {
					t.Fatalf("old reader received new reply: %d %s", status, body)
				}
				return
			}
			if status != 200 {
				t.Fatalf("claim poll: %d %s", status, body)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(body, &fields); err != nil {
				t.Fatal(err)
			}
			_, hintField := fields["queued_oci_work"]
			if hintField != accepts {
				t.Fatalf("hint negotiated=%t reply=%s", accepts, body)
			}
			var reply ClaimPollResponse
			if err := json.Unmarshal(body, &reply); err != nil {
				t.Fatal(err)
			}
			if reply.QueuedOCIWork != accepts || (reply.Claim != nil) != tc.process {
				t.Fatalf("reply=%s", body)
			}
			if tc.process && (reply.Job.JobID != processID || reply.Lease.AttemptID == "" || reply.AttemptToken == "") {
				t.Fatalf("process claim lost authority: %s", body)
			}
			if !tc.process && (fields["job"] != nil || fields["lease"] != nil || fields["attempt_token"] != nil) {
				t.Fatalf("hint minted authority: %s", body)
			}
		})
	}
}

func TestColdStartReplyRequiresIdleNode(t *testing.T) {
	for _, reason := range []contract.CapabilityReasonCode{contract.CapabilityReasonHostVMIdle, contract.CapabilityReasonPrerequisiteMissing, contract.CapabilityReasonLimaStopped, contract.CapabilityReasonHelperUnreachable} {
		t.Run(string(reason), func(t *testing.T) {
			f := newSeatLifecycleFixture(t)
			id := "seat_00000000000000000000000000001244"
			if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
				t.Fatal(err)
			}
			reg := contract.NodeRegistration{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, RootInstanceID: "root", OS: "darwin", Architecture: "arm64", AgentVersion: "test", Capabilities: map[string]bool{CapabilitySeatRequests: true}, CapabilityRevision: 2, CapabilityObservedAt: f.clock.Now(), MissingCapabilities: []string{"kind:oci"}, CapabilityReasonCode: reason}
			if _, err := f.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "fabric-computer-node"}, reg, NodePolicy{Tags: []string{contract.StableNodeTagPrefix + f.node.NodeID}, MaxServiceSlots: 4}, true); err != nil {
				t.Fatal(err)
			}
			f.request(t, 0x1244, true)
			result, err := f.store.GetSeatCall(t.Context(), "cli", id)
			if err != nil || result.HostVMStarting != (reason == contract.CapabilityReasonHostVMIdle) {
				t.Fatalf("reply=%+v err=%v", result, err)
			}
			if reason != contract.CapabilityReasonHostVMIdle && !result.Deadline.IsZero() {
				t.Fatalf("broken warm runtime acquired cold deadline: %+v", result)
			}
		})
	}
}

func TestColdStartHintRequiresRestorableCapabilities(t *testing.T) {
	for _, handler := range []string{"runc", "cannot-run"} {
		t.Run(handler, func(t *testing.T) {
			h := newIntegrationHarness(t, nil)
			node := registerCapabilityNode(t, h, "cold", map[string]bool{"kind:process": true, "kind:oci": true, "runtime_handler:runc": true})
			hb := heartbeatRequestForBoot(node, node.BootSessionID)
			hb.CapabilityRevision = node.CapabilityRevision + 1
			hb.Capabilities = map[string]bool{"kind:process": true}
			hb.MissingCapabilities = []string{"kind:oci", "runtime_handler:runc"}
			hb.CapabilityReasonCode = contract.CapabilityReasonHostVMIdle
			if _, err := h.store.HeartbeatNodeWithCapabilityObservation(t.Context(), "fabric-cold", node.NodeID, node.BootSessionID, heartbeatCapabilityObservation(hb), NodePolicy{MaxOneshotSlots: 1}); err != nil {
				t.Fatal(err)
			}
			spec := capabilityJobSpec("cold-capability", contract.JobKindOCI, contract.JobClassOneShot, handler, nil)
			job, _, err := h.store.CreateJobAs(t.Context(), spec, runLedgerOrigin)
			if err != nil {
				t.Fatal(err)
			}
			hint, err := h.store.ClaimJobPoll(t.Context(), "fabric-cold", node.NodeID, node.BootSessionID, contract.JobClassOneShot)
			want := handler == "runc"
			if err != nil || hint.QueuedOCIWork != want {
				t.Errorf("hint=%+v err=%v want=%v", hint, err, want)
			}
			caller := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
			status, _, body := h.do(caller, http.MethodGet, "/v1/jobs/"+job.JobID, nil)
			var got Job
			err = json.Unmarshal(body, &got)
			if err != nil || status != 200 || (got.Status == "host VM starting") != want {
				t.Fatalf("status=%s reason=%s HTTP=%d err=%v", got.Status, got.UnschedulableReason, status, err)
			}
		})
	}
}
