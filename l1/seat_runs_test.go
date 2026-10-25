package l1

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/upgradestate"
)

func TestSeatRunBindingRefusesEndedSeatAndAttemptReuse(t *testing.T) {
	f, id, secret, auth := liveSeatForCopy(t)
	binding, err := f.store.ResolveSeatRun(t.Context(), id, secret)
	if err != nil || binding.SeatID != id || binding.ComputerAttemptID != auth.AttemptID {
		t.Fatalf("binding=%+v %v", binding, err)
	}
	p := contract.SeatRunParams{Version: 1, SeatRun: contract.SeatRunInput{SeatID: id, Argv: []string{"env"}}, Binding: binding}
	raw, _ := json.Marshal(p)
	spec := computerCapabilityJobSpec("seat-run")
	spec.Kind = contract.JobKindProcess
	spec.Class = contract.JobClassOneShot
	spec.Execution = contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/false"}, Argv: []string{"false"}, WorkingDirectory: "/tmp"}
	spec.RoutingTags = []string{contract.StableNodeTagPrefix + binding.NodeID}
	spec.Labels = map[string]string{"run_id": "run_00000000000000000000000000000001", contract.LabelRunParams: string(raw)}
	if _, _, err = f.store.CreateJobAs(t.Context(), spec, JobOrigin{OriginatingSubmitter: "cli", SubmittedByRunLedger: true}); err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.EndSeatRequest(t.Context(), "fabric-computer-node", id, SeatEndRequest{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.ResolveSeatRun(t.Context(), id, secret); errorCode(err) != contract.ErrorCode("seat_ended") {
		t.Fatalf("ended binding=%v", err)
	}
	spec.DispatchKey = "later-seat-run"
	if _, _, err = f.store.CreateJobAs(t.Context(), spec, JobOrigin{OriginatingSubmitter: "cli", SubmittedByRunLedger: true}); err == nil {
		t.Fatal("old Seat binding admitted after release")
	}
}

func startSeatRunFixture(t *testing.T) (*seatLifecycleFixture, string, string, Claim, SeatRunControlRequest) {
	t.Helper()
	f, id, secret, _ := liveSeatForRun(t)
	binding, err := f.store.ResolveSeatRun(t.Context(), id, secret)
	if err != nil {
		t.Fatal(err)
	}
	p := contract.SeatRunParams{Version: 1, SeatRun: contract.SeatRunInput{SeatID: id, Argv: []string{"env"}, MaxRuntimeSeconds: 3600}, Binding: binding}
	raw, _ := json.Marshal(p)
	spec := contract.JobSpec{SchemaVersion: 1, DispatchKey: "seat-run-command", Kind: contract.JobKindProcess, Class: contract.JobClassOneShot, RoutingTags: []string{contract.StableNodeTagPrefix + f.node.NodeID}, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/false"}, Argv: []string{"false"}, WorkingDirectory: "/tmp"}, Labels: map[string]string{"run_id": "run_00000000000000000000000000000001", contract.LabelRunParams: string(raw)}}
	job, _, err := f.store.CreateJobAs(t.Context(), spec, JobOrigin{OriginatingSubmitter: "cli", SubmittedByRunLedger: true})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassOneShot)
	if err != nil || claim == nil {
		t.Fatalf("claim=%+v %v job=%s", claim, err, job.JobID)
	}
	input := SeatRunControlRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken, Phase: "start"}
	if _, err = f.store.ControlSeatRun(t.Context(), "fabric-computer-node", job.JobID, claim.Lease.AttemptID, input); err != nil {
		t.Fatal(err)
	}
	return f, id, secret, *claim, input
}
func TestSeatRunFinishRejectsExitCodeAtDeadline(t *testing.T) {
	f, id, secret, claim, input := startSeatRunFixture(t)
	zero := 0
	input.Phase = "finish"
	input.Result = &ProcessResult{ExitCode: &zero, LimitReached: contract.LimitReachedMaxRuntime}
	if _, err := f.store.ControlSeatRun(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID, input); err == nil {
		t.Fatal("deadline accepted a process exit code")
	}
	input.Result = &ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent, LimitReached: contract.LimitReachedMaxRuntime}
	for range 2 {
		if _, err := f.store.ControlSeatRun(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID, input); err != nil {
			t.Fatal(err)
		}
	}
	page, err := f.store.GetSeatLogs(t.Context(), "cli", id, secret, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	starts, ends := 0, 0
	for _, event := range page.Events {
		if event.RunID != claim.Job.Spec.Labels["run_id"] {
			continue
		}
		if event.Kind == SeatEventCommandStarted {
			starts++
		}
		if event.Kind == SeatEventCommandFinished {
			ends++
			if event.ExitCode != nil || event.Reason != "max_runtime" || !strings.Contains(event.Note, "longer --max-runtime") {
				t.Fatalf("deadline event=%+v", event)
			}
		}
	}
	if starts != 1 || ends != 1 {
		t.Fatalf("start=%d end=%d", starts, ends)
	}
	f.clock.Advance(29 * time.Minute)
	if err = f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	value, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || value.State != SeatLive {
		t.Fatalf("Seat after deadline=%+v %v", value, err)
	}
	f.clock.Advance(time.Minute)
	if err = f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	value, err = f.store.GetSeatRequest(t.Context(), id)
	if err != nil || value.State != SeatEnding {
		t.Fatalf("idle Seat=%+v %v", value, err)
	}
}

func liveSeatForRun(t *testing.T) (*seatLifecycleFixture, string, string, SeatCopyAuthorityInput) {
	t.Helper()
	f := newSeatLifecycleFixture(t)
	var registrationErr error
	f.node, registrationErr = f.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "fabric-computer-node"}, contract.NodeRegistration{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, RootInstanceID: "root", OS: "linux", Architecture: "amd64", AgentVersion: "test", Capabilities: map[string]bool{"kind:process": true, "kind:oci": true, "cgroup_v2": true, "computer": true, "computer:seat-requests": true}, CapabilityRevision: 2, CapabilityObservedAt: f.clock.Now(), MissingCapabilities: []string{}}, NodePolicy{Tags: []string{contract.StableNodeTagPrefix + f.node.NodeID}, MaxServiceSlots: 4, MaxOneshotSlots: 2}, true)
	if registrationErr != nil {
		t.Fatal(registrationErr)
	}

	id := "seat_00000000000000000000000000000001"
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
		t.Fatal(err)
	}
	f.request(t, 1, true)
	f.reserve(t, id, f.computer, 1, DefaultComputerRoomConfig(1<<30))
	if _, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ReadyWait: time.Minute}); err != nil {
		t.Fatal(err)
	}
	claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim=%v %v", claim, err)
	}
	auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken}
	list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 1, Sequence: 1, Seats: []ComputerSeatObservation{{SeatID: id, Place: 1, State: ComputerSeatReady}}}); err != nil {
		t.Fatal(err)
	}
	result, err := f.store.GetSeatCall(t.Context(), "cli", id)
	if err != nil || result.Credential == "" {
		t.Fatalf("credential: %+v %v", result, err)
	}
	return f, id, result.Credential, SeatCopyAuthorityInput{ComputerSeatAttemptRequest: auth, ComputerID: f.computer.ComputerID, JobID: claim.Job.JobID, AttemptID: claim.Lease.AttemptID, Credential: result.Credential}
}
func TestSeatRunLateCancelAfterPlaceReuseLeavesLaterRunLive(t *testing.T) {
	f, old, secret, first, input := startSeatRunFixture(t)
	binding, err := f.store.ResolveSeatRun(t.Context(), old, secret)
	if err != nil {
		t.Fatal(err)
	}
	pollInput := input
	pollInput.Phase = "poll"
	currentCommand, err := f.store.ControlSeatRun(t.Context(), "fabric-computer-node", first.Job.JobID, first.Lease.AttemptID, pollInput)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.EndSeatRequest(t.Context(), "fabric-computer-node", old, SeatEndRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	// Confirm cleanup before reusing the same place, exactly as the image does.
	computerAuthority := func(generation, sequence uint64, seats []ComputerSeatObservation) {
		t.Helper()
		// The start reply is node-only and contains the Computer's exact fence.
		fence := currentCommand.FencingToken
		auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: fence}
		list, e := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", binding.ComputerID, binding.ComputerJobID, binding.ComputerAttemptID, auth)
		if e != nil {
			t.Fatal(e)
		}
		_, e = f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", binding.ComputerID, binding.ComputerJobID, binding.ComputerAttemptID, ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: generation, Sequence: int64(sequence), Seats: seats})
		if e != nil {
			t.Fatal(e)
		}
	}
	computerAuthority(2, 2, []ComputerSeatObservation{{SeatID: old, Place: 1, State: ComputerSeatCleaned, CleanupComplete: true}})
	nextID := "seat_00000000000000000000000000000002"
	if _, err = f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: nextID, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
		t.Fatal(err)
	}
	next := f.request(t, 2, true)
	f.reserve(t, next, f.computer, 1, DefaultComputerRoomConfig(1<<30))
	if _, err = f.store.StartSeatRequest(t.Context(), "fabric-computer-node", next, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ReadyWait: time.Minute}); err != nil {
		t.Fatal(err)
	}
	computerAuthority(3, 3, []ComputerSeatObservation{{SeatID: next, Place: 1, State: ComputerSeatReady}})
	result, err := f.store.GetSeatCall(t.Context(), "cli", next)
	if err != nil {
		t.Fatal(err)
	}
	laterBinding, err := f.store.ResolveSeatRun(t.Context(), next, result.Credential)
	if err != nil {
		t.Fatal(err)
	}
	p := contract.SeatRunParams{Version: 1, SeatRun: contract.SeatRunInput{SeatID: next, Argv: []string{"sleep", "1000"}}, Binding: laterBinding}
	raw, _ := json.Marshal(p)
	spec := first.Job.Spec
	spec.DispatchKey = "later-command"
	spec.Labels = map[string]string{"run_id": "run_00000000000000000000000000000002", contract.LabelRunParams: string(raw)}
	job, _, err := f.store.CreateJobAs(t.Context(), spec, JobOrigin{OriginatingSubmitter: "cli", SubmittedByRunLedger: true})
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassOneShot)
	if err != nil || second == nil {
		t.Fatalf("second claim=%+v %v", second, err)
	}
	later := input
	later.FencingToken = second.Lease.FencingToken
	if _, err = f.store.ControlSeatRun(t.Context(), "fabric-computer-node", job.JobID, second.Lease.AttemptID, later); err != nil {
		t.Fatal(err)
	}
	input.Phase = "poll"
	oldPoll, err := f.store.ControlSeatRun(t.Context(), "fabric-computer-node", first.Job.JobID, first.Lease.AttemptID, input)
	if err != nil || !oldPoll.Cancel {
		t.Fatalf("old poll=%+v %v", oldPoll, err)
	}
	if _, err = f.store.ControlSeatRun(t.Context(), "fabric-computer-node", job.JobID, first.Lease.AttemptID, input); err == nil {
		t.Fatal("old Attempt controlled later Run")
	}
	input.Phase = "finish"
	input.Result = &ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}
	if _, err = f.store.ControlSeatRun(t.Context(), "fabric-computer-node", first.Job.JobID, first.Lease.AttemptID, input); err != nil {
		t.Fatal(err)
	}
	later.Phase = "poll"
	newPoll, err := f.store.ControlSeatRun(t.Context(), "fabric-computer-node", job.JobID, second.Lease.AttemptID, later)
	if err != nil || newPoll.Cancel {
		t.Fatalf("new poll=%+v %v", newPoll, err)
	}
	f.clock.Advance(31 * time.Minute)
	if err = f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	value, err := f.store.GetSeatRequest(t.Context(), next)
	if err != nil || value.State != SeatLive {
		t.Fatalf("later Seat=%+v %v", value, err)
	}
}

func TestSeatRunHoldRecoversAfterLostFinishReceipt(t *testing.T) {
	f, id, _, claim, _ := startSeatRunFixture(t)
	code := 0
	if _, err := f.store.CompleteAttempt(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID, CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "ended-without-seat-receipt", Result: ProcessResult{ExitCode: &code}}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(30 * time.Minute)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	value, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || value.State != SeatEnding {
		t.Fatalf("completed Run kept its Seat alive: %+v %v", value, err)
	}
}

func TestSeatRunResolveCountsAsOneTimeSeatCall(t *testing.T) {
	f, id, secret, _ := liveSeatForRun(t)
	f.clock.Advance(29 * time.Minute)
	if _, err := f.store.ResolveSeatRun(t.Context(), id, secret); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(time.Minute)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	value, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || value.State != SeatLive {
		t.Fatalf("command submission did not renew Seat: %+v %v", value, err)
	}
}

func TestSeatRunClusterDeadlineSettingReachesResolution(t *testing.T) {
	f, id, secret, _ := liveSeatForRun(t)
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	f.store, err = OpenStore(f.path, StoreOptions{Clock: f.clock, LeaseDuration: time.Hour, SeatCommandMaxRuntime: 2 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := f.store.ResolveSeatRun(t.Context(), id, secret)
	if err != nil || binding.DefaultMaxRuntimeSeconds != 7200 {
		t.Fatalf("configured deadline=%+v %v", binding, err)
	}
}

func TestSeatRunPollKeepsLiveStartingSeatRunning(t *testing.T) {
	f, id, _, claim, input := startSeatRunFixture(t)
	input.Phase = "poll"
	command, err := f.store.ControlSeatRun(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID, input)
	if err != nil {
		t.Fatal(err)
	}
	b := command.Binding
	auth := command.ComputerSeatAttemptRequest
	list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", b.ComputerID, b.ComputerJobID, b.ComputerAttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", b.ComputerID, b.ComputerJobID, b.ComputerAttemptID, ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 2, Sequence: 2, Seats: []ComputerSeatObservation{{SeatID: id, Place: 1, State: ComputerSeatStarting}}})
	if err != nil {
		t.Fatal(err)
	}
	command, err = f.store.ControlSeatRun(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID, input)
	if err != nil || command.Cancel {
		t.Fatalf("live Seat starting cancelled command: %+v %v", command, err)
	}
	if _, err = f.store.EndSeatRequest(t.Context(), "fabric-computer-node", id, SeatEndRequest{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	command, err = f.store.ControlSeatRun(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID, input)
	if err != nil || !command.Cancel {
		t.Fatalf("ended Seat did not cancel command: %+v %v", command, err)
	}
}

func TestSeatRunControlRefusesAnotherNodeBinding(t *testing.T) {
	f, _, _, claim, input := startSeatRunFixture(t)
	// Model a misrouted Job despite its pin tag: authenticated Attempt evidence
	// alone must not authorize a command bound to a different node.
	var raw []byte
	if err := f.store.db.QueryRow(`SELECT params_json FROM seat_runs WHERE job_id=?`, claim.Job.JobID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var params contract.SeatRunParams
	if err := json.Unmarshal(raw, &params); err != nil {
		t.Fatal(err)
	}
	params.Binding.NodeID = "another-node"
	raw, _ = json.Marshal(params)
	if _, err := f.store.db.Exec(`UPDATE seat_runs SET params_json=? WHERE job_id=?`, raw, claim.Job.JobID); err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{"start", "poll", "finish"} {
		input.Phase = phase
		input.Result = nil
		if phase == "finish" {
			code := 0
			input.Result = &ProcessResult{ExitCode: &code}
		}
		_, err := f.store.ControlSeatRun(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID, input)
		if errorCode(err) != contract.ErrorForbidden {
			t.Fatalf("%s node binding refusal=%v", phase, err)
		}
	}
}

func anotherSeatRun(t *testing.T, f *seatLifecycleFixture, first Claim) Claim {
	t.Helper()
	spec := first.Job.Spec
	spec.DispatchKey = "second-seat-run-command"
	spec.Labels = map[string]string{"run_id": "run_00000000000000000000000000000002", contract.LabelRunParams: first.Job.Spec.Labels[contract.LabelRunParams]}
	job, _, err := f.store.CreateJobAs(t.Context(), spec, JobOrigin{OriginatingSubmitter: "cli", SubmittedByRunLedger: true})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassOneShot)
	if err != nil || claim == nil {
		t.Fatalf("second claim=%+v %v", claim, err)
	}
	_, err = f.store.ControlSeatRun(t.Context(), "fabric-computer-node", job.JobID, claim.Lease.AttemptID, SeatRunControlRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken, Phase: "start"})
	if err != nil {
		t.Fatal(err)
	}
	return *claim
}
func TestSeatRunRecoveryInvalidReceiptDoesNotBlockLaterRun(t *testing.T) {
	f, id, secret, first, _ := startSeatRunFixture(t)
	second := anotherSeatRun(t, f, first)
	zero := 0
	for i, claim := range []Claim{first, second} {
		result := ProcessResult{ExitCode: &zero}
		if i == 0 {
			result = ProcessResult{OutputError: "cannot write result file", LimitReached: contract.LimitReachedMaxRuntime}
		}
		if _, err := f.store.CompleteAttempt(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID, CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "lost-receipt", Result: result}); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	page, err := f.store.GetSeatLogs(t.Context(), "cli", id, secret, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	finished := 0
	for _, event := range page.Events {
		if event.Kind == SeatEventCommandFinished {
			finished++
			if event.RunID == first.Job.Spec.Labels["run_id"] && (event.ExitCode != nil || event.Reason != "max_runtime") {
				t.Fatalf("recovered event=%+v", event)
			}
		}
	}
	if finished != 2 {
		t.Fatalf("recovered %d commands, want both", finished)
	}
	f.clock.Advance(30 * time.Minute)
	if err = f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	value, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || value.State != SeatEnding {
		t.Fatalf("recovered holds retained Seat: %+v %v", value, err)
	}
}

func TestSeatRunRecoverySkipsFailedRow(t *testing.T) {
	f, id, secret, first, _ := startSeatRunFixture(t)
	second := anotherSeatRun(t, f, first)
	zero := 0
	for _, claim := range []Claim{first, second} {
		if _, err := f.store.CompleteAttempt(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID, CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "lost-receipt", Result: ProcessResult{ExitCode: &zero}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.store.db.Exec(`CREATE TRIGGER fail_one_seat_run BEFORE UPDATE ON seat_runs WHEN OLD.run_id='run_00000000000000000000000000000001' BEGIN SELECT RAISE(ABORT,'injected finish failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	page, err := f.store.GetSeatLogs(t.Context(), "cli", id, secret, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range page.Events {
		if event.Kind == SeatEventCommandFinished && event.RunID == second.Job.Spec.Labels["run_id"] {
			return
		}
	}
	t.Fatal("one failed recovery row blocked the later command")
}

func TestSeatRunBackstopFinishesSilentNodeAndReleasesHold(t *testing.T) {
	f, id, secret, claim, input := startSeatRunFixture(t)
	// Keep the boot-relative start timeline while moving the wall clock
	// backwards. Only elapsed Seat time may decide the backstop.
	clock := &seatBootTestClock{fakeClock: f.clock, boot: "injected-clock", elapsed: time.Duration(f.clock.Now().UnixNano())}
	f.store.clock = clock
	clock.elapsed += 65*time.Minute - time.Nanosecond
	f.clock.Advance(-24 * time.Hour)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	page, err := f.store.GetSeatLogs(t.Context(), "cli", id, secret, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range page.Events {
		if event.Kind == SeatEventCommandFinished {
			t.Fatalf("early backstop: %+v", event)
		}
	}
	clock.elapsed++
	if err = f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	page, err = f.store.GetSeatLogs(t.Context(), "cli", id, secret, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range page.Events {
		if event.Kind == SeatEventCommandFinished {
			found = true
			if event.ExitCode != nil || event.Reason != "max_runtime_backstop" || !strings.Contains(event.Note, "longer --max-runtime") {
				t.Fatalf("backstop event=%+v", event)
			}
		}
	}
	if !found {
		t.Fatal("silent node command never force-finished")
	}
	job, err := f.store.GetJob(t.Context(), claim.Job.JobID)
	if err != nil || job.State != contract.JobFailed {
		t.Fatalf("backstop Job=%+v %v", job, err)
	}
	attempts, err := f.store.ListJobAttempts(t.Context(), claim.Job.JobID)
	if err != nil || len(attempts) != 1 || (attempts[0].Result != nil && attempts[0].Result.ExitCode != nil) {
		t.Fatalf("backstop Attempts=%+v %v", attempts, err)
	}
	input.Phase = "poll"
	if _, err = f.store.ControlSeatRun(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID, input); err == nil {
		t.Fatal("backstop left command authority active")
	}
	clock.elapsed += 30 * time.Minute
	if err = f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	value, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || value.State != SeatEnding {
		t.Fatalf("backstop hold retained Seat: %+v %v", value, err)
	}
}

func TestSeatRunPollDoesNotWaitForWriter(t *testing.T) {
	f, _, _, claim, input := startSeatRunFixture(t)
	input.Phase = "poll"
	write, err := f.store.beginWriteTransaction(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer write.rollback()
	// This timeout only bounds a deadlock in the test; it does not advance a
	// business timer. A WAL read snapshot must succeed beside a held writer.
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	command, err := f.store.ControlSeatRun(ctx, "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID, input)
	if err != nil || command.Cancel {
		t.Fatalf("poll waited for unrelated writer: %+v %v", command, err)
	}
}

func TestSeatRunLegacyRunningHoldRestampsOnUpgrade(t *testing.T) {
	f, id, _, claim, input := startSeatRunFixture(t)
	columns, err := sqliteTableColumns(t.Context(), f.store.db, "seat_runs")
	if err != nil {
		t.Fatal(err)
	}
	for _, col := range columns {
		if col != "start_boot_id" && col != "start_ns" && col != "computer_fencing_token" {
			continue
		}
		if _, err = f.store.db.Exec("ALTER TABLE seat_runs DROP COLUMN " + col); err != nil {
			t.Fatal(err)
		}
	}
	f.reopen(t)
	if err = f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertSeatRunContinues(t, f, claim, input)
	assertSeatRunStartStamp(t, f, claim, "injected-clock", f.clock.Now().UnixNano())
	f.clock.Advance(30 * time.Minute)
	if err = f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	value, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || value.State != SeatLive {
		t.Fatalf("legacy command lost its hold: %+v %v", value, err)
	}
	f.clock.Advance(35 * time.Minute)
	if err = f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	job, err := f.store.GetJob(t.Context(), claim.Job.JobID)
	if err != nil || job.State != contract.JobFailed {
		t.Fatalf("legacy fresh window never ended: %+v %v", job, err)
	}
	f.clock.Advance(30 * time.Minute)
	if err = f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	value, err = f.store.GetSeatRequest(t.Context(), id)
	if err != nil || value.State != SeatEnding {
		t.Fatalf("legacy backstop retained hold: %+v %v", value, err)
	}
}

func reopenSeatRunOnNewL1Boot(t *testing.T, f *seatLifecycleFixture) *seatBootTestClock {
	t.Helper()
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	clock := &seatBootTestClock{fakeClock: f.clock, boot: "new-l1-host-boot", elapsed: 10 * time.Minute}
	var err error
	f.store, err = OpenStore(f.path, StoreOptions{Clock: clock, LeaseDuration: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	return clock
}
func assertSeatRunContinues(t *testing.T, f *seatLifecycleFixture, claim Claim, input SeatRunControlRequest) {
	t.Helper()
	job, err := f.store.GetJob(t.Context(), claim.Job.JobID)
	if err != nil || (job.State != contract.JobClaimed && job.State != contract.JobRunning) {
		t.Fatalf("L1 reboot or missing stamp ended command: %+v %v", job, err)
	}
	input.Phase = "poll"
	reply, err := f.store.ControlSeatRun(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID, input)
	if err != nil || reply.Cancel {
		t.Fatalf("live command refused after clock recovery: %+v %v", reply, err)
	}
	var held bool
	if err = f.store.db.QueryRow(`SELECT active FROM seat_activity_holds WHERE seat_id=? AND kind='command' AND hold_id=?`, reply.Binding.SeatID, reply.RunID).Scan(&held); err != nil || !held {
		t.Fatalf("command hold=%t %v", held, err)
	}
}
func assertSeatRunStartStamp(t *testing.T, f *seatLifecycleFixture, claim Claim, wantBoot string, wantNS int64) {
	t.Helper()
	var boot string
	var ns int64
	err := f.store.db.QueryRow(`SELECT start_boot_id,start_ns FROM seat_runs WHERE job_id=?`, claim.Job.JobID).Scan(&boot, &ns)
	if err != nil || boot != wantBoot || ns != wantNS {
		t.Fatalf("stamp=(%q,%d), want (%q,%d): %v", boot, ns, wantBoot, wantNS, err)
	}
}
func TestSeatRunL1HostRebootRestampsWithoutEndingCommand(t *testing.T) {
	f, id, _, claim, input := startSeatRunFixture(t)
	clock := reopenSeatRunOnNewL1Boot(t, f)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertSeatRunContinues(t, f, claim, input)
	assertSeatRunStartStamp(t, f, claim, clock.boot, int64(10*time.Minute))
	clock.elapsed += 31 * time.Minute
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	value, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || value.State != SeatLive {
		t.Fatalf("reboot released running command hold: %+v %v", value, err)
	}
	assertSeatRunContinues(t, f, claim, input)
}
func TestSeatRunL1HostRebootBackstopWaitsForFreshWindow(t *testing.T) {
	f, id, secret, claim, input := startSeatRunFixture(t)
	clock := reopenSeatRunOnNewL1Boot(t, f)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Repeated maintenance must retain the re-stamp, rather than continually
	// renewing the window. The old boot's elapsed value is unrelated.
	clock.elapsed += 65*time.Minute - time.Nanosecond
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertSeatRunContinues(t, f, claim, input)
	assertSeatRunStartStamp(t, f, claim, clock.boot, int64(10*time.Minute))
	clock.elapsed++
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	job, err := f.store.GetJob(t.Context(), claim.Job.JobID)
	if err != nil || job.State != contract.JobFailed {
		t.Fatalf("fresh backstop did not finish: %+v %v", job, err)
	}
	page, err := f.store.GetSeatLogs(t.Context(), "cli", id, secret, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range page.Events {
		if event.Kind == SeatEventCommandFinished {
			found = true
			if event.Reason != "max_runtime_backstop" || event.ExitCode != nil {
				t.Fatalf("backstop event=%+v", event)
			}
		}
	}
	if !found {
		t.Fatal("fresh backstop omitted finished event")
	}
	clock.elapsed += 30 * time.Minute
	if err = f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	value, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || value.State != SeatEnding {
		t.Fatalf("fresh backstop did not release hold: %+v %v", value, err)
	}
}

func TestSeatRunPollCancelsReplacedComputerFence(t *testing.T) {
	f, _, _, claim, input := startSeatRunFixture(t)
	input.Phase = "poll"
	first, err := f.store.ControlSeatRun(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID, input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.db.Exec(`UPDATE attempts SET fencing_token='replacement-fence' WHERE attempt_id=?`, first.Binding.ComputerAttemptID); err != nil {
		t.Fatal(err)
	}
	next, err := f.store.ControlSeatRun(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID, input)
	if err != nil || !next.Cancel {
		t.Fatalf("changed Computer fence preserved command: %+v %v", next, err)
	}
}

func TestSeatRunSchemaUpgradeMatchesFreshFingerprint(t *testing.T) {
	fresh := newSeatLifecycleFixture(t)
	upgraded := newSeatLifecycleFixture(t)
	const legacy = `CREATE TABLE IF NOT EXISTS seat_runs (
 run_id TEXT PRIMARY KEY,
 job_id TEXT NOT NULL UNIQUE REFERENCES jobs(job_id) ON DELETE CASCADE,
 seat_id TEXT NOT NULL,
 params_json BLOB NOT NULL,
 computer_attempt_id TEXT REFERENCES attempts(attempt_id) ON DELETE SET NULL,
 command_attempt_id TEXT REFERENCES attempts(attempt_id) ON DELETE SET NULL,
 state TEXT NOT NULL CHECK(state IN ('queued','running','finished')),
 result_json BLOB
);`
	if _, err := upgraded.store.db.Exec(`DROP TABLE seat_runs;` + legacy); err != nil {
		t.Fatal(err)
	}
	upgraded.reopen(t)
	if err := fresh.store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := upgraded.store.Close(); err != nil {
		t.Fatal(err)
	}
	want, err := upgradestate.Fingerprint(t.Context(), fresh.path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := upgradestate.Fingerprint(t.Context(), upgraded.path)
	if err != nil || got != want {
		t.Fatalf("upgrade fingerprint=%s fresh=%s error=%v", got, want, err)
	}
}
