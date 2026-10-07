package main

import (
	"context"
	"encoding/json"
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

func TestServiceOperatorFactsFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	network := plain.NewNetwork()
	control := network.NewFabric(fabric.Identity{NodeID: "control"})
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	server, err := l1.NewServer(control, store, l1.ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := control.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := serveTestServer(ctx, func() error { return server.Serve(ctx, listener) })
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	node, err := store.RegisterNode(ctx, fabric.Identity{NodeID: "agent"}, contract.NodeRegistration{
		NodeID: "node", BootSessionID: "boot", RootInstanceID: "root", OS: "linux", Architecture: "arm64", AgentVersion: "test", Capabilities: map[string]bool{"kind:process": true},
	}, l1.NodePolicy{Tags: []string{"linux"}, MaxOneshotSlots: 1, MaxServiceSlots: 1}, true)
	if err != nil {
		t.Fatal(err)
	}
	spec := nodeCLIJobSpec("policy-service", contract.JobClassService, "linux")
	spec.Restart = contract.RestartNever
	job, _, err := store.CreateJob(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimJob(ctx, "agent", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim=%+v %v", claim, err)
	}
	if _, err := store.StartAttempt(ctx, "agent", job.JobID, claim.Lease.AttemptID, l1.StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
		t.Fatal(err)
	}
	zero := 0
	if _, err := store.CompleteAttempt(ctx, "agent", job.JobID, claim.Lease.AttemptID, l1.CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "complete", Result: l1.ProcessResult{ExitCode: &zero}, RuntimeQuiescenceEvidence: l1.RuntimeQuiescenceAttempt,
	}); err != nil {
		t.Fatal(err)
	}
	base := []string{"--fabric=plain", "--l1=" + listener.Addr().String(), "--l3="}
	call := func(args ...string) (int, string) {
		return runWefty(t, binary, 30*time.Second, append(append([]string{}, base...), args...)...)
	}
	type facts struct {
		Actions   []contract.AllowedAction `json:"allowed_actions"`
		Condition *contract.Condition      `json:"last_condition"`
	}
	var detail facts
	for _, verb := range []string{"list", "status"} {
		args := []string{"--json", "services", verb}
		if verb == "status" {
			args = append(args, job.JobID)
		}
		code, output := call(args...)
		if code != 0 {
			t.Fatalf("%s exit=%d %s", verb, code, output)
		}
		var got facts
		if verb == "list" {
			var page struct {
				Jobs []facts `json:"jobs"`
			}
			if err := json.Unmarshal([]byte(firstJSONDocument(output)), &page); err != nil {
				t.Fatal(err)
			}
			if len(page.Jobs) != 1 {
				t.Fatal(output)
			}
			got = page.Jobs[0]
		} else {
			if err := json.Unmarshal([]byte(firstJSONDocument(output)), &got); err != nil {
				t.Fatal(err)
			}
			detail = got
		}
		if len(got.Actions) != 5 || got.Condition == nil || got.Condition.Code != "policy_stop" {
			t.Fatalf("%s lost service facts: %s", verb, output)
		}
		for _, action := range got.Actions {
			if action.RefusedBecause != nil {
				t.Fatalf("policy stop action refused: %+v", action)
			}
		}
		code, output = call(args[1:]...)
		if code != 0 {
			t.Fatalf("table exit=%d %s", code, output)
		}
		for _, want := range []string{"LAST CONDITION", "ALLOWED ACTIONS", "policy_stop", "idempotency_key", "desired_state", "force"} {
			if !strings.Contains(output, want) {
				t.Fatalf("table missing %s: %s", want, output)
			}
		}
	}
	// Execute the advertised exact start precondition and verify the condition
	// clears in the new snapshot, rather than preserving stale policy advice.
	var start contract.AllowedAction
	for _, action := range detail.Actions {
		if action.Verb == "start" {
			start = action
		}
	}
	if !reflect.DeepEqual(start.Requires, map[string]any{"desired_state": "running"}) {
		t.Fatal(start)
	}
	code, output := call("--json", "services", "start", job.JobID)
	if code != 0 {
		t.Fatalf("start exit=%d %s", code, output)
	}
	var started facts
	if err := json.Unmarshal([]byte(firstJSONDocument(output)), &started); err != nil {
		t.Fatal(err)
	}
	if started.Condition != nil || len(started.Actions) != 5 {
		t.Fatalf("start facts=%s", output)
	}
	code, output = call("--json", "services", "remove", job.JobID)
	if code != 0 {
		t.Fatalf("remove exit=%d %s", code, output)
	}
	code, output = call("--json", "services", "status", job.JobID)
	if code != 0 {
		t.Fatalf("removal status exit=%d %s", code, output)
	}
	var removing facts
	if err := json.Unmarshal([]byte(firstJSONDocument(output)), &removing); err != nil {
		t.Fatal(err)
	}
	if len(removing.Actions) != 5 || removing.Condition == nil || removing.Condition.Code != "removal_pending" {
		t.Fatalf("removal facts=%s", output)
	}
	for _, action := range removing.Actions {
		if action.Verb == "start" || action.Verb == "stop" || action.Verb == "restart" {
			if action.RefusedBecause == nil {
				t.Fatalf("removal allowed lifecycle action: %+v", action)
			}
		}
	}
}
