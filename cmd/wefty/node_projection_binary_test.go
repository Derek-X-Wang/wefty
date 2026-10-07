package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestNodeOperatorFactsFromRealBinary(t *testing.T) {
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
		NodeID: "node", BootSessionID: "boot", OS: "linux", Architecture: "arm64", AgentVersion: "test", Capabilities: map[string]bool{"kind:process": true},
	}, l1.NodePolicy{Tags: []string{"linux"}, MaxOneshotSlots: 1, MaxServiceSlots: 1}, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CreateJob(ctx, nodeCLIJobSpec("resident", contract.JobClassOneShot, "linux")); err != nil {
		t.Fatal(err)
	}
	if claim, err := store.ClaimJob(ctx, "agent", node.NodeID, node.BootSessionID, contract.JobClassOneShot); err != nil || claim == nil {
		t.Fatalf("claim=%#v err=%v", claim, err)
	}
	base := []string{"--fabric=plain", "--l1=" + listener.Addr().String(), "--l3="}
	call := func(args ...string) (int, string) {
		return runWefty(t, binary, 30*time.Second, append(append([]string{}, base...), args...)...)
	}
	t.Run("list and detail facts", func(t *testing.T) {
		for _, args := range [][]string{{"--json", "nodes", "list"}, {"--json", "nodes", "inspect", "node"}} {
			code, output := call(args...)
			if code != 0 {
				t.Fatalf("exit=%d output=%s", code, output)
			}
			for _, field := range []string{"\"active_attempts\"", "\"last_condition\"", "\"allowed_actions\"", "\"job_id\"", "\"attempt_id\"", "\"drain\"", "\"set-claims\""} {
				if !strings.Contains(output, field) {
					t.Fatalf("missing %s: %s", field, output)
				}
			}
		}
		code, output := call("nodes", "list")
		if code != 0 {
			t.Fatalf("exit=%d output=%s", code, output)
		}
		for _, field := range []string{"ACTIVE ATTEMPTS", "LAST CONDITION", "ALLOWED ACTIONS", "drain", "set-claims"} {
			if !strings.Contains(output, field) {
				t.Fatalf("missing %s: %s", field, output)
			}
		}
	})
	t.Run("guarded drain reason and stale refusal", func(t *testing.T) {
		code, output := call("--json", "drain", "node", "--revision=0", "--reason=maintenance window")
		if code != 0 {
			t.Fatalf("guarded drain exit=%d output=%s", code, output)
		}
		var drained l1.Node
		if err := json.Unmarshal([]byte(firstJSONDocument(output)), &drained); err != nil {
			t.Fatal(err)
		}
		if drained.IntentRevision != 1 || drained.ClaimsEnabled || drained.IntentReason != "maintenance window" {
			t.Fatalf("drained=%#v", drained)
		}
		code, output = call("--json", "drain", "node", "--revision=0", "--reason=stale decision")
		if code != exitConflict {
			t.Fatalf("stale drain exit=%d output=%s", code, output)
		}
		var refusal contract.ErrorResponse
		if err := json.Unmarshal([]byte(firstJSONDocument(output)), &refusal); err != nil {
			t.Fatal(err)
		}
		if refusal.Error.Code != contract.ErrorStaleIntentRevision || refusal.Error.Details["expected_revision"] != float64(1) {
			t.Fatalf("stale refusal=%s", output)
		}
	})
	t.Run("invalid drain flags", func(t *testing.T) {
		for _, args := range [][]string{{"--revision=-1"}, {"--reason= "}, {"--revision=bad"}} {
			command := append([]string{"--json", "drain", "node"}, args...)
			code, output := call(command...)
			if code != exitUsage {
				t.Fatalf("%v exit=%d output=%s", args, code, output)
			}
			var refusal contract.ErrorResponse
			if err := json.Unmarshal([]byte(firstJSONDocument(output)), &refusal); err != nil {
				t.Fatal(err)
			}
			if refusal.Error.Code != contract.ErrorInvalidRequest {
				t.Fatalf("local error=%s", output)
			}
		}
	})

	t.Run("node command exit codes", func(t *testing.T) {
		for _, check := range []struct {
			name string
			args []string
			exit int
			code contract.ErrorCode
		}{
			{"stale set claims", []string{"nodes", "set-claims", "node", "--claims-enabled=false", "--intent-revision=0", "--reason=stale"}, exitConflict, contract.ErrorStaleIntentRevision},
			{"missing inspect", []string{"nodes", "inspect", "missing"}, exitNotFound, contract.ErrorNotFound},
			{"bad flag", []string{"nodes", "set-claims", "node", "--unknown=true"}, exitUsage, contract.ErrorInvalidRequest},
			{"bad boolean", []string{"nodes", "set-claims", "node", "--claims-enabled=bad"}, exitUsage, contract.ErrorInvalidRequest},
			{"bad revision", []string{"nodes", "set-claims", "node", "--intent-revision=bad"}, exitUsage, contract.ErrorInvalidRequest},
		} {
			t.Run(check.name, func(t *testing.T) {
				code, output := call(append([]string{"--json"}, check.args...)...)
				if code != check.exit {
					t.Fatalf("exit=%d want=%d output=%s", code, check.exit, output)
				}
				var response contract.ErrorResponse
				if err := json.Unmarshal([]byte(firstJSONDocument(output)), &response); err != nil {
					t.Fatal(err)
				}
				if response.Error.Code != check.code {
					t.Fatalf("error=%s", output)
				}
			})
		}
	})

	t.Run("convenient drain", func(t *testing.T) {
		code, output := call("--json", "drain", "node")
		if code != 0 {
			t.Fatalf("convenient drain exit=%d output=%s", code, output)
		}
		var drained l1.Node
		if err := json.Unmarshal([]byte(firstJSONDocument(output)), &drained); err != nil {
			t.Fatal(err)
		}
		if drained.ClaimsEnabled || drained.IntentReason != "operator requested drain" {
			t.Fatalf("drained=%#v", drained)
		}
	})
}
