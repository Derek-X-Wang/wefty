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

func TestComputerOperatorFactsFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	network := plain.NewNetwork()
	control := network.NewFabric(fabric.Identity{NodeID: "control"})
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "computer.sqlite"), l1.StoreOptions{})
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
	ctx, cancel := context.WithCancel(t.Context())
	done := serveTestServer(ctx, func() error { return server.Serve(ctx, listener) })
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	computer, _, err := store.CreateComputer(ctx, l1.CreateComputerRequest{Name: "facts", Spec: cliComputerSpec("operator-facts"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	base := []string{"--fabric=plain", "--l1=" + listener.Addr().String(), "--l3="}
	call := func(args ...string) (int, string) {
		return runWefty(t, binary, 30*time.Second, append(append([]string{}, base...), args...)...)
	}
	for _, jsonOutput := range []bool{true, false} {
		args := []string{"computers", "list", "--limit=1"}
		if jsonOutput {
			args = append([]string{"--json"}, args...)
		}
		exit, output := call(args...)
		if exit != 0 {
			t.Fatalf("exit=%d output=%s", exit, output)
		}
		if jsonOutput {
			var page struct {
				Computers []struct {
					ComputerID     string                   `json:"computer_id"`
					AllowedActions []contract.AllowedAction `json:"allowed_actions"`
					LastCondition  *contract.Condition      `json:"last_condition"`
				} `json:"computers"`
			}
			if err := json.Unmarshal([]byte(output), &page); err != nil {
				t.Fatal(err)
			}
			if len(page.Computers) != 1 || page.Computers[0].ComputerID != computer.ComputerID || len(page.Computers[0].AllowedActions) != 15 || page.Computers[0].LastCondition == nil {
				t.Fatalf("missing Computer facts: %s", output)
			}
			for _, action := range page.Computers[0].AllowedActions {
				if action.Requires["intent_revision"] != float64(computer.IntentRevision) || action.Requires["storage_id"] != computer.StorageID || action.Requires["storage_generation"] != float64(computer.StorageGeneration) {
					t.Fatalf("binary lost exact preconditions: %#v", action)
				}
				if action.Verb == "backup" && (action.RefusedBecause == nil || action.RefusedBecause.Code != contract.ErrorConflict) {
					t.Fatalf("binary lost Backup refusal: %#v", action)
				}
			}
		} else {
			for _, field := range []string{"LAST CONDITION", "ALLOWED ACTIONS", "computer_intent_create", `"intent_revision":1`, `"requires"`, `"refused_because"`, `"verb":"reset"`, `"in":"path"`} {
				if !strings.Contains(output, field) {
					t.Fatalf("table missing %s: %s", field, output)
				}
			}
		}
	}
}
