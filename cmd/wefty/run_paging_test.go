package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l3"
)

func TestRunPagingFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	store, err := l3.OpenStore(filepath.Join(t.TempDir(), "ledger.sqlite"), l3.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	expected := map[string]bool{}
	for i := 0; i < 507; i++ {
		record, _, err := store.CreateRun(t.Context(), l3.CreateRunInput{IdempotencyKey: fmt.Sprintf("binary-%d", i), Actor: "paging-operator", Request: computerRunRequest("#!/bin/sh\nexit 0\n")})
		if err != nil {
			t.Fatal(err)
		}
		expected[record.RunID] = true
	}
	if _, _, err := store.CreateRun(t.Context(), l3.CreateRunInput{IdempotencyKey: "foreign", Actor: "paging-operator-other", Request: computerRunRequest("#!/bin/sh\nexit 0\n")}); err != nil {
		t.Fatal(err)
	}
	originExpected := map[string]bool{}
	proof := l3.ComputerTokenScopeProof{ComputerID: "paging-computer", ComputerAttemptID: "attempt", HostNodeID: "node", HostStableNodeID: "stable-node", HostBootSessionID: "boot", ComputerStorageGeneration: 1, SubmitIntentRevision: 1, SubmitMaxInflight: 10}
	grant, err := store.MintComputerToken(t.Context(), proof)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := store.AuthenticateComputerToken(t.Context(), grant.Token)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		record, _, err := store.CreateRun(t.Context(), l3.CreateRunInput{IdempotencyKey: fmt.Sprintf("origin-%d", i), Actor: "computer:" + scope.ComputerID, ComputerScope: &scope, Request: computerRunRequest("#!/bin/sh\nexit 0\n"), VerifyComputerScope: func(context.Context, l3.ComputerTokenScope) error { return nil }})
		if err != nil {
			t.Fatal(err)
		}
		originExpected[record.RunID] = true
	}
	network := plain.NewNetwork()
	ledger := network.NewFabric(fabric.Identity{NodeID: "paging-ledger"})
	server, err := l3.NewServer(ledger, store, l3.ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := ledger.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: server.Handler()}
	go func() { _ = httpServer.Serve(listener) }()
	t.Cleanup(func() { httpServer.Close() })
	base := []string{"--json", "--l3=" + listener.Addr().String(), "--plain-identity=paging-operator", "runs", "list", "--mine", "--status=pending"}
	type page struct {
		Runs       []l3.RunSummary `json:"runs"`
		NextCursor string          `json:"next_cursor"`
	}
	call := func(extra ...string) page {
		t.Helper()
		code, output := runWefty(t, binary, 60*time.Second, append(append([]string{}, base...), extra...)...)
		if code != 0 {
			t.Fatalf("runs list exit=%d: %s", code, output)
		}
		var result page
		if err := json.Unmarshal([]byte(output), &result); err != nil {
			t.Fatalf("invalid JSON: %v: %s", err, output)
		}
		return result
	}
	if result := call("--status=failed", "--all"); len(result.Runs) != 0 || result.NextCursor != "" {
		t.Fatalf("empty filtered walk = %+v", result)
	}
	first := call("--limit=17")
	if len(first.Runs) != 17 || first.NextCursor == "" {
		t.Fatalf("first page = %+v", first)
	}
	second := call("--limit=13", "--cursor="+first.NextCursor)
	if len(second.Runs) != 13 || second.Runs[0].RunID == first.Runs[0].RunID {
		t.Fatalf("second page = %+v", second)
	}
	all := call("--limit=73", "--all")
	seen := map[string]bool{}
	for _, run := range all.Runs {
		if !expected[run.RunID] || seen[run.RunID] {
			t.Fatalf("unexpected or repeated run %s", run.RunID)
		}
		seen[run.RunID] = true
	}
	if len(seen) != 507 || all.NextCursor != "" {
		t.Fatalf("all returned %d rows, cursor=%q", len(seen), all.NextCursor)
	}
	tail := call("--limit=91", "--cursor="+first.NextCursor, "--all")
	if len(tail.Runs) != 490 || tail.NextCursor != "" {
		t.Fatalf("tail = %d rows cursor=%q", len(tail.Runs), tail.NextCursor)
	}
	originArgs := []string{"--json", "--l3=" + listener.Addr().String(), "runs", "list", "--origin=computer:paging-computer", "--limit=1", "--all"}
	code, output := runWefty(t, binary, 30*time.Second, originArgs...)
	var originPage l3.ComputerRunPage
	if err := json.Unmarshal([]byte(output), &originPage); code != 0 || err != nil || len(originPage.Runs) != 3 || originPage.NextCursor != "" {
		t.Fatalf("origin --all exit=%d output=%s err=%v", code, output, err)
	}
	for _, run := range originPage.Runs {
		if !originExpected[run.RunID] {
			t.Fatalf("unexpected origin row %s", run.RunID)
		}
		delete(originExpected, run.RunID)
	}
	if len(originExpected) != 0 {
		t.Fatal("origin --all missed rows")
	}
	for _, query := range [][]string{{"--limit=0"}, {"--limit=501"}, {"--origin=computer:any", "--mine"}} {
		code, _ := runWefty(t, binary, 30*time.Second, append(append([]string{}, base...), query...)...)
		if code == 0 {
			t.Fatalf("%v unexpectedly succeeded", query)
		}
	}
	human := []string{"--l3=" + listener.Addr().String(), "--plain-identity=paging-operator", "runs", "list", "--mine", "--limit=17"}
	code, output = runWefty(t, binary, 30*time.Second, human...)
	if code != 0 || !strings.Contains(output, "next_cursor:") {
		t.Fatalf("human paging exit=%d output=%s", code, output)
	}
}
