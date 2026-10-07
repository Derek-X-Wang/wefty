package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestJobsListFromRealBinaryOverL1(t *testing.T) {
	binary := buildWefty(t)
	network := plain.NewNetwork()
	control := network.NewFabric(fabric.Identity{NodeID: "jobs-test-control"})
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "jobs.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
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
	var expected []string
	for i := 0; i < 4; i++ {
		spec := contract.JobSpec{
			SchemaVersion: contract.SchemaVersionV1, DispatchKey: strings.Repeat("x", i+1), Kind: contract.JobKindProcess, Class: contract.JobClassOneShot,
			Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/echo"}, Argv: []string{"echo", "listing"}, WorkingDirectory: "/tmp", SensitiveEnv: map[string]string{"SECRET_LIST": "secret-value"}},
		}
		submitter := "wefty-cli"
		if i == 2 {
			submitter = "other"
		}
		if i == 3 {
			spec.Class = contract.JobClassService
			spec.Restart = contract.RestartAlways
		}
		job, _, err := store.CreateJobAs(t.Context(), spec, l1.JobOrigin{OriginatingSubmitter: submitter})
		if err != nil {
			t.Fatal(err)
		}
		if i < 2 {
			expected = append(expected, job.JobID)
		}
	}
	base := []string{"--l1=" + listener.Addr().String(), "--l3=", "--json", "jobs", "list"}
	filters := []string{"--class=one-shot", "--kind=process", "--state=queued", "--submitter=me", "--limit=1"}
	got := []string{}
	cursor := ""
	for {
		args := append(append([]string{}, base...), filters...)
		if cursor != "" {
			args = append(args, "--cursor="+cursor)
		}
		code, output := runWefty(t, binary, 30*time.Second, args...)
		if code != 0 {
			t.Fatalf("jobs list exit=%d output=%s", code, output)
		}
		if strings.Contains(output, "SECRET_LIST") || strings.Contains(output, "secret-value") {
			t.Fatalf("leaked secret: %s", output)
		}
		var page l1.JobList
		if err := json.Unmarshal([]byte(output), &page); err != nil {
			t.Fatalf("invalid JSON: %v %s", err, output)
		}
		for _, job := range page.Jobs {
			got = append(got, job.JobID)
		}
		cursor = page.NextCursor
		if cursor == "" {
			break
		}
	}
	sort.Strings(got)
	sort.Strings(expected)
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("job IDs=%v, want %v", got, expected)
	}
	code, output := runWefty(t, binary, 30*time.Second, "--l1="+listener.Addr().String(), "--l3=", "jobs", "list", "--limit=1")
	if code != 0 || !strings.Contains(output, "JOB ID") || !strings.Contains(output, "NEXT CURSOR") {
		t.Fatalf("table=%d %s", code, output)
	}
	code, output = runWefty(t, binary, 30*time.Second, append(base, "--class=service")...)
	if code != 0 || !strings.Contains(output, `"service"`) {
		t.Fatalf("service filter=%d %s", code, output)
	}
	code, output = runWefty(t, binary, 30*time.Second, "--l1="+listener.Addr().String(), "--l3=", "--json", "services", "list")
	if code != 0 || !strings.Contains(output, `"service"`) {
		t.Fatalf("existing services list=%d %s", code, output)
	}
	for _, flag := range []string{"--limit=0", "--class=invalid", "--state=restart-pending", "--submitter=other", "--cursor=invalid", "--unknown-flag", "unexpected-positional"} {
		code, output = runWefty(t, binary, 30*time.Second, append(base, flag)...)
		if code != exitUsage {
			t.Fatalf("%s exit=%d %s, want usage", flag, code, output)
		}
		var response contract.ErrorResponse
		if err := json.Unmarshal([]byte(output), &response); err != nil || response.Error.Code != contract.ErrorInvalidRequest {
			t.Fatalf("%s error is not structured JSON: %s (%v)", flag, output, err)
		}
	}
}
