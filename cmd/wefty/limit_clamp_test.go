package main

import (
	"context"
	"encoding/json"
	"fmt"
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

// TestLimitClampWalkFromRealBinary shows that the CLI no longer rejects the
// shared collection maximum: `--limit 1000` reaches the server, which clamps
// the Job pages to at most `l1.MaxJobListingPageLimit` and reports
// `next_cursor` while rows remain, and the walk continues to completion.
// Adaptive pages may end early on a slow machine, so only the upper bound and
// the complete walk are asserted. Limits above the shared maximum or below one
// stay usage errors.
func TestLimitClampWalkFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	network := plain.NewNetwork()
	control := network.NewFabric(fabric.Identity{NodeID: "limit-clamp-control"})
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "limit-clamp.sqlite"), l1.StoreOptions{})
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

	const extraOneShots = 15
	for i := 0; i < l1.MaxJobListingPageLimit+extraOneShots; i++ {
		spec := contract.JobSpec{
			SchemaVersion: contract.SchemaVersionV1, DispatchKey: fmt.Sprintf("clamp-%d", i), Kind: contract.JobKindProcess, Class: contract.JobClassOneShot,
			Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/echo"}, Argv: []string{"echo", "clamp"}, WorkingDirectory: "/tmp"},
		}
		if _, _, err := store.CreateJobAs(t.Context(), spec, l1.JobOrigin{OriginatingSubmitter: "wefty-cli"}); err != nil {
			t.Fatal(err)
		}
	}
	var serviceIDs []string
	for i := 0; i < 5; i++ {
		spec := contract.JobSpec{
			SchemaVersion: contract.SchemaVersionV1, DispatchKey: fmt.Sprintf("clamp-service-%d", i), Kind: contract.JobKindProcess, Class: contract.JobClassService, Restart: contract.RestartAlways,
			Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/echo"}, Argv: []string{"echo", "clamp"}, WorkingDirectory: "/tmp"},
		}
		job, _, err := store.CreateJobAs(t.Context(), spec, l1.JobOrigin{OriginatingSubmitter: "wefty-cli"})
		if err != nil {
			t.Fatal(err)
		}
		serviceIDs = append(serviceIDs, job.JobID)
	}

	// walk walks a JSON listing with one shared limit, following next_cursor
	// until it ends, and returns every row's Job ID in page order.
	walk := func(name string, pageLimit string, filters ...string) []string {
		t.Helper()
		base := []string{"--l1=" + listener.Addr().String(), "--l3=", "--json", name, "list"}
		base = append(base, filters...)
		base = append(base, "--limit="+pageLimit)
		var got []string
		cursor := ""
		pages := 0
		jobsWalk := name == "jobs"
		for {
			args := base
			if cursor != "" {
				args = append(args, "--cursor="+cursor)
			}
			code, output := runWefty(t, binary, 30*time.Second, args...)
			if code != 0 {
				t.Fatalf("%s --limit=%s exit=%d output=%s", name, pageLimit, code, output)
			}
			var page l1.JobList
			if err := json.Unmarshal([]byte(output), &page); err != nil {
				t.Fatalf("%s --limit=%s invalid JSON: %v %s", name, pageLimit, err, output)
			}
			pages++
			if len(page.Jobs) > l1.MaxJobListingPageLimit {
				t.Fatalf("%s --limit=%s page %d returned %d rows, above the clamped maximum %d",
					name, pageLimit, pages, len(page.Jobs), l1.MaxJobListingPageLimit)
			}
			for _, job := range page.Jobs {
				got = append(got, job.JobID)
			}
			cursor = page.NextCursor
			if cursor == "" {
				break
			}
			if len(page.Jobs) == 0 || pages > l1.MaxJobListingPageLimit+extraOneShots+len(serviceIDs) {
				t.Fatalf("%s --limit=%s walk stalled or did not end: %d pages so far", name, pageLimit, pages)
			}
		}
		if jobsWalk && pages < 2 {
			t.Fatalf("%s --limit=%s completed in %d page(s), want at least two to walk the cursor", name, pageLimit, pages)
		}
		return got
	}

	jobs := walk("jobs", fmt.Sprint(l1.MaxJobPageLimit))
	if len(jobs) != l1.MaxJobListingPageLimit+extraOneShots+len(serviceIDs) {
		t.Fatalf("jobs walk returned %d rows, want %d",
			len(jobs), l1.MaxJobListingPageLimit+extraOneShots+len(serviceIDs))
	}
	seen := map[string]bool{}
	for _, jobID := range jobs {
		if seen[jobID] {
			t.Fatalf("jobs walk repeated %s", jobID)
		}
		seen[jobID] = true
	}

	services := walk("services", fmt.Sprint(l1.MaxJobPageLimit))
	if !reflect.DeepEqual(services, serviceIDs) {
		t.Fatalf("services walk = %v, want %v", services, serviceIDs)
	}

	for _, command := range []string{"jobs", "services"} {
		for _, limit := range []string{"0", fmt.Sprint(l1.MaxJobPageLimit + 1)} {
			code, output := runWefty(t, binary, 30*time.Second, "--l1="+listener.Addr().String(), "--l3=", "--json", command, "list", "--limit="+limit)
			if code != exitUsage {
				t.Fatalf("%s list --limit=%s exit=%d output=%s, want usage", command, limit, code, output)
			}
			if !strings.Contains(output, "between 1 and") {
				t.Fatalf("%s list --limit=%s is not a usage error: %s", command, limit, output)
			}
		}
	}
}
