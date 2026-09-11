//go:build darwin || linux

package agent

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

var childJobPattern = regexp.MustCompile(`child=(\S+) self=(\S+)`)

// TestProcessJobSpawnsChildWithOnlyTheInjectedCredential is the whole point of
// the ticket in one test: a kind=process job submitted straight to L1, with no
// L3 anywhere, spawns a child and reads it back using nothing but the two
// injected environment variables.
//
// Leases are a real second here, per the repository timing rule for tests that
// run actual processes.
func TestProcessJobSpawnsChildWithOnlyTheInjectedCredential(t *testing.T) {
	directory := t.TempDir()
	readyFile := filepath.Join(directory, "ready.json")
	serverLogs := &lockedBuffer{}
	server := startManagedProcess(t, controlPlanePath,
		"--fabric=plain",
		"--listen=127.0.0.1:0",
		"--db="+filepath.Join(directory, "l1.sqlite"),
		"--lease-duration=3s",
		"--node-tags=stable-node=linux",
		"--ready-file="+readyFile,
	)
	server.command.Stdout = serverLogs
	server.command.Stderr = serverLogs
	server.start(t)
	address := waitForReadyAddress(t, readyFile, server, 10*time.Second)

	agentLogs := &lockedBuffer{}
	node := startManagedProcess(t, agentBinaryPath,
		"--fabric=plain",
		"--control-plane="+address,
		"--node-id=stable-node",
		"--log-spool-dir="+directory,
		"--plain-identity=fabric-node",
		"--heartbeat-interval=5s",
		"--claim-interval=10ms",
		"--renewal-interval=500ms",
	)
	node.command.Stdout = agentLogs
	node.command.Stderr = agentLogs
	node.start(t)

	clientFabric := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "spawn-client", Tags: []string{l1.DefaultClientPrincipalTag}})
	client := newHTTPClient(clientFabric, address)
	defer client.CloseIdleConnections()

	childKey := "e2e-child-" + fmt.Sprint(time.Now().UnixNano())
	workingDirectory := t.TempDir()
	parent := submitE2EJob(t, client, contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		DispatchKey:   "e2e-spawn-" + fmt.Sprint(time.Now().UnixNano()),
		Kind:          contract.JobKindProcess,
		Class:         contract.JobClassOneShot,
		RoutingTags:   []string{"linux"},
		Execution: contract.ExecutionSpec{
			Executable:       contract.ExecutableSpec{Path: agentHelperPath},
			Argv:             []string{"processhelper", "submit-child", childKey},
			WorkingDirectory: workingDirectory,
			HandoffDirectory: workingDirectory,
		},
	})
	waitForE2EJobState(t, client, parent.JobID, contract.JobSucceeded, 20*time.Second)

	logs := readAllE2ELogs(t, client, parent.JobID)
	match := childJobPattern.FindStringSubmatch(logs)
	if match == nil {
		t.Fatalf("workload did not report a spawned child\nlogs=%s\nagent=%s", logs, agentLogs.Bytes())
	}
	childID, reportedSelf := match[1], match[2]
	if reportedSelf != parent.JobID {
		t.Fatalf("workload identified itself as %q, want %q", reportedSelf, parent.JobID)
	}

	child := getE2EJob(t, client, childID)
	if child.ParentJobID != parent.JobID {
		t.Fatalf("child parent = %q, want %q", child.ParentJobID, parent.JobID)
	}
	if child.ParentAttemptID == "" {
		t.Fatal("child recorded no parent attempt")
	}
	if child.OriginatingSubmitter != "spawn-client" {
		t.Fatalf("child originating submitter = %q, want the root submitter", child.OriginatingSubmitter)
	}
	if child.SpawnDepth != 1 {
		t.Fatalf("child spawn depth = %d, want 1", child.SpawnDepth)
	}

	// The workload printed its credential deliberately. It is delivered
	// sensitively, so the agent must have replaced it before the bytes reached
	// the log sink.
	if !strings.Contains(logs, "token=[REDACTED]") {
		t.Fatalf("workload logs did not redact the printed credential: %s", logs)
	}
}

func readAllE2ELogs(t *testing.T, client *http.Client, jobID string) string {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		"http://control-plane.invalid/v1/jobs/"+jobID+"/logs?limit=100", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("read logs status = %d body=%s", response.StatusCode, body)
	}
	var page l1.LogPage
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	combined := ""
	for _, event := range page.Events {
		combined += string(event.Bytes)
	}
	return combined
}
