//go:build service_acceptance

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// #1340: exercise copy-target discovery and upload through the real servers,
// including production L3 wiring, rather than stubbing the Seat Run submission.
func TestExecSeatCheckoutThroughRealL1L3AndNodeCopyQueuesBoundJob(t *testing.T) {
	program, err := os.ReadFile(filepath.Join("..", "..", "runner", "ocihelper", "computer_seat_sync.py"))
	if err != nil {
		t.Fatal(err)
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	checkout := execTestRepo(t)
	writeExecFile(t, filepath.Join(checkout, "committed.txt"), "committed bytes\n")
	execTestGit(t, checkout, "add", ".")
	execTestGit(t, checkout, "commit", "-qm", "committed file")
	writeExecFile(t, filepath.Join(checkout, "sub", "tracked"), "modified bytes\n")
	writeExecFile(t, filepath.Join(checkout, "sub", "untracked.txt"), "untracked bytes\n")
	writeExecFile(t, filepath.Join(checkout, "sub", "ignored"), "ignored bytes\n")
	// Starting below the root also proves that working_directory preserves cwd.
	t.Chdir(filepath.Join(checkout, "sub"))
	t.Setenv("WEFTY_EXEC_STORAGE_ROOT", t.TempDir())
	h := newExecSeatIntegrationHarness(t)
	seatHome := t.TempDir()
	client, err := agent.NewClient(h.agentFabric, l3.DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	authority := workloadrunner.AttemptAuthority{NodeID: h.node.NodeID, BootSessionID: h.node.BootSessionID,
		JobID: h.claim.Job.JobID, AttemptID: h.claim.Lease.AttemptID, FencingToken: h.claim.Lease.FencingToken,
		WorkloadClass: contract.JobClassService}
	runtime := &execSeatSyncRuntime{home: seatHome, python: python, program: string(program), authority: authority}
	handler, err := agent.NewSeatCopyAcceptanceHandler(agent.SeatCopyAcceptanceConfig{
		Context: t.Context(), Fabric: h.agentFabric, Client: client, Authority: authority,
		ComputerID: h.computer.ComputerID, SeatID: h.seat.SeatID, Runtime: runtime,
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := h.agentFabric.Listen("tcp", "wefty://node/storage-node-seat-copy")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler}
	done := serveTestServer(h.ctx, func() error { return server.Serve(listener) })
	t.Cleanup(func() {
		_ = server.Close()
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			t.Error(err)
		}
	})
	ready, endpoint := true, "ws://"+listener.Addr().String()+contract.ComputerDisplayWebSocketPath
	if _, err := client.SetAttemptPublication(h.ctx, h.claim.Job.JobID, h.claim.Lease.AttemptID,
		l1.PublicationRequest{FencingToken: authority.FencingToken, Ready: &ready, DisplayEndpoint: &endpoint}); err != nil {
		t.Fatal(err)
	}

	claim := h.submitAndClaim(t, []string{"cat", "../committed.txt", "tracked", "untracked.txt"}, 2, "work/sub")
	params, _, err := contract.ParseSeatRunParams([]byte(claim.Job.Spec.Labels[contract.LabelRunParams]))
	if err != nil {
		t.Fatal(err)
	}
	// Resolve each path as the claimed Job would, from its dispatched directory.
	for name, want := range map[string]string{
		"../committed.txt": "committed bytes\n", "tracked": "modified bytes\n", "untracked.txt": "untracked bytes\n",
	} {
		data, err := os.ReadFile(filepath.Join(seatHome, filepath.FromSlash(params.SeatRun.WorkingDirectory), name))
		if err != nil || string(data) != want {
			t.Fatalf("uploaded %s=%q, want %q: %v", name, data, want, err)
		}
	}
	if _, err := os.Stat(filepath.Join(seatHome, "work", "sub", "ignored")); !os.IsNotExist(err) {
		t.Fatalf("Git-ignored file reached the Seat: %v", err)
	}
	if _, err := os.Stat(filepath.Join(seatHome, "work", ".git")); !os.IsNotExist(err) {
		t.Fatalf("uploaded copy contains Git metadata: %v", err)
	}
	page, err := h.store.GetSeatLogs(h.ctx, "operator", h.seat.SeatID, h.seat.Credential, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	var completed int
	for _, event := range page.Events {
		if event.Kind == l1.SeatEventCopyIn && event.Direction == "sync" && event.Path == "work" && event.Outcome == "completed" && event.Sync != nil && event.Sync.FilesWritten == 4 {
			completed++
		}
	}
	if completed != 1 {
		t.Fatalf("want one completed working-copy receipt, got %d: %+v", completed, page.Events)
	}
}

// Replace the container boundary only. Run the shipped sync program with the
// same commit trailer as ContainerdEngine.CopyComputerSeat, in a temporary home.
type execSeatSyncRuntime struct {
	home, python, program string
	authority             workloadrunner.AttemptAuthority
}

func (r *execSeatSyncRuntime) CopyComputerSeat(ctx context.Context, request ocihelper.CopyComputerSeatRequest, input io.Reader, emit func([]byte) error) (int64, error) {
	want := ocihelper.AttemptAuthority{NodeID: r.authority.NodeID, BootSessionID: r.authority.BootSessionID,
		JobID: r.authority.JobID, AttemptID: r.authority.AttemptID, FencingToken: r.authority.FencingToken, Class: r.authority.WorkloadClass}
	if request.Authority != want || request.Generation != 1 || request.Place != 1 || request.Direction != "sync" || request.Path != "work" {
		return 0, fmt.Errorf("unexpected copy binding: %+v", request)
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, r.python, "-I", "-S", "-c", r.program)
	command.WaitDelay = time.Second
	command.Dir = r.home
	command.Stdin = io.MultiReader(input, strings.NewReader("\nWEFTY_COPY_COMMIT_V1\n"))
	var diagnostic bytes.Buffer
	command.Stderr = &diagnostic
	output, err := command.Output()
	if err != nil {
		return 0, fmt.Errorf("Seat sync: %w: %s", err, &diagnostic)
	}
	var result struct {
		SizeBytes int64 `json:"size_bytes"`
	}
	if err := json.Unmarshal(diagnostic.Bytes(), &result); err != nil {
		return 0, err
	}
	return result.SizeBytes, emit(output)
}
