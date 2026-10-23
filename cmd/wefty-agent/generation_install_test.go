package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestRetirementProtocolsInspectionPrecedesAllStartupIO(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("WEFTY_PROFILE", filepath.Join(home, "unreadable-profile.json"))
	oldArgs, oldOut := os.Args, os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	os.Args = []string{"wefty-agent", "--retirement-protocols"}
	os.Stdout = writer
	defer func() { os.Args = oldArgs; os.Stdout = oldOut }()
	if err := run(); err != nil {
		t.Fatal(err)
	}
	writer.Close()
	var record struct {
		Version   int   `json:"version"`
		Protocols []int `json:"retirement_protocol_versions"`
	}
	if err := json.NewDecoder(reader).Decode(&record); err != nil {
		t.Fatal(err)
	}
	if record.Version != 1 || len(record.Protocols) != 1 || record.Protocols[0] != 1 {
		t.Fatalf("wrong inspection: %+v", record)
	}
	entries, err := os.ReadDir(home)
	if err != nil || len(entries) != 0 {
		t.Fatalf("inspection initialized state: %v %v", entries, err)
	}
}

// The retained executable is a real child process. Its deliberately unreadable
// profile proves the candidate wait happens before profile/Fabric/runtime I/O.
func TestInstalledCandidateWaitsBeforeOpeningProfileOrRegistering(t *testing.T) {
	if os.Getenv("WEFTY_1177_CANDIDATE_HELPER") == "1" {
		flag.CommandLine = flag.NewFlagSet("candidate", flag.ContinueOnError)
		installedGenerationBootIdentity = func() (string, error) { return os.Getenv("WEFTY_1177_BOOT"), nil }
		installedGenerationProcessStartIdentity = func(int) (string, error) { return "child-process-start", nil }
		root, id, plists := os.Getenv("WEFTY_1177_ROOT"), os.Getenv("WEFTY_1177_ID"), os.Getenv("WEFTY_1177_PLISTS")
		os.Args = []string{os.Args[0], "--profile=/wefty-1177-deliberately-missing/profile.json", "--agent-generation-id=" + id, "--agent-generation-root=" + root, "--agent-generation-launch-agents=" + plists, "--ready-file=" + filepath.Join(root, id, "ready.json")}
		if err := run(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(92)
		}
		os.Exit(0)
	}
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data, plists := filepath.Join(home, "data"), filepath.Join(home, "LaunchAgents")
	for _, folder := range []string{data, plists} {
		if err := os.Mkdir(folder, 0700); err != nil {
			t.Fatal(err)
		}
	}
	const boot = "darwin:11111111-1111-1111-1111-111111111111"
	source, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	store := agentinstall.Store{Data: data, LaunchAgents: plists, ProcessStartIdentity: func(int) (string, error) { return "child-process-start", nil }}
	if _, err := store.Initialize("node", source, version, boot, []int{1}); err != nil {
		t.Fatal(err)
	}
	g, err := store.PrepareCandidate(source, version, boot, []int{1})
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), g.Executable, "-test.run=^TestInstalledCandidateWaitsBeforeOpeningProfileOrRegistering$")
	command.Env = append(os.Environ(), "HOME="+home, "WEFTY_1177_CANDIDATE_HELPER=1", "WEFTY_1177_BOOT="+boot, "WEFTY_1177_ROOT="+store.Root(), "WEFTY_1177_ID="+g.ID, "WEFTY_1177_PLISTS="+plists)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	defer func() {
		if !stopped {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	var ready agentinstall.Ready
	for {
		payload, err := os.ReadFile(store.ReadyPath(g.ID))
		if err == nil {
			if err := json.Unmarshal(payload, &ready); err != nil {
				t.Fatal(err)
			}
			break
		}
		if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		select {
		case <-deadline.C:
			t.Fatal("candidate did not reach its private wait point")
		case <-tick.C:
		}
	}
	if ready.PID != command.Process.Pid || ready.BootSessionID != g.CandidateBootSessionID || ready.Phase != "candidate_waiting" || ready.ProcessStartID != "child-process-start" {
		t.Fatalf("wrong process readiness: %+v", ready)
	}
	store.Manager = launchagent.Manager{Run: func(context.Context, string, ...string) ([]byte, error) {
		return fmt.Appendf(nil, "state = running\npid = %d\n", command.Process.Pid), nil
	}}
	if _, err := store.BindReady(t.Context(), g.ID, agentinstall.Binding{PID: command.Process.Pid, BootSessionID: g.CandidateBootSessionID, SystemBootID: boot}, g.CreatedAt); err != nil {
		t.Fatal(err)
	}
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	err = command.Wait()
	stopped = true
	if err != nil {
		t.Fatalf("candidate opened profile or failed clean stop: %v: %s", err, output.String())
	}
	for _, folder := range []string{".config", ".local", "Library"} {
		if _, err := os.Stat(filepath.Join(home, folder)); !os.IsNotExist(err) {
			t.Fatalf("candidate initialized user runtime state: %s %v", folder, err)
		}
	}
}
