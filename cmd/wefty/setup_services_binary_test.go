package main

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/l3"
)

// TestSetupInstalledServicesFindEachOtherFromTheRealBinaries is the installed
// stack minus launchd. Setup installs the four real binaries, and this test
// starts each service exactly as its plist tells launchd to: the same program,
// arguments, environment and folder, and nothing else. The real CLI, given
// only HOME, then finds L1 and L3, sees this machine's node ready, and runs a
// process job that the node claims. It starts real processes and binds
// loopback ports.
func TestSetupInstalledServicesFindEachOtherFromTheRealBinaries(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and starts the four wefty binaries")
	}
	built := t.TempDir()
	build := exec.Command("go", "build", "-o", built+string(filepath.Separator),
		".", "../wefty-l1", "../wefty-l3", "../wefty-agent")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build the wefty binaries: %v\n%s", err, output)
	}

	var (
		fixture      *setupFixture
		report       setupReport
		weftyCommand func(time.Duration, ...string) (int, string)
	)
	for attempt := 1; ; attempt++ {
		fixture = newSetupFixture(t)
		fixture.host.executable = filepath.Join(built, "wefty")
		// Reserved together, so the two ports differ.
		addresses := distinctLoopbackAddresses(t, 2)
		fixture.host.l1Address, fixture.host.l3Address = addresses[0], addresses[1]
		// A home reached through a symbolic link, as /tmp and /var are on
		// macOS: the node agent refuses a state root behind a link, so setup
		// must spell every path from the real one.
		linkedHome := filepath.Join(t.TempDir(), "home-link")
		if err := os.Symlink(fixture.home, linkedHome); err != nil {
			t.Fatal(err)
		}
		fixture.host.home = linkedHome
		report = fixture.setup(t, setupRequest{})

		var stops []func()
		for _, label := range []string{setupL1Label, setupL3Label, setupNodeAgentLabel} {
			stops = append(stops, startPlistService(t, launchagent.Path(fixture.home, label)))
		}
		// A new shell: HOME and nothing else that names wefty.
		environment := []string{"HOME=" + fixture.home, "PATH=/usr/bin:/bin"}
		cli := filepath.Join(fixture.home, ".local", "bin", "wefty")
		weftyCommand = func(budget time.Duration, args ...string) (int, string) {
			t.Helper()
			ctx, cancel := context.WithTimeout(t.Context(), budget)
			defer cancel()
			command := exec.CommandContext(ctx, cli, args...)
			command.Env = environment
			output, err := command.Output()
			var exitErr *exec.ExitError
			switch {
			case err == nil:
				return 0, string(output)
			case errors.As(err, &exitErr):
				return exitErr.ExitCode(), string(output)
			default:
				t.Fatalf("run %v: %v", args, err)
				return 0, ""
			}
		}

		var status clusterStatus
		conflict := false
		deadline := time.Now().Add(60 * time.Second)
		for {
			code, output := weftyCommand(30*time.Second, "--json", "status")
			status = clusterStatus{}
			_ = json.Unmarshal([]byte(output), &status)
			if code == 0 && status.LocalNode != nil {
				break
			}
			// Another process took a reserved port before L1 or L3 bound it.
			if strings.Contains(serviceLogs(report), "address already in use") {
				conflict = true
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the installed services never became ready (exit %d): %s\n%s", code, output, serviceLogs(report))
			}
			time.Sleep(200 * time.Millisecond)
		}
		if conflict {
			if attempt == 3 {
				t.Fatalf("the reserved ports were taken three times:\n%s", serviceLogs(report))
			}
			for index := len(stops) - 1; index >= 0; index-- {
				stops[index]()
			}
			t.Logf("attempt %d: a reserved port was taken before the bind; installing again with fresh ports", attempt)
			continue
		}
		if status.LocalNode.NodeID != report.NodeID || len(status.Nodes) != 1 || !status.Nodes[0].ThisMachine ||
			status.Services[0].Endpoint != report.L1Address || status.Services[1].Endpoint != report.L3Address {
			t.Fatalf("status = %+v", status)
		}
		break
	}

	script := filepath.Join(t.TempDir(), "installed.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho ran-on-the-installed-node\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	code, output := weftyCommand(30*time.Second, "--json", "submit", "--script", script, "--params", "{}",
		"--tag", contract.StableNodeTagPrefix+report.NodeID, "--idempotency-key", "installed-services")
	var accepted l3.RunAccepted
	if code != 0 || json.Unmarshal([]byte(output), &accepted) != nil || accepted.RunID == "" {
		t.Fatalf("submit exited %d: %s", code, output)
	}
	if code, output := weftyCommand(90*time.Second, "wait", accepted.RunID, "--timeout", "60s"); code != 0 {
		t.Fatalf("wait exited %d: %s\n%s", code, output, serviceLogs(report))
	}
	if code, output := weftyCommand(30*time.Second, "logs", accepted.RunID); code != 0 || !strings.Contains(output, "ran-on-the-installed-node") {
		t.Fatalf("logs exited %d: %s", code, output)
	}
}

// startPlistService runs a LaunchAgent's program with exactly the plist's
// arguments, environment, folder and log. It returns a stop function, which
// also runs when the test ends.
func startPlistService(t *testing.T, path string) func() {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	plist, err := decodePlist(payload)
	if err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	arguments, _ := plist["ProgramArguments"].([]any)
	if len(arguments) == 0 {
		t.Fatalf("%s has no program", path)
	}
	var argv []string
	for _, argument := range arguments {
		argv = append(argv, argument.(string))
	}
	var environment []string
	variables, _ := plist["EnvironmentVariables"].(map[string]any)
	for name, value := range variables {
		environment = append(environment, name+"="+value.(string))
	}
	logPath, _ := plist["StandardOutPath"].(string)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(argv[0], argv[1:]...)
	command.Env = environment
	command.Dir, _ = plist["WorkingDirectory"].(string)
	command.Stdout, command.Stderr = logFile, logFile
	if err := command.Start(); err != nil {
		t.Fatalf("start %s: %v", argv[0], err)
	}
	var once sync.Once
	stop := func() {
		once.Do(func() {
			// Services stop in reverse order of their start.
			_ = command.Process.Signal(syscall.SIGTERM)
			done := make(chan struct{})
			go func() { _ = command.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(20 * time.Second):
				_ = command.Process.Kill()
				<-done
			}
			_ = logFile.Close()
		})
	}
	t.Cleanup(stop)
	return stop
}

func serviceLogs(report setupReport) string {
	var logs strings.Builder
	for _, service := range report.Services {
		payload, _ := os.ReadFile(service.Log)
		fmt.Fprintf(&logs, "--- %s\n%s", service.Label, payload)
	}
	return logs.String()
}

// decodePlist reads the property-list subset setup writes: dict, array,
// string, integer, true and false.
func decodePlist(payload []byte) (map[string]any, error) {
	decoder := xml.NewDecoder(strings.NewReader(string(payload)))
	var value func(start xml.StartElement) (any, error)
	value = func(start xml.StartElement) (any, error) {
		switch start.Name.Local {
		case "string", "integer":
			var text string
			err := decoder.DecodeElement(&text, &start)
			return text, err
		case "true", "false":
			return start.Name.Local == "true", decoder.Skip()
		case "array", "dict":
			var items []any
			for {
				token, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				if _, ok := token.(xml.EndElement); ok {
					break
				}
				child, ok := token.(xml.StartElement)
				if !ok {
					continue
				}
				if start.Name.Local == "dict" && child.Name.Local == "key" {
					var key string
					if err := decoder.DecodeElement(&key, &child); err != nil {
						return nil, err
					}
					items = append(items, key)
					continue
				}
				item, err := value(child)
				if err != nil {
					return nil, err
				}
				items = append(items, item)
			}
			if start.Name.Local == "array" {
				return items, nil
			}
			dict := make(map[string]any, len(items)/2)
			for index := 0; index+1 < len(items); index += 2 {
				dict[items[index].(string)] = items[index+1]
			}
			return dict, nil
		default:
			return nil, fmt.Errorf("unexpected plist element %q", start.Name.Local)
		}
	}
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return nil, errors.New("no plist dict")
		}
		if err != nil {
			return nil, err
		}
		if start, ok := token.(xml.StartElement); ok && start.Name.Local == "dict" {
			root, err := value(start)
			if err != nil {
				return nil, err
			}
			return root.(map[string]any), nil
		}
	}
}
