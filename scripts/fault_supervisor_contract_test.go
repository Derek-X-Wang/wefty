package scripts

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const faultSupervisorDirectory = "/tmp/wefty-oci-faults"

// A failed systemctl stop inside stop-helper-service-keep-socket must publish
// .failed and leave the shared root supervisor serving: under set -e an
// unhandled failure would kill the one executor every later lane fault needs,
// and the requesting test would only see a timeout.
func TestFaultSupervisorSurvivesFailedServiceOnlyStop(t *testing.T) {
	text := string(mustReadFile(t, "../scripts/oci-realtiming-fault-supervisor-v1.sh"))
	if !strings.Contains(text, faultSupervisorDirectory) {
		t.Fatalf("fault supervisor no longer uses %s", faultSupervisorDirectory)
	}

	t.Run("failed stop is recorded and the loop continues", func(t *testing.T) {
		supervisor := startFaultSupervisorWithFailingStop(t, text)
		supervisor.send(t, "stop-helper-service-keep-socket")
		failure := supervisor.await(t, "stop-helper-service-keep-socket.failed")
		if !strings.Contains(failure, "systemctl stop of the helper service failed") {
			t.Fatalf(".failed = %q, want the stop failure named", failure)
		}
		if _, err := os.Stat(filepath.Join(supervisor.directory, "stop-helper-service-keep-socket.done")); err == nil {
			t.Fatal("a failed stop was also acknowledged as done")
		}
		if supervisor.exited() {
			t.Fatal("the supervisor exited after a failed stop")
		}
		supervisor.send(t, "assert-helper-units-active")
		supervisor.await(t, "assert-helper-units-active.done")
	})

	t.Run("mutation control: an unhandled stop kills the supervisor", func(t *testing.T) {
		handled := "if ! systemctl stop wefty-oci-helper-realtiming.service; then\n" +
			"          record_action_failure 'systemctl stop of the helper service failed'\n" +
			"          continue\n" +
			"        fi\n"
		if strings.Count(text, handled) != 1 {
			t.Fatal("the handled service-only stop is not in the supervisor exactly once")
		}
		mutated := strings.Replace(text, handled, "systemctl stop wefty-oci-helper-realtiming.service\n", 1)
		supervisor := startFaultSupervisorWithFailingStop(t, mutated)
		supervisor.send(t, "stop-helper-service-keep-socket")
		deadline := time.Now().Add(10 * time.Second)
		for !supervisor.exited() {
			if time.Now().After(deadline) {
				t.Fatal("the mutated supervisor survived an unhandled stop failure; the control does not discriminate")
			}
			time.Sleep(25 * time.Millisecond)
		}
		if _, err := os.Stat(filepath.Join(supervisor.directory, "stop-helper-service-keep-socket.failed")); err == nil {
			t.Fatal("the mutated supervisor published .failed; the control does not discriminate")
		}
	})
}

type faultSupervisorHarness struct {
	directory string
	done      chan struct{}
}

// startFaultSupervisorWithFailingStop runs the supervisor text against a
// private fault directory with a systemctl stub whose stop always fails and
// whose is-active always succeeds.
func startFaultSupervisorWithFailingStop(t *testing.T, text string) *faultSupervisorHarness {
	t.Helper()
	root := t.TempDir()
	directory := filepath.Join(root, "faults")
	bin := filepath.Join(root, "bin")
	for _, path := range []string{directory, bin} {
		if err := os.Mkdir(path, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := syscall.Mkfifo(filepath.Join(directory, "control"), 0o600); err != nil {
		t.Fatal(err)
	}
	stub := "#!/bin/sh\ncase \"$1\" in\n  stop) echo 'stub: stop failed' >&2; exit 1 ;;\n  *) exit 0 ;;\nesac\n"
	if err := os.WriteFile(filepath.Join(bin, "systemctl"), []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(root, "supervisor.sh")
	if err := os.WriteFile(script, []byte(strings.ReplaceAll(text, faultSupervisorDirectory, directory)), 0o755); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("/bin/sh", script)
	command.Env = append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	harness := &faultSupervisorHarness{directory: directory, done: make(chan struct{})}
	go func() {
		_ = command.Wait()
		close(harness.done)
	}()
	t.Cleanup(func() {
		_ = command.Process.Kill()
		<-harness.done
	})
	return harness
}

func (harness *faultSupervisorHarness) exited() bool {
	select {
	case <-harness.done:
		return true
	default:
		return false
	}
}

func (harness *faultSupervisorHarness) send(t *testing.T, action string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		writer, err := os.OpenFile(filepath.Join(harness.directory, "control"), os.O_WRONLY|syscall.O_NONBLOCK, 0)
		if err == nil {
			_, writeErr := writer.Write([]byte(action + "\n"))
			if err = errors.Join(writeErr, writer.Close()); err == nil {
				return
			}
		}
		if harness.exited() || time.Now().After(deadline) {
			t.Fatalf("send %s to the fault supervisor: exited=%t err=%v", action, harness.exited(), err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func (harness *faultSupervisorHarness) await(t *testing.T, name string) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if payload, err := os.ReadFile(filepath.Join(harness.directory, name)); err == nil {
			return string(payload)
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("the fault supervisor never published %s (exited=%t)", name, harness.exited())
	return ""
}
