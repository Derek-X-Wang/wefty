package scripts

import (
	"encoding/json"
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

// The Storage fault actions (#1634) make the real helper fail without a hook
// in a product binary. Each freeze must apply the immutable flag to exactly
// one directory under the runtime root, each thaw must clear it, a fault that
// cannot be applied must publish .failed and leave the supervisor serving,
// and the manufactured quarantine record must be the helper's own cleanup
// record for exactly the named generation.
func TestFaultSupervisorStorageFaultActions(t *testing.T) {
	text := string(mustReadFile(t, "../scripts/oci-realtiming-fault-supervisor-v1.sh"))
	const runtimeRoot = "/var/lib/wefty/oci"
	if !strings.Contains(text, runtimeRoot+"/computer-disk-quarantine") {
		t.Fatalf("fault supervisor no longer names the helper quarantine root under %s", runtimeRoot)
	}
	root := t.TempDir()
	runtimeDirectory := filepath.Join(root, "runtime")
	chattrLog := filepath.Join(root, "chattr.log")
	supervisor := startFaultSupervisorWithStubs(t, strings.ReplaceAll(text, runtimeRoot, runtimeDirectory), map[string]string{
		"chattr":  "#!/bin/sh\nprintf '%s %s\\n' \"$1\" \"$2\" >> " + chattrLog + "\n",
		"install": "#!/bin/sh\nfor last; do :; done\nmkdir -p \"$last\"\n",
	})
	const disk = "wefty-computer-disk-0123456789abcdef0123456789abcdef"
	diskRoot := filepath.Join(runtimeDirectory, "computer-disks", disk)

	supervisor.send(t, "freeze-computer-disk:"+disk)
	if failure := supervisor.await(t, "freeze-computer-disk:"+disk+".failed"); !strings.Contains(failure, disk) {
		t.Fatalf(".failed = %q, want the missing generation root named", failure)
	}
	if supervisor.exited() {
		t.Fatal("the supervisor exited after a fault it could not apply")
	}
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{
		"freeze-storage-roots", "thaw-storage-roots", "freeze-backup-roots", "thaw-backup-roots",
		"freeze-computer-disk:" + disk, "thaw-computer-disk:" + disk,
	} {
		supervisor.send(t, action)
		supervisor.await(t, action+".done")
	}
	want := strings.Join([]string{
		"+i " + filepath.Join(runtimeDirectory, "computer-disks"), "-i " + filepath.Join(runtimeDirectory, "computer-disks"),
		"+i " + filepath.Join(runtimeDirectory, "computer-backups"), "-i " + filepath.Join(runtimeDirectory, "computer-backups"),
		"+i " + diskRoot, "-i " + diskRoot,
	}, "\n") + "\n"
	if got := string(mustReadFile(t, chattrLog)); got != want {
		t.Fatalf("immutable flag changes =\n%s\nwant\n%s", got, want)
	}

	quarantine := "quarantine-computer-storage:" + disk + ":computer-1:storage-1:7"
	supervisor.send(t, quarantine)
	supervisor.await(t, quarantine+".done")
	recordPath := filepath.Join(runtimeDirectory, "computer-disk-quarantine", disk+"-cleanup-1.json")
	var record struct {
		Kind            string `json:"kind"`
		ReceiptID       string `json:"receipt_id"`
		VolumeKind      string `json:"volume_kind"`
		ComputerStorage struct {
			ComputerID        string `json:"computer_id"`
			StorageID         string `json:"storage_id"`
			StorageGeneration int64  `json:"storage_generation"`
		} `json:"computer_storage"`
		Removal struct {
			RemovalGeneration uint64 `json:"removal_generation"`
		} `json:"removal"`
	}
	if err := json.Unmarshal(mustReadFile(t, recordPath), &record); err != nil {
		t.Fatal(err)
	}
	// These are the fields computerDiskQuarantined reads, and the file name
	// carries the same removal generation.
	if record.Kind != "managed_volume_cleanup_quarantined" || record.ReceiptID == "" || record.VolumeKind != "computer_disk" ||
		record.ComputerStorage.ComputerID != "computer-1" || record.ComputerStorage.StorageID != "storage-1" ||
		record.ComputerStorage.StorageGeneration != 7 || record.Removal.RemovalGeneration != 1 {
		t.Fatalf("manufactured quarantine record = %#v", record)
	}
	clearAction := "clear-computer-storage-quarantine:" + disk
	supervisor.send(t, clearAction)
	supervisor.await(t, clearAction+".done")
	if _, err := os.Stat(recordPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the quarantine record survived its clear action: %v", err)
	}
	if supervisor.exited() {
		t.Fatal("the supervisor exited during the Storage fault actions")
	}
}

// startFaultSupervisorWithStubs runs the supervisor text against a private
// fault directory with the given command stubs first on PATH.
func startFaultSupervisorWithStubs(t *testing.T, text string, stubs map[string]string) *faultSupervisorHarness {
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
	for name, stub := range stubs {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(stub), 0o755); err != nil {
			t.Fatal(err)
		}
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
