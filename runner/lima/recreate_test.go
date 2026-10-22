package lima

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// The external command boundary runs the real guest inspection script against
// durable files, while replacing Lima and systemd. No host VM is started.
type recreateFixture struct {
	barrier            *SupervisedBootBarrier
	root               string
	state              InstanceState
	commands           [][]string
	quiesced           bool
	recovered          bool
	inventoryOutput    []byte
	inventoryErr       error
	unit               string
	stopErr            bool
	helperStopped      bool
	afterInventory     func([]byte) ([]byte, error)
	deleteErr          error
	restoreCalls       int
	restoreContextErr  error
	restoreHasDeadline bool
}

func newRecreateFixture(t *testing.T) *recreateFixture {
	t.Helper()
	f := &recreateFixture{root: t.TempDir(), state: InstanceRunning}
	var err error
	f.root, err = filepath.EvalSymlinks(f.root)
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	marker := filepath.Join(bin, "helper-stopped")
	f.unit = GuestHelperPath + " " + ocihelper.InvocationArg + " --oci-allowed-mount-root=" + GuestAllowedMountRoot
	if err := os.WriteFile(filepath.Join(bin, "systemctl"), []byte(`#!/bin/sh
case "$1" in
show) printf '%s\n' "$TEST_HELPER_UNIT" ;;
stop) [ "$TEST_STOP_ERROR" = false ] && touch "$TEST_STOP_MARKER" ;;
start) rm -f "$TEST_STOP_MARKER" ;;
*) exit 1 ;;
esac
`), 0o700); err != nil {
		t.Fatal(err)
	}
	supervisor := newTestSupervisor(t, newMutableIntent(true), &supervisorRunner{})
	supervisor.config.run = func(ctx context.Context, _ string, args ...string) ([]byte, error) {
		f.commands = append(f.commands, append([]string(nil), args...))
		switch args[0] {
		case "list":
			return []byte(`{"name":"wefty-oci","status":"` + string(f.state) + `"}`), nil
		case "--tty=false":
			if len(args) == 8 && args[5] == "systemctl" && args[6] == "start" && args[7] == GuestHelperSocketUnit {
				f.restoreCalls++
				f.restoreContextErr = ctx.Err()
				_, f.restoreHasDeadline = ctx.Deadline()
				err := os.Remove(marker)
				if errors.Is(err, os.ErrNotExist) {
					err = nil
				}
				f.helperStopped = false
				return nil, err
			}
			if f.inventoryOutput != nil || f.inventoryErr != nil {
				return f.inventoryOutput, f.inventoryErr
			}
			// Execute sh, substituting only the guest runtime root and systemctl.
			if len(args) != 10 || args[4] != "sudo" || args[5] != "sh" {
				return nil, errors.New("unexpected guest command")
			}
			shellArgs := append([]string(nil), args[6:]...)
			shellArgs[len(shellArgs)-1] = f.root
			command := exec.CommandContext(ctx, "sh", shellArgs...)
			command.Env = append(os.Environ(), "PATH="+bin+":/usr/bin:/bin", "TEST_HELPER_UNIT="+f.unit, "TEST_STOP_MARKER="+marker, fmt.Sprintf("TEST_STOP_ERROR=%t", f.stopErr))
			output, err := command.CombinedOutput()
			_, statErr := os.Lstat(marker)
			f.helperStopped = statErr == nil
			if err == nil && f.afterInventory != nil {
				return f.afterInventory(output)
			}
			return output, err
		case "delete":
			if f.deleteErr != nil {
				return nil, f.deleteErr
			}
			return nil, os.RemoveAll(f.root)
		case "start":
			return nil, nil
		default:
			return nil, errors.New("unexpected Lima command")
		}
	}
	client := &ocihelper.Client{Version: ocihelper.ProtocolVersion, ExpectedChecksum: "sha256:" + strings.Repeat("a", 64), Dial: func(context.Context) (net.Conn, error) { return nil, errors.New("not reached") }}
	helper, err := ocihelper.NewBootBarrier(client, ocihelper.AcquireSessionRequest{NodeID: "node", BootSessionID: "boot"})
	if err != nil {
		t.Fatal(err)
	}
	f.barrier = &SupervisedBootBarrier{Supervisor: supervisor, Barrier: helper}
	return f
}

func (f *recreateFixture) recreate(t *testing.T) error {
	t.Helper()
	return f.barrier.Recreate(t.Context(), func(context.Context) error { f.quiesced = true; return nil }, func(context.Context) error { f.recovered = true; return nil }, TemplateConfig{Sizing: Sizing{Memory: "4GiB", CPUs: 4, Disk: "32GiB"}, HostAllowedMountRoot: "/Users/operator/wefty"})
}

func TestRecreateKeepsStoppedComputerStorageAndBackup(t *testing.T) {
	f := newRecreateFixture(t)
	for _, path := range []string{"computer-disks/wefty-computer-disk-stopped/disk.ext4", "computer-backups/wefty-backup-copy-kept/backup.ext4"} {
		full := filepath.Join(f.root, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte("retained bytes"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	err := f.recreate(t)
	var blocked *RecreateBlockedError
	if !errors.As(err, &blocked) || len(blocked.Blockers) != 2 || !strings.Contains(err.Error(), "wefty-computer-disk-stopped") || !strings.Contains(err.Error(), "wefty-backup-copy-kept") {
		t.Fatalf("recreate with stopped Computer = %v; commands=%v", err, f.commands)
	}
	for _, path := range []string{"computer-disks/wefty-computer-disk-stopped/disk.ext4", "computer-backups/wefty-backup-copy-kept/backup.ext4"} {
		if data, err := os.ReadFile(filepath.Join(f.root, path)); err != nil || string(data) != "retained bytes" {
			t.Fatalf("retained %s = %q, %v", path, data, err)
		}
	}
	for _, command := range f.commands {
		if command[0] == "delete" || command[0] == "start" {
			t.Fatalf("refusal changed host VM: %v", f.commands)
		}
	}
	if f.helperStopped {
		t.Fatal("refusal left helper socket activation stopped")
	}
}

func TestRecreateRefusesEveryRetainedOrUnknownDiskState(t *testing.T) {
	for _, test := range []struct {
		name    string
		prepare func(*recreateFixture)
		want    string
	}{
		{"Storage alone", func(f *recreateFixture) { mustRecreateFile(t, f.root, "computer-disks/retired-generation/disk.ext4") }, "retired-generation"},
		{"Backup alone", func(f *recreateFixture) {
			mustRecreateFile(t, f.root, "computer-backups/prune-pending-copy/backup.ext4")
		}, "prune-pending-copy"},
		{"staging", func(f *recreateFixture) {
			mustRecreateFile(t, f.root, "computer-disks/staging-generation/disk.ext4.staging")
		}, "staging-generation"},
		{"quarantine", func(f *recreateFixture) {
			mustRecreateFile(t, f.root, "computer-disk-quarantine/held-generation/disk.ext4")
		}, "held-generation"},
		{"reset", func(f *recreateFixture) { mustRecreateFile(t, f.root, "computer-storage-resets/reset-pending.json") }, "reset-pending.json"},
		{"stopped host VM", func(f *recreateFixture) { f.state = InstanceStopped }, "not known running"},
		{"broken host VM", func(f *recreateFixture) { f.state = InstanceBroken }, "not known running"},
		{"unknown host VM", func(f *recreateFixture) { f.state = InstanceUnknown }, "not known running"},
		{"inspection failed", func(f *recreateFixture) { f.inventoryErr = errors.New("unavailable private command output") }, "inspection failed"},
		{"helper cannot stop", func(f *recreateFixture) { f.stopErr = true }, "inspection failed"},
		{"custom helper root", func(f *recreateFixture) { f.unit += " --oci-runtime-root=/other-root" }, "inspection failed"},
		{"unrecognized helper", func(f *recreateFixture) { f.unit = "unknown helper" }, "inspection failed"},
		{"empty output", func(f *recreateFixture) { f.inventoryOutput = []byte{} }, "incomplete or invalid"},
		{"truncated inventory", func(f *recreateFixture) { f.inventoryOutput = []byte("wefty-recreate-inventory-v2\x00") }, "incomplete or invalid"},
		{"unknown inventory class", func(f *recreateFixture) {
			f.inventoryOutput = []byte("wefty-recreate-inventory-v2\x00unexpected/entry\x00\x00complete\x00")
		}, "incomplete or invalid"},
		{"invalid entry identity", func(f *recreateFixture) {
			f.inventoryOutput = []byte("wefty-recreate-inventory-v2\x00computer-disks/../outside\x00\x00complete\x00")
		}, "incomplete or invalid"},
		{"symlink inventory", func(f *recreateFixture) {
			if err := os.Symlink(t.TempDir(), filepath.Join(f.root, "computer-disks")); err != nil {
				t.Fatal(err)
			}
		}, "inspection failed"},
		{"non-directory inventory", func(f *recreateFixture) { mustRecreateFile(t, f.root, "computer-backups") }, "inspection failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newRecreateFixture(t)
			test.prepare(f)
			err := f.recreate(t)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("recreate = %v, want %q", err, test.want)
			}
			var blocked *RecreateBlockedError
			if !errors.As(err, &blocked) {
				t.Fatalf("untyped recreate refusal: %v", err)
			}
			if strings.Contains(err.Error(), "private command output") {
				t.Fatal("refusal disclosed command output")
			}
			if f.helperStopped {
				t.Fatal("refusal left helper socket activation stopped")
			}
			for _, command := range f.commands {
				if command[0] == "delete" || command[0] == "start" {
					t.Fatalf("refused recreate changed host VM: %v", f.commands)
				}
			}
		})
	}
}

func TestRecreateInspectionAcceptsRenderedGuestHelper(t *testing.T) {
	f := newRecreateFixture(t)
	unit := string(RenderGuestHelperServiceUnit(GuestHelperInstallConfig{HostMountRoot: "/Users/operator/wefty"}))
	var command []string
	for _, line := range strings.Split(unit, "\n") {
		if !strings.HasPrefix(line, "ExecStart=") {
			continue
		}
		for _, arg := range strings.Fields(strings.TrimPrefix(line, "ExecStart=")) {
			value, err := strconv.Unquote(arg)
			if err != nil {
				t.Fatal(err)
			}
			command = append(command, value)
		}
	}
	if len(command) == 0 {
		t.Fatal("rendered helper has no ExecStart")
	}
	f.unit = strings.Join(command, " ")
	if err := f.recreate(t); err != nil {
		t.Fatalf("guard drifted from rendered helper command: %v", err)
	}
}

func TestRecreateRestoresSocketAfterHostSideFailure(t *testing.T) {
	for _, failure := range []string{"invalid empty answer", "canceled caller", "command timeout", "delete fails"} {
		t.Run(failure, func(t *testing.T) {
			f := newRecreateFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch failure {
			case "invalid empty answer":
				f.afterInventory = func(out []byte) ([]byte, error) { return append([]byte("ssh warning\n"), out...), nil }
			case "canceled caller":
				f.afterInventory = func(out []byte) ([]byte, error) { cancel(); return nil, context.Canceled }
			case "command timeout":
				f.afterInventory = func(out []byte) ([]byte, error) { return nil, context.DeadlineExceeded }
			case "delete fails":
				f.deleteErr = errors.New("delete refused")
			}
			err := f.barrier.Recreate(ctx, func(context.Context) error { return nil }, func(context.Context) error { return nil }, TemplateConfig{Sizing: Sizing{Memory: "4GiB", CPUs: 4, Disk: "32GiB"}, HostAllowedMountRoot: "/Users/operator/wefty"})
			if err == nil {
				t.Fatal("expected recreate failure")
			}
			if f.helperStopped || f.restoreCalls != 1 || f.restoreContextErr != nil || !f.restoreHasDeadline {
				t.Fatalf("restoration: stopped=%t calls=%d context=%v deadline=%t; recreate=%v", f.helperStopped, f.restoreCalls, f.restoreContextErr, f.restoreHasDeadline, err)
			}
		})
	}
}

func mustRecreateFile(t *testing.T, root, path string) {
	t.Helper()
	full := filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte("retained bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestRecreateBackupSupersessionRecordsDoNotBlock(t *testing.T) {
	for _, copyRemains := range []bool{false, true} {
		t.Run(fmt.Sprintf("copy-remains=%t", copyRemains), func(t *testing.T) {
			f := newRecreateFixture(t)
			mustRecreateFile(t, f.root, "computer-backups/supersessions/pruned-copy.json")
			if copyRemains {
				mustRecreateFile(t, f.root, "computer-backups/live-copy/backup.ext4")
			}
			err := f.recreate(t)
			if copyRemains {
				if err == nil || !strings.Contains(err.Error(), "live-copy") {
					t.Fatalf("copy not protected: %v", err)
				}
				if strings.Contains(err.Error(), "Backup copy supersessions") {
					t.Fatalf("records mislabeled as a copy: %v", err)
				}
			} else if err != nil || !f.recovered {
				t.Fatalf("records-only recreate = %v, recovered=%t", err, f.recovered)
			}
		})
	}
}

func TestRecreateRefusalNamesComputerAndBackupIdentities(t *testing.T) {
	f := newRecreateFixture(t)
	storage := ocihelper.ComputerStorageReference{ComputerID: "computer_owner", StorageID: "storage_kept", StorageGeneration: 7}
	disk, err := ocihelper.DeterministicComputerDiskName(storage)
	if err != nil {
		t.Fatal(err)
	}
	for path, record := range map[string]any{
		"computer-disks/" + disk + "/attachment.json":         map[string]any{"version": 1, "storage": storage},
		"computer-backups/wefty-backup-copy-opaque/copy.json": map[string]any{"version": 1, "backup_id": "backup_kept", "copy_id": "copy_kept", "storage": storage},
		"computer-disk-quarantine/held/quarantine.json":       map[string]any{"storage": ocihelper.ComputerStorageReference{ComputerID: "computer_held", StorageID: "storage_held", StorageGeneration: 3}},
	} {
		payload, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		mustRecreateFile(t, f.root, path)
		if err := os.WriteFile(filepath.Join(f.root, path), payload, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	err = f.recreate(t)
	if err == nil {
		t.Fatal("recreate discarded retained data")
	}
	for _, want := range []string{"computer_owner", "storage_kept", "generation 7", "backup_kept", "copy_kept", "computer_held", "storage_held", "Remove or export", "then rerun setup-oci --recreate"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal lacks %q: %v", want, err)
		}
	}
}

func TestRecreateLabelsRefusedCopyRecords(t *testing.T) {
	f := newRecreateFixture(t)
	mustRecreateFile(t, f.root, "computer-disks/refused-copy/attachment.lock")
	mustRecreateFile(t, f.root, "computer-disks/refused-copy/copy-refused.json")
	err := f.recreate(t)
	if err == nil || !strings.Contains(err.Error(), "refused Computer copy record") {
		t.Fatalf("refused-copy label = %v", err)
	}
}

func TestRecreateMissingRuntimeRootNeedsReadableParent(t *testing.T) {
	for _, parentExists := range []bool{true, false} {
		t.Run(fmt.Sprintf("parent-exists=%t", parentExists), func(t *testing.T) {
			f := newRecreateFixture(t)
			if parentExists {
				f.root = filepath.Join(f.root, "oci")
			} else {
				f.root = filepath.Join(f.root, "absent-parent", "oci")
			}
			err := f.recreate(t)
			if parentExists {
				if err != nil || !f.recovered {
					t.Fatalf("known absent runtime root: %v", err)
				}
			} else if err == nil || f.helperStopped {
				t.Fatalf("unreadable parent: err=%v stopped=%t", err, f.helperStopped)
			}
		})
	}
}

func TestRecreateRejectsInvalidBackupRecordsFolder(t *testing.T) {
	for _, shape := range []string{"symlink folder", "nested directory", "symlink record", "non-directory"} {
		t.Run(shape, func(t *testing.T) {
			f := newRecreateFixture(t)
			path := filepath.Join(f.root, "computer-backups/supersessions")
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatal(err)
			}
			switch shape {
			case "symlink folder":
				if err := os.Symlink(t.TempDir(), path); err != nil {
					t.Fatal(err)
				}
			case "nested directory":
				if err := os.MkdirAll(filepath.Join(path, "nested"), 0o700); err != nil {
					t.Fatal(err)
				}
			case "symlink record":
				if err := os.MkdirAll(path, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("missing", filepath.Join(path, "record.json")); err != nil {
					t.Fatal(err)
				}
			case "non-directory":
				mustRecreateFile(t, f.root, "computer-backups/supersessions")
			}
			err := f.recreate(t)
			if err == nil || !strings.Contains(err.Error(), "inspection failed") || f.helperStopped {
				t.Fatalf("invalid records accepted or helper stopped: %v, %t", err, f.helperStopped)
			}
		})
	}
}
