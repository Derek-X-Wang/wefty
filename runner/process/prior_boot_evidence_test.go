//go:build darwin || linux

package process

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Derek-X-Wang/wefty/agent/managedroot"
	"github.com/Derek-X-Wang/wefty/contract"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	wr "github.com/Derek-X-Wang/wefty/runner"
)

func TestPriorBootProcessGroupEvidence(t *testing.T) {
	hostID := "linux:00000000-0000-0000-0000-000000000001"
	for _, name := range []string{"alive", "gone", "reboot", "legacy", "query-error", "boot-query-error", "pending-spawn", "older-live-group", "malformed-host", "cross-platform-host", "clock-change"} {
		t.Run(name, func(t *testing.T) {
			command := exec.Command(helperPath, "hang-both-streams")
			if name == "gone" {
				command = exec.Command(helperPath, "exit", "0")
			}
			command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			joined := false
			defer func() {
				if !joined {
					_ = killProcessGroup(command.Process.Pid)
					_ = command.Wait()
				}
				waitForGroupGone(t, command.Process.Pid)
			}()
			if name == "gone" {
				if err := command.Wait(); err != nil {
					t.Fatal(err)
				}
				joined = true
			}
			a := NewAdapterForBoot(nil, "current")
			a.hostBootIdentity = func() (string, error) { return hostID, nil }
			records := []wr.ProcessGroupRecord{{AttemptID: "attempt", BootSessionID: "prior", HostBootID: hostID, ProcessGroupID: command.Process.Pid}}
			if name == "reboot" {
				// The real recorded group is alive. Only the boot-reader seam changes.
				records[0].HostBootID = "darwin:1:0:00000000-0000-0000-0000-000000000001"
				a.hostBootIdentity = func() (string, error) { return "darwin:2:0:00000000-0000-0000-0000-000000000002", nil }
				a.queryGroup = func(int) error { t.Error("reboot proof queried an old group"); return syscall.EPERM }
			}
			if name == "clock-change" {
				records[0].HostBootID = "darwin:1:0:00000000-0000-0000-0000-000000000001"
				a.hostBootIdentity = func() (string, error) { return "darwin:2:0:00000000-0000-0000-0000-000000000001", nil }
			}
			if name == "legacy" {
				records = nil
			}
			if name == "query-error" {
				a.queryGroup = func(int) error { return syscall.EPERM }
			}
			if name == "boot-query-error" {
				a.hostBootIdentity = func() (string, error) { return "", errors.New("kernel query failed") }
			}
			if name == "pending-spawn" {
				records[0].ProcessGroupID = 0
			}
			if name == "malformed-host" {
				records[0].HostBootID = "invalid"
			}
			if name == "cross-platform-host" {
				records[0].HostBootID = "darwin:1:0:00000000-0000-0000-0000-000000000001"
				a.hostBootIdentity = func() (string, error) { return "linux:00000000-0000-0000-0000-000000000001", nil }
			}
			if name == "older-live-group" {
				records = append([]wr.ProcessGroupRecord{{AttemptID: "newer", BootSessionID: "prior", HostBootID: "darwin:1:0:00000000-0000-0000-0000-000000000001"}}, records...)
				records[1].HostBootID = "darwin:2:0:00000000-0000-0000-0000-000000000002"
				records[1].BootSessionID = "even-older"
				a.hostBootIdentity = func() (string, error) { return "darwin:2:0:00000000-0000-0000-0000-000000000002", nil }
			}
			bootProofRead, groupProofRead := false, false
			readBoot, queryGroup := a.hostBootIdentity, a.queryGroup
			a.hostBootIdentity = func() (string, error) { bootProofRead = true; return readBoot() }
			a.queryGroup = func(group int) error { groupProofRead = true; return queryGroup(group) }
			receipt, err := a.ReapPriorBoot(context.Background(), wr.PriorBootReapRequest{NodeID: "node", JobID: "job", PriorBootSessionID: "prior", CurrentBootSessionID: "current", ProcessGroups: records})
			positive := name == "gone" || name == "reboot"
			if positive {
				// A positive result from the old unconditional-receipt path is not
				// proof. Observe the production kernel-reader seams as well as result.
				if !bootProofRead || (name == "gone" && !groupProofRead) {
					t.Error("positive receipt skipped the boot/group absence proof")
				}
				if err != nil || !receipt.RuntimeQuiesced || receipt.Evidence != wr.ReapEvidencePriorBootGuardian {
					t.Fatalf("receipt=%+v err=%v, want positive absence/reboot proof", receipt, err)
				}
			} else if !errors.Is(err, wr.ErrPriorBootEvidenceUnavailable) || receipt.RuntimeQuiesced {
				t.Fatalf("receipt=%+v err=%v, want closed refusal", receipt, err)
			}
			if name != "gone" {
				if err := syscall.Kill(-command.Process.Pid, 0); err != nil {
					t.Fatalf("read-only reap disturbed created group: %v", err)
				}
			}
		})
	}
}

func TestPriorBootKnownSpawnFailureReceipt(t *testing.T) {
	rootDir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	host := "linux:00000000-0000-0000-0000-000000000001"
	config := managedroot.Config{Root: rootDir, NodeID: "node", BootSessionID: "prior", HostBootIdentity: func() (string, error) { return host, nil }}
	manager, err := managedroot.Initialize(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.CreateService("job", 1); err != nil {
		t.Fatal(err)
	}
	if err := manager.RecordProcessAttempt("job", "attempt", 0); err != nil {
		t.Fatal(err)
	}
	adapter := NewAdapterForBoot(New(Config{GuardianExecutable: guardianAgentPath}), "prior")
	authority := testAuthority("job", "attempt", "fence")
	authority.NodeID, authority.BootSessionID = "node", "prior"
	result, err := adapter.Run(context.Background(), wr.Request{Authority: authority, LifetimeBoundary: wr.AgentBootLifetime, IdlePolicy: IgnoreIdle, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: filepath.Join(rootDir, "missing")}, Argv: []string{"missing"}, WorkingDirectory: rootDir}, ProcessStarted: func(int) error { t.Error("failed payload reported started"); return nil }}, nil)
	if err != nil || result.Outcome.SpawnError == nil {
		t.Fatalf("spawn result=%+v err=%v", result, err)
	}
	proof, err := adapter.ReapAndVerify(context.Background(), wr.ReapRequest{Authority: authority})
	if err != nil || !proof.RuntimeQuiesced {
		t.Fatalf("spawn failure receipt=%+v err=%v", proof, err)
	}
	if err := manager.RecordProcessNotSpawned("job", "attempt"); err != nil {
		t.Fatal(err)
	}
	config.BootSessionID = "current"
	manager, err = managedroot.Open(config)
	if err != nil {
		t.Fatal(err)
	}
	records, err := manager.ProcessAttempts("job")
	if err != nil {
		t.Fatal(err)
	}
	groups := []wr.ProcessGroupRecord{}
	for _, record := range records {
		groups = append(groups, wr.ProcessGroupRecord{AttemptID: record.AttemptID, BootSessionID: record.BootSessionID, HostBootID: record.HostBootID, ProcessGroupID: record.ProcessGroupID, NotSpawned: record.NotSpawned})
	}
	adapter = NewAdapterForBoot(nil, "current")
	adapter.hostBootIdentity = func() (string, error) { return host, nil }
	adapter.queryGroup = func(int) error { t.Error("known non-spawn queried a group"); return syscall.EPERM }
	receipt, err := adapter.ReapPriorBoot(context.Background(), wr.PriorBootReapRequest{NodeID: "node", JobID: "job", PriorBootSessionID: "prior", CurrentBootSessionID: "current", ProcessGroups: groups})
	if err != nil || !receipt.RuntimeQuiesced {
		t.Fatalf("prior-boot spawn failure receipt=%+v err=%v", receipt, err)
	}
	if err := manager.Remove(context.Background(), managedroot.Removal{JobID: "job", Generation: 1, RootInstanceID: manager.Manifest().RootInstanceID, CleanupFence: "fence", BootSessionID: "current", ProcessTreeReaped: receipt.RuntimeQuiesced}); err != nil {
		t.Fatal(err)
	}
}

func TestPriorBootLegacyRebootReceipt(t *testing.T) {
	rootDir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	host := "linux:00000000-0000-0000-0000-000000000001"
	config := managedroot.Config{Root: rootDir, NodeID: "node", BootSessionID: "prior", HostBootIdentity: func() (string, error) { return host, nil }}
	manager, err := managedroot.Initialize(config)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := manager.CreateService("job", 1)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(managedroot.OwnershipManifest{Version: 1, JobID: "job", RemovalGeneration: 1})
	if err := os.WriteFile(filepath.Join(paths.Root, managedroot.OwnershipManifestName), data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ProcessAttempts("job"); !errors.Is(err, managedroot.ErrProcessTreeNotReaped) {
		t.Fatalf("same-host-boot legacy err=%v", err)
	}
	config.BootSessionID = "current"
	host = "linux:00000000-0000-0000-0000-000000000002"
	manager, err = managedroot.Open(config)
	if err != nil {
		t.Fatal(err)
	}
	records, err := manager.ProcessAttempts("job")
	if err != nil {
		t.Fatal(err)
	}
	groups := []wr.ProcessGroupRecord{}
	for _, record := range records {
		groups = append(groups, wr.ProcessGroupRecord{AttemptID: record.AttemptID, BootSessionID: record.BootSessionID, HostBootID: record.HostBootID, ProcessGroupID: record.ProcessGroupID})
	}
	adapter := NewAdapterForBoot(nil, "current")
	adapter.hostBootIdentity = func() (string, error) { return host, nil }
	adapter.queryGroup = func(int) error { t.Error("legacy reboot queried unknown group"); return syscall.EPERM }
	receipt, err := adapter.ReapPriorBoot(context.Background(), wr.PriorBootReapRequest{NodeID: "node", JobID: "job", PriorBootSessionID: "prior", CurrentBootSessionID: "current", ProcessGroups: groups})
	if err != nil || !receipt.RuntimeQuiesced {
		t.Fatalf("legacy reboot receipt=%+v err=%v", receipt, err)
	}
}

// Opening the upgraded root must observe every legacy service before any
// inventory call, even when none of those services restarts before reboot.
func TestPriorBootLegacyEagerStampRebootRemoval(t *testing.T) {
	for _, openRoot := range []struct {
		name string
		open func(managedroot.Config) (*managedroot.Manager, error)
	}{{"Open", managedroot.Open}, {"Initialize", managedroot.Initialize}} {
		t.Run(openRoot.name, func(t *testing.T) {
			rootDir, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			host := "linux:00000000-0000-0000-0000-000000000001"
			config := managedroot.Config{Root: rootDir, NodeID: "node", BootSessionID: "old-agent", HostBootIdentity: func() (string, error) { return host, nil }}
			manager, err := managedroot.Initialize(config)
			if err != nil {
				t.Fatal(err)
			}
			for _, job := range []string{"job", "other-job"} {
				paths, err := manager.CreateService(job, 1)
				if err != nil {
					t.Fatal(err)
				}
				data, err := json.Marshal(managedroot.OwnershipManifest{Version: 1, JobID: job, RemovalGeneration: 1})
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(paths.Root, managedroot.OwnershipManifestName), data, 0600); err != nil {
					t.Fatal(err)
				}
				// A real v1 service has no process-attempts directory.
				if err := os.Remove(filepath.Join(paths.Root, "process-attempts")); err != nil {
					t.Fatal(err)
				}
			}
			config.BootSessionID = "upgraded-agent"
			manager, err = openRoot.open(config)
			if err != nil {
				t.Fatal(err)
			}
			// No ProcessAttempts or RecordProcessAttempt call in H1.
			for _, job := range []string{"job", "other-job"} {
				paths, err := manager.ServicePaths(job)
				if err != nil {
					t.Fatal(err)
				}
				data, err := os.ReadFile(filepath.Join(paths.Root, managedroot.OwnershipManifestName))
				if err != nil {
					t.Fatal(err)
				}
				var manifest managedroot.OwnershipManifest
				if err := json.Unmarshal(data, &manifest); err != nil {
					t.Fatal(err)
				}
				if manifest.Version != 2 || !manifest.LegacyProcessInventory || manifest.LegacyHostBootID != host || manifest.LegacyBootSessionID != "upgraded-agent" {
					t.Fatalf("legacy service %s was not stamped eagerly: %+v", job, manifest)
				}
			}
			config.BootSessionID = "current"
			host = "linux:00000000-0000-0000-0000-000000000002"
			manager, err = managedroot.Open(config)
			if err != nil {
				t.Fatal(err)
			}
			adapter := NewAdapterForBoot(nil, "current")
			adapter.hostBootIdentity = func() (string, error) { return host, nil }
			adapter.queryGroup = func(int) error { t.Error("legacy reboot queried unknown group"); return syscall.EPERM }
			for _, job := range []string{"job", "other-job"} {
				records, err := manager.ProcessAttempts(job)
				if err != nil {
					t.Fatalf("first inventory after reboot for %s: %v", job, err)
				}
				if len(records) != 1 || records[0].BootSessionID != "upgraded-agent" || records[0].HostBootID == host {
					t.Fatalf("original observation for %s lost: %+v", job, records)
				}
				record := records[0]
				receipt, err := adapter.ReapPriorBoot(context.Background(), wr.PriorBootReapRequest{NodeID: "node", JobID: job, PriorBootSessionID: "upgraded-agent", CurrentBootSessionID: "current", ProcessGroups: []wr.ProcessGroupRecord{{AttemptID: record.AttemptID, BootSessionID: record.BootSessionID, HostBootID: record.HostBootID, ProcessGroupID: record.ProcessGroupID}}})
				if err != nil || !receipt.RuntimeQuiesced || receipt.Evidence != wr.ReapEvidencePriorBootGuardian {
					t.Fatalf("legacy reboot receipt=%+v err=%v", receipt, err)
				}
				if err := manager.Remove(context.Background(), managedroot.Removal{JobID: job, Generation: 1, RootInstanceID: manager.Manifest().RootInstanceID, CleanupFence: "fence", BootSessionID: "current", ProcessTreeReaped: receipt.RuntimeQuiesced}); err != nil {
					t.Fatal(err)
				}
				paths, err := manager.ServicePaths(job)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(paths.Root); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("service still present: %v", err)
				}
			}
		})
	}
}
