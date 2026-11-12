package ocicontrol

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/runner/lima"
)

func hostVMBaseFinding(t *testing.T, report DoctorResponse) DiagnosticFinding {
	t.Helper()
	if err := report.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, item := range report.Findings {
		if item.Check == "host-vm-base" {
			return item
		}
	}
	t.Fatalf("host-vm-base finding missing: %+v", report.Findings)
	return DiagnosticFinding{}
}

func recordedGuestOS(t *testing.T, osRelease string) string {
	t.Helper()
	limaHome := t.TempDir()
	if err := os.Mkdir(filepath.Join(limaHome, "wefty-oci"), 0o700); err != nil {
		t.Fatal(err)
	}
	if osRelease != "" {
		if _, err := lima.RecordGuestOS(limaHome, "wefty-oci", []byte(osRelease), time.Date(2026, 11, 11, 20, 0, 0, 0, time.UTC)); err != nil {
			t.Fatal(err)
		}
	}
	path, err := lima.GuestOSObservationPath(limaHome, "wefty-oci")
	if err != nil {
		t.Fatal(err)
	}
	return path
}

// A base difference is advisory: it passes, names both values and the kernel,
// and never changes the convergence class (#1593).
func TestDoctorReportsOlderHostVMBaseAsAdvisory(t *testing.T) {
	now := time.Date(2026, 11, 11, 21, 0, 0, 0, time.UTC)
	config := healthyDoctorConfig(now, "")
	config.GuestOSObservationPath = recordedGuestOS(t, "ID=ubuntu\nVERSION_ID=\"24.04\"\n")
	config.GuestKernel = func(context.Context) (string, error) { return "6.8.0-88-generic", nil }
	report := BuildDoctor(t.Context(), config)
	item := hostVMBaseFinding(t, report)
	if item.Outcome != DiagnosticOK || item.Code != "oci_host_vm_base_upgrade_available" || item.Severity != DiagnosticInfo ||
		!strings.Contains(item.Detail, "guest_os=ubuntu-24.04") || !strings.Contains(item.Detail, "template="+lima.TemplateGuestOS) ||
		!strings.Contains(item.Detail, "kernel=6.8.0-88-generic") || !strings.Contains(item.Detail, "staying on the current base is supported") {
		t.Fatalf("older base finding = %+v", item)
	}
	if report.Convergence.Class != ConvergenceUnchanged || report.Convergence.Base == nil || !report.Convergence.Base.UpgradeAvailable ||
		report.Convergence.Base.Current != "ubuntu-24.04" || report.Convergence.GuestKernel != "6.8.0-88-generic" {
		t.Fatalf("convergence facts = %+v", report.Convergence)
	}
}

func TestDoctorReportsCurrentHostVMBaseWithoutKernel(t *testing.T) {
	now := time.Date(2026, 11, 11, 21, 0, 0, 0, time.UTC)
	config := healthyDoctorConfig(now, "")
	config.GuestOSObservationPath = recordedGuestOS(t, "ID=ubuntu\nVERSION_ID=\"26.04\"\n")
	config.GuestKernel = func(context.Context) (string, error) { return "", errors.New("instance is stopped") }
	report := BuildDoctor(t.Context(), config)
	item := hostVMBaseFinding(t, report)
	if item.Outcome != DiagnosticOK || item.Code != "oci_host_vm_base_current" || !strings.Contains(item.Detail, "kernel=not read") || report.Convergence.GuestKernel != "" {
		t.Fatalf("current base finding = %+v, facts=%+v", item, report.Convergence)
	}
}

func TestDoctorNeverTreatsUnobservedHostVMBaseAsDifferent(t *testing.T) {
	now := time.Date(2026, 11, 11, 21, 0, 0, 0, time.UTC)
	for name, write := range map[string]func(string) error{
		"missing":    func(string) error { return nil },
		"unreadable": func(path string) error { return os.WriteFile(path, []byte(`{"guest_os":"ubuntu-24.04"}`), 0o600) },
	} {
		t.Run(name, func(t *testing.T) {
			config := healthyDoctorConfig(now, "")
			config.GuestOSObservationPath = recordedGuestOS(t, "")
			if err := write(config.GuestOSObservationPath); err != nil {
				t.Fatal(err)
			}
			report := BuildDoctor(t.Context(), config)
			item := hostVMBaseFinding(t, report)
			if item.Outcome != DiagnosticNotRun || item.Code != "oci_host_vm_base_not_observed" || item.NotRunCause != NotRunSourceUnavailable ||
				report.Convergence.Base == nil || report.Convergence.Base.UpgradeAvailable || report.Convergence.Base.Current != "" {
				t.Fatalf("unobserved base = %+v, facts=%+v", item, report.Convergence.Base)
			}
			if name == "unreadable" && !strings.Contains(item.Detail, "unreadable") {
				t.Fatalf("unreadable record not named: %s", item.Detail)
			}
		})
	}
}

func TestDoctorHostVMBaseNotApplicableWithoutLima(t *testing.T) {
	report := BuildDoctor(t.Context(), healthyDoctorConfig(time.Date(2026, 11, 11, 21, 0, 0, 0, time.UTC), ""))
	item := hostVMBaseFinding(t, report)
	if item.Outcome != DiagnosticNotRun || item.Code != "oci_host_vm_base_not_applicable" || item.NotRunCause != NotRunNotApplicable || report.Convergence.Base != nil {
		t.Fatalf("no-Lima base finding = %+v", item)
	}
}

func TestDoctorConvergenceNamesDifferingFieldsAndAction(t *testing.T) {
	config := healthyDoctorConfig(time.Date(2026, 11, 11, 21, 0, 0, 0, time.UTC), "")
	desired := config.ReadDesiredSetupState
	config.ReadDesiredSetupState = func(path string) (SetupState, error) {
		state, err := desired(path)
		state.VMMemory, state.VMCPUs = "6GiB", 6
		return state, err
	}
	report := BuildDoctor(t.Context(), config)
	for _, item := range report.Findings {
		if item.Check == "convergence" {
			if item.Code != "oci_convergence_restart_required" || !strings.Contains(item.Detail, "differs: vm_memory, vm_cpus") || !strings.Contains(item.Detail, "to apply, rerun setup-oci with --apply-restart") {
				t.Fatalf("convergence finding = %+v", item)
			}
			return
		}
	}
	t.Fatal("convergence finding missing")
}

func TestConvergenceDifferencesNameEveryField(t *testing.T) {
	current := SetupState{VMMemory: "4GiB", VMCPUs: 4, VMDisk: "32GiB", VMType: "vz", HostMountRoot: "/a", ProbeDigest: "sha256:a", MemoryCapacityBytes: 1, MemoryReserveBytes: 1, SystemdVersion: 255, HelperRestartPolicy: "a"}
	if got := ConvergenceDifferences(current, current); len(got) != 0 {
		t.Fatalf("identical states differ: %v", got)
	}
	desired := SetupState{VMMemory: "6GiB", VMCPUs: 6, VMDisk: "64GiB", VMType: "qemu", HostMountRoot: "/b", ProbeDigest: "sha256:b", MemoryCapacityBytes: 2, MemoryReserveBytes: 2, SystemdVersion: 259, HelperRestartPolicy: "b"}
	want := "vm_memory,vm_cpus,vm_disk,vm_type,host_mount_root,probe_digest,memory_capacity_bytes,memory_reserve_bytes,systemd_version,helper_restart_policy"
	if got := strings.Join(ConvergenceDifferences(current, desired), ","); got != want {
		t.Fatalf("differences = %s", got)
	}
	if ConvergenceAction(ConvergenceUnchanged) != "" || ConvergenceAction(ConvergenceLiveSafe) != "" || ConvergenceAction(ConvergenceRecreateRequired) != "rerun setup-oci with --recreate" {
		t.Fatal("convergence actions drifted")
	}
}
