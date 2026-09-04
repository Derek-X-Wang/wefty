package ocicontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

func healthyDoctorConfig(now time.Time, reason contract.CapabilityReasonCode) DoctorConfig {
	capabilities := map[string]bool{"kind:process": true, "kind:oci": true, "runtime_handler:io.containerd.runc.v2": true}
	missing := []string{}
	if reason != "" {
		delete(capabilities, "kind:oci")
		missing = []string{"kind:oci"}
	}
	return DoctorConfig{
		Clock:        &controlTestClock{now: now},
		HostPlatform: PlatformFacts{OS: "linux", Architecture: "amd64"},
		AgentUser:    "wefty-agent", LaunchUnit: "wefty-agent.service",
		CapabilitySnapshot: func() CapabilitySnapshot {
			probe := contract.CapabilityObservation{
				Revision: 9, ObservedAt: now.Add(-time.Minute), Capabilities: map[string]bool{
					"kind:process": true, "kind:oci": true, "runtime_handler:io.containerd.runc.v2": true,
				}, MissingCapabilities: []string{},
			}
			return CapabilitySnapshot{
				CapabilityObservation: contract.CapabilityObservation{
					Revision: 9, ObservedAt: now.Add(-2 * time.Minute), Capabilities: capabilities,
					MissingCapabilities: missing, ReasonCode: reason,
				},
				LastProbe: &probe,
			}
		},
		Intent: func(context.Context) (lima.OCIIntent, error) {
			return lima.OCIIntent{Version: lima.OCIIntentVersion, Revision: 4, Enabled: true, UpdatedAt: now.Add(-time.Hour)}, nil
		},
		Helper: func(context.Context) (HelperDoctorSnapshot, error) {
			return HelperDoctorSnapshot{
				ProtocolVersion: ocihelper.ProtocolVersion, Version: "v1.2.3", Checksum: "sha256:helper",
				InstanceID: "helper-instance", SessionGeneration: 7,
				RuntimePlatformRecorded: true,
				Runtime: ocihelper.DoctorStatus{
					RuntimePlatform:   ocihelper.OCIPlatform{OS: "linux", Architecture: "amd64"},
					ContainerdVersion: TestedContainerdVersion, ContainerdRead: ocihelper.DiagnosticReadReceipt{Outcome: ocihelper.DiagnosticReadOK},
					RuncVersion: TestedRuncVersion, RuncVersionSource: ocihelper.RuncVersionSourceConfiguredPath, RuncRead: ocihelper.DiagnosticReadReceipt{Outcome: ocihelper.DiagnosticReadOK},
					AllowedMountRoots: []string{"/srv/wefty", "/worktrees"}, MountRootsRead: ocihelper.DiagnosticReadReceipt{Outcome: ocihelper.DiagnosticReadOK},
					Cache: ocihelper.ImageCacheStatus{Bytes: 8 << 30, CapBytes: 16 << 30}, CacheRead: ocihelper.DiagnosticReadReceipt{Outcome: ocihelper.DiagnosticReadOK},
					LastProfile:   &ocihelper.ProfileReceipt{Computer: true, NetworkNamespacePresent: true, HelperNetworkNamespaceInode: "4026531992", TaskNetworkNamespaceInode: "4026532992", HostAbstractSocketVisible: false, ComputerNetworkAddress: "198.18.0.2", ComputerNetworkGateway: "198.18.0.1", MemoryLimitBytes: 2 << 30, MemoryMaxBytes: 2 << 30, MemoryOOMGroup: true, MemorySwapMaxBytes: 0, ComputerTmpfsCeilingBytes: 1600 << 20, LargestTmpfsCeilingBytes: 1 << 30, Warnings: []ocihelper.ProfileWarning{}},
					LastAdmission: &ocihelper.ResourceAdmissionReceipt{ObservedAt: now.Add(-30 * time.Second), Admitted: true, MemoryCapacityBytes: 4 << 30, MemoryReserveBytes: 1 << 30, MemoryCommittedBeforeBytes: 1 << 30, RequestedMemoryBytes: 1 << 30, MemoryCommittedAfterBytes: 2 << 30, MemTotalBytes: 4 << 30, MemAvailableBytes: 64 << 20, RequestedDiskBytes: 8 << 30, FilesystemAvailableBytes: 12 << 30, ComputerTmpfsCeilingBytes: 1600 << 20},
				},
				SweepReceiptRecorded: true,
				SweepReceipt: ocihelper.VerifiedSweepReceipt{SweepEpoch: "sweep-1", HelperSession: ocihelper.HelperSession{
					HelperInstanceID: "helper-instance", SessionGeneration: 7,
				}, VerifiedAbsent: true},
			}, nil
		},
		SetupStatePath: "/var/lib/wefty/setup.json",
		ReadSetupState: func(string) (SetupState, error) {
			return SetupState{VMMemory: "4GiB", VMCPUs: 4, VMDisk: "32GiB", VMType: "vz", HostMountRoot: "/srv/wefty", ProbeDigest: "sha256:probe", SystemdVersion: 255, HelperRestartPolicy: "geometric_capped_1s"}, nil
		},
		ReadDesiredSetupState: func(string) (SetupState, error) {
			return SetupState{VMMemory: "4GiB", VMCPUs: 4, VMDisk: "32GiB", VMType: "vz", HostMountRoot: "/srv/wefty", ProbeDigest: "sha256:probe", SystemdVersion: 255, HelperRestartPolicy: "geometric_capped_1s"}, nil
		},
		InstalledSystemdVersion: func(context.Context) (int, error) { return 255, nil },
		InstalledHelperServiceUnit: func(context.Context) (string, error) {
			return "[Unit]\nStartLimitIntervalSec=0\n[Service]\nRestart=on-failure\nRestartSec=250ms\nRestartSteps=6\nRestartMaxDelaySec=1s\n", nil
		},
		HelperHandshakeStalledWindows: func() uint64 { return 0 },
	}
}

func TestDoctorSurfacesComputerScreenIsolationReceipt(t *testing.T) {
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	report := BuildDoctor(t.Context(), healthyDoctorConfig(now, ""))
	if err := report.Validate(); err != nil {
		t.Fatal(err)
	}
	if report.ComputerScreenIsolation.Outcome != DiagnosticOK || !report.ComputerScreenIsolation.NetworkNamespacePresent || report.ComputerScreenIsolation.HostAbstractSocketVisible {
		t.Fatalf("screen isolation fact was not receipt-derived: %+v", report.ComputerScreenIsolation)
	}
	if !slices.ContainsFunc(report.Findings, func(item DiagnosticFinding) bool {
		return item.Check == "computer-screen-isolation" && item.Code == "oci_computer_screen_isolation_enforced" && item.Outcome == DiagnosticOK
	}) {
		t.Fatalf("screen isolation finding missing: %+v", report.Findings)
	}
	var human bytes.Buffer
	if err := WriteDoctorHuman(&human, report); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(human.String(), "SCREEN ISOLATION\tOK network_namespace_present=true helper_inode=4026531992 task_inode=4026532992 host_abstract_socket_visible=false address=198.18.0.2 gateway=198.18.0.1") {
		t.Fatalf("human doctor omitted screen isolation fact:\n%s", human.String())
	}
}

func TestDoctorFailsClosedWhenPostStartObservationWasSkipped(t *testing.T) {
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	config := healthyDoctorConfig(now, "")
	base := config.Helper
	config.Helper = func(ctx context.Context) (HelperDoctorSnapshot, error) {
		snapshot, err := base(ctx)
		snapshot.Runtime.LastProfile.HelperNetworkNamespaceInode = ""
		snapshot.Runtime.LastProfile.TaskNetworkNamespaceInode = ""
		return snapshot, err
	}
	report := BuildDoctor(t.Context(), config)
	if report.ComputerScreenIsolation.Outcome != DiagnosticFailed {
		t.Fatalf("skipped post-start observation = %+v", report.ComputerScreenIsolation)
	}
}

func TestDoctorFailsClosedOnVisibleComputerScreenSocket(t *testing.T) {
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	config := healthyDoctorConfig(now, "")
	base := config.Helper
	config.Helper = func(ctx context.Context) (HelperDoctorSnapshot, error) {
		snapshot, err := base(ctx)
		snapshot.Runtime.LastProfile.NetworkNamespacePresent = false
		snapshot.Runtime.LastProfile.HostAbstractSocketVisible = true
		return snapshot, err
	}
	report := BuildDoctor(t.Context(), config)
	if report.ComputerScreenIsolation.Outcome != DiagnosticFailed || !slices.ContainsFunc(report.Findings, func(item DiagnosticFinding) bool {
		return item.Check == "computer-screen-isolation" && item.Code == "oci_computer_screen_isolation_not_enforced" && item.Outcome == DiagnosticFailed
	}) {
		t.Fatalf("doctor did not fail closed: fact=%+v findings=%+v", report.ComputerScreenIsolation, report.Findings)
	}
}

func TestDoctorSurfacesComputerTmpfsCeilingPressureAsWarning(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	config := healthyDoctorConfig(now, "")
	base := config.Helper
	config.Helper = func(ctx context.Context) (HelperDoctorSnapshot, error) {
		snapshot, err := base(ctx)
		profile := snapshot.Runtime.LastProfile
		profile.MemoryLimitBytes = 512 << 20
		profile.Warnings = []ocihelper.ProfileWarning{
			{Code: ocihelper.ProfileWarningTmpfsCeilingExceedsMemory, Target: "/dev/shm", CeilingBytes: 1 << 30, LimitBytes: 512 << 20},
			{Code: ocihelper.ProfileWarningTmpfsCombinedExceedsMemory, CeilingBytes: 1600 << 20, LimitBytes: 512 << 20},
		}
		return snapshot, err
	}
	report := BuildDoctor(t.Context(), config)
	if err := report.Validate(); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, item := range report.Findings {
		if item.Check == "profile-ceilings" {
			found = item.Outcome == DiagnosticOK && item.Severity == DiagnosticWarn && item.Code == "oci_profile_tmpfs_ceilings_exceed_memory_limit"
		}
	}
	if !found || report.Profile.MemoryLimitBytes != 512<<20 || report.Profile.ComputerTmpfsCeilingBytes != 1600<<20 || len(report.Profile.Warnings) != 2 {
		t.Fatalf("profile warning was not assertion-derived: profile=%+v findings=%+v", report.Profile, report.Findings)
	}
}

func TestDoctorSurfacesDurableHelperRestartPolicyDrift(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	config := healthyDoctorConfig(now, "")
	config.ReadSetupState = func(string) (SetupState, error) {
		return SetupState{VMMemory: "4GiB", VMCPUs: 4, VMDisk: "32GiB", VMType: "vz", HostMountRoot: "/srv/wefty", ProbeDigest: "sha256:probe", SystemdVersion: 252, HelperRestartPolicy: "legacy_fixed_1s"}, nil
	}
	config.ReadDesiredSetupState = func(string) (SetupState, error) {
		return SetupState{VMMemory: "4GiB", VMCPUs: 4, VMDisk: "32GiB", VMType: "vz", HostMountRoot: "/srv/wefty", ProbeDigest: "sha256:probe", SystemdVersion: 252, HelperRestartPolicy: "legacy_fixed_1s"}, nil
	}
	config.InstalledSystemdVersion = func(context.Context) (int, error) { return 255, nil }
	report := BuildDoctor(t.Context(), config)
	if !slices.ContainsFunc(report.Findings, func(item DiagnosticFinding) bool {
		return item.Check == "helper-restart-policy" && item.Code == "oci_helper_restart_policy_drift" && item.Outcome == DiagnosticFailed
	}) {
		t.Fatalf("policy drift findings=%+v", report.Findings)
	}
}

func TestDoctorDetectsInstalledHelperUnitPolicyDrift(t *testing.T) {
	config := healthyDoctorConfig(time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC), "")
	config.InstalledHelperServiceUnit = func(context.Context) (string, error) {
		return "[Unit]\nStartLimitIntervalSec=0\n[Service]\nRestart=on-failure\nRestartSec=2s\nRestartSteps=6\nRestartMaxDelaySec=1s\n", nil
	}
	report := BuildDoctor(t.Context(), config)
	if !slices.ContainsFunc(report.Findings, func(item DiagnosticFinding) bool {
		return item.Check == "helper-restart-policy" && item.Code == "oci_helper_restart_policy_drift" && item.Outcome == DiagnosticFailed
	}) {
		t.Fatalf("installed unit drift findings=%+v", report.Findings)
	}
}

func TestDoctorReportsNonzeroNativeHelperHandshakeStallCount(t *testing.T) {
	config := healthyDoctorConfig(time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC), "")
	config.HelperHandshakeStalledWindows = func() uint64 { return 3 }
	report := BuildDoctor(t.Context(), config)
	if report.Helper.HandshakeStalledWindows != 3 || !slices.ContainsFunc(report.Findings, func(item DiagnosticFinding) bool {
		return item.Check == "helper-handshake-stalls" && item.Code == "oci_helper_handshake_stalls_observed" && item.Outcome == DiagnosticFailed
	}) {
		t.Fatalf("nonzero stall count was not typed: helper=%+v findings=%+v", report.Helper, report.Findings)
	}
}

func TestDoctorReportsQuarantinePayloadDropTimestamp(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	config := healthyDoctorConfig(now, "")
	mutateHelper(&config, func(snapshot *HelperDoctorSnapshot) {
		droppedAt := now.Add(-time.Hour)
		snapshot.SweepReceipt.VerifiedRetained.ComputerStorageQuarantined = []ocihelper.ComputerStorageRecoveryInventoryEntry{{
			DiskName: "wefty-computer-disk-example", Operation: "quarantine", Reason: "allocation_mismatch", PayloadDroppedAt: droppedAt.Format(time.RFC3339Nano),
		}}
		snapshot.SweepReceipt.ComputerStorageQuarantinedCount = 1
	})
	report := BuildDoctor(t.Context(), config)
	if len(report.ComputerStorageRecovery.Quarantined) != 1 || report.ComputerStorageRecovery.Quarantined[0].PayloadDroppedAt != now.Add(-time.Hour).Format(time.RFC3339Nano) {
		t.Fatalf("payload drop doctor facts=%+v", report.ComputerStorageRecovery)
	}
}

func TestDoctorSurfacesFactsOnlyResourceAdmissionWithoutFitForecast(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	report := BuildDoctor(t.Context(), healthyDoctorConfig(now, ""))
	if err := report.Validate(); err != nil {
		t.Fatal(err)
	}
	if report.ResourceAdmission == nil || report.ResourceAdmission.MemAvailableBytes != 64<<20 ||
		report.ResourceAdmission.MemoryCommittedAfterBytes != 2<<30 || report.ResourceAdmission.ComputerTmpfsCeilingBytes != 1600<<20 {
		t.Fatalf("resource admission facts = %+v", report.ResourceAdmission)
	}
	for _, finding := range report.Findings {
		if finding.Check == "resource-admission" && finding.Code == "oci_resource_admission_admitted" && finding.Outcome == DiagnosticOK {
			return
		}
	}
	t.Fatalf("resource admission finding missing: %+v", report.Findings)
}

func TestDoctorDoesNotRenderARefusedAdmissionAsHealthy(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	config := healthyDoctorConfig(now, "")
	base := config.Helper
	config.Helper = func(ctx context.Context) (HelperDoctorSnapshot, error) {
		snapshot, err := base(ctx)
		snapshot.Runtime.LastAdmission.Admitted = false
		snapshot.Runtime.LastAdmission.FailureCode = ocihelper.CodeInsufficientMemory
		snapshot.Runtime.LastAdmission.MemoryCommittedAfterBytes = snapshot.Runtime.LastAdmission.MemoryCommittedBeforeBytes
		return snapshot, err
	}
	report := BuildDoctor(t.Context(), config)
	for _, finding := range report.Findings {
		if finding.Check == "resource-admission" && finding.Code == "oci_resource_admission_refused" && finding.Outcome == DiagnosticFailed && finding.Severity == DiagnosticWarn {
			return
		}
	}
	t.Fatalf("refused admission was not preserved as typed non-healthy evidence: %+v", report.Findings)
}

func setProbeReason(config *DoctorConfig, reason contract.CapabilityReasonCode) {
	base := config.CapabilitySnapshot
	config.CapabilitySnapshot = func() CapabilitySnapshot {
		snapshot := base()
		delete(snapshot.Capabilities, "kind:oci")
		snapshot.MissingCapabilities = []string{"kind:oci"}
		snapshot.ReasonCode = reason
		return snapshot
	}
}

func setLastProbeReason(config *DoctorConfig, reason contract.CapabilityReasonCode) {
	base := config.CapabilitySnapshot
	config.CapabilitySnapshot = func() CapabilitySnapshot {
		snapshot := base()
		if snapshot.LastProbe == nil {
			return snapshot
		}
		delete(snapshot.LastProbe.Capabilities, "kind:oci")
		snapshot.LastProbe.MissingCapabilities = []string{"kind:oci"}
		snapshot.LastProbe.ReasonCode = reason
		return snapshot
	}
}

func mutateHelper(config *DoctorConfig, mutate func(*HelperDoctorSnapshot)) {
	base := config.Helper
	config.Helper = func(ctx context.Context) (HelperDoctorSnapshot, error) {
		snapshot, err := base(ctx)
		mutate(&snapshot)
		return snapshot, err
	}
}

func mutateDesired(config *DoctorConfig, mutate func(*SetupState)) {
	base := config.ReadDesiredSetupState
	config.ReadDesiredSetupState = func(path string) (SetupState, error) {
		state, err := base(path)
		mutate(&state)
		return state, err
	}
}

func setLimaReason(config *DoctorConfig, state lima.InstanceState, reason contract.CapabilityReasonCode) {
	config.HostPlatform.OS = "darwin"
	config.LimaFacts = func() lima.SupervisorFacts {
		return lima.SupervisorFacts{Instance: lima.DefaultInstanceName, State: state, Enabled: true, Recovering: true, ReasonCode: reason, ObservedAt: config.Clock.Now()}
	}
}

func TestDoctorGoldenParityCoversEveryReasonAndL1MetadataTuple(t *testing.T) {
	now := time.Date(2026, 8, 28, 18, 0, 0, 0, time.UTC)
	type reasonRow struct {
		reason     contract.CapabilityReasonCode
		derivation string
		configure  func(*DoctorConfig)
	}
	direct := "derived from a real doctor condition"
	passThrough := "not derivable by doctor (surfaced via probe pass-through)"
	rows := []reasonRow{
		{contract.CapabilityReasonOCIIntentDisabled, direct, func(config *DoctorConfig) {
			config.Intent = func(context.Context) (lima.OCIIntent, error) {
				return lima.OCIIntent{Version: lima.OCIIntentVersion, Revision: 1, Enabled: false, UpdatedAt: now}, nil
			}
		}},
		{contract.CapabilityReasonPrerequisiteMissing, direct, func(config *DoctorConfig) {
			config.ReadSetupState = func(string) (SetupState, error) { return SetupState{}, os.ErrNotExist }
		}},
		{contract.CapabilityReasonRuntimeVersionUnsupported, direct, func(config *DoctorConfig) {
			mutateHelper(config, func(snapshot *HelperDoctorSnapshot) { snapshot.Runtime.ContainerdVersion = "1.7.0" })
		}},
		{contract.CapabilityReasonHelperUnreachable, direct, func(config *DoctorConfig) {
			config.Helper = func(context.Context) (HelperDoctorSnapshot, error) {
				return HelperDoctorSnapshot{}, errors.New("dial refused")
			}
		}},
		{contract.CapabilityReasonHelperUnitUnavailable, passThrough, func(config *DoctorConfig) {
			setProbeReason(config, contract.CapabilityReasonHelperUnitUnavailable)
		}},
		{contract.CapabilityReasonHelperHandshakeStalled, passThrough, func(config *DoctorConfig) {
			setProbeReason(config, contract.CapabilityReasonHelperHandshakeStalled)
		}},
		{contract.CapabilityReasonHelperHandshakeStalledPersistent, passThrough, func(config *DoctorConfig) {
			setProbeReason(config, contract.CapabilityReasonHelperHandshakeStalledPersistent)
		}},
		{contract.CapabilityReasonHelperVersionMismatch, direct, func(config *DoctorConfig) {
			mutateHelper(config, func(snapshot *HelperDoctorSnapshot) { snapshot.ProtocolVersion++ })
		}},
		{contract.CapabilityReasonHelperHandshakeFailed, direct, func(config *DoctorConfig) {
			mutateHelper(config, func(snapshot *HelperDoctorSnapshot) { snapshot.Checksum = "" })
		}},
		{contract.CapabilityReasonBootSweepFailed, direct, func(config *DoctorConfig) {
			mutateHelper(config, func(snapshot *HelperDoctorSnapshot) { snapshot.SweepReceipt.SweepEpoch = "" })
		}},
		{contract.CapabilityReasonProbeFailed, direct, func(config *DoctorConfig) { setLastProbeReason(config, contract.CapabilityReasonProbeFailed) }},
		{contract.CapabilityReasonLimaStopped, direct, func(config *DoctorConfig) {
			setLimaReason(config, lima.InstanceStopped, contract.CapabilityReasonLimaStopped)
		}},
		{contract.CapabilityReasonLimaBroken, direct, func(config *DoctorConfig) {
			setLimaReason(config, lima.InstanceBroken, contract.CapabilityReasonLimaBroken)
		}},
		{contract.CapabilityReasonLimaStartTimeout, direct, func(config *DoctorConfig) {
			setLimaReason(config, lima.InstanceStopped, contract.CapabilityReasonLimaStartTimeout)
		}},
		{contract.CapabilityReasonTemplateRestartRequired, direct, func(config *DoctorConfig) { mutateDesired(config, func(state *SetupState) { state.VMMemory = "8GiB" }) }},
		{contract.CapabilityReasonTemplateRecreateRequired, direct, func(config *DoctorConfig) {
			mutateDesired(config, func(state *SetupState) { state.HostMountRoot = "/srv/other" })
		}},
		{contract.CapabilityReasonMountRootUnavailable, direct, func(config *DoctorConfig) {
			mutateHelper(config, func(snapshot *HelperDoctorSnapshot) { snapshot.Runtime.AllowedMountRoots = []string{"/different"} })
		}},
		{contract.CapabilityReasonLocalPermissionDenied, passThrough, func(config *DoctorConfig) { setProbeReason(config, contract.CapabilityReasonLocalPermissionDenied) }},
	}
	seen := make(map[contract.CapabilityReasonCode]string)
	for _, row := range rows {
		config := healthyDoctorConfig(now, "")
		row.configure(&config)
		report := BuildDoctor(t.Context(), config)
		if err := report.Validate(); err != nil {
			t.Fatalf("reason %s report: %v", row.reason, err)
		}
		if report.Probe.CapabilityRevision != 9 || !report.Probe.CapabilityObservedAt.Equal(now.Add(-2*time.Minute)) ||
			report.Probe.Capabilities == nil || report.Probe.MissingCapabilities == nil {
			t.Fatalf("reason %s lost L1 metadata tuple: %+v", row.reason, report.Probe)
		}
		if row.derivation == passThrough && (report.Probe.Verdict != "passed" || report.Probe.CapabilityReasonCode != row.reason) {
			t.Fatalf("capability restriction was misreported as the last probe: %+v", report.Probe)
		}
		found := false
		for _, item := range report.Findings {
			if item.ReasonCode == row.reason {
				found = true
			}
		}
		if !found {
			t.Fatalf("reason %s (%s) was not surfaced from its condition: %+v", row.reason, row.derivation, report.Findings)
		}
		var human bytes.Buffer
		if err := WriteDoctorHuman(&human, report); err != nil {
			t.Fatal(err)
		}
		payload, err := json.Marshal(report)
		if err != nil {
			t.Fatal(err)
		}
		for _, golden := range []string{string(row.reason), "kind:oci"} {
			if !bytes.Contains(payload, []byte(golden)) || !strings.Contains(human.String(), golden) {
				t.Fatalf("reason %s missing golden %q from JSON or human parity", row.reason, golden)
			}
		}
		if !bytes.Contains(payload, []byte(`"capability_revision":9`)) || !strings.Contains(human.String(), "revision=9") {
			t.Fatalf("reason %s lost Capability revision parity", row.reason)
		}
		seen[row.reason] = row.derivation
	}
	for _, reason := range StableDoctorReasonCodes() {
		if seen[reason] == "" {
			t.Fatalf("doctor parity table did not classify §8.3 reason %s", reason)
		}
	}
}

func TestDoctorAdversarialRowsFailClosedWithoutMutation(t *testing.T) {
	now := time.Date(2026, 8, 28, 19, 0, 0, 0, time.UTC)
	find := func(t *testing.T, report DoctorResponse, check string) DiagnosticFinding {
		t.Helper()
		for _, item := range report.Findings {
			if item.Check == check {
				return item
			}
		}
		t.Fatalf("finding %s missing", check)
		return DiagnosticFinding{}
	}

	t.Run("missing intent", func(t *testing.T) {
		config := healthyDoctorConfig(now, "")
		config.Intent = (lima.FileIntentSource{Path: filepath.Join(t.TempDir(), "missing-intent.json")}).ReadIntent
		report := BuildDoctor(t.Context(), config)
		item := find(t, report, "intent")
		if item.Outcome != DiagnosticFailed || item.ReasonCode != contract.CapabilityReasonPrerequisiteMissing || item.NotRunCause != "" || report.Intent.Enabled {
			t.Fatalf("missing intent = %+v %+v", item, report.Intent)
		}
	})

	t.Run("corrupt intent", func(t *testing.T) {
		config := healthyDoctorConfig(now, "")
		path := filepath.Join(t.TempDir(), "intent.json")
		if err := os.WriteFile(path, []byte(`{"version":1`), 0o600); err != nil {
			t.Fatal(err)
		}
		config.Intent = (lima.FileIntentSource{Path: path}).ReadIntent
		item := find(t, BuildDoctor(t.Context(), config), "intent")
		if item.Outcome != DiagnosticNotRun || item.ReasonCode != "" || item.NotRunCause != NotRunSourceUnavailable {
			t.Fatalf("corrupt intent = %+v", item)
		}
	})

	t.Run("helper socket present but unreachable", func(t *testing.T) {
		config := healthyDoctorConfig(now, "")
		config.Helper = func(context.Context) (HelperDoctorSnapshot, error) {
			return HelperDoctorSnapshot{}, errors.New("dial failed")
		}
		report := BuildDoctor(t.Context(), config)
		if item := find(t, report, "helper-handshake"); item.Outcome != DiagnosticFailed || item.ReasonCode != contract.CapabilityReasonHelperUnreachable {
			t.Fatalf("helper finding = %+v", item)
		}
		for _, check := range []string{"boot-sweep", "runtime-platform", "runtime-versions", "cache", "mount-roots"} {
			if item := find(t, report, check); item.Outcome != DiagnosticNotRun || item.ReasonCode != "" || !item.NotRunCause.Valid() {
				t.Fatalf("dependent %s claimed execution: %+v", check, item)
			}
		}
	})

	t.Run("handshake survives later diagnostic failure", func(t *testing.T) {
		config := healthyDoctorConfig(now, "")
		mutateHelper(&config, func(snapshot *HelperDoctorSnapshot) { snapshot.RuntimeError = errors.New("runc read failed") })
		report := BuildDoctor(t.Context(), config)
		if item := find(t, report, "helper-handshake"); item.Outcome != DiagnosticOK || report.Helper.InstanceID != "helper-instance" {
			t.Fatalf("handshake evidence was discarded: %+v helper=%+v", item, report.Helper)
		}
		if item := find(t, report, "runtime-platform"); item.Outcome != DiagnosticOK {
			t.Fatalf("later helper failure erased recorded probe platform: %+v", item)
		}
		for _, check := range []string{"runtime-versions", "cache", "mount-roots"} {
			if item := find(t, report, check); item.Outcome != DiagnosticNotRun || item.ReasonCode != "" {
				t.Fatalf("failed dependent read %s = %+v", check, item)
			}
		}
	})

	t.Run("partial helper reads degrade only their check", func(t *testing.T) {
		config := healthyDoctorConfig(now, "")
		mutateHelper(&config, func(snapshot *HelperDoctorSnapshot) {
			snapshot.Runtime.RuncVersion = ""
			snapshot.Runtime.RuncRead = ocihelper.DiagnosticReadReceipt{Outcome: ocihelper.DiagnosticReadFailed, ErrorCode: ocihelper.DiagnosticErrorRuncVersion}
		})
		report := BuildDoctor(t.Context(), config)
		if item := find(t, report, "helper-handshake"); item.Outcome != DiagnosticOK {
			t.Fatalf("partial read erased handshake: %+v", item)
		}
		if item := find(t, report, "runtime-versions"); item.Outcome != DiagnosticFailed || item.ReasonCode != "" {
			t.Fatalf("runc failure = %+v", item)
		}
		for _, check := range []string{"cache", "mount-roots"} {
			if item := find(t, report, check); item.Outcome != DiagnosticOK {
				t.Fatalf("runc failure degraded sibling %s: %+v", check, item)
			}
		}
	})

	t.Run("stale capability revision", func(t *testing.T) {
		config := healthyDoctorConfig(now, "")
		base := config.CapabilitySnapshot
		config.CapabilitySnapshot = func() CapabilitySnapshot {
			snapshot := base()
			snapshot.PendingPublicationRevision = snapshot.Revision
			return snapshot
		}
		item := find(t, BuildDoctor(t.Context(), config), "capability-revision")
		if item.Outcome != DiagnosticFailed || item.Code != "oci_capability_revision_pending" {
			t.Fatalf("stale capability = %+v", item)
		}
	})

	t.Run("cache over bound", func(t *testing.T) {
		config := healthyDoctorConfig(now, "")
		base := config.Helper
		config.Helper = func(ctx context.Context) (HelperDoctorSnapshot, error) {
			snapshot, err := base(ctx)
			snapshot.Runtime.Cache.Bytes = snapshot.Runtime.Cache.CapBytes + 1
			return snapshot, err
		}
		report := BuildDoctor(t.Context(), config)
		item := find(t, report, "cache")
		if item.Outcome != DiagnosticFailed || item.Code != "oci_cache_over_bound" || report.Cache.WithinBound {
			t.Fatalf("cache pressure = %+v %+v", item, report.Cache)
		}
	})

	t.Run("cache eviction error is sanitized and failed", func(t *testing.T) {
		config := healthyDoctorConfig(now, "")
		mutateHelper(&config, func(snapshot *HelperDoctorSnapshot) {
			snapshot.Runtime.CacheLastErrorCode = ocihelper.DiagnosticErrorCacheEviction
		})
		report := BuildDoctor(t.Context(), config)
		item := find(t, report, "cache")
		if item.Outcome != DiagnosticFailed || item.Code != "oci_cache_eviction_failed" || strings.Contains(item.Detail, "permission") {
			t.Fatalf("cache eviction error = %+v", item)
		}
	})

	t.Run("desired convergence unavailable", func(t *testing.T) {
		config := healthyDoctorConfig(now, "")
		config.ReadDesiredSetupState = func(string) (SetupState, error) { return SetupState{}, os.ErrNotExist }
		report := BuildDoctor(t.Context(), config)
		item := find(t, report, "convergence")
		if item.Outcome != DiagnosticNotRun || item.ReasonCode != "" || item.NotRunCause != NotRunDesiredUnavailable {
			t.Fatalf("missing desired setup = %+v", item)
		}
		policy := find(t, report, "helper-restart-policy")
		if policy.Outcome != DiagnosticNotRun || policy.Code != "oci_helper_restart_policy_not_read" || policy.NotRunCause != NotRunDesiredUnavailable {
			t.Fatalf("missing desired policy = %+v", policy)
		}
	})
}

func TestDoctorNotRunIsFirstClassAndNeverTurnsIntoOK(t *testing.T) {
	report := BuildDoctor(t.Context(), DoctorConfig{
		Clock:        &controlTestClock{now: time.Date(2026, 8, 28, 20, 0, 0, 0, time.UTC)},
		HostPlatform: PlatformFacts{OS: "linux", Architecture: "arm64"}, AgentUser: "agent",
	})
	if err := report.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, check := range []string{"intent", "capability-revision", "capability-observation", "probe", "lima", "helper-handshake", "boot-sweep", "computer-storage-recovery", "runtime-platform", "runtime-versions", "cache", "mount-roots", "convergence", "helper-restart-policy"} {
		found := false
		for _, item := range report.Findings {
			if item.Check == check {
				found = true
				if item.Outcome == DiagnosticOK || item.ReasonCode != "" || !item.NotRunCause.Valid() {
					t.Fatalf("unexecuted check %s carried execution or diagnosis: %+v", check, item)
				}
			}
		}
		if !found {
			t.Fatalf("unexecuted check %s missing", check)
		}
	}
	if report.ComputerStorageRecovery.Outcome != DiagnosticNotRun {
		t.Fatalf("unread recovery facts = %+v", report.ComputerStorageRecovery)
	}
}

func TestDoctorReadsSourcesExactlyOnceAndSurfacesUIDLimitation(t *testing.T) {
	now := time.Date(2026, 8, 28, 21, 0, 0, 0, time.UTC)
	config := healthyDoctorConfig(now, "")
	intentReads, capabilityReads, helperReads, setupReads, desiredReads := 0, 0, 0, 0, 0
	intent := config.Intent
	capability := config.CapabilitySnapshot
	helper := config.Helper
	setup := config.ReadSetupState
	desired := config.ReadDesiredSetupState
	config.Intent = func(ctx context.Context) (lima.OCIIntent, error) { intentReads++; return intent(ctx) }
	config.CapabilitySnapshot = func() CapabilitySnapshot { capabilityReads++; return capability() }
	config.Helper = func(ctx context.Context) (HelperDoctorSnapshot, error) { helperReads++; return helper(ctx) }
	config.ReadSetupState = func(path string) (SetupState, error) { setupReads++; return setup(path) }
	config.ReadDesiredSetupState = func(path string) (SetupState, error) { desiredReads++; return desired(path) }
	report := BuildDoctor(t.Context(), config)
	if intentReads != 1 || capabilityReads != 1 || helperReads != 1 || setupReads != 1 || desiredReads != 1 {
		t.Fatalf("read counts intent=%d capability=%d helper=%d setup=%d desired=%d", intentReads, capabilityReads, helperReads, setupReads, desiredReads)
	}
	if len(report.Limitations) != 1 || report.Limitations[0].Code != DoctorUIDLimitation || report.Limitations[0].Issue != DoctorUIDIssue {
		t.Fatalf("UID limitation = %+v", report.Limitations)
	}
}

func TestEveryDoctorCodeHasResolvableRunbookHeading(t *testing.T) {
	payload, err := os.ReadFile(filepath.Join("..", "..", RunbookPath))
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool)
	for _, code := range StableDoctorCodes() {
		if seen[code] {
			t.Fatalf("duplicate stable doctor code %q", code)
		}
		seen[code] = true
		heading := "## doctor-code-" + strings.ReplaceAll(code, "_", "-")
		if !bytes.Contains(payload, []byte(heading+"\n")) {
			t.Fatalf("runbook heading %q is missing", heading)
		}
		if got := runbookFor(code); got != RunbookPath+"#"+strings.TrimPrefix(heading, "## ") {
			t.Fatalf("runbook anchor for %q = %q", code, got)
		}
	}
}

func TestTestedRuntimeVersionsMatchRealtimeWorkflowPins(t *testing.T) {
	versionsPayload, err := os.ReadFile(filepath.Join("..", "..", "scripts", "oci-tested-versions.env"))
	if err != nil {
		t.Fatal(err)
	}
	wantVersionSet := "WEFTY_OCI_TESTED_VERSIONS='lima=2.2.0 containerd=" + TestedContainerdVersion + " runc=" + TestedRuncVersion + "'"
	if !bytes.Contains(versionsPayload, []byte(wantVersionSet)) {
		t.Fatalf("installer tested-version set drifted from doctor constants: want %q", wantVersionSet)
	}
	wantMinimumSet := "WEFTY_OCI_MINIMUM_VERSIONS='lima=2.2.0 containerd=" + MinimumContainerdVersion + " runc=" + MinimumRuncVersion + "'"
	if !bytes.Contains(versionsPayload, []byte(wantMinimumSet)) {
		t.Fatalf("installer minimum-version set drifted from doctor enforcement: want %q", wantMinimumSet)
	}
	for document, minimums := range map[string][]string{
		filepath.Join("..", "..", RunbookPath):                                  {"containerd 2.0", "runc 1.x"},
		filepath.Join("..", "..", "docs", "specs", "2026-08-22-m3-oci-spec.md"): {"containerd ≥2.0", "runc 1.x"},
	} {
		payload, err := os.ReadFile(document)
		if err != nil {
			t.Fatal(err)
		}
		for _, minimum := range minimums {
			if !bytes.Contains(payload, []byte(minimum)) {
				t.Fatalf("minimum policy drifted from %s: missing %q", document, minimum)
			}
		}
	}
	payload, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "service-acceptance-realtiming.yml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range []string{"CONTAINERD_VERSION: " + TestedContainerdVersion, "RUNC_VERSION: " + TestedRuncVersion} {
		if !bytes.Contains(payload, []byte(pin)) {
			t.Fatalf("tested runtime constant drifted from workflow pin %q", pin)
		}
	}
}

func TestOCIPrerequisiteInstallerMatrix(t *testing.T) {
	repositoryRoot := filepath.Join("..", "..")
	installer := filepath.Join(repositoryRoot, "scripts", "install-oci-deps.sh")
	matrix := filepath.Join(repositoryRoot, "scripts", "test-install-oci-deps.sh")
	if shellcheck, err := exec.LookPath("shellcheck"); err == nil {
		command := exec.Command(shellcheck, installer, matrix,
			filepath.Join(repositoryRoot, "scripts", "build-oci-install-manifest.sh"),
			filepath.Join(repositoryRoot, "scripts", "test-install-oci-privileged.sh"))
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("shellcheck OCI prerequisite installer: %v\n%s", err, output)
		}
	}
	command := exec.Command("bash", matrix)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("OCI prerequisite installer matrix: %v\n%s", err, output)
	}
}

func TestRuntimeVersionRangeIsAdvisoryAndHumanWriterDoesNotMutate(t *testing.T) {
	now := time.Date(2026, 8, 28, 22, 0, 0, 0, time.UTC)
	config := healthyDoctorConfig(now, "")
	mutateHelper(&config, func(snapshot *HelperDoctorSnapshot) {
		snapshot.Runtime.ContainerdVersion = "9.9.9"
		snapshot.Runtime.RuncVersion = "1.9.9"
	})
	report := BuildDoctor(t.Context(), config)
	report.Probe.MissingCapabilities = []string{"z-last", "a-first"}
	before := append([]string(nil), report.Probe.MissingCapabilities...)
	var human bytes.Buffer
	if err := WriteDoctorHuman(&human, report); err != nil {
		t.Fatal(err)
	}
	if !report.Versions.OutsideTestedRange || report.Versions.Outcome != DiagnosticOK {
		t.Fatalf("outside tested range became a verdict: %+v", report.Versions)
	}
	item := findDoctorFinding(t, report, "runtime-versions")
	if item.Outcome != DiagnosticOK || item.Severity != DiagnosticWarn || item.ReasonCode != "" {
		t.Fatalf("outside tested range finding = %+v", item)
	}
	if !slices.Equal(report.Probe.MissingCapabilities, before) {
		t.Fatalf("human writer mutated caller slice: before=%v after=%v", before, report.Probe.MissingCapabilities)
	}
}

func TestRuntimeVersionMinimumIsAHealthFailure(t *testing.T) {
	config := healthyDoctorConfig(time.Date(2026, 8, 28, 22, 30, 0, 0, time.UTC), "")
	mutateHelper(&config, func(snapshot *HelperDoctorSnapshot) {
		snapshot.Runtime.ContainerdVersion = "1.7.27"
	})
	report := BuildDoctor(t.Context(), config)
	item := findDoctorFinding(t, report, "runtime-versions")
	if item.Outcome != DiagnosticFailed || item.Code != "oci_runtime_versions_unsupported" || item.ReasonCode != contract.CapabilityReasonRuntimeVersionUnsupported {
		t.Fatalf("unsupported runtime finding = %+v", item)
	}
}

func TestZeroNumericFactsRemainPresentInJSON(t *testing.T) {
	report := BuildDoctor(t.Context(), DoctorConfig{Clock: &controlTestClock{now: time.Date(2026, 8, 28, 23, 0, 0, 0, time.UTC)}, HostPlatform: PlatformFacts{OS: "linux", Architecture: "amd64"}, AgentUser: "agent"})
	payload, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"protocol_version":0`, `"pending_publication_revision":0`, `"version":0`, `"revision":0`, `"bytes":0`, `"cap_bytes":0`} {
		if !bytes.Contains(payload, []byte(field)) {
			t.Fatalf("zero numeric fact %s was omitted: %s", field, payload)
		}
	}
}

func findDoctorFinding(t *testing.T, report DoctorResponse, check string) DiagnosticFinding {
	t.Helper()
	for _, item := range report.Findings {
		if item.Check == check {
			return item
		}
	}
	t.Fatalf("finding %s missing", check)
	return DiagnosticFinding{}
}
