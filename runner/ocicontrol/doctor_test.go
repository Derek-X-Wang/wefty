package ocicontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
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
		CapabilitySnapshot: func() agent.CapabilitySnapshot {
			return agent.CapabilitySnapshot{
				CapabilityObservation: contract.CapabilityObservation{
					Revision: 9, ObservedAt: now.Add(-2 * time.Minute), Capabilities: capabilities,
					MissingCapabilities: missing, ReasonCode: reason,
				},
				LastProbeAt: now.Add(-time.Minute),
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
					ContainerdVersion: "2.3.4", RuncVersion: "1.3.3",
					AllowedMountRoots: []string{"/srv/wefty", "/worktrees"},
					Cache:             ocihelper.ImageCacheStatus{Bytes: 8 << 30, CapBytes: 16 << 30},
				},
			}, nil
		},
		SetupStatePath: "/var/lib/wefty/setup.json",
		ReadSetupState: func(string) (SetupState, error) {
			return SetupState{VMMemory: "4GiB", VMCPUs: 4, VMDisk: "32GiB", VMType: "vz", HostMountRoot: "/srv/wefty", ProbeDigest: "sha256:probe"}, nil
		},
	}
}

func TestDoctorGoldenParityCoversEveryReasonAndL1MetadataTuple(t *testing.T) {
	now := time.Date(2026, 8, 28, 18, 0, 0, 0, time.UTC)
	var findings []DiagnosticFinding
	for _, reason := range StableDoctorReasonCodes() {
		report := BuildDoctor(t.Context(), healthyDoctorConfig(now, reason))
		if err := report.Validate(); err != nil {
			t.Fatalf("reason %s report: %v", reason, err)
		}
		if report.Probe.CapabilityRevision != 9 || !report.Probe.CapabilityObservedAt.Equal(now.Add(-2*time.Minute)) ||
			len(report.Probe.MissingCapabilities) != 1 || report.Probe.MissingCapabilities[0] != "kind:oci" || report.Probe.ReasonCode != reason {
			t.Fatalf("reason %s lost L1 metadata tuple: %+v", reason, report.Probe)
		}
		var human bytes.Buffer
		if err := WriteDoctorHuman(&human, report); err != nil {
			t.Fatal(err)
		}
		payload, err := json.Marshal(report)
		if err != nil {
			t.Fatal(err)
		}
		anchor := runbookFor(reason, "")
		for _, golden := range []string{string(reason), anchor, "kind:oci"} {
			if !bytes.Contains(payload, []byte(golden)) || !strings.Contains(human.String(), golden) {
				t.Fatalf("reason %s missing golden %q from JSON or human parity", reason, golden)
			}
		}
		if !bytes.Contains(payload, []byte(`"capability_revision":9`)) || !strings.Contains(human.String(), "revision=9") {
			t.Fatalf("reason %s lost Capability revision parity", reason)
		}
		findings = append(findings, report.Findings...)
	}
	covered := make(map[contract.CapabilityReasonCode]bool)
	for _, item := range findings {
		covered[item.ReasonCode] = true
	}
	for _, reason := range StableDoctorReasonCodes() {
		if !covered[reason] {
			t.Fatalf("doctor golden did not cover §8.3 reason %s", reason)
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
		if item.Outcome != DiagnosticFailed || item.ReasonCode != contract.CapabilityReasonOCIIntentDisabled || report.Intent.Enabled {
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
		if item.Outcome != DiagnosticFailed || item.ReasonCode != contract.CapabilityReasonOCIIntentDisabled {
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
		for _, check := range []string{"runtime-platform", "runtime-versions", "cache", "mount-roots"} {
			if item := find(t, report, check); item.Outcome != DiagnosticNotRun {
				t.Fatalf("dependent %s claimed execution: %+v", check, item)
			}
		}
	})

	t.Run("stale capability revision", func(t *testing.T) {
		config := healthyDoctorConfig(now, "")
		base := config.CapabilitySnapshot
		config.CapabilitySnapshot = func() agent.CapabilitySnapshot {
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
}

func TestDoctorNotRunIsFirstClassAndNeverTurnsIntoOK(t *testing.T) {
	report := BuildDoctor(t.Context(), DoctorConfig{
		Clock:        &controlTestClock{now: time.Date(2026, 8, 28, 20, 0, 0, 0, time.UTC)},
		HostPlatform: PlatformFacts{OS: "linux", Architecture: "arm64"}, AgentUser: "agent",
	})
	if err := report.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, check := range []string{"intent", "capability-revision", "probe", "lima", "helper-handshake", "runtime-platform", "runtime-versions", "cache", "mount-roots", "convergence"} {
		found := false
		for _, item := range report.Findings {
			if item.Check == check {
				found = true
				if item.Outcome == DiagnosticOK {
					t.Fatalf("unexecuted check %s reported OK: %+v", check, item)
				}
			}
		}
		if !found {
			t.Fatalf("unexecuted check %s missing", check)
		}
	}
}

func TestDoctorReadsSourcesExactlyOnceAndSurfacesUIDLimitation(t *testing.T) {
	now := time.Date(2026, 8, 28, 21, 0, 0, 0, time.UTC)
	config := healthyDoctorConfig(now, "")
	intentReads, capabilityReads, helperReads, setupReads := 0, 0, 0, 0
	intent := config.Intent
	capability := config.CapabilitySnapshot
	helper := config.Helper
	setup := config.ReadSetupState
	config.Intent = func(ctx context.Context) (lima.OCIIntent, error) { intentReads++; return intent(ctx) }
	config.CapabilitySnapshot = func() agent.CapabilitySnapshot { capabilityReads++; return capability() }
	config.Helper = func(ctx context.Context) (HelperDoctorSnapshot, error) { helperReads++; return helper(ctx) }
	config.ReadSetupState = func(path string) (SetupState, error) { setupReads++; return setup(path) }
	report := BuildDoctor(t.Context(), config)
	if intentReads != 1 || capabilityReads != 1 || helperReads != 1 || setupReads != 1 {
		t.Fatalf("read counts intent=%d capability=%d helper=%d setup=%d", intentReads, capabilityReads, helperReads, setupReads)
	}
	if len(report.Limitations) != 1 || report.Limitations[0].Code != DoctorUIDLimitation || report.Limitations[0].Issue != DoctorUIDIssue {
		t.Fatalf("UID limitation = %+v", report.Limitations)
	}
}
