//go:build service_acceptance_realtiming && linux

package ocihelper_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/lima"
	ocirunner "github.com/Derek-X-Wang/wefty/runner/oci"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
	"github.com/coder/websocket"
)

func TestMain(m *testing.M) {
	if ocihelper.IsLoggerInvocation(os.Args) {
		if err := ocihelper.RunLoggerInvocation(os.Args); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestNativeLinuxOCIAdapterLifecycle(t *testing.T) {
	address := os.Getenv("WEFTY_OCI_CONTAINERD_ADDRESS")
	helperSocket := os.Getenv("WEFTY_OCI_HELPER_SOCKET")
	helperChecksum := os.Getenv("WEFTY_OCI_HELPER_CHECKSUM")
	reference := os.Getenv("WEFTY_OCI_PROBE_REFERENCE")
	digest := os.Getenv("WEFTY_OCI_PROBE_DIGEST")
	archivePath := os.Getenv("WEFTY_OCI_PROBE_ARCHIVE")
	echoReference := os.Getenv("WEFTY_OCI_ECHO_REFERENCE")
	echoDigest := os.Getenv("WEFTY_OCI_ECHO_DIGEST")
	echoArchivePath := os.Getenv("WEFTY_OCI_ECHO_ARCHIVE")
	weftyCLI := os.Getenv("WEFTY_OCI_CLI")
	numericReference := os.Getenv("WEFTY_OCI_SERVICE_NUMERIC_REFERENCE")
	numericArchivePath := os.Getenv("WEFTY_OCI_SERVICE_NUMERIC_ARCHIVE")
	namedReference := os.Getenv("WEFTY_OCI_SERVICE_NAMED_REFERENCE")
	namedArchivePath := os.Getenv("WEFTY_OCI_SERVICE_NAMED_ARCHIVE")
	computerReference := os.Getenv("WEFTY_OCI_COMPUTER_REFERENCE")
	computerDigest := os.Getenv("WEFTY_OCI_COMPUTER_DIGEST")
	computerArchivePath := os.Getenv("WEFTY_OCI_COMPUTER_ARCHIVE")
	computerVariant := os.Getenv("WEFTY_OCI_COMPUTER_VARIANT")
	waylandComputerReference := os.Getenv("WEFTY_OCI_WAYLAND_COMPUTER_REFERENCE")
	waylandComputerDigest := os.Getenv("WEFTY_OCI_WAYLAND_COMPUTER_DIGEST")
	waylandComputerArchivePath := os.Getenv("WEFTY_OCI_WAYLAND_COMPUTER_ARCHIVE")
	provisionReceipt := os.Getenv("WEFTY_OCI_PROVISION_RECEIPT")
	evidenceSource := os.Getenv("WEFTY_REALTIME_EVIDENCE_SOURCE")
	if address == "" || helperSocket == "" || helperChecksum == "" || reference == "" || digest == "" || archivePath == "" || echoReference == "" || echoDigest == "" || echoArchivePath == "" || weftyCLI == "" || numericReference == "" || numericArchivePath == "" || namedReference == "" || namedArchivePath == "" || computerReference == "" || computerDigest == "" || computerArchivePath == "" || computerVariant == "" || waylandComputerReference == "" || waylandComputerDigest == "" || waylandComputerArchivePath == "" || provisionReceipt == "" {
		t.Fatal("Linux OCI realtiming provisioning is incomplete")
	}
	if evidenceSource != "pr-build" && evidenceSource != "published-artifact" {
		t.Fatalf("Linux OCI realtiming evidence source = %q, want pr-build or published-artifact", evidenceSource)
	}
	prBuildRegistryNotRun := evidenceSource == "pr-build"
	if reference != echoReference || digest != echoDigest || archivePath != echoArchivePath || reference != "ghcr.io/derek-x-wang/wefty-echo-service" {
		t.Fatalf("probe and workload did not consume one canonical public artifact: probe=%s@%s archive=%s echo=%s@%s archive=%s", reference, digest, archivePath, echoReference, echoDigest, echoArchivePath)
	}
	if err := validateNativeLinuxComputerArtifacts(nativeLinuxComputerArtifacts{
		Variant: computerVariant, GenericReference: echoReference, GenericArchive: echoArchivePath,
		SelectedReference: computerReference, SelectedDigest: computerDigest, SelectedArchive: computerArchivePath,
		WaylandReference: waylandComputerReference, WaylandDigest: waylandComputerDigest, WaylandArchive: waylandComputerArchivePath,
	}); err != nil {
		t.Fatalf("reference Computer artifact separation: %v: selected=%s@%s archive=%s wayland=%s@%s archive=%s", err, computerReference, computerDigest, computerArchivePath, waylandComputerReference, waylandComputerDigest, waylandComputerArchivePath)
	}
	if os.Geteuid() == 0 {
		t.Fatal("Linux OCI realtiming test process must be unprivileged")
	}
	assertUnprivilegedRunnerReceipt(t, provisionReceipt)
	if connection, err := net.DialTimeout("unix", address, 250*time.Millisecond); err == nil {
		_ = connection.Close()
		t.Fatal("unprivileged agent reached the root-only raw containerd socket")
	}

	var socketStat syscall.Stat_t
	if err := syscall.Stat(helperSocket, &socketStat); err != nil || socketStat.Uid != 0 {
		t.Fatalf("helper socket is not root-owned: uid=%d err=%v", socketStat.Uid, err)
	}
	client := ocihelper.NewUnixClient(helperSocket, helperChecksum)
	client.HeartbeatInterval = time.Second
	barrier, err := ocihelper.NewBootBarrier(client, ocihelper.AcquireSessionRequest{NodeID: "native-node", BootSessionID: "native-boot"})
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	barrierStartedAt := time.Now()
	var barrierReadyAt atomic.Int64
	barrier.SetLossHandler(func(generation ocihelper.HelperSession, lossErr error) {
		if err := recordNativeBarrierLoss(generation, barrierStartedAt, barrierReadyAt.Load(), lossErr); err != nil {
			t.Errorf("record native barrier loss evidence: %v", err)
		}
	})
	if err := barrier.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	barrierReadyAt.Store(time.Now().UTC().UnixNano())
	adapter := ocirunner.NewAdapter(barrier)
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	// A clean-cache pull must first prove that root-owned helper/containerd
	// registry HTTPS is rejected. The release archive is then the only successful
	// image source; the helper filters it to this node's platform before the 16 GiB
	// cache ceiling and probe pin are reconciled.
	requestRootFault(t, "reset-containerd")
	requestRootFault(t, "disable-registry")
	firstRegistryDisabled := true
	registryDisabledPullRejected := false
	t.Cleanup(func() {
		if firstRegistryDisabled {
			requestRootFault(t, "enable-registry")
		}
	})
	registryProbeContext, cancelRegistryProbe := context.WithTimeout(ctx, 30*time.Second)
	registryProbeErr := session.EnsureImage(registryProbeContext, ocihelper.EnsureImageRequest{
		Reference: reference, Digest: digest, Source: ocihelper.ImageSourceRegistry,
		Platform:         ocihelper.OCIPlatform{OS: "linux", Architecture: runtime.GOARCH},
		OperationTimeout: 5 * time.Second,
	}, nil)
	cancelRegistryProbe()
	var registryProbeFailure *ocihelper.RPCError
	if !errors.As(registryProbeErr, &registryProbeFailure) || registryProbeFailure.Code != ocihelper.CodeImageUnavailable ||
		registryProbeFailure.ImageFailure == nil || registryProbeFailure.ImageFailure.Kind != ocihelper.ImageFailureNetwork ||
		registryProbeFailure.ImageFailure.TopLevelDigest != digest {
		t.Fatalf("disabled-registry pull = %v, want image_unavailable with network mechanics for %s", registryProbeErr, digest)
	}
	registryDisabledPullRejected = true
	loadedByCLI := loadNativeImageThroughCLI(t, ctx, adapter, weftyCLI, archivePath)
	requestRootFault(t, "enable-registry")
	firstRegistryDisabled = false
	if loadedByCLI.TopLevelDigest != digest || loadedByCLI.PlatformDigest == "" {
		t.Fatalf("clean-cache wefty node load-image evidence = %+v", loadedByCLI)
	}
	const acceptanceCacheCap = int64(16 << 30)
	if _, err := session.ReconcileImagePins(ctx, ocihelper.ReconcileImagePinsRequest{ProbeDigests: []string{digest}, CacheMaxBytes: acceptanceCacheCap}); err != nil {
		t.Fatal(err)
	}
	cacheAfterCLI, err := session.ImageCacheStatus(ctx)
	if err != nil || cacheAfterCLI.CapBytes != acceptanceCacheCap || cacheAfterCLI.Bytes > cacheAfterCLI.CapBytes {
		t.Fatalf("clean-cache CLI import cache status = %+v err=%v", cacheAfterCLI, err)
	}
	probeStarted := time.Now()
	if err := adapter.Probe(ctx, "native-node", "native-boot", reference, digest, l1.DefaultLeaseDuration); err != nil {
		t.Fatal(err)
	}
	probeElapsed := time.Since(probeStarted)
	if probeElapsed > 10*time.Second {
		t.Fatalf("production functional probe took %s, want at most 10s", probeElapsed)
	}
	doctorBefore, err := session.Verify(ctx, ocihelper.VerifyRequest{Scope: ocihelper.VerifyNamespace})
	if err != nil {
		t.Fatal(err)
	}
	doctorObservedAt := time.Now().UTC().Round(0)
	doctorJSON := ocicontrol.BuildDoctor(ctx, ocicontrol.DoctorConfig{
		HostPlatform: ocicontrol.PlatformFacts{OS: "linux", Architecture: runtime.GOARCH},
		AgentUser:    fmt.Sprintf("uid:%d", os.Getuid()), LaunchUnit: "wefty-agent.service",
		CapabilitySnapshot: func() agent.CapabilitySnapshot {
			observation := contract.CapabilityObservation{
				Revision: 2, ObservedAt: doctorObservedAt, Capabilities: map[string]bool{
					"kind:process": true, "kind:oci": true, "runtime_handler:" + ocihelper.DefaultRuntimeHandler: true,
				}, MissingCapabilities: []string{},
			}
			return agent.CapabilitySnapshot{CapabilityObservation: observation, LastProbe: &observation}
		},
		Intent: func(context.Context) (lima.OCIIntent, error) {
			return lima.OCIIntent{Version: lima.OCIIntentVersion, Revision: 1, Enabled: true, UpdatedAt: doctorObservedAt}, nil
		},
		Helper: func(ctx context.Context) (ocicontrol.HelperDoctorSnapshot, error) {
			status, err := adapter.DoctorStatus(ctx)
			return ocicontrol.HelperDoctorSnapshot{
				ProtocolVersion: status.ProtocolVersion, Version: status.HelperVersion, Checksum: status.HelperChecksum,
				InstanceID: status.HelperInstanceID, SessionGeneration: status.SessionGeneration,
				Runtime: status.Runtime, RuntimePlatformRecorded: status.RuntimePlatformRecorded,
				SweepReceipt: status.SweepReceipt, SweepReceiptRecorded: status.SweepReceiptRecorded,
			}, err
		},
		SetupStatePath: "/var/lib/wefty/oci-setup.json",
		ReadSetupState: func(string) (ocicontrol.SetupState, error) {
			return ocicontrol.SetupState{VMMemory: "4GiB", VMCPUs: 4, VMDisk: "32GiB", VMType: "native", HostMountRoot: "/srv/wefty", ProbeDigest: digest}, nil
		},
	})
	if err := doctorJSON.Validate(); err != nil {
		t.Fatal(err)
	}
	doctorAfter, err := session.Verify(ctx, ocihelper.VerifyRequest{Scope: ocihelper.VerifyNamespace})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(doctorBefore, doctorAfter) {
		t.Fatalf("facts-only doctor mutated helper namespace: before=%+v after=%+v", doctorBefore, doctorAfter)
	}
	doctorBundle, err := json.MarshalIndent(struct {
		Before ocihelper.VerifyResponse  `json:"before"`
		Doctor ocicontrol.DoctorResponse `json:"doctor"`
		After  ocihelper.VerifyResponse  `json:"after"`
	}{Before: doctorBefore, Doctor: doctorJSON, After: doctorAfter}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}

	// Delete must accept the live owner-key-derived handoff name. It cannot be
	// reconstructed from attempt authority, so this reaches the engine identity
	// gate with the exact name retained by Run.
	handoffAuthority := nativeAuthority("owner-key-delete")
	const handoffOwner = "native-owner-key-delete"
	if _, err := session.Run(ctx, ocihelper.RunRequest{
		Authority: handoffAuthority, InitialDeadman: l1.DefaultLeaseDuration,
		Workload: ocihelper.WorkloadInput{
			ImageReference: reference, ImageDigest: digest, Argv: []string{"/bin/sh", "-c", "exit 0"},
			ManagedVolumes: []ocihelper.ManagedVolumeDescriptor{{Kind: ocihelper.ManagedVolumeHandoff, OwnerKey: handoffOwner}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := session.Watch(ctx, ocihelper.WatchRequest{Authority: handoffAuthority}, func(ocihelper.WatchEvent) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Delete(ctx, ocihelper.DeleteRequest{Authority: handoffAuthority}); err != nil {
		t.Fatalf("delete live owner-key handoff attempt: %v", err)
	}
	if _, err := session.DeleteManagedVolume(ctx, ocihelper.DeleteManagedVolumeRequest{Kind: ocihelper.ManagedVolumeHandoff, OwnerKey: handoffOwner}); err != nil {
		t.Fatal(err)
	}

	manifestRequest := nativeAdapterRequest(reference, digest, "manifest-inventory", []string{"/bin/sh", "-c", "exec sleep 60"})
	manifestRequest.Authority.WorkloadClass = contract.JobClassService
	manifestRequest.Authority.RemovalGeneration = fmt.Sprint(l1.InitialServiceRemovalGeneration)
	manifestRequest.ManagedVolumes = []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeServiceData}}
	manifest, err := adapter.RemovalResourceManifest(manifestRequest)
	if err != nil {
		t.Fatal(err)
	}
	manifestAuthority := ocirunner.HelperAuthority(manifestRequest.Authority)
	if _, err := session.Run(ctx, ocihelper.RunRequest{
		Authority: manifestAuthority, InitialDeadman: l1.DefaultLeaseDuration,
		Workload: ocihelper.WorkloadInput{
			ImageReference: reference, ImageDigest: digest, Argv: []string{"/bin/sh", "-c", "exec sleep 60"},
			ManagedVolumes: []ocihelper.ManagedVolumeDescriptor{{Kind: ocihelper.ManagedVolumeServiceData}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	verification, err := session.Verify(ctx, ocihelper.VerifyRequest{Scope: ocihelper.VerifyAttempt, Authority: &manifestAuthority})
	if err != nil {
		t.Fatal(err)
	}
	if manifest.TaskID != manifest.ContainerID || manifest.ShimID != manifest.ContainerID ||
		!slices.Contains(verification.Inventory.Leases, manifest.LeaseID) ||
		!slices.Contains(verification.Inventory.Snapshots, manifest.SnapshotID) ||
		!slices.Contains(verification.Inventory.Containers, manifest.ContainerID) ||
		!slices.Contains(verification.Inventory.Tasks, manifest.TaskID) ||
		!slices.Contains(verification.Inventory.Shims, manifest.ShimID) ||
		!slices.Contains(verification.Inventory.Cgroups, manifest.CgroupID) ||
		!slices.Contains(verification.Inventory.LogSegments, manifest.LogSegmentDirectory) ||
		!slices.Contains(verification.Inventory.ManagedVolumes, manifest.ServiceDataVolume) ||
		!slices.Contains(verification.Inventory.ManagedVolumeRecords, manifest.ServiceDataOwnerRecord) {
		t.Fatalf("frozen manifest does not match live helper inventory: manifest=%+v inventory=%+v", manifest, verification.Inventory)
	}
	if err := session.Signal(ctx, ocihelper.SignalRequest{Authority: manifestAuthority, Signal: ocihelper.SignalKILL}); err != nil {
		t.Fatal(err)
	}
	if err := session.Watch(ctx, ocihelper.WatchRequest{Authority: manifestAuthority}, func(ocihelper.WatchEvent) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Delete(ctx, ocihelper.DeleteRequest{Authority: manifestAuthority}); err != nil {
		t.Fatal(err)
	}

	// Published-artifact runs prove public pull from an empty containerd root;
	// PR builds record that row as NOT-RUN because their digest is not published.
	// The offline import row always starts empty and rejects registry HTTPS so the
	// tar stream is the only possible source of the imported bytes.
	requestRootFault(t, "reset-containerd")
	var pulled ocihelper.EnsureImageResponse
	bindingRepullReconciliation := false
	if !prBuildRegistryNotRun {
		err = session.EnsureImage(ctx, ocihelper.EnsureImageRequest{
			Reference: reference, Digest: digest, Source: ocihelper.ImageSourceRegistry,
			Platform:         ocihelper.OCIPlatform{OS: "linux", Architecture: runtime.GOARCH},
			OperationTimeout: 2 * time.Minute,
		}, func(event ocihelper.EnsureImageEvent) error {
			if event.Result != nil {
				pulled = *event.Result
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Real cache pressure uses three independently named top-level roots. The
	// probe root stays protected while the two variants prove actual LRU order
	// and one-record-with-real-bytes eviction evidence.
	lruRegistry := newRefloatRegistry(t, archivePath)
	olderDigest := lruRegistry.addVariant(t, "older")
	newerDigest := lruRegistry.addVariant(t, "newer")
	for _, value := range []string{olderDigest, newerDigest} {
		if err := session.EnsureImage(ctx, ocihelper.EnsureImageRequest{
			Reference: lruRegistry.reference(), Digest: value, Source: ocihelper.ImageSourceRegistry,
			Platform: ocihelper.OCIPlatform{OS: "linux", Architecture: runtime.GOARCH}, OperationTimeout: 2 * time.Minute,
		}, nil); err != nil {
			t.Fatalf("pull LRU variant %s: %v", value, err)
		}
	}
	lruRequest := ocihelper.ReconcileImagePinsRequest{ProbeDigests: []string{digest}, CacheMaxBytes: 1}
	if _, err := session.ReconcileImagePins(ctx, lruRequest); err != nil {
		t.Fatal(err)
	}
	firstEviction := waitForCacheEviction(t, ctx, session, "", olderDigest)
	if firstEviction.Reason != "reconcile" || firstEviction.Bytes <= 0 {
		t.Fatalf("first LRU eviction record = %+v", firstEviction)
	}
	if _, err := session.ReconcileImagePins(ctx, lruRequest); err != nil {
		t.Fatal(err)
	}
	secondEviction := waitForCacheEviction(t, ctx, session, olderDigest, newerDigest)
	if secondEviction.Reason != "reconcile" || secondEviction.Bytes <= 0 {
		t.Fatalf("second LRU eviction record = %+v", secondEviction)
	}

	if !prBuildRegistryNotRun {
		// Establish a durable service binding, wipe containerd behind the helper,
		// and require adapter-owned reconciliation to repull and reattach it.
		automaticBinding := nativeAdapterRequest(reference, digest, "automatic-repull", []string{"/bin/true"})
		automaticBinding.Authority.WorkloadClass = contract.JobClassService
		automaticBinding.ManagedVolumes = []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeServiceData}}
		automaticBinding.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error { return nil }
		if _, err := adapter.Run(ctx, automaticBinding, nil); err != nil {
			t.Fatal(err)
		}
		if receipt, err := adapter.ReapAndVerify(ctx, workloadrunner.ReapRequest{Authority: automaticBinding.Authority}); err != nil || !receipt.RuntimeQuiesced {
			t.Fatalf("automatic-repull seed cleanup = %+v err=%v", receipt, err)
		}
		requestRootFault(t, "reset-containerd")
		if failures, err := adapter.ReconcileOCIImagePins(ctx, func(context.Context, string) (bool, error) { return true, nil }); err != nil || len(failures) != 0 {
			t.Fatalf("automatic wipe/repull reconciliation = failures %+v err=%v", failures, err)
		}
		if err := adapter.ReleaseOCIImageBindingPin(ctx, automaticBinding.Authority.JobID); err != nil {
			t.Fatal(err)
		}
		bindingRepullReconciliation = true
	}
	// Start the archive row from an empty root before isolating the registry so
	// the direct reconciliation observes the intended retained-but-missing binding.
	requestRootFault(t, "reset-containerd")
	requestRootFault(t, "disable-registry")
	registryDisabled := true
	t.Cleanup(func() {
		if registryDisabled {
			requestRootFault(t, "enable-registry")
		}
	})
	archive, err := os.Open(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	var imported ocihelper.EnsureImageResponse
	bindingAuthority := nativeAuthority("cache-binding")
	bindingAuthority.Class = contract.JobClassService
	reconcileRequest := ocihelper.ReconcileImagePinsRequest{
		Bindings: []ocihelper.BindingImagePin{{
			JobID: bindingAuthority.JobID, Reference: reference, Digest: digest,
			Platform: ocihelper.OCIPlatform{OS: "linux", Architecture: runtime.GOARCH}, Snapshotter: ocihelper.DefaultSnapshotter,
		}},
		ProbeDigests: []string{digest}, CacheMaxBytes: 1,
	}
	wiped, err := session.ReconcileImagePins(ctx, reconcileRequest)
	if err != nil || len(wiped.MissingDigests) != 1 || wiped.MissingDigests[0] != digest {
		t.Fatalf("wiped-cache binding reconciliation = %+v err=%v", wiped, err)
	}
	importErr := session.ImportImage(ctx, ocihelper.EnsureImageRequest{
		Reference: reference, Digest: digest, Source: ocihelper.ImageSourceArchive,
		Platform:         ocihelper.OCIPlatform{OS: "linux", Architecture: runtime.GOARCH},
		OperationTimeout: 2 * time.Minute,
		Pin:              &ocihelper.ImagePin{Authority: bindingAuthority, Binding: true},
	}, archive, func(event ocihelper.EnsureImageEvent) error {
		if event.Result != nil {
			imported = *event.Result
		}
		return nil
	})
	closeErr := archive.Close()
	if importErr != nil || closeErr != nil {
		t.Fatal(errors.Join(importErr, closeErr))
	}
	if prBuildRegistryNotRun {
		if imported.TopLevelDigest != digest || imported.PlatformDigest == "" {
			t.Fatalf("PR archive import evidence = %+v, want immutable digest %s", imported, digest)
		}
	} else if !reflect.DeepEqual(pulled, imported) || pulled.TopLevelDigest != digest || pulled.PlatformDigest == "" {
		t.Fatalf("pull/import evidence differs: pull=%+v import=%+v", pulled, imported)
	}
	reconciled, err := session.ReconcileImagePins(ctx, reconcileRequest)
	if err != nil || len(reconciled.MissingDigests) != 0 {
		t.Fatalf("repulled binding reconciliation = %+v err=%v", reconciled, err)
	}
	pressure, err := session.ImageCacheStatus(ctx)
	if err != nil || pressure.Bytes <= pressure.CapBytes {
		t.Fatalf("cache pressure status = %+v err=%v", pressure, err)
	}
	if pressure.LastEviction != nil && pressure.LastEviction.Digest == digest {
		t.Fatalf("probe/bound image was evicted under pressure: %+v", pressure.LastEviction)
	}
	if err := session.ReleaseImagePin(ctx, bindingAuthority.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := session.ReconcileImagePins(ctx, ocihelper.ReconcileImagePinsRequest{CacheMaxBytes: 1}); err != nil {
		t.Fatal(err)
	}
	afterRelease, err := session.ImageCacheStatus(ctx)
	if err != nil || afterRelease.Bytes == 0 {
		t.Fatalf("binding release deleted cached content: %+v err=%v", afterRelease, err)
	}
	if afterRelease.LastEviction != nil && afterRelease.LastEviction.Digest == imported.TopLevelDigest {
		t.Fatalf("durable operator import hold was evicted: %+v", afterRelease.LastEviction)
	}
	importRun := nativeAdapterRequest(reference, imported.TopLevelDigest, "import-run", []string{"/bin/true"})
	importRun.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error { return nil }
	if result, err := adapter.Run(ctx, importRun, workloadrunner.OutputSinkFunc(func(context.Context, contract.LogEvent) error { return nil })); err != nil || result.Outcome.ExitCode == nil || *result.Outcome.ExitCode != 0 {
		t.Fatalf("imported image run result=%+v err=%v", result, err)
	}
	if receipt, err := adapter.ReapAndVerify(ctx, workloadrunner.ReapRequest{Authority: importRun.Authority}); err != nil || !receipt.RuntimeQuiesced {
		t.Fatalf("imported image cleanup receipt=%+v err=%v", receipt, err)
	}
	requestRootFault(t, "enable-registry")
	registryDisabled = false
	echoImage := loadNativeImageThroughCLI(t, ctx, adapter, weftyCLI, echoArchivePath)
	if echoImage.TopLevelDigest != echoDigest || echoImage.PlatformDigest == "" {
		t.Fatalf("wefty node load-image identity = %+v, want top-level %s", echoImage, echoDigest)
	}
	computerImage := loadNativeImageArchive(t, ctx, adapter, computerReference, computerArchivePath)
	if computerImage.TopLevelDigest != computerDigest || computerImage.PlatformDigest == "" {
		t.Fatalf("reference Computer archive identity = %+v, want top-level %s", computerImage, computerDigest)
	}
	referenceComputerReadiness := exerciseNativeLinuxReferenceComputer(t, ctx, session, adapter, "xfce", computerReference, computerDigest)
	waylandComputerImage := loadNativeImageArchive(t, ctx, adapter, waylandComputerReference, waylandComputerArchivePath)
	if waylandComputerImage.TopLevelDigest != waylandComputerDigest || waylandComputerImage.PlatformDigest == "" {
		t.Fatalf("Wayland reference Computer archive identity = %+v, want top-level %s", waylandComputerImage, waylandComputerDigest)
	}
	waylandComputerReadiness := exerciseNativeLinuxReferenceComputer(t, ctx, session, adapter, "wayland", waylandComputerReference, waylandComputerDigest)
	numericImage := loadNativeImageArchive(t, ctx, adapter, numericReference, numericArchivePath)
	namedImage := loadNativeImageArchive(t, ctx, adapter, namedReference, namedArchivePath)
	serviceDataEvidence := exerciseNativeLinuxServiceData(t, ctx, barrier, adapter, []nativeServiceDataImage{
		{name: "root", reference: echoReference, digest: echoImage.TopLevelDigest, owner: "0:0"},
		{name: "numeric", reference: numericReference, digest: numericImage.TopLevelDigest, owner: "13001:13002"},
		{name: "named", reference: namedReference, digest: namedImage.TopLevelDigest, owner: "12001:12002"},
	})
	exerciseNativeLinuxComputerCapacity(t, ctx, barrier, echoReference, echoImage.TopLevelDigest)
	computerDiskEvidence := exerciseNativeLinuxComputerDisk(t, ctx, barrier, echoReference, echoImage.TopLevelDigest)
	computerAgentRestartEvidence := exerciseNativeLinuxComputerAgentRestart(t, ctx, adapter, echoReference, echoImage.TopLevelDigest)
	session, err = barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	refloat := newRefloatRegistry(t, echoArchivePath)
	exerciseNativeLinuxPrestartRequeue(t, ctx, adapter, refloat.reference(), refloat.originalDigest(), refloat.moveTag, func() {
		barrier.Invalidate()
		if err := barrier.Ensure(ctx); err != nil {
			t.Fatal(err)
		}
		session, err = barrier.Session()
		if err != nil {
			t.Fatal(err)
		}
		if err := adapter.Probe(ctx, "native-node", "native-boot", reference, digest, l1.DefaultLeaseDuration); err != nil {
			t.Fatal(err)
		}
	})
	if requests := refloat.observedTagRequests(); requests != 1 {
		t.Fatalf("mutable tag was resolved %d times, want exactly the initial resolution", requests)
	}
	if echoImage.TopLevelDigest != refloat.originalDigest() {
		t.Fatalf("echo import/refloat digest = %s / %s", echoImage.TopLevelDigest, refloat.originalDigest())
	}

	activeDigest := lruRegistry.addVariant(t, "active-attempt")
	activeRequest := nativeAdapterRequest(lruRegistry.reference(), activeDigest, "active-cache-pressure", []string{"/bin/sh", "-c", "printf active-cache-pressure; sleep 2"})
	activeRequest.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error { return nil }
	activeLog := make(chan struct{}, 1)
	activeDone := make(chan error, 1)
	go func() {
		_, runErr := adapter.Run(ctx, activeRequest, workloadrunner.OutputSinkFunc(func(_ context.Context, event contract.LogEvent) error {
			if strings.Contains(string(event.Bytes), "active-cache-pressure") {
				select {
				case activeLog <- struct{}{}:
				default:
				}
			}
			return nil
		}))
		activeDone <- runErr
	}()
	select {
	case <-activeLog:
	case err := <-activeDone:
		t.Fatalf("active cache-pressure attempt ended early: %v", err)
	case <-time.After(time.Second):
		t.Fatal("active cache-pressure attempt did not start")
	}
	if _, err := session.ReconcileImagePins(ctx, ocihelper.ReconcileImagePinsRequest{CacheMaxBytes: 1}); err != nil {
		t.Fatal(err)
	}
	activePressure, err := session.ImageCacheStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if activePressure.LastEviction != nil && activePressure.LastEviction.Digest == activeDigest {
		t.Fatalf("active attempt image was evicted under pressure: %+v", activePressure.LastEviction)
	}
	if err := <-activeDone; err != nil {
		t.Fatal(err)
	}
	if receipt, err := adapter.ReapAndVerify(ctx, workloadrunner.ReapRequest{Authority: activeRequest.Authority}); err != nil || !receipt.RuntimeQuiesced {
		t.Fatalf("active cache-pressure cleanup = %+v err=%v", receipt, err)
	}
	if _, err := session.ReconcileImagePins(ctx, ocihelper.ReconcileImagePinsRequest{CacheMaxBytes: 1}); err != nil {
		t.Fatal(err)
	}
	releasedEviction := waitForCacheEviction(t, ctx, session, newerDigest, activeDigest)
	if releasedEviction.Bytes <= 0 {
		t.Fatalf("released attempt eviction record = %+v", releasedEviction)
	}
	handoffMarkerBytes := exerciseNativeLinuxOneshotContract(t, ctx, adapter, echoReference, echoImage.TopLevelDigest, reference, digest)

	liveRequest := nativeAdapterRequest(reference, digest, "live-logs", []string{"/bin/sh", "-c", "printf live-before-exit; sleep 2; exit 0"})
	liveRequest.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error { return nil }
	liveLog := make(chan struct{}, 1)
	liveDone := make(chan error, 1)
	go func() {
		_, runErr := adapter.Run(ctx, liveRequest, workloadrunner.OutputSinkFunc(func(_ context.Context, event contract.LogEvent) error {
			if strings.Contains(string(event.Bytes), "live-before-exit") {
				select {
				case liveLog <- struct{}{}:
				default:
				}
			}
			return nil
		}))
		liveDone <- runErr
	}()
	select {
	case <-liveLog:
	case err := <-liveDone:
		t.Fatalf("task completed before live log delivery: %v", err)
	case <-time.After(time.Second):
		t.Fatal("binary-v2 segment was not tailed while the task was running")
	}
	if err := <-liveDone; err != nil {
		t.Fatal(err)
	}
	if receipt, err := adapter.ReapAndVerify(ctx, workloadrunner.ReapRequest{Authority: liveRequest.Authority}); err != nil || !receipt.RuntimeQuiesced {
		t.Fatalf("live-log cleanup receipt=%+v err=%v", receipt, err)
	}

	var order []string
	var logs []contract.LogEvent
	request := nativeAdapterRequest(reference, digest, "exit", []string{"/bin/sh", "-c", "printf out; printf err >&2; exit 7"})
	request.OCIStarted = func(_ context.Context, observation workloadrunner.OCIImageObservation) error {
		if observation.TopLevelDigest != digest || observation.RuntimeHandler != ocihelper.DefaultRuntimeHandler || observation.Snapshotter != ocihelper.DefaultSnapshotter {
			return fmt.Errorf("unexpected helper image evidence: %+v", observation)
		}
		order = append(order, "l1-started")
		return nil
	}
	request.Started = func() { order = append(order, "local-started") }
	result, err := adapter.Run(ctx, request, workloadrunner.OutputSinkFunc(func(_ context.Context, event contract.LogEvent) error {
		logs = append(logs, event)
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome.ExitCode == nil || *result.Outcome.ExitCode != 7 || strings.Join(order, ",") != "l1-started,local-started" {
		t.Fatalf("exit outcome/order = %+v / %v", result.Outcome, order)
	}
	if !containsLog(logs, contract.LogStdout, "out") || !containsLog(logs, contract.LogStderr, "err") {
		t.Fatalf("binary-v2 logs = %+v", logs)
	}
	if receipt, err := adapter.ReapAndVerify(ctx, workloadrunner.ReapRequest{Authority: request.Authority}); err != nil || !receipt.RuntimeQuiesced {
		t.Fatalf("verified cleanup receipt=%+v err=%v", receipt, err)
	}

	signalAuthority := nativeAuthority("signal")
	if _, err := session.Run(ctx, ocihelper.RunRequest{
		Authority: signalAuthority, InitialDeadman: l1.DefaultLeaseDuration,
		Workload: ocihelper.WorkloadInput{ImageReference: reference, ImageDigest: digest, Argv: []string{"/bin/sh", "-c", "exec sleep 60"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := session.Signal(ctx, ocihelper.SignalRequest{Authority: signalAuthority, Signal: ocihelper.SignalKILL}); err != nil {
		t.Fatal(err)
	}
	var signalResult *ocihelper.WatchResponse
	if err := session.Watch(ctx, ocihelper.WatchRequest{Authority: signalAuthority}, func(event ocihelper.WatchEvent) error {
		if event.Result != nil {
			copy := *event.Result
			signalResult = &copy
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if signalResult == nil || signalResult.Signal != ocihelper.SignalKILL || signalResult.TerminationCause != "agent" {
		t.Fatalf("signal outcome = %+v", signalResult)
	}
	if _, err := session.Delete(ctx, ocihelper.DeleteRequest{Authority: signalAuthority}); err != nil {
		t.Fatal(err)
	}
	oomAuthority := nativeAuthority("oom")
	if _, err := session.Run(ctx, ocihelper.RunRequest{
		Authority: oomAuthority, InitialDeadman: l1.DefaultLeaseDuration,
		Workload: ocihelper.WorkloadInput{
			ImageReference: reference, ImageDigest: digest,
			Argv:   []string{"/bin/sh", "-c", "yes x | head -c 67108864 | sort >/dev/null"},
			Limits: ocihelper.WorkloadLimits{MemoryBytes: 8 << 20},
		},
	}); err != nil {
		t.Fatal(err)
	}
	var oomResult *ocihelper.WatchResponse
	if err := session.Watch(ctx, ocihelper.WatchRequest{Authority: oomAuthority}, func(event ocihelper.WatchEvent) error {
		if event.Result != nil {
			copy := *event.Result
			oomResult = &copy
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if oomResult == nil || !oomResult.OutOfMemory || oomResult.Signal != "" || oomResult.ExitCode == nil {
		t.Fatalf("OOM outcome = %+v", oomResult)
	}
	if _, err := session.Delete(ctx, ocihelper.DeleteRequest{Authority: oomAuthority}); err != nil {
		t.Fatal(err)
	}
	plain137 := nativeAuthority("plain-137")
	if _, err := session.Run(ctx, ocihelper.RunRequest{Authority: plain137, InitialDeadman: l1.DefaultLeaseDuration, Workload: ocihelper.WorkloadInput{ImageReference: reference, ImageDigest: digest, Argv: []string{"/bin/sh", "-c", "exit 137"}}}); err != nil {
		t.Fatal(err)
	}
	var plain137Result *ocihelper.WatchResponse
	if err := session.Watch(ctx, ocihelper.WatchRequest{Authority: plain137}, func(event ocihelper.WatchEvent) error {
		if event.Result != nil {
			copy := *event.Result
			plain137Result = &copy
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if plain137Result == nil || plain137Result.ExitCode == nil || *plain137Result.ExitCode != 137 || plain137Result.Signal != "" {
		t.Fatalf("plain exit 137 = %+v", plain137Result)
	}
	if _, err := session.Delete(ctx, ocihelper.DeleteRequest{Authority: plain137}); err != nil {
		t.Fatal(err)
	}

	for _, loss := range []string{"kill-shim", "stop-containerd"} {
		authority := nativeAuthority(loss)
		if _, err := session.Run(ctx, ocihelper.RunRequest{Authority: authority, InitialDeadman: l1.DefaultLeaseDuration, Workload: ocihelper.WorkloadInput{ImageReference: reference, ImageDigest: digest, Argv: []string{"/bin/sh", "-c", "exec sleep 60"}}}); err != nil {
			t.Fatal(err)
		}
		requestRootFault(t, loss)
		var lossResult *ocihelper.WatchResponse
		if err := session.Watch(ctx, ocihelper.WatchRequest{Authority: authority}, func(event ocihelper.WatchEvent) error {
			if event.Result != nil {
				copy := *event.Result
				lossResult = &copy
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if lossResult == nil || lossResult.RuntimeFailure == "" {
			t.Fatalf("%s result = %+v, want runtime failure", loss, lossResult)
		}
		if loss == "stop-containerd" {
			requestRootFault(t, "start-containerd")
		}
		if _, err := session.Delete(ctx, ocihelper.DeleteRequest{Authority: authority}); err != nil {
			t.Fatal(err)
		}
	}
	exerciseOrdinaryL3OCIOneshot(t, ctx, barrier, adapter, echoReference, echoImage.TopLevelDigest, reference, digest)
	if err := barrier.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Probe(ctx, "native-node", "native-boot", reference, digest, l1.DefaultLeaseDuration); err != nil {
		t.Fatal(err)
	}
	session, err = barrier.Session()
	if err != nil {
		t.Fatal(err)
	}

	controlAuthority := nativeAuthority("control-loss")
	if _, err := session.Run(ctx, ocihelper.RunRequest{Authority: controlAuthority, InitialDeadman: l1.DefaultLeaseDuration, Workload: ocihelper.WorkloadInput{ImageReference: reference, ImageDigest: digest, Argv: []string{"/bin/sh", "-c", "exec sleep 60"}}}); err != nil {
		t.Fatal(err)
	}
	if err := barrier.Close(); err != nil {
		t.Fatal(err)
	}
	replacement, err := ocihelper.NewBootBarrier(client, ocihelper.AcquireSessionRequest{NodeID: "native-node", BootSessionID: "native-boot-after-loss"})
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if err := replacement.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	session, err = replacement.Session()
	if err != nil {
		t.Fatal(err)
	}
	verification, err = session.Verify(ctx, ocihelper.VerifyRequest{Scope: ocihelper.VerifyNamespace})
	if err != nil || !verification.Absent {
		t.Fatalf("namespace cleanup verification=%+v err=%v", verification, err)
	}
	if evidenceDirectory := os.Getenv("WEFTY_REALTIME_EVIDENCE_DIR"); evidenceDirectory != "" {
		if err := os.WriteFile(filepath.Join(evidenceDirectory, "node-doctor.json"), append(doctorBundle, '\n'), 0o600); err != nil {
			t.Fatal(err)
		}
		registryEvidence := "pull_from_empty=true\npull_import_digest_equal=true\n"
		bindingRepullEvidence := fmt.Sprintf("binding_repull_reconciliation=%t\n", bindingRepullReconciliation)
		if prBuildRegistryNotRun {
			registryEvidence = "pull_from_empty=NOT-RUN\npull_from_empty_reason=pr-build: image not published\npull_import_digest_equal=NOT-RUN\npull_import_digest_equal_reason=pr-build: image not published\n"
			bindingRepullEvidence = "binding_repull_reconciliation=NOT-RUN\nbinding_repull_reconciliation_reason=pr-build: image not published\n"
		}
		evidence := fmt.Sprintf("agent_uid=%d\nhelper_uid=0\nhelper_socket_root_owned=true\nraw_socket_denied=true\nacceptance_reference=%s\nacceptance_index_digest=%s\npublic_acceptance_image=true\nnode_load_image=true\narchive_platform_filtered=true\ncache_cap_bytes=%d\nprobe_elapsed=%s\nproduction_deadman=%s\n%s%sregistry_disabled_pull_rejected=%t\nregistry_disabled_import=true\nimport_run=true\nprestart_requeue_pinned=true\ntag_refloat_resolved_once=true\nservice_echo_health=true\nservice_echo_body=true\nservice_data_root_user=%t\nservice_data_numeric_user=%t\nservice_data_named_user=%t\nservice_data_restart_persistent=%t\nservice_data_stop_start_persistent=%t\nservice_rootfs_discarded=%t\nservice_data_same_digest_replacement_fresh=%t\ncomputer_reference=%s\ncomputer_index_digest=%s\ncomputer_reference_separate=true\ncomputer_reference_archive_import=true\ncomputer_reference_atomic_readiness=%t\ncomputer_reference_started_to_ready_elapsed=%s\ncomputer_reference_publication_loss_recovery=%t\ncomputer_reference_wire_negatives=true\nwayland_computer_reference=%s\nwayland_computer_index_digest=%s\nwayland_computer_reference_separate=true\nwayland_computer_reference_archive_import=true\nwayland_computer_reference_atomic_readiness=%t\nwayland_computer_reference_started_to_ready_elapsed=%s\nwayland_computer_reference_publication_loss_recovery=%t\nwayland_computer_reference_wire_negatives=true\ncomputer_capacity_three_live_published_fourth_refused=true\ncomputer_disk_exactly_one_persistent_and_reset=%t\ncomputer_shm_mode_flags_size_1g=%t\ncomputer_shm_cgroup_charged=%t\ncomputer_cgroup_policy_readback=%t\ncomputer_disk_enospc_local=%t\ncomputer_oom_local=%t\ncomputer_agent_restart_same_generation=%t\ncomputer_reference_helper_stop_start_profile_sign_in_rootfs=%t\noneshot_handoff_marker_bytes=%t\noneshot_bridge_once=true\noneshot_split_streams=true\noneshot_digest_evidence=true\nordinary_l3_oci_submission=true\nordinary_l3_frozen_rerun=true\nwait_before_start=true\nlive_log_delivery=true\nexit_code=7\nplain_137_exit=true\nsignal=KILL\nsignal_cause=agent\noom_kill=true\nshim_loss=runtime_failure\ncontainerd_stop=runtime_failure\ncontrol_loss_reaped=true\nstdout_log=true\nstderr_log=true\nnamespace_absent=true\n", os.Getuid(), echoReference, echoDigest, acceptanceCacheCap, probeElapsed, l1.DefaultLeaseDuration, registryEvidence, bindingRepullEvidence, registryDisabledPullRejected, serviceDataEvidence.rootUser, serviceDataEvidence.numericUser, serviceDataEvidence.namedUser, serviceDataEvidence.restartPersistent, serviceDataEvidence.stopStartPersistent, serviceDataEvidence.rootfsDiscarded, serviceDataEvidence.sameDigestReplacementFresh, computerReference, computerDigest, referenceComputerReadiness.atomicPublication, referenceComputerReadiness.startedToReadyElapsed, referenceComputerReadiness.lossRecovery, waylandComputerReference, waylandComputerDigest, waylandComputerReadiness.atomicPublication, waylandComputerReadiness.startedToReadyElapsed, waylandComputerReadiness.lossRecovery, computerDiskEvidence.exactlyOnePersistentAndReset, computerDiskEvidence.shmModeFlagsSizeOneGiB, computerDiskEvidence.shmCgroupCharged, computerDiskEvidence.cgroupPolicyReadback, computerDiskEvidence.diskENOSPCLocal, computerDiskEvidence.oomLocal, computerAgentRestartEvidence, computerAgentRestartEvidence, handoffMarkerBytes)
		if err := os.WriteFile(filepath.Join(evidenceDirectory, "native-linux-oci.txt"), []byte(evidence), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

var nativeBarrierLossSequence atomic.Uint64

func recordNativeBarrierLoss(generation ocihelper.HelperSession, barrierStartedAt time.Time, barrierReadyUnixNano int64, lossErr error) error {
	evidenceDirectory := os.Getenv("WEFTY_REALTIME_EVIDENCE_DIR")
	if evidenceDirectory == "" {
		return nil
	}
	recordedAt := time.Now().UTC()
	readyAt := time.Time{}
	healthyElapsed := "not_ready"
	establishElapsed := "not_ready"
	if barrierReadyUnixNano != 0 {
		readyAt = time.Unix(0, barrierReadyUnixNano).UTC()
		healthyElapsed = recordedAt.Sub(readyAt).String()
		establishElapsed = readyAt.Sub(barrierStartedAt).String()
	}
	lossText := "OCI helper session lost without an error value"
	if lossErr != nil {
		lossText = lossErr.Error()
	}
	payload, err := json.MarshalIndent(map[string]any{
		"helper_session":              generation,
		"barrier_started_at":          barrierStartedAt.UTC(),
		"barrier_ready_at":            readyAt,
		"barrier_ready_recorded":      barrierReadyUnixNano != 0,
		"barrier_establish_elapsed":   establishElapsed,
		"healthy_before_loss_elapsed": healthyElapsed,
		"loss_recorded_at":            recordedAt,
		"loss_error":                  lossText,
	}, "", "  ")
	if err != nil {
		return err
	}
	name := fmt.Sprintf("native-linux-oci-barrier-loss-%02d.json", nativeBarrierLossSequence.Add(1))
	if err := os.WriteFile(filepath.Join(evidenceDirectory, name), append(payload, '\n'), 0o600); err != nil {
		return err
	}
	return nil
}

type referenceComputerEvidence struct {
	startedToReadyElapsed time.Duration
	atomicPublication     bool
	lossRecovery          bool
}

func exerciseNativeLinuxReferenceComputer(t *testing.T, ctx context.Context, session *ocihelper.Session, adapter *ocirunner.Adapter, identity, reference, digest string) referenceComputerEvidence {
	t.Helper()
	memory := int64(2 << 30)
	digestCopy := digest
	authority := workloadrunner.AttemptAuthority{NodeID: "native-node", BootSessionID: "native-boot", JobID: "reference-" + identity + "-job", AttemptID: "reference-" + identity + "-attempt", FencingToken: "reference-" + identity + "-fence", WorkloadClass: contract.JobClassService, RemovalGeneration: "attempt"}
	storage := &workloadrunner.ComputerStorage{ComputerID: "reference-" + identity + "-computer", StorageID: "reference-" + identity + "-storage", StorageGeneration: 1, IntentRevision: 1, DiskBytes: 128 << 20}
	request := workloadrunner.Request{Authority: authority, RuntimeHandler: ocihelper.DefaultRuntimeHandler, InitialDeadman: l1.DefaultLeaseDuration, LifetimeBoundary: workloadrunner.AgentBootLifetime,
		Execution:      contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: reference, Digest: &digestCopy}, Computer: &contract.OCIComputerSpec{Display: contract.OCIComputerDisplaySpec{Protocol: contract.ComputerDisplayProtocolRFBWebSocketV1}, DiskBytes: storage.DiskBytes}, Limits: &contract.OCILimits{MemoryBytes: &memory}}},
		ManagedVolumes: []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeComputerDisk, ComputerStorage: storage}}, AttemptEndpoints: []string{workloadrunner.AttemptEndpointView, workloadrunner.AttemptEndpointControl},
		OCIImageResolved: func(context.Context, workloadrunner.OCIImageObservation) error { return nil }, OCIStarted: func(context.Context, workloadrunner.OCIImageObservation) error { return nil }}
	helperStarted := make(chan time.Time, 1)
	request.OCIStartedAt = func(startedAt time.Time) { helperStarted <- startedAt }
	endpoints := make(map[string]workloadrunner.AttemptEndpoint)
	endpointReady := make(chan struct{}, 2)
	var endpointsBound atomic.Int32
	var endpointMu sync.Mutex
	request.AttemptEndpointReady = func(name string, endpoint workloadrunner.AttemptEndpoint) error {
		endpointMu.Lock()
		endpoints[name] = endpoint
		endpointMu.Unlock()
		endpointsBound.Add(1)
		endpointReady <- struct{}{}
		return nil
	}
	var available atomic.Bool
	available.Store(true)
	dial := func(dialContext context.Context, name string) (net.Conn, error) {
		if !available.Load() {
			return nil, errors.New("injected realtiming endpoint loss")
		}
		endpointMu.Lock()
		endpoint, ok := endpoints[name]
		endpointMu.Unlock()
		if !ok {
			return nil, fmt.Errorf("endpoint %q not ready", name)
		}
		return endpoint.Dial(dialContext)
	}
	type referencePublication struct {
		ready               bool
		beforeBothEndpoints bool
	}
	publications := make(chan referencePublication, 8)
	runContext, cancelRun := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	network := plain.NewNetwork()
	go func() {
		_, err := agent.RunComputerServiceRealtiming(runContext, adapter, request, network.NewFabric(fabric.Identity{NodeID: "reference-" + identity + "-agent"}), dial,
			func(_ context.Context, ready bool, _ string) error {
				publications <- referencePublication{ready: ready, beforeBothEndpoints: endpointsBound.Load() < 2}
				return nil
			})
		runDone <- err
	}()
	preparationDeadline := time.NewTimer(time.Minute)
	defer preparationDeadline.Stop()
	var startedAt time.Time
	select {
	case startedAt = <-helperStarted:
	case err := <-runDone:
		t.Fatalf("reference Computer ended before authoritative Started: %v", err)
	case <-ctx.Done():
		t.Fatalf("reference Computer helper setup before Started: %v", context.Cause(ctx))
	case <-preparationDeadline.C:
		t.Fatal("reference Computer helper setup did not reach authoritative Started within the dedicated preparation bound")
	}
	readinessRemaining := time.Until(startedAt.Add(contract.ComputerStartupReadinessTimeout))
	if readinessRemaining < 0 {
		readinessRemaining = 0
	}
	readinessDeadline := time.NewTimer(readinessRemaining)
	defer readinessDeadline.Stop()
	for range 2 {
		select {
		case <-endpointReady:
		case publication := <-publications:
			t.Fatalf("reference Computer published ready=%t before both helper endpoints were bound", publication.ready)
		case err := <-runDone:
			t.Fatalf("reference Computer ended before helper endpoints: %v", err)
		case <-ctx.Done():
			t.Fatalf("reference Computer helper endpoint setup: %v", context.Cause(ctx))
		case <-readinessDeadline.C:
			t.Fatal("helper did not return both reference endpoints after authoritative Started")
		}
	}
	select {
	case publication := <-publications:
		if publication.beforeBothEndpoints {
			t.Fatal("reference Computer publication was queued before both helper endpoints were bound")
		}
		if !publication.ready {
			t.Fatal("reference Computer first publication was not ready")
		}
	case err := <-runDone:
		t.Fatalf("reference Computer ended before atomic publication: %v", err)
	case <-ctx.Done():
		t.Fatalf("reference Computer publication: %v", context.Cause(ctx))
	case <-readinessDeadline.C:
		t.Fatal("reference Computer did not publish both bound endpoints inside the Started-based readiness deadline")
	}
	readyAt := time.Now()
	waitPublication := func(want bool) {
		t.Helper()
		select {
		case got := <-publications:
			if got.beforeBothEndpoints || got.ready != want {
				t.Fatalf("Computer publication=%+v, want ready=%t after both endpoints", got, want)
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("Computer publication did not become %t", want)
		}
	}
	helperAuthority := ocihelper.AttemptAuthority{NodeID: authority.NodeID, BootSessionID: authority.BootSessionID, JobID: authority.JobID, AttemptID: authority.AttemptID, FencingToken: authority.FencingToken, Class: authority.WorkloadClass, RemovalGeneration: authority.RemovalGeneration}
	assertReferenceComputerWireNegatives(t, ctx, session, helperAuthority)
	available.Store(false)
	waitPublication(false)
	available.Store(true)
	waitPublication(true)
	cancelRun()
	select {
	case <-runDone:
	case <-time.After(15 * time.Second):
		t.Fatal("reference Computer agent service did not stop")
	}
	if receipt, err := adapter.ReapAndFinalizeManagedVolumes(ctx, workloadrunner.ReapRequest{Authority: authority}, workloadrunner.ManagedVolumeFinalizationRequest{
		Authority: authority, Volumes: []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeComputerDisk, ComputerStorage: storage}},
		Removal: &workloadrunner.ManagedVolumeRemovalAuthority{NodeID: authority.NodeID, BootSessionID: authority.BootSessionID, JobID: authority.JobID, RemovalGeneration: 1, CleanupFence: "reference-" + identity + "-computer-cleanup"},
	}); err != nil || !receipt.RuntimeQuiesced {
		t.Fatalf("reference Computer runtime and disk cleanup = %+v err=%v", receipt, err)
	}
	return referenceComputerEvidence{startedToReadyElapsed: readyAt.Sub(startedAt), atomicPublication: true, lossRecovery: true}
}

func probeReferenceComputerPair(ctx context.Context, session *ocihelper.Session, authority ocihelper.AttemptAuthority) error {
	view, err := openReferenceComputerEndpoint(ctx, session, authority, contract.ComputerDisplayEndpointView, contract.ComputerDisplayWebSocketPath, []string{contract.ComputerDisplayWebSocketSubprotocol})
	if err != nil {
		return err
	}
	defer view.CloseNow()
	control, err := openReferenceComputerEndpoint(ctx, session, authority, contract.ComputerDisplayEndpointControl, contract.ComputerDisplayWebSocketPath, []string{contract.ComputerDisplayWebSocketSubprotocol})
	if err != nil {
		return err
	}
	defer control.CloseNow()
	return nil
}

func openReferenceComputerEndpoint(ctx context.Context, session *ocihelper.Session, authority ocihelper.AttemptAuthority, name, path string, protocols []string) (*websocket.Conn, error) {
	var used atomic.Bool
	transport := &http.Transport{DialContext: func(dialContext context.Context, _, _ string) (net.Conn, error) {
		if !used.CompareAndSwap(false, true) {
			return nil, errors.New("reference Computer probe attempted more than one helper dial")
		}
		return session.DialAttemptPort(dialContext, ocihelper.DialAttemptPortRequest{Authority: authority, Name: name})
	}}
	defer transport.CloseIdleConnections()
	connection, _, err := websocket.Dial(ctx, "ws://computer-backend.invalid"+path, &websocket.DialOptions{
		HTTPClient: &http.Client{Transport: transport}, Subprotocols: protocols,
	})
	if err != nil {
		return nil, err
	}
	if connection.Subprotocol() != contract.ComputerDisplayWebSocketSubprotocol {
		connection.CloseNow()
		return nil, fmt.Errorf("reference Computer %s negotiated subprotocol %q", name, connection.Subprotocol())
	}
	network := websocket.NetConn(ctx, connection, websocket.MessageBinary)
	banner := make([]byte, contract.ComputerRFBVersionBannerBytes)
	if _, err := io.ReadFull(network, banner); err != nil {
		connection.CloseNow()
		return nil, err
	}
	if !contract.ValidComputerRFBVersionBanner(banner) {
		connection.CloseNow()
		return nil, fmt.Errorf("reference Computer %s banner = %q", name, banner)
	}
	return connection, nil
}

func assertReferenceComputerWireNegatives(t *testing.T, ctx context.Context, session *ocihelper.Session, authority ocihelper.AttemptAuthority) {
	t.Helper()
	for _, test := range []struct {
		name      string
		path      string
		protocols []string
	}{
		{name: "wrong-path", path: "/wrong", protocols: []string{contract.ComputerDisplayWebSocketSubprotocol}},
		{name: "missing-protocol", path: contract.ComputerDisplayWebSocketPath},
		{name: "wrong-protocol", path: contract.ComputerDisplayWebSocketPath, protocols: []string{"base64"}},
	} {
		probeContext, cancel := context.WithTimeout(ctx, 5*time.Second)
		connection, err := openReferenceComputerEndpoint(probeContext, session, authority, contract.ComputerDisplayEndpointView, test.path, test.protocols)
		cancel()
		if connection != nil {
			connection.CloseNow()
		}
		if err == nil {
			t.Fatalf("reference Computer %s negative row upgraded", test.name)
		}
	}
	probeContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	connection, err := openReferenceComputerEndpoint(probeContext, session, authority, contract.ComputerDisplayEndpointView, contract.ComputerDisplayWebSocketPath, []string{contract.ComputerDisplayWebSocketSubprotocol})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	if err := connection.Write(probeContext, websocket.MessageText, []byte("forbidden")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := connection.Read(probeContext); err == nil {
		t.Fatal("reference Computer accepted a text frame and kept the connection open")
	} else {
		if probeContext.Err() != nil {
			t.Fatalf("reference Computer did not reject a text frame before the probe deadline: %v", err)
		}
		var timeout net.Error
		if errors.As(err, &timeout) && timeout.Timeout() {
			t.Fatalf("reference Computer did not reject a text frame before the probe deadline: %v", err)
		}
		if !ocihelper.ConformantComputerImageTextFrameRejection(err) {
			status := websocket.CloseStatus(err)
			t.Fatalf("reference Computer text-frame rejection = %v status=%v, want unsupported-data or EOF", err, status)
		}
	}
	live, err := openReferenceComputerEndpoint(probeContext, session, authority, contract.ComputerDisplayEndpointView,
		contract.ComputerDisplayWebSocketPath, []string{contract.ComputerDisplayWebSocketSubprotocol})
	if err != nil {
		t.Fatalf("reference Computer display was not live after text-frame rejection: %v", err)
	}
	live.CloseNow()
}

func exerciseNativeLinuxComputerAgentRestart(t *testing.T, ctx context.Context, adapter *ocirunner.Adapter, reference, digest string) bool {
	t.Helper()
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "native-computer.sqlite"), l1.StoreOptions{LeaseDuration: 3 * time.Second, Jitter: func(time.Duration) time.Duration { return 10 * time.Millisecond }})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registration := contract.NodeRegistration{NodeID: "native-computer-node", BootSessionID: "native-boot", OS: "linux", Architecture: "amd64", AgentVersion: "realtiming",
		Capabilities:       map[string]bool{"kind:oci": true, "cgroup_v2": true, "computer": true, "runtime_handler:" + ocihelper.DefaultRuntimeHandler: true},
		CapabilityRevision: 1, CapabilityObservedAt: time.Now().UTC(), MissingCapabilities: []string{}}
	policy := l1.NodePolicy{Tags: []string{contract.StableNodeTagPrefix + registration.NodeID}, MaxOneshotSlots: 1, MaxServiceSlots: 1}
	if _, err := store.RegisterNode(ctx, fabric.Identity{NodeID: "native-agent"}, registration, policy, true); err != nil {
		t.Fatal(err)
	}
	memory := int64(1 << 30)
	digestCopy := digest
	spec := contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: "native-computer-agent-restart", Kind: contract.JobKindOCI, Class: contract.JobClassService, Restart: contract.RestartAlways,
		RuntimeHandler: ocihelper.DefaultRuntimeHandler, RoutingTags: []string{contract.StableNodeTagPrefix + registration.NodeID},
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: reference, Digest: &digestCopy},
			Argv: []string{"/bin/sh", "-c", `if test -f /wefty/service/home/wefty/.config/chromium/wefty-profile-marker; then
  test "$(cat /wefty/service/home/wefty/.config/chromium/wefty-profile-marker)" = persistent-profile
  test "$(cat /wefty/service/home/wefty/.config/chromium/wefty-sign-in-marker)" = persistent-sign-in
  # Computer /tmp is tmpfs, so only this distinct root path proves rootfs discard.
  test ! -e /wefty-computer-rootfs-restart-witness
else
  mkdir -p /wefty/service/home/wefty/.config/chromium
  printf persistent-profile > /wefty/service/home/wefty/.config/chromium/wefty-profile-marker
  printf persistent-sign-in > /wefty/service/home/wefty/.config/chromium/wefty-sign-in-marker
  printf attempt-local > /wefty-computer-rootfs-restart-witness
  exit 17
fi`},
			Computer: &contract.OCIComputerSpec{Display: contract.OCIComputerDisplaySpec{Protocol: contract.ComputerDisplayProtocolRFBWebSocketV1}, DiskBytes: 32 << 20}, Limits: &contract.OCILimits{MemoryBytes: &memory}}}}
	computer, _, err := store.CreateComputer(ctx, l1.CreateComputerRequest{Name: "native-agent-restart", Spec: spec, Actor: "realtiming"})
	if err != nil {
		t.Fatal(err)
	}
	run := func(claim *l1.Claim) workloadrunner.Result {
		request := workloadrunner.Request{Authority: workloadrunner.AttemptAuthority{NodeID: registration.NodeID, BootSessionID: registration.BootSessionID, JobID: claim.Job.JobID,
			AttemptID: claim.Lease.AttemptID, FencingToken: claim.Lease.FencingToken, WorkloadClass: contract.JobClassService, RemovalGeneration: "attempt"},
			RuntimeHandler: claim.Job.Spec.RuntimeHandler, Execution: claim.Job.Spec.Execution, Limits: claim.Job.Spec.Limits, InitialDeadman: claim.Lease.LeaseTTL,
			ManagedVolumes:   []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeComputerDisk, ComputerStorage: &workloadrunner.ComputerStorage{ComputerID: claim.ComputerStorage.ComputerID, StorageID: claim.ComputerStorage.StorageID, StorageGeneration: claim.ComputerStorage.StorageGeneration, IntentRevision: claim.ComputerStorage.IntentRevision, DiskBytes: claim.Job.Spec.Execution.OCI.Computer.DiskBytes}}},
			AttemptEndpoints: []string{workloadrunner.AttemptEndpointView, workloadrunner.AttemptEndpointControl},
			AttemptEndpointReady: func(string, workloadrunner.AttemptEndpoint) error {
				return nil
			}}
		request.OCIImageResolved = func(callbackContext context.Context, observation workloadrunner.OCIImageObservation) error {
			_, err := store.ObserveAttemptImage(callbackContext, "native-agent", claim.Job.JobID, claim.Lease.AttemptID, nativeImageObservation(claim.Lease.FencingToken, observation))
			return err
		}
		request.OCIStarted = func(callbackContext context.Context, _ workloadrunner.OCIImageObservation) error {
			_, err := store.StartAttempt(callbackContext, "native-agent", claim.Job.JobID, claim.Lease.AttemptID, l1.StartedRequest{FencingToken: claim.Lease.FencingToken})
			return err
		}
		result, runErr := adapter.Run(ctx, request, workloadrunner.OutputSinkFunc(func(context.Context, contract.LogEvent) error { return nil }))
		if runErr != nil {
			t.Fatal(runErr)
		}
		if receipt, reapErr := adapter.ReapAndVerify(ctx, workloadrunner.ReapRequest{Authority: request.Authority}); reapErr != nil || !receipt.RuntimeQuiesced {
			t.Fatalf("Computer L1 adapter reap = %+v err=%v", receipt, reapErr)
		}
		if _, completeErr := store.CompleteAttempt(ctx, "native-agent", claim.Job.JobID, claim.Lease.AttemptID, l1.CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "complete-" + claim.Lease.AttemptID, Result: l1.ProcessResult(result.Outcome)}); completeErr != nil {
			t.Fatal(completeErr)
		}
		return result
	}
	first, err := store.ClaimJob(ctx, "native-agent", registration.NodeID, registration.BootSessionID, contract.JobClassService)
	if err != nil || first == nil || first.ComputerStorage == nil || first.Job.JobID != computer.CurrentJobID {
		t.Fatalf("first Computer claim = %+v err=%v", first, err)
	}
	firstResult := run(first)
	if firstResult.Outcome.ExitCode == nil || *firstResult.Outcome.ExitCode != 17 {
		t.Fatalf("first Computer marker run = %+v", firstResult.Outcome)
	}
	var second *l1.Claim
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && second == nil {
		second, err = store.ClaimJob(ctx, "native-agent", registration.NodeID, registration.BootSessionID, contract.JobClassService)
		if err != nil {
			t.Fatal(err)
		}
		if second == nil {
			time.Sleep(25 * time.Millisecond)
		}
	}
	if second == nil || second.ComputerStorage == nil || second.ComputerStorage.StorageGeneration != first.ComputerStorage.StorageGeneration {
		t.Fatalf("same-generation Computer restart claim = %+v", second)
	}
	secondResult := run(second)
	if secondResult.Outcome.ExitCode == nil || *secondResult.Outcome.ExitCode != 0 {
		t.Fatalf("agent-restart marker was not retained = %+v", secondResult.Outcome)
	}
	return true
}

type nativeComputerDiskEvidence struct {
	exactlyOnePersistentAndReset bool
	shmModeFlagsSizeOneGiB       bool
	shmCgroupCharged             bool
	cgroupPolicyReadback         bool
	diskENOSPCLocal              bool
	oomLocal                     bool
}

func exerciseNativeLinuxComputerCapacity(t *testing.T, ctx context.Context, barrier *ocihelper.BootBarrier, reference, digest string) {
	t.Helper()
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	requests := make([]ocihelper.RunRequest, 0, 3)
	responses := make([]ocihelper.RunResponse, 0, 3)
	inventories := make([]ocihelper.ResourceInventory, 0, 3)
	request := func(index int) ocihelper.RunRequest {
		authority := nativeAuthority(fmt.Sprintf("capacity-%d", index))
		authority.Class = contract.JobClassService
		storage := &ocihelper.ComputerStorageReference{
			ComputerID: fmt.Sprintf("capacity-computer-%d", index), StorageID: fmt.Sprintf("capacity-storage-%d", index),
			StorageGeneration: 1, IntentRevision: 1, DiskBytes: 32 << 20,
		}
		return ocihelper.RunRequest{
			Authority: authority, InitialDeadman: l1.DefaultLeaseDuration, AllocateEndpoints: []string{"view", "control"},
			Workload: ocihelper.WorkloadInput{ImageReference: reference, ImageDigest: digest, Computer: true,
				Argv: []string{"/bin/sh", "-c", `
WEFTY_SERVICE_PORT="$WEFTY_COMPUTER_VIEW_PORT" /usr/local/bin/wefty-echo-service &
WEFTY_SERVICE_PORT="$WEFTY_COMPUTER_CONTROL_PORT" /usr/local/bin/wefty-echo-service &
wait
`}, Limits: ocihelper.WorkloadLimits{MemoryBytes: 1 << 30},
				ManagedVolumes: []ocihelper.ManagedVolumeDescriptor{{Kind: ocihelper.ManagedVolumeComputerDisk, ComputerStorage: storage}}},
		}
	}
	assertPublished := func(run ocihelper.RunRequest, name string) {
		t.Helper()
		client := &http.Client{Transport: &http.Transport{DialContext: func(dialContext context.Context, _, _ string) (net.Conn, error) {
			return session.DialAttemptPort(dialContext, ocihelper.DialAttemptPortRequest{Authority: run.Authority, Name: name})
		}}, Timeout: 5 * time.Second}
		defer client.CloseIdleConnections()
		response, dialErr := client.Get("http://computer-endpoint/healthz")
		if dialErr != nil {
			t.Fatalf("Computer %s endpoint %s was not published: %v", run.Authority.JobID, name, dialErr)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("Computer %s endpoint %s status = %d", run.Authority.JobID, name, response.StatusCode)
		}
	}
	for index := 1; index <= 3; index++ {
		run := request(index)
		response, runErr := session.Run(ctx, run)
		if runErr != nil || !response.Started || !response.Admission.Admitted || len(response.Endpoints) != 2 {
			t.Fatalf("capacity resident %d = response %+v err=%v", index, response, runErr)
		}
		verification, verifyErr := session.Verify(ctx, ocihelper.VerifyRequest{Scope: ocihelper.VerifyAttempt, Authority: &run.Authority})
		if verifyErr != nil || verification.Absent || len(verification.Inventory.Tasks) != 1 || len(verification.Inventory.Containers) != 1 {
			t.Fatalf("capacity resident %d inventory = %+v err=%v", index, verification, verifyErr)
		}
		assertPublished(run, "view")
		assertPublished(run, "control")
		requests = append(requests, run)
		responses = append(responses, response)
		inventories = append(inventories, verification.Inventory)
	}
	fourth := request(4)
	if _, err := session.Run(ctx, fourth); err == nil {
		t.Fatal("fourth 1 GiB Computer was admitted above the configured 3 GiB cap budget")
	} else {
		var rpcErr *ocihelper.RPCError
		if !errors.As(err, &rpcErr) || rpcErr.Code != ocihelper.CodeInsufficientMemory {
			t.Fatalf("fourth Computer refusal = %v", err)
		}
	}
	for index, run := range requests {
		verification, verifyErr := session.Verify(ctx, ocihelper.VerifyRequest{Scope: ocihelper.VerifyAttempt, Authority: &run.Authority})
		if verifyErr != nil || verification.Absent || !reflect.DeepEqual(verification.Inventory, inventories[index]) || !responses[index].Admission.Admitted || len(responses[index].Endpoints) != 2 {
			t.Fatalf("resident %d changed after refusal: before=%+v after=%+v response=%+v err=%v", index+1, inventories[index], verification.Inventory, responses[index], verifyErr)
		}
		assertPublished(run, "view")
		assertPublished(run, "control")
	}
	for _, run := range requests {
		if err := session.Signal(ctx, ocihelper.SignalRequest{Authority: run.Authority, Signal: ocihelper.SignalKILL}); err != nil {
			t.Fatal(err)
		}
		if err := session.Watch(ctx, ocihelper.WatchRequest{Authority: run.Authority}, func(ocihelper.WatchEvent) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if deleted, err := session.Delete(ctx, ocihelper.DeleteRequest{Authority: run.Authority}); err != nil || !deleted.Deleted {
			t.Fatalf("capacity resident cleanup = %+v err=%v", deleted, err)
		}
	}
}

func exerciseNativeLinuxComputerDisk(t *testing.T, ctx context.Context, barrier *ocihelper.BootBarrier, reference, digest string) nativeComputerDiskEvidence {
	t.Helper()
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	storage := &ocihelper.ComputerStorageReference{
		ComputerID: "native-computer", StorageID: "native-storage", StorageGeneration: 1, IntentRevision: 1, DiskBytes: 32 << 20,
	}
	request := func(suffix string, argv []string) ocihelper.RunRequest {
		authority := nativeAuthority("computer-disk-" + suffix)
		authority.Class = contract.JobClassService
		return ocihelper.RunRequest{
			Authority: authority, InitialDeadman: l1.DefaultLeaseDuration,
			AllocateEndpoints: []string{"view", "control"},
			Workload: ocihelper.WorkloadInput{
				ImageReference: reference, ImageDigest: digest, Computer: true, Argv: argv,
				Limits:         ocihelper.WorkloadLimits{MemoryBytes: 64 << 20},
				ManagedVolumes: []ocihelper.ManagedVolumeDescriptor{{Kind: ocihelper.ManagedVolumeComputerDisk, ComputerStorage: storage}},
			},
		}
	}
	first := request("a", []string{"/bin/sh", "-c", `test "$(cat /wefty/control/driver.json)" = '{"version":1,"human_driving":false}' && grep -q ' /wefty/control tmpfs ' /proc/mounts && test ! -w /wefty/control/driver.json || exit 18; test "$(stat -c %a /dev/shm)" = 1777 || exit 20; awk '$2 == "/dev/shm" && $3 == "tmpfs" && index("," $4 ",", ",nosuid,") && index("," $4 ",", ",nodev,") && index("," $4 ",", ",noexec,") && index($4, "size=1048576k") { found=1 } END { exit !found }' /proc/mounts || exit 21; before=$(cat /sys/fs/cgroup/memory.current) || exit 22; dd if=/dev/zero of=/dev/shm/wefty-memory-charge bs=1048576 count=16 2>/dev/null || exit 23; after=$(cat /sys/fs/cgroup/memory.current) || exit 24; test "$after" -gt "$before" || exit 25; rm /dev/shm/wefty-memory-charge || exit 26; for i in $(seq 1 50); do test "$(cat /wefty/control/driver.json)" = '{"version":1,"human_driving":true}' && break; sleep .1; done; test "$(cat /wefty/control/driver.json)" = '{"version":1,"human_driving":true}' || exit 19; printf computer-disk-marker > /wefty/service/marker; exec sleep 60`})
	firstResponse, err := session.Run(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	if firstResponse.Profile.MemoryMaxBytes != 64<<20 || !firstResponse.Profile.MemoryOOMGroup || firstResponse.Profile.MemorySwapMaxBytes != 0 ||
		!firstResponse.Admission.Admitted || firstResponse.Admission.RequestedMemoryBytes != 64<<20 || firstResponse.Admission.RequestedDiskBytes != storage.DiskBytes {
		t.Fatalf("real Computer cgroup/admission readback = profile=%+v admission=%+v", firstResponse.Profile, firstResponse.Admission)
	}
	if err := session.SetComputerControlState(ctx, ocihelper.SetComputerControlStateRequest{Authority: first.Authority, HumanDriving: true}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	contender := request("b", []string{"/bin/true"})
	if _, err := session.Run(ctx, contender); err == nil {
		t.Fatal("real Computer attempt B attached while A owned the Storage generation")
	}
	requestRootFault(t, "kill-helper")
	barrier.Invalidate()
	if err := barrier.Ensure(ctx); err != nil {
		t.Fatalf("Computer helper-death sweep failed: %v", err)
	}
	receipt, ok := barrier.SweepReceipt()
	if !ok {
		t.Fatalf("Computer helper-death sweep receipt = %+v present=%t", receipt, ok)
	}
	assertNativeComputerSweepReceipt(t, receipt)
	assertNativeComputerHostCleanup(t, first.Authority)
	session, err = barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	second := request("c", []string{"/bin/sh", "-c", `test "$(cat /wefty/control/driver.json)" = '{"version":1,"human_driving":false}' && test "$(cat /wefty/service/marker)" = computer-disk-marker || exit 30; dd if=/dev/zero of=/wefty/service/fill bs=1048576 count=64 2>/tmp/disk-error && exit 31; grep -q 'No space left on device' /tmp/disk-error || exit 32; exit 42`})
	if _, err := session.Run(ctx, second); err != nil {
		t.Fatalf("real Computer attempt C did not consume A's reap receipt: %v", err)
	}
	var result *ocihelper.WatchResponse
	if err := session.Watch(ctx, ocihelper.WatchRequest{Authority: second.Authority}, func(event ocihelper.WatchEvent) error {
		if event.Result != nil {
			result = event.Result
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if result == nil || result.ExitCode == nil || *result.ExitCode != 42 || result.DiskExhausted {
		t.Fatalf("real Computer persistent marker/tenant-local ENOSPC result = %+v", result)
	}
	if _, err := session.Run(ctx, request("d", []string{"/bin/true"})); err == nil {
		t.Fatal("sweep receipt authorized more than one replacement Computer attach")
	}
	if deleted, err := session.Delete(ctx, ocihelper.DeleteRequest{Authority: second.Authority}); err != nil || !deleted.Deleted {
		t.Fatalf("real Computer attempt C reap = %+v err=%v", deleted, err)
	}
	assertNativeComputerHostCleanup(t, second.Authority)
	resetStorage := *storage
	resetStorage.IntentRevision = 2
	reset, err := session.ResetComputerStorage(ctx, ocihelper.ResetComputerStorageRequest{
		Storage: resetStorage, NewGeneration: 2,
		Authority: ocihelper.ComputerStorageResetAuthority{
			NodeID: second.Authority.NodeID, BootSessionID: second.Authority.BootSessionID,
			HelperGeneration: session.Handshake().SessionGeneration, RootInstanceID: "native-managed-root",
			JobID:          second.Authority.JobID,
			IntentRevision: 2, CleanupFence: "native-storage-reset",
		},
	})
	if err != nil || !reset.Verified || reset.Receipt.Kind != "computer_storage_reset_verified" ||
		reset.Receipt.HelperGeneration != session.Handshake().SessionGeneration {
		t.Fatalf("real Computer Storage reset = %+v err=%v", reset, err)
	}
	if _, err := session.Run(ctx, request("stale-after-reset", []string{"/bin/true"})); err == nil {
		t.Fatal("real retired Computer Storage generation attached after reset")
	}
	storage.StorageGeneration = 2
	storage.IntentRevision = 2
	fresh := request("fresh-after-reset", []string{"/bin/sh", "-c", "test ! -e /wefty/service/marker || exit 33; yes x | head -c 268435456 | sort >/dev/null"})
	if _, err := session.Run(ctx, fresh); err != nil {
		t.Fatalf("real empty Computer Storage generation did not attach: %v", err)
	}
	var freshResult *ocihelper.WatchResponse
	if err := session.Watch(ctx, ocihelper.WatchRequest{Authority: fresh.Authority}, func(event ocihelper.WatchEvent) error {
		if event.Result != nil {
			freshResult = event.Result
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if freshResult == nil || !freshResult.OutOfMemory {
		t.Fatalf("real reset generation/Computer-local OOM result = %+v", freshResult)
	}
	if deleted, err := session.Delete(ctx, ocihelper.DeleteRequest{Authority: fresh.Authority}); err != nil || !deleted.Deleted {
		t.Fatalf("real reset Computer cleanup = %+v err=%v", deleted, err)
	}
	// All receipt fields are assertion-derived: the guest's shm checks must
	// reach the persistent marker, and every disk/reset assertion above must
	// complete, before this evidence is emitted.
	return nativeComputerDiskEvidence{exactlyOnePersistentAndReset: true, shmModeFlagsSizeOneGiB: true, shmCgroupCharged: true, cgroupPolicyReadback: true, diskENOSPCLocal: true, oomLocal: true}
}

func assertNativeComputerSweepReceipt(t *testing.T, receipt ocihelper.VerifiedSweepReceipt) {
	t.Helper()
	retained := receipt.VerifiedRetained
	retainedRuntimeEmpty := len(retained.Leases)+len(retained.Snapshots)+len(retained.Containers)+len(retained.Tasks)+
		len(retained.Shims)+len(retained.Cgroups)+len(retained.LogSegments)+len(retained.ComputerDiskMounts)+
		len(retained.ComputerDiskLoops)+len(retained.ComputerAttachments)+len(retained.ComputerResetManifests)+
		len(retained.ComputerQuarantines)+len(retained.ComputerDiskAnomalies) == 0
	wantRecords := make([]string, len(retained.ManagedVolumes))
	for index, volume := range retained.ManagedVolumes {
		wantRecords[index] = volume + ".owner"
	}
	slices.Sort(wantRecords)
	computerDurableExact := len(retained.ComputerDiskImages) > 0 &&
		slices.Equal(retained.ComputerDiskImages, retained.ComputerDiskAllocations) &&
		slices.Equal(retained.ComputerDiskImages, retained.ComputerDiskQuotas) &&
		slices.Equal(retained.ComputerDiskImages, retained.ComputerDiskManifests)
	sweptRuntimeCovered := len(receipt.SweptInventory.Leases) > 0 && len(receipt.SweptInventory.Snapshots) > 0 &&
		len(receipt.SweptInventory.Containers) > 0 && len(receipt.SweptInventory.LogSegments) > 0 &&
		len(receipt.SweptInventory.ComputerDiskMounts) > 0 && len(receipt.SweptInventory.ComputerDiskLoops) > 0 &&
		len(receipt.SweptInventory.ComputerAttachments) > 0
	if !receipt.VerifiedAbsent || !ocihelper.InventoryEmpty(receipt.VerifiedResidue) ||
		!reflect.DeepEqual(receipt.VerifiedInventory, retained) || !retainedRuntimeEmpty ||
		len(retained.ManagedVolumes) == 0 || !slices.Equal(wantRecords, retained.ManagedVolumeRecords) ||
		!computerDurableExact || !sweptRuntimeCovered || inventoryHasDuplicateIdentity(receipt.SweptInventory) {
		t.Fatalf("Computer helper-death residue model = swept:%+v verified:%+v residue:%+v retained:%+v absent:%t",
			receipt.SweptInventory, receipt.VerifiedInventory, receipt.VerifiedResidue, retained, receipt.VerifiedAbsent)
	}
}

func inventoryHasDuplicateIdentity(inventory ocihelper.ResourceInventory) bool {
	classes := [][]string{
		inventory.Leases, inventory.Snapshots, inventory.Containers, inventory.Tasks, inventory.Shims, inventory.Cgroups,
		inventory.LogSegments, inventory.ManagedVolumes, inventory.ManagedVolumeRecords, inventory.ComputerDiskImages,
		inventory.ComputerDiskAllocations, inventory.ComputerDiskQuotas, inventory.ComputerDiskManifests,
		inventory.ComputerDiskMounts, inventory.ComputerDiskLoops, inventory.ComputerAttachments,
		inventory.ComputerResetManifests, inventory.ComputerQuarantines, inventory.ComputerDiskAnomalies,
	}
	for _, class := range classes {
		seen := make(map[string]struct{}, len(class))
		for _, identity := range class {
			if _, exists := seen[identity]; exists {
				return true
			}
			seen[identity] = struct{}{}
		}
	}
	return false
}

func assertNativeComputerHostCleanup(t *testing.T, authority ocihelper.AttemptAuthority) {
	t.Helper()
	identity, err := ocihelper.DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	requestRootFault(t, "assert-computer-clean:"+identity.LogSegmentDirectory)
}

type nativeServiceDataImage struct {
	name, reference, digest, owner string
}

type nativeServiceDataEvidence struct {
	rootUser, numericUser, namedUser                        bool
	restartPersistent, stopStartPersistent, rootfsDiscarded bool
	sameDigestReplacementFresh                              bool
}

func loadNativeImageArchive(t *testing.T, ctx context.Context, adapter *ocirunner.Adapter, reference, archivePath string) ocihelper.EnsureImageResponse {
	t.Helper()
	archive, err := os.Open(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	image, loadErr := adapter.LoadImage(ctx, reference, archive)
	closeErr := archive.Close()
	if loadErr != nil || closeErr != nil {
		t.Fatal(errors.Join(loadErr, closeErr))
	}
	return image
}

func loadNativeImageThroughCLI(t *testing.T, ctx context.Context, adapter *ocirunner.Adapter, cliPath, archivePath string) ocicontrol.LoadImageResponse {
	t.Helper()
	root := t.TempDir()
	intentPath := filepath.Join(root, "intent.json")
	if _, err := lima.InitializeOCIIntent(intentPath, time.Now()); err != nil {
		t.Fatal(err)
	}
	controller, err := ocicontrol.NewController(ocicontrol.ControllerConfig{IntentPath: intentPath, Images: adapter})
	if err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(root, "control.sock")
	server, err := ocicontrol.NewServer(socketPath, controller)
	if err != nil {
		t.Fatal(err)
	}
	serverContext, cancelServer := context.WithCancel(ctx)
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(serverContext) }()
	t.Cleanup(func() {
		cancelServer()
		if err := <-serverDone; err != nil {
			t.Errorf("stop node-local control server: %v", err)
		}
	})
	deadline := time.Now().Add(2 * time.Second)
	for {
		if info, statErr := os.Lstat(socketPath); statErr == nil && info.Mode()&os.ModeSocket != 0 {
			break
		} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			t.Fatal(statErr)
		}
		if time.Now().After(deadline) {
			t.Fatal("node-local control server did not publish its socket")
		}
		time.Sleep(10 * time.Millisecond)
	}
	configPath := filepath.Join(root, "node.json")
	if err := ocicontrol.WriteInstalledConfig(configPath, ocicontrol.InstalledConfig{Version: ocicontrol.InstalledConfigVersion, ControlSocket: socketPath}); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, cliPath,
		"--fabric=invalid-must-not-open", "--node-config="+configPath, "--json", "node", "load-image", archivePath,
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("wefty node load-image: %v output=%s", err, output)
	}
	var response ocicontrol.LoadImageResponse
	if err := json.Unmarshal(output, &response); err != nil {
		t.Fatalf("decode wefty node load-image response: %v output=%s", err, output)
	}
	return response
}

func exerciseNativeLinuxServiceData(t *testing.T, ctx context.Context, barrier *ocihelper.BootBarrier, adapter *ocirunner.Adapter, images []nativeServiceDataImage) nativeServiceDataEvidence {
	t.Helper()
	evidence := nativeServiceDataEvidence{}
	for _, image := range images {
		jobID := "service-data-" + image.name
		for attempt := 1; attempt <= 3; attempt++ {
			script := fmt.Sprintf(`
set -eu
test "$(stat -c '%%u:%%g' /wefty/service)" = %q
# The rootfs-discard witness must remain writable by numeric and named image users.
test ! -e /tmp/wefty-rootfs-attempt-marker
prior=0
if test -f /wefty/service/attempt-count; then prior="$(cat /wefty/service/attempt-count)"; fi
test "$prior" -eq %d
printf '%%d\n' %d > /wefty/service/attempt-count
touch /tmp/wefty-rootfs-attempt-marker
`, image.owner, attempt-1, attempt)
			request := nativeAdapterRequest(image.reference, image.digest, fmt.Sprintf("%s-%d", jobID, attempt), nil)
			request.Authority.JobID = jobID
			request.Authority.WorkloadClass = contract.JobClassService
			request.ManagedVolumes = []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeServiceData}}
			request.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error { return nil }
			runNativeEchoService(t, ctx, barrier, adapter, &request, script)
			if receipt, err := adapter.ReapAndVerify(ctx, workloadrunner.ReapRequest{Authority: request.Authority}); err != nil || !receipt.RuntimeQuiesced {
				t.Fatalf("%s attempt %d cleanup = %+v err=%v", image.name, attempt, receipt, err)
			}
		}
		switch image.name {
		case "root":
			evidence.rootUser = true
		case "numeric":
			evidence.numericUser = true
		case "named":
			evidence.namedUser = true
		}
		evidence.restartPersistent = true
		evidence.stopStartPersistent = true
		evidence.rootfsDiscarded = true
		if image.name == "root" {
			replacement := nativeAdapterRequest(image.reference, image.digest, jobID+"-replacement-1", nil)
			replacement.Authority.JobID = jobID + "-replacement"
			replacement.Authority.WorkloadClass = contract.JobClassService
			replacement.ManagedVolumes = []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeServiceData}}
			replacement.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error { return nil }
			runNativeEchoService(t, ctx, barrier, adapter, &replacement, "set -eu; test ! -e /wefty/service/attempt-count; printf 'replacement\\n' > /wefty/service/replacement")
			if receipt, err := adapter.ReapAndVerify(ctx, workloadrunner.ReapRequest{Authority: replacement.Authority}); err != nil || !receipt.RuntimeQuiesced {
				t.Fatalf("same-digest replacement cleanup = %+v err=%v", receipt, err)
			}
			original := nativeAdapterRequest(image.reference, image.digest, jobID+"-4", nil)
			original.Authority.JobID = jobID
			original.Authority.WorkloadClass = contract.JobClassService
			original.ManagedVolumes = []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeServiceData}}
			original.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error { return nil }
			runNativeEchoService(t, ctx, barrier, adapter, &original, "set -eu; test \"$(cat /wefty/service/attempt-count)\" -eq 3; test ! -e /wefty/service/replacement; printf '4\\n' > /wefty/service/attempt-count")
			if receipt, err := adapter.ReapAndVerify(ctx, workloadrunner.ReapRequest{Authority: original.Authority}); err != nil || !receipt.RuntimeQuiesced {
				t.Fatalf("original same-digest service cleanup = %+v err=%v", receipt, err)
			}
			evidence.sameDigestReplacementFresh = true
			if err := adapter.ReleaseOCIImageBindingPin(ctx, replacement.Authority.JobID); err != nil {
				t.Fatal(err)
			}
		}
		if err := adapter.ReleaseOCIImageBindingPin(ctx, jobID); err != nil {
			t.Fatal(err)
		}
	}
	return evidence
}

func runNativeEchoService(t *testing.T, ctx context.Context, barrier *ocihelper.BootBarrier, adapter *ocirunner.Adapter, request *workloadrunner.Request, prelude string) {
	t.Helper()
	request.Execution.OCI.Argv = []string{"/bin/sh", "-c", prelude + "\nexec /usr/local/bin/wefty-echo-service"}
	request.AttemptEndpoints = []string{workloadrunner.AttemptEndpointService}
	endpointReady := make(chan workloadrunner.AttemptEndpoint, 1)
	request.AttemptEndpointReady = func(name string, endpoint workloadrunner.AttemptEndpoint) error {
		if name != workloadrunner.AttemptEndpointService {
			return fmt.Errorf("unexpected service endpoint %q", name)
		}
		endpointReady <- endpoint
		return nil
	}
	type runResult struct {
		result workloadrunner.Result
		err    error
	}
	runDone := make(chan runResult, 1)
	go func() {
		result, err := adapter.Run(ctx, *request, nil)
		runDone <- runResult{result: result, err: err}
	}()
	var endpoint workloadrunner.AttemptEndpoint
	select {
	case endpoint = <-endpointReady:
	case result := <-runDone:
		t.Fatalf("echo service exited before endpoint readiness: result=%+v err=%v", result.result.Outcome, result.err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	transport := &http.Transport{DialContext: func(dialContext context.Context, _, _ string) (net.Conn, error) { return endpoint.Dial(dialContext) }}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	defer transport.CloseIdleConnections()
	var health struct {
		ServiceDirectory string `json:"service_directory"`
		ListeningPort    int    `json:"listening_port"`
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		response, err := client.Get("http://wefty-service/healthz")
		if err == nil && response.StatusCode == http.StatusOK {
			err = json.NewDecoder(response.Body).Decode(&health)
			_ = response.Body.Close()
			if err == nil {
				break
			}
		} else if response != nil {
			_ = response.Body.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("echo service health did not become ready: %v", err)
		}
		select {
		case result := <-runDone:
			t.Fatalf("echo service exited before health readiness: result=%+v err=%v", result.result.Outcome, result.err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	if health.ServiceDirectory != "/wefty/service" || health.ListeningPort != int(endpoint.Port) {
		t.Fatalf("echo service health = %+v endpoint=%+v", health, endpoint)
	}
	payload := []byte("published-echo-service:" + request.Authority.AttemptID)
	response, err := client.Post("http://wefty-service/echo", "application/octet-stream", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	echoed, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || response.StatusCode != http.StatusOK || !bytes.Equal(echoed, payload) {
		t.Fatalf("echo response status=%d bytes=%q err=%v", response.StatusCode, echoed, errors.Join(readErr, closeErr))
	}
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Signal(ctx, ocihelper.SignalRequest{Authority: ocirunner.HelperAuthority(request.Authority), Signal: ocihelper.SignalTERM}); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-runDone:
		if result.err != nil || result.result.Outcome.ExitCode == nil || *result.result.Outcome.ExitCode != 0 {
			t.Fatalf("echo service result=%+v err=%v", result.result.Outcome, result.err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func exerciseNativeLinuxOneshotContract(t *testing.T, ctx context.Context, adapter *ocirunner.Adapter, echoReference, echoDigest, probeReference, probeDigest string) bool {
	t.Helper()
	const token = "native-one-shot-token"
	volume := workloadrunner.ManagedVolume{Kind: workloadrunner.ManagedVolumeHandoff, OwnerKey: "native-one-shot-owner"}
	var bridgeCalls atomic.Int32
	bridge := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		bridgeCalls.Add(1)
		if request.Method != http.MethodGet || !strings.HasPrefix(request.URL.Path, "/v1/runs/native-one-shot") ||
			request.Header.Get("Authorization") != "Bearer "+token {
			t.Errorf("one-shot bridge request = %s %s authorization=%q", request.Method, request.URL.Path, request.Header.Get("Authorization"))
			response.WriteHeader(http.StatusUnauthorized)
			return
		}
		response.WriteHeader(http.StatusOK)
	}))
	defer bridge.Close()

	request := nativeAdapterRequest(echoReference, echoDigest, "one-shot-contract", []string{"/usr/local/bin/wefty-echo-service", "--once"})
	request.ManagedVolumes = []workloadrunner.ManagedVolume{volume}
	request.Execution.Env = map[string]string{
		contract.EnvRunID: "native-one-shot", contract.EnvL3Endpoint: bridge.URL,
		contract.EnvHandoffDir: "/operator-value-must-not-survive",
	}
	request.Execution.SensitiveEnv = map[string]string{contract.EnvRunToken: token}
	started := 0
	request.OCIStarted = func(_ context.Context, observation workloadrunner.OCIImageObservation) error {
		started++
		if observation.TopLevelDigest != echoDigest || observation.PlatformManifestDigest == "" {
			return fmt.Errorf("one-shot image evidence = %+v", observation)
		}
		return nil
	}
	var logs []contract.LogEvent
	result, err := adapter.Run(ctx, request, workloadrunner.OutputSinkFunc(func(_ context.Context, event contract.LogEvent) error {
		logs = append(logs, event)
		return nil
	}))
	if err != nil || result.Outcome.ExitCode == nil || *result.Outcome.ExitCode != 0 {
		t.Fatalf("native one-shot contract result=%+v err=%v", result.Outcome, err)
	}
	if started != 1 || bridgeCalls.Load() != 1 {
		t.Fatalf("native one-shot started=%d bridge_calls=%d, want exactly one each", started, bridgeCalls.Load())
	}
	if !containsLog(logs, contract.LogStdout, "wefty-echo-once-stdout\n") ||
		!containsLog(logs, contract.LogStderr, "wefty-echo-once-stderr\n") {
		t.Fatalf("native one-shot streams = %+v", logs)
	}
	if receipt, err := adapter.ReapAndVerify(ctx, workloadrunner.ReapRequest{Authority: request.Authority}); err != nil || !receipt.RuntimeQuiesced {
		t.Fatalf("native one-shot cleanup receipt=%+v err=%v", receipt, err)
	}

	// Attempt cleanup retains the stable helper-owned handoff. A second attempt
	// with the same owner must observe the exact marker bytes.
	readMarker := nativeAdapterRequest(probeReference, probeDigest, "handoff-read", []string{
		"/bin/sh", "-c", `test "$(cat /wefty/handoff/wefty-echo-once.txt)" = "wefty echo one-shot handoff"`,
	})
	readMarker.ManagedVolumes = []workloadrunner.ManagedVolume{volume}
	readMarker.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error { return nil }
	if result, err := adapter.Run(ctx, readMarker, nil); err != nil || result.Outcome.ExitCode == nil || *result.Outcome.ExitCode != 0 {
		t.Fatalf("retained handoff marker bytes result=%+v err=%v", result.Outcome, err)
	}
	if receipt, err := adapter.ReapAndVerify(ctx, workloadrunner.ReapRequest{Authority: readMarker.Authority}); err != nil || !receipt.RuntimeQuiesced {
		t.Fatalf("handoff read cleanup receipt=%+v err=%v", receipt, err)
	}
	if err := adapter.FinalizeManagedVolumes(ctx, workloadrunner.ManagedVolumeFinalizationRequest{
		Authority: request.Authority, Volumes: []workloadrunner.ManagedVolume{volume},
	}); err != nil {
		t.Fatal(err)
	}
	absentMarker := nativeAdapterRequest(probeReference, probeDigest, "handoff-absent", []string{
		"/bin/sh", "-c", `test ! -e /wefty/handoff/wefty-echo-once.txt`,
	})
	absentMarker.ManagedVolumes = []workloadrunner.ManagedVolume{volume}
	absentMarker.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error { return nil }
	if result, err := adapter.Run(ctx, absentMarker, nil); err != nil || result.Outcome.ExitCode == nil || *result.Outcome.ExitCode != 0 {
		t.Fatalf("accepted-completion handoff deletion result=%+v err=%v", result.Outcome, err)
	}
	if receipt, err := adapter.ReapAndVerify(ctx, workloadrunner.ReapRequest{Authority: absentMarker.Authority}); err != nil || !receipt.RuntimeQuiesced {
		t.Fatalf("handoff absence cleanup receipt=%+v err=%v", receipt, err)
	}

	postStarted := nativeAdapterRequest(echoReference, echoDigest, "one-shot-post-started-loss", []string{
		"/bin/sh", "-c", "/usr/local/bin/wefty-echo-service --once; sleep 60",
	})
	postStarted.ManagedVolumes = []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeHandoff, OwnerKey: "native-post-started-owner"}}
	postStarted.Execution.Env = map[string]string{contract.EnvRunID: "native-one-shot-loss", contract.EnvL3Endpoint: bridge.URL}
	postStarted.Execution.SensitiveEnv = map[string]string{contract.EnvRunToken: token}
	postStarted.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error {
		requestRootFault(t, "kill-shim")
		return nil
	}
	postResult, postErr := adapter.Run(ctx, postStarted, nil)
	if postErr != nil || postResult.Outcome.RuntimeFailure == nil || postResult.Outcome.RuntimeFailure.Code != contract.RuntimeFailureUnavailable {
		t.Fatalf("echo post-Started loss result=%+v err=%v", postResult.Outcome, postErr)
	}
	if receipt, err := adapter.ReapAndVerify(ctx, workloadrunner.ReapRequest{Authority: postStarted.Authority}); err != nil || !receipt.RuntimeQuiesced {
		t.Fatalf("echo post-Started loss cleanup receipt=%+v err=%v", receipt, err)
	}
	return true
}

func exerciseOrdinaryL3OCIOneshot(
	t *testing.T,
	ctx context.Context,
	barrier *ocihelper.BootBarrier,
	adapter *ocirunner.Adapter,
	echoReference, echoDigest, probeReference, probeDigest string,
) {
	t.Helper()
	network := plain.NewNetwork()
	controlFabric := network.NewFabric(fabric.Identity{NodeID: "native-control"})
	ledgerFabric := network.NewFabric(fabric.Identity{NodeID: "native-ledger", Tags: []string{l1.DefaultClientPrincipalTag}})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "native-agent", Tags: []string{l1.DefaultAgentPrincipalTag}})
	callerFabric := network.NewFabric(fabric.Identity{NodeID: "native-caller", UserID: "realtiming-person", Tags: []string{l3.DefaultCallerPrincipalTag}})

	l1Store, err := l1.OpenStore(filepath.Join(t.TempDir(), "ordinary-l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer l1Store.Close()
	l1Server, err := l1.NewServer(controlFabric, l1Store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{
		"native-node": l1.DefaultNodePolicy("linux"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	l1Listener, err := controlFabric.Listen("tcp", l3.DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	l3Store, err := l3.OpenStore(filepath.Join(t.TempDir(), "ordinary-l3.sqlite"), l3.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer l3Store.Close()
	l1Client, err := l3.NewL1Client(ledgerFabric, l3.DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	defer l1Client.CloseIdleConnections()
	reconciler, err := l3.NewReconciler(l3Store, l1Client, l3.ReconcilerConfig{Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	l3Server, err := l3.NewServer(ledgerFabric, l3Store, l3.ServerConfig{Reconciler: reconciler, Logs: l1Client})
	if err != nil {
		t.Fatal(err)
	}
	l3Listener, err := ledgerFabric.Listen("tcp", l3.DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}

	runContext, cancelRun := context.WithCancel(ctx)
	served := make(chan error, 2)
	go func() { served <- l1Server.Serve(runContext, l1Listener) }()
	go func() { served <- l3Server.Serve(runContext, l3Listener) }()
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	probe := realOCIProbe{run: func(probeContext context.Context) error {
		return adapter.Probe(probeContext, "native-node", "native-boot", probeReference, probeDigest, l1.DefaultLeaseDuration)
	}}
	nodeAgent, err := agent.New(agent.Config{
		Fabric: agentFabric, ControlPlaneAddress: l3.DefaultL1Address, RunLedgerAddress: l3.DefaultL3Address,
		NodeID: "native-node", BootSessionID: "native-boot", Version: "realtiming", OS: "linux", Architecture: runtime.GOARCH,
		Capabilities: map[string]bool{"kind:process": true}, CapabilityProbe: probe, OCIBootBarrier: barrier,
		WorkloadRuntimes:  map[string]agent.WorkloadRuntime{contract.JobKindOCI: adapter},
		HeartbeatInterval: time.Second, ClaimInterval: 25 * time.Millisecond, RenewalInterval: time.Second,
		LogSpoolDirectory: t.TempDir(), HandoffRoot: t.TempDir(), ManagedRootDirectory: managedRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	agentDone := make(chan error, 1)
	go func() { agentDone <- nodeAgent.Run(runContext) }()

	caller := &http.Client{Transport: &http.Transport{DialContext: func(dialContext context.Context, networkName, _ string) (net.Conn, error) {
		return callerFabric.Dial(dialContext, networkName, l3.DefaultL3Address)
	}}}
	defer caller.CloseIdleConnections()
	digest := echoDigest
	create := l3.CreateRunRequest{
		Image:  &contract.ImageProgram{Reference: echoReference, Digest: &digest, Argv: []string{"/usr/local/bin/wefty-echo-service", "--once"}},
		Params: json.RawMessage(`{"lane":"native-linux"}`), Tags: []string{"linux"},
	}
	var accepted l3.RunAccepted
	doNativeJSON(t, caller, http.MethodPost, "/v1/runs", create, http.Header{"Idempotency-Key": []string{"native-ordinary-oci"}}, http.StatusCreated, &accepted)
	original := waitNativeRun(t, caller, accepted.RunID, contract.RunSucceeded)
	if original.L1JobID == "" {
		t.Fatalf("ordinary OCI run omitted L1 job identity: %+v", original)
	}
	assertNativeRunLogs(t, caller, accepted.RunID)

	var rerun l3.RunAccepted
	doNativeJSON(t, caller, http.MethodPost, "/v1/runs/"+accepted.RunID+"/rerun", nil,
		http.Header{"Idempotency-Key": []string{"native-ordinary-oci-rerun"}}, http.StatusCreated, &rerun)
	rerunRecord := waitNativeRun(t, caller, rerun.RunID, contract.RunSucceeded)
	if rerunRecord.L1JobID == "" {
		t.Fatalf("ordinary OCI rerun omitted L1 job identity: %+v", rerunRecord)
	}
	assertNativeRunLogs(t, caller, rerun.RunID)

	cancelRun()
	if err := <-agentDone; err != nil {
		t.Fatal(err)
	}
	nodeAgent.Close()
	for range 2 {
		if err := <-served; err != nil {
			t.Fatal(err)
		}
	}
}

type realOCIProbe struct{ run func(context.Context) error }

func (probe realOCIProbe) Probe(ctx context.Context) (agent.CapabilityProbeResult, error) {
	if err := probe.run(ctx); err != nil {
		return agent.CapabilityProbeResult{}, err
	}
	return agent.CapabilityProbeResult{Capabilities: map[string]bool{
		"kind:oci": true, "runtime_handler:" + ocihelper.DefaultRuntimeHandler: true,
	}}, nil
}

func doNativeJSON(t *testing.T, client *http.Client, method, path string, input any, headers http.Header, wantStatus int, output any) {
	t.Helper()
	var body io.Reader
	if input != nil {
		payload, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		body = bytes.NewReader(payload)
	}
	request, err := http.NewRequest(method, "http://wefty.invalid"+path, body)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	for name, values := range headers {
		request.Header[name] = append([]string(nil), values...)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != wantStatus {
		t.Fatalf("%s %s status=%d want=%d body=%s", method, path, response.StatusCode, wantStatus, payload)
	}
	if output != nil {
		if err := json.Unmarshal(payload, output); err != nil {
			t.Fatalf("decode %s %s: %v body=%s", method, path, err, payload)
		}
	}
}

func waitNativeRun(t *testing.T, client *http.Client, runID string, want contract.RunState) contract.RunRecord {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var record contract.RunRecord
		doNativeJSON(t, client, http.MethodGet, "/v1/runs/"+runID, nil, nil, http.StatusOK, &record)
		if record.Status == want {
			return record
		}
		if record.Status == contract.RunFailed {
			t.Fatalf("run %s failed while waiting for %s", runID, want)
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for run %s state %s", runID, want)
	return contract.RunRecord{}
}

func assertNativeRunLogs(t *testing.T, client *http.Client, runID string) {
	t.Helper()
	var logs l1.LogPage
	doNativeJSON(t, client, http.MethodGet, "/v1/runs/"+runID+"/logs", nil, nil, http.StatusOK, &logs)
	var stdout, stderr int
	for _, event := range logs.Events {
		if event.Stream == contract.LogStdout && string(event.Bytes) == "wefty-echo-once-stdout\n" {
			stdout++
		}
		if event.Stream == contract.LogStderr && string(event.Bytes) == "wefty-echo-once-stderr\n" {
			stderr++
		}
	}
	if stdout != 1 || stderr != 1 {
		t.Fatalf("run %s echo execution markers = stdout %d stderr %d logs %+v", runID, stdout, stderr, logs.Events)
	}
}

func exerciseNativeLinuxPrestartRequeue(t *testing.T, ctx context.Context, adapter *ocirunner.Adapter, reference, expectedDigest string, afterResolution, recoverRuntime func()) {
	t.Helper()
	const (
		runID = "native-prestart-run"
		token = "native-prestart-token"
	)
	var bridgeCalls atomic.Int32
	bridge := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		bridgeCalls.Add(1)
		if request.URL.Path != "/v1/runs/"+runID || request.Header.Get("Authorization") != "Bearer "+token {
			response.WriteHeader(http.StatusUnauthorized)
			return
		}
		response.WriteHeader(http.StatusOK)
	}))
	defer bridge.Close()
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "native-prestart.sqlite"), l1.StoreOptions{
		Jitter: func(time.Duration) time.Duration { return 10 * time.Millisecond },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registration := contract.NodeRegistration{
		NodeID: "native-node", BootSessionID: "native-boot", OS: "linux", Architecture: "amd64", AgentVersion: "realtiming",
		Capabilities: map[string]bool{
			"kind:oci": true, "runtime_handler:" + ocihelper.DefaultRuntimeHandler: true,
		},
		CapabilityRevision: 1, CapabilityObservedAt: time.Now().UTC(), MissingCapabilities: []string{},
	}
	if _, err := store.RegisterNode(ctx, fabric.Identity{NodeID: "native-agent"}, registration, l1.DefaultNodePolicy(), true); err != nil {
		t.Fatal(err)
	}
	spec := contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "native-prestart-requeue", Kind: contract.JobKindOCI, Class: contract.JobClassOneShot,
		RuntimeHandler: ocihelper.DefaultRuntimeHandler,
		Labels:         map[string]string{"run_id": runID},
		Execution: contract.ExecutionSpec{
			Env:          map[string]string{contract.EnvRunID: runID, contract.EnvL3Endpoint: bridge.URL},
			SensitiveEnv: map[string]string{contract.EnvRunToken: token},
			OCI:          &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: reference}, Argv: []string{"/usr/local/bin/wefty-echo-service", "--once"}},
		},
	}
	job, _, err := store.CreateJob(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	first := claimNativeOCI(t, ctx, store, registration)
	if first.Job.Spec.Execution.OCI.Image.Digest != nil {
		t.Fatalf("initial realtiming claim unexpectedly pinned before resolution: %+v", first.Job.Spec.Execution.OCI.Image)
	}
	firstRequest := nativeL1AdapterRequest(first)
	firstRequest.ManagedVolumes = []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeHandoff, OwnerKey: runID}}
	firstRequest.OCIImageResolved = func(callbackContext context.Context, observation workloadrunner.OCIImageObservation) error {
		if _, err := store.ObserveAttemptImage(callbackContext, "native-agent", job.JobID, first.Lease.AttemptID, nativeImageObservation(first.Lease.FencingToken, observation)); err != nil {
			return err
		}
		afterResolution()
		requestRootFault(t, "stop-containerd")
		return nil
	}
	firstRequest.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error {
		return errors.New("pre-start engine loss unexpectedly reached Started")
	}
	firstResult, firstRunErr := adapter.Run(ctx, firstRequest, nil)
	requestRootFault(t, "start-containerd")
	recoverRuntime()
	if firstRunErr == nil || firstResult.Outcome.SpawnError == nil || firstResult.Outcome.SpawnError.Code != contract.SpawnFailureRuntimeUnavailable {
		t.Fatalf("pre-start engine loss = result %+v err %v", firstResult.Outcome, firstRunErr)
	}
	if receipt, err := adapter.ReapAndVerify(ctx, workloadrunner.ReapRequest{Authority: firstRequest.Authority}); err != nil || !receipt.RuntimeQuiesced {
		t.Fatalf("pre-start cleanup = receipt %+v err %v", receipt, err)
	}
	requeued, err := store.CompleteAttempt(ctx, "native-agent", job.JobID, first.Lease.AttemptID, l1.CompletionRequest{
		FencingToken: first.Lease.FencingToken, IdempotencyKey: "native-prestart-loss", Result: l1.ProcessResult(firstResult.Outcome),
	})
	if err != nil || requeued.State != contract.JobQueued {
		t.Fatalf("pre-start completion requeue = job %+v err %v", requeued, err)
	}

	var second *l1.Claim
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		second, err = store.ClaimJob(ctx, "native-agent", registration.NodeID, registration.BootSessionID, contract.JobClassOneShot)
		if err != nil {
			t.Fatal(err)
		}
		if second != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if second == nil || second.Job.Spec.Execution.OCI.Image.Digest == nil || *second.Job.Spec.Execution.OCI.Image.Digest != expectedDigest ||
		second.PrestartDeadline == nil || first.PrestartDeadline == nil || !second.PrestartDeadline.Equal(*first.PrestartDeadline) {
		t.Fatalf("digest-pinned second claim = %+v", second)
	}
	secondRequest := nativeL1AdapterRequest(second)
	secondRequest.ManagedVolumes = []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeHandoff, OwnerKey: runID}}
	secondRequest.OCIImageResolved = func(callbackContext context.Context, observation workloadrunner.OCIImageObservation) error {
		_, err := store.ObserveAttemptImage(callbackContext, "native-agent", job.JobID, second.Lease.AttemptID, nativeImageObservation(second.Lease.FencingToken, observation))
		return err
	}
	secondRequest.OCIStarted = func(callbackContext context.Context, observation workloadrunner.OCIImageObservation) error {
		if _, err := store.ObserveAttemptImage(callbackContext, "native-agent", job.JobID, second.Lease.AttemptID, nativeImageObservation(second.Lease.FencingToken, observation)); err != nil {
			return err
		}
		_, err := store.StartAttempt(callbackContext, "native-agent", job.JobID, second.Lease.AttemptID, l1.StartedRequest{FencingToken: second.Lease.FencingToken})
		return err
	}
	secondResult, err := adapter.Run(ctx, secondRequest, nil)
	if err != nil || secondResult.Outcome.ExitCode == nil || *secondResult.Outcome.ExitCode != 0 {
		t.Fatalf("digest-pinned retry = result %+v err %v", secondResult.Outcome, err)
	}
	if receipt, err := adapter.ReapAndVerify(ctx, workloadrunner.ReapRequest{Authority: secondRequest.Authority}); err != nil || !receipt.RuntimeQuiesced {
		t.Fatalf("retry cleanup = receipt %+v err %v", receipt, err)
	}
	completed, err := store.CompleteAttempt(ctx, "native-agent", job.JobID, second.Lease.AttemptID, l1.CompletionRequest{
		FencingToken: second.Lease.FencingToken, IdempotencyKey: "native-retry-success", Result: l1.ProcessResult(secondResult.Outcome),
	})
	if err != nil || completed.State != contract.JobSucceeded {
		t.Fatalf("retry completion = job %+v err %v", completed, err)
	}
	if bridgeCalls.Load() != 1 {
		t.Fatalf("pre-Started retry echo bridge calls = %d, want one payload execution", bridgeCalls.Load())
	}
	if err := adapter.FinalizeManagedVolumes(ctx, workloadrunner.ManagedVolumeFinalizationRequest{
		Authority: secondRequest.Authority, Volumes: secondRequest.ManagedVolumes,
	}); err != nil {
		t.Fatal(err)
	}
	attempts, err := store.ListJobAttempts(ctx, job.JobID)
	if err != nil || len(attempts) != 2 || attempts[0].Image == nil || attempts[1].Image == nil ||
		attempts[0].Image.TopLevelDigest != expectedDigest || attempts[1].Image.TopLevelDigest != expectedDigest ||
		attempts[0].Image.StartedAt != nil || attempts[1].Image.StartedAt == nil ||
		!attempts[1].Image.ResolvedAt.Before(*attempts[1].Image.StartedAt) {
		t.Fatalf("pre-start retry evidence = %+v err %v", attempts, err)
	}
}

func claimNativeOCI(t *testing.T, ctx context.Context, store *l1.Store, registration contract.NodeRegistration) *l1.Claim {
	t.Helper()
	claim, err := store.ClaimJob(ctx, "native-agent", registration.NodeID, registration.BootSessionID, contract.JobClassOneShot)
	if err != nil || claim == nil {
		t.Fatalf("native OCI claim = %+v err %v", claim, err)
	}
	return claim
}

func nativeL1AdapterRequest(claim *l1.Claim) workloadrunner.Request {
	return workloadrunner.Request{
		Authority: workloadrunner.AttemptAuthority{
			NodeID: claim.Job.NodeID, BootSessionID: "native-boot", JobID: claim.Job.JobID,
			AttemptID: claim.Lease.AttemptID, FencingToken: claim.Lease.FencingToken,
			WorkloadClass: contract.JobClassOneShot, RemovalGeneration: "attempt",
		},
		RuntimeHandler: claim.Job.Spec.RuntimeHandler, Execution: claim.Job.Spec.Execution,
		Limits: claim.Job.Spec.Limits, InitialDeadman: claim.Lease.LeaseTTL,
		OCIImageDeadline: *claim.PrestartDeadline,
	}
}

func nativeImageObservation(fence string, observation workloadrunner.OCIImageObservation) l1.ImageObservationRequest {
	return l1.ImageObservationRequest{
		FencingToken: fence, SubmittedReference: observation.SubmittedReference,
		TopLevelDigest: observation.TopLevelDigest, TopLevelMediaType: observation.TopLevelMediaType,
		IndexDigest: observation.IndexDigest, PlatformManifestDigest: observation.PlatformManifestDigest,
		Platform:       l1.OCIPlatform{OS: observation.PlatformOS, Architecture: observation.PlatformArchitecture, Variant: observation.PlatformVariant},
		RuntimeHandler: observation.RuntimeHandler, Snapshotter: observation.Snapshotter,
	}
}

func requestRootFault(t *testing.T, action string) {
	t.Helper()
	fifo := os.Getenv("WEFTY_OCI_FAULT_FIFO")
	directory := os.Getenv("WEFTY_OCI_FAULT_DIR")
	if fifo == "" || directory == "" {
		t.Fatal("Linux OCI root fault supervisor is not provisioned")
	}
	ack := filepath.Join(directory, action+".done")
	failure := filepath.Join(directory, action+".failed")
	_ = os.Remove(ack)
	_ = os.Remove(failure)
	if err := os.WriteFile(fifo, []byte(action+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(ack); err == nil {
			return
		}
		if payload, err := os.ReadFile(failure); err == nil {
			t.Fatalf("root assertion %s failed: %s", action, payload)
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("root fault %s was not acknowledged", action)
}

func assertUnprivilegedRunnerReceipt(t *testing.T, path string) {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read OCI provision receipt: %v", err)
	}
	facts := map[string]string{}
	for _, line := range strings.Split(string(payload), "\n") {
		if key, value, ok := strings.Cut(line, "="); ok {
			facts[key] = value
		}
	}
	for _, key := range []string{"runner_job_uid", "runner_listener_uid"} {
		uid, parseErr := strconv.ParseUint(facts[key], 10, 32)
		if parseErr != nil || uid == 0 {
			t.Fatalf("OCI provision receipt %s = %q, want an unprivileged uid", key, facts[key])
		}
	}
	if facts["runner_listener_owner"] == "" {
		t.Fatal("OCI provision receipt omitted the runner listener process owner")
	}
}

func waitForCacheEviction(t *testing.T, ctx context.Context, session *ocihelper.Session, previousDigest, wantDigest string) ocihelper.ImageCacheEviction {
	t.Helper()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		status, err := session.ImageCacheStatus(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if status.LastEviction != nil && status.LastEviction.Digest != previousDigest {
			if status.LastEviction.Digest != wantDigest {
				t.Fatalf("cache eviction digest = %s, want %s", status.LastEviction.Digest, wantDigest)
			}
			return *status.LastEviction
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for cache eviction %s: %v", wantDigest, ctx.Err())
		case <-ticker.C:
		}
	}
}

func nativeAuthority(suffix string) ocihelper.AttemptAuthority {
	return ocihelper.AttemptAuthority{
		NodeID: "native-node", BootSessionID: "native-boot", JobID: "job-" + suffix,
		AttemptID: "attempt-" + suffix, FencingToken: "fence-" + suffix,
		Class: "one-shot", RemovalGeneration: "attempt",
	}
}

func nativeAdapterRequest(reference, digest, suffix string, argv []string) workloadrunner.Request {
	return workloadrunner.Request{
		Authority: workloadrunner.AttemptAuthority{
			NodeID: "native-node", BootSessionID: "native-boot", JobID: "job-" + suffix,
			AttemptID: "attempt-" + suffix, FencingToken: "fence-" + suffix,
			WorkloadClass: contract.JobClassOneShot, RemovalGeneration: "attempt",
		},
		RuntimeHandler: ocihelper.DefaultRuntimeHandler,
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image: contract.OCIImageSpec{Reference: reference, Digest: &digest}, Argv: argv,
		}},
		InitialDeadman:   l1.DefaultLeaseDuration,
		OCIImageResolved: func(context.Context, workloadrunner.OCIImageObservation) error { return nil },
	}
}

func containsLog(events []contract.LogEvent, stream contract.LogStream, value string) bool {
	for _, event := range events {
		if event.Stream == stream && string(event.Bytes) == value {
			return true
		}
	}
	return false
}
