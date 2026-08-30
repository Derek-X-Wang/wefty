package oci

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

const adapterTestDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestRemovalResourceManifestNamesAllManagedResourcesWithoutBindSources(t *testing.T) {
	request := adapterTestRequest()
	request.Authority.WorkloadClass = contract.JobClassService
	request.Authority.RemovalGeneration = "1"
	request.ManagedVolumes = []workloadrunner.ManagedVolume{
		{Kind: workloadrunner.ManagedVolumeHandoff, OwnerKey: "manifest-owner"},
		{Kind: workloadrunner.ManagedVolumeServiceData},
	}
	request.Execution.OCI.Mounts = []contract.OCIMount{{NodePath: "/operator/secret/project", ContainerPath: "/workspace"}}
	manifest, err := (&Adapter{}).RemovalResourceManifest(request)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.RuntimeKind != contract.JobKindOCI || manifest.JobID != request.Authority.JobID ||
		manifest.AttemptID != request.Authority.AttemptID || manifest.RemovalGeneration != "1" ||
		manifest.LeaseID == "" || manifest.TaskID == "" || manifest.ContainerID == "" ||
		manifest.SnapshotID == "" || manifest.ShimID == "" || manifest.CgroupID == "" ||
		manifest.LogSegmentDirectory == "" || manifest.HandoffVolume == "" ||
		manifest.ServiceDataVolume == "" || manifest.ServiceDataOwnerRecord == "" {
		t.Fatalf("runtime removal manifest is incomplete: %+v", manifest)
	}
	wantHandoff, err := ocihelper.DeterministicHandoffVolumeDirectory("manifest-owner")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.HandoffVolume != wantHandoff || manifest.TaskID != manifest.ContainerID || manifest.ShimID != manifest.ContainerID {
		t.Fatalf("runtime removal manifest does not name live containerd identities: %+v", manifest)
	}
	payload, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte("/operator/secret/project")) || bytes.Contains(payload, []byte("/workspace")) {
		t.Fatalf("runtime removal manifest retained operator bind path: %s", payload)
	}
}

func TestComputerRemovalResourceManifestNamesStorageInsteadOfServiceData(t *testing.T) {
	request := adapterTestRequest()
	request.Authority.WorkloadClass = contract.JobClassService
	request.Authority.RemovalGeneration = "1"
	request.ManagedVolumes = []workloadrunner.ManagedVolume{{
		Kind: workloadrunner.ManagedVolumeComputerDisk,
		ComputerStorage: &workloadrunner.ComputerStorage{
			ComputerID: "computer-1", StorageID: "storage-1", StorageGeneration: 3, IntentRevision: 4, DiskBytes: 8 << 30,
		},
	}}
	manifest, err := (&Adapter{}).RemovalResourceManifest(request)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.ComputerStorage == nil || manifest.ComputerStorage.ComputerID != "computer-1" ||
		manifest.ComputerStorage.StorageID != "storage-1" || manifest.ComputerStorage.StorageGeneration != 3 ||
		manifest.ComputerStorage.IntentRevision != 4 || manifest.ComputerStorage.DiskBytes != 8<<30 {
		t.Fatalf("Computer removal manifest Storage = %+v", manifest.ComputerStorage)
	}
	if manifest.ServiceDataVolume != "" || manifest.ServiceDataOwnerRecord != "" {
		t.Fatalf("Computer removal manifest fabricated service-data identity: %+v", manifest)
	}
}

func TestAdapterRequiresAuthoritativeStartedBeforeLocalPromotion(t *testing.T) {
	engine := &adapterTestEngine{watch: ocihelper.WatchResponse{ExitCode: intPointer(0)}}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	request.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error {
		return errors.New("L1 rejected Started")
	}
	request.Started = func() { t.Fatal("local Started ran after L1 rejected authority") }
	result, err := adapter.Run(t.Context(), request, nil)
	if err == nil || result.Outcome.SpawnError == nil || result.Outcome.SpawnError.Code != contract.SpawnFailureProcessRequest {
		t.Fatalf("failed acknowledgement outcome=%+v err=%v", result, err)
	}
	engine.mu.Lock()
	deletes := engine.deletes
	engine.mu.Unlock()
	if deletes == 0 {
		t.Fatal("failed Started acknowledgement did not reap the real task")
	}
}

func TestAdapterCarriesHelperStartedEdgeAcrossL1Acknowledgement(t *testing.T) {
	helperStartedAt := time.Date(2026, 8, 28, 12, 0, 0, 123, time.UTC)
	engine := &adapterTestEngine{watch: ocihelper.WatchResponse{ExitCode: intPointer(0)}, startedAt: helperStartedAt}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	var observed time.Time
	request.OCIStartedAt = func(startedAt time.Time) { observed = startedAt }
	request.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error {
		if !observed.Equal(helperStartedAt) {
			t.Fatalf("helper Started edge before L1 acknowledgement = %s, want %s", observed, helperStartedAt)
		}
		return nil
	}
	if result, err := adapter.Run(t.Context(), request, nil); err != nil || result.Outcome.ExitCode == nil || *result.Outcome.ExitCode != 0 {
		t.Fatalf("Run = %+v err=%v", result, err)
	}
}

func TestAdapterPersistsResolutionBeforePrestartRunFailure(t *testing.T) {
	engine := &adapterTestEngine{runErr: errors.New("containerd stopped before task start")}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	recoveries := 0
	request.OCIRuntimeUnavailable = func(workloadrunner.RuntimeGeneration) { recoveries++ }
	request.Execution.OCI.Image.Digest = nil
	var resolved workloadrunner.OCIImageObservation
	request.OCIImageResolved = func(_ context.Context, observation workloadrunner.OCIImageObservation) error {
		resolved = observation
		return nil
	}
	request.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error {
		t.Fatal("pre-start Run failure reached Started")
		return nil
	}
	result, err := adapter.Run(t.Context(), request, nil)
	if err == nil || result.Outcome.SpawnError == nil || result.Outcome.SpawnError.Code != contract.SpawnFailureRuntimeUnavailable {
		t.Fatalf("pre-start failure outcome = (%+v, %v)", result.Outcome, err)
	}
	if resolved.TopLevelDigest != adapterTestDigest || resolved.PlatformManifestDigest != adapterTestDigest {
		t.Fatalf("pre-Run resolution evidence = %+v", resolved)
	}
	receipt, reapErr := adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: request.Authority})
	var reapLoss *workloadrunner.RuntimeLossError
	if !errors.As(reapErr, &reapLoss) || receipt.RuntimeQuiesced || reapLoss.Generation.InstanceID == "" || reapLoss.Generation.Generation == 0 {
		t.Fatalf("pre-Run helper failure reap evidence = (%+v, %v)", receipt, reapErr)
	}
	if recoveries != 1 {
		t.Fatalf("pre-Run helper loss recovery calls = %d, want 1", recoveries)
	}
}

func TestAdapterReapSessionLossReturnsTypedEvidenceAndRetainsAttempt(t *testing.T) {
	engine := &adapterTestEngine{watch: ocihelper.WatchResponse{ExitCode: intPointer(0)}}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	request := adapterTestRequest()
	if result, err := adapter.Run(t.Context(), request, nil); err != nil || result.Outcome.ExitCode == nil {
		t.Fatalf("run before reap loss = (%+v, %v)", result.Outcome, err)
	}
	closeAdapter()
	for attempt := 1; attempt <= 2; attempt++ {
		receipt, err := adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: request.Authority})
		var loss *workloadrunner.RuntimeLossError
		if !errors.As(err, &loss) || receipt.RuntimeQuiesced || loss.Generation.InstanceID == "" || loss.Generation.Generation == 0 {
			t.Fatalf("reap loss attempt %d = receipt %+v err %v", attempt, receipt, err)
		}
	}
}

func TestAdapterClassifiesPreRunObservationFailure(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want contract.SpawnFailureCode
	}{
		{name: "protocol refusal", err: &workloadrunner.OCIObservationRefusal{Err: errors.New("stale fence")}, want: contract.SpawnFailureProcessRequest},
		{name: "transport unavailable", err: errors.New("L1 connection reset"), want: contract.SpawnFailureRuntimeUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine := &adapterTestEngine{}
			adapter, closeAdapter := startAdapterTestServer(t, engine)
			defer closeAdapter()
			request := adapterTestRequest()
			request.OCIRuntimeUnavailable = func(workloadrunner.RuntimeGeneration) {
				t.Fatal("L1 observation failure triggered an OCI namespace sweep")
			}
			request.OCIImageResolved = func(context.Context, workloadrunner.OCIImageObservation) error { return test.err }
			result, err := adapter.Run(t.Context(), request, nil)
			if err == nil || result.Outcome.SpawnError == nil || result.Outcome.SpawnError.Code != test.want {
				t.Fatalf("observation failure = (%+v, %v), want %s", result.Outcome, err, test.want)
			}
		})
	}
}

func TestAdapterLoadImageUsesAgentBudgetAndReturnsDigests(t *testing.T) {
	engine := &adapterTestEngine{}
	adapter, closeAdapter := startAdapterTestServerWithPolicy(t, engine, ImagePolicy{Budget: 3 * time.Second})
	defer closeAdapter()
	result, err := adapter.LoadImage(t.Context(), "registry.invalid/offline:test", bytes.NewReader([]byte("archive")))
	if err != nil {
		t.Fatal(err)
	}
	if result.TopLevelDigest != adapterTestDigest || result.PlatformDigest != adapterTestDigest || engine.ensureCalls != 1 {
		t.Fatalf("load-image result=%+v calls=%d", result, engine.ensureCalls)
	}
}

func TestAdapterLoadImageBootstrapsPlatformWithoutFunctionalProbe(t *testing.T) {
	engine := &adapterTestEngine{doctorPlatform: ocihelper.OCIPlatform{OS: "linux", Architecture: "arm64"}}
	adapter, barrier, _, closeAdapter := startAdapterTestServerWithSnapshots(t, engine, ImagePolicy{Budget: 3 * time.Second})
	defer closeAdapter()
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	adapter.mu.Lock()
	delete(adapter.probePlatforms, helperSession(session))
	adapter.mu.Unlock()

	result, err := adapter.LoadImage(t.Context(), "", bytes.NewReader([]byte("archive")))
	if err != nil {
		t.Fatal(err)
	}
	if result.TopLevelDigest != adapterTestDigest || engine.ensureCalls != 1 {
		t.Fatalf("bootstrap load-image result=%+v calls=%d", result, engine.ensureCalls)
	}
	engine.mu.Lock()
	platform := engine.lastEnsure.Platform
	engine.mu.Unlock()
	if platform != (ocihelper.OCIPlatform{OS: "linux", Architecture: "arm64", Variant: "v8"}) {
		t.Fatalf("offline import platform = %+v, want canonical arm64 variant", platform)
	}
	if _, recorded := adapter.probePlatform(session); recorded {
		t.Fatal("offline import promoted diagnostic platform mechanics into functional probe evidence")
	}
}

func TestCanonicalProbePlatformIncludesDefaultArm64Variant(t *testing.T) {
	for _, test := range []struct {
		input ocihelper.OCIPlatform
		want  ocihelper.OCIPlatform
	}{
		{input: ocihelper.OCIPlatform{OS: "linux", Architecture: "arm64"}, want: ocihelper.OCIPlatform{OS: "linux", Architecture: "arm64", Variant: "v8"}},
		{input: ocihelper.OCIPlatform{OS: "linux", Architecture: "arm64", Variant: "v9"}, want: ocihelper.OCIPlatform{OS: "linux", Architecture: "arm64", Variant: "v9"}},
		{input: ocihelper.OCIPlatform{OS: "linux", Architecture: "amd64"}, want: ocihelper.OCIPlatform{OS: "linux", Architecture: "amd64"}},
	} {
		if got, err := canonicalProbePlatform(test.input); err != nil || got != test.want {
			t.Fatalf("canonicalProbePlatform(%+v) = %+v, %v, want %+v", test.input, got, err, test.want)
		}
	}
}

func TestAdapterRuntimeDeliveryStillRequiresFunctionalProbePlatform(t *testing.T) {
	engine := &adapterTestEngine{doctorPlatform: ocihelper.OCIPlatform{OS: "linux", Architecture: "amd64"}}
	adapter, barrier, _, closeAdapter := startAdapterTestServerWithSnapshots(t, engine, ImagePolicy{Budget: 3 * time.Second})
	defer closeAdapter()
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	adapter.mu.Lock()
	delete(adapter.probePlatforms, helperSession(session))
	adapter.mu.Unlock()
	result, err := adapter.Run(t.Context(), adapterTestRequest(), nil)
	if err == nil || result.Outcome.SpawnError == nil || result.Outcome.SpawnError.Code != contract.SpawnFailureRuntimeUnavailable {
		t.Fatalf("runtime delivery without probe = (%+v, %v)", result.Outcome, err)
	}
	engine.mu.Lock()
	ensureCalls, doctorCalls := engine.ensureCalls, engine.doctorCalls
	engine.mu.Unlock()
	if ensureCalls != 0 || doctorCalls != 0 {
		t.Fatalf("runtime delivery used archive bootstrap fallback: ensure_calls=%d doctor_calls=%d", ensureCalls, doctorCalls)
	}
}

func TestAdapterLoadImageClassifiesDiagnosticPlatformFailures(t *testing.T) {
	for _, test := range []struct {
		name     string
		platform ocihelper.OCIPlatform
		err      error
	}{
		{name: "diagnostic read", err: errors.New("diagnostic unavailable")},
		{name: "invalid diagnostic platform", platform: ocihelper.OCIPlatform{OS: "linux"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine := &adapterTestEngine{doctorPlatform: test.platform, doctorErr: test.err}
			adapter, barrier, _, closeAdapter := startAdapterTestServerWithSnapshots(t, engine, ImagePolicy{Budget: 3 * time.Second})
			defer closeAdapter()
			session, err := barrier.Session()
			if err != nil {
				t.Fatal(err)
			}
			adapter.mu.Lock()
			delete(adapter.probePlatforms, helperSession(session))
			adapter.mu.Unlock()
			_, err = adapter.LoadImage(t.Context(), "", bytes.NewReader([]byte("archive")))
			var reasoned interface{ ControlFailureReason() string }
			if !errors.As(err, &reasoned) || reasoned.ControlFailureReason() != string(ocihelper.CodeDiagnosticFailure) {
				t.Fatalf("offline import platform error = %v reason=%v", err, reasoned)
			}
		})
	}
}

func TestAdapterRejectsHelperDigestDifferentFromPinnedRequest(t *testing.T) {
	other := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	engine := &adapterTestEngine{responseDigest: other}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	recoveries := 0
	request.OCIRuntimeUnavailable = func(workloadrunner.RuntimeGeneration) { recoveries++ }
	result, err := adapter.Run(t.Context(), request, nil)
	if err == nil || result.Outcome.SpawnError == nil || result.Outcome.SpawnError.Code != contract.SpawnFailureImageManifestInvalid {
		t.Fatalf("digest mismatch outcome = (%+v, %v)", result.Outcome, err)
	}
	if recoveries != 0 {
		t.Fatalf("digest mismatch recovery calls = %d, want 0", recoveries)
	}
}

func TestAdapterBindsImageSelectionToCurrentProbePlatform(t *testing.T) {
	engine := &adapterTestEngine{watch: ocihelper.WatchResponse{ExitCode: intPointer(0)}}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	if _, err := adapter.Run(t.Context(), adapterTestRequest(), nil); err != nil {
		t.Fatal(err)
	}
	engine.mu.Lock()
	platform := engine.lastEnsure.Platform
	engine.mu.Unlock()
	if platform != (ocihelper.OCIPlatform{OS: "linux", Architecture: "amd64"}) {
		t.Fatalf("EnsureImage platform = %+v, want successful probe platform", platform)
	}
}

func TestAdapterRejectsImageEvidenceOutsideProbePlatform(t *testing.T) {
	engine := &adapterTestEngine{responsePlatform: ocihelper.OCIPlatform{OS: "linux", Architecture: "arm64"}}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	request.OCIImageResolved = func(context.Context, workloadrunner.OCIImageObservation) error {
		t.Fatal("mismatched first-binding evidence reached L1")
		return nil
	}
	result, err := adapter.Run(t.Context(), request, nil)
	if err == nil || result.Outcome.SpawnError == nil || result.Outcome.SpawnError.Code != contract.SpawnFailureImagePlatformUnsupported {
		t.Fatalf("platform mismatch = (%+v, %v)", result.Outcome, err)
	}
}

func TestAdapterRequiresPositiveDeleteReceipt(t *testing.T) {
	engine := &adapterTestEngine{watch: ocihelper.WatchResponse{ExitCode: intPointer(0)}, refuseDelete: true}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	request.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error { return nil }
	if _, err := adapter.Run(t.Context(), request, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: request.Authority}); err == nil {
		t.Fatal("negative helper Delete receipt produced quiescence evidence")
	}
}

func TestReferenceComputerCleanupReapsBeforeDiskFinalization(t *testing.T) {
	engine := &adapterTestEngine{watch: ocihelper.WatchResponse{ExitCode: intPointer(0)}, requireReapBeforeVolumeDelete: true}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	request.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error { return nil }
	if _, err := adapter.Run(t.Context(), request, nil); err != nil {
		t.Fatal(err)
	}
	storage := &workloadrunner.ComputerStorage{ComputerID: "computer", StorageID: "storage", StorageGeneration: 1, IntentRevision: 1, DiskBytes: 1}
	finalize := workloadrunner.ManagedVolumeFinalizationRequest{Authority: request.Authority,
		Volumes: []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeComputerDisk, ComputerStorage: storage}},
		Removal: &workloadrunner.ManagedVolumeRemovalAuthority{NodeID: request.Authority.NodeID, BootSessionID: request.Authority.BootSessionID,
			JobID: request.Authority.JobID, RemovalGeneration: 1, CleanupFence: "cleanup"}}
	if err := adapter.FinalizeManagedVolumes(t.Context(), finalize); err == nil {
		t.Fatal("attached Computer disk deletion unexpectedly succeeded")
	} else {
		var rpcErr *ocihelper.RPCError
		if !errors.As(err, &rpcErr) || rpcErr.EngineFailure == nil || rpcErr.EngineFailure.Operation != ocihelper.MethodDeleteVolume {
			t.Fatalf("attached Computer disk refusal = %v, want typed DeleteManagedVolume failure", err)
		}
	}
	receipt, err := adapter.ReapAndFinalizeManagedVolumes(t.Context(), workloadrunner.ReapRequest{Authority: request.Authority}, finalize)
	if err != nil || !receipt.RuntimeQuiesced || engine.runtimeDeletes != 1 || !engine.volumeDeleteBeforeReap {
		t.Fatalf("ordered Computer cleanup = receipt=%+v runtimeDeletes=%d refusedBeforeReap=%t err=%v",
			receipt, engine.runtimeDeletes, engine.volumeDeleteBeforeReap, err)
	}
}

func TestManagedVolumeCleanupRetriesThenQuarantinesWithTypedEvidence(t *testing.T) {
	finalization := func(authority workloadrunner.AttemptAuthority) workloadrunner.ManagedVolumeFinalizationRequest {
		storage := &workloadrunner.ComputerStorage{ComputerID: "computer", StorageID: "storage", StorageGeneration: 1, IntentRevision: 2, DiskBytes: 4096}
		return workloadrunner.ManagedVolumeFinalizationRequest{Authority: authority,
			Volumes: []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeComputerDisk, ComputerStorage: storage}},
			Removal: &workloadrunner.ManagedVolumeRemovalAuthority{NodeID: authority.NodeID, BootSessionID: authority.BootSessionID,
				JobID: authority.JobID, RemovalGeneration: 2, CleanupFence: "cleanup"}}
	}
	t.Run("transient operation failure", func(t *testing.T) {
		engine := &adapterTestEngine{volumeDeleteFailures: 2}
		adapter, closeAdapter := startAdapterTestServer(t, engine)
		defer closeAdapter()
		if err := adapter.FinalizeManagedVolumes(t.Context(), finalization(adapterTestRequest().Authority)); err != nil || engine.volumeDeleteCalls != 3 {
			t.Fatalf("bounded volume retry = calls=%d err=%v", engine.volumeDeleteCalls, err)
		}
	})
	t.Run("exhausted operation failure", func(t *testing.T) {
		engine := &adapterTestEngine{volumeDeleteFailures: 3}
		adapter, closeAdapter := startAdapterTestServer(t, engine)
		defer closeAdapter()
		err := adapter.FinalizeManagedVolumes(t.Context(), finalization(adapterTestRequest().Authority))
		var quarantined *workloadrunner.ManagedVolumeCleanupQuarantinedError
		if !errors.As(err, &quarantined) || quarantined.Receipt.Kind != "managed_volume_cleanup_quarantined" ||
			quarantined.Receipt.FailureReason != string(ocihelper.EngineFailureOperationFailed) || quarantined.Receipt.Attempts != 3 || engine.volumeDeleteCalls != 3 {
			t.Fatalf("quarantined cleanup = calls=%d err=%#v", engine.volumeDeleteCalls, err)
		}
	})
}

func TestAdapterConsumesMatchingPriorBootSweepEvidenceOnce(t *testing.T) {
	source := &adapterReceiptSource{receipt: ocihelper.VerifiedSweepReceipt{
		SweepEpoch: "sweep-1", HelperSession: ocihelper.HelperSession{HelperInstanceID: "helper-1", SessionGeneration: 7},
		VerifiedAbsent: true,
		Attempts:       []ocihelper.SweptAttemptAuthority{{NodeID: "node", JobID: "job", AttemptID: "attempt", FencingToken: "fence", PriorBootSessionID: "boot-old", Class: contract.JobClassService, RemovalGeneration: "remove-1"}},
	}}
	adapter := NewAdapter(source)
	request := workloadrunner.PriorBootReapRequest{NodeID: "node", JobID: "job", PriorBootSessionID: "boot-old", CurrentBootSessionID: "boot-new"}
	receipt, err := adapter.ReapPriorBoot(t.Context(), request)
	if err != nil || !receipt.RuntimeQuiesced || receipt.Evidence != workloadrunner.ReapEvidencePriorBootOCISweep || receipt.SweepEpoch != "sweep-1" || receipt.HelperGeneration != 7 {
		t.Fatalf("prior-boot receipt=%+v err=%v", receipt, err)
	}
	if _, err := adapter.ReapPriorBoot(t.Context(), request); !errors.Is(err, workloadrunner.ErrPriorBootEvidenceUnavailable) {
		t.Fatalf("reused sweep receipt error = %v", err)
	}
}

func TestAdapterUsesVerifiedPriorBootNamespaceSweepAfterAttemptWasAlreadyReaped(t *testing.T) {
	source := &adapterReceiptSource{receipt: ocihelper.VerifiedSweepReceipt{
		SweepEpoch: "sweep-empty", HelperSession: ocihelper.HelperSession{HelperInstanceID: "helper-1", SessionGeneration: 8},
		VerifiedAbsent: true, PriorBootSessionsSeen: []string{"boot-old"},
	}}
	adapter := NewAdapter(source)
	request := workloadrunner.PriorBootReapRequest{NodeID: "node", JobID: "job", PriorBootSessionID: "boot-old", CurrentBootSessionID: "boot-new"}
	receipt, err := adapter.ReapPriorBoot(t.Context(), request)
	if err != nil || !receipt.RuntimeQuiesced || receipt.Evidence != workloadrunner.ReapEvidencePriorBootOCISweep ||
		receipt.SweepEpoch != "sweep-empty" || receipt.HelperGeneration != 8 {
		t.Fatalf("empty prior-boot namespace receipt=%+v err=%v", receipt, err)
	}
}

func TestAdapterRejectsUnboundVerifiedPriorBootNamespaceSweep(t *testing.T) {
	source := &adapterReceiptSource{receipt: ocihelper.VerifiedSweepReceipt{
		SweepEpoch: "sweep-unbound", HelperSession: ocihelper.HelperSession{HelperInstanceID: "helper-1", SessionGeneration: 9},
		VerifiedAbsent: true, PriorBootSessionsSeen: []string{"another-boot"},
	}}
	adapter := NewAdapter(source)
	request := workloadrunner.PriorBootReapRequest{NodeID: "node", JobID: "job", PriorBootSessionID: "boot-old", CurrentBootSessionID: "boot-new"}
	if _, err := adapter.ReapPriorBoot(t.Context(), request); !errors.Is(err, workloadrunner.ErrPriorBootEvidenceUnavailable) {
		t.Fatalf("unbound prior-boot sweep error = %v", err)
	}
}

func TestAdapterRefreshesRunSweepBaselineAndRetainsItUntilRecovery(t *testing.T) {
	engine := &adapterTestEngine{watch: ocihelper.WatchResponse{ExitCode: intPointer(0)}}
	adapter, barrier, source, closeAdapter := startAdapterTestServerWithSnapshots(t, engine, ImagePolicy{})
	defer closeAdapter()
	request := adapterTestRequest()
	if _, _, err := adapter.Preflight(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	preflightReceipt, ok := barrier.SweepReceipt()
	if !ok {
		t.Fatal("preflight helper sweep receipt is unavailable")
	}
	barrier.Invalidate()
	if err := barrier.Ensure(t.Context()); err != nil {
		t.Fatal(err)
	}
	runSession, runReceipt, err := barrier.ExecutionSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if runReceipt.HelperSession == preflightReceipt.HelperSession {
		t.Fatal("Preflight to Run replacement did not change helper generation")
	}
	adapter.mu.Lock()
	adapter.probePlatforms[helperSession(runSession)] = ocihelper.OCIPlatform{OS: "linux", Architecture: "amd64"}
	adapter.mu.Unlock()
	if _, err := adapter.Run(t.Context(), request, nil); err != nil {
		t.Fatal(err)
	}

	// A different sweep epoch from the same helper generation is not recovery
	// proof. Reap times out and must retain the tracked attempt for a later
	// replacement-sweep receipt.
	sameGeneration := runReceipt
	sameGeneration.SweepEpoch += "-later"
	source.setUnavailable(sameGeneration)
	reapContext, cancel := context.WithTimeout(t.Context(), 60*time.Millisecond)
	_, err = adapter.ReapAndVerify(reapContext, workloadrunner.ReapRequest{Authority: request.Authority})
	cancel()
	if err == nil {
		t.Fatal("same-generation sweep epoch produced quiescence evidence")
	}

	source.clearUnavailable()
	barrier.Invalidate()
	if err := barrier.Ensure(t.Context()); err != nil {
		t.Fatal(err)
	}
	replacement, ok := barrier.SweepReceipt()
	if !ok {
		t.Fatal("replacement helper sweep receipt is unavailable")
	}
	receipt, err := adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: request.Authority})
	if err != nil || !receipt.RuntimeQuiesced || receipt.Evidence != workloadrunner.ReapEvidenceOCIRuntimeSweep ||
		receipt.SweepEpoch != replacement.SweepEpoch || receipt.HelperGeneration != replacement.HelperSession.SessionGeneration {
		t.Fatalf("replacement sweep receipt = %+v err=%v", receipt, err)
	}
	if _, err := adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: request.Authority}); err == nil {
		t.Fatal("replacement sweep receipt was reusable")
	}
}

func TestAdapterConsumesExactSameBootSweepEvidenceOnce(t *testing.T) {
	authority := workloadrunner.AttemptAuthority{
		NodeID: "node", BootSessionID: "boot", JobID: "job", AttemptID: "attempt",
		FencingToken: "fence", WorkloadClass: contract.JobClassService, RemovalGeneration: "remove-1",
	}
	source := &adapterReceiptSource{receipt: ocihelper.VerifiedSweepReceipt{
		SweepEpoch: "sweep-1", HelperSession: ocihelper.HelperSession{HelperInstanceID: "helper-2", SessionGeneration: 8},
		VerifiedAbsent:    true,
		VerifiedInventory: emptyAdapterInventory(),
		Attempts: []ocihelper.SweptAttemptAuthority{{
			NodeID: authority.NodeID, JobID: authority.JobID, AttemptID: authority.AttemptID,
			FencingToken: authority.FencingToken, PriorBootSessionID: authority.BootSessionID,
			Class: authority.WorkloadClass, RemovalGeneration: authority.RemovalGeneration,
		}},
	}}
	adapter := NewAdapter(source)
	receipt, err := adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: authority})
	if err != nil || !receipt.RuntimeQuiesced || receipt.Evidence != workloadrunner.ReapEvidenceOCISweep || receipt.SweepEpoch != "sweep-1" || receipt.HelperGeneration != 8 {
		t.Fatalf("same-boot sweep receipt=%+v err=%v", receipt, err)
	}
	if _, err := adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: authority}); err == nil {
		t.Fatal("reused same-boot sweep receipt as quiescence evidence")
	}

	mismatched := authority
	mismatched.FencingToken = "other-fence"
	if _, err := NewAdapter(source).ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: mismatched}); err == nil {
		t.Fatal("mismatched sweep authority produced quiescence evidence")
	}
}

func TestAdapterMapsLogsExitSignalOOMAndRuntimeLoss(t *testing.T) {
	tests := []struct {
		name    string
		watch   ocihelper.WatchResponse
		recover bool
		check   func(*testing.T, contract.ProcessResult)
	}{
		{name: "exit", watch: ocihelper.WatchResponse{ExitCode: intPointer(23)}, check: func(t *testing.T, result contract.ProcessResult) {
			if result.ExitCode == nil || *result.ExitCode != 23 {
				t.Fatalf("exit result = %+v", result)
			}
		}},
		{name: "signal", watch: ocihelper.WatchResponse{Signal: ocihelper.SignalTERM, TerminationCause: "agent"}, check: func(t *testing.T, result contract.ProcessResult) {
			if result.Signal != "terminated" || result.TerminationCause != contract.TerminationCauseAgent {
				t.Fatalf("signal result = %+v", result)
			}
		}},
		{name: "oom", watch: ocihelper.WatchResponse{Signal: ocihelper.SignalKILL, TerminationCause: "spontaneous", OutOfMemory: true}, check: func(t *testing.T, result contract.ProcessResult) {
			if !result.OOM || result.Signal != "killed" {
				t.Fatalf("OOM result = %+v", result)
			}
		}},
		{name: "isolated disk exhausted", watch: ocihelper.WatchResponse{ExitCode: intPointer(1), DiskExhausted: true}, check: func(t *testing.T, result contract.ProcessResult) {
			if !result.DiskExhausted || result.ExitCode == nil || *result.ExitCode != 1 {
				t.Fatalf("disk exhaustion result = %+v", result)
			}
		}},
		{name: "runtime-loss", watch: ocihelper.WatchResponse{RuntimeFailure: "shim connection lost"}, recover: true, check: func(t *testing.T, result contract.ProcessResult) {
			if result.RuntimeFailure == nil || result.RuntimeFailure.Code != contract.RuntimeFailureUnavailable {
				t.Fatalf("runtime loss = %+v", result)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			engine := &adapterTestEngine{watch: test.watch}
			adapter, closeAdapter := startAdapterTestServer(t, engine)
			defer closeAdapter()
			request := adapterTestRequest()
			recoveries := 0
			request.OCIRuntimeUnavailable = func(workloadrunner.RuntimeGeneration) { recoveries++ }
			var started bool
			request.OCIStarted = func(_ context.Context, evidence workloadrunner.OCIImageObservation) error {
				if evidence.TopLevelDigest != adapterTestDigest {
					t.Fatalf("image evidence = %+v", evidence)
				}
				started = true
				return nil
			}
			var log contract.LogEvent
			result, err := adapter.Run(t.Context(), request, workloadrunner.OutputSinkFunc(func(_ context.Context, event contract.LogEvent) error { log = event; return nil }))
			if err != nil {
				t.Fatal(err)
			}
			if !started || log.Stream != contract.LogStdout || string(log.Bytes) != "frame" || log.Sequence != 0 {
				t.Fatalf("started=%v log=%+v", started, log)
			}
			test.check(t, result.Outcome)
			if (recoveries == 1) != test.recover {
				t.Fatalf("runtime recovery calls = %d, want recovery %t", recoveries, test.recover)
			}
		})
	}
}

func TestAdapterServiceCancellationUsesTermBeforeKill(t *testing.T) {
	type runOutcome struct {
		result workloadrunner.Result
		err    error
	}
	tests := []struct {
		name       string
		ignoreTERM bool
		want       []ocihelper.Signal
		wantResult string
	}{
		{name: "graceful term", want: []ocihelper.Signal{ocihelper.SignalTERM}, wantResult: "terminated"},
		{name: "kill after grace", ignoreTERM: true, want: []ocihelper.Signal{ocihelper.SignalTERM, ocihelper.SignalKILL}, wantResult: "killed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			engine := &adapterTestEngine{watchSignals: make(chan ocihelper.Signal, 2), ignoreTERM: test.ignoreTERM}
			adapter, closeAdapter := startAdapterTestServer(t, engine)
			defer closeAdapter()
			request := adapterTestRequest()
			request.Authority.WorkloadClass = contract.JobClassService
			request.LifetimeBoundary = workloadrunner.AgentBootLifetime
			request.TerminationGrace = 20 * time.Millisecond
			started := make(chan struct{})
			request.Started = func() { close(started) }
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan runOutcome, 1)
			go func() {
				result, err := adapter.Run(ctx, request, nil)
				done <- runOutcome{result: result, err: err}
			}()
			<-started
			cancel()
			finished := <-done
			if finished.err != nil || finished.result.Outcome.Signal != test.wantResult || finished.result.Outcome.TerminationCause != contract.TerminationCauseAgent {
				t.Fatalf("cancellation outcome = (%+v, %v)", finished.result.Outcome, finished.err)
			}
			engine.mu.Lock()
			got := slices.Clone(engine.signals)
			engine.mu.Unlock()
			if !slices.Equal(got, test.want) {
				t.Fatalf("signals = %v, want %v", got, test.want)
			}
		})
	}
}

func TestAdapterIgnoreTERMWaitsForSlowPostKILLReleaseWithinStopBudget(t *testing.T) {
	engine := &adapterTestEngine{
		watchSignals: make(chan ocihelper.Signal, 2),
		ignoreTERM:   true,
		releaseDelay: 250 * time.Millisecond,
	}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	request.Authority.WorkloadClass = contract.JobClassService
	request.LifetimeBoundary = workloadrunner.AgentBootLifetime
	request.TerminationGrace = 50 * time.Millisecond
	started := make(chan struct{})
	request.Started = func() { close(started) }
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct {
		result workloadrunner.Result
		err    error
	}, 1)
	go func() {
		result, err := adapter.Run(ctx, request, nil)
		done <- struct {
			result workloadrunner.Result
			err    error
		}{result: result, err: err}
	}()
	<-started
	stopStarted := time.Now()
	cancel()
	finished := <-done
	elapsed := time.Since(stopStarted)
	if finished.err != nil || finished.result.Outcome.Signal != "killed" || finished.result.Outcome.TerminationCause != contract.TerminationCauseAgent {
		t.Fatalf("slow-release cancellation outcome = (%+v, %v)", finished.result.Outcome, finished.err)
	}
	if elapsed >= 2*time.Second {
		t.Fatalf("TERM -> grace -> KILL -> release took %s, want margin below the 10s acceptance budget", elapsed)
	}
	engine.mu.Lock()
	got := slices.Clone(engine.signals)
	engine.mu.Unlock()
	if !slices.Equal(got, []ocihelper.Signal{ocihelper.SignalTERM, ocihelper.SignalKILL}) {
		t.Fatalf("signals = %v, want TERM then KILL", got)
	}
}

func TestAdapterServiceExitRacesKillAndKeepsTerminalEvidence(t *testing.T) {
	engine := &adapterTestEngine{
		watchSignals:   make(chan ocihelper.Signal, 1),
		ignoreTERM:     true,
		exitOnKillRace: true,
	}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	request.Authority.WorkloadClass = contract.JobClassService
	request.LifetimeBoundary = workloadrunner.AgentBootLifetime
	request.TerminationGrace = 10 * time.Millisecond
	started := make(chan struct{})
	request.Started = func() { close(started) }
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct {
		result workloadrunner.Result
		err    error
	}, 1)
	go func() {
		result, err := adapter.Run(ctx, request, nil)
		done <- struct {
			result workloadrunner.Result
			err    error
		}{result: result, err: err}
	}()
	<-started
	cancel()
	finished := <-done
	if finished.err != nil || finished.result.Outcome.Signal != "terminated" || finished.result.Outcome.TerminationCause != contract.TerminationCauseAgent {
		t.Fatalf("exit-during-grace cancellation = (%+v, %v)", finished.result.Outcome, finished.err)
	}
	engine.mu.Lock()
	got := slices.Clone(engine.signals)
	engine.mu.Unlock()
	if !slices.Equal(got, []ocihelper.Signal{ocihelper.SignalTERM, ocihelper.SignalKILL}) {
		t.Fatalf("signal attempts = %v, want TERM then raced KILL", got)
	}
}

func TestAdapterServiceUninterruptiblePayloadDoesNotReportRuntimeLoss(t *testing.T) {
	engine := &adapterTestEngine{watchSignals: make(chan ocihelper.Signal, 2), ignoreTERM: true, ignoreKILL: true}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	request.Authority.WorkloadClass = contract.JobClassService
	request.LifetimeBoundary = workloadrunner.AgentBootLifetime
	request.TerminationGrace = 10 * time.Millisecond
	recoveries := 0
	request.OCIRuntimeUnavailable = func(workloadrunner.RuntimeGeneration) { recoveries++ }
	started := make(chan struct{})
	request.Started = func() { close(started) }
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := adapter.Run(ctx, request, nil)
		done <- err
	}()
	<-started
	cancel()
	if err := <-done; err == nil || !strings.Contains(err.Error(), "did not confirm exit after KILL") {
		t.Fatalf("uninterruptible service stop error = %v", err)
	}
	if recoveries != 0 {
		t.Fatalf("uninterruptible payload triggered %d namespace recoveries", recoveries)
	}
}

func TestAdapterReapedTaskWithoutWaitConfirmationReportsRuntimeLoss(t *testing.T) {
	engine := &adapterTestEngine{
		watchSignals:          make(chan ocihelper.Signal),
		ignoreTERM:            true,
		killAlreadyTerminated: true,
	}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	request.Authority.WorkloadClass = contract.JobClassService
	request.LifetimeBoundary = workloadrunner.AgentBootLifetime
	request.TerminationGrace = 10 * time.Millisecond
	recoveries := 0
	request.OCIRuntimeUnavailable = func(workloadrunner.RuntimeGeneration) { recoveries++ }
	started := make(chan struct{})
	request.Started = func() { close(started) }
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := adapter.Run(ctx, request, nil)
		done <- err
	}()
	<-started
	cancel()
	err := <-done
	var runtimeLoss *ocihelper.RuntimeLossError
	if !errors.As(err, &runtimeLoss) || !strings.Contains(err.Error(), "did not confirm exit after KILL") {
		t.Fatalf("reaped task without Wait confirmation = %T %v, want typed runtime loss", err, err)
	}
	if recoveries != 1 {
		t.Fatalf("reaped task without Wait confirmation recoveries = %d, want 1", recoveries)
	}
}

func TestAdapterServiceSignalDeadlineReportsRuntimeLoss(t *testing.T) {
	engine := &adapterTestEngine{watchSignals: make(chan ocihelper.Signal, 2), blockSignal: true}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	request.Authority.WorkloadClass = contract.JobClassService
	request.LifetimeBoundary = workloadrunner.AgentBootLifetime
	request.TerminationGrace = 10 * time.Millisecond
	recoveries := 0
	request.OCIRuntimeUnavailable = func(workloadrunner.RuntimeGeneration) { recoveries++ }
	started := make(chan struct{})
	request.Started = func() { close(started) }
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := adapter.Run(ctx, request, nil)
		done <- err
	}()
	<-started
	cancel()
	if err := <-done; err == nil {
		t.Fatal("helper-unreachable service stop returned no error")
	}
	if recoveries != 1 {
		t.Fatalf("helper-unreachable service stop recoveries = %d, want 1", recoveries)
	}
}

func TestAdapterOneShotCancellationDoesNotReportRuntimeLoss(t *testing.T) {
	engine := &adapterTestEngine{watchErrorOnCancel: true}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	recoveries := 0
	request.OCIRuntimeUnavailable = func(workloadrunner.RuntimeGeneration) { recoveries++ }
	started := make(chan struct{})
	request.Started = func() { close(started) }
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := adapter.Run(ctx, request, nil)
		done <- err
	}()
	<-started
	cancel()
	if err := <-done; err == nil {
		t.Fatal("cancelled one-shot Watch unexpectedly succeeded")
	}
	if recoveries != 0 {
		t.Fatalf("cancelled one-shot recovery calls = %d, want 0", recoveries)
	}
}

func TestAdapterPreservesImageUnavailableAsPermanentSpawnEvidence(t *testing.T) {
	engine := &adapterTestEngine{runErr: &ocihelper.RPCError{Code: ocihelper.CodeImageUnavailable, Message: "pinned image missing"}}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	result, err := adapter.Run(t.Context(), request, nil)
	if err == nil || result.Outcome.SpawnError == nil || result.Outcome.SpawnError.Code != contract.SpawnFailureImageUnavailable {
		t.Fatalf("image failure outcome=%+v err=%v", result.Outcome, err)
	}
}

func TestAdapterHonorsRetryAfterWithinOneImageBudget(t *testing.T) {
	engine := &adapterTestEngine{ensureErrors: []error{
		ocihelper.NewImageMechanicsError(ocihelper.ImageFailureFact{Kind: ocihelper.ImageFailureHTTP, HTTPStatus: 429, RetryAfter: 2 * time.Second, TopLevelDigest: adapterTestDigest}, errors.New("rate limited")),
		nil,
	}, watch: ocihelper.WatchResponse{ExitCode: intPointer(0)}}
	adapter, closeAdapter := startAdapterTestServerWithPolicy(t, engine, ImagePolicy{
		Budget: time.Minute,
		Sleep: func(_ context.Context, delay time.Duration) error {
			if delay != 2*time.Second {
				t.Fatalf("retry delay = %s, want Retry-After 2s", delay)
			}
			return nil
		},
	})
	defer closeAdapter()
	request := adapterTestRequest()
	request.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error { return nil }
	if _, err := adapter.Run(t.Context(), request, nil); err != nil {
		t.Fatal(err)
	}
	if engine.ensureCalls != 2 {
		t.Fatalf("EnsureImage calls = %d, want 2", engine.ensureCalls)
	}
}

func TestAdapterPumpsConstrainedMacHostBridgeFallback(t *testing.T) {
	engine := &adapterTestEngine{
		watch:          ocihelper.WatchResponse{ExitCode: intPointer(0)},
		bridgeExchange: make(chan error, 1),
	}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	request.Execution.Env = map[string]string{contract.EnvL3Endpoint: "http://127.0.0.1:43100/l3"}
	request.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error { return nil }
	request.HostBridgeDial = func(context.Context) (net.Conn, error) {
		adapterSide, bridgeSide := net.Pipe()
		go func() {
			defer bridgeSide.Close()
			payload := make([]byte, len("guest-request"))
			if _, err := io.ReadFull(bridgeSide, payload); err != nil || string(payload) != "guest-request" {
				return
			}
			_, _ = bridgeSide.Write([]byte("host-response"))
		}()
		return adapterSide, nil
	}
	result, err := adapter.Run(t.Context(), request, nil)
	if err != nil || result.Outcome.ExitCode == nil || *result.Outcome.ExitCode != 0 {
		t.Fatalf("fallback result=%+v err=%v", result, err)
	}
	engine.mu.Lock()
	requested := engine.lastRun.EnableHostBridgeFallback
	engine.mu.Unlock()
	if !requested {
		t.Fatal("adapter did not explicitly request helper fallback authority")
	}
}

func TestComputerAdapterActivatesForcedMacDialHostBridgeFallback(t *testing.T) {
	engine := &adapterTestEngine{watch: ocihelper.WatchResponse{ExitCode: intPointer(0)}, bridgeExchange: make(chan error, 1)}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	request.Authority.WorkloadClass = contract.JobClassService
	memory := int64(2 << 30)
	request.Execution.OCI.Computer = &contract.OCIComputerSpec{DiskBytes: 8 << 30}
	request.Execution.OCI.Limits = &contract.OCILimits{MemoryBytes: &memory}
	request.ManagedVolumes = []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeComputerDisk, ComputerStorage: &workloadrunner.ComputerStorage{
		ComputerID: "computer-1", StorageID: "storage-1", StorageGeneration: 1, IntentRevision: 1, DiskBytes: 8 << 30,
	}}}
	request.Execution.Env = map[string]string{contract.EnvL3Endpoint: "http://127.0.0.1:43100/l3"}
	request.Execution.SensitiveEnv = map[string]string{contract.EnvComputerToken: "computer-pass"}
	request.AttemptEndpoints = []string{workloadrunner.AttemptEndpointView, workloadrunner.AttemptEndpointControl}
	request.AttemptEndpointReady = func(string, workloadrunner.AttemptEndpoint) error { return nil }
	request.HostBridgeFallbackActive = true
	var guestEndpoint string
	request.HostBridgeEndpointReady = func(endpoint string) error {
		guestEndpoint = endpoint
		return nil
	}
	request.HostBridgeDial = func(context.Context) (net.Conn, error) {
		adapterSide, bridgeSide := net.Pipe()
		go func() {
			defer bridgeSide.Close()
			payload := make([]byte, len("guest-request"))
			if _, err := io.ReadFull(bridgeSide, payload); err == nil && string(payload) == "guest-request" {
				_, _ = bridgeSide.Write([]byte("host-response"))
			}
		}()
		return adapterSide, nil
	}
	result, err := adapter.Run(t.Context(), request, nil)
	if err != nil || result.Outcome.ExitCode == nil || *result.Outcome.ExitCode != 0 {
		t.Fatalf("Computer fallback result=%+v err=%v", result, err)
	}
	engine.mu.Lock()
	requested, activated, computer := engine.lastRun.EnableHostBridgeFallback, engine.lastRun.ActivateHostBridgeFallback, engine.lastRun.Workload.Computer
	engine.mu.Unlock()
	if !requested || !activated || !computer || guestEndpoint == "" {
		t.Fatalf("Computer fallback requested=%t activated=%t computer=%t endpoint=%q", requested, activated, computer, guestEndpoint)
	}
}

func TestWorkloadInputMakesManagedVolumeMountsAuthoritative(t *testing.T) {
	request := workloadrunner.Request{
		Authority:      workloadrunner.AttemptAuthority{WorkloadClass: contract.JobClassOneShot},
		ManagedVolumes: []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeHandoff, OwnerKey: "run-1"}},
		Execution: contract.ExecutionSpec{
			Env: map[string]string{contract.EnvHandoffDir: "/operator/pass-through"},
			OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: "ghcr.io/example/echo:latest"}},
		},
	}
	input := workloadInput(request)
	if len(input.ManagedVolumes) != 1 || input.ManagedVolumes[0].Kind != ocihelper.ManagedVolumeHandoff || input.ManagedVolumes[0].OwnerKey != "run-1" {
		t.Fatalf("one-shot managed volumes = %+v, want handoff", input.ManagedVolumes)
	}
	if len(input.ReservedEnvironment) != 0 {
		t.Fatalf("caller supplied one-shot reserved environment = %+v", input.ReservedEnvironment)
	}

	request.Authority.WorkloadClass = contract.JobClassService
	request.ManagedVolumes = []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeServiceData}}
	request.Execution.Env = map[string]string{contract.EnvServiceDir: "/operator/pass-through"}
	input = workloadInput(request)
	if len(input.ManagedVolumes) != 1 || input.ManagedVolumes[0].Kind != ocihelper.ManagedVolumeServiceData {
		t.Fatalf("service managed volumes = %+v, want service data", input.ManagedVolumes)
	}
	if len(input.ReservedEnvironment) != 0 {
		t.Fatalf("caller supplied service reserved environment = %+v", input.ReservedEnvironment)
	}

	request.ManagedVolumes = []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeComputerDisk, ComputerStorage: &workloadrunner.ComputerStorage{
		ComputerID: "computer-1", StorageID: "storage-1", StorageGeneration: 2, IntentRevision: 3, DiskBytes: 8 << 30,
	}}}
	request.Execution.OCI.Computer = &contract.OCIComputerSpec{DiskBytes: 8 << 30}
	input = workloadInput(request)
	if !input.Computer || len(input.ManagedVolumes) != 1 || input.ManagedVolumes[0].Kind != ocihelper.ManagedVolumeComputerDisk ||
		input.ManagedVolumes[0].ComputerStorage == nil || input.ManagedVolumes[0].ComputerStorage.ComputerID != "computer-1" ||
		input.ManagedVolumes[0].ComputerStorage.StorageID != "storage-1" || input.ManagedVolumes[0].ComputerStorage.StorageGeneration != 2 ||
		input.ManagedVolumes[0].ComputerStorage.IntentRevision != 3 || input.ManagedVolumes[0].ComputerStorage.DiskBytes != 8<<30 {
		t.Fatalf("Computer managed volume = %+v", input.ManagedVolumes)
	}
}

func TestWorkloadInputMakesWeftyBridgeAndTokenHelperAuthoritative(t *testing.T) {
	request := adapterTestRequest()
	request.Execution.OCI.Computer = &contract.OCIComputerSpec{DiskBytes: 8 << 30}
	request.Execution.Env = map[string]string{
		"PUBLIC": "value", contract.EnvL3Endpoint: "http://host.lima.internal:43100/l3",
		contract.EnvRunToken: "public-layer-attacker", contract.EnvComputerToken: "public-computer-attacker",
	}
	request.Execution.SensitiveEnv = map[string]string{
		contract.EnvRunToken: "secret-token", contract.EnvComputerToken: "unminted-computer-token",
		contract.EnvL3Endpoint: "http://sensitive-precedence/l3", "OPERATOR_SECRET": "secret-value",
	}
	input := workloadInput(request)
	if len(input.Environment) != 1 || input.Environment[0].Name != "PUBLIC" ||
		len(input.SensitiveEnvironment) != 1 || input.SensitiveEnvironment[0].Name != "OPERATOR_SECRET" {
		t.Fatalf("operator environment was not separated: public=%+v sensitive=%+v", input.Environment, input.SensitiveEnvironment)
	}
	if len(input.ReservedEnvironment) != 0 || input.L3Endpoint != "http://host.lima.internal:43100/l3" ||
		input.RunToken != "secret-token" || input.ComputerToken != "unminted-computer-token" {
		t.Fatalf("helper minting inputs: reserved=%+v l3=%q run=%q computer=%q", input.ReservedEnvironment,
			input.L3Endpoint, input.RunToken, input.ComputerToken)
	}
}

func TestPortfulRunTransfersExactAuthorityEndpoint(t *testing.T) {
	engine := &adapterTestEngine{watch: ocihelper.WatchResponse{ExitCode: intPointer(0)}}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	request.Authority.WorkloadClass = contract.JobClassService
	request.AttemptEndpoints = []string{workloadrunner.AttemptEndpointService}
	var endpoint workloadrunner.AttemptEndpoint
	request.AttemptEndpointReady = func(name string, value workloadrunner.AttemptEndpoint) error {
		if name != workloadrunner.AttemptEndpointService {
			t.Fatalf("endpoint name = %q", name)
		}
		endpoint = value
		return nil
	}
	request.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error { return nil }
	result, err := adapter.Run(t.Context(), request, nil)
	if err != nil || result.Outcome.ExitCode == nil {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if endpoint.Port != 42424 || endpoint.Dial == nil {
		t.Fatalf("endpoint = %+v", endpoint)
	}
	engine.mu.Lock()
	allocated := slices.Equal(engine.lastRun.AllocateEndpoints, []string{workloadrunner.AttemptEndpointService})
	engine.mu.Unlock()
	if !allocated {
		t.Fatal("adapter did not request helper attempt-port authority")
	}
}

func TestAttemptEndpointStaysBoundToAdmittingHelperSession(t *testing.T) {
	base := &adapterTestEngine{watchSignals: make(chan ocihelper.Signal, 1)}
	engine := &endpointAdapterTestEngine{adapterTestEngine: base}
	adapter, _, source, closeAdapter := startAdapterTestServerWithSnapshots(t, engine, ImagePolicy{})
	defer closeAdapter()
	request := adapterTestRequest()
	request.Authority.WorkloadClass = contract.JobClassService
	request.AttemptEndpoints = []string{workloadrunner.AttemptEndpointService}
	endpointReady := make(chan workloadrunner.AttemptEndpoint, 1)
	request.AttemptEndpointReady = func(_ string, endpoint workloadrunner.AttemptEndpoint) error {
		endpointReady <- endpoint
		return nil
	}
	request.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error { return nil }
	runDone := make(chan error, 1)
	go func() {
		_, err := adapter.Run(t.Context(), request, nil)
		runDone <- err
	}()
	endpoint := <-endpointReady
	source.setSessionError(errors.New("replacement barrier is still pending"))
	connection, err := endpoint.Dial(t.Context())
	if err != nil {
		t.Fatalf("attempt endpoint re-read replacement barrier instead of its admitting session: %v", err)
	}
	_ = connection.Close()
	source.setSessionError(nil)
	base.watchSignals <- ocihelper.SignalTERM
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
}

func TestAdapterImageBudgetExhaustionIsPermanentAndBounded(t *testing.T) {
	engine := &adapterTestEngine{ensureErrors: []error{ocihelper.NewImageMechanicsError(ocihelper.ImageFailureFact{Kind: ocihelper.ImageFailureNetwork, TopLevelDigest: adapterTestDigest}, errors.New("temporary DNS"))}}
	adapter, closeAdapter := startAdapterTestServerWithPolicy(t, engine, ImagePolicy{
		Budget: 5 * time.Millisecond,
		Sleep: func(ctx context.Context, _ time.Duration) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})
	defer closeAdapter()
	request := adapterTestRequest()
	recoveries := 0
	request.OCIRuntimeUnavailable = func(workloadrunner.RuntimeGeneration) { recoveries++ }
	result, err := adapter.Run(t.Context(), request, nil)
	if err == nil || result.Outcome.SpawnError == nil || result.Outcome.SpawnError.Code != contract.SpawnFailureImageUnavailable {
		t.Fatalf("budget outcome=%+v err=%v", result.Outcome, err)
	}
	if engine.ensureCalls != 1 {
		t.Fatalf("budget exhaustion EnsureImage calls = %d, want 1", engine.ensureCalls)
	}
	if recoveries != 0 {
		t.Fatalf("delivery budget recovery calls = %d, want 0", recoveries)
	}
}

func TestAdapterPermanentImageErrorsFailFast(t *testing.T) {
	tests := []struct {
		name string
		fact ocihelper.ImageFailureFact
		want contract.SpawnFailureCode
	}{
		{name: "not found", fact: ocihelper.ImageFailureFact{Kind: ocihelper.ImageFailureHTTP, HTTPStatus: 404}, want: contract.SpawnFailureImageNotFound},
		{name: "unauthorized", fact: ocihelper.ImageFailureFact{Kind: ocihelper.ImageFailureHTTP, HTTPStatus: 401}, want: contract.SpawnFailureImageUnavailable},
		{name: "invalid manifest", fact: ocihelper.ImageFailureFact{Kind: ocihelper.ImageFailureManifestRejected}, want: contract.SpawnFailureImageManifestInvalid},
		{name: "unsupported platform", fact: ocihelper.ImageFailureFact{Kind: ocihelper.ImageFailurePlatformMismatch}, want: contract.SpawnFailureImagePlatformUnsupported},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fact.TopLevelDigest = adapterTestDigest
			engine := &adapterTestEngine{ensureErrors: []error{ocihelper.NewImageMechanicsError(test.fact, errors.New(test.name))}}
			adapter, closeAdapter := startAdapterTestServer(t, engine)
			defer closeAdapter()
			result, err := adapter.Run(t.Context(), adapterTestRequest(), nil)
			if err == nil || result.Outcome.SpawnError == nil || result.Outcome.SpawnError.Code != test.want {
				t.Fatalf("outcome=%+v err=%v", result.Outcome, err)
			}
			if engine.ensureCalls != 1 {
				t.Fatalf("EnsureImage calls = %d, want fail-fast 1", engine.ensureCalls)
			}
		})
	}
}

func TestAgentOwnsImageMechanicsClassificationTable(t *testing.T) {
	tests := []struct {
		name      string
		fact      ocihelper.ImageFailureFact
		want      contract.SpawnFailureCode
		transient bool
	}{
		{name: "network", fact: ocihelper.ImageFailureFact{Kind: ocihelper.ImageFailureNetwork}, want: contract.SpawnFailureImageUnavailable, transient: true},
		{name: "503", fact: ocihelper.ImageFailureFact{Kind: ocihelper.ImageFailureHTTP, HTTPStatus: 503}, want: contract.SpawnFailureImageUnavailable, transient: true},
		{name: "429", fact: ocihelper.ImageFailureFact{Kind: ocihelper.ImageFailureHTTP, HTTPStatus: 429}, want: contract.SpawnFailureImageUnavailable, transient: true},
		{name: "404", fact: ocihelper.ImageFailureFact{Kind: ocihelper.ImageFailureHTTP, HTTPStatus: 404}, want: contract.SpawnFailureImageNotFound},
		{name: "401", fact: ocihelper.ImageFailureFact{Kind: ocihelper.ImageFailureHTTP, HTTPStatus: 401}, want: contract.SpawnFailureImageUnavailable},
		{name: "manifest rejected", fact: ocihelper.ImageFailureFact{Kind: ocihelper.ImageFailureManifestRejected}, want: contract.SpawnFailureImageManifestInvalid},
		{name: "platform mismatch", fact: ocihelper.ImageFailureFact{Kind: ocihelper.ImageFailurePlatformMismatch}, want: contract.SpawnFailureImagePlatformUnsupported},
		{name: "engine loss", fact: ocihelper.ImageFailureFact{Kind: ocihelper.ImageFailureEngineLoss}, want: contract.SpawnFailureRuntimeUnavailable},
		{name: "resource exhausted", fact: ocihelper.ImageFailureFact{Kind: ocihelper.ImageFailureResourceExhausted}, want: contract.SpawnFailureRuntimeUnavailable},
		{name: "unknown", fact: ocihelper.ImageFailureFact{Kind: ocihelper.ImageFailureUnavailable}, want: contract.SpawnFailureImageUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			classification := classifyImageFailure(&ocihelper.RPCError{Code: ocihelper.CodeImageUnavailable, ImageFailure: &test.fact})
			if classification.code != test.want || classification.transient != test.transient {
				t.Fatalf("classification = %+v, want code=%s transient=%t", classification, test.want, test.transient)
			}
		})
	}
}

func TestAdapterEngineLossMidPullFailsFast(t *testing.T) {
	engine := &adapterTestEngine{
		ensureErrors:  []error{ocihelper.NewImageMechanicsError(ocihelper.ImageFailureFact{Kind: ocihelper.ImageFailureEngineLoss, TopLevelDigest: adapterTestDigest}, errors.New("containerd stopped"))},
		ensureEntered: make(chan struct{}), releaseEnsure: make(chan struct{}),
	}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	type outcome struct {
		result workloadrunner.Result
		err    error
	}
	done := make(chan outcome, 1)
	recoveries := 0
	request := adapterTestRequest()
	request.OCIRuntimeUnavailable = func(workloadrunner.RuntimeGeneration) { recoveries++ }
	go func() {
		result, err := adapter.Run(t.Context(), request, nil)
		done <- outcome{result: result, err: err}
	}()
	<-engine.ensureEntered
	close(engine.releaseEnsure)
	finished := <-done
	if finished.err == nil || finished.result.Outcome.SpawnError == nil || finished.result.Outcome.SpawnError.Code != contract.SpawnFailureRuntimeUnavailable {
		t.Fatalf("engine-loss outcome=%+v err=%v", finished.result.Outcome, finished.err)
	}
	if engine.ensureCalls != 1 {
		t.Fatalf("engine loss retried EnsureImage %d times", engine.ensureCalls)
	}
	if recoveries != 1 {
		t.Fatalf("engine loss recovery calls = %d, want 1", recoveries)
	}
}

func TestAdapterPreRunImageFailureHasPositiveNoRuntimeReapEvidence(t *testing.T) {
	engine := &adapterTestEngine{ensureErrors: []error{ocihelper.NewImageMechanicsError(
		ocihelper.ImageFailureFact{Kind: ocihelper.ImageFailureHTTP, HTTPStatus: 404}, errors.New("missing"),
	)}}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	request.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error { return nil }
	if _, _, err := adapter.Preflight(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.Run(t.Context(), request, nil); err == nil {
		t.Fatal("image delivery unexpectedly succeeded")
	}
	receipt, err := adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: request.Authority})
	if err != nil || !receipt.RuntimeQuiesced || receipt.Evidence != workloadrunner.ReapEvidenceNoRuntime {
		t.Fatalf("pre-Run reap receipt = (%+v, %v)", receipt, err)
	}
	if engine.deletes != 0 {
		t.Fatalf("pre-Run failure called helper Delete %d times", engine.deletes)
	}
}

func TestCapacityRefusalsAreDefinitiveBeforeRuntimeCreation(t *testing.T) {
	for _, code := range []ocihelper.ErrorCode{ocihelper.CodeInsufficientMemory, ocihelper.CodeInsufficientDisk} {
		if !helperRunDefinitivelyRejected(&ocihelper.RPCError{Code: code, Message: "capacity refused"}) {
			t.Fatalf("capacity refusal %q would trigger a second helper Delete", code)
		}
	}
}

func TestTypedCapacityRefusalSurvivesAdapterFinalizationWithoutSecondDelete(t *testing.T) {
	engine := &adapterTestEngine{runErr: &ocihelper.RPCError{
		Code: ocihelper.CodeInsufficientMemory, Message: "capacity refused",
		MemoryFailure: &ocihelper.MemoryFailureFact{RequestedBytes: 1 << 30, ObservedAvailableBytes: 0},
	}, refuseDelete: true}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	result, err := adapter.Run(t.Context(), request, nil)
	if err == nil || result.Outcome.SpawnError == nil || result.Outcome.SpawnError.Code != contract.SpawnFailureInsufficientMemory {
		t.Fatalf("typed capacity result = %+v err=%v", result.Outcome, err)
	}
	receipt, reapErr := adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: request.Authority})
	if reapErr != nil || !receipt.RuntimeQuiesced || receipt.Evidence != workloadrunner.ReapEvidenceNoRuntime {
		t.Fatalf("capacity refusal finalization = %+v err=%v", receipt, reapErr)
	}
	if engine.runtimeDeletes != 0 {
		t.Fatalf("typed capacity refusal attempted %d second runtime deletes", engine.runtimeDeletes)
	}
}

func TestAdapterReleasesAttachedAttemptPinAfterPreRunAbandonment(t *testing.T) {
	engine := &adapterTestEngine{}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	request.OCIImageResolved = func(context.Context, workloadrunner.OCIImageObservation) error {
		return &workloadrunner.OCIObservationRefusal{Err: errors.New("stale fence")}
	}
	if _, err := adapter.Run(t.Context(), request, nil); err == nil {
		t.Fatal("observation refusal unexpectedly ran")
	}
	receipt, err := adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: request.Authority})
	if err != nil || !receipt.RuntimeQuiesced || receipt.Evidence != workloadrunner.ReapEvidenceNoRuntime {
		t.Fatalf("pre-Run pin reap = (%+v, %v)", receipt, err)
	}
	if engine.deletes != 1 {
		t.Fatalf("pre-Run attached pin release calls = %d, want 1", engine.deletes)
	}
}

func TestReleaseBindingPinWithoutLedgerRowDoesNotAcquireHelper(t *testing.T) {
	adapter := NewAdapter(&failingSessionSource{})
	if err := adapter.ReleaseOCIImageBindingPin(t.Context(), "process-job"); err != nil {
		t.Fatal(err)
	}
}

func TestAdapterReconciliationAutomaticallyRedeliversEveryMissingBinding(t *testing.T) {
	engine := &adapterTestEngine{missingUntilEnsure: true}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	pin := workloadrunner.OCIImageBindingPin{
		JobID: "service-a", Reference: "example.invalid/image", Digest: adapterTestDigest,
		PlatformOS: "linux", PlatformArchitecture: "amd64", Snapshotter: ocihelper.DefaultSnapshotter,
	}
	if _, created, err := adapter.pinLedger.PutOCIImageBindingPin(t.Context(), pin); err != nil || !created {
		t.Fatalf("persist binding pin = created %t err %v", created, err)
	}
	failures, err := adapter.ReconcileOCIImagePins(t.Context(), func(context.Context, string) (bool, error) { return true, nil })
	if err != nil || len(failures) != 0 {
		t.Fatalf("automatic redelivery = failures %+v err %v", failures, err)
	}
	if engine.ensureCalls != 1 || engine.reconcileCalls != 2 {
		t.Fatalf("automatic redelivery calls ensure=%d reconcile=%d", engine.ensureCalls, engine.reconcileCalls)
	}
}

func TestAdapterReconciliationReportsEveryBindingWhoseBudgetedRedeliveryFails(t *testing.T) {
	missing := ocihelper.NewImageMechanicsError(ocihelper.ImageFailureFact{Kind: ocihelper.ImageFailureHTTP, HTTPStatus: 404, TopLevelDigest: adapterTestDigest}, errors.New("missing"))
	engine := &adapterTestEngine{missingUntilEnsure: true, ensureErrors: []error{missing, missing}}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	for _, jobID := range []string{"service-a", "service-b"} {
		pin := workloadrunner.OCIImageBindingPin{
			JobID: jobID, Reference: "example.invalid/image", Digest: adapterTestDigest,
			PlatformOS: "linux", PlatformArchitecture: "amd64", Snapshotter: ocihelper.DefaultSnapshotter,
		}
		if _, _, err := adapter.pinLedger.PutOCIImageBindingPin(t.Context(), pin); err != nil {
			t.Fatal(err)
		}
	}
	failures, err := adapter.ReconcileOCIImagePins(t.Context(), func(context.Context, string) (bool, error) { return true, nil })
	if err != nil || len(failures) != 2 || engine.ensureCalls != 2 {
		t.Fatalf("failed redeliveries = failures %+v calls %d err %v", failures, engine.ensureCalls, err)
	}
	for _, failure := range failures {
		if failure.Failure.Code != contract.SpawnFailureImageNotFound {
			t.Fatalf("redelivery failure = %+v", failure)
		}
	}
}

func TestAdapterReconciliationDropsLedgerRowsWithoutPositiveBindingProof(t *testing.T) {
	engine := &adapterTestEngine{}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	pin := workloadrunner.OCIImageBindingPin{JobID: "stale", Reference: "example.invalid/image", Digest: adapterTestDigest, PlatformOS: "linux", PlatformArchitecture: "amd64", Snapshotter: ocihelper.DefaultSnapshotter}
	if _, _, err := adapter.pinLedger.PutOCIImageBindingPin(t.Context(), pin); err != nil {
		t.Fatal(err)
	}
	if failures, err := adapter.ReconcileOCIImagePins(t.Context(), func(context.Context, string) (bool, error) { return false, nil }); err != nil || len(failures) != 0 {
		t.Fatalf("stale binding reconciliation = failures %+v err %v", failures, err)
	}
	pins, err := adapter.pinLedger.ListOCIImageBindingPins(t.Context())
	if err != nil || len(pins) != 0 {
		t.Fatalf("stale ledger rows = %+v err %v", pins, err)
	}
}

func TestServiceTerminalDeliveryFailureRemovesNewBindingLedgerRow(t *testing.T) {
	engine := &adapterTestEngine{ensureErrors: []error{ocihelper.NewImageMechanicsError(
		ocihelper.ImageFailureFact{Kind: ocihelper.ImageFailureHTTP, HTTPStatus: 404, TopLevelDigest: adapterTestDigest}, errors.New("missing"),
	)}}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	request.Authority.WorkloadClass = contract.JobClassService
	if _, err := adapter.Run(t.Context(), request, nil); err == nil {
		t.Fatal("terminal service delivery unexpectedly succeeded")
	}
	pins, err := adapter.pinLedger.ListOCIImageBindingPins(t.Context())
	if err != nil || len(pins) != 0 {
		t.Fatalf("terminal delivery retained binding rows %+v err=%v", pins, err)
	}
}

func TestServiceRestartRejectsChangedProbePlatformWithoutMutatingFirstBinding(t *testing.T) {
	engine := &adapterTestEngine{}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	pin := workloadrunner.OCIImageBindingPin{
		JobID: "job", Reference: "example.invalid/image", Digest: adapterTestDigest,
		PlatformOS: "linux", PlatformArchitecture: "arm64", Snapshotter: ocihelper.DefaultSnapshotter,
	}
	if _, _, err := adapter.pinLedger.PutOCIImageBindingPin(t.Context(), pin); err != nil {
		t.Fatal(err)
	}
	request := adapterTestRequest()
	request.Authority.WorkloadClass = contract.JobClassService
	result, err := adapter.Run(t.Context(), request, nil)
	if err == nil || result.Outcome.SpawnError == nil || result.Outcome.SpawnError.Code != contract.SpawnFailureImagePlatformUnsupported {
		t.Fatalf("first-binding platform mismatch = (%+v, %v)", result.Outcome, err)
	}
	pins, listErr := adapter.pinLedger.ListOCIImageBindingPins(t.Context())
	if listErr != nil || len(pins) != 1 || pins[0] != pin {
		t.Fatalf("first binding mutated = %+v err=%v", pins, listErr)
	}
}

type adapterTestEngine struct {
	mu                            sync.Mutex
	watch                         ocihelper.WatchResponse
	deletes                       int
	runtimeDeletes                int
	refuseDelete                  bool
	runErr                        error
	startedAt                     time.Time
	ensureErrors                  []error
	ensureCalls                   int
	responseDigest                string
	responsePlatform              ocihelper.OCIPlatform
	ensureEntered                 chan struct{}
	releaseEnsure                 chan struct{}
	lastRun                       ocihelper.RunRequest
	lastEnsure                    ocihelper.EnsureImageRequest
	bridgeExchange                chan error
	missingUntilEnsure            bool
	reconcileCalls                int
	watchSignals                  chan ocihelper.Signal
	ignoreTERM                    bool
	ignoreKILL                    bool
	exitOnKillRace                bool
	killAlreadyTerminated         bool
	releaseDelay                  time.Duration
	blockSignal                   bool
	signals                       []ocihelper.Signal
	watchErrorOnCancel            bool
	inventoryRemoval              ocihelper.InventoryRemovalResponse
	inventoryErr                  error
	attestRemoval                 ocihelper.AttestRemovalResponse
	attestErr                     error
	doctorPlatform                ocihelper.OCIPlatform
	doctorErr                     error
	doctorCalls                   int
	requireReapBeforeVolumeDelete bool
	volumeDeleteBeforeReap        bool
	volumeDeleteFailures          int
	volumeDeleteCalls             int
}

type endpointAdapterTestEngine struct{ *adapterTestEngine }

func (*endpointAdapterTestEngine) DialAttemptPort(ctx context.Context, _ ocihelper.DialAttemptPortRequest, stream io.ReadWriteCloser) error {
	if _, err := stream.Write([]byte{1}); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

func (engine *adapterTestEngine) DoctorStatus(context.Context) (ocihelper.DoctorStatus, error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	engine.doctorCalls++
	platform := engine.doctorPlatform
	if platform.OS == "" {
		platform = ocihelper.OCIPlatform{OS: "linux", Architecture: "amd64"}
	}
	return ocihelper.DoctorStatus{RuntimePlatform: platform}, engine.doctorErr
}

func (engine *adapterTestEngine) ReconcileImagePins(_ context.Context, request ocihelper.ReconcileImagePinsRequest) (ocihelper.ReconcileImagePinsResponse, error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	engine.reconcileCalls++
	if engine.missingUntilEnsure && engine.ensureCalls == 0 && len(request.Bindings) != 0 {
		return ocihelper.ReconcileImagePinsResponse{MissingDigests: []string{request.Bindings[0].Digest}}, nil
	}
	return ocihelper.ReconcileImagePinsResponse{}, nil
}

func (*adapterTestEngine) ReleaseImagePin(context.Context, ocihelper.ReleaseImagePinRequest) error {
	return nil
}

func (engine *adapterTestEngine) ReleaseAttemptImagePin(context.Context, ocihelper.ReleaseAttemptImagePinRequest) error {
	engine.mu.Lock()
	engine.deletes++
	engine.mu.Unlock()
	return nil
}

func (*adapterTestEngine) ImageCacheStatus(context.Context) (ocihelper.ImageCacheStatus, error) {
	return ocihelper.ImageCacheStatus{}, nil
}

func (engine *adapterTestEngine) EnsureImage(_ context.Context, request ocihelper.EnsureImageRequest, archive io.Reader, emit func(ocihelper.EnsureImageEvent) error) error {
	engine.mu.Lock()
	engine.lastEnsure = request
	call := engine.ensureCalls
	engine.ensureCalls++
	var ensureErr error
	if call < len(engine.ensureErrors) {
		ensureErr = engine.ensureErrors[call]
	}
	ensureEntered := engine.ensureEntered
	releaseEnsure := engine.releaseEnsure
	engine.mu.Unlock()
	if ensureEntered != nil {
		close(ensureEntered)
		<-releaseEnsure
	}
	if archive != nil {
		if _, err := io.Copy(io.Discard, archive); err != nil {
			return err
		}
	}
	if ensureErr != nil {
		return ensureErr
	}
	digest := request.Digest
	if engine.responseDigest != "" {
		digest = engine.responseDigest
	}
	if digest == "" {
		digest = adapterTestDigest
	}
	evidence := adapterTestImageEvidence(digest)
	if engine.responsePlatform.OS != "" {
		evidence.Platform = engine.responsePlatform
	}
	return emit(ocihelper.EnsureImageEvent{Kind: ocihelper.ImageComplete, Result: &ocihelper.EnsureImageResponse{
		TopLevelDigest: digest, PlatformDigest: digest, Evidence: evidence,
	}})
}
func (engine *adapterTestEngine) Run(_ context.Context, request ocihelper.RunRequest) (ocihelper.RunResponse, error) {
	engine.mu.Lock()
	engine.lastRun = request
	engine.mu.Unlock()
	if engine.runErr != nil {
		return ocihelper.RunResponse{}, engine.runErr
	}
	startedAt := engine.startedAt
	if startedAt.IsZero() {
		startedAt = time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	}
	response := ocihelper.RunResponse{Started: true, StartedAt: startedAt, Image: &ocihelper.ImageEvidence{
		SubmittedReference: "example.invalid/image", TopLevelDigest: adapterTestDigest, TopLevelMediaType: "application/vnd.oci.image.manifest.v1+json",
		PlatformManifestDigest: adapterTestDigest, Platform: ocihelper.OCIPlatform{OS: "linux", Architecture: "amd64"},
		RuntimeHandler: ocihelper.DefaultRuntimeHandler, Snapshotter: ocihelper.DefaultSnapshotter,
	}}
	if request.EnableHostBridgeFallback {
		response.HostBridgeReady = true
		response.HostBridgeEndpoint = "http://127.0.0.1:42425/l3"
	}
	if len(request.AllocateEndpoints) > 0 {
		response.Endpoints = make(map[string]uint16, len(request.AllocateEndpoints))
		for index, name := range request.AllocateEndpoints {
			response.Endpoints[name] = uint16(42424 + index)
		}
	}
	return response, nil
}
func (engine *adapterTestEngine) Signal(ctx context.Context, request ocihelper.SignalRequest) error {
	engine.mu.Lock()
	engine.signals = append(engine.signals, request.Signal)
	watchSignals := engine.watchSignals
	ignore := request.Signal == ocihelper.SignalTERM && engine.ignoreTERM || request.Signal == ocihelper.SignalKILL && engine.ignoreKILL
	exitOnKillRace := request.Signal == ocihelper.SignalKILL && engine.exitOnKillRace
	killAlreadyTerminated := request.Signal == ocihelper.SignalKILL && engine.killAlreadyTerminated
	block := engine.blockSignal
	engine.mu.Unlock()
	if block {
		<-ctx.Done()
		return ctx.Err()
	}
	if watchSignals != nil && exitOnKillRace {
		watchSignals <- ocihelper.SignalTERM
		return ocihelper.ErrTaskAlreadyTerminated
	}
	if killAlreadyTerminated {
		return ocihelper.ErrTaskAlreadyTerminated
	}
	if watchSignals != nil && !ignore {
		watchSignals <- request.Signal
	}
	return nil
}
func (engine *adapterTestEngine) Watch(ctx context.Context, _ ocihelper.WatchRequest, emit func(ocihelper.WatchEvent) error) error {
	if engine.bridgeExchange != nil {
		if err := <-engine.bridgeExchange; err != nil {
			return err
		}
	}
	if err := emit(ocihelper.WatchEvent{Kind: ocihelper.WatchProgress, Log: &ocihelper.LogFrame{Stream: "stdout", Sequence: 0, Bytes: []byte("frame"), Checksum: "9dff50df08c635815f4b19da10f756605a34a79a48d4ba48712782502975a70e"}}); err != nil {
		return err
	}
	if engine.watchSignals != nil {
		signal := <-engine.watchSignals
		if engine.releaseDelay > 0 {
			timer := time.NewTimer(engine.releaseDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		engine.watch = ocihelper.WatchResponse{Signal: signal, TerminationCause: "agent"}
	}
	if engine.watchErrorOnCancel {
		<-ctx.Done()
		return errors.New("use of closed network connection")
	}
	return emit(ocihelper.WatchEvent{Kind: ocihelper.WatchComplete, Result: &engine.watch})
}
func (engine *adapterTestEngine) Delete(context.Context, ocihelper.DeleteRequest) (ocihelper.DeleteResponse, error) {
	engine.mu.Lock()
	engine.deletes++
	engine.runtimeDeletes++
	engine.mu.Unlock()
	return ocihelper.DeleteResponse{Deleted: !engine.refuseDelete}, nil
}

func (engine *adapterTestEngine) DeleteManagedVolume(_ context.Context, request ocihelper.DeleteManagedVolumeRequest) (ocihelper.DeleteManagedVolumeResponse, error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	engine.volumeDeleteCalls++
	if engine.requireReapBeforeVolumeDelete && engine.runtimeDeletes == 0 {
		engine.volumeDeleteBeforeReap = true
		return ocihelper.DeleteManagedVolumeResponse{}, errors.New("Computer disk remains mounted during deletion")
	}
	if engine.volumeDeleteCalls <= engine.volumeDeleteFailures {
		if request.QuarantineOnFailure {
			return ocihelper.DeleteManagedVolumeResponse{Quarantine: &ocihelper.ManagedVolumeQuarantineReceipt{
				Kind: "managed_volume_cleanup_quarantined", ReceiptID: "quarantine-receipt", VolumeKind: request.Kind,
				ComputerStorage: *request.ComputerStorage, Removal: *request.Removal,
				FailureReason: ocihelper.EngineFailureOperationFailed, Attempts: request.FailureAttempts,
			}}, nil
		}
		return ocihelper.DeleteManagedVolumeResponse{}, errors.New("loop remained attached")
	}
	return ocihelper.DeleteManagedVolumeResponse{Deleted: true}, nil
}
func (engine *adapterTestEngine) InventoryRemoval(context.Context, ocihelper.InventoryRemovalRequest) (ocihelper.InventoryRemovalResponse, error) {
	return engine.inventoryRemoval, engine.inventoryErr
}
func (engine *adapterTestEngine) AttestRemoval(context.Context, ocihelper.AttestRemovalRequest) (ocihelper.AttestRemovalResponse, error) {
	return engine.attestRemoval, engine.attestErr
}
func (*adapterTestEngine) Verify(context.Context, ocihelper.VerifyRequest) (ocihelper.VerifyResponse, error) {
	return ocihelper.VerifyResponse{Absent: true}, nil
}
func (*adapterTestEngine) Sweep(context.Context, ocihelper.SweepRequest) (ocihelper.SweepResponse, error) {
	return ocihelper.SweepResponse{Inventory: emptyAdapterInventory()}, nil
}
func (*adapterTestEngine) DialAttemptPort(context.Context, ocihelper.DialAttemptPortRequest, io.ReadWriteCloser) error {
	return errors.New("unsupported")
}
func (engine *adapterTestEngine) DialHostBridge(_ context.Context, _ ocihelper.DialHostBridgeRequest, stream io.ReadWriteCloser) error {
	if engine.bridgeExchange == nil {
		return errors.New("unsupported")
	}
	_, err := stream.Write([]byte("guest-request"))
	if err == nil {
		payload := make([]byte, len("host-response"))
		_, err = io.ReadFull(stream, payload)
		if err == nil && string(payload) != "host-response" {
			err = errors.New("unexpected host bridge response")
		}
	}
	engine.bridgeExchange <- err
	return err
}
func (*adapterTestEngine) ReapAttempt(context.Context, ocihelper.AttemptAuthority) error { return nil }
func (*adapterTestEngine) ReapSession(context.Context, ocihelper.SessionIdentity) error  { return nil }

func startAdapterTestServer(t *testing.T, engine ocihelper.Engine) (*Adapter, func()) {
	return startAdapterTestServerWithPolicy(t, engine, ImagePolicy{})
}

func startAdapterTestServerWithPolicy(t *testing.T, engine ocihelper.Engine, policy ImagePolicy) (*Adapter, func()) {
	adapter, _, _, closeAdapter := startAdapterTestServerWithSnapshots(t, engine, policy)
	return adapter, closeAdapter
}

func startAdapterTestServerWithSnapshots(t *testing.T, engine ocihelper.Engine, policy ImagePolicy) (*Adapter, *ocihelper.BootBarrier, *adapterSnapshotSource, func()) {
	t.Helper()
	directory, err := os.MkdirTemp("", "woci-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	socketPath := filepath.Join(directory, "helper.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	server, err := ocihelper.NewServer(engine, ocihelper.ServerConfig{AllowedUIDs: []uint32{uint32(os.Getuid())}, HelperChecksum: "adapter-test", HeartbeatTimeout: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	client := ocihelper.NewUnixClient(socketPath, "adapter-test")
	barrier, err := ocihelper.NewBootBarrier(client, ocihelper.AcquireSessionRequest{NodeID: "node", BootSessionID: "boot"})
	if err != nil {
		t.Fatal(err)
	}
	if err := barrier.Ensure(t.Context()); err != nil {
		t.Fatal(err)
	}
	source := &adapterSnapshotSource{barrier: barrier}
	adapter := NewAdapterWithPolicy(source, policy)
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	adapter.probePlatforms[helperSession(session)] = ocihelper.OCIPlatform{OS: "linux", Architecture: "amd64"}
	return adapter, barrier, source, func() { _ = barrier.Close(); cancel(); _ = listener.Close(); <-done }
}

func adapterTestRequest() workloadrunner.Request {
	digest := adapterTestDigest
	return workloadrunner.Request{
		Authority:        workloadrunner.AttemptAuthority{NodeID: "node", BootSessionID: "boot", JobID: "job", AttemptID: "attempt", FencingToken: "fence", WorkloadClass: "one-shot", RemovalGeneration: "attempt"},
		RuntimeHandler:   ocihelper.DefaultRuntimeHandler,
		Execution:        contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: "example.invalid/image", Digest: &digest}, Argv: []string{"/bin/true"}}},
		InitialDeadman:   time.Second,
		OCIImageResolved: func(context.Context, workloadrunner.OCIImageObservation) error { return nil },
		OCIStarted:       func(context.Context, workloadrunner.OCIImageObservation) error { return nil },
	}
}

func adapterTestImageEvidence(digest string) ocihelper.ImageEvidence {
	return ocihelper.ImageEvidence{
		SubmittedReference: "example.invalid/image", TopLevelDigest: digest,
		TopLevelMediaType: "application/vnd.oci.image.manifest.v1+json", PlatformManifestDigest: digest,
		Platform:       ocihelper.OCIPlatform{OS: "linux", Architecture: "amd64"},
		RuntimeHandler: ocihelper.DefaultRuntimeHandler, Snapshotter: ocihelper.DefaultSnapshotter,
	}
}

func emptyAdapterInventory() ocihelper.ResourceInventory {
	return ocihelper.ResourceInventory{Leases: []string{}, Snapshots: []string{}, Containers: []string{}, Tasks: []string{}, Shims: []string{}, Cgroups: []string{}, LogSegments: []string{}, ManagedVolumes: []string{}, ManagedVolumeRecords: []string{}}
}

func TestLegacyRemovalReconstructsFrozenInventoryFromCurrentHelperScan(t *testing.T) {
	authority := ocihelper.AttemptAuthority{NodeID: "node", BootSessionID: "boot", JobID: "legacy-service", AttemptID: "attempt", FencingToken: "fence", Class: contract.JobClassService, RemovalGeneration: "1"}
	identity, err := ocihelper.DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	engine := &adapterTestEngine{inventoryRemoval: ocihelper.InventoryRemovalResponse{Attempts: []ocihelper.RemovalAttemptManifest{{
		Authority: authority, Resources: ocihelper.ExpectedRemovalResources(identity, "", nil),
	}}}}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := workloadrunner.RuntimeRemovalProofRequest{NodeID: "node", BootSessionID: "boot", JobID: "legacy-service", RemovalGeneration: 1, CleanupFence: "cleanup"}
	attempts, err := adapter.ReconstructRuntimeRemoval(t.Context(), request)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("reconstructed attempts = %+v err=%v", attempts, err)
	}
	manifest := attempts[0]
	if manifest.RuntimeKind != contract.JobKindOCI || manifest.ServiceDataVolume == "" || manifest.ServiceDataOwnerRecord == "" || len(manifest.RemovalResources()) != 9 {
		t.Fatalf("reconstructed manifest is incomplete: %+v", manifest)
	}
	engine.inventoryRemoval.Attempts = nil
	if _, err := adapter.ReconstructRuntimeRemoval(t.Context(), request); err == nil {
		t.Fatal("legacy service without matching sweep inventory was upgraded to verified removal")
	}
}

func TestRemovalManifestRegistriesMatchHelperForEveryKind(t *testing.T) {
	authority := ocihelper.AttemptAuthority{NodeID: "node", BootSessionID: "boot", JobID: "service", AttemptID: "attempt", FencingToken: "fence", Class: contract.JobClassService, RemovalGeneration: "1"}
	identity, err := ocihelper.DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	computer := &workloadrunner.ComputerStorage{ComputerID: "computer", StorageID: "storage", StorageGeneration: 2, DiskBytes: 8 << 30}
	for _, test := range []struct {
		name    string
		handoff string
		storage *workloadrunner.ComputerStorage
	}{
		{name: "service with handoff", handoff: "wefty-handoff-volume-owner"},
		{name: "Computer", storage: computer},
	} {
		t.Run(test.name, func(t *testing.T) {
			manifest := adapterRemovalManifest(authority, identity, test.handoff, test.storage)
			var helperStorage *ocihelper.ComputerStorageReference
			if test.storage != nil {
				helperStorage = &ocihelper.ComputerStorageReference{
					ComputerID: test.storage.ComputerID, StorageID: test.storage.StorageID,
					StorageGeneration: test.storage.StorageGeneration, DiskBytes: test.storage.DiskBytes,
				}
			}
			if helper := ocihelper.ExpectedRemovalResources(identity, test.handoff, helperStorage); !sameRemovalRegistries(manifest, helper) {
				t.Fatalf("agent registry = %+v, helper registry = %+v", manifest.RemovalResources(), helper)
			}
		})
	}
}

func TestAdapterRejectsShortForgedAndNegativeRemovalReceipts(t *testing.T) {
	authority := ocihelper.AttemptAuthority{NodeID: "node", BootSessionID: "boot", JobID: "service", AttemptID: "attempt", FencingToken: "fence", Class: contract.JobClassService, RemovalGeneration: "1"}
	identity, err := ocihelper.DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	manifest := adapterRemovalManifest(authority, identity, "", nil)
	request := workloadrunner.RuntimeRemovalProofRequest{NodeID: authority.NodeID, BootSessionID: authority.BootSessionID, JobID: authority.JobID, RemovalGeneration: 1, CleanupFence: "cleanup", Attempts: []workloadrunner.RuntimeResourceManifest{manifest}}
	valid := make([]ocihelper.RemovalAssertion, 0, len(manifest.RemovalResources()))
	for _, resource := range manifest.RemovalResources() {
		valid = append(valid, ocihelper.RemovalAssertion{Class: ocihelper.RemovalResourceClass(resource.Class), ID: resource.ID, Absent: true})
	}
	for _, test := range []struct {
		name       string
		assertions []ocihelper.RemovalAssertion
	}{
		{name: "short", assertions: slices.Clone(valid[:len(valid)-1])},
		{name: "negative", assertions: func() []ocihelper.RemovalAssertion { rows := slices.Clone(valid); rows[0].Absent = false; return rows }()},
		{name: "forged", assertions: func() []ocihelper.RemovalAssertion { rows := slices.Clone(valid); rows[0].ID = "forged"; return rows }()},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine := &adapterTestEngine{attestRemoval: ocihelper.AttestRemovalResponse{Assertions: test.assertions}}
			adapter, closeAdapter := startAdapterTestServer(t, engine)
			defer closeAdapter()
			if receipt, err := adapter.AttestRuntimeRemoval(t.Context(), request); err == nil {
				t.Fatalf("malformed helper receipt accepted: %+v", receipt)
			}
		})
	}
}

func adapterRemovalManifest(authority ocihelper.AttemptAuthority, identity ocihelper.ResourceIdentity, handoff string, computer *workloadrunner.ComputerStorage) workloadrunner.RuntimeResourceManifest {
	manifest := workloadrunner.RuntimeResourceManifest{
		Version: 1, RuntimeKind: contract.JobKindOCI,
		NodeID: authority.NodeID, BootSessionID: authority.BootSessionID, JobID: authority.JobID,
		AttemptID: authority.AttemptID, FencingToken: authority.FencingToken, WorkloadClass: authority.Class,
		RemovalGeneration: authority.RemovalGeneration, LeaseID: identity.LeaseID, TaskID: identity.TaskID,
		ContainerID: identity.ContainerID, SnapshotID: identity.SnapshotID, ShimID: identity.ShimID,
		CgroupID: identity.CgroupID, LogSegmentDirectory: identity.LogSegmentDirectory, HandoffVolume: handoff,
		ComputerStorage: computer,
	}
	if computer == nil {
		manifest.ServiceDataVolume = identity.ServiceVolumeDirectory
		manifest.ServiceDataOwnerRecord = identity.ServiceVolumeOwnerRecord
	}
	return manifest
}

func intPointer(value int) *int { return &value }

type adapterReceiptSource struct {
	receipt ocihelper.VerifiedSweepReceipt
}

type failingSessionSource struct{}

func (*failingSessionSource) Session() (*ocihelper.Session, error) {
	return nil, errors.New("helper session must not be acquired")
}

func (*failingSessionSource) ExecutionSnapshot() (*ocihelper.Session, ocihelper.VerifiedSweepReceipt, error) {
	return nil, ocihelper.VerifiedSweepReceipt{}, errors.New("helper session must not be acquired")
}

type adapterSnapshotSource struct {
	barrier    *ocihelper.BootBarrier
	mu         sync.Mutex
	override   *ocihelper.VerifiedSweepReceipt
	sessionErr error
}

func (source *adapterSnapshotSource) Session() (*ocihelper.Session, error) {
	source.mu.Lock()
	err := source.sessionErr
	source.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return source.barrier.Session()
}

func (source *adapterSnapshotSource) ExecutionSnapshot() (*ocihelper.Session, ocihelper.VerifiedSweepReceipt, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.override != nil {
		return nil, *source.override, errors.New("helper session unavailable during recovery")
	}
	return source.barrier.ExecutionSnapshot()
}

func (source *adapterSnapshotSource) SweepReceipt() (ocihelper.VerifiedSweepReceipt, bool) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.override != nil {
		return *source.override, true
	}
	return source.barrier.SweepReceipt()
}

func (source *adapterSnapshotSource) setUnavailable(receipt ocihelper.VerifiedSweepReceipt) {
	source.mu.Lock()
	source.override = &receipt
	source.mu.Unlock()
}

func (source *adapterSnapshotSource) clearUnavailable() {
	source.mu.Lock()
	source.override = nil
	source.mu.Unlock()
}

func (source *adapterSnapshotSource) setSessionError(err error) {
	source.mu.Lock()
	source.sessionErr = err
	source.mu.Unlock()
}

func (*adapterReceiptSource) Session() (*ocihelper.Session, error) { return nil, errors.New("unused") }
func (source *adapterReceiptSource) ExecutionSnapshot() (*ocihelper.Session, ocihelper.VerifiedSweepReceipt, error) {
	return nil, source.receipt, errors.New("unused")
}
func (source *adapterReceiptSource) SweepReceipt() (ocihelper.VerifiedSweepReceipt, bool) {
	return source.receipt, true
}
