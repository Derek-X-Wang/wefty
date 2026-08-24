//go:build service_acceptance_realtiming && linux

package ocihelper_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	ocirunner "github.com/Derek-X-Wang/wefty/runner/oci"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
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
	if address == "" || helperSocket == "" || helperChecksum == "" || reference == "" || digest == "" || archivePath == "" {
		t.Fatal("Linux OCI realtiming provisioning is incomplete")
	}
	if os.Geteuid() == 0 {
		t.Fatal("Linux OCI realtiming test process must be unprivileged")
	}
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
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	if err := barrier.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	adapter := ocirunner.NewAdapter(barrier)
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	probeStarted := time.Now()
	if err := adapter.Probe(ctx, "native-node", "native-boot", reference, digest, l1.DefaultLeaseDuration); err != nil {
		t.Fatal(err)
	}
	probeElapsed := time.Since(probeStarted)
	if probeElapsed > 10*time.Second {
		t.Fatalf("production functional probe took %s, want at most 10s", probeElapsed)
	}

	// Pull and offline import each start from an empty containerd root. The
	// second row also rejects all registry HTTPS so the tar stream is the only
	// possible source of the imported bytes.
	requestRootFault(t, "reset-containerd")
	var pulled ocihelper.EnsureImageResponse
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
	importErr := session.ImportImage(ctx, ocihelper.EnsureImageRequest{
		Reference: reference, Digest: digest, Source: ocihelper.ImageSourceArchive,
		Platform:         ocihelper.OCIPlatform{OS: "linux", Architecture: runtime.GOARCH},
		OperationTimeout: 2 * time.Minute,
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
	if !reflect.DeepEqual(pulled, imported) || pulled.TopLevelDigest != digest || pulled.PlatformDigest == "" {
		t.Fatalf("pull/import evidence differs: pull=%+v import=%+v", pulled, imported)
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
	refloat := newRefloatRegistry(t, archivePath)
	exerciseNativeLinuxPrestartRequeue(t, ctx, adapter, barrier, reference, digest, refloat.reference(), refloat.originalDigest(), refloat.moveTag)
	if requests := refloat.observedTagRequests(); requests != 1 {
		t.Fatalf("mutable tag was resolved %d times, want exactly the initial resolution", requests)
	}
	session, err = barrier.Session()
	if err != nil {
		t.Fatalf("load recovered helper session: %v", err)
	}

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
	verification, err := session.Verify(ctx, ocihelper.VerifyRequest{Scope: ocihelper.VerifyNamespace})
	if err != nil || !verification.Absent {
		t.Fatalf("namespace cleanup verification=%+v err=%v", verification, err)
	}
	if evidenceDirectory := os.Getenv("WEFTY_REALTIME_EVIDENCE_DIR"); evidenceDirectory != "" {
		evidence := fmt.Sprintf("agent_uid=%d\nhelper_uid=0\nhelper_socket_root_owned=true\nraw_socket_denied=true\nprobe_elapsed=%s\nproduction_deadman=%s\npull_from_empty=true\nregistry_disabled_import=true\npull_import_digest_equal=true\nimport_run=true\nprestart_requeue_pinned=true\ntag_refloat_resolved_once=true\nwait_before_start=true\nlive_log_delivery=true\nexit_code=7\nplain_137_exit=true\nsignal=KILL\nsignal_cause=agent\noom_kill=true\nshim_loss=runtime_failure\ncontainerd_stop=runtime_failure\ncontrol_loss_reaped=true\nstdout_log=true\nstderr_log=true\nnamespace_absent=true\n", os.Getuid(), probeElapsed, l1.DefaultLeaseDuration)
		if err := os.WriteFile(filepath.Join(evidenceDirectory, "native-linux-oci.txt"), []byte(evidence), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func exerciseNativeLinuxPrestartRequeue(t *testing.T, ctx context.Context, adapter *ocirunner.Adapter, barrier *ocihelper.BootBarrier, probeReference, probeDigest, reference, expectedDigest string, afterResolution func()) {
	t.Helper()
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
		Execution:      contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: reference}, Argv: []string{"/bin/true"}}},
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
	if firstRunErr == nil || firstResult.Outcome.SpawnError == nil || firstResult.Outcome.SpawnError.Code != contract.SpawnFailureRuntimeUnavailable {
		t.Fatalf("pre-start engine loss = result %+v err %v", firstResult.Outcome, firstRunErr)
	}
	if err := barrier.Ensure(ctx); err != nil {
		t.Fatalf("re-establish boot barrier after pre-start engine loss: %v", err)
	}
	if err := adapter.Probe(ctx, firstRequest.Authority.NodeID, firstRequest.Authority.BootSessionID, probeReference, probeDigest, l1.DefaultLeaseDuration); err != nil {
		t.Fatalf("re-probe after pre-start engine loss: %v", err)
	}
	if sweep, ok := barrier.SweepReceipt(); !ok || sweep.SweepEpoch == "" || sweep.HelperSession.SessionGeneration == 0 {
		t.Fatalf("pre-start recovery omitted verified sweep evidence: %+v", sweep)
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
	_ = os.Remove(ack)
	if err := os.WriteFile(fifo, []byte(action+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(ack); err == nil {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("root fault %s was not acknowledged", action)
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
