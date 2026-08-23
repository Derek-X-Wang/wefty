package oci

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

const adapterTestDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

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

func TestAdapterPersistsResolutionBeforePrestartRunFailure(t *testing.T) {
	engine := &adapterTestEngine{runErr: errors.New("containerd stopped before task start")}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
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

func TestAdapterRejectsHelperDigestDifferentFromPinnedRequest(t *testing.T) {
	other := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	engine := &adapterTestEngine{responseDigest: other}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	result, err := adapter.Run(t.Context(), request, nil)
	if err == nil || result.Outcome.SpawnError == nil || result.Outcome.SpawnError.Code != contract.SpawnFailureImageManifestInvalid {
		t.Fatalf("digest mismatch outcome = (%+v, %v)", result.Outcome, err)
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

func TestAdapterConsumesMatchingPriorBootSweepEvidenceOnce(t *testing.T) {
	source := &adapterReceiptSource{receipt: ocihelper.VerifiedSweepReceipt{
		SweepEpoch: "sweep-1", HelperSession: ocihelper.HelperSession{HelperInstanceID: "helper-1", SessionGeneration: 7},
		Attempts: []ocihelper.SweptAttemptAuthority{{NodeID: "node", JobID: "job", AttemptID: "attempt", FencingToken: "fence", PriorBootSessionID: "boot-old", Class: contract.JobClassService, RemovalGeneration: "remove-1"}},
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

func TestAdapterMapsLogsExitSignalOOMAndRuntimeLoss(t *testing.T) {
	tests := []struct {
		name  string
		watch ocihelper.WatchResponse
		check func(*testing.T, contract.ProcessResult)
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
		{name: "runtime-loss", watch: ocihelper.WatchResponse{RuntimeFailure: "shim connection lost"}, check: func(t *testing.T, result contract.ProcessResult) {
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
		})
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

func TestWorkloadInputMakesWeftyBridgeAndTokenHelperAuthoritative(t *testing.T) {
	request := adapterTestRequest()
	request.Execution.Env = map[string]string{
		"PUBLIC": "value", contract.EnvL3Endpoint: "http://host.lima.internal:43100/l3",
	}
	request.Execution.SensitiveEnv = map[string]string{
		contract.EnvRunToken: "secret-token", contract.EnvL3Endpoint: "http://sensitive-precedence/l3", "OPERATOR_SECRET": "secret-value",
	}
	input := workloadInput(request)
	if len(input.Environment) != 1 || input.Environment[0].Name != "PUBLIC" ||
		len(input.SensitiveEnvironment) != 1 || input.SensitiveEnvironment[0].Name != "OPERATOR_SECRET" {
		t.Fatalf("operator environment was not separated: public=%+v sensitive=%+v", input.Environment, input.SensitiveEnvironment)
	}
	reserved := make(map[string]string)
	for _, variable := range input.ReservedEnvironment {
		reserved[variable.Name] = variable.Value
	}
	if reserved[contract.EnvL3Endpoint] != "http://sensitive-precedence/l3" || reserved[contract.EnvRunToken] != "secret-token" || len(reserved) != 2 {
		t.Fatalf("reserved environment = %+v", reserved)
	}
}

func TestPortfulRunTransfersExactAuthorityEndpoint(t *testing.T) {
	engine := &adapterTestEngine{watch: ocihelper.WatchResponse{ExitCode: intPointer(0)}}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	request := adapterTestRequest()
	request.Authority.WorkloadClass = contract.JobClassService
	request.AttemptPortRequired = true
	var endpoint workloadrunner.AttemptEndpoint
	request.AttemptEndpointReady = func(value workloadrunner.AttemptEndpoint) error { endpoint = value; return nil }
	request.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error { return nil }
	result, err := adapter.Run(t.Context(), request, nil)
	if err != nil || result.Outcome.ExitCode == nil {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if endpoint.Port != 42424 || endpoint.Dial == nil {
		t.Fatalf("endpoint = %+v", endpoint)
	}
	engine.mu.Lock()
	allocated := engine.lastRun.AllocateAttemptPort
	engine.mu.Unlock()
	if !allocated {
		t.Fatal("adapter did not request helper attempt-port authority")
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
	result, err := adapter.Run(t.Context(), adapterTestRequest(), nil)
	if err == nil || result.Outcome.SpawnError == nil || result.Outcome.SpawnError.Code != contract.SpawnFailureImageUnavailable {
		t.Fatalf("budget outcome=%+v err=%v", result.Outcome, err)
	}
	if engine.ensureCalls != 1 {
		t.Fatalf("budget exhaustion EnsureImage calls = %d, want 1", engine.ensureCalls)
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
	go func() {
		result, err := adapter.Run(t.Context(), adapterTestRequest(), nil)
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

type adapterTestEngine struct {
	mu               sync.Mutex
	watch            ocihelper.WatchResponse
	deletes          int
	refuseDelete     bool
	runErr           error
	ensureErrors     []error
	ensureCalls      int
	responseDigest   string
	responsePlatform ocihelper.OCIPlatform
	ensureEntered    chan struct{}
	releaseEnsure    chan struct{}
	lastRun          ocihelper.RunRequest
	lastEnsure       ocihelper.EnsureImageRequest
	bridgeExchange   chan error
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
	response := ocihelper.RunResponse{Started: true, Image: &ocihelper.ImageEvidence{
		SubmittedReference: "example.invalid/image", TopLevelDigest: adapterTestDigest, TopLevelMediaType: "application/vnd.oci.image.manifest.v1+json",
		PlatformManifestDigest: adapterTestDigest, Platform: ocihelper.OCIPlatform{OS: "linux", Architecture: "amd64"},
		RuntimeHandler: ocihelper.DefaultRuntimeHandler, Snapshotter: ocihelper.DefaultSnapshotter,
	}}
	if request.EnableHostBridgeFallback {
		response.HostBridgeReady = true
	}
	if request.AllocateAttemptPort {
		response.AttemptPort = 42424
	}
	return response, nil
}
func (*adapterTestEngine) Signal(context.Context, ocihelper.SignalRequest) error { return nil }
func (engine *adapterTestEngine) Watch(_ context.Context, _ ocihelper.WatchRequest, emit func(ocihelper.WatchEvent) error) error {
	if engine.bridgeExchange != nil {
		if err := <-engine.bridgeExchange; err != nil {
			return err
		}
	}
	if err := emit(ocihelper.WatchEvent{Kind: ocihelper.WatchProgress, Log: &ocihelper.LogFrame{Stream: "stdout", Sequence: 0, Bytes: []byte("frame"), Checksum: "9dff50df08c635815f4b19da10f756605a34a79a48d4ba48712782502975a70e"}}); err != nil {
		return err
	}
	return emit(ocihelper.WatchEvent{Kind: ocihelper.WatchComplete, Result: &engine.watch})
}
func (engine *adapterTestEngine) Delete(context.Context, ocihelper.DeleteRequest) (ocihelper.DeleteResponse, error) {
	engine.mu.Lock()
	engine.deletes++
	engine.mu.Unlock()
	return ocihelper.DeleteResponse{Deleted: !engine.refuseDelete}, nil
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
	adapter := NewAdapterWithPolicy(barrier, policy)
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	adapter.probePlatforms[helperSession(session)] = ocihelper.OCIPlatform{OS: "linux", Architecture: "amd64"}
	return adapter, func() { _ = barrier.Close(); cancel(); _ = listener.Close(); <-done }
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
	return ocihelper.ResourceInventory{Leases: []string{}, Snapshots: []string{}, Containers: []string{}, Tasks: []string{}, Shims: []string{}, Cgroups: []string{}, LogSegments: []string{}, ManagedVolumes: []string{}}
}

func intPointer(value int) *int { return &value }

type adapterReceiptSource struct {
	receipt ocihelper.VerifiedSweepReceipt
}

func (*adapterReceiptSource) Session() (*ocihelper.Session, error) { return nil, errors.New("unused") }
func (source *adapterReceiptSource) SweepReceipt() (ocihelper.VerifiedSweepReceipt, bool) {
	return source.receipt, true
}
