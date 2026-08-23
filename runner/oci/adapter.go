// Package oci implements the agent-side kind=oci workload adapter. Containerd
// remains entirely behind the privileged ocihelper session.
package oci

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// SessionSource returns the currently boot-barrier-pinned helper session.
type SessionSource interface {
	Session() (*ocihelper.Session, error)
}

type Adapter struct {
	sessions              SessionSource
	mu                    sync.Mutex
	consumedSweepEvidence map[string]struct{}
}

func NewAdapter(sessions SessionSource) *Adapter {
	return &Adapter{sessions: sessions, consumedSweepEvidence: make(map[string]struct{})}
}

type sweepReceiptSource interface {
	SweepReceipt() (ocihelper.VerifiedSweepReceipt, bool)
}

// Probe exercises the same pinned local image, runc-v2 task, Wait-before-Start,
// Watch, and verified Delete path used by production attempts.
func (adapter *Adapter) Probe(ctx context.Context, nodeID, bootSessionID, reference, digest string, deadman time.Duration) error {
	if adapter == nil || adapter.sessions == nil {
		return errors.New("OCI helper session is not configured")
	}
	if reference == "" || digest == "" {
		return errors.New("OCI functional probe requires a pinned local image reference and digest")
	}
	session, err := adapter.sessions.Session()
	if err != nil {
		return err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return fmt.Errorf("generate OCI probe identity: %w", err)
	}
	id := hex.EncodeToString(nonce[:])
	authority := ocihelper.AttemptAuthority{
		NodeID: nodeID, BootSessionID: bootSessionID, JobID: "probe-" + id,
		AttemptID: "probe-" + id, FencingToken: "probe-" + id,
		Class: contract.JobClassOneShot, RemovalGeneration: "probe",
	}
	response, err := session.Run(ctx, ocihelper.RunRequest{
		Authority: authority, InitialDeadman: deadman,
		Workload: ocihelper.WorkloadInput{ImageReference: reference, ImageDigest: digest, Argv: []string{"/bin/true"}},
	})
	if err != nil {
		return err
	}
	cleanupNeeded := true
	defer func() {
		if !cleanupNeeded {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = session.Delete(cleanupCtx, ocihelper.DeleteRequest{Authority: authority})
	}()
	if !response.Started || response.Image == nil {
		return errors.New("OCI functional probe did not receive truthful Started evidence")
	}
	var completion *ocihelper.WatchResponse
	if err := session.Watch(ctx, ocihelper.WatchRequest{Authority: authority}, func(event ocihelper.WatchEvent) error {
		if event.Result != nil {
			copy := *event.Result
			completion = &copy
		}
		return nil
	}); err != nil {
		return err
	}
	if completion == nil || completion.ExitCode == nil || *completion.ExitCode != 0 || completion.Signal != "" || completion.RuntimeFailure != "" {
		return fmt.Errorf("OCI functional probe returned %+v", completion)
	}
	deleted, err := session.Delete(ctx, ocihelper.DeleteRequest{Authority: authority})
	if err != nil {
		return err
	}
	if !deleted.Deleted {
		return errors.New("OCI functional probe cleanup was not verified")
	}
	cleanupNeeded = false
	return nil
}

func (adapter *Adapter) Preflight(_ context.Context, request workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	admission := workloadrunner.Admission{Request: request, Release: func() {}}
	if adapter == nil || adapter.sessions == nil {
		return failedAdmission(admission, contract.SpawnFailureRuntimeUnavailable, errors.New("OCI helper session is not configured"))
	}
	if request.Authority.NodeID == "" || request.Authority.BootSessionID == "" || request.Authority.JobID == "" ||
		request.Authority.AttemptID == "" || request.Authority.FencingToken == "" || request.Authority.WorkloadClass == "" || request.Authority.RemovalGeneration == "" {
		return failedAdmission(admission, contract.SpawnFailureProcessRequest, errors.New("OCI runtime authority is incomplete"))
	}
	if request.Authority.WorkloadClass != contract.JobClassOneShot {
		return failedAdmission(admission, contract.SpawnFailureProcessRequest, errors.New("native OCI service lifecycle is not installed"))
	}
	if request.Execution.OCI == nil {
		return failedAdmission(admission, contract.SpawnFailureProcessRequest, errors.New("OCI execution arm is required"))
	}
	if request.Execution.OCI.Image.Digest == nil || *request.Execution.OCI.Image.Digest == "" {
		return failedAdmission(admission, contract.SpawnFailureImageUnavailable, errors.New("OCI lifecycle requires a locally resolved immutable image digest"))
	}
	if request.RuntimeHandler != "" && request.RuntimeHandler != ocihelper.DefaultRuntimeHandler {
		return failedAdmission(admission, contract.SpawnFailureUnsupportedRuntimeHandler, fmt.Errorf("OCI runtime handler %q is unavailable", request.RuntimeHandler))
	}
	if request.OCIStarted == nil {
		return failedAdmission(admission, contract.SpawnFailureProcessRequest, errors.New("OCI Started acknowledgement hook is required"))
	}
	if request.InitialDeadman <= 0 {
		return failedAdmission(admission, contract.SpawnFailureProcessRequest, errors.New("OCI initial deadman is required"))
	}
	return admission, workloadrunner.Result{}, nil
}

func failedAdmission(admission workloadrunner.Admission, code contract.SpawnFailureCode, err error) (workloadrunner.Admission, workloadrunner.Result, error) {
	return admission, workloadrunner.Result{Outcome: contract.ProcessResult{SpawnError: &contract.SpawnFailure{Code: code, Message: err.Error()}}}, err
}

func (adapter *Adapter) Run(ctx context.Context, request workloadrunner.Request, sink workloadrunner.OutputSink) (workloadrunner.Result, error) {
	session, err := adapter.sessions.Session()
	if err != nil {
		return spawnResult(contract.SpawnFailureRuntimeUnavailable, err), err
	}
	authority := HelperAuthority(request.Authority)
	runResponse, err := session.Run(ctx, ocihelper.RunRequest{
		Authority: authority, InitialDeadman: request.InitialDeadman,
		Workload: workloadInput(request),
	})
	if err != nil {
		if failure := ocihelper.SpawnFailureForRunError(err); failure != nil {
			return workloadrunner.Result{Outcome: contract.ProcessResult{SpawnError: failure}}, err
		}
		return spawnResult(contract.SpawnFailureRuntimeUnavailable, err), err
	}
	if runResponse.Image == nil {
		err := errors.New("OCI helper Started response omitted image evidence")
		_ = reapAfterFailedStart(session, authority)
		return spawnResult(contract.SpawnFailureRuntimeUnavailable, err), err
	}
	if err := request.OCIStarted(ctx, imageObservation(*runResponse.Image)); err != nil {
		_ = reapAfterFailedStart(session, authority)
		// A fencing/authority refusal is a terminal fact about this attempt, not
		// infrastructure loss eligible for the OCI pre-start retry budget.
		return spawnResult(contract.SpawnFailureProcessRequest, err), err
	}
	if request.Started != nil {
		request.Started()
	}

	var completion *ocihelper.WatchResponse
	err = session.Watch(ctx, ocihelper.WatchRequest{Authority: authority}, func(event ocihelper.WatchEvent) error {
		if event.Log != nil && sink != nil {
			logEvent := contract.LogEvent{
				AttemptID: request.Authority.AttemptID, Stream: contract.LogStream(event.Log.Stream),
				Sequence: event.Log.Sequence, Timestamp: time.Now().UTC(), Bytes: event.Log.Bytes,
			}
			if event.Log.Gap != nil {
				logEvent.Gap = &contract.LogGap{ThroughSequence: event.Log.Gap.ThroughSequence, LostEventCount: event.Log.Gap.LostEventCount, LostByteCount: event.Log.Gap.LostByteCount, Reason: contract.LogGapLoggerSourceIncomplete}
			}
			return sink.WriteOutput(ctx, logEvent)
		}
		if event.Result != nil {
			copy := *event.Result
			completion = &copy
		}
		return nil
	})
	if err != nil {
		return runtimeFailure(err), err
	}
	if completion == nil {
		err := errors.New("OCI helper Watch ended without a completion result")
		return runtimeFailure(err), err
	}
	return workloadrunner.Result{Outcome: processResult(*completion)}, nil
}

func (adapter *Adapter) ReapAndVerify(ctx context.Context, request workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	session, err := adapter.sessions.Session()
	if err != nil {
		return workloadrunner.ReapReceipt{}, err
	}
	authority := HelperAuthority(request.Authority)
	deleted, err := session.Delete(ctx, ocihelper.DeleteRequest{Authority: authority})
	if err != nil {
		return workloadrunner.ReapReceipt{}, err
	}
	if !deleted.Deleted {
		return workloadrunner.ReapReceipt{}, errors.New("OCI helper Delete did not positively verify attempt absence")
	}
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt, BootSessionID: request.Authority.BootSessionID}, nil
}

func (adapter *Adapter) ReapPriorBoot(_ context.Context, request workloadrunner.PriorBootReapRequest) (workloadrunner.ReapReceipt, error) {
	if request.NodeID == "" || request.JobID == "" || request.PriorBootSessionID == "" || request.CurrentBootSessionID == "" || request.PriorBootSessionID == request.CurrentBootSessionID {
		return workloadrunner.ReapReceipt{}, workloadrunner.ErrPriorBootEvidenceUnavailable
	}
	source, ok := adapter.sessions.(sweepReceiptSource)
	if !ok {
		return workloadrunner.ReapReceipt{}, workloadrunner.ErrPriorBootEvidenceUnavailable
	}
	receipt, ok := source.SweepReceipt()
	if !ok || receipt.SweepEpoch == "" || receipt.HelperSession.HelperInstanceID == "" || receipt.HelperSession.SessionGeneration == 0 {
		return workloadrunner.ReapReceipt{}, workloadrunner.ErrPriorBootEvidenceUnavailable
	}
	found := false
	for _, attempt := range receipt.Attempts {
		if attempt.NodeID == request.NodeID && attempt.JobID == request.JobID && attempt.PriorBootSessionID == request.PriorBootSessionID && attempt.Class == contract.JobClassService {
			found = true
			break
		}
	}
	if !found {
		return workloadrunner.ReapReceipt{}, workloadrunner.ErrPriorBootEvidenceUnavailable
	}
	key := fmt.Sprintf("%s\x00%s\x00%d\x00%s\x00%s", receipt.SweepEpoch, receipt.HelperSession.HelperInstanceID, receipt.HelperSession.SessionGeneration, request.JobID, request.PriorBootSessionID)
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	if _, consumed := adapter.consumedSweepEvidence[key]; consumed {
		return workloadrunner.ReapReceipt{}, workloadrunner.ErrPriorBootEvidenceUnavailable
	}
	adapter.consumedSweepEvidence[key] = struct{}{}
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidencePriorBootOCISweep, BootSessionID: request.PriorBootSessionID, SweepEpoch: receipt.SweepEpoch, HelperGeneration: receipt.HelperSession.SessionGeneration}, nil
}

// HelperAuthority maps the agent's complete fenced attempt tuple without
// allowing callers to omit class or removal-generation evidence.
func HelperAuthority(authority workloadrunner.AttemptAuthority) ocihelper.AttemptAuthority {
	return ocihelper.AttemptAuthority{
		NodeID: authority.NodeID, BootSessionID: authority.BootSessionID, JobID: authority.JobID,
		AttemptID: authority.AttemptID, FencingToken: authority.FencingToken,
		Class: authority.WorkloadClass, RemovalGeneration: authority.RemovalGeneration,
	}
}

func workloadInput(request workloadrunner.Request) ocihelper.WorkloadInput {
	execution := request.Execution.OCI
	digest := ""
	if execution.Image.Digest != nil {
		digest = *execution.Image.Digest
	}
	workingDirectory := ""
	if execution.WorkingDirectory != nil {
		workingDirectory = *execution.WorkingDirectory
	}
	input := ocihelper.WorkloadInput{
		ImageReference: execution.Image.Reference, ImageDigest: digest,
		Argv: append([]string(nil), execution.Argv...), WorkingDirectory: workingDirectory,
		Environment: environment(request.Execution.Env), SensitiveEnvironment: environment(request.Execution.SensitiveEnv),
	}
	for _, mount := range execution.Mounts {
		input.OperatorMounts = append(input.OperatorMounts, ocihelper.OperatorMount{NodePath: mount.NodePath, ContainerPath: mount.ContainerPath, ReadOnly: mount.ReadOnly})
	}
	if execution.Limits != nil {
		if execution.Limits.MemoryBytes != nil {
			input.Limits.MemoryBytes = *execution.Limits.MemoryBytes
		}
		if execution.Limits.CPUMillicores != nil {
			input.Limits.CPUMillicores = *execution.Limits.CPUMillicores
		}
	}
	return input
}

func environment(values map[string]string) []ocihelper.EnvironmentVariable {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]ocihelper.EnvironmentVariable, 0, len(names))
	for _, name := range names {
		result = append(result, ocihelper.EnvironmentVariable{Name: name, Value: values[name]})
	}
	return result
}

func imageObservation(evidence ocihelper.ImageEvidence) workloadrunner.OCIImageObservation {
	return workloadrunner.OCIImageObservation{
		SubmittedReference: evidence.SubmittedReference, TopLevelDigest: evidence.TopLevelDigest,
		TopLevelMediaType: evidence.TopLevelMediaType, IndexDigest: evidence.IndexDigest,
		PlatformManifestDigest: evidence.PlatformManifestDigest,
		PlatformOS:             evidence.Platform.OS, PlatformArchitecture: evidence.Platform.Architecture, PlatformVariant: evidence.Platform.Variant,
		RuntimeHandler: evidence.RuntimeHandler, Snapshotter: evidence.Snapshotter,
	}
}

func processResult(response ocihelper.WatchResponse) contract.ProcessResult {
	result := contract.ProcessResult{ExitCode: response.ExitCode, OOM: response.OutOfMemory, LogEvidenceIncomplete: response.LogEvidenceIncomplete}
	if response.Signal != "" {
		result.ExitCode = nil
		result.Signal = signalName(response.Signal)
		result.TerminationCause = contract.TerminationCause(response.TerminationCause)
	}
	if response.RuntimeFailure != "" {
		result.ExitCode = nil
		result.RuntimeFailure = &contract.RuntimeFailure{Code: contract.RuntimeFailureUnavailable, Message: response.RuntimeFailure}
	}
	return result
}

func signalName(signal ocihelper.Signal) string {
	if signal == ocihelper.SignalKILL {
		return "killed"
	}
	return "terminated"
}

func spawnResult(code contract.SpawnFailureCode, err error) workloadrunner.Result {
	return workloadrunner.Result{Outcome: contract.ProcessResult{SpawnError: &contract.SpawnFailure{Code: code, Message: err.Error()}}}
}

func runtimeFailure(err error) workloadrunner.Result {
	return workloadrunner.Result{Outcome: contract.ProcessResult{RuntimeFailure: &contract.RuntimeFailure{Code: contract.RuntimeFailureUnavailable, Message: err.Error()}}}
}

func reapAfterFailedStart(session *ocihelper.Session, authority ocihelper.AttemptAuthority) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = session.Signal(ctx, ocihelper.SignalRequest{Authority: authority, Signal: ocihelper.SignalKILL})
	_, err := session.Delete(ctx, ocihelper.DeleteRequest{Authority: authority})
	return err
}

var _ workloadrunner.WorkloadRuntime = (*Adapter)(nil)
var _ workloadrunner.PriorBootReaper = (*Adapter)(nil)
