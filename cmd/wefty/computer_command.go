package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

const (
	defaultComputerMemoryBytes int64 = 1 << 30
	defaultComputerDiskBytes   int64 = 8 << 30
)

type computerEvidenceCell struct {
	Status                 string `json:"status"`
	Code                   string `json:"code,omitempty"`
	Reason                 string `json:"reason,omitempty"`
	RequestedBytes         *int64 `json:"requested_bytes,omitempty"`
	ObservedAvailableBytes *int64 `json:"observed_available_bytes,omitempty"`
	FailureCode            string `json:"failure_code,omitempty"`
}

type computerCapacityProjection struct {
	RequestedMemoryBytes *int64               `json:"requested_memory_bytes,omitempty"`
	RequestedDiskBytes   int64                `json:"requested_disk_bytes"`
	LastGrow             computerEvidenceCell `json:"last_grow"`
	ActiveFailure        computerEvidenceCell `json:"active_failure"`
}

type computerOperatorProjection struct {
	l1.Computer
	Capacity         computerCapacityProjection `json:"capacity"`
	Observation      *storageWaitObservation    `json:"observation,omitempty"`
	MutationApplied  *bool                      `json:"mutation_applied,omitempty"`
	IdempotentReplay *bool                      `json:"idempotent_replay,omitempty"`
}

func newComputerProjection(computer l1.Computer, mutationApplied, replay *bool) computerOperatorProjection {
	var memoryBytes *int64
	if oci := computer.CurrentJob.Spec.Execution.OCI; oci != nil && oci.Limits != nil && oci.Limits.MemoryBytes != nil {
		value := *oci.Limits.MemoryBytes
		memoryBytes = &value
	}
	lastGrow := computerEvidenceCell{Status: "NOT-RUN", Code: "grow_receipt_absent",
		Reason: "no completed Computer Storage grow receipt is available"}
	if grow := computer.LastGrowOperation; grow != nil {
		switch grow.Status {
		case "applied":
			requested := grow.RequestedBytes
			lastGrow = computerEvidenceCell{Status: "PASS", Code: "computer_storage_grow_applied", RequestedBytes: &requested}
		case "failed":
			requested := grow.RequestedBytes
			lastGrow = computerEvidenceCell{Status: "FAIL", Code: grow.FailureCode, FailureCode: grow.FailureCode,
				RequestedBytes: &requested, ObservedAvailableBytes: grow.ObservedAvailableBytes}
		case "planned":
			lastGrow = computerEvidenceCell{Status: "NOT-RUN", Code: "grow_pending",
				Reason: "the latest Computer Storage grow has no terminal helper receipt"}
		case "superseded":
			lastGrow = computerEvidenceCell{Status: "NOT-RUN", Code: "grow_superseded",
				Reason: "the latest Computer Storage grow was superseded without a terminal helper receipt"}
		}
	}
	activeFailure := computerEvidenceCell{Status: "NOT-RUN", Code: "capacity_failure_absent",
		Reason: "the current Computer Job has no active capacity failure latch"}
	if computer.CurrentJob.ServiceJob != nil && len(computer.CurrentJob.LastFailure) != 0 {
		var failure contract.SpawnFailure
		if err := json.Unmarshal(computer.CurrentJob.LastFailure, &failure); err != nil {
			activeFailure = computerEvidenceCell{Status: "NOT-RUN", Code: "capacity_failure_unreadable",
				Reason: "the current Computer Job failure is not a typed spawn failure"}
		} else if failure.Code == contract.SpawnFailureInsufficientMemory || failure.Code == contract.SpawnFailureInsufficientDisk {
			requested, observed := failure.RequestedBytes, failure.ObservedAvailableBytes
			activeFailure = computerEvidenceCell{Status: "FAIL", Code: string(failure.Code), FailureCode: string(failure.Code),
				RequestedBytes: &requested, ObservedAvailableBytes: &observed}
		}
	}
	return computerOperatorProjection{
		Computer: computer,
		Capacity: computerCapacityProjection{
			RequestedMemoryBytes: memoryBytes,
			RequestedDiskBytes:   computer.DesiredDiskBytes,
			LastGrow:             lastGrow,
			ActiveFailure:        activeFailure,
		},
		MutationApplied: mutationApplied, IdempotentReplay: replay,
	}
}

func executeComputerCreate(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("services create", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var name, idempotencyKey string
	var backupCap int64
	var diskBytes optionalInt64Flag
	var imageFlags imageFlagSet
	flags.StringVar(&name, "name", "", "durable Computer name")
	flags.StringVar(&idempotencyKey, "idempotency-key", "", "stable Computer creation key")
	flags.Int64Var(&backupCap, "backup-cap", 0, "maximum retained Backups (shipped default 0)")
	flags.Var(&diskBytes, "disk-bytes", "fully allocated Computer disk budget")
	imageFlags.bind(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || strings.TrimSpace(name) == "" || strings.TrimSpace(imageFlags.reference) == "" || strings.TrimSpace(imageFlags.nodeID) == "" || backupCap < 0 {
		return usageError("usage: wefty services create --computer --name NAME --image IMAGE --node NODE_ID [--argv ARG] [--working-directory PATH] [--mount NODE_PATH:CONTAINER_PATH[:ro]] [--memory-bytes BYTES] [--cpu-millicores VALUE] [--runtime-handler NAME] [--disk-bytes BYTES] [--backup-cap COUNT] [--idempotency-key KEY]")
	}
	if !imageFlags.memoryBytes.set {
		imageFlags.memoryBytes = optionalInt64Flag{value: defaultComputerMemoryBytes, set: true}
	}
	if !diskBytes.set {
		diskBytes = optionalInt64Flag{value: defaultComputerDiskBytes, set: true}
	}
	// Resolve the mutable reference before applying the service-class digest
	// requirement. This is the same immutable-image boundary as services create.
	program, tags, err := imageFlags.programAndTags(nil, contract.JobClassOneShot)
	if err != nil {
		return err
	}
	if program == nil {
		return usageError("services create requires --image")
	}
	if program.Digest == nil {
		resolver := clients.images
		if resolver == nil {
			resolver = newRegistryResolver(nil)
		}
		digest, err := resolver.ResolveDigest(ctx, program.Reference)
		if err != nil {
			return fmt.Errorf("resolve Computer image: %w", err)
		}
		program.Digest = &digest
	}
	if err := contract.ValidateImageProgram(*program, contract.JobClassService); err != nil {
		return usageError(fmt.Sprintf("invalid Computer image program: %v", err))
	}
	if idempotencyKey == "" {
		digest := sha256.Sum256([]byte(strings.TrimSpace(name)))
		idempotencyKey = "computer-" + hex.EncodeToString(digest[:])
	}
	idempotencyKey, err = validateIdempotencyKey(idempotencyKey)
	if err != nil {
		return err
	}
	spec := contract.JobSpec{
		SchemaVersion:  contract.SchemaVersionV1,
		DispatchKey:    idempotencyKey,
		Kind:           contract.JobKindOCI,
		Class:          contract.JobClassService,
		Restart:        contract.RestartAlways,
		RoutingTags:    tags,
		RuntimeHandler: program.RuntimeHandler,
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image: contract.OCIImageSpec{Reference: program.Reference, Digest: program.Digest},
			Argv:  append([]string(nil), program.Argv...), WorkingDirectory: program.WorkingDirectory,
			Mounts: append([]contract.OCIMount(nil), program.Mounts...), Limits: program.Limits,
			Computer: &contract.OCIComputerSpec{Display: contract.OCIComputerDisplaySpec{
				Protocol: contract.ComputerDisplayProtocolRFBWebSocketV1,
			}, DiskBytes: diskBytes.value},
		}},
	}
	computer, receipt, err := clients.createComputer(ctx, l1.CreateComputerRequest{
		Name: strings.TrimSpace(name), Spec: spec, BackupCap: &backupCap,
	})
	if err != nil {
		return err
	}
	return writeComputerMutation(stdout, computer, receipt, jsonOutput)
}

type computerMutationFlags struct {
	intentRevision    optionalPositiveRevision
	storageGeneration optionalPositiveRevision
	storageID         string
	expectCurrent     bool
	idempotencyKey    string
}

type optionalPositiveRevision struct {
	value int64
	set   bool
}

func (revision *optionalPositiveRevision) Set(value string) error {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 1 {
		return fmt.Errorf("must be a positive integer")
	}
	revision.value, revision.set = parsed, true
	return nil
}

func (revision *optionalPositiveRevision) String() string {
	if !revision.set {
		return ""
	}
	return strconv.FormatInt(revision.value, 10)
}

func (mutation *computerMutationFlags) bind(flags *flag.FlagSet, keyed bool) {
	flags.Var(&mutation.intentRevision, "intent-revision", "observed Computer intent revision")
	flags.StringVar(&mutation.storageID, "storage-id", "", "observed durable Storage ID")
	flags.Var(&mutation.storageGeneration, "storage-generation", "observed Storage generation")
	flags.BoolVar(&mutation.expectCurrent, "expect-current", false, "opt in to reading the current CAS tuple immediately before mutation")
	if keyed {
		flags.StringVar(&mutation.idempotencyKey, "idempotency-key", "", "stable mutation replay key")
	}
}

func (mutation computerMutationFlags) resolve(ctx context.Context, clients *apiClients, computerID string) (l1.ComputerMutationPrecondition, error) {
	if err := mutation.validate(false); err != nil {
		return l1.ComputerMutationPrecondition{}, err
	}
	if mutation.expectCurrent {
		computer, err := clients.getComputer(ctx, computerID)
		if err != nil {
			return l1.ComputerMutationPrecondition{}, err
		}
		return l1.ComputerMutationPrecondition{IntentRevision: computer.IntentRevision,
			StorageID: computer.StorageID, StorageGeneration: computer.StorageGeneration}, nil
	}
	return l1.ComputerMutationPrecondition{IntentRevision: mutation.intentRevision.value,
		StorageID: strings.TrimSpace(mutation.storageID), StorageGeneration: mutation.storageGeneration.value}, nil
}

func (mutation computerMutationFlags) validate(keyed bool) error {
	explicit := mutation.intentRevision.set || strings.TrimSpace(mutation.storageID) != "" || mutation.storageGeneration.set
	if mutation.expectCurrent && explicit {
		return usageError("--expect-current cannot be combined with explicit CAS flags")
	}
	if !mutation.expectCurrent && (!mutation.intentRevision.set || strings.TrimSpace(mutation.storageID) == "" || !mutation.storageGeneration.set) {
		return usageError("mutation requires --intent-revision, --storage-id, and --storage-generation, or explicit --expect-current")
	}
	if keyed {
		_, err := mutation.key()
		return err
	}
	return nil
}

func (mutation computerMutationFlags) key() (string, error) {
	if strings.TrimSpace(mutation.idempotencyKey) == "" {
		return "", usageError("mutation requires --idempotency-key so retries cannot apply twice")
	}
	return validateIdempotencyKey(mutation.idempotencyKey)
}

func resolveComputerID(ctx context.Context, clients *apiClients, target string) (string, error) {
	_, computer, err := resolveServiceTarget(ctx, clients, target)
	if err != nil {
		return "", err
	}
	if computer == nil {
		return "", usageError(fmt.Sprintf("service %q is not Computer-owned", target))
	}
	return computer.ComputerID, nil
}

func writeComputerMutation(stdout io.Writer, computer l1.Computer, receipt mutationReceipt, jsonOutput bool) error {
	return writeComputerProjection(stdout, newComputerProjection(computer, &receipt.Applied, &receipt.Replay), jsonOutput)
}

func executeComputerReimage(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	args = moveFirstPositionalToEnd(args)
	flags := flag.NewFlagSet("services reimage", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var mutation computerMutationFlags
	var image string
	var chown, terminateSessions bool
	mutation.bind(flags, true)
	flags.StringVar(&image, "image", "", "new OCI image reference with optional @sha256 digest")
	flags.BoolVar(&chown, "chown", false, "authorize the crash-resumable ownership migration")
	flags.BoolVar(&terminateSessions, "terminate-sessions", false, "close live take-over sessions before reimage")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 || strings.TrimSpace(image) == "" {
		return usageError("usage: wefty services reimage COMPUTER_ID --image IMAGE --idempotency-key KEY [CAS flags | --expect-current]")
	}
	if err := mutation.validate(true); err != nil {
		return err
	}
	computerID, err := resolveComputerID(ctx, clients, flags.Arg(0))
	if err != nil {
		return err
	}
	precondition, err := mutation.resolve(ctx, clients, computerID)
	if err != nil {
		return err
	}
	key, err := mutation.key()
	if err != nil {
		return err
	}
	reference, digest, err := splitImageReference(image)
	if err != nil {
		return err
	}
	if digest == nil {
		resolver := clients.images
		if resolver == nil {
			resolver = newRegistryResolver(nil)
		}
		resolved, err := resolver.ResolveDigest(ctx, reference)
		if err != nil {
			return fmt.Errorf("resolve Computer reimage target: %w", err)
		}
		digest = &resolved
	}
	computer, receipt, err := clients.reimageComputer(ctx, computerID, l1.ComputerReimageRequest{
		ComputerMutationPrecondition: precondition,
		Image:                        contract.OCIImageSpec{Reference: reference, Digest: digest}, Chown: chown,
		TerminateSessions: terminateSessions, IdempotencyKey: key,
	})
	if err != nil {
		return err
	}
	return writeComputerMutation(stdout, computer, receipt, jsonOutput)
}

func executeComputerReset(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	args = moveFirstPositionalToEnd(args)
	flags := flag.NewFlagSet("services reset", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var mutation computerMutationFlags
	var terminateSessions bool
	mutation.bind(flags, true)
	flags.BoolVar(&terminateSessions, "terminate-sessions", false, "close live take-over sessions before reset")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return usageError("usage: wefty services reset COMPUTER_ID --idempotency-key KEY [CAS flags | --expect-current]")
	}
	if err := mutation.validate(true); err != nil {
		return err
	}
	computerID, err := resolveComputerID(ctx, clients, flags.Arg(0))
	if err != nil {
		return err
	}
	precondition, err := mutation.resolve(ctx, clients, computerID)
	if err != nil {
		return err
	}
	key, err := mutation.key()
	if err != nil {
		return err
	}
	computer, receipt, err := clients.resetComputer(ctx, computerID, l1.ComputerStorageResetRequest{
		ComputerMutationPrecondition: precondition, IdempotencyKey: key, TerminateSessions: terminateSessions,
	})
	if err != nil {
		return err
	}
	return writeComputerMutation(stdout, computer, receipt, jsonOutput)
}

func executeComputerResize(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	args = moveFirstPositionalToEnd(args)
	flags := flag.NewFlagSet("services resize", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var mutation computerMutationFlags
	var diskBytes optionalInt64Flag
	var wait storageWaitFlags
	mutation.bind(flags, true)
	wait.bind(flags)
	flags.Var(&diskBytes, "disk-bytes", "new fully allocated disk budget")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 || !diskBytes.set {
		return usageError("usage: wefty services resize COMPUTER_ID --disk-bytes BYTES --idempotency-key KEY [CAS flags | --expect-current] [--wait DURATION]")
	}
	if err := wait.validate(flags); err != nil {
		return err
	}
	if err := mutation.validate(true); err != nil {
		return err
	}
	computerID, err := resolveComputerID(ctx, clients, flags.Arg(0))
	if err != nil {
		return err
	}
	precondition, err := mutation.resolve(ctx, clients, computerID)
	if err != nil {
		return err
	}
	key, err := mutation.key()
	if err != nil {
		return err
	}
	computer, receipt, err := clients.growComputer(ctx, computerID, l1.ComputerGrowRequest{
		ComputerMutationPrecondition: precondition, DiskBytes: diskBytes.value, IdempotencyKey: key,
	})
	if err != nil {
		return err
	}
	if wait.timeout > 0 {
		operationRevision := computer.IntentRevision
		if receipt.Replay {
			if computer.LastGrowOperation == nil {
				projection := newComputerProjection(computer, &receipt.Applied, &receipt.Replay)
				return writeComputerProjectionThenError(stdout, projection, jsonOutput,
					computerGrowOutcomeMismatch(operationRevision, "idempotent replay omitted its durable grow operation"))
			}
			operationRevision = computer.LastGrowOperation.OperationRevision
		}
		projection := newComputerProjection(computer, &receipt.Applied, &receipt.Replay)
		if receipt.Replay && operationRevision != computer.IntentRevision {
			now := time.Now().UTC().Format(time.RFC3339Nano)
			projection.Observation = &storageWaitObservation{Status: "observed", StartedAt: now, EndedAt: now}
			if failureErr := awaitedComputerGrowFailure(computer, operationRevision, true); failureErr != nil {
				return writeComputerProjectionThenError(stdout, projection, jsonOutput, failureErr)
			}
			return writeComputerProjection(stdout, projection, jsonOutput)
		}
		observed, observation, waitErr := waitForComputerGrowRevision(ctx, clients, computerID, operationRevision, wait)
		if waitErr != nil && observed.ComputerID == "" {
			return waitErr
		}
		projection = newComputerProjection(observed, &receipt.Applied, &receipt.Replay)
		projection.Observation = &observation
		if waitErr != nil {
			return writeComputerProjectionThenError(stdout, projection, jsonOutput, waitErr)
		}
		computer = observed
		if failureErr := awaitedComputerGrowFailure(computer, operationRevision, false); failureErr != nil {
			return writeComputerProjectionThenError(stdout, projection, jsonOutput, failureErr)
		}
		return writeComputerProjection(stdout, projection, jsonOutput)
	}
	return writeComputerMutation(stdout, computer, receipt, jsonOutput)
}

func waitForComputerGrowRevision(ctx context.Context, clients *apiClients, computerID string, operationRevision int64, wait storageWaitFlags) (l1.Computer, storageWaitObservation, error) {
	var observed l1.Computer
	observation, err := pollStorageObservation(ctx, wait, func() (bool, error) {
		var readErr error
		observed, readErr = clients.getComputerStorageAuthority(ctx, computerID)
		if readErr != nil {
			return false, readErr
		}
		if observed.AppliedRevision > operationRevision {
			return false, fmt.Errorf("Computer grow revision %d was superseded by applied revision %d", operationRevision, observed.AppliedRevision)
		}
		return observed.AppliedRevision == operationRevision && observed.ReconfigurationPhase == l1.ComputerReconfigurationStable, nil
	})
	return observed, observation, err
}

func waitForComputerRemoval(ctx context.Context, clients *apiClients, computerID string, wait storageWaitFlags) (l1.Computer, storageWaitObservation, error) {
	var observed l1.Computer
	observation, err := pollStorageObservation(ctx, wait, func() (bool, error) {
		var readErr error
		observed, readErr = clients.getComputerStorageAuthority(ctx, computerID)
		if readErr != nil {
			return false, readErr
		}
		return computerRemovalTerminal(observed) || computerRemovalQuarantine(observed) != nil, nil
	})
	return observed, observation, err
}

func computerRemovalTerminal(computer l1.Computer) bool {
	job := computer.CurrentJob
	if job.State != contract.JobRemovedVerified ||
		(computer.RemovalOutcome != "removed_verified" && computer.RemovalOutcome != "removed_reduced") {
		return false
	}
	// A never-bound Computer cannot have created node-owned resources, so L1
	// finalizes it without creating a removal directive or cleanup receipt.
	if job.Removal == nil {
		return computer.BoundNodeID == "" && (job.ServiceJob == nil || job.ServiceJob.BoundNodeID == "")
	}
	return job.Removal.RemovalBoundNodeID == "" || job.Removal.CleanupAcknowledgedAt != nil
}

func computerRemovalQuarantine(computer l1.Computer) *l1.ComputerStorageCleanupQuarantine {
	removal := computer.CurrentJob.Removal
	if removal == nil || computer.DesiredState != contract.ServiceDesiredRemoved ||
		computer.CurrentJob.State != contract.JobRemovalPending ||
		removal.CleanupStatus != l1.ServiceRemovalCleanupQuarantined ||
		removal.RemovalOutcome != l1.ServiceRemovalOutcomeCleanupQuarantined {
		return nil
	}
	for index := range computer.StorageCleanupQuarantines {
		quarantine := &computer.StorageCleanupQuarantines[index]
		if quarantine.Operation == l1.ComputerStorageCleanupRemoval && quarantine.JobID == computer.CurrentJobID &&
			quarantine.RemovalGeneration == removal.RemovalGeneration {
			return quarantine
		}
	}
	return nil
}

func awaitedComputerRemovalOutcome(computer l1.Computer) error {
	if computerRemovalTerminal(computer) {
		return nil
	}
	if quarantine := computerRemovalQuarantine(computer); quarantine != nil {
		if quarantine.Kind != "managed_volume_cleanup_quarantined" || quarantine.ReceiptID == "" ||
			quarantine.VolumeKind != "computer_disk" || quarantine.FailureReason != "operation_failed" || quarantine.Attempts != 3 {
			return computerRemovalOutcomeMismatch(computer, "Computer removal cleanup quarantine lacks complete receipt-derived facts")
		}
		return &apiResponseError{Service: "L1", StatusCode: 409, APIError: contract.APIError{
			Code: contract.ErrorConflict, Message: "Computer removal cleanup quarantined: operation_failed", Retryable: false,
			Details: map[string]any{"removal_computer_id": computer.ComputerID, "computer_id": quarantine.ComputerID,
				"job_id":     computer.CurrentJobID,
				"receipt_id": quarantine.ReceiptID, "storage_id": quarantine.StorageID,
				"storage_generation": quarantine.StorageGeneration, "failure_reason": quarantine.FailureReason,
				"attempts": quarantine.Attempts},
		}}
	}
	return computerRemovalOutcomeMismatch(computer, "awaited Computer removal lacks receipt-derived terminal Slot release")
}

func computerRemovalOutcomeMismatch(computer l1.Computer, message string) error {
	details := map[string]any{"computer_id": computer.ComputerID, "job_id": computer.CurrentJobID,
		"job_state": computer.CurrentJob.State, "removal_outcome": computer.RemovalOutcome}
	details["holds_slot"] = computer.CurrentJob.State == contract.JobRemovalPending ||
		computer.CurrentJob.State == contract.JobAgentCleaned
	return &apiResponseError{Service: "L1", StatusCode: 409, APIError: contract.APIError{
		Code: contract.ErrorConflict, Message: message, Retryable: false, Details: details,
	}}
}

func awaitedComputerGrowFailure(computer l1.Computer, operationRevision int64, historicalReplay bool) error {
	grow := computer.LastGrowOperation
	if grow == nil || grow.OperationRevision != operationRevision {
		return computerGrowOutcomeMismatch(operationRevision, "Computer projection omitted the awaited grow operation")
	}
	if grow.Status == "applied" {
		return nil
	}
	if grow.Status != "failed" {
		return computerGrowOutcomeMismatch(operationRevision, "awaited Computer grow has no receipt-derived terminal outcome")
	}
	var failure contract.SpawnFailure
	if grow.FailureCode != string(contract.SpawnFailureInsufficientDisk) ||
		grow.ObservedAvailableBytes == nil {
		return computerGrowOutcomeMismatch(operationRevision, "awaited Computer grow failure lacks receipt-derived capacity facts")
	}
	if !historicalReplay && (json.Unmarshal(computer.CurrentJob.LastFailure, &failure) != nil ||
		failure.Code != contract.SpawnFailureInsufficientDisk || failure.RequestedBytes != grow.RequestedBytes ||
		failure.ObservedAvailableBytes != *grow.ObservedAvailableBytes) {
		return computerGrowOutcomeMismatch(operationRevision, "awaited Computer grow failure does not match the active typed latch")
	}
	return &apiResponseError{Service: "L1", StatusCode: 409, APIError: contract.APIError{
		Code: contract.ErrorCapacityExhausted, Message: "Computer grow failed: insufficient_disk", Retryable: false,
		Details: map[string]any{"failure_code": grow.FailureCode, "requested_bytes": grow.RequestedBytes,
			"observed_available_bytes": *grow.ObservedAvailableBytes},
	}}
}

func computerGrowOutcomeMismatch(operationRevision int64, message string) error {
	return &apiResponseError{Service: "L1", StatusCode: 409, APIError: contract.APIError{
		Code: contract.ErrorConflict, Message: message, Retryable: false,
		Details: map[string]any{"operation_revision": operationRevision},
	}}
}

func writeComputerProjectionThenError(writer io.Writer, projection computerOperatorProjection, jsonOutput bool, err error) error {
	if writeErr := writeComputerProjection(writer, projection, jsonOutput); writeErr != nil {
		return errors.Join(err, writeErr)
	}
	return err
}

func executeComputerAbort(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	args = moveFirstPositionalToEnd(args)
	flags := flag.NewFlagSet("services abort", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var mutation computerMutationFlags
	mutation.bind(flags, true)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return usageError("usage: wefty services abort COMPUTER_ID --idempotency-key KEY [CAS flags | --expect-current]")
	}
	if err := mutation.validate(true); err != nil {
		return err
	}
	computerID, err := resolveComputerID(ctx, clients, flags.Arg(0))
	if err != nil {
		return err
	}
	precondition, err := mutation.resolve(ctx, clients, computerID)
	if err != nil {
		return err
	}
	key, err := mutation.key()
	if err != nil {
		return err
	}
	computer, receipt, err := clients.abortComputer(ctx, computerID, l1.ComputerReconfigurationAbortRequest{
		ComputerMutationPrecondition: precondition, IdempotencyKey: key,
	})
	if err != nil {
		return err
	}
	return writeComputerMutation(stdout, computer, receipt, jsonOutput)
}

func writeComputerProjection(writer io.Writer, computer computerOperatorProjection, jsonOutput bool) error {
	if jsonOutput {
		return writeJSON(writer, computer)
	}
	return writeComputersTable(writer, []computerOperatorProjection{computer})
}

func writeComputersTable(writer io.Writer, computers []computerOperatorProjection) error {
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "COMPUTER ID\tNAME\tDESIRED\tOBSERVED\tSTORAGE\tINTENT/APPLIED\tPHASE\tJOB ID\tATTEMPT\tNODE\tMEMORY\tDISK\tBACKUP CAP\tLAST GROW\tACTIVE CAPACITY FAILURE\tREADY\tDISPLAY ENDPOINT\tCONTROLLER TENURE\tLAST FAILURE\tREMOVAL\tMUTATION APPLIED\tIDEMPOTENT REPLAY"); err != nil {
		return err
	}
	for _, computer := range computers {
		job := computer.CurrentJob
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s@%d\t%d/%d\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			computer.ComputerID, computer.Name, computer.DesiredState, job.State,
			computer.StorageID, computer.StorageGeneration, computer.IntentRevision, computer.AppliedRevision,
			computer.ReconfigurationPhase, computer.CurrentJobID, valueOrNA(job.CurrentAttemptID),
			valueOrNA(computer.BoundNodeID), int64PointerOrNA(computer.Capacity.RequestedMemoryBytes), computer.Capacity.RequestedDiskBytes, computer.BackupCap,
			computer.Capacity.LastGrow.Status+"("+computer.Capacity.LastGrow.Code+")",
			computer.Capacity.ActiveFailure.Status+"("+computer.Capacity.ActiveFailure.Code+")",
			boolOrNA(job.Ready), pointerOrNA(computer.DisplayEndpoint, ""),
			computer.ControllerTenure,
			jsonOrNA(job.LastFailure), valueOrNA(computer.RemovalOutcome), boolPointerOrNA(computer.MutationApplied),
			boolPointerOrNA(computer.IdempotentReplay)); err != nil {
			return err
		}
	}
	return table.Flush()
}

func int64PointerOrNA(value *int64) string {
	if value == nil {
		return "N/A"
	}
	return strconv.FormatInt(*value, 10)
}

func boolPointerOrNA(value *bool) string {
	if value == nil {
		return "N/A"
	}
	return strconv.FormatBool(*value)
}
