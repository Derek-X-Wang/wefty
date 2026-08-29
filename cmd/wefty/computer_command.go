package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

const (
	defaultComputerMemoryBytes int64 = 1 << 30
	defaultComputerDiskBytes   int64 = 8 << 30
)

type computerEvidenceCell struct {
	Status string `json:"status"`
	Code   string `json:"code,omitempty"`
	Reason string `json:"reason,omitempty"`
}

type computerCapacityProjection struct {
	RequestedMemoryBytes *int64               `json:"requested_memory_bytes,omitempty"`
	RequestedDiskBytes   int64                `json:"requested_disk_bytes"`
	Admission            computerEvidenceCell `json:"admission"`
}

type computerOperatorProjection struct {
	l1.Computer
	Capacity         computerCapacityProjection `json:"capacity"`
	ControllerTenure computerEvidenceCell       `json:"controller_tenure"`
	MutationApplied  *bool                      `json:"mutation_applied,omitempty"`
	IdempotentReplay *bool                      `json:"idempotent_replay,omitempty"`
}

func newComputerProjection(computer l1.Computer, mutationApplied, replay *bool) computerOperatorProjection {
	var memoryBytes *int64
	if oci := computer.CurrentJob.Spec.Execution.OCI; oci != nil && oci.Limits != nil && oci.Limits.MemoryBytes != nil {
		value := *oci.Limits.MemoryBytes
		memoryBytes = &value
	}
	return computerOperatorProjection{
		Computer: computer,
		Capacity: computerCapacityProjection{
			RequestedMemoryBytes: memoryBytes,
			RequestedDiskBytes:   computer.DesiredDiskBytes,
			Admission: computerEvidenceCell{Status: "NOT-RUN", Code: "admission_receipt_not_projected",
				Reason: "the current L1 Computer route does not retain helper-local admission receipts"},
		},
		ControllerTenure: computerEvidenceCell{Status: "NOT-RUN", Code: "tenure_state_not_projected",
			Reason: "the lifecycle route exposes the display front door but no current tenure authority"},
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
		return usageError("usage: wefty services create --computer --name NAME --image IMAGE --node NODE_ID [--memory-bytes BYTES] [--disk-bytes BYTES] [--backup-cap COUNT] [--idempotency-key KEY]")
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
	mutation.bind(flags, true)
	flags.Var(&diskBytes, "disk-bytes", "new fully allocated disk budget")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 || !diskBytes.set {
		return usageError("usage: wefty services resize COMPUTER_ID --disk-bytes BYTES --idempotency-key KEY [CAS flags | --expect-current]")
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
	return writeComputerMutation(stdout, computer, receipt, jsonOutput)
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
	if _, err := fmt.Fprintln(table, "COMPUTER ID\tNAME\tDESIRED\tOBSERVED\tSTORAGE\tINTENT/APPLIED\tPHASE\tJOB ID\tATTEMPT\tNODE\tMEMORY\tDISK\tBACKUP CAP\tADMISSION\tREADY\tDISPLAY ENDPOINT\tCONTROLLER TENURE\tLAST FAILURE\tREMOVAL\tMUTATION APPLIED\tIDEMPOTENT REPLAY"); err != nil {
		return err
	}
	for _, computer := range computers {
		job := computer.CurrentJob
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s@%d\t%d/%d\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			computer.ComputerID, computer.Name, computer.DesiredState, job.State,
			computer.StorageID, computer.StorageGeneration, computer.IntentRevision, computer.AppliedRevision,
			computer.ReconfigurationPhase, computer.CurrentJobID, valueOrNA(job.CurrentAttemptID),
			valueOrNA(computer.BoundNodeID), int64PointerOrNA(computer.Capacity.RequestedMemoryBytes), computer.Capacity.RequestedDiskBytes, computer.BackupCap,
			computer.Capacity.Admission.Status+"("+computer.Capacity.Admission.Code+")", boolOrNA(job.Ready), pointerOrNA(computer.DisplayEndpoint, ""),
			computer.ControllerTenure.Status+"("+computer.ControllerTenure.Code+")",
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
