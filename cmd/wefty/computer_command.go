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
	computerTmpfsCeilingBytes  int64 = 1600 << 20
	computerUsage                    = "usage: wefty computers create|list|get|start|stop|restart|remove|reimage|reset|grow|abort"
)

type computerEvidenceCell struct {
	Status string `json:"status"`
	Code   string `json:"code,omitempty"`
	Reason string `json:"reason,omitempty"`
}

type computerCapacityProjection struct {
	RequestedMemoryBytes      int64                `json:"requested_memory_bytes"`
	RequestedDiskBytes        int64                `json:"requested_disk_bytes"`
	ComputerTmpfsCeilingBytes int64                `json:"computer_tmpfs_ceiling_bytes"`
	Admission                 computerEvidenceCell `json:"admission"`
}

type computerOperatorProjection struct {
	l1.Computer
	Capacity         computerCapacityProjection `json:"capacity"`
	ControllerTenure computerEvidenceCell       `json:"controller_tenure"`
	MutationApplied  *bool                      `json:"mutation_applied,omitempty"`
	IdempotentReplay *bool                      `json:"idempotent_replay,omitempty"`
}

type computerListProjection struct {
	Computers  []computerOperatorProjection `json:"computers"`
	NextCursor string                       `json:"next_cursor,omitempty"`
}

func newComputerProjection(computer l1.Computer, mutationApplied, replay *bool) computerOperatorProjection {
	memoryBytes := int64(0)
	if oci := computer.CurrentJob.Spec.Execution.OCI; oci != nil && oci.Limits != nil && oci.Limits.MemoryBytes != nil {
		memoryBytes = *oci.Limits.MemoryBytes
	}
	return computerOperatorProjection{
		Computer: computer,
		Capacity: computerCapacityProjection{
			RequestedMemoryBytes:      memoryBytes,
			RequestedDiskBytes:        computer.DesiredDiskBytes,
			ComputerTmpfsCeilingBytes: computerTmpfsCeilingBytes,
			Admission: computerEvidenceCell{Status: "NOT-RUN", Code: "admission_receipt_not_projected",
				Reason: "the current L1 Computer route does not retain helper-local admission receipts"},
		},
		ControllerTenure: computerEvidenceCell{Status: "NOT-RUN", Code: "tenure_state_not_projected",
			Reason: "the lifecycle route exposes the display front door but no current tenure authority"},
		MutationApplied: mutationApplied, IdempotentReplay: replay,
	}
}

func executeComputers(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usageError(computerUsage)
	}
	switch args[0] {
	case "submission":
		return executeComputerSubmission(ctx, clients, jsonOutput, args, stdout, stderr)
	case "create":
		return executeComputerCreate(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "list":
		return executeComputerList(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "get":
		return executeComputerGet(ctx, clients, jsonOutput, args[1:], stdout)
	case "start":
		return executeComputerDesiredState(ctx, clients, jsonOutput, args[1:], stdout, stderr, contract.ServiceDesiredRunning)
	case "stop":
		return executeComputerDesiredState(ctx, clients, jsonOutput, args[1:], stdout, stderr, contract.ServiceDesiredStopped)
	case "restart":
		return executeComputerRestart(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "remove":
		return executeComputerRemove(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "reimage":
		return executeComputerReimage(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "reset":
		return executeComputerReset(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "grow":
		return executeComputerGrow(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "abort":
		return executeComputerAbort(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	default:
		return usageError(fmt.Sprintf("unknown computers command %q", args[0]))
	}
}

func executeComputerCreate(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("computers create", flag.ContinueOnError)
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
		return usageError("usage: wefty computers create --name NAME --image IMAGE --node NODE_ID [--memory-bytes BYTES] [--disk-bytes BYTES] [--backup-cap COUNT] [--idempotency-key KEY]")
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
		return usageError("computers create requires --image")
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
	computer, replayed, err := clients.createComputer(ctx, l1.CreateComputerRequest{
		Name: strings.TrimSpace(name), Spec: spec, BackupCap: &backupCap,
	})
	if err != nil {
		return err
	}
	applied := !replayed
	return writeComputerProjection(stdout, newComputerProjection(computer, &applied, &replayed), jsonOutput)
}

func executeComputerList(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("computers list", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var cursor string
	var limit int
	flags.StringVar(&cursor, "cursor", "", "opaque cursor from the previous page")
	flags.IntVar(&limit, "limit", l1.DefaultJobPageLimit, "Computers per page")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || limit < 1 || limit > l1.MaxJobPageLimit {
		return usageError(fmt.Sprintf("usage: wefty computers list [--limit 1..%d] [--cursor CURSOR]", l1.MaxJobPageLimit))
	}
	page, err := clients.listComputers(ctx, cursor, limit)
	if err != nil {
		return err
	}
	output := computerListProjection{Computers: make([]computerOperatorProjection, 0, len(page.Computers)), NextCursor: page.NextCursor}
	for _, computer := range page.Computers {
		output.Computers = append(output.Computers, newComputerProjection(computer, nil, nil))
	}
	if jsonOutput {
		return writeJSON(stdout, output)
	}
	return writeComputersTable(stdout, output.Computers)
}

func executeComputerGet(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout io.Writer) error {
	if len(args) != 1 || strings.TrimSpace(args[0]) == "" {
		return usageError("usage: wefty computers get COMPUTER_ID")
	}
	computer, err := clients.getComputer(ctx, args[0])
	if err != nil {
		return err
	}
	return writeComputerProjection(stdout, newComputerProjection(computer, nil, nil), jsonOutput)
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
	explicit := mutation.intentRevision.set || strings.TrimSpace(mutation.storageID) != "" || mutation.storageGeneration.set
	if mutation.expectCurrent && explicit {
		return l1.ComputerMutationPrecondition{}, usageError("--expect-current cannot be combined with explicit CAS flags")
	}
	if mutation.expectCurrent {
		computer, err := clients.getComputer(ctx, computerID)
		if err != nil {
			return l1.ComputerMutationPrecondition{}, err
		}
		return l1.ComputerMutationPrecondition{IntentRevision: computer.IntentRevision,
			StorageID: computer.StorageID, StorageGeneration: computer.StorageGeneration}, nil
	}
	if !mutation.intentRevision.set || strings.TrimSpace(mutation.storageID) == "" || !mutation.storageGeneration.set {
		return l1.ComputerMutationPrecondition{}, usageError("mutation requires --intent-revision, --storage-id, and --storage-generation, or explicit --expect-current")
	}
	return l1.ComputerMutationPrecondition{IntentRevision: mutation.intentRevision.value,
		StorageID: strings.TrimSpace(mutation.storageID), StorageGeneration: mutation.storageGeneration.value}, nil
}

func (mutation computerMutationFlags) key() (string, error) {
	if strings.TrimSpace(mutation.idempotencyKey) == "" {
		return "", usageError("mutation requires --idempotency-key so retries cannot apply twice")
	}
	return validateIdempotencyKey(mutation.idempotencyKey)
}

func writeComputerMutation(stdout io.Writer, computer l1.Computer, observedRevision int64, replayed bool, jsonOutput bool) error {
	applied := !replayed && computer.IntentRevision > observedRevision
	return writeComputerProjection(stdout, newComputerProjection(computer, &applied, &replayed), jsonOutput)
}

func executeComputerDesiredState(ctx context.Context, clients *apiClients, jsonOutput bool, args []string,
	stdout, stderr io.Writer, desired contract.ServiceDesiredState,
) error {
	args = moveFirstPositionalToEnd(args)
	flags := flag.NewFlagSet("computers "+string(desired), flag.ContinueOnError)
	flags.SetOutput(stderr)
	var mutation computerMutationFlags
	mutation.bind(flags, false)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 || strings.TrimSpace(flags.Arg(0)) == "" {
		return usageError("usage: wefty computers start|stop COMPUTER_ID --intent-revision REV --storage-id ID --storage-generation GENERATION [--expect-current]")
	}
	precondition, err := mutation.resolve(ctx, clients, flags.Arg(0))
	if err != nil {
		return err
	}
	computer, err := clients.setComputerDesiredState(ctx, flags.Arg(0), l1.ComputerDesiredStateRequest{
		ComputerMutationPrecondition: precondition, DesiredState: desired,
	})
	if err != nil {
		return err
	}
	return writeComputerMutation(stdout, computer, precondition.IntentRevision, false, jsonOutput)
}

func executeComputerRestart(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	args = moveFirstPositionalToEnd(args)
	flags := flag.NewFlagSet("computers restart", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var mutation computerMutationFlags
	mutation.bind(flags, true)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return usageError("usage: wefty computers restart COMPUTER_ID --idempotency-key KEY [CAS flags | --expect-current]")
	}
	precondition, err := mutation.resolve(ctx, clients, flags.Arg(0))
	if err != nil {
		return err
	}
	key, err := mutation.key()
	if err != nil {
		return err
	}
	computer, replayed, err := clients.restartComputer(ctx, flags.Arg(0), l1.ComputerRestartRequest{
		ComputerMutationPrecondition: precondition, IdempotencyKey: key,
	})
	if err != nil {
		return err
	}
	return writeComputerMutation(stdout, computer, precondition.IntentRevision, replayed, jsonOutput)
}

func executeComputerRemove(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	args = moveFirstPositionalToEnd(args)
	flags := flag.NewFlagSet("computers remove", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var mutation computerMutationFlags
	mutation.bind(flags, false)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return usageError("usage: wefty computers remove COMPUTER_ID [CAS flags | --expect-current]")
	}
	precondition, err := mutation.resolve(ctx, clients, flags.Arg(0))
	if err != nil {
		return err
	}
	computer, err := clients.removeComputer(ctx, flags.Arg(0), l1.ComputerRemoveRequest{ComputerMutationPrecondition: precondition})
	if err != nil {
		return err
	}
	return writeComputerMutation(stdout, computer, precondition.IntentRevision, false, jsonOutput)
}

func executeComputerReimage(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	args = moveFirstPositionalToEnd(args)
	flags := flag.NewFlagSet("computers reimage", flag.ContinueOnError)
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
		return usageError("usage: wefty computers reimage COMPUTER_ID --image IMAGE --idempotency-key KEY [CAS flags | --expect-current]")
	}
	precondition, err := mutation.resolve(ctx, clients, flags.Arg(0))
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
	computer, replayed, err := clients.reimageComputer(ctx, flags.Arg(0), l1.ComputerReimageRequest{
		ComputerMutationPrecondition: precondition,
		Image:                        contract.OCIImageSpec{Reference: reference, Digest: digest}, Chown: chown,
		TerminateSessions: terminateSessions, IdempotencyKey: key,
	})
	if err != nil {
		return err
	}
	return writeComputerMutation(stdout, computer, precondition.IntentRevision, replayed, jsonOutput)
}

func executeComputerReset(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	args = moveFirstPositionalToEnd(args)
	flags := flag.NewFlagSet("computers reset", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var mutation computerMutationFlags
	var terminateSessions bool
	mutation.bind(flags, true)
	flags.BoolVar(&terminateSessions, "terminate-sessions", false, "close live take-over sessions before reset")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return usageError("usage: wefty computers reset COMPUTER_ID --idempotency-key KEY [CAS flags | --expect-current]")
	}
	precondition, err := mutation.resolve(ctx, clients, flags.Arg(0))
	if err != nil {
		return err
	}
	key, err := mutation.key()
	if err != nil {
		return err
	}
	computer, replayed, err := clients.resetComputer(ctx, flags.Arg(0), l1.ComputerStorageResetRequest{
		ComputerMutationPrecondition: precondition, IdempotencyKey: key, TerminateSessions: terminateSessions,
	})
	if err != nil {
		return err
	}
	return writeComputerMutation(stdout, computer, precondition.IntentRevision, replayed, jsonOutput)
}

func executeComputerGrow(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	args = moveFirstPositionalToEnd(args)
	flags := flag.NewFlagSet("computers grow", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var mutation computerMutationFlags
	var diskBytes optionalInt64Flag
	mutation.bind(flags, true)
	flags.Var(&diskBytes, "disk-bytes", "new fully allocated disk budget")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 || !diskBytes.set {
		return usageError("usage: wefty computers grow COMPUTER_ID --disk-bytes BYTES --idempotency-key KEY [CAS flags | --expect-current]")
	}
	precondition, err := mutation.resolve(ctx, clients, flags.Arg(0))
	if err != nil {
		return err
	}
	key, err := mutation.key()
	if err != nil {
		return err
	}
	computer, replayed, err := clients.growComputer(ctx, flags.Arg(0), l1.ComputerGrowRequest{
		ComputerMutationPrecondition: precondition, DiskBytes: diskBytes.value, IdempotencyKey: key,
	})
	if err != nil {
		return err
	}
	return writeComputerMutation(stdout, computer, precondition.IntentRevision, replayed, jsonOutput)
}

func executeComputerAbort(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	args = moveFirstPositionalToEnd(args)
	flags := flag.NewFlagSet("computers abort", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var mutation computerMutationFlags
	mutation.bind(flags, true)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return usageError("usage: wefty computers abort COMPUTER_ID --idempotency-key KEY [CAS flags | --expect-current]")
	}
	precondition, err := mutation.resolve(ctx, clients, flags.Arg(0))
	if err != nil {
		return err
	}
	key, err := mutation.key()
	if err != nil {
		return err
	}
	computer, replayed, err := clients.abortComputer(ctx, flags.Arg(0), l1.ComputerReconfigurationAbortRequest{
		ComputerMutationPrecondition: precondition, IdempotencyKey: key,
	})
	if err != nil {
		return err
	}
	return writeComputerMutation(stdout, computer, precondition.IntentRevision, replayed, jsonOutput)
}

func writeComputerProjection(writer io.Writer, computer computerOperatorProjection, jsonOutput bool) error {
	if jsonOutput {
		return writeJSON(writer, computer)
	}
	return writeComputersTable(writer, []computerOperatorProjection{computer})
}

func writeComputersTable(writer io.Writer, computers []computerOperatorProjection) error {
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "COMPUTER ID\tNAME\tDESIRED\tOBSERVED\tSTORAGE\tINTENT/APPLIED\tPHASE\tJOB ID\tATTEMPT\tNODE\tMEMORY\tDISK\tBACKUP CAP\tREADY\tDISPLAY ENDPOINT\tCONTROLLER TENURE\tLAST FAILURE\tREMOVAL\tMUTATION APPLIED\tIDEMPOTENT REPLAY"); err != nil {
		return err
	}
	for _, computer := range computers {
		job := computer.CurrentJob
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s@%d\t%d/%d\t%s\t%s\t%s\t%s\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			computer.ComputerID, computer.Name, computer.DesiredState, job.State,
			computer.StorageID, computer.StorageGeneration, computer.IntentRevision, computer.AppliedRevision,
			computer.ReconfigurationPhase, computer.CurrentJobID, valueOrNA(job.CurrentAttemptID),
			valueOrNA(computer.BoundNodeID), computer.Capacity.RequestedMemoryBytes, computer.Capacity.RequestedDiskBytes, computer.BackupCap,
			boolOrNA(job.Ready), pointerOrNA(computer.DisplayEndpoint, ""),
			computer.ControllerTenure.Status+"("+computer.ControllerTenure.Code+")",
			jsonOrNA(job.LastFailure), valueOrNA(computer.RemovalOutcome), boolPointerOrNA(computer.MutationApplied),
			boolPointerOrNA(computer.IdempotentReplay)); err != nil {
			return err
		}
	}
	return table.Flush()
}

func boolPointerOrNA(value *bool) string {
	if value == nil {
		return "N/A"
	}
	return strconv.FormatBool(*value)
}
