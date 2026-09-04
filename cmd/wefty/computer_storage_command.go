package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

const computerStorageUsage = "usage: wefty services backup create|list|prune|set-cap ... | wefty services restore|clone ... | wefty services custody export|import|attest ..."

type storageMutationFlags struct {
	intentRevision    optionalRevisionFlag
	storageID         string
	storageGeneration optionalRevisionFlag
	expectCurrent     bool
	idempotencyKey    string
}

func (mutation *storageMutationFlags) bind(flags *flag.FlagSet, keyed bool) {
	flags.Var(&mutation.intentRevision, "intent-revision", "observed Computer intent revision")
	flags.StringVar(&mutation.storageID, "storage-id", "", "observed durable Storage ID")
	flags.Var(&mutation.storageGeneration, "storage-generation", "observed Storage generation")
	flags.BoolVar(&mutation.expectCurrent, "expect-current", false, "opt in to reading the current CAS tuple immediately before mutation")
	if keyed {
		flags.StringVar(&mutation.idempotencyKey, "idempotency-key", "", "stable mutation replay key")
	}
}

func (mutation storageMutationFlags) resolve(ctx context.Context, clients *apiClients, computerID string) (l1.ComputerMutationPrecondition, error) {
	if err := mutation.validate(); err != nil {
		return l1.ComputerMutationPrecondition{}, err
	}
	if mutation.expectCurrent {
		computer, err := clients.getComputerStorageAuthority(ctx, computerID)
		if err != nil {
			return l1.ComputerMutationPrecondition{}, err
		}
		return l1.ComputerMutationPrecondition{IntentRevision: computer.IntentRevision,
			StorageID: computer.StorageID, StorageGeneration: computer.StorageGeneration}, nil
	}
	return l1.ComputerMutationPrecondition{IntentRevision: mutation.intentRevision.value,
		StorageID: strings.TrimSpace(mutation.storageID), StorageGeneration: mutation.storageGeneration.value}, nil
}

func (mutation storageMutationFlags) validate() error {
	explicit := mutation.intentRevision.set || strings.TrimSpace(mutation.storageID) != "" || mutation.storageGeneration.set
	if mutation.expectCurrent && explicit {
		return usageError("--expect-current cannot be combined with explicit CAS flags")
	}
	if mutation.expectCurrent {
		return nil
	}
	if !mutation.intentRevision.set || mutation.intentRevision.value < 1 || strings.TrimSpace(mutation.storageID) == "" ||
		!mutation.storageGeneration.set || mutation.storageGeneration.value < 1 {
		return usageError("mutation requires positive --intent-revision, --storage-id, and positive --storage-generation, or explicit --expect-current")
	}
	return nil
}

func (mutation storageMutationFlags) key() (string, error) {
	if strings.TrimSpace(mutation.idempotencyKey) == "" {
		return "", usageError("mutation requires --idempotency-key so retries cannot apply twice")
	}
	return ensureIdempotencyKey(mutation.idempotencyKey)
}

type storageWaitFlags struct {
	timeout      time.Duration
	pollInterval time.Duration
	pollSet      bool
}

func (wait *storageWaitFlags) bind(flags *flag.FlagSet) {
	defaultPollInterval := 250 * time.Millisecond
	if wait.pollInterval > 0 {
		defaultPollInterval = wait.pollInterval
	}
	flags.DurationVar(&wait.timeout, "wait", 0, "wait up to this duration for L1 to observe the operation finishing")
	flags.DurationVar(&wait.pollInterval, "poll-interval", defaultPollInterval, "interval between L1 observations")
}

func (wait *storageWaitFlags) validate(flags *flag.FlagSet) error {
	flags.Visit(func(visited *flag.Flag) { wait.pollSet = wait.pollSet || visited.Name == "poll-interval" })
	if err := wait.validateDurations(); err != nil {
		return err
	}
	if wait.pollSet && wait.timeout == 0 {
		return usageError("--poll-interval requires --wait DURATION")
	}
	return nil
}

func (wait storageWaitFlags) validateDurations() error {
	if wait.timeout < 0 || wait.pollInterval <= 0 {
		return usageError("--wait cannot be negative and --poll-interval must be positive")
	}
	return nil
}

type storageWaitObservation struct {
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
	StartedAt string `json:"started_at,omitempty"`
	EndedAt   string `json:"ended_at,omitempty"`
}

type storageMutationOutput struct {
	MutationApplied   bool                          `json:"mutation_applied"`
	IdempotentReplay  bool                          `json:"idempotent_replay"`
	Observation       *storageWaitObservation       `json:"observation,omitempty"`
	Computer          *l1.Computer                  `json:"computer,omitempty"`
	Backup            *l1.Backup                    `json:"backup,omitempty"`
	Backups           *l1.BackupList                `json:"backups,omitempty"`
	CustodyExport     *l1.ComputerCustodyExport     `json:"custody_export,omitempty"`
	CustodyImport     *l1.ComputerCustodyImport     `json:"custody_import,omitempty"`
	StorageProvenance *l1.ComputerStorageProvenance `json:"storage_provenance,omitempty"`
}

type computerBackupInventory struct {
	l1.BackupList
	l1.ComputerStorageProvenance
}

type storageObservationError struct{ cause error }

func (err *storageObservationError) Error() string {
	return "mutation was accepted but observing completion failed: " + err.cause.Error()
}

func (err *storageObservationError) Unwrap() error { return err.cause }

func executeComputerStorage(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usageError(computerStorageUsage)
	}
	switch args[0] {
	case "backup":
		return executeComputerBackups(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "restore":
		return executeComputerRestore(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "clone":
		return executeComputerClone(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "custody":
		return executeComputerCustody(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	default:
		return usageError(fmt.Sprintf("unknown services storage command %q", args[0]))
	}
}

func executeComputerBackups(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usageError("usage: wefty services backup create|list|prune|set-cap")
	}
	switch args[0] {
	case "create":
		return executeComputerBackupCreate(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "list":
		if len(args) != 2 || strings.TrimSpace(args[1]) == "" {
			return usageError("usage: wefty services backup list COMPUTER")
		}
		computerID, err := resolveComputerID(ctx, clients, args[1])
		if err != nil {
			return err
		}
		backups, err := clients.listComputerBackups(ctx, computerID)
		if err != nil {
			return err
		}
		provenance, err := clients.listComputerStorageProvenance(ctx, computerID)
		if err != nil {
			return err
		}
		if jsonOutput {
			return writeJSON(stdout, computerBackupInventory{BackupList: backups, ComputerStorageProvenance: provenance})
		}
		if err := writeComputerBackups(stdout, backups); err != nil {
			return err
		}
		return writeComputerStorageProvenance(stdout, provenance)
	case "prune":
		return executeComputerBackupPrune(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "set-cap":
		return executeComputerBackupSetCap(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	default:
		return usageError(fmt.Sprintf("unknown services backup command %q", args[0]))
	}
}

func newStorageFlagSet(name string, stderr io.Writer) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(stderr)
	return flags
}

func parseStorageFlags(flags *flag.FlagSet, args []string, leadingPositionals int) error {
	for range leadingPositionals {
		args = moveFirstPositionalToEnd(args)
	}
	if err := flags.Parse(args); err != nil {
		return usageError(err.Error())
	}
	return nil
}

func executeComputerBackupCreate(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	flags := newStorageFlagSet("services backup create", stderr)
	var mutation storageMutationFlags
	var wait storageWaitFlags
	var allowPowerOff bool
	mutation.bind(flags, true)
	wait.bind(flags)
	flags.BoolVar(&allowPowerOff, "allow-power-off", false, "acknowledge that a running Computer will be quiesced and resumed")
	if err := parseStorageFlags(flags, args, 1); err != nil {
		return err
	}
	if flags.NArg() != 1 || strings.TrimSpace(flags.Arg(0)) == "" {
		return usageError("usage: wefty services backup create COMPUTER --idempotency-key KEY [CAS flags | --expect-current] [--allow-power-off] [--wait DURATION]")
	}
	if err := wait.validate(flags); err != nil {
		return err
	}
	if err := mutation.validate(); err != nil {
		return err
	}
	key, err := mutation.key()
	if err != nil {
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
	computer, replayed, err := clients.createComputerBackup(ctx, computerID, l1.ComputerBackupCreateRequest{
		ComputerMutationPrecondition: precondition, IdempotencyKey: key, AllowPowerOff: allowPowerOff,
	})
	if err != nil {
		return err
	}
	output := storageMutationOutput{MutationApplied: !replayed && computer.IntentRevision > precondition.IntentRevision,
		IdempotentReplay: replayed, Computer: &computer}
	if wait.timeout > 0 {
		result, observation, waitErr := waitForBackupOperation(ctx, clients, computerID, computer.IntentRevision, wait)
		output.Backups, output.Observation = &result, &observation
		waitErr = attachStorageProvenance(ctx, clients, computerID, &output, waitErr)
		return writeStorageMutationThenError(stdout, output, jsonOutput, waitErr)
	}
	return writeStorageMutation(stdout, output, jsonOutput)
}

func executeComputerBackupPrune(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	flags := newStorageFlagSet("services backup prune", stderr)
	var mutation storageMutationFlags
	var wait storageWaitFlags
	mutation.bind(flags, true)
	wait.bind(flags)
	if err := parseStorageFlags(flags, args, 2); err != nil {
		return err
	}
	if flags.NArg() != 2 || strings.TrimSpace(flags.Arg(0)) == "" || strings.TrimSpace(flags.Arg(1)) == "" {
		return usageError("usage: wefty services backup prune COMPUTER BACKUP --idempotency-key KEY [CAS flags | --expect-current] [--wait DURATION]")
	}
	if err := wait.validate(flags); err != nil {
		return err
	}
	if err := mutation.validate(); err != nil {
		return err
	}
	key, err := mutation.key()
	if err != nil {
		return err
	}
	computerID, err := resolveComputerID(ctx, clients, flags.Arg(0))
	if err != nil {
		return err
	}
	backupID := flags.Arg(1)
	precondition, err := mutation.resolve(ctx, clients, computerID)
	if err != nil {
		return err
	}
	backup, replayed, err := clients.pruneComputerBackup(ctx, computerID, backupID, l1.ComputerBackupPruneRequest{
		ComputerMutationPrecondition: precondition, IdempotencyKey: key,
	})
	if err != nil {
		return err
	}
	output := storageMutationOutput{MutationApplied: !replayed, IdempotentReplay: replayed, Backup: &backup}
	if wait.timeout > 0 {
		observed, observation, waitErr := waitForBackupPrune(ctx, clients, computerID, backupID, wait)
		output.Backup, output.Observation = &observed, &observation
		return writeStorageMutationThenError(stdout, output, jsonOutput, waitErr)
	}
	return writeStorageMutation(stdout, output, jsonOutput)
}

func executeComputerBackupSetCap(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	flags := newStorageFlagSet("services backup set-cap", stderr)
	var mutation storageMutationFlags
	var capValue optionalRevisionFlag
	mutation.bind(flags, false)
	flags.Var(&capValue, "cap", "maximum retained Backups; zero disables creation")
	if err := parseStorageFlags(flags, args, 1); err != nil {
		return err
	}
	if flags.NArg() != 1 || !capValue.set || capValue.value < 0 {
		return usageError("usage: wefty services backup set-cap COMPUTER --cap NON_NEGATIVE [CAS flags | --expect-current]")
	}
	if err := mutation.validate(); err != nil {
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
	computer, err := clients.setComputerBackupCap(ctx, computerID, l1.ComputerBackupCapRequest{
		ComputerMutationPrecondition: precondition, BackupCap: capValue.value,
	})
	if err != nil {
		return err
	}
	output := storageMutationOutput{MutationApplied: computer.IntentRevision > precondition.IntentRevision, Computer: &computer}
	return writeStorageMutation(stdout, output, jsonOutput)
}

func executeComputerRestore(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	flags := newStorageFlagSet("services restore", stderr)
	var mutation storageMutationFlags
	var wait storageWaitFlags
	var keepOld, retireOld bool
	mutation.bind(flags, true)
	wait.bind(flags)
	flags.BoolVar(&keepOld, "keep-old-as-backup", false, "precommit retaining the predecessor as a Backup")
	flags.BoolVar(&retireOld, "retire-old", false, "precommit retiring the predecessor after publication")
	if err := parseStorageFlags(flags, args, 2); err != nil {
		return err
	}
	if flags.NArg() != 2 || keepOld == retireOld {
		return usageError("usage: wefty services restore COMPUTER BACKUP (--keep-old-as-backup | --retire-old) --idempotency-key KEY [CAS flags | --expect-current] [--wait DURATION]")
	}
	if err := wait.validate(flags); err != nil {
		return err
	}
	if err := mutation.validate(); err != nil {
		return err
	}
	key, err := mutation.key()
	if err != nil {
		return err
	}
	computerID, err := resolveComputerID(ctx, clients, flags.Arg(0))
	if err != nil {
		return err
	}
	backupID := flags.Arg(1)
	precondition, err := mutation.resolve(ctx, clients, computerID)
	if err != nil {
		return err
	}
	computer, replayed, err := clients.restoreComputerBackup(ctx, computerID, backupID, l1.ComputerRestoreRequest{
		ComputerMutationPrecondition: precondition, KeepOldBackup: keepOld, IdempotencyKey: key,
	})
	if err != nil {
		return err
	}
	output := storageMutationOutput{MutationApplied: !replayed && computer.IntentRevision > precondition.IntentRevision,
		IdempotentReplay: replayed, Computer: &computer}
	if wait.timeout > 0 {
		observed, observation, waitErr := waitForComputerRevision(ctx, clients, computer.ComputerID, computer.IntentRevision, wait)
		output.Computer, output.Observation = &observed, &observation
		waitErr = attachStorageProvenance(ctx, clients, computer.ComputerID, &output, waitErr)
		return writeStorageMutationThenError(stdout, output, jsonOutput, waitErr)
	}
	return writeStorageMutation(stdout, output, jsonOutput)
}

func executeComputerClone(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	flags := newStorageFlagSet("services clone", stderr)
	var mutation storageMutationFlags
	var wait storageWaitFlags
	var name string
	var diskBytes optionalRevisionFlag
	mutation.bind(flags, true)
	wait.bind(flags)
	flags.StringVar(&name, "name", "", "new Computer name")
	flags.Var(&diskBytes, "disk-bytes", "fully allocated destination disk size")
	if err := parseStorageFlags(flags, args, 2); err != nil {
		return err
	}
	if flags.NArg() != 2 || strings.TrimSpace(name) == "" || !diskBytes.set || diskBytes.value < 1 {
		return usageError("usage: wefty services clone COMPUTER BACKUP --name NAME --disk-bytes BYTES --idempotency-key KEY [CAS flags | --expect-current] [--wait DURATION]")
	}
	if err := wait.validate(flags); err != nil {
		return err
	}
	if err := mutation.validate(); err != nil {
		return err
	}
	key, err := mutation.key()
	if err != nil {
		return err
	}
	sourceComputerID, err := resolveComputerID(ctx, clients, flags.Arg(0))
	if err != nil {
		return err
	}
	backupID := flags.Arg(1)
	precondition, err := mutation.resolve(ctx, clients, sourceComputerID)
	if err != nil {
		return err
	}
	computer, replayed, err := clients.cloneComputerBackup(ctx, sourceComputerID, backupID, l1.ComputerCloneRequest{
		ComputerMutationPrecondition: precondition, Name: strings.TrimSpace(name), DiskBytes: diskBytes.value, IdempotencyKey: key,
	})
	if err != nil {
		return err
	}
	output := storageMutationOutput{MutationApplied: !replayed, IdempotentReplay: replayed, Computer: &computer}
	var waitErr error
	if wait.timeout > 0 {
		var observed l1.Computer
		var observation storageWaitObservation
		observed, observation, waitErr = waitForComputerRevision(ctx, clients, computer.ComputerID, computer.IntentRevision, wait)
		output.Computer, output.Observation = &observed, &observation
	}
	waitErr = attachStorageProvenance(ctx, clients, computer.ComputerID, &output, waitErr)
	return writeStorageMutationThenError(stdout, output, jsonOutput, waitErr)
}

func executeComputerCustody(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usageError("usage: wefty services custody export|import|attest")
	}
	switch args[0] {
	case "export":
		return executeComputerCustodyExport(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "import":
		return executeComputerCustodyImport(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "attest":
		return executeComputerCustodyAttest(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	default:
		return usageError(fmt.Sprintf("unknown services custody command %q", args[0]))
	}
}

func executeComputerCustodyExport(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	flags := newStorageFlagSet("services custody export", stderr)
	var mutation storageMutationFlags
	var wait storageWaitFlags
	var externalPath string
	mutation.bind(flags, true)
	wait.bind(flags)
	flags.StringVar(&externalPath, "path", "", "absolute operator-owned export directory")
	if err := parseStorageFlags(flags, args, 2); err != nil {
		return err
	}
	if flags.NArg() != 2 || strings.TrimSpace(externalPath) == "" {
		return usageError("usage: wefty services custody export COMPUTER BACKUP --path ABSOLUTE --idempotency-key KEY [CAS flags | --expect-current] [--wait DURATION]")
	}
	if err := wait.validate(flags); err != nil {
		return err
	}
	if err := mutation.validate(); err != nil {
		return err
	}
	key, err := mutation.key()
	if err != nil {
		return err
	}
	computerID, err := resolveComputerID(ctx, clients, flags.Arg(0))
	if err != nil {
		return err
	}
	backupID := flags.Arg(1)
	precondition, err := mutation.resolve(ctx, clients, computerID)
	if err != nil {
		return err
	}
	exported, replayed, err := clients.exportComputerBackup(ctx, computerID, backupID, l1.ComputerCustodyExportRequest{
		ComputerMutationPrecondition: precondition, ExternalPath: externalPath, IdempotencyKey: key,
	})
	if err != nil {
		return err
	}
	output := storageMutationOutput{MutationApplied: !replayed, IdempotentReplay: replayed, CustodyExport: &exported}
	var waitErr error
	if wait.timeout > 0 {
		var observed l1.ComputerCustodyExport
		var observation storageWaitObservation
		observed, observation, waitErr = waitForCustodyExport(ctx, clients, computerID, exported.ExportID, wait)
		output.CustodyExport, output.Observation = &observed, &observation
	}
	waitErr = attachStorageProvenance(ctx, clients, computerID, &output, waitErr)
	return writeStorageMutationThenError(stdout, output, jsonOutput, waitErr)
}

func executeComputerCustodyImport(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	flags := newStorageFlagSet("services custody import", stderr)
	var wait storageWaitFlags
	var name, nodeID, externalPath, manifestPath, manifestDigest, idempotencyKey string
	var diskBytes optionalRevisionFlag
	wait.bind(flags)
	flags.StringVar(&name, "name", "", "new Computer name")
	flags.StringVar(&nodeID, "node", "", "current destination Node")
	flags.StringVar(&externalPath, "path", "", "absolute operator-owned export directory")
	flags.StringVar(&manifestPath, "manifest", "", "custody.json path")
	flags.StringVar(&manifestDigest, "manifest-digest", "", "expected sha256 digest for the submitted custody.json")
	flags.StringVar(&idempotencyKey, "idempotency-key", "", "stable mutation replay key")
	flags.Var(&diskBytes, "disk-bytes", "fully allocated destination disk size")
	if err := parseStorageFlags(flags, args, 1); err != nil {
		return err
	}
	if flags.NArg() != 1 || strings.TrimSpace(name) == "" || strings.TrimSpace(nodeID) == "" ||
		strings.TrimSpace(externalPath) == "" || strings.TrimSpace(manifestPath) == "" || strings.TrimSpace(manifestDigest) == "" ||
		strings.TrimSpace(idempotencyKey) == "" || !diskBytes.set || diskBytes.value < 1 {
		return usageError("usage: wefty services custody import EXPORT --name NAME --disk-bytes BYTES --node NODE --path ABSOLUTE --manifest custody.json --manifest-digest sha256:... --idempotency-key KEY [--wait DURATION]")
	}
	if err := wait.validate(flags); err != nil {
		return err
	}
	key, err := ensureIdempotencyKey(idempotencyKey)
	if err != nil {
		return err
	}
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		return fmt.Errorf("read Custody manifest: %w", err)
	}
	var manifest contract.ComputerCustodyManifest
	decoder := json.NewDecoder(strings.NewReader(string(manifestBytes)))
	if err := decoder.Decode(&manifest); err != nil {
		return usageError("invalid Custody manifest: " + err.Error())
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return usageError("invalid Custody manifest: trailing JSON value")
	}
	imported, replayed, err := clients.importComputerCustody(ctx, flags.Arg(0), l1.ComputerCustodyImportRequest{
		Name: strings.TrimSpace(name), DiskBytes: diskBytes.value, NodeID: strings.TrimSpace(nodeID),
		ExternalPath: externalPath, Manifest: manifest, ManifestDigest: strings.TrimSpace(manifestDigest), IdempotencyKey: key,
	})
	if err != nil {
		return err
	}
	output := storageMutationOutput{MutationApplied: !replayed, IdempotentReplay: replayed, CustodyImport: &imported}
	if wait.timeout > 0 {
		observed, computer, observation, waitErr := waitForCustodyImport(ctx, clients, imported.ImportID, imported.OperationRevision, wait)
		imported.Status, imported.FailureCode = observed.Status, observed.FailureCode
		imported.PreparationOutcome, imported.CompletedAt = observed.PreparationOutcome, observed.CompletedAt
		output.CustodyImport, output.Observation = &imported, &observation
		if computer.ComputerID != "" {
			output.Computer = &computer
		}
		if waitErr == nil {
			waitErr = attachStorageProvenance(ctx, clients, imported.DestinationComputerID, &output, nil)
		}
		return writeStorageMutationThenError(stdout, output, jsonOutput, waitErr)
	}
	return writeStorageMutation(stdout, output, jsonOutput)
}

func executeComputerCustodyAttest(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	flags := newStorageFlagSet("services custody attest", stderr)
	var idempotencyKey string
	flags.StringVar(&idempotencyKey, "idempotency-key", "", "stable immutable attestation key")
	if err := parseStorageFlags(flags, args, 1); err != nil {
		return err
	}
	if flags.NArg() != 1 || strings.TrimSpace(idempotencyKey) == "" {
		return usageError("usage: wefty services custody attest EXPORT --idempotency-key KEY")
	}
	key, err := ensureIdempotencyKey(idempotencyKey)
	if err != nil {
		return err
	}
	exported, replayed, err := clients.attestComputerCustodyDeleted(ctx, flags.Arg(0), l1.ComputerCustodyAttestationRequest{IdempotencyKey: key})
	if err != nil {
		return err
	}
	output := storageMutationOutput{MutationApplied: !replayed, IdempotentReplay: replayed, CustodyExport: &exported}
	observationErr := attachStorageProvenance(ctx, clients, exported.ComputerID, &output, nil)
	return writeStorageMutationThenError(stdout, output, jsonOutput, observationErr)
}

func waitForBackupOperation(ctx context.Context, clients *apiClients, computerID string, operationRevision int64, wait storageWaitFlags) (l1.BackupList, storageWaitObservation, error) {
	var last l1.BackupList
	observation, err := pollStorageObservation(ctx, wait, func() (bool, error) {
		var readErr error
		last, readErr = clients.listComputerBackups(ctx, computerID)
		if readErr != nil {
			return false, readErr
		}
		return last.LastOperation != nil && last.LastOperation.OperationRevision == operationRevision && last.LastOperation.CompletedAt != nil, nil
	})
	return last, observation, err
}

func attachStorageProvenance(ctx context.Context, clients *apiClients, computerID string, output *storageMutationOutput, prior error) error {
	provenance, err := clients.listComputerStorageProvenance(ctx, computerID)
	if err == nil {
		output.StorageProvenance = &provenance
		return prior
	}
	if output.Observation == nil {
		now := time.Now().UTC().Format(time.RFC3339Nano)
		output.Observation = &storageWaitObservation{Status: "failed", Error: err.Error(), StartedAt: now, EndedAt: now}
	} else {
		output.Observation.Status = "failed"
		if prior == nil || output.Observation.Error == "" {
			output.Observation.Error = err.Error()
		} else {
			output.Observation.Error += "; provenance observation: " + err.Error()
		}
		output.Observation.EndedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	observationErr := &storageObservationError{cause: err}
	if prior != nil {
		return errors.Join(prior, observationErr)
	}
	return observationErr
}

func waitForBackupPrune(ctx context.Context, clients *apiClients, computerID, backupID string, wait storageWaitFlags) (l1.Backup, storageWaitObservation, error) {
	var observed l1.Backup
	observation, err := pollStorageObservation(ctx, wait, func() (bool, error) {
		backups, readErr := clients.listComputerBackups(ctx, computerID)
		if readErr != nil {
			return false, readErr
		}
		for _, backup := range backups.Backups {
			if backup.BackupID == backupID {
				observed = backup
				return backup.Status == "pruned", nil
			}
		}
		return false, nil
	})
	return observed, observation, err
}

func waitForComputerRevision(ctx context.Context, clients *apiClients, computerID string, operationRevision int64, wait storageWaitFlags) (l1.Computer, storageWaitObservation, error) {
	var observed l1.Computer
	observation, err := pollStorageObservation(ctx, wait, func() (bool, error) {
		var readErr error
		observed, readErr = clients.getComputerStorageAuthority(ctx, computerID)
		if readErr != nil {
			return false, readErr
		}
		return observed.AppliedRevision >= operationRevision && observed.ReconfigurationPhase == l1.ComputerReconfigurationStable, nil
	})
	return observed, observation, err
}

func waitForCustodyImport(ctx context.Context, clients *apiClients, importID string, operationRevision int64, wait storageWaitFlags) (l1.ComputerCustodyImportObservation, l1.Computer, storageWaitObservation, error) {
	var observed l1.ComputerCustodyImportObservation
	var computer l1.Computer
	observation, err := pollStorageObservation(ctx, wait, func() (bool, error) {
		var readErr error
		observed, readErr = clients.getComputerCustodyImport(ctx, importID)
		if readErr != nil {
			return false, readErr
		}
		if observed.OperationRevision != operationRevision {
			return false, fmt.Errorf("Custody import %q observation revision %d does not match accepted revision %d", importID, observed.OperationRevision, operationRevision)
		}
		if observed.Status == "failed" || observed.Status == "superseded" {
			return false, fmt.Errorf("Custody import %q failed with %s", importID, observed.FailureCode)
		}
		if observed.PreparationOutcome != nil {
			return false, fmt.Errorf("Custody import %q preparation reported %s", importID, observed.PreparationOutcome.Code)
		}
		if observed.Status == "reserved" {
			return false, nil
		}
		if observed.Status != "complete" {
			return false, fmt.Errorf("Custody import %q reported unexpected status %q", importID, observed.Status)
		}
		computer, readErr = clients.getComputerStorageAuthority(ctx, importID)
		if readErr != nil {
			return false, readErr
		}
		if computer.AppliedRevision < operationRevision || computer.ReconfigurationPhase != l1.ComputerReconfigurationStable {
			return false, fmt.Errorf("Custody import %q completed without matching stable Computer authority", importID)
		}
		return true, nil
	})
	return observed, computer, observation, err
}

func waitForCustodyExport(ctx context.Context, clients *apiClients, computerID, exportID string, wait storageWaitFlags) (l1.ComputerCustodyExport, storageWaitObservation, error) {
	var observed l1.ComputerCustodyExport
	observation, err := pollStorageObservation(ctx, wait, func() (bool, error) {
		exports, readErr := clients.listComputerCustodyExports(ctx, computerID)
		if readErr != nil {
			return false, readErr
		}
		for _, exported := range exports {
			if exported.ExportID == exportID {
				observed = exported
				return exported.CompletedAt != nil, nil
			}
		}
		return false, fmt.Errorf("L1 omitted Custody export %q while observing completion", exportID)
	})
	return observed, observation, err
}

func pollStorageObservation(ctx context.Context, wait storageWaitFlags, observe func() (bool, error)) (storageWaitObservation, error) {
	started := time.Now().UTC()
	observation := storageWaitObservation{Status: "waiting", StartedAt: started.Format(time.RFC3339Nano)}
	waitCtx, cancel := context.WithTimeout(ctx, wait.timeout)
	defer cancel()
	for {
		done, err := observe()
		if err != nil {
			observation.Status, observation.Error = "failed", err.Error()
			observation.EndedAt = time.Now().UTC().Format(time.RFC3339Nano)
			return observation, &storageObservationError{cause: err}
		}
		if done {
			observation.Status = "observed"
			observation.EndedAt = time.Now().UTC().Format(time.RFC3339Nano)
			return observation, nil
		}
		timer := time.NewTimer(wait.pollInterval)
		select {
		case <-waitCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			observation.Status, observation.Error = "failed", waitCtx.Err().Error()
			observation.EndedAt = time.Now().UTC().Format(time.RFC3339Nano)
			return observation, &storageObservationError{cause: waitCtx.Err()}
		case <-timer.C:
		}
	}
}

func writeStorageMutationThenError(writer io.Writer, output storageMutationOutput, jsonOutput bool, err error) error {
	if writeErr := writeStorageMutation(writer, output, jsonOutput); writeErr != nil {
		return errors.Join(err, writeErr)
	}
	return err
}

func writeStorageMutation(writer io.Writer, output storageMutationOutput, jsonOutput bool) error {
	if jsonOutput {
		return writeJSON(writer, output)
	}
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "MUTATION APPLIED\tIDEMPOTENT REPLAY\tOBSERVATION"); err != nil {
		return err
	}
	observation := "NOT-RUN"
	if output.Observation != nil {
		observation = output.Observation.Status
		if output.Observation.Error != "" {
			observation += ": " + output.Observation.Error
		}
	}
	if _, err := fmt.Fprintf(table, "%t\t%t\t%s\n", output.MutationApplied, output.IdempotentReplay, observation); err != nil {
		return err
	}
	if err := table.Flush(); err != nil {
		return err
	}
	if output.Computer != nil {
		if err := writeStorageComputer(writer, *output.Computer); err != nil {
			return err
		}
	}
	if output.Backup != nil {
		if err := writeComputerBackups(writer, l1.BackupList{Backups: []l1.Backup{*output.Backup}}); err != nil {
			return err
		}
	}
	if output.Backups != nil {
		if err := writeComputerBackups(writer, *output.Backups); err != nil {
			return err
		}
	}
	if output.CustodyExport != nil {
		if err := writeCustodyExports(writer, []l1.ComputerCustodyExport{*output.CustodyExport}); err != nil {
			return err
		}
		if output.CustodyExport.OperatorAttestedDeleted {
			table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
			if _, err := fmt.Fprintln(table, "ATTESTATION EFFECT\tDeletion attestation recorded; it never upgrades removed_reduced."); err != nil {
				return err
			}
			if err := table.Flush(); err != nil {
				return err
			}
		}
	}
	if output.CustodyImport != nil {
		table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
		if _, err := fmt.Fprintln(table, "IMPORT ID\tEXPORT ID\tOPERATION REVISION\tDESTINATION COMPUTER\tDESTINATION STORAGE\tNAME\tSIZE\tSTATUS\tFAILURE CODE\tPREPARATION OUTCOME\tHELPER GENERATION\tSWEEP EPOCH"); err != nil {
			return err
		}
		preparationCode, helperGeneration, sweepEpoch := "N/A", "N/A", "N/A"
		if outcome := output.CustodyImport.PreparationOutcome; outcome != nil {
			preparationCode = outcome.Code
			helperGeneration = strconv.FormatUint(outcome.HelperGeneration, 10)
			sweepEpoch = valueOrNA(outcome.SweepEpoch)
		}
		if _, err := fmt.Fprintf(table, "%s\t%s\t%d\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\n",
			output.CustodyImport.ImportID, output.CustodyImport.ExportID, output.CustodyImport.OperationRevision,
			output.CustodyImport.DestinationComputerID, output.CustodyImport.DestinationStorageID,
			output.CustodyImport.DestinationName, output.CustodyImport.DestinationSize, output.CustodyImport.Status,
			valueOrNA(output.CustodyImport.FailureCode), preparationCode, helperGeneration, sweepEpoch); err != nil {
			return err
		}
		if err := table.Flush(); err != nil {
			return err
		}
	}
	if output.StorageProvenance != nil {
		return writeComputerStorageProvenance(writer, *output.StorageProvenance)
	}
	return nil
}

func writeComputerStorageProvenance(writer io.Writer, projection l1.ComputerStorageProvenance) error {
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "CUSTODY COMPUTER\tSTORAGE\tGENERATION\tREMOVAL OUTCOME\tEXTERNAL CUSTODY TAINT"); err != nil {
		return err
	}
	for _, branch := range projection.CustodyForks {
		if _, err := fmt.Fprintf(table, "%s\t%s\t%d\t%s\t%t\n", branch.ComputerID, branch.StorageID,
			branch.StorageGeneration, valueOrNA(branch.RemovalOutcome), projection.CustodyTainted); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(table, "PROVENANCE ID\tKIND\tSOURCE STORAGE\tSOURCE GENERATION\tBACKUP\tDESTINATION COMPUTER\tDESTINATION STORAGE\tDESTINATION GENERATION\tCREATED"); err != nil {
		return err
	}
	for _, provenance := range projection.Provenance {
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\n",
			provenance.ProvenanceID, provenance.Kind, provenance.SourceStorageID, provenance.SourceGeneration,
			provenance.BackupID, valueOrNA(provenance.DestinationComputerID), valueOrNA(provenance.DestinationStorageID),
			int64OptionalOrNA(provenance.DestinationGeneration), provenance.CreatedAt.Format(time.RFC3339)); err != nil {
			return err
		}
	}
	if err := table.Flush(); err != nil {
		return err
	}
	if len(projection.CustodyExports) > 0 {
		return writeCustodyExports(writer, projection.CustodyExports)
	}
	return nil
}

func int64OptionalOrNA(value int64) string {
	if value == 0 {
		return "N/A"
	}
	return strconv.FormatInt(value, 10)
}

func writeStorageComputer(writer io.Writer, computer l1.Computer) error {
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "COMPUTER ID\tNAME\tDESIRED\tOBSERVED\tSTORAGE\tINTENT/APPLIED\tRECONFIGURATION\tBACKUP CAP\tGRANTS\tREMOVAL OUTCOME"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s@%d\t%d/%d\t%s\t%d\t%d\t%s\n",
		computer.ComputerID, computer.Name, computer.DesiredState, computer.CurrentJob.State,
		computer.StorageID, computer.StorageGeneration, computer.IntentRevision, computer.AppliedRevision,
		computer.ReconfigurationPhase, computer.BackupCap, len(computer.Grants), valueOrNA(computer.RemovalOutcome)); err != nil {
		return err
	}
	return table.Flush()
}

func writeComputerBackups(writer io.Writer, backups l1.BackupList) error {
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "BACKUP ID\tCOMPUTER\tSOURCE STORAGE\tGENERATION\tSTATUS\tSIZE\tDIGEST\tENCRYPTION\tPROVENANCE\tCOPY\tNODE\tROOT\tCOPY PHASE\tCREATED"); err != nil {
		return err
	}
	for _, backup := range backups.Backups {
		if len(backup.Copies) == 0 {
			if _, err := fmt.Fprintf(table, "%s\t%s\t%s\t%d\t%s\t%d\t%s\t%s\t%s:%s@%d\tN/A\tN/A\tN/A\tN/A\t%s\n",
				backup.BackupID, backup.ComputerID, backup.SourceStorageID, backup.SourceGeneration, backup.Status,
				backup.AllocatedSize, backup.ContentDigest, backup.Encryption, backup.Provenance.Kind,
				backup.Provenance.SourceStorageID, backup.Provenance.SourceGeneration, backup.CreatedAt.Format(time.RFC3339)); err != nil {
				return err
			}
			continue
		}
		for _, copy := range backup.Copies {
			if _, err := fmt.Fprintf(table, "%s\t%s\t%s\t%d\t%s\t%d\t%s\t%s\t%s:%s@%d\t%s\t%s\t%s\t%s\t%s\n",
				backup.BackupID, backup.ComputerID, backup.SourceStorageID, backup.SourceGeneration, backup.Status,
				backup.AllocatedSize, backup.ContentDigest, backup.Encryption, backup.Provenance.Kind,
				backup.Provenance.SourceStorageID, backup.Provenance.SourceGeneration, copy.CopyID,
				copy.NodeID, copy.RootInstanceID, copy.Phase, backup.CreatedAt.Format(time.RFC3339)); err != nil {
				return err
			}
		}
	}
	if backups.LastOperation != nil {
		if err := table.Flush(); err != nil {
			return err
		}
		last := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
		if _, err := fmt.Fprintln(last, "LAST OPERATION BACKUP\tOPERATION REVISION\tSTATUS\tFAILURE CODE\tCOMPLETED"); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(last, "%s\t%d\t%s\t%s\t%s\n", valueOrNA(backups.LastOperation.BackupID),
			backups.LastOperation.OperationRevision, backups.LastOperation.Status, valueOrNA(string(backups.LastOperation.FailureCode)),
			timeOrNA(backups.LastOperation.CompletedAt)); err != nil {
			return err
		}
		return last.Flush()
	}
	return table.Flush()
}

func writeCustodyExports(writer io.Writer, exports []l1.ComputerCustodyExport) error {
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "EXPORT ID\tCOMPUTER\tBACKUP\tCOPY\tSOURCE STORAGE\tGENERATION\tPATH\tSTATUS\tFAILURE\tMANIFEST DIGEST\tATTESTED DELETED\tREQUESTED\tCOMPLETED"); err != nil {
		return err
	}
	for _, exported := range exports {
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%t\t%s\t%s\n",
			exported.ExportID, exported.ComputerID, exported.BackupID, exported.CopyID,
			exported.SourceStorageID, exported.SourceGeneration, exported.ExternalPath, exported.Status,
			valueOrNA(exported.FailureCode), valueOrNA(exported.ManifestDigest), exported.OperatorAttestedDeleted,
			exported.RequestedAt.Format(time.RFC3339), timeOrNA(exported.CompletedAt)); err != nil {
			return err
		}
	}
	return table.Flush()
}
