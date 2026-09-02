package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

const (
	defaultServicePollInterval    = time.Second
	inlineServiceScriptLimitError = "script exceeds the 1 MiB inline limit; services take a small launcher script, not a binary"
)

func executeServices(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usageError(serviceUsage)
	}
	switch args[0] {
	case "create":
		return executeServiceCreate(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "list":
		return executeServiceList(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "status":
		return executeServiceStatus(ctx, clients, jsonOutput, args[1:], stdout)
	case "start":
		return executeServiceDesiredState(ctx, clients, jsonOutput, args[1:], stdout, stderr, contract.ServiceDesiredRunning)
	case "stop":
		return executeServiceDesiredState(ctx, clients, jsonOutput, args[1:], stdout, stderr, contract.ServiceDesiredStopped)
	case "restart":
		return executeServiceRestart(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "logs":
		return executeServiceLogs(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "remove":
		return executeServiceRemove(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "forget":
		return executeServiceForget(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "grants":
		return executeComputerGrants(ctx, clients, jsonOutput, args[1:], stdout)
	case "grant":
		return executeComputerGrant(ctx, clients, jsonOutput, args[1:], stdout, stderr, false)
	case "revoke":
		return executeComputerGrant(ctx, clients, jsonOutput, args[1:], stdout, stderr, true)
	case "takeover":
		return executeComputerTakeover(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "reimage":
		return executeComputerReimage(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "reset":
		return executeComputerReset(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "resize":
		return executeComputerResize(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "abort":
		return executeComputerAbort(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "submission":
		return executeComputerSubmission(ctx, clients, jsonOutput, args, stdout, stderr)
	case "backup", "restore", "clone", "custody":
		return executeComputerStorage(ctx, clients, jsonOutput, args, stdout, stderr)
	default:
		return usageError(fmt.Sprintf("unknown services command %q", args[0]))
	}
}

const serviceUsage = "usage: wefty services create|list|status|start|stop|restart|logs|remove|forget|reimage|reset|resize|abort|submission|grants|grant|revoke|takeover|backup|restore|clone|custody"

func executeServiceCreate(
	ctx context.Context,
	clients *apiClients,
	jsonOutput bool,
	args []string,
	stdout, stderr io.Writer,
) error {
	flags := flag.NewFlagSet("services create", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var scriptPath, idempotencyKey string
	var computer bool
	var computerName string
	var computerBackupCap int64
	var computerDiskBytes optionalInt64Flag
	var mode scriptMode
	var tags, interpreters stringListFlag
	var publishedPort optionalPortFlag
	var imageFlags imageFlagSet
	flags.StringVar(&scriptPath, "script", "", "service script file")
	flags.BoolVar(&computer, "computer", false, "create a durable Computer authority")
	flags.StringVar(&computerName, "name", "", "durable Computer name (requires --computer)")
	flags.Int64Var(&computerBackupCap, "backup-cap", 0, "maximum retained Computer Backups (requires --computer)")
	flags.Var(&computerDiskBytes, "disk-bytes", "fully allocated Computer disk budget (requires --computer)")
	imageFlags.bind(flags)
	flags.Var(&interpreters, "interpreter", "inline script interpreter argv entry (repeatable)")
	flags.Var(&mode, "mode", "inline script mode, such as 0755")
	flags.Var(&tags, "tag", "routing tag (repeatable)")
	flags.Var(&publishedPort, "published-port", "Fabric port to publish")
	flags.StringVar(&idempotencyKey, "idempotency-key", "", "stable service creation idempotency key")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return usageError("services create does not accept positional arguments")
	}
	if computer {
		computerArgs := make([]string, 0, len(args)-1)
		for _, arg := range args {
			if arg != "--computer" {
				computerArgs = append(computerArgs, arg)
			}
		}
		return executeComputerCreate(ctx, clients, jsonOutput, computerArgs, stdout, stderr)
	}
	if computerName != "" || computerBackupCap != 0 || computerDiskBytes.set {
		return usageError("--name, --backup-cap, and --disk-bytes require --computer")
	}
	if (strings.TrimSpace(scriptPath) == "") == (strings.TrimSpace(imageFlags.reference) == "") {
		return usageError("services create requires exactly one of --script or --image")
	}
	if imageFlags.reference == "" && imageFlags.nonRoutingOptionsSet() {
		return usageError("--argv, --working-directory, --mount, image limits, and --runtime-handler require --image")
	}
	if imageFlags.reference == "" && strings.TrimSpace(imageFlags.nodeID) != "" {
		return usageError("--node requires --image")
	}
	if imageFlags.reference != "" && (len(interpreters) > 0 || mode.value != nil) {
		return usageError("--interpreter and --mode apply only to --script")
	}
	var err error
	if idempotencyKey != "" {
		idempotencyKey, err = validateIdempotencyKey(idempotencyKey)
		if err != nil {
			return err
		}
	}
	var port *int
	if publishedPort.set {
		value := publishedPort.value
		port = &value
	}
	resolvedTags, err := pinnedImageTags(tags, imageFlags.nodeID, len(imageFlags.mounts) > 0)
	if err != nil {
		return err
	}
	spec := contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		Class:         contract.JobClassService,
		PublishedPort: port,
		Restart:       contract.RestartAlways,
		RoutingTags:   append([]string(nil), resolvedTags...),
	}
	if scriptPath != "" {
		content, err := os.ReadFile(scriptPath)
		if err != nil {
			return fmt.Errorf("read service script: %w", err)
		}
		if len(content) == 0 {
			return usageError("service script must not be empty")
		}
		canonicalScriptPath, err := filepath.Abs(scriptPath)
		if err != nil {
			return fmt.Errorf("resolve service script path: %w", err)
		}
		if resolved, resolveErr := filepath.EvalSymlinks(canonicalScriptPath); resolveErr == nil {
			canonicalScriptPath = resolved
		}
		workingDirectory, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("read working directory: %w", err)
		}
		if idempotencyKey == "" {
			idempotencyKey = serviceDispatchKey(canonicalScriptPath)
		}
		digest := sha256.Sum256(content)
		spec.Kind = contract.JobKindProcess
		spec.Execution = contract.ExecutionSpec{
			Executable: contract.ExecutableSpec{
				InlineBase64: base64.StdEncoding.EncodeToString(content),
				SHA256:       hex.EncodeToString(digest[:]),
				Interpreter:  append([]string(nil), interpreters...),
			},
			Argv:             []string{"wefty-service-" + filepath.Base(canonicalScriptPath)},
			WorkingDirectory: workingDirectory,
		}
		if mode.value != nil {
			spec.Execution.Executable.Mode = *mode.value
		}
	} else {
		program, tags, err := imageFlags.programAndTags(tags, contract.JobClassOneShot)
		if err != nil {
			return err
		}
		if clients == nil {
			return fmt.Errorf("service clients are not configured")
		}
		if program.Digest == nil {
			resolver := clients.images
			if resolver == nil {
				resolver = newRegistryResolver(nil)
			}
			digest, err := resolver.ResolveDigest(ctx, program.Reference)
			if err != nil {
				return fmt.Errorf("resolve service image: %w", err)
			}
			program.Digest = &digest
		}
		if err := contract.ValidateImageProgram(*program, contract.JobClassService); err != nil {
			return usageError(fmt.Sprintf("invalid service image program: %v", err))
		}
		if err := contract.ValidatePinnedRouting(*program, tags); err != nil {
			return usageError(fmt.Sprintf("invalid service image routing: %v", err))
		}
		if idempotencyKey == "" {
			idempotencyKey = serviceDispatchKey("image:" + program.Reference + "@" + *program.Digest)
		}
		spec.Kind = contract.JobKindOCI
		spec.RuntimeHandler = program.RuntimeHandler
		spec.RoutingTags = append([]string(nil), tags...)
		spec.Execution.OCI = &contract.OCIExecutionSpec{
			Image: contract.OCIImageSpec{
				Reference: program.Reference,
				Digest:    program.Digest,
			},
			Argv:             append([]string(nil), program.Argv...),
			WorkingDirectory: program.WorkingDirectory,
			Mounts:           append([]contract.OCIMount(nil), program.Mounts...),
			Limits:           program.Limits,
		}
	}
	if idempotencyKey, err = validateIdempotencyKey(idempotencyKey); err != nil {
		return err
	}
	spec.DispatchKey = idempotencyKey
	requestBody, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("encode service create request: %w", err)
	}
	if len(requestBody) > l1.MaxRequestBodyBytes {
		return usageError(inlineServiceScriptLimitError)
	}
	job, err := clients.createService(ctx, spec)
	if err != nil {
		return err
	}
	return writeServiceResult(stdout, job, jsonOutput)
}

func executeServiceList(
	ctx context.Context,
	clients *apiClients,
	jsonOutput bool,
	args []string,
	stdout, stderr io.Writer,
) error {
	flags := flag.NewFlagSet("services list", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var cursor string
	var limit int
	flags.StringVar(&cursor, "cursor", "", "opaque cursor from the previous page")
	flags.IntVar(&limit, "limit", l1.DefaultJobPageLimit, "services per page")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return usageError("services list does not accept positional arguments")
	}
	if limit < 1 || limit > l1.MaxJobPageLimit {
		return usageError(fmt.Sprintf("--limit must be between 1 and %d", l1.MaxJobPageLimit))
	}
	page, err := clients.listServices(ctx, cursor, limit)
	if err != nil {
		return err
	}
	return writeServiceList(stdout, page, jsonOutput)
}

func executeServiceStatus(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout io.Writer) error {
	if len(args) != 1 {
		return usageError("usage: wefty services status JOB_ID|COMPUTER_ID")
	}
	job, computer, err := resolveServiceTarget(ctx, clients, args[0])
	if err != nil {
		return err
	}
	if computer != nil {
		return writeComputerProjection(stdout, newComputerProjection(*computer, nil, nil), jsonOutput)
	}
	return writeServiceResult(stdout, *job, jsonOutput)
}

func resolveServiceTarget(ctx context.Context, clients *apiClients, target string) (*l1.Job, *l1.Computer, error) {
	job, err := clients.getService(ctx, target)
	if err == nil {
		if job.ComputerID == "" {
			return &job, nil, nil
		}
		computer, computerErr := clients.getComputer(ctx, job.ComputerID)
		if computerErr != nil {
			return nil, nil, computerErr
		}
		return &job, &computer, nil
	}
	var responseErr *apiResponseError
	if !errors.As(err, &responseErr) || responseErr.APIError.Code != contract.ErrorNotFound {
		return nil, nil, err
	}
	computer, computerErr := clients.getComputer(ctx, target)
	if computerErr != nil {
		return nil, nil, computerErr
	}
	return nil, &computer, nil
}

func computerMutationFlagsSet(mutation computerMutationFlags) bool {
	return mutation.expectCurrent || mutation.intentRevision.set || mutation.storageGeneration.set ||
		strings.TrimSpace(mutation.storageID) != ""
}

func executeServiceDesiredState(
	ctx context.Context,
	clients *apiClients,
	jsonOutput bool,
	args []string,
	stdout, stderr io.Writer,
	desired contract.ServiceDesiredState,
) error {
	args = moveFirstPositionalToEnd(args)
	verb := "start"
	if desired == contract.ServiceDesiredStopped {
		verb = "stop"
	}
	flags := flag.NewFlagSet("services "+verb, flag.ContinueOnError)
	flags.SetOutput(stderr)
	var wait, pollInterval time.Duration
	var mutation computerMutationFlags
	flags.DurationVar(&wait, "wait", 0, "wait up to this duration for observed completion")
	flags.DurationVar(&pollInterval, "poll-interval", defaultServicePollInterval, "wait polling interval")
	mutation.bind(flags, false)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return usageError("usage: wefty services " + verb + " JOB_ID [--wait DURATION]")
	}
	if wait < 0 {
		return usageError("--wait cannot be negative")
	}
	if pollInterval <= 0 {
		return usageError("--poll-interval must be positive")
	}
	if computerMutationFlagsSet(mutation) {
		if err := mutation.validate(false); err != nil {
			return err
		}
	}
	jobID := flags.Arg(0)
	jobTarget, computer, err := resolveServiceTarget(ctx, clients, jobID)
	if err != nil {
		return err
	}
	if computer != nil {
		if wait != 0 || pollInterval != defaultServicePollInterval {
			return usageError("--wait and --poll-interval are not valid for Computer lifecycle mutations")
		}
		if validateErr := mutation.validate(false); validateErr != nil {
			return validateErr
		}
		precondition, resolveErr := mutation.resolve(ctx, clients, computer.ComputerID)
		if resolveErr != nil {
			return resolveErr
		}
		updated, receipt, mutationErr := clients.setComputerDesiredState(ctx, computer.ComputerID, l1.ComputerDesiredStateRequest{
			ComputerMutationPrecondition: precondition, DesiredState: desired,
		})
		if mutationErr != nil {
			return mutationErr
		}
		return writeComputerMutation(stdout, updated, receipt, jsonOutput)
	}
	if computerMutationFlagsSet(mutation) {
		return usageError("Computer CAS flags are valid only for a Computer-owned service")
	}
	job, err := clients.setServiceDesiredState(ctx, jobTarget.JobID, desired)
	if err != nil {
		return err
	}
	if wait > 0 {
		predicate := func(current l1.Job) bool { return current.State == contract.JobRunning }
		description := "running"
		if desired == contract.ServiceDesiredStopped {
			predicate = serviceIsQuiescent
			description = "stopped"
		}
		job, err = waitForService(ctx, clients, job, wait, pollInterval, description, predicate)
		if err != nil {
			return err
		}
	}
	return writeServiceResult(stdout, job, jsonOutput)
}

func executeServiceRestart(
	ctx context.Context,
	clients *apiClients,
	jsonOutput bool,
	args []string,
	stdout, stderr io.Writer,
) error {
	args = moveFirstPositionalToEnd(args)
	flags := flag.NewFlagSet("services restart", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var mutation computerMutationFlags
	mutation.bind(flags, true)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return usageError("usage: wefty services restart JOB_ID --idempotency-key KEY")
	}
	if strings.TrimSpace(mutation.idempotencyKey) == "" {
		return usageError("services restart requires --idempotency-key so retries cannot restart twice")
	}
	if computerMutationFlagsSet(mutation) {
		if err := mutation.validate(true); err != nil {
			return err
		}
	}
	job, computer, err := resolveServiceTarget(ctx, clients, flags.Arg(0))
	if err != nil {
		return err
	}
	if computer != nil {
		if validateErr := mutation.validate(false); validateErr != nil {
			return validateErr
		}
		precondition, resolveErr := mutation.resolve(ctx, clients, computer.ComputerID)
		if resolveErr != nil {
			return resolveErr
		}
		key, keyErr := mutation.key()
		if keyErr != nil {
			return keyErr
		}
		updated, receipt, mutationErr := clients.restartComputer(ctx, computer.ComputerID, l1.ComputerRestartRequest{
			ComputerMutationPrecondition: precondition, IdempotencyKey: key,
		})
		if mutationErr != nil {
			return mutationErr
		}
		return writeComputerMutation(stdout, updated, receipt, jsonOutput)
	}
	if computerMutationFlagsSet(mutation) {
		return usageError("Computer CAS flags are valid only for a Computer-owned service")
	}
	key, err := validateIdempotencyKey(mutation.idempotencyKey)
	if err != nil {
		return err
	}
	updated, err := clients.restartService(ctx, job.JobID, key)
	if err != nil {
		return err
	}
	return writeServiceResult(stdout, updated, jsonOutput)
}

func executeServiceRemove(
	ctx context.Context,
	clients *apiClients,
	jsonOutput bool,
	args []string,
	stdout, stderr io.Writer,
) error {
	args = moveFirstPositionalToEnd(args)
	flags := flag.NewFlagSet("services remove", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var wait storageWaitFlags
	var mutation computerMutationFlags
	wait.pollInterval = defaultServicePollInterval
	wait.bind(flags)
	mutation.bind(flags, false)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return usageError("usage: wefty services remove JOB_ID [--wait DURATION]")
	}
	if err := wait.validate(flags); err != nil {
		return err
	}
	if computerMutationFlagsSet(mutation) {
		if err := mutation.validate(false); err != nil {
			return err
		}
	}
	jobID := flags.Arg(0)
	prior, computer, err := resolveServiceTarget(ctx, clients, jobID)
	if err != nil {
		return err
	}
	if computer != nil {
		precondition, resolveErr := mutation.resolve(ctx, clients, computer.ComputerID)
		if resolveErr != nil {
			return resolveErr
		}
		updated, receipt, mutationErr := clients.removeComputer(ctx, computer.ComputerID, l1.ComputerRemoveRequest{ComputerMutationPrecondition: precondition})
		if mutationErr != nil {
			return mutationErr
		}
		projection := newComputerProjection(updated, &receipt.Applied, &receipt.Replay)
		if wait.timeout > 0 {
			observed, observation, waitErr := waitForComputerRemoval(ctx, clients, computer.ComputerID, wait)
			if waitErr != nil && observed.ComputerID == "" {
				return waitErr
			}
			projection = newComputerProjection(observed, &receipt.Applied, &receipt.Replay)
			projection.Observation = &observation
			if waitErr != nil {
				return writeComputerProjectionThenError(stdout, projection, jsonOutput, waitErr)
			}
			if outcomeErr := awaitedComputerRemovalOutcome(observed); outcomeErr != nil {
				return writeComputerProjectionThenError(stdout, projection, jsonOutput, outcomeErr)
			}
		}
		return writeComputerProjection(stdout, projection, jsonOutput)
	}
	if computerMutationFlagsSet(mutation) {
		return usageError("Computer CAS flags are valid only for a Computer-owned service")
	}
	job, err := clients.removeService(ctx, prior.JobID)
	if err != nil {
		return err
	}
	if wait.timeout > 0 {
		job, err = waitForService(ctx, clients, job, wait.timeout, wait.pollInterval, "removed", serviceRemovalComplete)
		if err != nil {
			return err
		}
	}
	return writeServiceResultWithWorkingDirectory(stdout, job, prior.Spec.Execution.WorkingDirectory, jsonOutput)
}

func executeServiceForget(
	ctx context.Context,
	clients *apiClients,
	jsonOutput bool,
	args []string,
	stdout, stderr io.Writer,
) error {
	args = moveFirstPositionalToEnd(args)
	flags := flag.NewFlagSet("services forget", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var force bool
	flags.BoolVar(&force, "force", false, "waive cleanup proof without cancelling the deletion directive")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return usageError("usage: wefty services forget JOB_ID --force")
	}
	if !force {
		return usageError("services forget requires --force to waive cleanup verification")
	}
	job, err := clients.forceForgetService(ctx, flags.Arg(0))
	if err != nil {
		return err
	}
	return writeServiceResult(stdout, job, jsonOutput)
}

func executeServiceLogs(
	ctx context.Context,
	clients *apiClients,
	jsonOutput bool,
	args []string,
	stdout, stderr io.Writer,
) error {
	args = moveFirstPositionalToEnd(args)
	flags := flag.NewFlagSet("services logs", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var follow bool
	var followFor, pollInterval time.Duration
	var limit int
	flags.BoolVar(&follow, "follow", false, "keep polling across service attempts")
	flags.DurationVar(&followFor, "follow-for", 0, "stop following after this duration")
	flags.DurationVar(&pollInterval, "poll-interval", defaultServicePollInterval, "follow polling interval")
	flags.IntVar(&limit, "limit", l1.DefaultLogPageLimit, "events per poll")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return usageError("usage: wefty services logs JOB_ID [--follow]")
	}
	if limit < 1 || limit > l1.MaxLogPageLimit {
		return usageError(fmt.Sprintf("--limit must be between 1 and %d", l1.MaxLogPageLimit))
	}
	if pollInterval <= 0 {
		return usageError("--poll-interval must be positive")
	}
	if followFor < 0 {
		return usageError("--follow-for cannot be negative")
	}
	if followFor > 0 && !follow {
		return usageError("--follow-for requires --follow")
	}

	followCtx := ctx
	cancel := func() {}
	if followFor > 0 {
		followCtx, cancel = context.WithTimeout(ctx, followFor)
	}
	defer cancel()

	jobID := flags.Arg(0)
	cursor := ""
	lastAttemptID := ""
	for {
		page, err := clients.getServiceLogs(followCtx, jobID, cursor, limit)
		if err != nil {
			if followFor > 0 && ctx.Err() == nil && followCtx.Err() == context.DeadlineExceeded {
				return nil
			}
			return err
		}
		if jsonOutput {
			if follow {
				for _, event := range page.Events {
					if err := writeJSONLine(stdout, event); err != nil {
						return err
					}
				}
			} else {
				return writeJSON(stdout, page)
			}
		} else if err := writeServiceLogEvents(stdout, stderr, page.Events, &lastAttemptID); err != nil {
			return err
		}
		cursor = page.NextCursor
		if !follow {
			return nil
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-followCtx.Done():
			timer.Stop()
			if followFor > 0 && ctx.Err() == nil && followCtx.Err() == context.DeadlineExceeded {
				return nil
			}
			return followCtx.Err()
		case <-timer.C:
		}
	}
}

func waitForService(
	ctx context.Context,
	clients *apiClients,
	initial l1.Job,
	wait, pollInterval time.Duration,
	description string,
	predicate func(l1.Job) bool,
) (l1.Job, error) {
	if predicate(initial) {
		return initial, nil
	}
	waitCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	for {
		timer := time.NewTimer(pollInterval)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			if ctx.Err() != nil {
				return l1.Job{}, ctx.Err()
			}
			return l1.Job{}, fmt.Errorf("timed out after %s waiting for service %q to become %s", wait, initial.JobID, description)
		case <-timer.C:
		}
		job, err := clients.getService(waitCtx, initial.JobID)
		if err != nil {
			if ctx.Err() == nil && waitCtx.Err() == context.DeadlineExceeded {
				return l1.Job{}, fmt.Errorf("timed out after %s waiting for service %q to become %s", wait, initial.JobID, description)
			}
			return l1.Job{}, err
		}
		if predicate(job) {
			return job, nil
		}
	}
}

func serviceIsQuiescent(job l1.Job) bool {
	return job.State == contract.JobStopped ||
		(job.State == contract.JobFailed && job.DesiredState == contract.ServiceDesiredStopped && !job.SlotHeld)
}

func serviceRemovalComplete(job l1.Job) bool {
	return job.State == contract.JobRemovedVerified || job.State == contract.JobForgottenCleanupUnverified
}

func serviceDispatchKey(canonicalScriptPath string) string {
	digest := sha256.Sum256([]byte(canonicalScriptPath))
	return "wefty-service-" + hex.EncodeToString(digest[:])
}

func validateIdempotencyKey(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", usageError("idempotency key must not be empty")
	}
	if len(value) > 255 {
		return "", usageError("idempotency key cannot exceed 255 characters")
	}
	return value, nil
}

type optionalPortFlag struct {
	value int
	set   bool
}

func (port *optionalPortFlag) Set(value string) error {
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 || parsed > 65535 {
		return errors.New("published port must be an integer between 1 and 65535")
	}
	port.value = parsed
	port.set = true
	return nil
}

func (port *optionalPortFlag) String() string {
	if !port.set {
		return ""
	}
	return fmt.Sprint(port.value)
}
