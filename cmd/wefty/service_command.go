package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
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

const defaultServicePollInterval = time.Second

func executeServices(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usageError("usage: wefty services create|list|status|start|stop|restart|logs|remove")
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
	default:
		return usageError(fmt.Sprintf("unknown services command %q", args[0]))
	}
}

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
	var mode scriptMode
	var tags, interpreters stringListFlag
	var publishedPort optionalPortFlag
	flags.StringVar(&scriptPath, "script", "", "service script file")
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
	if strings.TrimSpace(scriptPath) == "" {
		return usageError("services create requires --script")
	}

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
	} else if idempotencyKey, err = validateIdempotencyKey(idempotencyKey); err != nil {
		return err
	}

	digest := sha256.Sum256(content)
	var port *int
	if publishedPort.set {
		value := publishedPort.value
		port = &value
	}
	spec := contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		DispatchKey:   idempotencyKey,
		Kind:          "process",
		Class:         contract.JobClassService,
		PublishedPort: port,
		Restart:       contract.RestartAlways,
		RoutingTags:   append([]string(nil), tags...),
		Execution: contract.ExecutionSpec{
			Executable: contract.ExecutableSpec{
				InlineBase64: base64.StdEncoding.EncodeToString(content),
				SHA256:       hex.EncodeToString(digest[:]),
				Interpreter:  append([]string(nil), interpreters...),
			},
			Argv:             []string{"wefty-service-" + filepath.Base(canonicalScriptPath)},
			WorkingDirectory: workingDirectory,
		},
	}
	if mode.value != nil {
		spec.Execution.Executable.Mode = *mode.value
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
		return usageError("usage: wefty services status JOB_ID")
	}
	job, err := clients.getService(ctx, args[0])
	if err != nil {
		return err
	}
	return writeServiceResult(stdout, job, jsonOutput)
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
	flags.DurationVar(&wait, "wait", 0, "wait up to this duration for observed completion")
	flags.DurationVar(&pollInterval, "poll-interval", defaultServicePollInterval, "wait polling interval")
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
	jobID := flags.Arg(0)
	job, err := clients.setServiceDesiredState(ctx, jobID, desired)
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
	var idempotencyKey string
	flags.StringVar(&idempotencyKey, "idempotency-key", "", "stable idempotency key for this restart request")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return usageError("usage: wefty services restart JOB_ID --idempotency-key KEY")
	}
	if strings.TrimSpace(idempotencyKey) == "" {
		return usageError("services restart requires --idempotency-key so retries cannot restart twice")
	}
	key, err := validateIdempotencyKey(idempotencyKey)
	if err != nil {
		return err
	}
	job, err := clients.restartService(ctx, flags.Arg(0), key)
	if err != nil {
		return err
	}
	return writeServiceResult(stdout, job, jsonOutput)
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
	var wait, pollInterval time.Duration
	flags.DurationVar(&wait, "wait", 0, "wait up to this duration for verified removal")
	flags.DurationVar(&pollInterval, "poll-interval", defaultServicePollInterval, "wait polling interval")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return usageError("usage: wefty services remove JOB_ID [--wait DURATION]")
	}
	if wait < 0 {
		return usageError("--wait cannot be negative")
	}
	if pollInterval <= 0 {
		return usageError("--poll-interval must be positive")
	}
	job, err := clients.removeService(ctx, flags.Arg(0))
	if err != nil {
		return err
	}
	if wait > 0 {
		job, err = waitForService(ctx, clients, job, wait, pollInterval, "removed", serviceRemovalComplete)
		if err != nil {
			return err
		}
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
