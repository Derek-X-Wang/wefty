package main

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

//go:embed exec_wrapper.sh
var execWrapper string

// The interval is fixed in production. Tests shorten it at this workload seam
// while still executing the shipped wrapper through L1 and the node agent.
var execHeartbeatSeconds = "60"
var execPollInterval = time.Second

// execHeartbeatTick bounds how long the wrapper's liveness loop takes to see
// that the command ended, which delays the end of every run by up to one tick.
const execHeartbeatTick = 0.5

// execHeartbeatSchedule splits the heartbeat period into ticks of at most
// execHeartbeatTick seconds.
func execHeartbeatSchedule() (tick string, ticks int) {
	period, err := strconv.ParseFloat(execHeartbeatSeconds, 64)
	if err != nil || period <= execHeartbeatTick {
		return execHeartbeatSeconds, 1
	}
	return strconv.FormatFloat(execHeartbeatTick, 'f', -1, 64), int(math.Ceil(period / execHeartbeatTick))
}

// Outlast the phase-1 service restart, which must complete in under 30 seconds.
var execReadRecoveryBudget = 40 * time.Second
var execCancelSettlementBudget = l1.CancelSettlementTimeout

type execParams struct {
	execCapture
	Argv []string `json:"argv"`
}

func executeExec(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("exec", flag.ContinueOnError)
	flags.SetOutput(flagErrorOutput(stderr, jsonOutput))
	var tags stringListFlag
	var maxRuntime time.Duration
	var keepRunning, detach bool
	flags.BoolVar(&keepRunning, "keep-running", false, "stopping ends only watching; the run continues")
	flags.BoolVar(&detach, "detach", false, "return the run ID after acceptance without watching")
	flags.BoolVar(&jsonOutput, "json", jsonOutput, "stream one JSON event per line")
	flags.Var(&tags, "tag", "additional tag that narrows this machine's Pinned run (repeatable)")
	flags.DurationVar(&maxRuntime, "max-runtime", 0, "optional runtime cap (no cap by default)")
	flags.Usage = func() {
		fmt.Fprintln(flags.Output(), "Usage: wefty exec [--tag TAG] [--max-runtime D] [--keep-running | --detach] [--json] -- <command...>\nFreeze the checkout and local changes; run on this machine's node.\nStream output; exit 0 on pass or 10 on failure. Stopping cancels the run unless --keep-running is set.")
		flags.PrintDefaults()
	}
	if err := parseCommandFlags(flags, args, false); err != nil {
		return flagUsageError(err)
	}
	separator := len(args) - flags.NArg() - 1
	if separator < 0 || flags.NArg() == 0 || args[separator] != "--" {
		return usageError("usage: wefty exec [--tag TAG] [--max-runtime D] [--keep-running | --detach] [--json] -- <command...>")
	}
	if keepRunning && detach {
		return usageError("--keep-running and --detach are alternatives")
	}
	argv := flags.Args()
	for _, arg := range argv {
		if strings.ContainsRune(arg, 0) {
			return usageError("command arguments cannot contain NUL")
		}
	}
	seconds := int(maxRuntime / time.Second)
	if maxRuntime%time.Second != 0 {
		seconds++
	}
	if maxRuntime < 0 {
		return usageError("--max-runtime must not be negative")
	}
	if err := contract.ValidateLimitSeconds("limits.max_runtime_seconds", seconds); err != nil {
		return usageError(err.Error())
	}
	// Refuse oversized argv before doing a potentially expensive capture.
	preliminary, _ := json.Marshal(execParams{Argv: argv})
	if len(preliminary) > l3.MaxDispatchedRunParamsBytes {
		return usageError("exec params exceed the 64 KiB mailbox limit")
	}
	if clients.l3.address == "" {
		return usageError("this command requires --l3, which is not configured")
	}
	// Only exec restores default signal handling after the first signal.
	// A second Ctrl-C exits immediately; a later exec sweeps abandoned captures.
	if reset, ok := ctx.Value(execSignalResetKey{}).(context.CancelFunc); ok {
		go func() { <-ctx.Done(); reset() }()
	}
	sweepExecStorage(ctx, clients, stderr)
	node, err := resolveLocalNode(ctx, clients)
	if err != nil {
		return err
	}
	resolvedTags, err := pinnedImageTags(tags, node.NodeID, true)
	if err != nil {
		return usageError("exec --tag conflicts with this machine's stable-node tag")
	}
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	capture, err := captureExec(ctx, cwd)
	if err != nil {
		return err
	}
	params, err := json.Marshal(execParams{execCapture: capture, Argv: argv})
	if err != nil {
		return errorsWithCleanup(err, capture.Storage)
	}
	if len(params) > l3.MaxDispatchedRunParamsBytes {
		return errorsWithCleanup(usageError("exec params exceed the 64 KiB mailbox limit"), capture.Storage)
	}
	key, err := ensureIdempotencyKey("")
	if err != nil {
		return errorsWithCleanup(err, capture.Storage)
	}
	script := buildExecWrapper(capture, argv)
	digest := sha256.Sum256([]byte(script))
	mode := uint32(0700)
	request := l3.CreateRunRequest{Params: params, Tags: resolvedTags, InlineScript: &l3.InlineScriptInput{Content: script, SHA256: hex.EncodeToString(digest[:]), Interpreter: []string{"/bin/bash"}, Mode: &mode}}
	if seconds > 0 {
		request.Limits = &contract.RunLimits{MaxRuntimeSeconds: seconds}
	}
	if ctx.Err() != nil {
		return errorsWithCleanup(ctx.Err(), capture.Storage)
	}
	// Submission follows the caller's cancellation. If an acknowledgement is
	// lost, bounded retries reuse the same frozen request and random key.
	// After a signal, recovery is read-only: it must never create a new run.
	submitCtx, stop := context.WithTimeout(ctx, 30*time.Second)
	accepted, uncertain, err := submitExecRun(submitCtx, clients, request, key, stderr)
	stop()
	if err != nil {
		if !uncertain {
			return errorsWithCleanup(err, capture.Storage)
		}
		if ctx.Err() != nil {
			recovered, recoverErr := recoverExecAcceptance(clients, capture.Storage)
			if recoverErr == nil {
				recordExecStorageRun(capture.Storage, recovered, stderr)
				if detach {
					return printExecDetached(stdout, jsonOutput, recovered)
				}
				if keepRunning {
					return endExecObservation(clients, capture, contract.RunRecord{RunID: recovered}, jsonOutput, stdout, stderr, ctx.Err())
				}
				return cancelExecObservation(clients, capture, recovered, jsonOutput, stdout, stderr, ctx.Err())
			}
		}
		fmt.Fprintf(stderr, "wefty: snapshot retained at %s (key %s)\n", capture.Storage, key)
		return &unavailableError{cause: fmt.Errorf("exec submission could not be confirmed (key %s); snapshot retained at %s: %w", key, capture.Storage, err)}
	}
	runID := accepted.RunID
	recordExecStorageRun(capture.Storage, runID, stderr)
	if detach {
		return printExecDetached(stdout, jsonOutput, runID)
	}
	if _, err = fmt.Fprintf(stderr, "wefty: run %s\n", runID); err != nil {
		return endExecObservation(clients, capture, contract.RunRecord{RunID: runID}, jsonOutput, stdout, stderr, err)
	}
	if ctx.Err() != nil {
		if keepRunning {
			return endExecObservation(clients, capture, contract.RunRecord{RunID: runID}, jsonOutput, stdout, stderr, ctx.Err())
		}
		return cancelExecObservation(clients, capture, runID, jsonOutput, stdout, stderr, ctx.Err())
	}
	record, err := observeExecRunOutput(ctx, clients, runID, jsonOutput, stdout, stderr)
	if err != nil && !runIsTerminal(record.Status) {
		if ctx.Err() != nil && !keepRunning {
			return cancelExecObservation(clients, capture, runID, jsonOutput, stdout, stderr, ctx.Err())
		}
		if record.RunID == "" {
			record.RunID = runID
		}
		return endExecObservation(clients, capture, record, jsonOutput, stdout, stderr, err)
	}
	if err != nil && (jsonOutput || ctx.Err() == nil) {
		fmt.Fprintf(stderr, "wefty: warning: could not drain logs for terminal run %s: %v\n", runID, err)
	}
	result := reportExecFinished(ctx, clients, record, capture, jsonOutput, stdout, stderr)
	if err = cleanupExecStorage(capture.Storage); err != nil {
		fmt.Fprintf(stderr, "wefty: warning: %v\n", err)
	}
	return result
}

func buildExecWrapper(capture execCapture, argv []string) string {
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }
	var input strings.Builder
	tick, ticks := execHeartbeatSchedule()
	fmt.Fprintf(&input, "storage=%s\nfolder=%s\nheartbeat_tick=%s\nheartbeat_ticks=%d\ncommand=()\n", quote(capture.Storage), quote(capture.Folder), quote(tick), ticks)
	// Scalar assignment avoids Bash 3.2 compound-array parsing of control bytes.
	for i, arg := range argv {
		fmt.Fprintf(&input, "command[%d]=%s\n", i, quote(arg))
	}
	return strings.Replace(execWrapper, "__WEFTY_EXEC_INPUT__", input.String(), 1)
}

func submitExecRun(ctx context.Context, clients *apiClients, request l3.CreateRunRequest, key string, stderr io.Writer) (acceptedOutput, bool, error) {
	var accepted acceptedOutput
	var err error
	uncertain := false
	announcedRecovery := false
	for attempt := 0; attempt < 4; attempt++ {
		if ctx.Err() != nil {
			return accepted, uncertain, ctx.Err()
		}
		accepted, err = clients.submitRun(ctx, request, key)
		if err == nil && accepted.RunID != "" {
			return accepted, false, nil
		}
		var response *apiResponseError
		if !errors.As(err, &response) {
			uncertain = true
			if err == nil {
				err = fmt.Errorf("submission acknowledgement has no run ID")
			}
		}
		// L3 can commit the run and then fail its response read. A server
		// internal error therefore leaves acceptance uncertain too.
		if response != nil && response.StatusCode >= 500 && response.APIError.Code != contract.ErrorUnavailable {
			uncertain = true
		}
		if uncertain && !announcedRecovery {
			fmt.Fprintf(stderr, "wefty: submission reply lost; recovering acceptance for key %s\n", key)
			announcedRecovery = true
		}
		if ctx.Err() != nil {
			return accepted, uncertain, ctx.Err()
		}
		if response != nil && response.StatusCode < 500 && !isRetryableL1Answer(err) {
			return accepted, uncertain, err
		}
		if attempt < 3 {
			if e := clients.wait(ctx, readRetryBackoff(attempt)); e != nil {
				return accepted, uncertain, e
			}
		}
	}
	return accepted, uncertain, err
}

// The capture's random private path identifies this call in its immutable
// params. An interrupted POST may have committed; find it with public reads
// rather than resubmitting after the caller has asked to stop.
func recoverExecAcceptance(clients *apiClients, storage string) (string, error) {
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	for {
		record, found, err := findExecAcceptance(ctx, clients, storage)
		if err != nil {
			return "", err
		}
		if found {
			return record.RunID, nil
		}
		if err := clients.wait(ctx, 100*time.Millisecond); err != nil {
			return "", err
		}
	}
}

// One complete public-read pass is shared by lost-reply recovery and the
// storage sweep. A successful miss is distinct from an unverifiable capture.
func findExecAcceptance(ctx context.Context, clients *apiClients, storage string) (contract.RunRecord, bool, error) {
	cursor := ""
	for {
		page, err := clients.listRunsPage(ctx, "", 100, cursor, true)
		if err != nil {
			return contract.RunRecord{}, false, err
		}
		for _, run := range page.Runs {
			record, err := clients.getRun(ctx, run.RunID)
			if err != nil {
				return contract.RunRecord{}, false, err
			}
			var params execParams
			if json.Unmarshal(record.Params, &params) == nil && params.Storage == storage {
				return record, true, nil
			}
		}
		if page.NextCursor == "" {
			return contract.RunRecord{}, false, nil
		}
		cursor = page.NextCursor
	}
}

func cancelExecRun(clients *apiClients, runID, storage string, cause error, stderr io.Writer) error {
	// Feedback must precede the network call, including a stalled L3.
	fmt.Fprintf(stderr, "wefty: cancelling run %s\n", runID)
	ctx, stop := context.WithTimeout(context.Background(), execCancelSettlementBudget)
	defer stop()
	var record contract.RunRecord
	err := clients.l3.do(ctx, http.MethodPost, "/v1/runs/"+url.PathEscape(runID)+"/cancel", nil, nil, &record, http.StatusOK)
	if err != nil {
		fmt.Fprintf(stderr, "wefty: snapshot retained at %s\n", storage)
		return fmt.Errorf("could not confirm cancellation of run %s; snapshot retained at %s: %w", runID, storage, err)
	}
	// L1 reserves cancellation independently of the eventual process result.
	// Terminal observation below serves only storage cleanup, not the verdict.
	fmt.Fprintf(stderr, "wefty: cancelled run %s\n", runID)
	for !runIsTerminal(record.Status) {
		if err = clients.wait(ctx, 100*time.Millisecond); err != nil {
			break
		}
		record, err = readExec(ctx, clients, func(readCtx context.Context) (contract.RunRecord, error) {
			return clients.getRun(readCtx, runID)
		})
		if err != nil {
			break
		}
	}
	if err != nil {
		fmt.Fprintf(stderr, "wefty: warning: could not confirm run %s ended; snapshot retained at %s: %v\n", runID, storage, err)
	} else if err = cleanupExecStorage(storage); err != nil {
		fmt.Fprintf(stderr, "wefty: warning: %v\n", err)
	}
	return fmt.Errorf("cancelled run %s: %w", runID, cause)
}

func printExecDetached(stdout io.Writer, jsonOutput bool, runID string) error {
	if jsonOutput {
		return writeJSONLine(stdout, struct {
			RunID string `json:"run_id"`
		}{runID})
	}
	_, err := fmt.Fprintln(stdout, runID)
	return err
}
