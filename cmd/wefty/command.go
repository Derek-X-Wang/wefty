package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

func execute(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	switch args[0] {
	case "cancel":
		return executeCancel(ctx, clients, jsonOutput, args[1:], stdout)
	case "status":
		return executeStatus(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "whoami":
		if len(args) != 1 {
			return usageError("usage: wefty whoami")
		}
		person, err := clients.whoAmI(ctx)
		if err != nil {
			return err
		}
		return writeWhoAmI(stdout, person, jsonOutput)
	case "admin":
		return executeAdmin(ctx, clients, jsonOutput, args[1:], stdout)
	case "admins":
		return executeAdmins(ctx, clients, jsonOutput, args[1:], stdout)
	case "computers":
		return executeComputers(ctx, clients, jsonOutput, args[1:], stdout)
	case "nodes":
		return executeNodes(ctx, clients, jsonOutput, args[1:], stdout)
	case "jobs":
		return executeJobs(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "services":
		return executeServices(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "runs":
		return executeRuns(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "submit":
		return executeSubmit(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "rerun":
		return executeRerun(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "logs":
		return executeLogs(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "wait":
		return executeWait(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "results":
		return executeResults(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "inspect":
		return executeInspect(ctx, clients, jsonOutput, args[1:], stdout)
	case "drain":
		return executeDrain(ctx, clients, jsonOutput, args[1:], stdout)
	case "help", "-h", "--help":
		_, err := io.WriteString(stdout, rootUsage)
		return err
	default:
		return usageError(fmt.Sprintf("unknown command %q", args[0]))
	}
}

func writeWhoAmI(writer io.Writer, person l1.AuthenticatedPerson, jsonOutput bool) error {
	if jsonOutput {
		return writeJSON(writer, person)
	}
	_, err := fmt.Fprintf(writer, "FABRIC ID\tUSER ID\tDEVICE ID\tSEEN\n%s\t%s\t%s\t%s\n",
		person.FabricID, person.UserID, person.DeviceID, person.SeenAt.Format(time.RFC3339))
	return err
}

func executeAdmin(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout io.Writer) error {
	if len(args) == 2 && args[0] == "bootstrap" && strings.TrimSpace(args[1]) != "" {
		policy, err := clients.bootstrapAdmin(ctx, args[1])
		if err != nil {
			return err
		}
		return writeAdminPolicy(stdout, policy, jsonOutput)
	}
	if len(args) > 0 && args[0] == "policy" {
		return executeAdminPolicy(ctx, clients, jsonOutput, args[1:], stdout)
	}
	return usageError("usage: wefty admin bootstrap NONCE | wefty admin policy get|add|remove")
}

type runInspection struct {
	Run     contract.RunRecord   `json:"run"`
	Lineage l3.RunLineage        `json:"lineage"`
	Runs    []contract.RunRecord `json:"runs"`
	// Steps is the root run's step intervals, derived from its own envelopes.
	// It is what "how long did each part take" is answered with, and it is
	// computed here from the record already fetched rather than by asking the
	// ledger a second question.
	Steps     l3.RunSteps      `json:"steps"`
	Results   *runResults      `json:"results,omitempty"`
	Execution *l3.RunExecution `json:"execution,omitempty"`
	// FailureReason is why the root run failed, in one line: the ledger's
	// recorded reason, or for a run failed before the ledger recorded one,
	// the L1 job's own evidence. Empty for a run that did not fail.
	FailureReason string `json:"failure_reason,omitempty"`
}

// runResults says where a finished run's files are, how long they last, and
// whether its result document reached the ledger.
//
// The two halves are known differently, and the field names say so. Uploaded is
// observed: the ledger either holds the document or it does not, and that answer
// is exact. Everything about the files on the node is computed -- the node that
// ran the job is the only thing that knows what is actually on its disk, and
// nothing reads a file back off a node. What is knowable is the rule: files live
// in the run's handoff directory on the node that produced it, and the node
// sweeps them on the contract's retention window. A node configured with a
// different window, or one that evicted the run early because it ran out of
// room, will differ; the honest word for that is "scheduled", not "observed".
type runResults struct {
	Location      string     `json:"location"`
	NodeID        string     `json:"node_id,omitempty"`
	RetainedUntil *time.Time `json:"retained_until,omitempty"`
	Expired       bool       `json:"expired"`
	Observed      bool       `json:"observed"`
	// Uploaded reports that the ledger holds this run's result document, so it
	// can be read with `wefty results` from anywhere, with no node involved.
	Uploaded bool `json:"uploaded"`
	// UploadSkipReason names why a result that exists on the node did not
	// travel. It is empty both when the document was uploaded and when the run
	// simply wrote none.
	UploadSkipReason contract.ResultUploadSkipReason `json:"upload_skip_reason,omitempty"`
	Note             string                          `json:"note"`
}

// runStepsFor projects a run's step intervals for a reader.
//
// A terminal run is in no step. If its last bracket never closed -- a workload
// that crashed mid-step -- the interval stays in the list, unmatched and
// without a duration, but the run is not described as running and no end time
// is invented for it. The listing and the lineage suppress the current step for
// a terminal run the same way, and inspection must not be the one surface that
// disagrees with them.
func runStepsFor(run contract.RunRecord) l3.RunSteps {
	steps := l3.DeriveRunSteps(run.Envelopes)
	if runIsTerminal(run.Status) {
		steps.Current = ""
	}
	return steps
}

// runResultsFor is nil for a run that has not finished: there are no results to
// locate until there is an outcome.
func runResultsFor(run contract.RunRecord, now time.Time) *runResults {
	if run.FinishedAt == nil {
		return nil
	}
	retainedUntil := run.FinishedAt.Add(contract.DefaultResultRetention)
	return &runResults{
		Location:      "the run's handoff directory on the node that produced it",
		NodeID:        run.NodeID,
		RetainedUntil: &retainedUntil,
		Expired:       now.After(retainedUntil),
		Observed:      false,
		Note:          resultsNote,
	}
}

// observeUploadedResult folds in the one thing about a run's results that is
// not a computed rule. A run whose result was never uploaded is the common
// case, not an error, so a not-found answer leaves the block as it was.
func observeUploadedResult(results *runResults, result l3.RunResult, err error) error {
	if results == nil {
		return nil
	}
	if err != nil {
		// Only "there is no result" is an ordinary answer here. Anything else
		// -- unauthorized, unreachable, a broken ledger -- would otherwise
		// print as a confident "not uploaded", which is exactly the kind of
		// unknown this command must not launder into a result.
		var responseErr *apiResponseError
		if errors.As(err, &responseErr) && responseErr.APIError.Code == contract.ErrorNotFound {
			return nil
		}
		return err
	}
	results.Uploaded = len(result.Document) > 0 && result.SkipReason == ""
	results.UploadSkipReason = result.SkipReason
	return nil
}

// resultsNote says what the block's two halves mean, including the case the
// ledger cannot describe: a node that could not upload at all leaves no row,
// so absence here is not proof the run produced nothing.
const resultsNote = "files are scheduled under the default retention window," +
	" which is a schedule rather than a guarantee: a node over its" +
	" retained-results budget gives files up earlier, published runs first." +
	" An uploaded result document is read with `wefty results` either way." +
	" A run with no result here either wrote none or could not upload one;" +
	" the node that ran it records which, beside its retained files"

// executeResults reads the result document the run uploaded when it finished.
//
// The document is the run's own, byte for byte as it wrote it, so the default
// output writes it straight to stdout or to a file: a result is usually input
// to the next thing, not something to read. --json wraps it in the provenance a
// person needs to trust it -- which attempt produced it, its digest, when it
// arrived -- and is how you see that a run produced a result it could not
// upload, and where to go looking for it.
func executeResults(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	args = moveFirstPositionalToEnd(args)
	flags := flag.NewFlagSet("results", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var out string
	flags.StringVar(&out, "out", "", "write the result document to this file instead of stdout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return usageError("usage: wefty results RUN_ID [--out FILE]")
	}
	result, err := clients.getRunResult(ctx, flags.Arg(0))
	if err != nil {
		return err
	}
	if jsonOutput {
		return writeJSON(stdout, newResultDocumentView(result))
	}
	if result.SkipReason == contract.ResultUploadSkipAbsent {
		return fmt.Errorf("run %s wrote no result document", result.RunID)
	}
	if result.SkipReason != "" {
		// Not an empty document and not an error: the run produced a result
		// the node could not upload, and the useful answer names the reason
		// and the node rather than printing nothing.
		return fmt.Errorf("run %s produced a result that was not uploaded (%s); it is retained on the node that ran it",
			result.RunID, result.SkipReason)
	}
	if out != "" {
		if err := os.WriteFile(out, result.Document, 0o600); err != nil {
			return err
		}
		_, err := fmt.Fprintf(stdout, "%s\t%d bytes\tsha256:%s\n", out, len(result.Document), result.SHA256)
		return err
	}
	// Exactly the document's bytes and nothing else. A trailing newline added
	// for the terminal's benefit would make redirected stdout differ from
	// --out and from the digest the ledger stored, which is the one thing a
	// byte-for-byte contract cannot afford.
	_, err = stdout.Write(result.Document)
	return err
}

// resultDocumentView is the --json shape. Document stays raw JSON rather than a
// base64 string, because a caller piping this into jq wants the run's own
// object, not an encoding of it.
type resultDocumentView struct {
	RunID      string                          `json:"run_id"`
	AttemptID  string                          `json:"attempt_id"`
	Bytes      int                             `json:"bytes"`
	SHA256     string                          `json:"sha256,omitempty"`
	UploadedAt time.Time                       `json:"uploaded_at"`
	SkipReason contract.ResultUploadSkipReason `json:"skip_reason,omitempty"`
	Document   json.RawMessage                 `json:"document,omitempty"`
}

func newResultDocumentView(result l3.RunResult) resultDocumentView {
	view := resultDocumentView{
		RunID: result.RunID, AttemptID: result.AttemptID, Bytes: len(result.Document),
		SHA256: result.SHA256, UploadedAt: result.UploadedAt, SkipReason: result.SkipReason,
	}
	if json.Valid(result.Document) {
		view.Document = json.RawMessage(result.Document)
	}
	return view
}

func executeInspect(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout io.Writer) error {
	args = moveFirstPositionalToEnd(args)
	flags := flag.NewFlagSet("inspect", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var includeExecution bool
	flags.BoolVar(&includeExecution, "execution", false, "include L1 execution diagnostics")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return usageError("usage: wefty inspect RUN_ID [--execution]")
	}
	runID := flags.Arg(0)
	root, err := clients.getRun(ctx, runID)
	if err != nil {
		return err
	}
	lineage, err := clients.getRunLineage(ctx, runID)
	if err != nil {
		return err
	}
	inspection := runInspection{
		Run: root, Lineage: lineage, Runs: []contract.RunRecord{root},
		Steps:   runStepsFor(root),
		Results: runResultsFor(root, time.Now().UTC()),
	}
	if inspection.Results != nil {
		result, resultErr := clients.getRunResult(ctx, runID)
		if err := observeUploadedResult(inspection.Results, result, resultErr); err != nil {
			return err
		}
	}
	for _, descendant := range lineage.Descendants {
		record, err := clients.getRun(ctx, descendant.RunID)
		if err != nil {
			return fmt.Errorf("read descendant %s: %w", descendant.RunID, err)
		}
		inspection.Runs = append(inspection.Runs, record)
	}
	if includeExecution {
		execution, err := clients.getRunExecution(ctx, runID)
		if err != nil {
			return err
		}
		inspection.Execution = &execution
	}
	switch {
	case root.Status != contract.RunFailed:
	case root.FailureReason != "":
		inspection.FailureReason = root.FailureReason
	case inspection.Execution != nil:
		inspection.FailureReason = executionFailureReason(*inspection.Execution)
	default:
		inspection.FailureReason = runFailureReason(ctx, clients, root)
	}
	if jsonOutput {
		return writeJSON(stdout, inspection)
	}
	return writeRunInspection(stdout, inspection)
}

func executeNodes(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout io.Writer) error {
	if len(args) > 0 && args[0] == "list" {
		return executeNodesList(ctx, clients, jsonOutput, args[1:], stdout)
	}
	if len(args) == 2 && args[0] == "inspect" {
		node, err := clients.getNode(ctx, args[1])
		if err != nil {
			return err
		}
		if jsonOutput {
			return writeJSON(stdout, node)
		}
		return writeNodesTable(stdout, []l1.Node{node})
	}
	if len(args) > 0 && args[0] == "set-claims" {
		return executeSetNodeClaims(ctx, clients, jsonOutput, args[1:], stdout)
	}
	return usageError("usage: wefty nodes list [--state STATE] [--claims-enabled BOOL] [--capability KEY] [--cursor CURSOR] [--limit N] [--all] | wefty nodes inspect NODE_ID | wefty nodes set-claims NODE_ID --claims-enabled BOOL --intent-revision REVISION --reason REASON")
}

func executeSetNodeClaims(
	ctx context.Context,
	clients *apiClients,
	jsonOutput bool,
	args []string,
	stdout io.Writer,
) error {
	args = moveFirstPositionalToEnd(args)
	flags := flag.NewFlagSet("nodes set-claims", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var claimsEnabled explicitBoolFlag
	var intentRevision int64
	var reason string
	flags.Var(&claimsEnabled, "claims-enabled", "whether the node may claim new jobs (true or false)")
	flags.Int64Var(&intentRevision, "intent-revision", 0, "intent revision observed in nodes list")
	flags.StringVar(&reason, "reason", "", "operator reason recorded with the intent")
	if err := flags.Parse(args); err != nil {
		return usageError(err.Error())
	}
	if flags.NArg() != 1 {
		return usageError("usage: wefty nodes set-claims NODE_ID --claims-enabled BOOL --intent-revision REVISION --reason REASON")
	}
	seenRevision := false
	flags.Visit(func(visited *flag.Flag) {
		if visited.Name == "intent-revision" {
			seenRevision = true
		}
	})
	if !claimsEnabled.set {
		return usageError("nodes set-claims requires --claims-enabled=true or --claims-enabled=false")
	}
	if !seenRevision || intentRevision < 0 {
		return usageError("nodes set-claims requires a non-negative --intent-revision")
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return usageError("nodes set-claims requires --reason")
	}
	node, err := clients.setNodeClaims(ctx, flags.Arg(0), l1.NodeIntentRequest{
		ClaimsEnabled: claimsEnabled.value, IntentRevision: intentRevision, Reason: reason,
	})
	if err != nil {
		return err
	}
	if jsonOutput {
		return writeJSON(stdout, node)
	}
	return writeNodesTable(stdout, []l1.Node{node})
}

func executeSubmit(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("submit", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var workflowRef, scriptPath, params, paramsFile, envelopeSchema, envelopeSchemaFile, idempotencyKey string
	var maxRuntime int
	var maxCost float64
	var requiredEnvelope, dispatchAuthority, again bool
	var mode scriptMode
	var tags, interpreters stringListFlag
	var imageFlags imageFlagSet
	flags.StringVar(&workflowRef, "workflow-ref", "", "saved workflow reference (latest hashes by name: pin /vN or use --again after updating)")
	flags.StringVar(&scriptPath, "script", "", "inline script file")
	imageFlags.bind(flags)
	flags.Lookup("image").Usage += "; mutable tags hash by name: pin by digest or use --again after moving a tag"
	flags.StringVar(&params, "params", "", "params JSON object")
	flags.StringVar(&paramsFile, "params-file", "", "file containing params JSON")
	flags.Var(&tags, "tag", "routing tag (repeatable)")
	flags.IntVar(&maxRuntime, "max-runtime", 0, "maximum runtime in seconds")
	flags.Float64Var(&maxCost, "max-cost", 0, "maximum cost recorded on the run")
	flags.Var(&interpreters, "interpreter", "inline script interpreter argv entry (repeatable)")
	flags.Var(&mode, "mode", "inline script mode, such as 0755")
	flags.StringVar(&envelopeSchema, "envelope-schema", "", "envelope JSON schema")
	flags.StringVar(&envelopeSchemaFile, "envelope-schema-file", "", "file containing envelope JSON schema")
	flags.BoolVar(&requiredEnvelope, "required-envelope", false, "require a valid envelope")
	flags.BoolVar(&dispatchAuthority, "dispatch-authority", false,
		"this run dispatches child work, so deliver the in-job credentials (default: report through the run mailbox and hold none)")
	flags.StringVar(&idempotencyKey, "idempotency-key", "", "explicit replay key scoped per authenticated actor (overrides the derived key and --again)")
	flags.BoolVar(&again, "again", false, "deliberately create a fresh run instead of replaying the same request")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return usageError("submit does not accept positional arguments")
	}
	sources := 0
	for _, present := range []bool{workflowRef != "", scriptPath != "", imageFlags.reference != ""} {
		if present {
			sources++
		}
	}
	if sources != 1 {
		return usageError("submit requires exactly one of --workflow-ref, --script, or --image")
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
	paramsJSON, err := readJSONObject(params, paramsFile, true)
	if err != nil {
		return fmt.Errorf("params: %w", err)
	}
	envelopeJSON, err := readJSONObject(envelopeSchema, envelopeSchemaFile, false)
	if err != nil {
		return fmt.Errorf("envelope schema: %w", err)
	}
	image, resolvedTags, err := imageFlags.programAndTags(tags, contract.JobClassOneShot)
	if err != nil {
		return err
	}
	request := l3.CreateRunRequest{
		WorkflowRef: workflowRef, Image: image, Params: paramsJSON, Tags: resolvedTags,
		EnvelopeSchema: envelopeJSON, RequiredEnvelope: requiredEnvelope,
		DispatchAuthority: dispatchAuthority,
	}
	if maxRuntime < 0 || maxCost < 0 {
		return usageError("run limits cannot be negative")
	}
	if maxRuntime > 0 || maxCost > 0 {
		request.Limits = &contract.RunLimits{MaxRuntimeSeconds: maxRuntime, MaxCost: maxCost}
	}
	if scriptPath != "" {
		content, err := os.ReadFile(scriptPath)
		if err != nil {
			return fmt.Errorf("read inline script: %w", err)
		}
		digest := sha256.Sum256(content)
		request.InlineScript = &l3.InlineScriptInput{
			Content: string(content), SHA256: hex.EncodeToString(digest[:]), Interpreter: interpreters, Mode: mode.value,
		}
	}
	idempotencyKey, err = runRequestKey("submit", request, idempotencyKey, again)
	if err != nil {
		return err
	}
	accepted, err := clients.submitRun(ctx, request, idempotencyKey)
	if err != nil {
		return err
	}
	var warnings []string
	if warning := routingWarning(ctx, clients, runKind(image), resolvedTags); warning != "" {
		warnings = append(warnings, warning)
		if _, err := fmt.Fprintf(stderr, "wefty: warning: %s\n", warning); err != nil {
			return err
		}
	}
	return writeAccepted(stdout, accepted, warnings, jsonOutput)
}

func executeRerun(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	args = moveFirstPositionalToEnd(args)
	flags := flag.NewFlagSet("rerun", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var idempotencyKey string
	var again bool
	flags.StringVar(&idempotencyKey, "idempotency-key", "", "explicit replay key scoped per authenticated actor (overrides the derived key and --again)")
	flags.BoolVar(&again, "again", false, "deliberately create a fresh rerun instead of replaying the same request")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return usageError("usage: wefty rerun RUN_ID")
	}
	sourceRunID := strings.TrimSpace(flags.Arg(0))
	// The rerun protocol currently accepts no overrides: its entire request
	// is the source run, whose immutable inputs are copied by L3.
	key, err := runRequestKey("rerun", struct {
		SourceRunID string `json:"source_run_id"`
	}{sourceRunID}, idempotencyKey, again)
	if err != nil {
		return err
	}
	accepted, err := clients.rerun(ctx, sourceRunID, key)
	if err != nil {
		return err
	}
	return writeAccepted(stdout, accepted, nil, jsonOutput)
}

func executeLogs(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	args = moveFirstPositionalToEnd(args)
	flags := flag.NewFlagSet("logs", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var follow bool
	var pollInterval time.Duration
	var limit int
	flags.BoolVar(&follow, "follow", false, "poll until the run reaches a terminal state and logs are drained")
	flags.DurationVar(&pollInterval, "poll-interval", time.Second, "follow polling interval")
	flags.IntVar(&limit, "limit", l1.DefaultLogPageLimit, "events per poll")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return usageError("usage: wefty logs RUN_ID [--follow]")
	}
	if limit < 1 || limit > l1.MaxLogPageLimit {
		return usageError(fmt.Sprintf("--limit must be between 1 and %d", l1.MaxLogPageLimit))
	}
	if pollInterval <= 0 {
		return usageError("--poll-interval must be positive")
	}
	runID := flags.Arg(0)
	cursor := ""
	var truncation truncationAnnouncer
	var lastStatus contract.RunState
	announcedWaiting := false
	followStarted := time.Now()
	// stopped is how an interrupted follow ends: a caller's timeout or ^C
	// is not a failure to explain as "context canceled", it is the reader
	// leaving, and the useful thing to say is where the run was.
	stopped := func() error {
		if lastStatus == "" {
			return fmt.Errorf("stopped following run %s", runID)
		}
		return fmt.Errorf("stopped following run %s while it was %s", runID, lastStatus)
	}
	for {
		page, err := clients.getRunLogs(ctx, runID, cursor, limit)
		if err != nil {
			if follow && ctx.Err() != nil {
				return stopped()
			}
			return err
		}
		if jsonOutput {
			if follow {
				// stdout stays one event per line; the notice goes to stderr,
				// so a fully trimmed run never follows as a silent one.
				if err := truncation.announce(stderr, page.Truncation); err != nil {
					return err
				}
				for _, event := range page.Events {
					if err := writeJSONLine(stdout, event); err != nil {
						return err
					}
				}
			} else {
				return writeJSON(stdout, page)
			}
		} else {
			if err := truncation.announce(stderr, page.Truncation); err != nil {
				return err
			}
			if err := writeLogEvents(stdout, stderr, page.Events); err != nil {
				return err
			}
		}
		cursor = page.NextCursor
		if !follow {
			return nil
		}
		run, err := clients.getRun(ctx, runID)
		if err != nil {
			if ctx.Err() != nil {
				return stopped()
			}
			return err
		}
		lastStatus = run.Status
		if isTerminalRun(run.Status) && len(page.Events) == 0 {
			return nil
		}
		// A run no node has started produces no output, and following it
		// used to be indistinguishable from following a hung one (#604).
		// Say once, on stderr, what the silence is -- after a short grace,
		// so a run that is simply being dispatched is not narrated.
		if !announcedWaiting && queuedRunIsWaiting(run.Status) && time.Since(followStarted) >= followWaitingNoticeAfter {
			announcedWaiting = true
			if err := announceWaitingForNode(ctx, clients, stderr, runID, run.Status); err != nil {
				return err
			}
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return stopped()
		case <-timer.C:
		}
	}
}

// followWaitingNoticeAfter is how long a follow waits on a run no node has
// started before saying so. Dispatch and a claim take a second or two on a
// healthy cluster; past this the silence is worth explaining.
var followWaitingNoticeAfter = 3 * time.Second

// announceWaitingForNode is the one line a follower of a not-yet-started run
// reads, with L1's reason when no node could ever take it as things stand.
func announceWaitingForNode(ctx context.Context, clients *apiClients, stderr io.Writer, runID string, status contract.RunState) error {
	line := fmt.Sprintf("wefty: run %s is %s; waiting for a node to start it", runID, status)
	if reason, _ := unschedulableReason(ctx, clients, runID); reason != "" {
		line += " (no eligible node: " + reason + ")"
	}
	_, err := fmt.Fprintln(stderr, line)
	return err
}

func moveFirstPositionalToEnd(args []string) []string {
	if len(args) < 2 || strings.HasPrefix(args[0], "-") {
		return args
	}
	reordered := append([]string(nil), args[1:]...)
	return append(reordered, args[0])
}

func executeDrain(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("drain", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	revision := flags.Int64("revision", 0, "intent revision observed in nodes list or inspect")
	reason := flags.String("reason", "operator requested drain", "operator reason recorded with the intent")
	if err := flags.Parse(moveFirstPositionalToEnd(args)); err != nil {
		return usageError(err.Error())
	}
	if flags.NArg() != 1 {
		return usageError("usage: wefty drain NODE_ID [--revision REVISION] [--reason REASON]")
	}
	var observedRevision *int64
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "revision" {
			observedRevision = revision
		}
	})
	if observedRevision != nil && *revision < 0 {
		return usageError("drain requires a non-negative --revision")
	}
	if strings.TrimSpace(*reason) == "" {
		return usageError("drain requires a non-empty --reason")
	}
	node, err := clients.drainNode(ctx, flags.Arg(0), observedRevision, strings.TrimSpace(*reason))
	if err != nil {
		return err
	}
	if jsonOutput {
		return writeJSON(stdout, node)
	}
	return writeNodesTable(stdout, []l1.Node{node})
}

func isTerminalRun(state contract.RunState) bool {
	return state == contract.RunSucceeded || state == contract.RunFailed
}

func ensureIdempotencyKey(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value != "" {
		if len(value) > 255 {
			return "", usageError("idempotency key cannot exceed 255 characters")
		}
		return value, nil
	}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("generate idempotency key: %w", err)
	}
	return "wefty-cli-" + hex.EncodeToString(random), nil
}

func readJSONObject(inline, path string, required bool) (json.RawMessage, error) {
	if inline != "" && path != "" {
		return nil, errors.New("inline JSON and file flags are mutually exclusive")
	}
	var raw []byte
	if path != "" {
		var err error
		raw, err = os.ReadFile(path)
		if err != nil {
			return nil, err
		}
	} else if inline != "" {
		raw = []byte(inline)
	} else if required {
		raw = []byte(`{}`)
	} else {
		return nil, nil
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil || object == nil {
		return nil, errors.New("must be a JSON object")
	}
	return json.RawMessage(raw), nil
}

type stringListFlag []string

func (f *stringListFlag) Set(value string) error {
	if strings.TrimSpace(value) == "" {
		return errors.New("value must not be empty")
	}
	*f = append(*f, value)
	return nil
}

func (f *stringListFlag) String() string { return strings.Join(*f, ",") }

type explicitBoolFlag struct {
	value bool
	set   bool
}

func (f *explicitBoolFlag) Set(value string) error {
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return errors.New("must be true or false")
	}
	f.value = parsed
	f.set = true
	return nil
}

func (f *explicitBoolFlag) String() string {
	if !f.set {
		return ""
	}
	return strconv.FormatBool(f.value)
}

type scriptMode struct{ value *uint32 }

func (m *scriptMode) Set(value string) error {
	parsed, err := strconv.ParseUint(value, 0, 32)
	if err != nil || parsed > 0o7777 {
		return errors.New("mode must be an integer between 0 and 07777")
	}
	mode := uint32(parsed)
	m.value = &mode
	return nil
}

func (m *scriptMode) String() string {
	if m.value == nil {
		return ""
	}
	return fmt.Sprintf("%#o", *m.value)
}
