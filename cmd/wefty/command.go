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
	case "nodes":
		return executeNodes(ctx, clients, jsonOutput, args[1:], stdout)
	case "services":
		return executeServices(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "submit":
		return executeSubmit(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "rerun":
		return executeRerun(ctx, clients, jsonOutput, args[1:], stdout, stderr)
	case "logs":
		return executeLogs(ctx, clients, jsonOutput, args[1:], stdout, stderr)
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

type runInspection struct {
	Run       contract.RunRecord   `json:"run"`
	Lineage   l3.RunLineage        `json:"lineage"`
	Runs      []contract.RunRecord `json:"runs"`
	Execution *l3.RunExecution     `json:"execution,omitempty"`
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
	inspection := runInspection{Run: root, Lineage: lineage, Runs: []contract.RunRecord{root}}
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
	if jsonOutput {
		return writeJSON(stdout, inspection)
	}
	return writeRunInspection(stdout, inspection)
}

func executeNodes(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout io.Writer) error {
	if len(args) == 1 && args[0] == "list" {
		result, err := clients.listNodes(ctx)
		if err != nil {
			return err
		}
		if jsonOutput {
			return writeJSON(stdout, result)
		}
		return writeNodesTable(stdout, result.Nodes)
	}
	if len(args) > 0 && args[0] == "set-claims" {
		return executeSetNodeClaims(ctx, clients, jsonOutput, args[1:], stdout)
	}
	return usageError("usage: wefty nodes list | wefty nodes set-claims NODE_ID --claims-enabled BOOL --intent-revision REVISION --reason REASON")
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
		return err
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
	var requiredEnvelope bool
	var mode scriptMode
	var tags, interpreters stringListFlag
	flags.StringVar(&workflowRef, "workflow-ref", "", "saved workflow reference")
	flags.StringVar(&scriptPath, "script", "", "inline script file")
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
	flags.StringVar(&idempotencyKey, "idempotency-key", "", "request idempotency key")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return usageError("submit does not accept positional arguments")
	}
	if (workflowRef == "") == (scriptPath == "") {
		return usageError("submit requires exactly one of --workflow-ref or --script")
	}
	paramsJSON, err := readJSONObject(params, paramsFile, true)
	if err != nil {
		return fmt.Errorf("params: %w", err)
	}
	envelopeJSON, err := readJSONObject(envelopeSchema, envelopeSchemaFile, false)
	if err != nil {
		return fmt.Errorf("envelope schema: %w", err)
	}
	request := l3.CreateRunRequest{
		WorkflowRef: workflowRef, Params: paramsJSON, Tags: tags,
		EnvelopeSchema: envelopeJSON, RequiredEnvelope: requiredEnvelope,
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
	idempotencyKey, err = ensureIdempotencyKey(idempotencyKey)
	if err != nil {
		return err
	}
	accepted, err := clients.submitRun(ctx, request, idempotencyKey)
	if err != nil {
		return err
	}
	return writeAccepted(stdout, accepted, jsonOutput)
}

func executeRerun(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	args = moveFirstPositionalToEnd(args)
	flags := flag.NewFlagSet("rerun", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var idempotencyKey string
	flags.StringVar(&idempotencyKey, "idempotency-key", "", "request idempotency key")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return usageError("usage: wefty rerun RUN_ID")
	}
	key, err := ensureIdempotencyKey(idempotencyKey)
	if err != nil {
		return err
	}
	accepted, err := clients.rerun(ctx, flags.Arg(0), key)
	if err != nil {
		return err
	}
	return writeAccepted(stdout, accepted, jsonOutput)
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
	for {
		page, err := clients.getRunLogs(ctx, runID, cursor, limit)
		if err != nil {
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
		} else if err := writeLogEvents(stdout, stderr, page.Events); err != nil {
			return err
		}
		cursor = page.NextCursor
		if !follow {
			return nil
		}
		run, err := clients.getRun(ctx, runID)
		if err != nil {
			return err
		}
		if isTerminalRun(run.Status) && len(page.Events) == 0 {
			return nil
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func moveFirstPositionalToEnd(args []string) []string {
	if len(args) < 2 || strings.HasPrefix(args[0], "-") {
		return args
	}
	reordered := append([]string(nil), args[1:]...)
	return append(reordered, args[0])
}

func executeDrain(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout io.Writer) error {
	if len(args) != 1 {
		return usageError("usage: wefty drain NODE_ID")
	}
	node, err := clients.drainNode(ctx, args[0])
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
