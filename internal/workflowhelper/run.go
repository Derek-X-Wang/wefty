package workflowhelper

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// RunUsage is the whole `wefty run` surface. It is short on purpose: a
// workflow author should be able to read it once and never open the contract.
const RunUsage = `Usage: wefty run <envelope|step|gate|result|params> [flags]

Report from inside a running job by writing run-mailbox events. The node agent
publishes them to the run ledger with the credential it already holds, so the
job needs none of its own.

  wefty run envelope --step STEP [--status succeeded|failed|partial]
                     [--summary TEXT] [--payload-file FILE | --payload-json-file FILE]
                     [--key KEY]
  wefty run step --name NAME [--end] [--summary TEXT]
  wefty run gate --name NAME --outcome pass|fail|error|skipped
                 [--evidence-file FILE] [--summary TEXT]
  wefty run result --file PATH [--status succeeded|failed|partial] [--summary TEXT]
  wefty run params [--json] [NAME]

--json prints the written event's path (for params, the JSON value).

Every subcommand reads WEFTY_RUN_DIR from the environment. wefty run result
also copies PATH to $WEFTY_HANDOFF_DIR/result.json when a handoff directory is
present. See docs/contracts/run-execution-context.md, "Run mailbox".
`

// ExecuteRun dispatches `wefty run ...`. It is deliberately reachable before
// the CLI opens a Fabric connection: the whole point of the mailbox is that
// reporting needs no cluster identity, and a job that had to dial the fabric to
// report would defeat it.
func ExecuteRun(args []string, jsonOutput bool, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return UsageError("a wefty run subcommand is required: envelope, step, gate, result or params")
	}
	if args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		_, err := io.WriteString(stdout, RunUsage)
		return err
	}
	// Every reporting subcommand refuses the same way and before any side
	// effect, so a job without a mailbox never half-reports.
	directory, err := runDirectory()
	if err != nil {
		return err
	}
	switch args[0] {
	case "envelope":
		return runEnvelope(directory, args[1:], jsonOutput, stdout)
	case "step":
		return runStep(directory, args[1:], jsonOutput, stdout)
	case "gate":
		return runGate(directory, args[1:], jsonOutput, stdout)
	case "result":
		return runResult(directory, args[1:], jsonOutput, stdout, stderr)
	case "params":
		return runParams(directory, args[1:], jsonOutput, stdout)
	default:
		return UsageError(fmt.Sprintf("unknown wefty run subcommand %q", args[0]))
	}
}

// runDirectory is the single place the absent-mailbox message comes from, so
// every subcommand refuses the same way.
func runDirectory() (string, error) {
	directory := strings.TrimSpace(os.Getenv(RunDirEnv))
	if directory == "" {
		return "", ErrNoRunDir
	}
	return directory, nil
}

// newFlagSet keeps flag errors on the caller's usage path instead of printing
// the flag package's own output to stderr and exiting.
func newFlagSet(name string) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.Usage = func() {}
	return flags
}

func parseFlags(flags *flag.FlagSet, args []string, jsonOutput *bool) error {
	flags.BoolVar(jsonOutput, "json", *jsonOutput, "print the written event path as JSON")
	if err := flags.Parse(args); err != nil {
		return UsageError(fmt.Sprintf("wefty run %s: %v", flags.Name(), err))
	}
	if flags.NArg() > 0 {
		return UsageError(fmt.Sprintf("wefty run %s does not take positional arguments (got %q)", flags.Name(), flags.Arg(0)))
	}
	return nil
}

func runEnvelope(directory string, args []string, jsonOutput bool, stdout io.Writer) error {
	flags := newFlagSet("envelope")
	step := flags.String("step", "", "the step this envelope reports")
	status := flags.String("status", "succeeded", "succeeded, failed or partial")
	summary := flags.String("summary", "", "one line of free text")
	key := flags.String("key", "", "the event's idempotency identity (default: its file name)")
	payloadFile := flags.String("payload-file", "", "a file whose contents become the envelope's text detail")
	payloadJSONFile := flags.String("payload-json-file", "", "a file whose JSON document becomes the envelope's payload")
	if err := parseFlags(flags, args, &jsonOutput); err != nil {
		return err
	}
	if strings.TrimSpace(*step) == "" {
		return UsageError("wefty run envelope requires --step")
	}
	if *payloadFile != "" && *payloadJSONFile != "" {
		return UsageError("wefty run envelope takes --payload-file or --payload-json-file, not both")
	}
	payload, format, err := readPayload(*payloadFile, *payloadJSONFile)
	if err != nil {
		return err
	}
	return emit(directory, Event{
		Kind: KindEnvelope, Step: *step, Status: *status, Summary: *summary, Key: *key,
		Payload: payload, PayloadFormat: format,
	}, jsonOutput, stdout)
}

func runStep(directory string, args []string, jsonOutput bool, stdout io.Writer) error {
	flags := newFlagSet("step")
	name := flags.String("name", "", "the step's name")
	end := flags.Bool("end", false, "the step ended (default: it started)")
	summary := flags.String("summary", "", "one line of free text")
	key := flags.String("key", "", "the event's idempotency identity (default: its file name)")
	if err := parseFlags(flags, args, &jsonOutput); err != nil {
		return err
	}
	status := "started"
	if *end {
		status = "ended"
	}
	return emit(directory, Event{Kind: KindStep, Name: *name, Status: status, Summary: *summary, Key: *key}, jsonOutput, stdout)
}

func runGate(directory string, args []string, jsonOutput bool, stdout io.Writer) error {
	flags := newFlagSet("gate")
	name := flags.String("name", "", "the gate's name")
	outcome := flags.String("outcome", "", "pass, fail, error or skipped")
	evidenceFile := flags.String("evidence-file", "", "a file whose contents become the gate's evidence")
	summary := flags.String("summary", "", "one line of free text")
	key := flags.String("key", "", "the event's idempotency identity (default: its file name)")
	if err := parseFlags(flags, args, &jsonOutput); err != nil {
		return err
	}
	if strings.TrimSpace(*outcome) == "" {
		return UsageError("wefty run gate requires --outcome (pass, fail, error or skipped)")
	}
	payload, format, err := readPayload(*evidenceFile, "")
	if err != nil {
		return err
	}
	return emit(directory, Event{
		Kind: KindGate, Name: *name, Outcome: *outcome, Summary: *summary, Key: *key,
		Payload: payload, PayloadFormat: format,
	}, jsonOutput, stdout)
}

// runResult carries the run's final document two ways on purpose: into the
// ledger as an event, and into the handoff directory under the result
// convention, which is what an operator reads off the node when a failing
// attempt retains its handoff.
func runResult(directory string, args []string, jsonOutput bool, stdout, stderr io.Writer) error {
	flags := newFlagSet("result")
	file := flags.String("file", "", "the result document to record")
	status := flags.String("status", "succeeded", "succeeded, failed or partial")
	summary := flags.String("summary", "", "one line of free text")
	key := flags.String("key", "", "the event's idempotency identity (default: its file name)")
	if err := parseFlags(flags, args, &jsonOutput); err != nil {
		return err
	}
	if strings.TrimSpace(*file) == "" {
		return UsageError("wefty run result requires --file")
	}
	raw, err := readBoundedFile(*file)
	if err != nil {
		return err
	}
	format := PayloadText
	if json.Valid(raw) {
		format = PayloadJSON
	}
	if handoff := strings.TrimSpace(os.Getenv(HandoffDirEnv)); handoff != "" {
		if err := copyResult(raw, handoff); err != nil {
			// A handoff copy that fails must not lose the event: the ledger
			// entry is the copy that survives a removed handoff directory.
			fmt.Fprintf(stderr, "wefty run result: %v\n", err)
		}
	}
	return emit(directory, Event{
		Kind: KindResult, Status: *status, Summary: *summary, Key: *key,
		Payload: raw, PayloadFormat: format,
	}, jsonOutput, stdout)
}

func copyResult(raw []byte, handoff string) error {
	if err := os.MkdirAll(handoff, 0o700); err != nil {
		return fmt.Errorf("create %s: %w", handoff, err)
	}
	target := filepath.Join(handoff, ResultFileName)
	if err := os.WriteFile(target, raw, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", target, err)
	}
	return nil
}

// runParams reads the params the agent delivered. A job never receives its own
// params in the environment, and reading them back over HTTP is exactly the
// credentialed call the mailbox exists to remove.
func runParams(directory string, args []string, jsonOutput bool, stdout io.Writer) error {
	flags := newFlagSet("params")
	flags.BoolVar(&jsonOutput, "json", jsonOutput, "print the parameter as JSON")
	name, err := parseWithPositional(flags, args, true)
	if err != nil {
		return err
	}
	params, err := readParams(directory)
	if err != nil {
		return err
	}
	if name == "" {
		return writeAllParams(stdout, params, jsonOutput)
	}
	value, present := params[name]
	if !present {
		// An absent param is an empty value, not an error: a shell reads it
		// with `$(wefty run params ref)` and decides for itself whether the
		// workflow needs it.
		if jsonOutput {
			_, err := io.WriteString(stdout, "null\n")
			return err
		}
		return nil
	}
	if jsonOutput {
		_, err := fmt.Fprintf(stdout, "%s\n", value)
		return err
	}
	_, err = fmt.Fprintf(stdout, "%s\n", scalar(value))
	return err
}

// parseWithPositional accepts flags on either side of the one positional
// argument these commands take. Go's flag package stops at the first
// non-flag word, so `wefty workflow init demo --lang ts` -- the form anyone
// actually types -- needs the tail parsed as flags too.
func parseWithPositional(flags *flag.FlagSet, args []string, optional bool) (string, error) {
	if err := flags.Parse(args); err != nil {
		return "", UsageError(fmt.Sprintf("wefty %s: %v", flags.Name(), err))
	}
	rest := flags.Args()
	if len(rest) == 0 {
		if optional {
			return "", nil
		}
		return "", UsageError(fmt.Sprintf("wefty %s requires one argument", flags.Name()))
	}
	positional := rest[0]
	if err := flags.Parse(rest[1:]); err != nil {
		return "", UsageError(fmt.Sprintf("wefty %s: %v", flags.Name(), err))
	}
	if flags.NArg() > 0 {
		return "", UsageError(fmt.Sprintf("wefty %s takes one argument (got %q and %q)", flags.Name(), positional, flags.Arg(0)))
	}
	return positional, nil
}

func writeAllParams(stdout io.Writer, params map[string]json.RawMessage, jsonOutput bool) error {
	if jsonOutput {
		document, err := json.Marshal(params)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(stdout, "%s\n", document)
		return err
	}
	names := make([]string, 0, len(params))
	for name := range params {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if _, err := fmt.Fprintf(stdout, "%s=%s\n", name, scalar(params[name])); err != nil {
			return err
		}
	}
	return nil
}

// readParams treats a missing params.json as no params. A run submitted
// without params, and one whose params were too large to deliver, both leave
// the file absent; neither is a workflow error.
func readParams(directory string) (map[string]json.RawMessage, error) {
	raw, err := os.ReadFile(filepath.Join(directory, paramsFileName))
	switch {
	case errors.Is(err, os.ErrNotExist):
		return map[string]json.RawMessage{}, nil
	case err != nil:
		return nil, err
	}
	if len(raw) > maxParamsBytes {
		return nil, fmt.Errorf("%s is larger than the %d byte params bound", paramsFileName, maxParamsBytes)
	}
	params := map[string]json.RawMessage{}
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, fmt.Errorf("%s is not a JSON object: %w", paramsFileName, err)
	}
	return params, nil
}

// scalar prints a string param as its text and everything else as its JSON, so
// `--ref=$(wefty run params ref)` never carries surrounding quotes.
func scalar(value json.RawMessage) string {
	var text string
	if err := json.Unmarshal(value, &text); err == nil {
		return text
	}
	return string(value)
}

func readPayload(textFile, jsonFile string) ([]byte, string, error) {
	switch {
	case jsonFile != "":
		raw, err := readBoundedFile(jsonFile)
		if err != nil {
			return nil, "", err
		}
		if !json.Valid(raw) {
			return nil, "", UsageError(fmt.Sprintf("%s is not a JSON document", jsonFile))
		}
		return raw, PayloadJSON, nil
	case textFile != "":
		raw, err := readBoundedFile(textFile)
		if err != nil {
			return nil, "", err
		}
		return raw, PayloadText, nil
	default:
		return nil, PayloadText, nil
	}
}

func readBoundedFile(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return raw, nil
}

func emit(directory string, event Event, jsonOutput bool, stdout io.Writer) error {
	path, err := Write(directory, event)
	if err != nil {
		return err
	}
	if jsonOutput {
		document, err := json.Marshal(struct {
			Event string `json:"event"`
		}{Event: path})
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(stdout, "%s\n", document)
		return err
	}
	return nil
}
