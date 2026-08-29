package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

const computerSubmissionUsage = "usage: wefty computers submission enable|disable|set-inflight COMPUTER_ID [--max-inflight LIMIT] [--policy-revision REVISION] [--submit-intent-revision REVISION] [--idempotency-key KEY]"

type optionalRevisionFlag struct {
	value int64
	set   bool
}

func (f *optionalRevisionFlag) Set(value string) error {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return errors.New("must be an integer")
	}
	f.value, f.set = parsed, true
	return nil
}

func (f *optionalRevisionFlag) String() string {
	if !f.set {
		return ""
	}
	return strconv.FormatInt(f.value, 10)
}

type computerSubmissionOutput struct {
	l1.ComputerSubmissionState
	MutationApplied                  bool `json:"mutation_applied"`
	RevocationCommittedBeforeSuccess bool `json:"revocation_committed_before_success"`
}

func executeComputers(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	if len(args) < 2 || args[0] != "submission" {
		return usageError(computerSubmissionUsage)
	}
	verb := args[1]
	if verb != "enable" && verb != "disable" && verb != "set-inflight" {
		return usageError(fmt.Sprintf("unknown computers submission command %q", verb))
	}
	args = moveFirstPositionalToEnd(args[2:])
	flags := flag.NewFlagSet("computers submission "+verb, flag.ContinueOnError)
	flags.SetOutput(stderr)
	var policyRevision, submitIntentRevision optionalRevisionFlag
	var maxInflight int
	var idempotencyKey string
	flags.Var(&policyRevision, "policy-revision", "admin policy revision observed before this CAS mutation")
	flags.Var(&submitIntentRevision, "submit-intent-revision", "Computer submission revision observed before this CAS mutation")
	flags.IntVar(&maxInflight, "max-inflight", 0, "maximum nonterminal Computer-root Lineages")
	flags.StringVar(&idempotencyKey, "idempotency-key", "", "stable mutation idempotency key")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 || strings.TrimSpace(flags.Arg(0)) == "" {
		return usageError(computerSubmissionUsage)
	}
	maxInflightSet := false
	flags.Visit(func(visited *flag.Flag) {
		if visited.Name == "max-inflight" {
			maxInflightSet = true
		}
	})
	if verb == "set-inflight" && !maxInflightSet {
		return usageError("computers submission set-inflight requires --max-inflight")
	}
	if verb != "set-inflight" && maxInflightSet {
		return usageError("--max-inflight is only valid with computers submission set-inflight")
	}
	if maxInflightSet && (maxInflight < 1 || maxInflight > 1000) {
		return usageError("--max-inflight must be between 1 and 1000")
	}
	if policyRevision.set && policyRevision.value < 1 {
		return usageError("--policy-revision must be positive")
	}
	if submitIntentRevision.set && submitIntentRevision.value < 0 {
		return usageError("--submit-intent-revision must be non-negative")
	}

	computerID := flags.Arg(0)
	current, err := clients.getComputerSubmission(ctx, computerID)
	if err != nil {
		return err
	}
	desiredEnabled := current.SubmitEnabled
	desiredMaxInflight := current.SubmitMaxInflight
	switch verb {
	case "enable":
		desiredEnabled = true
	case "disable":
		desiredEnabled = false
	case "set-inflight":
		desiredMaxInflight = maxInflight
	}
	desiredAlreadyObserved := desiredEnabled == current.SubmitEnabled && desiredMaxInflight == current.SubmitMaxInflight
	if desiredAlreadyObserved && !policyRevision.set && !submitIntentRevision.set {
		output := computerSubmissionOutput{ComputerSubmissionState: current}
		if jsonOutput {
			return writeJSON(stdout, output)
		}
		return writeComputerSubmissionOutput(stdout, output)
	}
	observedPolicyRevision := current.PolicyRevision
	if policyRevision.set {
		observedPolicyRevision = policyRevision.value
	}
	observedSubmitRevision := current.SubmitIntentRevision
	if submitIntentRevision.set {
		observedSubmitRevision = submitIntentRevision.value
	}
	request := l1.ComputerSubmissionRequest{
		PolicyRevision: observedPolicyRevision, SubmitIntentRevision: observedSubmitRevision,
		SubmitEnabled: desiredEnabled, SubmitMaxInflight: desiredMaxInflight,
	}
	idempotencyKey, err = ensureComputerSubmissionIdempotencyKey(idempotencyKey, computerID, request)
	if err != nil {
		return err
	}
	request.IdempotencyKey = idempotencyKey
	computer, revocationCommitted, err := clients.mutateComputerSubmission(ctx, computerID, request)
	if err != nil {
		return err
	}
	if !revocationCommitted {
		return errors.New("L1 submission mutation omitted its revocation-before-success receipt")
	}
	output := computerSubmissionOutput{
		ComputerSubmissionState: projectComputerSubmissionOutput(computer), MutationApplied: true,
		RevocationCommittedBeforeSuccess: true,
	}
	if jsonOutput {
		return writeJSON(stdout, output)
	}
	return writeComputerSubmissionOutput(stdout, output)
}

func ensureComputerSubmissionIdempotencyKey(value, computerID string, request l1.ComputerSubmissionRequest) (string, error) {
	if strings.TrimSpace(value) != "" {
		return ensureIdempotencyKey(value)
	}
	payload, err := json.Marshal(struct {
		ComputerID           string `json:"computer_id"`
		PolicyRevision       int64  `json:"policy_revision"`
		SubmitIntentRevision int64  `json:"submit_intent_revision"`
		SubmitEnabled        bool   `json:"submit_enabled"`
		SubmitMaxInflight    int    `json:"submit_max_inflight"`
	}{computerID, request.PolicyRevision, request.SubmitIntentRevision, request.SubmitEnabled, request.SubmitMaxInflight})
	if err != nil {
		return "", fmt.Errorf("encode Computer submission idempotency input: %w", err)
	}
	digest := sha256.Sum256(payload)
	return fmt.Sprintf("wefty-cli-computer-submission-%x", digest[:]), nil
}

func projectComputerSubmissionOutput(computer l1.Computer) l1.ComputerSubmissionState {
	state := l1.ComputerSubmissionState{
		ComputerID: computer.ComputerID, SubmitEnabled: computer.SubmitEnabled,
		SubmitIntentRevision: computer.SubmitIntentRevision, SubmitMaxInflight: computer.SubmitMaxInflight,
		PolicyRevision: computer.SubmitPolicyRevision, Status: computer.CurrentJob.Status,
	}
	if computer.CurrentJob.ServiceJob != nil {
		state.Ready = computer.CurrentJob.Ready
		state.PassUnavailable = l1.ComputerPassUnavailable(computer.CurrentJob.LastFailure)
	}
	return state
}

func writeComputerSubmissionOutput(writer io.Writer, output computerSubmissionOutput) error {
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "COMPUTER ID\tENABLED\tMAX INFLIGHT\tSUBMIT REVISION\tPOLICY REVISION\tREADY\tPASS UNAVAILABLE\tMUTATION APPLIED\tREVOCATION BEFORE SUCCESS"); err != nil {
		return err
	}
	passUnavailable := "N/A"
	if output.PassUnavailable != nil {
		passUnavailable = string(output.PassUnavailable.Code) + ": " + output.PassUnavailable.Message
	}
	if _, err := fmt.Fprintf(table, "%s\t%t\t%d\t%d\t%d\t%s\t%s\t%t\t%t\n",
		output.ComputerID, output.SubmitEnabled, output.SubmitMaxInflight,
		output.SubmitIntentRevision, output.PolicyRevision, boolOrNA(output.Ready),
		passUnavailable, output.MutationApplied, output.RevocationCommittedBeforeSuccess); err != nil {
		return err
	}
	return table.Flush()
}

func executeRuns(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] != "list" {
		return usageError("usage: wefty runs list --origin computer:COMPUTER_ID [--include-descendants] [--limit LIMIT] [--cursor CURSOR]")
	}
	flags := flag.NewFlagSet("runs list", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var origin, cursor string
	var includeDescendants bool
	var limit int
	flags.StringVar(&origin, "origin", "", "immutable Run origin, currently computer:COMPUTER_ID")
	flags.BoolVar(&includeDescendants, "include-descendants", false, "include chain descendants of matching roots")
	flags.IntVar(&limit, "limit", l3.DefaultComputerRunPageLimit, "Runs per page")
	flags.StringVar(&cursor, "cursor", "", "opaque cursor returned by the previous page")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return usageError("runs list does not accept positional arguments")
	}
	computerID, ok := strings.CutPrefix(origin, "computer:")
	if !ok || strings.TrimSpace(computerID) == "" || computerID == "self" || computerID != strings.TrimSpace(computerID) {
		return usageError("runs list requires --origin computer:COMPUTER_ID")
	}
	if limit < 1 || limit > l3.MaxComputerRunPageLimit {
		return usageError(fmt.Sprintf("--limit must be between 1 and %d", l3.MaxComputerRunPageLimit))
	}
	page, err := clients.listRunsByOrigin(ctx, origin, cursor, limit, includeDescendants)
	if err != nil {
		return err
	}
	if jsonOutput {
		return writeJSON(stdout, page)
	}
	return writeComputerOriginRuns(stdout, page)
}

func writeComputerOriginRuns(writer io.Writer, page l3.ComputerRunPage) error {
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "RUN ID\tPARENT\tSTATUS\tTRIGGER\tPRINCIPAL\tCOMPUTER\tATTEMPT\tSTORAGE GENERATION\tSUBMIT REVISION\tCREATED\tUPDATED"); err != nil {
		return err
	}
	for _, run := range page.Runs {
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			run.RunID, valueOrNA(run.ParentRunID), run.Status, run.Trigger.Type, run.Trigger.Principal,
			valueOrNA(run.Trigger.ComputerID), valueOrNA(run.Trigger.ComputerAttemptID),
			int64OrNA(run.Trigger.ComputerStorageGeneration), int64OrNA(run.Trigger.SubmitIntentRevision),
			run.CreatedAt.Format(time.RFC3339), run.UpdatedAt.Format(time.RFC3339)); err != nil {
			return err
		}
	}
	if err := table.Flush(); err != nil {
		return err
	}
	if page.NextCursor != "" {
		_, err := fmt.Fprintf(writer, "NEXT CURSOR\t%s\n", page.NextCursor)
		return err
	}
	return nil
}

func int64OrNA(value int64) string {
	if value == 0 {
		return "N/A"
	}
	return strconv.FormatInt(value, 10)
}
