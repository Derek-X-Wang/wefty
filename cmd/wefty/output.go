package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

func writeJSON(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func writeJSONLine(writer io.Writer, value any) error {
	return json.NewEncoder(writer).Encode(value)
}

func writeAccepted(writer io.Writer, accepted l3.RunAccepted, jsonOutput bool) error {
	if jsonOutput {
		return writeJSON(writer, accepted)
	}
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "RUN ID\tSTATUS URL\tLOGS URL"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(table, "%s\t%s\t%s\n", accepted.RunID, accepted.StatusURL, accepted.LogsURL); err != nil {
		return err
	}
	return table.Flush()
}

func writeNodesTable(writer io.Writer, nodes []l1.Node) error {
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "NODE ID\tSTATE\tCLAIMS\tOS/ARCH\tAGENT VERSION\tTAGS\tLAST HEARTBEAT"); err != nil {
		return err
	}
	for _, node := range nodes {
		if _, err := fmt.Fprintf(table, "%s\t%s\t%t\t%s/%s\t%s\t%s\t%s\n",
			node.NodeID, node.State, node.ClaimsEnabled, node.OS, node.Architecture, node.AgentVersion,
			strings.Join(node.AuthoritativeTags, ","), node.LastHeartbeatAt.Format("2006-01-02T15:04:05Z07:00")); err != nil {
			return err
		}
	}
	return table.Flush()
}

func writeLogEvents(stdout, stderr io.Writer, events []contract.LogEvent) error {
	for _, event := range events {
		writer := stdout
		if event.Stream == contract.LogStderr {
			writer = stderr
		}
		if _, err := writer.Write(event.Bytes); err != nil {
			return err
		}
	}
	return nil
}

func writeRunInspection(writer io.Writer, inspection runInspection) error {
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "RUN ID\tPARENT\tSTATUS\tENVELOPES\tGATES"); err != nil {
		return err
	}
	for _, run := range inspection.Runs {
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s\t%d\t%d\n", run.RunID, run.ParentRunID, run.Status, len(run.Envelopes), len(run.Gates)); err != nil {
			return err
		}
	}
	if err := table.Flush(); err != nil {
		return err
	}
	detail := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(detail, "KIND\tRUN\tSTEP\tSTATUS/OUTCOME\tSUMMARY/NAME"); err != nil {
		return err
	}
	for _, run := range inspection.Runs {
		for _, envelope := range run.Envelopes {
			if _, err := fmt.Fprintf(detail, "envelope\t%s\t%s\t%s\t%s\n", run.RunID, envelope.StepID, envelope.Status, envelope.Summary); err != nil {
				return err
			}
		}
		for _, gate := range run.Gates {
			if _, err := fmt.Fprintf(detail, "gate\t%s\t%s\t%s\t%s\n", run.RunID, gate.StepID, gate.Outcome, gate.Name); err != nil {
				return err
			}
		}
	}
	if err := detail.Flush(); err != nil {
		return err
	}
	if inspection.Execution == nil {
		return nil
	}
	return writeRunExecution(writer, *inspection.Execution)
}

func writeRunExecution(writer io.Writer, execution l3.RunExecution) error {
	if _, err := fmt.Fprintln(writer, "\nEXECUTION"); err != nil {
		return err
	}
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "L1 JOB ID\tDISPATCH ATTEMPTS\tDISPATCH ERROR"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(table, "%s\t%d\t%s\n", execution.L1JobID, execution.DispatchAttempts, formatAPIError(execution.DispatchError)); err != nil {
		return err
	}
	if execution.Job == nil {
		return table.Flush()
	}
	if _, err := fmt.Fprintln(table, "JOB STATE\tNODE\tCURRENT ATTEMPT"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(table, "%s\t%s\t%s\n", execution.Job.State, execution.Job.NodeID, execution.Job.CurrentAttemptID); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(table, "ATTEMPT ID\tNODE\tSTATE / EVIDENCE"); err != nil {
		return err
	}
	for _, attempt := range execution.Job.Attempts {
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s\n", attempt.AttemptID, attempt.NodeID, formatAttemptEvidence(attempt)); err != nil {
			return err
		}
	}
	return table.Flush()
}

func formatAPIError(apiError *contract.APIError) string {
	if apiError == nil {
		return ""
	}
	parts := []string{fmt.Sprintf("%s: %s", apiError.Code, apiError.Message), fmt.Sprintf("retryable=%t", apiError.Retryable)}
	if apiError.RequestID != "" {
		parts = append(parts, "request_id="+apiError.RequestID)
	}
	if len(apiError.Details) > 0 {
		if details, err := json.Marshal(apiError.Details); err == nil {
			parts = append(parts, "details="+string(details))
		}
	}
	return strings.Join(parts, "; ")
}

func formatAttemptEvidence(attempt l1.Attempt) string {
	if attempt.LateResult != nil {
		return string(attempt.State) + " — late evidence: " + formatLateResult(*attempt.LateResult)
	}
	if attempt.Result != nil {
		return string(attempt.State) + " — " + formatProcessResult(*attempt.Result)
	}
	return string(attempt.State)
}

func formatLateResult(evidence l1.LateResultEvidence) string {
	if evidence.Result != nil {
		return formatProcessResult(*evidence.Result)
	}
	if evidence.Gap != nil {
		return "unavailable (" + string(evidence.Gap.Reason) + ")"
	}
	return "unavailable"
}

func formatProcessResult(result l1.ProcessResult) string {
	switch {
	case result.ExitCode != nil:
		return fmt.Sprintf("exit %d", *result.ExitCode)
	case result.SpawnError != nil:
		return fmt.Sprintf("spawn %s: %s", result.SpawnError.Code, result.SpawnError.Message)
	case result.OutputError != "":
		return "output error: " + result.OutputError
	case result.Signal != "":
		return fmt.Sprintf("signal %s (%s)", result.Signal, result.TerminationCause)
	default:
		return "unknown result"
	}
}
