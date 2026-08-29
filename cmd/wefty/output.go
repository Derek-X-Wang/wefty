package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Derek-X-Wang/wefty/agent/managedroot"
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
	if _, err := fmt.Fprintln(table, "NODE ID\tREACHABILITY\tCLAIMS ENABLED (ELIGIBILITY)\tONE-SHOT SLOTS\tSERVICE SLOTS\tOVERCOMMITTED\tINTENT REVISION\tINTENT REASON\tINTENT ACTOR\tOS/ARCH\tAGENT VERSION\tTAGS\tLAST HEARTBEAT"); err != nil {
		return err
	}
	for _, node := range nodes {
		if _, err := fmt.Fprintf(table, "%s\t%s\t%t\t%d/%d\t%d/%d\t%t\t%d\t%s\t%s\t%s/%s\t%s\t%s\t%s\n",
			node.NodeID, node.State, node.ClaimsEnabled,
			node.OneshotOccupancy, node.MaxOneshotSlots, node.ServiceOccupancy, node.MaxServiceSlots,
			node.Overcommitted, node.IntentRevision, node.IntentReason, node.IntentActor,
			node.OS, node.Architecture, node.AgentVersion,
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

type serviceOutput struct {
	l1.Job
	Status                 string                       `json:"status"`
	DesiredState           contract.ServiceDesiredState `json:"desired_state"`
	BoundNodeID            string                       `json:"bound_node_id,omitempty"`
	Ready                  *bool                        `json:"ready"`
	ManagedDataPath        *string                      `json:"managed_data_path"`
	WorkingDirectory       *string                      `json:"working_directory"`
	WorkingDirectoryPolicy string                       `json:"working_directory_policy"`
}

type serviceListOutput struct {
	Jobs       []serviceOutput `json:"jobs"`
	NextCursor string          `json:"next_cursor,omitempty"`
}

func newServiceOutput(job l1.Job) serviceOutput {
	if job.ServiceJob == nil {
		job.ServiceJob = &l1.ServiceJob{}
	}
	desired := job.DesiredState
	boundNodeID := job.BoundNodeID
	if job.Removal != nil {
		desired = job.Removal.RemovalDesiredState
		boundNodeID = job.Removal.RemovalBoundNodeID
	}
	var managedDataPath *string
	if boundNodeID != "" {
		value := path.Join(
			"<managed-root>", "agent", "nodes", managedroot.EncodeID(boundNodeID),
			"services", managedroot.EncodeID(job.JobID), "data",
		)
		managedDataPath = &value
	}
	var workingDirectory *string
	workingDirectoryPolicy := "external; never deleted"
	if job.Spec.Execution.WorkingDirectory != "" {
		value := job.Spec.Execution.WorkingDirectory
		workingDirectory = &value
	} else if job.Spec.Execution.OCI != nil {
		workingDirectory = job.Spec.Execution.OCI.WorkingDirectory
		workingDirectoryPolicy = "container path; image default when absent"
	}
	return serviceOutput{
		Job: job, Status: serviceDisplayStatus(job), DesiredState: desired,
		BoundNodeID: boundNodeID, Ready: job.Ready,
		ManagedDataPath: managedDataPath, WorkingDirectory: workingDirectory,
		WorkingDirectoryPolicy: workingDirectoryPolicy,
	}
}

func serviceDisplayStatus(job l1.Job) string {
	switch job.State {
	case contract.JobRemovedVerified:
		return "removed (agent-confirmed)"
	case contract.JobForgottenCleanupUnverified:
		return "forgotten (cleanup unverified)"
	default:
		if job.Status != "" {
			return job.Status
		}
		return string(job.State)
	}
}

func writeServiceResult(writer io.Writer, job l1.Job, jsonOutput bool) error {
	service := newServiceOutput(job)
	return writeServiceOutput(writer, service, jsonOutput)
}

func writeServiceResultWithWorkingDirectory(
	writer io.Writer,
	job l1.Job,
	workingDirectory string,
	jsonOutput bool,
) error {
	service := newServiceOutput(job)
	if workingDirectory != "" {
		service.WorkingDirectory = &workingDirectory
	}
	return writeServiceOutput(writer, service, jsonOutput)
}

func writeServiceOutput(writer io.Writer, service serviceOutput, jsonOutput bool) error {
	if jsonOutput {
		return writeJSON(writer, service)
	}
	return writeServicesTable(writer, []serviceOutput{service})
}

func writeServiceList(writer io.Writer, page l1.JobList, jsonOutput bool) error {
	services := make([]serviceOutput, 0, len(page.Jobs))
	for _, job := range page.Jobs {
		services = append(services, newServiceOutput(job))
	}
	if jsonOutput {
		return writeJSON(writer, serviceListOutput{Jobs: services, NextCursor: page.NextCursor})
	}
	if err := writeServicesTable(writer, services); err != nil {
		return err
	}
	if page.NextCursor != "" {
		_, err := fmt.Fprintf(writer, "NEXT CURSOR\t%s\n", page.NextCursor)
		return err
	}
	return nil
}

func writeServicesTable(writer io.Writer, services []serviceOutput) error {
	table := tabwriter.NewWriter(writer, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "KIND\tCOMPUTER ID\tJOB ID\tSTATE\tSTATUS\tDESIRED\tBOUND NODE\tNODE STATE\tATTEMPT\tHOLDS SLOT\tREADY\tPORT\tRESTART STREAK\tNEXT RESTART\tRESTART SUPPRESSED\tLAST FAILURE\tCREATED\tUPDATED\tMANAGED DATA\tWORKING DIRECTORY"); err != nil {
		return err
	}
	for _, service := range services {
		kind := "service"
		if service.ComputerID != "" {
			kind = "Computer"
		}
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%t\t%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			kind,
			valueOrNA(service.ComputerID),
			service.JobID,
			service.State,
			service.Status,
			service.DesiredState,
			valueOrNA(service.BoundNodeID),
			valueOrNA(string(service.NodeState)),
			valueOrNA(service.CurrentAttemptID),
			service.SlotHeld,
			boolOrNA(service.Ready),
			intOrNA(service.PublishedPort),
			service.RestartStreak,
			timeOrNA(service.NextRestartAt),
			valueOrNA(service.RestartSuppressed),
			jsonOrNA(service.LastFailure),
			service.CreatedAt.Format(time.RFC3339),
			service.UpdatedAt.Format(time.RFC3339),
			pointerOrNA(service.ManagedDataPath, " (deleted by remove)"),
			pointerOrNA(service.WorkingDirectory, " (external; never deleted)"),
		); err != nil {
			return err
		}
	}
	return table.Flush()
}

func writeServiceLogEvents(stdout, stderr io.Writer, events []contract.LogEvent, lastAttemptID *string) error {
	for _, event := range events {
		if event.AttemptID != *lastAttemptID {
			if _, err := fmt.Fprintf(stdout, "--- attempt %s ---\n", event.AttemptID); err != nil {
				return err
			}
			*lastAttemptID = event.AttemptID
		}
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

func valueOrNA(value string) string {
	if value == "" {
		return "N/A"
	}
	return value
}

func boolOrNA(value *bool) string {
	if value == nil {
		return "N/A"
	}
	return fmt.Sprint(*value)
}

func intOrNA(value *int) string {
	if value == nil {
		return "N/A"
	}
	return fmt.Sprint(*value)
}

func timeOrNA(value *time.Time) string {
	if value == nil {
		return "N/A"
	}
	return value.Format(time.RFC3339)
}

func jsonOrNA(value json.RawMessage) string {
	if len(value) == 0 {
		return "N/A"
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, value); err != nil {
		return string(value)
	}
	return compact.String()
}

func pointerOrNA(value *string, suffix string) string {
	if value == nil || *value == "" {
		return "N/A"
	}
	return *value + suffix
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
	var summary string
	switch {
	case result.ExitCode != nil:
		summary = fmt.Sprintf("exit %d", *result.ExitCode)
	case result.SpawnError != nil:
		summary = fmt.Sprintf("spawn %s: %s", result.SpawnError.Code, result.SpawnError.Message)
	case result.RuntimeFailure != nil:
		summary = fmt.Sprintf("runtime %s: %s", result.RuntimeFailure.Code, result.RuntimeFailure.Message)
	case result.OutputError != "":
		summary = "output error: " + result.OutputError
	case result.Signal != "":
		summary = fmt.Sprintf("signal %s (%s)", result.Signal, result.TerminationCause)
	default:
		summary = "unknown result"
	}
	if result.OOM {
		summary += " [oom]"
	}
	return summary
}
