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
	return detail.Flush()
}
