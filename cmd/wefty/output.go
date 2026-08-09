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
	if _, err := fmt.Fprintln(table, "NODE ID\tSTATE\tOS/ARCH\tAGENT VERSION\tTAGS\tLAST HEARTBEAT"); err != nil {
		return err
	}
	for _, node := range nodes {
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s/%s\t%s\t%s\t%s\n",
			node.NodeID, node.State, node.OS, node.Architecture, node.AgentVersion,
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
