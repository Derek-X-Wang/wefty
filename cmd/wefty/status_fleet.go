package main

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Fixed class counts keep status bounded even when a Node has many attempts.
// Service slot occupancy includes bindings in backoff; these counts describe
// only the active attempts L1 actually placed on the Node.
func activeAttemptsByClass(attempts []l1.NodeActiveAttempt) map[string]int {
	counts := map[string]int{contract.JobClassOneShot: 0, contract.JobClassService: 0}
	for _, attempt := range attempts {
		class := attempt.Class
		if class != contract.JobClassOneShot && class != contract.JobClassService {
			class = "other"
		}
		counts[class]++
	}
	return counts
}

func writeFleetNodes(out io.Writer, nodes []nodeStatus) error {
	table := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(table, "\nNODE\tNETWORK NAME\tSTATE\tCLAIMS\tAUTO-ADMITTED\tAGENT VERSION\tKINDS\tONE-SHOT\tSERVICE\tACTIVE ONE-SHOT\tACTIVE SERVICE\tACTIVE OTHER"); err != nil {
		return err
	}
	for _, node := range nodes {
		id := node.NodeID
		if node.ThisMachine {
			id += " (this machine)"
		}
		claims := "enabled"
		if !node.ClaimsEnabled {
			claims = "disabled"
		}
		kinds := "none"
		if len(node.Kinds) > 0 {
			kinds = strings.Join(node.Kinds, ",")
		}
		if _, err := fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%t\t%s\t%s\t%d/%d free\t%d/%d free\t%d\t%d\t%d\n", id, valueOrNA(node.ConnectHost), node.State, claims, node.AutomaticallyAdmitted, valueOrNA(node.AgentVersion), kinds, node.FreeOneshot, node.TotalOneshot, node.FreeService, node.TotalService, node.ActiveAttemptsByClass[contract.JobClassOneShot], node.ActiveAttemptsByClass[contract.JobClassService], node.ActiveAttemptsByClass["other"]); err != nil {
			return err
		}
	}
	return table.Flush()
}
