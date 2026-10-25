package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
)

const seatWindowUsage = "usage: wefty seat window close\nClose this node's approval windows early. Only the native dialog can open one.\n"

func executeSeat(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] != "window" {
		return executeSeatRequest(ctx, clients, jsonOutput, args, stdout, stderr)
	}
	if len(args) != 2 || args[1] != "close" {
		return usageError(seatUsage)
	}
	local, err := resolveLocalNode(ctx, clients)
	if err != nil {
		return err
	}
	var result struct {
		Closed bool `json:"closed"`
	}
	err = clients.l1.do(ctx, http.MethodPost, "/v1/seat-approval-windows/close", struct {
		NodeID string `json:"node_id"`
	}{NodeID: local.NodeID}, nil, &result, http.StatusOK)
	if err != nil {
		return err
	}
	if jsonOutput {
		return writeJSON(stdout, result)
	}
	_, err = fmt.Fprintf(stdout, "approval windows closed for %s\n", local.NodeID)
	return err
}
func (c *apiClients) listSeatApprovalWindows(ctx context.Context) ([]l1.SeatApprovalWindow, error) {
	var windows []l1.SeatApprovalWindow
	err := c.l1.do(ctx, http.MethodGet, "/v1/seat-approval-windows", nil, nil, &windows, http.StatusOK)
	return windows, err
}
func writeSeatApprovalWindows(writer io.Writer, status clusterStatus) error {
	for _, window := range status.ApprovalWindows {
		network := "without internet"
		if window.Network == "internet" {
			network = "with internet, and requests without internet"
		}
		if _, err := fmt.Fprintf(writer, "approval window: %s; every request form %s; until %s\n", window.NodeID, network, window.ExpiresAt.Format(time.RFC3339)); err != nil {
			return err
		}
	}
	if status.ApprovalWindowsError != "" {
		_, err := fmt.Fprintln(writer, "approval windows: cannot verify:", status.ApprovalWindowsError)
		return err
	}
	return nil
}
