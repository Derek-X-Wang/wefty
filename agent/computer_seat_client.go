package agent

import (
	"context"
	"net/url"

	"github.com/Derek-X-Wang/wefty/l1"
)

func computerSeatPath(computerID, jobID, attemptID string) string {
	return "/v1/agent/computers/" + url.PathEscape(computerID) + "/jobs/" + url.PathEscape(jobID) + "/attempts/" + url.PathEscape(attemptID)
}
func (c *Client) GetComputerSeatList(ctx context.Context, computerID, jobID, attemptID string, request l1.ComputerSeatAttemptRequest) (l1.ComputerSeatList, error) {
	var list l1.ComputerSeatList
	err := c.post(ctx, computerSeatPath(computerID, jobID, attemptID)+"/seat-list", request, &list)
	return list, err
}
func (c *Client) ReportComputerSeats(ctx context.Context, computerID, jobID, attemptID string, request l1.ComputerSeatObservationReport) (l1.ComputerSeatObservationReceipt, error) {
	var receipt l1.ComputerSeatObservationReceipt
	err := c.post(ctx, computerSeatPath(computerID, jobID, attemptID)+"/seat-observations", request, &receipt)
	return receipt, err
}
