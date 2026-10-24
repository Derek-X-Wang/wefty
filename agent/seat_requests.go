package agent

import (
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
)

func (c *Client) PollSeatCalls(ctx context.Context, request l1.SeatCallPoll) (l1.SeatCallList, error) {
	var result l1.SeatCallList
	err := c.post(ctx, "/v1/agent/seat-requests/poll", request, &result)
	return result, err
}
func (c *Client) AdvanceSeatCall(ctx context.Context, id string, request l1.SeatCallPoll) (l1.SeatRequest, error) {
	var result l1.SeatRequest
	err := c.post(ctx, "/v1/agent/seat-requests/"+url.PathEscape(id)+"/advance", request, &result)
	return result, err
}

// runSeatRequests belongs to Run's lifetime. Losing a CLI wait cannot cancel a
// dialog. One node-owned controller serializes dialogs and L1 fences starts.
func (a *Agent) runSeatRequests(ctx context.Context) {
	if a.seatRoom == nil {
		return
	}
	select {
	case <-ctx.Done():
		return
	case <-a.Registered():
	}
	timer := a.clock.NewTimer(0)
	defer stopTimer(timer)
	var workers sync.WaitGroup
	defer workers.Wait()
	var mu sync.Mutex
	active := map[string]bool{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C():
		}
		enabled := false
		if a.session.ociIntent != nil {
			observation, err := a.session.ociIntent(ctx)
			if err != nil {
				a.log("node agent: Seat request intent unavailable: %v", err)
				timer.Reset(time.Second)
				continue
			}
			enabled = observation.Enabled
		}
		// Keep reconciling failed starts even if runtime capability is lost.
		// This loop never starts a host VM; cold-start demand is #1144.
		poll := l1.SeatCallPoll{SeatNodeRequest: l1.SeatNodeRequest{NodeID: a.registration.NodeID, BootSessionID: a.registration.BootSessionID}, Room: *a.seatRoom, HostEnabled: enabled, AnswerWait: a.seatApprovals.settings.AnswerWait, WindowHours: a.seatApprovals.settings.WindowHours}
		list, err := a.sessionClient().PollSeatCalls(ctx, poll)
		if err != nil {
			a.log("node agent: Seat request poll: %v", err)
			timer.Reset(time.Second)
			continue
		}
		for _, input := range list.Requests {
			mu.Lock()
			running := active[input.SeatID]
			if !running {
				active[input.SeatID] = true
			}
			mu.Unlock()
			if running {
				continue
			}
			workers.Add(1)
			go func(input l1.SeatCallInput, poll l1.SeatCallPoll) {
				defer workers.Done()
				defer func() { mu.Lock(); delete(active, input.SeatID); mu.Unlock() }()
				request := l1.SeatApprovalRequest{SeatID: input.SeatID, Harness: input.Harness, Folder: input.Folder, Form: input.Form, Network: input.Network, WindowHours: input.WindowHours}
				approval, err := a.RequestSeatApproval(ctx, request)
				if err == nil && approval.State == "approved" {
					// Re-read intent after an arbitrarily long owner dialog.
					var observation OCIIntentObservation
					var e error
					if a.session.ociIntent != nil {
						observation, e = a.session.ociIntent(ctx)
					}
					if e != nil {
						err = e
					} else {
						poll.HostEnabled = observation.Enabled
						_, err = a.sessionClient().AdvanceSeatCall(ctx, input.SeatID, poll)
					}
				}
				if err != nil && ctx.Err() == nil {
					a.log("node agent: Seat request %s: %v", input.SeatID, err)
				}
			}(input, poll)
		}
		timer.Reset(time.Second)
	}
}
