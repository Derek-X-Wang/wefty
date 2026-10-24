package agent

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
)

const DefaultSeatApprovalWait = 5 * time.Minute

type SeatApprovalSettings struct {
	AnswerWait  time.Duration
	WindowHours []int
}

func (s SeatApprovalSettings) defaults() (SeatApprovalSettings, error) {
	if s.AnswerWait == 0 {
		s.AnswerWait = DefaultSeatApprovalWait
	}
	if s.AnswerWait > l1.MaxSeatApprovalWait {
		return s, fmt.Errorf("Seat approval answer wait must not exceed 1h")
	}
	if s.AnswerWait < 0 {
		return s, fmt.Errorf("Seat approval answer wait must be positive")
	}
	if s.WindowHours == nil {
		s.WindowHours = []int{1, 8, 24}
	}
	for _, hours := range s.WindowHours {
		if hours != 1 && hours != 8 && hours != 24 {
			return s, fmt.Errorf("Seat approval windows must be 1, 8 or 24 hours")
		}
	}
	s.WindowHours = slices.Clone(s.WindowHours)
	return s, nil
}

// ParseSeatApprovalWindowHours parses the node startup setting. It can limit
// the offered lengths, but can never open a window.
func ParseSeatApprovalWindowHours(value string) ([]int, error) {
	hours := []int{}
	for _, part := range strings.Split(value, ",") {
		hour, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid Seat approval window hours")
		}
		hours = append(hours, hour)
	}
	_, err := (SeatApprovalSettings{WindowHours: hours}).defaults()
	return hours, err
}

type SeatApprovalChoice string

const (
	SeatApprovalApprove     SeatApprovalChoice = "approve"
	SeatApprovalDeny        SeatApprovalChoice = "deny"
	SeatApprovalNoAnswer    SeatApprovalChoice = "no-answer"
	SeatApprovalUnavailable SeatApprovalChoice = "unavailable"
)

type SeatApprovalPrompt struct {
	Message string
	Wait    time.Duration
}

// SeatApprovalDialog supplies the owner's answer. Production uses
// the native login-session dialog; tests supply SeatApprovalDialogFunc.
// Show must honor cancellation. Unavailable without an error means the owner
// session is absent; operational failures return an error and refuse at once.
type SeatApprovalDialog interface {
	Show(context.Context, SeatApprovalPrompt) (SeatApprovalChoice, error)
}
type SeatApprovalDialogFunc func(context.Context, SeatApprovalPrompt) (SeatApprovalChoice, error)

func (f SeatApprovalDialogFunc) Show(ctx context.Context, p SeatApprovalPrompt) (SeatApprovalChoice, error) {
	return f(ctx, p)
}

type SeatApprovalClient interface {
	BeginSeatApproval(context.Context, l1.SeatApprovalRequest) (l1.SeatApproval, error)
	AnswerSeatApproval(context.Context, string, l1.SeatApprovalAnswer) (l1.SeatApproval, error)
}

// SeatApprover is the interface the Seat-request controller (#1143) calls
// after checking room, the dedicated cap and OCI intent. It carries no CLI
// approval verb and cannot take an answer from the requesting AI agent.
type SeatApprover interface {
	RequestApproval(context.Context, l1.SeatApprovalRequest) (l1.SeatApproval, error)
}

type seatApprovalCall struct {
	done     chan struct{}
	request  l1.SeatApprovalRequest
	result   l1.SeatApproval
	err      error
	answer   *l1.SeatApprovalAnswer
	deadline time.Time
}
type SeatApprovalController struct {
	client                SeatApprovalClient
	nodeID, bootSessionID string
	settings              SeatApprovalSettings
	dialog                SeatApprovalDialog
	clock                 Clock
	mu                    sync.Mutex
	calls                 map[string]*seatApprovalCall
	dialogSlot            chan struct{}
}

func NewSeatApprovalController(client SeatApprovalClient, nodeID, boot string, settings SeatApprovalSettings, dialog SeatApprovalDialog, clock Clock) (*SeatApprovalController, error) {
	settings, err := settings.defaults()
	if err != nil {
		return nil, err
	}
	if client == nil || nodeID == "" || boot == "" {
		return nil, fmt.Errorf("Seat approval requires a node client and boot identity")
	}
	if dialog == nil {
		dialog = NativeSeatApprovalDialog{}
	}
	if clock == nil {
		clock = seatWaitClock{}
	} else if _, production := clock.(systemClock); production {
		clock = seatWaitClock{}
	}
	if c, ok := clock.(seatWaitClock); ok {
		if c.failure == nil {
			c.failure = &seatClockFailure{}
		}
		c.Now()
		if err := c.Err(); err != nil {
			return nil, err
		}
		clock = c
	}
	return &SeatApprovalController{client: client, nodeID: nodeID, bootSessionID: boot, settings: settings, dialog: dialog, clock: clock, calls: make(map[string]*seatApprovalCall), dialogSlot: make(chan struct{}, 1)}, nil
}
func (c *SeatApprovalController) RequestApproval(ctx context.Context, request l1.SeatApprovalRequest) (l1.SeatApproval, error) {
	// Authority and settings come from the node, never from the Seat request.
	request.NodeID = c.nodeID
	request.BootSessionID = c.bootSessionID
	request.AnswerWait = c.settings.AnswerWait
	if err := request.Validate(); err != nil {
		return l1.SeatApproval{}, err
	}
	if request.WindowHours != 0 && !slices.Contains(c.settings.WindowHours, request.WindowHours) {
		return l1.SeatApproval{}, fmt.Errorf("Seat approval window length is not enabled")
	}
	c.mu.Lock()
	if call, ok := c.calls[request.SeatID]; ok {
		c.mu.Unlock()
		if call.request != request {
			return l1.SeatApproval{}, fmt.Errorf("Seat approval ID has different input")
		}
		select {
		case <-ctx.Done():
			return l1.SeatApproval{}, ctx.Err()
		case <-call.done:
			return call.result, call.err
		}
	}
	call := &seatApprovalCall{done: make(chan struct{}), request: request}
	c.calls[request.SeatID] = call
	c.mu.Unlock()
	call.result, call.err = c.requestApproval(ctx, request, call)
	c.mu.Lock()
	settled := call.err == nil && call.result.State != "" && call.result.State != "requested"
	expired := call.answer != nil && !c.clock.Now().Before(call.deadline)
	if call.answer == nil || settled || expired {
		delete(c.calls, request.SeatID)
	}
	close(call.done)
	c.mu.Unlock()
	return call.result, call.err
}
func (c *SeatApprovalController) requestApproval(ctx context.Context, request l1.SeatApprovalRequest, call *seatApprovalCall) (l1.SeatApproval, error) {
	if err := seatApprovalClockError(c.clock); err != nil {
		return l1.SeatApproval{}, err
	}
	started := c.clock.Now()
	pending, err := c.client.BeginSeatApproval(ctx, request)
	if err != nil || pending.State != "requested" {
		return pending, err
	}
	// A queued request keeps its deadline from request start. The L1 deadline
	// independently limits it, including time spent in the initial exchange.
	deadline := started.Add(request.AnswerWait)
	remainingWait := pending.RemainingWait
	serverDeadline := c.clock.Now().Add(remainingWait)
	if serverDeadline.Before(deadline) {
		deadline = serverDeadline
	}
	call.deadline = deadline
	remaining := deadline.Sub(c.clock.Now())
	answer := l1.SeatApprovalAnswer{NodeID: c.nodeID, BootSessionID: c.bootSessionID, Reason: "no answer from the owner"}
	if remaining > 0 {
		timer := c.clock.NewTimer(remaining)
		defer stopTimer(timer)
		select {
		case c.dialogSlot <- struct{}{}:
			answer.Approved, answer.Reason = c.awaitSeatApprovalDialog(ctx, request, deadline, timer)
			// Show has returned. A slow L1 post must not hold up the next dialog.
			<-c.dialogSlot
		case <-ctx.Done():
			answer.Reason = "approval dialog lost"
		case <-timer.C():
		}
	}
	// Keep an unconfirmed answer until L1 settles it or its deadline passes.
	// A lost post must never turn it back into a new dialog on the next poll.
	if err := seatApprovalClockError(c.clock); err != nil {
		return pending, err
	}
	call.answer = &answer
	backoff := time.Second
	for {
		if err := seatApprovalClockError(c.clock); err != nil {
			return pending, err
		}
		postCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		value, err := c.client.AnswerSeatApproval(postCtx, request.SeatID, *call.answer)
		cancel()
		if err == nil || ctx.Err() != nil || !c.clock.Now().Before(deadline) {
			return value, err
		}
		retry := c.clock.NewTimer(min(backoff, deadline.Sub(c.clock.Now())))
		select {
		case <-ctx.Done():
			stopTimer(retry)
			return value, ctx.Err()
		case <-retry.C():
			stopTimer(retry)
		}
		backoff = min(30*time.Second, backoff*2)
	}
}

// The node owns the slot until Show has returned, including after cancellation.
// This prevents a second native alert from overlapping a closing subprocess.
func (c *SeatApprovalController) awaitSeatApprovalDialog(ctx context.Context, request l1.SeatApprovalRequest, deadline time.Time, timer Timer) (bool, string) {
	for {
		remaining := deadline.Sub(c.clock.Now())
		if remaining <= 0 {
			return false, "no answer from the owner"
		}
		if ctx.Err() != nil {
			return false, "approval dialog lost"
		}
		dialogCtx, cancel := context.WithCancel(ctx)
		type response struct {
			choice SeatApprovalChoice
			err    error
		}
		result := make(chan response, 1)
		go func() {
			choice, err := c.dialog.Show(dialogCtx, SeatApprovalPrompt{Message: seatApprovalMessage(request), Wait: remaining})
			result <- response{choice, err}
		}()
		var reply response
		returned := false
		reason := ""
		select {
		case <-ctx.Done():
			reason = "approval dialog lost"
		case <-timer.C():
			reason = "no answer from the owner"
		case reply = <-result:
			returned = true
		}
		cancel()
		if !returned {
			<-result
		} // Native Show respects context cancellation.
		if reason != "" {
			return false, reason
		}
		if !c.clock.Now().Before(deadline) {
			return false, "no answer from the owner"
		}
		switch {
		case reply.err != nil:
			return false, "approval dialog unavailable"
		case reply.choice == SeatApprovalApprove:
			return true, ""
		case reply.choice == SeatApprovalDeny:
			return false, "denied by the owner"
		case reply.choice == SeatApprovalUnavailable:
			// No owner session yet. Probe again without resetting the deadline.
			retry := c.clock.NewTimer(min(time.Second, deadline.Sub(c.clock.Now())))
			select {
			case <-ctx.Done():
				stopTimer(retry)
				return false, "approval dialog lost"
			case <-timer.C():
				stopTimer(retry)
				return false, "no answer from the owner"
			case <-retry.C():
				stopTimer(retry)
			}
		default:
			select {
			case <-ctx.Done():
				return false, "approval dialog lost"
			case <-timer.C():
				return false, "no answer from the owner"
			}
		}
	}
}

func seatApprovalMessage(r l1.SeatApprovalRequest) string {
	network := "without internet"
	if r.Network == "internet" {
		network = "with internet"
	}
	window := "one Seat"
	if r.WindowHours != 0 {
		unit := "hours"
		if r.WindowHours == 1 {
			unit = "hour"
		}
		window = fmt.Sprintf("%d %s; every request form %s", r.WindowHours, unit, network)
		if r.Network == "internet" {
			window += ", and requests without internet"
		}
	}
	return fmt.Sprintf("Network: %s\nForm: %s\nApproval: %s\nHarness (not verified): %s\nFolder: %s", network, r.Form, window, seatApprovalDisplayText(r.Harness), seatApprovalDisplayText(r.Folder))
}
func seatApprovalDisplayText(value string) string {
	text := []rune(strconv.Quote(strings.Join(strings.Fields(value), " ")))
	if len(text) > 200 {
		return string(text[:198]) + "…\""
	}
	return string(text)
}
func (c *Client) BeginSeatApproval(ctx context.Context, request l1.SeatApprovalRequest) (l1.SeatApproval, error) {
	var result l1.SeatApproval
	err := c.post(ctx, "/v1/agent/seat-approvals", request, &result)
	return result, err
}
func (c *Client) AnswerSeatApproval(ctx context.Context, seatID string, answer l1.SeatApprovalAnswer) (l1.SeatApproval, error) {
	var result l1.SeatApproval
	err := c.post(ctx, "/v1/agent/seat-approvals/"+url.PathEscape(seatID)+"/answer", answer, &result)
	return result, err
}

// RequestSeatApproval uses the node agent's Fabric identity, never a caller's
// connection. The Seat-request controller calls this after preflight checks.
// Its context belongs to the node-owned request, not a reconnectable CLI wait.
func (a *Agent) RequestSeatApproval(ctx context.Context, request l1.SeatApprovalRequest) (l1.SeatApproval, error) {
	return a.seatApprovals.RequestApproval(ctx, request)
}

func seatApprovalClockError(clock Clock) error {
	if c, ok := clock.(interface{ Err() error }); ok {
		return c.Err()
	}
	return nil
}
