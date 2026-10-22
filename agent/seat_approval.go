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

// SeatApprovalDialog is the only source of a positive answer. Production uses
// the native login-session dialog; tests supply SeatApprovalDialogFunc.
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
	done    chan struct{}
	request l1.SeatApprovalRequest
	result  l1.SeatApproval
	err     error
}
type SeatApprovalController struct {
	client                SeatApprovalClient
	nodeID, bootSessionID string
	settings              SeatApprovalSettings
	dialog                SeatApprovalDialog
	clock                 Clock
	mu                    sync.Mutex
	calls                 map[string]*seatApprovalCall
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
		clock = systemClock{}
	}
	return &SeatApprovalController{client: client, nodeID: nodeID, bootSessionID: boot, settings: settings, dialog: dialog, clock: clock, calls: make(map[string]*seatApprovalCall)}, nil
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
	call.result, call.err = c.requestApproval(ctx, request)
	c.mu.Lock()
	delete(c.calls, request.SeatID)
	close(call.done)
	c.mu.Unlock()
	return call.result, call.err
}
func (c *SeatApprovalController) requestApproval(ctx context.Context, request l1.SeatApprovalRequest) (l1.SeatApproval, error) {
	pending, err := c.client.BeginSeatApproval(ctx, request)
	if err != nil || pending.State != "requested" {
		return pending, err
	}
	remaining := pending.Deadline.Sub(wallNow(c.clock))
	answer := l1.SeatApprovalAnswer{NodeID: c.nodeID, BootSessionID: c.bootSessionID, Reason: "no answer from the owner"}
	if remaining > 0 {
		timer := c.clock.NewTimer(remaining)
		defer stopTimer(timer)
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
		select {
		case <-ctx.Done():
			answer.Reason = "approval dialog lost"
		case <-timer.C():
		case reply := <-result:
			switch {
			case reply.err != nil:
				answer.Reason = "approval dialog unavailable"
			case reply.choice == SeatApprovalApprove:
				answer.Approved = true
				answer.Reason = ""
			case reply.choice == SeatApprovalDeny:
				answer.Reason = "denied by the owner"
			default:
				// A locked screen, no login session, and the native timeout all use
				// the same deadline and the same refusal; none means Deny was clicked.
				select {
				case <-ctx.Done():
					answer.Reason = "approval dialog lost"
				case <-timer.C():
				}
			}
		}
		cancel() // dismiss the native dialog before publishing the answer
	}
	// Cancellation must not leave a requested row waiting forever. Client HTTP
	// operations still have their own finite timeout.
	return c.client.AnswerSeatApproval(context.WithoutCancel(ctx), request.SeatID, answer)
}
func seatApprovalMessage(r l1.SeatApprovalRequest) string {
	network := "without internet"
	if r.Network == "internet" {
		network = "with internet"
	}
	window := "one Seat"
	if r.WindowHours != 0 {
		window = fmt.Sprintf("%d hours; every request form %s, and requests without internet", r.WindowHours, network)
	}
	return fmt.Sprintf("Harness (not verified): %q\nFolder: %q\nRequest form: %s\nNetwork: %s\nApproval: %s", r.Harness, r.Folder, r.Form, network, window)
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
