package l1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// AP-6: the window is visible, covers both forms and lesser network access,
// expires at the boundary, and can only be opened by the registered node.
func TestSeatApprovalWindowAuthorityAndExpiry(t *testing.T) {
	h := newIntegrationHarnessWithReconcileInterval(t, StoreOptions{}, nil, true, time.Hour)
	nodeClient := h.client(fabric.Identity{NodeID: "dialog-node", Tags: []string{DefaultAgentPrincipalTag}})
	foreign := h.client(fabric.Identity{NodeID: "foreign", Tags: []string{DefaultAgentPrincipalTag}})
	cli := h.client(fabric.Identity{NodeID: "cli", Tags: []string{DefaultClientPrincipalTag}})
	node := h.register(nodeClient, "mac")
	base := SeatApprovalRequest{SeatID: "seat_" + strings.Repeat("a", 32), NodeID: node.NodeID, BootSessionID: node.BootSessionID, Harness: "test", Folder: "/workspace", Form: "any", Network: "internet", WindowHours: 1, AnswerWait: 5 * time.Minute}
	begin := func(request SeatApprovalRequest) SeatApproval {
		t.Helper()
		status, _, body := h.do(nodeClient, http.MethodPost, "/v1/agent/seat-approvals", request)
		if status != http.StatusOK {
			t.Fatalf("begin: %d %s", status, body)
		}
		var got SeatApproval
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		return got
	}
	answer := SeatApprovalAnswer{NodeID: node.NodeID, BootSessionID: node.BootSessionID, Approved: true}
	path := "/v1/agent/seat-approvals/" + base.SeatID + "/answer"
	pending := begin(base)
	if pending.State != "requested" {
		t.Fatalf("pending=%+v", pending)
	}
	for _, caller := range []struct {
		name   string
		client *http.Client
	}{{"CLI", cli}, {"foreign node", foreign}} {
		status, _, body := h.do(caller.client, http.MethodPost, path, answer)
		code := contract.ErrorForbidden
		if caller.name == "CLI" {
			code = contract.ErrorPrincipalForbidden
		}
		assertAPIError(t, status, body, http.StatusForbidden, code)
	}
	status, _, body := h.do(nodeClient, http.MethodPost, path, answer)
	if status != http.StatusOK {
		t.Fatalf("answer: %d %s", status, body)
	}
	windows, err := h.store.ListSeatApprovalWindows(context.Background(), "mac")
	if err != nil || len(windows) != 1 || windows[0].Network != "internet" {
		t.Fatalf("windows=%+v err=%v", windows, err)
	}
	for i, form := range []string{"any", "dedicated"} {
		req := base
		req.SeatID = fmt.Sprintf("seat_%032x", i+1)
		req.Form = form
		req.Network = "none"
		req.WindowHours = 0
		if got := begin(req); got.State != "approved" || got.Source != "window" {
			t.Fatalf("covered=%+v", got)
		}
	}
	h.clock.Advance(time.Hour - time.Nanosecond)
	if windows, err = h.store.ListSeatApprovalWindows(context.Background(), "mac"); err != nil || len(windows) != 1 {
		t.Fatalf("early expiry: %v %v", windows, err)
	}
	h.clock.Advance(time.Nanosecond)
	if windows, err = h.store.ListSeatApprovalWindows(context.Background(), "mac"); err != nil || len(windows) != 0 {
		t.Fatalf("late expiry: %v %v", windows, err)
	}
	req := base
	req.SeatID = "seat_" + strings.Repeat("b", 32)
	req.WindowHours = 0
	if got := begin(req); got.State != "requested" {
		t.Fatalf("expired window approved: %+v", got)
	}
	h.clock.Advance(5 * time.Minute)
	status, _, body = h.do(nodeClient, http.MethodPost, "/v1/agent/seat-approvals/"+req.SeatID+"/answer", answer)
	if status != http.StatusOK {
		t.Fatalf("late answer: %d %s", status, body)
	}
	var late SeatApproval
	if err := json.Unmarshal(body, &late); err != nil || late.State != "refused" || late.Reason != "no answer from the owner" {
		t.Fatalf("late=%+v err=%v", late, err)
	}
}
