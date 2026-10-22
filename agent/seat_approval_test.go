//go:build darwin || linux

package agent

import (
	"context"
	"fmt"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

func approvalTestClient(t *testing.T, clock *manualClock) (*Client, *l1.Store) {
	t.Helper()
	network := plain.NewNetwork()
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "approval.sqlite"), l1.StoreOptions{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{"mac": l1.DefaultNodePolicy()}})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: server.Handler()}
	done := make(chan error, 1)
	go func() { done <- httpServer.Serve(listener) }()
	client, err := NewClient(network.NewFabric(fabric.Identity{NodeID: "dialog-node", Tags: []string{l1.DefaultAgentPrincipalTag}}), "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close(); httpServer.Close(); <-done; store.Close() })
	_, err = client.Register(t.Context(), contract.NodeRegistration{NodeID: "mac", BootSessionID: "boot", RootInstanceID: "root", OS: "darwin", Architecture: "arm64", AgentVersion: "test", Capabilities: map[string]bool{"kind:process": true}, CapabilityRevision: 1, CapabilityObservedAt: clock.Now(), MissingCapabilities: []string{}})
	if err != nil {
		t.Fatal(err)
	}
	return client, store
}

// AP-5: all absent answers wait the same five minutes, cancel the dialog,
// and leave a terminal refusal at L1 under the node's identity.
func TestSeatApprovalUnansweredRefusedWithInjectedClock(t *testing.T) {
	for _, kind := range []string{"unanswered", "locked", "logged out"} {
		t.Run(kind, func(t *testing.T) {
			clock := newManualClock(time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC))
			client, store := approvalTestClient(t, clock)
			shown := make(chan SeatApprovalPrompt, 1)
			closed := make(chan struct{})
			dialog := SeatApprovalDialogFunc(func(ctx context.Context, prompt SeatApprovalPrompt) (SeatApprovalChoice, error) {
				shown <- prompt
				if kind != "unanswered" {
					close(closed)
					return SeatApprovalUnavailable, nil
				}
				<-ctx.Done()
				close(closed)
				return SeatApprovalNoAnswer, nil
			})
			controller, err := NewSeatApprovalController(client, "mac", "boot", SeatApprovalSettings{}, dialog, clock)
			if err != nil {
				t.Fatal(err)
			}
			req := l1.SeatApprovalRequest{SeatID: "seat_" + strings.Repeat("a", 32), Harness: "test harness", Folder: "/workspace", Form: "dedicated", Network: "none"}
			result := make(chan l1.SeatApproval, 1)
			fail := make(chan error, 1)
			go func() { value, err := controller.RequestApproval(t.Context(), req); result <- value; fail <- err }()
			prompt := awaitSeatApprovalTest(t, shown)
			if !strings.Contains(prompt.Message, "not verified") || !strings.Contains(prompt.Message, "/workspace") || !strings.Contains(prompt.Message, "dedicated") || !strings.Contains(prompt.Message, "without internet") || prompt.Wait != 5*time.Minute {
				t.Fatalf("prompt=%+v", prompt)
			}
			clock.Advance(5*time.Minute - time.Nanosecond)
			select {
			case value := <-result:
				t.Fatalf("early result=%+v", value)
			default:
			}
			clock.Advance(time.Nanosecond)
			value := awaitSeatApprovalTest(t, result)
			if err := awaitSeatApprovalTest(t, fail); err != nil || value.State != "refused" || value.Reason != "no answer from the owner" {
				t.Fatalf("result=%+v err=%v", value, err)
			}
			awaitSeatApprovalTest(t, closed)
			req.NodeID = "mac"
			req.BootSessionID = "boot"
			req.AnswerWait = 5 * time.Minute
			persisted, err := client.BeginSeatApproval(t.Context(), req)
			if err != nil || persisted.State != "refused" {
				t.Fatalf("L1=%+v err=%v", persisted, err)
			}
			windows, err := store.ListSeatApprovalWindows(t.Context(), "mac")
			if err != nil || len(windows) != 0 {
				t.Fatalf("refusal left windows: %v %v", windows, err)
			}
		})
	}
}

// AP-6 uses the same real L1 protocol and fake dialog as AP-5. Moving only the
// injected clocks proves the exact boundary, with no real-time sleep.
func TestSeatApprovalWindowClockAndClose(t *testing.T) {
	clock := newManualClock(time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC))
	client, store := approvalTestClient(t, clock)
	calls := 0
	controller, err := NewSeatApprovalController(client, "mac", "boot", SeatApprovalSettings{}, SeatApprovalDialogFunc(func(_ context.Context, prompt SeatApprovalPrompt) (SeatApprovalChoice, error) {
		calls++
		if calls == 1 && !strings.Contains(prompt.Message, "8 hours") {
			t.Errorf("window prompt: %s", prompt.Message)
		}
		return SeatApprovalApprove, nil
	}), clock)
	if err != nil {
		t.Fatal(err)
	}
	request := func(id int, form, network string, hours int) l1.SeatApproval {
		t.Helper()
		value, err := controller.RequestApproval(t.Context(), l1.SeatApprovalRequest{SeatID: fmt.Sprintf("seat_%032x", id), Harness: "test", Folder: "/workspace", Form: form, Network: network, WindowHours: hours})
		if err != nil || value.State != "approved" {
			t.Fatalf("approval=%+v err=%v", value, err)
		}
		return value
	}
	request(1, "any", "internet", 8)
	clock.Advance(8*time.Hour - time.Nanosecond)
	if value := request(2, "dedicated", "none", 0); value.Source != "window" || calls != 1 {
		t.Fatalf("covered=%+v dialogs=%d", value, calls)
	}
	clock.Advance(time.Nanosecond)
	if value := request(3, "any", "internet", 0); value.Source != "dialog" || calls != 2 {
		t.Fatalf("expired=%+v dialogs=%d", value, calls)
	}
	request(4, "dedicated", "none", 1)
	if value := request(5, "any", "internet", 0); value.Source != "dialog" {
		t.Fatal("offline window covered internet")
	}
	if err := store.CloseSeatApprovalWindows(t.Context(), "mac", ""); err != nil {
		t.Fatal(err)
	}
	if value := request(6, "dedicated", "none", 0); value.Source != "dialog" {
		t.Fatal("closed window approved a new request")
	}
	// Replaying the original affirmative cannot reopen the closed window.
	_, err = client.AnswerSeatApproval(t.Context(), fmt.Sprintf("seat_%032x", 4), l1.SeatApprovalAnswer{NodeID: "mac", BootSessionID: "boot", Approved: true})
	if err != nil {
		t.Fatal(err)
	}
	if windows, err := store.ListSeatApprovalWindows(t.Context(), "mac"); err != nil || len(windows) != 0 {
		t.Fatalf("answer replay reopened window: %+v %v", windows, err)
	}
}

func TestSeatApprovalRetriesTransportFailure(t *testing.T) {
	clock := newManualClock(time.Now())
	client, _ := approvalTestClient(t, clock)
	transport := client.httpClient.Transport
	first := true
	client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) {
		if first {
			first = false
			return nil, fmt.Errorf("test transport unavailable")
		}
		return transport.RoundTrip(r)
	})
	controller, err := NewSeatApprovalController(client, "mac", "boot", SeatApprovalSettings{}, SeatApprovalDialogFunc(func(context.Context, SeatApprovalPrompt) (SeatApprovalChoice, error) { return SeatApprovalDeny, nil }), clock)
	if err != nil {
		t.Fatal(err)
	}
	request := l1.SeatApprovalRequest{SeatID: "seat_" + strings.Repeat("c", 32), Folder: "/workspace", Form: "any", Network: "none"}
	if _, err := controller.RequestApproval(t.Context(), request); err == nil {
		t.Fatal("transport failure hidden")
	}
	result, err := controller.RequestApproval(t.Context(), request)
	if err != nil || result.State != "refused" || result.Reason != "denied by the owner" {
		t.Fatalf("retry=%+v err=%v", result, err)
	}
}

func TestSeatApprovalNodeSettingsFromBinary(t *testing.T) {
	help, err := exec.Command(agentBinaryPath, "--help").CombinedOutput()
	if err != nil || !strings.Contains(string(help), "seat-approval-answer-wait") || !strings.Contains(string(help), "seat-approval-window-hours") {
		t.Fatalf("settings flags absent: %v %s", err, help)
	}
	for _, value := range []struct{ arg, want string }{{"--seat-approval-answer-wait=0s", "answer wait must be positive"}, {"--seat-approval-window-hours=48", "windows must be 1, 8 or 24 hours"}} {
		output, err := exec.Command(agentBinaryPath, value.arg).CombinedOutput()
		if err == nil || !strings.Contains(string(output), value.want) {
			t.Fatalf("%s: err=%v output=%s", value.arg, err, output)
		}
	}
}

// This real-time bound is only a deadlock watchdog; all domain waits use the injected clock.
func awaitSeatApprovalTest[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("Seat approval did not finish after the injected deadline")
		var zero T
		return zero
	}
}
