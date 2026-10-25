package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
	"github.com/coder/websocket"
)

const viewerSeatID = "seat_00000000000000000000000000000001"

func viewerFixture(t *testing.T, admin bool) (*httptest.Server, *computerFrontDoorTestFixture, *screenControlFixture, *seatFrontDoorRegistration) {
	t.Helper()
	f, cache, _, _, old, identity := computerFrontDoorFixture(t, l1.ComputerGrantControl)
	old.Close()
	if admin {
		if _, err := cache.Install(policySnapshot(t, f.clock.Now(), 1, 2, []l1.ComputerPolicyAdmin{{FabricID: identity.FabricID, UserID: identity.UserID}})); err != nil {
			t.Fatal(err)
		}
	}
	runtime := &screenRuntimeFixture{view: newComputerBackend(t, computerBackendOptions{}), control: newComputerBackend(t, computerBackendOptions{rfbHandshake: true}), drivers: map[int]bool{}}
	t.Cleanup(runtime.view.Close)
	t.Cleanup(runtime.control.Close)
	control := &screenControlFixture{}
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	h, err := newSeatScreenHandler(seatScreenConfig{ctx: ctx, fabric: f.identity, authorizer: cache, clock: f.clock, runtime: runtime, control: control, computer: "computer-1", storage: "storage-1", storageGeneration: 1, authority: ocihelper.AttemptAuthority{NodeID: "node-1", BootSessionID: "boot-1", JobID: "job-1", AttemptID: "attempt-1", FencingToken: "fence-1"}})
	if err != nil {
		t.Fatal(err)
	}
	door := newSeatFrontDoor(f.identity)
	reg, err := door.register(ctx, "computer-1", "attempt-1", h)
	if err != nil {
		t.Fatal(err)
	}
	if err = reg.bind([]string{viewerSeatID}); err != nil {
		t.Fatal(err)
	}
	seat, err := h.seat(context.WithValue(ctx, seatAuthorityContextKey{}, reg.seatContexts[viewerSeatID]), viewerSeatID)
	if err != nil {
		t.Fatal(err)
	}
	f.frontDoor = seat.front
	server := httptest.NewServer(door)
	t.Cleanup(func() { cancel(); reg.close(); server.Close() })
	return server, f, control, reg
}

func TestSeatViewerPageAndBrowserSession(t *testing.T) {
	for _, admin := range []bool{false, true} {
		t.Run(map[bool]string{false: "non-admin", true: "admin"}[admin], func(t *testing.T) {
			server, _, _, _ := viewerFixture(t, admin)
			base := server.URL + "/v1/seats/" + viewerSeatID + "/person"
			response, err := http.Get(base + "/viewer")
			if err != nil {
				t.Fatal(err)
			}
			html, _ := io.ReadAll(response.Body)
			response.Body.Close()
			want := http.StatusForbidden
			if admin {
				want = http.StatusOK
			}
			if response.StatusCode != want {
				t.Fatalf("page status=%d body=%s", response.StatusCode, html)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			ws, reply, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(base, "http")+"/viewer-session", &websocket.DialOptions{Subprotocols: []string{"binary"}})
			if !admin {
				if ws != nil {
					ws.CloseNow()
				}
				if err == nil || reply.StatusCode != 403 {
					t.Fatalf("non-admin session=%v %+v", err, reply)
				}
				reply.Body.Close()
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer ws.CloseNow()
			kind, data, err := ws.Read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var session struct {
				Bearer    string `json:"bearer"`
				Remaining int    `json:"remaining_seconds"`
			}
			if kind != websocket.MessageText || json.Unmarshal(data, &session) != nil || session.Bearer == "" || session.Remaining != 3600 {
				t.Fatalf("browser session=%s kind=%v", data, kind)
			}
			if strings.Contains(string(html), session.Bearer) || strings.Contains(reply.Request.URL.String(), session.Bearer) {
				t.Fatal("bearer exposed in HTML or URL")
			}
			if response.Header.Get("Cache-Control") != "no-store" {
				t.Fatal("page can be cached")
			}
			kind, data, err = ws.Read(ctx)
			if err != nil || kind != websocket.MessageBinary || !strings.HasPrefix(string(data), "RFB ") {
				t.Fatalf("screen banner=%q %v", data, err)
			}
		})
	}
}

func openViewerSession(t *testing.T, server *httptest.Server) (*websocket.Conn, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/seats/"+viewerSeatID+"/person/viewer-session", &websocket.DialOptions{Subprotocols: []string{"binary"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ws.CloseNow() })
	_, data, err := ws.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var session struct {
		Bearer string `json:"bearer"`
	}
	if json.Unmarshal(data, &session) != nil || session.Bearer == "" {
		t.Fatalf("session=%s", data)
	}
	if _, _, err = ws.Read(ctx); err != nil {
		t.Fatal(err)
	}
	return ws, session.Bearer
}

func viewerStatusRequest(t *testing.T, server *httptest.Server, token string) (int, []byte) {
	t.Helper()
	r, _ := http.NewRequestWithContext(t.Context(), "GET", server.URL+"/v1/seats/"+viewerSeatID+"/person/viewer-status", nil)
	r.Header.Set(computerControlTokenHeader, token)
	response, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, body
}

func TestSeatViewerTakeReleaseAndEndedSignal(t *testing.T) {
	server, _, control, reg := viewerFixture(t, true)
	_, token := openViewerSession(t, server)
	status, body := viewerStatusRequest(t, server, token)
	if status != 200 || !strings.Contains(string(body), `"driver":"AI agent"`) {
		t.Fatalf("initial=%d %s", status, body)
	}
	if status = postComputerControl(t, server.URL, "/v1/seats/"+viewerSeatID+"/person"+computerControlTakePath, token); status != 200 {
		t.Fatalf("take=%d", status)
	}
	status, body = viewerStatusRequest(t, server, token)
	if status != 200 || !strings.Contains(string(body), `"driver":"you"`) {
		t.Fatalf("driver=%d %s", status, body)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	headers := http.Header{}
	headers.Set(l1.SeatCredentialHeader, viewerSeatID)
	ai, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/seats/"+viewerSeatID+"/screen/input", &websocket.DialOptions{Subprotocols: []string{"binary"}, HTTPHeader: headers})
	if ai != nil {
		ai.CloseNow()
	}
	if err == nil || response.StatusCode != 409 {
		t.Fatalf("AR-2 refusal=%v %+v", err, response)
	}
	body, _ = io.ReadAll(response.Body)
	response.Body.Close()
	if !strings.Contains(string(body), `"code":"person_driving"`) {
		t.Fatalf("AR-2 body=%s", body)
	}
	request, _ := http.NewRequestWithContext(ctx, "POST", server.URL+"/v1/seats/"+viewerSeatID+"/person"+computerControlReleasePath, strings.NewReader(`{"note":"The browser is ready."}`))
	request.Header.Set(computerControlTokenHeader, token)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("release=%d", response.StatusCode)
	}
	status, body = viewerStatusRequest(t, server, token)
	if status != 200 || !strings.Contains(string(body), `"driver":"AI agent"`) {
		t.Fatalf("released=%d %s", status, body)
	}
	control.mu.Lock()
	found := false
	for _, event := range control.events {
		if event.Kind == l1.SeatEventControlReleased && event.Note == "The browser is ready." {
			found = true
		}
	}
	control.mu.Unlock()
	if !found {
		t.Fatal("release note missing from Seat log")
	}
	ai, _, err = websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/seats/"+viewerSeatID+"/screen/input", &websocket.DialOptions{Subprotocols: []string{"binary"}, HTTPHeader: headers})
	if err != nil {
		t.Fatal(err)
	}
	ai.CloseNow()
	control.mu.Lock()
	control.authorityFailure = &ProtocolError{StatusCode: 410, APIError: contract.APIError{Code: "seat_ended", Message: "Seat ended: released"}}
	control.mu.Unlock()
	status, body = viewerStatusRequest(t, server, token)
	if status != 410 || !strings.Contains(string(body), `"code":"seat_ended"`) {
		t.Fatalf("end signal=%d %s", status, body)
	}
	if err = reg.bind(nil); err != nil {
		t.Fatal(err)
	}
	status, _ = viewerStatusRequest(t, server, token)
	if status != 404 {
		t.Fatalf("unbound Seat=%d", status)
	}
}

func TestSeatViewerCapCannotBeExtendedByPolling(t *testing.T) {
	server, f, control, _ := viewerFixture(t, true)
	ws, token := openViewerSession(t, server)
	f.clock.waitForDeadline(t, f.clock.Now().Add(time.Hour))
	// Keep the installed administrator policy fresh independently of the cap.
	// The viewer still uses the original admission deadline.
	for i := 0; i < 6; i++ {
		f.clock.Advance(9 * time.Minute)
		if _, err := f.frontDoor.config.authorizer.(*ComputerPolicyCache).Install(policySnapshot(t, f.clock.Now(), 1, 2, []l1.ComputerPolicyAdmin{{FabricID: "fabric-one", UserID: "person-a"}})); err != nil {
			t.Fatal(err)
		}
		status, body := viewerStatusRequest(t, server, token)
		if status != 200 {
			t.Fatalf("poll=%d %s", status, body)
		}
	}
	f.clock.Advance(6 * time.Minute)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, _, err := ws.Read(ctx); err == nil {
		t.Fatal("one-hour session stayed open")
	}
	control.mu.Lock()
	found := false
	for _, e := range control.events {
		if e.Audit != nil && e.Audit.Kind == l1.ComputerTakeoverSessionClose && e.Audit.Reason == l1.ComputerTakeoverSessionCapExpired {
			found = true
		}
	}
	control.mu.Unlock()
	// Socket closure precedes audit finalization; the status path also carries
	// the typed cap reason while that finalization is still in progress.
	status, body := viewerStatusRequest(t, server, token)
	if status != 410 || !strings.Contains(string(body), "session_cap_expired") {
		t.Fatalf("cap signal=%d %s (audit finalized=%t)", status, body, found)
	}
}

func TestSeatViewerPageController(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is required to exercise the embedded page controller")
	}
	command := exec.CommandContext(t.Context(), node, "testdata/seat_viewer.mjs")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("shipped page controller: %v\n%s", err, output)
	}
}
