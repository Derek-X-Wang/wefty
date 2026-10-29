package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/seatviewer"
	"github.com/Derek-X-Wang/wefty/internal/takeover"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/coder/websocket"
)

type relayOwnerFabric struct {
	fabric.Fabric
	dials atomic.Int32
}

func (f *relayOwnerFabric) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	f.dials.Add(1)
	return f.Fabric.Dial(ctx, network, address)
}

// #1336: use the production Fabric listener, not httptest's raw HTTP listener.
func TestSeatViewerLocalRelayPlainFabric(t *testing.T) {
	for _, ending := range []string{"seat-ended", "door-session-ended", "cap-ended", "door-close-without-poll", "cancel", "ended-before-session"} {
		t.Run(ending, func(t *testing.T) {
			_, fixture, control, original := viewerFixture(t, true)
			network, err := plain.NewNetworkWithID("plain-viewer-relay")
			if err != nil {
				t.Fatal(err)
			}
			node := network.NewFabric(fabric.Identity{NodeID: "node"})
			owner := &relayOwnerFabric{Fabric: network.NewFabric(fabric.Identity{NodeID: "owner", UserID: "person-a", DeviceID: "device-a"})}
			cache := NewComputerPolicyCache(fixture.clock, "node-1", "boot-1")
			defer cache.Close()
			snapshot := policySnapshot(t, fixture.clock.Now(), 1, 1, []l1.ComputerPolicyAdmin{{FabricID: "plain-viewer-relay", UserID: "person-a"}})
			snapshot.IssuingFabricID = "plain-viewer-relay"
			snapshot.SnapshotDigest, err = l1.ComputeComputerPolicySnapshotDigest(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = cache.Install(snapshot); err != nil {
				t.Fatal(err)
			}
			config := original.handler.(*seatScreenHandler).config
			config.fabric, config.authorizer = node, cache
			screens, err := newSeatScreenHandler(config)
			if err != nil {
				t.Fatal(err)
			}
			door, closeDoor, err := startSeatFrontDoor(t.Context(), node, "node")
			if err != nil {
				t.Fatal(err)
			}
			defer closeDoor()
			published, _ := url.Parse(door.endpoint)
			calls := atomic.Int32{}
			registration, err := door.register(t.Context(), "computer", "attempt", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Host != published.Host || r.Header.Get("Origin") != "" && r.Header.Get("Origin") != "http://"+published.Host {
					t.Error("relay did not rewrite Host and Origin to the door")
				}
				if r.Header.Get("Cookie") != "" {
					t.Error("relay cookie escaped to the door")
				}
				calls.Add(1)
				screens.ServeHTTP(w, r)
			}))
			if err != nil {
				t.Fatal(err)
			}
			defer registration.close()
			if err = registration.bind([]string{viewerSeatID}); err != nil {
				t.Fatal(err)
			}
			target, _ := url.Parse(door.endpoint)
			target.Scheme = "http"
			target.Path = "/v1/seats/" + viewerSeatID + "/person/viewer"
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			relay, err := seatviewer.Start(ctx, owner, target)
			if err != nil {
				t.Fatal(err)
			}
			defer relay.Close()
			link, _ := url.Parse(relay.URL)
			origin := link.Scheme + "://" + link.Host
			prefix := strings.TrimSuffix(link.Path, "/")
			if len(strings.TrimPrefix(prefix, "/")) < 26 || strings.TrimPrefix(prefix, "/") == link.Query().Get("secret") {
				t.Fatal("missing independent random path prefix")
			}
			person := origin + prefix + strings.TrimSuffix(target.Path, "/viewer")
			jar, _ := cookiejar.New(nil)
			browser := &http.Client{Jar: jar, Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
			do := func(method, address, origin, token string) (int, []byte) {
				t.Helper()
				request, _ := http.NewRequestWithContext(t.Context(), method, address, strings.NewReader(`{}`))
				if origin != "" {
					request.Header.Set("Origin", origin)
				}
				if token != "" {
					request.Header.Set(computerControlTokenHeader, token)
				}
				response, err := browser.Do(request)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				body, err := io.ReadAll(response.Body)
				if err != nil {
					t.Fatalf("%s %s: response truncated after %d bytes (Content-Length=%d, transfer=%v): %v", method, strings.TrimPrefix(request.URL.Path, prefix), len(body), response.ContentLength, response.TransferEncoding, err)
				}
				return response.StatusCode, body
			}
			for _, address := range []string{person + "/viewer", origin + "/", origin + "/?secret=wrong"} {
				if status, _ := do("GET", address, "", ""); status != 403 {
					t.Fatalf("unauthenticated status=%d", status)
				}
			}
			if calls.Load() != 0 || owner.dials.Load() != 0 {
				t.Fatal("unauthenticated browser reached the door")
			}
			wsURL := "ws" + strings.TrimPrefix(person, "http") + "/viewer-session"
			_, refused, err := websocket.Dial(t.Context(), wsURL, &websocket.DialOptions{Subprotocols: []string{"binary"}, HTTPHeader: http.Header{"Origin": []string{origin}}})
			if err == nil || refused == nil || refused.StatusCode != 403 {
				t.Fatal("cookie-less WebSocket accepted")
			}
			refused.Body.Close()
			if calls.Load() != 0 || owner.dials.Load() != 0 {
				t.Fatal("cookie-less WebSocket reached the door")
			}
			response, err := browser.Get(relay.URL)
			if err != nil {
				t.Fatal("bootstrap request failed")
			}
			response.Body.Close()
			cookies := response.Cookies()
			if response.StatusCode != 303 || response.Header.Get("Location") != prefix+target.Path || len(cookies) != 1 || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteStrictMode || cookies[0].Domain != "" || cookies[0].Path != prefix+"/" || cookies[0].Value == link.Query().Get("secret") || strings.Contains(response.Header.Get("Location"), link.Query().Get("secret")) {
				t.Fatal("invalid secret-to-cookie exchange")
			}
			if status, _ := do("GET", relay.URL, "", ""); status != 403 {
				t.Fatal("bootstrap secret was reusable")
			}
			for _, path := range []string{"/viewer", "/viewer/viewer.js", "/viewer/novnc/core/rfb.js"} {
				status, body := do("GET", person+path, "", "")
				file := strings.TrimPrefix(path, "/viewer/")
				if path == "/viewer" {
					file = "index.html"
				}
				expected, readErr := seatViewerFiles.ReadFile("viewer/" + file)
				if readErr != nil {
					t.Fatal(readErr)
				}
				if status != 200 || !bytes.Equal(body, expected) {
					t.Fatalf("asset %s status=%d bytes=%d/%d", path, status, len(body), len(expected))
				}
			}
			for _, alias := range []string{"/viewer/", "/viewer/index.html"} {
				response, err := browser.Get(person + alias)
				if err != nil {
					t.Fatal("viewer alias failed")
				}
				response.Body.Close()
				if response.StatusCode != 307 || response.Header.Get("Location") != prefix+target.Path {
					t.Fatal("viewer alias escaped the random prefix")
				}
			}
			before := calls.Load()
			beforeDials := owner.dials.Load()
			// Even a stolen cookie is insufficient without the random path prefix.
			outside, _ := http.NewRequestWithContext(t.Context(), "GET", origin+target.Path, nil)
			outside.AddCookie(cookies[0])
			outside.Header.Set("Origin", origin)
			response, err = browser.Do(outside)
			if err != nil {
				t.Fatal("outside-prefix request failed")
			}
			response.Body.Close()
			if response.StatusCode != 403 || calls.Load() != before || owner.dials.Load() != beforeDials {
				t.Fatal("cookie outside random prefix reached the door")
			}

			rebound, _ := http.NewRequestWithContext(t.Context(), "GET", person+"/viewer", nil)
			rebound.Host = "rebound.invalid"
			response, err = browser.Do(rebound)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != 403 {
				t.Fatal("rebound Host accepted")
			}
			for _, badOrigin := range []string{"", "https://attacker.invalid"} {
				if status, _ := do("POST", person+computerControlTakePath, badOrigin, ""); status != 403 {
					t.Fatal("cross-origin take accepted")
				}
			}
			for _, badOrigin := range []string{"", "https://attacker.invalid"} {
				ws, reply, err := websocket.Dial(t.Context(), wsURL, &websocket.DialOptions{HTTPClient: browser, Subprotocols: []string{"binary"}, HTTPHeader: http.Header{"Origin": []string{badOrigin}}})
				if ws != nil {
					ws.CloseNow()
				}
				if err == nil || reply == nil || reply.StatusCode != 403 {
					t.Fatal("cross-origin WebSocket accepted")
				}
				reply.Body.Close()
			}
			if calls.Load() != before || owner.dials.Load() != beforeDials {
				t.Fatal("cross-origin request reached the door")
			}
			for _, bad := range []string{"connection", "upgrade", "missing-upgrade", "version", "key", "duplicate-key", "method"} {
				request, _ := http.NewRequestWithContext(t.Context(), "GET", person+"/viewer-session", nil)
				request.Header = http.Header{"Origin": []string{origin}, "Connection": []string{"Upgrade"}, "Upgrade": []string{"websocket"}, "Sec-Websocket-Version": []string{"13"}, "Sec-Websocket-Key": []string{"MTIzNDU2Nzg5MDEyMzQ1Ng=="}}
				switch bad {
				case "connection":
					request.Header.Del("Connection")
				case "upgrade":
					request.Header.Set("Upgrade", "bogus")
				case "missing-upgrade":
					request.Header.Del("Upgrade")
				case "version":
					request.Header.Set("Sec-WebSocket-Version", "12")
				case "key":
					request.Header.Set("Sec-WebSocket-Key", "bad")
				case "duplicate-key":
					request.Header.Add("Sec-WebSocket-Key", "MTIzNDU2Nzg5MDEyMzQ1Ng==")
				case "method":
					request.Method = "POST"
				}
				response, err = browser.Do(request)
				if err != nil {
					t.Fatal("malformed upgrade request failed")
				}
				response.Body.Close()
				if response.StatusCode < 400 || calls.Load() != before || owner.dials.Load() != beforeDials {
					t.Fatalf("malformed %s upgrade reached the door", bad)
				}
			}
			if ending == "ended-before-session" {
				control.mu.Lock()
				control.authorityFailure = &ProtocolError{StatusCode: 410, APIError: contract.APIError{Code: "seat_ended", Message: "Seat ended: released"}}
				control.mu.Unlock()
				_, refused, err := websocket.Dial(t.Context(), wsURL, &websocket.DialOptions{HTTPClient: browser, Subprotocols: []string{"binary"}, HTTPHeader: http.Header{"Origin": []string{origin}}})
				if err == nil || refused == nil || refused.StatusCode != 410 {
					t.Fatal("ended Seat did not refuse the WebSocket")
				}
				body, _ := io.ReadAll(refused.Body)
				refused.Body.Close()
				if !strings.Contains(string(body), `"seat_ended"`) {
					t.Fatal("ended handshake lost its typed reply")
				}
				select {
				case <-relay.Done():
				case <-time.After(seatviewer.DrainTimeout + 15*time.Second):
					t.Fatal("ended handshake left the relay running")
				}
				return
			}
			ws, _, err := websocket.Dial(t.Context(), wsURL, &websocket.DialOptions{HTTPClient: browser, Subprotocols: []string{"binary"}, HTTPHeader: http.Header{"Origin": []string{origin}}})
			if err != nil {
				t.Fatal(err)
			}
			defer ws.CloseNow()
			read := func() (websocket.MessageType, []byte, error) {
				readCtx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
				defer cancel()
				return ws.Read(readCtx)
			}
			kind, data, err := read()
			var session struct {
				Bearer    string `json:"bearer"`
				Remaining int    `json:"remaining_seconds"`
			}
			if err != nil || kind != websocket.MessageText || json.Unmarshal(data, &session) != nil || session.Bearer == "" || session.Remaining != 3600 {
				t.Fatal("missing browser session metadata")
			}
			kind, data, err = read()
			if err != nil || kind != websocket.MessageBinary || !strings.HasPrefix(string(data), "RFB ") {
				t.Fatalf("RFB banner: %v", err)
			}
			for _, action := range []string{computerControlTakePath, computerControlReleasePath} {
				status, body := do("POST", person+action, origin, session.Bearer)
				if status != 200 {
					t.Fatalf("control status=%d body=%s", status, body)
				}
			}
			switch ending {
			case "seat-ended":
				control.mu.Lock()
				control.authorityFailure = &ProtocolError{StatusCode: 410, APIError: contract.APIError{Code: "seat_ended", Message: "Seat ended: released"}}
				control.mu.Unlock()
				status, body := do("GET", person+"/viewer-status", "", session.Bearer)
				if status != 410 || !strings.Contains(string(body), `"seat_ended"`) {
					t.Fatalf("ended response=%d %s", status, body)
				}
			case "door-session-ended", "cap-ended", "door-close-without-poll":
				screens.mu.Lock()
				front := screens.seats[viewerSeatID].front
				screens.mu.Unlock()
				reason := l1.ComputerTakeoverAttemptAuthorityLost
				if ending == "cap-ended" {
					reason = l1.ComputerTakeoverSessionCapExpired
				}
				front.EndSessions(reason)
				if _, _, err = read(); err == nil {
					t.Fatal("door session stayed open")
				}
				// The relay must leave time for the page's next status poll,
				// so the typed node reason remains visible after a socket close.
				if ending == "door-session-ended" || ending == "cap-ended" {
					status, body := do("GET", person+"/viewer-status", "", session.Bearer)
					if status != 410 || !strings.Contains(string(body), `"takeover_session_ended"`) {
						t.Fatalf("ended session response=%d %s", status, body)
					}
					if ending == "cap-ended" && !strings.Contains(string(body), "session_cap_expired") {
						t.Fatal("cap reason was lost")
					}
				}
			case "cancel":
				cancel()
			}
			select {
			case <-relay.Done():
			case <-time.After(seatviewer.DrainTimeout + 15*time.Second):
				t.Fatal("relay did not stop")
			}
			if relay.Err() == nil {
				t.Fatal("relay stopped without a reason")
			}
			if ending != "cancel" {
				var action *takeover.ActionError
				if !errors.As(relay.Err(), &action) {
					t.Fatal("relay lost the typed terminal outcome")
				}
				want := contract.ErrorTakeoverSessionEnded
				if ending == "seat-ended" {
					want = "seat_ended"
				}
				if ending == "cap-ended" && (action.Receipt == nil || action.Receipt.SessionEndReason != string(l1.ComputerTakeoverSessionCapExpired)) {
					t.Fatal("cap receipt was lost in relay exit")
				}
				if action.APIError.Code != want {
					t.Fatal("wrong terminal outcome")
				}
			}
			if _, _, err = read(); err == nil {
				t.Fatal("relay left the browser stream open")
			}
		})
	}
}
