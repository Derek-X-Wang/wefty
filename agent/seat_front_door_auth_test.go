package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/coder/websocket"
)

const unboundDoorSeatID = "seat_00000000000000000000000000000002"

type seatDoorCredentialFunc func(context.Context, string, string) error

func (f seatDoorCredentialFunc) AuthenticateSeatCopy(ctx context.Context, id, credential string) error {
	return f(ctx, id, credential)
}

func (f seatDoorCredentialFunc) AuthenticateSeatScreen(ctx context.Context, id, credential string) error {
	return f(ctx, id, credential)
}

type seatDoorPolicyCounter struct {
	*ComputerPolicyCache
	checks atomic.Int32
	grants atomic.Int32
}

func (p *seatDoorPolicyCounter) CheckAdministrator(identity fabric.Identity) (bool, int64) {
	p.checks.Add(1)
	return p.ComputerPolicyCache.CheckAdministrator(identity)
}
func (p *seatDoorPolicyCounter) AcquireGrant(id string, identity fabric.Identity) (*ComputerGrantAuthorization, error) {
	p.grants.Add(1)
	return p.ComputerPolicyCache.AcquireGrant(id, identity)
}

type seatDoorScreenCounter struct {
	*screenControlFixture
	calls atomic.Int32
}

func (c *seatDoorScreenCounter) AuthorizeSeatScreen(ctx context.Context, id string, input l1.SeatScreenAuthorityInput) (l1.SeatScreenAuthority, error) {
	c.calls.Add(1)
	if input.Credential == "expired-secret" && id == viewerSeatID {
		return l1.SeatScreenAuthority{}, &ProtocolError{StatusCode: 409, APIError: contract.APIError{Code: "seat_ended", Message: "Seat ended: expired", RequestID: "bound-expired-trace"}}
	}
	result, err := c.screenControlFixture.AuthorizeSeatScreen(ctx, id, input)
	var failure *ProtocolError
	if errors.As(err, &failure) {
		copy := *failure
		copy.APIError.RequestID = "bound-wrong-trace"
		err = &copy
	}
	return result, err
}

func (c *seatDoorScreenCounter) AuthorizeSeatCopy(ctx context.Context, id string, input l1.SeatCopyAuthorityInput) (l1.SeatCopyAuthority, error) {
	value, err := c.AuthorizeSeatScreen(ctx, id, l1.SeatScreenAuthorityInput{SeatCopyAuthorityInput: input})
	return value.SeatCopyAuthority, err
}
func (*seatDoorScreenCounter) RecordSeatCopy(context.Context, string, l1.SeatCopyReceipt) error {
	return errors.New("unexpected copy admission")
}

// Use the real screen and person handlers on the bound branch, and the real
// control-client transport/decoder on the unbound branch. No host runtime runs.
func seatDoorAuthFixture(t *testing.T, admin bool) (*seatFrontDoor, *computerFrontDoorTestFixture, *seatDoorPolicyCounter, *seatDoorScreenCounter, *atomic.Int32, *[]string) {
	t.Helper()
	_, f, control, registration := viewerFixture(t, admin)
	policy := &seatDoorPolicyCounter{ComputerPolicyCache: f.frontDoor.config.authorizer.(*ComputerPolicyCache)}
	f.frontDoor.config.authorizer = policy
	screen := &seatDoorScreenCounter{screenControlFixture: control}
	screens := registration.handler.(*seatScreenHandler)
	screens.config.control = screen
	registration.handler = newSeatFeatureMux(screens, newSeatCopyHandler(copyRuntimeFixture{t.TempDir()}, screen, workloadrunner.AttemptAuthority{}, "computer-1"))
	door := registration.door
	calls := &atomic.Int32{}
	client := &Client{baseURL: "http://control.test", operationTimeout: time.Second, httpClient: &http.Client{Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		return seatDoorCredentialReply(t, r), nil
	})}}
	logs := &[]string{}
	door.authentication = seatFrontDoorAuthentication{control: client, policy: policy, logf: func(format string, args ...any) { *logs = append(*logs, fmt.Sprintf(format, args...)) }}
	return door, f, policy, screen, calls, logs
}

func seatDoorAuthRequest(door *seatFrontDoor, id, suffix, credential, revision string) *httptest.ResponseRecorder {
	method := http.MethodGet
	if strings.HasPrefix(suffix, "/copy") || strings.HasPrefix(suffix, "/person/wefty/control/") {
		method = http.MethodPost
	}
	r := httptest.NewRequest(method, "http://127.0.0.1/v1/seats/"+id+suffix, nil)
	if credential != "" {
		r.Header.Set(l1.SeatCredentialHeader, credential)
	}
	if revision != "" {
		r.Header.Set(contract.ComputerPolicyRevisionHeader, revision)
	}
	r.Header.Set("Sec-WebSocket-Protocol", "binary")
	w := httptest.NewRecorder()
	door.ServeHTTP(w, r)
	return w
}

func assertSeatDoorSameAnswer(t *testing.T, bound, unbound *httptest.ResponseRecorder, want int) {
	t.Helper()
	// IDs are random on both paths. Keep the field/presence identical and
	// normalize only its value, just as copy comparisons do for copy_id.
	normalize := regexp.MustCompile(`"request_id":"[^"]+"`)
	boundBody := normalize.ReplaceAllString(bound.Body.String(), `"request_id":"REQUEST_ID"`)
	unboundBody := normalize.ReplaceAllString(unbound.Body.String(), `"request_id":"REQUEST_ID"`)
	if bound.Code != want || bound.Code != unbound.Code || bound.Header().Get("Content-Type") != unbound.Header().Get("Content-Type") || boundBody != unboundBody {
		t.Fatalf("answers differ: bound=(%d %q %q) unbound=(%d %q %q), want status %d", bound.Code, bound.Header().Get("Content-Type"), bound.Body.String(), unbound.Code, unbound.Header().Get("Content-Type"), unbound.Body.String(), want)
	}
}

func TestSeatFrontDoorIdenticalAuthenticationRefusals(t *testing.T) {
	for _, tc := range []struct {
		name, suffix, credential string
		status                   int
	}{
		{"missing-credential", "/screen/view", "", 401},
		{"wrong-credential", "/screen/view", "wrong-secret", 403},
		{"expired-credential", "/screen/input", "expired-secret", 409},
		{"non-administrator", "/person/viewer", "", 403},
		{"non-administrator-websocket", "/person/websockify", "", 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			door, _, _, _, _, _ := seatDoorAuthFixture(t, false)
			bound := seatDoorAuthRequest(door, viewerSeatID, tc.suffix, tc.credential, "")
			unboundID := unboundDoorSeatID
			if tc.credential == "expired-secret" {
				if err := door.seats[viewerSeatID].bind(nil); err != nil {
					t.Fatal(err)
				}
				unboundID = viewerSeatID
			}
			unbound := seatDoorAuthRequest(door, unboundID, tc.suffix, tc.credential, "")
			assertSeatDoorSameAnswer(t, bound, unbound, tc.status)
		})
	}
}

func TestSeatFrontDoorForeignLiveCredentialMapping(t *testing.T) {
	door, _, _, _, calls, _ := seatDoorAuthFixture(t, false)
	otherBound := "seat_00000000000000000000000000000003"
	registration := door.seats[viewerSeatID]
	if err := registration.bind([]string{viewerSeatID, otherBound}); err != nil {
		t.Fatal(err)
	}
	bound := seatDoorAuthRequest(door, otherBound, "/screen/view", viewerSeatID, "")
	w := seatDoorAuthRequest(door, unboundDoorSeatID, "/screen/view", viewerSeatID, "")
	assertSeatDoorSameAnswer(t, bound, w, 403)
	var trace contract.ErrorResponse
	if json.Unmarshal(w.Body.Bytes(), &trace) != nil || trace.Error.RequestID != "unbound-wrong-trace" {
		t.Fatal("scope mapping lost upstream trace")
	}
	var failure contract.ErrorResponse
	if w.Code != 403 || json.Unmarshal(w.Body.Bytes(), &failure) != nil || failure.Error.Message != "invalid Seat credential" || calls.Load() != 1 {
		t.Fatalf("scope refusal=%d %s calls=%d", w.Code, w.Body, calls.Load())
	}
}

func TestSeatFrontDoorIdenticalFabricRefusals(t *testing.T) {
	for _, suffix := range []string{"/screen/view", "/person/viewer"} {
		for _, failure := range []error{fabric.ErrIdentityNotFound, errors.New("private backend detail")} {
			t.Run(suffix+failure.Error(), func(t *testing.T) {
				door, f, _, _, calls, logs := seatDoorAuthFixture(t, false)
				f.identity.set(fabric.Identity{}, failure)
				credential := "wrong-secret"
				if strings.HasPrefix(suffix, "/person/") {
					credential = ""
				}
				want := 503
				if errors.Is(failure, fabric.ErrIdentityNotFound) {
					want = 401
				}
				assertSeatDoorSameAnswer(t, seatDoorAuthRequest(door, viewerSeatID, suffix, credential, ""), seatDoorAuthRequest(door, unboundDoorSeatID, suffix, credential, ""), want)
				if calls.Load() != 0 || len(*logs) != 2 || strings.Contains(strings.Join(*logs, "\n"), "private backend detail") {
					t.Fatalf("lookup failure calls=%d logs=%v", calls.Load(), *logs)
				}
			})
		}
	}
}

func TestSeatFrontDoorIdenticalStalePolicyRefusal(t *testing.T) {
	door, f, policy, _, _, _ := seatDoorAuthFixture(t, false)
	if _, err := policy.Install(policySnapshot(t, f.clock.Now(), 1, 3, nil)); err != nil {
		t.Fatal(err)
	}
	bound := seatDoorAuthRequest(door, viewerSeatID, "/person/viewer", "", "4")
	unbound := seatDoorAuthRequest(door, unboundDoorSeatID, "/person/viewer", "", "4")
	assertSeatDoorSameAnswer(t, bound, unbound, 403)
	var failure contract.ErrorResponse
	if json.Unmarshal(unbound.Body.Bytes(), &failure) != nil || failure.Error.Code != contract.ErrorStalePolicyRevision || !failure.Error.Retryable || failure.Error.Details["installed_policy_revision"] != float64(3) || failure.Error.Details["requested_policy_revision"] != float64(4) {
		t.Fatalf("stale refusal=%s", unbound.Body)
	}
}

func TestSeatFrontDoorUnknownSeatRequiresAuthentication(t *testing.T) {
	for _, person := range []bool{false, true} {
		t.Run(fmt.Sprint(person), func(t *testing.T) {
			door, _, _, _, calls, logs := seatDoorAuthFixture(t, true)
			suffix, credential := "/screen/view", unboundDoorSeatID
			if person {
				suffix, credential = "/person/viewer", ""
			}
			w := seatDoorAuthRequest(door, unboundDoorSeatID, suffix, credential, "")
			var failure contract.ErrorResponse
			if w.Code != 404 || w.Header().Get("Content-Type") != "application/json" || json.Unmarshal(w.Body.Bytes(), &failure) != nil || failure.Error.Code != contract.ErrorNotFound || failure.Error.Message != "unknown Seat" || failure.Error.Retryable {
				t.Fatalf("unknown Seat=%d %s", w.Code, w.Body)
			}
			want := int32(1)
			if person {
				want = 0
			}
			if calls.Load() != want || len(*logs) != 0 {
				t.Fatalf("calls=%d logs=%v", calls.Load(), *logs)
			}
		})
	}
}

func TestSeatFrontDoorBoundSuccessHasNoExtraAuthorization(t *testing.T) {
	door, _, policy, screen, calls, _ := seatDoorAuthFixture(t, true)
	w := seatDoorAuthRequest(door, viewerSeatID, "/person/viewer", "", "")
	if w.Code != 200 || policy.grants.Load() != 1 || policy.checks.Load() != 0 {
		t.Fatalf("person success=%d grants=%d extra checks=%d", w.Code, policy.grants.Load(), policy.checks.Load())
	}
	server := httptest.NewServer(door)
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ws, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/v1/seats/"+viewerSeatID+"/screen/view", &websocket.DialOptions{Subprotocols: []string{"binary"}, HTTPHeader: http.Header{l1.SeatCredentialHeader: []string{viewerSeatID}}})
	if err != nil {
		t.Fatalf("bound screen failed: %v response=%v", err, response)
	}
	_ = ws.CloseNow()
	if calls.Load() != 0 || screen.calls.Load() != 1 || policy.checks.Load() != 0 {
		t.Fatalf("target calls=%d screen calls=%d extra policy=%d", calls.Load(), screen.calls.Load(), policy.checks.Load())
	}
}

func TestSeatFrontDoorUnboundDenialLogIsRedacted(t *testing.T) {
	for _, tc := range []struct{ suffix, credential, kind, code string }{
		{"/screen/view", "", "ai_agent", "unauthorized"},
		{"/screen/view", "wrong-secret", "ai_agent", "forbidden"},
		{"/person/viewer", "", "person", "control_not_authorized"},
	} {
		t.Run(tc.kind+tc.code, func(t *testing.T) {
			door, _, _, control, _, logs := seatDoorAuthFixture(t, false)
			seatDoorAuthRequest(door, unboundDoorSeatID, tc.suffix, tc.credential, "")
			if len(*logs) != 1 {
				t.Fatalf("logs=%v", *logs)
			}
			line := (*logs)[0]
			if !strings.Contains(line, "path="+tc.kind) || !strings.Contains(line, "code="+tc.code) || strings.Contains(line, unboundDoorSeatID) || strings.Contains(line, "wrong-secret") {
				t.Fatalf("unsafe or incomplete log: %s", line)
			}
			control.mu.Lock()
			defer control.mu.Unlock()
			if len(control.events) != 0 {
				t.Fatalf("unbound denial wrote durable evidence: %+v", control.events)
			}
		})
	}
}

func TestSeatFrontDoorIdenticalAdministratorRestrictions(t *testing.T) {
	for _, kind := range []string{"expired-policy", "watch-lost", "wrong-fabric", "machine", "missing-device", "wrong-admin-device", "unbound-admin-device"} {
		t.Run(kind, func(t *testing.T) {
			door, f, policy, _, _, _ := seatDoorAuthFixture(t, true)
			identity, _ := f.identity.WhoIs(t.Context(), "")
			switch kind {
			case "expired-policy":
				f.clock.Advance(11 * time.Minute)
			case "watch-lost":
				policy.Invalidate(ComputerPolicyWatchLost)
			case "wrong-fabric":
				identity.FabricID = "foreign-fabric"
			case "machine":
				identity.Kind = fabric.IdentityKindMachine
			case "missing-device":
				identity.DeviceID = ""
			case "wrong-admin-device", "unbound-admin-device":
				device := "other-device"
				if kind == "unbound-admin-device" {
					device = ""
				}
				snapshot := policySnapshot(t, f.clock.Now(), 1, 3, []l1.ComputerPolicyAdmin{{FabricID: identity.FabricID, UserID: identity.UserID, DeviceID: device}})
				snapshot.AdminDeviceRequired = true
				snapshot.SnapshotDigest, _ = l1.ComputeComputerPolicySnapshotDigest(snapshot)
				if _, err := policy.Install(snapshot); err != nil {
					t.Fatal(err)
				}
			}
			f.identity.set(identity, nil)
			assertSeatDoorSameAnswer(t, seatDoorAuthRequest(door, viewerSeatID, "/person/viewer", "", ""), seatDoorAuthRequest(door, unboundDoorSeatID, "/person/viewer", "", ""), 403)
		})
	}
}

func TestSeatFrontDoorAdministratorWithoutComputerMembership(t *testing.T) {
	door, f, policy, _, _, _ := seatDoorAuthFixture(t, true)
	identity, _ := f.identity.WhoIs(t.Context(), "")
	snapshot := policySnapshot(t, f.clock.Now(), 1, 3, []l1.ComputerPolicyAdmin{{FabricID: identity.FabricID, UserID: identity.UserID, DeviceID: identity.DeviceID}})
	snapshot.AdminDeviceRequired = true
	snapshot.Computers = nil
	snapshot.SnapshotDigest, _ = l1.ComputeComputerPolicySnapshotDigest(snapshot)
	if _, err := policy.Install(snapshot); err != nil {
		t.Fatal(err)
	}
	w := seatDoorAuthRequest(door, unboundDoorSeatID, "/person/viewer", "", "")
	if w.Code != 404 {
		t.Fatalf("administrator required Computer membership: %d %s", w.Code, w.Body)
	}
}

func TestSeatFrontDoorBoundDenialAuditIsPreserved(t *testing.T) {
	door, f, _, control, _, _ := seatDoorAuthFixture(t, false)
	seatDoorAuthRequest(door, viewerSeatID, "/person/viewer", "", "")
	f.frontDoor.denials.flush(t.Context())
	seatDoorAuthRequest(door, unboundDoorSeatID, "/person/viewer", "", "")
	f.frontDoor.denials.flush(t.Context())
	control.mu.Lock()
	defer control.mu.Unlock()
	if len(control.events) != 1 || control.events[0].SeatID != viewerSeatID || control.events[0].Audit == nil || control.events[0].Audit.Kind != l1.ComputerTakeoverAdmissionDenied || control.events[0].Audit.Reason != l1.ComputerTakeoverUnauthorizedIdentity || control.events[0].Audit.EventCount != 1 {
		t.Fatalf("bound denial evidence changed: %+v", control.events)
	}
}

func TestSeatFrontDoorHostCheckPrecedesAuthentication(t *testing.T) {
	door, f, policy, _, calls, logs := seatDoorAuthFixture(t, true)
	before := f.identity.callCount()
	for _, id := range []string{viewerSeatID, unboundDoorSeatID} {
		r := httptest.NewRequest("GET", "http://untrusted.test/v1/seats/"+id+"/person/viewer", nil)
		w := httptest.NewRecorder()
		door.ServeHTTP(w, r)
		if w.Code != 403 || !strings.Contains(w.Body.String(), "Seat viewer host is not allowed") {
			t.Fatalf("host refusal=%d %s", w.Code, w.Body)
		}
	}
	if f.identity.callCount() != before || policy.checks.Load() != 0 || policy.grants.Load() != 0 || calls.Load() != 0 || len(*logs) != 2 {
		t.Fatalf("host refusal invoked authentication: identity=%d policy=%d grants=%d target=%d logs=%v", f.identity.callCount()-before, policy.checks.Load(), policy.grants.Load(), calls.Load(), *logs)
	}
}

func TestSeatFrontDoorUnboundCredentialAvailability(t *testing.T) {
	for _, tc := range []struct {
		name, message string
		status        int
		code          contract.ErrorCode
		want          int
	}{
		{"credential-accepted", "Seat belongs to another Computer", 403, contract.ErrorForbidden, 404},
		{"other-Computer-refusal", "another Computer refusal", 403, contract.ErrorForbidden, 403},
		{"control-unavailable", "control unavailable", 503, contract.ErrorUnavailable, 503},
		{"other-conflict", "another conflict", 409, contract.ErrorConflict, 409},
	} {
		t.Run(tc.name, func(t *testing.T) {
			door, _, _, _, _, _ := seatDoorAuthFixture(t, false)
			calls := 0
			client := newRoundTripClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				var input l1.SeatScreenAuthorityInput
				if r.Method != http.MethodPost || r.URL.Path != "/v1/agent/seats/"+unboundDoorSeatID+"/screen-authority" || json.NewDecoder(r.Body).Decode(&input) != nil || input.Credential != unboundDoorSeatID || input.ComputerID != "" {
					t.Errorf("wrong authentication request: %s %+v", r.URL, input)
				}
				w.WriteHeader(tc.status)
				_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: tc.code, Message: tc.message}})
			}))
			door.authentication.control = client
			w := seatDoorAuthRequest(door, unboundDoorSeatID, "/screen/view", unboundDoorSeatID, "")
			if w.Code != tc.want || calls != 1 {
				t.Fatalf("authentication status=%d body=%s calls=%d", w.Code, w.Body, calls)
			}
		})
	}
}

func TestSeatFrontDoorRefusalRoundTrips(t *testing.T) {
	for _, tc := range []struct {
		name, suffix, credential string
		want, status             int
	}{
		{"screen-missing", "/screen/view", "", 0, 401},
		{"screen-wrong", "/screen/view", "wrong-secret", 1, 403},
		{"screen-expired", "/screen/input", "expired-secret", 1, 409},
		{"copy-missing", "/copy?direction=in&path=file", "", 0, 200},
		{"copy-wrong", "/copy?direction=in&path=file", "wrong-secret", 1, 200},
		{"copy-expired", "/copy?direction=in&path=file", "expired-secret", 1, 200},
		{"person-non-administrator", "/person/viewer", "", 0, 403},
		{"person-sideband-no-token", "/person/wefty/control/take", "", 0, 401},
	} {
		t.Run(tc.name, func(t *testing.T) {
			door, f, _, boundCalls, unboundCalls, _ := seatDoorAuthFixture(t, false)
			beforeIdentity := f.identity.callCount()
			bound := seatDoorAuthRequest(door, viewerSeatID, tc.suffix, tc.credential, "")
			boundIdentityCalls := f.identity.callCount() - beforeIdentity
			beforeIdentity = f.identity.callCount()
			unboundID := unboundDoorSeatID
			if tc.credential == "expired-secret" {
				if err := door.seats[viewerSeatID].bind(nil); err != nil {
					t.Fatal(err)
				}
				unboundID = viewerSeatID
			}
			unbound := seatDoorAuthRequest(door, unboundID, tc.suffix, tc.credential, "")
			unboundIdentityCalls := f.identity.callCount() - beforeIdentity
			if boundIdentityCalls != unboundIdentityCalls {
				t.Fatalf("Fabric identity calls: bound=%d unbound=%d", boundIdentityCalls, unboundIdentityCalls)
			}
			assertSeatDoorAnswersIgnoringCopyID(t, bound, unbound, tc.status)
			if boundCalls.calls.Load() != int32(tc.want) || unboundCalls.Load() != int32(tc.want) {
				t.Fatalf("L1 round trips: bound=%d unbound=%d want=%d", boundCalls.calls.Load(), unboundCalls.Load(), tc.want)
			}
		})
	}
}

var seatCopyIDForComparison = regexp.MustCompile(`"copy_id":"[^"]+"`)

func assertSeatDoorAnswersIgnoringCopyID(t *testing.T, bound, unbound *httptest.ResponseRecorder, status int) {
	t.Helper()
	if bound.Header().Get("Content-Type") == "application/x-ndjson" {
		for _, answer := range []*httptest.ResponseRecorder{bound, unbound} {
			var event l1.SeatCopyStreamEvent
			if json.Unmarshal(answer.Body.Bytes(), &event) != nil || event.CopyID == "" {
				t.Fatalf("copy response has no node copy_id: %s", answer.Body)
			}
			data := seatCopyIDForComparison.ReplaceAll(answer.Body.Bytes(), []byte(`"copy_id":"COPY_ID"`))
			answer.Body.Reset()
			_, _ = answer.Body.Write(data)
		}
	}
	assertSeatDoorSameAnswer(t, bound, unbound, status)
}

func TestSeatFrontDoorIdenticalCopyRefusals(t *testing.T) {
	for _, tc := range []struct{ name, suffix, credential, message string }{
		{"wrong", "/copy?direction=in&path=file", "wrong-secret", "invalid Seat credential"},
		{"missing", "/copy?direction=in&path=file", "", "Seat credential is required"},
		{"bad-direction-without-credential", "/copy?direction=bad&path=file", "", "copy needs direction in, out or sync and a path"},
		{"missing-path-without-credential", "/copy?direction=in", "", "copy needs direction in, out or sync and a path"},
		{"nul-path-without-credential", "/copy?direction=in&path=%00", "", "copy needs direction in, out or sync and a path"},
		{"bad-sync-path-without-credential", "/copy?direction=sync&path=other", "", "sync path must be work"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			door, _, _, boundCalls, unboundCalls, _ := seatDoorAuthFixture(t, false)
			bound := seatDoorAuthRequest(door, viewerSeatID, tc.suffix, tc.credential, "")
			unbound := seatDoorAuthRequest(door, unboundDoorSeatID, tc.suffix, tc.credential, "")
			assertSeatDoorAnswersIgnoringCopyID(t, bound, unbound, 200)
			var event l1.SeatCopyStreamEvent
			if json.Unmarshal(unbound.Body.Bytes(), &event) != nil || event.Error == nil || event.Error.Message != tc.message {
				t.Fatalf("copy refusal=%s", unbound.Body)
			}
			if tc.credential == "" && (boundCalls.calls.Load() != 0 || unboundCalls.Load() != 0) {
				t.Fatal("copy validation/presence refusal reached L1")
			}
		})
	}
}

func TestSeatFrontDoorIdenticalSidebandRefusals(t *testing.T) {
	for _, action := range []string{"take", "release"} {
		for _, token := range []string{"", "bogus-session-token"} {
			for _, admin := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/admin=%t", action, token, admin), func(t *testing.T) {
					door, _, policy, _, calls, logs := seatDoorAuthFixture(t, admin)
					answers := []*httptest.ResponseRecorder{}
					for _, id := range []string{viewerSeatID, unboundDoorSeatID} {
						r := httptest.NewRequest("POST", "http://127.0.0.1/v1/seats/"+id+"/person/wefty/control/"+action, nil)
						if token != "" {
							r.Header.Set(computerControlTokenHeader, token)
						}
						w := httptest.NewRecorder()
						door.ServeHTTP(w, r)
						answers = append(answers, w)
					}
					assertSeatDoorSameAnswer(t, answers[0], answers[1], 401)
					if policy.checks.Load() != 0 || policy.grants.Load() != 0 || calls.Load() != 0 || len(*logs) != 1 {
						t.Fatalf("sideband checked policy/L1 before token: checks=%d grants=%d L1=%d logs=%v", policy.checks.Load(), policy.grants.Load(), calls.Load(), *logs)
					}
				})
			}
		}
	}
}

func TestSeatFrontDoorSharedAdministratorPolicyParity(t *testing.T) {
	for _, kind := range []string{"administrator", "non-administrator", "unknown-identity", "wrong-fabric", "machine", "missing-device", "wrong-admin-device", "missing-admin-device", "expired-snapshot", "missing-snapshot", "watch-lost", "stale-revision-non-grant", "stale-revision-administrator"} {
		t.Run(kind, func(t *testing.T) {
			door, f, policy, _, _, _ := seatDoorAuthFixture(t, true)
			identity, _ := f.identity.WhoIs(t.Context(), "")
			admins := []l1.ComputerPolicyAdmin{{FabricID: identity.FabricID, UserID: identity.UserID, DeviceID: identity.DeviceID}}
			snapshot := policySnapshot(t, f.clock.Now(), 1, 3, admins)
			snapshot.AdminDeviceRequired = true
			revision := ""
			switch kind {
			case "non-administrator", "stale-revision-non-grant":
				snapshot.Admins = nil
			case "unknown-identity":
				identity = fabric.Identity{}
			case "wrong-fabric":
				identity.FabricID = "other-fabric"
			case "machine":
				identity.Kind = fabric.IdentityKindMachine
			case "missing-device":
				identity.DeviceID = ""
			case "wrong-admin-device":
				snapshot.Admins[0].DeviceID = "other-device"
			case "missing-admin-device":
				snapshot.Admins[0].DeviceID = ""
			}
			snapshot.SnapshotDigest, _ = l1.ComputeComputerPolicySnapshotDigest(snapshot)
			if _, err := policy.Install(snapshot); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "expired-snapshot":
				f.clock.Advance(11 * time.Minute)
			case "missing-snapshot":
				empty := NewComputerPolicyCache(f.clock, "node-1", "boot-1")
				t.Cleanup(empty.Close)
				policy.ComputerPolicyCache = empty
			case "watch-lost":
				policy.Invalidate(ComputerPolicyWatchLost)
			case "stale-revision-non-grant", "stale-revision-administrator":
				revision = "4"
			}
			f.identity.set(identity, nil)
			local, _ := policy.CheckAdministrator(identity)
			policy.mu.Lock()
			installed, valid := policy.snapshot, policy.valid
			policy.mu.Unlock()
			l1Decision := l1.EvaluateComputerPolicyPerson(installed, valid, "computer-1", identity)
			authorization, err := policy.AcquireGrant("computer-1", identity)
			if local != l1Decision.Administrator || local != authorization.IsAdministrator() {
				t.Fatalf("administrator verdicts differ: local=%t L1=%+v bound=%+v err=%v", local, l1Decision, authorization, err)
			}
			authorization.Release()
			bound := seatDoorAuthRequest(door, viewerSeatID, "/person/viewer", "", revision)
			unbound := seatDoorAuthRequest(door, unboundDoorSeatID, "/person/viewer", "", revision)
			if !local || revision != "" {
				assertSeatDoorSameAnswer(t, bound, unbound, 403)
				if revision != "" {
					var failure contract.ErrorResponse
					if json.Unmarshal(unbound.Body.Bytes(), &failure) != nil || failure.Error.Code != contract.ErrorStalePolicyRevision || failure.Error.Details["requested_policy_revision"] != float64(4) || failure.Error.Details["installed_policy_revision"] != float64(3) {
						t.Fatalf("stale revision refusal=%s", unbound.Body)
					}
				}
			} else if bound.Code != 200 || unbound.Code != 404 {
				t.Fatalf("administrator responses: bound=%d unbound=%d", bound.Code, unbound.Code)
			}
		})
	}
}

func TestSeatFrontDoorAuthenticatedUnboundCopyKeepsStreamContract(t *testing.T) {
	door, _, _, boundCalls, unboundCalls, logs := seatDoorAuthFixture(t, false)
	w := seatDoorAuthRequest(door, unboundDoorSeatID, "/copy?direction=in&path=file", unboundDoorSeatID, "")
	var event l1.SeatCopyStreamEvent
	if w.Code != 200 || w.Header().Get("Content-Type") != "application/x-ndjson" || json.Unmarshal(w.Body.Bytes(), &event) != nil || event.CopyID == "" || event.Error == nil || event.Error.Code != contract.ErrorNotFound || event.Error.Message != "unknown Seat" {
		t.Fatalf("authenticated unknown copy=%d %s", w.Code, w.Body)
	}
	if boundCalls.calls.Load() != 0 || unboundCalls.Load() != 1 || len(*logs) != 0 {
		t.Fatalf("copy authority calls bound=%d unbound=%d logs=%v", boundCalls.calls.Load(), unboundCalls.Load(), *logs)
	}
}

func TestSeatFrontDoorIdenticalRequestValidation(t *testing.T) {
	for _, tc := range []struct {
		name, method, suffix, credential, protocol string
		status                                     int
	}{
		{"screen-subprotocol", "GET", "/screen/view", "wrong-secret", "", 400},
		{"screen-method", "POST", "/screen/view", "wrong-secret", "binary", 405},
		{"screen-mode", "GET", "/screen/other", "wrong-secret", "binary", 404},
		{"screen-query", "GET", "/screen/view?query=bad", "wrong-secret", "binary", 404},
		{"person-method", "POST", "/person/viewer", "", "", 404},
		{"sideband-method", "GET", "/person/wefty/control/take", "", "", 404},
		{"credential-on-person-path", "GET", "/person/viewer", "wrong-secret", "", 404},
	} {
		t.Run(tc.name, func(t *testing.T) {
			door, _, _, boundCalls, unboundCalls, _ := seatDoorAuthFixture(t, false)
			answers := []*httptest.ResponseRecorder{}
			for _, id := range []string{viewerSeatID, unboundDoorSeatID} {
				r := httptest.NewRequest(tc.method, "http://127.0.0.1/v1/seats/"+id+tc.suffix, nil)
				if tc.credential != "" {
					r.Header.Set(l1.SeatCredentialHeader, tc.credential)
				}
				if tc.protocol != "" {
					r.Header.Set("Sec-WebSocket-Protocol", tc.protocol)
				}
				w := httptest.NewRecorder()
				door.ServeHTTP(w, r)
				answers = append(answers, w)
			}
			assertSeatDoorSameAnswer(t, answers[0], answers[1], tc.status)
			if boundCalls.calls.Load() != 0 || unboundCalls.Load() != 0 {
				t.Fatalf("request validation reached L1: bound=%d unbound=%d", boundCalls.calls.Load(), unboundCalls.Load())
			}
		})
	}
}

func TestSeatFrontDoorCrossSeatCredentialParity(t *testing.T) {
	for _, suffix := range []string{"/screen/view", "/screen/input", "/copy?direction=in&path=file"} {
		for _, expired := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/expired=%t", suffix, expired), func(t *testing.T) {
				door, _, _, boundCalls, unboundCalls, _ := seatDoorAuthFixture(t, false)
				seatC := "seat_00000000000000000000000000000003"
				if err := door.seats[viewerSeatID].bind([]string{viewerSeatID, seatC}); err != nil {
					t.Fatal(err)
				}
				credentialB := viewerSeatID
				if expired {
					credentialB = "expired-secret"
				}
				bound := seatDoorAuthRequest(door, seatC, suffix, credentialB, "")
				unbound := seatDoorAuthRequest(door, unboundDoorSeatID, suffix, credentialB, "")
				want := 403
				if strings.HasPrefix(suffix, "/copy") {
					want = 200
				}
				assertSeatDoorAnswersIgnoringCopyID(t, bound, unbound, want)
				var failure contract.ErrorResponse
				if json.Unmarshal(unbound.Body.Bytes(), &failure) != nil || failure.Error.Code != contract.ErrorForbidden || failure.Error.Message != "invalid Seat credential" {
					t.Fatalf("cross-Seat refusal=%s", unbound.Body)
				}
				if boundCalls.calls.Load() != 1 || unboundCalls.Load() != 1 {
					t.Fatalf("cross-Seat L1 calls bound=%d unbound=%d", boundCalls.calls.Load(), unboundCalls.Load())
				}
			})
		}
	}
}

// Model the existing authority endpoint and the old screen-target route so
// restoring r2 proves its cross-Seat expiry failure using real refusal shapes.
func seatDoorCredentialReply(t *testing.T, r *http.Request) *http.Response {
	t.Helper()
	feature := r.URL.Path[strings.LastIndex(r.URL.Path, "/"):]
	probe := feature == "/screen-authority" || feature == "/copy-authority"
	credential := r.Header.Get(l1.SeatCredentialHeader)
	requested := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/seats/"), "/screen-target")
	if probe {
		var input l1.SeatScreenAuthorityInput
		if r.Method != http.MethodPost || json.NewDecoder(r.Body).Decode(&input) != nil || input.ComputerID != "" || input.JobID != "" || input.AttemptID != "" || input.NodeID != "" || input.BootSessionID != "" || input.FencingToken != "" || input.Person || input.Input {
			t.Errorf("invalid credential-only authority probe: %+v", input)
		}
		credential = input.Credential
		requested = strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/agent/seats/"), feature)
	} else if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, "/screen-target") {
		t.Errorf("unexpected credential call: %s %s", r.Method, r.URL)
	}
	owner, expired := "", credential == "expired-secret"
	switch credential {
	case viewerSeatID, "expired-secret":
		owner = viewerSeatID
	case unboundDoorSeatID:
		owner = unboundDoorSeatID
	}
	w := httptest.NewRecorder()
	w.Header().Set("Content-Type", "application/json")
	code, message, status := contract.ErrorForbidden, "invalid Seat credential", 403
	switch {
	case owner == "":
	case !probe && expired:
		code, message, status = "seat_ended", "Seat ended: expired", 409
	case requested != owner:
		if !probe {
			message = "Seat credential does not authorize this operation"
		}
	case expired:
		code, message, status = "seat_ended", "Seat ended: expired", 409
	case probe:
		message = "Seat belongs to another Computer"
	default:
		_ = json.NewEncoder(w).Encode(l1.SeatCopyTarget{Endpoint: "ws://other.test/websockify"})
		return w.Result()
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: code, Message: message, RequestID: "unbound-wrong-trace"}})
	return w.Result()
}

// Both branches use the real Client, including net/http's URL-bearing error.
// Different Seat IDs must not appear in the public transport-failure response.
func TestSeatFrontDoorTransportFailureParity(t *testing.T) {
	for _, suffix := range []string{"/screen/view", "/screen/input", "/copy?direction=in&path=file"} {
		t.Run(suffix, func(t *testing.T) {
			door, _, _, _, _, logs := seatDoorAuthFixture(t, false)
			var paths []string
			client := &Client{baseURL: "http://unreachable.test", operationTimeout: time.Second, httpClient: &http.Client{Transport: captureRoundTrip(func(r *http.Request) (*http.Response, error) {
				paths = append(paths, r.URL.Path)
				return nil, &net.OpError{Op: "dial", Net: "tcp", Err: fmt.Errorf("private cause URL=%s credential=wrong-secret: %w", r.URL, syscall.ECONNREFUSED)}
			})}}
			door.authentication.control = client
			registration := door.seats[viewerSeatID]
			screens := &seatScreenHandler{config: seatScreenConfig{ctx: t.Context(), control: client}, seats: map[string]*seatScreen{viewerSeatID: {ctx: t.Context()}}}
			registration.handler = newSeatFeatureMux(screens, newSeatCopyHandler(copyRuntimeFixture{t.TempDir()}, client, workloadrunner.AttemptAuthority{}, "computer-1"))
			bound := seatDoorAuthRequest(door, viewerSeatID, suffix, "wrong-secret", "")
			unbound := seatDoorAuthRequest(door, unboundDoorSeatID, suffix, "wrong-secret", "")
			status, feature := 503, "/screen-authority"
			if strings.HasPrefix(suffix, "/copy") {
				status, feature = 200, "/copy-authority"
			}
			assertSeatDoorAnswersIgnoringCopyID(t, bound, unbound, status)
			if len(paths) != 2 || paths[0] != "/v1/agent/seats/"+viewerSeatID+feature || paths[1] != "/v1/agent/seats/"+unboundDoorSeatID+feature {
				t.Fatalf("authority calls=%v", paths)
			}
			var diagnostics []string
			stage := "screen authorization"
			if strings.HasPrefix(suffix, "/copy") {
				stage = "copy authorization"
			}
			for _, line := range *logs {
				if strings.HasPrefix(line, "Seat front door transport ") {
					diagnostics = append(diagnostics, line)
					if !strings.Contains(line, fmt.Sprintf("stage=%q", stage)) || !strings.Contains(line, fmt.Sprintf("cause=%q", syscall.ECONNREFUSED.Error())) || len(line) > 256 {
						t.Fatalf("missing/bounded diagnostic: %s", line)
					}
					for _, secret := range []string{viewerSeatID, unboundDoorSeatID, "wrong-secret", "unreachable.test", "private cause"} {
						if strings.Contains(line, secret) {
							t.Fatalf("diagnostic leaked request data: %s", line)
						}
					}
				}
			}
			if len(diagnostics) != 2 {
				t.Fatalf("want one diagnostic per branch, got %v", *logs)
			}
			if strings.Contains(unbound.Body.String(), "unreachable.test") || strings.Contains(unbound.Body.String(), unboundDoorSeatID) {
				t.Fatalf("transport details leaked: %s", unbound.Body)
			}
		})
	}
}

func TestSeatCredentialProbeClassifier(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		code     contract.ErrorCode
		message  string
		accepted bool
	}{
		{"exact", 403, contract.ErrorForbidden, contract.SeatComputerMismatchMessage, true},
		{"wrong-status", 409, contract.ErrorForbidden, contract.SeatComputerMismatchMessage, false},
		{"wrong-code", 403, contract.ErrorPrincipalForbidden, contract.SeatComputerMismatchMessage, false},
		{"wrong-message", 403, contract.ErrorForbidden, "invalid Seat credential", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failure := &ProtocolError{StatusCode: tc.status, APIError: contract.APIError{Code: tc.code, Message: tc.message}}
			err := seatCredentialProbeError(fmt.Errorf("wrapped: %w", failure))
			if (err == nil) != tc.accepted || (!tc.accepted && !errors.Is(err, failure)) {
				t.Fatalf("accepted=%t err=%v", tc.accepted, err)
			}
		})
	}
}
