package tsnet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"tailscale.com/client/local"
	"tailscale.com/ipn"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tailcfg"
	"tailscale.com/types/key"
)

func TestParticipantRefusesUnusableOrReplacedIdentityBeforeDial(t *testing.T) {
	for _, tc := range []struct {
		name, status string
		wantError    bool
	}{
		{"needs-login", `{"BackendState":"NeedsLogin"}`, true},
		{"needs-machine-auth", `{"BackendState":"NeedsMachineAuth"}`, true},
		{"expired", `{"BackendState":"Running","Self":{"ID":"saved","Tags":["tag:wefty-client"],"Expired":true}}`, true},
		{"copied-state", `{"BackendState":"Running","Self":{"ID":"other","Tags":["tag:wefty-client"]}}`, true},
		{"untagged-client", `{"BackendState":"Running","Self":{"ID":"saved","UserID":42}}`, true},
		{"wrong-tag", `{"BackendState":"Running","Self":{"ID":"saved","Tags":["tag:wefty-control"]}}`, true},
		{"extra-tag", `{"BackendState":"Running","Self":{"ID":"saved","Tags":["tag:wefty-client","tag:wefty-control"]}}`, true},
		{"client", `{"BackendState":"Running","Self":{"ID":"saved","Tags":["tag:wefty-client"],"TailscaleIPs":["100.64.0.1"]}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &Fabric{expectedIdentity: &fabric.Identity{NodeID: "saved", Tags: []string{"tag:wefty-client"}}, client: statusClient(t, tc.status)}
			if !tc.wantError {
				status := destinationStatus()
				if err := json.Unmarshal([]byte(tc.status), status); err != nil {
					t.Fatal(err)
				}
				withDestinationStatus(t, f, status)
			}
			dialled := false
			stop := errors.New("dial boundary")
			f.dialNode = func(context.Context, string, string) (net.Conn, error) { dialled = true; return nil, stop }
			f.dialPlan = func(context.Context, string, string) (netip.AddrPort, bool, error) {
				return netip.MustParseAddrPort("100.64.0.3:80"), true, nil
			}
			_, err := f.Dial(t.Context(), "tcp", "100.64.0.3:80")
			if tc.wantError {
				var refused *fabric.ParticipantIdentityError
				if dialled || !errors.As(err, &refused) || refused.Person {
					t.Fatalf("dialled=%v err=%v", dialled, err)
				}
			} else if !dialled || !errors.Is(err, stop) {
				t.Fatalf("dialled=%v err=%v", dialled, err)
			}
		})
	}
	for _, tc := range []struct {
		name, status string
		bad          bool
	}{
		{"person", `{"BackendState":"Running","Self":{"ID":"saved","UserID":42}}`, false},
		{"wrong-user", `{"BackendState":"Running","Self":{"ID":"saved","UserID":43}}`, true},
		{"tagged-person", `{"BackendState":"Running","Self":{"ID":"saved","UserID":42,"Tags":["tag:wefty-client"]}}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &Fabric{expectedIdentity: &fabric.Identity{NodeID: "saved", UserID: "42"}, client: statusClient(t, tc.status)}
			err := f.checkParticipant(t.Context())
			if (err != nil) != tc.bad {
				t.Fatalf("validation=%v", err)
			}
			if tc.bad {
				var refused *fabric.ParticipantIdentityError
				if !errors.As(err, &refused) || !refused.Person {
					t.Fatalf("person refusal=%v", err)
				}
			}
		})
	}
}

func statusClient(t *testing.T, status string) *local.Client {
	t.Helper()
	return &local.Client{OmitAuth: true, Transport: identityRoundTripper(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/localapi/v0/status" {
			t.Fatalf("unexpected API %s", r.URL.Path)
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(status))}, nil
	})}
}

func TestOpaqueListenBoundsWaitAndRefusesLogin(t *testing.T) {
	for _, state := range []string{"NeedsLogin", "NeedsMachineAuth", "Starting"} {
		t.Run(state, func(t *testing.T) {
			watched := false
			f := &Fabric{expectedIdentity: &fabric.Identity{NodeID: "saved"}, client: &local.Client{OmitAuth: true, Transport: identityRoundTripper(func(r *http.Request) (*http.Response, error) {
				body := `{"BackendState":"` + state + `"}`
				if r.URL.Path == "/localapi/v0/watch-ipn-bus" {
					watched = true
					if _, ok := r.Context().Deadline(); !ok {
						return nil, errors.New("unbounded listen wait")
					}
					return nil, context.DeadlineExceeded
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
			})}}
			_, err := f.Listen("tcp", "opaque-local")
			if err == nil {
				t.Fatal("accepted non-running state")
			}
			if state == "Starting" {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("listen=%v", err)
				}
			} else {
				var refused *fabric.ParticipantIdentityError
				if watched || !errors.As(err, &refused) {
					t.Fatalf("watched=%v err=%v", watched, err)
				}
			}
		})
	}
}

func TestSavedParticipantAndEnrollerNeverPrintLoginHints(t *testing.T) {
	t.Setenv(printEnrollmentURLEnv, "1")
	var lines []string
	f, err := New(Config{Name: "wefty://client/one", StateDir: savedBackendState(t), ExpectedIdentity: &fabric.Identity{NodeID: "saved", Tags: []string{"tag:wefty-client"}}, Logf: func(format string, args ...any) { lines = append(lines, format) }})
	if err != nil {
		t.Fatal(err)
	}
	f.server.UserLogf("To authenticate, visit https://login.example.test/a/secret")
	f.server.Logf("To authenticate, visit https://login.example.test/a/secret")
	if len(lines) != 0 {
		t.Fatalf("participant login emitted: %v", lines)
	}
	e := Enroller{newNode: func(c Config) (enrolNode, error) {
		if c.Logf == nil {
			t.Fatal("enroller left the default user logger enabled")
		}
		c.Logf("enrollment hint")
		return nil, errors.New("constructor boundary")
	}}
	_, _ = e.Enrol(t.Context(), fabric.EnrolSpec{Name: "wefty://person/one", StateDirectory: t.TempDir(), LoginURL: func(string) {}})
	_ = e.Logout(t.Context(), fabric.EnrolSpec{Name: "wefty://person/one", StateDirectory: t.TempDir()})
}

func TestParticipantRefusesLoginTransitionBeforeAnyTransport(t *testing.T) {
	for _, state := range []ipn.State{ipn.NeedsLogin, ipn.NeedsMachineAuth} {
		t.Run(state.String(), func(t *testing.T) {
			f := &Fabric{expectedIdentity: &fabric.Identity{NodeID: "saved", Tags: []string{"tag:wefty-client"}}}
			f.client = &local.Client{OmitAuth: true, Transport: identityRoundTripper(func(r *http.Request) (*http.Response, error) {
				body := `{"BackendState":"Starting"}`
				if r.URL.Path == "/localapi/v0/watch-ipn-bus" {
					body = fmt.Sprintf(`{"State":%d}`, state)
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
			})}
			f.dialNode = func(context.Context, string, string) (net.Conn, error) {
				t.Fatal("transport opened during login")
				return nil, nil
			}
			f.listenNode = func(string, string) (net.Listener, error) { t.Fatal("listener opened during login"); return nil, nil }
			_, err := f.Dial(t.Context(), "tcp", "wefty://control-plane")
			var refused *fabric.ParticipantIdentityError
			if !errors.As(err, &refused) {
				t.Fatalf("Dial = %v", err)
			}
			if _, err := f.Listen("tcp", "opaque-local"); !errors.As(err, &refused) {
				t.Fatalf("Listen = %v", err)
			}
			if _, err := f.WhoIs(t.Context(), "remote"); !errors.As(err, &refused) {
				t.Fatalf("WhoIs = %v", err)
			}
		})
	}
}

func TestLegacyOpaqueJoinWaitsThroughLoginStates(t *testing.T) {
	for _, state := range []ipn.State{ipn.NeedsLogin, ipn.NeedsMachineAuth} {
		for _, operation := range []string{"dial", "listen"} {
			t.Run(state.String()+"/"+operation, func(t *testing.T) {
				f, err := New(Config{Name: "wefty://client/legacy", StateDir: t.TempDir(), Credential: fabric.Credential{Value: "test-join-key"}})
				if err != nil {
					t.Fatal(err)
				}
				statusCalls, watched := 0, false
				f.client = &local.Client{OmitAuth: true, Transport: identityRoundTripper(func(r *http.Request) (*http.Response, error) {
					var body string
					switch r.URL.Path {
					case "/localapi/v0/status":
						statusCalls++
						if statusCalls == 1 {
							body = fmt.Sprintf(`{"BackendState":%q}`, state.String())
						} else {
							payload, err := json.Marshal(ipnstate.Status{BackendState: "Running", Self: &ipnstate.PeerStatus{ID: "local"}, Peer: map[key.NodePublic]*ipnstate.PeerStatus{key.NewNode().Public(): {ID: "remote", TailscaleIPs: []netip.Addr{netip.MustParseAddr("100.64.0.1")}}}})
							if err != nil {
								t.Fatal(err)
							}
							body = string(payload)
						}
					case "/localapi/v0/watch-ipn-bus":
						watched = true
						body = fmt.Sprintf(`{"State":%d} {"State":%d}`, state, ipn.Running)
					default:
						t.Fatalf("unexpected API %s", r.URL.Path)
					}
					return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
				})}
				stop := errors.New("transport boundary")
				reached := false
				f.dialNode = func(context.Context, string, string) (net.Conn, error) { reached = true; return nil, stop }
				f.dialPlan = func(_ context.Context, _, address string) (netip.AddrPort, bool, error) {
					ipp, err := netip.ParseAddrPort(address)
					return ipp, err == nil, err
				}
				f.listenNode = func(string, string) (net.Listener, error) { reached = true; return nil, stop }
				if operation == "dial" {
					_, err = f.Dial(t.Context(), "tcp", "remote")
				} else {
					_, err = f.Listen("tcp", "local")
				}
				if !watched || !reached || !errors.Is(err, stop) {
					t.Fatalf("watched=%v reached=%v err=%v", watched, reached, err)
				}
			})
		}
	}
}

func TestSavedParticipantKeepsOrdinaryDiagnostics(t *testing.T) {
	for _, optedIn := range []string{"0", "1"} {
		t.Run(optedIn, func(t *testing.T) {
			t.Setenv(printEnrollmentURLEnv, optedIn)
			var lines []string
			f, err := New(Config{Name: "wefty://client/one", StateDir: savedBackendState(t), ExpectedIdentity: &fabric.Identity{NodeID: "saved"}, Logf: func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }})
			if err != nil {
				t.Fatal(err)
			}
			for _, logf := range []func(string, ...any){f.server.Logf, f.server.UserLogf} {
				logf("To authenticate, visit %s", "https://login.example.test/a/secret")
				logf("%s", enrollmentRequiredMessage)
				logf("backend connection retry %d", 7)
			}
			if len(lines) != 2 || lines[0] != "backend connection retry 7" || lines[1] != "backend connection retry 7" {
				t.Fatalf("logs=%q", lines)
			}
		})
	}
}

func TestParticipantUnreachableIsTypedAndCancellationStaysLocal(t *testing.T) {
	for _, phase := range []string{"status", "watch", "watch-read", "confirm-status"} {
		for _, interrupt := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cancel=%v", phase, interrupt), func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if !interrupt {
					var stop context.CancelFunc
					ctx, stop = context.WithTimeout(ctx, 20*time.Millisecond)
					defer stop()
				}
				f := &Fabric{expectedIdentity: &fabric.Identity{NodeID: "saved", Tags: []string{"tag:wefty-client"}}}
				reads := 0
				f.client = &local.Client{OmitAuth: true, Transport: identityRoundTripper(func(r *http.Request) (*http.Response, error) {
					if r.URL.Path == "/localapi/v0/status" {
						reads++
						if phase != "status" && (phase != "confirm-status" || reads == 1) {
							return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"BackendState":"Starting"}`))}, nil
						}
					} else if phase == "confirm-status" {
						return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(fmt.Sprintf(`{"State":%d}`, ipn.Running)))}, nil
					} else if phase == "watch-read" {
						var stop func()
						if interrupt {
							stop = cancel
						}
						return &http.Response{StatusCode: 200, Header: make(http.Header), Body: &participantWaitBody{ctx: r.Context(), cancel: stop}}, nil
					}
					if interrupt {
						cancel()
					}
					<-r.Context().Done()
					return nil, r.Context().Err()
				})}
				err := f.VerifyParticipant(ctx)
				var unreachable *fabric.ParticipantUnreachableError
				if interrupt {
					if !errors.Is(err, context.Canceled) || errors.As(err, &unreachable) {
						t.Fatalf("cancellation=%T %v", err, err)
					}
				} else if !errors.As(err, &unreachable) || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("untyped timeout=%T %v", err, err)
				}
			})
		}
	}
}

// Model a stalled watch response without a socket or a real backend.
type participantWaitBody struct {
	ctx    context.Context
	cancel func()
}

func (b *participantWaitBody) Read([]byte) (int, error) {
	if b.cancel != nil {
		b.cancel()
	}
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}
func (*participantWaitBody) Close() error { return nil }

func TestParticipantLoginFilterHandlesMultipleURLsAndLines(t *testing.T) {
	t.Setenv(printEnrollmentURLEnv, "1")
	var lines []string
	f, err := New(Config{Name: "wefty://client/one", StateDir: savedBackendState(t), ExpectedIdentity: &fabric.Identity{NodeID: "saved"}, Logf: func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }})
	if err != nil {
		t.Fatal(err)
	}
	for _, logf := range []func(string, ...any){f.server.Logf, f.server.UserLogf} {
		logf("connected url=https://control.example.test,login=https://login.example.test/a/secret")
		logf("backend before\nTo authenticate, visit https://login.example.test/a/secret\nbackend after")
		logf("register: HTTPS://control.example.test/register/secret")
	}
	if len(lines) != 2 || lines[0] != "backend before\nbackend after" || lines[1] != lines[0] {
		t.Fatalf("logs=%q", lines)
	}
}

func TestParticipantUnreachableHidesBackendDetails(t *testing.T) {
	for _, cause := range []error{context.DeadlineExceeded, errors.New("tailscale backend failed at local-tailscaled.sock/localapi/v0/status")} {
		for _, phase := range []string{"status", "watch"} {
			t.Run(phase+"/"+cause.Error(), func(t *testing.T) {
				f := &Fabric{expectedIdentity: &fabric.Identity{NodeID: "saved", Tags: []string{"tag:wefty-client"}}}
				f.client = &local.Client{OmitAuth: true, Transport: identityRoundTripper(func(r *http.Request) (*http.Response, error) {
					if phase == "watch" && r.URL.Path == "/localapi/v0/status" {
						return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"BackendState":"Starting"}`))}, nil
					}
					return nil, cause
				})}
				err := f.VerifyParticipant(t.Context())
				var unreachable *fabric.ParticipantUnreachableError
				if !errors.As(err, &unreachable) || !errors.Is(err, cause) {
					t.Fatalf("lost typed cause: %v", err)
				}
				for _, fragment := range []string{"tailscale", "tailscaled", "localapi", ".sock"} {
					if strings.Contains(strings.ToLower(err.Error()), fragment) {
						t.Errorf("public error exposes %q: %v", fragment, err)
					}
				}
			})
		}
	}
}

func TestSavedParticipantRefusesLogicalPeerDefaults(t *testing.T) {
	for _, saved := range []bool{true, false} {
		for _, address := range []string{"wefty://control-plane", "wefty://run-ledger", "wefty://node/studio", "wefty://client/studio", "WEFTY://person/studio", "100.64.0.2:80"} {
			t.Run(fmt.Sprintf("saved=%v/%s", saved, address), func(t *testing.T) {
				// Advertise real remote peers as well: these cases must remain
				// valid after authenticated peer-name resolution (#1075) lands.
				peers := make(map[key.NodePublic]*ipnstate.PeerStatus)
				for i, label := range []string{"control-plane", "run-ledger", "node-da0daaba4b156961", "client-da0daaba4b156961", "person-da0daaba4b156961"} {
					peers[key.NewNode().Public()] = &ipnstate.PeerStatus{ID: tailcfg.StableNodeID(label), DNSName: label + ".identity-domain.", TailscaleIPs: []netip.Addr{netip.MustParseAddr(fmt.Sprintf("100.64.0.%d", i+2))}}
				}
				payload, err := json.Marshal(map[string]any{"BackendState": "Running", "CurrentTailnet": map[string]string{"MagicDNSSuffix": "identity-domain"}, "Self": map[string]any{"ID": "saved", "Tags": []string{"tag:wefty-client"}}, "Peer": peers})
				if err != nil {
					t.Fatal(err)
				}
				f := &Fabric{client: statusClient(t, string(payload))}
				if saved {
					f.expectedIdentity = &fabric.Identity{NodeID: "saved", Tags: []string{"tag:wefty-client"}}
				}
				planned, dialled := false, false
				stop := errors.New("transport boundary")
				f.dialPlan = func(context.Context, string, string) (netip.AddrPort, bool, error) {
					planned = true
					return netip.MustParseAddrPort("100.64.0.2:80"), true, nil
				}
				f.dialNode = func(context.Context, string, string) (net.Conn, error) { dialled = true; return nil, stop }
				_, err = f.Dial(t.Context(), "tcp", address)
				if saved && address != "100.64.0.2:80" {
					var refused *fabric.DestinationRefusedError
					if !errors.As(err, &refused) || planned || dialled {
						t.Fatalf("logical target reached transport: planned=%v dialled=%v err=%v", planned, dialled, err)
					}
				} else if !planned || !dialled || !errors.Is(err, stop) {
					t.Fatalf("allowed target blocked: planned=%v dialled=%v err=%v", planned, dialled, err)
				}
			})
		}
	}
}

func TestSavedParticipantChecksRecordedNetworkName(t *testing.T) {
	for _, role := range []string{"node", "client", "person"} {
		person := role == "person"
		for _, tc := range []struct {
			recorded, granted string
			refused           bool
		}{
			{"wefty-studio-client-2", "wefty-studio-client-2.identity-domain.", false},
			{"wefty-studio-client-2", "wefty-studio-client-2.identity-domain", false},
			{"wefty-studio-client-2", "wefty-renamed.identity-domain.", true},
			{"wefty-studio-client-2", "wefty-studio-client-3.identity-domain.", true},
			{"wefty-studio-client-2", "", true},
			{"", "legacy-name.identity-domain.", false},
			{"", "", false},
		} {
			t.Run(fmt.Sprintf("%s/%s/%s", role, tc.recorded, tc.granted), func(t *testing.T) {
				expected := fabric.Identity{NodeID: "saved", Tags: []string{"tag:wefty-client"}}
				logical, roleFields := "wefty://client/studio", `"Tags":["tag:wefty-client"]`
				if role == "node" {
					expected.Tags = []string{"tag:wefty-agent"}
					logical, roleFields = "wefty://node/studio", `"Tags":["tag:wefty-agent"]`
				}
				if person {
					expected.Tags = nil
					expected.UserID = "42"
					logical = "wefty://person/studio"
					roleFields = `"UserID":42`
				}
				f, err := New(Config{Name: logical, NetworkName: tc.recorded, StateDir: savedBackendState(t), ExpectedIdentity: &expected})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = f.Close() })
				f.client = statusClient(t, fmt.Sprintf(`{"BackendState":"Running","Self":{"ID":"saved","DNSName":%q,%s}}`, tc.granted, roleFields))
				err = f.VerifyParticipant(t.Context())
				if tc.refused && role == "node" {
					var refusal *fabric.ParticipantIdentityError
					if !errors.As(err, &refusal) || refusal.Person != person || !strings.Contains(err.Error(), "restore") || !strings.Contains(err.Error(), "re-record") {
						t.Fatalf("stale name not refused with recovery: %T %v", err, err)
					}
					f.dialPlan = func(context.Context, string, string) (netip.AddrPort, bool, error) {
						t.Fatal("planned with stale name")
						return netip.AddrPort{}, false, nil
					}
					f.listenNode = func(string, string) (net.Listener, error) { t.Fatal("listened with stale name"); return nil, nil }
					if _, err := f.Dial(t.Context(), "tcp", "100.64.0.2:80"); !errors.As(err, &refusal) {
						t.Fatalf("Dial error=%v", err)
					}
					if _, err := f.Listen("tcp", logical); !errors.As(err, &refusal) {
						t.Fatalf("Listen error=%v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
