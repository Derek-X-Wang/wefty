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
		{"client", `{"BackendState":"Running","Self":{"ID":"saved","Tags":["tag:wefty-client"]}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &Fabric{expectedIdentity: &fabric.Identity{NodeID: "saved", Tags: []string{"tag:wefty-client"}}, client: statusClient(t, tc.status)}
			dialled := false
			stop := errors.New("dial boundary")
			f.dialNode = func(context.Context, string, string) (net.Conn, error) { dialled = true; return nil, stop }
			f.dialPlan = func(context.Context, string, string) (netip.AddrPort, bool, error) {
				return netip.MustParseAddrPort("100.64.0.1:80"), true, nil
			}
			_, err := f.Dial(t.Context(), "tcp", "wefty://control-plane")
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
	f, err := New(Config{Name: "wefty://client/one", StateDir: t.TempDir(), ExpectedIdentity: &fabric.Identity{NodeID: "saved", Tags: []string{"tag:wefty-client"}}, Logf: func(format string, args ...any) { lines = append(lines, format) }})
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
			f, err := New(Config{Name: "wefty://client/one", StateDir: t.TempDir(), ExpectedIdentity: &fabric.Identity{NodeID: "saved"}, Logf: func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }})
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
	f, err := New(Config{Name: "wefty://client/one", StateDir: t.TempDir(), ExpectedIdentity: &fabric.Identity{NodeID: "saved"}, Logf: func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }})
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
