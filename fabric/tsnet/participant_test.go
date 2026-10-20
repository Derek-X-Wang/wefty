package tsnet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
	"tailscale.com/client/local"
	"tailscale.com/ipn"
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
			_, err := f.Dial(t.Context(), "tcp", "wefty://control-plane")
			if tc.wantError {
				var refused *fabric.ParticipantIdentityError
				if dialled || !errors.As(err, &refused) {
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
			f := &Fabric{client: &local.Client{OmitAuth: true, Transport: identityRoundTripper(func(r *http.Request) (*http.Response, error) {
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
