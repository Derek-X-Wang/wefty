package l1

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

type identityAnswerFabric struct {
	fabric.Fabric
	identity fabric.Identity
	err      error
	calls    int
}

func (f *identityAnswerFabric) WhoIs(context.Context, string) (fabric.Identity, error) {
	f.calls++
	return f.identity, f.err
}

func (*identityAnswerFabric) PersonIdentityTrust() fabric.PersonIdentityTrust {
	return fabric.PersonIdentityAuthenticated
}

func newIdentityAnswerServer(t *testing.T, f *identityAnswerFabric) *Server {
	t.Helper()
	store, err := OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	server, err := NewServer(f, store, ServerConfig{RunLedgerNodeID: "configured-ledger"})
	if err != nil {
		t.Fatal(err)
	}
	server.logf = func(string, ...any) {}
	return server
}

func TestIdentityLookupAvailabilityAcrossAuthenticatedProtocols(t *testing.T) {
	// Exercise the served route tree, including cancel's distinct selector and
	// every attempt-credential route, rather than calling middleware directly.
	paths := []struct {
		name, method, path string
		bearer             bool
	}{
		{"client jobs", "GET", "/v1/jobs", false},
		{"client nodes", "GET", "/v1/nodes", false},
		{"agent", "POST", "/v1/agent/nodes/register", false},
		{"person", "GET", "/v1/whoami", false},
		{"person policy", "GET", "/v1/admin-policy", false},
		{"person Computer", "GET", "/v1/computers/missing/submission", false},
		{"cancel", "POST", "/v1/jobs/missing/cancel", false},
		{"credential submit", "POST", "/v1/jobs", true},
		{"credential list", "GET", "/v1/jobs", true},
		{"credential list slash", "GET", "/v1/jobs/", true},
		{"credential read", "GET", "/v1/jobs/missing", true},
		{"credential children", "GET", "/v1/jobs/missing/children", true},
		{"credential cancel", "POST", "/v1/jobs/missing/cancel", true},
		{"credential out of scope", "PUT", "/v1/jobs/missing/desired-state", true},
		{"ledger dispatch", "GET", "/v1/dispatch-keys/missing/job", false},
		{"ledger Computer", "POST", "/v1/computers/missing/token-scope-proof", false},
		{"ledger host", "POST", "/v1/host-boot-session-proof", false},
	}
	for _, answer := range []struct {
		name   string
		err    error
		status int
		code   contract.ErrorCode
		cause  string
	}{
		{"operational", errors.New("lookup backend failed: private-credential"), 503, contract.ErrorUnavailable, "lookup_failed"},
		{"timeout", fmt.Errorf("private-endpoint: %w", context.DeadlineExceeded), 503, contract.ErrorUnavailable, "timeout"},
		{"canceled", context.Canceled, 503, contract.ErrorUnavailable, "canceled"},
		{"absent", fabric.ErrIdentityNotFound, 401, contract.ErrorUnauthorized, ""},
		{"wrapped absent", fmt.Errorf("lookup: %w", fabric.ErrIdentityNotFound), 401, contract.ErrorUnauthorized, ""},
	} {
		t.Run(answer.name, func(t *testing.T) {
			f := &identityAnswerFabric{err: answer.err}
			s := newIdentityAnswerServer(t, f)
			for _, path := range paths {
				t.Run(path.name, func(t *testing.T) {
					var logs bytes.Buffer
					s.logf = func(format string, args ...any) { fmt.Fprintf(&logs, format+"\n", args...) }
					r := httptest.NewRequest(path.method, path.path+"?secret=private-query", strings.NewReader("private-body"))
					if path.bearer {
						r.Header.Set("Authorization", "Bearer private-bearer")
					}
					w := httptest.NewRecorder()
					calls := f.calls
					s.Handler().ServeHTTP(w, r)
					if f.calls != calls+1 {
						t.Fatalf("WhoIs calls = %d, want %d", f.calls, calls+1)
					}
					var response contract.ErrorResponse
					if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
						t.Fatal(err)
					}
					got := response.Error
					if w.Code != answer.status || got.Code != answer.code || got.Retryable != (answer.status == 503) {
						t.Fatalf("status=%d error=%+v, want %d %s retryable=%t", w.Code, got, answer.status, answer.code, answer.status == 503)
					}
					if answer.status == 503 && got.Details["reason"] != "identity_unverifiable" {
						t.Fatalf("details = %#v, want identity_unverifiable", got.Details)
					}
					if answer.status == 401 && len(got.Details) != 0 {
						t.Fatalf("absence details = %#v", got.Details)
					}
					if got.RequestID == "" || got.RequestID != w.Header().Get(contract.RequestIDHeader) {
						t.Fatalf("request ID = %q, header=%q", got.RequestID, w.Header().Get(contract.RequestIDHeader))
					}
					if answer.status == 503 && (!strings.Contains(logs.String(), "event=l1_identity_unverifiable") ||
						!strings.Contains(logs.String(), "class=") || !strings.Contains(logs.String(), "cause="+answer.cause) ||
						!strings.Contains(logs.String(), "request_id="+got.RequestID)) {
						t.Fatalf("missing cause diagnostic: %s", logs.String())
					}
					if answer.status == 401 && strings.Contains(logs.String(), "event=l1_identity_unverifiable") {
						t.Fatalf("absence logged as operational failure: %s", logs.String())
					}
					for _, secret := range []string{"private-credential", "private-endpoint", "private-query", "private-body", "private-bearer"} {
						if strings.Contains(logs.String(), secret) || strings.Contains(w.Body.String(), secret) {
							t.Fatalf("secret %q disclosed in logs or response", secret)
						}
					}
				})
			}
		})
	}
}

func TestRunLedgerAdmissionReasonIsSeparateFromResourceAuthority(t *testing.T) {
	f := &identityAnswerFabric{identity: fabric.Identity{NodeID: "run-ledger", Tags: []string{DefaultClientPrincipalTag}}}
	s := newIdentityAnswerServer(t, f)
	for _, test := range []struct {
		name, method, path, body string
		ledger, reason           bool
	}{
		{"wrong ledger dispatch", "GET", "/v1/dispatch-keys/missing/job", "", false, true},
		{"wrong ledger Computer", "POST", "/v1/computers/missing/token-scope-proof", "{}", false, true},
		{"wrong ledger host", "POST", "/v1/host-boot-session-proof", "{}", false, true},
		{"Computer host binding", "POST", "/v1/computers/missing/token-scope-proof", `{"computer_attempt_id":"attempt"}`, true, false},
		{"Computer current authority", "POST", "/v1/computers/missing/token-scope-proof", `{"computer_attempt_id":"attempt","host_node_id":"host"}`, true, false},
		{"host current authority", "POST", "/v1/host-boot-session-proof", `{"host_identity_node_id":"fabric-host","host_stable_node_id":"host","boot_session_id":"old-boot"}`, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			f.identity.NodeID = "run-ledger"
			if test.ledger {
				f.identity.NodeID = "configured-ledger"
			}
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, httptest.NewRequest(test.method, test.path, strings.NewReader(test.body)))
			var response contract.ErrorResponse
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if w.Code != http.StatusForbidden || response.Error.Code != contract.ErrorForbidden || response.Error.Retryable {
				t.Fatalf("status=%d error=%+v, want non-retryable 403 forbidden", w.Code, response.Error)
			}
			if test.reason {
				if response.Error.Details["reason"] != "run_ledger_not_admitted" {
					t.Fatalf("details = %#v, want run_ledger_not_admitted", response.Error.Details)
				}
			} else if len(response.Error.Details) != 0 {
				t.Fatalf("resource authority acquired details: %#v", response.Error.Details)
			}
		})
	}
}

func TestUnavailableErrorWriter(t *testing.T) {
	w := httptest.NewRecorder()
	writeError(w, protocolError(contract.ErrorUnavailable, "service temporarily unavailable"))
	var response contract.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusServiceUnavailable || response.Error.Code != contract.ErrorUnavailable || !response.Error.Retryable {
		t.Fatalf("status=%d error=%+v, want 503 unavailable retryable", w.Code, response.Error)
	}
}
