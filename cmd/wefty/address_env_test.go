package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/l3"
)

// TestAddressesDefaultFromTheEnvironment is the day-one papercut (#604): the
// README exports WEFTY_L1_ADDR and WEFTY_L3_ADDR, and a bare `wefty status`
// then looked at wefty://control-plane anyway.
func TestAddressesDefaultFromTheEnvironment(t *testing.T) {
	tests := map[string]struct {
		env              map[string]string
		args             []string
		wantL1, wantL3   string
		wantL3Configured bool
	}{
		"nothing set keeps the logical defaults": {
			args:   []string{"status"},
			wantL1: l3.DefaultL1Address, wantL3: l3.DefaultL3Address,
		},
		"the environment supplies both": {
			env:    map[string]string{l1AddressEnv: "127.0.0.1:42101", l3AddressEnv: "127.0.0.1:42102"},
			args:   []string{"status"},
			wantL1: "127.0.0.1:42101", wantL3: "127.0.0.1:42102",
		},
		"a flag wins over the environment": {
			env:    map[string]string{l1AddressEnv: "127.0.0.1:42101", l3AddressEnv: "127.0.0.1:42102"},
			args:   []string{"--l1=10.0.0.1:1", "--l3=10.0.0.1:2", "status"},
			wantL1: "10.0.0.1:1", wantL3: "10.0.0.1:2",
		},
		"an explicit --l3= still chooses an L1-only installation": {
			env:    map[string]string{l3AddressEnv: "127.0.0.1:42102"},
			args:   []string{"--l3=", "status"},
			wantL1: l3.DefaultL1Address, wantL3: "",
		},
		"a blank variable is unset": {
			env:    map[string]string{l1AddressEnv: "  ", l3AddressEnv: ""},
			args:   []string{"status"},
			wantL1: l3.DefaultL1Address, wantL3: l3.DefaultL3Address,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Setenv(l1AddressEnv, "")
			t.Setenv(l3AddressEnv, "")
			for key, value := range test.env {
				t.Setenv(key, value)
			}
			options, _, err := parseGlobalOptions(test.args, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			if options.l1Address != test.wantL1 || options.l3Address != test.wantL3 {
				t.Fatalf("addresses = %q, %q; want %q, %q", options.l1Address, options.l3Address, test.wantL1, test.wantL3)
			}
		})
	}
}

// TestUnreachableServiceNamesTheFlagAndVariable keeps the refusal actionable:
// the reader learns which flag or variable moves the address.
func TestUnreachableServiceNamesTheFlagAndVariable(t *testing.T) {
	t.Parallel()

	h := newStatusHarness(t)
	h.l1Down = true
	h.l3Down = true
	var out bytes.Buffer
	_ = executeStatus(t.Context(), h.clients(), false, nil, &out, io.Discard)
	for _, want := range []string{"pass --l1 or set WEFTY_L1_ADDR", "pass --l3 or set WEFTY_L3_ADDR"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("status does not say %q:\n%s", want, out.String())
		}
	}

	failing := &apiClient{name: "L1", flag: "l1", address: "wefty://control-plane", client: &http.Client{
		Transport: addressTestTransport(func(*http.Request) (*http.Response, error) { return nil, errors.New("connection refused") }),
	}}
	err := failing.do(context.Background(), http.MethodGet, "/v1/nodes", nil, nil, nil, http.StatusOK)
	if err == nil || !strings.Contains(err.Error(), "wefty://control-plane") || !strings.Contains(err.Error(), "WEFTY_L1_ADDR") {
		t.Fatalf("transport error = %v, want it to name the address and WEFTY_L1_ADDR", err)
	}
}

// TestUnconfiguredLedgerIsNotPrefixedTwice: main prints "wefty: " once.
func TestUnconfiguredLedgerIsNotPrefixedTwice(t *testing.T) {
	t.Parallel()

	var stderr bytes.Buffer
	err := (&apiClient{name: "L3", flag: "l3"}).do(context.Background(), http.MethodGet, "/v1/runs", nil, nil, nil, http.StatusOK)
	writeCommandError(&stderr, err, false)
	if strings.Contains(stderr.String(), "wefty: wefty:") {
		t.Fatalf("error is double-prefixed: %q", stderr.String())
	}
}

type addressTestTransport func(*http.Request) (*http.Response, error)

func (f addressTestTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
