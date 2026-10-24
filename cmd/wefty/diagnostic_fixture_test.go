package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
)

// Check the resolved profile, not just fixture defaults: an omitted endpoint
// must never let a diagnostic test contact an owner's installed service.
func diagnosticFixtureEndpointError(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	ip := net.ParseIP(host)
	_, l1Port, _ := net.SplitHostPort(defaultSetupL1Address)
	_, l3Port, _ := net.SplitHostPort(defaultSetupL3Address)
	number, parseErr := strconv.Atoi(port)
	l1Number, _ := strconv.Atoi(l1Port)
	l3Number, _ := strconv.Atoi(l3Port)
	if ip == nil || !ip.IsLoopback() || parseErr != nil || number <= 0 || number > 65535 || number == l1Number || number == l3Number {
		return fmt.Errorf("diagnostic fixture requires an explicit ephemeral loopback fake, got %q", address)
	}
	return nil
}
func runFixturePlainDoctor(t *testing.T, host setupHost, options globalOptions) plainDoctorReport {
	t.Helper()
	checked := options
	if err := applyConnectionProfile(&checked); err != nil {
		t.Fatal(err)
	}
	for _, address := range []string{checked.l1Address, checked.l3Address} {
		if err := diagnosticFixtureEndpointError(address); err != nil {
			t.Fatal(err)
		}
	}
	return runPlainDoctor(t.Context(), host, options)
}
func TestDiagnosticFixtureRejectsDefaultEndpoints(t *testing.T) {
	for _, address := range []string{"", defaultSetupL1Address, defaultSetupL3Address, "127.0.0.1:0", "localhost:47101", "127.0.0.1:047101", "[::1]:047102"} {
		if diagnosticFixtureEndpointError(address) == nil {
			t.Fatalf("unsafe endpoint accepted: %q", address)
		}
	}
	if err := diagnosticFixtureEndpointError("127.0.0.1:58123"); err != nil {
		t.Fatal(err)
	}
}

// Exercise both diagnostic output contracts with the same collected evidence.
func reviewDiagnosticOutputs(t *testing.T, r rollingStatusFields, contains string, excludes string) {
	t.Helper()
	for _, asJSON := range []bool{false, true} {
		for _, doctor := range []bool{false, true} {
			var out bytes.Buffer
			if doctor {
				if err := writeDoctorReport(&out, plainDoctorReport{rollingStatusFields: r, Healthy: len(r.findings()) == 0}, asJSON); err != nil {
					t.Fatal(err)
				}
			} else {
				_ = writeStatusResult(&out, clusterStatus{rollingStatusFields: r, Ready: len(r.findings()) == 0}, asJSON)
			}
			if !strings.Contains(out.String(), contains) || (excludes != "" && strings.Contains(out.String(), excludes)) {
				t.Fatalf("doctor=%t json=%t: %s", doctor, asJSON, out.String())
			}
		}
	}
}

const diagnosticGuardArguments = "WEFTY_TEST_DIAGNOSTIC_GUARD_ARGUMENTS"

// Both option parsing and profile layering are the production functions. This
// is called only from TestMain's preflight branch with the child's real env.
func resolveDiagnosticFixtureEndpoints(payload string) error {
	var args []string
	if err := json.Unmarshal([]byte(payload), &args); err != nil {
		return err
	}
	options, commands, err := parseGlobalOptions(args, io.Discard)
	if err != nil {
		return nil
	} // The actual command rejects these args before dialing.
	diagnostic := len(commands) > 0 && (commands[0] == "status" || commands[0] == "doctor" || (commands[0] == "node" && len(commands) > 1 && commands[1] == "doctor"))
	if !diagnostic {
		return nil
	}
	// Even an unreadable selected profile cannot exempt an explicit default port.
	for _, address := range []string{options.l1Address, options.l3Address} {
		if err := effectiveDiagnosticEndpointError(address); err != nil {
			return err
		}
	}
	if err := applyConnectionProfile(&options); err != nil {
		return nil
	} // Refuses before any network read.
	if options.fabricMode != "plain" {
		return fmt.Errorf("diagnostic fixture requires plain Fabric, got %q", options.fabricMode)
	}
	for _, address := range []string{options.l1Address, options.l3Address} {
		if err := effectiveDiagnosticEndpointError(address); err != nil {
			return err
		}
	}
	return nil
}
func effectiveDiagnosticEndpointError(address string) error {
	// Explicitly absent endpoints cannot dial. Built-in logical plain addresses
	// are also non-dialable without a test-owned listener in that private network.
	if address == "" || address == "wefty://control-plane" || address == "wefty://run-ledger" {
		return nil
	}
	return diagnosticFixtureEndpointError(address)
}
