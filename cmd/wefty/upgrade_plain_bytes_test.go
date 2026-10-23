package main

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

// Hold the human report bytes, including the separately reported drain counts.
// Optional capture is a test artifact, not a CLI or product flag.
func TestUpgradePlainByteComparison(t *testing.T) {
	type output struct{ Stdout, Stderr string }
	captured := map[string]output{}
	for _, jsonMode := range []bool{false, true} {
		f := newUpgradeFixture(t)
		var stdout, stderr bytes.Buffer
		if err := executeUpgradeWithHost(t.Context(), f.host, globalOptions{jsonOutput: jsonMode}, false, &stdout, &stderr); err != nil {
			t.Fatal(err)
		}
		name := "human"
		if jsonMode {
			name = "json"
		}
		normalize := func(s string) string { return strings.ReplaceAll(s, f.home, "<HOME>") }
		captured[name] = output{normalize(stdout.String()), normalize(stderr.String())}
		if !jsonMode && stdout.String() != "Installed version: "+f.commit+"\nNode agent replaced; services come back.\nwaiting_nonrestartable=0 stopping_restartable=0 restart_pending=0\n" {
			t.Fatalf("plain stdout changed: %q", stdout.String())
		}
		stdout.Reset()
		stderr.Reset()
		if err := executeUpgradeWithHost(t.Context(), f.host, globalOptions{jsonOutput: jsonMode}, false, &stdout, &stderr); err != nil {
			t.Fatal(err)
		}
		captured[name+"-already"] = output{normalize(stdout.String()), normalize(stderr.String())}
	}
	// Captured by running this test in an isolated git archive of e3facc51.
	// Only the temporary home prefix is normalized; report bytes stay exact.
	fixture, err := os.ReadFile("testdata/upgrade_plain_e3facc51.golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var golden map[string]output
	if err := json.Unmarshal(fixture, &golden); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(captured, golden) {
		t.Error("default upgrade output differs from e3facc51")
		for name, expected := range golden {
			if got := captured[name]; got != expected {
				t.Errorf("%s output changed\nstdout got=%q\nstdout want=%q\nstderr got=%q\nstderr want=%q", name, got.Stdout, expected.Stdout, got.Stderr, expected.Stderr)
			}
		}
	}
	if path := os.Getenv("WEFTY_UPGRADE_BYTE_CAPTURE"); path != "" {
		payload, err := json.MarshalIndent(captured, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, payload, 0600); err != nil {
			t.Fatal(err)
		}
	}
}
