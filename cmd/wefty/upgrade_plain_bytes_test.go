package main

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// This exact test is also run against the dispatch base. Optional capture is
// only a test artifact for byte comparison; it is not a CLI or product flag.
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
		if !jsonMode && stdout.String() != "Installed version: "+f.commit+"\nNode agent replaced; services come back.\n" {
			t.Fatalf("plain stdout changed: %q", stdout.String())
		}
		stdout.Reset()
		stderr.Reset()
		if err := executeUpgradeWithHost(t.Context(), f.host, globalOptions{jsonOutput: jsonMode}, false, &stdout, &stderr); err != nil {
			t.Fatal(err)
		}
		captured[name+"-already"] = output{normalize(stdout.String()), normalize(stderr.String())}
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
