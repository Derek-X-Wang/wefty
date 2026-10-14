package main

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
)

func TestUpgradeJSONOutputKeepsWorkAndRenewalOutOfStderr(t *testing.T) {
	for _, kind := range []string{"running-work-refusal", "success", "replacement-failure"} {
		t.Run(kind, func(t *testing.T) {
			f := newUpgradeFixture(t)
			f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "json-job", AttemptID: "json-attempt", LeaseExpiresAt: f.now.Add(30 * time.Second)}}
			if kind == "replacement-failure" {
				f.failure = "replacement"
			}
			var out, stderr bytes.Buffer
			err := executeUpgradeWithHost(t.Context(), f.host, globalOptions{jsonOutput: true}, kind != "running-work-refusal", &out, &stderr)
			if kind == "success" && err != nil {
				t.Fatal(err)
			}
			if kind != "success" && err == nil {
				t.Fatal("missing expected failure")
			}
			if stderr.Len() != 0 {
				t.Fatalf("plain text leaked to stderr: %s", stderr.String())
			}
			var report map[string]json.RawMessage
			if e := json.Unmarshal(out.Bytes(), &report); e != nil {
				t.Fatalf("invalid JSON: %v %s", e, out.String())
			}
			if !bytes.Contains(report["running_work"], []byte("json-job")) {
				t.Fatalf("missing work: %s", out.String())
			}
			if kind != "running-work-refusal" && string(report["lease_renewal_ns"]) != "200000000" {
				t.Fatalf("missing measured renewal: %s", out.String())
			}
		})
	}
}
