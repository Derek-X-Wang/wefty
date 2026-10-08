package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestInspectDispatchHoldHuman(t *testing.T) {
	since := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	hold := &contract.DispatchHold{Reason: "run_identity_not_entitled", Since: since, NextProbeAt: since.Add(time.Minute)}
	run := contract.RunRecord{RunID: "run-held", Status: contract.RunDispatching, DispatchHold: hold}
	var out bytes.Buffer
	if err := writeRunInspection(&out, runInspection{Run: run, Runs: []contract.RunRecord{run}}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"dispatch hold", "run-held", hold.Reason, since.Format(time.RFC3339), hold.NextProbeAt.Format(time.RFC3339)} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("human inspect missing %q: %s", want, out.String())
		}
	}
}
