package computerconformance

import (
	"testing"
	"time"
)

func TestRecorderStartsEveryCheckNotRunAndUsesInjectedClock(t *testing.T) {
	start := time.Unix(100, 0)
	recorder := NewRecorder("example.invalid/image@sha256:abc", "docker", "linux/amd64", start)
	receipt := recorder.Finish(start.Add(5 * time.Second))
	if receipt.Status != StatusNotRun || len(receipt.Checks) != len(CheckCatalog) {
		t.Fatalf("receipt = %+v", receipt)
	}
	if !receipt.StartedAt.Equal(start) || !receipt.FinishedAt.Equal(start.Add(5*time.Second)) {
		t.Fatalf("injected timestamps changed: %+v", receipt)
	}
	for _, check := range receipt.Checks {
		if check.Status != StatusNotRun {
			t.Fatalf("initial check %s = %s", check.ID, check.Status)
		}
	}
}

func TestRecorderAggregatesImageAndHarnessEvidenceSeparately(t *testing.T) {
	recorder := NewRecorder("image", "docker", "linux/amd64", time.Unix(100, 0))
	for _, definition := range CheckCatalog {
		status, detail := StatusPass, "observed"
		if definition.Scope == ScopeProfile {
			status, detail = StatusNotRun, ContainerdProfileNotRun
		}
		if err := recorder.Record(definition.ID, status, detail); err != nil {
			t.Fatal(err)
		}
	}
	receipt := recorder.Finish(time.Unix(101, 0))
	if receipt.Status != StatusPass || receipt.ImageStatus != StatusPass || receipt.HarnessStatus != StatusPass || receipt.ContainerdProfileStatus != StatusNotRun {
		t.Fatalf("conformant receipt = %s", receipt.Status)
	}
}

func TestUnknownCheckAndStatusFailClosed(t *testing.T) {
	recorder := NewRecorder("image", "docker", "", time.Time{})
	if err := recorder.Record("unknown", StatusPass, ""); err == nil {
		t.Fatal("unknown check was accepted")
	}
	if err := recorder.Record(CheckCatalog[0].ID, Status("UNKNOWN"), ""); err == nil {
		t.Fatal("unknown status was accepted")
	}
	if Aggregate([]Check{{Status: Status("UNKNOWN")}}) != StatusFail {
		t.Fatal("unknown aggregate status did not fail closed")
	}
}

func TestReadinessDeadlineUsesInjectedClockAndRejectsExactBoundary(t *testing.T) {
	startedAt := time.Unix(1_000, 0)
	if !BeforeReadinessDeadline(startedAt.Add(60*time.Second-time.Nanosecond), startedAt) {
		t.Fatal("instant before the deadline was rejected")
	}
	if BeforeReadinessDeadline(startedAt.Add(60*time.Second), startedAt) {
		t.Fatal("exact 60 second boundary was accepted")
	}
}
