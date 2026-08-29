package computerconformance

import (
	"encoding/json"
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

func TestReceiptFailsEveryNegativeMutationTwentyOfTwenty(t *testing.T) {
	recorder := NewRecorder("image", "docker", "linux/amd64", time.Unix(100, 0))
	for _, definition := range CheckCatalog {
		if err := recorder.Record(definition.ID, StatusPass, "observed"); err != nil {
			t.Fatal(err)
		}
	}
	receipt := recorder.Finish(time.Unix(101, 0))
	if receipt.Status != StatusPass {
		t.Fatalf("conformant receipt = %s", receipt.Status)
	}

	// These rows are the assertion-derived publication boundary. Each mutation
	// flips one independently load-bearing claim and must reject a receipt that
	// otherwise passes.
	mutations := []string{
		"runtime.started",
		"environment.service-dir",
		"environment.view-port",
		"environment.control-port",
		"environment.service-port-omitted",
		"endpoints.distinct",
		"endpoints.view-loopback",
		"endpoints.control-loopback",
		"transport.view-ready",
		"transport.control-ready",
		"transport.text-frame-rejected",
		"readiness.before-deadline",
		"input.view-isolated",
		"input.control-accepted",
		"driver.read-only",
		"driver.true-consumed",
		"profile.rootfs-read-only",
		"profile.no-new-privileges",
		"profile.shm-size",
		"persistence.service-survives",
	}
	if len(mutations) != 20 {
		t.Fatalf("negative mutation rows = %d, want 20", len(mutations))
	}
	failed := 0
	for _, id := range mutations {
		t.Run(id, func(t *testing.T) {
			payload, err := json.Marshal(receipt)
			if err != nil {
				t.Fatal(err)
			}
			var candidate Receipt
			if err := json.Unmarshal(payload, &candidate); err != nil {
				t.Fatal(err)
			}
			for index := range candidate.Checks {
				if candidate.Checks[index].ID == id {
					candidate.Checks[index].Status = StatusFail
				}
			}
			candidate.Status = Aggregate(candidate.Checks)
			if candidate.Status == StatusPass {
				t.Fatalf("mutation %s retained PASS", id)
			}
			failed++
		})
	}
	if failed != 20 {
		t.Fatalf("negative mutation coverage = %d/20", failed)
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
