package computerconformance

import (
	"strings"
	"testing"
	"time"
)

func TestReceiptV2CatalogIDsArePinned(t *testing.T) {
	if ReceiptVersion != 2 {
		t.Fatalf("receipt version = %d, want 2", ReceiptVersion)
	}
	expected := strings.Fields(`
		runtime.started runtime.image-config
		environment.service-dir environment.view-port environment.control-port environment.service-port-omitted environment.handoff-dir-omitted environment.authority-omitted environment.other-wefty-preserved
		targets.service-nonshadowable targets.control-nonshadowable targets.handoff-nonshadowable targets.token-mode targets.endpoint-mode
		endpoints.distinct endpoints.view-loopback endpoints.control-loopback
		transport.view-ready transport.control-ready transport.plain-tcp-rejected transport.query-ignored transport.fragment-ignored transport.wrong-path-rejected transport.missing-subprotocol-rejected transport.wrong-subprotocol-rejected transport.text-frame-rejected
		readiness.before-deadline
		input.view-isolated input.view-isolated-during-tenure input.control-accepted
		driver.read-only driver.mode driver.initial-false driver.true-consumed driver.release-consumed driver.malformed-fails-closed driver.unknown-version-fails-closed driver.missing-fails-closed
		harness.image-user harness.rootfs-read-only harness.service-writable harness.no-new-privileges harness.forbidden-privilege harness.shm-private harness.shm-size harness.shm-flags harness.tmp-ceilings
		profile.capabilities profile.seccomp profile.namespaces profile.devices profile.cgroup-memory-max profile.cgroup-oom-group profile.cgroup-swap-max
		persistence.service-survives persistence.profile-survives persistence.sign-in-survives persistence.rootfs-discarded persistence.edge-recovers targets.control-nonpersistent
	`)
	if len(CheckCatalog) != len(expected) {
		t.Fatalf("receipt v2 catalog length = %d, want %d", len(CheckCatalog), len(expected))
	}
	for index, definition := range CheckCatalog {
		if definition.ID != expected[index] {
			t.Fatalf("receipt v2 catalog id[%d] = %q, want %q", index, definition.ID, expected[index])
		}
	}
}

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
	if receipt.Teardown.Observations == nil || receipt.Teardown.Leftovers == nil || len(receipt.Teardown.Leftovers) != 0 {
		t.Fatalf("initial teardown evidence = %+v", receipt.Teardown)
	}
	for _, check := range receipt.Checks {
		if check.Status != StatusNotRun {
			t.Fatalf("initial check %s = %s", check.ID, check.Status)
		}
	}
}

func TestRecorderPreservesTypedTeardownEvidence(t *testing.T) {
	recorder := NewRecorder("image", "docker", "linux/arm64", time.Unix(100, 0))
	recorder.RecordTeardownObservation("container_stop_failed", "daemon response")
	recorder.RecordTeardownRetry("temporary_root_not_empty", "retry=1/8")
	recorder.RecordPermissionRepair(1250 * time.Millisecond)
	recorder.RecordTeardownLeftovers([]string{"temporary-root:/tmp/checker"})
	receipt := recorder.Finish(time.Unix(101, 0))
	if receipt.Teardown.RetriesUsed != 1 || !receipt.Teardown.PermissionRepairPerformed || receipt.Teardown.PermissionRepairSeconds != 1.25 {
		t.Fatalf("teardown counters = %+v", receipt.Teardown)
	}
	if len(receipt.Teardown.Observations) != 2 || len(receipt.Teardown.Leftovers) != 1 {
		t.Fatalf("teardown evidence = %+v", receipt.Teardown)
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
	if err := recorder.RecordFailure(CheckCatalog[0].ID, StatusFail, "late", FailureReadinessTimeout, ReadinessEvent("eventually")); err == nil {
		t.Fatal("unknown readiness event was accepted")
	}
	if Aggregate([]Check{{Status: Status("UNKNOWN")}}) != StatusFail {
		t.Fatal("unknown aggregate status did not fail closed")
	}
}

func TestRecorderRejectsReadinessEventWithoutReadinessTimeout(t *testing.T) {
	for _, reason := range []FailureReason{FailureAssertionFailed, FailureMutationDetected} {
		t.Run(string(reason), func(t *testing.T) {
			recorder := NewRecorder("image", "docker", "linux/arm64", time.Unix(100, 0))
			err := recorder.RecordFailure("transport.view-ready", StatusFail, "failed", reason, ReadinessEventViewEndpointReady)
			if err == nil {
				t.Fatalf("%s accepted a readiness event", reason)
			}
		})
	}
}

func TestReadinessTimeoutCheckEventPairingsAreClosed(t *testing.T) {
	valid := map[string][]ReadinessEvent{
		"transport.view-ready":              {ReadinessEventViewEndpointReady, ReadinessEventFirstRFBFrame},
		"transport.control-ready":           {ReadinessEventControlEndpointReady, ReadinessEventFirstRFBFrame},
		"input.view-isolated":               {ReadinessEventInputOracleReady, ReadinessEventKeyObserverAdvanced},
		"input.view-isolated-during-tenure": {ReadinessEventKeyObserverAdvanced},
	}
	allEvents := []ReadinessEvent{
		ReadinessEventInputOracleReady,
		ReadinessEventKeyObserverAdvanced,
		ReadinessEventViewEndpointReady,
		ReadinessEventControlEndpointReady,
		ReadinessEventFirstRFBFrame,
	}
	for _, definition := range CheckCatalog {
		allowed := valid[definition.ID]
		for _, event := range allEvents {
			want := false
			for _, candidate := range allowed {
				want = want || candidate == event
			}
			recorder := NewRecorder("image", "docker", "linux/arm64", time.Unix(100, 0))
			err := recorder.RecordReadinessTimeout(definition.ID, "late", event, 1500*time.Millisecond, 1625*time.Millisecond)
			if (err == nil) != want {
				t.Fatalf("readiness pairing %s -> %s accepted=%t, want %t: %v", definition.ID, event, err == nil, want, err)
			}
		}
	}
}

func TestReadinessTimeoutRequiresMeasuredWindowAndLaterObservationIsMonotonic(t *testing.T) {
	recorder := NewRecorder("image", "docker", "linux/arm64", time.Unix(100, 0))
	if err := recorder.RecordReadinessTimeout("input.view-isolated", "late", ReadinessEventKeyObserverAdvanced, 2*time.Second, time.Second); err == nil {
		t.Fatal("elapsed observation shorter than its applied window was accepted")
	}
	if err := recorder.RecordReadinessTimeout("input.view-isolated", "late", ReadinessEventKeyObserverAdvanced, 1500*time.Millisecond, 1625*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	recorder.ObserveReadinessEvent(ReadinessEventKeyObserverAdvanced)
	check := recorder.Finish(time.Unix(101, 0)).Checks[0]
	for _, candidate := range recorder.Finish(time.Unix(101, 0)).Checks {
		if candidate.ID == "input.view-isolated" {
			check = candidate
		}
	}
	if !check.ReadinessObservedLater || check.ReadinessObservationWindowSeconds != 1.5 || check.ReadinessObservationElapsedSeconds != 1.625 {
		t.Fatalf("readiness observation evidence = %+v", check)
	}
}

func TestRecorderRelabelsOnlyUnobservedReadinessTimeouts(t *testing.T) {
	recorder := NewRecorder("image", "docker", "linux/arm64", time.Unix(100, 0))
	if err := recorder.RecordReadinessTimeout("input.view-isolated", "late", ReadinessEventKeyObserverAdvanced, 1500*time.Millisecond, 1625*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if !recorder.HasReadinessTimeout("input.view-isolated") {
		t.Fatal("recorded readiness timeout was not queryable")
	}
	relabelled, err := recorder.RelabelUnobservedReadinessTimeout("input.view-isolated")
	if err != nil {
		t.Fatal(err)
	}
	if !relabelled || recorder.HasReadinessTimeout("input.view-isolated") {
		t.Fatal("unobserved readiness timeout was not relabelled")
	}
	for _, check := range recorder.Finish(time.Unix(101, 0)).Checks {
		if check.ID == "input.view-isolated" && (check.FailureReason != FailureAssertionFailed || check.ReadinessEvent != "" || check.ReadinessObservationWindowSeconds != 1.5 || check.ReadinessObservationElapsedSeconds != 1.625) {
			t.Fatalf("relabelled readiness evidence = %+v", check)
		}
	}

	observed := NewRecorder("image", "docker", "linux/arm64", time.Unix(100, 0))
	if err := observed.RecordReadinessTimeout("input.view-isolated", "late", ReadinessEventKeyObserverAdvanced, 1500*time.Millisecond, 1625*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	observed.ObserveReadinessEvent(ReadinessEventKeyObserverAdvanced)
	if relabelled, err := observed.RelabelUnobservedReadinessTimeout("input.view-isolated"); err != nil || relabelled {
		t.Fatalf("observed readiness timeout relabelled=%t err=%v", relabelled, err)
	}
}

func TestObservedCompatibilityFailsClosedWhenCatalogIDIsMissing(t *testing.T) {
	recorder := NewRecorder("image", "docker", "linux/amd64", time.Unix(100, 0))
	for _, definition := range CheckCatalog {
		if err := recorder.Record(definition.ID, StatusPass, "observed"); err != nil {
			t.Fatal(err)
		}
	}
	delete(recorder.index, "runtime.started")
	receipt := recorder.Finish(time.Unix(101, 0))
	if receipt.Executed {
		t.Fatal("missing runtime.started catalog id projected check zero as executed")
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
