//go:build service_acceptance_realtiming && linux

package serviceacceptance

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l3"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

const (
	// A page-aligned request, so the kernel's round-down of memory.max to
	// PAGE_SIZE cannot make an exact readback disagree with what was asked.
	ociCgroupLimitsMemoryBytes = int64(64 << 20)
	// A quarter of one CPU: low enough that a single busy loop is certain to
	// exhaust the quota in every period and be throttled.
	ociCgroupLimitsCPUMillicores = int64(250)
	ociCgroupLimitsMarker        = "wefty-cgroup "
	ociCgroupLimitsReceipt       = "oci-cgroup-limits-linux.txt"
)

// ociCgroupLimitsProbe runs inside the capped container. The runtime profile
// gives every ordinary OCI job its own cgroup namespace and a read-only
// /sys/fs/cgroup mount, so the files it reads here are the container's own
// cgroup, the one the kernel enforces against. Every line is newline-terminated
// (the one-shot redactor holds an unterminated tail) and every failure names
// itself on stderr, because a failed attempt's only voice is its output.
const ociCgroupLimitsProbe = `
cg=/sys/fs/cgroup
refuse() { printf 'wefty-cgroup-limits: FAILED %s\n' "$1" >&2; exit "$2"; }
test -f "$cg/cgroup.controllers" || refuse "/sys/fs/cgroup is not a cgroup v2 mount" 64
memory_max=$(cat "$cg/memory.max") || refuse "memory.max is unreadable" 65
cpu_max=$(cat "$cg/cpu.max") || refuse "cpu.max is unreadable" 66
throttled_before=$(awk '$1 == "nr_throttled" { print $2 }' "$cg/cpu.stat") || refuse "cpu.stat is unreadable" 67
( while :; do :; done ) &
spinner=$!
sleep 2
kill "$spinner"
wait "$spinner"
throttled_after=$(awk '$1 == "nr_throttled" { print $2 }' "$cg/cpu.stat") || refuse "cpu.stat is unreadable after the busy loop" 68
printf 'wefty-cgroup memory.max=%s\n' "$memory_max"
printf 'wefty-cgroup cpu.max=%s\n' "$cpu_max"
printf 'wefty-cgroup nr_throttled_before=%s\n' "$throttled_before"
printf 'wefty-cgroup nr_throttled_after=%s\n' "$throttled_after"
exit 0
`

// TestOCICgroupV2LimitsReadBackFromTheRunningContainer is the live proof of the
// linux.only.cgroup_v2_limits cell (#402). The OOM kill elsewhere in the lane
// shows a memory limit bites; it never shows the limit that reached the kernel
// is the one the job asked for, and no CPU-capped container ran at all. Here a
// kind=oci image run carrying both limits goes the whole product path -- an L3
// run, the L1 claim gated on cgroup_v2, the real agent, the root helper's
// runtime profile, containerd and runc -- and the running container reads its
// own memory.max and cpu.max back. It is submitted as a run, not straight to
// L1, because a one-shot OCI attempt's handoff volume is owned by its run:
// without the run's owner key the helper refuses the attempt (#494). The
// expected values come from ocihelper.CgroupResources, the code that wrote the
// runtime spec, so this proves the spec reached the kernel verbatim; the
// millicore arithmetic is then checked on its own meaning, and the throttle
// counter proves the kernel enforces the quota.
func TestOCICgroupV2LimitsReadBackFromTheRunningContainer(t *testing.T) {
	reference := os.Getenv("WEFTY_OCI_PROBE_REFERENCE")
	digest := os.Getenv("WEFTY_OCI_PROBE_DIGEST")
	if reference == "" || digest == "" {
		t.Skip("the OCI probe image is not published for this lane")
	}
	expectedMemory, expectedQuota, expectedPeriod := expectedOCICgroupLimits(t)

	evidence := newRealTimingEvidence(t)
	harness := newAcceptanceHarnessWithOptions(t, acceptanceHarnessOptions{
		leaseDuration: 10 * time.Second, runLedgerLane: true, agentArguments: ociAgentArguments(t),
	})
	t.Cleanup(func() {
		evidence.recordProcessOutput("oci-cgroup-limits-agent.log", harness.agent)
	})

	memoryBytes, cpuMillicores := ociCgroupLimitsMemoryBytes, ociCgroupLimitsCPUMillicores
	request := l3.CreateRunRequest{
		Image: &contract.ImageProgram{
			Reference: reference, Digest: stringPointer(digest),
			Argv:           []string{"/bin/sh", "-c", ociCgroupLimitsProbe},
			Limits:         &contract.OCILimits{MemoryBytes: &memoryBytes, CPUMillicores: &cpuMillicores},
			RuntimeHandler: ocihelper.DefaultRuntimeHandler,
		},
		Params: json.RawMessage(`{}`),
	}
	var accepted l3.RunAccepted
	status, body := runLedgerJSON(t, harness, http.MethodPost, "/v1/runs",
		"oci-cgroup-limits-"+strconv.FormatInt(time.Now().UnixNano(), 10), request, &accepted)
	if (status != http.StatusCreated && status != http.StatusOK) || accepted.RunID == "" {
		t.Fatalf("submit cgroup-limited OCI run status = %d body=%s", status, body)
	}
	waitForOCIMailboxRun(t, harness, accepted.RunID)
	_, jobID := readRunLedgerStatus(t, harness, accepted.RunID)
	if jobID == "" {
		t.Fatalf("run %s succeeded without an L1 job", accepted.RunID)
	}
	observed, attemptID := readOCICgroupLimitsLines(t, harness, jobID)

	memoryMax := observed["memory.max"]
	quota, period, cpuParsed := parseCgroupCPUMax(observed["cpu.max"])
	before, beforeErr := strconv.ParseUint(observed["nr_throttled_before"], 10, 64)
	after, afterErr := strconv.ParseUint(observed["nr_throttled_after"], 10, 64)

	memoryReadback := memoryMax == strconv.FormatInt(expectedMemory, 10)
	cpuReadback := cpuParsed && quota == expectedQuota && period == expectedPeriod
	throttled := beforeErr == nil && afterErr == nil && after > before

	evidence.write(ociCgroupLimitsReceipt, fmt.Appendf(nil,
		"cgroup_memory_max_readback=%t\ncgroup_cpu_max_readback=%t\ncgroup_cpu_throttled=%t\n"+
			"cgroup_run_id=%s\ncgroup_job_id=%s\ncgroup_attempt_id=%s\n"+
			"cgroup_memory_bytes_requested=%d\ncgroup_memory_max_expected=%d\ncgroup_memory_max_observed=%s\n"+
			"cgroup_cpu_millicores_requested=%d\ncgroup_cpu_quota_expected=%d\ncgroup_cpu_period_expected=%d\n"+
			"cgroup_cpu_max_observed=%s\ncgroup_cpu_nr_throttled_before=%s\ncgroup_cpu_nr_throttled_after=%s\n",
		memoryReadback, cpuReadback, throttled,
		accepted.RunID, jobID, attemptID,
		memoryBytes, expectedMemory, strings.ReplaceAll(memoryMax, " ", "_"),
		cpuMillicores, expectedQuota, expectedPeriod,
		strings.ReplaceAll(observed["cpu.max"], " ", "_"),
		observed["nr_throttled_before"], observed["nr_throttled_after"]))

	if !memoryReadback {
		t.Errorf("container memory.max = %q, want %d (the runtime profile's memory limit for %d requested bytes)",
			memoryMax, expectedMemory, memoryBytes)
	}
	if !cpuReadback {
		t.Errorf("container cpu.max = %q, want %q (the runtime profile's quota and period for %d millicores)",
			observed["cpu.max"], fmt.Sprintf("%d %d", expectedQuota, expectedPeriod), cpuMillicores)
	}
	if !throttled {
		t.Errorf("a busy loop under a %d-millicore cap was never throttled: nr_throttled %q -> %q",
			cpuMillicores, observed["nr_throttled_before"], observed["nr_throttled_after"])
	}
}

// expectedOCICgroupLimits derives what the kernel must hold from the product's
// own runtime-profile code, then checks that mapping means what a millicore is
// -- a thousandth of one CPU's time per period -- so the live readback cannot
// pass on a mapping that is merely self-consistent.
func expectedOCICgroupLimits(t *testing.T) (memory, quota int64, period uint64) {
	t.Helper()
	resources, err := ocihelper.CgroupResources(ocihelper.WorkloadLimits{
		MemoryBytes: ociCgroupLimitsMemoryBytes, CPUMillicores: ociCgroupLimitsCPUMillicores,
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	if resources.Memory == nil || resources.Memory.Limit == nil {
		t.Fatalf("the runtime profile sets no memory limit for %d bytes: %+v", ociCgroupLimitsMemoryBytes, resources.Memory)
	}
	if resources.CPU == nil || resources.CPU.Quota == nil || resources.CPU.Period == nil {
		t.Fatalf("the runtime profile sets no CPU quota for %d millicores: %+v", ociCgroupLimitsCPUMillicores, resources.CPU)
	}
	memory, quota, period = *resources.Memory.Limit, *resources.CPU.Quota, *resources.CPU.Period
	if memory != ociCgroupLimitsMemoryBytes {
		t.Fatalf("the runtime profile maps a %d-byte request to a %d-byte limit; the kernel must hold exactly what was asked for",
			ociCgroupLimitsMemoryBytes, memory)
	}
	if pageSize := int64(os.Getpagesize()); memory%pageSize != 0 {
		t.Fatalf("memory limit %d is not a multiple of the %d-byte page; the kernel would round memory.max down", memory, pageSize)
	}
	if period == 0 || uint64(quota)*1000 != uint64(ociCgroupLimitsCPUMillicores)*period {
		t.Fatalf("the runtime profile maps %d millicores to quota %d per %d us, which is not %d/1000 of a CPU",
			ociCgroupLimitsCPUMillicores, quota, period, ociCgroupLimitsCPUMillicores)
	}
	return memory, quota, period
}

// readOCICgroupLimitsLines collects the probe's marker lines from the job's
// stdout the way the run-mailbox lane reads workload output: straight from
// L1's log_events, which is where the agent delivered it. Completion and the
// final log flush are not ordered for a reader, so it polls until all four
// lines are present.
func readOCICgroupLimitsLines(t *testing.T, harness *acceptanceHarness, jobID string) (map[string]string, string) {
	t.Helper()
	want := []string{"memory.max", "cpu.max", "nr_throttled_before", "nr_throttled_after"}
	deadline := time.Now().Add(30 * time.Second)
	for {
		stdout, attemptID := ociCgroupLimitsStdout(t, harness, jobID)
		observed := map[string]string{}
		for _, line := range strings.Split(stdout, "\n") {
			fact, found := strings.CutPrefix(line, ociCgroupLimitsMarker)
			if !found {
				continue
			}
			if name, value, ok := strings.Cut(fact, "="); ok {
				observed[name] = value
			}
		}
		complete := true
		for _, name := range want {
			_, present := observed[name]
			complete = complete && present
		}
		if complete {
			return observed, attemptID
		}
		if time.Now().After(deadline) {
			t.Fatalf("the probe's cgroup readback never reached L1: have %v%s", observed, lastAttemptOutput(t, harness, jobID))
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// ociCgroupLimitsStdout concatenates the job's stdout chunks in delivery
// order. The run is one-shot with a single successful attempt, which it also
// returns for the receipt.
func ociCgroupLimitsStdout(t *testing.T, harness *acceptanceHarness, jobID string) (string, string) {
	t.Helper()
	database, err := sql.Open("sqlite", harness.l1Database+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	rows, err := database.Query(`SELECT attempt_id, bytes FROM log_events
		WHERE job_id=? AND stream=? ORDER BY ordinal`, jobID, string(contract.LogStdout))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var stdout strings.Builder
	var attemptID string
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&attemptID, &payload); err != nil {
			t.Fatal(err)
		}
		stdout.Write(payload)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return stdout.String(), attemptID
}

// parseCgroupCPUMax reads cgroup v2's "$MAX $PERIOD" pair. An unlimited cgroup
// reads "max 100000", which does not parse as a quota and so fails the proof.
func parseCgroupCPUMax(value string) (quota int64, period uint64, ok bool) {
	fields := strings.Fields(value)
	if len(fields) != 2 {
		return 0, 0, false
	}
	quota, quotaErr := strconv.ParseInt(fields[0], 10, 64)
	period, periodErr := strconv.ParseUint(fields[1], 10, 64)
	return quota, period, quotaErr == nil && periodErr == nil
}
