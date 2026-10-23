package computerconformance

import (
	"os"
	"strings"
	"testing"
)

func TestSeatUsersRequirePasswdEntries(t *testing.T) {
	valid := "root:x:0:0:root:/root:/bin/sh\n"
	for _, entry := range []string{
		"seat1:x:2001:2001::/wefty/service/seats/1/home:/bin/sh\n",
		"seat2:x:2002:2002::/wefty/service/seats/2/home:/bin/sh\n",
		"seat3:x:2003:2003::/wefty/service/seats/3/home:/bin/sh\n",
		"seat4:x:2004:2004::/wefty/service/seats/4/home:/bin/sh\n",
	} {
		valid += entry
	}
	if err := CheckSeatUsers([]byte(valid)); err != nil {
		t.Fatal(err)
	}
	fixture, err := os.ReadFile("testdata/seats-missing-passwd")
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckSeatUsers(fixture); err == nil || !strings.Contains(err.Error(), "seat4") {
		t.Fatalf("missing passwd entry accepted: %v", err)
	}
}

func TestSeatEvidenceNeverPromotesMissingRuntimeProof(t *testing.T) {
	r := CheckSeats(SeatEvidence{Version: 1, Image: "example.invalid/default@sha256:abc"})
	if r.Status == StatusPass {
		t.Fatal("empty observations reported PASS")
	}
	for _, c := range r.Checks {
		if c.ID == "IM-4.endpoints" && c.Status != StatusNotRun {
			t.Fatal("claimed P7")
		}
	}
}

func TestSeatEvidenceDetectsViewInputAndStaleRestartSeats(t *testing.T) {
	evidence := SeatEvidence{Version: 1, Image: "example.invalid/default@sha256:abc", Observations: map[string]bool{
		"other_endpoints_refused": true, "cross_computer_endpoints_refused": true,
		"second_browser": true, "own_screen_input": true, "view_ignores_input": true,
		"own_display": true, "other_files_refused": true, "other_display_refused": true,
		"removed_processes_absent": true, "removed_endpoints_absent": true,
		"removed_folder_absent": true, "shared_survives": true, "memory_returns": true,
		"restart_processes_absent": true, "restart_folders_absent": true,
		"restart_answers_absent": true, "passwd_entries": true, "missing_passwd_refused": true,
		"driver_per_seat": true, "deadline_per_seat": true, "tools_present": true,
		"readonly_root": true, "profile_unmodified": true, "seat_command_python": true,
	}, InstallBytes: 1, MemoryLimitBytes: 1 << 30, TmpfsCeilingBytes: 465 << 20, MemoryBeforeRemovalBytes: 512 << 20, MemoryAfterRemovalBytes: 256 << 20}
	if r := CheckSeats(evidence); r.Status != StatusPass {
		t.Fatalf("complete evidence: %+v", r)
	}
	for _, name := range []string{"view_ignores_input", "restart_processes_absent", "missing_passwd_refused"} {
		evidence.Observations[name] = false
		if r := CheckSeats(evidence); r.Status != StatusFail {
			t.Fatalf("%s mutation accepted: %+v", name, r)
		}
		evidence.Observations[name] = true
	}
}

func TestSeatEvidenceRejectsTempCeilingsAboveOneSeatRoom(t *testing.T) {
	evidence := SeatEvidence{Version: 1, Image: "example.invalid/default@sha256:abc", InstallBytes: 1,
		MemoryLimitBytes: 1 << 30, TmpfsCeilingBytes: 1728 << 20}
	report := CheckSeats(evidence)
	for _, check := range report.Checks {
		if check.ID == "seats.measurements" {
			if check.Status != StatusFail {
				t.Fatalf("oversized tmpfs ceilings accepted: %+v", check)
			}
			return
		}
	}
	t.Fatal("missing temp-space evidence cell")
}

func TestSeatTempEvidenceCannotHideOneGiBLimitByClaimingLargerComputer(t *testing.T) {
	evidence := SeatEvidence{Version: 1, Image: "example.invalid/image", InstallBytes: 1, MemoryLimitBytes: 2 << 30, TmpfsCeilingBytes: 768 << 20}
	for _, check := range CheckSeats(evidence).Checks {
		if check.ID == "seats.measurements" && check.Status != StatusFail {
			t.Fatalf("oversized one-Seat temp space accepted: %+v", check)
		}
	}
}

func TestSeatMemoryReturnsRejectsNoise(t *testing.T) {
	if MemoryReclaimed(300<<20, 299<<20) {
		t.Fatal("one MiB of sampling noise claimed memory returns")
	}
	if !MemoryReclaimed(300<<20, 200<<20) {
		t.Fatal("substantial Seat memory drop rejected")
	}
	if MemoryReclaimed(100<<20, 200<<20) {
		t.Fatal("memory growth claimed reclamation")
	}
}

func TestSeatEndpointConfinementRequiresObservedCrossovers(t *testing.T) {
	e := SeatEvidence{Version: 1, Image: "example.invalid/image", Observations: map[string]bool{"other_endpoints_refused": true, "cross_computer_endpoints_refused": true}}
	find := func() Status {
		for _, c := range CheckSeats(e).Checks {
			if c.ID == "IM-4.endpoints" {
				return c.Status
			}
		}
		t.Fatal("missing endpoint evidence")
		return StatusFail
	}
	if got := find(); got != StatusPass {
		t.Fatalf("real crossover refusals not counted: %s", got)
	}
	for _, name := range []string{"other_endpoints_refused", "cross_computer_endpoints_refused"} {
		e.Observations[name] = false
		if got := find(); got != StatusFail {
			t.Fatalf("%s successful crossover accepted: %s", name, got)
		}
		delete(e.Observations, name)
		if got := find(); got != StatusNotRun {
			t.Fatalf("%s missing proof promoted: %s", name, got)
		}
		e.Observations[name] = true
	}
}
