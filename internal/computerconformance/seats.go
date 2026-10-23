package computerconformance

import (
	"fmt"
	"strconv"
	"strings"
)

// CheckSeatUsers checks the default image's four independent desktop identities.
// A numeric uid alone is insufficient: the desktop's session bus needs passwd.
func CheckSeatUsers(passwd []byte) error {
	entries := map[string][]string{}
	for _, line := range strings.Split(string(passwd), "\n") {
		fields := strings.Split(line, ":")
		if len(fields) == 7 {
			entries[fields[0]] = fields
		}
	}
	for place := 1; place <= 4; place++ {
		name := fmt.Sprintf("seat%d", place)
		f, ok := entries[name]
		if !ok {
			return fmt.Errorf("%s: passwd entry missing", name)
		}
		uid, e1 := strconv.Atoi(f[2])
		gid, e2 := strconv.Atoi(f[3])
		if e1 != nil || e2 != nil || uid != 2000+place || gid != 2000+place {
			return fmt.Errorf("%s: expected uid and gid %d", name, 2000+place)
		}
		if f[5] != fmt.Sprintf("/wefty/service/seats/%d/home", place) {
			return fmt.Errorf("%s: home is outside its Seat folder", name)
		}
	}
	return nil
}

// SeatEvidence contains observations from the real-profile Linux lane. It is
// separate from the single-desktop Docker approximation's v2 receipt.
// Missing observations stay NOT-RUN; endpoint confinement belongs to P7.
type SeatEvidence struct {
	Version                  int             `json:"version"`
	Image                    string          `json:"image"`
	Platform                 string          `json:"platform"`
	Observations             map[string]bool `json:"observations"`
	MemoryBeforeRemovalBytes int64           `json:"memory_before_removal_bytes"`
	MemoryAfterRemovalBytes  int64           `json:"memory_after_removal_bytes"`
	InstallBytes             int64           `json:"install_bytes"`
	MemoryLimitBytes         int64           `json:"memory_limit_bytes"`
	TmpfsCeilingBytes        int64           `json:"tmpfs_ceiling_bytes"`
}

type SeatReport struct {
	Version  int          `json:"version"`
	Image    string       `json:"image"`
	Platform string       `json:"platform"`
	Status   Status       `json:"status"`
	Checks   []Check      `json:"checks"`
	Evidence SeatEvidence `json:"evidence"`
}

// CheckSeats checks observed file, process, screen and runtime outcomes. This
// deliberately does not infer runtime success from the supervisor's ready answer.
func CheckSeats(e SeatEvidence) SeatReport {
	r := SeatReport{Version: 1, Image: e.Image, Platform: e.Platform, Status: StatusPass, Evidence: e}
	rows := []struct {
		id           string
		observations []string
	}{
		{"IM-1", []string{"second_browser", "own_screen_input"}},
		{"IM-2", []string{"view_ignores_input"}},
		{"IM-4.display-files", []string{"own_display", "other_files_refused", "other_display_refused"}},
		{"IM-5", []string{"removed_processes_absent", "removed_endpoints_absent", "removed_folder_absent", "shared_survives", "memory_returns"}},
		{"IM-6", []string{"restart_processes_absent", "restart_folders_absent", "restart_answers_absent"}},
		{"seats.passwd", []string{"passwd_entries", "missing_passwd_refused"}},
		{"seats.driver", []string{"driver_per_seat"}},
		{"seats.deadline", []string{"deadline_per_seat"}},
		{"seats.tools", []string{"tools_present"}},
		{"seats.profile", []string{"readonly_root", "profile_unmodified"}},
	}
	for _, row := range rows {
		check := Check{ID: row.id, Scope: ScopeImage, Status: StatusPass, Summary: strings.Join(row.observations, ", ")}
		for _, name := range row.observations {
			value, observed := e.Observations[name]
			if !observed && check.Status != StatusFail {
				check.Status = StatusNotRun
			}
			if observed && !value {
				check.Status = StatusFail
				check.Detail += name + " failed; "
			}
		}
		if row.id == "IM-5" && e.Observations["memory_returns"] {
			if e.MemoryBeforeRemovalBytes <= 0 || e.MemoryAfterRemovalBytes <= 0 {
				if check.Status != StatusFail {
					check.Status = StatusNotRun
				}
			} else if !MemoryReclaimed(e.MemoryBeforeRemovalBytes, e.MemoryAfterRemovalBytes) {
				check.Status = StatusFail
				check.Detail += "memory drop is below 64 MiB"
			}
		}
		r.Checks = append(r.Checks, check)
	}
	measurements := Check{ID: "seats.measurements", Scope: ScopeImage, Status: StatusNotRun, Summary: "disk install and bounded memory-backed temp space"}
	if e.InstallBytes > 0 && e.MemoryLimitBytes > 0 && e.TmpfsCeilingBytes > 0 {
		measurements.Status = StatusPass
		if e.TmpfsCeilingBytes > min(e.MemoryLimitBytes, int64(1<<30))/2 {
			measurements.Status = StatusFail
		}
	}
	r.Checks = append(r.Checks, measurements)
	for _, check := range r.Checks {
		if check.Status == StatusFail {
			r.Status = StatusFail
			break
		}
		if check.Status == StatusNotRun {
			r.Status = StatusNotRun
		}
	}
	if e.Version != 1 || e.Image == "" {
		r.Status = StatusFail
	}
	r.Checks = append(r.Checks, Check{ID: "IM-4.endpoints", Scope: ScopeImage, Status: StatusNotRun,
		Summary: "per-Seat endpoint confinement", Detail: "P7 belongs to #1138; loopback is shared"})
	return r
}

// MemoryReclaimed requires a meaningful drop, beyond ordinary sample noise.
func MemoryReclaimed(before, after int64) bool {
	return before > 0 && after > 0 && before-after >= 64<<20
}
