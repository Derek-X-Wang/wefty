// Package computerconformance owns the stable evidence vocabulary emitted by
// the Computer image conformance checker.
package computerconformance

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"
)

const ReceiptVersion = 1

type Status string

const (
	StatusPass   Status = "PASS"
	StatusFail   Status = "FAIL"
	StatusNotRun Status = "NOT-RUN"
)

type Check struct {
	ID      string `json:"id"`
	Status  Status `json:"status"`
	Summary string `json:"summary"`
	Detail  string `json:"detail,omitempty"`
}

type Receipt struct {
	Version    int       `json:"version"`
	Image      string    `json:"image"`
	Runtime    string    `json:"runtime"`
	Platform   string    `json:"platform,omitempty"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	Status     Status    `json:"status"`
	Checks     []Check   `json:"checks"`

	// Compatibility projection consumed by the already-published #234 image
	// promotion assertion. Every positive value is derived from the complete
	// stable-check aggregate, so the old seam cannot hide a NOT-RUN cell.
	Executed               bool                   `json:"executed"`
	RFBWebSocketV1         bool                   `json:"rfb_websocket_v1"`
	TransportAssertions    bool                   `json:"transport_assertions"`
	NegativeRows           CompatibilityNegatives `json:"negative_rows"`
	Endpoints              CompatibilityEndpoints `json:"endpoints"`
	Roles                  CompatibilityRoles     `json:"roles"`
	DriverSignalConsumed   bool                   `json:"driver_signal_consumed"`
	ProfilePersistent      bool                   `json:"profile_persistent"`
	SignInPersistent       bool                   `json:"sign_in_persistent"`
	RootFSReadOnly         bool                   `json:"rootfs_read_only"`
	AttemptTmpfsDiscarded  bool                   `json:"attempt_tmpfs_discarded"`
	Shm                    CompatibilityShm       `json:"shm"`
	ReadinessSeconds       float64                `json:"readiness_seconds"`
	RestartedEdgeRecovered bool                   `json:"restarted_edge_recovered"`
}

type CompatibilityNegatives struct {
	DriverFailClosed bool `json:"driver_fail_closed"`
}

type CompatibilityEndpoints struct {
	View    string `json:"view"`
	Control string `json:"control"`
}

type CompatibilityRoles struct {
	ViewProcessViewOnly       bool `json:"view_process_view_only"`
	ControlProcessInteractive bool `json:"control_process_interactive"`
	ViewPointerDiscarded      bool `json:"view_pointer_discarded"`
}

type CompatibilityShm struct {
	Private    bool `json:"private"`
	Conformant bool `json:"conformant"`
}

// Recorder starts every catalogued cell at NOT-RUN. A crash or early return
// therefore cannot silently turn missing evidence into a pass.
type Recorder struct {
	receipt Receipt
	index   map[string]int
}

func NewRecorder(image, runtimeName, platform string, startedAt time.Time) *Recorder {
	checks := make([]Check, len(CheckCatalog))
	index := make(map[string]int, len(CheckCatalog))
	for i, definition := range CheckCatalog {
		checks[i] = Check{ID: definition.ID, Status: StatusNotRun, Summary: definition.Summary}
		index[definition.ID] = i
	}
	return &Recorder{receipt: Receipt{
		Version: ReceiptVersion, Image: image, Runtime: runtimeName, Platform: platform,
		StartedAt: startedAt.UTC(), Status: StatusNotRun, Checks: checks,
	}, index: index}
}

func (r *Recorder) Record(id string, status Status, detail string) error {
	if status != StatusPass && status != StatusFail && status != StatusNotRun {
		return fmt.Errorf("invalid conformance status %q", status)
	}
	i, ok := r.index[id]
	if !ok {
		return fmt.Errorf("unknown Computer conformance check %q", id)
	}
	r.receipt.Checks[i].Status = status
	r.receipt.Checks[i].Detail = detail
	return nil
}

func (r *Recorder) Finish(finishedAt time.Time) Receipt {
	r.receipt.FinishedAt = finishedAt.UTC()
	r.receipt.Status = Aggregate(r.receipt.Checks)
	passed := r.receipt.Status == StatusPass
	r.receipt.Executed = passed
	r.receipt.RFBWebSocketV1 = passed
	r.receipt.TransportAssertions = passed
	r.receipt.NegativeRows.DriverFailClosed = passed
	if passed {
		r.receipt.Endpoints = CompatibilityEndpoints{View: "loopback", Control: "loopback"}
	}
	r.receipt.Roles = CompatibilityRoles{ViewProcessViewOnly: passed, ControlProcessInteractive: passed, ViewPointerDiscarded: passed}
	r.receipt.DriverSignalConsumed = passed
	r.receipt.ProfilePersistent = passed
	r.receipt.SignInPersistent = passed
	r.receipt.RootFSReadOnly = passed
	r.receipt.AttemptTmpfsDiscarded = passed
	r.receipt.Shm = CompatibilityShm{Private: passed, Conformant: passed}
	r.receipt.RestartedEdgeRecovered = passed
	r.receipt.Checks = slices.Clone(r.receipt.Checks)
	return r.receipt
}

func (r *Recorder) RecordReadiness(duration time.Duration) {
	r.receipt.ReadinessSeconds = duration.Seconds()
}

func Aggregate(checks []Check) Status {
	status := StatusPass
	for _, check := range checks {
		switch check.Status {
		case StatusFail:
			return StatusFail
		case StatusNotRun:
			status = StatusNotRun
		case StatusPass:
		default:
			return StatusFail
		}
	}
	return status
}

func Marshal(receipt Receipt) ([]byte, error) {
	return json.MarshalIndent(receipt, "", "  ")
}

type CheckDefinition struct {
	ID      string
	Summary string
}

// CheckCatalog is append-only within receipt version 1. Stable IDs let CI and
// image authors compare evidence without scraping human prose.
var CheckCatalog = []CheckDefinition{
	{ID: "runtime.started", Summary: "image starts under the Computer OCI profile"},
	{ID: "runtime.image-config", Summary: "image USER, ENTRYPOINT, CMD, and working directory retain OCI semantics"},
	{ID: "environment.service-dir", Summary: "WEFTY_SERVICE_DIR is authoritative"},
	{ID: "environment.view-port", Summary: "view port is authoritative"},
	{ID: "environment.control-port", Summary: "control port is authoritative"},
	{ID: "environment.service-port-omitted", Summary: "WEFTY_SERVICE_PORT is omitted"},
	{ID: "environment.authority-omitted", Summary: "default-off Computer authority environment is omitted"},
	{ID: "environment.other-wefty-preserved", Summary: "unreserved WEFTY_* environment remains tenant-owned"},
	{ID: "endpoints.distinct", Summary: "view and control use distinct allocated ports"},
	{ID: "endpoints.view-loopback", Summary: "view binds IPv4 loopback only"},
	{ID: "endpoints.control-loopback", Summary: "control binds IPv4 loopback only"},
	{ID: "transport.view-ready", Summary: "view completes rfb-websocket-v1"},
	{ID: "transport.control-ready", Summary: "control completes rfb-websocket-v1"},
	{ID: "transport.query-ignored", Summary: "query does not change routing"},
	{ID: "transport.fragment-ignored", Summary: "fragment does not change routing"},
	{ID: "transport.wrong-path-rejected", Summary: "wrong WebSocket path is rejected"},
	{ID: "transport.missing-subprotocol-rejected", Summary: "missing binary subprotocol is rejected"},
	{ID: "transport.wrong-subprotocol-rejected", Summary: "wrong subprotocol is rejected"},
	{ID: "transport.text-frame-rejected", Summary: "text RFB frame is rejected"},
	{ID: "readiness.before-deadline", Summary: "both endpoints are ready before the 60 second deadline"},
	{ID: "input.view-isolated", Summary: "view discards byte-identical pointer and key input"},
	{ID: "input.control-accepted", Summary: "control accepts byte-identical pointer and key input"},
	{ID: "driver.read-only", Summary: "driver.json is tenant read-only"},
	{ID: "driver.mode", Summary: "driver.json mode is exactly 0444"},
	{ID: "driver.initial-false", Summary: "fresh attempt starts with exact false driver document"},
	{ID: "driver.true-consumed", Summary: "tenant reopens and consumes true driver document"},
	{ID: "driver.release-consumed", Summary: "tenant consumes the restored false document"},
	{ID: "driver.malformed-fails-closed", Summary: "malformed and wrong-type driver documents fail closed"},
	{ID: "driver.missing-fails-closed", Summary: "missing driver document fails closed"},
	{ID: "profile.image-user", Summary: "runtime retains the image USER"},
	{ID: "profile.rootfs-read-only", Summary: "root filesystem is read-only"},
	{ID: "profile.service-writable", Summary: "/wefty/service is the persistent writable path"},
	{ID: "profile.no-new-privileges", Summary: "noNewPrivileges is active"},
	{ID: "profile.capabilities", Summary: "capability bounding set has no forbidden capability"},
	{ID: "profile.shm-private", Summary: "/dev/shm is attempt-private"},
	{ID: "profile.shm-size", Summary: "/dev/shm ceiling is exactly 1 GiB"},
	{ID: "profile.shm-flags", Summary: "/dev/shm is mode 1777,nosuid,nodev,noexec"},
	{ID: "profile.tmp-ceilings", Summary: "/tmp and /var/tmp retain the bounded Computer ceilings"},
	{ID: "persistence.service-survives", Summary: "/wefty/service survives a fresh attempt"},
	{ID: "persistence.rootfs-discarded", Summary: "attempt-local rootfs state does not survive"},
	{ID: "targets.control-nonpersistent", Summary: "/wefty/control state does not enter service data"},
}
