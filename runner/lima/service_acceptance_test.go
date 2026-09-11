//go:build service_acceptance && darwin

package lima

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type attendedArtifact struct {
	SessionID string                    `json:"session_id"`
	Commit    string                    `json:"commit"`
	Versions  map[string]string         `json:"versions"`
	Rows      map[string]attendedResult `json:"rows"`
}

type attendedResult struct {
	Status                 string              `json:"status"`
	Reason                 string              `json:"reason,omitempty"`
	SessionID              string              `json:"session_id"`
	Command                []string            `json:"command"`
	ExitCode               int                 `json:"exit_code"`
	HelperGenerations      []uint64            `json:"helper_generations"`
	CapabilityRevisions    []int64             `json:"capability_revisions"`
	Inventories            []json.RawMessage   `json:"inventories"`
	RoundTrip              bool                `json:"round_trip"`
	DynamicListeners       map[string]bool     `json:"dynamic_listeners"`
	LaunchUnits            []string            `json:"launch_units,omitempty"`
	LimaStates             []InstanceState     `json:"lima_states,omitempty"`
	OCIEnabled             *bool               `json:"oci_enabled,omitempty"`
	ProcessAvailable       bool                `json:"process_available,omitempty"`
	SocketMode             string              `json:"socket_mode,omitempty"`
	SocketOwner            string              `json:"socket_owner,omitempty"`
	SocketGroup            string              `json:"socket_group,omitempty"`
	MinimalDoctor          *MinimalDoctorFacts `json:"minimal_doctor,omitempty"`
	AttemptIDs             []string            `json:"attempt_ids"`
	TopLevelDigests        []string            `json:"top_level_digests"`
	PlatformDigests        []string            `json:"platform_digests"`
	PayloadExecutions      int                 `json:"payload_executions"`
	StdoutMarkers          []string            `json:"stdout_markers"`
	StderrMarkers          []string            `json:"stderr_markers"`
	HandoffMarkerBytes     []string            `json:"handoff_marker_bytes"`
	HandoffAbsent          bool                `json:"handoff_absent_after_completion"`
	ServiceOwners          []string            `json:"service_owners,omitempty"`
	ServiceAttemptCounts   []int               `json:"service_attempt_counts,omitempty"`
	GuestNativeData        bool                `json:"guest_native_data,omitempty"`
	VirtioFSData           *bool               `json:"virtiofs_data,omitempty"`
	RootfsDiscarded        bool                `json:"rootfs_discarded,omitempty"`
	RemovalPhase           string              `json:"removal_phase,omitempty"`
	RemovalPendingObserved bool                `json:"removal_pending_observed,omitempty"`
	RemovalCompleted       bool                `json:"removal_completed,omitempty"`
	RuntimeQuiesced        bool                `json:"runtime_quiesced,omitempty"`
	ResourceManifests      []json.RawMessage   `json:"resource_manifests,omitempty"`
	PostDeleteAttestation  bool                `json:"post_delete_attestation,omitempty"`
	ServiceDataBytesAbsent bool                `json:"service_data_bytes_absent,omitempty"`
	OwnerRecordAbsent      bool                `json:"service_data_owner_record_absent,omitempty"`
	DeleteAttestRestart    bool                `json:"delete_attest_restart_observed,omitempty"`
	BindSourcesUntouched   bool                `json:"bind_sources_untouched,omitempty"`
	ImageCacheRetained     bool                `json:"image_cache_retained,omitempty"`
	RemovalAssertions      []removalAssertion  `json:"removal_assertions,omitempty"`
}

type removalAssertion struct {
	Class  string `json:"class"`
	ID     string `json:"id"`
	Absent bool   `json:"absent"`
}

var requiredAttendedRows = []string{
	"template_permissions", "probe", "task_logs_delete", "mount_validation",
	"host_to_guest", "guest_to_host_primary", "guest_to_host_fallback",
	"helper_loss", "vm_loss", "sweep_before_recovery",
	"dynamic_forwarding_disabled", "raw_containerd_denied",
	"service_health_echo", "service_startup_timeout", "service_withdrawal_republication",
	"service_port_collision", "service_portless_started",
	"service_restart_fresh_attempt", "service_stop_start_capacity", "service_failed_quiescence",
	"service_data_guest_native", "service_removal_manifest_offline",
	"launch_daemon", "no_lima_autostart", "helper_install_permissions",
	"stopped_enabled_recovery", "stopped_disabled_no_recovery", "broken_enabled_recovery",
	"process_only_degradation", "minimal_doctor",
	"oci_oneshot_run", "oci_oneshot_prestarted_loss",
	"oci_oneshot_poststarted_loss", "oci_oneshot_rerun_identity",
}

func missingRequiredAttendedRow(rows map[string]attendedResult) string {
	for _, name := range requiredAttendedRows {
		if _, ok := rows[name]; !ok {
			return name
		}
	}
	return ""
}

func TestServiceAcceptanceLimaTemplateValidatesWithInstalledLima(t *testing.T) {
	limactl, err := exec.LookPath("limactl")
	if err != nil {
		receipt, _ := json.Marshal(map[string]string{"status": "NOT-RUN", "reason": "limactl is not installed; Mac/Lima is owner-hardware acceptance"})
		t.Skip(string(receipt))
	}
	payload, err := RenderTemplate(TemplateConfig{
		Sizing:               Sizing{Memory: "4GiB", CPUs: 4, Disk: "32GiB"},
		HostAllowedMountRoot: filepath.Join(t.TempDir(), "allowed"),
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "wefty-oci.yaml")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command(limactl, "--tty=false", "template", "validate", path).CombinedOutput()
	if err != nil {
		t.Fatalf("Lima 2.2 template validation: %v: %s", err, output)
	}
	filled, err := exec.Command(limactl, "--tty=false", "template", "copy", "--fill", path, "-").CombinedOutput()
	if err != nil {
		t.Fatalf("fill Lima 2.2 template: %v: %s", err, filled)
	}
	var effective struct {
		PortForwards []struct {
			GuestIP           string `yaml:"guestIP"`
			GuestIPMustBeZero *bool  `yaml:"guestIPMustBeZero"`
			GuestPortRange    [2]int `yaml:"guestPortRange"`
			Proto             string `yaml:"proto"`
			Ignore            bool   `yaml:"ignore"`
		} `yaml:"portForwards"`
	}
	if err := yaml.Unmarshal(filled, &effective); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, rule := range effective.PortForwards {
		if rule.GuestIP == "0.0.0.0" && rule.GuestIPMustBeZero != nil && !*rule.GuestIPMustBeZero &&
			rule.GuestPortRange == [2]int{1, 65535} && rule.Proto == "any" && rule.Ignore {
			found = true
		}
	}
	if !found {
		t.Fatalf("filled template omitted effective all-address ignore rule: %+v", effective.PortForwards)
	}
}

func TestServiceAcceptanceAttendedLimaGatewayBindingReceipt(t *testing.T) {
	if os.Getenv("WEFTY_LIMA_ATTENDED") != "1" {
		receipt, _ := json.Marshal(map[string]string{
			"status": "NOT-RUN", "reason": "set WEFTY_LIMA_ATTENDED=1 only during the docs/acceptance/m3-lima-transport.md owner-hardware procedure",
		})
		t.Skip(string(receipt))
	}
	instance := os.Getenv("WEFTY_LIMA_INSTANCE")
	if instance == "" {
		instance = DefaultInstanceName
	}
	binding, err := NewBridgeBinder(instance).Bind(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer binding.Listener.Close()
	if binding.AdvertiseHost != HostGatewayName || binding.HostBridgeFallback {
		t.Fatalf("unsafe attended bridge binding: %+v", binding)
	}
	marker := "wefty-lima-gateway-roundtrip"
	done := make(chan error, 1)
	go func() {
		connection, err := binding.Listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer connection.Close()
		buffer := make([]byte, 4096)
		_, err = connection.Read(buffer)
		if err == nil {
			_, err = fmt.Fprintf(connection, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(marker), marker)
		}
		done <- err
	}()
	port := binding.Listener.Addr().(*net.TCPAddr).Port
	output, err := exec.Command("limactl", "--tty=false", "shell", "--workdir=/", instance, "wget", "-qO-", fmt.Sprintf("http://%s:%d/", HostGatewayName, port)).CombinedOutput()
	if err != nil || string(output) != marker {
		t.Fatalf("guest gateway round trip: %v: %q", err, output)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	t.Log(`{"status":"PASS","row":"gateway-discovery-and-constrained-bind"}`)
}

func TestServiceAcceptanceAttendedLimaArtifact(t *testing.T) {
	path := os.Getenv("WEFTY_LIMA_ACCEPTANCE_ARTIFACT")
	if path == "" {
		receipt, _ := json.Marshal(map[string]string{
			"status": "NOT-RUN", "reason": "set WEFTY_LIMA_ACCEPTANCE_ARTIFACT to the redacted owner-hardware receipt from docs/acceptance/m3-lima-transport.md",
		})
		t.Skip(string(receipt))
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var artifact attendedArtifact
	if err := decoder.Decode(&artifact); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		t.Fatal("attended artifact contains trailing data")
	}
	command := exec.Command("git", "rev-parse", "HEAD")
	command.Dir = filepath.Join("..", "..")
	head, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Commit != string(bytes.TrimSpace(head)) {
		t.Fatalf("attended artifact commit %q does not match candidate %q", artifact.Commit, bytes.TrimSpace(head))
	}
	if artifact.SessionID == "" || len(artifact.Versions) == 0 {
		t.Fatal("attended artifact omitted session or tool versions")
	}
	if missing := missingRequiredAttendedRow(artifact.Rows); missing != "" {
		t.Fatalf("attended artifact omitted required row %q", missing)
	}
	for _, name := range requiredAttendedRows {
		row, ok := artifact.Rows[name]
		if name == "stopped_disabled_no_recovery" && ok && row.Status == "NOT-RUN" {
			if strings.TrimSpace(row.Reason) == "" {
				t.Fatalf("attended artifact row %q has an unsanitized empty NOT-RUN reason", name)
			}
			continue
		}
		if !ok || row.Status != "PASS" || row.SessionID != artifact.SessionID || len(row.Command) == 0 || row.ExitCode != 0 {
			t.Fatalf("attended artifact row %q = %+v, present=%t; destination success requires PASS evidence", name, row, ok)
		}
	}
	if !artifact.Rows["guest_to_host_primary"].RoundTrip {
		t.Fatal("gateway row lacks a real guest round trip")
	}
	oneshot := artifact.Rows["oci_oneshot_run"]
	if !oneshot.RoundTrip || oneshot.PayloadExecutions != 1 || len(oneshot.AttemptIDs) != 1 ||
		!sameNonEmptyStrings(oneshot.TopLevelDigests) || !sameNonEmptyStrings(oneshot.PlatformDigests) ||
		!containsString(oneshot.StdoutMarkers, "wefty-echo-once-stdout") ||
		!containsString(oneshot.StderrMarkers, "wefty-echo-once-stderr") ||
		!containsString(oneshot.HandoffMarkerBytes, "wefty echo one-shot handoff\n") || !oneshot.HandoffAbsent {
		t.Fatalf("ordinary OCI one-shot row lacks bridge/digest/single-execution evidence: %+v", oneshot)
	}
	prestarted := artifact.Rows["oci_oneshot_prestarted_loss"]
	if !prestarted.RoundTrip || prestarted.PayloadExecutions != 1 || len(prestarted.AttemptIDs) != 2 ||
		prestarted.AttemptIDs[0] == prestarted.AttemptIDs[1] ||
		!sameNonEmptyStrings(prestarted.TopLevelDigests) || !sameNonEmptyStrings(prestarted.PlatformDigests) {
		t.Fatalf("pre-Started loss row lacks one requeue without duplicate payload execution: %+v", prestarted)
	}
	poststarted := artifact.Rows["oci_oneshot_poststarted_loss"]
	if poststarted.PayloadExecutions != 1 || len(poststarted.AttemptIDs) != 1 ||
		!sameNonEmptyStrings(poststarted.TopLevelDigests) || !sameNonEmptyStrings(poststarted.PlatformDigests) {
		t.Fatalf("post-Started loss row lacks one terminal payload execution: %+v", poststarted)
	}
	rerun := artifact.Rows["oci_oneshot_rerun_identity"]
	if !rerun.RoundTrip || rerun.PayloadExecutions != 2 || len(rerun.AttemptIDs) != 2 ||
		rerun.AttemptIDs[0] == rerun.AttemptIDs[1] ||
		!sameNonEmptyStrings(rerun.TopLevelDigests) || !sameNonEmptyStrings(rerun.PlatformDigests) ||
		!containsString(rerun.HandoffMarkerBytes, "wefty echo one-shot handoff\n") || !rerun.HandoffAbsent {
		t.Fatalf("OCI rerun row lacks frozen identity and distinct execution evidence: %+v", rerun)
	}
	dynamic := artifact.Rows["dynamic_forwarding_disabled"].DynamicListeners
	if dynamic["127.0.0.1"] || dynamic["0.0.0.0"] || len(dynamic) != 2 {
		t.Fatal("dynamic forwarding receipt lacks both unreachable listener proofs")
	}
	for _, name := range []string{"helper_loss", "vm_loss", "sweep_before_recovery"} {
		row := artifact.Rows[name]
		if len(row.HelperGenerations) < 2 || len(row.CapabilityRevisions) < 2 || len(row.Inventories) < 2 {
			t.Fatalf("row %s lacks generation/revision/inventory transition evidence", name)
		}
	}
	serviceData := artifact.Rows["service_data_guest_native"]
	for _, owner := range []string{"0:0", "13001:13002", "12001:12002"} {
		if !slices.Contains(serviceData.ServiceOwners, owner) {
			t.Fatalf("service data receipt omitted initialized owner %s: %+v", owner, serviceData)
		}
	}
	if !slices.Equal(serviceData.ServiceAttemptCounts, []int{0, 1, 2}) || !serviceData.GuestNativeData || serviceData.VirtioFSData == nil || *serviceData.VirtioFSData || !serviceData.RootfsDiscarded {
		t.Fatalf("service data receipt lacks restart/stop-start persistence and fresh-rootfs proof: %+v", serviceData)
	}
	removal := artifact.Rows["service_removal_manifest_offline"]
	if removal.RemovalPhase != "complete" || !removal.RemovalPendingObserved || !removal.RemovalCompleted || !removal.RuntimeQuiesced || len(removal.ResourceManifests) == 0 ||
		!removal.PostDeleteAttestation || !removal.ServiceDataBytesAbsent || !removal.OwnerRecordAbsent || !removal.DeleteAttestRestart ||
		!removal.BindSourcesUntouched || !removal.ImageCacheRetained {
		t.Fatalf("offline removal row lacks pending manifest/quiescence evidence: %+v", removal)
	}
	expectedAssertions := make(map[string]struct{})
	for index, payload := range removal.ResourceManifests {
		var manifest struct {
			JobID                  string `json:"job_id"`
			AttemptID              string `json:"attempt_id"`
			RemovalGeneration      string `json:"removal_generation"`
			LeaseID                string `json:"lease_id"`
			TaskID                 string `json:"task_id"`
			ContainerID            string `json:"container_id"`
			SnapshotID             string `json:"snapshot_id"`
			ShimID                 string `json:"shim_id"`
			CgroupID               string `json:"cgroup_id"`
			LogSegmentDirectory    string `json:"log_segment_directory"`
			ServiceDataVolume      string `json:"service_data_volume"`
			ServiceDataOwnerRecord string `json:"service_data_owner_record"`
		}
		if err := json.Unmarshal(payload, &manifest); err != nil || manifest.JobID == "" || manifest.AttemptID == "" ||
			manifest.RemovalGeneration == "" || manifest.LeaseID == "" || manifest.TaskID == "" ||
			manifest.ContainerID == "" || manifest.SnapshotID == "" || manifest.ShimID == "" ||
			manifest.CgroupID == "" || manifest.LogSegmentDirectory == "" || manifest.ServiceDataVolume == "" ||
			manifest.ServiceDataOwnerRecord == "" {
			t.Fatalf("offline removal resource manifest %d is incomplete: %s", index, payload)
		}
		for class, id := range map[string]string{
			"lease": manifest.LeaseID, "task": manifest.TaskID, "container": manifest.ContainerID,
			"snapshot": manifest.SnapshotID, "shim": manifest.ShimID, "cgroup": manifest.CgroupID,
			"log_segments": manifest.LogSegmentDirectory, "service_data": manifest.ServiceDataVolume,
			"service_data_owner_record": manifest.ServiceDataOwnerRecord,
		} {
			expectedAssertions[class+"\x00"+id] = struct{}{}
		}
	}
	if len(removal.RemovalAssertions) != len(expectedAssertions) {
		t.Fatalf("offline removal assertions did not cover every manifest row: %+v", removal.RemovalAssertions)
	}
	for _, assertion := range removal.RemovalAssertions {
		key := assertion.Class + "\x00" + assertion.ID
		if !assertion.Absent {
			t.Fatalf("offline removal assertion did not pass: %+v", assertion)
		}
		if _, ok := expectedAssertions[key]; !ok {
			t.Fatalf("offline removal asserted an unmanifested row: %+v", assertion)
		}
		delete(expectedAssertions, key)
	}
	launch := artifact.Rows["launch_daemon"]
	if !slices.Contains(launch.LaunchUnits, LaunchDaemonLabel) {
		t.Fatalf("launch receipt omitted %s: %+v", LaunchDaemonLabel, launch)
	}
	for _, unit := range artifact.Rows["no_lima_autostart"].LaunchUnits {
		if strings.HasPrefix(unit, "io.lima-vm.daemon.") || strings.HasPrefix(unit, "io.lima-vm.autostart.") {
			t.Fatalf("competing Lima autostart unit present: %s", unit)
		}
	}
	permissions := artifact.Rows["helper_install_permissions"]
	if permissions.SocketMode != "0660" || permissions.SocketOwner != "root" || permissions.SocketGroup != "wefty-oci" {
		t.Fatalf("helper permission receipt = %+v", permissions)
	}
	assertStateSequence(t, artifact.Rows["stopped_enabled_recovery"], InstanceStopped, InstanceRunning)
	disabled := artifact.Rows["stopped_disabled_no_recovery"]
	if disabled.Status == "PASS" && (disabled.OCIEnabled == nil || *disabled.OCIEnabled || !slices.Equal(disabled.LimaStates, []InstanceState{InstanceStopped})) {
		t.Fatalf("disabled recovery receipt = %+v", disabled)
	}
	assertStateSequence(t, artifact.Rows["broken_enabled_recovery"], InstanceBroken, InstanceStopped, InstanceRunning)
	degraded := artifact.Rows["process_only_degradation"]
	if !degraded.ProcessAvailable || len(degraded.CapabilityRevisions) == 0 {
		t.Fatalf("process-only degradation receipt = %+v", degraded)
	}
	doctor := artifact.Rows["minimal_doctor"].MinimalDoctor
	if doctor == nil || doctor.Version != MinimalDoctorFactsVersion || doctor.Unit.Label != LaunchDaemonLabel || doctor.Unit.State != UnitStateLaunchedByUnit ||
		doctor.CapabilityRevision <= 0 || doctor.Lima.Instance != DefaultInstanceName ||
		!doctor.Lima.State.Valid() || !doctor.Helper.State.Valid() || !doctor.Probe.State.Valid() ||
		(!doctor.ReasonCode.Valid() && doctor.ReasonCode != "") {
		t.Fatalf("minimal doctor receipt = %+v", doctor)
	}
}

func TestServiceAcceptanceAttendedArtifactRejectsMissingServiceRows(t *testing.T) {
	rows := make(map[string]attendedResult, len(requiredAttendedRows))
	for _, name := range requiredAttendedRows {
		rows[name] = attendedResult{}
	}
	for _, name := range []string{
		"service_health_echo", "service_startup_timeout", "service_withdrawal_republication",
		"service_port_collision", "service_portless_started", "service_data_guest_native", "service_removal_manifest_offline",
	} {
		t.Run(name, func(t *testing.T) {
			delete(rows, name)
			if missing := missingRequiredAttendedRow(rows); missing != name {
				t.Fatalf("missing row = %q, want %q", missing, name)
			}
			rows[name] = attendedResult{}
		})
	}
}

func assertStateSequence(t *testing.T, row attendedResult, want ...InstanceState) {
	t.Helper()
	if !slices.Equal(row.LimaStates, want) {
		t.Fatalf("Lima state sequence = %v, want %v", row.LimaStates, want)
	}
	if row.OCIEnabled == nil || !*row.OCIEnabled {
		t.Fatalf("enabled recovery row lacks enabled intent: %+v", row)
	}
}

func sameNonEmptyStrings(values []string) bool {
	if len(values) == 0 || values[0] == "" {
		return false
	}
	for _, value := range values[1:] {
		if value != values[0] {
			return false
		}
	}
	return true
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// --- M3 OCI acceptance matrix (#157): the Mac half -------------------------
//
// The attended artifact proves 34 owner-hardware rows. The matrix in spec
// section 9 is coarser: nine class rows plus a capability row plus the Mac-only
// list. This maps one onto the other and emits the fragment that
// scripts/assemble-oci-acceptance-matrix.sh merges with the Linux receipts.

const (
	// The Lima vz gateway guard that kills every OCI attempt at bridge bind.
	macMatrixGatewayIssue = 394
	// Headless cold-reboot evidence is owned by the #128 prototype.
	macMatrixHeadlessIssue = 128
	// Rows the runbook has no attended procedure for. Owned by #157 until the
	// follow-up documentation ticket is filed.
	macMatrixRunbookIssue  = 157
	macMatrixRunbookReason = "runbook_no_procedure: the runbook gives no attended procedure for holding an exclusive helper session while the dev.wefty.agent LaunchDaemon runs"
)

// attendedRowsWithoutProcedure are NOT-RUN because the runbook never describes
// how to run them, not because of any product defect. They are never attributed
// to #394.
var attendedRowsWithoutProcedure = map[string]struct{}{
	"task_logs_delete": {}, "mount_validation": {}, "host_to_guest": {},
	"helper_loss": {}, "vm_loss": {}, "sweep_before_recovery": {},
}

// macMatrixRows is the frozen Mac half of the matrix. Dependencies are listed
// dominant cause first: the first non-PASS dependency names the row's reason.
var macMatrixRows = []struct {
	ID       string
	Attended []string
}{
	{"mac.oneshot.image_identity", []string{"oci_oneshot_run", "oci_oneshot_rerun_identity"}},
	{"mac.oneshot.delivery", []string{"oci_oneshot_run", "guest_to_host_primary", "guest_to_host_fallback", "mount_validation", "task_logs_delete"}},
	{"mac.oneshot.engine_loss", []string{"oci_oneshot_prestarted_loss", "oci_oneshot_poststarted_loss", "vm_loss"}},
	{"mac.service.publication", []string{"service_health_echo", "service_startup_timeout", "service_port_collision", "service_portless_started"}},
	{"mac.service.restart", []string{"service_restart_fresh_attempt"}},
	{"mac.service.stop_start", []string{"service_stop_start_capacity", "service_withdrawal_republication"}},
	{"mac.service.data", []string{"service_data_guest_native"}},
	{"mac.service.crash_recovery", []string{"service_failed_quiescence", "helper_loss", "sweep_before_recovery"}},
	{"mac.service.removal", []string{"service_removal_manifest_offline"}},
	{"mac.node.capability_claims", []string{"probe", "minimal_doctor", "process_only_degradation"}},
	{"mac.only.stopped_vm_autostart", []string{"stopped_enabled_recovery", "broken_enabled_recovery", "stopped_disabled_no_recovery"}},
	{"mac.only.helper_socket_authorization", []string{"helper_install_permissions"}},
	{"mac.only.raw_containerd_denied", []string{"raw_containerd_denied"}},
	{"mac.only.dynamic_forwarding_disabled", []string{"dynamic_forwarding_disabled"}},
	{"mac.only.dial_attempt_port", []string{"host_to_guest"}},
	{"mac.only.template_convergence", []string{"template_permissions"}},
	{"mac.only.launch_topology", []string{"launch_daemon", "no_lima_autostart"}},
	{"mac.only.headless_reboot", nil},
}

type macMatrixRow struct {
	Status       string            `json:"status"`
	Assertions   map[string]bool   `json:"assertions"`
	Evidence     map[string]string `json:"evidence"`
	Gaps         map[string]string `json:"gaps"`
	NotRunIssue  int               `json:"not_run_issue"`
	NotRunReason string            `json:"not_run_reason"`
}

type macMatrixFragment struct {
	Version           int                     `json:"version"`
	SessionID         string                  `json:"session_id"`
	Commit            string                  `json:"commit"`
	ArtifactSHA256    string                  `json:"artifact_sha256"`
	AttendedRowCounts map[string]int          `json:"attended_row_counts"`
	Rows              map[string]macMatrixRow `json:"rows"`
}

func buildMacMatrixFragment(artifact attendedArtifact, artifactSHA256 string) macMatrixFragment {
	counts := map[string]int{"pass": 0, "fail": 0, "not_run": 0}
	for _, row := range artifact.Rows {
		switch row.Status {
		case "PASS":
			counts["pass"]++
		case "FAIL":
			counts["fail"]++
		case "NOT-RUN":
			counts["not_run"]++
		}
	}
	fragment := macMatrixFragment{
		Version: 1, SessionID: artifact.SessionID, Commit: artifact.Commit,
		ArtifactSHA256: artifactSHA256, AttendedRowCounts: counts,
		Rows: make(map[string]macMatrixRow, len(macMatrixRows)),
	}
	for _, specification := range macMatrixRows {
		fragment.Rows[specification.ID] = buildMacMatrixRow(artifact, specification.ID, specification.Attended)
	}
	return fragment
}

func buildMacMatrixRow(artifact attendedArtifact, id string, attended []string) macMatrixRow {
	row := macMatrixRow{
		Status: "PASS", Assertions: map[string]bool{}, Evidence: map[string]string{}, Gaps: map[string]string{},
	}
	if len(attended) == 0 {
		row.Status, row.NotRunIssue = "NOT-RUN", macMatrixHeadlessIssue
		row.NotRunReason = "headless cold-reboot evidence is owned by the #128 prototype and is not part of the attended transport session"
		row.Gaps["headless_reboot_evidence"] = row.NotRunReason
		return row
	}
	row.Evidence["attended_rows"] = strings.Join(attended, ",")
	var firstFailure, firstSkip string
	for _, name := range attended {
		result, ok := artifact.Rows[name]
		if !ok {
			row.Status = "MISSING"
			row.Assertions, row.NotRunReason = map[string]bool{}, "attended artifact omitted row "+name
			return row
		}
		switch result.Status {
		case "PASS":
			row.Assertions[name] = true
		case "FAIL":
			row.Assertions[name] = false
			if firstFailure == "" {
				firstFailure = name
			}
		default:
			if firstSkip == "" {
				firstSkip = name
			}
		}
	}
	switch {
	case firstFailure != "":
		row.Status = "FAIL"
		row.NotRunReason = artifact.Rows[firstFailure].Reason
		row.Evidence["attended_failed_row"] = firstFailure
		row.Evidence["attended_reason"] = artifact.Rows[firstFailure].Reason
	case firstSkip != "":
		row.Status = "NOT-RUN"
		row.Evidence["attended_skipped_row"] = firstSkip
		row.Evidence["attended_reason"] = artifact.Rows[firstSkip].Reason
		row.Gaps[firstSkip] = artifact.Rows[firstSkip].Reason
		if _, withoutProcedure := attendedRowsWithoutProcedure[firstSkip]; withoutProcedure {
			row.NotRunIssue = macMatrixRunbookIssue
			row.NotRunReason = macMatrixRunbookReason + " (" + firstSkip + ")"
		} else {
			row.NotRunIssue = macMatrixGatewayIssue
			row.NotRunReason = artifact.Rows[firstSkip].Reason
		}
	}
	if row.Status == "NOT-RUN" && strings.TrimSpace(row.NotRunReason) == "" {
		row.NotRunReason = "the attended lane recorded " + firstSkip + " as NOT-RUN without a reason"
	}
	return row
}

func writeMatrixFragment(path string, fragment macMatrixFragment) error {
	payload, err := json.MarshalIndent(fragment, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(payload, '\n'), 0o600)
}

// TestServiceAcceptanceAttendedLimaMatrixFragment folds the attended artifact
// into the Mac half of the #157 matrix. It runs only during the owner-hardware
// procedure, and unlike TestServiceAcceptanceAttendedLimaArtifact it does not
// require destination success: a red attended session must still produce an
// honest, typed fragment.
func TestServiceAcceptanceAttendedLimaMatrixFragment(t *testing.T) {
	path := os.Getenv("WEFTY_LIMA_ACCEPTANCE_ARTIFACT")
	if path == "" {
		receipt, _ := json.Marshal(map[string]string{
			"status": "NOT-RUN", "reason": "set WEFTY_LIMA_ACCEPTANCE_ARTIFACT to the redacted owner-hardware receipt from docs/acceptance/m3-lima-transport.md",
		})
		t.Skip(string(receipt))
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var artifact attendedArtifact
	if err := decoder.Decode(&artifact); err != nil {
		t.Fatal(err)
	}
	if missing := missingRequiredAttendedRow(artifact.Rows); missing != "" {
		t.Fatalf("attended artifact omitted required row %q", missing)
	}
	fragment := buildMacMatrixFragment(artifact, fmt.Sprintf("%x", sha256.Sum256(payload)))
	assertMacMatrixFragmentIsTyped(t, fragment)
	t.Logf("attended rows: %d PASS / %d FAIL / %d NOT-RUN", fragment.AttendedRowCounts["pass"],
		fragment.AttendedRowCounts["fail"], fragment.AttendedRowCounts["not_run"])
	if out := os.Getenv("WEFTY_LIMA_MATRIX_OUT"); out != "" {
		if err := writeMatrixFragment(out, fragment); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote Mac matrix fragment to %s", out)
	}
}

func assertMacMatrixFragmentIsTyped(t *testing.T, fragment macMatrixFragment) {
	t.Helper()
	if len(fragment.Rows) != len(macMatrixRows) {
		t.Fatalf("Mac matrix fragment carries %d rows, want %d", len(fragment.Rows), len(macMatrixRows))
	}
	for _, specification := range macMatrixRows {
		row, ok := fragment.Rows[specification.ID]
		if !ok {
			t.Fatalf("Mac matrix fragment omitted %s", specification.ID)
		}
		switch row.Status {
		case "PASS":
			if len(row.Assertions) == 0 || len(row.Gaps) != 0 {
				t.Fatalf("row %s claims PASS without earning it: %+v", specification.ID, row)
			}
			for name, passed := range row.Assertions {
				if !passed {
					t.Fatalf("row %s claims PASS with a false assertion %s", specification.ID, name)
				}
			}
		case "NOT-RUN":
			if row.NotRunIssue <= 0 || strings.TrimSpace(row.NotRunReason) == "" {
				t.Fatalf("row %s is an untyped skip: %+v", specification.ID, row)
			}
		case "FAIL":
			if strings.TrimSpace(row.NotRunReason) == "" {
				t.Fatalf("row %s fails without a reason", specification.ID)
			}
		default:
			t.Fatalf("row %s carries status %q", specification.ID, row.Status)
		}
	}
}

func TestAttendedMatrixFragmentMapping(t *testing.T) {
	base := func() attendedArtifact {
		rows := make(map[string]attendedResult, len(requiredAttendedRows))
		for _, name := range requiredAttendedRows {
			rows[name] = attendedResult{Status: "PASS", SessionID: "attended-session"}
		}
		return attendedArtifact{SessionID: "attended-session", Commit: strings.Repeat("a", 40), Rows: rows}
	}

	t.Run("all attended rows PASS yields a complete Mac half", func(t *testing.T) {
		fragment := buildMacMatrixFragment(base(), "sha")
		assertMacMatrixFragmentIsTyped(t, fragment)
		for _, specification := range macMatrixRows {
			want := "PASS"
			if specification.ID == "mac.only.headless_reboot" {
				want = "NOT-RUN"
			}
			if got := fragment.Rows[specification.ID].Status; got != want {
				t.Fatalf("row %s = %s, want %s", specification.ID, got, want)
			}
		}
		if fragment.Rows["mac.only.headless_reboot"].NotRunIssue != macMatrixHeadlessIssue {
			t.Fatal("headless reboot is not owned by #128")
		}
	})

	t.Run("a failed attended row fails exactly its owning matrix rows", func(t *testing.T) {
		artifact := base()
		artifact.Rows["oci_oneshot_run"] = attendedResult{Status: "FAIL", Reason: "gateway 192.168.5.2 routes through a physical interface"}
		fragment := buildMacMatrixFragment(artifact, "sha")
		assertMacMatrixFragmentIsTyped(t, fragment)
		for _, id := range []string{"mac.oneshot.image_identity", "mac.oneshot.delivery"} {
			row := fragment.Rows[id]
			if row.Status != "FAIL" || row.NotRunReason != "gateway 192.168.5.2 routes through a physical interface" ||
				row.Evidence["attended_failed_row"] != "oci_oneshot_run" {
				t.Fatalf("row %s = %+v, want a typed FAIL carrying the attended reason verbatim", id, row)
			}
		}
		if fragment.Rows["mac.service.removal"].Status != "PASS" {
			t.Fatal("an unrelated row was dragged into FAIL")
		}
	})

	t.Run("a blocked attended row is NOT-RUN on #394", func(t *testing.T) {
		artifact := base()
		artifact.Rows["service_health_echo"] = attendedResult{Status: "NOT-RUN", Reason: "blocked by the run-bridge gateway guard"}
		row := buildMacMatrixFragment(artifact, "sha").Rows["mac.service.publication"]
		if row.Status != "NOT-RUN" || row.NotRunIssue != macMatrixGatewayIssue ||
			row.NotRunReason != "blocked by the run-bridge gateway guard" {
			t.Fatalf("row = %+v, want NOT-RUN owned by #394", row)
		}
	})

	t.Run("a row the runbook cannot drive is never blamed on #394", func(t *testing.T) {
		artifact := base()
		for name := range attendedRowsWithoutProcedure {
			artifact.Rows[name] = attendedResult{Status: "NOT-RUN", Reason: "not executed: the LaunchDaemon holds the exclusive helper session"}
		}
		fragment := buildMacMatrixFragment(artifact, "sha")
		assertMacMatrixFragmentIsTyped(t, fragment)
		for _, id := range []string{"mac.oneshot.delivery", "mac.oneshot.engine_loss", "mac.service.crash_recovery", "mac.only.dial_attempt_port"} {
			row := fragment.Rows[id]
			if row.Status != "NOT-RUN" || row.NotRunIssue != macMatrixRunbookIssue ||
				!strings.HasPrefix(row.NotRunReason, "runbook_no_procedure: ") {
				t.Fatalf("row %s = %+v, want a runbook_no_procedure skip", id, row)
			}
		}
	})

	t.Run("a missing attended row is MISSING, never a silent PASS", func(t *testing.T) {
		artifact := base()
		delete(artifact.Rows, "service_data_guest_native")
		if row := buildMacMatrixFragment(artifact, "sha").Rows["mac.service.data"]; row.Status != "MISSING" {
			t.Fatalf("row = %+v, want MISSING", row)
		}
	})
}
