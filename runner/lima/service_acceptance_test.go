//go:build service_acceptance && darwin

package lima

import (
	"bytes"
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
	Status              string              `json:"status"`
	Reason              string              `json:"reason,omitempty"`
	SessionID           string              `json:"session_id"`
	Command             []string            `json:"command"`
	ExitCode            int                 `json:"exit_code"`
	HelperGenerations   []uint64            `json:"helper_generations"`
	CapabilityRevisions []int64             `json:"capability_revisions"`
	Inventories         []json.RawMessage   `json:"inventories"`
	RoundTrip           bool                `json:"round_trip"`
	DynamicListeners    map[string]bool     `json:"dynamic_listeners"`
	LaunchUnits         []string            `json:"launch_units,omitempty"`
	LimaStates          []InstanceState     `json:"lima_states,omitempty"`
	OCIEnabled          *bool               `json:"oci_enabled,omitempty"`
	ProcessAvailable    bool                `json:"process_available,omitempty"`
	SocketMode          string              `json:"socket_mode,omitempty"`
	SocketOwner         string              `json:"socket_owner,omitempty"`
	SocketGroup         string              `json:"socket_group,omitempty"`
	MinimalDoctor       *MinimalDoctorFacts `json:"minimal_doctor,omitempty"`
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
	required := []string{
		"template_permissions", "probe", "task_logs_delete", "mount_validation",
		"host_to_guest", "guest_to_host_primary", "guest_to_host_fallback",
		"helper_loss", "vm_loss", "sweep_before_recovery",
		"dynamic_forwarding_disabled", "raw_containerd_denied",
		"launch_daemon", "no_lima_autostart", "helper_install_permissions",
		"stopped_enabled_recovery", "stopped_disabled_no_recovery", "broken_enabled_recovery",
		"process_only_degradation", "minimal_doctor",
	}
	for _, name := range required {
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

func assertStateSequence(t *testing.T, row attendedResult, want ...InstanceState) {
	t.Helper()
	if !slices.Equal(row.LimaStates, want) {
		t.Fatalf("Lima state sequence = %v, want %v", row.LimaStates, want)
	}
	if row.OCIEnabled == nil || !*row.OCIEnabled {
		t.Fatalf("enabled recovery row lacks enabled intent: %+v", row)
	}
}
