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
	Status              string            `json:"status"`
	SessionID           string            `json:"session_id"`
	Command             []string          `json:"command"`
	ExitCode            int               `json:"exit_code"`
	HelperGenerations   []uint64          `json:"helper_generations"`
	CapabilityRevisions []int64           `json:"capability_revisions"`
	Inventories         []json.RawMessage `json:"inventories"`
	RoundTrip           bool              `json:"round_trip"`
	DynamicListeners    map[string]bool   `json:"dynamic_listeners"`
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
	}
	for _, name := range required {
		row, ok := artifact.Rows[name]
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
}
