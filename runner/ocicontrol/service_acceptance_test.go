//go:build service_acceptance

package ocicontrol

import (
	"encoding/json"
	"os"
	"runtime"
	"testing"
)

type rebootReceipt struct {
	Version           int    `json:"version"`
	Status            string `json:"status"`
	Platform          string `json:"platform"`
	MissingCapability string `json:"missing_capability,omitempty"`
	IntentRevision    uint64 `json:"intent_revision,omitempty"`
	IntentEnabled     *bool  `json:"intent_enabled,omitempty"`
	BootBefore        string `json:"boot_before,omitempty"`
	BootAfter         string `json:"boot_after,omitempty"`
	AgentUnitActive   bool   `json:"agent_unit_active,omitempty"`
	HelperSocketMode  string `json:"helper_socket_mode,omitempty"`
}

func TestServiceAcceptanceOCIIntentRebootDurability(t *testing.T) {
	path := os.Getenv("WEFTY_OCI_INTENT_RECEIPT")
	if runtime.GOOS != "linux" || os.Getenv("WEFTY_SYSTEMD_REBOOT_ACCEPTANCE") != "1" {
		missing := "systemd_reboot"
		if runtime.GOOS == "darwin" {
			missing = "attended_mac_cold_reboot"
		}
		receipt := rebootReceipt{Version: 1, Status: "NOT-RUN", Platform: runtime.GOOS + "/" + runtime.GOARCH, MissingCapability: missing}
		writeRebootReceipt(t, path, receipt)
		payload, _ := json.Marshal(receipt)
		t.Logf("structured skip receipt: %s", payload)
		return
	}
	if path == "" {
		t.Fatal("WEFTY_OCI_INTENT_RECEIPT is required for systemd reboot acceptance")
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var receipt rebootReceipt
	if err := json.Unmarshal(payload, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Version != 1 || receipt.Status != "VERIFIED" || receipt.Platform == "" || receipt.IntentRevision == 0 ||
		receipt.IntentEnabled == nil || receipt.BootBefore == "" || receipt.BootAfter == "" || receipt.BootBefore == receipt.BootAfter ||
		!receipt.AgentUnitActive || receipt.HelperSocketMode != "0660" {
		t.Fatalf("systemd reboot receipt did not prove durable intent and units: %+v", receipt)
	}
}

func writeRebootReceipt(t *testing.T, path string, receipt rebootReceipt) {
	t.Helper()
	if path == "" {
		return
	}
	payload, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	payload = append(payload, '\n')
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
}
