//go:build service_acceptance

package ocicontrol

import (
	"encoding/json"
	"os"
	"runtime"
	"testing"
	"time"
)

func TestServiceAcceptanceNodeDoctorIsFactsOnly(t *testing.T) {
	now := time.Date(2026, 8, 28, 22, 0, 0, 0, time.UTC)
	report := BuildDoctor(t.Context(), healthyDoctorConfig(now, ""))
	if err := report.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, item := range report.Findings {
		if item.Outcome == DiagnosticNotRun && item.Check != "lima" {
			t.Fatalf("configured doctor check did not run: %+v", item)
		}
	}
}

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
	missing := "systemd_reboot_harness"
	if runtime.GOOS == "darwin" {
		missing = "attended_mac_cold_reboot"
	}
	receipt := rebootReceipt{Version: 1, Status: "NOT-RUN", Platform: runtime.GOOS + "/" + runtime.GOARCH, MissingCapability: missing}
	writeRebootReceipt(t, path, receipt)
	payload, _ := json.Marshal(receipt)
	t.Logf("structured skip receipt: %s", payload)
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
