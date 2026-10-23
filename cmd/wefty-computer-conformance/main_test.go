package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestUsageErrorExits64WithoutReceipt(t *testing.T) {
	receipt := filepath.Join(t.TempDir(), "must-not-exist.json")
	if code := run([]string{"--receipt", receipt}); code != 64 {
		t.Fatalf("usage exit = %d, want 64", code)
	}
	if _, err := os.Stat(receipt); !os.IsNotExist(err) {
		t.Fatalf("usage error emitted receipt: %v", err)
	}
}

func TestMutableRepairImageIsUsageError(t *testing.T) {
	receipt := filepath.Join(t.TempDir(), "must-not-exist.json")
	if code := run([]string{"--image", "broken:local", "--repair-image", "reference:latest", "--receipt", receipt}); code != 64 {
		t.Fatalf("mutable repair-image usage exit = %d, want 64", code)
	}
	if _, err := os.Stat(receipt); !os.IsNotExist(err) {
		t.Fatalf("usage error emitted receipt: %v", err)
	}
}

func TestSeatEvidenceModeRefusesMissingProof(t *testing.T) {
	evidence := filepath.Join(t.TempDir(), "seats.json")
	receipt := filepath.Join(t.TempDir(), "receipt.json")
	if err := os.WriteFile(evidence, []byte(`{"version":1,"image":"example.invalid/default@sha256:abc"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if code := run([]string{"--seat-evidence", evidence, "--receipt", receipt}); code != 2 {
		t.Fatalf("exit=%d, want NOT-RUN exit 2", code)
	}
	if _, err := os.Stat(receipt); err != nil {
		t.Fatal(err)
	}
}
