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
