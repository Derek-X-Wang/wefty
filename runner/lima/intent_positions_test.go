package lima

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewIntentDefaultsOnDemandAndLegacyMapping(t *testing.T) {
	path := filepath.Join(t.TempDir(), "intent.json")
	if _, err := InitializeOCIIntent(path, time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var marker map[string]any
	if err := json.Unmarshal(payload, &marker); err != nil {
		t.Fatal(err)
	}
	if marker["position"] != "on-demand" || marker["version"] != float64(2) {
		t.Fatalf("new install marker = %s; want version 2 on-demand", payload)
	}
	for _, enabled := range []bool{false, true} {
		payload, _ := json.Marshal(map[string]any{"version": 1, "revision": 7, "enabled": enabled, "updated_at": time.Unix(1, 0)})
		if err := os.WriteFile(path, payload, 0600); err != nil {
			t.Fatal(err)
		}
		intent, err := (FileIntentSource{Path: path}).ReadIntent(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		payload, _ = json.Marshal(intent)
		marker = nil
		_ = json.Unmarshal(payload, &marker)
		want := "off"
		if enabled {
			want = "always-on"
		}
		if marker["position"] != want || intent.Revision != 7 {
			t.Fatalf("legacy marker = %s; want %s revision 7", payload, want)
		}
	}
}

func TestOCIPositionUpgradesLegacyMarkerWithoutRecordingDemand(t *testing.T) {
	path := filepath.Join(t.TempDir(), "intent.json")
	if err := os.WriteFile(path, []byte(`{"version":1,"revision":7,"enabled":true,"updated_at":"1970-01-01T00:00:01Z"}`), 0600); err != nil {
		t.Fatal(err)
	}
	intent, err := SetOCIPosition(t.Context(), path, 7, OCIPositionOnDemand, time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	if intent.Revision != 8 || intent.PositionValue() != OCIPositionOnDemand {
		t.Fatalf("upgrade=%+v", intent)
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var marker map[string]any
	if err := json.Unmarshal(payload, &marker); err != nil {
		t.Fatal(err)
	}
	if len(marker) != 5 || marker["position"] != "on-demand" {
		t.Fatalf("intent must contain only switch and revision facts: %s", payload)
	}
	// This is the version-1 reader's exact JSON field set and strict decoder.
	var legacy struct {
		Version   int       `json:"version"`
		Revision  uint64    `json:"revision"`
		Enabled   bool      `json:"enabled"`
		UpdatedAt time.Time `json:"updated_at"`
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&legacy); err == nil && legacy.Version == 1 {
		t.Fatal("old reader accepted on-demand as enabled")
	}
	again, err := SetOCIPosition(t.Context(), path, 8, OCIPositionOnDemand, time.Unix(3, 0))
	if err != nil || again.Revision != 8 {
		t.Fatalf("same switch is not idempotent: %+v %v", again, err)
	}
}
