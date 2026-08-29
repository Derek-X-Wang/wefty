//go:build service_acceptance

package computerconformance

import (
	"testing"
	"time"
)

func TestBrokenComputerImageRowsOwnStableFailureCells(t *testing.T) {
	rows := map[string]string{
		"no-control-endpoint":   "transport.control-ready",
		"text-frames-accepted":  "transport.text-frame-rejected",
		"driver-json-ignored":   "driver.true-consumed",
		"input-leaks-on-view":   "input.view-isolated",
		"readiness-over-60s":    "readiness.before-deadline",
		"dev-shm-too-small":     "profile.shm-size",
		"reserved-env-shadowed": "environment.view-port",
	}
	for row, owner := range rows {
		t.Run(row, func(t *testing.T) {
			recorder := NewRecorder("deliberately-broken", "docker", "linux/amd64", time.Unix(1, 0))
			for _, definition := range CheckCatalog {
				status := StatusPass
				if definition.ID == owner {
					status = StatusFail
				}
				if err := recorder.Record(definition.ID, status, row); err != nil {
					t.Fatal(err)
				}
			}
			receipt := recorder.Finish(time.Unix(2, 0))
			if receipt.Status != StatusFail {
				t.Fatalf("broken row %s produced %s", row, receipt.Status)
			}
			for _, check := range receipt.Checks {
				if check.Status == StatusFail && check.ID != owner {
					t.Fatalf("row %s leaked into %s, owner %s", row, check.ID, owner)
				}
			}
		})
	}
}
