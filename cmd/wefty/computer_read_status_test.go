package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestComputerTableUsesComputedStatus(t *testing.T) {
	for _, status := range []string{"restart-pending", "unschedulable"} {
		var out bytes.Buffer
		computer := l1.Computer{ComputerID: "computed", CurrentJob: l1.Job{State: contract.JobQueued, Status: status}}
		if err := writeStorageComputer(&out, computer); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), status) || strings.Contains(out.String(), "queued") {
			t.Fatalf("table=%s", out.String())
		}
	}
}
