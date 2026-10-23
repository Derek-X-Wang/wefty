//go:build linux

package ocihelper

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	containerd "github.com/containerd/containerd/v2/client"
)

type memoryStatusTask struct {
	containerd.Task
	status  containerd.ProcessStatus
	failure error
}

func (t memoryStatusTask) Status(context.Context) (containerd.Status, error) {
	return containerd.Status{Status: t.status}, t.failure
}
func TestComputerMemoryLimitEngineRefusesEndedAttemptsButPreservesReadFailure(t *testing.T) {
	for _, tc := range []struct {
		name             string
		present, deleted bool
		status           containerd.ProcessStatus
		failure          error
		refused          bool
	}{
		{name: "foreign", refused: true},
		{name: "deleted", present: true, deleted: true, refused: true},
		{name: "stopped", present: true, status: containerd.Stopped, refused: true},
		{name: "unreadable runtime", present: true, failure: errors.New("runtime read failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			authority := testAuthority()
			engine := &ContainerdEngine{attempts: map[string]*containerdAttempt{}}
			if tc.present {
				engine.attempts[authority.key()] = &containerdAttempt{authority: authority, deleted: tc.deleted, task: memoryStatusTask{status: tc.status, failure: tc.failure}, controlDirectory: t.TempDir(), networkNamespace: &pinnedNetworkNamespace{}}
			}
			_, err := engine.SetComputerMemoryLimit(t.Context(), SetComputerMemoryLimitRequest{Authority: authority, MemoryBytes: 2 << 30})
			var refusal *SeatOperationRefusalError
			if err == nil || errors.As(err, &refusal) != tc.refused {
				t.Fatalf("refusal=%t err=%v", tc.refused, err)
			}
			if tc.refused && refusal.Reason != SeatRefusalAttemptNotLive {
				t.Fatalf("wrong refusal: %v", err)
			}
		})
	}
}

func TestComputerMemoryLimitChangesRealAdmissionReservation(t *testing.T) {
	root := t.TempDir()
	meminfo := filepath.Join(root, "meminfo")
	if err := os.WriteFile(meminfo, []byte("MemTotal: 4194304 kB\nMemAvailable: 2097152 kB\n"), 0600); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root, MemoryCapacityBytes: 4 << 30, MemoryReserveBytes: 1 << 30}, memoryFactsPath: meminfo}
	request := func(job string) RunRequest {
		a := testAuthority()
		a.JobID = job
		return RunRequest{Authority: a, Workload: WorkloadInput{Computer: true, Limits: WorkloadLimits{MemoryBytes: 1 << 30}}}
	}
	if _, err := engine.admitResources(request("resident")); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.admitResources(request("neighbour")); err != nil {
		t.Fatal(err)
	}
	reservation := engine.capacityReservations["resident"]
	if reservation.tmpfsCeilingBytes != profileReceipt(request("resident").Workload).ComputerTmpfsCeilingBytes {
		t.Fatal("creation-time tmpfs floor was not retained")
	}
	seats, runtime, _, clock := memoryLimitFixture(t)
	runtime.reserved = &reservation.memoryBytes
	available, err := engine.availableComputerMemoryLocked()
	if err != nil {
		t.Fatal(err)
	}
	grown, err := changeComputerMemoryLimit(t.Context(), SetComputerMemoryLimitRequest{MemoryBytes: 2 << 30, Generation: seats.generation}, seats, reservation.tmpfsCeilingBytes, &reservation.memoryBytes, available, runtime, clock, func() error { return nil })
	if err != nil || grown.ReservedMemoryBytes != 2<<30 {
		t.Fatalf("grow: %+v %v", grown, err)
	}
	restart := request("resident")
	restart.Authority.AttemptID = "next-attempt"
	restart.Workload.Limits.MemoryBytes = 2 << 30
	if _, err := engine.admitResources(restart); err != nil {
		t.Fatal(err)
	}
	if reservation.tmpfsCeilingBytes != profileReceipt(restart.Workload).ComputerTmpfsCeilingBytes {
		t.Fatal("new attempt's creation-time scratch ceiling was lost")
	}
	receipt, err := engine.admitResources(request("newcomer"))
	var insufficient *insufficientMemoryError
	if !errors.As(err, &insufficient) || receipt.MemoryCommittedBeforeBytes != 3<<30 || insufficient.ObservedAvailableBytes != 0 {
		t.Fatalf("newcomer did not see the live reservation: %+v %v", receipt, err)
	}
}
