//go:build linux

package ocihelper

import (
	"context"
	"errors"
	"fmt"
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
	if err := reservation.claimMutableMemory(request("resident").Authority.key()); err != nil {
		t.Fatal(err)
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
	// Neither the original nor the grown declaration may overlap the exact
	// cgroup that owns the mutable reservation.
	for _, size := range []int64{1 << 30, 2 << 30} {
		restart.Workload.Limits.MemoryBytes = size
		var refusal *SeatOperationRefusalError
		if _, err := engine.admitResources(restart); !errors.As(err, &refusal) || refusal.Reason != SeatRefusalAttemptOverlap {
			t.Fatalf("overlap size=%d: %v", size, err)
		}
	}
	receipt, err := engine.admitResources(request("newcomer"))
	var insufficient *insufficientMemoryError
	if !errors.As(err, &insufficient) || receipt.MemoryCommittedBeforeBytes != 3<<30 || insufficient.ObservedAvailableBytes != 0 {
		t.Fatalf("newcomer did not see the live reservation: %+v %v", receipt, err)
	}
	// Production calls this only after verified Attempt cleanup (or rollback
	// before runtime creation). The replacement gets its original declaration.
	engine.releaseCapacityReservation(request("resident").Authority.key())
	restart.Workload.Limits.MemoryBytes = 1 << 30
	if _, err := engine.admitResources(restart); err != nil {
		t.Fatal(err)
	}
	replacement := engine.capacityReservations["resident"]
	if replacement.memoryBytes != 1<<30 || replacement.memoryOwner != "" || replacement.tmpfsCeilingBytes != profileReceipt(restart.Workload).ComputerTmpfsCeilingBytes {
		t.Fatalf("replacement inherited grown reservation: %+v", replacement)
	}
}

// setHighMargin writes the creation-time margin of the confirmed limit and
// accepts only that exact readback (#1579). A plain directory stands in for
// the cgroup: this proves the helper's write and readback, not kernel reclaim.
func TestComputerCgroupMemorySetHighMarginReadsBackExactBytes(t *testing.T) {
	for _, tc := range []struct {
		name, file string
		ok         bool
	}{
		{name: "written", ok: true},
		{name: "missing file", file: "-"},
		{name: "file is a directory", file: "/"},
		// The write overwrites only the first 10 bytes, so the readback is a
		// valid integer that is not the margin.
		{name: "kernel kept another value", file: "99999999999"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "memory.high")
			switch tc.file {
			case "":
				if err := os.WriteFile(path, nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "/":
				// A directory accepts no write and reads back no integer.
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			case "-":
			default:
				if err := os.WriteFile(path, []byte(tc.file), 0600); err != nil {
					t.Fatal(err)
				}
			}
			directory, err := os.Open(root)
			if err != nil {
				t.Fatal(err)
			}
			defer directory.Close()
			m := &computerCgroupMemory{directory: directory, followsLimit: true}
			err = m.setHighMargin(2 << 30)
			if tc.ok != (err == nil) {
				t.Fatalf("setHighMargin: %v", err)
			}
			if tc.ok {
				b, _ := os.ReadFile(path)
				if want := fmt.Sprint(computerMemoryHighBytes(2 << 30)); string(b) != want {
					t.Fatalf("memory.high=%q want %q", b, want)
				}
			}
		})
	}
}
