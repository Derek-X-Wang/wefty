package ocihelper

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestComputerMemoryAdmissionNewcomerPaysWithLowMemAvailable(t *testing.T) {
	root := t.TempDir()
	meminfo := filepath.Join(root, "meminfo")
	if err := os.WriteFile(meminfo, []byte("MemTotal: 4194304 kB\nMemAvailable: 1 kB\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{
		config:               NativeEngineConfig{RuntimeRoot: root, MemoryCapacityBytes: 4 << 30, MemoryReserveBytes: 1 << 30},
		capacityReservations: make(map[string]*capacityReservation), memoryFactsPath: meminfo,
	}
	request := func(id string) RunRequest {
		return RunRequest{
			Authority: AttemptAuthority{NodeID: "node", JobID: "job-" + id, AttemptID: "attempt-" + id, FencingToken: "fence", BootSessionID: "boot", Class: "service", RemovalGeneration: "0"},
			Workload:  WorkloadInput{Computer: true, Limits: WorkloadLimits{MemoryBytes: 1 << 30}},
		}
	}
	for _, id := range []string{"a", "b", "c"} {
		receipt, err := engine.admitResources(request(id))
		if err != nil {
			t.Fatalf("admit %s with low MemAvailable: %v", id, err)
		}
		if !receipt.Admitted || receipt.FailureCode != "" || receipt.MemAvailableBytes != 1024 || receipt.MemoryCommittedAfterBytes > 3<<30 {
			t.Fatalf("admission receipt %s = %+v", id, receipt)
		}
	}
	_, err := engine.admitResources(request("d"))
	var insufficient *insufficientMemoryError
	if !errors.As(err, &insufficient) || insufficient.RequestedBytes != 1<<30 || insufficient.ObservedAvailableBytes != 0 {
		t.Fatalf("fourth admission = %v (%+v)", err, insufficient)
	}
	if engine.lastAdmission == nil || engine.lastAdmission.Admitted || engine.lastAdmission.FailureCode != CodeInsufficientMemory || engine.lastAdmission.MemoryCommittedAfterBytes != 3<<30 {
		t.Fatalf("refused newcomer receipt = %+v", engine.lastAdmission)
	}
	if len(engine.capacityReservations) != 3 {
		t.Fatalf("refused newcomer changed resident reservations: %+v", engine.capacityReservations)
	}
	engine.releaseCapacityReservation(request("a").Authority.key())
	if _, err := engine.admitResources(request("d")); err != nil {
		t.Fatalf("explicitly freed cap did not admit newcomer: %v", err)
	}
}

func TestEngineStartupDoesNotDeriveOrRejectConfiguredCapacity(t *testing.T) {
	for _, config := range []NativeEngineConfig{
		{RuntimeRoot: t.TempDir(), Address: filepath.Join(t.TempDir(), "containerd.sock")},
		{RuntimeRoot: t.TempDir(), Address: filepath.Join(t.TempDir(), "containerd.sock"), MemoryCapacityBytes: 1 << 30, MemoryReserveBytes: 1 << 30},
	} {
		engine, err := NewContainerdEngine(config)
		if err != nil {
			t.Fatalf("configured capacity prevented helper startup: %v", err)
		}
		if engine.config.MemoryCapacityBytes != config.MemoryCapacityBytes || engine.config.MemoryReserveBytes != config.MemoryReserveBytes {
			t.Fatalf("helper derived capacity: got %d/%d want %d/%d", engine.config.MemoryCapacityBytes, engine.config.MemoryReserveBytes, config.MemoryCapacityBytes, config.MemoryReserveBytes)
		}
		if err := engine.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOneShotMemoryCapDoesNotConsumeBoundServiceCapacity(t *testing.T) {
	root := t.TempDir()
	meminfo := filepath.Join(root, "meminfo")
	if err := os.WriteFile(meminfo, []byte("MemTotal: 4194304 kB\nMemAvailable: 1 kB\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{
		config:               NativeEngineConfig{RuntimeRoot: root, MemoryCapacityBytes: 4 << 30, MemoryReserveBytes: 1 << 30},
		capacityReservations: make(map[string]*capacityReservation), memoryFactsPath: meminfo,
	}
	request := RunRequest{
		Authority: AttemptAuthority{NodeID: "node", JobID: "job", AttemptID: "attempt", FencingToken: "fence", BootSessionID: "boot", Class: "one-shot", RemovalGeneration: "attempt"},
		Workload:  WorkloadInput{Limits: WorkloadLimits{MemoryBytes: 4 << 30}},
	}
	receipt, err := engine.admitResources(request)
	if err != nil || receipt.RequestedMemoryBytes != 0 || receipt.MemoryCommittedAfterBytes != 0 || len(engine.capacityReservations) != 0 {
		t.Fatalf("one-shot admission = %+v reservations=%+v err=%v", receipt, engine.capacityReservations, err)
	}
}

func TestOrdinaryOCIServiceDoesNotConsumeComputerCapacity(t *testing.T) {
	root := t.TempDir()
	meminfo := filepath.Join(root, "meminfo")
	if err := os.WriteFile(meminfo, []byte("MemTotal: 4194304 kB\nMemAvailable: 1 kB\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{
		config:               NativeEngineConfig{RuntimeRoot: root, MemoryCapacityBytes: 4 << 30, MemoryReserveBytes: 1 << 30},
		capacityReservations: make(map[string]*capacityReservation), memoryFactsPath: meminfo,
	}
	request := RunRequest{
		Authority: AttemptAuthority{NodeID: "node", JobID: "ordinary-service", AttemptID: "attempt", FencingToken: "fence", BootSessionID: "boot", Class: "service", RemovalGeneration: "0"},
		Workload:  WorkloadInput{Limits: WorkloadLimits{MemoryBytes: 4 << 30}},
	}
	receipt, err := engine.admitResources(request)
	if err != nil || !receipt.Admitted || receipt.RequestedMemoryBytes != 0 || receipt.RequestedDiskBytes != 0 || len(engine.capacityReservations) != 0 {
		t.Fatalf("ordinary service admission = %+v reservations=%+v err=%v", receipt, engine.capacityReservations, err)
	}
}

func TestComputerRestartReusesJobReservationUntilLastAttemptDelete(t *testing.T) {
	root := t.TempDir()
	meminfo := filepath.Join(root, "meminfo")
	if err := os.WriteFile(meminfo, []byte("MemTotal: 4194304 kB\nMemAvailable: 2097152 kB\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{
		config:               NativeEngineConfig{RuntimeRoot: root, MemoryCapacityBytes: 2 << 30, MemoryReserveBytes: 1 << 30},
		capacityReservations: make(map[string]*capacityReservation), memoryFactsPath: meminfo,
	}
	request := func(attemptID string) RunRequest {
		return RunRequest{
			Authority: AttemptAuthority{NodeID: "node", JobID: "computer-job", AttemptID: attemptID, FencingToken: "fence-" + attemptID, BootSessionID: "boot", Class: "service", RemovalGeneration: "0"},
			Workload:  WorkloadInput{Computer: true, Limits: WorkloadLimits{MemoryBytes: 1 << 30}},
		}
	}
	first := request("attempt-a")
	if _, err := engine.admitResources(first); err != nil {
		t.Fatal(err)
	}
	second := request("attempt-b")
	receipt, err := engine.admitResources(second)
	if err != nil || receipt.MemoryCommittedBeforeBytes != 1<<30 || receipt.MemoryCommittedAfterBytes != 1<<30 || len(engine.capacityReservations) != 1 {
		t.Fatalf("restart reservation = %+v reservations=%+v err=%v", receipt, engine.capacityReservations, err)
	}
	engine.releaseCapacityReservation(first.Authority.key())
	if len(engine.capacityReservations) != 1 {
		t.Fatalf("old-attempt delete released live restart reservation: %+v", engine.capacityReservations)
	}
	engine.releaseCapacityReservation(second.Authority.key())
	if len(engine.capacityReservations) != 0 {
		t.Fatalf("last-attempt delete retained reservation: %+v", engine.capacityReservations)
	}
}

func TestJobUnbindingAndVerifiedBootSweepReleaseReservations(t *testing.T) {
	engine := &ContainerdEngine{capacityReservations: map[string]*capacityReservation{
		"job-a": {memoryBytes: 1 << 30, attempts: map[string]struct{}{"attempt-a": {}}},
		"job-b": {memoryBytes: 1 << 30, attempts: map[string]struct{}{"attempt-b": {}}},
	}}
	if err := engine.ReleaseImagePin(t.Context(), ReleaseImagePinRequest{JobID: "job-a"}); err != nil {
		t.Fatal(err)
	}
	if _, exists := engine.capacityReservations["job-a"]; exists || len(engine.capacityReservations) != 1 {
		t.Fatalf("job unbinding did not release exactly its reservation: %+v", engine.capacityReservations)
	}
	engine.releaseVerifiedNamespace()
	if len(engine.capacityReservations) != 0 {
		t.Fatalf("verified boot sweep retained reservations: %+v", engine.capacityReservations)
	}
}

func TestPreexistingFullyAllocatedDiskRejoinsJobReservation(t *testing.T) {
	root := t.TempDir()
	meminfo := filepath.Join(root, "meminfo")
	if err := os.WriteFile(meminfo, []byte("MemTotal: 4194304 kB\nMemAvailable: 2097152 kB\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	storage := ComputerStorageReference{ComputerID: "computer", StorageID: "storage", StorageGeneration: 1, IntentRevision: 1, DiskBytes: 1 << 20}
	name, err := DeterministicComputerDiskName(storage)
	if err != nil {
		t.Fatal(err)
	}
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	image, err := os.OpenFile(filepath.Join(diskRoot, "disk.ext4"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	allocateErr := unix.Fallocate(int(image.Fd()), 0, 0, storage.DiskBytes)
	closeErr := image.Close()
	if allocateErr != nil || closeErr != nil {
		t.Fatal(errors.Join(allocateErr, closeErr))
	}
	engine := &ContainerdEngine{
		config:               NativeEngineConfig{RuntimeRoot: root, MemoryCapacityBytes: 4 << 30, MemoryReserveBytes: 1 << 30},
		capacityReservations: make(map[string]*capacityReservation), memoryFactsPath: meminfo,
	}
	receipt, err := engine.admitResources(RunRequest{
		Authority: AttemptAuthority{NodeID: "node", JobID: "job", AttemptID: "attempt", FencingToken: "fence", BootSessionID: "boot", Class: "service", RemovalGeneration: "0"},
		Workload: WorkloadInput{Computer: true, Limits: WorkloadLimits{MemoryBytes: 1 << 30}, ManagedVolumes: []ManagedVolumeDescriptor{{
			Kind: ManagedVolumeComputerDisk, ComputerStorage: &storage,
		}}},
	})
	reservation := engine.capacityReservations["job"]
	if err != nil || receipt.DiskCommittedAfterBytes != storage.DiskBytes || reservation == nil || !reservation.diskMaterialized {
		t.Fatalf("rejoined materialized reservation = receipt %+v reservation %+v err=%v", receipt, reservation, err)
	}
}

func TestDiskRefusalIsAtomicWithMemoryAndUsesDiskRootFilesystem(t *testing.T) {
	root := t.TempDir()
	meminfo := filepath.Join(root, "meminfo")
	if err := os.WriteFile(meminfo, []byte("MemTotal: 4194304 kB\nMemAvailable: 2097152 kB\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{
		config:               NativeEngineConfig{RuntimeRoot: root, MemoryCapacityBytes: 4 << 30, MemoryReserveBytes: 1 << 30},
		capacityReservations: make(map[string]*capacityReservation), memoryFactsPath: meminfo,
	}
	request := RunRequest{
		Authority: AttemptAuthority{NodeID: "node", JobID: "too-large", AttemptID: "attempt", FencingToken: "fence", BootSessionID: "boot", Class: "service", RemovalGeneration: "0"},
		Workload: WorkloadInput{Computer: true, Limits: WorkloadLimits{MemoryBytes: 1 << 30}, ManagedVolumes: []ManagedVolumeDescriptor{{
			Kind: ManagedVolumeComputerDisk, ComputerStorage: &ComputerStorageReference{ComputerID: "computer", StorageID: "storage", StorageGeneration: 1, IntentRevision: 1, DiskBytes: int64(^uint64(0) >> 1)},
		}}},
	}
	receipt, err := engine.admitResources(request)
	var insufficient *insufficientDiskError
	if !errors.As(err, &insufficient) || receipt.FailureCode != CodeInsufficientDisk || receipt.MemoryCommittedAfterBytes != receipt.MemoryCommittedBeforeBytes || receipt.DiskCommittedAfterBytes != receipt.DiskCommittedBeforeBytes || len(receipt.Warnings) != 1 || len(engine.capacityReservations) != 0 {
		t.Fatalf("atomic disk refusal = %+v reservations=%+v err=%v", receipt, engine.capacityReservations, err)
	}
	if receipt.FilesystemAvailableBytes <= 0 {
		t.Fatalf("disk-root filesystem advisory fact = %d", receipt.FilesystemAvailableBytes)
	}
}

func TestAdmissionReceiptUsesInjectedClockAndCarriesTmpfsWarnings(t *testing.T) {
	root := t.TempDir()
	meminfo := filepath.Join(root, "meminfo")
	if err := os.WriteFile(meminfo, []byte("MemTotal: 4194304 kB\nMemAvailable: 2097152 kB\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	clock := newManualClock(time.Date(2026, 8, 28, 12, 34, 0, 0, time.UTC))
	engine := &ContainerdEngine{
		config:               NativeEngineConfig{RuntimeRoot: root, MemoryCapacityBytes: 4 << 30, MemoryReserveBytes: 1 << 30, Clock: clock},
		capacityReservations: make(map[string]*capacityReservation), memoryFactsPath: meminfo,
	}
	receipt, err := engine.admitResources(RunRequest{
		Authority: AttemptAuthority{NodeID: "node", JobID: "computer", AttemptID: "attempt", FencingToken: "fence", BootSessionID: "boot", Class: "service", RemovalGeneration: "0"},
		Workload:  WorkloadInput{Computer: true, Limits: WorkloadLimits{MemoryBytes: 512 << 20}},
	})
	if err != nil || !receipt.ObservedAt.Equal(clock.Now()) || len(receipt.Warnings) != 2 {
		t.Fatalf("clocked warning receipt = %+v err=%v", receipt, err)
	}
}

func TestMemoryAdmissionFactsFailClosedWhenARequiredRowDidNotRun(t *testing.T) {
	for _, payload := range []string{
		"MemTotal: 4194304 kB\n",
		"MemAvailable: 1 kB\n",
		"MemTotal: 4194304 kB\nMemAvailable: unknown kB\n",
	} {
		path := filepath.Join(t.TempDir(), "meminfo")
		if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readMemoryFacts(path); err == nil {
			t.Fatalf("incomplete admission facts passed: %q", payload)
		}
	}
	path := filepath.Join(t.TempDir(), "meminfo")
	if err := os.WriteFile(path, []byte("Bogus: unknown kB\nMemTotal: 4194304 kB\nMemAvailable: 1 kB\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readMemoryFacts(path); err != nil {
		t.Fatalf("unrelated malformed meminfo row failed required facts: %v", err)
	}
}

func TestComputerCgroupMemoryPolicyRequiresExactReadback(t *testing.T) {
	root := t.TempDir()
	cgroupID := "computer"
	cgroup := filepath.Join(root, cgroupID)
	if err := os.Mkdir(cgroup, 0o700); err != nil {
		t.Fatal(err)
	}
	write := func(name, value string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(cgroup, name), []byte(value+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("memory.max", "1073741824")
	write("memory.oom.group", "1")
	write("memory.swap.max", "0")
	max, group, swap, err := verifyComputerCgroupMemoryPolicy(root, cgroupID, 1<<30)
	if err != nil || max != 1<<30 || !group || swap != 0 {
		t.Fatalf("exact policy = %d/%t/%d err=%v", max, group, swap, err)
	}
	for _, mutation := range []struct{ name, value string }{
		{name: "memory.max", value: "1073741823"},
		{name: "memory.oom.group", value: "0"},
		{name: "memory.swap.max", value: "1"},
	} {
		write(mutation.name, mutation.value)
		if _, _, _, err := verifyComputerCgroupMemoryPolicy(root, cgroupID, 1<<30); err == nil {
			t.Fatalf("mutation %s=%s passed", mutation.name, mutation.value)
		}
		write("memory.max", "1073741824")
		write("memory.oom.group", "1")
		write("memory.swap.max", "0")
	}
}

func TestComputerProfileRequestsWholeCgroupOOM(t *testing.T) {
	resources, err := isolationResources(WorkloadLimits{MemoryBytes: 1 << 30}, true)
	if err != nil {
		t.Fatal(err)
	}
	if resources.Unified["memory.oom.group"] != "1" || resources.Memory == nil || resources.Memory.Limit == nil || resources.Memory.Swap == nil {
		t.Fatalf("Computer memory resources = %+v", resources)
	}
	ordinary, err := isolationResources(WorkloadLimits{MemoryBytes: 1 << 30}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(ordinary.Unified) != 0 {
		t.Fatalf("ordinary OCI inherited Computer OOM policy: %+v", ordinary.Unified)
	}
}
