package ocihelper

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

type memoryFacts struct {
	total     int64
	available int64
}

type capacityReservation struct {
	memoryBytes      int64
	diskBytes        int64
	diskMaterialized bool
	attempts         map[string]struct{}
}

func readMemoryFacts(path string) (memoryFacts, error) {
	file, err := os.Open(path)
	if err != nil {
		return memoryFacts{}, err
	}
	defer file.Close()
	facts := memoryFacts{}
	seenTotal, seenAvailable := false, false
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 3 || fields[2] != "kB" {
			continue
		}
		switch fields[0] {
		case "MemTotal:":
			value, parseErr := strconv.ParseInt(fields[1], 10, 64)
			if parseErr != nil || value <= 0 || value > int64(^uint64(0)>>1)/1024 {
				return memoryFacts{}, errors.New("invalid MemTotal byte fact")
			}
			facts.total = value * 1024
			seenTotal = true
		case "MemAvailable:":
			value, parseErr := strconv.ParseInt(fields[1], 10, 64)
			if parseErr != nil || value < 0 || value > int64(^uint64(0)>>1)/1024 {
				return memoryFacts{}, errors.New("invalid MemAvailable byte fact")
			}
			facts.available = value * 1024
			seenAvailable = true
		}
	}
	if err := scanner.Err(); err != nil {
		return memoryFacts{}, err
	}
	if !seenTotal || !seenAvailable || facts.total <= 0 || facts.available < 0 {
		return memoryFacts{}, errors.New("/proc/meminfo omitted bounded memory facts")
	}
	return facts, nil
}

func filesystemAvailableBytes(path string) (int64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, err
	}
	if stat.Bsize <= 0 || stat.Bavail > uint64(int64(^uint64(0)>>1)/stat.Bsize) {
		return 0, errors.New("filesystem available-byte fact overflows int64")
	}
	return int64(stat.Bavail) * stat.Bsize, nil
}

func verifyComputerCgroupMemoryPolicy(cgroupRoot, cgroupID string, memoryBytes int64) (int64, bool, int64, error) {
	if memoryBytes <= 0 {
		return 0, false, 0, errors.New("Computer cgroup memory cap must be positive")
	}
	readInt := func(name string) (int64, error) {
		payload, err := os.ReadFile(cgroupRoot + "/" + cgroupID + "/" + name)
		if err != nil {
			return 0, err
		}
		value, err := strconv.ParseInt(strings.TrimSpace(string(payload)), 10, 64)
		if err != nil || value < 0 {
			return 0, fmt.Errorf("%s did not contain a bounded integer", name)
		}
		return value, nil
	}
	memoryMax, err := readInt("memory.max")
	if err != nil {
		return 0, false, 0, err
	}
	oomGroup, err := readInt("memory.oom.group")
	if err != nil {
		return 0, false, 0, err
	}
	swapMax, err := readInt("memory.swap.max")
	if err != nil {
		return 0, false, 0, err
	}
	if memoryMax != memoryBytes || oomGroup != 1 || swapMax != 0 {
		return memoryMax, oomGroup == 1, swapMax, errors.New("Computer cgroup memory policy readback did not match the requested cap")
	}
	return memoryMax, true, swapMax, nil
}

func requestedComputerDiskBytes(workload WorkloadInput) int64 {
	for _, volume := range workload.ManagedVolumes {
		if volume.Kind == ManagedVolumeComputerDisk && volume.ComputerStorage != nil {
			return volume.ComputerStorage.DiskBytes
		}
	}
	return 0
}

func materializedComputerDisk(workload WorkloadInput, diskRoot string) bool {
	for _, volume := range workload.ManagedVolumes {
		if volume.Kind != ManagedVolumeComputerDisk || volume.ComputerStorage == nil {
			continue
		}
		name, err := DeterministicComputerDiskName(*volume.ComputerStorage)
		if err != nil {
			return false
		}
		return verifyComputerDiskAllocation(filepath.Join(diskRoot, name, "disk.ext4"), volume.ComputerStorage.DiskBytes) == nil
	}
	return false
}

func (engine *ContainerdEngine) admitResources(request RunRequest) (ResourceAdmissionReceipt, error) {
	clock := engine.config.Clock
	if clock == nil {
		clock = systemClock{}
	}
	if !request.Workload.Computer {
		return ResourceAdmissionReceipt{ObservedAt: clock.Now().UTC().Round(0), Admitted: true, Warnings: []ProfileWarning{}}, nil
	}
	memoryFactsPath := engine.memoryFactsPath
	if memoryFactsPath == "" {
		memoryFactsPath = "/proc/meminfo"
	}
	facts, err := readMemoryFacts(memoryFactsPath)
	if err != nil {
		return ResourceAdmissionReceipt{}, fmt.Errorf("read memory admission facts: %w", err)
	}
	requestedMemory := request.Workload.Limits.MemoryBytes
	requestedDisk := requestedComputerDiskBytes(request.Workload)
	tmpfsCeiling := int64(computerDevShmKilobytes+computerTmpKilobytes+computerVarTmpKilobytes) * 1024
	diskRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disks")
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		return ResourceAdmissionReceipt{}, fmt.Errorf("prepare Computer disk admission root: %w", err)
	}
	engine.capacityMu.Lock()
	defer engine.capacityMu.Unlock()
	if engine.capacityReservations == nil {
		engine.capacityReservations = make(map[string]*capacityReservation)
	}
	filesystemAvailable, err := filesystemAvailableBytes(diskRoot)
	if err != nil {
		return ResourceAdmissionReceipt{}, fmt.Errorf("read Computer disk admission facts: %w", err)
	}
	memoryCommitted, diskCommitted, diskPending := int64(0), int64(0), int64(0)
	for _, reservation := range engine.capacityReservations {
		if reservation.memoryBytes > int64(^uint64(0)>>1)-memoryCommitted || reservation.diskBytes > int64(^uint64(0)>>1)-diskCommitted ||
			!reservation.diskMaterialized && reservation.diskBytes > int64(^uint64(0)>>1)-diskPending {
			return ResourceAdmissionReceipt{}, errors.New("resource capacity accounting overflowed")
		}
		memoryCommitted += reservation.memoryBytes
		diskCommitted += reservation.diskBytes
		if !reservation.diskMaterialized {
			diskPending += reservation.diskBytes
		}
	}
	warnings := append([]ProfileWarning{}, profileReceipt(request.Workload).Warnings...)
	receipt := ResourceAdmissionReceipt{
		ObservedAt: clock.Now().UTC().Round(0), MemoryCapacityBytes: engine.config.MemoryCapacityBytes,
		MemoryReserveBytes: engine.config.MemoryReserveBytes, MemoryCommittedBeforeBytes: memoryCommitted,
		RequestedMemoryBytes: requestedMemory, MemoryCommittedAfterBytes: memoryCommitted,
		DiskCommittedBeforeBytes: diskCommitted, DiskCommittedAfterBytes: diskCommitted,
		MemTotalBytes: facts.total, MemAvailableBytes: facts.available,
		RequestedDiskBytes: requestedDisk, FilesystemAvailableBytes: filesystemAvailable,
		ComputerTmpfsCeilingBytes: tmpfsCeiling, Warnings: warnings,
	}
	jobKey, attemptKey := request.Authority.JobID, request.Authority.key()
	existing := engine.capacityReservations[jobKey]
	additionalMemory, additionalDisk := requestedMemory, requestedDisk
	diskAlreadyMaterialized := materializedComputerDisk(request.Workload, diskRoot)
	if existing != nil {
		if existing.memoryBytes != requestedMemory || existing.diskBytes != requestedDisk {
			return ResourceAdmissionReceipt{}, errors.New("bound Computer changed its reserved resource declaration")
		}
		if _, duplicate := existing.attempts[attemptKey]; duplicate {
			return ResourceAdmissionReceipt{}, errors.New("attempt already holds a Computer resource reservation")
		}
		additionalMemory, additionalDisk = 0, 0
	}
	diskGateBytes := additionalDisk
	if existing == nil && diskAlreadyMaterialized {
		diskGateBytes = 0
	}
	usable := int64(0)
	if engine.config.MemoryCapacityBytes > engine.config.MemoryReserveBytes {
		usable = engine.config.MemoryCapacityBytes - engine.config.MemoryReserveBytes
	}
	memoryAvailable := usable - memoryCommitted
	if memoryAvailable < 0 {
		memoryAvailable = 0
	}
	if engine.config.MemoryCapacityBytes > 0 && additionalMemory > memoryAvailable {
		receipt.FailureCode = CodeInsufficientMemory
		record := receipt
		engine.lastAdmission = &record
		return receipt, &insufficientMemoryError{RequestedBytes: requestedMemory, ObservedAvailableBytes: memoryAvailable}
	}
	diskAvailable := filesystemAvailable - diskPending
	if diskAvailable < 0 {
		diskAvailable = 0
	}
	if diskGateBytes > diskAvailable {
		receipt.FailureCode = CodeInsufficientDisk
		record := receipt
		engine.lastAdmission = &record
		return receipt, &insufficientDiskError{RequestedBytes: requestedDisk, ObservedAvailableBytes: diskAvailable}
	}
	if existing == nil {
		existing = &capacityReservation{
			memoryBytes: requestedMemory, diskBytes: requestedDisk,
			diskMaterialized: diskAlreadyMaterialized, attempts: make(map[string]struct{}),
		}
		engine.capacityReservations[jobKey] = existing
	}
	existing.attempts[attemptKey] = struct{}{}
	receipt.MemoryCommittedAfterBytes += additionalMemory
	receipt.DiskCommittedAfterBytes += additionalDisk
	receipt.Admitted = true
	record := receipt
	engine.lastAdmission = &record
	return receipt, nil
}

func (engine *ContainerdEngine) markCapacityDiskMaterialized(jobID string) {
	engine.capacityMu.Lock()
	if reservation := engine.capacityReservations[jobID]; reservation != nil {
		reservation.diskMaterialized = true
	}
	engine.capacityMu.Unlock()
}

func (engine *ContainerdEngine) recordAdmissionFailure(receipt ResourceAdmissionReceipt, code ErrorCode) {
	receipt.Admitted = false
	receipt.FailureCode = code
	engine.capacityMu.Lock()
	record := receipt
	engine.lastAdmission = &record
	engine.capacityMu.Unlock()
}

func (engine *ContainerdEngine) releaseCapacityReservation(attemptKey string) {
	engine.capacityMu.Lock()
	for jobID, reservation := range engine.capacityReservations {
		if _, present := reservation.attempts[attemptKey]; !present {
			continue
		}
		delete(reservation.attempts, attemptKey)
		if len(reservation.attempts) == 0 {
			delete(engine.capacityReservations, jobID)
		}
		break
	}
	engine.capacityMu.Unlock()
}

func (engine *ContainerdEngine) releaseJobCapacityReservation(jobID string) {
	engine.capacityMu.Lock()
	delete(engine.capacityReservations, jobID)
	engine.capacityMu.Unlock()
}

func (engine *ContainerdEngine) clearCapacityReservations() {
	engine.capacityMu.Lock()
	engine.capacityReservations = make(map[string]*capacityReservation)
	engine.capacityMu.Unlock()
}
