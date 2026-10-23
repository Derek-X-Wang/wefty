package ocihelper

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"
)

func TestComputerSeatMemoryRecoveryRejectsTmpfsLeakDespiteDesktopDrop(t *testing.T) {
	const mib = int64(1 << 20)
	recovery := seatMemoryRecovery{baseline: 32 * mib, tolerance: 32 * mib, required: 5}
	// A 333 MiB desktop disappeared, but the 128 MiB tmpfs fixture leaked.
	// The former memory.current drop oracle would accept this result.
	for i := 0; i < 10; i++ {
		if recovery.observe(160 * mib) {
			t.Fatal("desktop drop hid the leaked tmpfs file")
		}
	}
	for i := 0; i < 4; i++ {
		if recovery.observe(40 * mib) {
			t.Fatal("accepted fewer than five samples")
		}
	}
	if recovery.observe(160 * mib) {
		t.Fatal("leak spike accepted")
	}
	for i := 0; i < 4; i++ {
		if recovery.observe(40 * mib) {
			t.Fatal("spike did not reset consecutive samples")
		}
	}
	if !recovery.observe(40 * mib) {
		t.Fatal("stable return to baseline was not accepted")
	}
}
func TestComputerSeatMemoryChargeUsesAnonAndShmemOnly(t *testing.T) {
	got, err := seatMemoryCharge([]byte("anon 1048576\nfile 999999999\nshmem 2097152\nfile_mapped 888888\n"))
	if err != nil || got != 3145728 {
		t.Fatalf("memory charge = %d, %v", got, err)
	}
	for _, payload := range []string{"", "anon 1\n", "shmem 1\n", "anon -1\nshmem 1\n", "anon invalid\nshmem 1\n", "anon 1\nanon 1\nshmem 1\n", "anon 9223372036854775807\nshmem 1\n"} {
		if _, err := seatMemoryCharge([]byte(payload)); err == nil {
			t.Fatalf("invalid memory.stat accepted: %q", payload)
		}
	}
}

// Only anon and shmem belong in this oracle: ordinary image and Storage file
// cache can outlive a desktop without leaking Seat-owned memory.
func seatMemoryCharge(payload []byte) (int64, error) {
	values := map[string]int64{}
	for _, line := range strings.Split(string(payload), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if fields[0] != "anon" && fields[0] != "shmem" {
			continue
		}
		if len(fields) != 2 {
			return 0, errors.New("invalid memory.stat field")
		}
		if _, exists := values[fields[0]]; exists {
			return 0, errors.New("duplicate memory.stat field")
		}
		value, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil || value < 0 {
			return 0, errors.New("invalid memory.stat value")
		}
		values[fields[0]] = value
	}
	anon, a := values["anon"]
	shmem, b := values["shmem"]
	if !a || !b || anon > math.MaxInt64-shmem {
		return 0, errors.New("incomplete or overflowing memory.stat")
	}
	return anon + shmem, nil
}

type seatMemoryRecovery struct {
	baseline, tolerance   int64
	required, consecutive int
}

func (r *seatMemoryRecovery) observe(charge int64) bool {
	if charge-r.baseline <= r.tolerance {
		r.consecutive++
	} else {
		r.consecutive = 0
	}
	return r.consecutive >= r.required
}
