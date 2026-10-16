package nodeload

import (
	"encoding/binary"
	"fmt"

	"golang.org/x/sys/unix"
)

func Read() (float64, error) {
	raw, err := unix.SysctlRaw("vm.loadavg")
	if err != nil {
		return 0, err
	}
	return parseLoadavg(raw)
}

func parseLoadavg(raw []byte) (float64, error) {
	// Darwin's struct loadavg has three uint32 fixed-point values, padding,
	// then a 64-bit long scale on both supported Darwin architectures.
	if len(raw) != 24 {
		return 0, fmt.Errorf("vm.loadavg: unexpected size %d", len(raw))
	}
	scale := int64(binary.NativeEndian.Uint64(raw[16:24]))
	if scale <= 0 {
		return 0, fmt.Errorf("vm.loadavg: invalid scale %d", scale)
	}
	return float64(binary.NativeEndian.Uint32(raw[:4])) / float64(scale), nil
}
