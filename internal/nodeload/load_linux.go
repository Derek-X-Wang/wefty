package nodeload

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
)

func Read() (float64, error) {
	raw, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0, err
	}
	return parseLoadavg(raw)
}

func parseLoadavg(raw []byte) (float64, error) {
	fields := strings.Fields(string(raw))
	if len(fields) == 0 {
		return 0, fmt.Errorf("/proc/loadavg: empty load")
	}
	load, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0, err
	}
	if load < 0 || math.IsNaN(load) || math.IsInf(load, 0) {
		return 0, fmt.Errorf("/proc/loadavg: invalid load")
	}
	return load, nil
}
