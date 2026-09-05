// Package systemdpolicy owns the versioned systemd restart policy for the
// privileged OCI helper. Renderers and diagnostics consume this one authority.
package systemdpolicy

import (
	"strconv"
	"time"
)

const (
	InitialDelay   = 250 * time.Millisecond
	RestartSteps   = 6
	MaximumDelay   = time.Second
	FailureBurst   = 6
	TakeoverMargin = 2 * time.Second
)

func Directives(systemdVersion int) map[string]string {
	if systemdVersion >= 254 {
		return map[string]string{"RestartSec": InitialDelay.String(), "RestartSteps": strconv.Itoa(RestartSteps), "RestartMaxDelaySec": MaximumDelay.String()}
	}
	return map[string]string{"RestartSec": "1s"}
}

func Render(systemdVersion int) string {
	directives := Directives(systemdVersion)
	result := "RestartSec=" + directives["RestartSec"] + "\n"
	if steps := directives["RestartSteps"]; steps != "" {
		result += "RestartSteps=" + steps + "\nRestartMaxDelaySec=" + directives["RestartMaxDelaySec"] + "\n"
	}
	return result
}

func Name(systemdVersion int) string {
	if systemdVersion == 0 {
		return "conservative_fixed_1s"
	}
	if systemdVersion >= 254 {
		return "geometric_capped_1s"
	}
	return "legacy_fixed_1s"
}

func UnitPolicy(systemdVersion int) map[string]string {
	policy := map[string]string{"Unit.StartLimitIntervalSec": "0", "Service.Restart": "on-failure"}
	for key, value := range Directives(systemdVersion) {
		policy["Service."+key] = value
	}
	return policy
}
