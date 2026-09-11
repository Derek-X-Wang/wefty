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

	// StartupWedgedExitStatus is the helper's distinguished exit status for a
	// deterministic startup-barrier failure that has already repeated
	// StartupFailureBound times. The unit names it in
	// RestartPreventExitStatus, so the loop stops at a failed unit carrying a
	// typed reason instead of restarting forever.
	//
	// Bounding this in the helper rather than through the systemd start limiter
	// is deliberate: StartLimitIntervalSec stays 0, so the triggering socket is
	// never failed with service-start-limit-hit, and a transient crash after a
	// healthy boot barrier still restarts as many times as it needs to.
	StartupWedgedExitStatus = 78
	// StartupFailureBound is the number of consecutive failed startup barriers
	// the helper will burn before it declares itself wedged. A barrier that
	// succeeds clears the count.
	StartupFailureBound = 5
)

func Directives(systemdVersion int) map[string]string {
	preventRestart := strconv.Itoa(StartupWedgedExitStatus)
	if systemdVersion >= 254 {
		return map[string]string{"RestartSec": InitialDelay.String(), "RestartSteps": strconv.Itoa(RestartSteps),
			"RestartMaxDelaySec": MaximumDelay.String(), "RestartPreventExitStatus": preventRestart}
	}
	return map[string]string{"RestartSec": "1s", "RestartPreventExitStatus": preventRestart}
}

func Render(systemdVersion int) string {
	directives := Directives(systemdVersion)
	result := "RestartSec=" + directives["RestartSec"] + "\n"
	if steps := directives["RestartSteps"]; steps != "" {
		result += "RestartSteps=" + steps + "\nRestartMaxDelaySec=" + directives["RestartMaxDelaySec"] + "\n"
	}
	return result + "RestartPreventExitStatus=" + directives["RestartPreventExitStatus"] + "\n"
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
