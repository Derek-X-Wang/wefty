package computerconformance

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const (
	missingEndpointObservationWindow = 15 * time.Second
	driverObservationInterval        = 250 * time.Millisecond
	legacyInputObserverWindow        = 3 * 4 * 125 * time.Millisecond
	// driverObservationSleepBudget bounds only scheduled sleeps. Each phase
	// also performs up to 40 runtime exec round-trips; attach plus mutation can
	// therefore take twice this sleep budget plus runtime latency.
	driverObservationSleepBudget = 10 * time.Second
	containerStopGrace           = 15 * time.Second
	// Run 33695618869 measured permission repair at 118-206 ms across the
	// four reference image/platform builds; 15 seconds retains QEMU margin.
	permissionRepairBudget      = 15 * time.Second
	teardownRemoveRetryInterval = 250 * time.Millisecond
	// teardownRemoveRetryBudget bounds only retries after the runtime has
	// detached every bind mount. It is not a blanket teardown delay.
	teardownRemoveRetryBudget = 2 * time.Second
	permissionRepairScript    = `chmod -R u+rwX,go+rwX "$1"`
	teardownPermissionFixture = "teardown-permission-repair"
)

type TeardownFailureReason string

const (
	TeardownContainerStopFailed    TeardownFailureReason = "container_stop_failed"
	TeardownContainerDetachFailed  TeardownFailureReason = "container_detach_failed"
	TeardownPermissionRepairFailed TeardownFailureReason = "temporary_root_permission_repair_failed"
	TeardownTemporaryRootBusy      TeardownFailureReason = "temporary_root_busy"
	TeardownTemporaryRootNotEmpty  TeardownFailureReason = "temporary_root_not_empty"
	TeardownTemporaryRootRemove    TeardownFailureReason = "temporary_root_remove_failed"
)

// TeardownFailure keeps cleanup failures machine-classifiable while naming
// the exact runtime or filesystem object that remains.
type TeardownFailure struct {
	Reason   TeardownFailureReason
	Leftover string
	Err      error
}

func (e *TeardownFailure) Error() string {
	return fmt.Sprintf("runtime teardown failed: reason=%s leftover=%q: %v", e.Reason, e.Leftover, e.Err)
}

func (e *TeardownFailure) Unwrap() error { return e.Err }

type RuntimeConfig struct {
	Image              string
	RepairImage        string
	Runtime            string
	Platform           string
	InputOraclePath    string
	DriverOraclePath   string
	EdgeProcessPattern string
	ReceiptPath        string
	// MutationProfile is used only by repository acceptance fixtures to mutate
	// Docker's approximation of the production profile or teardown state.
	MutationProfile string
	Now             func() time.Time
	Sleep           func(context.Context, time.Duration) error
}

type RuntimeResult struct {
	Receipt Receipt
	Err     error
}

type runtimeRunner struct {
	config                                   RuntimeConfig
	recorder                                 *Recorder
	root, serviceDir, controlDir, handoffDir string
	containerID                              string
	containerLeftoverConfirmed               bool
	viewPort, controlPort, attempt           int
	firstBootReadiness                       time.Duration
	tools                                    map[string]bool
	failed                                   bool
	readDriverObservationHook                func(context.Context) (driverObservation, error)
	writeDriverHook                          func(string) error
	runCommandHook                           func(context.Context, ...string) (commandResult, error)
	removeAllHook                            func(string) error
	teardownLogHook                          func(string)
}

func Run(ctx context.Context, config RuntimeConfig) RuntimeResult {
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.Sleep == nil {
		config.Sleep = sleepContext
	}
	startedAt := config.Now()
	runner := &runtimeRunner{config: config, recorder: NewRecorder(config.Image, config.Runtime, config.Platform, startedAt), tools: make(map[string]bool)}
	err := runner.run(ctx)
	return RuntimeResult{Receipt: runner.recorder.Finish(config.Now()), Err: err}
}

func (r *runtimeRunner) run(ctx context.Context) (err error) {
	if r.config.Image == "" {
		return errors.New("--image is required")
	}
	if !strings.Contains(r.config.RepairImage, "@sha256:") {
		return errors.New("--repair-image must be an immutable sha256 digest reference")
	}
	if r.config.Runtime != "docker" && r.config.Runtime != "nerdctl" {
		return errors.New("--runtime must be docker or nerdctl")
	}
	if _, err := exec.LookPath(r.config.Runtime); err != nil {
		return fmt.Errorf("runtime unavailable: %w", err)
	}
	root, err := os.MkdirTemp("", "wefty-computer-conformance-")
	if err != nil {
		return err
	}
	r.root = root
	defer func() { err = errors.Join(err, r.cleanup()) }()
	r.serviceDir, r.controlDir, r.handoffDir = filepath.Join(root, "service"), filepath.Join(root, "control"), filepath.Join(root, "handoff")
	for _, directory := range []string{r.serviceDir, r.controlDir, r.handoffDir} {
		if err := os.Mkdir(directory, 0o777); err != nil {
			return err
		}
		if err := os.Chmod(directory, 0o777); err != nil {
			return err
		}
	}
	if err := r.writeDriver(`{"version":1,"human_driving":false}`); err != nil {
		return err
	}
	if err := r.allocatePorts(); err != nil {
		return err
	}
	if r.config.MutationProfile == "duplicate-endpoint" {
		r.controlPort = r.viewPort
	}
	if r.viewPort == r.controlPort && r.config.MutationProfile != "duplicate-endpoint" {
		r.record("endpoints.distinct", StatusFail, "view and control received the same attempt-local port")
		return errors.New("duplicate endpoint allocation")
	}
	if r.config.MutationProfile != "duplicate-endpoint" {
		r.record("endpoints.distinct", StatusPass, "two distinct attempt-local ports allocated")
	}

	startedAt := r.config.Now()
	if err := r.startContainer(ctx); err != nil {
		r.record("runtime.started", StatusFail, r.runtimeDetail("image did not start", err))
		return err
	}
	r.record("runtime.started", StatusPass, "container task started")
	if err := r.waitReady(ctx, startedAt, false); err != nil {
		return r.withStartupLogs(ctx, err)
	}
	if r.config.MutationProfile == teardownPermissionFixture {
		return nil
	}
	if r.config.MutationProfile == "duplicate-endpoint" {
		r.record("endpoints.distinct", StatusFail, "view and control received the same attempt-local port")
		return errors.New("fixture returned duplicate endpoint authority after the image ran")
	}
	r.discoverTools(ctx)
	r.markContainerdProfileNotRun()
	r.checkEnvironment(ctx)
	if r.mutationFailed() {
		return nil
	}
	r.checkTargets(ctx)
	if r.mutationFailed() {
		return nil
	}
	r.checkLoopback(ctx)
	if r.mutationFailed() {
		return nil
	}
	r.checkTransportNegatives(ctx)
	if r.mutationFailed() {
		return nil
	}
	r.checkDriver(ctx)
	if r.mutationFailed() {
		return nil
	}
	r.checkInput(ctx)
	if r.mutationFailed() {
		return nil
	}
	r.checkHarnessProfile(ctx)
	if r.mutationFailed() {
		return nil
	}
	r.checkEdgeRecovery(ctx)
	if r.mutationFailed() {
		return nil
	}
	return r.checkPersistence(ctx)
}

func (r *runtimeRunner) mutationFailed() bool {
	return r.config.MutationProfile != "" && r.failed
}

var mutationFailureCells = map[string]string{
	"missing-control-endpoint":        "transport.control-ready",
	"missing-view-endpoint":           "transport.view-ready",
	"duplicate-endpoint":              "endpoints.distinct",
	"plain-tcp-control":               "transport.plain-tcp-rejected",
	"plain-rfb-control":               "transport.plain-tcp-rejected",
	"view-accepts-input":              "input.view-isolated",
	"text-frames-accepted":            "transport.text-frame-rejected",
	"driver-json-ignored":             "driver.true-consumed",
	"malformed-driver-accepted":       "driver.malformed-fails-closed",
	"unknown-driver-version-accepted": "driver.unknown-version-fails-closed",
	"writable-driver":                 "driver.read-only",
	"reserved-env-shadowed":           "environment.view-port",
	"forbidden-privilege":             "harness.forbidden-privilege",
	"readiness-over-60s":              "readiness.before-deadline",
	"shm-too-small":                   "harness.shm-size",
	"writable-rootfs":                 "harness.rootfs-read-only",
	"view-wildcard-bind":              "endpoints.view-loopback",
	"control-wildcard-bind":           "endpoints.control-loopback",
	"profile-state-lost":              "persistence.profile-survives",
	"sign-in-state-lost":              "persistence.sign-in-survives",
	"edge-does-not-recover":           "persistence.edge-recovers",
}

func (r *runtimeRunner) withStartupLogs(ctx context.Context, readinessErr error) error {
	// A readiness receipt deliberately records only contract assertions, but an
	// operator still needs the tenant's bounded startup diagnostics to repair an
	// image that never exposed an edge. Keep those diagnostics on stderr, redact
	// endpoint-looking values, and cap their size instead of putting them in the
	// durable receipt.
	result, err := r.runCommand(ctx, "logs", "--tail", "200", r.containerID)
	if err != nil {
		return readinessErr
	}
	logs := strings.TrimSpace(result.stdout + result.stderr)
	if logs == "" {
		return readinessErr
	}
	logs = strings.TrimSpace(portPattern.ReplaceAllString(logs, "<endpoint>"))
	if len(logs) > 8192 {
		logs = logs[:8192]
	}
	return fmt.Errorf("%w; tenant startup logs: %s", readinessErr, logs)
}

func (r *runtimeRunner) allocatePorts() error {
	view, err := availablePort()
	if err != nil {
		return err
	}
	control, err := availablePort()
	if err != nil {
		return err
	}
	for control == view {
		control, err = availablePort()
		if err != nil {
			return err
		}
	}
	r.viewPort, r.controlPort = view, control
	return nil
}

func (r *runtimeRunner) startContainer(ctx context.Context) error {
	r.attempt++
	name := fmt.Sprintf("wefty-computer-conformance-%d-%d", os.Getpid(), r.attempt)
	args := []string{"run", "--detach", "--name", name}
	if r.config.Platform != "" {
		args = append(args, "--platform", r.config.Platform)
	}
	args = append(args, "--network", "host")
	if r.config.MutationProfile != "writable-rootfs" {
		args = append(args, "--read-only")
	}
	args = append(args, "--security-opt", "no-new-privileges:true", "--cap-drop", "ALL")
	for _, capability := range []string{"CHOWN", "DAC_OVERRIDE", "FSETID", "FOWNER", "MKNOD", "SETGID", "SETUID", "SETFCAP", "SETPCAP", "SYS_CHROOT", "KILL", "AUDIT_WRITE"} {
		args = append(args, "--cap-add", capability)
	}
	if r.config.MutationProfile == "forbidden-privilege" {
		args = append(args, "--cap-add", "SYS_ADMIN")
	}
	shmSize := "1073741824"
	if r.config.MutationProfile == "shm-too-small" {
		shmSize = "67108864"
	}
	args = append(args,
		"--tmpfs", "/tmp:rw,nosuid,nodev,size=536870912,mode=1777",
		"--tmpfs", "/var/tmp:rw,nosuid,nodev,size=67108864,mode=1777",
		"--tmpfs", "/run:rw,nosuid,nodev,size=67108864,mode=0755",
		"--tmpfs", "/dev/shm:rw,nosuid,nodev,noexec,size="+shmSize+",mode=1777",
		"--mount", "type=bind,src="+r.serviceDir+",dst=/wefty/service",
	)
	controlMount := "type=bind,src=" + r.controlDir + ",dst=/wefty/control"
	if r.config.MutationProfile != "writable-driver" {
		controlMount += ",readonly"
	}
	args = append(args, "--mount", controlMount, "--mount", "type=bind,src="+r.handoffDir+",dst=/wefty/handoff,readonly")
	args = append(args, "--env", contract.EnvServiceDir+"="+contract.OCIContainerServiceDirectory)
	if r.config.MutationProfile != "reserved-env-shadowed" {
		args = append(args, "--env", contract.EnvComputerViewPort+"="+strconv.Itoa(r.viewPort))
	} else {
		args = append(args, "--env", "WEFTY_CONFORMANCE_REAL_VIEW_PORT="+strconv.Itoa(r.viewPort))
	}
	args = append(args,
		"--env", contract.EnvComputerControlPort+"="+strconv.Itoa(r.controlPort),
		"--env", "WEFTY_CONFORMANCE_TENANT_VALUE=preserved",
		r.config.Image,
	)
	output, err := r.runCommand(ctx, args...)
	if err != nil {
		return fmt.Errorf("start container: %s", r.runtimeDetail("runtime rejected start", errWithOutput(err, output)))
	}
	r.containerID = strings.TrimSpace(output.stdout)
	if r.containerID == "" {
		return errors.New("runtime returned an empty container id")
	}
	return nil
}

func (r *runtimeRunner) waitReady(ctx context.Context, startedAt time.Time, restart bool) error {
	viewReady, controlReady := false, false
	viewEvent, controlEvent := ReadinessEventViewEndpointReady, ReadinessEventControlEndpointReady
	var viewProbeErr, controlProbeErr error
	for BeforeReadinessDeadline(r.config.Now(), startedAt) {
		if !viewReady {
			viewReady, viewEvent, viewProbeErr = r.probeReady(ctx, r.viewPort, ReadinessEventViewEndpointReady)
		}
		if !controlReady {
			controlReady, controlEvent, controlProbeErr = r.probeReady(ctx, r.controlPort, ReadinessEventControlEndpointReady)
		}
		if viewReady && controlReady {
			r.record("transport.view-ready", StatusPass, "binary RFB greeting observed")
			r.record("transport.control-ready", StatusPass, "binary RFB greeting observed")
			r.record("transport.plain-tcp-rejected", StatusPass, "both endpoints required the exact WebSocket upgrade")
			r.record("readiness.before-deadline", StatusPass, "both exact endpoints became ready before t0 + 60s")
			readiness := r.config.Now().Sub(startedAt)
			r.recorder.RecordReadiness(restart, readiness)
			if !restart {
				r.firstBootReadiness = readiness
			}
			return nil
		}
		if (r.config.MutationProfile == "missing-control-endpoint" || r.config.MutationProfile == "missing-view-endpoint") && r.config.Now().Sub(startedAt) >= missingEndpointObservationWindow {
			break
		}
		if err := r.config.Sleep(ctx, 250*time.Millisecond); err != nil {
			return err
		}
	}
	viewPlainTCP := !viewReady && terminalProbeProvesPlainTCP(ctx, r.viewPort, viewProbeErr)
	controlPlainTCP := !controlReady && terminalProbeProvesPlainTCP(ctx, r.controlPort, controlProbeErr)
	if viewPlainTCP || controlPlainTCP {
		r.record("transport.plain-tcp-rejected", StatusFail, "endpoint accepted TCP but did not complete the required WebSocket upgrade")
		return errors.New("plain TCP is not rfb-websocket-v1 readiness")
	}
	if r.config.MutationProfile == "readiness-over-60s" {
		r.record("readiness.before-deadline", StatusFail, "endpoint pair was not ready before t0 + 60s")
		return errors.New("Computer startup readiness timeout")
	}
	if probeProtocolFailed(viewProbeErr) {
		r.record("transport.view-ready", StatusFail, r.runtimeDetail("view violated rfb-websocket-v1", viewProbeErr))
		return fmt.Errorf("view endpoint protocol failure: %w", viewProbeErr)
	}
	if probeProtocolFailed(controlProbeErr) {
		r.record("transport.control-ready", StatusFail, r.runtimeDetail("control violated rfb-websocket-v1", controlProbeErr))
		return fmt.Errorf("control endpoint protocol failure: %w", controlProbeErr)
	}
	observationWindow := contract.ComputerStartupReadinessTimeout
	if r.config.MutationProfile == "missing-control-endpoint" || r.config.MutationProfile == "missing-view-endpoint" {
		observationWindow = missingEndpointObservationWindow
	}
	observationElapsed := r.config.Now().Sub(startedAt)
	if observationElapsed < observationWindow {
		observationElapsed = observationWindow
	}
	if !viewReady {
		r.recordReadinessTimeout("transport.view-ready", "view never completed rfb-websocket-v1", viewEvent, observationWindow, observationElapsed)
		if viewProbeErr != nil {
			return fmt.Errorf("view endpoint readiness timeout: %w", viewProbeErr)
		}
		return errors.New("view endpoint readiness timeout")
	}
	r.record("transport.view-ready", StatusPass, "binary RFB greeting observed")
	if !controlReady {
		r.recordReadinessTimeout("transport.control-ready", "control never completed rfb-websocket-v1", controlEvent, observationWindow, observationElapsed)
		if controlProbeErr != nil {
			return fmt.Errorf("control endpoint readiness timeout: %w", controlProbeErr)
		}
		return errors.New("control endpoint readiness timeout")
	}
	return errors.New("Computer startup readiness timeout")
}

func (r *runtimeRunner) probeReady(ctx context.Context, port int, endpointEvent ReadinessEvent) (bool, ReadinessEvent, error) {
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	connection, err := OpenRFB(probeCtx, port, contract.ComputerDisplayWebSocketPath)
	if err == nil {
		connection.close()
		return true, endpointEvent, nil
	}
	var firstFrame *rfbFirstFrameReadError
	if errors.As(err, &firstFrame) {
		return false, ReadinessEventFirstRFBFrame, err
	}
	return false, endpointEvent, err
}

func probeProtocolFailed(err error) bool {
	var protocol *rfbProtocolError
	return errors.As(err, &protocol)
}

func terminalProbeProvesPlainTCP(ctx context.Context, port int, err error) bool {
	var rejected *rfbUpgradeRejectedError
	if errors.As(err, &rejected) {
		return true
	}
	var incomplete *websocketUpgradeIncompleteError
	return errors.As(err, &incomplete) && rawTCPAccepted(ctx, port)
}

func rawTCPAccepted(ctx context.Context, port int) bool {
	dialer := net.Dialer{Timeout: 250 * time.Millisecond}
	connection, err := dialer.DialContext(ctx, "tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		return false
	}
	_ = connection.Close()
	return true
}

// BeforeReadinessDeadline is the publication edge shared by runtime polling
// and injected-clock tests. Success at exactly t0 + 60 seconds is too late.
func BeforeReadinessDeadline(now, startedAt time.Time) bool {
	return now.Before(startedAt.Add(contract.ComputerStartupReadinessTimeout))
}

func (r *runtimeRunner) discoverTools(ctx context.Context) {
	for _, tool := range []string{"sh", "awk", "stat", "touch", "cat", "tr"} {
		result, _ := r.runCommand(ctx, "exec", r.containerID, "sh", "-c", "command -v \"$1\" >/dev/null", "wefty-conformance", tool)
		r.tools[tool] = result.exitCode == 0
	}
}

func (r *runtimeRunner) requireTools(ids []string, names ...string) bool {
	for _, name := range names {
		if !r.tools[name] {
			for _, id := range ids {
				r.record(id, StatusNotRun, "guest tool "+name+" is unavailable")
			}
			return false
		}
	}
	return true
}

func (r *runtimeRunner) markContainerdProfileNotRun() {
	for _, id := range []string{"profile.capabilities", "profile.seccomp", "profile.namespaces", "profile.devices", "profile.cgroup-memory-max", "profile.cgroup-oom-group", "profile.cgroup-swap-max"} {
		r.record(id, StatusNotRun, ContainerdProfileNotRun)
	}
}

func (r *runtimeRunner) checkEnvironment(ctx context.Context) {
	ids := []string{"environment.service-dir", "environment.view-port", "environment.control-port", "environment.service-port-omitted", "environment.handoff-dir-omitted", "environment.authority-omitted", "environment.other-wefty-preserved"}
	if !r.requireTools(ids, "sh") {
		return
	}
	checks := []struct{ id, script, arg string }{
		{"environment.service-dir", `test "${WEFTY_SERVICE_DIR-}" = /wefty/service`, ""},
		{"environment.view-port", `test "${WEFTY_COMPUTER_VIEW_PORT-}" = "$1"`, strconv.Itoa(r.viewPort)},
		{"environment.control-port", `test "${WEFTY_COMPUTER_CONTROL_PORT-}" = "$1"`, strconv.Itoa(r.controlPort)},
		{"environment.service-port-omitted", `test "${WEFTY_SERVICE_PORT+x}" != x`, ""},
		{"environment.handoff-dir-omitted", `test "${WEFTY_HANDOFF_DIR+x}" != x`, ""},
		{"environment.authority-omitted", `test "${WEFTY_COMPUTER_TOKEN+x}" != x && test "${WEFTY_L3_ENDPOINT+x}" != x && test "${WEFTY_RUN_TOKEN+x}" != x`, ""},
		{"environment.other-wefty-preserved", `test "${WEFTY_CONFORMANCE_TENANT_VALUE-}" = preserved`, ""},
	}
	for _, check := range checks {
		status := StatusPass
		detail := "authoritative runtime environment observed"
		if result := r.execShell(ctx, check.script, check.arg); result.exitCode != 0 {
			status, detail = StatusFail, "reserved environment did not match the authoritative value"
		}
		r.record(check.id, status, detail)
	}
}

func (r *runtimeRunner) checkTargets(ctx context.Context) {
	if !r.requireTools([]string{"targets.service-nonshadowable", "targets.control-nonshadowable", "targets.handoff-nonshadowable"}, "sh") {
		return
	}
	for _, target := range []struct{ id, path string }{{"targets.service-nonshadowable", "/wefty/service"}, {"targets.control-nonshadowable", "/wefty/control"}, {"targets.handoff-nonshadowable", "/wefty/handoff"}} {
		if r.execShell(ctx, `test -d "$1"`, target.path).exitCode == 0 {
			r.record(target.id, StatusPass, "helper-owned target mount observed")
		} else {
			r.record(target.id, StatusFail, "reserved target was shadowed or missing")
		}
	}
	for _, target := range []struct{ id, path string }{{"targets.token-mode", "/wefty/control/computer-token"}, {"targets.endpoint-mode", "/wefty/control/l3-endpoint"}} {
		if !r.tools["stat"] {
			r.record(target.id, StatusNotRun, "guest tool stat is unavailable")
			continue
		}
		if r.execShell(ctx, `test ! -e "$1" || test "$(stat -c %a "$1")" = 400`, target.path).exitCode == 0 {
			r.record(target.id, StatusPass, "default-off file absent or exact 0400 mode observed")
		} else {
			r.record(target.id, StatusFail, "optional authority file mode differed from 0400")
		}
	}
}

func (r *runtimeRunner) checkLoopback(ctx context.Context) {
	ids := []string{"endpoints.view-loopback", "endpoints.control-loopback"}
	if !r.requireTools(ids, "sh", "awk") {
		return
	}
	for _, endpoint := range []struct {
		id   string
		port int
	}{{ids[0], r.viewPort}, {ids[1], r.controlPort}} {
		hexPort := fmt.Sprintf("%04X", endpoint.port)
		script := `awk -v loop="0100007F:$1" -v wildcard="00000000:$1" '$2 == loop { good=1 } $2 == wildcard { bad=1 } END { exit !(good && !bad) }' /proc/net/tcp`
		if r.execShell(ctx, script, hexPort).exitCode == 0 {
			r.record(endpoint.id, StatusPass, "only the injected IPv4 loopback bind was observed")
		} else {
			r.record(endpoint.id, StatusFail, "endpoint was missing or wildcard-bound")
		}
	}
}

func (r *runtimeRunner) checkTransportNegatives(ctx context.Context) {
	probe := func(id string, operation func(context.Context, int) error) {
		for _, port := range []int{r.viewPort, r.controlPort} {
			probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := operation(probeCtx, port)
			cancel()
			if err != nil {
				r.record(id, StatusFail, "one endpoint violated the negative wire assertion")
				return
			}
		}
		r.record(id, StatusPass, "both endpoints satisfied the negative wire assertion")
	}
	probe("transport.query-ignored", func(ctx context.Context, port int) error {
		c, err := OpenRFB(ctx, port, contract.ComputerDisplayWebSocketPath+"?token=ignored")
		if err == nil {
			c.close()
		}
		return err
	})
	probe("transport.fragment-ignored", func(ctx context.Context, port int) error {
		c, err := OpenRFB(ctx, port, contract.ComputerDisplayWebSocketPath+"#viewer")
		if err == nil {
			c.close()
		}
		return err
	})
	probe("transport.wrong-path-rejected", func(ctx context.Context, port int) error {
		p := contract.ComputerDisplayWebSocketSubprotocol
		return AssertUpgradeRejected(ctx, port, "/wrong", &p)
	})
	probe("transport.missing-subprotocol-rejected", func(ctx context.Context, port int) error {
		return AssertUpgradeRejected(ctx, port, contract.ComputerDisplayWebSocketPath, nil)
	})
	probe("transport.wrong-subprotocol-rejected", func(ctx context.Context, port int) error {
		p := "base64"
		return AssertUpgradeRejected(ctx, port, contract.ComputerDisplayWebSocketPath, &p)
	})
	probe("transport.text-frame-rejected", AssertTextRejected)
}

type driverObservation struct {
	Version        int    `json:"version"`
	HumanDriving   bool   `json:"human_driving"`
	Generation     uint64 `json:"generation"`
	Fingerprint    string `json:"fingerprint"`
	Classification string `json:"classification"`
}

func (r *runtimeRunner) readDriverObservation(ctx context.Context) (driverObservation, error) {
	var value driverObservation
	payload, err := r.readContainerFile(ctx, r.config.DriverOraclePath)
	if err != nil {
		return value, err
	}
	if err := json.Unmarshal([]byte(payload), &value); err != nil {
		return value, err
	}
	if value.Version != 1 {
		return value, fmt.Errorf("oracle version %d", value.Version)
	}
	return value, nil
}

func driverFingerprint(document string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(document)))
}

func (r *runtimeRunner) observeDriver(ctx context.Context) (driverObservation, error) {
	if r.readDriverObservationHook != nil {
		return r.readDriverObservationHook(ctx)
	}
	return r.readDriverObservation(ctx)
}

func (r *runtimeRunner) writeObservedDriver(document string) error {
	if r.writeDriverHook != nil {
		return r.writeDriverHook(document)
	}
	return r.writeDriver(document)
}

func (r *runtimeRunner) waitDriverObservation(ctx context.Context, after uint64, fingerprint, classification string, requireAdvance bool) (driverObservation, bool, bool) {
	attempts := int(driverObservationSleepBudget / driverObservationInterval)
	var last driverObservation
	for attempt := 0; attempt < attempts; attempt++ {
		value, err := r.observeDriver(ctx)
		if err == nil {
			last = value
			if requireAdvance && value.Generation < after {
				return last, false, true
			}
			advanced := !requireAdvance || value.Generation > after
			classified := classification == "" || value.Classification == classification
			if advanced && classified && value.Fingerprint == fingerprint {
				return value, true, false
			}
		}
		if r.config.Sleep(ctx, driverObservationInterval) != nil {
			return last, false, false
		}
	}
	return last, false, false
}

type driverMutationFailure string

const (
	driverMutationOK                     driverMutationFailure = ""
	driverObserverNeverAttached          driverMutationFailure = "observer-never-attached"
	driverMutationNoOp                   driverMutationFailure = "no-op-rewrite"
	driverMutationWriteFailed            driverMutationFailure = "write-failed"
	driverMutationNotObserved            driverMutationFailure = "not-observed"
	driverObserverGenerationReset        driverMutationFailure = "observer-generation-reset"
	driverMutationUnexpectedHumanDriving driverMutationFailure = "unexpected-human-driving"
)

type driverMutationResult struct {
	Failure driverMutationFailure
}

func (result driverMutationResult) OK() bool { return result.Failure == driverMutationOK }

func (r *runtimeRunner) mutateAndObserveDriver(ctx context.Context, document, classification string, expected bool) driverMutationResult {
	current, err := os.ReadFile(filepath.Join(r.controlDir, "driver.json"))
	if err != nil {
		return driverMutationResult{Failure: driverObserverNeverAttached}
	}
	// Rewriting identical bytes cannot advance a fingerprint-keyed observer and
	// would turn an assertion into a timeout that looks like an observer race.
	if string(current) == document {
		return driverMutationResult{Failure: driverMutationNoOp}
	}
	// Prove the observer is attached to the exact current document before
	// publishing the mutation. A generation alone can belong to an earlier
	// document and let a late poll mistake unrelated state for this assertion.
	previous, attached, _ := r.waitDriverObservation(ctx, 0, driverFingerprint(string(current)), "", false)
	if !attached {
		return driverMutationResult{Failure: driverObserverNeverAttached}
	}
	if err := r.writeObservedDriver(document); err != nil {
		return driverMutationResult{Failure: driverMutationWriteFailed}
	}
	observed, ok, reset := r.waitDriverObservation(ctx, previous.Generation, driverFingerprint(document), classification, true)
	if reset {
		return driverMutationResult{Failure: driverObserverGenerationReset}
	}
	if !ok {
		return driverMutationResult{Failure: driverMutationNotObserved}
	}
	if observed.HumanDriving != expected {
		return driverMutationResult{Failure: driverMutationUnexpectedHumanDriving}
	}
	return driverMutationResult{}
}

func negativeDriverFailureDetail(subject string, result driverMutationResult) string {
	switch result.Failure {
	case driverMutationUnexpectedHumanDriving:
		return subject + " was accepted"
	case driverMutationNotObserved:
		return subject + " was not observed"
	case driverObserverNeverAttached:
		return "driver observer never attached before " + subject
	case driverObserverGenerationReset:
		return "driver observer generation reset before " + subject + " was observed"
	case driverMutationNoOp:
		return "driver assertion attempted a no-op rewrite before " + subject
	case driverMutationWriteFailed:
		return subject + " could not be published"
	default:
		return subject + " failed for an unknown reason"
	}
}

func (r *runtimeRunner) checkDriver(ctx context.Context) {
	if !r.requireTools([]string{"driver.read-only"}, "sh") {
	} else if r.execShell(ctx, `test ! -w /wefty/control/driver.json && ! (printf probe > /wefty/control/.wefty-write-probe) 2>/dev/null`, "").exitCode == 0 {
		r.record("driver.read-only", StatusPass, "tenant cannot write or replace driver.json")
	} else {
		r.record("driver.read-only", StatusFail, "tenant can write driver.json")
	}
	if !r.requireTools([]string{"driver.mode"}, "sh", "stat") {
	} else if r.execShell(ctx, `test "$(stat -c %a /wefty/control/driver.json)" = 444`, "").exitCode == 0 {
		r.record("driver.mode", StatusPass, "mode 0444 observed")
	} else {
		r.record("driver.mode", StatusFail, "driver.json mode differs from 0444")
	}
	const falseDocument = `{"version":1,"human_driving":false}`
	if payload, err := os.ReadFile(filepath.Join(r.controlDir, "driver.json")); err == nil && string(payload) == falseDocument {
		r.record("driver.initial-false", StatusPass, "exact version-1 false document observed")
	} else {
		r.record("driver.initial-false", StatusFail, "fresh driver document was not exact false")
	}
	ids := []string{"driver.true-consumed", "driver.release-consumed", "driver.malformed-fails-closed", "driver.unknown-version-fails-closed", "driver.missing-fails-closed"}
	if r.config.DriverOraclePath == "" {
		for _, id := range ids {
			r.record(id, StatusNotRun, "no --driver-oracle-path was supplied")
		}
		return
	}
	if !r.requireTools(ids, "cat") {
		return
	}
	if r.mutateAndObserveDriver(ctx, `{"version":1,"human_driving":true}`, "valid", true).OK() {
		r.record("driver.true-consumed", StatusPass, "tenant observed the new generation and consumed true")
	} else {
		r.record("driver.true-consumed", StatusFail, "tenant ignored the true driver generation")
	}
	if r.mutateAndObserveDriver(ctx, falseDocument, "valid", false).OK() {
		r.record("driver.release-consumed", StatusPass, "tenant observed the new generation and consumed release")
	} else {
		r.record("driver.release-consumed", StatusFail, "tenant did not consume the release generation")
	}
	malformed := true
	var malformedResult driverMutationResult
	for _, document := range []string{`{"version":true,"human_driving":true}`, `{"version":1,"human_driving":1}`, `{malformed`} {
		malformedResult = r.mutateAndObserveDriver(ctx, document, "malformed", false)
		if !malformedResult.OK() {
			malformed = false
			break
		}
	}
	if malformed {
		r.record("driver.malformed-fails-closed", StatusPass, "each malformed generation advanced and remained false")
	} else {
		r.record("driver.malformed-fails-closed", StatusFail, negativeDriverFailureDetail("a malformed generation", malformedResult))
		return
	}
	unknownResult := r.mutateAndObserveDriver(ctx, `{"version":2,"human_driving":true}`, "unknown-version", false)
	if unknownResult.OK() {
		r.record("driver.unknown-version-fails-closed", StatusPass, "unknown-version generation advanced and remained false")
	} else {
		r.record("driver.unknown-version-fails-closed", StatusFail, negativeDriverFailureDetail("unknown-version generation", unknownResult))
		return
	}
	current, readErr := os.ReadFile(filepath.Join(r.controlDir, "driver.json"))
	var previous driverObservation
	if readErr == nil {
		previous, _, _ = r.waitDriverObservation(ctx, 0, driverFingerprint(string(current)), "", false)
	}
	missing := filepath.Join(r.controlDir, "driver.json.missing")
	_ = os.Remove(missing)
	renameErr := errors.New("driver observer was not attached before removal")
	if readErr == nil && previous.Fingerprint == driverFingerprint(string(current)) {
		renameErr = os.Rename(filepath.Join(r.controlDir, "driver.json"), missing)
	}
	var missingObservation driverObservation
	missingObserved := false
	if renameErr == nil {
		missingObservation, missingObserved, _ = r.waitDriverObservation(ctx, previous.Generation, "missing", "missing", true)
	}
	if readErr == nil && previous.Fingerprint == driverFingerprint(string(current)) && renameErr == nil && missingObserved && !missingObservation.HumanDriving {
		r.record("driver.missing-fails-closed", StatusPass, "missing generation advanced and remained false")
	} else {
		r.record("driver.missing-fails-closed", StatusFail, "missing document was not observed fail-closed")
	}
	_ = os.Remove(missing)
	_ = r.writeDriver(falseDocument)
}

type inputObservation struct {
	Version        int      `json:"version"`
	Ready          *bool    `json:"ready,omitempty"`
	Generation     uint64   `json:"generation"`
	KeyEvents      uint64   `json:"key_events"`
	X              int      `json:"x"`
	Y              int      `json:"y"`
	PointerHistory [][2]int `json:"pointer_history"`
	ObserverLines  *uint64  `json:"observer_lines,omitempty"`
}

func (r *runtimeRunner) waitInputReady(ctx context.Context) (bool, time.Duration, time.Duration, error) {
	const window = 400 * 125 * time.Millisecond
	startedAt := r.config.Now()
	for attempt := 0; attempt < 400; attempt++ {
		observation, err := r.readInputObservation(ctx)
		if err == nil && (observation.Ready == nil || *observation.Ready) {
			return true, window, r.config.Now().Sub(startedAt), nil
		}
		if err := r.config.Sleep(ctx, 125*time.Millisecond); err != nil {
			return false, window, r.config.Now().Sub(startedAt), err
		}
	}
	elapsed := r.config.Now().Sub(startedAt)
	if elapsed < window {
		elapsed = window
	}
	return false, window, elapsed, nil
}

func (r *runtimeRunner) readInputObservation(ctx context.Context) (inputObservation, error) {
	var value inputObservation
	payload, err := r.readContainerFile(ctx, r.config.InputOraclePath)
	if err != nil {
		return value, err
	}
	if err := json.Unmarshal([]byte(payload), &value); err != nil {
		return value, err
	}
	if value.Version != 1 {
		return value, fmt.Errorf("oracle version %d", value.Version)
	}
	return value, nil
}
func historyContains(observation inputObservation, x, y int) bool {
	for _, point := range observation.PointerHistory {
		if point[0] == x && point[1] == y {
			return true
		}
	}
	return false
}
func (r *runtimeRunner) waitInputSentinel(ctx context.Context, after uint64, x, y int) (inputObservation, bool) {
	last := inputObservation{}
	for attempt := 0; attempt < 80; attempt++ {
		value, err := r.readInputObservation(ctx)
		if err == nil {
			last = value
			if value.Generation > after && historyContains(value, x, y) {
				return value, true
			}
		}
		if r.config.Sleep(ctx, 125*time.Millisecond) != nil {
			break
		}
	}
	return last, false
}

func inputObserverAdvanced(before, after inputObservation) bool {
	if after.KeyEvents <= before.KeyEvents {
		return false
	}
	if before.ObserverLines == nil {
		return after.ObserverLines == nil
	}
	return after.ObserverLines != nil && *after.ObserverLines > *before.ObserverLines
}

func inputObserverLines(observation inputObservation) uint64 {
	if observation.ObserverLines == nil {
		return 0
	}
	return *observation.ObserverLines
}

func (r *runtimeRunner) waitInputObserverAdvance(ctx context.Context, before inputObservation, session *InputSession) (inputObservation, bool, time.Duration, time.Duration, error) {
	last := inputObservation{}
	// wayvnc can create a new client's virtual keyboard after its first RFB
	// messages arrive. Reuse that established client while proving liveness so
	// a cold-start discard cannot look like a dead guest observer.
	window := r.readinessEventObservationBudget()
	startedAt := r.config.Now()
	deadline := startedAt.Add(window)
	for r.config.Now().Before(deadline) {
		for poll := 0; poll < 4 && r.config.Now().Before(deadline); poll++ {
			value, err := r.readInputObservation(ctx)
			if err == nil {
				last = value
				if inputObserverAdvanced(before, value) {
					return value, true, window, r.config.Now().Sub(startedAt), nil
				}
			}
			if err := r.config.Sleep(ctx, 125*time.Millisecond); err != nil {
				return last, false, window, r.config.Now().Sub(startedAt), err
			}
		}
		if r.config.Now().Before(deadline) {
			if err := session.SendKey(); err != nil {
				return last, false, window, r.config.Now().Sub(startedAt), err
			}
		}
	}
	elapsed := r.config.Now().Sub(startedAt)
	if elapsed < window {
		elapsed = window
	}
	return last, false, window, elapsed, nil
}

func (r *runtimeRunner) readinessEventObservationBudget() time.Duration {
	if r.config.Platform != "linux/arm64" || r.firstBootReadiness <= 0 {
		return legacyInputObserverWindow
	}
	// The current arm64/QEMU run supplies the measurement. Two observed startup
	// intervals give an event that lags endpoint publication one full startup
	// cycle to arrive, without changing native/amd64 polling or using a fixed
	// emulation timeout.
	return max(legacyInputObserverWindow, 2*r.firstBootReadiness)
}

func (r *runtimeRunner) sendControlInput(ctx context.Context, after uint64, x, y int) (inputObservation, bool) {
	last := inputObservation{}
	for attempt := 0; attempt < 2; attempt++ {
		probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		session, err := StartPointer(probeCtx, r.controlPort, x, y)
		cancel()
		if err != nil {
			continue
		}
		observation, observed := r.waitInputSentinel(ctx, after, x, y)
		last = observation
		if !observed {
			session.Close()
			continue
		}
		if err := session.SendKey(); err != nil {
			session.Close()
			continue
		}
		keyObserved := false
		for poll := 0; poll < 80; poll++ {
			value, readErr := r.readInputObservation(ctx)
			if readErr == nil {
				last = value
			}
			if readErr == nil && value.KeyEvents > observation.KeyEvents {
				keyObserved = true
				break
			}
			if r.config.Sleep(ctx, 125*time.Millisecond) != nil {
				break
			}
		}
		session.Close()
		if keyObserved {
			if inputObserverAdvanced(observation, last) {
				r.observeReadinessEvent(ReadinessEventKeyObserverAdvanced)
			}
			return last, true
		}
	}
	return last, false
}

func (r *runtimeRunner) proveViewIsolation(ctx context.Context, id string, targetX, targetY, sentinelX, sentinelY int) bool {
	// A newly started wayvnc client can need one event cycle before its virtual
	// input objects are active. Probe twice so isolation is proved after that
	// warm-up too; a broken view edge must not get a free first connection.
	for round := 0; round < 2; round++ {
		x, y := targetX+round*29, targetY+round*31
		sx, sy := sentinelX-round*17, sentinelY+round*19
		before, err := r.readInputObservation(ctx)
		if err != nil {
			r.record(id, StatusNotRun, "input oracle was unavailable")
			return false
		}
		probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		probeStartedAt := r.config.Now()
		probeWindow := 10 * time.Second
		probeDeadline, hasProbeDeadline := probeCtx.Deadline()
		if hasProbeDeadline {
			probeWindow = time.Until(probeDeadline)
		}
		observerSession, err := StartKey(probeCtx, r.controlPort)
		deadlineExpired := errors.Is(probeCtx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded)
		var networkError net.Error
		if !deadlineExpired && hasProbeDeadline && errors.As(err, &networkError) && networkError.Timeout() {
			deadlineExpired = !time.Now().Before(probeDeadline)
		}
		cancel()
		if err != nil {
			if observerSession != nil {
				observerSession.Close()
			}
			if deadlineExpired {
				elapsed := r.config.Now().Sub(probeStartedAt)
				if elapsed < probeWindow {
					elapsed = probeWindow
				}
				r.recordReadinessTimeout(id, "key observer liveness control keystroke could not be sent", ReadinessEventKeyObserverAdvanced, probeWindow, elapsed)
			} else {
				r.record(id, StatusFail, r.runtimeDetail("key observer liveness RFB negotiation failed", err))
			}
			return false
		}
		observer, observerLive, window, elapsed, observerErr := r.waitInputObserverAdvance(ctx, before, observerSession)
		observerSession.Close()
		if observerErr != nil {
			r.record(id, StatusFail, r.runtimeDetail("key observer liveness probe failed", observerErr))
			return false
		}
		if !observerLive {
			r.recordReadinessTimeout(id, fmt.Sprintf("key observer did not advance after the control liveness keystroke (key_events=%d observer_lines=%d)", observer.KeyEvents, inputObserverLines(observer)), ReadinessEventKeyObserverAdvanced, window, elapsed)
			return false
		}
		r.observeReadinessEvent(ReadinessEventKeyObserverAdvanced)
		before = observer
		probeCtx, cancel = context.WithTimeout(ctx, 10*time.Second)
		viewSession, err := StartInput(probeCtx, r.viewPort, x, y)
		cancel()
		if err == nil {
			for dispatch := 0; dispatch < 3; dispatch++ {
				for poll := 0; poll < 4; poll++ {
					observation, readErr := r.readInputObservation(ctx)
					if readErr == nil && (observation.KeyEvents != before.KeyEvents || (observation.Generation > before.Generation && historyContains(observation, x, y))) {
						viewSession.Close()
						r.record(id, StatusFail, "view pointer or key input reached the guest before the control sentinel")
						return false
					}
					if r.config.Sleep(ctx, 125*time.Millisecond) != nil {
						break
					}
				}
				if dispatch < 2 {
					err = viewSession.SendInput(x, y)
					if err != nil {
						break
					}
				}
			}
		}
		var sentinelSession *InputSession
		if err == nil {
			probeCtx, cancel = context.WithTimeout(ctx, 10*time.Second)
			sentinelSession, err = StartPointer(probeCtx, r.controlPort, sx, sy)
			cancel()
		}
		if err != nil {
			if viewSession != nil {
				viewSession.Close()
			}
			r.record(id, StatusFail, "RFB view probe or control consumption barrier failed")
			return false
		}
		after, ok := r.waitInputSentinel(ctx, before.Generation, sx, sy)
		if after.KeyEvents != before.KeyEvents || (after.Generation > before.Generation && historyContains(after, x, y)) {
			viewSession.Close()
			sentinelSession.Close()
			r.record(id, StatusFail, "view pointer or key input reached the guest before the control sentinel")
			return false
		}
		if !ok {
			viewSession.Close()
			sentinelSession.Close()
			detail := "control sentinel was not observed after view input"
			if r.config.MutationProfile == "" {
				detail = fmt.Sprintf("%s (generation=%d key_events=%d pointer=%d,%d observer_lines=%d)", detail, after.Generation, after.KeyEvents, after.X, after.Y, inputObserverLines(after))
			}
			r.record(id, StatusFail, detail)
			return false
		}
		viewSession.Close()
		sentinelSession.Close()
	}
	r.record(id, StatusPass, "key-observer liveness and a control pointer sentinel proved the view input was consumed without guest input")
	return true
}

func (r *runtimeRunner) checkInput(ctx context.Context) {
	ids := []string{"input.view-isolated", "input.view-isolated-during-tenure", "input.control-accepted"}
	if r.config.InputOraclePath == "" {
		for _, id := range ids {
			r.record(id, StatusNotRun, "no --input-oracle-path was supplied")
		}
		return
	}
	if !r.requireTools(ids, "cat") {
		return
	}
	defer r.finalizeInputReadinessTimeouts()
	inputReady, window, elapsed, waitErr := r.waitInputReady(ctx)
	if waitErr != nil {
		r.record(ids[0], StatusFail, r.runtimeDetail("guest input observer wait failed", waitErr))
		return
	}
	if !inputReady {
		r.recordReadinessTimeout(ids[0], "guest input observer did not become ready", ReadinessEventInputOracleReady, window, elapsed)
		return
	}
	r.observeReadinessEvent(ReadinessEventInputOracleReady)
	if !r.proveViewIsolation(ctx, ids[0], 211, 173, 947, 411) {
		if !r.recorder.HasReadinessTimeout(ids[0]) {
			r.failed = true
			return
		}
	}
	if !r.mutateAndObserveDriver(ctx, `{"version":1,"human_driving":true}`, "valid", true).OK() {
		r.record(ids[1], StatusNotRun, "driver tenure oracle was unavailable")
		r.record(ids[2], StatusNotRun, "driver tenure oracle was unavailable")
		r.failed = true
		return
	}
	if !r.proveViewIsolation(ctx, ids[1], 337, 229, 901, 477) {
		if !r.recorder.HasReadinessTimeout(ids[1]) {
			r.failed = true
			_ = r.writeDriver(`{"version":1,"human_driving":false}`)
			return
		}
	}
	before, err := r.readInputObservation(ctx)
	if err != nil {
		r.record(ids[2], StatusNotRun, "input oracle was unavailable")
		r.failed = true
		return
	}
	controlObservation, controlObserved := r.sendControlInput(ctx, before.Generation, 1103, 389)
	if controlObserved {
		r.record(ids[2], StatusPass, "control pointer coordinates and key event were both observed")
	} else {
		r.record(ids[2], StatusFail, fmt.Sprintf("control pointer and key were not both observed (generation=%d key_events=%d pointer=%d,%d)", controlObservation.Generation, controlObservation.KeyEvents, controlObservation.X, controlObservation.Y))
	}
	_ = r.writeDriver(`{"version":1,"human_driving":false}`)
}

func (r *runtimeRunner) checkHarnessProfile(ctx context.Context) {
	imageUser, imageErr := r.runCommand(ctx, "image", "inspect", "--format", "{{.Config.User}}", r.config.Image)
	containerUser, containerErr := r.runCommand(ctx, "inspect", "--format", "{{.Config.User}}", r.containerID)
	if imageErr == nil && containerErr == nil && strings.TrimSpace(imageUser.stdout) == strings.TrimSpace(containerUser.stdout) {
		r.record("harness.image-user", StatusPass, "container retained the image USER")
	} else {
		r.record("harness.image-user", StatusFail, "container USER differed from image metadata")
	}
	configTemplate := "{{json .Config.Entrypoint}} {{json .Config.Cmd}} {{json .Config.WorkingDir}}"
	imageConfig, imageConfigErr := r.runCommand(ctx, "image", "inspect", "--format", configTemplate, r.config.Image)
	containerConfig, containerConfigErr := r.runCommand(ctx, "inspect", "--format", configTemplate, r.containerID)
	if imageConfigErr == nil && containerConfigErr == nil && imageConfig.stdout == containerConfig.stdout {
		r.record("runtime.image-config", StatusPass, "runtime did not replace image process metadata")
	} else {
		r.record("runtime.image-config", StatusFail, "runtime replaced image process metadata")
	}
	if !r.requireTools([]string{"harness.rootfs-read-only"}, "sh", "awk", "touch", "cat") {
	} else {
		mountReadOnly := r.execShell(ctx, `awk '$2 == "/" { n=split($4,a,","); for(i=1;i<=n;i++) if(a[i]=="ro") found=1 } END { exit !found }' /proc/mounts`).exitCode == 0
		result := r.execShell(ctx, `touch /wefty-conformance-rootfs`, "")
		switch {
		case mountReadOnly && result.exitCode != 0 && strings.Contains(result.stderr, "Read-only file system"):
			r.record("harness.rootfs-read-only", StatusPass, "root mount is ro and write failed with EROFS")
		case result.exitCode == 0:
			r.record("harness.rootfs-read-only", StatusFail, "rootfs write succeeded")
		case strings.Contains(result.stderr, "Permission denied"):
			r.record("harness.rootfs-read-only", StatusFail, "write failed with EACCES, not EROFS")
		default:
			r.record("harness.rootfs-read-only", StatusFail, "root mount/write evidence did not prove EROFS")
		}
	}
	if !r.requireTools([]string{"harness.service-writable", "harness.no-new-privileges"}, "sh", "awk") {
	} else {
		if r.execShell(ctx, `test -w /wefty/service`, "").exitCode == 0 {
			r.record("harness.service-writable", StatusPass, "service mount is tenant writable")
		} else {
			r.record("harness.service-writable", StatusFail, "service mount is not tenant writable")
		}
		if r.execShell(ctx, `awk '$1 == "NoNewPrivs:" { exit !($2 == 1) }' /proc/1/status`, "").exitCode == 0 {
			r.record("harness.no-new-privileges", StatusPass, "NoNewPrivs is 1")
		} else {
			r.record("harness.no-new-privileges", StatusFail, "NoNewPrivs is not 1")
		}
	}
	containerHostConfig, hostErr := r.runCommand(ctx, "inspect", "--format", "{{json .HostConfig.CapAdd}}", r.containerID)
	if hostErr != nil {
		r.record("harness.forbidden-privilege", StatusNotRun, "runtime did not expose CapAdd")
	} else if strings.Contains(strings.ToUpper(containerHostConfig.stdout), "SYS_ADMIN") {
		r.record("harness.forbidden-privilege", StatusFail, "forbidden SYS_ADMIN capability was added")
	} else {
		r.record("harness.forbidden-privilege", StatusPass, "no forbidden added capability observed")
	}
	ids := []string{"harness.shm-private", "harness.shm-size", "harness.shm-flags", "harness.tmp-ceilings"}
	if !r.requireTools(ids, "sh", "awk", "stat", "cat") {
		return
	}
	checks := []struct{ id, script, pass, fail string }{
		{"harness.shm-private", `awk '$2 == "/dev/shm" && $3 == "tmpfs" { found=1 } END { exit !found }' /proc/mounts`, "private tmpfs observed in guest /proc/mounts", "/dev/shm is not tmpfs"},
		{"harness.shm-size", `awk '$2 == "/dev/shm" && index($4,"size=1048576k") { found=1 } END { exit !found }' /proc/mounts`, "1 GiB ceiling observed in guest /proc/mounts", "/dev/shm ceiling is not 1 GiB"},
		{"harness.shm-flags", `test "$(stat -c %a /dev/shm)" = 1777 && awk '$2 == "/dev/shm" && index(","$4",",",nosuid,") && index(","$4",",",nodev,") && index(","$4",",",noexec,") { found=1 } END { exit !found }' /proc/mounts`, "mode and mount flags are exact", "/dev/shm mode or flags differ"},
		{"harness.tmp-ceilings", `awk '$2 == "/tmp" && index($4,"size=524288k") { tmp=1 } $2 == "/var/tmp" && index($4,"size=65536k") { vartmp=1 } END { exit !(tmp && vartmp) }' /proc/mounts`, "bounded /tmp and /var/tmp ceilings observed", "bounded /tmp or /var/tmp ceiling differs"},
	}
	for _, check := range checks {
		if r.execShell(ctx, check.script, "").exitCode == 0 {
			r.record(check.id, StatusPass, check.pass)
		} else {
			r.record(check.id, StatusFail, check.fail)
		}
	}
}

func (r *runtimeRunner) checkEdgeRecovery(ctx context.Context) {
	if r.config.EdgeProcessPattern == "" {
		r.record("persistence.edge-recovers", StatusNotRun, "no --edge-process-pattern was supplied")
		return
	}
	if !r.requireTools([]string{"persistence.edge-recovers"}, "sh", "cat", "tr") {
		return
	}
	holdCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	hold, err := OpenRFB(holdCtx, r.viewPort, contract.ComputerDisplayWebSocketPath)
	if err != nil {
		r.record("persistence.edge-recovers", StatusFail, "could not open edge withdrawal witness")
		r.recorder.RecordEdgeRecovery(false)
		return
	}
	script := `for p in /proc/[0-9]*; do test "${p##*/}" = "$$" && continue; cmd=$(tr '\000' ' ' < "$p/cmdline" 2>/dev/null || true); case "$cmd" in *"$1"*"$2"*) kill "${p##*/}"; exit 0;; esac; done; exit 1`
	result := r.execShell(ctx, script, r.config.EdgeProcessPattern, strconv.Itoa(r.viewPort))
	if result.exitCode != 0 {
		hold.close()
		r.record("persistence.edge-recovers", StatusFail, "named edge process could not be killed")
		r.recorder.RecordEdgeRecovery(false)
		return
	}
	_ = hold.connection.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _, withdrawalErr := hold.readFrame()
	hold.close()
	if withdrawalErr == nil {
		r.record("persistence.edge-recovers", StatusFail, "existing edge session did not withdraw after process loss")
		r.recorder.RecordEdgeRecovery(false)
		return
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		view, viewErr := OpenRFB(ctx, r.viewPort, contract.ComputerDisplayWebSocketPath)
		if viewErr == nil {
			view.close()
		}
		control, controlErr := OpenRFB(ctx, r.controlPort, contract.ComputerDisplayWebSocketPath)
		if controlErr == nil {
			control.close()
		}
		if viewErr == nil && controlErr == nil {
			r.record("persistence.edge-recovers", StatusPass, "edge loss closed the live session and fresh success recovered both endpoints")
			r.recorder.RecordEdgeRecovery(true)
			return
		}
		_ = r.config.Sleep(ctx, 250*time.Millisecond)
	}
	r.record("persistence.edge-recovers", StatusFail, "both endpoints did not recover after edge withdrawal")
	r.recorder.RecordEdgeRecovery(false)
}

func (r *runtimeRunner) checkPersistence(ctx context.Context) error {
	ids := []string{"persistence.service-survives", "persistence.profile-survives", "persistence.sign-in-survives", "persistence.rootfs-discarded", "targets.control-nonpersistent"}
	if !r.requireTools(ids, "sh", "touch", "cat") {
		return nil
	}
	plant := `test -n "$HOME" && mkdir -p "$HOME/.config/wefty-conformance" "$HOME/.local/share/wefty-conformance" && printf profile > "$HOME/.config/wefty-conformance/profile" && printf sign-in > "$HOME/.local/share/wefty-conformance/sign-in" && printf persistent > /wefty/service/.wefty-conformance-persistent && printf attempt > /tmp/.wefty-conformance-attempt && printf shm > /dev/shm/.wefty-conformance-shm`
	if result := r.execShell(ctx, plant, ""); result.exitCode != 0 {
		for _, id := range ids {
			r.record(id, StatusFail, "could not plant persistence assertions")
		}
		return errors.New("plant persistence assertions")
	}
	if err := r.stopContainer(context.Background()); err != nil {
		return err
	}
	if err := r.allocatePorts(); err != nil {
		return err
	}
	startedAt := r.config.Now()
	if err := r.startContainer(ctx); err != nil {
		return fmt.Errorf("restart container: %w", err)
	}
	if err := r.waitReady(ctx, startedAt, true); err != nil {
		return fmt.Errorf("restart readiness: %w", err)
	}
	checks := []struct{ id, script, pass, fail string }{
		{"persistence.service-survives", `test "$(cat /wefty/service/.wefty-conformance-persistent)" = persistent`, "service marker survived a fresh attempt", "service marker did not survive"},
		{"persistence.profile-survives", `test "$(cat "$HOME/.config/wefty-conformance/profile")" = profile`, "profile marker under HOME survived restart", "profile marker under HOME was lost"},
		{"persistence.sign-in-survives", `test "$(cat "$HOME/.local/share/wefty-conformance/sign-in")" = sign-in`, "sign-in marker under HOME survived restart", "sign-in marker under HOME was lost"},
		{"persistence.rootfs-discarded", `test ! -e /tmp/.wefty-conformance-attempt && test ! -e /dev/shm/.wefty-conformance-shm`, "attempt-local tmpfs state was absent", "attempt-local state survived"},
		{"targets.control-nonpersistent", `test ! -e /wefty/service/driver.json && test ! -e /wefty/service/control`, "control state was absent from service data", "control state leaked into service data"},
	}
	profile, signIn := false, false
	for _, check := range checks {
		if r.execShell(ctx, check.script, "").exitCode == 0 {
			r.record(check.id, StatusPass, check.pass)
			if check.id == "persistence.profile-survives" {
				profile = true
			}
			if check.id == "persistence.sign-in-survives" {
				signIn = true
			}
		} else {
			r.record(check.id, StatusFail, check.fail)
		}
	}
	r.recorder.RecordPersistence(profile, signIn)
	return nil
}

func (r *runtimeRunner) readContainerFile(ctx context.Context, path string) (string, error) {
	if path == "" || !filepath.IsAbs(path) || strings.ContainsRune(path, '\x00') {
		return "", errors.New("oracle path must be absolute")
	}
	result, err := r.runCommand(ctx, "exec", r.containerID, "cat", path)
	return result.stdout, err
}

func (r *runtimeRunner) writeDriver(document string) error {
	temporary := filepath.Join(r.controlDir, "driver.json.new")
	_ = os.Remove(temporary)
	if err := os.WriteFile(temporary, []byte(document), 0o444); err != nil {
		return err
	}
	if err := os.Chmod(temporary, 0o444); err != nil {
		return err
	}
	if err := os.Rename(temporary, filepath.Join(r.controlDir, "driver.json")); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	return nil
}

type commandResult struct {
	stdout, stderr string
	exitCode       int
}

func (r *runtimeRunner) runCommand(ctx context.Context, arguments ...string) (commandResult, error) {
	if r.runCommandHook != nil {
		return r.runCommandHook(ctx, arguments...)
	}
	command := exec.CommandContext(ctx, r.config.Runtime, arguments...)
	var stdout, stderr strings.Builder
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	result := commandResult{stdout: stdout.String(), stderr: stderr.String()}
	if err == nil {
		return result, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		result.exitCode = exitErr.ExitCode()
	} else {
		result.exitCode = -1
	}
	return result, err
}
func (r *runtimeRunner) execShell(ctx context.Context, script string, arguments ...string) commandResult {
	args := []string{"exec", r.containerID, "sh", "-c", script, "wefty-conformance"}
	args = append(args, arguments...)
	result, _ := r.runCommand(ctx, args...)
	return result
}

var portPattern = regexp.MustCompile(`\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}:[0-9]{1,5}\b|\b[0-9]{4,5}\b`)

func (r *runtimeRunner) runtimeDetail(prefix string, err error) string {
	if err == nil {
		return prefix
	}
	detail := strings.TrimSpace(portPattern.ReplaceAllString(err.Error(), "<endpoint>"))
	if len(detail) > 512 {
		detail = detail[:512]
	}
	return prefix + ": " + detail
}
func errWithOutput(err error, output commandResult) error {
	if strings.TrimSpace(output.stderr) == "" {
		return err
	}
	return fmt.Errorf("%w: %s", err, strings.TrimSpace(output.stderr))
}
func (r *runtimeRunner) record(id string, status Status, detail string) {
	reason := FailureReason("")
	if status == StatusFail {
		reason = FailureAssertionFailed
		if mutationFailureCells[r.config.MutationProfile] == id {
			reason = FailureMutationDetected
		}
	}
	if err := r.recorder.RecordFailure(id, status, detail, reason, ""); err != nil {
		panic(err)
	}
	if status == StatusFail {
		r.failed = true
	}
}

func (r *runtimeRunner) recordReadinessTimeout(id, detail string, event ReadinessEvent, window, elapsed time.Duration) {
	if mutationFailureCells[r.config.MutationProfile] == id {
		r.record(id, StatusFail, detail)
		return
	}
	if err := r.recorder.RecordReadinessTimeout(id, detail, event, window, elapsed); err != nil {
		panic(err)
	}
}

func (r *runtimeRunner) observeReadinessEvent(event ReadinessEvent) {
	r.recorder.ObserveReadinessEvent(event)
}

func (r *runtimeRunner) finalizeInputReadinessTimeouts() {
	for _, id := range []string{"input.view-isolated", "input.view-isolated-during-tenure"} {
		relabelled, err := r.recorder.RelabelUnobservedReadinessTimeout(id)
		if err != nil {
			panic(err)
		}
		if relabelled {
			r.failed = true
		}
	}
}

func (r *runtimeRunner) stopContainer(ctx context.Context) error {
	if r.containerID == "" {
		return nil
	}
	id := r.containerID
	// Ask the already-running tenant to make only its own bind content removable.
	_ = r.execShell(ctx, `chmod -R u+rwX,go+rwX /wefty/service 2>/dev/null || true`, "")
	result, stopErr := r.runCommand(ctx, "stop", "--time", strconv.Itoa(int(containerStopGrace/time.Second)), id)
	if stopErr != nil {
		detail := r.runtimeDetail("runtime stop did not confirm exit", errWithOutput(stopErr, result))
		r.teardownObserve(TeardownContainerStopFailed, detail)
	}

	removeResult, rmErr := r.runCommand(ctx, "rm", "--force", id)
	if rmErr != nil {
		r.containerLeftoverConfirmed = false
		inspectResult, inspectErr := r.runCommand(ctx, "inspect", id)
		if inspectErr == nil {
			r.containerLeftoverConfirmed = true
		} else if runtimeObjectAbsent(inspectResult) {
			r.teardownObserve(TeardownContainerDetachFailed, r.runtimeDetail("runtime remove failed but inspect proved container absent", errWithOutput(rmErr, removeResult)))
			r.containerID = ""
			return nil
		}
		leftovers := make([]string, 0, 2)
		if r.containerLeftoverConfirmed {
			leftovers = append(leftovers, "container:"+id)
		}
		leftovers = append(leftovers, "temporary-root:"+r.root)
		return &TeardownFailure{Reason: TeardownContainerDetachFailed,
			Leftover: strings.Join(leftovers, ","),
			Err:      errors.New(r.runtimeDetail("runtime remove did not detach mounts", errWithOutput(rmErr, removeResult)))}
	}
	r.containerID = ""
	r.containerLeftoverConfirmed = false
	return nil
}

func runtimeObjectAbsent(result commandResult) bool {
	detail := strings.ToLower(result.stdout + "\n" + result.stderr)
	return strings.Contains(detail, "no such object") ||
		strings.Contains(detail, "no such container")
}

func (r *runtimeRunner) cleanup() error {
	defer r.recordTeardownLeftovers()
	ctx := context.Background()
	err := r.stopContainer(ctx)
	if r.containerID != "" || r.root == "" {
		return err
	}
	return errors.Join(err, r.removeTemporaryRoot(ctx))
}

func (r *runtimeRunner) removeTemporaryRoot(ctx context.Context) error {
	root := r.root
	if r.config.MutationProfile == teardownPermissionFixture {
		fixture := filepath.Join(root, "service", "teardown-permission-fixture")
		if err := os.MkdirAll(fixture, 0o755); err != nil {
			return &TeardownFailure{Reason: TeardownTemporaryRootRemove, Leftover: "temporary-root:" + root, Err: fmt.Errorf("prepare permission-repair fixture: %w", err)}
		}
		if err := os.WriteFile(filepath.Join(fixture, "late-write"), []byte("fixture"), 0o600); err != nil {
			return &TeardownFailure{Reason: TeardownTemporaryRootRemove, Leftover: "temporary-root:" + root, Err: fmt.Errorf("prepare permission-repair fixture: %w", err)}
		}
		if err := os.Chmod(fixture, 0); err != nil {
			return &TeardownFailure{Reason: TeardownTemporaryRootRemove, Leftover: "temporary-root:" + root, Err: fmt.Errorf("prepare permission-repair fixture: %w", err)}
		}
	}
	permissionRepairAttempted := false
	removeRetries := 0
	removeRetryLimit := int(teardownRemoveRetryBudget / teardownRemoveRetryInterval)
	for {
		removeErr := r.removeAll(root)
		if removeErr == nil {
			r.root = ""
			r.teardownLog(fmt.Sprintf("teardown complete: removed=%q permission_repair=%t removal_retries=%d", root, permissionRepairAttempted, removeRetries))
			return nil
		}
		if errors.Is(removeErr, os.ErrPermission) && !permissionRepairAttempted {
			permissionRepairAttempted = true
			r.teardownLog(fmt.Sprintf("teardown repair: reason=temporary_root_permission target=%q", root))
			if repairErr := r.repairTemporaryRootPermissions(ctx); repairErr != nil {
				return &TeardownFailure{Reason: TeardownPermissionRepairFailed, Leftover: "temporary-root:" + root, Err: repairErr}
			}
			continue
		}
		retryReason := TeardownFailureReason("")
		switch {
		case errors.Is(removeErr, syscall.EBUSY):
			retryReason = TeardownTemporaryRootBusy
		case errors.Is(removeErr, syscall.ENOTEMPTY):
			retryReason = TeardownTemporaryRootNotEmpty
		}
		if retryReason != "" && removeRetries < removeRetryLimit {
			removeRetries++
			detail := fmt.Sprintf("target=%q retry=%d/%d budget=%s", root, removeRetries, removeRetryLimit, teardownRemoveRetryBudget)
			r.teardownRetry(retryReason, detail)
			if sleepErr := r.teardownSleep(ctx, teardownRemoveRetryInterval); sleepErr != nil {
				return &TeardownFailure{Reason: retryReason, Leftover: "temporary-root:" + root, Err: errors.Join(removeErr, sleepErr)}
			}
			continue
		}
		reason := TeardownTemporaryRootRemove
		if retryReason != "" {
			reason = retryReason
		}
		return &TeardownFailure{Reason: reason, Leftover: "temporary-root:" + root, Err: removeErr}
	}
}

func (r *runtimeRunner) repairTemporaryRootPermissions(ctx context.Context) error {
	repairCtx, cancel := context.WithTimeout(ctx, permissionRepairBudget)
	defer cancel()
	startedAt := time.Now()
	arguments := []string{
		"run", "--rm", "--network", "none", "--user", "0:0", "--read-only",
		"--security-opt", "no-new-privileges:true", "--cap-drop", "ALL",
		"--cap-add", "DAC_OVERRIDE", "--cap-add", "FOWNER",
		"--mount", "type=bind,src=" + r.root + ",dst=/wefty-cleanup",
	}
	if r.config.Platform != "" {
		arguments = append(arguments, "--platform", r.config.Platform)
	}
	arguments = append(arguments, "--entrypoint", "/bin/sh", r.config.RepairImage, "-c", permissionRepairScript, "wefty-repair", "/wefty-cleanup")
	result, err := r.runCommand(repairCtx, arguments...)
	duration := time.Since(startedAt)
	if r.recorder != nil {
		r.recorder.RecordPermissionRepair(duration)
	}
	r.teardownLog(fmt.Sprintf("teardown repair complete: duration=%s budget=%s", duration.Round(time.Millisecond), permissionRepairBudget))
	if err != nil {
		return errors.New(r.runtimeDetail("detached permission repair failed", errWithOutput(err, result)))
	}
	return nil
}

func (r *runtimeRunner) removeAll(path string) error {
	if r.removeAllHook != nil {
		return r.removeAllHook(path)
	}
	return os.RemoveAll(path)
}

func (r *runtimeRunner) teardownSleep(ctx context.Context, duration time.Duration) error {
	if r.config.Sleep != nil {
		return r.config.Sleep(ctx, duration)
	}
	return sleepContext(ctx, duration)
}

func (r *runtimeRunner) teardownLog(message string) {
	if r.teardownLogHook != nil {
		r.teardownLogHook(message)
		return
	}
	fmt.Fprintln(os.Stderr, message)
}

func (r *runtimeRunner) teardownObserve(reason TeardownFailureReason, detail string) {
	if r.recorder != nil {
		r.recorder.RecordTeardownObservation(string(reason), detail)
	}
	r.teardownLog(fmt.Sprintf("teardown observation: reason=%s detail=%q", reason, detail))
}

func (r *runtimeRunner) teardownRetry(reason TeardownFailureReason, detail string) {
	if r.recorder != nil {
		r.recorder.RecordTeardownRetry(string(reason), detail)
	}
	r.teardownLog(fmt.Sprintf("teardown retry: reason=%s %s", reason, detail))
}

func (r *runtimeRunner) recordTeardownLeftovers() {
	if r.recorder == nil {
		return
	}
	leftovers := make([]string, 0, 2)
	if r.containerID != "" && r.containerLeftoverConfirmed {
		leftovers = append(leftovers, "container:"+r.containerID)
	}
	if r.root != "" {
		leftovers = append(leftovers, "temporary-root:"+r.root)
	}
	r.recorder.RecordTeardownLeftovers(leftovers)
}

func availablePort() (int, error) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}
func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
