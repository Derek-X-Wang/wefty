package computerconformance

import (
	"context"
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
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

type RuntimeConfig struct {
	Image              string
	Runtime            string
	Platform           string
	InputOraclePath    string
	DriverOraclePath   string
	EdgeProcessPattern string
	ReceiptPath        string
	// MutationProfile is used only by the repository's broken-image lane to
	// mutate Docker's approximation of the production profile.
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
	viewPort, controlPort, attempt           int
	tools                                    map[string]bool
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

func (r *runtimeRunner) run(ctx context.Context) error {
	if r.config.Image == "" {
		return errors.New("--image is required")
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
	defer r.cleanup()
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
	if r.config.MutationProfile == "duplicate-endpoint" {
		r.record("endpoints.distinct", StatusFail, "view and control received the same attempt-local port")
		return errors.New("fixture returned duplicate endpoint authority after the image ran")
	}
	r.discoverTools(ctx)
	r.markContainerdProfileNotRun()
	r.checkEnvironment(ctx)
	r.checkTargets(ctx)
	r.checkLoopback(ctx)
	r.checkTransportNegatives(ctx)
	r.checkDriver(ctx)
	r.checkInput(ctx)
	r.checkHarnessProfile(ctx)
	r.checkEdgeRecovery(ctx)
	return r.checkPersistence(ctx)
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
	viewReady, controlReady, plainTCP := false, false, false
	var viewProbeErr, controlProbeErr error
	for BeforeReadinessDeadline(r.config.Now(), startedAt) {
		if !viewReady {
			viewReady, plainTCP, viewProbeErr = r.probeReady(ctx, r.viewPort, plainTCP)
		}
		if !controlReady {
			controlReady, plainTCP, controlProbeErr = r.probeReady(ctx, r.controlPort, plainTCP)
		}
		if viewReady && controlReady {
			r.record("transport.view-ready", StatusPass, "binary RFB greeting observed")
			r.record("transport.control-ready", StatusPass, "binary RFB greeting observed")
			r.record("transport.plain-tcp-rejected", StatusPass, "both endpoints required the exact WebSocket upgrade")
			r.record("readiness.before-deadline", StatusPass, "both exact endpoints became ready before t0 + 60s")
			r.recorder.RecordReadiness(restart, r.config.Now().Sub(startedAt))
			return nil
		}
		if (r.config.MutationProfile == "missing-control-endpoint" || r.config.MutationProfile == "missing-view-endpoint") && r.config.Now().Sub(startedAt) >= 5*time.Second {
			break
		}
		if plainTCP && viewReady {
			break
		}
		if err := r.config.Sleep(ctx, 250*time.Millisecond); err != nil {
			return err
		}
	}
	if plainTCP {
		r.record("transport.plain-tcp-rejected", StatusFail, "endpoint accepted TCP but did not complete the required WebSocket upgrade")
		return errors.New("plain TCP is not rfb-websocket-v1 readiness")
	}
	if r.config.MutationProfile == "readiness-over-60s" {
		r.record("readiness.before-deadline", StatusFail, "endpoint pair was not ready before t0 + 60s")
		return errors.New("Computer startup readiness timeout")
	}
	if !viewReady {
		r.record("transport.view-ready", StatusFail, "view never completed rfb-websocket-v1")
		if viewProbeErr != nil {
			return fmt.Errorf("view endpoint readiness timeout: %w", viewProbeErr)
		}
		return errors.New("view endpoint readiness timeout")
	}
	r.record("transport.view-ready", StatusPass, "binary RFB greeting observed")
	if !controlReady {
		r.record("transport.control-ready", StatusFail, "control never completed rfb-websocket-v1")
		if controlProbeErr != nil {
			return fmt.Errorf("control endpoint readiness timeout: %w", controlProbeErr)
		}
		return errors.New("control endpoint readiness timeout")
	}
	return errors.New("Computer startup readiness timeout")
}

func (r *runtimeRunner) probeReady(ctx context.Context, port int, plainSeen bool) (bool, bool, error) {
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	connection, err := OpenRFB(probeCtx, port, contract.ComputerDisplayWebSocketPath)
	if err == nil {
		connection.close()
		return true, plainSeen, nil
	}
	dialer := net.Dialer{Timeout: 250 * time.Millisecond}
	tcp, tcpErr := dialer.DialContext(probeCtx, "tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if tcpErr == nil {
		plainSeen = true
		_ = tcp.Close()
	}
	return false, plainSeen, err
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
	Version      int    `json:"version"`
	HumanDriving bool   `json:"human_driving"`
	Generation   uint64 `json:"generation"`
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

func (r *runtimeRunner) waitDriverAdvance(ctx context.Context, after uint64, expected bool) bool {
	for attempt := 0; attempt < 40; attempt++ {
		value, err := r.readDriverObservation(ctx)
		if err == nil && value.Generation > after {
			return value.HumanDriving == expected
		}
		if r.config.Sleep(ctx, 250*time.Millisecond) != nil {
			return false
		}
	}
	return false
}

func (r *runtimeRunner) mutateAndObserveDriver(ctx context.Context, document string, expected bool) bool {
	previous, err := r.readDriverObservation(ctx)
	if err != nil {
		return false
	}
	if err := r.writeDriver(document); err != nil {
		return false
	}
	return r.waitDriverAdvance(ctx, previous.Generation, expected)
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
	if r.mutateAndObserveDriver(ctx, `{"version":1,"human_driving":true}`, true) {
		r.record("driver.true-consumed", StatusPass, "tenant observed the new generation and consumed true")
	} else {
		r.record("driver.true-consumed", StatusFail, "tenant ignored the true driver generation")
	}
	if r.mutateAndObserveDriver(ctx, falseDocument, false) {
		r.record("driver.release-consumed", StatusPass, "tenant observed the new generation and consumed release")
	} else {
		r.record("driver.release-consumed", StatusFail, "tenant did not consume the release generation")
	}
	malformed := true
	for _, document := range []string{`{"version":true,"human_driving":true}`, `{"version":1,"human_driving":1}`, `{malformed`} {
		if !r.mutateAndObserveDriver(ctx, document, false) {
			malformed = false
			break
		}
	}
	if malformed {
		r.record("driver.malformed-fails-closed", StatusPass, "each malformed generation advanced and remained false")
	} else {
		r.record("driver.malformed-fails-closed", StatusFail, "a malformed generation was accepted or not observed")
		return
	}
	if r.mutateAndObserveDriver(ctx, `{"version":2,"human_driving":true}`, false) {
		r.record("driver.unknown-version-fails-closed", StatusPass, "unknown-version generation advanced and remained false")
	} else {
		r.record("driver.unknown-version-fails-closed", StatusFail, "unknown-version generation was accepted or not observed")
		return
	}
	previous, readErr := r.readDriverObservation(ctx)
	missing := filepath.Join(r.controlDir, "driver.json.missing")
	_ = os.Remove(missing)
	renameErr := os.Rename(filepath.Join(r.controlDir, "driver.json"), missing)
	if readErr == nil && renameErr == nil && r.waitDriverAdvance(ctx, previous.Generation, false) {
		r.record("driver.missing-fails-closed", StatusPass, "missing generation advanced and remained false")
	} else {
		r.record("driver.missing-fails-closed", StatusFail, "missing document was not observed fail-closed")
	}
	_ = os.Remove(missing)
	_ = r.writeDriver(falseDocument)
}

type inputObservation struct {
	Version        int      `json:"version"`
	Generation     uint64   `json:"generation"`
	KeyEvents      uint64   `json:"key_events"`
	X              int      `json:"x"`
	Y              int      `json:"y"`
	PointerHistory [][2]int `json:"pointer_history"`
	ObserverLines  uint64   `json:"observer_lines"`
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

func (r *runtimeRunner) proveViewIsolation(ctx context.Context, id string, targetX, targetY, sentinelX, sentinelY int) bool {
	before, err := r.readInputObservation(ctx)
	if err != nil {
		r.record(id, StatusNotRun, "input oracle was unavailable")
		return false
	}
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	viewSession, err := StartInput(probeCtx, r.viewPort, targetX, targetY)
	cancel()
	var sentinelSession *InputSession
	if err == nil {
		probeCtx, cancel = context.WithTimeout(ctx, 10*time.Second)
		sentinelSession, err = StartPointer(probeCtx, r.controlPort, sentinelX, sentinelY)
		cancel()
	}
	if err != nil {
		if viewSession != nil {
			viewSession.Close()
		}
		r.record(id, StatusFail, "RFB view probe or control consumption barrier failed")
		return false
	}
	after, ok := r.waitInputSentinel(ctx, before.Generation, sentinelX, sentinelY)
	viewSession.Close()
	sentinelSession.Close()
	if !ok {
		detail := "control sentinel was not observed after view input"
		if r.config.MutationProfile == "" {
			detail = fmt.Sprintf("%s (generation=%d key_events=%d pointer=%d,%d observer_lines=%d)", detail, after.Generation, after.KeyEvents, after.X, after.Y, after.ObserverLines)
		}
		r.record(id, StatusFail, detail)
		return false
	}
	if after.KeyEvents != before.KeyEvents || historyContains(after, targetX, targetY) {
		r.record(id, StatusFail, "view pointer or key input reached the guest before the control sentinel")
		return false
	}
	r.record(id, StatusPass, "control sentinel proved the preceding view pointer and key were consumed without guest input")
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
	if !r.proveViewIsolation(ctx, ids[0], 211, 173, 947, 411) {
		return
	}
	if !r.mutateAndObserveDriver(ctx, `{"version":1,"human_driving":true}`, true) {
		r.record(ids[1], StatusNotRun, "driver tenure oracle was unavailable")
		r.record(ids[2], StatusNotRun, "driver tenure oracle was unavailable")
		return
	}
	if !r.proveViewIsolation(ctx, ids[1], 337, 229, 901, 477) {
		_ = r.writeDriver(`{"version":1,"human_driving":false}`)
		return
	}
	before, err := r.readInputObservation(ctx)
	if err != nil {
		r.record(ids[2], StatusNotRun, "input oracle was unavailable")
		return
	}
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	controlSession, err := StartInput(probeCtx, r.controlPort, 503, 389)
	cancel()
	if err != nil {
		r.record(ids[2], StatusFail, "control input probe failed")
		return
	}
	observed := false
	for attempt := 0; attempt < 80; attempt++ {
		after, readErr := r.readInputObservation(ctx)
		if readErr == nil && after.Generation > before.Generation && after.KeyEvents > before.KeyEvents && historyContains(after, 503, 389) {
			observed = true
			break
		}
		if r.config.Sleep(ctx, 125*time.Millisecond) != nil {
			break
		}
	}
	controlSession.Close()
	if observed {
		r.record(ids[2], StatusPass, "control pointer coordinates and key event were both observed")
	} else {
		r.record(ids[2], StatusFail, "control pointer and key were not both observed")
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
	if err := r.recorder.Record(id, status, detail); err != nil {
		panic(err)
	}
}

func (r *runtimeRunner) stopContainer(ctx context.Context) error {
	if r.containerID == "" {
		return nil
	}
	id := r.containerID
	// Ask the already-running tenant to make only its own bind content removable.
	_ = r.execShell(ctx, `chmod -R u+rwX,go+rwX /wefty/service 2>/dev/null || true`, "")
	result, stopErr := r.runCommand(ctx, "stop", "--time", "15", id)
	_, rmErr := r.runCommand(ctx, "rm", "--force", id)
	r.containerID = ""
	if stopErr != nil {
		return fmt.Errorf("stop container: %s", r.runtimeDetail("runtime stop failed", errWithOutput(stopErr, result)))
	}
	return rmErr
}
func (r *runtimeRunner) cleanup() {
	_ = r.stopContainer(context.Background())
	if r.root != "" {
		_ = os.RemoveAll(r.root)
	}
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
