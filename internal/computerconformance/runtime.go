package computerconformance

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

type RuntimeConfig struct {
	Image            string
	Runtime          string
	Platform         string
	InputOraclePath  string
	DriverOraclePath string
	ReceiptPath      string
	Now              func() time.Time
	Sleep            func(context.Context, time.Duration) error
}

type RuntimeResult struct {
	Receipt Receipt
	Err     error
}

type runtimeRunner struct {
	config      RuntimeConfig
	recorder    *Recorder
	root        string
	serviceDir  string
	controlDir  string
	containerID string
	viewPort    int
	controlPort int
}

func Run(ctx context.Context, config RuntimeConfig) RuntimeResult {
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.Sleep == nil {
		config.Sleep = sleepContext
	}
	startedAt := config.Now()
	runner := &runtimeRunner{config: config, recorder: NewRecorder(config.Image, config.Runtime, config.Platform, startedAt)}
	err := runner.run(ctx, startedAt)
	receipt := runner.recorder.Finish(config.Now())
	return RuntimeResult{Receipt: receipt, Err: err}
}

func (runner *runtimeRunner) run(ctx context.Context, startedAt time.Time) error {
	if runner.config.Image == "" {
		return errors.New("--image is required")
	}
	if runner.config.Runtime != "docker" && runner.config.Runtime != "nerdctl" {
		return errors.New("--runtime must be docker or nerdctl")
	}
	if _, err := exec.LookPath(runner.config.Runtime); err != nil {
		return fmt.Errorf("runtime unavailable: %w", err)
	}
	root, err := os.MkdirTemp("", "wefty-computer-conformance-")
	if err != nil {
		return err
	}
	runner.root = root
	defer runner.cleanup()
	runner.serviceDir = filepath.Join(root, "service")
	runner.controlDir = filepath.Join(root, "control")
	for _, directory := range []string{runner.serviceDir, runner.controlDir} {
		if err := os.Mkdir(directory, 0o777); err != nil {
			return err
		}
		if err := os.Chmod(directory, 0o777); err != nil {
			return err
		}
	}
	if err := runner.writeDriver(`{"version":1,"human_driving":false}`); err != nil {
		return err
	}
	runner.viewPort, err = availablePort()
	if err != nil {
		return err
	}
	runner.controlPort, err = availablePort()
	if err != nil {
		return err
	}
	if runner.viewPort == runner.controlPort {
		runner.record("endpoints.distinct", StatusFail, "runtime allocated duplicate endpoint authority")
		return errors.New("duplicate allocated endpoint")
	}
	runner.record("endpoints.distinct", StatusPass, "two distinct attempt-local ports allocated")

	containerStartedAt := runner.config.Now()
	if err := runner.startContainer(ctx); err != nil {
		runner.record("runtime.started", StatusFail, "image did not start under the closed profile")
		return err
	}
	runner.record("runtime.started", StatusPass, "container task started")
	if err := runner.waitReady(ctx, containerStartedAt); err != nil {
		return err
	}
	runner.checkEnvironment(ctx)
	runner.checkLoopback(ctx)
	runner.checkTransportNegatives(ctx)
	runner.checkInput(ctx)
	runner.checkDriver(ctx)
	runner.checkProfile(ctx)
	if err := runner.checkPersistence(ctx); err != nil {
		return err
	}
	_ = startedAt // receipt start includes preflight; readiness uses task start above.
	return nil
}

func (runner *runtimeRunner) startContainer(ctx context.Context) error {
	name := fmt.Sprintf("wefty-computer-conformance-%d", os.Getpid())
	args := []string{"run", "--detach", "--rm", "--name", name}
	if runner.config.Platform != "" {
		args = append(args, "--platform", runner.config.Platform)
	}
	args = append(args,
		"--network", "host", "--read-only",
		"--security-opt", "no-new-privileges:true",
		"--cap-drop", "ALL",
	)
	for _, capability := range []string{"CHOWN", "DAC_OVERRIDE", "FSETID", "FOWNER", "MKNOD", "SETGID", "SETUID", "SETFCAP", "SETPCAP", "SYS_CHROOT", "KILL", "AUDIT_WRITE"} {
		args = append(args, "--cap-add", capability)
	}
	args = append(args,
		"--tmpfs", "/tmp:rw,nosuid,nodev,size=536870912,mode=1777",
		"--tmpfs", "/var/tmp:rw,nosuid,nodev,size=67108864,mode=1777",
		"--tmpfs", "/run:rw,nosuid,nodev,size=67108864,mode=0755",
		"--tmpfs", "/dev/shm:rw,nosuid,nodev,noexec,size=1073741824,mode=1777",
		"--mount", "type=bind,src="+runner.serviceDir+",dst=/wefty/service",
		"--mount", "type=bind,src="+runner.controlDir+",dst=/wefty/control,readonly",
		"--env", contract.EnvServiceDir+"="+contract.OCIContainerServiceDirectory,
		"--env", contract.EnvComputerViewPort+"="+strconv.Itoa(runner.viewPort),
		"--env", contract.EnvComputerControlPort+"="+strconv.Itoa(runner.controlPort),
		"--env", "WEFTY_CONFORMANCE_TENANT_VALUE=preserved",
		runner.config.Image,
	)
	output, err := runner.command(ctx, args...).Output()
	if err != nil {
		return fmt.Errorf("start container: %w", err)
	}
	runner.containerID = strings.TrimSpace(string(output))
	if runner.containerID == "" {
		return errors.New("runtime returned an empty container id")
	}
	return nil
}

func (runner *runtimeRunner) waitReady(ctx context.Context, startedAt time.Time) error {
	viewReady, controlReady := false, false
	for BeforeReadinessDeadline(runner.config.Now(), startedAt) {
		probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		if !viewReady {
			connection, err := OpenRFB(probeCtx, runner.viewPort, contract.ComputerDisplayWebSocketPath)
			if err == nil {
				connection.close()
				viewReady = true
			}
		}
		cancel()
		probeCtx, cancel = context.WithTimeout(ctx, 3*time.Second)
		if !controlReady {
			connection, err := OpenRFB(probeCtx, runner.controlPort, contract.ComputerDisplayWebSocketPath)
			if err == nil {
				connection.close()
				controlReady = true
			}
		}
		cancel()
		if viewReady && controlReady {
			if !BeforeReadinessDeadline(runner.config.Now(), startedAt) {
				break
			}
			runner.record("transport.view-ready", StatusPass, "binary RFB greeting observed")
			runner.record("transport.control-ready", StatusPass, "binary RFB greeting observed")
			runner.record("readiness.before-deadline", StatusPass, "both exact endpoints became ready before t0 + 60s")
			runner.recorder.RecordReadiness(runner.config.Now().Sub(startedAt))
			return nil
		}
		if err := runner.config.Sleep(ctx, 250*time.Millisecond); err != nil {
			return err
		}
	}
	if viewReady {
		runner.record("transport.view-ready", StatusPass, "binary RFB greeting observed")
	} else {
		runner.record("transport.view-ready", StatusFail, "view never completed rfb-websocket-v1")
	}
	if controlReady {
		runner.record("transport.control-ready", StatusPass, "binary RFB greeting observed")
	} else {
		runner.record("transport.control-ready", StatusFail, "control never completed rfb-websocket-v1")
	}
	runner.record("readiness.before-deadline", StatusFail, "the endpoint pair was not ready before t0 + 60s")
	return errors.New("Computer startup readiness timeout")
}

// BeforeReadinessDeadline is the publication edge shared by runtime polling
// and injected-clock tests. Success at exactly t0 + 60 seconds is too late.
func BeforeReadinessDeadline(now, startedAt time.Time) bool {
	return now.Before(startedAt.Add(contract.ComputerStartupReadinessTimeout))
}

func (runner *runtimeRunner) checkEnvironment(ctx context.Context) {
	checks := []struct {
		id      string
		command string
	}{
		{"environment.service-dir", `test "${WEFTY_SERVICE_DIR-}" = /wefty/service`},
		{"environment.view-port", `test "${WEFTY_COMPUTER_VIEW_PORT-}" = "$1"`},
		{"environment.control-port", `test "${WEFTY_COMPUTER_CONTROL_PORT-}" = "$1"`},
		{"environment.service-port-omitted", `test "${WEFTY_SERVICE_PORT+x}" != x`},
		{"environment.authority-omitted", `test "${WEFTY_COMPUTER_TOKEN+x}" != x && test "${WEFTY_L3_ENDPOINT+x}" != x && test "${WEFTY_RUN_TOKEN+x}" != x`},
		{"environment.other-wefty-preserved", `test "${WEFTY_CONFORMANCE_TENANT_VALUE-}" = preserved`},
	}
	for _, check := range checks {
		argument := ""
		switch check.id {
		case "environment.view-port":
			argument = strconv.Itoa(runner.viewPort)
		case "environment.control-port":
			argument = strconv.Itoa(runner.controlPort)
		}
		if runner.execShell(ctx, check.command, argument) == nil {
			runner.record(check.id, StatusPass, "authoritative runtime environment observed")
		} else {
			runner.record(check.id, StatusFail, "reserved environment did not match the authoritative value")
		}
	}
}

func (runner *runtimeRunner) checkLoopback(ctx context.Context) {
	for _, endpoint := range []struct {
		id   string
		port int
	}{{"endpoints.view-loopback", runner.viewPort}, {"endpoints.control-loopback", runner.controlPort}} {
		hexPort := fmt.Sprintf("%04X", endpoint.port)
		script := `awk -v loop="0100007F:$1" -v wildcard="00000000:$1" '$2 == loop { good=1 } $2 == wildcard { bad=1 } END { exit !(good && !bad) }' /proc/net/tcp`
		if runner.execShell(ctx, script, hexPort) == nil {
			runner.record(endpoint.id, StatusPass, "only the injected IPv4 loopback bind was observed")
		} else {
			runner.record(endpoint.id, StatusFail, "endpoint was missing or wildcard-bound")
		}
	}
}

func (runner *runtimeRunner) checkTransportNegatives(ctx context.Context) {
	probe := func(id string, operation func(context.Context, int) error) {
		for _, port := range []int{runner.viewPort, runner.controlPort} {
			probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := operation(probeCtx, port)
			cancel()
			if err != nil {
				runner.record(id, StatusFail, "one endpoint violated the negative wire assertion")
				return
			}
		}
		runner.record(id, StatusPass, "both endpoints satisfied the negative wire assertion")
	}
	probe("transport.query-ignored", func(ctx context.Context, port int) error {
		connection, err := OpenRFB(ctx, port, contract.ComputerDisplayWebSocketPath+"?token=ignored")
		if err == nil {
			connection.close()
		}
		return err
	})
	probe("transport.fragment-ignored", func(ctx context.Context, port int) error {
		connection, err := OpenRFB(ctx, port, contract.ComputerDisplayWebSocketPath+"#viewer")
		if err == nil {
			connection.close()
		}
		return err
	})
	probe("transport.wrong-path-rejected", func(ctx context.Context, port int) error {
		protocol := contract.ComputerDisplayWebSocketSubprotocol
		return AssertUpgradeRejected(ctx, port, "/wrong", &protocol)
	})
	probe("transport.missing-subprotocol-rejected", func(ctx context.Context, port int) error {
		return AssertUpgradeRejected(ctx, port, contract.ComputerDisplayWebSocketPath, nil)
	})
	probe("transport.wrong-subprotocol-rejected", func(ctx context.Context, port int) error {
		protocol := "base64"
		return AssertUpgradeRejected(ctx, port, contract.ComputerDisplayWebSocketPath, &protocol)
	})
	probe("transport.text-frame-rejected", AssertTextRejected)
}

func (runner *runtimeRunner) checkInput(ctx context.Context) {
	if runner.config.InputOraclePath == "" {
		runner.record("input.view-isolated", StatusNotRun, "no --input-oracle-path was supplied")
		runner.record("input.control-accepted", StatusNotRun, "no --input-oracle-path was supplied")
		return
	}
	before, err := runner.readContainerFile(ctx, runner.config.InputOraclePath)
	if err != nil {
		runner.record("input.view-isolated", StatusNotRun, "input oracle was unavailable")
		runner.record("input.control-accepted", StatusNotRun, "input oracle was unavailable")
		return
	}
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = SendInput(probeCtx, runner.viewPort, 320, 180)
	cancel()
	if err != nil {
		runner.record("input.view-isolated", StatusFail, "view input probe could not complete")
		runner.record("input.control-accepted", StatusNotRun, "view assertion failed first")
		return
	}
	_ = runner.config.Sleep(ctx, time.Second)
	afterView, err := runner.readContainerFile(ctx, runner.config.InputOraclePath)
	if err != nil || afterView != before {
		runner.record("input.view-isolated", StatusFail, "view changed the deterministic input oracle")
	} else {
		runner.record("input.view-isolated", StatusPass, "view discarded the pointer and key sequence")
	}
	const trueDocument = `{"version":1,"human_driving":true}`
	const falseDocument = `{"version":1,"human_driving":false}`
	if err := runner.writeDriver(trueDocument); err != nil {
		runner.record("input.control-accepted", StatusFail, "could not signal Controller tenure before opening control")
		return
	}
	if runner.config.DriverOraclePath != "" && !runner.waitFile(ctx, runner.config.DriverOraclePath, trueDocument) {
		runner.record("input.control-accepted", StatusFail, "tenant did not observe Controller tenure before control input")
		_ = runner.writeDriver(falseDocument)
		return
	}
	probeCtx, cancel = context.WithTimeout(ctx, 10*time.Second)
	err = SendInput(probeCtx, runner.controlPort, 320, 180)
	cancel()
	if err != nil {
		runner.record("input.control-accepted", StatusFail, "control input probe could not complete")
		_ = runner.writeDriver(falseDocument)
		return
	}
	changed := false
	for attempt := 0; attempt < 40; attempt++ {
		afterControl, readErr := runner.readContainerFile(ctx, runner.config.InputOraclePath)
		if readErr == nil && afterControl != afterView {
			changed = true
			break
		}
		_ = runner.config.Sleep(ctx, 250*time.Millisecond)
	}
	if changed {
		runner.record("input.control-accepted", StatusPass, "after Controller tenure, control changed the oracle with the byte-identical pointer and key sequence")
	} else {
		runner.record("input.control-accepted", StatusFail, "control did not change the deterministic input oracle")
	}
	_ = runner.writeDriver(falseDocument)
	if runner.config.DriverOraclePath != "" {
		_ = runner.waitFile(ctx, runner.config.DriverOraclePath, falseDocument)
	}
}

func (runner *runtimeRunner) checkDriver(ctx context.Context) {
	if runner.execShell(ctx, `test ! -w /wefty/control/driver.json`, "") == nil {
		runner.record("driver.read-only", StatusPass, "tenant cannot write driver.json")
	} else {
		runner.record("driver.read-only", StatusFail, "tenant can write driver.json")
	}
	if runner.execShell(ctx, `test "$(stat -c %a /wefty/control/driver.json)" = 444`, "") == nil {
		runner.record("driver.mode", StatusPass, "mode 0444 observed")
	} else {
		runner.record("driver.mode", StatusFail, "driver.json mode differs from 0444")
	}
	const falseDocument = `{"version":1,"human_driving":false}`
	if payload, err := os.ReadFile(filepath.Join(runner.controlDir, "driver.json")); err == nil && string(payload) == falseDocument {
		runner.record("driver.initial-false", StatusPass, "exact version-1 false document observed")
	} else {
		runner.record("driver.initial-false", StatusFail, "fresh driver document was not exact false")
	}
	if runner.config.DriverOraclePath == "" {
		for _, id := range []string{"driver.true-consumed", "driver.release-consumed", "driver.malformed-fails-closed", "driver.missing-fails-closed"} {
			runner.record(id, StatusNotRun, "no --driver-oracle-path was supplied")
		}
		return
	}
	if err := runner.writeDriver(`{"version":1,"human_driving":true}`); err == nil && runner.waitFile(ctx, runner.config.DriverOraclePath, `{"version":1,"human_driving":true}`) {
		runner.record("driver.true-consumed", StatusPass, "tenant reopened the atomically replaced true document")
	} else {
		runner.record("driver.true-consumed", StatusFail, "tenant ignored the true driver document")
	}
	if err := runner.writeDriver(falseDocument); err == nil && runner.waitFile(ctx, runner.config.DriverOraclePath, falseDocument) {
		runner.record("driver.release-consumed", StatusPass, "tenant consumed release to false")
	} else {
		runner.record("driver.release-consumed", StatusFail, "tenant did not consume release to false")
	}
	malformedPass := true
	for _, document := range []string{`{"version":2,"human_driving":true}`, `{"version":true,"human_driving":true}`, `{"version":1,"human_driving":1}`, `{malformed`} {
		if err := runner.writeDriver(document); err != nil || !runner.waitFile(ctx, runner.config.DriverOraclePath, falseDocument) {
			malformedPass = false
			break
		}
	}
	if malformedPass {
		runner.record("driver.malformed-fails-closed", StatusPass, "wrong version, exact types, and malformed JSON all produced false")
	} else {
		runner.record("driver.malformed-fails-closed", StatusFail, "a malformed driver document did not fail closed")
	}
	if err := os.Rename(filepath.Join(runner.controlDir, "driver.json"), filepath.Join(runner.controlDir, "driver.json.missing")); err == nil && runner.waitFile(ctx, runner.config.DriverOraclePath, falseDocument) {
		runner.record("driver.missing-fails-closed", StatusPass, "missing driver document produced false")
	} else {
		runner.record("driver.missing-fails-closed", StatusFail, "missing driver document did not fail closed")
	}
	_ = os.Remove(filepath.Join(runner.controlDir, "driver.json.missing"))
	_ = runner.writeDriver(falseDocument)
}

func (runner *runtimeRunner) checkProfile(ctx context.Context) {
	imageUser, imageErr := runner.command(ctx, "image", "inspect", "--format", "{{.Config.User}}", runner.config.Image).Output()
	containerUser, containerErr := runner.command(ctx, "inspect", "--format", "{{.Config.User}}", runner.containerID).Output()
	if imageErr == nil && containerErr == nil && strings.TrimSpace(string(imageUser)) == strings.TrimSpace(string(containerUser)) {
		runner.record("profile.image-user", StatusPass, "container retained the image USER")
	} else {
		runner.record("profile.image-user", StatusFail, "container USER differed from image metadata")
	}
	configTemplate := "{{json .Config.Entrypoint}} {{json .Config.Cmd}} {{json .Config.WorkingDir}}"
	imageConfig, imageConfigErr := runner.command(ctx, "image", "inspect", "--format", configTemplate, runner.config.Image).Output()
	containerConfig, containerConfigErr := runner.command(ctx, "inspect", "--format", configTemplate, runner.containerID).Output()
	if imageConfigErr == nil && containerConfigErr == nil && string(imageConfig) == string(containerConfig) {
		runner.record("runtime.image-config", StatusPass, "runtime did not replace image process metadata")
	} else {
		runner.record("runtime.image-config", StatusFail, "runtime replaced image ENTRYPOINT, CMD, or working directory")
	}
	rootfsProbeAvailable := runner.execShell(ctx, `command -v touch >/dev/null`, "") == nil
	rootfsWriteErr := runner.command(ctx, "exec", runner.containerID, "touch", "/wefty-conformance-rootfs").Run()
	if rootfsProbeAvailable && rootfsWriteErr != nil {
		runner.record("profile.rootfs-read-only", StatusPass, "rootfs write was rejected")
	} else {
		_ = runner.command(ctx, "exec", runner.containerID, "rm", "-f", "/wefty-conformance-rootfs").Run()
		runner.record("profile.rootfs-read-only", StatusFail, "rootfs rejection probe was unavailable or the write succeeded")
	}
	if runner.execShell(ctx, `test -w /wefty/service`, "") == nil {
		runner.record("profile.service-writable", StatusPass, "service data mount is tenant writable")
	} else {
		runner.record("profile.service-writable", StatusFail, "service data mount is not tenant writable")
	}
	if runner.execShell(ctx, `awk '$1 == "NoNewPrivs:" { exit !($2 == 1) }' /proc/1/status`, "") == nil {
		runner.record("profile.no-new-privileges", StatusPass, "NoNewPrivs is 1")
	} else {
		runner.record("profile.no-new-privileges", StatusFail, "NoNewPrivs is not 1")
	}
	// 0xa80401fb is the exact Linux bit mask for wefty-v1's 12 bounding
	// capabilities. A non-root image may have fewer, but never more.
	if runner.execShell(ctx, `value=$(awk '$1 == "CapBnd:" { print $2 }' /proc/1/status); test -n "$value"; extra=$((0x$value & ~0xa80401fb)); test "$extra" -eq 0`, "") == nil {
		runner.record("profile.capabilities", StatusPass, "bounding set contains no capability outside wefty-v1")
	} else {
		runner.record("profile.capabilities", StatusFail, "forbidden capability was present")
	}
	mountCheck := `awk '$2 == "/dev/shm" && $3 == "tmpfs" { found=1 } END { exit !found }' /proc/mounts`
	if runner.execShell(ctx, mountCheck, "") == nil {
		runner.record("profile.shm-private", StatusPass, "private tmpfs mount observed")
	} else {
		runner.record("profile.shm-private", StatusFail, "/dev/shm is not tmpfs")
	}
	sizeCheck := `awk '$2 == "/dev/shm" && index($4, "size=1048576k") { found=1 } END { exit !found }' /proc/mounts`
	if runner.execShell(ctx, sizeCheck, "") == nil {
		runner.record("profile.shm-size", StatusPass, "1 GiB ceiling observed")
	} else {
		runner.record("profile.shm-size", StatusFail, "/dev/shm ceiling is not 1 GiB")
	}
	flagsCheck := `test "$(stat -c %a /dev/shm)" = 1777 && awk '$2 == "/dev/shm" && index("," $4 ",", ",nosuid,") && index("," $4 ",", ",nodev,") && index("," $4 ",", ",noexec,") { found=1 } END { exit !found }' /proc/mounts`
	if runner.execShell(ctx, flagsCheck, "") == nil {
		runner.record("profile.shm-flags", StatusPass, "mode and mount flags are exact")
	} else {
		runner.record("profile.shm-flags", StatusFail, "/dev/shm mode or flags differ")
	}
	tmpCheck := `awk '$2 == "/tmp" && index($4, "size=524288k") { tmp=1 } $2 == "/var/tmp" && index($4, "size=65536k") { vartmp=1 } END { exit !(tmp && vartmp) }' /proc/mounts`
	if runner.execShell(ctx, tmpCheck, "") == nil {
		runner.record("profile.tmp-ceilings", StatusPass, "512 MiB /tmp and 64 MiB /var/tmp observed")
	} else {
		runner.record("profile.tmp-ceilings", StatusFail, "bounded /tmp or /var/tmp ceiling differs")
	}
}

func (runner *runtimeRunner) checkPersistence(ctx context.Context) error {
	if runner.execShell(ctx, `printf persistent > /wefty/service/.wefty-conformance-persistent && printf attempt > /tmp/.wefty-conformance-attempt && printf shm > /dev/shm/.wefty-conformance-shm`, "") != nil {
		for _, id := range []string{"persistence.service-survives", "persistence.rootfs-discarded", "targets.control-nonpersistent"} {
			runner.record(id, StatusFail, "could not plant persistence assertions")
		}
		return errors.New("plant persistence assertions")
	}
	if err := runner.stopContainer(context.Background()); err != nil {
		return err
	}
	startedAt := runner.config.Now()
	if err := runner.startContainer(ctx); err != nil {
		return fmt.Errorf("restart container: %w", err)
	}
	if err := runner.waitReady(ctx, startedAt); err != nil {
		return fmt.Errorf("restart readiness: %w", err)
	}
	if runner.execShell(ctx, `test "$(cat /wefty/service/.wefty-conformance-persistent)" = persistent`, "") == nil {
		runner.record("persistence.service-survives", StatusPass, "service marker survived a fresh attempt")
	} else {
		runner.record("persistence.service-survives", StatusFail, "service marker did not survive a fresh attempt")
	}
	if runner.execShell(ctx, `test ! -e /tmp/.wefty-conformance-attempt && test ! -e /dev/shm/.wefty-conformance-shm`, "") == nil {
		runner.record("persistence.rootfs-discarded", StatusPass, "attempt-local tmpfs state was absent")
	} else {
		runner.record("persistence.rootfs-discarded", StatusFail, "attempt-local state survived")
	}
	if runner.execShell(ctx, `test ! -e /wefty/service/driver.json && test ! -e /wefty/service/control`, "") == nil {
		runner.record("targets.control-nonpersistent", StatusPass, "control state was absent from service data")
	} else {
		runner.record("targets.control-nonpersistent", StatusFail, "control state leaked into service data")
	}
	return nil
}

func (runner *runtimeRunner) waitFile(ctx context.Context, path, expected string) bool {
	for attempt := 0; attempt < 40; attempt++ {
		value, err := runner.readContainerFile(ctx, path)
		if err == nil && strings.TrimSpace(value) == expected {
			return true
		}
		if runner.config.Sleep(ctx, 250*time.Millisecond) != nil {
			return false
		}
	}
	return false
}

func (runner *runtimeRunner) readContainerFile(ctx context.Context, path string) (string, error) {
	if path == "" || !filepath.IsAbs(path) || strings.ContainsRune(path, '\x00') {
		return "", errors.New("oracle path must be an absolute container path")
	}
	output, err := runner.command(ctx, "exec", runner.containerID, "cat", path).Output()
	return string(output), err
}

func (runner *runtimeRunner) writeDriver(document string) error {
	temporary := filepath.Join(runner.controlDir, "driver.json.new")
	if err := os.WriteFile(temporary, []byte(document), 0o444); err != nil {
		return err
	}
	if err := os.Chmod(temporary, 0o444); err != nil {
		return err
	}
	return os.Rename(temporary, filepath.Join(runner.controlDir, "driver.json"))
}

func (runner *runtimeRunner) execShell(ctx context.Context, script, argument string) error {
	return runner.command(ctx, "exec", runner.containerID, "sh", "-c", script, "wefty-conformance", argument).Run()
}

func (runner *runtimeRunner) command(ctx context.Context, arguments ...string) *exec.Cmd {
	command := exec.CommandContext(ctx, runner.config.Runtime, arguments...)
	command.Stderr = nil
	return command
}

func (runner *runtimeRunner) record(id string, status Status, detail string) {
	if err := runner.recorder.Record(id, status, detail); err != nil {
		panic(err)
	}
}

func (runner *runtimeRunner) stopContainer(ctx context.Context) error {
	if runner.containerID == "" {
		return nil
	}
	err := runner.command(ctx, "stop", "--time", "15", runner.containerID).Run()
	runner.containerID = ""
	return err
}

func (runner *runtimeRunner) cleanup() {
	_ = runner.stopContainer(context.Background())
	if runner.root != "" {
		// Container writes retain the image uid on the host bind. Re-open the
		// exact bind through the selected image as root before removing only our
		// mktemp tree; this is cleanup authority, not a profile claim.
		args := []string{"run", "--rm"}
		if runner.config.Platform != "" {
			args = append(args, "--platform", runner.config.Platform)
		}
		args = append(args, "--user", "0:0", "--entrypoint", "/bin/chmod", "--mount", "type=bind,src="+runner.root+",dst=/cleanup", runner.config.Image, "-R", "a+rwX", "/cleanup")
		_ = runner.command(context.Background(), args...).Run()
		_ = os.RemoveAll(runner.root)
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
