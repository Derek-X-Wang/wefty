//go:build darwin

package lima

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
	"howett.net/plist"
)

func TestLaunchDaemonIsOperatorUserExplicitAndSecretFree(t *testing.T) {
	config := testLaunchDaemonConfig(t)
	payload, err := RenderLaunchDaemon(config)
	if err != nil {
		t.Fatal(err)
	}
	plistText := string(payload)
	for _, want := range []string{
		"<string>dev.wefty.agent</string>", "<key>UserName</key>", "<string>operator</string>",
		"<key>HOME</key>", "<key>LIMA_HOME</key>", "<key>USER</key>", "<key>LOGNAME</key>", "<key>PATH</key>",
		"<key>WorkingDirectory</key>", "<key>RunAtLoad</key>", "<key>KeepAlive</key>", "<key>ThrottleInterval</key>",
		"<key>WEFTY_LAUNCH_UNIT</key>",
	} {
		if !strings.Contains(plistText, want) {
			t.Fatalf("LaunchDaemon omitted %q:\n%s", want, plistText)
		}
	}
	for _, forbidden := range []string{"TS_AUTHKEY", "--auth-key", "io.lima-vm.daemon", "limactl autostart"} {
		if strings.Contains(plistText, forbidden) {
			t.Fatalf("LaunchDaemon contains forbidden %q:\n%s", forbidden, plistText)
		}
	}
	if plutil, err := exec.LookPath("plutil"); err == nil {
		command := exec.Command(plutil, "-lint", "-")
		command.Stdin = strings.NewReader(plistText)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("plutil rejected LaunchDaemon: %v: %s", err, output)
		}
	}
	var decoded struct {
		Label                string
		ProgramArguments     []string
		UserName             string
		WorkingDirectory     string
		EnvironmentVariables map[string]string
		RunAtLoad            bool
		KeepAlive            struct {
			SuccessfulExit bool
		}
		ThrottleInterval uint64
	}
	if _, err := plist.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode LaunchDaemon plist: %v", err)
	}
	if decoded.Label != LaunchDaemonLabel || decoded.UserName != "operator" || !decoded.RunAtLoad ||
		decoded.KeepAlive.SuccessfulExit || decoded.ThrottleInterval != 10 || len(decoded.ProgramArguments) < 2 ||
		decoded.EnvironmentVariables["HOME"] != config.Home || decoded.EnvironmentVariables["LIMA_HOME"] != config.LimaHome {
		t.Fatalf("typed LaunchDaemon = %+v", decoded)
	}
	config.Arguments = append(config.Arguments, "--auth-key=secret")
	if _, err := RenderLaunchDaemon(config); err == nil {
		t.Fatal("LaunchDaemon accepted an auth key argument")
	}
}

func TestLaunchDaemonInstallerRejectsCompetingLimaAutostart(t *testing.T) {
	commands := [][]string{}
	installer := launchDaemonInstaller{
		run: func(_ context.Context, name string, arguments ...string) ([]byte, error) {
			commands = append(commands, append([]string{name}, arguments...))
			if name == "/usr/bin/id" {
				return []byte("501\n"), nil
			}
			return nil, nil
		},
		glob: func(string) ([]string, error) {
			return []string{"/Library/LaunchDaemons/io.lima-vm.daemon.wefty-oci.plist"}, nil
		},
	}
	if err := installer.install(t.Context(), testLaunchDaemonConfig(t)); err == nil {
		t.Fatal("installer accepted a competing Lima autostart unit")
	}
	if len(commands) != 0 {
		t.Fatalf("installer mutated before rejecting autostart: %v", commands)
	}
}

func TestLaunchDaemonInstallerRejectsLoadedLimaAutostart(t *testing.T) {
	var commands [][]string
	installer := launchDaemonInstaller{
		run: func(_ context.Context, name string, arguments ...string) ([]byte, error) {
			commands = append(commands, append([]string{name}, arguments...))
			return []byte("service = io.lima-vm.daemon.wefty-oci"), nil
		},
		glob: func(string) ([]string, error) { return nil, nil },
	}
	if err := installer.install(t.Context(), testLaunchDaemonConfig(t)); err == nil {
		t.Fatal("installer accepted a loaded Lima autostart unit")
	}
	if !reflect.DeepEqual(commands, [][]string{{"/bin/launchctl", "print", "system"}}) {
		t.Fatalf("installer mutated after loaded autostart: %v", commands)
	}
}

func TestLaunchDaemonInstallerRejectsLoginAutostartFamilies(t *testing.T) {
	config := testLaunchDaemonConfig(t)
	installer := launchDaemonInstaller{
		run: func(_ context.Context, name string, arguments ...string) ([]byte, error) {
			if name == "/usr/bin/id" {
				return []byte("501\n"), nil
			}
			if reflect.DeepEqual(arguments, []string{"print", "user/501"}) {
				return []byte("service = io.lima-vm.autostart.wefty-oci"), nil
			}
			return nil, nil
		},
		glob: func(pattern string) ([]string, error) {
			if strings.Contains(pattern, "/Library/LaunchAgents/") {
				return []string{filepath.Join(config.Home, "Library/LaunchAgents/io.lima-vm.autostart.wefty-oci.plist")}, nil
			}
			return nil, nil
		},
	}
	if err := installer.install(t.Context(), config); err == nil || !strings.Contains(err.Error(), "autostart") {
		t.Fatalf("LaunchAgent autostart = %v", err)
	}
	installer.glob = func(string) ([]string, error) { return nil, nil }
	if err := installer.install(t.Context(), config); err == nil || !strings.Contains(err.Error(), "login autostart") {
		t.Fatalf("loaded user-domain autostart = %v", err)
	}
}

func TestLaunchDaemonInstallerUsesSystemDomainAndIdempotentReload(t *testing.T) {
	var commands [][]string
	installer := launchDaemonInstaller{
		run: func(_ context.Context, name string, arguments ...string) ([]byte, error) {
			commands = append(commands, append([]string{name}, arguments...))
			if name == "/usr/bin/id" {
				return []byte("501\n"), nil
			}
			return nil, nil
		},
		glob: func(string) ([]string, error) { return nil, nil },
	}
	if err := installer.install(t.Context(), testLaunchDaemonConfig(t)); err != nil {
		t.Fatal(err)
	}
	if len(commands) != 9 {
		t.Fatalf("install commands = %v", commands)
	}
	for _, want := range [][]string{
		{"/bin/launchctl", "print", "system"},
		{"/bin/launchctl", "print", "system/dev.wefty.agent"},
		{"/usr/bin/sudo", "/bin/launchctl", "bootout", "system/dev.wefty.agent"},
		{"/usr/bin/sudo", "/bin/launchctl", "bootstrap", "system", LaunchDaemonPath},
		{"/usr/bin/sudo", "/bin/launchctl", "kickstart", "-k", "system/dev.wefty.agent"},
	} {
		found := false
		for _, command := range commands {
			if reflect.DeepEqual(command, want) {
				found = true
			}
		}
		if !found {
			t.Fatalf("install commands omitted %v: %v", want, commands)
		}
	}
}

func TestLaunchDaemonRemovalIsIdempotentAndVerified(t *testing.T) {
	var commands [][]string
	prints := 0
	installer := launchDaemonInstaller{
		run: func(_ context.Context, name string, arguments ...string) ([]byte, error) {
			commands = append(commands, append([]string{name}, arguments...))
			if name == "/bin/launchctl" && len(arguments) > 0 && arguments[0] == "print" {
				prints++
				return nil, os.ErrNotExist
			}
			return nil, nil
		},
		stat: func(string) (os.FileInfo, error) { return nil, os.ErrNotExist },
	}
	evidence, err := installer.remove(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !evidence.Unloaded || !evidence.PlistAbsent || prints != 2 {
		t.Fatalf("removal evidence = %+v, prints=%d", evidence, prints)
	}
	for _, command := range commands {
		if reflect.DeepEqual(command, []string{"/usr/bin/sudo", "/bin/launchctl", "bootout", "system/dev.wefty.agent"}) {
			t.Fatalf("idempotent removal booted out an absent unit: %v", commands)
		}
	}
}

func TestGuestHelperUnitsPinSocketAuthorityAndPrivateMode(t *testing.T) {
	config := GuestHelperInstallConfig{HostMountRoot: "/Users/operator/wefty-mounts", GuestUID: 501, MemoryCapacityBytes: 4 << 30, MemoryReserveBytes: 1 << 30}
	socket := string(renderGuestSocketUnit())
	service := string(renderGuestServiceUnit(config))
	for _, want := range []string{"ListenStream=/run/wefty/oci-helper.sock", "SocketUser=root", "SocketGroup=wefty-oci", "SocketMode=0660", "DirectoryMode=0755"} {
		if !strings.Contains(socket, want) {
			t.Fatalf("socket unit missing %q:\n%s", want, socket)
		}
	}
	for _, want := range []string{
		"User=root", "Group=root", "WEFTY_OCI_HELPER_ALLOWED_UIDS=501", `"` + GuestHelperPath + `" "` + ocihelper.InvocationArg + `"`,
		"--oci-allowed-mount-root=/mnt/wefty-host", "--oci-lima-host-mount-root=/Users/operator/wefty-mounts",
		"--oci-memory-capacity-bytes=4294967296", "--oci-memory-reserve-bytes=1073741824",
		"StartLimitIntervalSec=0", "Restart=on-failure", "RestartSec=250ms", "RestartSteps=6", "RestartMaxDelaySec=10s",
	} {
		if !strings.Contains(service, want) {
			t.Fatalf("service unit missing %q:\n%s", want, service)
		}
	}
	if strings.Contains(socket+service, "containerd.sock\nHost") {
		t.Fatalf("guest units expose raw containerd:\n%s\n%s", socket, service)
	}
}

func TestGuestHelperSourceChecksumMustMatchBeforeMutation(t *testing.T) {
	helper := filepath.Join(t.TempDir(), "wefty-agent-linux")
	if err := os.WriteFile(helper, []byte("linux helper"), 0o700); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "probe.tar")
	if err := os.WriteFile(archive, []byte("archive"), 0o600); err != nil {
		t.Fatal(err)
	}
	var commands int
	installer := guestHelperInstaller{run: func(context.Context, string, ...string) ([]byte, error) {
		commands++
		return nil, nil
	}}
	err := installer.install(t.Context(), GuestHelperInstallConfig{
		Instance: DefaultInstanceName, GuestUser: "operator", GuestUID: 501,
		HelperBinary: helper, ExpectedVersion: "candidate", ExpectedChecksum: "sha256:" + strings.Repeat("0", 64),
		HostMountRoot: "/Users/operator/wefty-mounts", HelperSocket: "/Users/operator/.lima/wefty-oci/sock/wefty-oci-helper.sock",
		ProbeArchive: archive, ProbeReference: "example.invalid/probe", ProbeDigest: "sha256:" + strings.Repeat("a", 64),
		NodeID: "node-mac", BootSessionID: "boot-mac",
	})
	if err == nil || commands != 0 {
		t.Fatalf("checksum mismatch = %v, commands=%d", err, commands)
	}
}

func TestGuestHelperRefreshesNewGroupMembershipOnlyOnce(t *testing.T) {
	for _, test := range []struct {
		name        string
		groups      string
		wantRestart bool
	}{
		{name: "new membership", groups: "operator", wantRestart: true},
		{name: "existing membership", groups: "operator wefty-oci", wantRestart: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := validGuestHelperInstallConfig(t)
			var commands [][]string
			installer := guestHelperInstaller{run: func(_ context.Context, name string, arguments ...string) ([]byte, error) {
				command := append([]string{name}, arguments...)
				commands = append(commands, command)
				if slices.Contains(command, "id") {
					return []byte(test.groups), nil
				}
				if slices.Contains(command, "stat") {
					return []byte("wrong"), nil
				}
				return nil, nil
			}}
			if err := installer.install(t.Context(), config); err == nil {
				t.Fatal("fixture unexpectedly passed socket verification")
			}
			restarted := false
			for _, command := range commands {
				if reflect.DeepEqual(command, []string{"limactl", "stop", DefaultInstanceName}) {
					restarted = true
				}
			}
			if restarted != test.wantRestart {
				t.Fatalf("restart=%t, want %t; commands=%v", restarted, test.wantRestart, commands)
			}
		})
	}
}

func TestGuestHelperStopsUnitsBeforeVersionReplacementAndDropsUnusedSidecar(t *testing.T) {
	config := validGuestHelperInstallConfig(t)
	var commands [][]string
	installer := guestHelperInstaller{run: func(_ context.Context, name string, arguments ...string) ([]byte, error) {
		command := append([]string{name}, arguments...)
		commands = append(commands, command)
		if slices.Contains(command, "id") {
			return []byte("operator wefty-oci"), nil
		}
		if slices.Contains(command, "stat") {
			return []byte("wrong"), nil
		}
		return nil, nil
	}}
	if err := installer.install(t.Context(), config); err == nil {
		t.Fatal("fixture unexpectedly passed socket verification")
	}
	serviceStop := []string{"limactl", "--tty=false", "shell", "--workdir=/", DefaultInstanceName, "sudo", "systemctl", "stop", GuestHelperServiceUnit}
	socketStop := []string{"limactl", "--tty=false", "shell", "--workdir=/", DefaultInstanceName, "sudo", "systemctl", "stop", GuestHelperSocketUnit}
	if len(commands) < 4 || !reflect.DeepEqual(commands[1], serviceStop) || !reflect.DeepEqual(commands[2], socketStop) {
		t.Fatalf("helper version replacement was not quiesced first: %v", commands)
	}
	for _, command := range commands {
		if strings.Contains(strings.Join(command, " "), "wefty-agent.sha256") {
			t.Fatalf("unused checksum sidecar was installed: %v", commands)
		}
	}
}

func TestGuestHelperRemovalIsIdempotentAndVerified(t *testing.T) {
	var commands [][]string
	installer := guestHelperInstaller{run: func(_ context.Context, name string, arguments ...string) ([]byte, error) {
		commands = append(commands, append([]string{name}, arguments...))
		if len(arguments) > 0 && arguments[0] == "list" {
			return []byte(`{"name":"` + DefaultInstanceName + `","status":"running"}`), nil
		}
		if slices.Contains(arguments, "disable") {
			return nil, errors.New("unit already absent")
		}
		return nil, nil
	}}
	evidence, err := installer.remove(t.Context(), GuestHelperRemovalConfig{Instance: DefaultInstanceName, Limactl: "limactl"})
	if err != nil {
		t.Fatal(err)
	}
	if !evidence.SocketStopped || !evidence.ServiceStopped || !evidence.FilesAbsent {
		t.Fatalf("guest removal evidence = %+v", evidence)
	}
	if len(commands) != 6 {
		t.Fatalf("guest removal commands = %v", commands)
	}
}

func TestGuestHelperRemovalTemporarilyStartsStoppedInstance(t *testing.T) {
	var commands [][]string
	installer := guestHelperInstaller{run: func(_ context.Context, name string, arguments ...string) ([]byte, error) {
		commands = append(commands, append([]string{name}, arguments...))
		if len(arguments) > 0 && arguments[0] == "list" {
			return []byte(`{"name":"` + DefaultInstanceName + `","status":"stopped"}`), nil
		}
		return nil, nil
	}}
	if _, err := installer.remove(t.Context(), GuestHelperRemovalConfig{Instance: DefaultInstanceName, Limactl: "limactl"}); err != nil {
		t.Fatal(err)
	}
	if len(commands) != 8 || !reflect.DeepEqual(commands[1], []string{"limactl", "start", DefaultInstanceName}) ||
		!reflect.DeepEqual(commands[len(commands)-1], []string{"limactl", "stop", DefaultInstanceName}) {
		t.Fatalf("stopped guest removal lifecycle = %v", commands)
	}
}

func TestMinimalDoctorFactsAreBoundedAndAtomic(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 30, 0, 0, time.UTC)
	checksum := sha256.Sum256([]byte("helper"))
	handshake := &ocihelper.AcquireSessionResponse{
		ProtocolVersion: ocihelper.ProtocolVersion, HelperVersion: "candidate",
		HelperChecksum: "sha256:" + hex.EncodeToString(checksum[:]), SessionCapability: "must-not-leak",
	}
	observation := contract.CapabilityObservation{
		Revision: 7, Capabilities: map[string]bool{"kind:process": true, "kind:oci": true}, ObservedAt: now,
	}
	facts := BuildMinimalDoctorFacts(UnitStateLaunchedByUnit, SupervisorFacts{Instance: DefaultInstanceName, State: InstanceRunning, Enabled: true, ObservedAt: now}, handshake, observation, &observation, now)
	path := filepath.Join(t.TempDir(), "facts", "minimal.json")
	if err := WriteMinimalDoctorFacts(path, facts); err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload), "must-not-leak") || strings.Contains(string(payload), "raw_error") {
		t.Fatalf("minimal doctor leaked private detail: %s", payload)
	}
	var decoded MinimalDoctorFacts
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Version != 1 || decoded.Unit.Label != LaunchDaemonLabel || decoded.Lima.State != InstanceRunning || decoded.Helper.State != HelperStateReady || decoded.Probe.State != ProbeStatePassed || decoded.CapabilityRevision != 7 || decoded.ReasonCode != "" {
		t.Fatalf("minimal doctor facts = %+v", decoded)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("minimal doctor mode = %o", info.Mode().Perm())
	}
	if err := WriteMinimalDoctorFacts(path, facts); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(info, after) {
		t.Fatal("unchanged minimal doctor facts replaced the file")
	}
}

func TestMinimalDoctorFactsPreferStableLimaReason(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 30, 0, 0, time.UTC)
	facts := BuildMinimalDoctorFacts(UnitStateLaunchedByUnit, SupervisorFacts{
		Instance: DefaultInstanceName, State: InstanceBroken, Enabled: true,
		ReasonCode: contract.CapabilityReasonLimaBroken, ObservedAt: now,
	}, nil, contract.CapabilityObservation{Revision: 8, Capabilities: map[string]bool{"kind:process": true}}, nil, now)
	if facts.ReasonCode != contract.CapabilityReasonLimaBroken || facts.Helper.State != HelperStateUnavailable || facts.Probe.State != ProbeStateNotRun {
		t.Fatalf("degraded minimal facts = %+v", facts)
	}
	payload, err := json.Marshal(facts)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(payload, []byte(`"protocol_version":0`)) {
		t.Fatalf("zero-valued protocol fact was omitted: %s", payload)
	}
}

func testLaunchDaemonConfig(t *testing.T) LaunchDaemonConfig {
	t.Helper()
	root := t.TempDir()
	for _, directory := range []string{
		filepath.Join(root, "bin"), filepath.Join(root, "home"), filepath.Join(root, "home", ".lima"), filepath.Join(root, "logs"),
	} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "bin", "wefty-agent"), []byte("agent"), 0o700); err != nil {
		t.Fatal(err)
	}
	return LaunchDaemonConfig{
		AgentPath: filepath.Join(root, "bin", "wefty-agent"), Arguments: []string{"--node-id=node-mac", "--fabric=tsnet"},
		OperatorUser: "operator", Home: filepath.Join(root, "home"), LimaHome: filepath.Join(root, "home", ".lima"),
		PATH: DefaultLaunchPATH, WorkingDirectory: root,
		StandardOutPath: filepath.Join(root, "logs", "agent.log"), StandardErrorPath: filepath.Join(root, "logs", "agent.err"),
	}
}

func validGuestHelperInstallConfig(t *testing.T) GuestHelperInstallConfig {
	t.Helper()
	helper := filepath.Join(t.TempDir(), "wefty-agent-linux")
	content := []byte("linux helper")
	if err := os.WriteFile(helper, content, 0o700); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(content)
	archive := filepath.Join(t.TempDir(), "probe.tar")
	if err := os.WriteFile(archive, []byte("archive"), 0o600); err != nil {
		t.Fatal(err)
	}
	return GuestHelperInstallConfig{
		Instance: DefaultInstanceName, Limactl: "limactl", GuestUser: "operator", GuestUID: 501,
		HelperBinary: helper, ExpectedVersion: "candidate", ExpectedChecksum: "sha256:" + hex.EncodeToString(digest[:]),
		HostMountRoot: "/Users/operator/wefty-mounts", HelperSocket: "/Users/operator/.lima/wefty-oci/sock/wefty-oci-helper.sock",
		ProbeArchive: archive, ProbeReference: "example.invalid/probe", ProbeDigest: "sha256:" + strings.Repeat("a", 64),
		NodeID: "node-mac", BootSessionID: "boot-mac",
		MemoryCapacityBytes: 4 << 30, MemoryReserveBytes: 1 << 30,
	}
}
