package linuxunit

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestRenderLinuxUnitsKeepsAgentUnprivilegedAndHelperNarrow(t *testing.T) {
	units, err := Render(Config{
		AgentPath: "/usr/local/libexec/wefty-agent", OperatorUser: "wefty", OperatorGroup: "wefty", OperatorUID: 1001, OperatorGID: 1001,
		WorkingDirectory: "/var/lib/wefty", ContainerdAddress: "/run/containerd/containerd.sock",
		ContainerdStateRoot: "/run/containerd", RuntimeRoot: "/var/lib/wefty/oci", RuncExecutable: "/usr/local/sbin/runc",
		SystemdVersion:    255,
		AllowedMountRoots: []string{"/srv/wefty"}, AgentArguments: []string{
			"--node-id=node-linux", "--oci-helper-socket=" + HelperSocketPath,
			"--oci-intent-file=/var/lib/wefty/oci-intent.json", "--oci-control-socket=/run/wefty-agent/control.sock",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	agent := string(units.Agent)
	helper := string(units.HelperService)
	socket := string(units.HelperSocket)
	for _, want := range []string{"User=wefty", "SupplementaryGroups=wefty-oci", "NoNewPrivileges=true", "Restart=on-failure", "Environment=WEFTY_LAUNCH_UNIT=wefty-agent.service"} {
		if !strings.Contains(agent, want) {
			t.Fatalf("agent unit missing %q:\n%s", want, agent)
		}
	}
	for _, want := range []string{"User=root", "WEFTY_OCI_HELPER_ALLOWED_UIDS=1001", "__wefty_oci_helper", "--oci-allowed-mount-root=/srv/wefty", "--oci-runc-executable=/usr/local/sbin/runc", "--oci-memory-capacity-bytes=0", "--oci-memory-reserve-bytes=0", "StartLimitIntervalSec=0", "Restart=on-failure", "RestartSec=250ms", "RestartSteps=6", "RestartMaxDelaySec=1s"} {
		if !strings.Contains(helper, want) {
			t.Fatalf("helper unit missing %q:\n%s", want, helper)
		}
	}
	for _, want := range []string{"ListenStream=/run/wefty-oci/oci-helper.sock", "RuntimeDirectory=wefty-oci", "SocketUser=root", "SocketGroup=wefty-oci", "SocketMode=0660"} {
		if !strings.Contains(socket, want) {
			t.Fatalf("socket unit missing %q:\n%s", want, socket)
		}
	}
	for name, value := range map[string]string{"agent": agent, "helper": helper, "socket": socket} {
		if strings.Contains(strings.ToLower(value), "auth-key") || strings.Contains(value, "TS_AUTHKEY") {
			t.Fatalf("%s unit contains a credential source", name)
		}
	}
}

func TestRenderUsesBoundedLegacySystemdRestartPolicy(t *testing.T) {
	config := Config{AgentPath: "/usr/local/libexec/wefty-agent", OperatorUser: "wefty", OperatorGroup: "wefty",
		OperatorUID: 1001, OperatorGID: 1001, WorkingDirectory: "/var/lib/wefty",
		ContainerdAddress: "/run/containerd/containerd.sock", ContainerdStateRoot: "/run/containerd",
		RuntimeRoot: "/var/lib/wefty/oci", AllowedMountRoots: []string{"/srv/wefty"}, SystemdVersion: 252}
	units, err := Render(config)
	if err != nil {
		t.Fatal(err)
	}
	service := string(units.HelperService)
	if !strings.Contains(service, "RestartSec=1s") || strings.Contains(service, "RestartSteps=") || strings.Contains(service, "RestartMaxDelaySec=") {
		t.Fatalf("legacy helper policy = %s", service)
	}
}

func TestUnknownSystemdVersionUsesConservativeRestartPolicy(t *testing.T) {
	if got := HelperRestartPolicy(0); got != "RestartSec=1s\n" || HelperRestartPolicyName(0) != "conservative_fixed_1s" {
		t.Fatalf("unknown systemd policy = %q name=%q", got, HelperRestartPolicyName(0))
	}
}

func TestSystemd252LaneValidatesLegacyHelperPolicy(t *testing.T) {
	if os.Getenv("WEFTY_REQUIRE_SYSTEMD_252") != "1" {
		t.Skip("set WEFTY_REQUIRE_SYSTEMD_252=1 in the Debian 12 validation lane")
	}
	output, err := exec.Command("systemd-analyze", "--version").Output()
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(output))
	if len(fields) < 2 || fields[0] != "systemd" {
		t.Fatalf("systemd version output = %q", output)
	}
	version, err := strconv.Atoi(fields[1])
	if err != nil || version != 252 {
		t.Fatalf("legacy lane systemd version = %q, want 252", fields[1])
	}
	working := t.TempDir()
	units, err := Render(Config{AgentPath: "/bin/true", OperatorUser: "wefty", OperatorGroup: "wefty", OperatorUID: 1000, OperatorGID: 1000,
		WorkingDirectory: working, ContainerdAddress: "/run/containerd/containerd.sock", ContainerdStateRoot: "/run/containerd", RuntimeRoot: "/var/lib/wefty/oci", SystemdVersion: version})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(working, HelperServiceUnit)
	if err := os.WriteFile(path, units.HelperService, 0o600); err != nil {
		t.Fatal(err)
	}
	fixtures := map[string]string{
		"basic.target":       "[Unit]\nDescription=Test basic target\nDefaultDependencies=no\n",
		"containerd.service": "[Unit]\nDescription=Test containerd dependency\nDefaultDependencies=no\n[Service]\nType=oneshot\nExecStart=/bin/true\n",
		"shutdown.target":    "[Unit]\nDescription=Test shutdown target\nDefaultDependencies=no\n",
		"sysinit.target":     "[Unit]\nDescription=Test sysinit target\nDefaultDependencies=no\n",
		"system.slice":       "[Unit]\nDescription=Test system slice\nDefaultDependencies=no\n",
	}
	for name, contents := range fixtures {
		if err := os.WriteFile(filepath.Join(working, name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.Command("systemd-analyze", "verify", path)
	command.Env = append(os.Environ(), "SYSTEMD_UNIT_PATH="+working)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("systemd 252 rejected rendered legacy policy: %v\n%s", err, output)
	}
}

func TestSupplementaryGroupAdditionForcesOneAgentRestart(t *testing.T) {
	added := ReconcileCommands(true)
	replay := ReconcileCommands(false)
	if got := strings.Join(added[len(added)-1], " "); got != "systemctl restart wefty-agent.service" {
		t.Fatalf("group-add convergence = %q", got)
	}
	if got := strings.Join(replay[len(replay)-1], " "); got != "systemctl start wefty-agent.service" {
		t.Fatalf("idempotent convergence = %q", got)
	}
}

func TestRenderLinuxUnitsRejectsAdversarialConfiguration(t *testing.T) {
	base := Config{
		AgentPath: "/usr/local/libexec/wefty-agent", OperatorUser: "wefty", OperatorGroup: "wefty", OperatorUID: 1001, OperatorGID: 1001,
		WorkingDirectory: "/var/lib/wefty", ContainerdAddress: "/run/containerd/containerd.sock",
		ContainerdStateRoot: "/run/containerd", RuntimeRoot: "/var/lib/wefty/oci", AllowedMountRoots: []string{"/srv/wefty"},
	}
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "root agent", mutate: func(config *Config) { config.OperatorUID = 0 }},
		{name: "root group", mutate: func(config *Config) { config.OperatorGID = 0 }},
		{name: "missing operator group", mutate: func(config *Config) { config.OperatorGroup = "" }},
		{name: "relative binary", mutate: func(config *Config) { config.AgentPath = "wefty-agent" }},
		{name: "relative runc", mutate: func(config *Config) { config.RuncExecutable = "runc" }},
		{name: "root mount", mutate: func(config *Config) { config.AllowedMountRoots = []string{"/"} }},
		{name: "credential argument", mutate: func(config *Config) { config.AgentArguments = []string{"--auth-key=secret"} }},
		{name: "newline", mutate: func(config *Config) { config.AgentArguments = []string{"--node-id=x\nEnvironment=oops"} }},
		{name: "agent path newline", mutate: func(config *Config) { config.AgentPath = "/usr/local/bin/wefty-agent\nEnvironment=oops" }},
		{name: "working directory newline", mutate: func(config *Config) { config.WorkingDirectory = "/var/lib/wefty\nEnvironment=oops" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := base
			test.mutate(&config)
			if _, err := Render(config); err == nil {
				t.Fatal("adversarial unit configuration was accepted")
			}
		})
	}
}
