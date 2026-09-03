package linuxunit

import (
	"strings"
	"testing"
)

func TestRenderLinuxUnitsKeepsAgentUnprivilegedAndHelperNarrow(t *testing.T) {
	units, err := Render(Config{
		AgentPath: "/usr/local/libexec/wefty-agent", OperatorUser: "wefty", OperatorGroup: "wefty", OperatorUID: 1001, OperatorGID: 1001,
		WorkingDirectory: "/var/lib/wefty", ContainerdAddress: "/run/containerd/containerd.sock",
		ContainerdStateRoot: "/run/containerd", RuntimeRoot: "/var/lib/wefty/oci", RuncExecutable: "/usr/local/sbin/runc",
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
