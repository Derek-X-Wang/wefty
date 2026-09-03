package linuxunit

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

type configureTestRunner struct{ commands [][]string }

func (runner *configureTestRunner) Run(_ context.Context, command string, arguments ...string) ([]byte, error) {
	runner.commands = append(runner.commands, append([]string{command}, arguments...))
	switch command {
	case "systemctl":
		return []byte("systemd 255 (255.4)\n"), nil
	case "getent":
		return nil, errors.New("group absent")
	case "id":
		return []byte("wefty\n"), nil
	default:
		return nil, nil
	}
}

func TestConfigureWritesUnitsAndConfigButOnlyPrintsServiceCommands(t *testing.T) {
	root := t.TempDir()
	unitDirectory := filepath.Join(root, "units")
	if err := os.Mkdir(unitDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	runner := &configureTestRunner{}
	var chowns []string
	receipt, err := Configure(t.Context(), Config{
		AgentPath: "/usr/local/libexec/wefty-agent", OperatorUser: "wefty", OperatorGroup: "wefty", OperatorUID: 1001, OperatorGID: 1001,
		WorkingDirectory: "/var/lib/wefty", ContainerdAddress: "/run/containerd/containerd.sock",
		ContainerdStateRoot: "/run/containerd", RuntimeRoot: "/var/lib/wefty/oci", AllowedMountRoots: []string{"/srv/wefty"},
	}, ConfigurePaths{
		UnitDirectory: unitDirectory, NodeConfig: filepath.Join(root, "home", ".config", "wefty", "node.json"), ControlSocket: "/run/wefty-agent/control.sock",
		chown: func(path string, _, _ int) error { chowns = append(chowns, path); return nil },
	}, runner)
	if err != nil {
		t.Fatal(err)
	}
	if !receipt.GroupCreated || !receipt.UserAdded || len(receipt.Commands) == 0 {
		t.Fatalf("configure receipt=%+v", receipt)
	}
	for _, command := range runner.commands {
		if command[0] == "systemctl" && !reflect.DeepEqual(command, []string{"systemctl", "--version"}) {
			t.Fatalf("configure executed service convergence: %v", runner.commands)
		}
	}
	wantControl := "/run/wefty-agent/control.sock"
	installed, err := ocicontrol.ReadInstalledConfig(filepath.Join(root, "home", ".config", "wefty", "node.json"))
	if err != nil || installed.ControlSocket != wantControl {
		t.Fatalf("installed config=%+v err=%v", installed, err)
	}
	for _, name := range []string{AgentUnit, HelperSocketUnit, HelperServiceUnit} {
		if info, err := os.Stat(filepath.Join(unitDirectory, name)); err != nil || !info.Mode().IsRegular() {
			t.Fatalf("unit %s info=%v err=%v", name, info, err)
		}
	}
	if len(chowns) != 3 || !reflect.DeepEqual(runner.commands, [][]string{
		{"systemctl", "--version"},
		{"getent", "group", HelperGroup}, {"groupadd", "--system", HelperGroup},
		{"id", "-nG", "wefty"}, {"usermod", "-a", "-G", HelperGroup, "wefty"},
	}) {
		t.Fatalf("chowns=%v commands=%v", chowns, runner.commands)
	}
}
