package ocihelper

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// Use the wire shape so this test also detects a helper that silently loses
// the Computer setting before it constructs the profile.
func setComputerNetworkForTest(t *testing.T, workload *WorkloadInput, network string) {
	t.Helper()
	payload, err := json.Marshal(map[string]string{"computer_network": network})
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(payload, workload); err != nil {
		t.Fatal(err)
	}
}

func TestNoneNetworkFilesExcludeNodeResolverAndHosts(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := prepareComputerNoneNetworkFiles(root); err != nil {
		t.Fatal(err)
	}
	for name, expected := range map[string]string{
		"resolv.conf": "nameserver 127.0.0.1\noptions timeout:1 attempts:1\n",
		"hosts":       "127.0.0.1 localhost\n::1 localhost\n",
	} {
		path := filepath.Join(root, "network", "none", name)
		data, err := os.ReadFile(path)
		if err != nil || string(data) != expected {
			t.Fatalf("loopback-only %s: %q %v", name, data, err)
		}
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0444 {
			t.Fatalf("network file not read-only and image-readable: %v %v", info, err)
		}
	}
	// Existing retained profiles must see complete, unchanged bytes while a
	// later Computer prepares its files.
	retained, err := os.Open(filepath.Join(root, "network", "none", "resolv.conf"))
	if err != nil {
		t.Fatal(err)
	}
	defer retained.Close()
	if err := prepareComputerNoneNetworkFiles(root); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 128)
	n, err := retained.Read(data)
	if err != nil || string(data[:n]) != "nameserver 127.0.0.1\noptions timeout:1 attempts:1\n" {
		t.Fatalf("retained resolver changed: %q %v", data[:n], err)
	}
}

func TestComputerNetworkWireRejectsUnsupportedSettings(t *testing.T) {
	for _, network := range []string{"filtered", "None", "none ", "proxy"} {
		t.Run(network, func(t *testing.T) {
			input := computerNetworkInputForTest(t)
			setComputerNetworkForTest(t, &input.Workload, network)
			if err := validateWorkloadWire(input.Workload); err == nil || !strings.Contains(err.Error(), "network") {
				t.Fatalf("unsupported network accepted: %v", err)
			}
		})
	}
	for _, network := range []string{"internet", "none"} {
		input := goldenRuntimeSpecInput(t, "amd64")
		setComputerNetworkForTest(t, &input.Workload, network)
		if err := validateWorkloadWire(input.Workload); err == nil || !strings.Contains(err.Error(), "network") {
			t.Fatalf("ordinary workload accepted Computer network %s: %v", network, err)
		}
	}
}

func computerNetworkInputForTest(t *testing.T) RuntimeSpecInput {
	t.Helper()
	input := goldenRuntimeSpecInput(t, "amd64")
	for _, fixture := range runtimeSpecGoldenCases() {
		if fixture.computer {
			fixture.configure(&input)
			return input
		}
	}
	t.Fatal("Computer profile fixture missing")
	return input
}

func TestComputerNetworkProfiles(t *testing.T) {
	for _, network := range []string{"", "internet", "none"} {
		t.Run("network="+network, func(t *testing.T) {
			input := computerNetworkInputForTest(t)
			setComputerNetworkForTest(t, &input.Workload, network)
			spec, err := buildRuntimeSpec(context.Background(), input, goldenDependencies(t, filepath.Join("testdata", "containerd-v2.3.7", "seccomp-linux-amd64.json")))
			if err != nil {
				t.Fatal(err)
			}
			resolver, hosts := input.ResolverPath, input.HostsPath
			if network == "none" {
				resolver = "/run/wefty/fixtures/network/none/resolv.conf"
				hosts = "/run/wefty/fixtures/network/none/hosts"
			}
			assertMount(t, spec.Mounts, "/etc/resolv.conf", resolver, true)
			assertMount(t, spec.Mounts, "/etc/hosts", hosts, true)
			if !slices.Contains(spec.Linux.Namespaces, specs.LinuxNamespace{Type: specs.NetworkNamespace}) || !spec.Root.Readonly {
				t.Fatalf("network setting lost the private namespace or read-only root: %+v", spec)
			}
			if slices.Contains(spec.Process.Capabilities.Effective, "CAP_NET_ADMIN") || slices.Contains(spec.Process.Capabilities.Effective, "CAP_NET_RAW") {
				t.Fatal("Computer can change its network")
			}
		})
	}
}
