//go:build linux

package ocihelper

import (
	"bufio"
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

func requireRootNetworkNamespaceTest(t *testing.T) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("requires root network namespace authority")
	}
	if _, err := exec.LookPath("unshare"); err != nil {
		t.Skip("unshare is unavailable")
	}
}

func startIsolatedNetworkTask(t *testing.T) *exec.Cmd {
	t.Helper()
	command := exec.Command("unshare", "--net", "--", "sh", "-c", "ip link set lo up; readlink /proc/self/ns/net; exec sleep 30")
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	if line, readErr := bufio.NewReader(stdout).ReadString('\n'); readErr != nil || !strings.HasPrefix(line, "net:[") {
		t.Fatalf("isolated task did not publish its namespace: %q %v", line, readErr)
	}
	t.Cleanup(func() {
		_ = command.Process.Kill()
		_ = command.Wait()
	})
	return command
}

func TestComputerIsolationObservationUsesLiveNamespaceFacts(t *testing.T) {
	requireRootNetworkNamespaceTest(t)
	command := startIsolatedNetworkTask(t)
	namespace, err := pinTaskNetworkNamespace(uint32(command.Process.Pid))
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.close()
	helperInode, taskInode, visible, err := observeComputerNetworkIsolation(namespace, "@/tmp/.X11-unix/X42000")
	if err != nil {
		t.Fatal(err)
	}
	if helperInode == "" || taskInode == "" || helperInode == taskInode || visible {
		t.Fatalf("post-start namespace observation helper=%q task=%q visible=%t", helperInode, taskInode, visible)
	}
}

func TestPinnedNamespaceCannotDialForeignLoopbackAfterTaskExit(t *testing.T) {
	requireRootNetworkNamespaceTest(t)
	command := startIsolatedNetworkTask(t)
	namespace, err := pinTaskNetworkNamespace(uint32(command.Process.Pid))
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.close()
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("killed namespace task exited successfully")
	}

	host, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	port := uint16(host.Addr().(*net.TCPAddr).Port)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	connection, err := dialTaskLoopback(ctx, namespace, port)
	if connection != nil {
		_ = connection.Close()
		t.Fatal("exited task namespace dial reached the helper loopback listener")
	}
	var errno syscall.Errno
	if err == nil || !errors.As(err, &errno) || errno != syscall.ECONNREFUSED {
		t.Fatalf("exited task namespace dial = %v, want typed ECONNREFUSED", err)
	}
	if err := namespace.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := dialTaskLoopback(ctx, namespace, port); err == nil || !strings.Contains(err.Error(), "released") {
		t.Fatalf("released namespace dial = %v", err)
	}
}

func TestComputerNetworkAddressesAreDisjointPerReservedPort(t *testing.T) {
	hostA, guestA, err := computerNetworkAddresses(42000, 42000)
	if err != nil {
		t.Fatal(err)
	}
	hostB, guestB, err := computerNetworkAddresses(42001, 42000)
	if err != nil {
		t.Fatal(err)
	}
	if hostA.Equal(guestA) || hostA.Equal(hostB) || hostA.Equal(guestB) || guestA.Equal(hostB) || guestA.Equal(guestB) || hostB.Equal(guestB) {
		t.Fatalf("Computer /30 address allocation overlaps: %s %s %s %s", hostA, guestA, hostB, guestB)
	}
}

func TestAbstractSocketObservationMatchesExactToken(t *testing.T) {
	const sockets = "Num RefCount Protocol Flags Type St Inode Path\n000: 2 0 0 0001 01 1 7 @/tmp/.X11-unix/X42001\n"
	visible, err := abstractSocketVisible(strings.NewReader(sockets), "@/tmp/.X11-unix/X4200")
	if err != nil {
		t.Fatal(err)
	}
	if visible {
		t.Fatal("X4200 was inferred from the X42001 token")
	}
	visible, err = abstractSocketVisible(strings.NewReader(sockets), "@/tmp/.X11-unix/X42001")
	if err != nil || !visible {
		t.Fatalf("exact abstract socket visible=%t err=%v", visible, err)
	}
}

func TestComputerVethPreservesDNSAndOutbound(t *testing.T) {
	requireRootNetworkNamespaceTest(t)
	if os.Getenv("WEFTY_RUN_NETWORK_NAMESPACE_EGRESS_TEST") != "1" {
		t.Skip("set WEFTY_RUN_NETWORK_NAMESPACE_EGRESS_TEST=1 for the networked Linux proof")
	}
	command := startIsolatedNetworkTask(t)
	namespace, err := pinTaskNetworkNamespace(uint32(command.Process.Pid))
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.close()
	ipPath, err := resolveRootOwnedNetworkTool("", "ip", "/usr/sbin/ip", "/usr/bin/ip", "/sbin/ip")
	if err != nil {
		t.Fatal(err)
	}
	iptablesPath, err := resolveRootOwnedNetworkTool("", "iptables", "/usr/sbin/iptables", "/usr/bin/iptables", "/sbin/iptables")
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureComputerFirewall(t.Context(), iptablesPath); err != nil {
		t.Fatal(err)
	}
	attachment, err := setupComputerNetwork(t.Context(), namespace, 42000, 42000, ipPath, iptablesPath)
	if err != nil {
		t.Fatal(err)
	}
	defer attachment.close()
	var resolved, fetched bool
	err = inNetworkNamespace(namespace, func() error {
		if output, commandErr := exec.Command("getent", "hosts", "example.com").CombinedOutput(); commandErr != nil || len(strings.TrimSpace(string(output))) == 0 {
			return errors.New("Computer DNS lookup failed: " + strings.TrimSpace(string(output)))
		}
		resolved = true
		probeURL := "http://" + net.JoinHostPort(attachment.gateway, "42000") + "/health"
		if output, commandErr := exec.Command("curl", "--fail", "--silent", "--show-error", "--max-time", "10", probeURL).CombinedOutput(); commandErr != nil || string(output) != computerEgressProbeBody {
			return errors.New("Computer helper egress probe failed: " + strings.TrimSpace(string(output)))
		}
		fetched = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resolved || !fetched {
		t.Fatalf("Computer veth egress resolved=%t fetched=%t", resolved, fetched)
	}
	var gatewayErr error
	err = inNetworkNamespace(namespace, func() error {
		connection, dialErr := net.DialTimeout("tcp4", net.JoinHostPort(attachment.gateway, "22"), time.Second)
		if connection != nil {
			_ = connection.Close()
		}
		gatewayErr = dialErr
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var errno syscall.Errno
	if !errors.As(gatewayErr, &errno) || errno != syscall.ECONNREFUSED {
		t.Fatalf("Computer to Node gateway = %v, want typed ECONNREFUSED", gatewayErr)
	}
	t.Logf("Computer veth facts address=%s gateway=%s dns_resolved=%t helper_http_fetched=%t node_gateway_errno=%s", attachment.guestAddress, attachment.gateway, resolved, fetched, errno)
}
