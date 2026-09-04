//go:build linux

package ocihelper

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	netlink "github.com/tailscale/netlink"
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

func resetComputerFirewallChainsForTest(t *testing.T) {
	t.Helper()
	for _, tool := range []struct {
		name       string
		candidates []string
	}{
		{name: "iptables", candidates: []string{"/usr/sbin/iptables", "/usr/bin/iptables", "/sbin/iptables"}},
		{name: "ip6tables", candidates: []string{"/usr/sbin/ip6tables", "/usr/bin/ip6tables", "/sbin/ip6tables"}},
	} {
		executable, err := resolveRootOwnedNetworkTool("", tool.name, tool.candidates...)
		if err != nil {
			t.Fatal(err)
		}
		for _, chain := range []struct{ table, name string }{{"filter", computerFirewallInput}, {"filter", computerFirewallForward}, {"nat", computerFirewallNAT}} {
			arguments := []string{"-w", "5"}
			if chain.table != "filter" {
				arguments = append(arguments, "-t", chain.table)
			}
			arguments = append(arguments, "-F", chain.name)
			if output, err := exec.Command(executable, arguments...).CombinedOutput(); err != nil && !computerFirewallRuleAbsent(output) {
				t.Fatalf("flush test Computer firewall chain through %s: %v: %s", executable, err, strings.TrimSpace(string(output)))
			}
		}
	}
}

func startIsolatedNetworkTask(t *testing.T) *exec.Cmd {
	t.Helper()
	return startIsolatedNetworkTaskCommand(t, exec.Command("unshare", "--net", "--", "sh", "-c", "ip link set lo up; readlink /proc/self/ns/net; exec sleep 30"))
}

func startIsolatedNetworkTaskWithResolver(t *testing.T, resolverPath string) *exec.Cmd {
	t.Helper()
	if _, err := exec.LookPath("nsenter"); err != nil {
		t.Skip("nsenter is unavailable")
	}
	return startIsolatedNetworkTaskCommand(t, exec.Command("unshare", "--net", "--mount", "--propagation", "private", "--", "sh", "-c", `mount --bind "$1" /etc/resolv.conf
ip link set lo up
readlink /proc/self/ns/net
exec sleep 30`, "sh", resolverPath))
}

func startIsolatedNetworkTaskCommand(t *testing.T, command *exec.Cmd) *exec.Cmd {
	t.Helper()
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

func TestPinnedNamespaceObservesOwningThreadLoopbackListener(t *testing.T) {
	requireRootNetworkNamespaceTest(t)
	command := startIsolatedNetworkTask(t)
	namespace, err := pinTaskNetworkNamespace(uint32(command.Process.Pid))
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.close()
	listener, err := listenTaskLoopback(namespace, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := uint16(listener.Addr().(*net.TCPAddr).Port)
	inode, found, err := loopbackListenInode(namespace, port)
	if err != nil {
		t.Fatal(err)
	}
	if !found || inode == "" {
		t.Fatalf("pinned namespace listener port %d was not observed from the owning thread", port)
	}
}

func TestComputerDNSProxyForwardsLoopbackResolver(t *testing.T) {
	requireRootNetworkNamespaceTest(t)
	command := startIsolatedNetworkTask(t)
	namespace, err := pinTaskNetworkNamespace(uint32(command.Process.Pid))
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.close()
	upstream, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	port := upstream.LocalAddr().(*net.UDPAddr).Port
	tcpUpstream, err := net.Listen("tcp4", upstream.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer tcpUpstream.Close()
	proxy, err := startComputerDNSProxy(namespace, net.JoinHostPort("127.0.0.53", strconv.Itoa(port)), upstream.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.close()
	query := []byte{0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	wantResponse := slices.Clone(query)
	wantResponse[2] |= 0x80
	go func() {
		buffer := make([]byte, 512)
		length, client, readErr := upstream.ReadFrom(buffer)
		if readErr == nil {
			response := slices.Clone(buffer[:length])
			response[2] |= 0x80
			_, _ = upstream.WriteTo(response, client)
		}
	}()
	go func() {
		connection, acceptErr := tcpUpstream.Accept()
		if acceptErr != nil {
			return
		}
		defer connection.Close()
		header := make([]byte, 2)
		if _, readErr := io.ReadFull(connection, header); readErr != nil {
			return
		}
		query := make([]byte, int(binary.BigEndian.Uint16(header)))
		if _, readErr := io.ReadFull(connection, query); readErr == nil {
			response := slices.Clone(query)
			response[2] |= 0x80
			binary.BigEndian.PutUint16(header, uint16(len(response)))
			_, _ = connection.Write(append(header, response...))
		}
	}()
	var response []byte
	err = inNetworkNamespace(namespace, func() error {
		connection, dialErr := net.DialTimeout("udp4", net.JoinHostPort("127.0.0.53", strconv.Itoa(port)), time.Second)
		if dialErr != nil {
			return dialErr
		}
		defer connection.Close()
		_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
		if _, writeErr := connection.Write(query); writeErr != nil {
			return writeErr
		}
		buffer := make([]byte, 512)
		length, readErr := connection.Read(buffer)
		response = slices.Clone(buffer[:length])
		return readErr
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(response, wantResponse) {
		t.Fatalf("Computer DNS proxy response = %x", response)
	}
	err = inNetworkNamespace(namespace, func() error {
		connection, dialErr := net.DialTimeout("tcp4", net.JoinHostPort("127.0.0.53", strconv.Itoa(port)), time.Second)
		if dialErr != nil {
			return dialErr
		}
		defer connection.Close()
		_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
		header := []byte{0, byte(len(query))}
		if _, writeErr := connection.Write(append(header, query...)); writeErr != nil {
			return writeErr
		}
		if _, readErr := io.ReadFull(connection, header); readErr != nil {
			return readErr
		}
		payload := make([]byte, int(binary.BigEndian.Uint16(header)))
		if _, readErr := io.ReadFull(connection, payload); readErr != nil {
			return readErr
		}
		response = payload
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(response, wantResponse) {
		t.Fatalf("Computer TCP DNS proxy response = %x", response)
	}
}

func TestComputerDNSProxyRejectsNonDNSPayload(t *testing.T) {
	requireRootNetworkNamespaceTest(t)
	command := startIsolatedNetworkTask(t)
	namespace, err := pinTaskNetworkNamespace(uint32(command.Process.Pid))
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.close()
	upstream, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	proxy, err := startComputerDNSProxy(namespace, "127.0.0.53:40531", upstream.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.close()
	if err := inNetworkNamespace(namespace, func() error {
		connection, dialErr := net.DialTimeout("udp4", "127.0.0.53:40531", time.Second)
		if dialErr != nil {
			return dialErr
		}
		defer connection.Close()
		_, writeErr := connection.Write([]byte("not-dns"))
		return writeErr
	}); err != nil {
		t.Fatal(err)
	}
	if err := upstream.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 64)
	if _, _, err := upstream.ReadFrom(buffer); err == nil {
		t.Fatal("Computer DNS proxy forwarded a non-DNS payload")
	} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatalf("observe rejected non-DNS payload: %v", err)
	}
}

func TestComputerDNSProxyRateLimitIsAttemptLocalAndBounded(t *testing.T) {
	proxy := &computerDNSProxy{}
	now := time.Unix(1, 0)
	for index := 0; index < 128; index++ {
		if !proxy.allowDNSQuery(now) {
			t.Fatalf("Computer DNS query %d was refused before the bound", index)
		}
	}
	if proxy.allowDNSQuery(now) {
		t.Fatal("Computer DNS proxy admitted a query beyond the per-attempt rate bound")
	}
	if !proxy.allowDNSQuery(now.Add(time.Second)) {
		t.Fatal("Computer DNS proxy did not reset its per-attempt rate window")
	}
}

func TestComputerNetworkUsesMountedResolverForLoopbackProxy(t *testing.T) {
	requireRootNetworkNamespaceTest(t)
	resetComputerFirewallChainsForTest(t)
	resolverPath := filepath.Join(t.TempDir(), "resolv.conf")
	if err := os.WriteFile(resolverPath, []byte("nameserver 127.0.0.53\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	command := startIsolatedNetworkTaskWithResolver(t, resolverPath)
	namespace, err := pinTaskNetworkNamespace(uint32(command.Process.Pid))
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.close()
	engine := &ContainerdEngine{config: NativeEngineConfig{ResolverPath: resolverPath, AttemptPortMin: 42120}}
	attachment, err := engine.prepareComputerNetwork(t.Context(), namespace, 42120)
	if err != nil {
		t.Fatal(err)
	}
	defer attachment.close()
	if attachment.dns == nil {
		t.Fatal("Computer mounted loopback resolver did not start a private DNS proxy")
	}
	output, err := exec.Command("nsenter", "--target", strconv.Itoa(command.Process.Pid), "--net", "--mount", "--", "getent", "ahostsv4", "example.com").CombinedOutput()
	if err != nil || len(strings.TrimSpace(string(output))) == 0 {
		t.Fatalf("Computer mounted loopback resolver lookup failed: %v: %s", err, strings.TrimSpace(string(output)))
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

func TestComputerNetworkRouteConflictIsTyped(t *testing.T) {
	requireRootNetworkNamespaceTest(t)
	ipPath, err := resolveRootOwnedNetworkTool("", "ip", "/usr/sbin/ip", "/usr/bin/ip", "/sbin/ip")
	if err != nil {
		t.Fatal(err)
	}
	if output, err := exec.CommandContext(t.Context(), ipPath, "route", "add", "blackhole", "198.18.0.0/15", "metric", "42777").CombinedOutput(); err != nil {
		t.Fatalf("install conflict route: %v: %s", err, strings.TrimSpace(string(output)))
	}
	t.Cleanup(func() {
		_, _ = exec.Command(ipPath, "route", "del", "blackhole", "198.18.0.0/15", "metric", "42777").CombinedOutput()
	})
	err = validateComputerNetworkRouteSpace()
	var conflict *ComputerNetworkConflictError
	if !errors.As(err, &conflict) || !strings.Contains(conflict.Route, "198.18.0.0/15") {
		t.Fatalf("Computer route conflict = %v, want typed 198.18.0.0/15 refusal", err)
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
	resetComputerFirewallChainsForTest(t)
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
	attachment, err := setupComputerNetwork(t.Context(), namespace, 42000, 42000, ipPath, iptablesPath, "/etc/resolv.conf")
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

func TestComputerNetworkDisablesIPv6(t *testing.T) {
	requireRootNetworkNamespaceTest(t)
	resetComputerFirewallChainsForTest(t)
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
	attachment, err := setupComputerNetwork(t.Context(), namespace, 42008, 42000, ipPath, iptablesPath, "/etc/resolv.conf")
	if err != nil {
		t.Fatal(err)
	}
	defer attachment.close()
	if attachment.ipv6State != computerIPv6DisabledByHelper {
		t.Fatalf("Computer IPv6 disable state = %q, want %q", attachment.ipv6State, computerIPv6DisabledByHelper)
	}

	for _, name := range []string{"all", "default"} {
		var value string
		if err := inNetworkNamespace(namespace, func() error {
			payload, readErr := os.ReadFile("/proc/sys/net/ipv6/conf/" + name + "/disable_ipv6")
			value = strings.TrimSpace(string(payload))
			return readErr
		}); err != nil {
			t.Fatal(err)
		}
		if value != "1" {
			t.Fatalf("Computer net.ipv6.conf.%s.disable_ipv6 = %q, want 1", name, value)
		}
	}
}

func TestComputerNetworkTreatsMissingIPv6SysctlsAsKernelDisabled(t *testing.T) {
	state, err := disableComputerIPv6(filepath.Join(t.TempDir(), "missing", "ipv6", "conf"))
	if err != nil || state != computerIPv6DisabledByKernel {
		t.Fatalf("missing IPv6 sysctls state = %q, err %v; want %q", state, err, computerIPv6DisabledByKernel)
	}
}

func TestComputerIPv4FirewallRejectsLiveNodeListener(t *testing.T) {
	requireRootNetworkNamespaceTest(t)
	resetComputerFirewallChainsForTest(t)
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
	attachment, err := setupComputerNetwork(t.Context(), namespace, 42009, 42000, ipPath, iptablesPath, "/etc/resolv.conf")
	if err != nil {
		t.Fatal(err)
	}
	defer attachment.close()

	listener, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	assertTCPConnected(t, "tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	assertNamespaceTCPRefused(t, namespace, "tcp4", net.JoinHostPort(attachment.gateway, strconv.Itoa(port)))

	rejectRule := []string{"-w", "5", "-D", computerFirewallInput, "-i", computerHostLinkPrefix + "+", "-j", "REJECT", "--reject-with", "icmp-port-unreachable"}
	if output, err := exec.CommandContext(t.Context(), iptablesPath, rejectRule...).CombinedOutput(); err != nil {
		t.Fatalf("delete Computer INPUT rejection: %v: %s", err, strings.TrimSpace(string(output)))
	}
	t.Cleanup(func() { _ = ensureComputerFirewall(context.Background(), iptablesPath) })
	assertNamespaceTCPConnected(t, namespace, "tcp4", net.JoinHostPort(attachment.gateway, strconv.Itoa(port)))
}

func TestComputerFirewallReconcilesOnEveryAttemptStart(t *testing.T) {
	requireRootNetworkNamespaceTest(t)
	resetComputerFirewallChainsForTest(t)
	ipPath, err := resolveRootOwnedNetworkTool("", "ip", "/usr/sbin/ip", "/usr/bin/ip", "/sbin/ip")
	if err != nil {
		t.Fatal(err)
	}
	iptablesPath, err := resolveRootOwnedNetworkTool("", "iptables", "/usr/sbin/iptables", "/usr/bin/iptables", "/sbin/iptables")
	if err != nil {
		t.Fatal(err)
	}
	ip6tablesPath, err := resolveRootOwnedNetworkTool("", "ip6tables", "/usr/sbin/ip6tables", "/usr/bin/ip6tables", "/sbin/ip6tables")
	if err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{IPExecutable: ipPath, IPTablesExecutable: iptablesPath, IP6TablesExecutable: ip6tablesPath, ResolverPath: "/etc/resolv.conf", AttemptPortMin: 42000}}
	commandA := startIsolatedNetworkTask(t)
	namespaceA, err := pinTaskNetworkNamespace(uint32(commandA.Process.Pid))
	if err != nil {
		t.Fatal(err)
	}
	defer namespaceA.close()
	attachmentA, err := engine.prepareComputerNetwork(t.Context(), namespaceA, 42010)
	if err != nil {
		t.Fatal(err)
	}
	defer attachmentA.close()
	listener, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	assertNamespaceTCPRefused(t, namespaceA, "tcp4", net.JoinHostPort(attachmentA.gateway, strconv.Itoa(port)))
	prependForeignAccept(t, iptablesPath, "filter", "INPUT")
	assertNamespaceTCPConnected(t, namespaceA, "tcp4", net.JoinHostPort(attachmentA.gateway, strconv.Itoa(port)))
	present, err := observeComputerFirewall(t.Context(), iptablesPath, ip6tablesPath, []computerNetworkAttachment{*attachmentA})
	if err != nil || present {
		t.Fatalf("Computer firewall observation after INPUT prepend = present %t, err %v; want absent without read failure", present, err)
	}

	commandB := startIsolatedNetworkTask(t)
	namespaceB, err := pinTaskNetworkNamespace(uint32(commandB.Process.Pid))
	if err != nil {
		t.Fatal(err)
	}
	defer namespaceB.close()
	attachmentB, err := engine.prepareComputerNetwork(t.Context(), namespaceB, 42011)
	if err != nil {
		t.Fatal(err)
	}
	defer attachmentB.close()
	assertNamespaceTCPRefused(t, namespaceA, "tcp4", net.JoinHostPort(attachmentA.gateway, strconv.Itoa(port)))

	var peerListener net.Listener
	if err := inNetworkNamespace(namespaceB, func() error {
		var listenErr error
		peerListener, listenErr = net.Listen("tcp4", net.JoinHostPort(attachmentB.guestAddress, "0"))
		return listenErr
	}); err != nil {
		t.Fatal(err)
	}
	defer peerListener.Close()
	peerPort := peerListener.Addr().(*net.TCPAddr).Port
	peerAddress := net.JoinHostPort(attachmentB.guestAddress, strconv.Itoa(peerPort))
	assertNamespaceTCPRefused(t, namespaceA, "tcp4", peerAddress)
	prependForeignAccept(t, iptablesPath, "filter", "FORWARD")
	assertNamespaceTCPConnected(t, namespaceA, "tcp4", peerAddress)
	if err := engine.reconcileComputerFirewall(t.Context()); err != nil {
		t.Fatal(err)
	}
	present, err = observeComputerFirewall(t.Context(), iptablesPath, ip6tablesPath, []computerNetworkAttachment{*attachmentA, *attachmentB})
	if err != nil || !present {
		t.Fatalf("Computer firewall observation after periodic reconcile = present %t, err %v; want present", present, err)
	}
	assertNamespaceTCPRefused(t, namespaceA, "tcp4", peerAddress)
}

func TestComputerFirewallObservationAndRepairRequireFirstJump(t *testing.T) {
	requireRootNetworkNamespaceTest(t)
	resetComputerFirewallChainsForTest(t)
	iptablesPath, err := resolveRootOwnedNetworkTool("", "iptables", "/usr/sbin/iptables", "/usr/bin/iptables", "/sbin/iptables")
	if err != nil {
		t.Fatal(err)
	}
	ip6tablesPath, err := resolveRootOwnedNetworkTool("", "ip6tables", "/usr/sbin/ip6tables", "/usr/bin/ip6tables", "/sbin/ip6tables")
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureComputerFirewallFamilies(t.Context(), iptablesPath, ip6tablesPath); err != nil {
		t.Fatal(err)
	}
	for _, executable := range []string{iptablesPath, ip6tablesPath} {
		for _, chain := range []struct {
			table string
			name  string
		}{{"filter", "INPUT"}, {"filter", "FORWARD"}, {"nat", "POSTROUTING"}} {
			t.Run(filepath.Base(executable)+"/"+chain.name, func(t *testing.T) {
				prependForeignAccept(t, executable, chain.table, chain.name)
				present, err := observeComputerFirewall(t.Context(), iptablesPath, ip6tablesPath, nil)
				if err != nil || present {
					t.Fatalf("Computer firewall with foreign first rule = present %t, err %v; want absent", present, err)
				}
				if err := ensureComputerFirewallFamilies(t.Context(), iptablesPath, ip6tablesPath); err != nil {
					t.Fatal(err)
				}
				present, err = observeComputerFirewall(t.Context(), iptablesPath, ip6tablesPath, nil)
				if err != nil || !present {
					t.Fatalf("Computer firewall after ordered repair = present %t, err %v; want present", present, err)
				}
			})
		}
	}
}

func prependForeignAccept(t *testing.T, executable, table, chain string) {
	t.Helper()
	prefix := []string{"-w", "5"}
	if table != "filter" {
		prefix = append(prefix, "-t", table)
	}
	arguments := append(slices.Clone(prefix), "-I", chain, "1", "-j", "ACCEPT")
	if output, err := exec.CommandContext(t.Context(), executable, arguments...).CombinedOutput(); err != nil {
		t.Fatalf("prepend foreign %s/%s ACCEPT through %s: %v: %s", table, chain, executable, err, strings.TrimSpace(string(output)))
	}
	t.Cleanup(func() {
		arguments := append(slices.Clone(prefix), "-D", chain, "-j", "ACCEPT")
		_, _ = exec.Command(executable, arguments...).CombinedOutput()
	})
}

func TestComputerIPv6FirewallRejectsLiveNodeListenerWhenGuestReenablesIPv6(t *testing.T) {
	requireRootNetworkNamespaceTest(t)
	resetComputerFirewallChainsForTest(t)
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
	ip6tablesPath, err := resolveRootOwnedNetworkTool("", "ip6tables", "/usr/sbin/ip6tables", "/usr/bin/ip6tables", "/sbin/ip6tables")
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureComputerFirewallFamilies(t.Context(), iptablesPath, ip6tablesPath); err != nil {
		t.Fatal(err)
	}
	attachment, err := setupComputerNetworkWithTools(t.Context(), namespace, 42013, 42000, ipPath, iptablesPath, ip6tablesPath, "/etc/resolv.conf")
	if err != nil {
		t.Fatal(err)
	}
	defer attachment.close()
	const hostIPv6 = "fd46:5746:a41d::1"
	const guestIPv6 = "fd46:5746:a41d::2"
	if output, err := exec.CommandContext(t.Context(), ipPath, "-6", "addr", "add", hostIPv6+"/64", "dev", attachment.hostLink, "nodad").CombinedOutput(); err != nil {
		t.Fatalf("address Computer host veth for IPv6 defence test: %v: %s", err, strings.TrimSpace(string(output)))
	}
	if err := inNetworkNamespace(namespace, func() error {
		for _, name := range []string{"all", "default", computerGuestLinkName} {
			if err := os.WriteFile("/proc/sys/net/ipv6/conf/"+name+"/disable_ipv6", []byte("0\n"), 0); err != nil {
				return err
			}
		}
		output, commandErr := exec.CommandContext(t.Context(), ipPath, "-6", "addr", "add", guestIPv6+"/64", "dev", computerGuestLinkName, "nodad").CombinedOutput()
		if commandErr != nil {
			return fmt.Errorf("address Computer guest veth for IPv6 defence test: %w: %s", commandErr, strings.TrimSpace(string(output)))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp6", "[::]:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	assertTCPConnected(t, "tcp6", net.JoinHostPort("::1", strconv.Itoa(port)))
	assertNamespaceTCPRefused(t, namespace, "tcp6", net.JoinHostPort(hostIPv6, strconv.Itoa(port)))

	for _, rejectRule := range [][]string{
		{"-w", "5", "-D", computerFirewallInput, "-i", computerHostLinkPrefix + "+", "-p", "tcp", "-j", "REJECT", "--reject-with", "tcp-reset"},
		{"-w", "5", "-D", computerFirewallInput, "-i", computerHostLinkPrefix + "+", "-j", "REJECT", "--reject-with", "icmp6-port-unreachable"},
	} {
		if output, err := exec.CommandContext(t.Context(), ip6tablesPath, rejectRule...).CombinedOutput(); err != nil {
			t.Fatalf("delete Computer IPv6 INPUT rejection: %v: %s", err, strings.TrimSpace(string(output)))
		}
	}
	t.Cleanup(func() { _ = ensureComputerFirewallFamilies(context.Background(), iptablesPath, ip6tablesPath) })
	assertNamespaceTCPConnected(t, namespace, "tcp6", net.JoinHostPort(hostIPv6, strconv.Itoa(port)))
}

func TestComputerProbeRuleRejectsForeignBinderAfterHelperListenerCloses(t *testing.T) {
	requireRootNetworkNamespaceTest(t)
	resetComputerFirewallChainsForTest(t)
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
	attachment, err := setupComputerNetwork(t.Context(), namespace, 42012, 42000, ipPath, iptablesPath, "/etc/resolv.conf")
	if err != nil {
		t.Fatal(err)
	}
	defer attachment.close()
	if err := attachment.probe.Close(); err != nil {
		t.Fatal(err)
	}
	attachment.probe = nil
	foreign, err := net.Listen("tcp4", net.JoinHostPort(attachment.gateway, strconv.Itoa(int(attachment.probePort))))
	if err != nil {
		t.Fatal(err)
	}
	defer foreign.Close()
	assertNamespaceTCPRefused(t, namespace, "tcp4", foreign.Addr().String())
}

func TestComputerNetworkCrashResidueIsSweptWithTypedEvidence(t *testing.T) {
	requireRootNetworkNamespaceTest(t)
	resetComputerFirewallChainsForTest(t)
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
	ip6tablesPath, err := resolveRootOwnedNetworkTool("", "ip6tables", "/usr/sbin/ip6tables", "/usr/bin/ip6tables", "/sbin/ip6tables")
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureComputerFirewallFamilies(t.Context(), iptablesPath, ip6tablesPath); err != nil {
		t.Fatal(err)
	}
	cleanupEngine := &ContainerdEngine{config: NativeEngineConfig{IPExecutable: ipPath, IPTablesExecutable: iptablesPath, IP6TablesExecutable: ip6tablesPath}, computerFirewallConfigured: true}
	defer cleanupEngine.closeComputerFirewall(context.Background())
	attachment, err := setupComputerNetworkWithTools(t.Context(), namespace, 42014, 42000, ipPath, iptablesPath, ip6tablesPath, "/etc/resolv.conf")
	if err != nil {
		t.Fatal(err)
	}
	staleLink := attachment.hostLink
	if err := attachment.probe.Close(); err != nil {
		t.Fatal(err)
	}
	attachment.probe = nil
	if attachment.dns != nil {
		if err := attachment.dns.close(); err != nil {
			t.Fatal(err)
		}
		attachment.dns = nil
	}
	staleGuestLink := computerGuestLinkPrefix + "64998"
	if output, err := exec.CommandContext(t.Context(), ipPath, "link", "add", staleGuestLink, "type", "dummy").CombinedOutput(); err != nil {
		t.Fatalf("create stale Computer guest link: %v: %s", err, strings.TrimSpace(string(output)))
	}
	t.Cleanup(func() { _, _ = exec.Command(ipPath, "link", "del", staleGuestLink).CombinedOutput() })
	staleIPv6Link := computerHostLinkPrefix + "64999"
	staleIPv6Rule := []string{"-w", "5", "-A", computerFirewallInput, "-i", staleIPv6Link, "-j", "REJECT", "--reject-with", "icmp6-port-unreachable"}
	if output, err := exec.CommandContext(t.Context(), ip6tablesPath, staleIPv6Rule...).CombinedOutput(); err != nil {
		t.Fatalf("create stale Computer IPv6 firewall rule: %v: %s", err, strings.TrimSpace(string(output)))
	}
	t.Cleanup(func() {
		staleIPv6Rule[2] = "-D"
		_, _ = exec.Command(ip6tablesPath, staleIPv6Rule...).CombinedOutput()
	})
	engine := &ContainerdEngine{config: NativeEngineConfig{IPExecutable: ipPath, IPTablesExecutable: iptablesPath, IP6TablesExecutable: ip6tablesPath}, attempts: map[string]*containerdAttempt{}}
	inventory, evidence, err := engine.sweepComputerNetworkResidue(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(inventory.ComputerNetworkLinks, staleLink) || !slices.Contains(inventory.ComputerNetworkLinks, staleGuestLink) || len(inventory.ComputerFirewallRules) < 3 {
		t.Fatalf("crash residue inventory = %+v", inventory)
	}
	if !slices.ContainsFunc(evidence, func(item SweepEvidence) bool {
		return item.Class == RemovalResourceComputerNetworkLink && item.ID == staleLink && item.Action == SweepActionRemoved && item.Method == "ip_link_delete"
	}) || !slices.ContainsFunc(evidence, func(item SweepEvidence) bool {
		return item.Class == RemovalResourceComputerFirewallRule && item.Action == SweepActionRemoved && item.Method == "iptables_delete"
	}) || !slices.ContainsFunc(evidence, func(item SweepEvidence) bool {
		return item.Class == RemovalResourceComputerFirewallRule && item.Action == SweepActionRemoved && item.Method == "ip6tables_delete"
	}) {
		t.Fatalf("crash residue evidence = %+v", evidence)
	}
	if _, err := netlink.LinkByName(staleLink); err == nil {
		t.Fatalf("stale Computer link %s remained", staleLink)
	}
	remaining, err := computerResidueFirewallRules(t.Context(), iptablesPath, ip6tablesPath)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("stale Computer firewall rules remained: %+v err=%v", remaining, err)
	}
	attachment.hostLink = ""
}

func TestContainerdEngineCloseTearsDownComputerNetworkState(t *testing.T) {
	requireRootNetworkNamespaceTest(t)
	resetComputerFirewallChainsForTest(t)
	command := startIsolatedNetworkTask(t)
	namespace, err := pinTaskNetworkNamespace(uint32(command.Process.Pid))
	if err != nil {
		t.Fatal(err)
	}
	ipPath, err := resolveRootOwnedNetworkTool("", "ip", "/usr/sbin/ip", "/usr/bin/ip", "/sbin/ip")
	if err != nil {
		t.Fatal(err)
	}
	iptablesPath, err := resolveRootOwnedNetworkTool("", "iptables", "/usr/sbin/iptables", "/usr/bin/iptables", "/sbin/iptables")
	if err != nil {
		t.Fatal(err)
	}
	ip6tablesPath, err := resolveRootOwnedNetworkTool("", "ip6tables", "/usr/sbin/ip6tables", "/usr/bin/ip6tables", "/sbin/ip6tables")
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureComputerFirewallFamilies(t.Context(), iptablesPath, ip6tablesPath); err != nil {
		t.Fatal(err)
	}
	attachment, err := setupComputerNetworkWithTools(t.Context(), namespace, 42015, 42000, ipPath, iptablesPath, ip6tablesPath, "/etc/resolv.conf")
	if err != nil {
		t.Fatal(err)
	}
	link := attachment.hostLink
	engine := &ContainerdEngine{
		config:                     NativeEngineConfig{IPExecutable: ipPath, IPTablesExecutable: iptablesPath, IP6TablesExecutable: ip6tablesPath},
		attempts:                   map[string]*containerdAttempt{"attempt": {networkNamespace: namespace, computerNetwork: attachment}},
		ports:                      map[uint16]string{42015: "attempt"},
		computerFirewallConfigured: true,
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := netlink.LinkByName(link); err == nil {
		t.Fatalf("ContainerdEngine.Close left Computer link %s", link)
	}
	for _, executable := range []string{iptablesPath, ip6tablesPath} {
		output, err := exec.CommandContext(t.Context(), executable, "-w", "5", "-S").CombinedOutput()
		if err != nil || strings.Contains(string(output), computerFirewallInput) {
			t.Fatalf("inspect Computer firewall after ContainerdEngine.Close through %s: err=%v output=%s", executable, err, strings.TrimSpace(string(output)))
		}
	}
}

func assertTCPConnected(t *testing.T, network, address string) {
	t.Helper()
	connection, err := net.DialTimeout(network, address, time.Second)
	if err != nil {
		t.Fatalf("Node dial %s %s: %v", network, address, err)
	}
	_ = connection.Close()
}

func assertNamespaceTCPConnected(t *testing.T, namespace *pinnedNetworkNamespace, network, address string) {
	t.Helper()
	var connection net.Conn
	err := inNetworkNamespace(namespace, func() error {
		var dialErr error
		connection, dialErr = net.DialTimeout(network, address, time.Second)
		return dialErr
	})
	if connection != nil {
		_ = connection.Close()
	}
	if err != nil {
		t.Fatalf("Computer dial %s %s: %v", network, address, err)
	}
}

func assertNamespaceTCPRefused(t *testing.T, namespace *pinnedNetworkNamespace, network, address string) {
	t.Helper()
	var connection net.Conn
	err := inNetworkNamespace(namespace, func() error {
		var dialErr error
		connection, dialErr = net.DialTimeout(network, address, time.Second)
		return dialErr
	})
	if connection != nil {
		_ = connection.Close()
		t.Fatalf("Computer dial %s %s unexpectedly connected", network, address)
	}
	var errno syscall.Errno
	if err == nil || !errors.As(err, &errno) || (errno != syscall.ECONNREFUSED && errno != syscall.ENETUNREACH && errno != syscall.EHOSTUNREACH) {
		t.Fatalf("Computer dial %s %s = %v, want typed refusal/unreachable", network, address, err)
	}
}
