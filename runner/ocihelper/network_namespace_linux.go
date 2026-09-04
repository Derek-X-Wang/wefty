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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	netlink "github.com/tailscale/netlink"
	"golang.org/x/sys/unix"
)

const (
	computerNetworkBase     = uint32(198)<<24 | uint32(18)<<16
	computerHostLinkPrefix  = "wftch"
	computerGuestLinkPrefix = "wftcg"
	computerGuestLinkName   = "eth0"
	computerFirewallInput   = "WEFTY-COMPUTER-IN"
	computerFirewallForward = "WEFTY-COMPUTER-FWD"
	computerFirewallNAT     = "WEFTY-COMPUTER-NAT"
	computerEgressProbeBody = "wefty-computer-egress-v1\n"
)

type pinnedNetworkNamespace struct {
	mu   sync.Mutex
	file *os.File
}

func pinTaskNetworkNamespace(pid uint32) (*pinnedNetworkNamespace, error) {
	if pid == 0 {
		return nil, errors.New("task PID is required for network namespace access")
	}
	file, err := os.Open(fmt.Sprintf("/proc/%d/ns/net", pid))
	if err != nil {
		return nil, fmt.Errorf("pin task network namespace: %w", err)
	}
	return &pinnedNetworkNamespace{file: file}, nil
}

func (namespace *pinnedNetworkNamespace) duplicate() (*os.File, error) {
	if namespace == nil {
		return nil, errors.New("task network namespace is not pinned")
	}
	namespace.mu.Lock()
	defer namespace.mu.Unlock()
	if namespace.file == nil {
		return nil, errors.New("task network namespace is released")
	}
	fd, err := unix.Dup(int(namespace.file.Fd()))
	if err != nil {
		return nil, fmt.Errorf("duplicate task network namespace: %w", err)
	}
	unix.CloseOnExec(fd)
	return os.NewFile(uintptr(fd), "task-network-namespace"), nil
}

func (namespace *pinnedNetworkNamespace) close() error {
	if namespace == nil {
		return nil
	}
	namespace.mu.Lock()
	defer namespace.mu.Unlock()
	if namespace.file == nil {
		return nil
	}
	err := namespace.file.Close()
	namespace.file = nil
	return err
}

// inNetworkNamespace runs one socket or observation operation on a locked OS
// thread. The target descriptor is duplicated before the thread starts, so an
// attempt release can never redirect the operation through a reused task PID.
func inNetworkNamespace(namespace *pinnedNetworkNamespace, operation func() error) error {
	if operation == nil {
		return errors.New("network namespace operation is required")
	}
	target, err := namespace.duplicate()
	if err != nil {
		return err
	}
	defer target.Close()
	current, err := os.Open("/proc/self/ns/net")
	if err != nil {
		return fmt.Errorf("open helper network namespace: %w", err)
	}
	defer current.Close()

	result := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		restored := false
		defer func() {
			if restored {
				runtime.UnlockOSThread()
			}
		}()
		if err := unix.Setns(int(target.Fd()), unix.CLONE_NEWNET); err != nil {
			restored = true
			result <- fmt.Errorf("enter task network namespace: %w", err)
			return
		}
		operationErr := operation()
		if err := unix.Setns(int(current.Fd()), unix.CLONE_NEWNET); err != nil {
			result <- errors.Join(operationErr, fmt.Errorf("restore helper network namespace: %w", err))
			return
		}
		restored = true
		result <- operationErr
	}()
	return <-result
}

type computerNetworkAttachment struct {
	hostLink     string
	hostAddress  string
	guestAddress string
	gateway      string
	ipPath       string
	iptablesPath string
	probePort    uint16
	probe        net.Listener
	dns          *computerDNSProxy
}

type computerDNSProxy struct {
	udp       net.PacketConn
	tcp       net.Listener
	upstream  string
	closeOnce sync.Once
	closeErr  error
}

func resolveRootOwnedNetworkTool(configured, name string, candidates ...string) (string, error) {
	paths := candidates
	if configured != "" {
		paths = []string{configured}
	}
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) && configured == "" {
				continue
			}
			return "", fmt.Errorf("inspect %s executable %s: %w", name, path, err)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0o022 != 0 || info.Mode().Perm()&0o111 == 0 {
			return "", fmt.Errorf("%s executable must be an absolute root-owned non-writable executable: %s", name, path)
		}
		return path, nil
	}
	return "", fmt.Errorf("%s executable is unavailable", name)
}

func (engine *ContainerdEngine) prepareComputerNetwork(ctx context.Context, namespace *pinnedNetworkNamespace, port uint16) (*computerNetworkAttachment, error) {
	ipPath, err := resolveRootOwnedNetworkTool(engine.config.IPExecutable, "ip", "/usr/sbin/ip", "/usr/bin/ip", "/sbin/ip")
	if err != nil {
		return nil, err
	}
	iptablesPath, err := resolveRootOwnedNetworkTool(engine.config.IPTablesExecutable, "iptables", "/usr/sbin/iptables", "/usr/bin/iptables", "/sbin/iptables")
	if err != nil {
		return nil, err
	}
	engine.computerFirewallOnce.Do(func() {
		engine.computerFirewallErr = ensureComputerFirewall(ctx, iptablesPath)
	})
	if engine.computerFirewallErr != nil {
		return nil, engine.computerFirewallErr
	}
	return setupComputerNetwork(ctx, namespace, port, engine.config.AttemptPortMin, ipPath, iptablesPath)
}

func computerNetworkAddresses(port, minimum uint16) (host, guest net.IP, err error) {
	if port < minimum {
		return nil, nil, errors.New("Computer endpoint port precedes the allocation range")
	}
	offset := uint32(port-minimum) * 4
	if offset > (1<<17)-4 {
		return nil, nil, errors.New("Computer network allocation exceeds the helper-owned address range")
	}
	return uint32IPv4(computerNetworkBase + offset + 1), uint32IPv4(computerNetworkBase + offset + 2), nil
}

func uint32IPv4(value uint32) net.IP {
	return net.IPv4(byte(value>>24), byte(value>>16), byte(value>>8), byte(value))
}

func setupComputerNetwork(ctx context.Context, namespace *pinnedNetworkNamespace, port, minimum uint16, ipPath, iptablesPath string) (_ *computerNetworkAttachment, err error) {
	if namespace == nil || port == 0 || ipPath == "" || iptablesPath == "" {
		return nil, errors.New("Computer network setup requires namespace, port, ip, and iptables")
	}
	hostAddress, guestAddress, err := computerNetworkAddresses(port, minimum)
	if err != nil {
		return nil, err
	}
	hostLink := computerHostLinkPrefix + strconv.Itoa(int(port))
	guestLink := computerGuestLinkPrefix + strconv.Itoa(int(port))
	attachment := &computerNetworkAttachment{
		hostLink: hostLink, hostAddress: hostAddress.String(), guestAddress: guestAddress.String(),
		gateway: hostAddress.String(), ipPath: ipPath, iptablesPath: iptablesPath, probePort: port,
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, attachment.close())
		}
	}()
	run := func(arguments ...string) error {
		output, commandErr := exec.CommandContext(ctx, ipPath, arguments...).CombinedOutput()
		if commandErr != nil {
			return fmt.Errorf("%s %s: %w: %s", ipPath, strings.Join(arguments, " "), commandErr, strings.TrimSpace(string(output)))
		}
		return nil
	}
	if output, cleanupErr := exec.CommandContext(ctx, ipPath, "link", "del", hostLink).CombinedOutput(); cleanupErr != nil && !strings.Contains(string(output), "Cannot find device") {
		return nil, fmt.Errorf("remove stale Computer veth %s: %w: %s", hostLink, cleanupErr, strings.TrimSpace(string(output)))
	}
	if err = run("link", "add", hostLink, "type", "veth", "peer", "name", guestLink); err != nil {
		return nil, err
	}
	target, duplicateErr := namespace.duplicate()
	if duplicateErr != nil {
		return nil, duplicateErr
	}
	link, linkErr := netlink.LinkByName(guestLink)
	if linkErr == nil {
		linkErr = netlink.LinkSetNsFd(link, int(target.Fd()))
	}
	closeErr := target.Close()
	if linkErr != nil || closeErr != nil {
		err = errors.Join(linkErr, closeErr)
		return nil, err
	}
	if err = run("addr", "add", hostAddress.String()+"/30", "dev", hostLink); err != nil {
		return nil, err
	}
	if err = run("link", "set", hostLink, "up"); err != nil {
		return nil, err
	}
	err = inNetworkNamespace(namespace, func() error {
		for _, arguments := range [][]string{
			{"link", "set", "lo", "up"},
			{"link", "set", guestLink, "name", computerGuestLinkName},
			{"addr", "add", guestAddress.String() + "/30", "dev", computerGuestLinkName},
			{"link", "set", computerGuestLinkName, "up"},
			{"route", "add", "default", "via", hostAddress.String()},
		} {
			if commandErr := run(arguments...); commandErr != nil {
				return commandErr
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	resolverAddress, err := computerResolverAddress("/etc/resolv.conf")
	if err != nil {
		return nil, err
	}
	if net.ParseIP(resolverAddress).IsLoopback() {
		attachment.dns, err = startComputerDNSProxy(namespace, net.JoinHostPort(resolverAddress, "53"), net.JoinHostPort(resolverAddress, "53"))
		if err != nil {
			return nil, fmt.Errorf("start Computer loopback resolver proxy: %w", err)
		}
	}
	attachment.probe, err = net.Listen("tcp4", net.JoinHostPort(attachment.gateway, strconv.Itoa(int(port))))
	if err != nil {
		return nil, fmt.Errorf("listen on Computer egress probe: %w", err)
	}
	allowRule := []string{"-w", "5", "-I", computerFirewallInput, "1", "-i", hostLink, "-d", attachment.gateway, "-p", "tcp", "--dport", strconv.Itoa(int(port)), "-j", "ACCEPT"}
	if output, commandErr := exec.CommandContext(ctx, iptablesPath, allowRule...).CombinedOutput(); commandErr != nil {
		return nil, fmt.Errorf("allow Computer egress probe: %w: %s", commandErr, strings.TrimSpace(string(output)))
	}
	go serveComputerEgressProbe(attachment.probe)
	return attachment, nil
}

func computerResolverAddress(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open Computer resolver configuration: %w", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 || fields[0] != "nameserver" {
			continue
		}
		address := net.ParseIP(fields[1])
		if address == nil || address.To4() == nil {
			continue
		}
		return address.String(), nil
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("read Computer resolver configuration: %w", err)
	}
	return "", errors.New("Computer resolver configuration has no IPv4 nameserver")
}

func startComputerDNSProxy(namespace *pinnedNetworkNamespace, listenAddress, upstreamAddress string) (_ *computerDNSProxy, err error) {
	if namespace == nil || listenAddress == "" || upstreamAddress == "" {
		return nil, errors.New("Computer DNS proxy requires namespace, listener, and upstream")
	}
	proxy := &computerDNSProxy{upstream: upstreamAddress}
	defer func() {
		if err != nil {
			_ = proxy.close()
		}
	}()
	err = inNetworkNamespace(namespace, func() error {
		var listenErr error
		proxy.udp, listenErr = net.ListenPacket("udp4", listenAddress)
		if listenErr != nil {
			return listenErr
		}
		proxy.tcp, listenErr = net.Listen("tcp4", listenAddress)
		return listenErr
	})
	if err != nil {
		return nil, err
	}
	go proxy.serveUDP()
	go proxy.serveTCP()
	return proxy, nil
}

func (proxy *computerDNSProxy) serveUDP() {
	listener := proxy.udp
	buffer := make([]byte, 65535)
	for {
		length, client, err := listener.ReadFrom(buffer)
		if err != nil {
			return
		}
		upstream, err := net.DialTimeout("udp4", proxy.upstream, 2*time.Second)
		if err != nil {
			continue
		}
		_ = upstream.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := upstream.Write(buffer[:length]); err != nil {
			_ = upstream.Close()
			continue
		}
		response := make([]byte, 65535)
		length, err = upstream.Read(response)
		_ = upstream.Close()
		if err == nil {
			_, _ = listener.WriteTo(response[:length], client)
		}
	}
}

func (proxy *computerDNSProxy) serveTCP() {
	listener := proxy.tcp
	for {
		client, err := listener.Accept()
		if err != nil {
			return
		}
		_ = proxy.forwardTCP(client)
		_ = client.Close()
	}
}

func (proxy *computerDNSProxy) forwardTCP(client net.Conn) error {
	upstream, err := net.DialTimeout("tcp4", proxy.upstream, 2*time.Second)
	if err != nil {
		return err
	}
	defer upstream.Close()
	_ = client.SetDeadline(time.Now().Add(5 * time.Second))
	_ = upstream.SetDeadline(time.Now().Add(5 * time.Second))
	for {
		header := make([]byte, 2)
		if _, err := io.ReadFull(client, header); err != nil {
			return err
		}
		query := make([]byte, int(binary.BigEndian.Uint16(header)))
		if _, err := io.ReadFull(client, query); err != nil {
			return err
		}
		if _, err := upstream.Write(append(header, query...)); err != nil {
			return err
		}
		if _, err := io.ReadFull(upstream, header); err != nil {
			return err
		}
		response := make([]byte, int(binary.BigEndian.Uint16(header)))
		if _, err := io.ReadFull(upstream, response); err != nil {
			return err
		}
		if _, err := client.Write(append(header, response...)); err != nil {
			return err
		}
	}
}

func (proxy *computerDNSProxy) close() error {
	if proxy == nil {
		return nil
	}
	proxy.closeOnce.Do(func() {
		var errs []error
		if proxy.udp != nil {
			errs = append(errs, proxy.udp.Close())
		}
		if proxy.tcp != nil {
			errs = append(errs, proxy.tcp.Close())
		}
		proxy.closeErr = errors.Join(errs...)
	})
	return proxy.closeErr
}

func serveComputerEgressProbe(listener net.Listener) {
	for {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
		_, _ = connection.Write([]byte("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 25\r\nConnection: close\r\n\r\n" + computerEgressProbeBody))
		_ = connection.Close()
	}
}

func (attachment *computerNetworkAttachment) close() error {
	if attachment == nil || attachment.hostLink == "" || attachment.ipPath == "" {
		return nil
	}
	if attachment.probe != nil {
		_ = attachment.probe.Close()
		attachment.probe = nil
	}
	if attachment.dns != nil {
		_ = attachment.dns.close()
		attachment.dns = nil
	}
	if attachment.iptablesPath != "" && attachment.probePort != 0 {
		arguments := []string{"-w", "5", "-D", computerFirewallInput, "-i", attachment.hostLink, "-d", attachment.gateway, "-p", "tcp", "--dport", strconv.Itoa(int(attachment.probePort)), "-j", "ACCEPT"}
		_, _ = exec.Command(attachment.iptablesPath, arguments...).CombinedOutput()
	}
	output, err := exec.Command(attachment.ipPath, "link", "del", attachment.hostLink).CombinedOutput()
	if err != nil && !strings.Contains(string(output), "Cannot find device") {
		return fmt.Errorf("remove Computer veth %s: %w: %s", attachment.hostLink, err, strings.TrimSpace(string(output)))
	}
	attachment.hostLink = ""
	return nil
}

func ensureComputerFirewall(ctx context.Context, iptablesPath string) error {
	if iptablesPath == "" {
		return errors.New("Computer firewall requires iptables")
	}
	if err := os.WriteFile("/proc/sys/net/ipv4/ip_forward", []byte("1\n"), 0); err != nil {
		return fmt.Errorf("enable Computer IPv4 forwarding: %w", err)
	}
	run := func(arguments ...string) error {
		output, err := exec.CommandContext(ctx, iptablesPath, append([]string{"-w", "5"}, arguments...)...).CombinedOutput()
		if err != nil {
			return fmt.Errorf("%s %s: %w: %s", iptablesPath, strings.Join(arguments, " "), err, strings.TrimSpace(string(output)))
		}
		return nil
	}
	ensureChain := func(table, chain string) error {
		arguments := []string{}
		if table != "filter" {
			arguments = append(arguments, "-t", table)
		}
		if err := run(append(arguments, "-N", chain)...); err != nil {
			if checkErr := run(append(arguments, "-L", chain)...); checkErr != nil {
				return err
			}
		}
		return nil
	}
	ensureRule := func(table, chain string, rule ...string) error {
		prefix := []string{}
		if table != "filter" {
			prefix = append(prefix, "-t", table)
		}
		if err := run(append(append(prefix, "-C", chain), rule...)...); err == nil {
			return nil
		}
		return run(append(append(prefix, "-I", chain, "1"), rule...)...)
	}
	for _, chain := range []struct{ table, name string }{{"filter", computerFirewallInput}, {"filter", computerFirewallForward}, {"nat", computerFirewallNAT}} {
		if err := ensureChain(chain.table, chain.name); err != nil {
			return err
		}
		arguments := []string{}
		if chain.table != "filter" {
			arguments = append(arguments, "-t", chain.table)
		}
		if err := run(append(arguments, "-F", chain.name)...); err != nil {
			return err
		}
	}
	for _, jump := range []struct {
		table, chain, target string
	}{{"filter", "INPUT", computerFirewallInput}, {"filter", "FORWARD", computerFirewallForward}, {"nat", "POSTROUTING", computerFirewallNAT}} {
		if err := ensureRule(jump.table, jump.chain, "-j", jump.target); err != nil {
			return err
		}
	}
	for _, rule := range []struct {
		table, chain string
		arguments    []string
	}{
		{"filter", computerFirewallInput, []string{"-i", computerHostLinkPrefix + "+", "-j", "REJECT", "--reject-with", "icmp-port-unreachable"}},
		{"filter", computerFirewallForward, []string{"-i", computerHostLinkPrefix + "+", "-o", computerHostLinkPrefix + "+", "-j", "REJECT", "--reject-with", "icmp-port-unreachable"}},
		{"filter", computerFirewallForward, []string{"-o", computerHostLinkPrefix + "+", "-m", "conntrack", "--ctstate", "ESTABLISHED,RELATED", "-j", "ACCEPT"}},
		{"filter", computerFirewallForward, []string{"-i", computerHostLinkPrefix + "+", "-j", "ACCEPT"}},
		{"filter", computerFirewallForward, []string{"-o", computerHostLinkPrefix + "+", "-j", "REJECT", "--reject-with", "icmp-port-unreachable"}},
		{"nat", computerFirewallNAT, []string{"-s", "198.18.0.0/15", "-j", "MASQUERADE"}},
	} {
		prefix := []string{}
		if rule.table != "filter" {
			prefix = append(prefix, "-t", rule.table)
		}
		if err := run(append(append(prefix, "-A", rule.chain), rule.arguments...)...); err != nil {
			return err
		}
	}
	return nil
}

func listenTaskLoopback(namespace *pinnedNetworkNamespace, port uint16) (net.Listener, error) {
	var listener net.Listener
	err := inNetworkNamespace(namespace, func() error {
		var listenErr error
		listener, listenErr = net.Listen("tcp4", net.JoinHostPort("127.0.0.1", fmt.Sprint(port)))
		return listenErr
	})
	return listener, err
}

func dialTaskLoopback(ctx context.Context, namespace *pinnedNetworkNamespace, port uint16) (net.Conn, error) {
	var connection net.Conn
	err := inNetworkNamespace(namespace, func() error {
		var dialErr error
		connection, dialErr = (&net.Dialer{}).DialContext(ctx, "tcp4", net.JoinHostPort("127.0.0.1", fmt.Sprint(port)))
		return dialErr
	})
	return connection, err
}

func loopbackListenInode(namespace *pinnedNetworkNamespace, port uint16) (inode string, found bool, err error) {
	read := func(path string) error {
		file, openErr := os.Open(path)
		if openErr != nil {
			return openErr
		}
		defer file.Close()
		want := fmt.Sprintf("0100007F:%04X", port)
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) > 9 && fields[1] == want && fields[3] == "0A" {
				inode, found = fields[9], true
				return nil
			}
		}
		return scanner.Err()
	}
	if namespace == nil {
		err = read("/proc/net/tcp")
	} else {
		// setns is scoped to the locked worker thread. /proc/net aliases the
		// thread-group leader's namespace, so it can keep reporting the helper
		// namespace after this thread enters the Computer. thread-self follows
		// the actual reader and preserves the old /proc/<task>/net authority
		// without re-resolving a raw task PID.
		err = inNetworkNamespace(namespace, func() error { return read("/proc/thread-self/net/tcp") })
	}
	return inode, found, err
}

func observeComputerNetworkIsolation(namespace *pinnedNetworkNamespace, abstractSocketName string) (helperInode, taskInode string, hostVisible bool, err error) {
	if abstractSocketName == "" {
		return "", "", false, errors.New("Computer abstract X socket name is required")
	}
	helper, err := os.Open("/proc/self/ns/net")
	if err != nil {
		return "", "", false, fmt.Errorf("open helper network namespace for observation: %w", err)
	}
	defer helper.Close()
	target, err := namespace.duplicate()
	if err != nil {
		return "", "", false, err
	}
	defer target.Close()
	helperInfo, err := helper.Stat()
	if err != nil {
		return "", "", false, fmt.Errorf("stat helper network namespace: %w", err)
	}
	taskInfo, err := target.Stat()
	if err != nil {
		return "", "", false, fmt.Errorf("stat task network namespace: %w", err)
	}
	helperStat, helperOK := helperInfo.Sys().(*syscall.Stat_t)
	taskStat, taskOK := taskInfo.Sys().(*syscall.Stat_t)
	if !helperOK || !taskOK {
		return "", "", false, errors.New("network namespace inode metadata is unavailable")
	}
	helperInode, taskInode = strconv.FormatUint(helperStat.Ino, 10), strconv.FormatUint(taskStat.Ino, 10)
	file, err := os.Open("/proc/net/unix")
	if err != nil {
		return "", "", false, fmt.Errorf("read helper abstract sockets: %w", err)
	}
	defer file.Close()
	hostVisible, err = abstractSocketVisible(file, abstractSocketName)
	if err != nil {
		return "", "", false, err
	}
	return helperInode, taskInode, hostVisible, nil
}

func abstractSocketVisible(source io.Reader, exactName string) (bool, error) {
	scanner := bufio.NewScanner(source)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 8 && fields[len(fields)-1] == exactName {
			return true, nil
		}
	}
	return false, scanner.Err()
}
