//go:build linux

package process

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"strconv"
	"strings"
)

// inspectServiceListeners matches the kernel's listening sockets on the
// backend port against the socket descriptors of the payload's process group.
// It reads only the network namespace the Guardian shares with its payload,
// and only the descriptors of processes in that group, which run as this
// user.
func inspectServiceListeners(ctx context.Context, backend serviceBackend, processGroupID int) (serviceListenerVerdict, error) {
	var found []procListener
	for _, table := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		contents, err := os.ReadFile(table)
		if errors.Is(err, fs.ErrNotExist) {
			// tcp6 is absent on a kernel without IPv6.
			continue
		}
		if err != nil {
			return 0, err
		}
		listeners, err := portListeners(contents, backend.port)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", table, err)
		}
		found = append(found, listeners...)
	}
	if len(backend.receiving(procServiceListeners(found, nil, nil))) == 0 {
		return serviceListenerUnseen, nil
	}
	owned, unreadable, err := processGroupSockets(ctx, processGroupID)
	if err != nil {
		return 0, err
	}
	return backend.verdict(procServiceListeners(found, owned, unreadable), unreadable)
}

// procListener is one LISTEN row of /proc/net/tcp or tcp6.
type procListener struct {
	ip    net.IP
	inode string
}

// procServiceListeners attributes each listener: the payload's when the
// group holds its inode; unknown when it does not but some member's
// descriptors could not be read, since the listener may be that member's;
// otherwise another process's.
func procServiceListeners(found []procListener, owned map[string]bool, unreadable error) []serviceListener {
	listeners := make([]serviceListener, 0, len(found))
	for _, listener := range found {
		owner := listenerOfOther
		switch {
		case owned[listener.inode]:
			owner = listenerOfPayload
		case unreadable != nil:
			owner = listenerOfUnknown
		}
		listeners = append(listeners, serviceListener{ip: listener.ip, owner: owner})
	}
	return listeners
}

// portListeners returns the LISTEN sockets on port in one /proc/net/tcp or
// tcp6 table.
func portListeners(table []byte, port int) ([]procListener, error) {
	var listeners []procListener
	lines := strings.Split(string(table), "\n")
	for _, line := range lines[1:] {
		// sl local_address rem_address st tx:rx tr:when retrnsmt uid timeout inode
		fields := strings.Fields(line)
		if len(fields) < 10 || fields[3] != "0A" { // 0A is TCP_LISTEN
			continue
		}
		hexAddress, hexPort, found := strings.Cut(fields[1], ":")
		if !found {
			return nil, fmt.Errorf("malformed local address %q", fields[1])
		}
		value, err := strconv.ParseUint(hexPort, 16, 16)
		if err != nil {
			return nil, fmt.Errorf("malformed local port %q: %w", hexPort, err)
		}
		if int(value) != port {
			continue
		}
		ip, err := procNetAddress(hexAddress)
		if err != nil {
			return nil, err
		}
		listeners = append(listeners, procListener{ip: ip, inode: fields[9]})
	}
	return listeners, nil
}

// procNetAddress decodes a /proc/net/tcp address: the network-order bytes
// printed as 32-bit words in host byte order.
func procNetAddress(text string) (net.IP, error) {
	raw, err := hex.DecodeString(text)
	if err != nil || (len(raw) != net.IPv4len && len(raw) != net.IPv6len) {
		return nil, fmt.Errorf("malformed local address %q", text)
	}
	ip := make(net.IP, len(raw))
	for offset := 0; offset < len(raw); offset += 4 {
		binary.NativeEndian.PutUint32(ip[offset:], binary.BigEndian.Uint32(raw[offset:]))
	}
	return ip, nil
}

// readProcLink is os.Readlink outside tests, which inject a denial.
var readProcLink = os.Readlink

// processGroupSockets returns the socket inodes held open by members of
// processGroupID, and why some member's descriptors could not be read.
func processGroupSockets(ctx context.Context, processGroupID int) (sockets map[string]bool, unreadable error, err error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, nil, err
	}
	group := strconv.Itoa(processGroupID)
	sockets = make(map[string]bool)
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		directory := "/proc/" + entry.Name()
		stat, err := os.ReadFile(directory + "/stat")
		if err != nil {
			// The process exited, or it is another user's and hidden.
			continue
		}
		// The command name may contain spaces and parentheses; the fields
		// after its closing parenthesis start with state, ppid, pgrp.
		end := bytes.LastIndexByte(stat, ')')
		if end < 0 {
			continue
		}
		fields := strings.Fields(string(stat[end+1:]))
		if len(fields) < 3 || fields[2] != group {
			continue
		}
		descriptors, err := os.ReadDir(directory + "/fd")
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			unreadable = fmt.Errorf("read descriptors of payload group member %s: %w", entry.Name(), err)
			continue
		}
		for _, descriptor := range descriptors {
			target, err := readProcLink(directory + "/fd/" + descriptor.Name())
			if errors.Is(err, fs.ErrNotExist) {
				// The descriptor was closed meanwhile.
				continue
			}
			if err != nil {
				// Linux checks ptrace access per link, so a readable
				// directory does not prove readable links.
				unreadable = fmt.Errorf("read descriptor %s of payload group member %s: %w", descriptor.Name(), entry.Name(), err)
				continue
			}
			if inode, found := strings.CutPrefix(target, "socket:["); found {
				sockets[strings.TrimSuffix(inode, "]")] = true
			}
		}
	}
	return sockets, unreadable, nil
}
