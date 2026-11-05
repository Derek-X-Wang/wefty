//go:build linux

package process

import (
	"bytes"
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

// inspectServiceListeners matches the kernel's listening sockets that can
// receive the backend's connections against the socket descriptors of the
// payload's process group. It reads only the network namespace the Guardian
// shares with its payload, and only the descriptors of processes in that
// group, which run as this user.
func inspectServiceListeners(backend serviceBackend, processGroupID int) (serviceListenerVerdict, error) {
	listening := make(map[string]bool)
	for _, table := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		contents, err := os.ReadFile(table)
		if errors.Is(err, fs.ErrNotExist) {
			// tcp6 is absent on a kernel without IPv6.
			continue
		}
		if err != nil {
			return 0, err
		}
		inodes, err := receivingListenerInodes(contents, backend)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", table, err)
		}
		for _, inode := range inodes {
			listening[inode] = true
		}
	}
	if len(listening) == 0 {
		return serviceListenerUnseen, nil
	}
	owned, unreadable, err := processGroupSockets(processGroupID)
	if err != nil {
		return 0, err
	}
	for inode := range listening {
		if owned[inode] {
			continue
		}
		if unreadable != nil {
			// The listener may belong to the member whose descriptors this
			// user cannot read (for example a setuid child): it is neither
			// confirmed as the payload's nor proven foreign.
			return 0, unreadable
		}
		return serviceListenerForeign, nil
	}
	return serviceListenerOwned, nil
}

// receivingListenerInodes returns the inodes of the LISTEN sockets in one
// /proc/net/tcp or tcp6 table that can receive the backend's connections.
func receivingListenerInodes(table []byte, backend serviceBackend) ([]string, error) {
	var inodes []string
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
		port, err := strconv.ParseUint(hexPort, 16, 16)
		if err != nil {
			return nil, fmt.Errorf("malformed local port %q: %w", hexPort, err)
		}
		if int(port) != backend.port {
			continue
		}
		ip, err := procNetAddress(hexAddress)
		if err != nil {
			return nil, err
		}
		if backend.receives(ip) {
			inodes = append(inodes, fields[9])
		}
	}
	return inodes, nil
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

// processGroupSockets returns the socket inodes held open by members of
// processGroupID, and why some member's descriptors could not be read.
func processGroupSockets(processGroupID int) (sockets map[string]bool, unreadable error, err error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, nil, err
	}
	group := strconv.Itoa(processGroupID)
	sockets = make(map[string]bool)
	for _, entry := range entries {
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
			target, err := os.Readlink(directory + "/fd/" + descriptor.Name())
			if err != nil {
				continue
			}
			if inode, found := strings.CutPrefix(target, "socket:["); found {
				sockets[strings.TrimSuffix(inode, "]")] = true
			}
		}
	}
	return sockets, unreadable, nil
}
