//go:build linux

package process

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
)

// inspectServiceListeners matches the kernel's listening-socket table for
// port against the socket descriptors of the payload's process group. It
// reads only the network namespace the Guardian shares with its payload, and
// only the descriptors of processes in that group, which run as this user.
func inspectServiceListeners(port, processGroupID int) (serviceListenerVerdict, error) {
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
		lines := strings.Split(string(contents), "\n")
		for _, line := range lines[1:] {
			// sl local_address rem_address st tx:rx tr:when retrnsmt uid timeout inode
			fields := strings.Fields(line)
			if len(fields) < 10 || fields[3] != "0A" { // 0A is TCP_LISTEN
				continue
			}
			_, hexPort, found := strings.Cut(fields[1], ":")
			if !found {
				return 0, fmt.Errorf("malformed %s local address %q", table, fields[1])
			}
			value, err := strconv.ParseUint(hexPort, 16, 16)
			if err != nil {
				return 0, fmt.Errorf("malformed %s local port %q: %w", table, hexPort, err)
			}
			if int(value) == port {
				listening[fields[9]] = true
			}
		}
	}
	if len(listening) == 0 {
		return serviceListenerUnseen, nil
	}
	owned, err := processGroupSockets(processGroupID)
	if err != nil {
		return 0, err
	}
	for inode := range listening {
		if !owned[inode] {
			return serviceListenerForeign, nil
		}
	}
	return serviceListenerOwned, nil
}

// processGroupSockets returns the socket inodes held open by members of
// processGroupID.
func processGroupSockets(processGroupID int) (map[string]bool, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	group := strconv.Itoa(processGroupID)
	sockets := make(map[string]bool)
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
			// A member whose sockets cannot be read cannot prove ownership.
			return nil, fmt.Errorf("read descriptors of payload group member %s: %w", entry.Name(), err)
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
	return sockets, nil
}
