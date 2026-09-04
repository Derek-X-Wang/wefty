//go:build linux

package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

func taskNetworkNamespacePath(pid uint32) (string, error) {
	if pid == 0 {
		return "", errors.New("task PID is required for network namespace access")
	}
	return fmt.Sprintf("/proc/%d/ns/net", pid), nil
}

// inNetworkNamespace runs one socket-creation operation on a locked OS thread.
// A socket retains its network-namespace association after the thread returns
// to the helper namespace, so callers never leave a general-purpose worker in
// the Computer namespace.
func inNetworkNamespace(path string, operation func() error) error {
	if path == "" || operation == nil {
		return errors.New("network namespace path and operation are required")
	}
	current, err := os.Open("/proc/self/ns/net")
	if err != nil {
		return fmt.Errorf("open helper network namespace: %w", err)
	}
	defer current.Close()
	target, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open task network namespace: %w", err)
	}
	defer target.Close()

	result := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		restored := false
		defer func() {
			// A locked goroutine that exits without UnlockOSThread causes the Go
			// runtime to retire the OS thread. That is the only safe outcome if
			// restoring the helper namespace unexpectedly fails.
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

func bringUpTaskLoopback(pid uint32) error {
	path, err := taskNetworkNamespacePath(pid)
	if err != nil {
		return err
	}
	return inNetworkNamespace(path, func() error {
		fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
		if err != nil {
			return fmt.Errorf("open loopback control socket: %w", err)
		}
		defer unix.Close(fd)
		request, err := unix.NewIfreq("lo")
		if err != nil {
			return fmt.Errorf("address loopback interface: %w", err)
		}
		if err := unix.IoctlIfreq(fd, unix.SIOCGIFFLAGS, request); err != nil {
			return fmt.Errorf("read loopback flags: %w", err)
		}
		request.SetUint16(request.Uint16() | unix.IFF_UP)
		if err := unix.IoctlIfreq(fd, unix.SIOCSIFFLAGS, request); err != nil {
			return fmt.Errorf("bring loopback up: %w", err)
		}
		return nil
	})
}

func listenTaskLoopback(pid uint32, port uint16) (net.Listener, error) {
	path, err := taskNetworkNamespacePath(pid)
	if err != nil {
		return nil, err
	}
	var listener net.Listener
	err = inNetworkNamespace(path, func() error {
		var listenErr error
		listener, listenErr = net.Listen("tcp4", net.JoinHostPort("127.0.0.1", fmt.Sprint(port)))
		return listenErr
	})
	if err != nil {
		return nil, err
	}
	return listener, nil
}

func dialTaskLoopback(ctx context.Context, pid uint32, port uint16) (net.Conn, error) {
	path, err := taskNetworkNamespacePath(pid)
	if err != nil {
		return nil, err
	}
	var connection net.Conn
	err = inNetworkNamespace(path, func() error {
		var dialErr error
		connection, dialErr = (&net.Dialer{}).DialContext(ctx, "tcp4", net.JoinHostPort("127.0.0.1", fmt.Sprint(port)))
		return dialErr
	})
	if err != nil {
		return nil, err
	}
	return connection, nil
}
