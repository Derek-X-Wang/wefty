//go:build darwin || linux

package agentinstall

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func owned(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}
func lockDirectory(path string) (func(), error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, err
	}
	return func() { _ = unix.Flock(int(f.Fd()), unix.LOCK_UN); _ = f.Close() }, nil
}

func lockCatalog(path string) (func(), error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return func() { _ = unix.Flock(int(f.Fd()), unix.LOCK_UN); _ = f.Close() }, nil
}

// A successful nonblocking acquisition proves that no generation process holds
// its lifetime lock. Only the kernel's would-block answer is positive evidence.
func requireLifetimeLock(path string) error {
	release, err := lockDirectory(path)
	if err == nil {
		release()
		return &VerificationRefusal{Message: "generation lifetime lock is free"}
	}
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return nil
	}
	return fmt.Errorf("cannot verify generation lifetime lock: %w", err)
}
