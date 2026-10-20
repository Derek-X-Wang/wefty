//go:build darwin || linux

package local

import (
	"errors"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

// Socket publication is independent of participant identity ownership.
func acquireSocketLock(socket string) (func() error, error) {
	fd, err := unix.Open(socket+".lock", unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0o600)
	if err != nil {
		return nil, errors.New("cannot open client-service socket publication lock")
	}
	file := os.NewFile(uintptr(fd), "client-service socket lock")
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, errors.New("client-service publication lock must be a regular file")
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, &AlreadyRunningError{}
		}
		return nil, errors.New("cannot lock client-service socket publication")
	}
	var once sync.Once
	var result error
	return func() error { once.Do(func() { result = file.Close() }); return result }, nil
}
