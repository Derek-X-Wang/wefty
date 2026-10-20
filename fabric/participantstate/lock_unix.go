//go:build darwin || linux

package participantstate

import (
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/Derek-X-Wang/wefty/fabric"
	"golang.org/x/sys/unix"
)

// Acquire takes a nonblocking advisory lock before an embedded backend starts.
// The file stays in place; only its open descriptor confers ownership.
func Acquire(directory, participant string) (func() error, error) {
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, errors.New("cannot create participant folder")
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() {
		return nil, errors.New("participant folder must be an unlinked directory")
	}

	// Keep the directory inode locked as well as the persistent file. Deletion
	// may unlink the file before removing the directory; ownership cannot then
	// be replaced with a new file while RemoveAll is still in progress.
	dirFD, err := unix.Open(directory, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, errors.New("cannot open participant folder for ownership")
	}
	dir := os.NewFile(uintptr(dirFD), "participant folder")
	dirInfo, err := dir.Stat()
	if err != nil || !os.SameFile(info, dirInfo) {
		_ = dir.Close()
		return nil, errors.New("participant folder changed during ownership acquisition")
	}
	if err := unix.Flock(dirFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = dir.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, &fabric.ParticipantInUseError{Participant: participant}
		}
		return nil, errors.New("cannot lock participant folder")
	}
	success := false
	defer func() {
		if !success {
			_ = dir.Close()
		}
	}()
	fd, err := unix.Open(filepath.Join(directory, LockName), unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0o600)
	if err != nil {
		return nil, errors.New("cannot open participant ownership lock")
	}
	file := os.NewFile(uintptr(fd), LockName)
	info, err = file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, errors.New("participant ownership lock must be a regular file")
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, &fabric.ParticipantInUseError{Participant: participant}
		}
		return nil, errors.New("cannot lock participant folder")
	}
	success = true
	var once sync.Once
	var closeErr error
	return func() error { once.Do(func() { closeErr = errors.Join(file.Close(), dir.Close()) }); return closeErr }, nil
}
