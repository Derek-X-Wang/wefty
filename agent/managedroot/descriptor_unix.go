//go:build darwin || linux

package managedroot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

const maxMetadataBytes = 64 << 10

type objectIdentity struct {
	device uint64
	inode  uint64
	mode   uint32
}

func openAbsoluteDirectory(path string, create bool) (int, error) {
	if !filepath.IsAbs(path) {
		return -1, ErrUnsafeRoot
	}
	fd, err := unix.Open(string(filepath.Separator), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("open filesystem root: %w", err)
	}
	trimmed := strings.TrimPrefix(filepath.Clean(path), string(filepath.Separator))
	for _, component := range strings.Split(trimmed, string(filepath.Separator)) {
		if component == "" {
			continue
		}
		if err := validateComponent(component); err != nil {
			unix.Close(fd)
			return -1, err
		}
		if create {
			if err := unix.Mkdirat(fd, component, 0o700); err != nil && !errors.Is(err, unix.EEXIST) {
				unix.Close(fd)
				return -1, fmt.Errorf("create managed root component %q: %w", component, err)
			}
		}
		nextFD, err := openDirectoryAt(fd, component)
		unix.Close(fd)
		if err != nil {
			return -1, err
		}
		fd = nextFD
	}
	return fd, nil
}

func openChildDirectory(parentFD int, name string, create bool, wantMount mountIdentity, identityFD func(int) (mountIdentity, error)) (int, error) {
	if err := validateComponent(name); err != nil {
		return -1, err
	}
	if create {
		if err := unix.Mkdirat(parentFD, name, 0o700); err != nil && !errors.Is(err, unix.EEXIST) {
			return -1, fmt.Errorf("create directory %q: %w", name, err)
		}
	}
	before, err := statAt(parentFD, name)
	if err != nil {
		return -1, err
	}
	if fileType(before.mode) == unix.S_IFLNK {
		return -1, fmt.Errorf("%w: %q", ErrSymlink, name)
	}
	if fileType(before.mode) != unix.S_IFDIR {
		return -1, fmt.Errorf("%w: %q is not a directory", ErrUnsafeLayout, name)
	}
	fd, err := openDirectoryAt(parentFD, name)
	if err != nil {
		return -1, err
	}
	after, err := statFD(fd)
	if err != nil {
		unix.Close(fd)
		return -1, err
	}
	if before != after {
		unix.Close(fd)
		return -1, fmt.Errorf("%w: directory %q was swapped", ErrConcurrentMutation, name)
	}
	mount, err := identityFD(fd)
	if err != nil {
		unix.Close(fd)
		return -1, err
	}
	if mount != wantMount {
		unix.Close(fd)
		return -1, fmt.Errorf("%w: %q", ErrMountBoundary, name)
	}
	return fd, nil
}

func openDirectoryAt(parentFD int, name string) (int, error) {
	fd, err := unix.Openat(parentFD, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, unix.ELOOP) {
			return -1, fmt.Errorf("%w: %q", ErrSymlink, name)
		}
		if errors.Is(err, unix.ENOENT) {
			return -1, fs.ErrNotExist
		}
		return -1, fmt.Errorf("open directory %q without following links: %w", name, err)
	}
	return fd, nil
}

func statAt(parentFD int, name string) (objectIdentity, error) {
	var stat unix.Stat_t
	if err := unix.Fstatat(parentFD, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return objectIdentity{}, fs.ErrNotExist
		}
		return objectIdentity{}, fmt.Errorf("inspect %q without following links: %w", name, err)
	}
	return objectIdentity{device: uint64(stat.Dev), inode: uint64(stat.Ino), mode: uint32(stat.Mode)}, nil
}

func statFD(fd int) (objectIdentity, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return objectIdentity{}, err
	}
	return objectIdentity{device: uint64(stat.Dev), inode: uint64(stat.Ino), mode: uint32(stat.Mode)}, nil
}

func fileType(mode uint32) uint32 { return mode & unix.S_IFMT }

func entryExistsAt(parentFD int, name string) (bool, objectIdentity, error) {
	identity, err := statAt(parentFD, name)
	if errors.Is(err, fs.ErrNotExist) {
		return false, objectIdentity{}, nil
	}
	return err == nil, identity, err
}

func preflightTree(ctx context.Context, directoryFD int, rootMount mountIdentity, identityFD func(int) (mountIdentity, error), identityAt func(int, string) (mountIdentity, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	names, err := readDirectoryNames(directoryFD)
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return err
		}
		before, err := statAt(directoryFD, name)
		if err != nil {
			return err
		}
		if fileType(before.mode) == unix.S_IFLNK {
			return fmt.Errorf("%w: %q", ErrSymlink, name)
		}
		mount, err := identityAt(directoryFD, name)
		if err != nil {
			return err
		}
		if mount != rootMount {
			return fmt.Errorf("%w: %q", ErrMountBoundary, name)
		}
		if fileType(before.mode) != unix.S_IFDIR {
			continue
		}
		childFD, err := openDirectoryAt(directoryFD, name)
		if err != nil {
			return err
		}
		after, err := statFD(childFD)
		if err != nil {
			unix.Close(childFD)
			return err
		}
		if before != after {
			unix.Close(childFD)
			return fmt.Errorf("%w: directory %q was swapped", ErrConcurrentMutation, name)
		}
		childMount, err := identityFD(childFD)
		if err != nil {
			unix.Close(childFD)
			return err
		}
		if childMount != rootMount {
			unix.Close(childFD)
			return fmt.Errorf("%w: %q", ErrMountBoundary, name)
		}
		err = preflightTree(ctx, childFD, rootMount, identityFD, identityAt)
		unix.Close(childFD)
		if err != nil {
			return err
		}
	}
	return nil
}

func removeTreeAt(ctx context.Context, parentFD int, name string, rootMount mountIdentity, identityFD func(int) (mountIdentity, error), identityAt func(int, string) (mountIdentity, error)) error {
	before, err := statAt(parentFD, name)
	if err != nil {
		return err
	}
	if fileType(before.mode) == unix.S_IFLNK {
		return fmt.Errorf("%w: %q", ErrSymlink, name)
	}
	if fileType(before.mode) != unix.S_IFDIR {
		return fmt.Errorf("%w: deletion target %q is not a directory", ErrUnsafeLayout, name)
	}
	directoryFD, err := openDirectoryAt(parentFD, name)
	if err != nil {
		return err
	}
	defer unix.Close(directoryFD)
	afterOpen, err := statFD(directoryFD)
	if err != nil {
		return err
	}
	if before != afterOpen {
		return fmt.Errorf("%w: deletion target %q was swapped", ErrConcurrentMutation, name)
	}
	mount, err := identityFD(directoryFD)
	if err != nil {
		return err
	}
	if mount != rootMount {
		return fmt.Errorf("%w: %q", ErrMountBoundary, name)
	}
	names, err := readDirectoryNames(directoryFD)
	if err != nil {
		return err
	}
	for _, childName := range names {
		if err := ctx.Err(); err != nil {
			return err
		}
		childBefore, err := statAt(directoryFD, childName)
		if err != nil {
			return err
		}
		if fileType(childBefore.mode) == unix.S_IFLNK {
			return fmt.Errorf("%w: %q", ErrSymlink, childName)
		}
		childMount, err := identityAt(directoryFD, childName)
		if err != nil {
			return err
		}
		if childMount != rootMount {
			return fmt.Errorf("%w: %q", ErrMountBoundary, childName)
		}
		if fileType(childBefore.mode) == unix.S_IFDIR {
			if err := removeTreeAt(ctx, directoryFD, childName, rootMount, identityFD, identityAt); err != nil {
				return err
			}
			continue
		}
		childAfter, err := statAt(directoryFD, childName)
		if err != nil {
			return err
		}
		if childBefore != childAfter {
			return fmt.Errorf("%w: entry %q was swapped", ErrConcurrentMutation, childName)
		}
		if err := unix.Unlinkat(directoryFD, childName, 0); err != nil {
			return fmt.Errorf("unlink entry %q: %w", childName, err)
		}
	}
	current, err := statAt(parentFD, name)
	if err != nil {
		return err
	}
	if current != before {
		return fmt.Errorf("%w: directory %q moved during deletion", ErrConcurrentMutation, name)
	}
	if err := unix.Unlinkat(parentFD, name, unix.AT_REMOVEDIR); err != nil {
		return fmt.Errorf("remove directory %q: %w", name, err)
	}
	return nil
}

func readDirectoryNames(fd int) ([]string, error) {
	duplicate, err := unix.Dup(fd)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(duplicate), "managed-directory")
	if file == nil {
		unix.Close(duplicate)
		return nil, errors.New("wrap directory descriptor")
	}
	defer file.Close()
	names, err := file.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	return names, nil
}

func readJSONAt(parentFD int, name string, destination any) error {
	if err := validateComponent(name); err != nil {
		return err
	}
	fd, err := unix.Openat(parentFD, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, unix.ENOENT) {
			return fs.ErrNotExist
		}
		if errors.Is(err, unix.ELOOP) {
			return ErrSymlink
		}
		return err
	}
	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		unix.Close(fd)
		return errors.New("wrap metadata descriptor")
	}
	defer file.Close()
	identity, err := statFD(fd)
	if err != nil {
		return err
	}
	if fileType(identity.mode) != unix.S_IFREG {
		return fmt.Errorf("metadata %q is not a regular file", name)
	}
	payload, err := io.ReadAll(io.LimitReader(file, maxMetadataBytes+1))
	if err != nil {
		return err
	}
	if len(payload) > maxMetadataBytes {
		return fmt.Errorf("metadata %q exceeds %d bytes", name, maxMetadataBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("metadata has trailing JSON values")
	}
	return nil
}

func writeJSONAtomicAt(parentFD int, name string, value any, replace bool) error {
	if err := validateComponent(name); err != nil {
		return err
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	suffix, err := randomToken(12)
	if err != nil {
		return err
	}
	temporary := ".tmp-" + suffix
	fd, err := unix.Openat(parentFD, temporary, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), temporary)
	if file == nil {
		unix.Close(fd)
		return errors.New("wrap temporary metadata descriptor")
	}
	cleanup := true
	defer func() {
		file.Close()
		if cleanup {
			_ = unix.Unlinkat(parentFD, temporary, 0)
		}
	}()
	if _, err := file.Write(payload); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if replace {
		if exists, identity, err := entryExistsAt(parentFD, name); err != nil {
			return err
		} else if exists && fileType(identity.mode) != unix.S_IFREG {
			if fileType(identity.mode) == unix.S_IFLNK {
				return fmt.Errorf("%w: metadata %q", ErrSymlink, name)
			}
			return fmt.Errorf("%w: metadata %q is not a regular file", ErrUnsafeLayout, name)
		}
		if err := unix.Renameat(parentFD, temporary, parentFD, name); err != nil {
			return err
		}
	} else if err := renameNoReplace(parentFD, temporary, parentFD, name); err != nil {
		return err
	}
	cleanup = false
	return syncDirectory(parentFD)
}

func lockDescriptor(fd int) error {
	if err := unix.Flock(fd, unix.LOCK_EX); err != nil {
		return fmt.Errorf("lock managed node root: %w", err)
	}
	return nil
}

func unlockDescriptor(fd int) { _ = unix.Flock(fd, unix.LOCK_UN) }

func syncDirectory(fd int) error {
	if err := unix.Fsync(fd); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOTSUP) {
		return fmt.Errorf("sync containing directory: %w", err)
	}
	return nil
}
