//go:build linux || darwin

package ocihelper

import (
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
)

const seatStatusPath = "/wefty/seat-status"
const seatAnswerLimit = 8192

func ValidateSeatStatusMount(payload []byte) error {
	count := 0
	for _, line := range strings.Split(string(payload), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 10 {
			continue
		}
		target := fields[4]
		if strings.HasPrefix(target, seatStatusPath+"/") {
			return errors.New("status mount is shadowed")
		}
		if target != seatStatusPath {
			continue
		}
		count++
		separator := slices.Index(fields, "-")
		if fields[3] != "/" || separator != 6 || len(fields) <= separator+3 || fields[separator+1] != "tmpfs" {
			return errors.New("status is not tmpfs")
		}
		options := strings.Split(fields[5], ",")
		for _, flag := range []string{"rw", "nosuid", "nodev", "noexec"} {
			if !slices.Contains(options, flag) {
				return fmt.Errorf("status mount lacks %s", flag)
			}
		}
		sizes := 0
		for _, option := range strings.Split(fields[separator+3], ",") {
			if !strings.HasPrefix(option, "size=") {
				continue
			}
			sizes++
			number := strings.TrimPrefix(option, "size=")
			factor := int64(1)
			if strings.HasSuffix(number, "k") {
				factor = 1024
				number = strings.TrimSuffix(number, "k")
			}
			if strings.HasSuffix(number, "m") {
				factor = 1024 * 1024
				number = strings.TrimSuffix(number, "m")
			}
			size, err := strconv.ParseInt(number, 10, 64)
			if err != nil || size < 1 || size > (1<<20)/factor || size*factor < 4096 {
				return errors.New("status tmpfs has invalid ceiling")
			}
		}
		if sizes != 1 {
			return errors.New("status tmpfs needs one ceiling")
		}
	}
	if count != 1 {
		return errors.New("status mount missing or shadowed")
	}
	return nil
}

// root is the trusted kernel /proc/<task-pid>/root anchor in the real lane.
func OpenSeatStatusDirectory(root string) (*os.File, error) {
	// Following the trusted kernel root anchor is intentional. All image
	// path components below that anchor are opened without following links.
	anchor, err := os.Open(root)
	if err != nil {
		return nil, err
	}
	defer anchor.Close()
	fd := int(anchor.Fd())
	owned := -1
	for _, component := range []string{"wefty", "seat-status"} {
		next, err := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if owned >= 0 {
			unix.Close(owned)
		}
		if err != nil {
			return nil, err
		}
		fd, owned = next, next
	}
	return os.NewFile(uintptr(owned), seatStatusPath), nil
}

// This is the lane's validated-open boundary, including the mountinfo check.
func OpenValidatedSeatStatusDirectory(root string, mountinfo []byte) (*os.File, error) {
	if err := ValidateSeatStatusMount(mountinfo); err != nil {
		return nil, err
	}
	var major, minor uint64
	for _, line := range strings.Split(string(mountinfo), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 10 || fields[4] != seatStatusPath {
			continue
		}
		a, b, ok := strings.Cut(fields[2], ":")
		if !ok {
			return nil, errors.New("invalid status mount device")
		}
		var err error
		major, err = strconv.ParseUint(a, 10, 32)
		if err != nil {
			return nil, err
		}
		minor, err = strconv.ParseUint(b, 10, 32)
		if err != nil {
			return nil, err
		}
	}
	directory, err := OpenSeatStatusDirectory(root)
	if err != nil {
		return nil, err
	}
	var st unix.Stat_t
	err = unix.Fstat(int(directory.Fd()), &st)
	if err == nil && (uint64(unix.Major(uint64(st.Dev))) != major || uint64(unix.Minor(uint64(st.Dev))) != minor) {
		err = errors.New("opened status directory does not match mountinfo device")
	}
	if err != nil {
		directory.Close()
		return nil, err
	}
	return directory, nil
}

func ReadSeatStatusAnswer(directory *os.File, name string) ([]byte, error) {
	if name == "" || name == "." || name == ".." || strings.Contains(name, "/") {
		return nil, errors.New("invalid answer name")
	}
	fd, err := unix.Openat(int(directory.Fd()), name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), name)
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > seatAnswerLimit {
		return nil, errors.New("answer is not a bounded regular file")
	}
	payload, err := io.ReadAll(io.LimitReader(f, seatAnswerLimit+1))
	if err == nil && len(payload) > seatAnswerLimit {
		err = errors.New("answer exceeds size cap")
	}
	return payload, err
}
