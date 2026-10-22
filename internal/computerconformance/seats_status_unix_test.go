//go:build linux || darwin

package computerconformance_test

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

const seatStatusPath = "/wefty/seat-status"
const seatAnswerLimit = 8192

func validateSeatStatusMount(payload []byte) error {
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
func openSeatStatusDirectory(root string) (*os.File, error) {
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

func readSeatStatusAnswer(directory *os.File, name string) ([]byte, error) {
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

func TestSeatStatusReaderRejectsLinksSpecialFilesAndOversizedAnswers(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "wefty", "seat-status")
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	dir, err := openSeatStatusDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	outside := filepath.Join(root, "outside")
	if err := os.WriteFile(outside, []byte(`{"state":"removed"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(directory, "link")); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(directory, "fifo"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "large"), []byte(strings.Repeat("x", seatAnswerLimit+1)), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "1.json"), []byte(`{"state":"ready"}`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"link", "fifo", "large", "../outside", "../seat-status/1.json"} {
		if _, err := readSeatStatusAnswer(dir, name); err == nil {
			t.Errorf("unsafe answer accepted: %s", name)
		}
	}
	if b, err := readSeatStatusAnswer(dir, "1.json"); err != nil || string(b) != `{"state":"ready"}` {
		t.Fatalf("regular answer: %q %v", b, err)
	}
}

func TestSeatStatusReaderRefusesSymlinkAtEveryComponent(t *testing.T) {
	for _, component := range []string{"wefty", "seat-status"} {
		t.Run(component, func(t *testing.T) {
			root, outside := t.TempDir(), t.TempDir()
			parent := root
			target := outside
			if component == "wefty" {
				if err := os.Mkdir(filepath.Join(outside, "seat-status"), 0700); err != nil {
					t.Fatal(err)
				}
			} else {
				parent = filepath.Join(root, "wefty")
				if err := os.Mkdir(parent, 0700); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Symlink(target, filepath.Join(parent, component)); err != nil {
				t.Fatal(err)
			}
			f, err := openSeatStatusDirectory(root)
			if f != nil {
				f.Close()
			}
			if err == nil {
				t.Fatal("linked status component accepted")
			}
		})
	}
}

func TestSeatStatusMountMustBePrivateBoundedWritableTmpfs(t *testing.T) {
	good := "40 30 0:22 / /wefty/seat-status rw,nosuid,nodev,noexec,relatime - tmpfs tmpfs rw,size=1024k,mode=700\n"
	if err := validateSeatStatusMount([]byte(good)); err != nil {
		t.Fatal(err)
	}
	for name, bad := range map[string]string{
		"missing": "", "filesystem": strings.Replace(good, "- tmpfs", "- ext4", 1),
		"writable":           strings.Replace(good, "rw,nosuid", "ro,nosuid", 1),
		"flags":              strings.Replace(good, ",noexec", "", 1),
		"ceiling":            strings.Replace(good, "size=1024k", "size=2048k", 1),
		"unbounded":          strings.Replace(good, "size=1024k,", "", 1),
		"shadowed":           good + good,
		"shared propagation": strings.Replace(good, " - tmpfs", " shared:9 - tmpfs", 1),
		"subtree bind":       strings.Replace(good, " / /wefty/", " /subtree /wefty/", 1),
		"child mount":        good + strings.Replace(good, seatStatusPath, seatStatusPath+"/1.json", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateSeatStatusMount([]byte(bad)); err == nil {
				t.Fatal(fmt.Sprintf("unsafe status mount accepted: %s", name))
			}
		})
	}
}
