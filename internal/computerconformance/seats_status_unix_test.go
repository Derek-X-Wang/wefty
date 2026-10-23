//go:build linux || darwin

package computerconformance_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
	"golang.org/x/sys/unix"
)

const seatStatusPath = "/wefty/seat-status"
const seatAnswerLimit = 8192

func validateSeatStatusMount(b []byte) error { return ocihelper.ValidateSeatStatusMount(b) }
func openSeatStatusDirectory(root string) (*os.File, error) {
	return ocihelper.OpenSeatStatusDirectory(root)
}
func openValidatedSeatStatusDirectory(root string, b []byte) (*os.File, error) {
	return ocihelper.OpenValidatedSeatStatusDirectory(root, b)
}
func readSeatStatusAnswer(dir *os.File, name string) ([]byte, error) {
	return ocihelper.ReadSeatStatusAnswer(dir, name)
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

func TestSeatStatusDirectoryMustMatchValidatedMountDevice(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "wefty", "seat-status")
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	var st unix.Stat_t
	if err := unix.Stat(directory, &st); err != nil {
		t.Fatal(err)
	}
	major, minor := unix.Major(uint64(st.Dev)), unix.Minor(uint64(st.Dev))
	mountinfo := func(major, minor uint32) []byte {
		return []byte(fmt.Sprintf("40 30 %d:%d / /wefty/seat-status rw,nosuid,nodev,noexec - tmpfs tmpfs rw,size=1024k,mode=700\n", major, minor))
	}
	f, err := openValidatedSeatStatusDirectory(root, mountinfo(major, minor))
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	for _, device := range [][2]uint32{{major + 1, minor}, {major, minor + 1}} {
		f, err = openValidatedSeatStatusDirectory(root, mountinfo(device[0], device[1]))
		if f != nil {
			f.Close()
		}
		if err == nil {
			t.Fatal("opened directory did not belong to the validated status mount")
		}
	}
}
