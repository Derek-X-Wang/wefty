package ocihelper

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestComputerSeatInetAbsenceRefusesUIDAndEitherEndpoint(t *testing.T) {
	seat := ComputerSeat{1, "seat1", 7102, 7103}
	for _, address := range []string{"0100007F", "00000000000000000000000001000000"} {
		row := func(port, uid, inode string) []byte {
			return []byte(fmt.Sprintf("sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n0: %s:%s %s:0000 0A 00000000:00000000 00:00000000 00000000 %s 0 %s 1\n", address, port, address, uid, inode))
		}
		for _, tc := range []struct {
			name, port, uid, inode string
			present                bool
		}{
			{"view", "1BBE", "2002", "4242", true}, {"control", "1BBF", "0", "4242", true}, {"uid", "1C22", "2001", "4242", true},
			{"another Seat", "1C22", "2002", "4242", false}, {"time wait", "1BBE", "2001", "0", false},
		} {
			t.Run(address+"/"+tc.name, func(t *testing.T) {
				if err := checkSeatInetSockets(row(tc.port, tc.uid, tc.inode), seat); (err != nil) != tc.present {
					t.Fatalf("presence=%t, error=%v", tc.present, err)
				}
			})
		}
	}
	if err := checkSeatInetSockets([]byte("sl local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt uid timeout inode\n"), seat); err != nil {
		t.Fatal(err)
	}
}

func TestComputerSeatUnixAbsenceRefusesSeatAndDisplaySockets(t *testing.T) {
	seat := ComputerSeat{1, "seat1", 7102, 7103}
	for _, tc := range []struct {
		path    string
		present bool
	}{{"/wefty/service/seats/1/run/display", true}, {"/tmp/.X11-unix/X1", true}, {"@/tmp/.X11-unix/X1", true}, {"/wefty/service/seats/2/run/display", false}, {"/wefty/service/seats/10/run/display", false}, {"@/tmp/.X11-unix/X10", false}, {"", false}} {
		payload := []byte("Num RefCount Protocol Flags Type St Inode Path\n0000: 00000002 00000000 00010000 0001 01 12345 " + tc.path + "\n")
		if err := checkSeatUnixSockets(payload, seat); (err != nil) != tc.present {
			t.Fatalf("path %q presence=%t, error=%v", tc.path, tc.present, err)
		}
	}
}

func TestComputerSeatProcessAbsenceChecksAllUIDsInExactNamespace(t *testing.T) {
	root := t.TempDir()
	for _, pid := range []string{"1", "2", "3"} {
		if err := os.MkdirAll(filepath.Join(root, pid, "ns"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	namespace := filepath.Join(root, "1/ns/pid")
	if err := os.WriteFile(namespace, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(namespace, filepath.Join(root, "2/ns/pid")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "3/ns/pid"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	write := func(pid, uid string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, pid, "status"), []byte("Name:\tfixture\nUid:\t"+uid+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("1", "0 0 0 0")
	write("3", "2001 2001 2001 2001") // Other Computer.
	for field := 0; field < 4; field++ {
		uids := []int{2002, 2002, 2002, 2002}
		uids[field] = 2001
		write("2", fmt.Sprintf("%d %d %d %d", uids[0], uids[1], uids[2], uids[3]))
		if err := confirmComputerSeatProcessesAbsent(root, namespace, ComputerSeat{Place: 1}); err == nil {
			t.Fatalf("uid field %d was missed", field)
		}
	}
	write("2", "2002 2002 2002 2002")
	if err := confirmComputerSeatProcessesAbsent(root, namespace, ComputerSeat{Place: 1}); err != nil {
		t.Fatalf("absent Seat refused: %v", err)
	}
	if err := os.Remove(filepath.Join(root, "2/status")); err != nil {
		t.Fatal(err)
	}
	if err := confirmComputerSeatProcessesAbsent(root, namespace, ComputerSeat{Place: 1}); err != nil {
		t.Fatalf("exited process refused: %v", err)
	}
}

func TestComputerSeatProcessDisappearanceIncludesESRCH(t *testing.T) {
	for _, err := range []error{os.ErrNotExist, syscall.ESRCH, &os.PathError{Op: "read", Path: "status", Err: syscall.ESRCH}} {
		if !seatProcessVanished(err) {
			t.Errorf("exited process treated as a hard failure: %v", err)
		}
	}
	for _, err := range []error{nil, os.ErrPermission, errors.New("read failure")} {
		if seatProcessVanished(err) {
			t.Errorf("real failure ignored: %v", err)
		}
	}
}
