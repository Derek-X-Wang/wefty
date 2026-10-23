package ocihelper

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

func checkSeatInetSockets(payload []byte, seat ComputerSeat) error {
	scanner := bufio.NewScanner(strings.NewReader(string(payload)))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 10 || fields[0] == "sl" {
			continue
		}
		// TIME_WAIT entries with inode 0 are not live sockets and cannot accept
		// input; requiring their disappearance would add the TCP close timeout.
		if fields[9] == "0" {
			continue
		}
		if fields[7] == strconv.Itoa(2000+seat.Place) {
			return errors.New("Seat user still owns a socket")
		}
		_, portText, ok := strings.Cut(fields[1], ":")
		if !ok {
			return errors.New("invalid kernel socket table")
		}
		port, err := strconv.ParseUint(portText, 16, 16)
		if err != nil {
			return err
		}
		if uint16(port) == seat.ViewPort || uint16(port) == seat.ControlPort {
			return errors.New("Seat endpoint socket remains")
		}
	}
	return scanner.Err()
}
func checkSeatUnixSockets(payload []byte, seat ComputerSeat) error {
	prefix := fmt.Sprintf("/wefty/service/seats/%d/", seat.Place)
	for _, line := range strings.Split(string(payload), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 8 {
			continue
		}
		path := fields[7]
		if strings.HasPrefix(path, prefix) || path == fmt.Sprintf("/tmp/.X11-unix/X%d", seat.Place) || path == fmt.Sprintf("@/tmp/.X11-unix/X%d", seat.Place) {
			return errors.New("Seat Unix socket remains")
		}
	}
	return nil
}
func confirmComputerSeatProcessesAbsent(procRoot, namespacePath string, seat ComputerSeat) error {
	pidNamespace, err := os.Stat(namespacePath)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(procRoot)
	if err != nil {
		return err
	}
	uid := strconv.Itoa(2000 + seat.Place)
	for _, entry := range entries {
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		path := filepath.Join(procRoot, entry.Name())
		ns, err := os.Stat(filepath.Join(path, "ns/pid"))
		if seatProcessVanished(err) {
			continue
		}
		if err != nil {
			return err
		}
		if !os.SameFile(pidNamespace, ns) {
			continue
		}
		status, err := os.ReadFile(filepath.Join(path, "status"))
		if seatProcessVanished(err) {
			continue
		}
		if err != nil {
			return err
		}
		for _, line := range strings.Split(string(status), "\n") {
			fields := strings.Fields(line)
			if len(fields) > 1 && fields[0] == "Uid:" {
				for _, value := range fields[1:] {
					if value == uid {
						return errors.New("Seat user still has processes")
					}
				}
			}
		}
	}
	return nil
}
func seatProcessVanished(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH)
}
