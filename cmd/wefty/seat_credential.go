package main

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/Derek-X-Wang/wefty/l1"
)

// Roots follow exec's state-folder convention. A denied write never changes
// Fabric access; the macOS fallback is only a private local storage location.
var seatStorageGOOS = runtime.GOOS
var seatCredentialUID = os.Getuid

func seatCredentialRoots() ([]string, error) {
	root, err := userStateStorageRoot("WEFTY_SEAT_STATE_ROOT", "seats")
	if err != nil {
		return nil, err
	}
	roots := []string{root}
	if seatStorageGOOS == "darwin" && os.Getenv("WEFTY_SEAT_STATE_ROOT") == "" {
		fallback := filepath.Join(os.TempDir(), "wefty-seats")
		if fallback != root {
			roots = append(roots, fallback)
		}
	}
	return roots, nil
}
func openSeatCredentialDirectory(directory string, create bool) (*os.Root, error) {
	if create {
		if err := os.MkdirAll(directory, 0700); err != nil {
			return nil, err
		}
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil, fmt.Errorf("Seat credential directory must be a private directory")
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != uint32(seatCredentialUID()) {
		return nil, fmt.Errorf("Seat credential directory must be owned by the current uid")
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	opened, err := root.Stat(".")
	if err != nil || !os.SameFile(info, opened) {
		root.Close()
		return nil, fmt.Errorf("Seat credential directory changed")
	}
	return root, nil
}
func seatStorageDenied(err error) bool {
	return errors.Is(err, os.ErrPermission) || errors.Is(err, syscall.EROFS)
}
func storeSeatPrivateFile(id, suffix string, value any) (string, error) {
	if !seatCLIIdentifier.MatchString(id) {
		return "", fmt.Errorf("invalid Seat ID")
	}
	roots, err := seatCredentialRoots()
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	attempt := func(directory string) (string, error) {
		root, err := openSeatCredentialDirectory(directory, true)
		if err != nil {
			return "", err
		}
		defer root.Close()
		stage := id + suffix + "." + rand.Text() + ".writing"
		file, err := root.OpenFile(stage, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return "", err
		}
		defer root.Remove(stage)
		_, writeErr := file.Write(data)
		err = errors.Join(writeErr, file.Sync(), file.Close())
		if err != nil {
			return "", err
		}
		name := id + suffix
		if err := root.Rename(stage, name); err != nil {
			return "", err
		}
		return filepath.Join(root.Name(), name), nil
	}
	path, first := attempt(roots[0])
	if first == nil {
		return path, nil
	}
	if seatStorageDenied(first) && len(roots) > 1 {
		if path, err := attempt(roots[1]); err == nil {
			return path, nil
		}
	}
	return "", first
}
func storeSeatCredential(id, secret string) (string, error) {
	return storeSeatPrivateFile(id, ".json", struct {
		SeatID     string `json:"seat_id"`
		Credential string `json:"credential"`
	}{id, secret})
}
func storeSeatCallInput(value l1.SeatCallInput) error {
	_, err := storeSeatPrivateFile(value.SeatID, ".request.json", value)
	return err
}
func loadSeatCallInput(id string) (l1.SeatCallInput, error) {
	var result l1.SeatCallInput
	if !seatCLIIdentifier.MatchString(id) {
		return result, fmt.Errorf("invalid Seat ID")
	}
	roots, err := seatCredentialRoots()
	if err != nil {
		return result, err
	}
	var data []byte
	for i, directory := range roots {
		root, e := openSeatCredentialDirectory(directory, false)
		if e == nil {
			data, e = root.ReadFile(id + ".request.json")
			root.Close()
		}
		if e == nil {
			err = nil
			break
		}
		if i == 0 {
			err = e
		}
		if !os.IsNotExist(e) && !seatStorageDenied(e) {
			return result, e
		}
	}
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return result, err
	}
	if result.SeatID != id {
		return result, fmt.Errorf("Seat request receipt has a different ID")
	}
	return result, nil
}
