package main

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/Derek-X-Wang/wefty/l1"
)

// P3 stores credentials in the system temp folder. A refused write stays refused.
// OpenRoot pins the directory; atomic relative rename cannot follow a replaced
// credential-file symlink. Neither this file nor the receipt bypasses Fabric.
func openSeatCredentialRoot() (*os.Root, error) {
	directory := filepath.Join(os.TempDir(), "wefty-seats-"+strconv.Itoa(os.Getuid()))
	if err := os.Mkdir(directory, 0700); err != nil && !os.IsExist(err) {
		return nil, err
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil, fmt.Errorf("Seat credential directory must be a private directory")
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
func storeSeatPrivateFile(id, suffix string, value any) (string, error) {
	if !seatCLIIdentifier.MatchString(id) {
		return "", fmt.Errorf("invalid Seat ID")
	}
	root, err := openSeatCredentialRoot()
	if err != nil {
		return "", err
	}
	defer root.Close()
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	// An exclusive staging file prevents another writer from replacing bytes
	// before they are complete. The final file is always a 0600 regular file.
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
	root, err := openSeatCredentialRoot()
	if err != nil {
		return result, err
	}
	defer root.Close()
	data, err := root.ReadFile(id + ".request.json")
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
