//go:build linux

package ocihelper

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
)

type computerStorageIdentityFacts struct {
	MachineIDDigest string
	Repaired        bool
	RepairReason    string
}

func ensureIdentityDirectory(path string) (bool, string, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return true, "identity directory was missing", os.Mkdir(path, 0o755)
	}
	if err != nil {
		return false, "", err
	}
	if info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
		return false, "", nil
	}
	if err := os.Remove(path); err != nil {
		return false, "", err
	}
	return true, "identity directory was not a real directory", os.Mkdir(path, 0o755)
}

func newComputerMachineID() ([]byte, error) {
	identity := make([]byte, 16)
	if _, err := rand.Read(identity); err != nil {
		return nil, err
	}
	return []byte(hex.EncodeToString(identity) + "\n"), nil
}

func validComputerMachineID(payload []byte) bool {
	value := strings.TrimSuffix(string(payload), "\n")
	if len(value) != 32 || len(payload) != 33 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 16
}

func computerMachineIDDigest(payload []byte) string {
	digest := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func ensureComputerStorageIdentity(root string) (computerStorageIdentityFacts, error) {
	paths := computerStorageIdentityAt(root)
	repaired, reason, err := ensureIdentityDirectory(paths.Directory)
	if err != nil {
		return computerStorageIdentityFacts{}, fmt.Errorf("repair Computer Storage identity directory: %w", err)
	}
	machineID, readErr := readRegularFile(paths.MachineID)
	if readErr != nil || !validComputerMachineID(machineID) {
		if removeErr := os.RemoveAll(paths.MachineID); removeErr != nil {
			return computerStorageIdentityFacts{}, fmt.Errorf("remove invalid Computer Storage machine-id: %w", removeErr)
		}
		machineID, err = newComputerMachineID()
		if err == nil {
			err = writeDurableFile(paths.Directory, ".machine-id.tmp-", computerStorageMachineIDName, machineID, 0o444)
		}
		repaired = true
		if reason == "" {
			switch {
			case errors.Is(readErr, os.ErrNotExist):
				reason = "machine-id was missing"
			case readErr != nil:
				reason = "machine-id was not a regular file"
			default:
				reason = "machine-id was malformed"
			}
		}
	}
	if err != nil {
		return computerStorageIdentityFacts{}, fmt.Errorf("repair Computer Storage machine-id: %w", err)
	}
	verified, err := readRegularFile(paths.MachineID)
	if err != nil || !validComputerMachineID(verified) {
		return computerStorageIdentityFacts{}, errors.New("repaired Computer Storage machine-id is invalid")
	}
	if repaired {
		log.Printf("repaired Computer Storage machine-id path=%q reason=%q", paths.MachineID, reason)
		if err := syncComputerDiskFilesystem(root); err != nil {
			return computerStorageIdentityFacts{}, err
		}
	}
	return computerStorageIdentityFacts{MachineIDDigest: computerMachineIDDigest(verified), Repaired: repaired, RepairReason: reason}, nil
}
