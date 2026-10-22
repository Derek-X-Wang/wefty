package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const plainConversionInventoryPrefix = "wefty-conversion-inventory-"

func validPlainConversionInventoryDirectory(paths setupPaths, folder string) bool {
	if filepath.Dir(folder) != paths.data || folder != filepath.Join(paths.data, filepath.Base(folder)) {
		return false
	}
	suffix, ok := strings.CutPrefix(filepath.Base(folder), plainConversionInventoryPrefix)
	token, err := hex.DecodeString(suffix)
	return ok && err == nil && len(token) == 8 && suffix == strings.ToLower(suffix)
}

// Reserve the exact home-confined path durably before mkdir. A kill at any
// later instruction leaves enough evidence to remove both data and clone pins.
func createPlainConversionInventory(root *os.Root, paths setupPaths, record *plainConversion) (string, error) {
	if record.InventoryDirectory != "" {
		return "", errors.New("previous conversion inventory has not been removed")
	}
	var token [8]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	record.InventoryDirectory = filepath.Join(paths.data, plainConversionInventoryPrefix+hex.EncodeToString(token[:]))
	if err := savePlainConversion(paths, record); err != nil {
		return "", err
	}
	relative, err := filepath.Rel(paths.home, record.InventoryDirectory)
	if err != nil {
		return "", err
	}
	if err := root.Mkdir(relative, 0700); err != nil {
		return "", err
	}
	if err := syncConversionFolder(root, paths.dataRel); err != nil {
		return "", err
	}
	return record.InventoryDirectory, nil
}

func cleanupPlainConversionInventory(root *os.Root, paths setupPaths, record *plainConversion) error {
	folder := record.InventoryDirectory
	if folder == "" {
		return nil
	}
	if !validPlainConversionInventoryDirectory(paths, folder) {
		return errors.New("invalid plain conversion inventory directory")
	}
	relative, err := filepath.Rel(paths.home, folder)
	if err != nil {
		return err
	}
	if info, err := root.Lstat(relative); errors.Is(err, fs.ErrNotExist) {
		// A kill may occur after reserving the path but before creating it.
	} else if err != nil {
		return err
	} else if !info.IsDir() || info.Mode()&fs.ModeSymlink != 0 {
		return errors.New("conversion inventory directory must be unlinked")
	} else if err := root.RemoveAll(relative); err != nil {
		return err
	}
	if err := syncConversionFolder(root, paths.dataRel); err != nil {
		return err
	}
	record.InventoryDirectory = ""
	if err := savePlainConversion(paths, record); err != nil {
		record.InventoryDirectory = folder
		return err
	}
	return nil
}
