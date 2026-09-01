package ocihelper

import "path/filepath"

const (
	computerStorageIdentityDirectory = "etc"
	computerStorageMachineIDName     = "machine-id"
	computerMachineIDDestination     = "/etc/machine-id"
)

type computerStorageIdentityPaths struct {
	Directory string
	MachineID string
}

func computerStorageIdentityAt(root string) computerStorageIdentityPaths {
	directory := filepath.Join(root, computerStorageIdentityDirectory)
	return computerStorageIdentityPaths{Directory: directory, MachineID: filepath.Join(directory, computerStorageMachineIDName)}
}
