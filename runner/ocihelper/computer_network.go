package ocihelper

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// The zero value preserves the existing internet profile for owner-made
// Computers. Explicit settings use the same vocabulary as the Computer.
const (
	ComputerNetworkInternet = "internet"
	ComputerNetworkNone     = "none"
)

func validateComputerNetwork(workload WorkloadInput) error {
	if workload.ComputerNetwork != "" && !workload.Computer {
		return errors.New("Computer network requires a Computer workload")
	}
	switch workload.ComputerNetwork {
	case "", ComputerNetworkInternet, ComputerNetworkNone:
		return nil
	default:
		return errors.New("Computer network must be internet or none")
	}
}

// Publish constant helper-owned files without replacing identities retained by
// another Computer's profile. No Node names or resolver addresses enter none.
func prepareComputerNoneNetworkFiles(managedRoot string) error {
	if !filepath.IsAbs(managedRoot) || filepath.Clean(managedRoot) != managedRoot || managedRoot == string(filepath.Separator) {
		return errors.New("managed root must be clean, absolute, and not root")
	}
	directory := filepath.Join(managedRoot, "network", "none")
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	for name, payload := range map[string]string{
		"resolv.conf": "nameserver 127.0.0.1\noptions timeout:1 attempts:1\n",
		"hosts":       "127.0.0.1 localhost\n::1 localhost\n",
	} {
		if err := publishComputerNoneNetworkFile(directory, name, []byte(payload)); err != nil {
			return fmt.Errorf("prepare Computer network none %s: %w", name, err)
		}
	}
	return nil
}

func publishComputerNoneNetworkFile(directory, name string, payload []byte) error {
	temporary, err := os.CreateTemp(directory, ".network-*")
	if err != nil {
		return err
	}
	defer os.Remove(temporary.Name())
	defer temporary.Close()
	if _, err := temporary.Write(payload); err != nil {
		return err
	}
	if err := temporary.Chmod(0444); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	target := filepath.Join(directory, name)
	if err := os.Link(temporary.Name(), target); !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := os.Lstat(target)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0444 || info.Size() != int64(len(payload)) {
		return errors.New("existing network file is not the fixed read-only file")
	}
	existing, err := os.ReadFile(target)
	if err != nil {
		return err
	}
	if !bytes.Equal(existing, payload) {
		return errors.New("existing network file content differs")
	}
	return nil
}

func computerNetworkProfileInput(input RuntimeSpecInput) RuntimeSpecInput {
	if input.Workload.Computer && input.Workload.ComputerNetwork == ComputerNetworkNone {
		input.ResolverPath = filepath.Join(input.ManagedRoot, "network", "none", "resolv.conf")
		input.HostsPath = filepath.Join(input.ManagedRoot, "network", "none", "hosts")
	}
	return input
}
