//go:build linux

package ocihelper

import (
	"bytes"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/crypto/ssh"
)

func ensureRealDirectoryOrCreate(path string, mode os.FileMode) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return os.Mkdir(path, mode)
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("Computer Storage identity path %q is not a real directory", path)
	}
	return nil
}

func writeIdentityFile(directory, name string, payload []byte, mode os.FileMode) error {
	temporary, err := os.CreateTemp(directory, "."+name+".tmp-")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	writeErr := temporary.Chmod(mode)
	if writeErr == nil {
		_, writeErr = temporary.Write(payload)
	}
	if writeErr == nil {
		writeErr = temporary.Sync()
	}
	writeErr = errors.Join(writeErr, temporary.Close())
	if writeErr != nil {
		return writeErr
	}
	if err := os.Rename(temporaryPath, filepath.Join(directory, name)); err != nil {
		return err
	}
	return syncDirectory(directory)
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

func newComputerSSHHostKey() (private, public []byte, returnedErr error) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	block, err := ssh.MarshalPrivateKey(privateKey, "wefty-computer")
	if err != nil {
		return nil, nil, err
	}
	sshPublic, err := ssh.NewPublicKey(publicKey)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(block), ssh.MarshalAuthorizedKey(sshPublic), nil
}

func writeFreshComputerSSHHostKey(directory string) error {
	private, public, err := newComputerSSHHostKey()
	if err != nil {
		return err
	}
	if err := writeIdentityFile(directory, computerStorageSSHPrivate, private, 0o600); err != nil {
		return err
	}
	return writeIdentityFile(directory, computerStorageSSHPublic, public, 0o644)
}

func computerSSHPublicForPrivate(private []byte) ([]byte, error) {
	parsed, err := ssh.ParseRawPrivateKey(private)
	if err != nil {
		return nil, err
	}
	signer, ok := parsed.(crypto.Signer)
	if !ok {
		return nil, fmt.Errorf("Computer Storage SSH private key type %T cannot derive a public key", parsed)
	}
	public, err := ssh.NewPublicKey(signer.Public())
	if err != nil {
		return nil, err
	}
	return ssh.MarshalAuthorizedKey(public), nil
}

func ensureComputerStorageIdentity(root string) error {
	etc := filepath.Join(root, computerStorageEtcDirectory)
	if err := ensureRealDirectoryOrCreate(etc, 0o755); err != nil {
		return err
	}
	machineIDPath := filepath.Join(etc, computerStorageMachineID)
	machineID, err := readRegularFile(machineIDPath)
	if errors.Is(err, os.ErrNotExist) {
		machineID, err = newComputerMachineID()
		if err == nil {
			err = writeIdentityFile(etc, computerStorageMachineID, machineID, 0o444)
		}
	}
	if err != nil {
		return fmt.Errorf("initialize Computer Storage machine-id: %w", err)
	}
	if !validComputerMachineID(machineID) {
		return errors.New("Computer Storage machine-id is invalid")
	}
	sshDirectory := filepath.Join(etc, computerStorageSSHDirectory)
	if err := ensureRealDirectoryOrCreate(sshDirectory, 0o755); err != nil {
		return err
	}
	privatePath := filepath.Join(sshDirectory, computerStorageSSHPrivate)
	publicPath := filepath.Join(sshDirectory, computerStorageSSHPublic)
	private, privateErr := readRegularFile(privatePath)
	public, publicErr := readRegularFile(publicPath)
	if errors.Is(privateErr, os.ErrNotExist) && errors.Is(publicErr, os.ErrNotExist) {
		if err := writeFreshComputerSSHHostKey(sshDirectory); err != nil {
			return fmt.Errorf("initialize Computer Storage SSH host key: %w", err)
		}
		private, privateErr = readRegularFile(privatePath)
		public, publicErr = readRegularFile(publicPath)
	}
	if privateErr == nil && errors.Is(publicErr, os.ErrNotExist) {
		// The private key is committed first. If the helper crashed before
		// publishing its derived public half, resume that exact identity
		// instead of rotating a partly committed key.
		public, publicErr = computerSSHPublicForPrivate(private)
		if publicErr == nil {
			publicErr = writeIdentityFile(sshDirectory, computerStorageSSHPublic, public, 0o644)
		}
	}
	if privateErr != nil || publicErr != nil || len(private) == 0 || len(public) == 0 {
		return errors.New("Computer Storage SSH host identity is incomplete")
	}
	expectedPublic, err := computerSSHPublicForPrivate(private)
	expectedKey, _, _, _, expectedErr := ssh.ParseAuthorizedKey(expectedPublic)
	observedKey, _, _, rest, observedErr := ssh.ParseAuthorizedKey(public)
	if err != nil || expectedErr != nil || observedErr != nil || len(bytes.TrimSpace(rest)) != 0 ||
		!bytes.Equal(expectedKey.Marshal(), observedKey.Marshal()) {
		return errors.New("Computer Storage SSH host identity is invalid")
	}
	return syncComputerDiskFilesystem(root)
}
