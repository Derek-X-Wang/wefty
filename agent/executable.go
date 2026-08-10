package agent

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/Derek-X-Wang/wefty/contract"
)

func materializeExecutable(execution contract.ExecutionSpec, attemptID string) (contract.ExecutionSpec, func(), error) {
	if execution.Executable.InlineBase64 == "" {
		return execution, func() {}, nil
	}
	content, err := base64.StdEncoding.DecodeString(execution.Executable.InlineBase64)
	if err != nil {
		return contract.ExecutionSpec{}, func() {}, fmt.Errorf("decode inline executable: %w", err)
	}
	digest := sha256.Sum256(content)
	if hex.EncodeToString(digest[:]) != execution.Executable.SHA256 {
		return contract.ExecutionSpec{}, func() {}, fmt.Errorf("inline executable SHA-256 does not match content")
	}
	directory, err := os.MkdirTemp("", "wefty-executable-"+attemptID+"-")
	if err != nil {
		return contract.ExecutionSpec{}, func() {}, fmt.Errorf("create inline executable directory: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(directory) }
	path := filepath.Join(directory, "workflow")
	mode := os.FileMode(execution.Executable.Mode)
	if mode == 0 {
		mode = 0o700
	}
	if err := os.WriteFile(path, content, mode); err != nil {
		cleanup()
		return contract.ExecutionSpec{}, func() {}, fmt.Errorf("materialize inline executable: %w", err)
	}
	if err := os.Chmod(path, mode); err != nil {
		cleanup()
		return contract.ExecutionSpec{}, func() {}, fmt.Errorf("set inline executable permissions: %w", err)
	}

	if len(execution.Executable.Interpreter) == 0 {
		execution.Executable.Path = path
		return execution, cleanup, nil
	}
	arguments := append([]string(nil), execution.Executable.Interpreter...)
	arguments = append(arguments, path)
	if len(execution.Argv) > 1 {
		arguments = append(arguments, execution.Argv[1:]...)
	}
	interpreterPath, err := exec.LookPath(execution.Executable.Interpreter[0])
	if err != nil {
		cleanup()
		return contract.ExecutionSpec{}, func() {}, fmt.Errorf("resolve inline interpreter on agent node: %w", err)
	}
	execution.Executable.Path = interpreterPath
	execution.Argv = arguments
	return execution, cleanup, nil
}
