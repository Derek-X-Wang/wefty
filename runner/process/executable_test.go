//go:build darwin || linux

package process

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestMaterializeExecutableResolvesInterpreterOnAgentNode(t *testing.T) {
	bin := t.TempDir()
	interpreter := filepath.Join(bin, "node")
	if err := os.WriteFile(interpreter, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	content := []byte("console.log('agent-local interpreter')\n")
	digest := sha256.Sum256(content)
	execution, cleanup, err := materializeExecutable(contract.ExecutionSpec{
		Executable: contract.ExecutableSpec{
			InlineBase64: base64.StdEncoding.EncodeToString(content),
			SHA256:       hex.EncodeToString(digest[:]),
			Interpreter:  []string{"node"},
			Mode:         0o700,
		},
		Argv: []string{"workflow"},
	}, "attempt-interpreter")
	defer cleanup()
	if err != nil {
		t.Fatal(err)
	}
	if execution.Executable.Path != interpreter {
		t.Fatalf("resolved interpreter = %q, want agent-local %q", execution.Executable.Path, interpreter)
	}
	if len(execution.Argv) != 2 || execution.Argv[0] != "node" || execution.Argv[1] == "" {
		t.Fatalf("interpreter argv = %#v", execution.Argv)
	}
}
