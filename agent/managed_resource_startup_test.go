//go:build darwin || linux

package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/agent/managedroot"
)

func TestManagedResourceInitializeWithCorruptLegacyService(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root, err := managedroot.Initialize(managedroot.Config{Root: dir, NodeID: "node", BootSessionID: "first", HostBootIdentity: rootHostIdentity})
	if err != nil {
		t.Fatal(err)
	}
	paths, err := root.CreateService("bad", 1)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(paths.Root, managedroot.OwnershipManifestName)
	corrupt := []byte(`{"version":1,"job_id":"bad",`)
	if err := os.WriteFile(manifestPath, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	var logs []string
	resource, err := initializeManagedResource(dir, "node", "second", func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf(format, args...))
	})
	if err != nil {
		t.Fatalf("corrupt service stopped agent managed-root initialization: %v", err)
	}
	if resource == nil || resource.rootInstanceID() != root.Manifest().RootInstanceID {
		t.Fatal("managed-root initialization lost root identity")
	}
	if len(logs) != 1 || !strings.Contains(logs[0], "skipped legacy service stamping") {
		t.Fatalf("missing agent startup diagnostic: %v", logs)
	}
	if err := resource.remove(context.Background(), localRemoval{jobID: "bad", generation: 1, rootInstanceID: resource.rootInstanceID(), cleanupFence: "fence", processTreeReaped: true}); !errors.Is(err, managedroot.ErrOwnershipManifest) {
		t.Fatalf("corrupt service removal = %v, want ownership refusal", err)
	}
	if got, err := os.ReadFile(manifestPath); err != nil || string(got) != string(corrupt) {
		t.Fatalf("corrupt manifest changed: got=%q err=%v", got, err)
	}
}
