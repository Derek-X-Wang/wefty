package systemduser

import (
	"os"
	"path/filepath"
	"testing"
)

func TestManagerAcceptsFragmentByFileIdentity(t *testing.T) {
	root, path := managerFixture(t)
	if err := root.WriteFile(filepath.Base(path), []byte("current"), 0644); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(t.TempDir(), "linked-home")
	if err := os.Symlink(filepath.Dir(path), alias); err != nil {
		t.Fatal(err)
	}
	f := &fakeManager{loaded: true, running: true, enabled: true, fragment: filepath.Join(alias, filepath.Base(path))}
	m := Manager{Run: f.run}
	for attempt := 0; attempt < 2; attempt++ {
		action, err := m.Converge(t.Context(), root, path, filepath.Base(path), []byte("current"))
		if err != nil || action != Unchanged {
			t.Fatalf("same fragment, attempt %d: %s %v", attempt, action, err)
		}
	}
	if len(f.mutations()) != 0 {
		t.Fatal("identity alias restarted the unit", f.calls)
	}
}
