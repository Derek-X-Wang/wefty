package agentinstall

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/testwait"
)

func TestMaintenanceDiagnosticIsIndependentAndNonblocking(t *testing.T) {
	for _, location := range []string{"locked", "archived", "absent"} {
		t.Run(location, func(t *testing.T) {
			f := newFixture(t)
			f.prepare(t, true)
			c, err := f.s.Read()
			must(t, err)
			m, err := f.s.BeginMaintenance("remove", "diagnostic-operation", c)
			must(t, err)
			var release func()
			if location == "locked" {
				_, close, err := f.s.open(false)
				must(t, err)
				release = close
			} else {
				target := filepath.Join(f.s.Data, m.Archive())
				if location == "absent" {
					target = filepath.Join(t.TempDir(), "saved-catalog")
				}
				must(t, os.Rename(f.s.Root(), target))
			}
			if release != nil {
				defer func() {
					if release != nil {
						release()
					}
				}()
			}
			// Diagnostic discovery needs DATA alone, even after launch definitions and
			// the live catalog have been removed. It must not acquire the catalog lock.
			diagnostic := Store{Data: f.s.Data}
			type result struct {
				m   *MaintenanceObservation
				err error
			}
			done := make(chan result, 1)
			go func() { m, err := diagnostic.ReadMaintenanceDiagnostic(); done <- result{m, err} }()
			select {
			case got := <-done:
				if got.err != nil || got.m == nil || got.m.Operation != m.Operation || got.m.Kind != m.Kind {
					t.Fatalf("%+v", got)
				}
			case <-time.After(testwait.Bound):
				if release != nil {
					release()
					release = nil
				}
				<-done
				t.Fatal("diagnostic waited on maintenance catalog lock")
			}
		})
	}
}
func TestMaintenanceDiagnosticRejectsUnreadableRecord(t *testing.T) {
	for _, kind := range []string{"json", "wide-mode", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			f := newFixture(t)
			name := filepath.Join(f.s.Data, MaintenanceFile)
			switch kind {
			case "json":
				must(t, os.WriteFile(name, []byte("{"), 0600))
			case "wide-mode":
				must(t, os.WriteFile(name, []byte("{}"), 0644))
			case "symlink":
				must(t, os.Symlink(filepath.Join(t.TempDir(), "missing"), name))
			}
			if m, err := f.s.ReadMaintenanceDiagnostic(); err == nil || m != nil {
				t.Fatalf("unreadable evidence became absence/health: m=%+v err=%v", m, err)
			}
		})
	}
}
