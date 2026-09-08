//go:build (service_acceptance || service_acceptance_realtiming) && (darwin || linux)

package serviceacceptance

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The second connection models the ACK owner's committed intent release.
// Real ACK/finalization ordering is covered separately in agent's HTTP proof.
func TestRemovalSpoolObservation(t *testing.T) {
	open := func(t *testing.T) (*sql.DB, *sql.DB) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "spool.sqlite")
		reader, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { reader.Close() })
		writer, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { writer.Close() })
		for _, query := range []string{"CREATE TABLE spool_attempts(job_id TEXT)", "CREATE TABLE spool_removals(job_id TEXT, removal_generation INTEGER, cleanup_fence TEXT, root_instance_id TEXT)", "INSERT INTO spool_removals VALUES('job',3,'fence','root')"} {
			if _, err := writer.Exec(query); err != nil {
				t.Fatal(err)
			}
		}
		return reader, writer
	}
	noAgentError := func() error { return nil }
	t.Run("held_intent_then_real_commit", func(t *testing.T) {
		reader, writer := open(t)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		held := make(chan struct{})
		release := make(chan struct{})
		done := make(chan error, 1)
		first := true
		go func() {
			done <- waitRemovalSpool(ctx, reader, "job", noAgentError, func(snapshot removalSpoolSnapshot) {
				if first {
					first = false
					close(held)
					select {
					case <-release:
					case <-ctx.Done():
					}
				}
			})
		}()
		defer func() { cancel(); <-done }()
		select {
		case <-held:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		// This committed write must succeed while the observer callback is held:
		// its read transaction has already ended.
		if _, err := writer.ExecContext(ctx, "DELETE FROM spool_removals WHERE job_id='job' AND removal_generation=3 AND cleanup_fence='fence' AND root_instance_id='root'"); err != nil {
			t.Fatal(err)
		}
		close(release)
		// Preserve result for the deferred join without consuming it twice.
		select {
		case err := <-done:
			done <- err
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	})
	t.Run("held_intent_cancellation_preserves_evidence", func(t *testing.T) {
		reader, _ := open(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		err := waitRemovalSpool(ctx, reader, "job", noAgentError, func(removalSpoolSnapshot) { cancel() })
		if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "attempts=0 removals=1") || !strings.Contains(err.Error(), "generation=3 fence=fence root=root") {
			t.Fatalf("held intent error=%v", err)
		}
	})
	t.Run("expired_context_cannot_accept_zero", func(t *testing.T) {
		reader, writer := open(t)
		if _, err := writer.Exec("DELETE FROM spool_removals"); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
		defer cancel()
		if err := waitRemovalSpool(ctx, reader, "job", noAgentError, nil); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expired result=%v", err)
		}
	})
	t.Run("SQL_error_is_not_completion", func(t *testing.T) {
		reader, writer := open(t)
		if _, err := writer.Exec("DROP TABLE spool_removals"); err != nil {
			t.Fatal(err)
		}
		if err := waitRemovalSpool(t.Context(), reader, "job", noAgentError, nil); err == nil {
			t.Fatal("missing table accepted as completion")
		}
	})
	t.Run("agent_exit_is_not_completion", func(t *testing.T) {
		reader, _ := open(t)
		sentinel := errors.New("agent exited")
		if err := waitRemovalSpool(t.Context(), reader, "job", func() error { return sentinel }, nil); !errors.Is(err, sentinel) {
			t.Fatalf("agent exit=%v", err)
		}
	})
}
