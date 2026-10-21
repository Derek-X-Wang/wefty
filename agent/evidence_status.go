package agent

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
)

// PendingEvidence inspects the installed outbox without initializing or
// migrating it. Receipt/tombstone history is not pending delivery. Missing
// state means no local evidence; unreadable or old unknown state is unverified.
func PendingEvidence(ctx context.Context, directory, nodeID string) (int, error) {
	path := filepath.Join(directory, spoolFileName(nodeID))
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if !info.Mode().IsRegular() {
		return 0, fmt.Errorf("outbox must be a regular file")
	}
	query := url.Values{"mode": {"ro"}, "_pragma": {"query_only(1)", "busy_timeout(1000)"}}
	db, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: path, RawQuery: query.Encode()}).String())
	if err != nil {
		return 0, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	attempts, err := (&logSpool{db: db}).pendingAttempts(ctx)
	if err != nil {
		return 0, err
	}
	var removals int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM spool_removals").Scan(&removals); err != nil {
		return 0, err
	}
	return len(attempts) + removals, nil
}
