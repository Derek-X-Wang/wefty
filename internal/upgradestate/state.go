// Package upgradestate reads SQLite state without invoking either store's
// migrations. Its copies are disposable preflight inputs, never recovery data.
package upgradestate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"

	_ "modernc.org/sqlite"
)

func open(path string) (*sql.DB, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("upgrade database %s must be a regular file", path)
	}
	query := url.Values{"mode": {"ro"}, "_pragma": {"busy_timeout(1000)", "query_only(1)"}}
	db, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: path, RawQuery: query.Encode()}).String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

// Snapshot uses SQLite's online snapshot operation: copying the .sqlite file
// alone would miss WAL commits, and copying its sidecars can race a writer.
func Snapshot(ctx context.Context, live, destination string) error {
	db, err := open(live)
	if err != nil {
		return err
	}
	defer db.Close()
	// query_only prevents even VACUUM INTO; mode=ro still protects the source.
	if _, err := db.ExecContext(ctx, "PRAGMA query_only=OFF"); err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, "VACUUM INTO ?", destination)
	return err
}

// Fingerprint covers DDL, application/user versions and the names of L1 data
// migrations (including partially applied ones). Authority rows and WAL/page
// layout do not participate. Any read failure forbids binary rollback.
func Fingerprint(ctx context.Context, path string) (string, error) {
	db, err := open(path)
	if err != nil {
		return "", err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	var values []any
	for _, pragma := range []string{"user_version", "application_id"} {
		var value int64
		if err := tx.QueryRowContext(ctx, "PRAGMA "+pragma).Scan(&value); err != nil {
			return "", err
		}
		values = append(values, value)
	}
	rows, err := tx.QueryContext(ctx, "SELECT type, name, tbl_name, coalesce(sql, '') FROM sqlite_schema ORDER BY type, name")
	if err != nil {
		return "", err
	}
	var migrationTables []string
	for rows.Next() {
		var kind, name, table, ddl string
		if err := rows.Scan(&kind, &name, &table, &ddl); err != nil {
			rows.Close()
			return "", err
		}
		values = append(values, []string{kind, name, table, ddl})
		if kind == "table" && (name == "l1_data_migrations" || name == "l1_data_migration_cursors") {
			migrationTables = append(migrationTables, name)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return "", err
	}
	for _, table := range migrationTables {
		rows, err := tx.QueryContext(ctx, "SELECT name FROM "+table+" ORDER BY name")
		if err != nil {
			return "", err
		}
		values = append(values, table)
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				return "", err
			}
			values = append(values, name)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return "", err
		}
	}
	payload, err := json.Marshal(values)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}
