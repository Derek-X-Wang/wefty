package l1

import (
	"context"
	"fmt"
	"strings"
)

// migrateAdminPersonDevice is additive: legacy administrators stay unbound.
// Their authority must be recovered explicitly, never from observed devices.
func (s *Store) migrateAdminPersonDevice(ctx context.Context) error {
	if err := s.ensureColumn(ctx, "admins", "device_id", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "admin_policy_audit", "subject_device_id", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	var sourceSQL string
	if err := s.db.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='table' AND name='admin_policy_audit'`).Scan(&sourceSQL); err != nil {
		return fmt.Errorf("l1: inspect admin device audit schema: %w", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("l1: begin admin person device migration: %w", err)
	}
	defer tx.Rollback()
	if !strings.Contains(sourceSQL, "'rebind'") {
		createSQL, err := migratedSQLiteCreateTable(sourceSQL, "admin_policy_audit_device_migration", map[string]string{"'reset')": "'reset', 'rebind')"})
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, createSQL); err != nil {
			return fmt.Errorf("l1: create admin device audit schema: %w", err)
		}
		if err := copySQLiteTableColumns(ctx, tx, "admin_policy_audit", "admin_policy_audit_device_migration"); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DROP TABLE admin_policy_audit; ALTER TABLE admin_policy_audit_device_migration RENAME TO admin_policy_audit;`); err != nil {
			return fmt.Errorf("l1: replace admin device audit schema: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `CREATE TRIGGER IF NOT EXISTS admin_policy_audit_no_update
 BEFORE UPDATE ON admin_policy_audit BEGIN SELECT RAISE(ABORT,'admin policy audit is immutable');END;
 CREATE TRIGGER IF NOT EXISTS admin_policy_audit_no_delete
 BEFORE DELETE ON admin_policy_audit BEGIN SELECT RAISE(ABORT,'admin policy audit is immutable');END;`); err != nil {
		return fmt.Errorf("l1: protect admin policy audit: %w", err)
	}
	return tx.Commit()
}
