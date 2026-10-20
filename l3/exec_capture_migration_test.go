package l3

import (
	"path/filepath"
	"testing"
)

func TestExecCaptureOwnerColumnMigratesExistingLedger(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.sqlite")
	store, e := OpenStore(path, StoreOptions{})
	if e != nil {
		t.Fatal(e)
	}
	var count int
	if e = store.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('exec_captures') WHERE name='uploader'`).Scan(&count); e != nil {
		t.Fatal(e)
	}
	if count != 0 {
		if _, e = store.db.Exec(`ALTER TABLE exec_captures DROP COLUMN uploader`); e != nil {
			t.Fatal(e)
		}
	}
	store.Close()
	store, e = OpenStore(path, StoreOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	if e = store.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('exec_captures') WHERE name='uploader'`).Scan(&count); e != nil || count != 1 {
		t.Fatalf("owner migration: columns=%d err=%v", count, e)
	}
}
