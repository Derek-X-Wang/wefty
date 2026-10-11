package l3

import "testing"

// The ledger zeroes what it deletes or clears, as L1 does (#52).
func TestLedgerOpensWithSecureDelete(t *testing.T) {
	s, _, _ := recoveryStore(t)
	var secureDelete int
	if err := s.db.QueryRow(`PRAGMA secure_delete`).Scan(&secureDelete); err != nil {
		t.Fatal(err)
	}
	if secureDelete != 1 {
		t.Fatalf("secure_delete = %d, want 1", secureDelete)
	}
}
