package l3

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

// endedAttempt is L1's answer once the minting attempt is no longer the
// Computer's live attempt.
func endedAttempt(context.Context) (ComputerTokenScopeProof, error) {
	return ComputerTokenScopeProof{}, protocolError(contract.ErrorForbidden, "Computer submission authority is not current")
}

func stillCurrent(proof ComputerTokenScopeProof) ComputerScopeReprover {
	return func(context.Context) (ComputerTokenScopeProof, error) { return proof, nil }
}

func openMintRaceStore(t *testing.T) *Store {
	t.Helper()
	store, err := OpenStore(filepath.Join(t.TempDir(), "computer-mint-race.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func grantRevocation(t *testing.T, store *Store, attemptID string) (revoked bool, reason string, count int) {
	t.Helper()
	rows, err := store.db.Query(`SELECT revoked_ns IS NOT NULL, revocation_reason FROM computer_token_grants
		WHERE computer_id='computer-1' AND computer_attempt_id=? ORDER BY grant_revision`, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		count++
		if err := rows.Scan(&revoked, &reason); err != nil {
			t.Fatal(err)
		}
	}
	return revoked, reason, count
}

func assertForbidden(t *testing.T, grant ComputerTokenGrant, err error) {
	t.Helper()
	var coded *Error
	if !errors.As(err, &coded) || coded.Code != contract.ErrorForbidden {
		t.Fatalf("late mint error = %v, want forbidden", err)
	}
	if grant.Token != "" {
		t.Fatal("late mint returned a bearer")
	}
}

// #605: L3 proves a mint's scope with L1 and then mints. A late mint from an
// attempt that ended in between must never revoke its successor's grant; it
// is refused, and its own grant is revoked before its bearer is returned.
func TestLateMintFromAnEndedAttemptNeverRevokesTheSuccessorsGrant(t *testing.T) {
	ctx := context.Background()
	late := testComputerScope()
	successor := testComputerScope()
	successor.ComputerAttemptID = "attempt-2"

	t.Run("successor minted first", func(t *testing.T) {
		store := openMintRaceStore(t)
		next, err := store.MintReprovedComputerToken(ctx, successor, stillCurrent(successor))
		if err != nil {
			t.Fatal(err)
		}
		grant, err := store.MintReprovedComputerToken(ctx, late, endedAttempt)
		assertForbidden(t, grant, err)
		if _, err := store.AuthenticateComputerToken(ctx, next.Token); err != nil {
			t.Fatalf("late mint revoked the successor's grant: %v", err)
		}
		if revoked, reason, count := grantRevocation(t, store, late.ComputerAttemptID); count != 1 || !revoked || reason != "mint_scope_not_current" {
			t.Fatalf("late grant revoked=%t reason=%q count=%d", revoked, reason, count)
		}
	})

	t.Run("late grant committed before the successor's", func(t *testing.T) {
		store := openMintRaceStore(t)
		var next ComputerTokenGrant
		// The successor mints entirely while the late grant awaits its re-proof.
		grant, err := store.MintReprovedComputerToken(ctx, late, func(ctx context.Context) (ComputerTokenScopeProof, error) {
			var mintErr error
			next, mintErr = store.MintReprovedComputerToken(ctx, successor, stillCurrent(successor))
			if mintErr != nil {
				t.Fatal(mintErr)
			}
			return endedAttempt(ctx)
		})
		assertForbidden(t, grant, err)
		if _, err := store.AuthenticateComputerToken(ctx, next.Token); err != nil {
			t.Fatalf("late mint revoked the successor's grant: %v", err)
		}
		if revoked, _, count := grantRevocation(t, store, late.ComputerAttemptID); count != 1 || !revoked {
			t.Fatalf("late grant revoked=%t count=%d", revoked, count)
		}
	})

	t.Run("late grant committed after the successor's", func(t *testing.T) {
		store := openMintRaceStore(t)
		// The late mint runs entirely while the successor's grant awaits its
		// re-proof; the successor's settlement must not touch the higher late
		// revision either, since that grant settles itself.
		next, err := store.MintReprovedComputerToken(ctx, successor, func(ctx context.Context) (ComputerTokenScopeProof, error) {
			grant, lateErr := store.MintReprovedComputerToken(ctx, late, endedAttempt)
			assertForbidden(t, grant, lateErr)
			return successor, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.AuthenticateComputerToken(ctx, next.Token); err != nil {
			t.Fatalf("late mint revoked the successor's grant: %v", err)
		}
		if revoked, reason, count := grantRevocation(t, store, late.ComputerAttemptID); count != 1 || !revoked || reason != "mint_scope_not_current" {
			t.Fatalf("late grant revoked=%t reason=%q count=%d", revoked, reason, count)
		}
	})
}

func TestReprovedMintStillRegrantsOverOlderGrants(t *testing.T) {
	ctx := context.Background()
	store := openMintRaceStore(t)
	first := testComputerScope()
	older, err := store.MintReprovedComputerToken(ctx, first, stillCurrent(first))
	if err != nil {
		t.Fatal(err)
	}
	same, err := store.MintReprovedComputerToken(ctx, first, stillCurrent(first))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateComputerToken(ctx, older.Token); err == nil {
		t.Fatal("a same-attempt remint left the earlier grant active")
	}
	next := testComputerScope()
	next.ComputerAttemptID = "attempt-2"
	newer, err := store.MintReprovedComputerToken(ctx, next, stillCurrent(next))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateComputerToken(ctx, same.Token); err == nil {
		t.Fatal("a newer attempt's mint left the older attempt's grant active")
	}
	if revoked, reason, _ := grantRevocation(t, store, first.ComputerAttemptID); !revoked || reason != "regranted" {
		t.Fatalf("older grant revoked=%t reason=%q, want regranted", revoked, reason)
	}
	if _, err := store.AuthenticateComputerToken(ctx, newer.Token); err != nil {
		t.Fatal(err)
	}

	// A re-proof that finds the scope moved (a submission revision bumped
	// between proof and mint) refuses rather than minting a stale grant.
	moved := next
	moved.SubmitIntentRevision++
	grant, err := store.MintReprovedComputerToken(ctx, next, stillCurrent(moved))
	assertForbidden(t, grant, err)
}
