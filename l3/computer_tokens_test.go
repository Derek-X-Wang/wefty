package l3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func testComputerScope() ComputerTokenScopeProof {
	return ComputerTokenScopeProof{ComputerID: "computer-1", ComputerAttemptID: "attempt-1",
		ComputerStorageGeneration: 7, SubmitIntentRevision: 3, HostNodeID: "node-1",
		HostBootSessionID: "boot-1", SubmitMaxInflight: 2}
}

func TestComputerTokenIsHashOnlyRevocableAndPromotionInvalidated(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "computer-tokens.sqlite")
	clock := &mutableClock{now: time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)}
	store, err := OpenStore(path, StoreOptions{Clock: clock, ComputerAuthorityInstanceID: "instance-1"})
	if err != nil {
		t.Fatal(err)
	}
	grant, err := store.MintComputerToken(ctx, testComputerScope())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(grant.Token, "wcomputer_") || len(strings.TrimPrefix(grant.Token, "wcomputer_")) != 64 {
		t.Fatalf("token does not contain 256 random bits: %q", grant.Token)
	}
	var plaintextMatches int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM computer_token_grants WHERE CAST(token_hash AS TEXT)=?`, grant.Token).Scan(&plaintextMatches); err != nil {
		t.Fatal(err)
	}
	if plaintextMatches != 0 {
		t.Fatal("plaintext Computer token was stored")
	}
	if _, err := store.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	databaseBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(databaseBytes, []byte(grant.Token)) {
		t.Fatal("plaintext Computer token leaked into the persisted L3 artifact")
	}
	scope, err := store.AuthenticateComputerToken(ctx, grant.Token)
	if err != nil || scope.ComputerID != grant.ComputerID || scope.GrantRevision != grant.GrantRevision {
		t.Fatalf("authenticate = (%#v, %v)", scope, err)
	}
	receipt, err := store.RevokeComputerTokens(ctx, ComputerTokenRevocationRequest{ComputerID: grant.ComputerID,
		SubmitIntentRevision: grant.SubmitIntentRevision + 1, Reason: "disabled"})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ComputerID != grant.ComputerID || receipt.SubmitIntentRevision != grant.SubmitIntentRevision+1 ||
		receipt.RevokedGrantCount != 1 || receipt.CommittedAt.IsZero() {
		t.Fatalf("revocation receipt = %#v", receipt)
	}
	if _, err := store.AuthenticateComputerToken(ctx, grant.Token); err == nil {
		t.Fatal("revoked Computer token authenticated")
	}
	grant, err = store.MintComputerToken(ctx, testComputerScope())
	if err != nil {
		t.Fatal(err)
	}
	previousToken := grant.Token
	newAttemptProof := testComputerScope()
	newAttemptProof.ComputerAttemptID = "attempt-2"
	grant, err = store.MintComputerToken(ctx, newAttemptProof)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateComputerToken(ctx, previousToken); err == nil {
		t.Fatal("prior-attempt token survived replacement grant")
	}
	if _, err := store.db.Exec(`UPDATE computer_token_audit SET reason='forged'`); err == nil {
		t.Fatal("Computer token audit accepted mutation")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(path, StoreOptions{Clock: clock, ComputerAuthorityInstanceID: "instance-1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateComputerToken(ctx, grant.Token); err != nil {
		t.Fatalf("ordinary process restart invalidated Computer token: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(path, StoreOptions{Clock: clock, ComputerAuthorityInstanceID: "instance-2"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.AuthenticateComputerToken(ctx, grant.Token); err == nil {
		t.Fatal("pre-promotion Computer token survived L3 authority generation advance")
	}
}

func TestComputerRunProvenanceAndAtomicInflightLimit(t *testing.T) {
	ctx := context.Background()
	store, err := OpenStore(filepath.Join(t.TempDir(), "computer-runs.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	proof := testComputerScope()
	grant, err := store.MintComputerToken(ctx, proof)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := store.AuthenticateComputerToken(ctx, grant.Token)
	if err != nil {
		t.Fatal(err)
	}
	create := func(key, content string) (contract.RunRecord, bool, error) {
		digest := sha256.Sum256([]byte(content))
		return store.CreateRun(ctx, CreateRunInput{IdempotencyKey: key, Actor: "computer:" + scope.ComputerID,
			ComputerScope: &scope, Request: CreateRunRequest{InlineScript: &InlineScriptInput{Content: content,
				SHA256: hex.EncodeToString(digest[:]), Interpreter: []string{"/bin/sh"}}, Params: []byte(`{}`)},
			VerifyComputerScope: func(context.Context, ComputerTokenScope) error { return nil }})
	}
	first, _, err := create("computer-root-1", "exit 0\n")
	if err != nil {
		t.Fatal(err)
	}
	reminted, err := store.MintComputerToken(ctx, proof)
	if err != nil {
		t.Fatal(err)
	}
	scope, err = store.AuthenticateComputerToken(ctx, reminted.Token)
	if err != nil {
		t.Fatal(err)
	}
	if replay, replayed, err := create("computer-root-1", "exit 0\n"); err != nil || !replayed || replay.RunID != first.RunID {
		t.Fatalf("stable-principal replay after re-mint = (%#v, %t, %v)", replay, replayed, err)
	}
	computerOneScope := scope
	foreignProof := proof
	foreignProof.ComputerID = "computer-2"
	foreignProof.ComputerAttemptID = "attempt-2"
	foreignGrant, err := store.MintComputerToken(ctx, foreignProof)
	if err != nil {
		t.Fatal(err)
	}
	scope, err = store.AuthenticateComputerToken(ctx, foreignGrant.Token)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := create("computer-root-1", "exit 0\n"); err == nil {
		t.Fatal("cross-Computer idempotency replay unexpectedly succeeded")
	} else if code, _ := errorDetails(err); code != contract.ErrorIdempotencyConflict {
		t.Fatalf("cross-Computer idempotency replay error = %v", err)
	}
	scope = computerOneScope
	trigger, err := store.GetTrigger(ctx, first.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if trigger.Source != "computer" || trigger.SourceRunID != "" || trigger.ComputerID != proof.ComputerID ||
		trigger.ComputerAttemptID != proof.ComputerAttemptID || trigger.ComputerStorageGeneration != proof.ComputerStorageGeneration ||
		trigger.SubmitIntentRevision != proof.SubmitIntentRevision {
		t.Fatalf("Computer root provenance = %#v", trigger)
	}
	newGenerationScope := scope
	newGenerationScope.ComputerStorageGeneration++
	if allowed, err := store.CanComputerReadRun(ctx, newGenerationScope, first.RunID); err != nil || allowed {
		t.Fatalf("earlier-generation read = (%t, %v), want denied", allowed, err)
	}
	foreignScope := scope
	foreignScope.ComputerID = "computer-2"
	if allowed, err := store.CanComputerReadRun(ctx, foreignScope, first.RunID); err != nil || allowed {
		t.Fatalf("foreign-Computer read = (%t, %v), want denied", allowed, err)
	}
	childContent := "exit 4\n"
	childDigest := sha256.Sum256([]byte(childContent))
	child, _, err := store.CreateRun(ctx, CreateRunInput{IdempotencyKey: "computer-child-1", Actor: "run:" + first.RunID,
		Request: CreateRunRequest{ParentRunID: first.RunID, Params: []byte(`{}`), InlineScript: &InlineScriptInput{
			Content: childContent, SHA256: hex.EncodeToString(childDigest[:]), Interpreter: []string{"/bin/sh"}}}})
	if err != nil {
		t.Fatal(err)
	}
	childTrigger, err := store.GetTrigger(ctx, child.RunID)
	if err != nil || childTrigger.Source != "chain" || childTrigger.SourceRunID != first.RunID || childTrigger.ComputerID != "" {
		t.Fatalf("Computer descendant provenance = (%#v, %v)", childTrigger, err)
	}
	if allowed, err := store.CanComputerReadRun(ctx, scope, child.RunID); err != nil || !allowed {
		t.Fatalf("Computer descendant read = (%t, %v), want allowed", allowed, err)
	}
	if _, _, err := create("computer-root-2", "exit 1\n"); err != nil {
		t.Fatal(err)
	}
	if _, replayed, err := create("computer-root-2", "exit 1\n"); err != nil || !replayed {
		t.Fatalf("replay at limit = (%v, %v)", replayed, err)
	}
	if _, _, err := create("computer-root-3", "exit 2\n"); err == nil {
		t.Fatal("third Computer root passed inflight limit")
	} else {
		var protocolErr *Error
		if !errors.As(err, &protocolErr) || protocolErr.Code != contract.ErrorSubmitInflightLimit {
			t.Fatalf("limit error = %v", err)
		}
	}
	if _, err := store.db.Exec(`UPDATE runs SET status=? WHERE run_id=?`, contract.RunSucceeded, first.RunID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := create("computer-root-3", "exit 2\n"); err == nil {
		t.Fatal("terminal root with nonterminal descendant released inflight capacity")
	}
	if _, err := store.db.Exec(`UPDATE runs SET status=? WHERE run_id=?`, contract.RunSucceeded, child.RunID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := create("computer-root-3", "exit 2\n"); err != nil {
		t.Fatalf("capacity did not reopen after terminal lineage: %v", err)
	}
	var parent sql.NullString
	if err := store.db.QueryRow(`SELECT parent_run_id FROM runs WHERE run_id=?`, first.RunID).Scan(&parent); err != nil || parent.Valid {
		t.Fatalf("Computer root parent = (%#v, %v)", parent, err)
	}
}

func TestComputerAttemptAndHostRevocationsAreIdentityScoped(t *testing.T) {
	ctx := context.Background()
	store, err := OpenStore(filepath.Join(t.TempDir(), "computer-revocations.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	proof := testComputerScope()
	grant, err := store.MintComputerToken(ctx, proof)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RevokeComputerAttemptTokens(ctx, proof.ComputerID, proof.ComputerAttemptID, "foreign-node", "attempt_terminal"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateComputerToken(ctx, grant.Token); err != nil {
		t.Fatalf("foreign host revoked attempt grant: %v", err)
	}
	if err := store.RevokeComputerAttemptTokens(ctx, proof.ComputerID, proof.ComputerAttemptID, proof.HostNodeID, "attempt_terminal"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateComputerToken(ctx, grant.Token); err == nil {
		t.Fatal("exact attempt revocation left grant active")
	}
	grant, err = store.MintComputerToken(ctx, proof)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.RevokeHostComputerTokens(ctx, "foreign-node", "boot-foreign", "agent_restart", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateComputerToken(ctx, grant.Token); err != nil {
		t.Fatalf("foreign host restart revoked grant: %v", err)
	}
	previousBootProof := proof
	previousBootProof.HostBootSessionID = "boot-previous"
	grant, err = store.MintComputerToken(ctx, previousBootProof)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.RevokeHostComputerTokens(ctx, proof.HostNodeID, proof.HostBootSessionID, "agent_restart", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateComputerToken(ctx, grant.Token); err == nil {
		t.Fatal("host restart revocation left grant active")
	}
}

func TestDelayedRevokeHostEndsOnlyEarlierBootsGrants(t *testing.T) {
	ctx := context.Background()
	store, err := OpenStore(filepath.Join(t.TempDir(), "delayed-host-revoke.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	mint := func(computerID, attemptID, hostNodeID, bootSessionID string) ComputerTokenGrant {
		t.Helper()
		proof := testComputerScope()
		proof.ComputerID = computerID
		proof.ComputerAttemptID = attemptID
		proof.HostNodeID = hostNodeID
		proof.HostBootSessionID = bootSessionID
		grant, err := store.MintComputerToken(ctx, proof)
		if err != nil {
			t.Fatal(err)
		}
		return grant
	}
	previous := mint("computer-previous", "attempt-previous", "node-1", "boot-previous")
	current := mint("computer-current", "attempt-current", "node-1", "boot-current")
	foreign := mint("computer-foreign", "attempt-foreign", "node-2", "boot-foreign")

	var next, currentAfterProof ComputerTokenGrant
	revoked, err := store.RevokeHostComputerTokens(ctx, "node-1", "boot-current", "agent_restart", func(context.Context) error {
		// These grants commit after revoke-host read its high-water mark. The
		// later boot simulates a successor taking over after L1 answered but
		// before this delayed request reaches its L3 transaction.
		next = mint("computer-next", "attempt-next", "node-1", "boot-next")
		currentAfterProof = mint("computer-current-2", "attempt-current-2", "node-1", "boot-current")
		return nil
	})
	if err != nil || revoked != 1 {
		t.Fatalf("revoke-host = (%d, %v), want one earlier-boot grant", revoked, err)
	}
	if _, err := store.AuthenticateComputerToken(ctx, previous.Token); err == nil {
		t.Fatal("earlier boot's grant survived revoke-host")
	}
	for name, grant := range map[string]ComputerTokenGrant{
		"current": current, "next": next, "current after proof": currentAfterProof, "foreign": foreign,
	} {
		if _, err := store.AuthenticateComputerToken(ctx, grant.Token); err != nil {
			t.Fatalf("%s grant was revoked: %v", name, err)
		}
	}

	refusal := protocolError(contract.ErrorForbidden, "boot session is not the host's current registration")
	if _, err := store.RevokeHostComputerTokens(ctx, "node-1", "boot-current", "agent_restart", func(context.Context) error {
		return refusal
	}); !errors.Is(err, refusal) {
		t.Fatalf("replayed stale claim = %v, want L1 refusal", err)
	}
	for name, grant := range map[string]ComputerTokenGrant{"current": current, "next": next, "current after proof": currentAfterProof} {
		if _, err := store.AuthenticateComputerToken(ctx, grant.Token); err != nil {
			t.Fatalf("%s grant changed after refused replay: %v", name, err)
		}
	}
}

func TestRevokeHostRevokesLegacyGrantsWithoutBootSession(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy-host-revoke.sqlite")
	open := func() *Store {
		t.Helper()
		store, err := OpenStore(path, StoreOptions{ComputerAuthorityInstanceID: "legacy-test"})
		if err != nil {
			t.Fatal(err)
		}
		return store
	}
	store := open()
	legacy, err := store.MintComputerToken(ctx, testComputerScope())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`ALTER TABLE computer_token_grants DROP COLUMN host_boot_session_id`); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Opening twice proves the additive migration is safe on every open.
	store = open()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = open()
	defer store.Close()
	currentProof := testComputerScope()
	currentProof.ComputerID = "computer-current"
	currentProof.ComputerAttemptID = "attempt-current"
	current, err := store.MintComputerToken(ctx, currentProof)
	if err != nil {
		t.Fatal(err)
	}
	revoked, err := store.RevokeHostComputerTokens(ctx, currentProof.HostNodeID, currentProof.HostBootSessionID,
		"agent_restart", func(context.Context) error { return nil })
	if err != nil || revoked != 1 {
		t.Fatalf("legacy revoke-host = (%d, %v), want one revoked grant", revoked, err)
	}
	if _, err := store.AuthenticateComputerToken(ctx, legacy.Token); err == nil {
		t.Fatal("legacy grant without a boot session survived revoke-host")
	}
	if _, err := store.AuthenticateComputerToken(ctx, current.Token); err != nil {
		t.Fatalf("current boot grant was revoked: %v", err)
	}
}

// #553 review: L1 ends exactly one attempt's authority when it accepts that
// attempt's completion, so it asks for exactly that attempt's grants to be
// revoked. The request may arrive after the next attempt was minted; that
// grant, and every other Computer's grant, is untouched.
func TestControlPlaneAttemptScopedRevocationRevokesOnlyThatAttempt(t *testing.T) {
	ctx := context.Background()
	store, err := OpenStore(filepath.Join(t.TempDir(), "computer-attempt-scoped.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	oldProof := testComputerScope()
	if _, err := store.MintComputerToken(ctx, oldProof); err != nil {
		t.Fatal(err)
	}
	replacementProof := testComputerScope()
	replacementProof.ComputerAttemptID = "attempt-2"
	replacement, err := store.MintComputerToken(ctx, replacementProof)
	if err != nil {
		t.Fatal(err)
	}
	otherProof := testComputerScope()
	otherProof.ComputerID = "computer-2"
	otherProof.HostNodeID = "node-2"
	other, err := store.MintComputerToken(ctx, otherProof)
	if err != nil {
		t.Fatal(err)
	}
	assertActive := func(stage string, tokens ...string) {
		t.Helper()
		for _, token := range tokens {
			if _, err := store.AuthenticateComputerToken(ctx, token); err != nil {
				t.Fatalf("%s: a grant outside the revoked attempt was revoked: %v", stage, err)
			}
		}
	}
	activeGrants := func() int {
		t.Helper()
		var count int
		if err := store.db.QueryRow(`SELECT COUNT(*) FROM computer_token_grants WHERE revoked_ns IS NULL`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}

	for name, invalid := range map[string]ComputerTokenRevocationRequest{
		"with_revoke_all":       {ComputerID: "computer-1", ComputerAttemptID: "attempt-2", RevokeAll: true, Reason: "attempt_terminal"},
		"with_submit_revision":  {ComputerID: "computer-1", ComputerAttemptID: "attempt-2", SubmitIntentRevision: 99, Reason: "attempt_terminal"},
		"with_restore_revision": {ComputerID: "computer-1", ComputerAttemptID: "attempt-2", RevokeAll: true, RestoreOperationRevision: 4, Reason: "computer_restoring"},
		"blank_attempt":         {ComputerID: "computer-1", ComputerAttemptID: " ", RevokeAll: true, SubmitIntentRevision: 1, Reason: "attempt_terminal"},
		"padded_attempt":        {ComputerID: "computer-1", ComputerAttemptID: " attempt-2", Reason: "attempt_terminal"},
		"oversized_attempt":     {ComputerID: "computer-1", ComputerAttemptID: strings.Repeat("a", 256), Reason: "attempt_terminal"},
		"missing_computer":      {ComputerAttemptID: "attempt-2", Reason: "attempt_terminal"},
		"missing_reason":        {ComputerID: "computer-1", ComputerAttemptID: "attempt-2"},
	} {
		receipt, err := store.RevokeComputerTokens(ctx, invalid)
		var protocolErr *Error
		if !errors.As(err, &protocolErr) || protocolErr.Code != contract.ErrorInvalidRequest || receipt.RevokedGrantCount != 0 {
			t.Fatalf("%s: receipt=%#v err=%v, want invalid_request", name, receipt, err)
		}
	}
	if got := activeGrants(); got != 2 {
		t.Fatalf("refused requests changed the grant table: %d active, want 2", got)
	}

	// The completed attempt's revocation arrives after the replacement mint.
	receipt, err := store.RevokeComputerTokens(ctx, ComputerTokenRevocationRequest{
		ComputerID: oldProof.ComputerID, ComputerAttemptID: oldProof.ComputerAttemptID, Reason: "attempt_terminal"})
	if err != nil || receipt.ComputerID != oldProof.ComputerID || receipt.ComputerAttemptID != oldProof.ComputerAttemptID ||
		receipt.RevokedGrantCount != 0 || receipt.SubmitIntentRevision != 0 || receipt.CommittedAt.IsZero() {
		t.Fatalf("late attempt-scoped receipt = %#v err=%v", receipt, err)
	}
	assertActive("late revocation of the completed attempt", replacement.Token, other.Token)

	// Scoped to the replacement attempt, whichever host holds it: only it.
	receipt, err = store.RevokeComputerTokens(ctx, ComputerTokenRevocationRequest{
		ComputerID: "computer-1", ComputerAttemptID: "attempt-2", Reason: "attempt_terminal"})
	if err != nil || receipt.RevokedGrantCount != 1 || receipt.ComputerAttemptID != "attempt-2" {
		t.Fatalf("attempt-scoped receipt = %#v err=%v", receipt, err)
	}
	if _, err := store.AuthenticateComputerToken(ctx, replacement.Token); err == nil {
		t.Fatal("attempt-scoped revocation left the attempt's grant active")
	}
	assertActive("revocation of attempt-2 of computer-1", other.Token)
	var audited int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM computer_token_audit WHERE operation='revoked'
		AND computer_id='computer-1' AND computer_attempt_id='attempt-2' AND reason='attempt_terminal'`).Scan(&audited); err != nil || audited != 1 {
		t.Fatalf("attempt-scoped revocation audit rows = %d err=%v, want 1", audited, err)
	}
}
