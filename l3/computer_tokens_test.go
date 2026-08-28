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
		ComputerStorageGeneration: 7, SubmitIntentRevision: 3, HostNodeID: "node-1", SubmitMaxInflight: 2}
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
	if err := store.RevokeComputerTokens(ctx, ComputerTokenRevocationRequest{ComputerID: grant.ComputerID,
		SubmitIntentRevision: grant.SubmitIntentRevision + 1, Reason: "disabled"}); err != nil {
		t.Fatal(err)
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
	if err := store.RevokeHostComputerTokens(ctx, "foreign-node", "agent_restart"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateComputerToken(ctx, grant.Token); err != nil {
		t.Fatalf("foreign host restart revoked grant: %v", err)
	}
	if err := store.RevokeHostComputerTokens(ctx, proof.HostNodeID, "agent_restart"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateComputerToken(ctx, grant.Token); err == nil {
		t.Fatal("host restart revocation left grant active")
	}
}
