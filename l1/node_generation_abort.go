package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/upgradecompat"
)

// StagedGenerationAbort binds removal to the same installation operation and
// closed admission. It can end only a staged session, which never held work.
type StagedGenerationAbort struct {
	OperationID          string `json:"operation_id"`
	PrimaryGenerationID  string `json:"primary_generation_id"`
	PrimaryBootSessionID string `json:"primary_boot_session_id"`
	AuthorityGeneration  int64  `json:"authority_generation"`
	CatalogRevision      int64  `json:"catalog_revision"`
	IntentRevision       int64  `json:"intent_revision"`
	IntentReason         string `json:"intent_reason"`
}

func (s *Store) abortStagedGeneration(ctx context.Context, identity, nodeID, generation string, request CloseNodeGenerationRequest) (NodeGenerationInventory, error) {
	abort := request.Abort
	if abort == nil || abort.OperationID == "" || abort.PrimaryGenerationID == "" || abort.PrimaryBootSessionID == "" || abort.AuthorityGeneration <= 0 || abort.IntentRevision <= 0 || abort.IntentReason == "" || request.BootSessionID == "" || request.PromotionOperationID != abort.OperationID || !request.RuntimesReaped || !request.EvidenceSettled || len(request.ResidentAttempts) != 0 {
		return NodeGenerationInventory{}, stagedAbortConflict("staged abort requires exact operation and absent runtimes")
	}
	request.ResidentAttempts = canonicalResidents(request.ResidentAttempts)
	payload, _ := json.Marshal(struct {
		Generation string                     `json:"generation_id"`
		Request    CloseNodeGenerationRequest `json:"request"`
	}{generation, request})
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return NodeGenerationInventory{}, err
	}
	defer write.rollback()
	tx := write.tx
	if err = requireGenerationIdentity(ctx, tx, nodeID, identity); err != nil {
		return NodeGenerationInventory{}, err
	}
	var prior, result []byte
	operation := "abort:" + abort.OperationID
	err = tx.QueryRowContext(ctx, `SELECT request_json,result_json FROM node_generation_promotions WHERE node_id=? AND operation_id=?`, nodeID, operation).Scan(&prior, &result)
	if err == nil {
		if string(prior) != string(payload) {
			return NodeGenerationInventory{}, stagedAbortConflict("staged abort operation payload changed")
		}
		var in NodeGenerationInventory
		err = json.Unmarshal(result, &in)
		return in, err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return NodeGenerationInventory{}, err
	}
	in, err := readNodeGenerations(ctx, tx, nodeID)
	if err != nil {
		return in, err
	}
	primary, target := false, false
	for _, g := range in.Generations {
		if g.AgentGenerationID == abort.PrimaryGenerationID {
			primary = g.Role == NodeGenerationPrimary && g.BootSessionID == abort.PrimaryBootSessionID && g.AuthorityGeneration == abort.AuthorityGeneration
		}
		if g.AgentGenerationID == generation {
			target = g.Role == NodeGenerationStaged && g.BootSessionID == request.BootSessionID && len(g.ResidentAttempts) == 0
		}
	}
	var boot, reason string
	var authority, intent int64
	var claims bool
	if err = tx.QueryRowContext(ctx, `SELECT boot_session_id,authority_generation,intent_revision,claims_enabled,intent_reason FROM nodes WHERE node_id=?`, nodeID).Scan(&boot, &authority, &intent, &claims, &reason); err != nil {
		return in, err
	}
	if !primary || !target || in.CatalogRevision != abort.CatalogRevision || boot != abort.PrimaryBootSessionID || authority != abort.AuthorityGeneration || claims || intent != abort.IntentRevision || reason != abort.IntentReason {
		return in, stagedAbortConflict("staged abort primary or admission fence changed")
	}
	if _, err = tx.ExecContext(ctx, `UPDATE node_generations SET role='closed',promotion_operation_id=?,closed_ns=? WHERE node_id=? AND generation_id=?`, abort.OperationID, write.at.UnixNano(), nodeID, generation); err != nil {
		return in, err
	}
	in, err = readNodeGenerations(ctx, tx, nodeID)
	if err != nil {
		return in, err
	}
	result, _ = json.Marshal(in)
	if _, err = tx.ExecContext(ctx, `INSERT INTO node_generation_promotions(node_id,operation_id,request_json,result_json) VALUES(?,?,?,?)`, nodeID, operation, payload, result); err != nil {
		return in, err
	}
	return in, tx.Commit()
}

func stagedAbortConflict(message string) error {
	return protocolErrorWithDetails(contract.ErrorConflict, map[string]any{"reason": string(upgradecompat.InvalidInventory)}, "%s", message)
}
