package l1

import (
	"context"
	"database/sql"
	"net/http"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// PersonAdminCheckRequest is Fabric evidence vouched for by the authenticated
// run ledger. It grants no job authority and is never accepted by job cancel.
type PersonAdminCheckRequest struct {
	FabricID string `json:"fabric_id"`
	UserID   string `json:"user_id"`
	DeviceID string `json:"device_id"`
}

type PersonAdminCheck struct {
	CurrentAdmin   bool  `json:"current_admin"`
	PolicyRevision int64 `json:"policy_revision"`
}

func (s *Store) CheckPersonAdmin(ctx context.Context, request PersonAdminCheckRequest) (PersonAdminCheck, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return PersonAdminCheck{}, internalError(err, "begin person admin check")
	}
	defer tx.Rollback()
	var answer PersonAdminCheck
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM admin_policy WHERE singleton=1`).Scan(&answer.PolicyRevision); err != nil {
		return PersonAdminCheck{}, internalError(err, "read admin check policy revision")
	}
	err = requireCurrentAdmin(ctx, tx, fabric.Identity{FabricID: request.FabricID, UserID: request.UserID, DeviceID: request.DeviceID})
	if err != nil && errorCode(err) != contract.ErrorAdminRequired {
		return PersonAdminCheck{}, err
	}
	answer.CurrentAdmin = err == nil
	if err := tx.Commit(); err != nil {
		return PersonAdminCheck{}, internalError(err, "commit person admin check")
	}
	return answer, nil
}

func (s *Server) checkPersonAdmin(w http.ResponseWriter, r *http.Request) {
	if !s.isRunLedgerSubmitter(identityFromRequest(r).NodeID) {
		writeError(w, protocolError(contract.ErrorForbidden, "only the L3 run ledger may check person admin membership"))
		return
	}
	var request PersonAdminCheckRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	answer, err := s.store.CheckPersonAdmin(r.Context(), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, answer)
}
