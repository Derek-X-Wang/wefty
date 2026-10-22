package l1

import (
	"context"

	"github.com/Derek-X-Wang/wefty/contract"
)

// RebindAdminDevice is an offline recovery operation requiring local database
// access. It has no network route and never uses the last observed device.
func (s *Store) RebindAdminDevice(ctx context.Context, userID, deviceID string) (AdminPolicy, error) {
	var err error
	if userID, err = validateAdminUserID(userID); err != nil {
		return AdminPolicy{}, err
	}
	if deviceID, err = validateAdminDeviceID(deviceID); err != nil {
		return AdminPolicy{}, err
	}
	now := canonicalTime(s.clock.Now())
	write, err := s.beginWriteTransaction(ctx, nil, now)
	if err != nil {
		return AdminPolicy{}, internalError(err, "begin admin device rebind")
	}
	defer write.rollback()
	tx := write.tx
	var count int
	var fabricID string
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(MIN(fabric_id),'') FROM admins WHERE user_id=?`, userID).Scan(&count, &fabricID); err != nil {
		return AdminPolicy{}, internalError(err, "read admin for device rebind")
	}
	if count == 0 {
		return AdminPolicy{}, protocolError(contract.ErrorNotFound, "administrator not found")
	}
	if count != 1 {
		return AdminPolicy{}, protocolError(contract.ErrorInvalidRequest, "administrator user_id is ambiguous across issuing Fabrics; reset the local admin policy")
	}
	var revision int64
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM admin_policy WHERE singleton=1`).Scan(&revision); err != nil {
		return AdminPolicy{}, internalError(err, "read admin rebind revision")
	}
	nextRevision := revision + 1
	if _, err := tx.ExecContext(ctx, `UPDATE admins SET device_id=? WHERE fabric_id=? AND user_id=?`, deviceID, fabricID, userID); err != nil {
		return AdminPolicy{}, internalError(err, "rebind administrator device")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE admin_policy SET revision=?,updated_ns=? WHERE singleton=1 AND revision=?`, nextRevision, now.UnixNano(), revision); err != nil {
		return AdminPolicy{}, internalError(err, "advance admin rebind revision")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO admin_policy_audit(revision,operation,actor_kind,actor_fabric_id,actor_user_id,actor_device_id,subject_fabric_id,subject_user_id,subject_device_id,created_ns) VALUES(?,?,?,'','','',?,?,?,?)`, nextRevision, AdminPolicyRebind, AdminPolicyActorLocalOperator, fabricID, userID, deviceID, now.UnixNano()); err != nil {
		return AdminPolicy{}, internalError(err, "append admin device rebind audit")
	}
	policy, err := readAdminPolicy(ctx, tx)
	if err != nil {
		return AdminPolicy{}, internalError(err, "read rebound admin policy")
	}
	if err := tx.Commit(); err != nil {
		return AdminPolicy{}, internalError(err, "commit admin device rebind")
	}
	s.notifyComputerPolicyChanged()
	return policy, nil
}
