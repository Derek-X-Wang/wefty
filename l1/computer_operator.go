package l1

import (
	"context"
	"net/http"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// Computer client verbs share the middleware's real identity decision. Person
// policy, grant and take-over routes are separate surfaces and are omitted here.
// Agent acknowledgement responses therefore describe refused client verbs.
type computerActionActor = nodeIntentActor

type computerActionValues struct {
	Precondition      ComputerMutationPrecondition
	DiskBytes         int64
	BackupID          string
	TerminateSessions bool
	AllowPowerOff     bool
	KeepOldBackup     bool
	Spec              contract.JobSpec
}

func computerActionDecision(ctx context.Context, tx readModel, computer Computer, verb string, actor computerActionActor, values computerActionValues) error {
	values.Precondition.Actor = actor.Identity.NodeID
	if err := computerActorPreconditionDecision(computer, actor, values.Precondition); err != nil {
		return err
	}
	var err error
	switch verb {
	case "start", "stop":
		desired := contract.ServiceDesiredRunning
		if verb == "stop" {
			desired = contract.ServiceDesiredStopped
		}
		_, err = computerDesiredDecision(ctx, tx, computer, ComputerDesiredStateRequest{ComputerMutationPrecondition: values.Precondition, DesiredState: desired})
		if err == nil && computer.DesiredState != desired {
			err = computerServiceDesiredDecision(ctx, tx, computer.CurrentJob, desired)
		}
	case "restart":
		_, err = computerRestartDecision(ctx, tx, computer, ComputerRestartRequest{ComputerMutationPrecondition: values.Precondition})
	case "remove":
		if computer.DesiredState != contract.ServiceDesiredRemoved {
			err = computerRemoveDecision(ctx, tx, computer, ComputerRemoveRequest{ComputerMutationPrecondition: values.Precondition})
		}
	case "backup-cap":
		err = computerBackupCapDecision(ctx, tx, computer, ComputerBackupCapRequest{ComputerMutationPrecondition: values.Precondition})
	case "reset":
		err = computerResetDecision(ctx, tx, computer, ComputerStorageResetRequest{ComputerMutationPrecondition: values.Precondition, TerminateSessions: values.TerminateSessions})
		if err == nil {
			nodeID := computer.BoundNodeID
			if nodeID == "" {
				nodeID = computer.PlacementNodeID
			}
			_, err = computerResetCapacityDecision(ctx, tx, computer, nodeID)
		}
	case "resize":
		_, _, err = computerGrowDecision(ctx, tx, computer, ComputerGrowRequest{ComputerMutationPrecondition: values.Precondition, DiskBytes: values.DiskBytes})
	case "backup":
		_, _, err = computerBackupDecision(ctx, tx, computer, ComputerBackupCreateRequest{ComputerMutationPrecondition: values.Precondition, AllowPowerOff: values.AllowPowerOff})
		if err == nil {
			err = computerServiceDesiredDecision(ctx, tx, computer.CurrentJob, contract.ServiceDesiredStopped)
		}
	case "restore":
		_, _, err = computerRestoreDecision(ctx, tx, computer, ComputerRestoreRequest{ComputerMutationPrecondition: values.Precondition, BackupID: values.BackupID, KeepOldBackup: values.KeepOldBackup})
	case "clone":
		var copy BackupCopy
		_, copy, err = tx.availableBackup(ctx, values.BackupID)
		if err == nil {
			err = computerCloneDecision(ctx, tx, computer, copy, ComputerCloneRequest{ComputerMutationPrecondition: values.Precondition})
		}
	case "prune":
		_, err = computerPruneDecision(ctx, tx, computer, ComputerBackupPruneRequest{ComputerMutationPrecondition: values.Precondition, BackupID: values.BackupID})
	case "custody-export":
		_, _, err = computerExportDecision(ctx, tx, computer, ComputerCustodyExportRequest{ComputerMutationPrecondition: values.Precondition, BackupID: values.BackupID})
	case "reconfiguration-abort":
		_, err = computerAbortDecision(ctx, tx, computer, ComputerReconfigurationAbortRequest{ComputerMutationPrecondition: values.Precondition})
	case "reimage", "projections":
		operation := ComputerIntentProject
		if verb == "reimage" {
			operation = ComputerIntentReimage
			err = computerReimageSessionsDecision(computer, values.TerminateSessions)
		}
		if err == nil {
			err = computerProjectionDecision(ctx, tx, computer, ComputerProjectionRequest{ComputerMutationPrecondition: values.Precondition, Spec: values.Spec}, operation)
		}
		if err == nil {
			err = computerProjectionReadinessDecision(ctx, tx, computer, operation)
		}
	default:
		err = protocolError(contract.ErrorInvalidRequest, "unknown Computer action %q", verb)
	}
	return err
}

var computerOperatorVerbs = []string{"start", "stop", "restart", "remove", "reimage", "reset", "resize", "backup", "backup-cap", "restore", "clone", "prune", "custody-export", "projections", "reconfiguration-abort"}

func computerAllowedActions(ctx context.Context, tx readModel, computer Computer, actor computerActionActor) []contract.AllowedAction {
	actions := make([]contract.AllowedAction, 0, len(computerOperatorVerbs))
	authorizationErr := taggedIdentityDecision(actor.Identity, actor.ClientPrincipalTag)
	for _, verb := range computerOperatorVerbs {
		var inputDecisionErr error
		values := computerActionValues{Precondition: ComputerMutationPrecondition{IntentRevision: computer.IntentRevision, StorageID: computer.StorageID, StorageGeneration: computer.StorageGeneration}, DiskBytes: computer.DesiredDiskBytes + 1, TerminateSessions: true, AllowPowerOff: true, Spec: computer.CurrentJob.Spec}
		// A projection is a caller's new specification, never inherited run identity.
		values.Spec.Labels = nil
		action := contract.AllowedAction{Verb: verb, Requires: map[string]any{"intent_revision": computer.IntentRevision, "storage_id": computer.StorageID, "storage_generation": computer.StorageGeneration}}
		input := func(name, kind string, required bool) {
			action.Inputs = append(action.Inputs, contract.ActionInput{Name: name, Type: kind, Required: required})
		}
		key := func() { input("idempotency_key", "string", true) }
		switch verb {
		case "start":
			action.Requires["desired_state"] = contract.ServiceDesiredRunning
		case "stop":
			action.Requires["desired_state"] = contract.ServiceDesiredStopped
		case "restart", "reconfiguration-abort":
			key()
		case "backup-cap":
			input("backup_cap", "integer", true)
		case "resize":
			key()
			input("disk_bytes", "integer", true)
		case "projections":
			input("spec", "object", true)
		case "reimage", "reset":
			key()
			if computerRuntimeActive(computer) {
				action.Requires["terminate_sessions"] = true
			} else {
				input("terminate_sessions", "boolean", false)
			}
			if verb == "reimage" {
				input("image", "object", true)
				input("chown", "boolean", false)
			}
		case "backup":
			key()
			if computer.DesiredState == contract.ServiceDesiredRunning {
				action.Requires["allow_power_off"] = true
			} else {
				input("allow_power_off", "boolean", true)
			}
		case "restore", "clone", "prune", "custody-export":
			key()
			action.Inputs = append(action.Inputs, contract.ActionInput{Name: "backup_id", Type: "string", Required: true, In: "path"})
			switch verb {
			case "restore":
				var retentionErr error
				if authorizationErr == nil {
					retentionErr = computerRestoreRetentionDecision(ctx, tx, computer)
				}
				if retentionErr != nil && errorCode(retentionErr) == contract.ErrorConflict {
					action.Requires["keep_old_as_backup"] = false
				} else {
					input("keep_old_as_backup", "boolean", true)
					inputDecisionErr = retentionErr
				}
			case "clone":
				input("name", "string", true)
				input("disk_bytes", "integer", true)
			case "custody-export":
				input("external_path", "string", true)
			}
		}
		decision := func() error { return computerActionDecision(ctx, tx, computer, verb, actor, values) }
		switch {
		case authorizationErr != nil:
			action.RefusedBecause = apiErrorFromDecision(authorizationErr)
		case inputDecisionErr != nil:
			action.RefusedBecause = apiErrorFromDecision(inputDecisionErr)
		case verb == "restore" || verb == "clone" || verb == "prune" || verb == "custody-export":
			// A path choice is valid only for this Computer and this operation. Allowed
			// means at least one such choice exists; it does not bless arbitrary IDs.
			action.RefusedBecause = apiErrorFromDecision(computerBackupChoiceWithReads(ctx, tx, computer, &values, decision))
		default:
			action.RefusedBecause = apiErrorFromDecision(decision())
		}
		actions = append(actions, action)
	}
	return actions
}

func computerBackupChoiceWithReads(ctx context.Context, tx readModel, computer Computer, values *computerActionValues, decision func() error) error {
	ids, err := tx.backupChoices(ctx, computer.ComputerID)
	if err != nil {
		return err
	}
	// With no Backup, still run the actor and resource checks before the absent
	// choice refusal, so an agent-only caller sees its own authorization failure.
	if len(ids) == 0 {
		return decision()
	}
	var first, internal error
	for _, id := range ids {
		values.BackupID = id
		err := decision()
		if err == nil {
			return nil
		}
		if errorCode(err) == contract.ErrorInternal {
			// A corrupt choice cannot authorize this action, but another choice
			// can. Preserve the internal refusal if no valid choice succeeds.
			if internal == nil {
				internal = err
			}
		}
		if first == nil {
			first = err
		}
	}
	if internal != nil {
		return internal
	}
	return first
}

// One projection for every route returning a Computer, including nested agent
// acknowledgement responses. No read grants authority or changes intent.
func (s *Server) projectComputerForCaller(r *http.Request, computer Computer) (Computer, error) {
	return s.projectComputerForCallerWithSelection(r, computer, nil)
}

// Named-operation selectors and the returned Computer share a single door.
// Selection may capture immutable header fields; it must use these reads.
func (s *Server) projectComputerForCallerWithSelection(r *http.Request, computer Computer, selectOperation func(context.Context, readModel) error) (Computer, error) {
	var projection Computer
	err := s.store.withReadSnapshot(r.Context(), nil, func(ctx context.Context, reads readModel) error {
		if selectOperation != nil {
			if err := selectOperation(ctx, reads); err != nil {
				return err
			}
		}
		var err error
		projection, err = projectComputerResponse(ctx, reads, computer, computerActionActor{Identity: identityFromRequest(r), ClientPrincipalTag: s.clientPrincipalTag})
		return err
	})
	return projection, err
}

// Reload authority and named operation observations after a commit. The write's
// row is only a selector, never combined with facts from a later moment.
func projectComputerResponse(ctx context.Context, reads readModel, selected Computer, actor computerActionActor) (Computer, error) {
	// Failed provisional imports retain their existing empty acknowledgement;
	// there is no surviving authority to reload or mix with later observations.
	if selected.ComputerID == "" {
		return projectComputerForCallerTx(ctx, reads, Computer{}, actor)
	}
	computer, err := reads.computerViewGetComputer(ctx, selected.ComputerID)
	if err != nil {
		return Computer{}, err
	}
	if selected.CloneOperation != nil {
		op, err := reads.computerViewComputerCloneOperation(ctx, computer.ComputerID, selected.CloneOperation.OperationRevision)
		if err != nil {
			return Computer{}, err
		}
		computer.CloneOperation = &op
	}
	if selected.RestoreOperation != nil {
		op, err := reads.computerViewComputerRestoreOperation(ctx, computer.ComputerID, selected.RestoreOperation.OperationRevision)
		if err != nil {
			return Computer{}, err
		}
		computer.RestoreOperation = &op
	}
	return projectComputerForCallerTx(ctx, reads, computer, actor)
}

func projectComputerForCallerTx(ctx context.Context, tx readModel, computer Computer, actor computerActionActor) (Computer, error) {
	computer.AllowedActions = computerAllowedActions(ctx, tx, computer, actor)
	var err error
	computer.LastCondition, err = computerLastCondition(ctx, tx, computer)
	if err != nil {
		return Computer{}, err
	}
	return redactComputer(computer), nil
}

func (s *Server) writeComputerForCaller(w http.ResponseWriter, r *http.Request, status int, computer Computer) {
	projection, err := s.projectComputerForCaller(r, computer)
	if err != nil {
		writeError(w, appliedComputerReadError(err, computer.ComputerID))
		return
	}
	writeJSON(w, status, projection)
}

func computerLastCondition(ctx context.Context, tx readModel, computer Computer) (*contract.Condition, error) {
	var condition *contract.Condition
	record := func(code, scope string, since time.Time, details map[string]any) {
		if since.IsZero() {
			return
		}
		if condition == nil || !since.Before(condition.Since) {
			condition = &contract.Condition{Code: code, Scope: scope, Since: since, Details: details}
		}
	}
	intents, err := tx.intents(ctx, computer.ComputerID, computer.IntentRevision-1, 1)
	if err != nil {
		return nil, internalError(err, "read Computer last intent condition")
	}
	if len(intents) != 0 {
		intent := intents[0]
		record("computer_intent_"+string(intent.Operation), "computer.intent", intent.CreatedAt, map[string]any{"intent_revision": intent.IntentRevision, "desired_state": intent.DesiredState, "storage_id": intent.StorageID, "storage_generation": intent.StorageGeneration})
	}
	if grow := computer.LastGrowOperation; grow != nil && grow.CompletedAt != nil {
		code := "computer_storage_grow_" + grow.Status
		if grow.FailureCode != "" {
			code = grow.FailureCode
		}
		details := map[string]any{"operation_revision": grow.OperationRevision, "requested_bytes": grow.RequestedBytes}
		if grow.ObservedAvailableBytes != nil {
			details["observed_available_bytes"] = *grow.ObservedAvailableBytes
		}
		record(code, "computer.storage", *grow.CompletedAt, details)
	}
	if backup := computer.LastBackupOperation; backup != nil && backup.CompletedAt != nil {
		code := "computer_backup_" + backup.Status
		if backup.FailureCode != "" {
			code = string(backup.FailureCode)
		}
		record(code, "computer.backup", *backup.CompletedAt, map[string]any{"operation_revision": backup.OperationRevision, "backup_id": backup.BackupID})
	}
	if removal := computer.CurrentJob.Removal; removal != nil && removal.StalledAt != nil && removal.Stall != nil {
		record(removal.Stall.LastRefusalCode, "computer.removal", *removal.StalledAt, map[string]any{"attempts": removal.Stall.Attempts, "phase": removal.Stall.Phase, "holds_slot": false})
	}
	return condition, nil
}

func (s *Server) getComputerForCaller(r *http.Request, cloneRevision, restoreRevision int64) (Computer, error) {
	selected := Computer{ComputerID: r.PathValue("computer_id")}
	if cloneRevision > 0 {
		selected.CloneOperation = &ComputerCloneOperation{OperationRevision: cloneRevision}
	}
	if restoreRevision > 0 {
		selected.RestoreOperation = &ComputerRestoreOperation{OperationRevision: restoreRevision}
	}
	return s.projectComputerForCaller(r, selected)
}

// HTTP mutations carry the actual authenticated identity and deployment tag
// into the transaction. The public Store methods remain trusted local seams,
// with their original request validation.
type computerActionActorContextKey struct{}

func computerActorPreconditionDecision(computer Computer, actor computerActionActor, request ComputerMutationPrecondition) error {
	if err := taggedIdentityDecision(actor.Identity, actor.ClientPrincipalTag); err != nil {
		return err
	}
	request.Actor = actor.Identity.NodeID
	return validateComputerPrecondition(computer, request)
}

func computerWritePreconditionDecision(ctx context.Context, computer Computer, request ComputerMutationPrecondition) error {
	actor, authenticated := ctx.Value(computerActionActorContextKey{}).(computerActionActor)
	if !authenticated {
		return validateComputerPrecondition(computer, request)
	}
	return computerActorPreconditionDecision(computer, actor, request)
}

func (r *databaseReads) backupChoices(ctx context.Context, computerID string) ([]string, error) {
	rows, err := r.q.QueryContext(ctx, `SELECT backup_id FROM backups WHERE computer_id=? AND status='available' ORDER BY backup_id`, computerID)
	if err != nil {
		return nil, internalError(err, "list Computer action Backup choices")
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, internalError(err, "read Computer action Backup choice")
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, internalError(err, "iterate Computer action Backup choices")
	}
	return ids, nil
}

// A failed post-commit observation must not invite replay as an unapplied write.
func appliedComputerReadError(err error, id string) error {
	details := map[string]any{"reason": "read_snapshot_post_change_failed", "mutation_applied": true, "computer_id": id, "read_reason": string(errorCode(err))}
	retryable := false
	if api := apiErrorFromDecision(err); api != nil && api.Code == contract.ErrorUnavailable {
		if reason, ok := api.Details["reason"].(string); ok && (reason == "read_snapshot_admission_expired" || reason == "read_snapshot_expired") {
			details["read_reason"] = reason
			retryable = true
		}
	}
	return &Error{Code: contract.ErrorUnavailable, Message: "Computer change committed but its view is unavailable", Details: details, notRetryable: !retryable}
}
