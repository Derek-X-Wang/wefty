package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

const generationMaintenanceRemove = "remove"

// Only these two callers accept a quiet catalog. Other maintenance gates keep
// their conservative refusal. Catalog and rolling receipt reads share the lock.
func checkGenerationMaintenance(host setupHost, paths setupPaths, kind string) (*agentinstall.Catalog, *agentinstall.Maintenance, error) {
	store := upgradeGenerationStore(host, paths, upgradeOperations(host))
	m, err := store.ReadMaintenance()
	if err != nil {
		return nil, nil, maintenanceError(err)
	}
	if m != nil {
		if m.Kind != kind {
			return nil, nil, maintenanceError(&agentinstall.MaintenanceRefusal{Message: "another maintenance operation is pending; resume setup --" + map[string]string{"remove": "remove", "convert": "role join --convert-plain"}[m.Kind]})
		}
		return &m.Catalog, m, nil
	}
	c, err := store.CheckMaintenance(func(c agentinstall.Catalog) error {
		r, err := readUpgradeRolling(paths)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if r.NodeID != c.NodeID {
			return &agentinstall.MaintenanceRefusal{Message: "rolling receipt belongs to another node; let rolling recovery complete with `wefty upgrade --rolling`"}
		}
		if r.Phase != "complete" && r.Phase != "rolled_back" || r.RecoveryState != "" && r.RecoveryState != "complete" && r.RecoveryState != "rolled_back" && r.RecoveryState != "recovered" {
			return &agentinstall.MaintenanceRefusal{Message: "interrupted or unresolved rolling operation " + r.OperationID + " at " + r.Phase + "; let the rolling recovery complete with `wefty upgrade --rolling`"}
		}
		return nil
	})
	if err != nil {
		var refusal *agentinstall.MaintenanceRefusal
		if !errors.As(err, &refusal) {
			return nil, nil, &maintenanceFault{Cause: err}
		}
	}
	return c, nil, maintenanceError(err)
}

// Catalog state is diagnostic here; it never authorizes legacy activation.
func refuseGenerationMaintenance(host setupHost, paths setupPaths, command string) error {
	store := upgradeGenerationStore(host, paths, upgradeOperations(host))
	err := agentinstall.RefuseMaintenance(store, command)
	var refusal *agentinstall.MaintenanceRefusal
	if !errors.As(err, &refusal) {
		return err
	}
	// Pending maintenance owns recovery even if a rolling receipt also remains.
	if pending := store.RefusePendingMaintenance(command); pending != nil {
		return maintenanceError(pending)
	}
	r, readErr := readUpgradeRolling(paths)
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		return readErr
	}
	if readErr == nil && r.Phase != "complete" && r.Phase != "rolled_back" && r.Phase != "reboot_reconciled" {
		return maintenanceError(&agentinstall.MaintenanceRefusal{Message: refusal.Message + "; unfinished operation " + r.OperationID + " at " + r.Phase})
	}
	return maintenanceError(err)
}

func maintenanceError(err error) error {
	var refusal *agentinstall.MaintenanceRefusal
	if errors.As(err, &refusal) {
		return &apiResponseError{APIError: contract.APIError{Code: contract.ErrorConflict, Message: refusal.Error()}}
	}
	return err
}
func generationMaintenanceStep(host setupHost, name string) error {
	if host.maintenanceStep != nil {
		return host.maintenanceStep(name)
	}
	return nil
}

func removeMaintenanceGeneration(ctx context.Context, host setupHost, paths setupPaths, c *agentinstall.Catalog, m *agentinstall.Maintenance, report *setupRemoveReport) (resultErr error) {
	if c == nil {
		return nil
	}
	op := upgradeOperations(host)
	store := upgradeGenerationStore(host, paths, op)
	var err error
	if m == nil {
		m, err = store.BeginMaintenance("remove", rand.Text(), *c)
		if err != nil {
			return maintenanceError(err)
		}
		if err = generationMaintenanceStep(host, "maintenance:intent"); err != nil {
			return err
		}
	}
	if err = store.PreserveGenerationData(m); err != nil {
		return &maintenanceFault{Cause: err}
	}
	if m.Cleaning {
		if _, e := os.Lstat(store.Root()); errors.Is(e, os.ErrNotExist) {
			if _, e = store.RetireMaintenance(ctx, m); e != nil {
				return maintenanceError(e)
			}
		}
	}
	if m.Retired {
		report.Changes = append(report.Changes, "retired generation catalog")
		report.Retained = append(report.Retained, fmt.Sprintf("left %s: generation audit receipts, empty directories and unknown files are preserved", store.Data+"/"+m.Archive()))
		report.Retained = append(report.Retained, "L1 admission remains closed; after a plain reinstall, explicitly enable claims when ready")
		return nil
	}
	profile, err := connprofile.Read(paths.profile)
	if err != nil {
		return err
	}
	if profile.Identities.Node != c.NodeID || profile.Fabric != "plain" {
		return maintenanceError(&agentinstall.MaintenanceRefusal{Message: "generation maintenance requires matching plain profile"})
	}
	primary := c.Generations[c.Primary]
	if primary.Process == nil {
		return maintenanceError(&agentinstall.MaintenanceRefusal{Message: "recorded primary lacks a process binding; repair with `wefty upgrade --rolling`"})
	}
	if m.Request != nil && m.Result == nil {
		rejected, e := store.DeEnrollmentRejected(*m.Request)
		if e != nil {
			return &maintenanceFault{Cause: e}
		}
		if rejected {
			if e = store.AbortDeEnrollment(*m.Request); e != nil {
				return &maintenanceFault{Cause: e}
			}
			// The primary recorded a definitive refusal before registration.
			// Restore precommit state, retaining newer operator intent.
			m.Request = nil
			m.Restoring = true
		}
	}
	// Before any L1 request is issued the primary stays intact. Ambiguous issued
	// de-enrollment is resolved from its exact durable intent, never rolled back.
	if m.Restoring {
		if err = store.SaveMaintenance(m); err != nil {
			return &maintenanceFault{Cause: err}
		}
		if err = restoreGenerationRemoval(ctx, op, store, paths, profile, m); err != nil {
			return &maintenanceFault{Cause: err}
		}
		fresh, _, err := checkGenerationMaintenance(host, paths, "remove")
		if err != nil {
			return err
		}
		return removeMaintenanceGeneration(ctx, host, paths, fresh, nil, report)
	}
	publicationUncertain := false
	defer func() {
		if resultErr == nil || publicationUncertain || m.Request != nil || len(m.Admission) == 0 {
			return
		}
		m.Restoring = true
		if err := store.SaveMaintenance(m); err != nil {
			resultErr = &maintenanceFault{Cause: errors.Join(resultErr, err)}
			return
		}
		if err := restoreGenerationRemoval(ctx, op, store, paths, profile, m); err != nil {
			resultErr = &maintenanceFault{Cause: errors.Join(resultErr, err)}
		}
	}()
	if m.Result == nil {
		var drain upgradeDrainReceipt
		if len(m.Admission) == 0 {
			node, e := op.readNode(ctx, profile)
			if e != nil {
				return e
			}
			if node.AgentGenerationID != primary.ID || node.BootSessionID != primary.Process.BootSessionID {
				return maintenanceError(&agentinstall.MaintenanceRefusal{Message: "primary boot changed before removal; retry rolling recovery"})
			}
			drain = upgradeDrainReceipt{Version: 1, Home: paths.home, NodeID: c.NodeID, Revision: node.IntentRevision + 1, PreviousReason: node.IntentReason, Reason: "wefty maintenance " + m.Operation, RestoreClaims: node.ClaimsEnabled, OldBoot: node.BootSessionID}
			for _, a := range node.ActiveAttempts {
				if a.Class == contract.JobClassService {
					drain.Services = append(drain.Services, a)
				}
			}
			m.Admission, _ = json.Marshal(drain)
			if err = store.SaveMaintenance(m); err != nil {
				return err
			}
			if err = generationMaintenanceStep(host, "maintenance:admission_intent"); err != nil {
				return err
			}
		} else if err = json.Unmarshal(m.Admission, &drain); err != nil {
			return err
		}
		if m.Request == nil {
			node, e := op.readNode(ctx, profile)
			if e != nil {
				return e
			}
			if node.IntentRevision == drain.Revision-1 && node.IntentReason == drain.PreviousReason {
				node, e = op.setClaims(ctx, profile, l1.NodeIntentRequest{IntentRevision: node.IntentRevision, ClaimsEnabled: false, Reason: drain.Reason})
				if e != nil {
					return e
				}
			}
			if node.IntentRevision != drain.Revision || node.IntentReason != drain.Reason || node.ClaimsEnabled {
				return maintenanceError(&agentinstall.MaintenanceRefusal{Message: "admission changed during removal; retain receipt and repair operator intent"})
			}
			if err = generationMaintenanceStep(host, "maintenance:admission_closed"); err != nil {
				return err
			}
			// Services use the existing positive reap/evidence barrier. One-shots are
			// allowed to finish; no retirement deadline or force path is introduced.
			proof := agent.UpgradeServiceQuiescence{OperationID: m.Operation, NodeID: c.NodeID, AgentGenerationID: primary.ID, BootSessionID: primary.Process.BootSessionID, IntentRevision: drain.Revision, IntentReason: drain.Reason, Services: slices.Clone(drain.Services)}
			drain.ServiceQuiescence = &proof
			m.Admission, _ = json.Marshal(drain)
			if err = store.SaveMaintenance(m); err != nil {
				return err
			}
			body, _ := json.Marshal(proof)
			answer, e := op.generationControl(ctx, store, agentinstall.ControlRequest{ID: m.Operation, NodeID: c.NodeID, GenerationID: primary.ID, BootSessionID: primary.Process.BootSessionID, Action: "quiesce_services", Payload: body})
			if e != nil {
				return e
			}
			var accepted agent.UpgradeServiceQuiescence
			if e = json.Unmarshal(answer, &accepted); e != nil {
				return e
			}
			if !reflect.DeepEqual(proof, accepted) {
				return errors.New("maintenance service barrier reply differs")
			}
			m.ServiceSettled = true
			if err = store.SaveMaintenance(m); err != nil {
				return err
			}
			node, e = op.readNode(ctx, profile)
			if e != nil {
				return e
			}
			inventoryBody, e := op.generationControl(ctx, store, agentinstall.ControlRequest{ID: m.Operation, NodeID: c.NodeID, GenerationID: primary.ID, BootSessionID: primary.Process.BootSessionID, Action: "generation_inventory"})
			var inventory l1.NodeGenerationInventory
			if e == nil {
				e = json.Unmarshal(inventoryBody, &inventory)
			}
			if e == nil && inventory.NodeID != c.NodeID {
				e = errors.New("maintenance inventory node differs")
			}
			if e != nil {
				return e
			}
			request := l1.DeEnrollNodeGenerationRequest{OperationID: m.Operation, PrimaryGenerationID: primary.ID, PrimaryBootSessionID: primary.Process.BootSessionID, AuthorityGeneration: node.AuthorityGeneration, CatalogRevision: inventory.CatalogRevision, IntentRevision: drain.Revision, IntentReason: drain.Reason}
			next := *m
			next.Request = &request
			if err = store.SaveMaintenance(&next); err != nil {
				// A failed atomic publication may have left either document.
				// Never use the candidate pointer as evidence of publication.
				saved, readErr := store.ReadMaintenance()
				if readErr != nil || saved == nil || saved.Operation != m.Operation || saved.Kind != m.Kind || saved.Device != m.Device || saved.Inode != m.Inode || saved.Catalog.Primary != m.Catalog.Primary {
					publicationUncertain = true
					return &maintenanceFault{Cause: errors.Join(err, readErr, errors.New("cannot reconcile request publication; retry removal"))}
				}
				if saved.Request != nil {
					publicationUncertain = true
					if !reflect.DeepEqual(saved.Request, next.Request) {
						return &maintenanceFault{Cause: errors.Join(err, errors.New("published maintenance request differs"))}
					}
				}
				*m = *saved
				return &maintenanceFault{Cause: err}
			}
			*m = next
			if err = generationMaintenanceStep(host, "maintenance:de_enrollment_intent"); err != nil {
				return err
			}
		}
		// A previous writer may have published the request but failed its
		// directory sync. Re-publish before any RPC depends on that intent.
		if err = store.SaveMaintenance(m); err != nil {
			return &maintenanceFault{Cause: err}
		}
		// The adapter retains its authoritative result independently of the CLI's
		// transport reply. After death, startup replays that exact attested request.
		saved, e := store.ReadDeEnrollment()
		if e != nil {
			return e
		}
		if saved != nil && saved.Result != nil {
			m.Result = saved.Result
		} else {
			state, e := store.Manager.State(ctx, primary.Label)
			if e != nil {
				return e
			}
			if saved != nil && !state.Running {
				if e = store.RestoreMaintenancePrimary(ctx, m); e != nil {
					return e
				}
				deadline := time.Now().Add(upgradeMutationBudget)
				for time.Now().Before(deadline) {
					saved, e = store.ReadDeEnrollment()
					if e != nil {
						return e
					}
					rejected, rejectErr := store.DeEnrollmentRejected(*m.Request)
					if rejectErr != nil {
						return rejectErr
					}
					if rejected {
						refreshed, refreshErr := store.ReadMaintenance()
						if refreshErr != nil {
							return refreshErr
						}
						if refreshed == nil {
							return errors.New("maintenance intent disappeared during rejection recovery")
						}
						*m = *refreshed
						m.Request = nil
						return maintenanceError(&agentinstall.MaintenanceRefusal{Message: "de-enrollment refused; restoring precommit maintenance"})
					}
					if saved != nil && saved.Result != nil {
						m.Result = saved.Result
						break
					}
					if e = op.wait(ctx, 100*time.Millisecond); e != nil {
						return e
					}
				}
				if m.Result == nil {
					return errors.New("de-enrollment recovery has not published its recorded result; retry removal")
				}
			}
			body, _ := json.Marshal(m.Request)
			var answer json.RawMessage
			if m.Result != nil {
				answer, e = json.Marshal(m.Result)
			} else {
				answer, e = op.generationControl(ctx, store, agentinstall.ControlRequest{ID: m.Operation, NodeID: c.NodeID, GenerationID: primary.ID, BootSessionID: primary.Process.BootSessionID, Action: "de_enroll_generation", Payload: body})
			}
			if e != nil {
				var refusal *apiResponseError
				if (errors.As(e, &refusal) && refusal.StatusCode >= 400 && refusal.StatusCode < 500) || errors.As(e, new(*generationOperationFault)) {
					attested, readErr := store.ReadDeEnrollment()
					node, nodeErr := op.readNode(ctx, profile)
					if readErr == nil && attested == nil && nodeErr == nil && node.AgentGenerationID == primary.ID && node.BootSessionID == primary.Process.BootSessionID {
						m.Request = nil
						if saveErr := store.SaveMaintenance(m); saveErr != nil {
							return errors.Join(e, saveErr)
						}
					}
				}
				return e
			}
			var in l1.NodeGenerationInventory
			if e = json.Unmarshal(answer, &in); e != nil {
				return e
			}
			if in.NodeID != c.NodeID || in.CatalogRevision != m.Request.CatalogRevision+1 {
				return errors.New("invalid de-enrollment reply identity")
			}
			found := false
			for _, g := range in.Generations {
				if g.Role != l1.NodeGenerationClosed || g.ClosedAt == nil {
					return errors.New("de-enrollment reply retains a live generation")
				}
				found = found || g.AgentGenerationID == primary.ID && g.BootSessionID == primary.Process.BootSessionID
			}
			if !found {
				return errors.New("de-enrollment reply omits recorded primary")
			}
			m.Result = &in
		}
		if err = store.SaveMaintenance(m); err != nil {
			return err
		}
		if err = generationMaintenanceStep(host, "maintenance:de_enrolled"); err != nil {
			return err
		}
	}
	if err = store.StopMaintenancePrimary(ctx, m); err != nil {
		return maintenanceError(err)
	}
	if err = generationMaintenanceStep(host, "maintenance:unloaded"); err != nil {
		return err
	}
	retained, err := store.RetireMaintenance(ctx, m)
	if err != nil {
		return maintenanceError(err)
	}
	if err = generationMaintenanceStep(host, "maintenance:catalog_retired"); err != nil {
		return err
	}
	report.Changes = append(report.Changes, primary.Plist, primary.Executable, "retired generation catalog")
	for _, path := range retained {
		report.Retained = append(report.Retained, "left "+path+": generation audit receipts, empty directories and unknown files are preserved")
	}
	report.Retained = append(report.Retained, "left "+paths.data+": live SQLite databases and shared evidence/custody storage are preserved; L1 now accepts a fresh legacy registration")
	report.Retained = append(report.Retained, "L1 admission remains closed; after a plain reinstall, explicitly enable claims when ready")
	return nil
}

func conversionGenerationIntent(host setupHost, paths setupPaths, r *plainConversion) (*agentinstall.Maintenance, error) {
	store := upgradeGenerationStore(host, paths, upgradeOperations(host))
	m, err := store.ReadMaintenance()
	if err != nil {
		return nil, err
	}
	if m == nil {
		c, e := store.CheckMaintenance(nil)
		if e != nil {
			return nil, e
		}
		if c == nil || c.Primary != r.GenerationID {
			return nil, errors.New("conversion lost its recorded primary installation")
		}
		m, err = store.BeginMaintenance("convert", rand.Text(), *c)
	}
	if err == nil && (m.Kind != "convert" || m.Catalog.Primary != r.GenerationID) {
		return nil, errors.New("conversion generation intent differs")
	}
	return m, err
}
func retireConversionGeneration(ctx context.Context, host setupHost, paths setupPaths, r *plainConversion) ([]string, error) {
	if !conversionForward(r) {
		return nil, errors.New("generation cleanup requires durable read-only history")
	}
	store := upgradeGenerationStore(host, paths, upgradeOperations(host))
	m, err := conversionGenerationIntent(host, paths, r)
	if err != nil {
		return nil, err
	}
	if err = generationMaintenanceStep(host, "conversion:history_durable"); err != nil {
		return nil, err
	}
	retained, err := store.RetireMaintenance(ctx, m)
	if err != nil {
		return nil, maintenanceError(err)
	}
	if err = generationMaintenanceStep(host, "conversion:catalog_retired"); err != nil {
		return nil, err
	}
	var notes []string
	for _, path := range retained {
		notes = append(notes, "Retained generation audit evidence, empty directories and unknown files: "+path)
	}
	return notes, nil
}

type maintenanceFault struct{ Cause error }

func (e *maintenanceFault) Error() string {
	return "generation maintenance failed: " + e.Cause.Error()
}
func (e *maintenanceFault) Unwrap() error { return e.Cause }

// Recovery intent is durable before any rollback action. Each successful
// barrier/resume observation is durable before its dependent admission change.
func restoreGenerationRemoval(ctx context.Context, op upgradeHost, store agentinstall.Store, paths setupPaths, profile connprofile.Profile, m *agentinstall.Maintenance) error {
	var admission upgradeDrainReceipt
	if err := json.Unmarshal(m.Admission, &admission); err != nil {
		return err
	}
	recovery, cancel := context.WithTimeout(context.WithoutCancel(ctx), upgradeDrainRecoveryBudget)
	defer cancel()
	primary := m.Catalog.Generations[m.Catalog.Primary]
	control := func(action string, payload any) (json.RawMessage, error) {
		body, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		return op.generationControl(recovery, store, agentinstall.ControlRequest{ID: m.Operation, NodeID: m.Catalog.NodeID, GenerationID: primary.ID, BootSessionID: primary.Process.BootSessionID, Action: action, Payload: body})
	}
	// A registered replacement boot has no local gate belonging to the dead
	// session. Keep the old proof for fenced L1 service recovery, but never send
	// that dead session's control operation to its replacement.
	replaced := primary.Process.BootSessionID != admission.OldBoot
	if replaced {
		node, err := op.readNode(recovery, profile)
		if err != nil {
			return err
		}
		if node.AgentGenerationID != primary.ID || node.BootSessionID != primary.Process.BootSessionID {
			return &agentinstall.MaintenanceRefusal{Message: "replacement primary registration differs"}
		}
	}
	if proof := admission.ServiceQuiescence; proof != nil && !replaced {
		if !m.ServiceSettled {
			result, err := control("quiesce_services", proof)
			if err != nil {
				return err
			}
			var accepted agent.UpgradeServiceQuiescence
			if err = json.Unmarshal(result, &accepted); err != nil {
				return err
			}
			if !reflect.DeepEqual(*proof, accepted) {
				return errors.New("restored service barrier reply differs")
			}
			m.ServiceSettled = true
			if err = store.SaveMaintenance(m); err != nil {
				return err
			}
		}
		if !m.ServiceResumed {
			if _, err := control("resume_services", nil); err != nil {
				return err
			}
			m.ServiceResumed = true
			if err := store.SaveMaintenance(m); err != nil {
				return err
			}
		}
	}
	node, err := op.readNode(recovery, profile)
	if err != nil {
		return err
	}
	if err = resumeUpgradeServices(recovery, op, paths, profile, &admission, node, false, io.Discard); err != nil {
		return err
	}
	m.Admission, err = json.Marshal(admission)
	if err != nil {
		return err
	}
	if err = store.SaveMaintenance(m); err != nil {
		return err
	}
	if node.IntentRevision == admission.Revision && node.IntentReason == admission.Reason && !node.ClaimsEnabled {
		reason := admission.PreviousReason
		if strings.TrimSpace(reason) == "" {
			reason = "wefty maintenance aborted; previous admission restored"
		}
		if _, err = op.setClaims(recovery, profile, l1.NodeIntentRequest{IntentRevision: node.IntentRevision, ClaimsEnabled: admission.RestoreClaims, Reason: reason}); err != nil {
			return err
		}
	}
	confirmed, err := op.readNode(recovery, profile)
	if err != nil {
		return err
	}
	if confirmed.NodeID != m.Catalog.NodeID || confirmed.IntentRevision == admission.Revision && confirmed.IntentReason == admission.Reason && !confirmed.ClaimsEnabled {
		return errors.New("maintenance admission restoration is not confirmed")
	}
	if err = removeUpgradeDrain(paths.config); err != nil {
		return err
	}
	return store.FinishMaintenance()
}
