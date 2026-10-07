package l1

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

type computerOperatorFacts struct {
	ComputerID     string              `json:"computer_id"`
	IntentRevision int64               `json:"intent_revision"`
	LastCondition  *contract.Condition `json:"last_condition"`
	AllowedActions []struct {
		Verb     string         `json:"verb"`
		Requires map[string]any `json:"requires"`
		Inputs   []struct {
			Name     string `json:"name"`
			Type     string `json:"type"`
			Required bool   `json:"required"`
			In       string `json:"in"`
		} `json:"inputs"`
		RefusedBecause *contract.APIError `json:"refused_because"`
	} `json:"allowed_actions"`
}

// The independent route vocabulary makes missing verbs fail, rather than
// iterating only what the implementation happens to publish.
var testedComputerVerbs = []string{"start", "stop", "restart", "remove", "reimage", "reset", "resize", "backup", "backup-cap", "restore", "clone", "prune", "custody-export", "projections", "reconfiguration-abort"}

func computerActionHTTP(computer Computer, verb, backupID string) (string, string, map[string]any) {
	body := map[string]any{"intent_revision": computer.IntentRevision, "storage_id": computer.StorageID, "storage_generation": computer.StorageGeneration, "idempotency_key": "action-" + verb}
	method, path := http.MethodPost, "/v1/computers/"+computer.ComputerID
	switch verb {
	case "start", "stop":
		delete(body, "idempotency_key")
		method, path = http.MethodPut, path+"/desired-state"
		body["desired_state"] = "running"
		if verb == "stop" {
			body["desired_state"] = "stopped"
		}
	case "backup-cap":
		delete(body, "idempotency_key")
		method, path = http.MethodPut, path+"/backup-cap"
		body["backup_cap"] = 4
	case "remove":
		delete(body, "idempotency_key")
		path += "/remove"
	case "restart", "reconfiguration-abort":
		path += "/" + verb
	case "reimage":
		path += "/reimage"
		body["image"] = reimageTarget('e')
		body["terminate_sessions"] = true
	case "reset":
		path += "/storage-reset"
		body["terminate_sessions"] = true
	case "resize":
		path += "/grow"
		body["disk_bytes"] = computer.DesiredDiskBytes + 4096
	case "backup":
		path += "/backups"
		body["allow_power_off"] = true
	case "restore":
		path += "/backups/" + backupID + "/restore"
		body["keep_old_as_backup"] = false
	case "clone":
		path += "/backups/" + backupID + "/clone"
		body["name"] = "new-clone"
		body["disk_bytes"] = computer.DesiredDiskBytes
	case "prune":
		path += "/backups/" + backupID + "/prune"
	case "custody-export":
		path += "/backups/" + backupID + "/export"
		body["external_path"] = "/external/computer-copy"
	case "projections":
		delete(body, "idempotency_key")
		path += "/projections"
		spec := computer.CurrentJob.Spec
		spec.DispatchKey = "new-operator-projection"
		body["spec"] = spec
	}
	return method, path, body
}

func TestComputerAllowedActionsMatchEveryVerbHTTP(t *testing.T) {
	states := []string{"stopped", "queued", "claimed", "running", "stopping", "failed", "resource-latch", "projecting", "resetting", "backing_up", "restoring", "cloning", "exporting", "importing", "reimaging", "growing", "removing", "removed", "capacity-full", "retired-storage", "root-missing", "backup-cap-zero", "backup-pruned", "backup-corrupt", "dead-growing"}
	for _, state := range states {
		for _, verb := range testedComputerVerbs {
			t.Run(state+"/"+verb, func(t *testing.T) {
				h, node, computer, backup, _ := publishedBackupForStorageCopy(t, 4)
				h.server.computerTokenRevoker = recordingComputerTokenRevoker{revoke: func(_ context.Context, request ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error) {
					return contract.ComputerTokenRevocationReceipt{ComputerID: request.ComputerID, ComputerAttemptID: request.ComputerAttemptID, SubmitIntentRevision: request.NewSubmitIntentRevision, CommittedAt: h.clock.Now()}, nil
				}}
				exec := func(query string, args ...any) {
					t.Helper()
					if _, err := h.store.db.Exec(query, args...); err != nil {
						t.Fatal(err)
					}
				}
				switch state {
				case "queued", "claimed", "running", "stopping", "failed", "resource-latch":
					jobState, desired := state, "running"
					if state == "stopping" {
						desired = "stopped"
					}
					if state == "resource-latch" {
						jobState = "running"
					}
					exec(`UPDATE jobs SET state=? WHERE job_id=?`, jobState, computer.CurrentJobID)
					exec(`UPDATE computers SET desired_state=? WHERE computer_id=?`, desired, computer.ComputerID)
					exec(`UPDATE service_jobs SET desired_state=? WHERE job_id=?`, desired, computer.CurrentJobID)
					if state == "resource-latch" {
						exec(`UPDATE service_jobs SET last_failure=? WHERE job_id=?`, `{"code":"insufficient_disk","message":"allocation failed"}`, computer.CurrentJobID)
					}
				case "projecting":
					exec(`UPDATE jobs SET state='running' WHERE job_id=?`, computer.CurrentJobID)
					exec(`UPDATE computers SET desired_state='running' WHERE computer_id=?`, computer.ComputerID)
					exec(`UPDATE service_jobs SET desired_state='running' WHERE job_id=?`, computer.CurrentJobID)
					spec := computer.CurrentJob.Spec
					spec.DispatchKey = "staged-projection"
					if _, err := h.store.InstallComputerProjection(t.Context(), computer.ComputerID, ComputerProjectionRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), Spec: spec}); err != nil {
						t.Fatal(err)
					}
				case "reimaging":
					if _, err := h.store.ReimageComputer(t.Context(), computer.ComputerID, ComputerReimageRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), Image: reimageTarget('b'), IdempotencyKey: "staged-reimage"}); err != nil {
						t.Fatal(err)
					}
				case "resetting", "backing_up", "restoring", "cloning", "exporting", "importing", "growing", "removing", "dead-growing":
					phase := state
					if state == "dead-growing" {
						phase = "growing"
						exec(`UPDATE nodes SET state='dead' WHERE node_id=?`, node.NodeID)
					}
					exec(`UPDATE computers SET reconfiguration_phase=?, reconfiguration_revision=intent_revision WHERE computer_id=?`, phase, computer.ComputerID)
				case "removed":
					exec(`UPDATE computers SET desired_state='removed', reconfiguration_phase='removing' WHERE computer_id=?`, computer.ComputerID)
				case "capacity-full":
					exec(`UPDATE nodes SET max_service_slots=0 WHERE node_id=?`, node.NodeID)
				case "retired-storage":
					exec(`UPDATE computer_storage_generations SET phase='retired' WHERE computer_id=?`, computer.ComputerID)
				case "root-missing":
					exec(`UPDATE nodes SET root_instance_id='' WHERE node_id=?`, node.NodeID)
				case "backup-cap-zero":
					exec(`UPDATE computers SET backup_cap=0 WHERE computer_id=?`, computer.ComputerID)
				case "backup-pruned":
					exec(`UPDATE backups SET status='pruned' WHERE backup_id=?`, backup.BackupID)
					exec(`DELETE FROM backup_copies WHERE backup_id=?`, backup.BackupID)
				case "backup-corrupt":
					exec(`UPDATE backup_copies SET content_digest='invalid' WHERE backup_id=?`, backup.BackupID)
				}
				var err error
				computer, err = h.store.GetComputer(t.Context(), computer.ComputerID)
				if err != nil {
					t.Fatal(err)
				}
				caller := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
				status, _, wire := h.do(caller, http.MethodGet, "/v1/computers/"+computer.ComputerID, nil)
				if status != http.StatusOK {
					t.Fatalf("detail %d: %s", status, wire)
				}
				var facts computerOperatorFacts
				if err := json.Unmarshal(wire, &facts); err != nil {
					t.Fatal(err)
				}
				if len(facts.AllowedActions) != len(testedComputerVerbs) || facts.LastCondition == nil {
					t.Fatalf("missing Computer facts: %s", wire)
				}
				status, _, listWire := h.do(caller, http.MethodGet, "/v1/computers?limit=1", nil)
				var page struct {
					Computers []computerOperatorFacts `json:"computers"`
				}
				if status != http.StatusOK || json.Unmarshal(listWire, &page) != nil || len(page.Computers) != 1 || !reflect.DeepEqual(page.Computers[0], facts) {
					t.Fatalf("list/detail disagree: %s / %s", listWire, wire)
				}
				found := false
				for _, action := range facts.AllowedActions {
					if action.Verb != verb {
						continue
					}
					found = true
					method, path, body := computerActionHTTP(computer, verb, backup.BackupID)
					for field, value := range action.Requires {
						if field == "revision" || value == nil {
							t.Fatalf("nonliteral or null requirement: %#v", action)
						}
						body[field] = value
					}
					if action.Requires["intent_revision"] != float64(computer.IntentRevision) || action.Requires["storage_id"] != computer.StorageID || action.Requires["storage_generation"] != float64(computer.StorageGeneration) {
						t.Fatalf("incorrect CAS facts: %#v", action)
					}
					for _, input := range action.Inputs {
						if _, overlap := action.Requires[input.Name]; overlap {
							t.Fatalf("input/require overlap: %#v", action)
						}
						if input.Name == "backup_id" && input.In != "path" {
							t.Fatalf("Backup choice missing path location: %#v", input)
						}
					}
					status, _, result := h.do(caller, method, path, body)
					accepted := status >= 200 && status < 300
					if accepted && verb != "prune" && verb != "custody-export" {
						var returned computerOperatorFacts
						if json.Unmarshal(result, &returned) != nil || len(returned.AllowedActions) != len(testedComputerVerbs) {
							t.Fatalf("mutation omitted action projection: %s", result)
						}
					}
					if accepted != (action.RefusedBecause == nil) {
						t.Fatalf("advertised=%#v accepted=%t HTTP=%d %s", action, accepted, status, result)
					}
					if !accepted {
						var refusal contract.ErrorResponse
						if json.Unmarshal(result, &refusal) != nil || refusal.Error.Code != action.RefusedBecause.Code || refusal.Error.Retryable != action.RefusedBecause.Retryable {
							t.Fatalf("listed refusal %#v differs from HTTP %s", action.RefusedBecause, result)
						}
					}
				}
				if !found {
					t.Fatalf("missing verb %s", verb)
				}
			})
		}
	}
}

func TestComputerActionsUseActualCallerAndOmitPersonVerbs(t *testing.T) {
	for _, tag := range []string{DefaultClientPrincipalTag, "custom-client"} {
		for _, identity := range []fabric.Identity{
			{NodeID: "operator", Tags: []string{tag}},
			{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}},
			{NodeID: "dual", Tags: []string{tag, DefaultAgentPrincipalTag}},
			{NodeID: "untagged"},
			{NodeID: "person", FabricID: "fabric", UserID: "user", DeviceID: "device"},
		} {
			t.Run(tag+"/"+identity.NodeID, func(t *testing.T) {
				h, _, computer, backup, _ := publishedBackupForStorageCopy(t, 4)
				h.server.clientPrincipalTag = tag
				r := httptest.NewRequest(http.MethodGet, "/v1/computers/"+computer.ComputerID, nil)
				r.SetPathValue("computer_id", computer.ComputerID)
				r = r.WithContext(context.WithValue(r.Context(), identityContextKey{}, identity))
				w := httptest.NewRecorder()
				h.server.getComputer(w, r)
				var facts computerOperatorFacts
				if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &facts) != nil || len(facts.AllowedActions) != len(testedComputerVerbs) {
					t.Fatalf("projection: %d %s", w.Code, w.Body.String())
				}
				caller := h.client(identity)
				for _, action := range facts.AllowedActions {
					if strings.Contains(action.Verb, "grant") || strings.Contains(action.Verb, "takeover") || strings.Contains(action.Verb, "submission") {
						t.Fatalf("person verb on client Computer: %s", action.Verb)
					}
					method, path, body := computerActionHTTP(computer, action.Verb, backup.BackupID)
					if identity.NodeID == "operator" || identity.NodeID == "dual" {
						// Do not mutate the fixture: independent state/action HTTP cases above
						// establish accept parity; here verify actual caller authority.
						if action.RefusedBecause != nil && action.RefusedBecause.Code == contract.ErrorUnauthorized {
							t.Fatalf("authorized caller refused: %#v", action)
						}
						continue
					}
					status, _, response := h.do(caller, method, path, body)
					var refused contract.ErrorResponse
					if status != http.StatusForbidden || json.Unmarshal(response, &refused) != nil || action.RefusedBecause == nil || action.RefusedBecause.Code != refused.Error.Code || action.RefusedBecause.Retryable != refused.Error.Retryable {
						t.Fatalf("caller/action parity: %#v HTTP=%d %s", action, status, response)
					}
				}
			})
		}
	}
}

func TestComputerActionsConsentAndConditionEvidence(t *testing.T) {
	h, node, computer := backupHarness(t, 2, nil)
	computer, _ = startBackupComputer(t, h, node, computer)
	caller := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
	status, _, body := h.do(caller, http.MethodGet, "/v1/computers/"+computer.ComputerID, nil)
	var facts computerOperatorFacts
	if status != http.StatusOK || json.Unmarshal(body, &facts) != nil || facts.LastCondition == nil {
		t.Fatalf("facts: %d %s", status, body)
	}
	for _, verb := range []string{"reset", "reimage", "backup"} {
		for _, action := range facts.AllowedActions {
			if action.Verb != verb {
				continue
			}
			field := "terminate_sessions"
			if verb == "backup" {
				field = "allow_power_off"
			}
			if action.Requires[field] != true {
				t.Fatalf("missing mandatory consent: %#v", action)
			}
			method, path, request := computerActionHTTP(computer, verb, "")
			request[field] = false
			status, _, response := h.do(caller, method, path, request)
			assertAPIError(t, status, response, http.StatusConflict, contract.ErrorConflict)
		}
	}
	before, _ := json.Marshal(facts.LastCondition)
	h.clock.Advance(time.Minute)
	status, _, body = h.do(caller, http.MethodGet, "/v1/computers/"+computer.ComputerID, nil)
	if status != http.StatusOK || json.Unmarshal(body, &facts) != nil {
		t.Fatalf("repeat read %d %s", status, body)
	}
	after, _ := json.Marshal(facts.LastCondition)
	if !bytes.Equal(before, after) {
		t.Fatalf("read advanced condition: %s -> %s", before, after)
	}
	if facts.LastCondition.Scope != "computer.intent" || facts.LastCondition.Code != "computer_intent_create" {
		t.Fatalf("condition not recorded intent: %#v", facts.LastCondition)
	}
}

func TestComputerReadSnapshotDoesNotTakeWriteLock(t *testing.T) {
	h, _, computer := backupHarness(t, 0, nil)
	// Finish server startup recovery before acquiring the writer lock.
	caller := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
	status, _, body := h.do(caller, http.MethodGet, "/v1/computers", nil)
	if status != http.StatusOK {
		t.Fatalf("warm read: %d %s", status, body)
	}
	// The writer transaction and the read use separate pooled connections.
	tx, err := h.store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE computers SET backup_cap=1 WHERE computer_id=?`, computer.ComputerID); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/v1/computers", "/v1/computers/" + computer.ComputerID} {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://control-plane.invalid"+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := caller.Do(req)
		cancel()
		if err != nil {
			t.Fatalf("read while writer holds immediate lock: %s: %v", path, err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("read while writer holds lock: %s status=%d", path, response.StatusCode)
		}
	}
}

func TestComputerListingQueryPlanSeeksPagingIndex(t *testing.T) {
	h, _, _ := backupHarness(t, 0, nil)
	tx, err := h.store.db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	source, err := os.ReadFile("computers.go")
	if err != nil {
		t.Fatal(err)
	}
	// Explain the production statement, including the original OR predicate in
	// the red check, rather than independently spelling a better query here.
	listing := string(source)[strings.Index(string(source), "func (s *Store) ListComputers("):]
	marker := "rows, err := tx.QueryContext(ctx, `"
	start := strings.Index(listing, marker)
	if start < 0 {
		t.Fatal("Computer listing query was not found")
	}
	query := strings.SplitN(listing[start+len(marker):], "`", 2)[0]
	args := []any{1, "computer", 10}
	if strings.Count(query, "?") == 4 {
		args = []any{1, 1, "computer", 10}
	}
	rows, err := tx.Query("EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	plan := ""
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan += detail + "\n"
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "SEARCH computers USING COVERING INDEX computers_created_id") || strings.Contains(plan, "TEMP B-TREE") {
		t.Fatalf("Computer page is not an indexed seek: %s", plan)
	}
	t.Log(plan)
}

func TestComputerActionProjectionNestedAgentAcknowledgement(t *testing.T) {
	h, node, computer := backupHarness(t, 2, nil)
	computer, claim := startBackupComputer(t, h, node, computer)
	if _, _, err := h.store.BeginComputerBackup(t.Context(), computer.ComputerID, ComputerBackupCreateRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "agent-facts", AllowPowerOff: true}); err != nil {
		t.Fatal(err)
	}
	finishBackupQuiescence(t, h, claim, "agent-facts-stop")
	directives, err := h.store.ListNodeComputerBackupDirectives(t.Context(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("directives=%#v err=%v", directives, err)
	}
	h.clock.Advance(time.Second)
	receipt := successfulBackupReceipt(directives[0])
	agent := h.client(fabric.Identity{NodeID: "fabric-computer-node", Tags: []string{DefaultAgentPrincipalTag}})
	request := ComputerBackupAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, IdempotencyKey: receipt.ReceiptID, Receipt: receipt}
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/computers/"+computer.ComputerID+"/backup-acknowledgement", request)
	var response struct {
		Computer computerOperatorFacts `json:"computer"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &response) != nil || len(response.Computer.AllowedActions) != len(testedComputerVerbs) {
		t.Fatalf("nested agent projection %d %s", status, body)
	}
	for _, action := range response.Computer.AllowedActions {
		if action.RefusedBecause == nil || action.RefusedBecause.Code != contract.ErrorPrincipalForbidden {
			t.Fatalf("agent granted client authority: %#v", action)
		}
	}
	condition := response.Computer.LastCondition
	if condition == nil || condition.Scope != "computer.backup" || !condition.Since.Equal(h.clock.Now()) || condition.Details["backup_id"] != directives[0].BackupID {
		t.Fatalf("missing recorded completion condition: %#v", condition)
	}
	// A replay is still projected for its agent, never saved client authority.
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/computers/"+computer.ComputerID+"/backup-acknowledgement", request)
	if status != http.StatusOK || json.Unmarshal(body, &response) != nil || !reflect.DeepEqual(response.Computer.LastCondition, condition) {
		t.Fatalf("replay changed condition: %d %s", status, body)
	}
}

func TestComputerActionsNoBackupAndUnknownErrorsFailClosed(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "no Backup", true: "unknown database error"}[corrupt], func(t *testing.T) {
			h, _, computer := backupHarness(t, 2, nil)
			caller := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
			// Let startup recovery finish before deliberately breaking a decision input.
			if status, _, body := h.do(caller, http.MethodGet, "/v1/computers", nil); status != http.StatusOK {
				t.Fatalf("warm read %d %s", status, body)
			}
			if corrupt {
				if _, err := h.store.db.Exec(`ALTER TABLE backups RENAME TO hidden_backups`); err != nil {
					t.Fatal(err)
				}
			}
			status, _, body := h.do(caller, http.MethodGet, "/v1/computers/"+computer.ComputerID, nil)
			var facts computerOperatorFacts
			if status != http.StatusOK || json.Unmarshal(body, &facts) != nil || len(facts.AllowedActions) != len(testedComputerVerbs) {
				t.Fatalf("facts %d %s", status, body)
			}
			for _, action := range facts.AllowedActions {
				if action.Verb != "restore" && action.Verb != "clone" && action.Verb != "prune" && action.Verb != "custody-export" {
					continue
				}
				if action.RefusedBecause == nil {
					t.Fatalf("allowed with absent decision input: %#v", action)
				}
				if corrupt && (action.RefusedBecause.Code != contract.ErrorInternal || !action.RefusedBecause.Retryable || action.RefusedBecause.Message != "internal server error") {
					t.Fatalf("unknown error not scrubbed and refused: %#v", action)
				}
			}
			if corrupt && (bytes.Contains(body, []byte("hidden_backups")) || bytes.Contains(body, []byte("no such table"))) {
				t.Fatalf("decision exposed internal cause: %s", body)
			}
		})
	}
}
