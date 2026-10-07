package l1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

type serviceOperatorWire struct {
	Actions   []contract.AllowedAction `json:"allowed_actions"`
	Condition *contract.Condition      `json:"last_condition"`
}

func serviceOperatorRead(t *testing.T, h *integrationHarness, client *http.Client, id string) serviceOperatorWire {
	t.Helper()
	status, _, body := h.do(client, http.MethodGet, "/v1/jobs/"+id+"?class=service", nil)
	if status != http.StatusOK {
		t.Fatalf("read=%d %s", status, body)
	}
	var facts serviceOperatorWire
	if err := json.Unmarshal(body, &facts); err != nil {
		t.Fatal(err)
	}
	if len(facts.Actions) != 5 {
		t.Fatalf("service actions=%s; want all five verbs", body)
	}
	return facts
}

func serviceOperatorWrite(t *testing.T, h *integrationHarness, client *http.Client, id, verb string) (int, []byte) {
	t.Helper()
	method, path, body := http.MethodPost, verb, any(nil)
	switch verb {
	case "start", "stop":
		method, path = http.MethodPut, "desired-state"
		desired := contract.ServiceDesiredRunning
		if verb == "stop" {
			desired = contract.ServiceDesiredStopped
		}
		body = ServiceDesiredStateRequest{DesiredState: desired}
	case "restart":
		body = ServiceRestartRequest{IdempotencyKey: "fresh-restart"}
	case "forget":
		body = ForceForgetRequest{Force: true}
	}
	status, _, response := h.do(client, method, serviceMutationPath(id, path), body)
	return status, response
}

func assertServiceActionParity(t *testing.T, action contract.AllowedAction, status int, body []byte) {
	t.Helper()
	if (status >= 200 && status < 300) != (action.RefusedBecause == nil) {
		t.Fatalf("action=%+v write=%d %s", action, status, body)
	}
	if action.RefusedBecause != nil {
		var response contract.ErrorResponse
		if err := json.Unmarshal(body, &response); err != nil {
			t.Fatal(err)
		}
		// request_id belongs to a write request, not a snapshot decision.
		response.Error.RequestID = ""
		if !reflect.DeepEqual(action.RefusedBecause, &response.Error) {
			t.Fatalf("advertised=%+v actual=%+v", action.RefusedBecause, response.Error)
		}
	}
}

func TestServiceAllowedActionsStateHTTP(t *testing.T) {
	cases := []string{"queued", "claimed", "running", "stopped", "stopping", "latch", "streak-latch", "never-interruption", "on-failure-policy", "never-clean-policy", "never-failed-policy", "capacity", "missing-root", "removal_pending", "agent_cleaned", "forgotten_cleanup_unverified", "stalled_cleanup_unverified", "removed_verified"}
	for _, state := range cases {
		for _, verb := range []string{"start", "stop", "restart", "remove", "forget"} {
			t.Run(state+"/"+verb, func(t *testing.T) {
				h, client, agent, node, job, claim := neverFixtureWith(t, "process", func(spec *contract.JobSpec) {
					if state == "on-failure-policy" {
						spec.Restart = contract.RestartOnFailure
					}
					if state == "streak-latch" {
						n := 1
						spec.MaxRestartStreak = &n
					}
				})
				now := h.clock.Now()
				exec := func(query string, args ...any) {
					t.Helper()
					if _, err := h.store.db.Exec(query, args...); err != nil {
						t.Fatal(err)
					}
				}
				switch state {
				case "on-failure-policy", "never-clean-policy", "never-failed-policy":
					neverStarted(t, h, agent, job, claim)
					exit := 0
					if state == "never-failed-policy" {
						exit = 1
					}
					neverComplete(t, h, agent, job, claim, CompletionRequest{Result: ProcessResult{ExitCode: &exit}})
				case "never-interruption":
					neverStarted(t, h, agent, job, claim)
					neverComplete(t, h, agent, job, claim, CompletionRequest{Result: ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}})
				case "latch", "streak-latch":
					neverStarted(t, h, agent, job, claim)
					neverComplete(t, h, agent, job, claim, CompletionRequest{Result: ProcessResult{OutputError: "durable output failed"}})
					if state == "streak-latch" {
						exec("UPDATE service_jobs SET restart_streak=1 WHERE job_id=?", job.JobID)
					}
				case "capacity":
					exec("UPDATE jobs SET state='stopped' WHERE job_id=?", job.JobID)
					exec("UPDATE service_jobs SET desired_state='stopped' WHERE job_id=?", job.JobID)
					exec("UPDATE nodes SET max_service_slots=0 WHERE node_id=?", node.NodeID)
				case "missing-root":
					exec("UPDATE nodes SET root_instance_id='' WHERE node_id=?", node.NodeID)
				case "removal_pending", "agent_cleaned", "forgotten_cleanup_unverified", "stalled_cleanup_unverified":
					if _, err := h.store.RemoveService(t.Context(), job.JobID); err != nil {
						t.Fatal(err)
					}
					if state != "removal_pending" {
						exec("UPDATE service_removals SET status=? WHERE job_id=?", state, job.JobID)
						exec("UPDATE jobs SET state=? WHERE job_id=?", state, job.JobID)
					}
					if state == "agent_cleaned" {
						exec("UPDATE service_removals SET agent_cleaned_ns=?, cleanup_status='acknowledged' WHERE job_id=?", now.UnixNano(), job.JobID)
					}
					if state == "stalled_cleanup_unverified" {
						exec("UPDATE service_removals SET stalled_ns=? WHERE job_id=?", now.UnixNano(), job.JobID)
					}
				case "removed_verified":
					exec("UPDATE service_jobs SET bound_node_id=NULL WHERE job_id=?", job.JobID)
					if _, err := h.store.RemoveService(t.Context(), job.JobID); err != nil {
						t.Fatal(err)
					}
				default:
					exec("UPDATE jobs SET state=? WHERE job_id=?", state, job.JobID)
					if state == "stopped" || state == "stopping" {
						exec("UPDATE service_jobs SET desired_state='stopped' WHERE job_id=?", job.JobID)
					}
				}
				facts := serviceOperatorRead(t, h, client, job.JobID)
				if state == "on-failure-policy" || state == "never-clean-policy" || state == "never-failed-policy" {
					if facts.Condition == nil || facts.Condition.Code != "policy_stop" {
						t.Fatalf("policy condition=%+v", facts.Condition)
					}
				}
				if state == "latch" || state == "streak-latch" {
					if facts.Condition == nil || facts.Condition.Code != "failure_latched" {
						t.Fatalf("latch condition=%+v", facts.Condition)
					}
				}
				h.clock.Advance(time.Second)
				again := serviceOperatorRead(t, h, client, job.JobID)
				if !reflect.DeepEqual(facts.Condition, again.Condition) {
					t.Fatalf("read changed condition: %+v -> %+v", facts.Condition, again.Condition)
				}
				for _, action := range facts.Actions {
					switch action.Verb {
					case "start":
						if action.Requires["desired_state"] != "running" {
							t.Fatal(action)
						}
					case "stop":
						if action.Requires["desired_state"] != "stopped" {
							t.Fatal(action)
						}
					case "forget":
						if action.Requires["force"] != true {
							t.Fatal(action)
						}
					case "restart":
						if len(action.Requires) != 0 || !reflect.DeepEqual(action.Inputs, []contract.ActionInput{{Name: "idempotency_key", Type: "string", Required: true}}) {
							t.Fatal(action)
						}
					}
					if action.Verb == verb {
						status, body := serviceOperatorWrite(t, h, client, job.JobID, verb)
						assertServiceActionParity(t, action, status, body)
						// Independently pin crucial distinctions so matching bugs cannot pass.
						if verb == "start" && (state == "latch" || state == "streak-latch" || state == "stopping" || state == "capacity") && action.RefusedBecause == nil {
							t.Fatal("start must be refused")
						}
						if verb == "start" && (state == "on-failure-policy" || state == "never-clean-policy" || state == "never-failed-policy" || state == "never-interruption") && action.RefusedBecause != nil {
							t.Fatal("explicit start must be allowed")
						}
					}
				}
			})
		}
	}
}

func TestServiceAllowedActionsCallerHTTP(t *testing.T) {
	for _, tag := range []string{DefaultClientPrincipalTag, "custom-client"} {
		for _, identity := range []fabric.Identity{
			{NodeID: "client", Tags: []string{tag}}, {NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}},
			{NodeID: "dual", Tags: []string{tag, DefaultAgentPrincipalTag}}, {NodeID: "untagged"},
			{NodeID: "person", FabricID: "fabric", UserID: "person", DeviceID: "device"},
		} {
			for _, verb := range []string{"start", "stop", "restart", "remove", "forget"} {
				t.Run(fmt.Sprintf("%s/%s/%s", tag, identity.NodeID, verb), func(t *testing.T) {
					h := newIntegrationHarness(t, nil)
					job, _, err := h.store.CreateJob(t.Context(), operatorServiceSpec("caller", nil))
					if err != nil {
						t.Fatal(err)
					}
					h.server.clientPrincipalTag = tag
					r := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.JobID+"?class=service", nil)
					r.SetPathValue("job_id", job.JobID)
					r = r.WithContext(context.WithValue(r.Context(), identityContextKey{}, identity))
					w := httptest.NewRecorder()
					h.server.getJob(w, r)
					if w.Code != http.StatusOK {
						t.Fatalf("read=%d %s", w.Code, w.Body.String())
					}
					var facts serviceOperatorWire
					if err := json.Unmarshal(w.Body.Bytes(), &facts); err != nil {
						t.Fatal(err)
					}
					if len(facts.Actions) != 5 {
						t.Fatalf("actions=%s", w.Body.String())
					}
					for _, action := range facts.Actions {
						if action.Verb == verb {
							status, body := serviceOperatorWrite(t, h, h.client(identity), job.JobID, verb)
							assertServiceActionParity(t, action, status, body)
						}
					}
				})
			}
		}
	}
}

func TestServiceOperatorListAndReadOnly(t *testing.T) {
	h, client, agent, _, job, claim := neverFixture(t, "process")
	neverStarted(t, h, agent, job, claim)
	zero := 0
	neverComplete(t, h, agent, job, claim, CompletionRequest{Result: ProcessResult{ExitCode: &zero}})
	// A service read must finish while another connection holds the writer lock.
	writer, err := h.store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	r := httptest.NewRequest(http.MethodGet, "/v1/jobs?class=service", nil).WithContext(context.WithValue(ctx, identityContextKey{}, fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}}))
	w := httptest.NewRecorder()
	h.server.listJobs(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("list acquired writer lock: %d %s", w.Code, w.Body.String())
	}
	var page struct {
		Jobs []serviceOperatorWire `json:"jobs"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Jobs) != 1 || len(page.Jobs[0].Actions) != 5 || page.Jobs[0].Condition == nil {
		t.Fatalf("list facts=%s", w.Body.String())
	}
	detail := serviceOperatorRead(t, h, client, job.JobID)
	if !reflect.DeepEqual(detail, page.Jobs[0]) {
		t.Fatalf("list/detail mismatch: %+v %+v", page.Jobs[0], detail)
	}
}

func TestServiceAllowedActionsComputerOwnershipHTTP(t *testing.T) {
	for _, verb := range []string{"start", "stop", "restart", "remove", "forget"} {
		t.Run(verb, func(t *testing.T) {
			h := newIntegrationHarness(t, nil)
			computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "operator-computer", Spec: computerCapabilityJobSpec("owned"), Actor: "operator"})
			if err != nil {
				t.Fatal(err)
			}
			client := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
			facts := serviceOperatorRead(t, h, client, computer.CurrentJobID)
			for _, action := range facts.Actions {
				if action.Verb == verb {
					if action.RefusedBecause == nil || action.RefusedBecause.Code != contract.ErrorComputerResourceRequired {
						t.Fatalf("Computer advertised Job mutation: %+v", action)
					}
					status, body := serviceOperatorWrite(t, h, client, computer.CurrentJobID, verb)
					assertServiceActionParity(t, action, status, body)
				}
			}
		})
	}
}

func TestServiceAllowedActionsAttemptCredentialHTTP(t *testing.T) {
	h, _, _, _, job, claim := neverFixture(t, "process")
	// Even a dual-tag holding node stays in the credential protocol.
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag, DefaultClientPrincipalTag}})
	status, body := h.credentialRequest(agent, http.MethodGet, "/v1/jobs/"+job.JobID+"?class=service", claim.AttemptToken, nil)
	if status != http.StatusOK {
		t.Fatalf("credential read=%d %s", status, body)
	}
	var facts serviceOperatorWire
	if err := json.Unmarshal(body, &facts); err != nil {
		t.Fatal(err)
	}
	if len(facts.Actions) != 5 {
		t.Fatalf("credential actions=%s", body)
	}
	for _, action := range facts.Actions {
		if action.RefusedBecause == nil {
			t.Fatalf("credential advertised operator authority: %+v", action)
		}
		method, path, request := http.MethodPost, action.Verb, any(nil)
		switch action.Verb {
		case "start", "stop":
			method, path = http.MethodPut, "desired-state"
			request = ServiceDesiredStateRequest{DesiredState: contract.ServiceDesiredRunning}
		case "restart":
			request = ServiceRestartRequest{IdempotencyKey: "fresh"}
		case "forget":
			request = ForceForgetRequest{Force: true}
		}
		status, body := h.credentialRequest(agent, method, serviceMutationPath(job.JobID, path), claim.AttemptToken, request)
		assertServiceActionParity(t, action, status, body)
	}
}
