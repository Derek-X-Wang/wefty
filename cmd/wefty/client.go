package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/takeover"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

type apiClients struct {
	l1     *apiClient
	l3     *apiClient
	images imageDigestResolver
	fabric fabric.Fabric
	wait   func(context.Context, time.Duration) error
}

type apiClient struct {
	name   string
	client *http.Client
}

type apiResponseError struct {
	Service    string
	StatusCode int
	APIError   contract.APIError
}

func (e *apiResponseError) Error() string {
	return formatAPIError(&e.APIError)
}

func newAPIClients(participant fabric.Fabric, l1Address, l3Address string) (*apiClients, error) {
	if participant == nil {
		return nil, fmt.Errorf("wefty: fabric is required")
	}
	if strings.TrimSpace(l1Address) == "" || strings.TrimSpace(l3Address) == "" {
		return nil, fmt.Errorf("wefty: L1 and L3 addresses are required")
	}
	return &apiClients{
		l1:     newAPIClient("L1", participant, l1Address),
		l3:     newAPIClient("L3", participant, l3Address),
		images: newRegistryResolver(nil),
		fabric: participant,
		wait:   waitForContext,
	}, nil
}

func waitForContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}

func newAPIClient(name string, participant fabric.Fabric, address string) *apiClient {
	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return participant.Dial(ctx, network, address)
	}}
	return &apiClient{name: name, client: &http.Client{Transport: transport}}
}

func (c *apiClients) close() {
	c.l1.client.CloseIdleConnections()
	c.l3.client.CloseIdleConnections()
}

func (c *apiClients) listNodes(ctx context.Context) (l1.NodeList, error) {
	var result l1.NodeList
	err := c.l1.do(ctx, http.MethodGet, "/v1/nodes", nil, nil, &result, http.StatusOK)
	return result, err
}

func (c *apiClients) bootstrapAdmin(ctx context.Context, nonce string) (l1.AdminPolicy, error) {
	var policy l1.AdminPolicy
	err := c.l1.do(ctx, http.MethodPost, "/v1/admin-bootstrap", l1.BootstrapAdminRequest{Nonce: nonce},
		nil, &policy, http.StatusCreated)
	return policy, err
}

func (c *apiClients) getAdminPolicy(ctx context.Context) (l1.AdminPolicy, error) {
	var policy l1.AdminPolicy
	err := c.l1.do(ctx, http.MethodGet, "/v1/admin-policy", nil, nil, &policy, http.StatusOK)
	return policy, err
}

func (c *apiClients) mutateAdmin(ctx context.Context, userID string, revision int64, remove bool) (l1.AdminPolicy, error) {
	method := http.MethodPut
	if remove {
		method = http.MethodDelete
	}
	var policy l1.AdminPolicy
	path := "/v1/admin-policy/admins/" + url.PathEscape(userID)
	err := c.l1.do(ctx, method, path, l1.AdminPolicyMutationRequest{PolicyRevision: revision}, nil, &policy, http.StatusOK)
	return policy, err
}

func (c *apiClients) listComputerGrants(ctx context.Context, computerID string) (l1.ComputerGrantList, error) {
	var grants l1.ComputerGrantList
	path := "/v1/computers/" + url.PathEscape(computerID) + "/grants"
	err := c.l1.do(ctx, http.MethodGet, path, nil, nil, &grants, http.StatusOK)
	return grants, err
}

func (c *apiClients) mutateComputerGrant(
	ctx context.Context,
	computerID, userID string,
	request l1.ComputerGrantMutationRequest,
) (l1.ComputerGrantMutationResult, error) {
	var result l1.ComputerGrantMutationResult
	path := "/v1/computers/" + url.PathEscape(computerID) + "/grants/" + url.PathEscape(userID)
	err := c.l1.do(ctx, http.MethodPut, path, request, nil, &result, http.StatusOK)
	return result, err
}

func (c *apiClients) getComputerPolicyRevocation(
	ctx context.Context,
	computerID, fabricID, userID string,
	policyRevision int64,
) (l1.ComputerPolicyRevocation, error) {
	query := url.Values{"fabric_id": []string{fabricID}, "user_id": []string{userID}}
	path := "/v1/computers/" + url.PathEscape(computerID) + "/revocations/" +
		strconv.FormatInt(policyRevision, 10) + "?" + query.Encode()
	var revocation l1.ComputerPolicyRevocation
	err := c.l1.do(ctx, http.MethodGet, path, nil, nil, &revocation, http.StatusOK)
	return revocation, err
}

func (c *apiClients) getComputer(ctx context.Context, computerID string) (l1.Computer, error) {
	var computer l1.Computer
	path := "/v1/computers/" + url.PathEscape(computerID)
	err := c.l1.do(ctx, http.MethodGet, path, nil, nil, &computer, http.StatusOK)
	return computer, err
}

func (c *apiClients) listComputerTakeoverSessions(ctx context.Context, computerID string) (l1.ComputerTakeoverSessionList, error) {
	var sessions l1.ComputerTakeoverSessionList
	path := "/v1/computers/" + url.PathEscape(computerID) + "/takeover/sessions"
	err := c.l1.do(ctx, http.MethodGet, path, nil, nil, &sessions, http.StatusOK)
	return sessions, err
}

func (c *apiClients) getComputerTakeoverAccess(ctx context.Context, computerID string) (l1.ComputerTakeoverAccess, error) {
	var access l1.ComputerTakeoverAccess
	path := "/v1/computers/" + url.PathEscape(computerID) + "/takeover"
	err := c.l1.do(ctx, http.MethodGet, path, nil, nil, &access, http.StatusOK)
	return access, err
}

type takeoverActionError = takeover.ActionError

func (c *apiClients) performComputerTakeoverAction(ctx context.Context, endpoint, token, action string) error {
	return takeover.Perform(ctx, c.fabric, endpoint, token, action)
}

func (c *apiClients) listComputerTakeoverAudit(
	ctx context.Context,
	computerID, cursor string,
	limit int,
	tail bool,
) (l1.ComputerTakeoverAuditList, error) {
	query := url.Values{"limit": []string{strconv.Itoa(limit)}}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	if tail {
		query.Set("tail", "true")
	}
	var page l1.ComputerTakeoverAuditList
	path := "/v1/computers/" + url.PathEscape(computerID) + "/takeover/audit?" + query.Encode()
	err := c.l1.do(ctx, http.MethodGet, path, nil, nil, &page, http.StatusOK)
	return page, err
}

func (c *apiClients) setNodeClaims(ctx context.Context, nodeID string, request l1.NodeIntentRequest) (l1.Node, error) {
	var node l1.Node
	path := "/v1/nodes/" + url.PathEscape(nodeID) + "/claims"
	err := c.l1.do(ctx, http.MethodPost, path, request, nil, &node, http.StatusOK)
	return node, err
}

func (c *apiClients) createService(ctx context.Context, spec contract.JobSpec) (l1.Job, error) {
	var job l1.Job
	err := c.l1.do(ctx, http.MethodPost, "/v1/jobs", spec, nil, &job, http.StatusCreated, http.StatusOK)
	return job, err
}

func (c *apiClients) listServices(ctx context.Context, cursor string, limit int) (l1.JobList, error) {
	query := url.Values{
		"class": []string{contract.JobClassService},
		"limit": []string{strconv.Itoa(limit)},
	}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	var result l1.JobList
	err := c.l1.do(ctx, http.MethodGet, "/v1/jobs?"+query.Encode(), nil, nil, &result, http.StatusOK)
	return result, err
}

func (c *apiClients) getService(ctx context.Context, jobID string) (l1.Job, error) {
	var job l1.Job
	path := "/v1/jobs/" + url.PathEscape(jobID) + "?class=" + contract.JobClassService
	err := c.l1.do(ctx, http.MethodGet, path, nil, nil, &job, http.StatusOK)
	return job, err
}

func (c *apiClients) setServiceDesiredState(
	ctx context.Context,
	jobID string,
	desired contract.ServiceDesiredState,
) (l1.Job, error) {
	var job l1.Job
	path := "/v1/jobs/" + url.PathEscape(jobID) + "/desired-state?class=" + contract.JobClassService
	err := c.l1.do(ctx, http.MethodPut, path, l1.ServiceDesiredStateRequest{DesiredState: desired}, nil, &job, http.StatusAccepted)
	return job, err
}

func (c *apiClients) restartService(ctx context.Context, jobID, idempotencyKey string) (l1.Job, error) {
	var job l1.Job
	path := "/v1/jobs/" + url.PathEscape(jobID) + "/restart?class=" + contract.JobClassService
	err := c.l1.do(ctx, http.MethodPost, path, l1.ServiceRestartRequest{IdempotencyKey: idempotencyKey}, nil,
		&job, http.StatusAccepted, http.StatusOK)
	return job, err
}

func (c *apiClients) removeService(ctx context.Context, jobID string) (l1.Job, error) {
	var job l1.Job
	path := "/v1/jobs/" + url.PathEscape(jobID) + "/remove?class=" + contract.JobClassService
	err := c.l1.do(ctx, http.MethodPost, path, nil, nil, &job, http.StatusAccepted)
	return job, err
}

func (c *apiClients) forceForgetService(ctx context.Context, jobID string) (l1.Job, error) {
	var job l1.Job
	path := "/v1/jobs/" + url.PathEscape(jobID) + "/forget?class=" + contract.JobClassService
	err := c.l1.do(ctx, http.MethodPost, path, l1.ForceForgetRequest{Force: true}, nil, &job, http.StatusOK)
	return job, err
}

func (c *apiClients) getServiceLogs(ctx context.Context, jobID, cursor string, limit int) (l1.LogPage, error) {
	query := url.Values{
		"class": []string{contract.JobClassService},
		"limit": []string{strconv.Itoa(limit)},
	}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	var page l1.LogPage
	path := "/v1/jobs/" + url.PathEscape(jobID) + "/logs?" + query.Encode()
	err := c.l1.do(ctx, http.MethodGet, path, nil, nil, &page, http.StatusOK)
	return page, err
}

func (c *apiClients) drainNode(ctx context.Context, nodeID string) (l1.Node, error) {
	nodes, err := c.listNodes(ctx)
	if err != nil {
		return l1.Node{}, err
	}
	var revision int64
	found := false
	for _, node := range nodes.Nodes {
		if node.NodeID == nodeID {
			revision = node.IntentRevision
			found = true
			break
		}
	}
	if !found {
		return l1.Node{}, fmt.Errorf("node %q was not found", nodeID)
	}
	var node l1.Node
	path := "/v1/nodes/" + url.PathEscape(nodeID) + "/drain"
	err = c.l1.do(ctx, http.MethodPost, path, l1.NodeIntentRequest{
		ClaimsEnabled: false, IntentRevision: revision, Reason: "operator requested drain",
	}, nil, &node, http.StatusOK)
	return node, err
}

func (c *apiClients) submitRun(ctx context.Context, request l3.CreateRunRequest, idempotencyKey string) (l3.RunAccepted, error) {
	var accepted l3.RunAccepted
	headers := http.Header{"Idempotency-Key": []string{idempotencyKey}}
	err := c.l3.do(ctx, http.MethodPost, "/v1/runs", request, headers, &accepted, http.StatusCreated, http.StatusOK)
	return accepted, err
}

func (c *apiClients) rerun(ctx context.Context, runID, idempotencyKey string) (l3.RunAccepted, error) {
	var accepted l3.RunAccepted
	headers := http.Header{"Idempotency-Key": []string{idempotencyKey}}
	path := "/v1/runs/" + url.PathEscape(runID) + "/rerun"
	err := c.l3.do(ctx, http.MethodPost, path, nil, headers, &accepted, http.StatusCreated, http.StatusOK)
	return accepted, err
}

func (c *apiClients) getRun(ctx context.Context, runID string) (contract.RunRecord, error) {
	var run contract.RunRecord
	path := "/v1/runs/" + url.PathEscape(runID)
	err := c.l3.do(ctx, http.MethodGet, path, nil, nil, &run, http.StatusOK)
	return run, err
}

func (c *apiClients) getRunExecution(ctx context.Context, runID string) (l3.RunExecution, error) {
	var execution l3.RunExecution
	path := "/v1/runs/" + url.PathEscape(runID) + "/execution"
	err := c.l3.do(ctx, http.MethodGet, path, nil, nil, &execution, http.StatusOK)
	return execution, err
}

func (c *apiClients) getRunLineage(ctx context.Context, runID string) (l3.RunLineage, error) {
	var lineage l3.RunLineage
	path := "/v1/runs/" + url.PathEscape(runID) + "/lineage"
	err := c.l3.do(ctx, http.MethodGet, path, nil, nil, &lineage, http.StatusOK)
	return lineage, err
}

func (c *apiClients) getRunLogs(ctx context.Context, runID, cursor string, limit int) (l1.LogPage, error) {
	query := url.Values{"limit": []string{strconv.Itoa(limit)}}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	var page l1.LogPage
	path := "/v1/runs/" + url.PathEscape(runID) + "/logs?" + query.Encode()
	err := c.l3.do(ctx, http.MethodGet, path, nil, nil, &page, http.StatusOK)
	return page, err
}

func (c *apiClients) getComputerSubmission(ctx context.Context, computerID string) (l1.ComputerSubmissionState, error) {
	var state l1.ComputerSubmissionState
	path := "/v1/computers/" + url.PathEscape(computerID) + "/submission"
	err := c.l1.do(ctx, http.MethodGet, path, nil, nil, &state, http.StatusOK)
	return state, err
}

func (c *apiClients) mutateComputerSubmission(ctx context.Context, computerID string, request l1.ComputerSubmissionRequest) (l1.ComputerSubmissionMutationResult, bool, error) {
	var result l1.ComputerSubmissionMutationResult
	path := "/v1/computers/" + url.PathEscape(computerID) + "/submission"
	headers, err := c.l1.doWithResponse(ctx, http.MethodPut, path, request, nil, &result, http.StatusOK)
	return result, err == nil && headers.Get("Idempotent-Replay") == "true", err
}

func (c *apiClients) listRunsByOrigin(ctx context.Context, origin, cursor string, limit int, includeDescendants bool) (l3.ComputerRunPage, error) {
	query := url.Values{
		"origin":              []string{origin},
		"include_descendants": []string{strconv.FormatBool(includeDescendants)},
		"limit":               []string{strconv.Itoa(limit)},
	}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	var page l3.ComputerRunPage
	err := c.l3.do(ctx, http.MethodGet, "/v1/runs?"+query.Encode(), nil, nil, &page, http.StatusOK)
	return page, err
}

func (c *apiClient) do(ctx context.Context, method, path string, body any, headers http.Header, target any, success ...int) error {
	_, err := c.doWithResponse(ctx, method, path, body, headers, target, success...)
	return err
}

func (c *apiClient) doWithResponse(ctx context.Context, method, path string, body any, headers http.Header, target any, success ...int) (http.Header, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("encode %s request: %w", c.name, err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, "http://wefty.invalid"+path, reader)
	if err != nil {
		return nil, fmt.Errorf("create %s request: %w", c.name, err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	for name, values := range headers {
		request.Header[name] = append([]string(nil), values...)
	}
	response, err := c.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("call %s: %w", c.name, err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 16<<20))
	if err != nil {
		return nil, fmt.Errorf("read %s response: %w", c.name, err)
	}
	for _, status := range success {
		if response.StatusCode == status {
			if target == nil || len(responseBody) == 0 {
				return response.Header.Clone(), nil
			}
			if err := json.Unmarshal(responseBody, target); err != nil {
				return nil, fmt.Errorf("decode %s response: %w", c.name, err)
			}
			return response.Header.Clone(), nil
		}
	}
	var responseError contract.ErrorResponse
	if err := json.Unmarshal(responseBody, &responseError); err == nil && responseError.Error.Code != "" {
		return nil, &apiResponseError{Service: c.name, StatusCode: response.StatusCode, APIError: responseError.Error}
	}
	return nil, fmt.Errorf("%s returned HTTP %d", c.name, response.StatusCode)
}
