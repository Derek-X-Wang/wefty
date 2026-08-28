package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

const (
	DefaultOperationTimeout    = 10 * time.Second
	DefaultMaxIdleConnsPerHost = 16

	// DefaultFinalizationTimeout bounds the uncancelable finalization phase —
	// the final redaction flush and log upload for one attempt. Larger than a
	// single operation timeout because a flush may upload several batches with
	// retries, and deliberately FINITE: an unbounded finalization prevents the
	// attempt from ever completing, which blocks agent drain and service
	// removal indefinitely rather than merely delaying them.
	DefaultFinalizationTimeout = 30 * time.Second
)

// ProtocolError is an error response returned by the L1 agent protocol.
type ProtocolError struct {
	StatusCode int
	APIError   contract.APIError
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("l1 agent protocol: HTTP %d %s: %s", e.StatusCode, e.APIError.Code, e.APIError.Message)
}

// Client calls the Fabric-authenticated L1 agent protocol.
type Client struct {
	baseURL          string
	httpClient       *http.Client
	transport        *http.Transport
	operationTimeout time.Duration
}

// NewClient creates a client whose HTTP connections are obtained exclusively
// through the Fabric seam.
func NewClient(f fabric.Fabric, controlPlaneAddress string) (*Client, error) {
	return newClient(f, controlPlaneAddress, DefaultOperationTimeout)
}

func newClient(f fabric.Fabric, controlPlaneAddress string, operationTimeout time.Duration) (*Client, error) {
	if f == nil {
		return nil, errors.New("agent: fabric is required")
	}
	if strings.TrimSpace(controlPlaneAddress) == "" {
		return nil, errors.New("agent: control-plane address is required")
	}
	if operationTimeout <= 0 {
		return nil, errors.New("agent: operation timeout must be positive")
	}
	transport := &http.Transport{
		MaxIdleConnsPerHost: DefaultMaxIdleConnsPerHost,
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return f.Dial(ctx, network, controlPlaneAddress)
		},
	}
	return &Client{
		baseURL: "http://control-plane.invalid",
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   operationTimeout,
		},
		transport: transport, operationTimeout: operationTimeout,
	}, nil
}

// Close releases idle Fabric-backed HTTP connections.
func (c *Client) Close() { c.transport.CloseIdleConnections() }

func (c *Client) Register(ctx context.Context, registration contract.NodeRegistration) (l1.Node, error) {
	var node l1.Node
	err := c.post(ctx, "/v1/agent/nodes/register", registration, &node)
	return node, err
}

func (c *Client) Heartbeat(ctx context.Context, nodeID string, request l1.HeartbeatRequest) (l1.HeartbeatResponse, error) {
	var response l1.HeartbeatResponse
	err := c.post(ctx, "/v1/agent/nodes/"+url.PathEscape(nodeID)+"/heartbeat", request, &response)
	return response, err
}

func (c *Client) Drain(ctx context.Context, nodeID, bootSessionID string) (l1.Node, error) {
	var node l1.Node
	err := c.post(ctx, "/v1/agent/nodes/"+url.PathEscape(nodeID)+"/drain", l1.DrainRequest{BootSessionID: bootSessionID}, &node)
	return node, err
}

func (c *Client) Claim(ctx context.Context, nodeID, bootSessionID, class string, excludedJobIDs ...string) (*l1.Claim, error) {
	var claim l1.Claim
	noContent, err := c.postAllowNoContent(ctx, "/v1/agent/jobs/claim", l1.ClaimRequest{
		NodeID: nodeID, BootSessionID: bootSessionID, Class: class, ExcludedJobIDs: excludedJobIDs,
	}, &claim)
	if err != nil || noContent {
		return nil, err
	}
	return &claim, nil
}

func (c *Client) ProveServiceBinding(ctx context.Context, nodeID, bootSessionID, jobID string) (bool, error) {
	var response l1.ServiceBindingProofResponse
	err := c.post(ctx, "/v1/agent/jobs/"+url.PathEscape(jobID)+"/service-binding-proof", l1.ServiceBindingProofRequest{
		NodeID: nodeID, BootSessionID: bootSessionID,
	}, &response)
	return response.Bound, err
}

func (c *Client) LatchServiceImageReconciliationFailure(ctx context.Context, nodeID, bootSessionID, jobID string, failure contract.SpawnFailure) (l1.Job, error) {
	var job l1.Job
	err := c.post(ctx, "/v1/agent/jobs/"+url.PathEscape(jobID)+"/image-reconciliation-failure", l1.ServiceImageReconciliationFailureRequest{
		NodeID: nodeID, BootSessionID: bootSessionID, Failure: failure,
	}, &job)
	return job, err
}

func (c *Client) Renew(ctx context.Context, jobID, attemptID, fencingToken string) (l1.AttemptLease, error) {
	var lease l1.AttemptLease
	path := attemptPath(jobID, attemptID) + "/lease"
	err := c.post(ctx, path, l1.RenewalRequest{FencingToken: fencingToken}, &lease)
	return lease, err
}

func (c *Client) ObserveAttemptImage(ctx context.Context, jobID, attemptID string, request l1.ImageObservationRequest) (l1.Job, error) {
	var job l1.Job
	err := c.request(ctx, http.MethodPut, attemptPath(jobID, attemptID)+"/image", request, &job)
	return job, err
}

func (c *Client) StartAttempt(ctx context.Context, jobID, attemptID string, request l1.StartedRequest) (l1.Job, error) {
	var job l1.Job
	err := c.post(ctx, attemptPath(jobID, attemptID)+"/started", request, &job)
	return job, err
}

func (c *Client) AppendLogs(ctx context.Context, jobID, attemptID string, request l1.AppendLogsRequest) (l1.AppendLogsResponse, error) {
	var response l1.AppendLogsResponse
	err := c.post(ctx, attemptPath(jobID, attemptID)+"/logs", request, &response)
	return response, err
}

func (c *Client) Complete(ctx context.Context, jobID, attemptID string, request l1.CompletionRequest) (l1.Job, error) {
	var job l1.Job
	err := c.post(ctx, attemptPath(jobID, attemptID)+"/complete", request, &job)
	return job, err
}

// SetAttemptPublication sends an absolute publication state for one attempt.
// Serialization and same-state suppression belong to the attempt-local
// publication controller, not this transport method.
func (c *Client) SetAttemptPublication(
	ctx context.Context,
	jobID, attemptID string,
	request l1.PublicationRequest,
) (l1.Job, error) {
	var job l1.Job
	err := c.request(ctx, http.MethodPut, attemptPath(jobID, attemptID)+"/publication", request, &job)
	return job, err
}

// AcknowledgeRemoval attests that local deletion has already completed. The
// control plane never asks this client to inspect or delete a filesystem path.
func (c *Client) AcknowledgeRemoval(ctx context.Context, jobID string, request l1.RemovalAcknowledgementRequest) (l1.Job, error) {
	var job l1.Job
	err := c.post(ctx, "/v1/agent/jobs/"+url.PathEscape(jobID)+"/removal-acknowledgement", request, &job)
	return job, err
}

func (c *Client) AcknowledgeComputerStorageReset(ctx context.Context, computerID string, request l1.ComputerStorageResetAcknowledgementRequest) (l1.Computer, error) {
	var computer l1.Computer
	err := c.post(ctx, "/v1/agent/computers/"+url.PathEscape(computerID)+"/storage-reset-acknowledgement", request, &computer)
	return computer, err
}

func attemptPath(jobID, attemptID string) string {
	return "/v1/agent/jobs/" + url.PathEscape(jobID) + "/attempts/" + url.PathEscape(attemptID)
}

func (c *Client) post(ctx context.Context, path string, body, target any) error {
	return c.request(ctx, http.MethodPost, path, body, target)
}

func (c *Client) request(ctx context.Context, method, path string, body, target any) error {
	_, err := c.requestAllowNoContent(ctx, method, path, body, target)
	return err
}

func (c *Client) postAllowNoContent(ctx context.Context, path string, body, target any) (bool, error) {
	return c.requestAllowNoContent(ctx, http.MethodPost, path, body, target)
}

func (c *Client) requestAllowNoContent(ctx context.Context, method, path string, body, target any) (bool, error) {
	requestContext, cancel := c.boundedContext(ctx)
	defer cancel()
	payload, err := json.Marshal(body)
	if err != nil {
		return false, fmt.Errorf("agent: encode request: %w", err)
	}
	request, err := http.NewRequestWithContext(requestContext, method, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return false, fmt.Errorf("agent: build request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return false, fmt.Errorf("agent: call L1: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNoContent {
		return true, nil
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return false, decodeProtocolError(response)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 1<<20))
	if err := decoder.Decode(target); err != nil {
		return false, fmt.Errorf("agent: decode L1 response: %w", err)
	}
	return false, nil
}

func (c *Client) boundedContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= c.operationTimeout {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, c.operationTimeout)
}

func decodeProtocolError(response *http.Response) error {
	var protocolResponse contract.ErrorResponse
	decoder := json.NewDecoder(io.LimitReader(response.Body, 1<<20))
	if err := decoder.Decode(&protocolResponse); err != nil {
		return fmt.Errorf("l1 agent protocol: HTTP %d with invalid error response: %w", response.StatusCode, err)
	}
	return &ProtocolError{StatusCode: response.StatusCode, APIError: protocolResponse.Error}
}
