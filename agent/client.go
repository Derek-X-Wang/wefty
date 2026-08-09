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

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
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
	baseURL    string
	httpClient *http.Client
	transport  *http.Transport
}

// NewClient creates a client whose HTTP connections are obtained exclusively
// through the Fabric seam.
func NewClient(f fabric.Fabric, controlPlaneAddress string) (*Client, error) {
	if f == nil {
		return nil, errors.New("agent: fabric is required")
	}
	if strings.TrimSpace(controlPlaneAddress) == "" {
		return nil, errors.New("agent: control-plane address is required")
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return f.Dial(ctx, network, controlPlaneAddress)
		},
	}
	return &Client{
		baseURL:    "http://control-plane.invalid",
		httpClient: &http.Client{Transport: transport},
		transport:  transport,
	}, nil
}

// Close releases idle Fabric-backed HTTP connections.
func (c *Client) Close() { c.transport.CloseIdleConnections() }

func (c *Client) Register(ctx context.Context, registration contract.NodeRegistration) (l1.Node, error) {
	var node l1.Node
	err := c.post(ctx, "/v1/agent/nodes/register", registration, &node)
	return node, err
}

func (c *Client) Heartbeat(ctx context.Context, nodeID, bootSessionID string) (l1.Node, error) {
	var node l1.Node
	err := c.post(ctx, "/v1/agent/nodes/"+url.PathEscape(nodeID)+"/heartbeat", l1.HeartbeatRequest{BootSessionID: bootSessionID}, &node)
	return node, err
}

func (c *Client) Claim(ctx context.Context, nodeID, bootSessionID string) (*l1.Claim, error) {
	var claim l1.Claim
	noContent, err := c.postAllowNoContent(ctx, "/v1/agent/jobs/claim", l1.ClaimRequest{NodeID: nodeID, BootSessionID: bootSessionID}, &claim)
	if err != nil || noContent {
		return nil, err
	}
	return &claim, nil
}

func (c *Client) Renew(ctx context.Context, jobID, attemptID, fencingToken string) (l1.AttemptLease, error) {
	var lease l1.AttemptLease
	path := attemptPath(jobID, attemptID) + "/lease"
	err := c.post(ctx, path, l1.RenewalRequest{FencingToken: fencingToken}, &lease)
	return lease, err
}

func (c *Client) Complete(ctx context.Context, jobID, attemptID string, request l1.CompletionRequest) (l1.Job, error) {
	var job l1.Job
	err := c.post(ctx, attemptPath(jobID, attemptID)+"/complete", request, &job)
	return job, err
}

func attemptPath(jobID, attemptID string) string {
	return "/v1/agent/jobs/" + url.PathEscape(jobID) + "/attempts/" + url.PathEscape(attemptID)
}

func (c *Client) post(ctx context.Context, path string, body, target any) error {
	_, err := c.postAllowNoContent(ctx, path, body, target)
	return err
}

func (c *Client) postAllowNoContent(ctx context.Context, path string, body, target any) (bool, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return false, fmt.Errorf("agent: encode request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(payload))
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

func decodeProtocolError(response *http.Response) error {
	var protocolResponse contract.ErrorResponse
	decoder := json.NewDecoder(io.LimitReader(response.Body, 1<<20))
	if err := decoder.Decode(&protocolResponse); err != nil {
		return fmt.Errorf("l1 agent protocol: HTTP %d with invalid error response: %w", response.StatusCode, err)
	}
	return &ProtocolError{StatusCode: response.StatusCode, APIError: protocolResponse.Error}
}
