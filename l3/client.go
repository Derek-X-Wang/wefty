package l3

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
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

// JobClient is the only L1 dependency of the ledger reconciler.
type JobClient interface {
	SubmitJob(context.Context, contract.JobSpec) (l1.Job, error)
	GetJob(context.Context, string) (l1.Job, error)
}

// JobImageEvidenceClient is the L1 result-ingestion seam for tag-only image
// runs. It remains separate from JobClient so tests and alternate L1 clients
// can implement the evidence projection independently.
type JobImageEvidenceClient interface {
	GetJobImageEvidence(context.Context, string) ([]AttemptImageEvidence, error)
}

// JobLogClient is the public L1 log-polling dependency of the L3 API.
type JobLogClient interface {
	GetJobLogs(context.Context, string, string, int) (l1.LogPage, error)
}

type ComputerGrantVerifier interface {
	ProveComputerTokenScope(context.Context, string, string, string, string) (ComputerTokenScopeProof, error)
}

// L1Client calls the L1 client protocol exclusively through Fabric.Dial.
type L1Client struct {
	client           *http.Client
	operationTimeout time.Duration
}

const l1OperationTimeout = 10 * time.Second

type l1RequestContextKey struct{}

func NewL1Client(f fabric.Fabric, address string) (*L1Client, error) {
	return newL1Client(f, address, l1OperationTimeout, l1OperationTimeout, l1OperationTimeout)
}

func newL1Client(f fabric.Fabric, address string, operationTimeout, dialTimeout, headerTimeout time.Duration) (*L1Client, error) {
	if operationTimeout <= 0 || dialTimeout <= 0 || headerTimeout <= 0 {
		return nil, fmt.Errorf("l3: positive L1 client timeouts are required")
	}
	if f == nil {
		return nil, fmt.Errorf("l3: fabric is required for L1 client")
	}
	if strings.TrimSpace(address) == "" {
		address = DefaultL1Address
	}
	transport := &http.Transport{ResponseHeaderTimeout: headerTimeout, DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		// net/http detaches dial cancellation to permit connection reuse. Restore
		// this operation's bound so a canceled request cannot leave Fabric dialing.
		if requestCtx, ok := ctx.Value(l1RequestContextKey{}).(context.Context); ok {
			ctx = requestCtx
		}
		dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
		defer cancel()
		return f.Dial(dialCtx, network, address)
	}}
	return &L1Client{client: &http.Client{Transport: transport}, operationTimeout: operationTimeout}, nil
}

func (c *L1Client) CloseIdleConnections() { c.client.CloseIdleConnections() }

func (c *L1Client) SubmitJob(ctx context.Context, spec contract.JobSpec) (l1.Job, error) {
	var job l1.Job
	if err := c.do(ctx, http.MethodPost, "/v1/jobs", spec, &job, http.StatusCreated, http.StatusOK); err != nil {
		return l1.Job{}, err
	}
	return job, nil
}

func (c *L1Client) GetJob(ctx context.Context, jobID string) (l1.Job, error) {
	var job l1.Job
	path := "/v1/jobs/" + url.PathEscape(jobID)
	if err := c.do(ctx, http.MethodGet, path, nil, &job, http.StatusOK); err != nil {
		var remote *l1ResponseError
		if errors.As(err, &remote) && remote.status == http.StatusNotFound && remote.method == http.MethodGet && remote.path == path && remote.protocol.Code == contract.ErrorNotFound && remote.validEnvelope {
			return l1.Job{}, &JobNotFoundError{JobID: jobID, Cause: err}
		}
		return l1.Job{}, err
	}
	return job, nil
}

func (c *L1Client) GetJobImageEvidence(ctx context.Context, jobID string) ([]AttemptImageEvidence, error) {
	job, err := c.GetJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	evidence := make([]AttemptImageEvidence, 0, len(job.Attempts))
	for _, attempt := range job.Attempts {
		if attempt.Image == nil {
			continue
		}
		var platformDigest *string
		if attempt.Image.PlatformManifestDigest != "" {
			value := attempt.Image.PlatformManifestDigest
			platformDigest = &value
		}
		evidence = append(evidence, AttemptImageEvidence{
			AttemptID: attempt.AttemptID, SubmittedReference: attempt.Image.SubmittedReference,
			TopLevelDigest: attempt.Image.TopLevelDigest, PlatformDigest: platformDigest,
			ObservedAt: attempt.Image.ResolvedAt,
		})
	}
	return evidence, nil
}

func (c *L1Client) GetJobLogs(ctx context.Context, jobID, cursor string, limit int) (l1.LogPage, error) {
	path := "/v1/jobs/" + jobID + "/logs?limit=" + strconv.Itoa(limit)
	if cursor != "" {
		path += "&cursor=" + url.QueryEscape(cursor)
	}
	var page l1.LogPage
	if err := c.do(ctx, http.MethodGet, path, nil, &page, http.StatusOK); err != nil {
		return l1.LogPage{}, err
	}
	return page, nil
}

func (c *L1Client) ProveComputerTokenScope(ctx context.Context, computerID, attemptID, hostIdentityNodeID, hostNodeID string) (ComputerTokenScopeProof, error) {
	var proof l1.ComputerTokenScopeProof
	path := "/v1/computers/" + url.PathEscape(computerID) + "/token-scope-proof"
	request := map[string]string{"computer_attempt_id": attemptID, "host_identity_node_id": hostIdentityNodeID, "host_node_id": hostNodeID}
	if err := c.do(ctx, http.MethodPost, path, request, &proof, http.StatusOK); err != nil {
		return ComputerTokenScopeProof{}, err
	}
	return ComputerTokenScopeProof{ComputerID: proof.ComputerID, ComputerAttemptID: proof.ComputerAttemptID,
		ComputerStorageGeneration: proof.ComputerStorageGeneration, SubmitIntentRevision: proof.SubmitIntentRevision,
		HostNodeID: proof.HostNodeID, SubmitMaxInflight: proof.SubmitMaxInflight}, nil
}

func (c *L1Client) do(ctx context.Context, method, path string, body any, target any, success ...int) error {
	ctx, cancel := context.WithTimeout(ctx, c.operationTimeout)
	defer cancel()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return internalError(err, "encode L1 request")
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(context.WithValue(ctx, l1RequestContextKey{}, ctx), method, "http://control-plane.invalid"+path, reader)
	if err != nil {
		return internalError(err, "create L1 request")
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.client.Do(request)
	if err != nil {
		return &Error{Code: contract.ErrorInternal, Message: "call L1 control plane", Retryable: true, Cause: err}
	}
	defer response.Body.Close()
	// A redirected response belongs to its final endpoint, not the original
	// GetJob request. Preserve that origin before classifying absence.
	if response.Request != nil {
		method, path = response.Request.Method, response.Request.URL.RequestURI()
	}
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, (2<<20)+1))
	if err != nil {
		return &Error{Code: contract.ErrorInternal, Message: "read L1 response", Retryable: true, Cause: err}
	}
	// Read one extra byte so a truncated JSON prefix cannot establish absence.
	if len(responseBody) > 2<<20 {
		return &l1ResponseError{status: response.StatusCode, method: method, path: path, protocol: &Error{
			Code: contract.ErrorInternal, Message: "L1 response exceeds size limit",
			Retryable: response.StatusCode >= 500 || response.StatusCode == http.StatusTooManyRequests,
		}}
	}
	for _, status := range success {
		if response.StatusCode == status {
			if err := json.Unmarshal(responseBody, target); err != nil {
				return internalError(err, "decode L1 response")
			}
			return nil
		}
	}
	var responseError contract.ErrorResponse
	if err := json.Unmarshal(responseBody, &responseError); err != nil || responseError.Error.Code == "" {
		return &l1ResponseError{status: response.StatusCode, method: method, path: path, protocol: &Error{Code: contract.ErrorInternal, Message: fmt.Sprintf("L1 returned HTTP %d", response.StatusCode), Retryable: response.StatusCode >= 500 || response.StatusCode == http.StatusTooManyRequests}}
	}
	return &l1ResponseError{status: response.StatusCode, method: method, path: path, validEnvelope: validL1ErrorEnvelope(responseBody), protocol: &Error{
		Code: responseError.Error.Code, Message: responseError.Error.Message,
		Retryable: responseError.Error.Retryable, Details: responseError.Error.Details,
		RequestID: responseError.Error.RequestID,
	}}
}

var _ JobClient = (*L1Client)(nil)
var _ JobImageEvidenceClient = (*L1Client)(nil)
var _ JobLogClient = (*L1Client)(nil)
var _ ComputerGrantVerifier = (*L1Client)(nil)

// Keep the response origin internal while preserving errors.As(*Error).
type l1ResponseError struct {
	status        int
	method, path  string
	validEnvelope bool
	protocol      *Error
}

func (e *l1ResponseError) Error() string { return e.protocol.Error() }
func (e *l1ResponseError) Unwrap() error { return e.protocol }

// Absence is destructive evidence: require all mandatory envelope fields rather
// than accepting a partial JSON object whose missing fields decode to zero.
func validL1ErrorEnvelope(body []byte) bool {
	var envelope struct {
		Error *struct {
			Code      *string `json:"code"`
			Message   *string `json:"message"`
			Retryable *bool   `json:"retryable"`
		} `json:"error"`
	}
	return json.Unmarshal(body, &envelope) == nil && envelope.Error != nil && envelope.Error.Code != nil && *envelope.Error.Code != "" && envelope.Error.Message != nil && envelope.Error.Retryable != nil
}
